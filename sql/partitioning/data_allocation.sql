--------------------------------------------------------------------------------------------------------------------------------------------------------
-- stage_hash_distributed_source
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- stage_hash_distributed_source ensures table_name_in has a hash-distributed
-- copy available before the per-tile spatiotemporal allocation scan below
-- runs against it, and returns whichever table name the allocation should
-- actually read from.
--
-- Why this helps: shape_allocation/segmentation_and_allocation each run one
-- "INSERT INTO table_name_out SELECT ... FROM table_name_in, pg_dist_spatiotemporal_tiles
-- WHERE ... distCol && tile_bbox ..." per tile -- a real spatial join
-- against every row of table_name_in, once per tile. Against a single-node
-- (non-Citus) table, that whole scan runs on the coordinator alone for
-- every tile in turn; against a hash-distributed copy with a GIST index on
-- distCol, each worker can index-scan just its own shard for each tile's
-- bbox, and Citus fans the per-tile queries out across every worker in
-- parallel instead -- the same shape of speedup already confirmed
-- empirically elsewhere in this project (an unindexed, single-node spatial
-- join vs. an indexed, sharded one).
--
-- If table_name_in is already a Citus table of any kind (hash/range/
-- reference), it's used as-is -- staging only helps a genuinely single-node
-- source. The staged copy lives in dist_mobilitydb (this extension's own
-- internal catalog schema, created at CREATE EXTENSION time and never
-- exposed to users the way table_name_in/table_name_out are -- see e.g.
-- pg_spatiotemporal_join_operations in that same schema), not public, so
-- it's only ever reachable through this planning code. Citus creates every
-- shard of a distributed table in its parent's own schema, so putting the
-- staging table in dist_mobilitydb also means its shards land in
-- dist_mobilitydb on every worker with no extra step needed. The staged
-- copy is deliberately never dropped once built: naming it deterministically
-- from table_name_in and checking to_regclass first means a second
-- create_spatiotemporal_distributed_table call against the same source
-- table (a different tiling method, or a different table_name_out) reuses
-- it instead of repeating the copy+distribute+index work.
--
-- was_freshly_staged (OUT) tells the caller whether this call actually just
-- created the staging copy (true) or found an existing/no-op case (false --
-- already a Citus table, or an existing staging copy reused). This matters
-- because of a real Citus limitation, reproduced directly: creating a new
-- distributed table (this staging copy, via create_distributed_table) and
-- then creating a *different* distributed table (the tiled table_name_out,
-- via create_range_shards) in the same transaction leaves one worker's
-- shard/connection metadata cache stale by the time the later INSERT runs
-- against it, failing with "relation ... does not exist" on whatever shard
-- landed there -- confirmed by the exact same pipeline succeeding end-to-end
-- (157,630 rows, no error) when the input was already a Citus table
-- (nothing new got distributed in that transaction at all) vs. failing
-- every time the input needed fresh staging first. create_spatiotemporal_
-- distributed_table is a FUNCTION, not a PROCEDURE, so it can't COMMIT
-- mid-call to separate these two distributed-table-creation events itself
-- -- see its own call site for how it surfaces this to the caller instead.
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION stage_hash_distributed_source(table_name_in text, distCol text,
                                                          OUT staged_table text, OUT was_freshly_staged boolean)
    AS $$
DECLARE
    staged_ident text;
    hash_col text;
    already_distributed boolean;
    bare_name text;
BEGIN
    was_freshly_staged := false;
    SELECT EXISTS (
        SELECT 1 FROM pg_dist_partition WHERE logicalrelid = table_name_in::regclass
    ) INTO already_distributed;

    IF already_distributed THEN
        staged_table := table_name_in;
        RETURN;
    END IF;

    -- Resolve the bare table name via the regclass itself rather than
    -- assuming table_name_in is unqualified -- it can already be schema-
    -- qualified here (e.g. crange's own dist_mobilitydb.<x>_temp point-based
    -- preprocessing table, itself already staged once by a caller further
    -- up), and naively concatenating '_hash' onto an already schema-
    -- qualified string would double-qualify it into one broken identifier
    -- (e.g. "dist_mobilitydb.dist_mobilitydb.x_temp_hash") instead of a
    -- real schema.table pair.
    SELECT c.relname INTO bare_name
    FROM pg_class c WHERE c.oid = table_name_in::regclass;

    staged_ident := concat(bare_name, '_hash');
    staged_table := format('%I.%I', 'dist_mobilitydb', staged_ident);

    IF to_regclass(staged_table) IS NOT NULL THEN
        RETURN;
    END IF;

    hash_col := getGroupColWithFallback(bare_name);

    -- Bare "LIKE table_name_in", not "... INCLUDING ALL": the source table
    -- can carry its own UNIQUE/EXCLUDE/PRIMARY KEY constraints that don't
    -- include hash_col (e.g. trips' own UNIQUE (vehicleid, startdate,
    -- seqno), which has nothing to do with tripid) -- INCLUDING ALL copies
    -- those over verbatim, and create_distributed_table below then rejects
    -- the staging table outright with "cannot create constraint ... does
    -- not include the partition column" (reproduced directly against the
    -- real trips table). This is a throwaway internal copy solely to let
    -- the allocation scan below run distributed/indexed instead of as one
    -- big local scan -- it doesn't need the source's own constraints or
    -- indexes at all, only matching columns (which bare LIKE already
    -- provides, NOT NULL included) plus the one GIST index added explicitly
    -- right after.
    EXECUTE format('CREATE TABLE %s (LIKE %I)', staged_table, table_name_in);
    EXECUTE format('SELECT create_distributed_table(%L::regclass, %L)', staged_table, hash_col);
    EXECUTE format('INSERT INTO %s SELECT * FROM %I', staged_table, table_name_in);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %s USING gist (%I)',
                   concat(staged_ident, '_', distCol, '_gist_idx'), staged_table, distCol);
    -- Without this, a brand-new table has zero planner statistics
    -- (pg_stat_user_tables.last_analyze stays NULL) and the tile-generation
    -- queries that read staged_table right after this returns get whatever
    -- plan the planner guesses blind -- reproduced directly: the exact same
    -- 157,630-row trips table took 9m20s for tile generation freshly staged
    -- and unanalyzed here, vs. 44s against an otherwise-equivalent (same
    -- shard count, same GIST index) but already-ANALYZEd hash-distributed
    -- copy built earlier this session. Same root cause as the cluster-wide
    -- ANALYZE gap found and fixed separately (tune_cluster_performance.sh)
    -- -- that fix only helps tables that already existed when it ran, not
    -- one freshly created afterward by this function.
    EXECUTE format('ANALYZE %s', staged_table);

    was_freshly_staged := true;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Data Distribution
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION spatiotemporal_data_allocation(table_name_in varchar(250), tiling tiling, table_name_out varchar(250), table_id integer)
    RETURNS boolean AS $$
DECLARE
    org_table_columns text;
    starttime timestamp;
    group_by_clause text;
    org_table_name_in varchar(250);
BEGIN
    -- The exploded-per-instant _temp table is crange_method's own point-based
    -- preprocessing convention; hierarchical/period_method also support
    -- point-based but weight against the original table directly, so this
    -- swap is scoped to tiling.method='crange' (else "_temp does not exist").
    IF tiling.method = 'crange' and tiling.isMobilityDB and tiling.internalType not in('point','polygon', 'instant') and tiling.granularity = 'point-based' THEN
        org_table_name_in := table_name_in;
        table_name_in := concat(table_name_in, '_temp');
    ELSE
        org_table_name_in := table_name_in;
    END IF;
    -- Set the shards count
    EXECUTE format('%s', concat('set citus.shard_count to ',tiling.numTiles));
    -- Create another table
    set client_min_messages to WARNING;
    EXECUTE format('%s', concat('DROP TABLE IF EXISTS ',table_name_out)); -- Does not work with Citus
    set client_min_messages to INFO;
    EXECUTE format('%s', concat('CREATE TABLE ',table_name_out,' (LIKE ', org_table_name_in, '); '));
    IF not tiling.isMobilityDB and tiling.internaltype = 'linestring' THEN
        EXECUTE format('%s', concat('ALTER TABLE ', table_name_out,' ALTER COLUMN ', tiling.distCol,' TYPE geometry;'));
    END IF;
    -- lz4 TOAST compression is faster to compress than the default pglz,
    -- cutting the dominant cost of the segmentation/allocation INSERT (the
    -- write itself, which parallel workers can't help with). Measured
    -- ~2.75x faster (58.8s -> 21.4s); no effect on query results.
    EXECUTE format('%s', concat('ALTER TABLE ', table_name_out,' ALTER COLUMN ', tiling.distCol,' SET COMPRESSION lz4;'));
    -- Add the distributed column
    EXECUTE format('%s', concat('ALTER TABLE ',table_name_out, ' ADD column ',tiling.tileKey,' integer'));
    -- Distribute the table using range multirelation
    EXECUTE format('%s', concat('SELECT create_distributed_table(''', table_name_out,''', ''',tiling.tileKey,''', ''range'');'));
    -- Modify the shard ranges in the Citus Catalog as Citus does not support range multirelation by default
    EXECUTE format('%s', concat('SELECT create_range_shards(', tiling.numTiles,' , ''', table_name_out, ''');'));
    -- Get the table columns
    -- Resolved via pg_attribute/regclass, not information_schema.columns
    -- string-matched by table_name -- see the identical comment in
    -- getDistColType (catalog_info_check.sql). org_table_name_in is now
    -- routinely a schema-qualified dist_mobilitydb.* staged copy (see
    -- stage_hash_distributed_source / create_spatiotemporal_distributed_table),
    -- and information_schema.columns.table_name only ever holds the *bare*
    -- name -- a schema-qualified string never matched it at all, silently
    -- returning a NULL/empty column list instead of erroring, which then
    -- surfaced many statements later as a plain SQL syntax error ("SELECT
    -- ,tile_key ...", the leading column list missing entirely). No EXECUTE
    -- needed either -- org_table_name_in::regclass is a plain expression,
    -- not something that needs building as dynamic SQL text. ORDER BY
    -- a.attnum preserves column order explicitly, where the previous
    -- information_schema query relied on unordered aggregation.
    IF tiling.isMobilityDB THEN
        SELECT array_to_string(array_agg(concat('t1.', a.attname) ORDER BY a.attnum), ',')
        INTO org_table_columns
        FROM pg_attribute a
        WHERE a.attrelid = org_table_name_in::regclass
          AND a.attnum > 0 AND NOT a.attisdropped;
    ELSE
        SELECT array_to_string(array_agg(concat('t1."', a.attname, '"') ORDER BY a.attnum), ',')
        INTO org_table_columns
        FROM pg_attribute a
        WHERE a.attrelid = org_table_name_in::regclass
          AND a.attnum > 0 AND NOT a.attisdropped;
    end if;

    -- update srid
    --EXECUTE format('%s', concat('update ', table_name_out, ' set ', tiling.distCol, ' = st_setsrid(',tiling.distCol,', ',tiling.srid,')'));

    -- Group by clause preparation
    group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1.',tiling.distCol,')'), ''), ',,',','), '^,',''), ',$','');
    IF group_by_clause = org_table_columns THEN
        group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1."',tiling.distCol,'")'), ''), ',,',','), '^,',''), ',$','');
    END IF;
    starttime := clock_timestamp();

    -- table_name_in here is used as-is -- NOT staged -- deliberately.
    -- create_spatiotemporal_distributed_table (tiling.sql) already stages
    -- table_name_in for the read-only granularity-detection/tile-boundary
    -- queries earlier in the pipeline, but reproduced directly that using
    -- that same hash-distributed staging copy as the source of *this*
    -- INSERT INTO table_name_out (range-distributed, exactly tiling.numTiles
    -- shards) fails with "relation ... does not exist" on whichever worker
    -- Citus picks: the staging copy and table_name_out aren't colocated (no
    -- colocate_with was given, and couldn't be -- hash vs. range, different
    -- distribution columns entirely), so Citus can't safely push this
    -- INSERT...SELECT down shard-by-shard the way it does for a genuinely
    -- local (undistributed) source, which is what table_name_in actually is
    -- by the time it reaches this function -- and is what this INSERT has
    -- always run against reliably.
    IF tiling.segmentation THEN
        PERFORM segmentation_and_allocation(table_name_in, org_table_name_in,tiling, table_name_out, table_id, org_table_columns, group_by_clause);
    ELSE
        PERFORM shape_allocation(table_name_in, tiling, table_name_out, table_id, org_table_columns, group_by_clause);
    END IF;
    -- Update the catalog to add the distributed table oid
    PERFORM update_catalog_oid(table_name_out,table_id);
    return true;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Shape allocation
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION shape_allocation(table_name_in varchar(250), tiling tiling, table_name_out varchar(250), table_id integer,org_table_columns text, group_by_clause text)
    RETURNS boolean AS $$
DECLARE
    bbox_with_srid text;
BEGIN
    IF tiling.isMobilityDB THEN
        SELECT concat('setsrid(mobdb_bbox, ',tiling.srid,')')
        INTO bbox_with_srid;
    ELSE
        SELECT concat('st_setsrid(postgis_bbox, ',tiling.srid,')')
        INTO bbox_with_srid;
    END IF;

    IF tiling.internaltype = 'instant' THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles:',tiling.internaltype;
        SELECT replace(org_table_columns, concat(',',tiling.distCol), concat(',',tiling.distCol))
        INTO org_table_columns;
        -- Insert all overlapping points + points that touch the tile boundries
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,'
                AND ', tiling.distCol,' && ',bbox_with_srid,'
            GROUP BY ',tiling.tileKey,',', group_by_clause));
    ELSIF tiling.internaltype = 'point' THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles:', tiling.internaltype;
        -- Insert all overlapping points + points that touch the tile boundries
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,'
                and (st_contains(',bbox_with_srid,',', tiling.distCol,') or st_intersects(ST_Boundary(',bbox_with_srid,'), ',tiling.distCol,'))
            GROUP BY ',tiling.tileKey,',', group_by_clause));
    ELSIF tiling.internaltype in  ('linestring','polygon') THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles without segmenting (i.e., replication) them:',tiling.internaltype;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', tiling.distCol,' && ',bbox_with_srid));
    ELSIF tiling.internaltype in ('sequence','sequenceset') THEN
        -- Mirrors linestring/polygon above: replicate each trip whole into
        -- every tile it overlaps (vs. segmentation_and_allocation's clipping
        -- counterpart). Was previously missing, so shape_segmentation=>false
        -- errored out for MobilityDB sequence/sequenceset tables.
        RAISE INFO 'Distributing the %s into the overlapping tiles without segmenting (i.e., replication) them:',tiling.internaltype;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', tiling.distCol,' && ',bbox_with_srid));
    ELSE
        RAISE Exception 'The column type is not detected!';
    END IF;
    return true;
END;
$$ LANGUAGE 'plpgsql';