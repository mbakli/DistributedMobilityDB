-- Set search path to allow using some data types from other extensions
SET search_path = SCHEMA,public;
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- MD Tiling - Generic (Spatiotemporal)
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- num_tiles moved after table_name_out (and gained a default) so a
-- reference table can be created without mentioning num_tiles at all; since
-- that changes the parameter type order, the old signature is a distinct
-- overload as far as Postgres is concerned and must be dropped explicitly
-- or it would keep existing alongside this one.
DROP FUNCTION IF EXISTS create_spatiotemporal_distributed_table(text, integer, text, text, text, text, text, text, varchar(50), boolean, boolean);
CREATE OR REPLACE FUNCTION create_spatiotemporal_distributed_table(
                                                                    table_name_in text,
                                                                    table_name_out text,
                                                                    num_tiles integer DEFAULT 1,
                                                                    tiling_method text DEFAULT 'crange',
                                                                    tiling_granularity text default NULL,
                                                                    tiling_type text default NULL,
                                                                    colocation_table text default NULL,
                                                                    colocation_column text default NULL,
                                                                    spatiotemporal_col_name varchar(50) default NULL,
                                                                    physical_partitioning boolean default TRUE,
                                                                    shape_segmentation boolean default TRUE,
                                                                    is_reference_table boolean default FALSE
)
    RETURNS boolean AS $$
DECLARE
    start_time timestamp;
    temp_start_time timestamp;
    mobilitydb_type boolean;
    table_out_id integer;
    temp text;
    tiling tiling;
    staged_source text;
    staged_fresh boolean;
BEGIN
    -- Needed for the hash-staging dblink connection further down (the
    -- package/extension files being present on disk isn't the same as the
    -- extension being CREATEd in this database yet). Via EXECUTE, not a
    -- bare statement, to be certain this utility command runs correctly
    -- from inside a plpgsql function body regardless of Postgres version
    -- quirks.
    EXECUTE 'CREATE EXTENSION IF NOT EXISTS dblink';
    PERFORM checkTileSizeCreations();
    -- Check if the output table exists or not
    EXECUTE format('%s', concat('SELECT to_regclass(''public.', table_name_out, ''')'))
    INTO temp;
    IF temp IS NOT NULL THEN
        RAISE EXCEPTION 'Please use different table name or drop it before calling this function!';
    END IF;

    -- Reference (broadcast) table: no spatiotemporal tiling at all -- just
    -- copy the data into table_name_out and hand it to Citus' own
    -- create_reference_table(), which replicates the whole table to every
    -- node so it can be joined against any distributed table without
    -- repartitioning. tiling_method/etc are all ignored in this path since
    -- there's no tiling to do. num_tiles defaults to 1 so callers don't
    -- need to think about it for a reference table, but a reference table
    -- is never tiled, so any other value is rejected outright rather than
    -- silently ignored.
    IF is_reference_table THEN
        IF num_tiles != 1 THEN
            RAISE EXCEPTION 'num_tiles must be 1 (or omitted) when is_reference_table is true -- reference tables are replicated whole, not tiled; got %', num_tiles;
        END IF;
        RAISE INFO 'Creating reference (broadcast) table %', table_name_out;
        EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING ALL)', table_name_out, table_name_in);
        EXECUTE format('INSERT INTO %I SELECT * FROM %I', table_name_out, table_name_in);
        -- create_reference_table() takes a regclass; casting table_name_out
        -- (plain text) to regclass directly applies standard unquoted-
        -- identifier folding (lowercasing it), which doesn't match the
        -- case-preserved table just created above via %I whenever
        -- table_name_out has any uppercase letters. Quoting it through %I
        -- first makes the regclass cast resolve the exact same identifier.
        EXECUTE format('SELECT create_reference_table(%L::regclass)', format('%I', table_name_out));
        RETURN true;
    END IF;

    -- Auto-tune parallel query settings for the BinarySearch-driven tiling
    -- pass below (see search_in_dimensions.sql for the full rationale);
    -- transaction-local, reverts automatically once this call returns.
    PERFORM AutoTuneParallelQueryForDistribution();
    -- shared_buffers can't be auto-tuned the same way (postmaster-context
    -- GUC, needs a restart) -- just warn if it looks undersized relative to
    -- the source table, so the operator can decide whether to act on it.
    PERFORM WarnIfSharedBuffersUndersized(table_name_in);

    temp_start_time := clock_timestamp();
    -- Preprocessing
    RAISE INFO 'Collecting information:';
    -- Get Primary key column
    SELECT getGroupCol(table_name_in)
    INTO tiling.groupCol;
    -- Get the column name
    IF spatiotemporal_col_name IS NULL THEN
        SELECT getDistributedCol(table_name_in)
        INTO tiling.distCol;
    ELSE
        SELECT spatiotemporal_col_name
        INTO tiling.distCol;
    END IF;

    SELECT getDistColType(table_name_in, tiling.distCol)
    INTO tiling.distColType;

    IF tiling_type IS NULL THEN
        SELECT getTilingType(table_name_in)
        INTO tiling.type;
    ELSE
        tiling.type := tiling_type;
    END IF;

    -- Check whether the column is Postgis or Mobilitydb
    SELECT IsMobilityDBType(table_name_in, tiling)
    INTO tiling.isMobilityDB;
    -- Check the multirelation type: Single Point or Sequenece
    SELECT getDistributedColInternalType(table_name_in, tiling.distCol)
    INTO tiling.internalType;

    IF shape_segmentation THEN
        IF tiling.internaltype in ('point', 'instant') THEN
            tiling.segmentation := false;
        ELSE
            tiling.segmentation := shape_segmentation;
        end if;
    ELSE
        -- Previously missing: with no ELSE, tiling.segmentation stayed NULL
        -- here, which rendered as an invalid empty-string boolean literal
        -- in add_distributed_table_metadata's INSERT.
        tiling.segmentation := false;
    end if;
    -- Get the tile key
    SELECT getTileKey(table_name_in)
    INTO tiling.tileKey;

    -- Get Spatiotemporal information
    SELECT getCoordinateSystem(table_name_in, tiling.distCol, tiling.isMobilityDB)
    INTO tiling.srid;

    -- set the number of tiles
    SELECT num_tiles
    INTO tiling.numTiles;
    -- Granularity detection -- against table_name_in, not staged_source
    -- (which isn't staged yet at this point).
    IF tiling_granularity IS NULL THEN
        RAISE INFO 'Granularity Detection';
        SELECT tilingGranularityDetection(table_name_in, table_name_out, tiling)
        INTO tiling.granularity;
        RAISE INFO 'Granularity: %',tiling.granularity;
    ELSE
        tiling.granularity := tiling_granularity;
    END IF;

    -- Stage table_name_in as hash-distributed + GIST-indexed (if it isn't a
    -- Citus table already), right here where it's first needed (the tiling
    -- method dispatch right below). Run over a SEPARATE dblink connection,
    -- not inline in this function's own transaction: Citus can't safely
    -- create a second distributed table (this staging copy, alongside
    -- table_name_out's own shards further down) within one transaction --
    -- one worker's shard/connection metadata cache is left stale, causing a
    -- "relation ... does not exist" error on the later INSERT. dblink
    -- defaults to autocommit per statement when no explicit BEGIN is sent,
    -- so stage_hash_distributed_source runs and COMMITS entirely within its
    -- own session before this call even returns -- well before this
    -- function's own (still-open) transaction goes on to create
    -- table_name_out's shards below. A blocking call, not the async
    -- send_query/get_result split tried previously: that let staging
    -- overlap with granularity detection's own heavy queries, and the two
    -- running concurrently against the same workers is suspected to have
    -- contributed to an OOM kill on one worker during testing.
    SELECT r.staged_table, r.was_freshly_staged
    INTO staged_source, staged_fresh
    FROM dblink(format('dbname=%s user=%s', current_database(), current_user),
                format('SELECT * FROM stage_hash_distributed_source(%L, %L)', table_name_in, tiling.distCol))
         AS r(staged_table text, was_freshly_staged boolean);
    IF staged_fresh THEN
        RAISE INFO 'Table % hash-distributed for the first time as % (staged in a separate session, already committed)', table_name_in, staged_source;
    END IF;
    -- Check if the table exists, tell the user to write another table name
    IF lower(tiling_method) = 'crange' THEN
        tiling.disjointTiles := TRUE;
        tiling.method := 'crange';
        SELECT crange_method(staged_source, table_name_out, tiling)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'colocation' THEN
        SELECT colocation_method(staged_source, table_name_out, tiling)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'hierarchical' THEN
        tiling.disjointTiles := TRUE;
        tiling.method := 'hierarchical';
        SELECT hierarchical_method(staged_source, table_name_out, tiling)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'period' THEN
        tiling.disjointTiles := TRUE;
        tiling.method := 'period';
        SELECT period_method(staged_source, table_name_out, tiling)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'quadtree' THEN
        tiling.disjointTiles := TRUE;
        tiling.method := 'quadtree';
        SELECT quadtree_method(staged_source, table_name_out, tiling)
        INTO table_out_id;
    ELSE
        RAISE EXCEPTION 'Please choose one of the following tiling methods: CRANGE, HIERARCHICAL, PERIOD, QUADTREE, STR, OCTREE';
    END IF;
    IF table_out_id < 1 THEN
        RAISE EXCEPTION 'Something went wrong with the tiling method!';
    END IF;
    -- Re-fetch the real tile count: crange's always matches tiling.numTiles
    -- by construction, but hierarchical/period/quadtree's real count is only
    -- known after they finish (and tiling was passed by value, so their own
    -- corrected numTiles doesn't propagate back here otherwise).
    SELECT numTiles FROM pg_dist_spatiotemporal_tables WHERE id = table_out_id
    INTO tiling.numTiles;
    -- Move data into tiles. Deliberately table_name_in here, not
    -- staged_source: reproduced directly that INSERT INTO table_name_out
    -- (range-distributed, exactly tiling.numTiles shards via
    -- create_range_shards) SELECT ... FROM staged_source (hash-distributed,
    -- Citus' own default shard count, no colocation relationship to
    -- table_name_out at all) fails with "relation ... does not exist" on
    -- whichever worker Citus picks -- the two tables aren't colocated, so
    -- Citus can't safely push this down shard-by-shard the way it can for a
    -- genuinely local (undistributed) source, which is what table_name_in
    -- actually is here and what this INSERT has always run against
    -- reliably. staged_source is only used above, for the read-only
    -- granularity-detection/tile-boundary-search queries (the confirmed
    -- slow part -- 13m26s of tile generation vs. ~8-10s for this allocation
    -- step against the very same unstaged table), not for this write.
    IF physical_partitioning THEN
        start_time := clock_timestamp();
        PERFORM spatiotemporal_data_allocation(table_name_in, tiling, table_name_out, table_out_id);
        IF tiling.internaltype in ('sequence', 'sequenceset', 'linestring', 'polygon') or position('temp' in table_name_in) > 0 THEN
            RAISE INFO 'Run-time for constructing and multirelation segments:%', (clock_timestamp() - start_time);
        ELSIF tiling.internaltype in ('instant','point') THEN
            RAISE INFO 'Run-time for multirelation:%', (clock_timestamp() - start_time);
        END IF;
        -- table_name_out itself was never indexed anywhere in this pipeline
        -- -- only the reshuffled copy Neighbor Scan builds on demand gets a
        -- GIST index, via IndexReshuffledData() in multi_phase_executor.c.
        -- Colocation-strategy (Self Tiling Scan) queries run directly
        -- against table_name_out, so without this they fall back to a full
        -- unindexed cross-tile scan -- reproduced directly: a 392-row single
        -- tile self-join ran ~10.5s unindexed, with EXPLAIN (ANALYZE)
        -- confirming no index-based plan was even available (forcing
        -- enable_seqscan=off still produced a Seq Scan). Same %I/gist
        -- pattern stage_hash_distributed_source already uses for its own
        -- staged copy (data_allocation.sql).
        start_time := clock_timestamp();
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING gist (%I)',
                       concat(table_name_out, '_', tiling.distCol, '_gist_idx'), table_name_out, tiling.distCol);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING btree (%I)',
                       concat(table_name_out, '_', tiling.groupCol, '_btree_idx'), table_name_out, tiling.groupCol);
        RAISE INFO 'Run-time for indexing %:%', table_name_out, (clock_timestamp() - start_time);
    END IF;
    -- IF EXISTS: only crange's point-based preprocessing creates this _temp
    -- table; other methods' point-based support never creates it. Named
    -- from staged_source (what actually got passed into crange_method
    -- above, which builds the _temp table's name from whatever table_name_in
    -- value it receives), not the original table_name_in parameter -- those
    -- differ whenever staging actually ran, and dropping the wrong name here
    -- would silently leave the real _temp table behind.
    IF tiling.internaltype not in ('instant', 'point') and tiling.granularity = 'point-based' THEN
        EXECUTE format('%s', concat('DROP TABLE IF EXISTS ', staged_source, '_temp'));
    END IF;
    RAISE INFO 'Total elapsed time:%', (clock_timestamp() - temp_start_time);
    return true;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION getTileSize(table_name_in text, tiling tiling, tileSize tileSize)
    RETURNS tileSize AS $$
BEGIN
    IF tiling.internaltype = 'instant' THEN
        EXECUTE format('%s', concat('SELECT count(distinct '',group_by_col,''), count(*) FROM ',table_name_in,' WHERE setsrid(',tiling.distCol,',',tiling.srid,') && ''',tileSize.mobilitydb_extent,'''::stbox'))
        INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF tiling.internaltype = 'point' THEN
        EXECUTE format('%s', concat('SELECT count(distinct '',group_by_col,''), count(*) FROM ',table_name_in,' WHERE st_contains(''',tileSize.postgis_extent,'''::geometry,',tiling.distCol,' )'))
        INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF tiling.internaltype in ('sequence', 'sequenceset') THEN
        EXECUTE format('%s', concat('SELECT count(*), sum(numInstants(atStbox(',tiling.distCol,', ''',tileSize.mobilitydb_extent,'''::stbox))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && ''',tileSize.mobilitydb_extent,'''::stbox'))
        INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF tiling.internaltype in ('linestring', 'polygon', 'multilinestring', 'multipolygon') THEN
        EXECUTE format('%s', concat('SELECT count(*), sum(st_npoints(st_intersection('',tiling.distCol,'', ''',tileSize.postgis_extent,'''::geometry))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && ''',tileSize.postgis_extent,'''::geometry AND st_intersects(', tiling.distCol, ',''', tileSize.postgis_extent,'''::geometry);'))
        INTO tileSize.numShapes, tileSize.numPoints;
    END IF;
    RETURN tileSize;
END;
$$ LANGUAGE 'plpgsql';