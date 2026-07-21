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
    /*
     * The exploded-per-instant _temp table is crange_method's own
     * point-based preprocessing convention (create_temporary_points_table,
     * see crange.sql) -- hierarchical_method/period_method also support
     * tiling.granularity = 'point-based' (weighting each row's bucket
     * assignment by its instant count via WeightedNtileExpr) but do so
     * directly against the original whole-trajectory table, without ever
     * creating that exploded table, so swapping to it here for their
     * point-based runs errored with "relation ... _temp does not exist".
     * Scoped to tiling.method = 'crange' specifically rather than to
     * granularity alone.
     */
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
    /*
     * The distributed column holds large TOASTed values (whole/clipped
     * trajectories for MobilityDB, geometries for PostGIS) -- switching its
     * TOAST compression from the default pglz to lz4 (much faster to
     * compress, at a modest space cost) cuts the segmentation/allocation
     * INSERT's dominant cost, which is writing these rows, not scanning
     * them (confirmed via EXPLAIN ANALYZE: the write phase alone accounts
     * for the majority of this step's time, and that write can't be sped
     * up by parallel workers -- Postgres disables parallel query entirely
     * for any statement containing a write, regardless of GUCs). Measured
     * ~2.75x faster (58.8s -> 21.4s) on a same-data before/after comparison
     * of this exact INSERT. No effect on query results, only on-disk
     * compression of this one column.
     */
    EXECUTE format('%s', concat('ALTER TABLE ', table_name_out,' ALTER COLUMN ', tiling.distCol,' SET COMPRESSION lz4;'));
    -- Add the distributed column
    EXECUTE format('%s', concat('ALTER TABLE ',table_name_out, ' ADD column ',tiling.tileKey,' integer'));
    -- Distribute the table using range multirelation
    EXECUTE format('%s', concat('SELECT create_distributed_table(''', table_name_out,''', ''',tiling.tileKey,''', ''range'');'));
    -- Modify the shard ranges in the Citus Catalog as Citus does not support range multirelation by default
    EXECUTE format('%s', concat('SELECT create_range_shards(', tiling.numTiles,' , ''', table_name_out, ''');'));
    -- Get the table columns
    IF tiling.isMobilityDB THEN
        EXECUTE format('%s', concat('
		SELECT array_to_string(array_agg(concat(''t1.'', column_name))::text[], '','')
		FROM information_schema.columns
		WHERE table_schema = ''public''
			AND table_name   = ''',org_table_name_in,''' ;'))
            INTO org_table_columns;
    ELSE
        EXECUTE format('%s', concat('
		SELECT array_to_string(array_agg(concat(''t1."'', column_name,''"''))::text[], '','')
		FROM information_schema.columns
		WHERE table_schema = ''public''
			AND table_name   = ''',org_table_name_in,''' ;'))
            INTO org_table_columns;
    end if;

    -- update srid
    --EXECUTE format('%s', concat('update ', table_name_out, ' set ', tiling.distCol, ' = st_setsrid(',tiling.distCol,', ',tiling.srid,')'));

    -- Group by clause preparation
    group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1.',tiling.distCol,')'), ''), ',,',','), '^,',''), ',$','');
    IF group_by_clause = org_table_columns THEN
        group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1."',tiling.distCol,'")'), ''), ',,',','), '^,',''), ',$','');
    END IF;
    starttime := clock_timestamp();

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
        /*
         * Mirrors the linestring/polygon branch above: replicate each trip
         * whole (unclipped) into every tile it overlaps, rather than
         * segmenting/clipping it (see segmentation_and_allocation for the
         * clipping counterpart, selected instead whenever
         * tiling.segmentation is true). Previously missing entirely --
         * shape_segmentation => false (the "replicate" choice) fell into
         * the catch-all ELSE below and errored out for any MobilityDB
         * sequence/sequenceset table (e.g. BerlinMOD trips), even though
         * the segmenting path already worked.
         */
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