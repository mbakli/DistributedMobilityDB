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
BEGIN
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
        /*
         * Previously missing: with no ELSE, tiling.segmentation stayed
         * NULL (its palloc0 default) whenever the caller explicitly passed
         * shape_segmentation => false, since the outer IF was never
         * entered at all. add_distributed_table_metadata then embedded
         * that NULL as a literal empty string in its dynamic INSERT text
         * (concat() renders NULL as ''), producing "invalid input syntax
         * for type boolean: ''" -- shape_segmentation => false was
         * unreachable end-to-end even after fixing shape_allocation's
         * missing sequence/sequenceset branch.
         */
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
    -- Granularity detection
    IF tiling_granularity IS NULL THEN
        RAISE INFO 'Granularity Detection';
        SELECT tilingGranularityDetection(table_name_in, table_name_out, tiling)
        INTO tiling.granularity;
        RAISE INFO 'Granularity: %',tiling.granularity;
    ELSE
        tiling.granularity := tiling_granularity;
    END IF;
    -- Check if the table exists, tell the user to write another table name
    IF lower(tiling_method) = 'crange' THEN
        tiling.disjointTiles := TRUE;
        tiling.method := 'crange';
        SELECT crange_method(table_name_in, table_name_out, tiling)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'colocation' THEN
        SELECT colocation_method(table_name_in, table_name_out, tiling)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'hierarchical' THEN
        tiling.disjointTiles := TRUE;
        tiling.method := 'hierarchical';
        SELECT hierarchical_method(table_name_in, table_name_out, tiling)
        INTO table_out_id;
    ELSE
        RAISE EXCEPTION 'Please choose one of the following tiling methods: CRANGE, HIERARCHICAL, STR, OCTREE, Quadtree';
    END IF;
    IF table_out_id < 1 THEN
        RAISE EXCEPTION 'Something went wrong with the tiling method!';
    END IF;
    /*
     * crange's real tile count always equals tiling.numTiles by
     * construction (it's the fixed target the whole algorithm builds
     * around), so this was never needed before -- but hierarchical_method's
     * real leaf count (periods x per-period spatial chunks) is only known
     * after it finishes, and tiling was passed to it by value, so its own
     * corrected tiling.numTiles never propagates back to this outer copy.
     * Re-fetching it from the catalog row hierarchical_method (or crange)
     * just wrote keeps citus.shard_count/create_range_shards below (inside
     * spatiotemporal_data_allocation) in sync with the actual tile count
     * regardless of which method produced it -- a no-op for crange, whose
     * count already matched.
     */
    SELECT numTiles FROM pg_dist_spatiotemporal_tables WHERE id = table_out_id
    INTO tiling.numTiles;
    -- Move data into tiles
    IF physical_partitioning THEN
        start_time := clock_timestamp();
        PERFORM spatiotemporal_data_allocation(table_name_in, tiling, table_name_out, table_out_id);
        IF tiling.internaltype in ('sequence', 'sequenceset', 'linestring', 'polygon') or position('temp' in table_name_in) > 0 THEN
            RAISE INFO 'Run-time for constructing and multirelation segments:%', (clock_timestamp() - start_time);
        ELSIF tiling.internaltype in ('instant','point') THEN
            RAISE INFO 'Run-time for multirelation:%', (clock_timestamp() - start_time);
        END IF;
    END IF;
    IF tiling.internaltype not in ('instant', 'point') and tiling.granularity = 'point-based' THEN
        EXECUTE format('%s', concat('DROP TABLE ', table_name_in, '_temp'));
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