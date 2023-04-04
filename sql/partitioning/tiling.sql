-- Set search path to allow using some data types from other extensions
SET search_path = SCHEMA,public;
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- MD Tiling - Generic (Spatiotemporal)
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_spatiotemporal_distributed_table(
                                                                    table_name_in text,
                                                                    num_tiles integer,
                                                                    table_name_out text,
                                                                    tiling_method text DEFAULT 'crange',
                                                                    tiling_granularity text default NULL,
                                                                    tiling_type text default NULL,
                                                                    colocation_table text default NULL,
                                                                    colocation_column text default NULL,
                                                                    spatiotemporal_col_name varchar(50) default NULL,
                                                                    physical_partitioning boolean default TRUE,
                                                                    shape_segmentation boolean default TRUE
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
    END IF;

    tiling.segmentation := shape_segmentation;


    -- Check whether the column is Postgis or Mobilitydb
    SELECT IsMobilityDBType(table_name_in, tiling)
    INTO tiling.isMobilityDB;
    -- Check the partitioning type: Single Point or Sequenece
    SELECT getDistributedColInternalType(table_name_in, tiling.distCol)
    INTO tiling.internalType;
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
    END IF;
    -- Check if the table exists, tell the user to write another table name
    IF lower(tiling_method) = 'crange' THEN
        tiling.disjointTiles := TRUE;
        SELECT crange_method(table_name_in, table_name_out, tiling)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'colocation' THEN
        SELECT colocation_method(table_name_in, table_name_out, tiling)
        INTO table_out_id;
    ELSE
        RAISE EXCEPTION 'Please choose one of the following tiling methods: CRANGE, HIERARCHICAL, STR, OCTREE, Quadtree';
    END IF;
    IF table_out_id < 1 THEN
        RAISE EXCEPTION 'Something went wrong with the tiling method!';
    END IF;
    -- Move data into tiles
    IF physical_partitioning THEN
        start_time := clock_timestamp();
        PERFORM spatiotemporal_data_allocation(table_name_in, tiling, table_name_out, table_out_id);
        IF tiling.internaltype in ('sequence', 'sequenceset', 'linestring', 'polygon') or position('temp' in table_name_in) > 0 THEN
            RAISE INFO 'Run-time for constructing and partitioning segments:%', (clock_timestamp() - start_time);
        ELSIF tiling.internaltype in ('instant','point') THEN
            RAISE INFO 'Run-time for partitioning:%', (clock_timestamp() - start_time);
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
        EXECUTE format('%s', concat('SELECT count(*), sum(numInstants(aStbox(',tiling.distCol,', ''',tileSize.mobilitydb_extent,'''::stbox))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && ''',tileSize.mobilitydb_extent,'''::stbox'))
        INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF tiling.internaltype in ('linestring', 'polygon', 'multilinestring', 'multipolygon') THEN
        EXECUTE format('%s', concat('SELECT count(*), sum(st_npoints(st_intersection('',tiling.distCol,'', ''',tileSize.postgis_extent,'''::geometry))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && ''',tileSize.postgis_extent,'''::geometry AND st_intersects(', tiling.distCol, ',''', tileSize.postgis_extent,'''::geometry);'))
        INTO tileSize.numShapes, tileSize.numPoints;
    END IF;
    RETURN tileSize;
END;
$$ LANGUAGE 'plpgsql';