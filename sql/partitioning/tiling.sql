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
    group_by_col varchar(50);
    mobilitydb_type boolean;
    column_sub_type varchar(10);
    tile_key varchar(50);
    table_out_id integer;
    temp text;
BEGIN
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
    SELECT group_by_col_name(table_name_in)
    INTO group_by_col;
    -- Get the column name
    IF spatiotemporal_col_name IS NULL THEN
        SELECT partitioning_column_name(table_name_in)
        INTO spatiotemporal_col_name;
    END IF;
    -- Check whether the column is Postgis or Mobilitydb
    SELECT IsMobilityDBType(table_name_in, spatiotemporal_col_name)
    INTO mobilitydb_type;
    -- Check the partitioning type: Single Point or Sequenece
    SELECT partitioning_column_type(table_name_in, spatiotemporal_col_name)
    INTO column_sub_type;
    -- Get the tile key
    SELECT getTileKey(table_name_in)
    INTO tile_key;

    -- Granularity detection
    IF tiling_granularity IS NULL THEN
        RAISE INFO 'Granularity Detection';
        SELECT tiling_granularity_detection()
        INTO tiling_granularity;
        RAISE INFO 'Granularity: %',tiling_granularity;
    END IF;
    -- Check if the table exists, tell the user to write another table name
    IF lower(tiling_method) = 'crange' THEN
        SELECT crange_method(table_name_in, spatiotemporal_col_name, num_tiles, table_name_out, tiling_granularity, lower(tiling_type),shape_segmentation, group_by_col, mobilitydb_type, column_sub_type, tile_key)
        INTO table_out_id;
    ELSIF lower(tiling_method) = 'colocation' THEN
        SELECT colocation_method(table_name_in, spatiotemporal_col_name, num_tiles, table_name_out, colocation_table, colocation_column,shape_segmentation, group_by_col, mobilitydb_type, column_sub_type, tile_key)
        INTO table_out_id;
    ELSE
        RAISE EXCEPTION 'Please choose one of the following tiling methods: CRANGE, STR, OCTREE, HIERARCHICAL';
    END IF;
    IF table_out_id < 1 THEN
        RAISE EXCEPTION 'Something went wrong with the tiling method!';
    END IF;
    -- Move data into tiles
    IF physical_partitioning THEN
        start_time := clock_timestamp();
        PERFORM spatiotemporal_data_allocation(table_name_in, tile_key,shape_segmentation,spatiotemporal_col_name, group_by_col, mobilitydb_type,column_sub_type, num_tiles, tile_key, table_name_out, table_out_id);
        IF column_sub_type in ('sequence', 'sequenceset', 'linestring', 'polygon') or position('temp' in table_name_in) > 0 THEN
            RAISE INFO 'Run-time for constructing and partitioning segments:%', (clock_timestamp() - start_time);
        ELSIF column_sub_type in ('instant','point') THEN
            RAISE INFO 'Run-time for partitioning:%', (clock_timestamp() - start_time);
        END IF;
    END IF;
    IF column_sub_type not in ('instant', 'point') and mobilitydb_type THEN
        EXECUTE format('%s', concat('DROP TABLE ', table_name_in, '_temp'));
    END IF;
    RAISE INFO 'Total elapsed time:%', (clock_timestamp() - temp_start_time);
    return true;
END;
$$ LANGUAGE 'plpgsql';

-- Move it to the helper functions
CREATE OR REPLACE FUNCTION getTileKey(table_name_in text)
    RETURNS text AS $$
BEGIN
    RETURN 'tile_key';
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION create_catalog_table(table_name_out text, mobilitydb_type boolean)
    RETURNS boolean AS $$
DECLARE
    catalog_table text;
BEGIN
    set client_min_messages to WARNING;
    catalog_table := concat(table_name_out, '_catalog');
    EXECUTE format('%s', concat('DROP TABLE IF EXISTS ', catalog_table));
    IF mobilitydb_type THEN
        EXECUTE format('%s', concat('CREATE TABLE ',catalog_table, '(id serial, bbox stbox, numPoints integer, numShapes integer);'));
    ELSE
        EXECUTE format('%s', concat('CREATE TABLE ',catalog_table, '(id serial, bbox geometry, numPoints integer, numShapes integer);'));
    END IF;
    set client_min_messages to INFO;
    RETURN true;
END;
$$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION getCoordinateSystem(table_name_in text, spatiotemporal_col_name text, mobilitydb_type boolean)
    RETURNS boolean AS $$
DECLARE
    srid int;
BEGIN
    IF mobilitydb_type THEN
        EXECUTE format('%s', concat('SELECT srid(',spatiotemporal_col_name,') FROM ',table_name_in,' LIMIT 1')) INTO srid;
    ELSE
        EXECUTE format('%s', concat('SELECT st_srid(',spatiotemporal_col_name,') FROM ',table_name_in,' LIMIT 1')) INTO srid;
    END IF;
    RETURN srid;
END;
$$ LANGUAGE 'plpgsql';

CREATE TYPE spatiotemporal_extent AS (
    postgis_extent geometry,
    mobilitydb_extent stbox,
    numShapes int,
    numPoints  int
);

CREATE TYPE tile_size AS (
    numShapes int,
    numPoints  int
);

CREATE OR REPLACE FUNCTION getExtentInfo(table_name_in text, spatiotemporal_col_name text, mobilitydb_type boolean, column_sub_type text)
    RETURNS spatiotemporal_extent AS $$
DECLARE
    extent spatiotemporal_extent;
BEGIN
    if column_sub_type = 'instant' THEN
        EXECUTE format('%s', concat('SELECT extent(',spatiotemporal_col_name,'), count(*) from ', table_name_in))
        INTO extent.mobilitydb_extent, extent.numShapes;
        extent.numPoints := extent.numShapes;
    ELSIF column_sub_type = 'point' THEN
        EXECUTE format('%s', concat('SELECT st_extent(',spatiotemporal_col_name,'), count(*) from ', table_name_in))
        INTO extent.postgis_extent, extent.numShapes;
        extent.numPoints := extent.numShapes;
    ELSIF column_sub_type in ('sequence', 'sequenceset') THEN
        EXECUTE format('%s', concat('SELECT extent(',spatiotemporal_col_name,'), count(*), sum(numInstants(',spatiotemporal_col_name,')) from ', table_name_in))
        INTO extent.mobilitydb_extent, extent.numShapes,extent.numPoints;
    elsIF column_sub_type in ('linestring', 'polygon','multilinestring', 'multipolygon') THEN
        EXECUTE format('%s', concat('SELECT st_extent(',spatiotemporal_col_name,'), count(*), sum(st_npoints(',spatiotemporal_col_name,')) from ', table_name_in))
        INTO extent.postgis_extent, extent.numShapes,extent.numPoints;
    end if;
    RETURN extent;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION getTileSize(table_name_in text, spatiotemporal_col_name text, column_sub_type text, srid int, tileSize spatiotemporal_extent)
    RETURNS spatiotemporal_extent AS $$
BEGIN
    IF column_sub_type = 'instant' THEN
        EXECUTE format('%s', concat('SELECT count(distinct '',group_by_col,''), count(*) FROM ',table_name_in,' WHERE setsrid(',spatiotemporal_col_name,',',srid,') && ''',tileSize.mobilitydb_extent,'''::stbox'))
        INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF column_sub_type = 'point' THEN
        EXECUTE format('%s', concat('SELECT count(distinct '',group_by_col,''), count(*) FROM ',table_name_in,' WHERE st_contains(''',tileSize.postgis_extent,'''::geometry,',spatiotemporal_col_name,' )'))
        INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF column_sub_type in ('sequence', 'sequenceset') THEN
        EXECUTE format('%s', concat('SELECT count(*), sum(numInstants(aStbox(',spatiotemporal_col_name,', ''',tileSize.mobilitydb_extent,'''::stbox))) FROM ',table_name_in,' WHERE ',spatiotemporal_col_name,' && ''',tileSize.mobilitydb_extent,'''::stbox'))
        INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF column_sub_type in ('linestring', 'polygon', 'multilinestring', 'multipolygon') THEN
        EXECUTE format('%s', concat('SELECT count(*), sum(st_npoints(st_intersection('',spatiotemporal_col_name,'', ''',tileSize.postgis_extent,'''::geometry))) FROM ',table_name_in,' WHERE ',spatiotemporal_col_name,' && ''',tileSize.postgis_extent,'''::geometry AND st_intersects(', spatiotemporal_col_name, ',''', tileSize.postgis_extent,'''::geometry);'))
        INTO tileSize.numShapes, tileSize.numPoints;
    END IF;
    RETURN tileSize;
END;
$$ LANGUAGE 'plpgsql';