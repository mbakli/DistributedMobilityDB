CREATE OR REPLACE FUNCTION crange_method(table_name_in text, spatiotemporal_col_name varchar(50), num_tiles integer, table_name_out text, tiling_granularity smallint, tiling_type text ,shape_segmentation boolean, group_by_col varchar(50), mobilitydb_type boolean, column_sub_type varchar(10), shard_key varchar(50))
    RETURNS integer AS $$
DECLARE
    extent_info spatiotemporal_extent;
    tileSize spatiotemporal_extent;
    start_time timestamp;
    start_time_temp timestamp;
    input_column_sub_type varchar(10);
    table_out_id integer;
    timePeriod tstzrange;
    shard_numPoints bigint;
    tendVal timestamptz;
    tstartVal timestamptz;
    catalog_table text;
    counter integer;
    xstartVal float;
    ystartVal float;
    xendVal float;
    yendVal float;
    srid integer;
    dim_role integer;
    result text;
    org_table_name_in text;
    shard_trips integer;
BEGIN
    start_time := clock_timestamp();

    --------------------------------------------------------------------------------------------------------------------------------------------------------
    --Preprocessing
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    input_column_sub_type := column_sub_type;

    -- Determine how to partition the table
    IF column_sub_type in ('instant', 'point') THEN
        shape_segmentation := FALSE;
    ELSIF column_sub_type in ('linestring', 'polygon', 'multilinestring', 'multipolygon', 'sequence','sequenceset') THEN
        org_table_name_in := table_name_in;
        IF tiling_granularity = 0 THEN
            -- Generate a new temporary distributed table to store the object points
            PERFORM create_temporary_points_table(table_name_in, spatiotemporal_col_name, group_by_col, mobilitydb_type ,concat(table_name_in , '_temp'));
            table_name_in := concat(table_name_in , '_temp');
            IF mobilitydb_type THEN
                column_sub_type := 'instant';
            ELSE
                column_sub_type := 'point';
            END IF;
        END IF;
    END IF;

    -- Partition data
    IF mobilitydb_type THEN
        RAISE INFO 'MobilityDB Column: % - TGeomPoint(%)', spatiotemporal_col_name, input_column_sub_type;
    ELSE
        RAISE INFO 'PostGIS Column: % - Geometry(%)', spatiotemporal_col_name, input_column_sub_type;
    END IF;

    IF tiling_type = 'temporal' THEN
        IF mobilitydb_type THEN
            RAISE INFO 'Generate Temporal Partitions';
        ELSE
            RAISE Exception 'The temporal partitioning can not be applied on a spatial type!';
        END IF;
    ELSIF tiling_type = 'spatial' THEN
        RAISE INFO 'Generate Spatial Partitions';
    ELSIF tiling_type = 'spatiotemporal' THEN
        IF mobilitydb_type THEN
            RAISE INFO 'Generate Spatiotemporal Partitions:';
        ELSE
            RAISE Exception 'The spatiotemporal partitioning can not be applied on a spatial type!';
        END IF;
    ELSE
        RAISE Exception 'Please choose one of the following: temporal, spatial, or spatiotemporal';
    END IF;
    -- Create the catalog table
    PERFORM create_catalog_table(table_name_out, mobilitydb_type);
    -- Get Spatiotemporal information
    SELECT getCoordinateSystem(table_name_in, spatiotemporal_col_name, mobilitydb_type)
    INTO srid;

    SELECT getExtentInfo(table_name_in, spatiotemporal_col_name, mobilitydb_type, column_sub_type)
    INTO extent_info;
--------------------------------------------------------------------------------------------------------------------------------------------------------
--Partitioning
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Partition data
    RAISE INFO 'Generating Tiles:';
    -- Initialization
    counter := 1;
    IF mobilitydb_type THEN
        tstartVal := tmin(extent_info.mobilitydb_extent);
        tendVal := tmax(extent_info.mobilitydb_extent);
        timePeriod := tstzrange(tstartVal, tendVal);
        shard_numPoints := extent_info.numPoints / num_tiles;
        shard_trips := extent_info.numPoints / num_tiles;
        xstartVal := xmin(extent_info.mobilitydb_extent);
        xendVal := xmax(extent_info.mobilitydb_extent);
        ystartVal := ymin(extent_info.mobilitydb_extent);
        yendVal := ymax(extent_info.mobilitydb_extent);
    ELSE
        shard_numPoints := extent_info.numPoints / num_tiles;
        shard_trips := extent_info.numShapes / num_tiles;
        xstartVal := st_xmin(extent_info.postgis_extent);
        xendVal := st_xmax(extent_info.postgis_extent);
        ystartVal := st_ymin(extent_info.postgis_extent);
        yendVal := st_ymax(extent_info.postgis_extent);
    END IF;

    SELECT get_next_dim(tiling_type, 0)
    INTO dim_role; -- 1 means temporal, 2 means x, 3 means y
    LOOP
    -- T Partitioning
    IF dim_role = 1 THEN
        IF counter < num_tiles THEN
            SELECT BinarySearch(dim_role, table_name_in, spatiotemporal_col_name, mobilitydb_type, column_sub_type, shape_segmentation,shard_numPoints, shard_trips,num_tiles, xstartVal, xendVal, ystartVal, yendVal, srid, tstartVal,tendVal)
            INTO result;
            tendVal := result::timestamptz;
            tileSize.mobilitydb_extent := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan);
            tstartVal := tendVal ;
            tendVal := tmax(extent_info.mobilitydb_extent);
        ELSE
            tileSize.mobilitydb_extent := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan);
        END IF;
        -- X Partitioning
        ELSIF dim_role = 2 THEN
            IF counter < num_tiles THEN
                IF mobilitydb_type THEN
                    SELECT BinarySearch(dim_role, table_name_in, spatiotemporal_col_name, mobilitydb_type, column_sub_type, shape_segmentation,shard_numPoints, shard_trips, num_tiles, xstartVal, xendVal, ystartVal, yendVal, srid,tstartVal,tendVal)
                    INTO result;
                    xendVal := result::float;
                    tileSize.mobilitydb_extent := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan);
                    xstartVal := xendVal ;
                    xendVal := xmax(extent_info.mobilitydb_extent);
                ELSE
                    SELECT BinarySearch(dim_role, table_name_in, spatiotemporal_col_name, mobilitydb_type, column_sub_type, shape_segmentation,shard_numPoints, shard_trips, num_tiles, xstartVal, xendVal, ystartVal, yendVal, srid,'1900-01-01'::timestamp, '1900-01-01'::timestamp)
                    INTO result;
                    xendVal := result::float;
                    tileSize.postgis_extent := ST_MakeEnvelope(xstartVal,ystartVal,xendVal, yendVal, srid);
                    xstartVal := xendVal ;--+ stepX;
                    xendVal := st_xmax(extent_info.postgis_extent);
                END IF;
            ELSE
                IF mobilitydb_type THEN
                    tileSize.mobilitydb_extent := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan);
                ELSE
                    tileSize.postgis_extent := ST_MakeEnvelope(xstartVal,ystartVal,xendVal, yendVal, srid);
                END IF;
            END IF;
        -- Y Partitioning
        ELSIF dim_role = 3 THEN
            IF counter < num_tiles THEN
                IF mobilitydb_type THEN
                    SELECT BinarySearch(dim_role, table_name_in, spatiotemporal_col_name, mobilitydb_type, column_sub_type, shape_segmentation,shard_numPoints, shard_trips, num_tiles, xstartVal, xendVal, ystartVal, yendVal, srid, tstartVal,tendVal)
                    INTO result;
                    yendVal := result::float;
                    tileSize.mobilitydb_extent := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan);
                    ystartVal := yendVal ;--+ stepY;
                    yendVal := ymax(extent_info.mobilitydb_extent);
                ELSE
                    SELECT BinarySearch(dim_role, table_name_in, spatiotemporal_col_name, mobilitydb_type, column_sub_type, shape_segmentation,shard_numPoints, shard_trips, num_tiles, xstartVal, xendVal, ystartVal, yendVal, srid,'1900-01-01'::timestamp, '1900-01-01'::timestamp)
                    INTO result;
                    yendVal := result::float;
                    tileSize.postgis_extent := ST_MakeEnvelope(xstartVal,ystartVal,xendVal, yendVal, srid);
                    ystartVal := yendVal ;--+ stepY;
                    yendVal := st_ymax(extent_info.postgis_extent);
                END IF;
            ELSE
                IF mobilitydb_type THEN
                    tileSize.mobilitydb_extent := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan);
                ELSE
                    tileSize.postgis_extent := ST_MakeEnvelope(xstartVal,ystartVal,xendVal, yendVal, srid);
                END IF;
            END IF;
        END IF;

        -- Get tile information
        SELECT getTileSize(table_name_in, spatiotemporal_col_name, column_sub_type, srid, tileSize)
        INTO tileSize;

        -- Insert partition data
        IF mobilitydb_type THEN
            EXECUTE format('%s', concat('INSERT INTO ',catalog_table, ' (bbox, numPoints, numShapes) values(''',tileSize.mobilitydb_extent,'''::stbox,',tileSize.numPoints,',',tileSize.numShapes,');'));
            RAISE INFO 'Tile:#% created! With BBOX:%,Total Shapes:%,Total Points:%', counter,tileSize.mobilitydb_extent, tileSize.numShapes, tileSize.numPoints;
        ELSE
            EXECUTE format('%s', concat('INSERT INTO ',catalog_table, ' (bbox, numPoints, numShapes) values(''',tileSize.postgis_extent,'''::geometry,',tileSize.numPoints,',',tileSize.numShapes,');'));
            RAISE INFO 'Tile:#% created! With BBOX:%,Total Shapes:%,Total Points:%', counter,tileSize.postgis_extent::box2d, tileSize.numShapes, tileSize.numPoints;
        END IF;
        counter := counter + 1;
        IF counter > num_tiles THEN
            exit;
        END IF;
        SELECT get_next_dim(tiling_type, dim_role)
        INTO dim_role;

    END LOOP;

    RAISE INFO 'Run-time for generating tiles:%', (clock_timestamp() - start_time);
    -- Add the tiling scheme to catalog and return the table oid
    -- Create tile key for each bbox
    EXECUTE format('%s', concat('ALTER TABLE ',catalog_table, ' ADD column ',shard_key,' serial'));
    SELECT add_distributed_table_metadata(table_name_out, spatiotemporal_col_name, mobilitydb_type, num_tiles, TRUE, shard_key, shape_segmentation)
    INTO table_out_id;

return table_out_id;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION tiling_granularity_detection(table_name_in text, spatiotemporal_col_name varchar(50), num_tiles integer, table_name_out text, tiling_type text ,shape_segmentation boolean, group_by_col varchar(50), mobilitydb_type boolean, column_sub_type varchar(10), shard_key varchar(50))
    RETURNS integer AS $$
DECLARE
tileSize.mobilitydb_extent stbox;
    tileSize.postgis_extent geometry;
    start_time timestamp;
    start_time_temp timestamp;
    input_column_sub_type varchar(10);
    table_out_id integer;
    timePeriod tstzrange;
    shard_numPoints bigint;
    mobilitydb_cellExtent stbox;
    postgis_cellExtent box2d;
    numPoints bigint;
    tendVal timestamptz;
    total_Points bigint;
    total_Trajs integer;
    tstartVal timestamptz;
    catalog_table text;
    counter integer;
    xstartVal float;
    ystartVal float;
    xendVal float;
    yendVal float;
    srid integer;
    dim_role integer;
    result text;
    org_table_name_in text;
    numShapes integer;
    shard_trips integer;
BEGIN
    start_time := clock_timestamp();
    -- 0 means shape-based and 1 means point-based
RETURN 0;
END;
$$ LANGUAGE 'plpgsql';