CREATE OR REPLACE FUNCTION crange_method(table_name_in text, table_name_out text, tiling tiling)
    RETURNS integer AS $$
DECLARE
    extent tileSize;
    tileSize tileSize;
    start_time timestamp;
    start_time_temp timestamp;
    input_column_sub_type varchar(10);
    table_out_id integer;
    timePeriod tstzrange;
    tile_numPoints bigint;
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
    tile_numShapes integer;
BEGIN
    start_time := clock_timestamp();
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    --Preprocessing
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    input_column_sub_type := tiling.internaltype;
    -- Determine how to partition the table
    IF tiling.internaltype in ('instant', 'point') THEN
        tiling.segmentation := FALSE;
    ELSIF tiling.internaltype in ('linestring', 'polygon', 'multilinestring', 'multipolygon', 'sequence','sequenceset') THEN
        org_table_name_in := table_name_in;
        IF tiling.granularity = 'point-based' THEN
            -- Generate a new temporary distributed table to store the object points
            PERFORM create_temporary_points_table(table_name_in, tiling, concat(table_name_in , '_temp'));
            table_name_in := concat(table_name_in , '_temp');
            IF tiling.isMobilityDB THEN
                tiling.internaltype := 'instant';
            ELSE
                tiling.internaltype := 'point';
            END IF;
        END IF;
    END IF;

    -- Partition data
    IF tiling.isMobilityDB THEN
        RAISE INFO 'MobilityDB Column: % - TGeomPoint(%)', tiling.distCol, input_column_sub_type;
    ELSE
        RAISE INFO 'PostGIS Column: % - Geometry(%)', tiling.distCol, input_column_sub_type;
    END IF;

    IF tiling.type = 'temporal' THEN
        IF tiling.isMobilityDB THEN
            RAISE INFO 'Generate Temporal Partitions';
        ELSE
            RAISE Exception 'The temporal partitioning can not be applied on a spatial type!';
        END IF;
    ELSIF tiling.type = 'spatial' THEN
        RAISE INFO 'Generate Spatial Partitions';
    ELSIF tiling.type = 'spatiotemporal' THEN
        IF tiling.isMobilityDB THEN
            RAISE INFO 'Generate Spatiotemporal Partitions:';
        ELSE
            RAISE Exception 'The spatiotemporal partitioning can not be applied on a spatial type!';
        END IF;
    ELSE
        RAISE Exception 'Please choose one of the following: temporal, spatial, or spatiotemporal';
    END IF;
    -- Create the catalog table
    PERFORM create_catalog_table(table_name_out, tiling.isMobilityDB);

    if tiling.internaltype = 'instant' THEN
        EXECUTE format('%s', concat('SELECT extent(',tiling.distCol,'), count(*) from ', table_name_in))
            INTO extent.mobilitydb_extent, extent.numShapes;
        extent.numPoints := extent.numShapes;
    ELSIF tiling.internaltype = 'point' THEN
        EXECUTE format('%s', concat('SELECT st_extent(',tiling.distCol,'), count(*) from ', table_name_in))
        INTO extent.postgis_extent, extent.numShapes;
        extent.numPoints := extent.numShapes;
    ELSIF tiling.internaltype in ('sequence', 'sequenceset') THEN
        EXECUTE format('%s', concat('SELECT extent(',tiling.distCol,'), count(*), sum(numInstants(',tiling.distCol,')) from ', table_name_in))
            INTO extent.mobilitydb_extent, extent.numShapes,extent.numPoints;
    elsIF tiling.internaltype in ('linestring', 'polygon','multilinestring', 'multipolygon') THEN
        EXECUTE format('%s', concat('SELECT st_extent(',tiling.distCol,'), count(*), sum(st_npoints(',tiling.distCol,')) from ', table_name_in))
            INTO extent.postgis_extent, extent.numShapes,extent.numPoints;
    END IF;
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    --Partitioning
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Partition data
        RAISE INFO 'Generating Tiles:';
        -- Initialization
        counter := 1;
        IF tiling.isMobilityDB THEN
            tstartVal := tmin(extent.mobilitydb_extent);
            tendVal := tmax(extent.mobilitydb_extent);
            timePeriod := tstzrange(tstartVal, tendVal);
            tile_numPoints := extent.numPoints / tiling.numTiles;
            tile_numShapes := extent.numPoints / tiling.numTiles;
            xstartVal := xmin(extent.mobilitydb_extent);
            xendVal := xmax(extent.mobilitydb_extent);
            ystartVal := ymin(extent.mobilitydb_extent);
            yendVal := ymax(extent.mobilitydb_extent);
        ELSE
            tile_numPoints := extent.numPoints / tiling.numTiles;
            tile_numShapes := extent.numShapes / tiling.numTiles;
            xstartVal := st_xmin(extent.postgis_extent);
            xendVal := st_xmax(extent.postgis_extent);
            ystartVal := st_ymin(extent.postgis_extent);
            yendVal := st_ymax(extent.postgis_extent);
        END IF;

        SELECT get_next_dim(tiling.type, 0)
        INTO dim_role; -- 1 means temporal, 2 means x, 3 means y
        catalog_table := concat(table_name_out, '_catalog');
        LOOP
        -- T Partitioning
        IF dim_role = 1 THEN
            IF counter < tiling.numTiles THEN
                SELECT BinarySearch(dim_role, table_name_in, tiling,tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, tstartVal,tendVal)
                INTO result;
                tendVal := result::timestamptz;
                SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
                INTO tileSize.mobilitydb_extent;
                tstartVal := tendVal ;
                tendVal := tmax(extent.mobilitydb_extent);
            ELSE
                SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
                INTO tileSize.mobilitydb_extent;
            END IF;
            -- X Partitioning
            ELSIF dim_role = 2 THEN
                IF counter < tiling.numTiles THEN
                    IF tiling.isMobilityDB THEN
                        SELECT BinarySearch(dim_role, table_name_in, tiling, tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, tstartVal,tendVal)
                        INTO result;
                        xendVal := result::float;
                        SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
                        INTO tileSize.mobilitydb_extent;
                        xstartVal := xendVal ;
                        xendVal := xmax(extent.mobilitydb_extent);
                    ELSE
                        SELECT BinarySearch(dim_role, table_name_in, tiling, tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, '1900-01-01'::timestamp, '1900-01-01'::timestamp)
                        INTO result;
                        xendVal := result::float;
                        SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(xstartVal,ystartVal), ST_Point(xendVal, yendVal)), tiling.srid)
                        INTO tileSize.postgis_extent;
                        xstartVal := xendVal ;
                        xendVal := st_xmax(extent.postgis_extent);
                    END IF;
                ELSE
                    IF tiling.isMobilityDB THEN
                        SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
                        INTO tileSize.mobilitydb_extent;
                    ELSE
                        SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(xstartVal,ystartVal), ST_Point(xendVal, yendVal)), tiling.srid)
                        INTO tileSize.postgis_extent;
                    END IF;
                END IF;
            -- Y Partitioning
            ELSIF dim_role = 3 THEN
                IF counter < tiling.numTiles THEN
                    IF tiling.isMobilityDB THEN
                        SELECT BinarySearch(dim_role, table_name_in, tiling,tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, tstartVal,tendVal)
                        INTO result;
                        yendVal := result::float;
                        SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
                        INTO tileSize.mobilitydb_extent;
                        ystartVal := yendVal ;
                        yendVal := ymax(extent.mobilitydb_extent);
                    ELSE
                        SELECT BinarySearch(dim_role, table_name_in, tiling,tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, '1900-01-01'::timestamp, '1900-01-01'::timestamp)
                        INTO result;
                        yendVal := result::float;
                        SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(xstartVal,ystartVal), ST_Point(xendVal, yendVal)), tiling.srid)
                        INTO tileSize.postgis_extent;
                        ystartVal := yendVal ;
                        yendVal := st_ymax(extent.postgis_extent);
                    END IF;
                ELSE
                    IF tiling.isMobilityDB THEN
                        SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
                        INTO tileSize.mobilitydb_extent;
                    ELSE
                        SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(xstartVal,ystartVal), ST_Point(xendVal, yendVal)), tiling.srid)
                        INTO tileSize.postgis_extent;
                    END IF;
                END IF;
            END IF;

            -- Get tile information
            --SELECT getTileSize(table_name_in, tiling)
            --INTO tileSize;
            IF tiling.internaltype = 'instant' THEN
                EXECUTE format('%s', concat('SELECT count(distinct ',tiling.groupCol,'), count(*) FROM ',table_name_in,' WHERE setsrid(',tiling.distCol,',',tiling.srid,') && ''',tileSize.mobilitydb_extent,'''::stbox'))
                INTO tileSize.numShapes, tileSize.numPoints;
            ELSIF tiling.internaltype = 'point' THEN
                EXECUTE format('%s', concat('SELECT count(distinct ',tiling.groupCol,'), count(*) FROM ',table_name_in,' WHERE ''',tileSize.postgis_extent,'''::box2d && ',tiling.distCol))
                INTO tileSize.numShapes, tileSize.numPoints;
            ELSIF tiling.internaltype in ('sequence', 'sequenceset') THEN
                EXECUTE format('%s', concat('SELECT count(*), sum(numInstants(aStbox(',tiling.distCol,', ''',tileSize.mobilitydb_extent,'''::stbox))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && ''',tileSize.mobilitydb_extent,'''::stbox'))
                INTO tileSize.numShapes, tileSize.numPoints;
            ELSIF tiling.internaltype in ('linestring', 'polygon', 'multilinestring', 'multipolygon') THEN
                EXECUTE format('%s', concat('SELECT count(*), sum(st_npoints(st_intersection(',tiling.distCol,', st_setsrid(''',tileSize.postgis_extent,'''::box2d::geometry,',tiling.srid,')))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && st_setsrid(''',tileSize.postgis_extent,'''::box2d::geometry,',tiling.srid,') AND st_intersects(', tiling.distCol, ',st_setsrid(''', tileSize.postgis_extent,'''::box2d::geometry,',tiling.srid,'));'))
                INTO tileSize.numShapes, tileSize.numPoints;
            END IF;
            -- Insert partition data
            IF tiling.isMobilityDB THEN
                EXECUTE format('%s', concat('INSERT INTO ',catalog_table, ' (bbox, numPoints, numShapes) values(''',tileSize.mobilitydb_extent,'''::stbox,',tileSize.numPoints,',',tileSize.numShapes,');'));
                RAISE INFO 'Tile:#% created! With BBOX:%,Total Shapes:%,Total Points:%', counter,tileSize.mobilitydb_extent, tileSize.numShapes, tileSize.numPoints;
            ELSE
                EXECUTE format('%s', concat('INSERT INTO ',catalog_table, ' (bbox, numPoints, numShapes) values(''',tileSize.postgis_extent,'''::box2d::geometry,',tileSize.numPoints,',',tileSize.numShapes,');'));
                RAISE INFO 'Tile:#% created! With BBOX:%,Total Shapes:%,Total Points:%', counter,tileSize.postgis_extent::box2d, tileSize.numShapes, tileSize.numPoints;
            END IF;
            counter := counter + 1;
            IF counter > tiling.numTiles THEN
                exit;
            END IF;
            SELECT get_next_dim(tiling.type, dim_role)
            INTO dim_role;

        END LOOP;

        RAISE INFO 'Run-time for generating tiles:%', (clock_timestamp() - start_time);
        -- Add the tiling scheme to catalog and return the table oid
        -- Create tile key for each bbox
        EXECUTE format('%s', concat('ALTER TABLE ',catalog_table, ' ADD column ',tiling.tileKey,' serial'));
        SELECT add_distributed_table_metadata(table_name_out, tiling)
        INTO table_out_id;

    return table_out_id;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION tilingGranularityDetection(table_name_in text, table_name_out text, tiling tiling)
    RETURNS text AS $$
DECLARE
    tileSize tileSize;
    extent tileSize;
    start_time timestamp;
    tile_numPoints bigint;
    tendVal timestamptz;
    tstartVal timestamptz;
    xstartVal float;
    ystartVal float;
    xendVal float;
    yendVal float;
    timePeriod tstzrange;
    srid integer;
    dim_role integer;
    result text;
    tile_numShapes integer;
BEGIN
    start_time := clock_timestamp();
    if tiling.internaltype = 'instant' THEN
        EXECUTE format('%s', concat('SELECT extent(',tiling.distCol,'), count(*) from ', table_name_in))
            INTO extent.mobilitydb_extent, extent.numShapes;
        extent.numPoints := extent.numShapes;
    ELSIF tiling.internaltype = 'point' THEN
        EXECUTE format('%s', concat('SELECT st_extent(',tiling.distCol,'), count(*) from ', table_name_in))
            INTO extent.postgis_extent, extent.numShapes;
        extent.numPoints := extent.numShapes;
    ELSIF tiling.internaltype in ('sequence', 'sequenceset') THEN
        EXECUTE format('%s', concat('SELECT extent(',tiling.distCol,'), count(*), sum(numInstants(',tiling.distCol,')) from ', table_name_in))
            INTO extent.mobilitydb_extent, extent.numShapes,extent.numPoints;
    elsIF tiling.internaltype in ('linestring', 'polygon','multilinestring', 'multipolygon') THEN
        EXECUTE format('%s', concat('SELECT st_extent(',tiling.distCol,'), count(*), sum(st_npoints(',tiling.distCol,')) from ', table_name_in))
            INTO extent.postgis_extent, extent.numShapes,extent.numPoints;
    END IF;
    SELECT get_next_dim(tiling.type, 0)
    INTO dim_role; -- 1 means temporal, 2 means x, 3 means y
    tile_numPoints := extent.numPoints / 2;
    tile_numShapes := extent.numShapes / 2;
    IF tiling.isMobilityDB THEN
        tstartVal := tmin(extent.mobilitydb_extent);
        tendVal := tmax(extent.mobilitydb_extent);
        timePeriod := tstzrange(tstartVal, tendVal);
        xstartVal := xmin(extent.mobilitydb_extent);
        xendVal := xmax(extent.mobilitydb_extent);
        ystartVal := ymin(extent.mobilitydb_extent);
        yendVal := ymax(extent.mobilitydb_extent);
    ELSE
        xstartVal := st_xmin(extent.postgis_extent);
        xendVal := st_xmax(extent.postgis_extent);
        ystartVal := st_ymin(extent.postgis_extent);
        yendVal := st_ymax(extent.postgis_extent);
    END IF;
    -- T Partitioning
    IF dim_role = 1 THEN
        SELECT BinarySearch(dim_role, table_name_in, tiling,tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, tstartVal,tendVal)
        INTO result;
        tendVal := result::timestamptz;
        SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
        INTO tileSize.mobilitydb_extent;
        tstartVal := tendVal;
        tendVal := tmax(extent.mobilitydb_extent);
        -- X Partitioning
    ELSIF dim_role = 2 THEN
        IF tiling.isMobilityDB THEN
            SELECT BinarySearch(dim_role, table_name_in, tiling, tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, tstartVal,tendVal)
            INTO result;
            xendVal := result::float;
            SELECT STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(xstartVal,ystartVal), ST_MakePoint(xendVal, yendVal))), srid), tstzrange(tstartVal, tendVal)::tstzspan)
            INTO tileSize.mobilitydb_extent;
            xstartVal := xendVal ;
            xendVal := xmax(extent.mobilitydb_extent);
        ELSE
            SELECT BinarySearch(dim_role, table_name_in, tiling, tile_numPoints, tile_numShapes, xstartVal, xendVal, ystartVal, yendVal, '1900-01-01'::timestamp, '1900-01-01'::timestamp)
            INTO result;
            xendVal := result::float;
            SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(xstartVal,ystartVal), ST_Point(xendVal, yendVal)), tiling.srid)
            INTO tileSize.postgis_extent;
            xstartVal := xendVal ;
            xendVal := st_xmax(extent.postgis_extent);
        END IF;
    END IF;
    -- Get tile information
    IF tiling.internaltype = 'instant' THEN
        EXECUTE format('%s', concat('SELECT count(distinct ',tiling.groupCol,'), count(*) FROM ',table_name_in,' WHERE setsrid(',tiling.distCol,',',tiling.srid,') && ''',tileSize.mobilitydb_extent,'''::stbox'))
            INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF tiling.internaltype = 'point' THEN
        EXECUTE format('%s', concat('SELECT count(distinct ',tiling.groupCol,'), count(*) FROM ',table_name_in,' WHERE ''',tileSize.postgis_extent,'''::box2d && ',tiling.distCol))
            INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF tiling.internaltype in ('sequence', 'sequenceset') THEN
        EXECUTE format('%s', concat('SELECT count(*), sum(numInstants(aStbox(',tiling.distCol,', ''',tileSize.mobilitydb_extent,'''::stbox))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && ''',tileSize.mobilitydb_extent,'''::stbox'))
            INTO tileSize.numShapes, tileSize.numPoints;
    ELSIF tiling.internaltype in ('linestring', 'polygon', 'multilinestring', 'multipolygon') THEN
        EXECUTE format('%s', concat('SELECT count(*), sum(st_npoints(st_intersection(',tiling.distCol,', st_setsrid(''',tileSize.postgis_extent,'''::box2d::geometry, ',tiling.srid,')))) FROM ',table_name_in,' WHERE ',tiling.distCol,' && st_setsrid(''',tileSize.postgis_extent,'''::box2d::geometry,',tiling.srid,') AND st_intersects(', tiling.distCol, ',st_setsrid(''', tileSize.postgis_extent,'''::box2d::geometry, ',tiling.srid,'));'))
        INTO tileSize.numShapes, tileSize.numPoints;
    END IF;
    IF (extent.numShapes - tileSize.numShapes * 2) < (extent.numShapes * 0.2) THEN
        return 'shape-based';
    ELSE
        return 'point-based';
    END IF;
END;
$$ LANGUAGE 'plpgsql';