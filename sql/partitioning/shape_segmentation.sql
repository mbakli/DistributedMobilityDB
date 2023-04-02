--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Data Distribution
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION segmentation(table_name_in varchar(250), shard_key varchar(50), object_segmentation boolean, spatiotemporal_col_name varchar(100), group_by_col varchar(50), mobilitydb_type boolean, column_sub_type varchar(10), num_tiles integer, distributed_column_name varchar(100), table_name_out varchar(250), table_id integer)
    RETURNS boolean AS $$
DECLARE
    org_table_columns text;
    temp text;
    curr_srid integer;
    starttime timestamp;
    endtime timestamp;
    group_by_clause text;
    org_table_name_in varchar(250);
    obj_construction_query text;
BEGIN
    --set client_min_messages to WARNING;
    RAISE INFO 'Distributing the %s into the overlapping tiles along with segmenting them:',column_sub_type;
    IF mobilitydb_type THEN
        SELECT regexp_replace(org_table_columns, concat(',t1.',spatiotemporal_col_name), concat(',tgeompoint_discseq(array_agg(t2.',spatiotemporal_col_name,' ORDER BY starttimestamp(t2.',spatiotemporal_col_name,'))) '))
        INTO org_table_columns;
        -- Segmentation on the fly
        EXECUTE format('%s', concat('
		INSERT INTO ',table_name_out,'
		SELECT ',org_table_columns, ',',shard_key,'
		FROM ',org_table_name_in,' t1, ',table_name_in,' t2, pg_dist_spatiotemporal_tiles
		WHERE t1.',group_by_col,' = t2.',group_by_col,' AND table_id=',table_id,' and t2.', spatiotemporal_col_name,' && mobdb_bbox GROUP BY ',shard_key,',', group_by_clause));
    ELSIF not mobilitydb_type and column_sub_type = 'linestring' THEN -- geometry(linestring)
        temp := org_table_columns;
        -- multilinestring
        obj_construction_query := concat('ST_Intersection(t1.',spatiotemporal_col_name,', postgis_bbox) ');
        SELECT regexp_replace(org_table_columns, concat(',t1.',spatiotemporal_col_name), concat(',',obj_construction_query))
        INTO org_table_columns;
        IF temp = org_table_columns THEN
            SELECT regexp_replace(org_table_columns, concat(',t1."',spatiotemporal_col_name,'"'), concat(',',obj_construction_query))
            INTO org_table_columns;
        END IF;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',org_table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', spatiotemporal_col_name,' && postgis_bbox AND ST_Intersects(',spatiotemporal_col_name,', postgis_bbox) GROUP BY ',shard_key,',postgis_bbox,', group_by_clause));
    ELSIF not mobilitydb_type and column_sub_type = 'polygon' THEN -- geometry(polygon)
        temp := org_table_columns;
        SELECT replace(org_table_columns, concat(',',spatiotemporal_col_name), concat(',ST_Intersection(',spatiotemporal_col_name,', postgis_bbox) '))
        INTO org_table_columns;
        IF temp = org_table_columns THEN
            SELECT replace(org_table_columns, concat(',t1."',spatiotemporal_col_name,'"'), concat(',ST_Intersection(t1.',spatiotemporal_col_name,', postgis_bbox) '))
            INTO org_table_columns;
        END IF;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', spatiotemporal_col_name,' && postgis_bbox AND ST_Intersects(',spatiotemporal_col_name,', postgis_bbox) GROUP BY ',shard_key,',postgis_bbox,', group_by_clause));
    ELSIF mobilitydb_type and column_sub_type in ('sequence','sequenceset') THEN
        SELECT replace(org_table_columns, concat(',',spatiotemporal_col_name), concat(',tgeompoint_discseq(array_agg(',spatiotemporal_col_name,' ORDER BY starttimestamp(',spatiotemporal_col_name,'))) '))
        INTO org_table_columns;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', spatiotemporal_col_name,' && mobdb_bbox GROUP BY ',shard_key,',', group_by_clause));
    ELSIF mobilitydb_type and column_sub_type = 'instant' THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles:',column_sub_type;
        SELECT replace(org_table_columns, concat(',',spatiotemporal_col_name), concat(',',spatiotemporal_col_name))
        INTO org_table_columns;
        -- Insert all overlapping points + points that touch the tile boundries
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,'
                and ', spatiotemporal_col_name,' && mobdb_bbox
            GROUP BY ',shard_key,',', group_by_clause));
    ELSIF not mobilitydb_type and column_sub_type = 'linestring' THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles without segmenting them:',column_sub_type;
        SELECT replace(org_table_columns, concat(',',spatiotemporal_col_name), concat(',ST_LineFromMultiPoint(ST_Collect(array_agg(',spatiotemporal_col_name,'))) '))
        INTO org_table_columns;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', spatiotemporal_col_name,' && postgis_bbox GROUP BY ',shard_key,',', group_by_clause));
    ELSIF not mobilitydb_type and not object_segmentation and column_sub_type = 'polygon' THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles without segmenting them:',column_sub_type;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', spatiotemporal_col_name,' && postgis_bbox AND ST_Intersects(', spatiotemporal_col_name, ',postgis_bbox);'));
    ELSIF not mobilitydb_type and column_sub_type = 'point' THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles:',column_sub_type;
        SELECT replace(org_table_columns, concat(',',spatiotemporal_col_name), concat(',',spatiotemporal_col_name))
        INTO org_table_columns;
        -- Insert all overlapping points + points that touch the tile boundries
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,'
                and (st_contains(postgis_bbox,', spatiotemporal_col_name,') or st_intersects(ST_Boundary(postgis_bbox), ',spatiotemporal_col_name,'))
            GROUP BY ',shard_key,',', group_by_clause));
    ELSE
        RAISE Exception 'The spatiotemporal type is not detected!';
    END IF;
    --set client_min_messages to INFO;
    --Spatiotempora: AND st_intersects(st_setsrid(bbox::box2d::geometry, ',curr_srid,'), trajectory(',spatiotemporal_col_name,'))
    endtime := clock_timestamp();
    return true;
END;
$$ LANGUAGE 'plpgsql';