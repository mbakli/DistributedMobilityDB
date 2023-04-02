--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Data Distribution
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION spatiotemporal_data_allocation(table_name_in varchar(250), shard_key varchar(50), shape_segmentation boolean, spatiotemporal_col_name varchar(100), group_by_col varchar(50), mobilitydb_type boolean, column_sub_type varchar(10), num_tiles integer, distributed_column_name varchar(100), table_name_out varchar(250), table_id integer)
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
    IF mobilitydb_type and column_sub_type not in('point','polygon', 'instant') THEN
        org_table_name_in := table_name_in;
        table_name_in := concat(table_name_in, '_temp');
    else
        org_table_name_in := table_name_in;
    end if;
    -- Get the spatial reference
    SELECT getCoordinateSystem(table_name_in, spatiotemporal_col_name, mobilitydb_type)
    INTO curr_srid;
    -- Set the shards count
    EXECUTE format('%s', concat('set citus.shard_count to ',num_tiles));
    -- Create another table
    set client_min_messages to WARNING;
    EXECUTE format('%s', concat('DROP TABLE IF EXISTS ',table_name_out)); -- Does not work with Citus
    set client_min_messages to INFO;
    EXECUTE format('%s', concat('CREATE TABLE ',table_name_out,' (LIKE ', org_table_name_in, '); '));
    IF not mobilitydb_type and column_sub_type = 'linestring' THEN
        EXECUTE format('%s', concat('ALTER TABLE ', table_name_out,' ALTER COLUMN ', spatiotemporal_col_name,' TYPE geometry;'));
    END IF;

    -- Add the distributed column
    EXECUTE format('%s', concat('ALTER TABLE ',table_name_out, ' ADD column ',distributed_column_name,' integer'));
    -- Distribute the table using range partitioning
    EXECUTE format('%s', concat('SELECT create_distributed_table(''', table_name_out,''', ''',distributed_column_name,''', ''range'');'));
    -- Modify the shard ranges in the Citus Catalog as Citus does not support range partitioning by default
    EXECUTE format('%s', concat('SELECT create_range_shards(', num_tiles,' , ''', table_name_out, ''');'));
    -- Get the table columns
    IF mobilitydb_type THEN
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


    -- Distribute the sub-trajectories over the partitions
    -- Group by clause preparation
    group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1.',spatiotemporal_col_name,')'), ''), ',,',','), '^,',''), ',$','');
    IF group_by_clause = org_table_columns THEN
        group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1."',spatiotemporal_col_name,'")'), ''), ',,',','), '^,',''), ',$','');
    END IF;
    starttime := clock_timestamp();
    IF shape_segmentation THEN
        PERFORM segmentation_and_allocation();
    ELSE
        PERFORM allocation();
    END IF;


    --set client_min_messages to WARNING;
    IF shape_segmentation and mobilitydb_type and column_sub_type in ('sequence', 'sequenceset') THEN -- tgeompoint(sequence)
        RAISE INFO 'Distributing the %s into the overlapping tiles along with segmenting them:',column_sub_type;
        SELECT regexp_replace(org_table_columns, concat(',t1.',spatiotemporal_col_name), concat(',tgeompoint_discseq(array_agg(t2.',spatiotemporal_col_name,' ORDER BY starttimestamp(t2.',spatiotemporal_col_name,'))) '))
        INTO org_table_columns;
        EXECUTE format('%s', concat('
		INSERT INTO ',table_name_out,'
		SELECT ',org_table_columns, ',',shard_key,'
		FROM ',org_table_name_in,' t1, ',table_name_in,' t2, pg_dist_spatiotemporal_tiles
		WHERE t1.',group_by_col,' = t2.',group_by_col,' AND table_id=',table_id,' and t2.', spatiotemporal_col_name,' && mobdb_bbox GROUP BY ',shard_key,',', group_by_clause));
    ELSIF shape_segmentation and not mobilitydb_type and column_sub_type = 'linestring' THEN -- geometry(linestring)
        RAISE INFO 'Distributing the %s into the overlapping tiles along with segmenting them:',column_sub_type;
        temp := org_table_columns;
        -- Insert linestring for more than one point and insert linestring with the same point

        -- Construct traj from points
        /*
        obj_construction_query := concat('CASE
                                            WHEN ST_Npoints(ST_collect(array_agg(t2.',spatiotemporal_col_name,'))) > 1 THEN ST_MakeLine(array_agg(t2.',spatiotemporal_col_name,'))
                                            ELSE ST_MakeLine((array_agg(t2.',spatiotemporal_col_name,'))[1],(array_agg(t2.',spatiotemporal_col_name,'))[1])
                                          END ');
         */
        -- Construct trajectory from the current one
        --One line string
        --obj_construction_query := concat('ST_makeline(array_agg(ST_Intersection(t1.',spatiotemporal_col_name,', postgis_bbox))) ');
        -- multilinestring
        obj_construction_query := concat('ST_Intersection(t1.',spatiotemporal_col_name,', postgis_bbox) ');
        SELECT regexp_replace(org_table_columns, concat(',t1.',spatiotemporal_col_name), concat(',',obj_construction_query))
        INTO org_table_columns;
        IF temp = org_table_columns THEN
            SELECT regexp_replace(org_table_columns, concat(',t1."',spatiotemporal_col_name,'"'), concat(',',obj_construction_query))
            INTO org_table_columns;
        END IF;
        /*
        EXECUTE format('%s', concat('
		INSERT INTO ',table_name_out,'
		SELECT ',org_table_columns, ',',shard_key,'
		FROM ',org_table_name_in,' t1, ',table_name_in,' t2, pg_dist_spatiotemporal_tiles
		WHERE t1.',group_by_col,' = t2.',group_by_col,' AND table_id=',table_id,' and t2.', spatiotemporal_col_name,' && postgis_bbox GROUP BY ',shard_key,',', group_by_clause));
        */
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',org_table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', spatiotemporal_col_name,' && postgis_bbox AND ST_Intersects(',spatiotemporal_col_name,', postgis_bbox) GROUP BY ',shard_key,',postgis_bbox,', group_by_clause));

    ELSIF shape_segmentation and not mobilitydb_type and column_sub_type = 'polygon' THEN -- geometry(polygon)
        RAISE INFO 'Distributing the %s into the overlapping tiles along with segmenting them:',column_sub_type;
        temp := org_table_columns;
        SELECT replace(org_table_columns, concat(',',spatiotemporal_col_name), concat(',ST_Intersection(',spatiotemporal_col_name,', postgis_bbox) '))
        INTO org_table_columns;
        IF temp = org_table_columns THEN
            SELECT replace(org_table_columns, concat(',t1."',spatiotemporal_col_name,'"'), concat(',ST_Intersection(t1.',spatiotemporal_col_name,', postgis_bbox) '))
            INTO org_table_columns;
        end if;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',shard_key,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', spatiotemporal_col_name,' && postgis_bbox AND ST_Intersects(',spatiotemporal_col_name,', postgis_bbox) GROUP BY ',shard_key,',postgis_bbox,', group_by_clause));
    ELSIF mobilitydb_type and column_sub_type in ('sequence','sequenceset') THEN
        RAISE INFO 'Distributing the %s into the overlapping tiles without segmenting them:',column_sub_type;
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
    ELSIF not mobilitydb_type and not shape_segmentation and column_sub_type = 'polygon' THEN
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