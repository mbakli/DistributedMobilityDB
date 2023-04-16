CREATE OR REPLACE FUNCTION colocation_method(table_name_in text, spatiotemporal_col_name varchar(50), num_tiles integer, table_name_out text, colocation_table text , colocation_column text, object_segmentation boolean, group_by_col varchar(50), mobilitydb_type boolean, column_sub_type varchar(10), shard_key varchar(50))
    RETURNS integer AS $$
DECLARE
    start_time timestamp;
    catalog_table text;
    colocation_column_sub_type text;
    mobilitydb_cellExtent stbox;
    mobilitydb_bbox stbox;
    tripPoints bigint;
    record record;
    total_Trajs integer;
    total_Points integer;
    table_out_id integer;
BEGIN
    start_time := clock_timestamp();
    -- Partition data
    IF mobilitydb_type THEN
        RAISE INFO 'MobilityDB Column: % - TGeomPoint(%)', spatiotemporal_col_name, column_sub_type;
    ELSE
        RAISE INFO 'PostGIS Column: % - Geometry(%)', spatiotemporal_col_name, column_sub_type;
    END IF;
    -- Check the multirelation type: Single Point or Sequenece
    SELECT partitioning_column_type(colocation_table, colocation_column)
    INTO colocation_column_sub_type;

    IF colocation_column_sub_type in ('polygon','box2d','box3d') THEN
        RAISE INFO 'Generate Spatial Partitions';
    ELSIF colocation_column_sub_type in ('stbox') THEN
        RAISE INFO 'Generate Spatiotemporal Partitions:';
    ELSE
        RAISE Exception 'Can not identify the colocation column type!';
    END IF;

    -- Create the catalog table
    PERFORM create_catalog_table(table_name_out, mobilitydb_type);

    RAISE INFO 'Generating Tiles:';

    -- Insert the given geometries into the catalog table
    IF column_sub_type in ('instant') and colocation_column_sub_type in ('polygon','box2d') THEN
        EXECUTE format('%s', concat('SELECT extent(',spatiotemporal_col_name,'), count(*) from ', table_name_in)) INTO mobilitydb_cellExtent, tripPoints;
        FOR record in EXECUTE format('%s', concat('SELECT ', colocation_column, ' FROM ', colocation_table))
            LOOP
                mobilitydb_bbox := STBOX(record.geom, mobilitydb_cellExtent::tstzspan);
                EXECUTE format('%s', concat('SELECT count(*),count(*) FROM ',table_name_in,' WHERE ',spatiotemporal_col_name,' <@ ''',mobilitydb_bbox,'''::stbox'))
                    INTO total_Trajs, total_Points;
                EXECUTE format('%s', concat('INSERT INTO ',catalog_table, ' (bbox, totalPoints, totalTrajs) values(''',mobilitydb_bbox,'''::stbox,',total_Points,',',total_Trajs,');'));
            END LOOP;
    end if;
    -- Create shard key for each bbox
    EXECUTE format('%s', concat('ALTER TABLE ',catalog_table, ' ADD column ',shard_key,' serial'));
    SELECT add_distributed_table_metadata(table_name_out, spatiotemporal_col_name, mobilitydb_type, num_tiles, TRUE, shard_key, object_segmentation)
    INTO table_out_id;
    return table_out_id;
END;
$$ LANGUAGE 'plpgsql';