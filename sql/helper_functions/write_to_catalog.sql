DROP FUNCTION IF EXISTS add_distributed_table_metadata;
CREATE OR REPLACE FUNCTION add_distributed_table_metadata(table_name_out varchar(250), tiling tiling)
	RETURNS integer AS $$
DECLARE
    table_out_id integer;
    table_oid oid;
    column_type varchar(100);
BEGIN
    IF tiling.isMobilityDB THEN
        column_type := 'tgeompoint';
    ELSE
        column_type := 'geometry';
    END IF;
    EXECUTE format('%s', concat('' ||
                            'INSERT INTO pg_dist_spatiotemporal_tables (table_oid, table_name, tiles_count, disjoint_tiles, distcol, distcoltype, tilekey, shapeSegmented) ' ||
                            'VALUES (',0,',''',table_name_out,''',',tiling.numTiles,',''',tiling.disjointTiles,''', ''',tiling.distCol,''', ''',tiling.distColType,''', ''',tiling.tileKey,''', ''',tiling.segmentation,''') ' ||
                                                                                                                                                                                                           'ON CONFLICT (table_name) ' ||
                                                                                                                                                                                                           'DO ' ||
                                                                                                                                                                                                           'UPDATE set table_name = EXCLUDED.table_name,' ||
                                                                                                                                                                                                           'tiles_count = EXCLUDED.tiles_count,' ||
                                                                                                                                                                                                           'disjoint_tiles = EXCLUDED.disjoint_tiles,' ||
                                                                                                                                                                                                           'distcol = EXCLUDED.distcol,' ||
                                                                                                                                                                                                           'distcoltype = EXCLUDED.distcoltype,' ||
                                                                                                                                                                                                           'tilekey = EXCLUDED.tilekey,' ||
                                                                                                                                                                                                           'shapeSegmented = EXCLUDED.shapeSegmented'));

    EXECUTE format('%s', concat('SELECT id FROM pg_dist_spatiotemporal_tables WHERE table_name = ''',table_name_out,''''))
        INTO table_out_id;

    -- Insert the metadata into the catalog table
    EXECUTE format('%s', concat('DELETE FROM pg_dist_spatiotemporal_tiles WHERE table_id=', table_out_id));
    IF tiling.isMobilityDB THEN
            EXECUTE format('%s', concat('insert into pg_dist_spatiotemporal_tiles (table_id, tile_key, mobdb_bbox, num_shapes, num_points) ' ||
                                    'SELECT ',table_out_id,',',tiling.tileKey,', bbox, numShapes, numPoints FROM ', table_name_out, '_catalog'));
    ELSE
            EXECUTE format('%s', concat('insert into pg_dist_spatiotemporal_tiles (table_id, tile_key, postgis_bbox, num_shapes, num_points) ' ||
                                    'SELECT ',table_out_id,',',tiling.tileKey,', bbox, numShapes, numPoints FROM ', table_name_out, '_catalog'));
    END IF;
    -- Drop the temporary table
    set client_min_messages to WARNING;
    EXECUTE format('%s', concat('DROP TABLE IF EXISTS ', table_name_out, '_catalog'));
    set client_min_messages to INFO;
    return table_out_id;
END;
$$ LANGUAGE 'plpgsql' STRICT;
