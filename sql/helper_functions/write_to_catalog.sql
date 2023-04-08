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
                            'INSERT INTO pg_dist_spatiotemporal_tables (tblOid, tableName, numTiles, tilingMethod, tilingType, granularity, disjoint, isMobilityDB, distcol, distcoltype, tilekey, shapeSegmented, srid) ' ||
                            'VALUES (',0,',''',table_name_out,''',',tiling.numTiles,',''',tiling.method,''',''',tiling.type,''',''',tiling.granularity,''',''',tiling.disjointTiles,''',''',tiling.isMobilityDB,''', ''',tiling.distCol,''', ''',tiling.distColType,''', ''',tiling.tileKey,''', ''',tiling.segmentation,''', ',tiling.srid,') ' ||
                                                                                                                                                                                                           'ON CONFLICT (tableName) ' ||
                                                                                                                                                                                                           'DO ' ||
                                                                                                                                                                                                           'UPDATE set tableName = EXCLUDED.tableName,' ||
                                                                                                                                                                                                           'numTiles = EXCLUDED.numTiles,' ||
                                                                                                                                                                                                            'tilingMethod = EXCLUDED.tilingMethod,' ||
                                                                                                                                                                                                            'tilingType = EXCLUDED.tilingType,' ||
                                                                                                                                                                                                            'granularity = EXCLUDED.granularity,' ||
                                                                                                                                                                                                           'disjoint = EXCLUDED.disjoint,' ||
                                                                                                                                                                                                            'isMobilityDB = EXCLUDED.isMobilityDB,' ||
                                                                                                                                                                                                           'distcol = EXCLUDED.distcol,' ||
                                                                                                                                                                                                           'distcoltype = EXCLUDED.distcoltype,' ||
                                                                                                                                                                                                           'tilekey = EXCLUDED.tilekey,' ||
                                                                                                                                                                                                           'shapeSegmented = EXCLUDED.shapeSegmented,' ||
                                                                                                                                                                                                            'srid = EXCLUDED.srid'  ));

    EXECUTE format('%s', concat('SELECT id FROM pg_dist_spatiotemporal_tables WHERE tableName = ''',table_name_out,''''))
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

CREATE OR REPLACE FUNCTION update_catalog_oid(table_name_out varchar(250), table_id int)
	RETURNS boolean AS $$
DECLARE
    oid int;
BEGIN
    SELECT table_name_out::regclass
    INTO oid;
    UPDATE pg_dist_spatiotemporal_tables
    SET tblOid = oid
    WHERE id = table_id;
    RETURN true;
END;
$$ LANGUAGE 'plpgsql' STRICT;