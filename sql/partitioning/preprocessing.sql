CREATE OR REPLACE FUNCTION create_temporary_points_table(table_name_in varchar(250), spatiotemporal_col_name varchar(100), group_by_col varchar(50), mobilitydb_type boolean, table_name_out varchar(250), numtiles integer, srid int)
    RETURNS boolean AS $$
DECLARE
BEGIN
    -- Distribute the table using hash partitioning
    IF mobilitydb_type THEN
        EXECUTE format('%s', concat('CREATE TABLE ',table_name_out,' (',group_by_col,' integer, ',spatiotemporal_col_name,' tgeompoint); '));
        EXECUTE format('%s', concat('INSERT INTO ', table_name_out , ' ' ||
                                                                     ' SELECT ',group_by_col,', unnest(instants(',spatiotemporal_col_name,'))  ',spatiotemporal_col_name,' ' ||
                                                                                                                                                                         'FROM ' , table_name_in));
        EXECUTE format('%s', concat('SELECT create_distributed_table(''', table_name_out,''', ''',group_by_col,''',colocate_with=>''',table_name_in,''');'));
    ELSE
        EXECUTE format('%s', concat('CREATE TABLE ',table_name_out,' (',group_by_col,' integer, ',spatiotemporal_col_name,' geometry); '));
        EXECUTE format('%s', concat('INSERT INTO ', table_name_out , ' ' ||
                                                                     ' SELECT ',group_by_col,', (points).geom as ',spatiotemporal_col_name,' FROM (SELECT ',group_by_col,', ST_DumpPoints(',spatiotemporal_col_name,')  points ' ||
                                                                                                                                                                                                                    'FROM ' , table_name_in, ')t;'));
    END IF;
    EXECUTE format('%s', concat('update ',table_name_in,' set ',spatiotemporal_col_name,'=setsrid(',spatiotemporal_col_name,', ', srid, ')'));

-- TODO: Distribute the table
    EXECUTE format('%s', concat('CREATE INDEX ' , table_name_out , '_gist_idx on ' , table_name_out , '' ||
                                                                                                      ' using gist(' , spatiotemporal_col_name, ');'));
    EXECUTE format('%s', concat('CREATE INDEX ' , table_name_out , '_btree_idx on ' , table_name_out , '' ||
                                                                                                       ' using btree(',group_by_col,');'));
    return true;
END;
$$ LANGUAGE 'plpgsql';