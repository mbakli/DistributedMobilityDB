CREATE OR REPLACE FUNCTION create_temporary_points_table(table_name_in varchar(250), tiling tiling, table_name_out varchar(250))
    RETURNS boolean AS $$
DECLARE
BEGIN
    -- Distribute the table using hash partitioning
    IF tiling.isMobilityDB THEN
        EXECUTE format('%s', concat('CREATE TABLE ',table_name_out,' (',tiling.groupCol,' integer, ',tiling.distCol,' tgeompoint); '));
        EXECUTE format('%s', concat('INSERT INTO ', table_name_out , ' ' ||
                                                                     ' SELECT ',tiling.groupCol,', unnest(instants(',tiling.distCol,'))  ',tiling.distCol,' ' ||
                                                                                                                                                                         'FROM ' , table_name_in));
        EXECUTE format('%s', concat('update ',table_name_in,' set ',tiling.distCol,'=setsrid(',tiling.distCol,', ', tiling.srid, ')'));
        EXECUTE format('%s', concat('SELECT create_distributed_table(''', table_name_out,''', ''',tiling.groupCol,''',colocate_with=>''',table_name_in,''');'));
    ELSE
        EXECUTE format('%s', concat('CREATE TABLE ',table_name_out,' (',tiling.groupCol,' integer, ',tiling.distCol,' geometry); '));
        EXECUTE format('%s', concat('INSERT INTO ', table_name_out , ' ' ||
                                                                     ' SELECT ',tiling.groupCol,', (points).geom as ',tiling.distCol,' FROM (SELECT ',tiling.groupCol,', ST_DumpPoints(',tiling.distCol,')  points ' ||
                                                                                                                                                                                                                    'FROM ' , table_name_in, ')t;'));
        EXECUTE format('%s', concat('update ',table_name_in,' set ',tiling.distCol,'=st_setsrid(',tiling.distCol,', ', tiling.srid, ')'));
    END IF;

-- TODO: Distribute the table
    EXECUTE format('%s', concat('CREATE INDEX ' , table_name_out , '_gist_idx on ' , table_name_out , '' ||
                                                                                                      ' using gist(' , tiling.distCol, ');'));
    EXECUTE format('%s', concat('CREATE INDEX ' , table_name_out , '_btree_idx on ' , table_name_out , '' ||
                                                                                                       ' using btree(',tiling.groupCol,');'));
    return true;
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
    RETURNS int AS $$
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

CREATE OR REPLACE FUNCTION getTileKey(table_name_in text)
    RETURNS text AS $$
BEGIN
    -- We keep it static for now
    RETURN 'tile_key';
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION getTilingType(table_name_in text)
    RETURNS text AS $$
BEGIN
    -- Change it to the column type
    RETURN 'spatial';
END;
$$ LANGUAGE 'plpgsql';

create or replace function getExtentInfo(table_name_in text, tiling tiling)
    RETURNS tileSize AS $$
declare
    extent tileSize;
begin
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
    RETURN ROW(
        extent.postgis_extent,
        extent.mobilitydb_extent,
        extent.numShapes,
        extent.numPoints
        )::tileSize;
end;
$$  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_next_dim(partitioning_type varchar(200), dim_role integer)
    RETURNS integer AS $$
DECLARE
BEGIN
    IF dim_role = 0 THEN
        IF partitioning_type  in  ('temporal', 'spatiotemporal') THEN
            return 1;
        ELSIF partitioning_type = 'spatial' THEN
            return 2;
        END IF;
    ELSIF dim_role = 1 THEN
        IF partitioning_type = 'temporal' THEN
            return 1;
        ELSIF partitioning_type = 'spatiotemporal' THEN
            return 2;
        END IF;
    ELSIF dim_role = 2 THEN
        return 3;
    ELSIF dim_role = 3 THEN
        IF partitioning_type = 'spatial' THEN
            return 2;
        ELSIF partitioning_type = 'spatiotemporal' THEN
            return 1;
        END IF;
    ELSE
        return 0;
    END IF;
END;
$$ LANGUAGE 'plpgsql' STRICT;
