--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Data Distribution
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION segmentation_and_allocation(table_name_in varchar(250), org_table_name_in varchar(250), tiling tiling, table_name_out varchar(250), table_id integer, org_table_columns text, group_by_clause text)
    RETURNS boolean AS $$
DECLARE
    temp text;
    obj_construction_query text;
    bbox_with_srid text;
BEGIN
    IF tiling.isMobilityDB THEN
        SELECT concat('setsrid(mobdb_bbox, ',tiling.srid,')')
        INTO bbox_with_srid;
    ELSE
        SELECT concat('st_setsrid(postgis_bbox, ',tiling.srid,')')
        INTO bbox_with_srid;
    END IF;
    --set client_min_messages to WARNING;
    RAISE INFO 'Distributing the %s into the overlapping tiles along with segmenting them:',tiling.internaltype;
    IF tiling.internaltype in ('linestring', 'multilinestring') THEN
        temp := org_table_columns;
        obj_construction_query := concat('ST_Intersection(t1.',tiling.distCol,', ',bbox_with_srid,') ');
        SELECT regexp_replace(org_table_columns, concat(',t1.',tiling.distCol), concat(',',obj_construction_query))
        INTO org_table_columns;
        IF temp = org_table_columns THEN
            SELECT regexp_replace(org_table_columns, concat(',t1."',tiling.distCol,'"'), concat(',',obj_construction_query))
            INTO org_table_columns;
        END IF;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',org_table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', tiling.distCol,' && postgis_bbox AND ST_Intersects(',tiling.distCol,', ',bbox_with_srid,') GROUP BY ',tiling.tileKey,',',bbox_with_srid,',', group_by_clause));
    ELSIF tiling.internaltype in ('polygon', 'multipolygon') THEN
        temp := org_table_columns;
        SELECT replace(org_table_columns, concat(',',tiling.distCol), concat(',ST_Intersection(',tiling.distCol,', ',bbox_with_srid,') '))
        INTO org_table_columns;
        IF temp = org_table_columns THEN
            SELECT replace(org_table_columns, concat(',t1."',tiling.distCol,'"'), concat(',ST_Intersection(t1.',tiling.distCol,', ',bbox_with_srid,') '))
            INTO org_table_columns;
        END IF;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', tiling.distCol,' && ',bbox_with_srid,' AND ST_Intersects(',tiling.distCol,', ',bbox_with_srid,') GROUP BY ',tiling.tileKey,',',bbox_with_srid,',', group_by_clause));
    ELSIF tiling.internaltype in ('sequence','sequenceset') THEN
        SELECT replace(org_table_columns, concat(',',tiling.distCol), concat(',tgeompoint_discseq(array_agg(',tiling.distCol,' ORDER BY starttimestamp(',tiling.distCol,'))) '))
        INTO org_table_columns;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', tiling.distCol,' && ',bbox_with_srid,' GROUP BY ',tiling.tileKey,',', group_by_clause));
    END IF;
    return true;
END;
$$ LANGUAGE 'plpgsql';