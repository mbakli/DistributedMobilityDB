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
        /*
         * The previous version of this branch tried to rebuild each trip's
         * per-tile trajectory via `tgeompoint_discseq(array_agg(distCol
         * ORDER BY starttimestamp(distCol)))`. Two bugs made this a no-op
         * rather than an actual clip:
         *  (1) the replace() search pattern was `,<distCol>` (e.g. ",trip")
         *      but org_table_columns' entries are table-prefixed (e.g.
         *      "t1.trip") -- the pattern never matched, so distCol was
         *      never substituted at all; the INSERT just selected the
         *      plain, unclipped t1.<distCol> column through unchanged.
         *      (Postgres didn't reject the then-still-present GROUP BY
         *      mismatch only because tripid, the source table's primary
         *      key, was in GROUP BY, and Postgres' functional-dependency
         *      rule lets every other column of that same row ride along
         *      ungrouped/unaggregated.)
         *  (2) tgeompoint_discseq(tgeompoint[]) doesn't exist in this
         *      MobilityDB version, and even its likely replacement,
         *      tgeompointseq(tgeompoint[], 'Discrete', ...), requires an
         *      array of temporal *instants* -- but distCol here is already
         *      a whole per-row sequence (one row = one trip), so
         *      array_agg(distCol) over a table with one row per trip always
         *      produces a trivial one-element array, which is the wrong
         *      shape of input for reconstructing a clipped trajectory
         *      either way.
         * atStbox(distCol, bbox) is the actual clipping primitive (already
         * used elsewhere in this codebase, e.g. crange.sql's tile-sizing
         * instant counts) -- applied directly per row/tile pair, with no
         * aggregation needed, since each source row already is one trip.
         * Verified: a trip spanning two tiles (2658 instants) clips to
         * 1489 in one tile and 1171 in the other, summing back to ~2658
         * with the small overlap expected at the tile boundary.
         */
        SELECT replace(org_table_columns, concat(',t1.',tiling.distCol), concat(',atStbox(t1.',tiling.distCol,', ',bbox_with_srid,') '))
        INTO org_table_columns;
        EXECUTE format('%s', concat('
            INSERT INTO ',table_name_out,'
            SELECT ',org_table_columns, ',',tiling.tileKey,'
            FROM ',table_name_in,' t1, pg_dist_spatiotemporal_tiles
            WHERE table_id=',table_id,' and ', tiling.distCol,' && ',bbox_with_srid));
    END IF;
    return true;
END;
$$ LANGUAGE 'plpgsql';