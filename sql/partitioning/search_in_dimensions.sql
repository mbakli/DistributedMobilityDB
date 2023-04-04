----------------------------------------------------------------------------------------------------------------------
-- Binary Search for the spatial and spatiotemporal dimensions
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION BinarySearch(dim integer, tableName text, tiling tiling,tileNumPoints bigint, tileNumShapes integer, x1 float, x2 float, y1 float, y2 float, t1 timestamptz, t2 timestamptz)
    RETURNS text AS $$
DECLARE
    binValue bigint;
    startVal float;
    mid float;
    timePeriod tstzrange;
    stepVal float;
    endVal_temp float;
    mobilitydb_bbox stbox;
    postgis_bbox geometry;
    startT timestamptz;
    midT timestamptz;
    stepInterval interval;
    rounds integer;
    step float;
    check_step float;
BEGIN
    rounds := 0;
    mobilitydb_bbox := NULL;
    IF tiling.isMobilityDB THEN
        step := 0.1;
        check_step := 0.1;
    ELSE
        step := 0.2;
        check_step := 0.05;
    END IF;
    IF dim = 1 THEN
        timePeriod := tstzrange(t1,t2);
        startT := t1;
        stepInterval := (t2 - t1) / tiling.numTiles;
        --Select mid
        select (t1 + (t2 - t1)/2)
        INTO midT;
    ELSIF dim = 2 THEN
        IF tiling.isMobilityDB THEN
            timePeriod := tstzrange(t1,t2);
        END IF;
        startVal := x1;
        endVal_temp := x2;
        stepVal := (x2 - x1) * 0.1;
        select (x1 + (x2 - x1)/2.0)
        INTO mid;
    ELSIF dim = 3 THEN
        IF tiling.isMobilityDB THEN
            timePeriod := tstzrange(t1,t2);
        END IF;
        startVal := y1;
        endVal_temp := y2;
        stepVal := (y2 - y1) * 0.1;
        SELECT (y1 + (y2 - y1)/2.0)
        INTO mid;
    END IF;
    binValue := 0;
    LOOP
        --Select mid
        IF dim = 1 THEN
            mobilitydb_bbox := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(x1,y1),
                ST_MakePoint(x2, y2))), tiling.srid), tstzrange(startT,midT)::tstzspan);
        ELSIF dim = 2 THEN
            IF tiling.isMobilityDB THEN
                mobilitydb_bbox := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(startVal,y1),
                    ST_MakePoint(mid, y2))), tiling.srid), timePeriod::tstzspan);
            ELSE
                postgis_bbox := ST_MakeEnvelope(startVal,y1, mid, y2, tiling.srid);
            END IF;
        ELSIF dim = 3 THEN
            IF tiling.isMobilityDB THEN
                mobilitydb_bbox := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(ST_MakePoint(x1,startVal),
                    ST_MakePoint(x2, mid))), tiling.srid), timePeriod::tstzspan);
            ELSE
                postgis_bbox := ST_MakeEnvelope(x1,startVal,x2, mid, tiling.srid);
            END IF;
        END IF;

        --SELECT getBinVal(tableName, tiling , mobilitydb_bbox, postgis_bbox)
        --INTO binValue;
        IF tiling.isMobilityDB THEN
            EXECUTE format('%s', concat('SELECT count(*) FROM ',tableName,' ' ||
                                                                          'WHERE setsrid(',tiling.distCol,',',tiling.srid,') && ''', mobilitydb_bbox,'''::stbox '))
                INTO binValue;
        ELSE
            EXECUTE format('%s', concat('SELECT count(*)
                FROM ',tableName,' WHERE ',tiling.distCol,' && ''', postgis_bbox,'''::geometry'))
                INTO binValue;
        END IF;

        IF binValue IS NULL THEN
            binValue := 0;
        END IF;
        /*if rounds > 5 and mid <> 1 THEN
            step := step * 10.0;
            rounds := 0;
        end if;*/
        IF not tiling.isMobilityDB AND binValue between (tileNumShapes - tileNumShapes * check_step) AND
            (tileNumShapes + tileNumShapes * check_step) THEN
            IF dim = 1 THEN
                return midT::text;
            ELSE
                return mid::text;
            END IF;
        ELSIF tiling.isMobilityDB AND binValue between (tileNumPoints - tileNumPoints * check_step) AND
            (tileNumPoints + tileNumPoints * check_step) THEN
            IF dim = 1 THEN
                return midT::text;
            ELSE
                return mid::text;
            END IF;
        ELSIF not tiling.isMobilityDB AND binValue < tileNumShapes THEN
            rounds := rounds + 1;
            IF dim = 1 THEN
                midT := midT + (midT - t1) * step;
            ELSIF dim = 2 THEN
                mid := mid + (mid - x1) * step;
            ELSIF dim = 3 THEN
                mid := mid + (mid - y1) * step;
            END IF;
        ELSIF tiling.isMobilityDB AND binValue < tileNumPoints THEN
            rounds := rounds + 1;
            IF dim = 1 THEN
                midT := midT + (midT - t1) * step;
            ELSIF dim = 2 THEN
                mid := mid + (mid - x1) * step;
                -- x1 := mid + stepVal;
                -- x2 := endVal_temp;
            ELSIF dim = 3 THEN
                mid := mid + (mid - y1) * step;
                -- y1 := mid + stepVal;
                -- y2 := endVal_temp;
            END IF;
        ELSIF not tiling.isMobilityDB AND binValue > tileNumShapes THEN
            rounds := rounds + 1;
            IF dim = 1 THEN
                midT := midT - (midT - t1) * step;
            ELSIF dim = 2 THEN
                mid := mid - (mid - x1) * step;
                -- x2 := mid - stepVal;
            ELSIF dim = 3 THEN
                mid := mid - (mid - y1) * step;
                -- y2 := mid - stepVal;
            END IF;
        ELSIF  tiling.isMobilityDB AND binValue > tileNumPoints THEN
            rounds := rounds + 1;
            IF dim = 1 THEN
                midT := midT - (midT - t1) * step;
            ELSIF dim = 2 THEN
                mid := mid - (mid - x1) * step;
            ELSIF dim = 3 THEN
                mid := mid - (mid - y1) * step;
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE 'plpgsql' STRICT;


CREATE OR REPLACE FUNCTION getBinVal(tableName text, tiling tiling, mobilitydb_bbox stbox, postgis_bbox geometry)
    RETURNS bigint AS $$
DECLARE
    binValue bigint;
BEGIN

    IF tiling.isMobilityDB THEN
        EXECUTE format('%s', concat('SELECT count(*) FROM ',tableName,' ' ||
                            'WHERE setsrid(',tiling.distCol,',',tiling.srid,') && ''', mobilitydb_bbox,'''::stbox '))
        INTO binValue;
    ELSE
        EXECUTE format('%s', concat('SELECT count(*)
                FROM ',tableName,' WHERE ',tiling.distCol,' && ''', postgis_bbox,'''::geometry'))
        INTO binValue;
    END IF;
    return binValue;
END;
$$ LANGUAGE 'plpgsql' STRICT;