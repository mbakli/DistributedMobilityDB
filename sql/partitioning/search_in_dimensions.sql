----------------------------------------------------------------------------------------------------------------------
-- Granularity-aware bucket assignment (equal row count vs. equal weighted count)
----------------------------------------------------------------------------------------------------------------------
/*
 * Returns a window-function expression (text, spliced into the caller's own
 * dynamic SELECT) that assigns each row to one of num_buckets_expr buckets:
 * plain ntile() (equal ROW count) for 'shape-based'; a cumulative-weight
 * quantile bucketing (equal TOTAL WEIGHT, e.g. instant count) for
 * 'point-based', which ntile() can't express. weight_expr/order_expr are
 * caller-supplied expression text; partition_clause is '' or a literal
 * 'PARTITION BY ...'. Used by period_method and hierarchical_method so both
 * respect tiling.granularity.
 */
CREATE OR REPLACE FUNCTION WeightedNtileExpr(granularity text, num_buckets_expr text, weight_expr text, partition_clause text, order_expr text)
    RETURNS text AS $$
BEGIN
    IF granularity = 'point-based' THEN
        RETURN format(
            'LEAST(%s, GREATEST(1, ceil(sum(%s) OVER (%s ORDER BY %s) * %s::numeric / sum(%s) OVER (%s))))::integer',
            num_buckets_expr, weight_expr, partition_clause, order_expr, num_buckets_expr, weight_expr, partition_clause);
    ELSE
        RETURN format('ntile(%s) OVER (%s ORDER BY %s)', num_buckets_expr, partition_clause, order_expr);
    END IF;
END;
$$ LANGUAGE 'plpgsql';

----------------------------------------------------------------------------------------------------------------------
-- Warning for an undersized shared_buffers relative to the table being distributed
----------------------------------------------------------------------------------------------------------------------
/*
 * RAISE WARNING (no config change -- shared_buffers is postmaster-context,
 * restart-only) when table_name_in's on-disk size exceeds shared_buffers.
 * Confirmed to matter: BinarySearch/segmentation repeatedly re-scan
 * overlapping portions of the table; undersized, every scan re-fetches from
 * OS cache with 0% hit rate, versus 100% once shared_buffers covers it
 * (measured ~68s -> ~59s on a real 4-tile distribution).
 */
CREATE OR REPLACE FUNCTION WarnIfSharedBuffersUndersized(table_name_in text)
    RETURNS void AS $$
DECLARE
    table_size bigint;
    shared_buffers_size bigint;
BEGIN
    SELECT pg_total_relation_size(table_name_in::regclass) INTO table_size;
    SELECT pg_size_bytes(current_setting('shared_buffers')) INTO shared_buffers_size;
    IF table_size > shared_buffers_size THEN
        RAISE WARNING 'shared_buffers (%) is smaller than %''s on-disk size (%) -- tile generation and the segmentation/allocation step both repeatedly re-scan this table, so a larger shared_buffers (at least %, ideally more headroom) may significantly speed up distribution. This can only be changed via postgresql.conf/ALTER SYSTEM plus a server restart -- it cannot be set for just this session.',
            pg_size_pretty(shared_buffers_size), table_name_in, pg_size_pretty(table_size), pg_size_pretty(table_size);
    END IF;
END;
$$ LANGUAGE 'plpgsql';

----------------------------------------------------------------------------------------------------------------------
-- Auto-tuning parallel query settings for BinarySearch
----------------------------------------------------------------------------------------------------------------------
/*
 * Applies transaction-local parallel-query settings (via set_config(...,
 * true), auto-reverting -- never leaks into the caller's session) derived
 * from max_parallel_workers, so BinarySearch's repeated scans can use idle
 * cores instead of running serially under conservative defaults (the
 * planner never parallelizes these on its own: default
 * max_parallel_workers_per_gather=2, and its cost model underrates
 * MobilityDB's && operator on large TOASTed trajectories). Doesn't touch any
 * search logic. Measured ~3.8x (4m13s -> 1m7s) on a 4-tile BerlinMOD SF1 run.
 */
CREATE OR REPLACE FUNCTION AutoTuneParallelQueryForDistribution()
    RETURNS void AS $$
DECLARE
    available_workers integer;
BEGIN
    available_workers := GREATEST(current_setting('max_parallel_workers')::integer, 1);
    PERFORM set_config('max_parallel_workers_per_gather', available_workers::text, true);
    PERFORM set_config('min_parallel_table_scan_size', '0', true);
    PERFORM set_config('parallel_setup_cost', '0', true);
    PERFORM set_config('parallel_tuple_cost', '0', true);
    RAISE INFO 'Auto-tuned parallel query settings for distribution (max_parallel_workers_per_gather=%, derived from this instance''s max_parallel_workers)', available_workers;
END;
$$ LANGUAGE 'plpgsql';

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
        IF rounds > 100 THEN
            -- Safety valve: the per-tile target can be unreachable (e.g. a
            -- row count too small relative to the requested tile count
            -- rounds the target down to 0, or the split point plateaus at
            -- floating-point precision before ever exactly matching), which
            -- would otherwise loop forever. Settle for the current best
            -- split point instead of hanging indefinitely.
            IF dim = 1 THEN
                return midT::text;
            ELSE
                return mid::text;
            END IF;
        END IF;
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
            -- For sequence(set) types, the search targets (tileNumPoints) are counted in
            -- instants, not rows: count the instants of each row clipped to the candidate
            -- box instead of the row count, or the search can never converge (a row count
            -- of at most a few thousand trips can never approach a per-tile instant target
            -- derived from hundreds of thousands of GPS pings).
            IF tiling.internaltype IN ('sequence', 'sequenceset') THEN
                EXECUTE format('%s', concat('SELECT sum(numInstants(atStbox(',tiling.distCol,', ''', mobilitydb_bbox,'''::stbox))) FROM ',tableName,' ' ||
                                                                              'WHERE setsrid(',tiling.distCol,',',tiling.srid,') && ''', mobilitydb_bbox,'''::stbox '))
                    INTO binValue;
            ELSE
                EXECUTE format('%s', concat('SELECT count(*) FROM ',tableName,' ' ||
                                                                              'WHERE setsrid(',tiling.distCol,',',tiling.srid,') && ''', mobilitydb_bbox,'''::stbox '))
                    INTO binValue;
            END IF;
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