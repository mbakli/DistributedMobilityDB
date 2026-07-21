----------------------------------------------------------------------------------------------------------------------
-- Warning for an undersized shared_buffers relative to the table being distributed
----------------------------------------------------------------------------------------------------------------------
/*
 * WarnIfSharedBuffersUndersized emits a RAISE WARNING (nothing else -- no
 * config is changed) when table_name_in's on-disk size exceeds the
 * instance's shared_buffers. Unlike the parallel-query settings below,
 * shared_buffers has GUC context 'postmaster': it can only be set at server
 * startup (postgresql.conf + a restart) and can't be changed, even
 * temporarily, from a running session or function -- so there is no
 * automatic fix to apply here, only a diagnostic.
 *
 * Confirmed by direct measurement this matters: BinarySearch and the
 * segmentation/allocation step both repeatedly re-scan overlapping portions
 * of table_name_in. With shared_buffers smaller than the table, every one
 * of those scans re-fetches from the OS page cache into Postgres' own
 * buffers from scratch (observed: 3 back-to-back identical queries all
 * showed ~68.5k buffer *reads*, 0 improvement between runs). Raising
 * shared_buffers past the table's size on the same test data/queries
 * showed the 2nd/3rd run drop to 0 reads (100% hit), and cut a real
 * 4-tile distribution's total time from ~68s to ~59s.
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
 * AutoTuneParallelQueryForDistribution derives and applies transaction-local
 * parallel-query settings from specs Postgres already knows about this
 * machine, so BinarySearch's repeated count(*)/sum(numInstants(...)) scans
 * below (up to ~100 rounds per dimension split, each independently
 * seq-scanning the source table) can actually use its idle cores instead of
 * running serially under the (usually conservative) session/database
 * defaults -- confirmed the planner never parallelizes these on its own,
 * since max_parallel_workers_per_gather defaults to 2 and its cost model
 * doesn't account for MobilityDB's && overlap operator being expensive
 * per-row on tgeompoint (large TOASTed trajectories).
 *
 * max_parallel_workers is the instance-wide cap on concurrently active
 * parallel workers, already sized to this machine by whoever configured
 * postgresql.conf (defaults to max_worker_processes, itself normally set
 * relative to CPU count) -- plain SQL/plpgsql has no direct nproc()-style
 * primitive, so reading this GUC back is the portable stand-in for "how
 * much parallelism does Postgres think this machine can support." Applied
 * via set_config(..., true) (transaction-local, like SET LOCAL): reverts
 * automatically at the end of the calling transaction, never leaking into
 * the caller's session. Does not touch BinarySearch/crange_method's own
 * search logic -- called once, at the top of
 * create_spatiotemporal_distributed_table.
 *
 * Measured effect (BerlinMOD SF1, 4-tile `trips` distribution): tile
 * generation went from 4m13s to 1m7s (~3.8x) with max_parallel_workers=8
 * on the test machine; the actual number here scales with whatever
 * max_parallel_workers is configured to on the host it runs on.
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