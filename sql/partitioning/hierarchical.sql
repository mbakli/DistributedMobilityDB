--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Hierarchical tiling method: temporal periods, each split spatially via Z-order-ordered
-- equi-count chunks (quadtree-style spatial locality via geohash ordering, rather than a
-- literal recursive quadtree cell subdivision).
--------------------------------------------------------------------------------------------------------------------------------------------------------

/*
 * DetermineHierarchicalPeriods reports each period's boundaries/count from an
 * already-populated period_assign_table (row_id, period_no, row_ts, geohash)
 * -- see hierarchical_method, which assigns period_no via a single ntile()
 * pass before calling this. Kept separate from that assignment step so the
 * boundary-reporting logic is independently readable/testable.
 */
CREATE OR REPLACE FUNCTION DetermineHierarchicalPeriods(period_assign_table text)
    RETURNS TABLE(period_no integer, period_start timestamptz, period_end timestamptz, period_count bigint) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT period_no, min(row_ts), max(row_ts), count(*)
        FROM %I
        GROUP BY period_no
        ORDER BY period_no', period_assign_table);
END;
$$ LANGUAGE 'plpgsql';

/*
 * hierarchical_method builds a spatiotemporal tiling scheme for table_name_in
 * as (period x spatial-chunk) STBOX cells:
 *
 *  1. Every trajectory is assigned to one of num_periods temporal periods via
 *     ntile() ordered by its start time -- equi-COUNT periods (~the same
 *     number of trajectories each), not equi-duration.
 *  2. Within each period, every trajectory's centroid is geohashed (a cheap,
 *     built-in Z-order/Morton-locality proxy: sorting by geohash string
 *     approximates a Z-order curve traversal without hand-written bit
 *     interleaving) and rows are bucketed into tiles_per_period further
 *     equi-count chunks ordered along that geohash -- so each chunk is
 *     spatially coherent (nearby trajectories grouped together).
 *
 * num_periods/tiles_per_period are auto-derived from tiling.numTiles (the
 * caller's total tile budget) as a balanced sqrt split, so the total
 * produced tile count never exceeds it (see the RAISE INFO below for the
 * exact numbers chosen).
 *
 * Concurrency: the per-period spatial bucketing (step 2 above) runs against a
 * PLAIN LOCAL (non-Citus-distributed) period-assignment table, using ordinary
 * intra-node parallel query workers (already tuned by
 * AutoTuneParallelQueryForDistribution) rather than spreading it across
 * worker nodes via a second Citus-distributed table. An earlier version of
 * this function did Citus-distribute the period-assignment table by
 * period_no for genuine cross-worker concurrency -- confirmed via EXPLAIN to
 * push the partition-aligned window function down per-shard with no
 * repartition step -- but creating and dropping a *second* Citus-distributed
 * table within the same transaction as the real output table's own
 * distribution corrupted Citus's worker connections under load: reproducibly
 * (not a one-off race) triggered "unexpected message type 0x58 during COPY
 * from stdin" / "protocol synchronization was lost" on worker connections at
 * num_tiles=8, with some shards silently never created at all ("relation ...
 * does not exist" mid-COPY). Fixing that properly would require converting
 * this call chain from FUNCTIONs to PROCEDUREs so the period-assignment
 * table's lifecycle could commit in its own transaction before the output
 * table's distribution begins (plain PL/pgSQL FUNCTIONs can't issue COMMIT
 * internally) -- a much larger, more invasive change than reverting to
 * single-node parallelism here.
 *
 * Scope: MobilityDB sequence/sequenceset (trajectory) columns only, shape-
 * based (trajectory-count) granularity -- mirrors crange_method's own flat
 * per-call catalog-table lifecycle (build <table_name_out>_catalog across
 * the whole run, assign tileKey serial once at the end, one single
 * add_distributed_table_metadata call) so the same query-time C code and
 * Citus shard-creation helpers (create_range_shards) that already work for
 * crange work here unchanged, and so no concurrent-writer race exists on
 * pg_dist_spatiotemporal_tiles (every write to shared catalog state happens
 * serially, in this one function, after all per-shard computation is done).
 */
CREATE OR REPLACE FUNCTION hierarchical_method(table_name_in text, table_name_out text, tiling tiling)
    RETURNS integer AS $$
DECLARE
    num_periods integer;
    tiles_per_period integer;
    catalog_table text;
    period_assign_table text;
    table_out_id integer;
    start_time timestamp;
    centroid_expr text;
    period_rec RECORD;
    period_starts timestamptz[];
    period_ends timestamptz[];
    chunk_rec RECORD;
    chunk_stbox stbox;
    chunk_extent stbox;
    chunk_numshapes integer;
    chunk_numpoints bigint;
BEGIN
    start_time := clock_timestamp();
    PERFORM AutoTuneParallelQueryForDistribution();

    IF NOT tiling.isMobilityDB OR tiling.internaltype NOT IN ('sequence', 'sequenceset') THEN
        RAISE EXCEPTION 'The hierarchical tiling method currently only supports MobilityDB sequence/sequenceset columns (got isMobilityDB=%, internaltype=%)',
            tiling.isMobilityDB, tiling.internaltype;
    END IF;

    num_periods := GREATEST(1, floor(sqrt(tiling.numTiles))::integer);
    tiles_per_period := GREATEST(1, floor(tiling.numTiles::numeric / num_periods)::integer);
    RAISE INFO 'Hierarchical tiling: % period(s) x ~% spatial tile(s)/period (target %, max %)',
        num_periods, tiles_per_period, tiling.numTiles, num_periods * tiles_per_period;

    PERFORM create_catalog_table(table_name_out, tiling.isMobilityDB);
    catalog_table := concat(table_name_out, '_catalog');

    -- Step 1: plain local table -- see the concurrency note above for why
    -- this isn't Citus-distributed.
    period_assign_table := concat(table_name_out, '_period_assign');
    EXECUTE format('DROP TABLE IF EXISTS %I', period_assign_table);
    EXECUTE format('CREATE TABLE %I (row_id integer, period_no integer, row_ts timestamptz, geohash text)', period_assign_table);

    -- Step 2: single pass -- assign every trajectory its period (equi-count,
    -- by start time) and its Z-order proxy (geohash of its centroid), in one
    -- INSERT ... SELECT over table_name_in. Plain local INSERT, eligible for
    -- the same intra-node parallel workers AutoTuneParallelQueryForDistribution
    -- already tunes for BinarySearch's scans elsewhere in this extension.
    centroid_expr := format('ST_Transform(ST_SetSRID(ST_Centroid(trajectory(%I)), %s), 4326)', tiling.distCol, tiling.srid);

    EXECUTE format('
        INSERT INTO %I (row_id, period_no, row_ts, geohash)
        SELECT %I, ntile(%s) OVER (ORDER BY starttimestamp(%I)), starttimestamp(%I), ST_GeoHash(%s, 20)
        FROM %I',
        period_assign_table, tiling.groupCol, num_periods, tiling.distCol, tiling.distCol, centroid_expr, table_name_in);

    -- Report + cache period boundaries (used below for each chunk's temporal range).
    period_starts := array_fill(NULL::timestamptz, ARRAY[num_periods]);
    period_ends := array_fill(NULL::timestamptz, ARRAY[num_periods]);
    FOR period_rec IN SELECT * FROM DetermineHierarchicalPeriods(period_assign_table) LOOP
        RAISE INFO 'Period #%: [%, %] ~% trajectories', period_rec.period_no, period_rec.period_start, period_rec.period_end, period_rec.period_count;
        period_starts[period_rec.period_no] := period_rec.period_start;
        period_ends[period_rec.period_no] := period_rec.period_end;
    END LOOP;

    -- Step 3: within each period, bucket rows into tiles_per_period
    -- equi-count, geohash-ordered chunks (PARTITION BY period_no keeps each
    -- period's ntile() independent of the others).
    FOR chunk_rec IN
        EXECUTE format('
            SELECT period_no, spatial_chunk, array_agg(row_id) AS row_ids
            FROM (
                SELECT row_id, period_no,
                       ntile(%s) OVER (PARTITION BY period_no ORDER BY geohash) AS spatial_chunk
                FROM %I
            ) bucketed
            GROUP BY period_no, spatial_chunk
            ORDER BY period_no, spatial_chunk',
            tiles_per_period, period_assign_table)
    LOOP
        -- Step 4: this chunk's tight spatial bbox, combined with its period's
        -- temporal range -- same extent()/STBOX() construction pattern
        -- crange_method uses throughout (xmin/xmax/ymin/ymax of a single
        -- extent() call, wrapped into an envelope + tstzspan).
        EXECUTE format('SELECT extent(%I), count(*), sum(numInstants(%I)) FROM %I WHERE %I = ANY(%L)',
            tiling.distCol, tiling.distCol, table_name_in, tiling.groupCol, chunk_rec.row_ids)
        INTO chunk_stbox, chunk_numshapes, chunk_numpoints;

        chunk_extent := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(
                ST_MakePoint(xmin(chunk_stbox), ymin(chunk_stbox)),
                ST_MakePoint(xmax(chunk_stbox), ymax(chunk_stbox)))), tiling.srid),
            tstzrange(period_starts[chunk_rec.period_no], period_ends[chunk_rec.period_no])::tstzspan);

        EXECUTE format('INSERT INTO %I (bbox, numPoints, numShapes) VALUES (%L, %s, %s)',
            catalog_table, chunk_extent, chunk_numpoints, chunk_numshapes);

        RAISE INFO 'Tile (period #%, chunk #%) created! BBOX:%, Shapes:%, Points:%',
            chunk_rec.period_no, chunk_rec.spatial_chunk, chunk_extent, chunk_numshapes, chunk_numpoints;
    END LOOP;

    -- Same finishing pattern as crange_method: assign dense tileKey values
    -- once, refresh tiling.numTiles to the REAL produced count (only known
    -- now, unlike crange's fixed-upfront count) before the single
    -- add_distributed_table_metadata call -- avoids any concurrent-write
    -- race on pg_dist_spatiotemporal_tiles, since every write above only
    -- ever touched this one local temp catalog table.
    EXECUTE format('ALTER TABLE %I ADD column %I serial', catalog_table, tiling.tileKey);
    EXECUTE format('SELECT count(*) FROM %I', catalog_table) INTO tiling.numTiles;

    SELECT add_distributed_table_metadata(table_name_out, tiling)
    INTO table_out_id;

    EXECUTE format('DROP TABLE IF EXISTS %I', period_assign_table);

    RAISE INFO 'Run-time for hierarchical tiling:%', (clock_timestamp() - start_time);
    RETURN table_out_id;
END;
$$ LANGUAGE 'plpgsql';
