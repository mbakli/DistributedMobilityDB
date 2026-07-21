--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Hierarchical tiling method: temporal periods, each split spatially via Z-order-ordered
-- equi-count chunks (quadtree-style spatial locality via geohash ordering, rather than a
-- literal recursive quadtree cell subdivision).
--------------------------------------------------------------------------------------------------------------------------------------------------------

/*
 * Reports each period's boundaries/count from an already-populated
 * period_assign_table (row_id, period_no, row_ts, geohash, weight) --
 * hierarchical_method assigns period_no before calling this.
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
 * hierarchical_method tiles table_name_in as (period x spatial-chunk) STBOX
 * cells: every trajectory is assigned to one of num_periods equi-count
 * temporal periods (by start time), then within each period bucketed by
 * geohash (a Z-order/Morton-locality proxy) into tiles_per_period further
 * equi-count spatial chunks. num_periods/tiles_per_period are auto-derived
 * from tiling.numTiles as a balanced sqrt split.
 *
 * The per-period bucketing runs against a plain LOCAL table (intra-node
 * parallel workers), not a second Citus-distributed table: an earlier
 * version distributed it by period_no for genuine cross-worker concurrency
 * (confirmed via EXPLAIN to push down per-shard, no repartition), but a
 * second Citus-distributed table in the same transaction as the output
 * table's own distribution corrupted worker connections under load
 * (reproducible at num_tiles=8+: COPY protocol errors, silently-missing
 * shards). Fixing that needs FUNCTIONs -> PROCEDUREs so the period table can
 * commit separately -- out of scope for now.
 *
 * Scope: MobilityDB sequence/sequenceset columns only. Same flat
 * catalog-table lifecycle as crange_method (build the temp catalog table,
 * assign tileKey once, single add_distributed_table_metadata call) -- also
 * what keeps this race-free, since every catalog write happens serially here
 * after all per-shard computation is done.
 *
 * Granularity: 'point-based' buckets both periods and spatial chunks by
 * total instant count (via WeightedNtileExpr) instead of trajectory count;
 * each row's weight is computed once and stored in period_assign_table for
 * reuse in the spatial bucketing step.
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
    weight_expr text;
    period_bucket_expr text;
    spatial_bucket_expr text;
BEGIN
    start_time := clock_timestamp();
    PERFORM AutoTuneParallelQueryForDistribution();

    IF NOT tiling.isMobilityDB OR tiling.internaltype NOT IN ('sequence', 'sequenceset') THEN
        RAISE EXCEPTION 'The hierarchical tiling method currently only supports MobilityDB sequence/sequenceset columns (got isMobilityDB=%, internaltype=%)',
            tiling.isMobilityDB, tiling.internaltype;
    END IF;

    num_periods := GREATEST(1, floor(sqrt(tiling.numTiles))::integer);
    tiles_per_period := GREATEST(1, floor(tiling.numTiles::numeric / num_periods)::integer);
    RAISE INFO 'Hierarchical tiling: % period(s) x ~% spatial tile(s)/period (target %, max %), granularity=%',
        num_periods, tiles_per_period, tiling.numTiles, num_periods * tiles_per_period, tiling.granularity;

    PERFORM create_catalog_table(table_name_out, tiling.isMobilityDB);
    catalog_table := concat(table_name_out, '_catalog');

    -- Plain local table (see concurrency note above); weight is each row's
    -- bucketing weight, computed once and reused by the chunking step below.
    period_assign_table := concat(table_name_out, '_period_assign');
    EXECUTE format('DROP TABLE IF EXISTS %I', period_assign_table);
    EXECUTE format('CREATE TABLE %I (row_id integer, period_no integer, row_ts timestamptz, geohash text, weight numeric)', period_assign_table);

    -- Single pass: assign each trajectory its period and Z-order proxy (geohash of its centroid).
    centroid_expr := format('ST_Transform(ST_SetSRID(ST_Centroid(trajectory(%I)), %s), 4326)', tiling.distCol, tiling.srid);

    IF tiling.granularity = 'point-based' THEN
        weight_expr := format('numInstants(%I)', tiling.distCol);
    ELSE
        weight_expr := '1';
    END IF;
    period_bucket_expr := WeightedNtileExpr(tiling.granularity, num_periods::text, 'w', '', 't');

    EXECUTE format('
        INSERT INTO %I (row_id, period_no, row_ts, geohash, weight)
        SELECT row_id, %s, t, geohash, w
        FROM (
            SELECT %I AS row_id, starttimestamp(%I) AS t, ST_GeoHash(%s, 20) AS geohash, (%s)::numeric AS w
            FROM %I
        ) base',
        period_assign_table, period_bucket_expr, tiling.groupCol, tiling.distCol, centroid_expr, weight_expr, table_name_in);

    -- Report + cache period boundaries (used below for each chunk's temporal range).
    period_starts := array_fill(NULL::timestamptz, ARRAY[num_periods]);
    period_ends := array_fill(NULL::timestamptz, ARRAY[num_periods]);
    FOR period_rec IN SELECT * FROM DetermineHierarchicalPeriods(period_assign_table) LOOP
        RAISE INFO 'Period #%: [%, %] ~% trajectories', period_rec.period_no, period_rec.period_start, period_rec.period_end, period_rec.period_count;
        period_starts[period_rec.period_no] := period_rec.period_start;
        period_ends[period_rec.period_no] := period_rec.period_end;
    END LOOP;

    -- Within each period, bucket into tiles_per_period geohash-ordered chunks
    -- (PARTITION BY period_no keeps periods independent).
    spatial_bucket_expr := WeightedNtileExpr(tiling.granularity, tiles_per_period::text, 'weight', 'PARTITION BY period_no', 'geohash');
    FOR chunk_rec IN
        EXECUTE format('
            SELECT period_no, spatial_chunk, array_agg(row_id) AS row_ids
            FROM (
                SELECT row_id, period_no,
                       %s AS spatial_chunk
                FROM %I
            ) bucketed
            GROUP BY period_no, spatial_chunk
            ORDER BY period_no, spatial_chunk',
            spatial_bucket_expr, period_assign_table)
    LOOP
        -- This chunk's tight spatial bbox + its period's temporal range,
        -- via the same extent()/STBOX() pattern crange_method uses.
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

    -- Same finishing pattern as crange_method: assign tileKey once, refresh
    -- tiling.numTiles to the real produced count, single metadata call.
    EXECUTE format('ALTER TABLE %I ADD column %I serial', catalog_table, tiling.tileKey);
    EXECUTE format('SELECT count(*) FROM %I', catalog_table) INTO tiling.numTiles;

    SELECT add_distributed_table_metadata(table_name_out, tiling)
    INTO table_out_id;

    EXECUTE format('DROP TABLE IF EXISTS %I', period_assign_table);

    RAISE INFO 'Run-time for hierarchical tiling:%', (clock_timestamp() - start_time);
    RETURN table_out_id;
END;
$$ LANGUAGE 'plpgsql';
