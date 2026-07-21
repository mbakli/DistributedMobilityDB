--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Period tiling method: partitions purely by temporal period -- each tile's STBOX is the
-- table's full spatial extent (identical across every tile) combined with that period's own
-- [period_start, period_end) time range. No spatial subdivision at all (contrast with
-- hierarchical_method, which further Z-order-splits each period spatially).
--------------------------------------------------------------------------------------------------------------------------------------------------------

/*
 * period_method builds a spatiotemporal tiling scheme for table_name_in as a
 * flat sequence of temporal-only tiles: every trajectory is assigned to one
 * of tiling.numTiles periods via ntile() ordered by its start time --
 * equi-COUNT periods (~the same number of trajectories each), not
 * equi-duration, matching the same period-boundary approach as
 * hierarchical_method. Each tile's bbox pairs the table's own full x/y
 * extent (constant across every tile) with that period's own time range, so
 * this method partitions strictly by time -- no row is excluded from its
 * period's tile on spatial grounds.
 *
 * Mirrors crange_method's/hierarchical_method's own flat per-call
 * catalog-table lifecycle (build <table_name_out>_catalog across the whole
 * run, assign tileKey serial once at the end, one single
 * add_distributed_table_metadata call) so the same query-time C code and
 * Citus shard-creation helpers that already work for those methods work here
 * unchanged.
 *
 * Scope: MobilityDB sequence/sequenceset (trajectory) columns only.
 *
 * Granularity-aware: tiling.granularity = 'point-based' buckets periods so
 * each gets ~the same TOTAL instant count (via WeightedNtileExpr), instead of
 * the default 'shape-based' ~equal trajectory COUNT -- matters when
 * trajectory lengths vary widely, since equal row counts wouldn't otherwise
 * balance actual data volume per period.
 */
CREATE OR REPLACE FUNCTION period_method(table_name_in text, table_name_out text, tiling tiling)
    RETURNS integer AS $$
DECLARE
    catalog_table text;
    table_out_id integer;
    start_time timestamp;
    full_extent stbox;
    tile_bbox stbox;
    period_rec RECORD;
    period_numshapes integer;
    period_numpoints bigint;
    weight_expr text;
    bucket_expr text;
BEGIN
    start_time := clock_timestamp();
    PERFORM AutoTuneParallelQueryForDistribution();

    IF NOT tiling.isMobilityDB OR tiling.internaltype NOT IN ('sequence', 'sequenceset') THEN
        RAISE EXCEPTION 'The period tiling method currently only supports MobilityDB sequence/sequenceset columns (got isMobilityDB=%, internaltype=%)',
            tiling.isMobilityDB, tiling.internaltype;
    END IF;

    RAISE INFO 'Period tiling: % period(s), granularity=%', tiling.numTiles, tiling.granularity;

    PERFORM create_catalog_table(table_name_out, tiling.isMobilityDB);
    catalog_table := concat(table_name_out, '_catalog');

    -- Full spatial extent, shared by every period tile -- same STBOX()
    -- construction pattern crange_method/hierarchical_method use throughout.
    EXECUTE format('SELECT extent(%I) FROM %I', tiling.distCol, table_name_in) INTO full_extent;

    IF tiling.granularity = 'point-based' THEN
        weight_expr := format('numInstants(%I)', tiling.distCol);
    ELSE
        weight_expr := '1';
    END IF;
    bucket_expr := WeightedNtileExpr(tiling.granularity, tiling.numTiles::text, 'w', '', 't');

    FOR period_rec IN
        EXECUTE format('
            WITH base AS (
                SELECT starttimestamp(%I) AS t, (%s)::numeric AS w
                FROM %I
            ), assigned AS (
                SELECT t, %s AS period_no
                FROM base
            )
            SELECT period_no, min(t) AS period_start, max(t) AS period_end, count(*) AS period_count
            FROM assigned
            GROUP BY period_no
            ORDER BY period_no',
            tiling.distCol, weight_expr, table_name_in, bucket_expr)
    LOOP
        tile_bbox := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(
                ST_MakePoint(xmin(full_extent), ymin(full_extent)),
                ST_MakePoint(xmax(full_extent), ymax(full_extent)))), tiling.srid),
            tstzrange(period_rec.period_start, period_rec.period_end)::tstzspan);

        EXECUTE format('SELECT count(*), sum(numInstants(%I)) FROM %I WHERE starttimestamp(%I) >= %L AND starttimestamp(%I) <= %L',
            tiling.distCol, table_name_in, tiling.distCol, period_rec.period_start, tiling.distCol, period_rec.period_end)
        INTO period_numshapes, period_numpoints;

        EXECUTE format('INSERT INTO %I (bbox, numPoints, numShapes) VALUES (%L, %s, %s)',
            catalog_table, tile_bbox, period_numpoints, period_numshapes);

        RAISE INFO 'Tile (period #%) created! [%, %), Shapes:%, Points:%',
            period_rec.period_no, period_rec.period_start, period_rec.period_end, period_numshapes, period_numpoints;
    END LOOP;

    -- Same finishing pattern as crange_method/hierarchical_method: assign
    -- dense tileKey values once, refresh tiling.numTiles to the real
    -- produced count, then a single add_distributed_table_metadata call.
    EXECUTE format('ALTER TABLE %I ADD column %I serial', catalog_table, tiling.tileKey);
    EXECUTE format('SELECT count(*) FROM %I', catalog_table) INTO tiling.numTiles;

    SELECT add_distributed_table_metadata(table_name_out, tiling)
    INTO table_out_id;

    RAISE INFO 'Run-time for period tiling:%', (clock_timestamp() - start_time);
    RETURN table_out_id;
END;
$$ LANGUAGE 'plpgsql';
