--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Quadtree tiling method: spatial-only partitioning via a genuine recursive region-quadtree
-- (contrast with hierarchical_method's Z-order/geohash-sort approximation, which never builds an
-- actual tree). No temporal dimension at all -- every tile's STBOX is spatial-only (constructed
-- via the single-argument stbox(geometry) constructor, which MobilityDB/&&  treats as having an
-- unconstrained/always-matching time dimension), so tiles partition purely by location.
--------------------------------------------------------------------------------------------------------------------------------------------------------

/*
 * quadtree_method builds a spatiotemporal tiling scheme for table_name_in as
 * a flat set of purely-spatial STBOX tiles, produced by a data-adaptive
 * recursive quadtree: starting from the table's own full x/y extent (the
 * root cell), a cell is only quartered into 4 children (NW/NE/SW/SE, split
 * at its own x/y midpoint) if its own row/point count -- of trajectories
 * genuinely CLIPPED to the cell via atStbox, not just bbox-overlapping it --
 * exceeds a target derived from tiling.numTiles; otherwise it's a leaf tile
 * as-is. Clipping first (rather than a plain "&&" overlap count) matters
 * specifically for wide-spanning trajectories: confirmed empirically that a
 * raw overlap count doesn't shrink as cells get smaller (a trip's bbox
 * either touches a cell or it doesn't, regardless of the cell's size), so
 * the recursion never converged near num_tiles with that cheaper check --
 * clipped content genuinely shrinks with cell size instead.
 *
 * This is deliberately NOT a fixed-depth quadtree (e.g. always splitting
 * exactly 3 levels for a constant 4^3=64 tiles): a fixed geometric depth
 * ignores density heterogeneity entirely -- if trips cluster more densely in
 * some areas than others (as BerlinMOD's do, around the city center), a
 * uniform geometric split produces wildly uneven per-tile content even
 * though the CELLS are evenly sized. Confirmed via research into how actual
 * distributed spatial partitioners handle this (GeoSpark/Sedona's quadtree
 * partitioner explicitly documents this same limitation for uniform
 * geometric splits, and uses a leaf-capacity/density-adaptive stopping rule
 * instead, same as here) -- and matches this codebase's own existing
 * philosophy: crange_method's BinarySearch converges on a target row/point
 * count per tile too, rather than splitting at fixed geometric midpoints.
 *
 * Implemented as an explicit work-queue (a temp table of pending cells),
 * processed iteratively rather than via PL/pgSQL recursion: pop one pending
 * cell, check its content against the target, and either record it as a
 * leaf tile or push its 4 children back onto the queue. A max-depth safety
 * cap (mirroring crange's BinarySearch "rounds > 100" safety valve) prevents
 * runaway recursion for degenerate cases (e.g. many trips clustered at
 * effectively the same point, which no amount of further splitting would
 * ever separate below the target). The queue table's cell_* column names
 * avoid xmin/xmax specifically because those collide with Postgres's own
 * reserved system column names.
 *
 * Granularity-aware: tiling.granularity = 'point-based' targets ~equal
 * TOTAL instant count per leaf; the default 'shape-based' targets ~equal
 * trajectory COUNT per leaf.
 *
 * IMPORTANT, empirically confirmed: 'shape-based' converges far worse than
 * 'point-based' specifically for this method, and gets WORSE (not better) at
 * higher num_tiles -- e.g. on a 1500-trip BerlinMOD sample, num_tiles=4
 * produced 28 leaves (7x) and num_tiles=16 produced 280 (17.5x) with
 * shape-based, versus 10 (2.5x) and 43 (2.7x) with point-based. Root cause:
 * a trajectory's *trip-count* contribution to a cell (how many distinct
 * trips merely pass through it) shrinks only roughly linearly with cell side
 * length as cells get smaller (a single route can still touch arbitrarily
 * many small cells along its path), not quadratically with cell area the
 * way point/instant *density* does -- so a shape-based target needs far more
 * splitting to converge. A RAISE WARNING below surfaces this to the caller
 * for shape-based runs rather than silently overriding their choice.
 *
 * Scope: MobilityDB sequence/sequenceset (trajectory) columns only. Mirrors
 * crange_method's/hierarchical_method's/period_method's own flat per-call
 * catalog-table lifecycle (build <table_name_out>_catalog across the whole
 * run, assign tileKey serial once at the end, one single
 * add_distributed_table_metadata call).
 */
CREATE OR REPLACE FUNCTION quadtree_method(table_name_in text, table_name_out text, tiling tiling)
    RETURNS integer AS $$
DECLARE
    catalog_table text;
    queue_table text;
    table_out_id integer;
    start_time timestamp;
    full_extent stbox;
    total_count numeric;
    target_count numeric;
    max_depth integer;
    cell_rec RECORD;
    cell_count integer;
    cell_points bigint;
    cell_bbox stbox;
    xmid float;
    ymid float;
    leaves_created integer;
    target_label text;
BEGIN
    start_time := clock_timestamp();
    PERFORM AutoTuneParallelQueryForDistribution();

    IF NOT tiling.isMobilityDB OR tiling.internaltype NOT IN ('sequence', 'sequenceset') THEN
        RAISE EXCEPTION 'The quadtree tiling method currently only supports MobilityDB sequence/sequenceset columns (got isMobilityDB=%, internaltype=%)',
            tiling.isMobilityDB, tiling.internaltype;
    END IF;

    IF tiling.granularity = 'shape-based' THEN
        RAISE WARNING 'quadtree tiling with shape-based granularity converges poorly (empirically confirmed: leaf count can overshoot num_tiles by 7-17x, worsening at higher num_tiles) because trip-count-per-cell shrinks only linearly, not quadratically, with cell size -- point-based granularity (tiling_granularity => ''point-based'') converges far more predictably for this method.';
    END IF;

    PERFORM create_catalog_table(table_name_out, tiling.isMobilityDB);
    catalog_table := concat(table_name_out, '_catalog');

    EXECUTE format('SELECT extent(%I) FROM %I', tiling.distCol, table_name_in) INTO full_extent;

    IF tiling.granularity = 'point-based' THEN
        EXECUTE format('SELECT sum(numInstants(%I)) FROM %I', tiling.distCol, table_name_in) INTO total_count;
        target_label := 'points';
    ELSE
        EXECUTE format('SELECT count(*) FROM %I', table_name_in) INTO total_count;
        target_label := 'shapes';
    END IF;
    target_count := GREATEST(1, total_count / tiling.numTiles);
    -- Enough headroom over the balanced-tree depth (log4(numTiles)) that
    -- legitimately uneven density can still converge, while still bounding
    -- runaway recursion for degenerate clustered data.
    max_depth := GREATEST(3, ceil(log(4, GREATEST(tiling.numTiles, 4)::numeric))::integer + 3);
    RAISE INFO 'Quadtree tiling: target % % per leaf (total %, num_tiles %), max_depth %',
        target_count, target_label, total_count, tiling.numTiles, max_depth;

    queue_table := concat(table_name_out, '_qtree_queue');
    EXECUTE format('DROP TABLE IF EXISTS %I', queue_table);
    EXECUTE format('CREATE TABLE %I (id serial, cell_xmin float, cell_xmax float, cell_ymin float, cell_ymax float, depth integer)', queue_table);
    EXECUTE format('INSERT INTO %I (cell_xmin, cell_xmax, cell_ymin, cell_ymax, depth) VALUES (%s, %s, %s, %s, 0)',
        queue_table, xmin(full_extent), xmax(full_extent), ymin(full_extent), ymax(full_extent));

    leaves_created := 0;
    LOOP
        EXECUTE format('SELECT id, cell_xmin, cell_xmax, cell_ymin, cell_ymax, depth FROM %I ORDER BY id LIMIT 1', queue_table)
        INTO cell_rec;
        EXIT WHEN cell_rec IS NULL;
        EXECUTE format('DELETE FROM %I WHERE id = %s', queue_table, cell_rec.id);

        cell_bbox := STBOX(ST_SetSrid(ST_Envelope(ST_MakeLine(
                ST_MakePoint(cell_rec.cell_xmin, cell_rec.cell_ymin),
                ST_MakePoint(cell_rec.cell_xmax, cell_rec.cell_ymax))), tiling.srid));

        /*
         * Genuinely clipped content (atStbox), not a raw bbox-overlap count:
         * a wide-spanning trajectory's bbox can overlap a cell far smaller
         * than the trajectory itself, so a plain "&& count" doesn't shrink
         * as cells get smaller and the recursion never converges (confirmed
         * empirically -- see the comment block above). Clipping first means
         * content genuinely shrinks with cell size. atStbox can legitimately
         * return NULL for a bbox-overlapping-but-actually-disjoint
         * trajectory (same subtlety already handled for the real
         * segmentation step this session), so those are excluded via
         * "WHERE clipped IS NOT NULL" rather than counted as zero-length
         * content.
         */
        EXECUTE format('
            SELECT count(*), sum(numInstants(clipped)) FROM (
                SELECT atStbox(%I, %L::stbox) AS clipped FROM %I WHERE %I && %L::stbox
            ) c WHERE clipped IS NOT NULL',
            tiling.distCol, cell_bbox, table_name_in, tiling.distCol, cell_bbox)
        INTO cell_count, cell_points;

        IF (CASE WHEN tiling.granularity = 'point-based' THEN cell_points ELSE cell_count END) <= target_count
                OR cell_rec.depth >= max_depth THEN
            -- Leaf: record as-is, no further splitting.
            EXECUTE format('INSERT INTO %I (bbox, numPoints, numShapes) VALUES (%L, %s, %s)',
                catalog_table, cell_bbox, coalesce(cell_points, 0), coalesce(cell_count, 0));
            leaves_created := leaves_created + 1;
            RAISE INFO 'Leaf tile #% created (depth %)! BBOX:%, Shapes:%, Points:%',
                leaves_created, cell_rec.depth, cell_bbox, cell_count, cell_points;
        ELSE
            -- Split into 4 quadrants (NW/NE/SW/SE) at this cell's own midpoint.
            xmid := (cell_rec.cell_xmin + cell_rec.cell_xmax) / 2.0;
            ymid := (cell_rec.cell_ymin + cell_rec.cell_ymax) / 2.0;
            EXECUTE format('INSERT INTO %I (cell_xmin, cell_xmax, cell_ymin, cell_ymax, depth) VALUES
                (%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s)',
                queue_table,
                cell_rec.cell_xmin, xmid, cell_rec.cell_ymin, ymid, cell_rec.depth + 1,
                xmid, cell_rec.cell_xmax, cell_rec.cell_ymin, ymid, cell_rec.depth + 1,
                cell_rec.cell_xmin, xmid, ymid, cell_rec.cell_ymax, cell_rec.depth + 1,
                xmid, cell_rec.cell_xmax, ymid, cell_rec.cell_ymax, cell_rec.depth + 1);
        END IF;
    END LOOP;

    EXECUTE format('DROP TABLE IF EXISTS %I', queue_table);

    -- Same finishing pattern as crange_method/hierarchical_method/period_method.
    EXECUTE format('ALTER TABLE %I ADD column %I serial', catalog_table, tiling.tileKey);
    EXECUTE format('SELECT count(*) FROM %I', catalog_table) INTO tiling.numTiles;

    SELECT add_distributed_table_metadata(table_name_out, tiling)
    INTO table_out_id;

    RAISE INFO 'Run-time for quadtree tiling:%', (clock_timestamp() - start_time);
    RETURN table_out_id;
END;
$$ LANGUAGE 'plpgsql';
