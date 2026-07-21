--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Quadtree tiling method: spatial-only partitioning via a genuine recursive region-quadtree
-- (contrast with hierarchical_method's Z-order/geohash-sort approximation, which never builds an
-- actual tree). No temporal dimension at all -- every tile's STBOX is spatial-only (constructed
-- via the single-argument stbox(geometry) constructor, which MobilityDB/&&  treats as having an
-- unconstrained/always-matching time dimension), so tiles partition purely by location.
--------------------------------------------------------------------------------------------------------------------------------------------------------

/*
 * quadtree_method: data-adaptive recursive quadtree over table_name_in's
 * full x/y extent -- a cell is quartered (NW/NE/SW/SE) only while its
 * genuinely-clipped content (atStbox, not just bbox overlap -- a raw
 * overlap count never shrinks with cell size for wide-spanning
 * trajectories) exceeds a target derived from tiling.numTiles; otherwise
 * it's a leaf. Not a fixed depth: uniform geometric splits ignore density
 * (same limitation GeoSpark/Sedona's quadtree partitioner documents),
 * matching crange_method's own target-convergence philosophy.
 *
 * Implemented as an explicit work-queue (temp table), not PL/pgSQL
 * recursion, with a max-depth safety cap. cell_* column names avoid
 * xmin/xmax, which collide with Postgres system columns.
 *
 * Granularity: 'point-based' targets equal instant count per leaf;
 * 'shape-based' targets equal trajectory count, but converges far worse
 * here (empirically: 7-17x num_tiles, worsening at larger num_tiles,
 * vs ~2.5-3x for point-based) since trip-count-per-cell shrinks linearly
 * with cell size, not quadratically like point density -- hence the
 * RAISE WARNING below rather than silently switching granularity.
 *
 * Scope: MobilityDB sequence/sequenceset columns only. Same flat
 * catalog-table lifecycle as crange_method/hierarchical_method/period_method.
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

        -- Clip first (atStbox), not a raw "&&" count -- see top comment.
        -- atStbox can legitimately return NULL for an overlapping-but-
        -- disjoint trajectory, hence "WHERE clipped IS NOT NULL".
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
