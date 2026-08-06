--------------------------------------------------------------------------------------------------------------------------------------------------------
-- *** EXPERIMENTAL / LIGHTLY TESTED -- verify carefully before pointing this at real data ***
--
-- Deliberately excluded from the CMakeLists.txt build (sql/partitioning/CMakeLists.txt) -- these functions are NOT part of the shipped extension yet.
--
-- History: an earlier debugging pass here reproducibly crashed the Citus backend (SIGSEGV) on a table built by
-- parallel_spatiotemporal_data_allocation. Root-caused via manual bisection to the TEST HARNESS, not this code: update_catalog_oid (an existing
-- helper, write_to_catalog.sql, not part of this file) was called with a table_id borrowed from a DIFFERENT, already-existing table's catalog row
-- (a shortcut used while testing parallel_spatiotemporal_data_allocation directly, bypassing create_spatiotemporal_distributed_table_parallel's own
-- correct id lookup) -- update_catalog_oid has no validation that the given table_id's row actually belongs to table_name_out, so this silently
-- overwrote a different table's tblOid. Retested end to end with a properly, uniquely inserted catalog row: correct results, no crash, cluster stays
-- healthy throughout, for BOTH the staged path (parallel_spatiotemporal_data_allocation) AND the direct path
-- (parallel_spatiotemporal_data_allocation_direct) -- but only against a small PostGIS point fixture.
--
-- Retested since against a real MobilityDB trajectory table (300 real trips/~150k instants copied read-only from berlinmod.trips, real crange_method
-- tile boundaries, segmentation=true): the STAGED path (parallel_spatiotemporal_data_allocation) produces correct results, matching the existing
-- serial spatiotemporal_data_allocation row-for-row, with no crash. The DIRECT path (parallel_spatiotemporal_data_allocation_direct) does NOT --
-- querying its output table crashes the backend (SIGSEGV) even on a trivial non-aggregate SELECT, not just the aggregate/GROUP BY shape that
-- triggered the (now-fixed) planner bug. This reproduces specifically for MobilityDB tgeompoint data; the same direct-write mechanism was fine
-- against the small PostGIS point fixture. Suspected cause: the direct variant's table_name_out shell is created AND written by several independent
-- dblink connections (see the design-rationale comment below) -- something about a freshly-created table with a tgeompoint column apparently doesn't
-- reconcile safely across that many separate sessions, unlike a plain geometry column. Not yet root-caused further.
--
-- *** DO NOT USE parallel_spatiotemporal_data_allocation_direct / create_spatiotemporal_distributed_table_parallel_direct ON MOBILITYDB DATA. ***
-- The staged path is the one to use; it's the one that's actually verified against real trajectory data.
--
-- Linestring/polygon branches (both variants) remain entirely unexercised. berlinmod/berlinmod_sf1 have only ever been read from, never written to,
-- by any of this testing.
--------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Parallel data allocation -- an alternative to spatiotemporal_data_allocation (data_allocation.sql/shape_segmentation.sql) that fans the actual
-- trajectory-to-tile movement out across multiple concurrent connections instead of one serial "INSERT INTO table_name_out SELECT ... FROM
-- table_name_in, pg_dist_spatiotemporal_tiles" statement covering every tile at once.
--
-- Deliberately kept in its own file, calling its own new functions only -- nothing here is called by tiling.sql/data_allocation.sql/
-- shape_segmentation.sql, and nothing in those files is modified. The existing create_spatiotemporal_distributed_table path is completely unaffected;
-- this is an opt-in alternative entry point (create_spatiotemporal_distributed_table_parallel below).
--
-- Two things make the existing single-statement allocation slow at scale:
--   1. It deliberately reads from table_name_in UNSTAGED (a plain coordinator-local table, not the hash-distributed/GIST-indexed staged_source that
--      granularity detection and tile-boundary generation already use) -- staged_source can't be used there because Citus can't push down an
--      INSERT...SELECT between a hash-distributed source and a range-distributed target that aren't colocated (see the comment at its call site in
--      tiling.sql). So the whole scan+match runs as one serial, unindexed, coordinator-local pass.
--   2. It's one query covering every tile, so there's nothing to parallelize across connections even if the scan itself were faster.
--
-- The fix: per tile, stage matching rows into their own local UNLOGGED table (a plain local relation -- Citus raises no colocation objection to
-- reading a hash-distributed table into a local sink), then move each staged table into its real shard.
--   - Phase 1 (stage) fans out over a pool of dblink connections (dblink_parallel_exec below): N tiles' scans run concurrently, and each one can
--     use staged_source's GIST index and Citus' own cluster-wide fan-out for that one query -- exactly the pattern stage_hash_distributed_source
--     (tiling.sql) already uses safely for a read-only query into a local sink.
--   - Phase 2 (finalize) deliberately does NOT use dblink -- see the connection-safety note below. It runs in-process, one INSERT per tile, but
--     each one now reads from a small pre-filtered local table instead of the original source, so it's still far cheaper than the single
--     all-tiles-at-once INSERT spatiotemporal_data_allocation runs today, just not concurrent across tiles.
--
-- Connection safety: this version creates table_name_out's shell + range shards in-process (create_tiled_output_table_shell, called directly, not
-- via dblink), and Phase 2's finalize inserts also run in-process, one per tile -- table_name_out only ever needs to be visible to this function's
-- own transaction. The *_direct variant further down (parallel_spatiotemporal_data_allocation_direct) instead creates the shell over its own
-- committing dblink connection and writes every tile directly into table_name_out from the dblink pool -- an earlier debugging session suspected
-- that combination of a crash, but that was root-caused to an unrelated test-harness bug (see the file-level comment above); retested properly,
-- both designs work. This one is kept as the safer-by-construction default (only in-process code ever touches table_name_out); the direct variant
-- trades that margin for one fewer write pass overall.
--
-- Known gap: does not support crange tiling method with point-based granularity on sequence/sequenceset columns -- that combination needs the
-- exploded "_temp" preprocessing table crange_method builds during tile-boundary generation, which create_spatiotemporal_distributed_table
-- (called here with physical_partitioning => false, to reuse its boundary-generation/staging/catalog logic unmodified) already drops before
-- returning, regardless of physical_partitioning. Use the standard create_spatiotemporal_distributed_table for that specific combination.
--------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- dblink_parallel_exec: fans `queries` out over up to `degree` concurrent dblink connections (round-robin -- as soon as one connection's query
-- finishes, it picks up the next queued one), waits for all of them, then closes every connection. Generic over the query text, so it's reused for
-- both the stage phase and the finalize phase below.
--
-- dblink_get_result raises a local exception if the remote query it was waiting on failed (its default fail_on_error => true), so a single bad tile
-- surfaces as a normal plpgsql exception here -- caught just long enough to close every open connection before re-raising, so a failure never leaks
-- dblink sessions.
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dblink_parallel_exec(degree integer, queries text[])
    RETURNS void AS $$
DECLARE
    conn_str text := format('dbname=%s user=%s port=%s', current_database(), current_user, current_setting('port'));
    conn_names text[];
    conn_slot_query integer[]; -- index into queries running on each slot right now, NULL if idle
    n integer := array_length(queries, 1);
    next_idx integer := 1;
    busy_count integer := 0;
    i integer;
    drained integer;
BEGIN
    IF n IS NULL OR n = 0 THEN
        RETURN;
    END IF;
    degree := LEAST(GREATEST(degree, 1), n);

    FOR i IN 1..degree LOOP
        conn_names[i] := format('dmdb_pdba_%s_%s', pg_backend_pid(), i);
        PERFORM dblink_connect(conn_names[i], conn_str);
        conn_slot_query[i] := NULL;
    END LOOP;

    LOOP
        EXIT WHEN next_idx > n AND busy_count = 0;
        FOR i IN 1..degree LOOP
            IF conn_slot_query[i] IS NOT NULL AND dblink_is_busy(conn_names[i]) = 0 THEN
                -- dblink_get_result must be called until it returns zero rows to fully drain a
                -- command (confirmed directly: even a single CREATE TABLE AS/INSERT needs a second,
                -- empty call) -- one call alone leaves the connection still "mid-command", and the
                -- next dblink_send_query on it fails with "another command is already in progress".
                LOOP
                    PERFORM x FROM dblink_get_result(conn_names[i]) AS r(x text);
                    GET DIAGNOSTICS drained = ROW_COUNT;
                    EXIT WHEN drained = 0;
                END LOOP;
                conn_slot_query[i] := NULL;
                busy_count := busy_count - 1;
            END IF;
            IF conn_slot_query[i] IS NULL AND next_idx <= n THEN
                PERFORM dblink_send_query(conn_names[i], queries[next_idx]);
                conn_slot_query[i] := next_idx;
                busy_count := busy_count + 1;
                next_idx := next_idx + 1;
            END IF;
        END LOOP;
        IF next_idx <= n OR busy_count > 0 THEN
            PERFORM pg_sleep(0.02);
        END IF;
    END LOOP;

    FOR i IN 1..degree LOOP
        PERFORM dblink_disconnect(conn_names[i]);
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    FOR i IN 1..degree LOOP
        BEGIN
            PERFORM dblink_disconnect(conn_names[i]);
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END LOOP;
    RAISE;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- create_tiled_output_table_shell: table_name_out's shell (LIKE table_name_in, lz4-compressed distCol, tileKey column) plus its range shards --
-- exactly the preamble spatiotemporal_data_allocation itself runs (data_allocation.sql), pulled out here so it can be run via its own committing
-- dblink call (see the file header for why).
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_tiled_output_table_shell(
    table_name_in text, dist_col text, internal_type text, is_mobilitydb boolean,
    tile_key_col text, num_tiles integer, table_name_out text
) RETURNS boolean AS $$
BEGIN
    EXECUTE format('SET citus.shard_count = %s', num_tiles);
    EXECUTE format('CREATE TABLE %s (LIKE %I)', table_name_out, table_name_in);
    IF NOT is_mobilitydb AND internal_type = 'linestring' THEN
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I TYPE geometry', table_name_out, dist_col);
    END IF;
    EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET COMPRESSION lz4', table_name_out, dist_col);
    EXECUTE format('ALTER TABLE %s ADD COLUMN %I integer', table_name_out, tile_key_col);
    EXECUTE format('SELECT create_distributed_table(%L, %L, %L)', table_name_out, tile_key_col, 'range');
    EXECUTE format('SELECT create_range_shards(%s, %L)', num_tiles, table_name_out);
    RETURN true;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- parallel_spatiotemporal_data_allocation: the parallel replacement for spatiotemporal_data_allocation. table_name_in here should be the
-- hash-distributed/GIST-indexed staged_source (same table create_spatiotemporal_distributed_table's own granularity detection/tile-boundary search
-- already read from) -- unlike the serial path, staging is exactly what makes the per-tile scans below fast and Citus-parallelizable.
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION parallel_spatiotemporal_data_allocation(
    table_name_in text,
    tiling tiling,
    table_name_out text,
    table_id integer,
    parallel_degree integer DEFAULT NULL
) RETURNS boolean AS $$
DECLARE
    conn_str text := format('dbname=%s user=%s port=%s', current_database(), current_user, current_setting('port'));
    degree integer;
    shell_ok boolean;
    org_table_columns text;
    group_by_clause text;
    n_tiles integer;
    tile_keys integer[];
    mobdb_bboxes text[];
    postgis_bboxes text[];
    exp_shapes bigint[];
    stage_queries text[];
    stage_tables text[];
    bbox_expr text;
    col_expr text;
    where_clause text;
    group_clause text;
    temp_check text;
    j integer;
    tk integer;
    got_count bigint;
    start_time timestamp := clock_timestamp();
    -- pg_dist_spatiotemporal_tiles has its own table_id column -- plpgsql's default
    -- #variable_conflict=error rejects even a qualified reference to it (t.table_id = table_id)
    -- once any bare "table_id" is in scope, since a bare column with that name exists too. A local
    -- copy under a different name sidesteps the whole ambiguity instead of relying on qualification.
    tbl_id integer := table_id;
BEGIN
    IF tiling.method = 'crange' AND tiling.internaltype NOT IN ('point','polygon','instant') AND tiling.granularity = 'point-based' THEN
        RAISE EXCEPTION 'parallel_spatiotemporal_data_allocation does not support crange point-based granularity on % columns (needs the exploded _temp table, already dropped earlier in the pipeline) -- use create_spatiotemporal_distributed_table for this combination instead.', tiling.internaltype;
    END IF;

    degree := coalesce(parallel_degree, (SELECT count(*) FROM pg_dist_node WHERE noderole = 'primary'));
    IF degree < 1 THEN
        degree := 1;
    END IF;

    -- Run in-process (this transaction), NOT via dblink: table_name_out needs to be visible to
    -- Phase 2's finalize inserts below, which also now run in-process for the same reason -- see
    -- the file header. An earlier version created this shell over its own dblink connection
    -- (mirroring stage_hash_distributed_source) to make it visible to a separate Phase 2 connection
    -- pool; reproduced directly that doing so leaves Citus's shard/connection metadata caches
    -- inconsistent enough across that many independent sessions touching one freshly-created
    -- distributed table to crash the backend (SIGSEGV) on a later plain SELECT against it -- not
    -- merely an error, an actual crash. Phase 1 (staging) never touches table_name_out at all, so
    -- it keeps its full dblink-based concurrency; only Phase 2 gives up connection-level
    -- parallelism, and only because table_name_out is small-per-tile input by the time it runs.
    SELECT create_tiled_output_table_shell(table_name_in, tiling.distCol, tiling.internaltype, tiling.isMobilityDB,
                                            tiling.tileKey, tiling.numTiles, table_name_out)
    INTO shell_ok;
    IF NOT coalesce(shell_ok, false) THEN
        RAISE EXCEPTION 'Failed to create the shell/shards for %', table_name_out;
    END IF;

    -- Same column-list/GROUP BY derivation as spatiotemporal_data_allocation (data_allocation.sql).
    IF tiling.isMobilityDB THEN
        SELECT array_to_string(array_agg(concat('t1.', a.attname) ORDER BY a.attnum), ',')
        INTO org_table_columns FROM pg_attribute a
        WHERE a.attrelid = table_name_in::regclass AND a.attnum > 0 AND NOT a.attisdropped;
    ELSE
        SELECT array_to_string(array_agg(concat('t1."', a.attname, '"') ORDER BY a.attnum), ',')
        INTO org_table_columns FROM pg_attribute a
        WHERE a.attrelid = table_name_in::regclass AND a.attnum > 0 AND NOT a.attisdropped;
    END IF;
    group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1.',tiling.distCol,')'), ''), ',,',','), '^,',''), ',$','');
    IF group_by_clause = org_table_columns THEN
        group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1."',tiling.distCol,'")'), ''), ',,',','), '^,',''), ',$','');
    END IF;

    -- Tile list -- read locally (this transaction wrote these rows itself, earlier in this same call, via the tiling method's own dispatch).
    SELECT array_agg(t.tile_key ORDER BY t.tile_key),
           array_agg(t.mobdb_bbox::text ORDER BY t.tile_key),
           array_agg(t.postgis_bbox::text ORDER BY t.tile_key),
           array_agg(t.num_shapes ORDER BY t.tile_key)
    INTO tile_keys, mobdb_bboxes, postgis_bboxes, exp_shapes
    FROM pg_dist_spatiotemporal_tiles t
    WHERE t.table_id = tbl_id;

    n_tiles := coalesce(array_length(tile_keys, 1), 0);
    IF n_tiles = 0 THEN
        RAISE EXCEPTION 'No tiles found in pg_dist_spatiotemporal_tiles for table_id %', table_id;
    END IF;
    RAISE INFO 'Parallel allocation: % tiles, parallel_degree %', n_tiles, degree;

    -- Phase 1: stage each tile's matching rows into its own local UNLOGGED table.
    stage_tables := array_fill(NULL::text, ARRAY[n_tiles]);
    stage_queries := array_fill(NULL::text, ARRAY[n_tiles]);
    FOR j IN 1..n_tiles LOOP
        tk := tile_keys[j];
        stage_tables[j] := format('dist_mobilitydb.%I', concat(table_name_out, '_pstage_', tk));

        IF tiling.isMobilityDB THEN
            bbox_expr := format('setsrid(%L::stbox, %s)', mobdb_bboxes[j], tiling.srid);
        ELSE
            bbox_expr := format('st_setsrid(%L::geometry, %s)', postgis_bboxes[j], tiling.srid);
        END IF;

        col_expr := org_table_columns;
        group_clause := NULL;

        IF tiling.internaltype = 'instant' THEN
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
            group_clause := group_by_clause;
        ELSIF tiling.internaltype = 'point' THEN
            where_clause := concat('(st_contains(', bbox_expr, ', t1.', tiling.distCol, ') or st_intersects(ST_Boundary(', bbox_expr, '), t1.', tiling.distCol, '))');
            group_clause := group_by_clause;
        ELSIF tiling.internaltype IN ('linestring','polygon') AND NOT tiling.segmentation THEN
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
        ELSIF tiling.internaltype IN ('linestring','multilinestring') AND tiling.segmentation THEN
            temp_check := col_expr;
            col_expr := regexp_replace(col_expr, concat(',t1.',tiling.distCol), concat(',ST_Intersection(t1.',tiling.distCol,', ',bbox_expr,') '));
            IF col_expr = temp_check THEN
                col_expr := regexp_replace(col_expr, concat(',t1."',tiling.distCol,'"'), concat(',ST_Intersection(t1.',tiling.distCol,', ',bbox_expr,') '));
            END IF;
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr, ' AND ST_Intersects(t1.', tiling.distCol, ', ', bbox_expr, ')');
            group_clause := concat(bbox_expr, ',', group_by_clause);
        ELSIF tiling.internaltype IN ('polygon','multipolygon') AND tiling.segmentation THEN
            temp_check := col_expr;
            col_expr := replace(col_expr, concat(',',tiling.distCol), concat(',ST_Intersection(',tiling.distCol,', ',bbox_expr,') '));
            IF col_expr = temp_check THEN
                col_expr := replace(col_expr, concat(',t1."',tiling.distCol,'"'), concat(',ST_Intersection(t1.',tiling.distCol,', ',bbox_expr,') '));
            END IF;
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr, ' AND ST_Intersects(t1.', tiling.distCol, ', ', bbox_expr, ')');
            group_clause := concat(bbox_expr, ',', group_by_clause);
        ELSIF tiling.internaltype IN ('sequence','sequenceset') AND NOT tiling.segmentation THEN
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
        ELSIF tiling.internaltype IN ('sequence','sequenceset') AND tiling.segmentation THEN
            col_expr := replace(col_expr, concat(',t1.',tiling.distCol), concat(',atStbox(t1.',tiling.distCol,', ',bbox_expr,') '));
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
        ELSE
            RAISE EXCEPTION 'Unsupported internaltype/segmentation combination for parallel allocation: % / segmentation=%', tiling.internaltype, tiling.segmentation;
        END IF;

        stage_queries[j] := concat('CREATE UNLOGGED TABLE ', stage_tables[j], ' AS SELECT ', col_expr, ', ', tk, ' AS ', tiling.tileKey,
                                    ' FROM ', table_name_in, ' t1 WHERE ', where_clause,
                                    CASE WHEN group_clause IS NOT NULL THEN concat(' GROUP BY ', group_clause) ELSE '' END);
    END LOOP;

    PERFORM dblink_parallel_exec(degree, stage_queries);
    RAISE INFO 'Parallel allocation: phase 1 (stage) done, elapsed %', clock_timestamp() - start_time;

    -- Validation checkpoint: compare each tile's staged row count against what the tiling method already computed for
    -- pg_dist_spatiotemporal_tiles.num_shapes. Not fatal on mismatch (there are legitimate reasons the two could drift, e.g. tie-breaking on a
    -- shared tile boundary) but surfaced loudly rather than shipped silently.
    FOR j IN 1..n_tiles LOOP
        EXECUTE format('SELECT count(*) FROM %s', stage_tables[j]) INTO got_count;
        IF got_count IS DISTINCT FROM exp_shapes[j] THEN
            RAISE WARNING 'Tile % staged % rows, catalog (num_shapes) expected % -- verify this run before trusting it', tile_keys[j], got_count, exp_shapes[j];
        END IF;
    END LOOP;

    -- Phase 2: move each tile's staged rows into its real shard -- in-process, not via dblink (see
    -- the shell-creation comment above for why). Each INSERT now reads from a small, already-
    -- filtered local table instead of the original source, so this is still far cheaper than the
    -- single all-tiles-at-once INSERT spatiotemporal_data_allocation runs, just no longer
    -- concurrent across tiles.
    FOR j IN 1..n_tiles LOOP
        EXECUTE format('INSERT INTO %s SELECT * FROM %s', table_name_out, stage_tables[j]);
    END LOOP;
    RAISE INFO 'Parallel allocation: phase 2 (finalize) done, total elapsed %', clock_timestamp() - start_time;

    FOR j IN 1..n_tiles LOOP
        EXECUTE format('DROP TABLE IF EXISTS %s', stage_tables[j]);
    END LOOP;

    PERFORM update_catalog_oid(table_name_out, table_id);
    RETURN true;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- create_spatiotemporal_distributed_table_parallel: same signature/behaviour as create_spatiotemporal_distributed_table (tiling.sql), minus
-- is_reference_table/physical_partitioning (reference tables aren't tiled at all; physical partitioning is always on here, that's the point) plus
-- parallel_degree. Delegates tile-boundary generation, staging, and catalog population entirely to the existing, unmodified
-- create_spatiotemporal_distributed_table (called with physical_partitioning => false, so it does everything except move any data), then runs
-- parallel_spatiotemporal_data_allocation for the actual move.
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_spatiotemporal_distributed_table_parallel(
    table_name_in text,
    table_name_out text,
    num_tiles integer DEFAULT 1,
    tiling_method text DEFAULT 'crange',
    tiling_granularity text DEFAULT NULL,
    tiling_type text DEFAULT NULL,
    spatiotemporal_col_name varchar(50) DEFAULT NULL,
    shape_segmentation boolean DEFAULT TRUE,
    parallel_degree integer DEFAULT NULL
) RETURNS boolean AS $$
DECLARE
    tiling tiling;
    table_id integer;
    staged_source text;
    staged_fresh boolean;
    start_time timestamp := clock_timestamp();
BEGIN
    PERFORM create_spatiotemporal_distributed_table(
        table_name_in, table_name_out, num_tiles, tiling_method, tiling_granularity, tiling_type,
        NULL::text, NULL::text, spatiotemporal_col_name,
        physical_partitioning => false, shape_segmentation => shape_segmentation, is_reference_table => false
    );

    SELECT id, tilingMethod, granularity, disjoint, isMobilityDB, distcol, distcoltype, tilekey, shapeSegmented, srid, groupcol, numTiles
    INTO table_id, tiling.method, tiling.granularity, tiling.disjointTiles, tiling.isMobilityDB, tiling.distCol,
         tiling.distColType, tiling.tileKey, tiling.segmentation, tiling.srid, tiling.groupCol, tiling.numTiles
    FROM pg_dist_spatiotemporal_tables
    WHERE tableName = table_name_out;

    IF table_id IS NULL THEN
        RAISE EXCEPTION 'create_spatiotemporal_distributed_table did not register % in pg_dist_spatiotemporal_tables', table_name_out;
    END IF;

    SELECT getDistributedColInternalType(table_name_in, tiling.distCol)
    INTO tiling.internalType;

    -- Same staged, hash-distributed + GIST-indexed copy the boundary-generation step above already built/reused -- calling this again is a cheap
    -- no-op lookup (stage_hash_distributed_source names the staging table deterministically and checks to_regclass first), not a second copy.
    SELECT r.staged_table, r.was_freshly_staged
    INTO staged_source, staged_fresh
    FROM dblink(format('dbname=%s user=%s port=%s', current_database(), current_user, current_setting('port')),
                format('SELECT * FROM stage_hash_distributed_source(%L, %L)', table_name_in, tiling.distCol))
         AS r(staged_table text, was_freshly_staged boolean);

    PERFORM parallel_spatiotemporal_data_allocation(staged_source, tiling, table_name_out, table_id, parallel_degree);

    RAISE INFO 'Parallel create_spatiotemporal_distributed_table_parallel total elapsed %', clock_timestamp() - start_time;
    RETURN true;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- parallel_spatiotemporal_data_allocation_direct: the other design discussed alongside the staged version above -- no per-tile UNLOGGED staging
-- table, no separate finalize pass. Each tile's matching rows are selected straight out of staged_source and written directly into table_name_out,
-- one INSERT per tile, fanned out over the same dblink connection pool. One write pass instead of two, at the cost of losing the staged version's
-- per-tile checkpoint: if one tile's connection dies mid-write, that tile is left partially populated in the real, live table_name_out with nothing
-- to detect or cleanly retry it by (the staged version isolates that risk in a disposable local table instead).
--
-- table_name_out's shell needs to be visible to every dblink connection in the pool below (unlike the staged version, which only needs it visible
-- in-process for phase 2), so it's created via its own committing dblink call here, not in-process -- same reasoning/pattern as
-- stage_hash_distributed_source (tiling.sql).
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION parallel_spatiotemporal_data_allocation_direct(
    table_name_in text,
    tiling tiling,
    table_name_out text,
    table_id integer,
    parallel_degree integer DEFAULT NULL
) RETURNS boolean AS $$
DECLARE
    conn_str text := format('dbname=%s user=%s port=%s', current_database(), current_user, current_setting('port'));
    degree integer;
    shell_ok boolean;
    org_table_columns text;
    group_by_clause text;
    n_tiles integer;
    tile_keys integer[];
    mobdb_bboxes text[];
    postgis_bboxes text[];
    exp_shapes bigint[];
    write_queries text[];
    bbox_expr text;
    col_expr text;
    where_clause text;
    group_clause text;
    temp_check text;
    j integer;
    tk integer;
    got_count bigint;
    start_time timestamp := clock_timestamp();
    tbl_id integer := table_id;
BEGIN
    IF tiling.method = 'crange' AND tiling.internaltype NOT IN ('point','polygon','instant') AND tiling.granularity = 'point-based' THEN
        RAISE EXCEPTION 'parallel_spatiotemporal_data_allocation_direct does not support crange point-based granularity on % columns (needs the exploded _temp table, already dropped earlier in the pipeline) -- use create_spatiotemporal_distributed_table for this combination instead.', tiling.internaltype;
    END IF;

    degree := coalesce(parallel_degree, (SELECT count(*) FROM pg_dist_node WHERE noderole = 'primary'));
    IF degree < 1 THEN
        degree := 1;
    END IF;

    SELECT r.ok INTO shell_ok
    FROM dblink(conn_str,
        format('SELECT create_tiled_output_table_shell(%L, %L, %L, %L, %L, %s, %L)',
               table_name_in, tiling.distCol, tiling.internaltype, tiling.isMobilityDB,
               tiling.tileKey, tiling.numTiles, table_name_out))
         AS r(ok boolean);
    IF NOT coalesce(shell_ok, false) THEN
        RAISE EXCEPTION 'Failed to create the shell/shards for %', table_name_out;
    END IF;

    IF tiling.isMobilityDB THEN
        SELECT array_to_string(array_agg(concat('t1.', a.attname) ORDER BY a.attnum), ',')
        INTO org_table_columns FROM pg_attribute a
        WHERE a.attrelid = table_name_in::regclass AND a.attnum > 0 AND NOT a.attisdropped;
    ELSE
        SELECT array_to_string(array_agg(concat('t1."', a.attname, '"') ORDER BY a.attnum), ',')
        INTO org_table_columns FROM pg_attribute a
        WHERE a.attrelid = table_name_in::regclass AND a.attnum > 0 AND NOT a.attisdropped;
    END IF;
    group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1.',tiling.distCol,')'), ''), ',,',','), '^,',''), ',$','');
    IF group_by_clause = org_table_columns THEN
        group_by_clause := regexp_replace(regexp_replace(regexp_replace(regexp_replace(org_table_columns,concat('(,t1."',tiling.distCol,'")'), ''), ',,',','), '^,',''), ',$','');
    END IF;

    SELECT array_agg(t.tile_key ORDER BY t.tile_key),
           array_agg(t.mobdb_bbox::text ORDER BY t.tile_key),
           array_agg(t.postgis_bbox::text ORDER BY t.tile_key),
           array_agg(t.num_shapes ORDER BY t.tile_key)
    INTO tile_keys, mobdb_bboxes, postgis_bboxes, exp_shapes
    FROM pg_dist_spatiotemporal_tiles t
    WHERE t.table_id = tbl_id;

    n_tiles := coalesce(array_length(tile_keys, 1), 0);
    IF n_tiles = 0 THEN
        RAISE EXCEPTION 'No tiles found in pg_dist_spatiotemporal_tiles for table_id %', table_id;
    END IF;
    RAISE INFO 'Parallel allocation (direct): % tiles, parallel_degree %', n_tiles, degree;

    write_queries := array_fill(NULL::text, ARRAY[n_tiles]);
    FOR j IN 1..n_tiles LOOP
        tk := tile_keys[j];

        IF tiling.isMobilityDB THEN
            bbox_expr := format('setsrid(%L::stbox, %s)', mobdb_bboxes[j], tiling.srid);
        ELSE
            bbox_expr := format('st_setsrid(%L::geometry, %s)', postgis_bboxes[j], tiling.srid);
        END IF;

        col_expr := org_table_columns;
        group_clause := NULL;

        IF tiling.internaltype = 'instant' THEN
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
            group_clause := group_by_clause;
        ELSIF tiling.internaltype = 'point' THEN
            where_clause := concat('(st_contains(', bbox_expr, ', t1.', tiling.distCol, ') or st_intersects(ST_Boundary(', bbox_expr, '), t1.', tiling.distCol, '))');
            group_clause := group_by_clause;
        ELSIF tiling.internaltype IN ('linestring','polygon') AND NOT tiling.segmentation THEN
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
        ELSIF tiling.internaltype IN ('linestring','multilinestring') AND tiling.segmentation THEN
            temp_check := col_expr;
            col_expr := regexp_replace(col_expr, concat(',t1.',tiling.distCol), concat(',ST_Intersection(t1.',tiling.distCol,', ',bbox_expr,') '));
            IF col_expr = temp_check THEN
                col_expr := regexp_replace(col_expr, concat(',t1."',tiling.distCol,'"'), concat(',ST_Intersection(t1.',tiling.distCol,', ',bbox_expr,') '));
            END IF;
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr, ' AND ST_Intersects(t1.', tiling.distCol, ', ', bbox_expr, ')');
            group_clause := concat(bbox_expr, ',', group_by_clause);
        ELSIF tiling.internaltype IN ('polygon','multipolygon') AND tiling.segmentation THEN
            temp_check := col_expr;
            col_expr := replace(col_expr, concat(',',tiling.distCol), concat(',ST_Intersection(',tiling.distCol,', ',bbox_expr,') '));
            IF col_expr = temp_check THEN
                col_expr := replace(col_expr, concat(',t1."',tiling.distCol,'"'), concat(',ST_Intersection(t1.',tiling.distCol,', ',bbox_expr,') '));
            END IF;
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr, ' AND ST_Intersects(t1.', tiling.distCol, ', ', bbox_expr, ')');
            group_clause := concat(bbox_expr, ',', group_by_clause);
        ELSIF tiling.internaltype IN ('sequence','sequenceset') AND NOT tiling.segmentation THEN
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
        ELSIF tiling.internaltype IN ('sequence','sequenceset') AND tiling.segmentation THEN
            col_expr := replace(col_expr, concat(',t1.',tiling.distCol), concat(',atStbox(t1.',tiling.distCol,', ',bbox_expr,') '));
            where_clause := concat('t1.', tiling.distCol, ' && ', bbox_expr);
        ELSE
            RAISE EXCEPTION 'Unsupported internaltype/segmentation combination for parallel allocation: % / segmentation=%', tiling.internaltype, tiling.segmentation;
        END IF;

        write_queries[j] := concat('INSERT INTO ', table_name_out, ' SELECT ', col_expr, ', ', tk, ' AS ', tiling.tileKey,
                                    ' FROM ', table_name_in, ' t1 WHERE ', where_clause,
                                    CASE WHEN group_clause IS NOT NULL THEN concat(' GROUP BY ', group_clause) ELSE '' END);
    END LOOP;

    PERFORM dblink_parallel_exec(degree, write_queries);
    RAISE INFO 'Parallel allocation (direct): writes done, elapsed %', clock_timestamp() - start_time;

    -- Validation checkpoint -- same idea as the staged version, just read back from table_name_out itself (no per-tile staging tables to check
    -- individually here) grouped by the tile key actually written.
    FOR j IN 1..n_tiles LOOP
        EXECUTE format('SELECT count(*) FROM %s WHERE %I = %s', table_name_out, tiling.tileKey, tile_keys[j]) INTO got_count;
        IF got_count IS DISTINCT FROM exp_shapes[j] THEN
            RAISE WARNING 'Tile % wrote % rows, catalog (num_shapes) expected % -- verify this run before trusting it', tile_keys[j], got_count, exp_shapes[j];
        END IF;
    END LOOP;

    PERFORM update_catalog_oid(table_name_out, table_id);
    RETURN true;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- create_spatiotemporal_distributed_table_parallel_direct: same idea as create_spatiotemporal_distributed_table_parallel above, wired to the direct
-- (no-staging) allocation function instead.
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_spatiotemporal_distributed_table_parallel_direct(
    table_name_in text,
    table_name_out text,
    num_tiles integer DEFAULT 1,
    tiling_method text DEFAULT 'crange',
    tiling_granularity text DEFAULT NULL,
    tiling_type text DEFAULT NULL,
    spatiotemporal_col_name varchar(50) DEFAULT NULL,
    shape_segmentation boolean DEFAULT TRUE,
    parallel_degree integer DEFAULT NULL
) RETURNS boolean AS $$
DECLARE
    tiling tiling;
    table_id integer;
    staged_source text;
    staged_fresh boolean;
    start_time timestamp := clock_timestamp();
BEGIN
    PERFORM create_spatiotemporal_distributed_table(
        table_name_in, table_name_out, num_tiles, tiling_method, tiling_granularity, tiling_type,
        NULL::text, NULL::text, spatiotemporal_col_name,
        physical_partitioning => false, shape_segmentation => shape_segmentation, is_reference_table => false
    );

    SELECT id, tilingMethod, granularity, disjoint, isMobilityDB, distcol, distcoltype, tilekey, shapeSegmented, srid, groupcol, numTiles
    INTO table_id, tiling.method, tiling.granularity, tiling.disjointTiles, tiling.isMobilityDB, tiling.distCol,
         tiling.distColType, tiling.tileKey, tiling.segmentation, tiling.srid, tiling.groupCol, tiling.numTiles
    FROM pg_dist_spatiotemporal_tables
    WHERE tableName = table_name_out;

    IF table_id IS NULL THEN
        RAISE EXCEPTION 'create_spatiotemporal_distributed_table did not register % in pg_dist_spatiotemporal_tables', table_name_out;
    END IF;

    SELECT getDistributedColInternalType(table_name_in, tiling.distCol)
    INTO tiling.internalType;

    SELECT r.staged_table, r.was_freshly_staged
    INTO staged_source, staged_fresh
    FROM dblink(format('dbname=%s user=%s port=%s', current_database(), current_user, current_setting('port')),
                format('SELECT * FROM stage_hash_distributed_source(%L, %L)', table_name_in, tiling.distCol))
         AS r(staged_table text, was_freshly_staged boolean);

    PERFORM parallel_spatiotemporal_data_allocation_direct(staged_source, tiling, table_name_out, table_id, parallel_degree);

    RAISE INFO 'Parallel create_spatiotemporal_distributed_table_parallel_direct total elapsed %', clock_timestamp() - start_time;
    RETURN true;
END;
$$ LANGUAGE 'plpgsql';
