/*
 * create_distfunction registers a new distributed function for the
 * planner to recognize (RewriteSegmentedDistFuncCalls/RewriteWhereClause
 * DistFuncCalls, src/planner/query_semantics.c): once registered, a bare
 * call to func_name over a shape-segmented table's own distributed column,
 * anywhere in the SELECT list or WHERE clause, is automatically rewritten
 * to recombine its per-fragment results into the row's true, complete
 * value, instead of silently running per-fragment the way an unregistered
 * function would.
 *
 * Wraps Postgres' own CREATE AGGREGATE (worker_func/combiner_func/
 * final_func become that aggregate's SFUNC/COMBINEFUNC/FINALFUNC) so the
 * usual Postgres/Citus aggregate machinery does the actual cross-fragment,
 * cross-shard combination -- this function only creates the aggregate and
 * records its name in pg_dist_spatiotemporal_dist_functions so the planner
 * can find it. Usage, mirroring the three-function shape a distributed
 * function is designed around:
 *
 *   CREATE FUNCTION my_func_worker(state my_state_type, traj tgeompoint)
 *     RETURNS my_state_type AS $$ ... $$ LANGUAGE ...;
 *   CREATE FUNCTION my_func_combiner(state1 my_state_type, state2 my_state_type)
 *     RETURNS my_state_type AS $$ ... $$ LANGUAGE ...;
 *   CREATE FUNCTION my_func_final(state my_state_type)
 *     RETURNS my_result_type AS $$ ... $$ LANGUAGE ...;
 *   SELECT create_distfunction('my_func', 'tgeompoint', 'my_state_type',
 *     'my_func_worker', 'my_func_combiner', 'my_func_final');
 *
 * worker_func's own state-transition semantics decide what "combining"
 * actually means for this function -- there's no single right shape.
 * cumulativeLength (cumulative_length.sql, alongside this file) is a
 * worked example: its worker collects each fragment's own raw trajectory
 * unchanged (not yet the distributed function's own result), and its
 * final merges those fragments into the row's complete trajectory before
 * running cumulativeLength() once, rather than computing cumulativeLength
 * per-fragment and trying to combine those results afterward -- the two
 * are *not* interchangeable for a function like this one, where a
 * fragment's own per-fragment result isn't independently meaningful (see
 * cumulative_length.sql's own comment for the concrete failure this
 * causes if gotten the other way around).
 *
 * A function whose per-fragment result *is* independently meaningful and
 * just needs a plain reduction (sum, min, max, bool_or) -- or one that
 * simply needs its source column reconstructed before running at all, no
 * custom combination logic beyond that -- doesn't need this: registering
 * it with a single word in the "final" column, e.g. `SELECT 'length',
 * NULL, 'sum', 2` or `SELECT 'speed', NULL, 'merge', 2` (see category1.sql),
 * already covers those directly, no aggregate of its own required.
 */
CREATE OR REPLACE FUNCTION create_distfunction(
    func_name text,
    arg_type regtype,
    state_type regtype,
    worker_func text,
    combiner_func text,
    final_func text,
    init_cond text DEFAULT '{}'
) RETURNS text AS $$
DECLARE
    agg_name text := lower(func_name) || '_agg';
BEGIN
    EXECUTE format(
        'CREATE AGGREGATE %I(%s) (SFUNC = %I, STYPE = %s, COMBINEFUNC = %I, FINALFUNC = %I, INITCOND = %L)',
        agg_name, arg_type, worker_func, state_type, combiner_func, final_func, init_cond
    );
    INSERT INTO pg_dist_spatiotemporal_dist_functions (worker, combiner, final, sExec_id)
    VALUES (lower(func_name), agg_name, NULL, 2);
    RETURN agg_name;
END;
$$ LANGUAGE plpgsql;
