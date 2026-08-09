/*
 * User-defined distributed functions, registered via create_distfunction
 * (see that file's own comment for the general mechanism). Each one below
 * is a full worked example: its own worker/combiner/final functions plus
 * the create_distfunction call that wires them into the planner.
 */

/*
 * cumulativeLength(tgeompoint) returns a tfloat -- the trip's distance
 * traveled so far, at each instant. worker collects each fragment's own
 * raw trajectory unchanged, final merges those fragments into the row's
 * complete trajectory *first* and only then runs cumulativeLength() once,
 * rather than computing cumulativeLength per fragment and combining the
 * results afterward.
 *
 * That ordering matters, and isn't just a style choice: a fragment's own
 * cumulativeLength restarts at 0 from that fragment's own first instant,
 * not the trip's true start, so combining already-computed per-fragment
 * results needs every subsequent fragment's curve re-offset by the
 * running total before merging -- and fragments can genuinely overlap in
 * time (a trip briefly re-entering a tile it had already left leaves that
 * tile's own fragment, and whichever other fragment covers the same
 * window, both claiming the same stretch), at different point sampling
 * densities between the two. cumulativeLength computed over the sparser
 * fragment alone for that overlapping stretch systematically undercounts
 * the true path length there (fewer, more widely spaced points
 * approximate a curved path more crudely than densely spaced ones do) --
 * reproduced directly on a real trip in this dataset: combining
 * per-fragment results this way gave a total of 43369, against a true
 * total of 62885 confirmed against the non-distributed source table.
 * Merging the raw trajectory fragments first uses every fragment's own
 * full point density for whatever stretch it covers, and Postgres/Citus's
 * own MVCC-safe row semantics already guarantee two fragments agree at
 * any timestamp they both happen to cover (both are clips of the exact
 * same underlying trip), so there's nothing left to reconcile once
 * they're merged -- cumulativeLength then only ever runs once, over the
 * complete, correctly-ordered result.
 */
CREATE FUNCTION cumulativelength_worker(state tgeompoint[], traj tgeompoint)
RETURNS tgeompoint[] AS $$
  SELECT array_append(state, traj);
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION cumulativelength_combiner(state1 tgeompoint[], state2 tgeompoint[])
RETURNS tgeompoint[] AS $$
  SELECT state1 || state2;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION cumulativelength_final(state tgeompoint[])
RETURNS tfloat AS $$
  SELECT cumulativeLength(merge(state));
$$ LANGUAGE SQL IMMUTABLE;

SELECT create_distfunction('cumulativelength', 'tgeompoint', 'tgeompoint[]',
    'cumulativelength_worker', 'cumulativelength_combiner', 'cumulativelength_final');
