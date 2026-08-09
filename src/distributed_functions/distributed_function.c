/*****************************************************************************
 *
 * DistributedMobilityDB - Large-Scale Spatial, Temporal, and Spatiotemporal Data Management Within PostgreSQL
 * https://github.com/mbakli/DistributedMobilityDB
 *
 * DistributedMobilityDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Copyright (c) 2020-2026 Mohamed Bakli <mohamed_bakli@hotmail.com>
 *
 *****************************************************************************/

#include "postgres.h"
#include "utils/planner_utils.h"
#include "utils/helper_functions.h"
#include <distributed/metadata_cache.h>
#include <nodes/nodeFuncs.h>
#include "distributed_functions/distributed_function.h"
#include "general/general_types.h"
#include "utils/builtins.h"


/* addDistributedFunction is a function used in the post processing phase, where each function is defined using
 * three operations: worker, intermediate, coordinator.
 * */
extern DistributedFunction *
addDistributedFunction(TargetEntry *targetEntry)
{
    DistributedFunction *dist_function = (DistributedFunction *)palloc0(sizeof(DistributedFunction));
    dist_function->coordinatorOp = (CoordinatorOperation *) palloc0(sizeof(CoordinatorOperation));
    dist_function->workerOp = (WorkerOperation *) palloc0(sizeof (WorkerOperation));
    dist_function->targetEntry = targetEntry;
    Datum datumArray[Natts_DistFun];
    bool isNullArray[Natts_DistFun];
    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation distFuns = table_open(DisFuncRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_DistFun_worker,
                BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(dist_function->targetEntry->resname));

    SysScanDesc scanDescriptor = systable_beginscan(distFuns,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    /* The tuple systable_getnext() returns is only valid while the scan is
     * open; heap_deform_tuple()-ing it directly leaves worker/final_op/
     * intermediate_op holding Datums (by-reference varlena pointers) into
     * that soon-to-be-released buffer. Copy it first so those Datums stay
     * valid for the rest of this DistributedFunction's lifetime. */
    heapTuple = heap_copytuple(heapTuple);
    heap_deform_tuple(heapTuple, RelationGetDescr(distFuns), datumArray,
                      isNullArray);

    dist_function->workerOp->op = datumArray[Anum_DistFun_worker - 1];
    dist_function->coordinatorOp->final_op = datumArray[Anum_DistFun_final - 1];
    dist_function->coordinatorOp->intermediate_op = datumArray[Anum_DistFun_combiner - 1];

    systable_endscan(scanDescriptor);
    table_close(distFuns, NoLock);

    return dist_function;
}

/* AddQOperation pairs a distributed operation (des) with its argument column (cur), aliased by cur's name. */
extern QOperation *
AddQOperation(Datum des, Datum cur)
{
    QOperation *qOp = (QOperation *) palloc0(sizeof(QOperation));
    qOp->op = des;
    qOp->alias = makeAlias(DatumToString(cur, TEXTOID), NIL);
    qOp->col = cur;
    return qOp;
}

/*
 * ContainsAggrefWalker reports (via *found) whether expr contains an
 * Aggref node anywhere in its tree, not just at the top level -- lets
 * IsDistFunc recognize a genuine aggregate call even when composed inside
 * another expression (e.g. `round(sum(length(Trip))::numeric, 2) AS
 * length`), rather than only the exact `sum(length(Trip)) AS length` shape.
 * The composing function(s) themselves need no special handling here: the
 * downstream rewrite (RewriterDistFuncs) works by substituting resname's
 * own text, not by re-deriving the expression's shape, so it's unaffected
 * either way by what (if anything) wraps the Aggref -- this walker only
 * decides whether a target entry is even worth handing to it.
 */
static bool
ContainsAggrefWalker(Node *node, bool *found)
{
    if (node == NULL)
        return false;
    if (IsA(node, Aggref))
    {
        *found = true;
        return true;
    }
    return expression_tree_walker(node, ContainsAggrefWalker, found);
}

/*
 * IsDistFunc reports whether targetEntry is a genuine aggregate call (e.g.
 * `sum(length(Trip)) AS length`, or that same call composed inside another
 * expression, e.g. `round(sum(length(Trip))::numeric, 2) AS length`) whose
 * result name matches a registered distributed function's worker name.
 *
 * The worker/combiner/final rewrite this feeds exists to recombine partial
 * per-tile results into one true total for a single (possibly
 * shape-segmented) trip -- that's only what an actual aggregate call is
 * asking for. A bare per-row call (e.g. `length(Trip)`, no aggregate
 * wrapper) wants one result per row instead. Since Postgres defaults an
 * unaliased function call's column name to the function's own name,
 * `length(Trip)` alone -- with no `sum()`/aggregate around it -- defaults
 * to the very same column name ("length") this match is keyed on; without
 * the Aggref check below, that ordinary per-row call collided with the
 * name and got wrongly collapsed into a single summed row, silently
 * discarding every row but one. This is not length-specific: it applies to
 * every function registered in pg_dist_spatiotemporal_dist_functions. Bare
 * calls over a shape-segmented table are handled separately (see
 * RewriteSegmentedDistFuncCalls) by rewriting them into exactly this kind
 * of explicit aggregate, grouped per trip, before this check ever runs.
 */
extern bool
IsDistFunc(TargetEntry *targetEntry)
{
    bool containsAggref = false;
    ContainsAggrefWalker((Node *) targetEntry->expr, &containsAggref);
    if (!containsAggref)
        return false;

    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation distFuns = table_open(DisFuncRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_DistFun_worker,
                BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(targetEntry->resname));

    SysScanDesc scanDescriptor = systable_beginscan(distFuns,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);

    HeapTuple heapTuple = systable_getnext(scanDescriptor);

    bool heapTupleIsValid = HeapTupleIsValid(heapTuple);
    systable_endscan(scanDescriptor);
    table_close(distFuns, NoLock);
    return heapTupleIsValid;
}

/*
 * LookupDistFuncFinalOp looks up workerFuncName (a bare function call's own
 * name, e.g. "length" -- not a column alias) in
 * pg_dist_spatiotemporal_dist_functions and returns its registered "final"
 * combining operation (e.g. "sum"), or NULL if workerFuncName isn't
 * registered. Used by RewriteSegmentedDistFuncCalls to find which aggregate
 * to wrap a bare call in before handing it to Citus' native distributed
 * GROUP BY/aggregate support, rather than matching (as IsDistFunc does) by
 * a target entry's possibly-coincidental result column name.
 */
extern char *
LookupDistFuncFinalOp(const char *workerFuncName)
{
    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation distFuns = table_open(DisFuncRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_DistFun_worker,
                BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(workerFuncName));

    SysScanDesc scanDescriptor = systable_beginscan(distFuns,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);

    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    char *finalOp = NULL;
    if (HeapTupleIsValid(heapTuple))
    {
        bool isNull;
        Datum finalDatum = heap_getattr(heapTuple, Anum_DistFun_final,
                                        RelationGetDescr(distFuns), &isNull);
        if (!isNull)
            finalOp = TextDatumGetCString(finalDatum);
    }
    systable_endscan(scanDescriptor);
    table_close(distFuns, NoLock);
    return finalOp;
}

/*
 * LookupDistFuncCombinerOp mirrors LookupDistFuncFinalOp but returns the
 * registered "combiner" column instead. Two-phase functions (worker/final
 * only, e.g. length/sum, speed/merge) leave this NULL -- for those, EXPLAIN
 * display is this value's only use.
 *
 * A non-NULL combiner instead names a real Postgres aggregate (registered
 * via CREATE AGGREGATE, with its own SFUNC/COMBINEFUNC/FINALFUNC) that
 * fully implements the function's own worker/combine/final recombination
 * internally -- for these, this rewrite composes a call to the aggregate
 * itself directly (BuildRecombinedFuncCall), rather than wrapping a bare
 * per-row function call in a separate "final" op the way the two-phase
 * case does; the "final" op has no meaning for a row like this and is left
 * NULL. Needed for a function whose correct recombination isn't a single
 * associative/commutative op applied to independently-meaningful
 * per-fragment results (unlike sum/min/max/bool_or) -- e.g. a cumulative
 * distance-over-time profile, where each subsequent fragment's own partial
 * result has to be time-ordered and offset by every earlier fragment's
 * running total before the pieces can be merged into one true value; see
 * sql/distributed_functions/cumulative_length.sql for a full worked
 * example of registering one.
 */
extern char *
LookupDistFuncCombinerOp(const char *workerFuncName)
{
    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation distFuns = table_open(DisFuncRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_DistFun_worker,
                BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(workerFuncName));

    SysScanDesc scanDescriptor = systable_beginscan(distFuns,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);

    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    char *combinerOp = NULL;
    if (HeapTupleIsValid(heapTuple))
    {
        bool isNull;
        Datum combinerDatum = heap_getattr(heapTuple, Anum_DistFun_combiner,
                                           RelationGetDescr(distFuns), &isNull);
        if (!isNull)
            combinerOp = TextDatumGetCString(combinerDatum);
    }
    systable_endscan(scanDescriptor);
    table_close(distFuns, NoLock);
    return combinerOp;
}

