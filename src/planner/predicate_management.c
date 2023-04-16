#include "postgres.h"
#include "planner/predicate_management.h"
#include "optimizer/planner.h"
#include <utils/timestamp.h>


static float GetDistanceVal(Node *node);

/*
 * analyseDistancePredicate analyses the distance predicate.
 */
extern DistancePredicate *analyseDistancePredicate(Node *clause)
{
    DistancePredicate *distancePredicate = (DistancePredicate *)palloc0(sizeof(DistancePredicate));

    distancePredicate->distance = GetDistanceVal(clause);
    return distancePredicate;
}

/*
 * IsIntersectionOperation returns whether the given operation is an
 * intersection operation or not.
 *
 * It does so by searching pg_spatiotemporal_join_operations.
 */
bool
IsIntersectionOperation(Oid operationId)
{
    HeapTuple heapTuple = PgSpatiotemporalJoinOperationTupleViaCatalog(operationId, false);

    bool heapTupleIsValid = HeapTupleIsValid(heapTuple);

    if (heapTupleIsValid)
    {
        heap_freetuple(heapTuple);
    }
    return heapTupleIsValid;
}

static float
GetDistanceVal(Node *node)
{
    OpExpr *opExpr = (OpExpr *) node;
    ListCell *arg;
    foreach(arg, opExpr->args)
    {
        Node *dist_node = (Node *) lfirst(arg);
        if (IsA(dist_node, Const))
            return DatumGetFloat8(((Const *)dist_node)->constvalue);
    }
    return 0;
}

/*
 * IsDistanceOperation returns whether the given operation is a
 * distance operation or not.
 *
 * It does so by searching pg_spatiotemporal_join_operations.
 */
extern bool
IsDistanceOperation(Oid operationId)
{
    HeapTuple heapTuple = PgSpatiotemporalJoinOperationTupleViaCatalog(operationId, true);

    bool heapTupleIsValid = HeapTupleIsValid(heapTuple);

    if (heapTupleIsValid)
    {
        heap_freetuple(heapTuple);
    }
    return heapTupleIsValid;
}