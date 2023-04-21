#include "postgres.h"
#include "planner/predicate_management.h"
#include "optimizer/planner.h"
#include "postgresql/liblwgeom.h"
#include "general/spatiotemporal_processing.h"
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

extern Datum
get_query_range(SpatiotemporalTables *tbls, OpExpr *opExpr)
{
    ListCell *arg;
    foreach(arg, opExpr->args)
    {
        Node *dist_node = (Node *) lfirst(arg);
        if (IsA(dist_node, Const))
        {
            /* TODO: replace the GBOX with STBOX when I fix the conflicts caused by meos.h */
            GBOX * gbox = S_BOX_PTR(((Const *)dist_node)->constvalue);
            return BOX_GET_DATUM(gbox);
        }
    }
    return 0;
}

extern bool
CheckTileRebalancerActivation(SpatiotemporalTables *tbls, OpExpr *opExpr, Datum box)
{
    /* Currently I removed the rebalacer to fix the citus issue */
    return false;
}