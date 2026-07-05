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
 * Copyright (c) 2023-2024 Mohamed Bakli <mohamed_bakli@hotmail.com>
 *
 *****************************************************************************/

#include "postgres.h"
#include "planner/predicate_management.h"
#include "optimizer/planner.h"
#include "liblwgeom.h"
#include "general/spatiotemporal_processing.h"
#include <utils/timestamp.h>


static float GetDistanceVal(Node *node);

/*
 * GetPredicateOidAndArgs normalizes an OpExpr or FuncExpr WHERE-clause node
 * into a (callable oid, args) pair. MobilityDB/PostGIS predicates such as
 * eDwithin(...)/ST_Intersects(...) parse as FuncExpr, not OpExpr -- treating
 * every predicate as an OpExpr (as earlier revisions of this function did)
 * silently misidentified them (and risked undefined behavior reading
 * OpExpr-shaped fields out of a FuncExpr node).
 */
extern bool
GetPredicateOidAndArgs(Node *clause, Oid *oid, List **args)
{
    if (IsA(clause, OpExpr))
    {
        OpExpr *opExpr = (OpExpr *) clause;
        *oid = opExpr->opno;
        *args = opExpr->args;
        return true;
    }
    else if (IsA(clause, FuncExpr))
    {
        FuncExpr *funcExpr = (FuncExpr *) clause;
        *oid = funcExpr->funcid;
        *args = funcExpr->args;
        return true;
    }
    return false;
}

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
    Oid oid;
    List *args;
    ListCell *arg;

    if (!GetPredicateOidAndArgs(node, &oid, &args))
        return 0;
    foreach(arg, args)
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

/* get_query_range extracts the constant bounding-box argument from clause, if any, as its search range. */
extern Datum
get_query_range(STMultirelations *tbls, Node *clause)
{
    Oid oid;
    List *args;
    ListCell *arg;

    if (!GetPredicateOidAndArgs(clause, &oid, &args))
        return 0;
    foreach(arg, args)
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

/*
 * CheckTileRebalancerActivation would decide whether box spans enough of
 * tbls' tiles to be worth rebalancing before the scan.
 * Currently disabled (always returns false) to work around a Citus issue;
 * see TileScanRebalanceStrategyPlan() in planner_strategies.c.
 */
extern bool
CheckTileRebalancerActivation(STMultirelations *tbls, Node *clause, Datum box)
{
    /* Currently I removed the rebalacer to fix the citus issue */
    return false;
}