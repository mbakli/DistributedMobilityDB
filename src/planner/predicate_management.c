#include "postgres.h"
#include "planner/predicate_management.h"
#include "optimizer/planner.h"


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