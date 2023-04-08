#ifndef PLANNER_STRATEGIES_H
#define PLANNER_STRATEGIES_H
#include "planner/predicate_management.h"
#include "planner/planner_utils.h"
#include "planner/spatiotemporal_planner.h"


/*
 * NeighborScan
 */
typedef struct NeighborScan
{
    PredicateType predicateType;
    Node *predicateInfo;
} NeighborScan;

#endif /* PLANNER_STRATEGIES_H */
