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

/* Planner strategies */
typedef enum PlannerStrategy
{
    Colocation,
    NonColocation,
    TileScanRebalancer,
    PredicatePushDown,
    KNN
} PlannerStrategy;

extern void ColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);
extern void NonColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);
extern void TileScanRebalanceStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);

#endif /* PLANNER_STRATEGIES_H */
