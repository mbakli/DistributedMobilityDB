#ifndef PLANNER_STRATEGIES_H
#define PLANNER_STRATEGIES_H
#include "planner/predicate_management.h"
#include "utils/planner_utils.h"
#include "planner/distributed_mobilitydb_planner.h"
#define Var_Temp_Reshuffled "_reshuffled"

/*
 * NeighborScan
 */
typedef struct NeighborScan
{
    PredicateType predicateType;
    Node *predicateInfo;
} NeighborScan;

/* Planner strategies */
typedef enum StrategyType
{
    Colocation,
    NonColocation,
    TileScanRebalancer,
    PredicatePushDown,
    KNN
} StrategyType;

typedef struct PlanTask
{
    StrategyType type;
    SpatiotemporalTable *tbl1;
    SpatiotemporalTable *tbl2;
    Datum tileKey;
} PlanTask;

extern void ColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);
extern void NonColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);
extern void TileScanRebalanceStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);
extern void PredicatePushDownStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);
extern void AddStrategy(DistributedSpatiotemporalQueryPlan *distPlan, StrategyType type);
#endif /* PLANNER_STRATEGIES_H */
