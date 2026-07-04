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

#ifndef PLANNER_STRATEGIES_H
#define PLANNER_STRATEGIES_H
#include "planner/predicate_management.h"
#include "utils/planner_utils.h"
#include "planner/distributed_mobilitydb_planner.h"
#define Var_Temp_Reshuffled "_reshuffled"

/*
 * NeighborScan
 *
 * A join predicate that must be evaluated against neighboring tiles (not
 * just within the same tile), e.g. a distance/kNN predicate near a tile
 * boundary.
 */
typedef struct NeighborScan
{
    PredicateType predicateType;
    Node *predicateInfo;
} NeighborScan;

/*
 * StrategyType
 *
 *   Colocation         - joined tables share the same tiling; join tile-by-tile with no data movement
 *   NonColocation      - tables aren't colocated; one side may need broadcasting/reshuffling
 *   TileScanRebalancer - tiles must be rebalanced/reshuffled before the query can run
 *   PredicatePushDown  - the predicate can be evaluated entirely on the worker, no coordinator merge needed
 *   KNN                - nearest-neighbor query, requiring iterative tile-radius expansion
 */
typedef enum StrategyType
{
    Colocation,
    NonColocation,
    TileScanRebalancer,
    PredicatePushDown,
    KNN
} StrategyType;

/* One planned unit of work: apply `type`'s strategy to tbl1 (and tbl2, for joins) within tileKey. */
typedef struct PlanTask
{
    StrategyType type;
    STMultirelation *tbl1;
    STMultirelation *tbl2;
    Datum tileKey;
} PlanTask;

/* Plans a join between tables that are colocated (same tiling scheme). */
extern void ColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);

/* Plans a join between tables that are not colocated (requires broadcast/reshuffle). */
extern void NonColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);

/* Plans a scan that first rebalances tiles via RearrangeTiles() before executing. */
extern void TileScanRebalanceStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);

/* Plans a scan where the predicate is pushed down and fully evaluated on the worker. */
extern void PredicatePushDownStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan);

/* Appends `type` to distPlan's list of chosen strategies. */
extern void AddStrategy(DistributedSpatiotemporalQueryPlan *distPlan, StrategyType type);

/* Determines which of rtes' tables (if any) needs its tiles reshuffled before the query runs. */
extern ReshufflingRte *GetReshufflingRte(STMultirelations *rtes);

/* True if localNode is small/static enough to be broadcast to all workers instead of distributed. */
extern bool CheckBroadcastingPossibility(LocalRteNode *localNode);
#endif /* PLANNER_STRATEGIES_H */
