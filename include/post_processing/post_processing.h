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

#ifndef POST_PROCESSING_H
#define POST_PROCESSING_H
#include "postgres.h"
#include "distributed/listutils.h"
#include "distributed_functions/distributed_function.h"

/* Whether per-tile worker results still need to be collected at the coordinator. */
typedef struct CollectOperator
{
    bool active;

} CollectOperator;

/* Whether collected per-tile results need to be merged into a single result set. */
typedef struct MergeOperator
{
    bool active;

} MergeOperator;

/* Post-processing steps that run on each worker before results are shipped back. */
typedef struct WorkerLevelOperator
{
    CollectOperator *collectOperator;
} WorkerLevelOperator;

/* Whether duplicate rows (e.g. from neighboring-tile joins) must be removed. */
typedef struct RemDupOperator
{
    bool active;
} RemDupOperator;

/* Post-processing steps that run at the coordinator once worker results arrive. */
typedef struct CoordinatorLevelOperator
{
    MergeOperator *mergeOperator;
    RemDupOperator * dupRemOperator;
    List *intermediateOp;
    List *finalOp;
} CoordinatorLevelOperator;

/*
 * PostProcessing
 *
 * Aggregates every post-execution step a distributed query still needs
 * after its per-tile tasks complete: collecting/merging/deduplicating
 * results (workerLevelOperator/coordinatorLevelOperator), any registered
 * distributed aggregate functions (distfuns), and whether a synthetic
 * GROUP BY / primary key had to be introduced by the rewrite.
 */
typedef struct PostProcessing
{
    CoordinatorLevelOperator *coordinatorLevelOperator;
    WorkerLevelOperator  *workerLevelOperator;
    List *distfuns; // DistributedFunction;
    bool group_by_required;
    Datum *genPrimKey;
    char * worker;
    Datum intermediate;
    Datum final;
} PostProcessing;

/* Runs the post-processing steps appropriate for the chosen strategies. */
extern void PostProcessingQuery(PostProcessing *postProcessing,List *strategies);

/* Allocates and zero-initializes a PostProcessing struct. */
extern PostProcessing *InitializePostProcessing();

/* Rewrites parse's targetlist to call the distributed worker function, recording the coordinator half in postProcessing. */
extern void RewriterDistFuncs(Query *parse, PostProcessing *postProcessing, const char *query_string);
#endif /* POST_PROCESSING_H */
