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

typedef struct CollectOperator
{
    bool active;

} CollectOperator;

typedef struct MergeOperator
{
    bool active;

} MergeOperator;

typedef struct WorkerLevelOperator
{
    CollectOperator *collectOperator;
} WorkerLevelOperator;

typedef struct RemDupOperator
{
    bool active;
} RemDupOperator;


typedef struct CoordinatorLevelOperator
{
    MergeOperator *mergeOperator;
    RemDupOperator * dupRemOperator;
    List *intermediateOp;
    List *finalOp;
} CoordinatorLevelOperator;


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

extern void PostProcessingQuery(PostProcessing *postProcessing,List *strategies);
extern PostProcessing *InitializePostProcessing();
extern void RewriterDistFuncs(Query *parse, PostProcessing *postProcessing, const char *query_string);
#endif /* POST_PROCESSING_H */
