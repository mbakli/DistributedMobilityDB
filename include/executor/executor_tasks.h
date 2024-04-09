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

#ifndef EXECUTOR_TASKS_H
#define EXECUTOR_TASKS_H
#include "postgres.h"
#include "multirelation/multirelation_utils.h"

/* Executor Task Type */
typedef enum ExecTaskType
{
    NeighborTilingScan,
    SelfTilingScan,
    PushDownScan,
    INTERMEDIATEScan,
    FINALScan
} ExecTaskType;

/*
 * MultiPhaseExecutor
 */
typedef struct TaskNode
{
    Datum node;
    int port;
    Datum db;
} TaskNode;

typedef struct ExecutorTask
{
    StringInfo taskQuery;
    ExecTaskType taskType;
    int numCores;
    int candidates;
    CatalogFilter *catalog_filtered;
} ExecutorTask;

extern ExecutorTask *ProcessIntermediateTasks(List *op);
extern ExecutorTask *ProcessFinalTasks(List *op);

#endif /* EXECUTOR_TASKS_H */
