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
 * Copyright (c) 2020-2026 Mohamed Bakli <mohamed_bakli@hotmail.com>
 *
 *****************************************************************************/

#ifndef EXECUTOR_TASKS_H
#define EXECUTOR_TASKS_H
#include "postgres.h"
#include "multirelation/multirelation_utils.h"

/*
 * ExecTaskType
 *
 * The kind of scan a task performs against a tile:
 *   NeighborTilingScan - scans a tile against a neighboring tile (join across tile boundaries)
 *   SelfTilingScan     - scans a tile against itself (self-join / single-tile predicate)
 *   PushDownScan        - predicate is pushed down and run entirely on the worker
 *   INTERMEDIATEScan    - a worker-phase task whose result feeds the combiner
 *   FINALScan           - the coordinator-phase task producing the final result
 */
typedef enum ExecTaskType
{
    NeighborTilingScan,
    SelfTilingScan,
    PushDownScan,
    INTERMEDIATEScan,
    FINALScan
} ExecTaskType;

/*
 * TaskNode
 *
 * Connection info (hostname/db) for the node a task must be dispatched to.
 */
typedef struct TaskNode
{
    Datum node;
    int port;
    Datum db;
} TaskNode;

/*
 * ExecutorTask
 *
 * A single unit of distributed work: the SQL to run (taskQuery), its
 * ExecTaskType, resource hints (numCores, candidates), and the catalog
 * filter identifying which tiles it applies to.
 */
typedef struct ExecutorTask
{
    StringInfo taskQuery;
    ExecTaskType taskType;
    int numCores;
    int candidates;
    CatalogFilter *catalog_filtered;
} ExecutorTask;

/* Builds the worker-phase tasks (INTERMEDIATEScan) for the given operations. */
extern ExecutorTask *ProcessIntermediateTasks(List *op);

/* Builds the coordinator-phase task (FINALScan) that combines worker results. */
extern ExecutorTask *ProcessFinalTasks(List *op);

#endif /* EXECUTOR_TASKS_H */
