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

#ifndef SPATIOTEMPORAL_EXECUTOR_H
#define SPATIOTEMPORAL_EXECUTOR_H

#include "planner/distributed_mobilitydb_planner.h"
#include "post_processing/post_processing.h"
#include "tile_tasks.h"
#include <utils/snapmgr.h>
#include <executor/spi.h>
#include <utils/lsyscache.h>

/*
 * MultiPhaseExecutor
 *
 * Tracks the state of a distributed query as it runs through its
 * execution phases: `tasks` are the worker-phase (intermediate) tasks and
 * `coordTasks` the coordinator-phase (final) tasks derived from distPlan.
 * The boolean flags record one-time side effects performed along the way
 * (whether tile data had to be reshuffled, or a reshuffled table/index had
 * to be created) so later phases and cleanup don't repeat them.
 */
typedef struct MultiPhaseExecutor
{
    DistributedSpatiotemporalQueryPlan distPlan;
    List *tasks;
    bool dataReshuffled;
    bool tableCreated;
    bool indexCreated;
    List *coordTasks;
} MultiPhaseExecutor;

/* Executes distPlan end-to-end and returns the resulting scan/plan node. */
extern GeneralScan * QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain);

/* Drives all phases (intermediate + final) of distPlan, returning the executor state. */
extern MultiPhaseExecutor * RunQueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain);

/* Drops the temporary reshuffled_table, if a previous run created one. */
extern void DropReshuffledTableIfExists(char * reshuffled_table);

/* Creates reshuffled_table from org_table (optionally keyed by tile) if it doesn't already exist. */
extern void CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table, bool tile_key);

/* Task type of an ExecutorTask, exposed for EXPLAIN output. */
extern Datum GetTaskType(ExecutorTask *task);

/* True if `other` shares the same tiling/colocation as the base relation. */
extern bool ColocateRte(STMultirelation *base, Rte *other, bool explain);
#endif /* SPATIOTEMPORAL_EXECUTOR_H */
