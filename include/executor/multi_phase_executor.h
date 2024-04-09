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


extern GeneralScan * QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain);
extern MultiPhaseExecutor * RunQueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain);
extern void DropReshuffledTableIfExists(char * reshuffled_table);
extern void CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table, bool tile_key);
extern Datum GetTaskType(ExecutorTask *task);
extern bool ColocateRte(STMultirelation *base, Rte *other);
#endif /* SPATIOTEMPORAL_EXECUTOR_H */
