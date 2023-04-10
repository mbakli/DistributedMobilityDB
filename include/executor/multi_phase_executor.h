#ifndef SPATIOTEMPORAL_EXECUTOR_H
#define SPATIOTEMPORAL_EXECUTOR_H

#include "planner/spatiotemporal_planner.h"
#include <utils/snapmgr.h>
#include <executor/spi.h>
#include <utils/lsyscache.h>


/* Planner strategies */
typedef enum ExecTaskType
{
    NeighborTilingScan,
    SelfTilingScan
} ExecTaskType;

typedef struct ExecutorTask
{
    StringInfo taskQuery;
    ExecTaskType taskType;
} ExecutorTask;

/*
 * MultiPhaseExecutor
 */
typedef struct MultiPhaseExecutor
{
    DistributedSpatiotemporalQueryPlan distPlan;
    List *tasks;
    int numCores;
    bool dataReshuffled;
    bool table_created;
    PostProcessing postProcessing;
} MultiPhaseExecutor;

extern GeneralScan * QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain);
extern void DropReshuffledTableIfExists(char * reshuffled_table);
extern void CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table);
#endif /* SPATIOTEMPORAL_EXECUTOR_H */
