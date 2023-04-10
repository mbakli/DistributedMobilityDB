#ifndef SPATIOTEMPORAL_EXECUTOR_H
#define SPATIOTEMPORAL_EXECUTOR_H

#include "planner/spatiotemporal_planner.h"
#include <utils/snapmgr.h>
#include <executor/spi.h>
#include <utils/lsyscache.h>

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
    bool active;
} WorkerLevelOperator;

typedef struct RemDupOperator
{
    bool active;
} RemDupOperator;

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

typedef struct CoordinatorLevelOperator
{
    MergeOperator *mergeOperator;
    RemDupOperator * dupRemOperator;
} CoordinatorLevelOperator;


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
    CoordinatorLevelOperator *coordinatorLevelOperator;
    WorkerLevelOperator  *workerLevelOperator;
} MultiPhaseExecutor;

extern GeneralScan * QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain);
extern void DropReshuffledTableIfExists(char * reshuffled_table);
extern void CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table);
#endif /* SPATIOTEMPORAL_EXECUTOR_H */
