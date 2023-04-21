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
