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

#include "postgres.h"
#include "executor/executor_tasks.h"
#include "distributed_functions/distributed_function.h"
#include "utils/helper_functions.h"

/*
 * ProcessIntermediateTasks builds the worker-phase (INTERMEDIATEScan) task
 * query: a SELECT list applying each QOperation's worker function to its
 * argument column, run against the local per-tile result set.
 */
extern ExecutorTask *
ProcessIntermediateTasks(List *op)
{
    ExecutorTask *intermTask = (ExecutorTask *) palloc0(sizeof(ExecutorTask));
    intermTask->taskQuery = makeStringInfo();
    ListCell *cell = NULL;
    foreach(cell, op)
    {
        QOperation *qOp = (QOperation *) lfirst(cell);
        intermTask->taskType = INTERMEDIATEScan;
        if (intermTask->taskQuery->len == 0)
            appendStringInfo(intermTask->taskQuery, "%s", "SELECT ");
        else
            appendStringInfo(intermTask->taskQuery, "%s", ",");
        appendStringInfo(intermTask->taskQuery, "%s(%s) as %s ",
                         DatumToString(qOp->op, TEXTOID),
                         DatumToString(qOp->col, TEXTOID),
                         qOp->alias->aliasname);
    }
    if (list_length(op) > 0)
    {
        appendStringInfo(intermTask->taskQuery," FROM (local) as wQ");
    }
    return intermTask;
}

/*
 * ProcessFinalTasks builds the coordinator-phase (FINALScan) task query: a
 * SELECT list applying each QOperation's coordinator function over the
 * combined intermediate results from all workers.
 */
extern ExecutorTask *
ProcessFinalTasks(List *op)
{
    ExecutorTask *task = (ExecutorTask *) palloc0(sizeof(ExecutorTask));
    task->taskQuery = makeStringInfo();
    ListCell *cell = NULL;
    foreach(cell, op)
    {
        QOperation *qOp = (QOperation *) lfirst(cell);
        task->taskType = FINALScan;
        if (task->taskQuery->len == 0)
            appendStringInfo(task->taskQuery, "%s", "SELECT ");
        else
            appendStringInfo(task->taskQuery, "%s", ",");
        appendStringInfo(task->taskQuery, "%s(%s) as %s ",
                         DatumToString(qOp->op, TEXTOID),
                         DatumToString(qOp->col, TEXTOID),
                         qOp->alias->aliasname);
    }
    if (list_length(op) > 0)
    {
        appendStringInfo(task->taskQuery," FROM (intermediate) as fQ;");
    }
    return task;
}