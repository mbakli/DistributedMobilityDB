#include "postgres.h"
#include "executor/executor_tasks.h"
#include "distributed_functions/distributed_function.h"

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