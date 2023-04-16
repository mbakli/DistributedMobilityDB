#include "postgres.h"
#include <utils/snapmgr.h>
#include <utils/lsyscache.h>
#include <executor/spi.h>
#include "executor/tile_tasks.h"
#include "utils/helper_functions.h"

char *
taskQuery (List *tasks, ExecTaskType taskType)
{
    ListCell *cell = NULL;
    foreach(cell, tasks)
    {
        ExecutorTask *task = (ExecutorTask *) lfirst(cell);
        if (task->taskType == taskType)
            return task->taskQuery->data;
        else
            continue;
    }
}

/* Executor Job: Rearrange the generated tiles */
void
RearrangeTiles(Oid relid, int numTiles, char *reshuffledTable)
{
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    StringInfo reshuffledTablesQuery = makeStringInfo();
    appendStringInfo(reshuffledTablesQuery,
                     "SELECT create_reshuffled_multirelation(tablename=> '%s', "
                     "shards=>%d , reshuffled_table=> '%s.%s');",
                     get_rel_name(relid) ,
                     numTiles, Var_Schema, reshuffledTable);
    ExecuteQueryViaSPI(reshuffledTablesQuery->data, SPI_OK_SELECT);
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
}

Datum
AddTilingKey(SpatiotemporalTableCatalog tblCatalog, Alias *alias ,char * query_string)
{
    StringInfo tmp = makeStringInfo();
    StringInfo task_prep = makeStringInfo();
    appendStringInfo(tmp,"%s.%s", Var_Schema, tblCatalog.reshuffledTable);
    appendStringInfo(task_prep, "%s",replaceWord((char *)query_string,
                                                 get_rel_name(tblCatalog.table_oid), tmp->data));

    resetStringInfo(tmp);
    appendStringInfo(tmp, "WHERE %s.%s = %s.%s AND", alias->aliasname, tblCatalog.tileKey,
                     alias->aliasname, tblCatalog.tileKey);
    return CStringGetDatum(replaceWord( replaceWord(task_prep->data,"where", tmp->data), ";", " "));
}