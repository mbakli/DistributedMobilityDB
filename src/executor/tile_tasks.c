#include "postgres.h"
#include <utils/snapmgr.h>
#include <utils/lsyscache.h>
#include <executor/spi.h>
#include "executor/tile_tasks.h"
#include "utils/helper_functions.h"
#include "general/rte.h"
#include "utils/planner_utils.h"

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
AddTilingKey(STMultirelationCatalog tblCatalog, Alias *alias ,char * query_string)
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

Datum
AddNonStRteTilingKey(Rte *tbl, Alias *alias ,char * query_string)
{
    if(tbl->RteType == CitusRte)
    {
        CitusRteNode *citusRteNode = (CitusRteNode *)tbl->rte;
        RangeTblEntry * cell = (RangeTblEntry *) lfirst(citusRteNode->rangeTableCell);
        StringInfo tmp = makeStringInfo();
        StringInfo task_prep = makeStringInfo();
        appendStringInfo(tmp,"%s.%s", Var_Schema, citusRteNode->reshuffledTable);
        appendStringInfo(task_prep, "%s",replaceWord((char *)query_string,
                                                     get_rel_name(cell->relid), tmp->data));

        resetStringInfo(tmp);
        appendStringInfo(tmp, "WHERE %s.%s = %s.%s AND", cell->alias->aliasname, Var_Catalog_Tile_Key,
                         alias->aliasname, Var_Catalog_Tile_Key);
        return CStringGetDatum(replaceWord( replaceWord(task_prep->data,"where", tmp->data), ";", " "));
    }
    return 0;
}

extern int
GetRandTileNum(STMultirelation *rte)
{
    int spi_result;
    int res = 0;
    Datum datum;
    bool isNull;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    /* Execute the query, noting the readonly status of this SQL */
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "SELECT shardminvalue::int FROM pg_dist_shard "
                                   "WHERE logicalrelid = '%s'::regclass "
                                   "ORDER BY random() limit 1;",
                     get_rel_name(rte->catalogTableInfo.table_oid));
    spi_result = SPI_execute(catalogQuery->data, true, 1);
    /* Read back the PROJ text */
    if (spi_result == SPI_OK_SELECT)
    {
        TupleDesc rowDescriptor = SPI_tuptable->tupdesc;
        HeapTuple row = SPI_copytuple(SPI_tuptable->vals[0]);
        heap_deform_tuple(row, rowDescriptor, &datum,
                          &isNull);
        res = DatumGetInt32(datum);

        spi_result = SPI_finish();

        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
    }
    return res;
}