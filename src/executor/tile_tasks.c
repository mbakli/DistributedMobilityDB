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
#include <utils/snapmgr.h>
#include <utils/lsyscache.h>
#include <executor/spi.h>
#include <access/xact.h>
#include "executor/tile_tasks.h"
#include "utils/helper_functions.h"
#include "general/rte.h"
#include "utils/planner_utils.h"

/* taskQuery returns the SQL text of the first task in `tasks` matching taskType. */
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

/*
 * RearrangeTiles calls the create_reshuffled_multirelation() SQL helper to
 * redistribute relid's rows into numTiles tiles inside reshuffledTable, so
 * two previously non-colocated tables end up sharing the same tiling
 * scheme before being joined.
 */
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

/*
 * AddTilingKey rewrites query_string so it targets the reshuffled table
 * (dist_mobilitydb.<reshuffledTable> instead of the original table name)
 * and replaces its `where` clause with a tile-key equality predicate
 * between alias (tblCatalog's own alias) and otherAlias (the base table's
 * alias), ensuring the neighbor scan only compares rows sharing the same
 * tile. A prior revision used `alias` on both sides of the equality (always
 * comparing a table's tile_key to itself), which built a nonsensical
 * self-referential predicate and produced a malformed query once folded
 * into the caller's WHERE clause.
 */
Datum
AddTilingKey(STMultirelationCatalog tblCatalog, Alias *alias, Alias *otherAlias, char * query_string)
{
    StringInfo tmp = makeStringInfo();
    StringInfo task_prep = makeStringInfo();
    appendStringInfo(tmp,"%s.%s", Var_Schema, tblCatalog.reshuffledTable);
    appendStringInfo(task_prep, "%s",replaceWord((char *)query_string,
                                                 get_rel_name(tblCatalog.table_oid), tmp->data));

    resetStringInfo(tmp);
    appendStringInfo(tmp, "WHERE %s.%s = %s.%s AND", alias->aliasname, tblCatalog.tileKey,
                     otherAlias->aliasname, tblCatalog.tileKey);
    return CStringGetDatum(replaceWord( replaceWord(task_prep->data,"where", tmp->data), ";", " "));
}

/* AddNonStRteTilingKey is AddTilingKey()'s counterpart for a plain Citus-distributed (non-spatiotemporal) relation. */
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

/* GetRandTileNum returns a randomly-chosen shard's shardminvalue for rte, used to sample a single tile. */
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
    /* A table with no shards registered in pg_dist_shard yet legitimately returns zero rows here
     * -- same SPI_tuptable->vals[0] bug fixed in table_ops.c (DistributedColumnType/
     * GetSpatiotemporalCol) and nodes.c (GetNodeInfo/GetDBName): SPI_OK_SELECT alone doesn't
     * guarantee any rows, and reading vals[0] without checking SPI_processed first reads past an
     * empty result. */
    if (spi_result == SPI_OK_SELECT && SPI_processed > 0)
    {
        TupleDesc rowDescriptor = SPI_tuptable->tupdesc;
        HeapTuple row = SPI_copytuple(SPI_tuptable->vals[0]);
        heap_deform_tuple(row, rowDescriptor, &datum,
                          &isNull);
        res = DatumGetInt32(datum);
    }
    /* Always paired with SPI_connect() above -- the previous version only called this inside the
     * SPI_OK_SELECT branch, leaking the SPI connection on any other result status. */
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
    return res;
}