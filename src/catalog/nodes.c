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

#include "postgres.h"
#include "general/general_types.h"
#include <executor/spi.h>
#include "catalog/nodes.h"
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include "executor/executor_tasks.h"
#include "catalog/table_ops.h"


extern Datum GetDBName();

/* Nodes */
#define Natts_DistNodes 2
#define Anum_DistNodes_nodename 0
#define Anum_DistNodes_nodeport 1

/*
 * GetNodeInfo picks a random worker node from Citus' pg_dist_node catalog
 * and returns its name/port together with the current database name, for
 * use as the target of a dispatched task.
 */
extern TaskNode *
GetNodeInfo()
{
    TaskNode *taskNode = (TaskNode *) palloc0(sizeof(TaskNode));
    taskNode->db = GetDBName();
    int spi_result;
    Datum datumArray[Natts_DistNodes];
    bool isNullArray[Natts_DistNodes];
    bool indexOK = false;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }
    spi_result = SPI_execute("SELECT nodename, nodeport FROM pg_dist_node ORDER BY random() limit 1",
                             true, 1);
    /* Read back the PROJ text */
    if (spi_result == SPI_OK_SELECT)
    {
        TupleDesc rowDescriptor = SPI_tuptable->tupdesc;
        HeapTuple row = SPI_copytuple(SPI_tuptable->vals[0]);
        heap_deform_tuple(row, rowDescriptor, datumArray,
                          isNullArray);
        taskNode->node = PointerGetDatum(datumArray[Anum_DistNodes_nodename]);
        taskNode->port = DatumGetInt32(datumArray[Anum_DistNodes_nodeport]);
        spi_result = SPI_finish();
        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
    }

    return taskNode;
}

/*
 * GetRandomTileId looks up the Citus shard id whose shardminvalue matches
 * rand_tile for relationId (accounting for the dist_mobilitydb schema when
 * relationId has already been reshuffled) and returns it as a
 * "<table>_<shardid> " task identifier string.
 */
extern
char * GetRandomTileId(Oid relationId, ExecTaskType taskType, int rand_tile)
{
    int spi_result;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    /* Execute the query, noting the readonly status of this SQL */
    StringInfo logicalrel = makeStringInfo();
    if (IsReshuffledTable(relationId))
        appendStringInfo(logicalrel, "%s.%s", Var_Schema, get_rel_name(relationId));
    else
        appendStringInfo(logicalrel, "%s", get_rel_name(relationId));
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "SELECT concat('%s_',shard.shardid,' ')\n"
                                   "FROM pg_dist_shard As shard\n"
                                   "WHERE shard.logicalrelid = '%s'::regclass\n"
                                   "    AND shard.shardminvalue = %d::text",
                     logicalrel->data, logicalrel->data, rand_tile);
    spi_result = SPI_execute(catalogQuery->data, true, 1);

    /*
     * SPI_copytuple (unlike SPI_getvalue) allocates in the context that was
     * current before SPI_connect(), so row/rowDescriptor stay valid past
     * SPI_finish() -- extracting the actual C string is deferred until
     * after SPI_finish() below (mirroring GetDBName()) so the result isn't
     * built from memory SPI_finish() is about to free out from under it.
     */
    HeapTuple row = NULL;
    TupleDesc rowDescriptor = NULL;
    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);
    if (found)
    {
        row = SPI_copytuple(SPI_tuptable->vals[0]);
        rowDescriptor = SPI_tuptable->tupdesc;
    }

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }

    if (!found)
        return NULL;

    return SPI_getvalue(row, rowDescriptor, 1);
}

/*
 * GetReferenceTableShardName returns relationId's single shard's
 * "<table>_<shardid> " identifier string, same format as GetRandomTileId
 * but with no shardminvalue filter -- a Citus reference table has exactly
 * one logical shard (replicated, under the same name, to every node), so
 * there's no per-tile shard to select between and no rand_tile to match.
 */
extern char *
GetReferenceTableShardName(Oid relationId)
{
    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "SELECT concat('%s_',shard.shardid,' ')\n"
                                   "FROM pg_dist_shard As shard\n"
                                   "WHERE shard.logicalrelid = '%s'::regclass",
                     get_rel_name(relationId), get_rel_name(relationId));
    spi_result = SPI_execute(catalogQuery->data, true, 1);

    HeapTuple row = NULL;
    TupleDesc rowDescriptor = NULL;
    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);
    if (found)
    {
        row = SPI_copytuple(SPI_tuptable->vals[0]);
        rowDescriptor = SPI_tuptable->tupdesc;
    }

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }

    if (!found)
        return NULL;

    return SPI_getvalue(row, rowDescriptor, 1);
}

/*
 * GetShardHostNode looks up the (nodename, nodeport) actually hosting
 * relationId's shard whose shardminvalue matches rand_tile, for dispatching
 * a command to the specific worker that holds that tile's data (unlike
 * GetNodeInfo(), which just picks a random node from pg_dist_node).
 */
extern TaskNode *
GetShardHostNode(Oid relationId, int rand_tile)
{
    TaskNode *taskNode = (TaskNode *) palloc0(sizeof(TaskNode));
    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    StringInfo logicalrel = makeStringInfo();
    if (IsReshuffledTable(relationId))
        appendStringInfo(logicalrel, "%s.%s", Var_Schema, get_rel_name(relationId));
    else
        appendStringInfo(logicalrel, "%s", get_rel_name(relationId));

    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery,
                     "SELECT node.nodename, node.nodeport\n"
                     "FROM pg_dist_shard shard\n"
                     "JOIN pg_dist_placement placement ON placement.shardid = shard.shardid\n"
                     "JOIN pg_dist_node node ON node.groupid = placement.groupid AND node.noderole = 'primary'\n"
                     "WHERE shard.logicalrelid = '%s'::regclass\n"
                     "  AND shard.shardminvalue = %d::text",
                     logicalrel->data, rand_tile);
    spi_result = SPI_execute(catalogQuery->data, true, 1);

    /*
     * SPI_copytuple (unlike SPI_getvalue/CStringGetTextDatum) allocates in
     * the context that was current before SPI_connect(), so row/
     * rowDescriptor stay valid past SPI_finish() -- extracting nodename and
     * building taskNode->node is deferred until after SPI_finish() below
     * (mirroring GetDBName()) so those results aren't built from memory
     * SPI_finish() is about to free out from under them. Building them
     * beforehand (as this used to) left taskNode->node dangling into SPI's
     * freed context, silently corrupted by whatever the next SPI call
     * happened to allocate over that same memory -- observed as
     * ExplainOnHostingWorker's target node name coming out mangled.
     */
    HeapTuple row = NULL;
    TupleDesc rowDescriptor = NULL;
    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);
    if (found)
    {
        row = SPI_copytuple(SPI_tuptable->vals[0]);
        rowDescriptor = SPI_tuptable->tupdesc;
    }

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }

    if (found)
    {
        bool isNull;
        char *nodename = SPI_getvalue(row, rowDescriptor, 1);
        taskNode->node = CStringGetTextDatum(nodename);
        taskNode->port = DatumGetInt32(SPI_getbinval(row, rowDescriptor, 2, &isNull));
    }
    return taskNode;
}

/* GetDBName returns the name of the database the current backend is connected to. */
extern Datum
GetDBName()
{
    int spi_result;
    bool isNull = false;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }
    spi_result = SPI_execute("SELECT current_database()", true, 1);
    /* Read back the PROJ text */
    if (spi_result == SPI_OK_SELECT)
    {
        TupleDesc rowDescriptor = SPI_tuptable->tupdesc;
        HeapTuple row = SPI_copytuple(SPI_tuptable->vals[0]);
        spi_result = SPI_finish();
        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
        return SPI_getbinval(row, rowDescriptor, 1, &isNull);
    }
    return 0;
}

