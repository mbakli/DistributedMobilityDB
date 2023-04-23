#include "postgres.h"
#include "general/general_types.h"
#include <executor/spi.h>
#include "catalog/nodes.h"
#include <utils/lsyscache.h>
#include "executor/executor_tasks.h"


extern Datum GetDBName();

/* Nodes */
#define Natts_DistNodes 2
#define Anum_DistNodes_nodename 0
#define Anum_DistNodes_nodeport 1

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
    }
    return taskNode;
}

extern
char * GetRandomTileId(Oid relationId, ExecTaskType taskType)
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
    /*if (taskType == NeighborTilingScan)
        appendStringInfo(logicalrel, "%s.%s", Var_Schema, get_rel_name(relationId));
    else*/
    appendStringInfo(logicalrel, "%s", get_rel_name(relationId));
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "SELECT concat('%s_',shard.shardid,' ')\n"
                                   "FROM pg_dist_placement AS placement, pg_dist_node AS node, pg_dist_shard As shard\n"
                                   "WHERE placement.groupid = node.groupid\n"
                                   "    AND shard.logicalrelid = '%s'::regclass\n"
                                   "    AND placement.shardid = shard.shardid\n"
                                   "    AND node.noderole = 'primary' ORDER BY random() limit 1; ",
                     get_rel_name(relationId), logicalrel->data);
    spi_result = SPI_execute(catalogQuery->data, true, 1);
    /* Read back the PROJ text */
    if (spi_result == SPI_OK_SELECT)
    {
        char * res = DatumGetCString(SPI_getvalue(SPI_tuptable->vals[0],
                                                  SPI_tuptable->tupdesc,
                                                  1));
        resetStringInfo(catalogQuery);
        appendStringInfo(catalogQuery, "%s", res);
        spi_result = SPI_finish();

        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
        return catalogQuery->data;
    }
    return NULL;
}

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
        return SPI_getbinval(row, rowDescriptor, 1, &isNull);
        spi_result = SPI_finish();
        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
    }
    return 0;
}

