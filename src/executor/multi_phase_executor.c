#include <utils/lsyscache.h>
#include <distributed/multi_join_order.h>
#include <utils/snapmgr.h>
#include <executor/spi.h>
#include "executor/multi_phase_executor.h"
#include "planner/planner_utils.h"

/* Executor Job: Create the reshuffling table */
extern void
createReshuffledTable(DistributedSpatiotemporalQueryPlan *distPlan)
{
    SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
                    PGC_S_OVERRIDE);
    /* Prepare the reshuffled table Query */
    char *reshuffled_table = get_rel_name(distPlan->reshuffledTable->catalogTableInfo.table_oid);
    Var *distributionColumn = DistPartitionKey(distPlan->reshuffledTable->catalogTableInfo.table_oid);
    int shardCount = ShardIntervalCount(distPlan->reshuffled_table_base->catalogTableInfo.table_oid);
    char *parentRelationName = NULL;

    DropReshuffledTableIfExists(distPlan->reshuffledTable->catalogTableInfo.reshuffledTable);
    CreateReshuffledTableIfNotExists(distPlan->reshuffledTable->catalogTableInfo.reshuffledTable,
                                     reshuffled_table);

    Oid reshuffled_table_oid = get_relname_relid(distPlan->reshuffledTable->catalogTableInfo.reshuffledTable,
                                                 get_namespace_oid(Var_Schema, false));

    Relation relation = try_relation_open(reshuffled_table_oid, ExclusiveLock);
    if (relation == NULL)
    {
        ereport(ERROR, (errmsg("could not create distributed table: "
                               "relation %s does not exist", get_rel_name(reshuffled_table_oid))));
    }
    relation_close(relation, NoLock);
    CreateDistributedTable(reshuffled_table_oid, distributionColumn,
                           DISTRIBUTE_BY_RANGE, shardCount, true,
                           parentRelationName, true);
    StringInfo reshuffledTablesQuery = makeStringInfo();
    appendStringInfo(reshuffledTablesQuery,
                     "SELECT reshuffled_table_creation(tablename=> '%s', "
                                "shards=>%d , reshuffled_table=> '%s');",
                     get_rel_name(distPlan->reshuffled_table_base->catalogTableInfo.table_oid) ,
                     shardCount, distPlan->reshuffledTable->catalogTableInfo.reshuffledTable);
    ExecuteQueryViaSPI(reshuffledTablesQuery->data, SPI_OK_SELECT);
}

/* Executor Job: Drop the reshuffled table if exists */
extern void
DropReshuffledTableIfExists(char * reshuffled_table)
{
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "DROP TABLE IF EXISTS %s;", reshuffled_table);
    ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
}

/* Executor Job: Create the reshuffled table if not exists */
extern void
CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table)
{
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "CREATE TABLE %s(LIKE %s);", reshuffled_table, org_table);
    ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
}