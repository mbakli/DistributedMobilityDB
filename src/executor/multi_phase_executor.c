#include <distributed/multi_join_order.h>
#include <distributed/multi_executor.h>
#include "executor/multi_phase_executor.h"
#include "planner/planner_utils.h"

static void ConstructNeighborScanQuery(SpatiotemporalTable *tbl, char * query_string,
                                       MultiPhaseExecutor *multiPhaseExecutor);
static void ConstructSelfTilingScanQuery(char *query_string,
                                         MultiPhaseExecutor *multiPhaseExecutor);
static void ReshuffleData(char * query_string, MultiPhaseExecutor *multiPhaseExecutor);
extern bool createReshuffledTable(SpatiotemporalTable *base, SpatiotemporalTable *other);
static void DistributeReshuffledTable(int numTiles, char *tileKey, char *reshuffledTable);
static void RearrangeTiles(Oid relid, int numTiles, char *reshuffledTable);
static GeneralScan * ConstructGeneralQuery(DistributedSpatiotemporalQueryPlan *distPlan,
                                           MultiPhaseExecutor *multiPhaseExecutor);

/* QueryExecutor */
extern GeneralScan *
QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain)
{
    MultiPhaseExecutor *multiPhaseExecutor = (MultiPhaseExecutor *) palloc0(sizeof(MultiPhaseExecutor));
    strcpy((char *)distPlan->org_query_string,toLower((char *) distPlan->org_query_string));
    if (distPlan->strategy == NonColocation)
    {
        /* Neighbor Scan */
        multiPhaseExecutor->table_created = createReshuffledTable( distPlan->reshuffled_table_base,
                                                                   distPlan->reshuffledTable);
        if (multiPhaseExecutor->table_created)
        {
            ReshuffleData(distPlan->reshuffling_query, multiPhaseExecutor);
            if (multiPhaseExecutor->dataReshuffled)
                ConstructNeighborScanQuery(distPlan->reshuffledTable,distPlan->org_query_string, multiPhaseExecutor);
            else
                elog(ERROR, "Could not reshuffle data!");
        }
        else
            elog(ERROR, "Could not reshuffle data!");
    }
    if (distPlan->strategy == Colocation)
    {
        /* Self Tiling Scan */
        ConstructSelfTilingScanQuery(distPlan->org_query_string, multiPhaseExecutor);
    }
    GeneralScan *generalScan = ConstructGeneralQuery(distPlan, multiPhaseExecutor);

    return generalScan;
}


/* Executor Job: Create the reshuffling table */
extern bool
createReshuffledTable(SpatiotemporalTable *base, SpatiotemporalTable *other)
{
    SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
                    PGC_S_OVERRIDE);
    /* Prepare the reshuffled table Query */
    char *reshuffled_table = get_rel_name(other->catalogTableInfo.table_oid);
    Var *distributionColumn = DistPartitionKey(other->catalogTableInfo.table_oid);
    int shardCount = ShardIntervalCount(base->catalogTableInfo.table_oid);
    char *parentRelationName = NULL;

    DropReshuffledTableIfExists(other->catalogTableInfo.reshuffledTable);
    CreateReshuffledTableIfNotExists(other->catalogTableInfo.reshuffledTable,
                                     reshuffled_table);
    Oid reshuffled_table_oid = get_relname_relid(other->catalogTableInfo.reshuffledTable,
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

    RearrangeTiles(base->catalogTableInfo.table_oid, base->catalogTableInfo.numTiles,
                   other->catalogTableInfo.reshuffledTable);
    return true;
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
    appendStringInfo(query, "DROP TABLE IF EXISTS %s.%s;", Var_Schema,reshuffled_table);
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
    appendStringInfo(query, "CREATE TABLE %s.%s(LIKE %s);", Var_Schema,reshuffled_table, org_table);
    ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
}

/* Executor Job: Rearrange the generated tiles */
static void
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

static void
ConstructNeighborScanQuery(SpatiotemporalTable *tbl, char * query_string, MultiPhaseExecutor *multiPhaseExecutor)
{
    multiPhaseExecutor->neighborScanQuery = makeStringInfo();
    StringInfo tmp = makeStringInfo();
    appendStringInfo(tmp,"%s.%s", Var_Schema, tbl->catalogTableInfo.reshuffledTable);
    appendStringInfo(multiPhaseExecutor->neighborScanQuery , "%s", replaceWord(replaceWord(replaceWord((char *)query_string, get_rel_name(tbl->catalogTableInfo.table_oid), tmp->data), "where", "WHERE T1.tile_key = T2.tile_key AND "), ";", " "));
}

static void
ConstructSelfTilingScanQuery(char * query_string, MultiPhaseExecutor *multiPhaseExecutor)
{
    multiPhaseExecutor->selfTilingScanQuery = makeStringInfo();
    appendStringInfo(multiPhaseExecutor->selfTilingScanQuery, "%s", replaceWord(query_string, "where", "WHERE T1.tile_key = T2.tile_key AND "));
}

/* Executor Job: Execute the reshuffling query */
static void
ReshuffleData(char *query_string, MultiPhaseExecutor *multiPhaseExecutor)
{
    int spi_result;
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }
    spi_result = SPI_execute(query_string, false, 1);
    if (spi_result == SPI_OK_INSERT)
    {
        spi_result = SPI_finish();
        multiPhaseExecutor->dataReshuffled = true;
        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
    }
    else
        multiPhaseExecutor->dataReshuffled = false;
}


/* Executor Job: GeneralScan */
static GeneralScan *
ConstructGeneralQuery(DistributedSpatiotemporalQueryPlan *distPlan, MultiPhaseExecutor *multiPhaseExecutor)
{
    GeneralScan *generalScan = (GeneralScan *) palloc0(sizeof(GeneralScan));
    generalScan->query_string = makeStringInfo();
    if (distPlan->strategy == NonColocation)
        appendStringInfo(generalScan->query_string, "%s", multiPhaseExecutor->neighborScanQuery->data);
    else
        appendStringInfo(generalScan->query_string, "%s", multiPhaseExecutor->selfTilingScanQuery->data);

    generalScan->query = ParseQueryString(generalScan->query_string->data,
                                          NULL, 0);
    return generalScan;
}
