/*-------------------------------------------------------------------------
 *
 * multi_phase_executor.c
 *	  General Distributed MobilityDB executor code.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/executor_tasks.h"
#include "executor/multi_phase_executor.h"
#include <distributed/multi_join_order.h>
#include <distributed/multi_executor.h>
#include "utils/planner_utils.h"
#include "planner/planner_strategies.h"

static void ConstructNeighborScanQuery(Rte *tbl, char * query_string, STMultirelation *base,
                                       MultiPhaseExecutor *multiPhaseExecutor);
static void ConstructSelfTilingScanQuery(PlanTask *plan, char *query_string,
                                         MultiPhaseExecutor *multiPhaseExecutor);
static void ReshuffleData(char * query_string, MultiPhaseExecutor *multiPhaseExecutor);
extern bool createReshuffledTable(STMultirelation *base, STMultirelation *other);
static void DistributeReshuffledTable(int numTiles, char *tileKey, char *reshuffledTable);
static void ConstructPredicatePushDownQuery(PlanTask *plan, char * query_string,
                                            MultiPhaseExecutor *multiPhaseExecutor);
static GeneralScan *ConstructGeneralQuery(DistributedSpatiotemporalQueryPlan *distPlan,
                                           MultiPhaseExecutor *multiPhaseExecutor);
static void IndexReshuffledData(Rte *reshuffledTable, MultiPhaseExecutor *multiPhaseExecutor);
static void ConstructPostProcessingPhase(CoordinatorLevelOperator *coordOp, MultiPhaseExecutor *multiPhaseExecutor);



/* QueryExecutor */
extern GeneralScan *
QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain)
{
    MultiPhaseExecutor *multiPhaseExecutor = RunQueryExecutor(distPlan, explain);
    GeneralScan *generalScan = ConstructGeneralQuery(distPlan, multiPhaseExecutor);
    return generalScan;
}

/* RunQueryExecutor */
extern MultiPhaseExecutor *
RunQueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain)
{
    MultiPhaseExecutor *multiPhaseExecutor = (MultiPhaseExecutor *) palloc0(sizeof(MultiPhaseExecutor));
    multiPhaseExecutor->tasks = NIL;
    strcpy((char *)distPlan->org_query_string,toLower((char *) distPlan->org_query_string));
    /* Loop through all strategies */
    ListCell *cell = NULL;
    foreach(cell, distPlan->strategyPlans)
    {
        PlanTask *task = (PlanTask *) lfirst(cell);
        if (task->type == NonColocation)
        {
            /* Neighbor Scan */
            if (distPlan->reshuffledTable->RteType == STRte)
                multiPhaseExecutor->tableCreated = createReshuffledTable( distPlan->reshuffled_table_base,
                                                                          (STMultirelation *)distPlan->reshuffledTable->rte);
            else if (distPlan->reshuffledTable->RteType == CitusRte)
                multiPhaseExecutor->tableCreated = ColocateRte( distPlan->reshuffled_table_base,
                                                                          distPlan->reshuffledTable);
            if (multiPhaseExecutor->tableCreated)
            {
                if (!explain)
                    ReshuffleData(distPlan->reshuffling_query, multiPhaseExecutor);
                IndexReshuffledData(distPlan->reshuffledTable, multiPhaseExecutor);
                if (multiPhaseExecutor->dataReshuffled || explain)
                    ConstructNeighborScanQuery(distPlan->reshuffledTable,
                                               distPlan->org_query_string,
                                               distPlan->reshuffled_table_base,
                                               multiPhaseExecutor);
                else
                    elog(ERROR, "Could not reshuffle data!");
            }
            else
                elog(ERROR, "Could not reshuffle data!");
        }
        else if (task->type == Colocation)
        {
            /* Self Tiling Scan */
            ConstructSelfTilingScanQuery(task, distPlan->org_query_string,
                                         multiPhaseExecutor);
        }
        else if (task->type == PredicatePushDown)
        {
            /* Predicate Push Down Scan */
            ConstructPredicatePushDownQuery(task, distPlan->org_query_string,
                                         multiPhaseExecutor);
        }
    }

    /* Post processing processing */
    ConstructPostProcessingPhase(distPlan->postProcessing->coordinatorLevelOperator, multiPhaseExecutor);
    return multiPhaseExecutor;
}


/* Executor Job: Colocation  */
extern bool
ColocateRte(STMultirelation *base, Rte *other)
{
    SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
                    PGC_S_OVERRIDE);
    if (other->RteType == CitusRte)
    {
        CitusRteNode *citusRteNode = (CitusRteNode *)other->rte;
        RangeTblEntry * cell = (RangeTblEntry *) lfirst(citusRteNode->rangeTableCell);
        /* Prepare the reshuffled table Query */
        char *reshuffled_table = get_rel_name(cell->relid);
        Var *distributionColumn = DistPartitionKey(base->catalogTableInfo.table_oid);
        int shardCount = ShardIntervalCount(base->catalogTableInfo.table_oid);
        char *parentRelationName = NULL;

        DropReshuffledTableIfExists(citusRteNode->reshuffledTable);
        CreateReshuffledTableIfNotExists(citusRteNode->reshuffledTable,
                                         reshuffled_table, true);
        Oid reshuffled_table_oid = get_relname_relid(citusRteNode->reshuffledTable,
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
                       citusRteNode->reshuffledTable);
        return true;
    }
    return false;

}

/* Executor Job: Create the reshuffling table */
extern bool
createReshuffledTable(STMultirelation *base, STMultirelation *other)
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
                                     reshuffled_table, false);
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
CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table, bool tile_key)
{
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "CREATE TABLE %s.%s(LIKE %s);", Var_Schema,reshuffled_table, org_table);
    if(tile_key)
        appendStringInfo(query, "ALTER TABLE %s.%s ADD COLUMN %s integer;", Var_Schema,reshuffled_table,
                         Var_Catalog_Tile_Key);
    ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
}

/* Executor Job: Construct the neighbor scan */
static void
ConstructNeighborScanQuery(Rte *tbl, char * query_string, STMultirelation *base,MultiPhaseExecutor *multiPhaseExecutor)
{
    ExecutorTask *task = (ExecutorTask *) palloc0(sizeof(ExecutorTask));
    task->taskType = NeighborTilingScan;
    task->taskQuery = makeStringInfo();
    if (tbl->RteType == STRte)
    {
        STMultirelation *stMultirelation = (STMultirelation *)tbl->rte;
        task->catalog_filtered = stMultirelation->catalogFilter;
        task->numCores = stMultirelation->catalogTableInfo.numTiles;
        /* Add the catalog filter */
        if (stMultirelation->catalogFilter->candidates > stMultirelation->catalogTableInfo.numTiles)
            task->catalog_filtered = stMultirelation->catalogFilter;
        appendStringInfo(task->taskQuery, "%s",
                         DatumGetCString(AddTilingKey(stMultirelation->catalogTableInfo, tbl->alias, query_string)));
    }
    else if (tbl->RteType == CitusRte)
    {
        task->catalog_filtered = base->catalogFilter;
        task->numCores = base->catalogTableInfo.numTiles;
        /* Add the catalog filter */
        if (base->catalogFilter->candidates > base->catalogTableInfo.numTiles)
            task->catalog_filtered = base->catalogFilter;
        appendStringInfo(task->taskQuery, "%s",
                         DatumGetCString( AddNonStRteTilingKey(tbl, base->alias, query_string)));
    }
    multiPhaseExecutor->tasks = lappend(multiPhaseExecutor->tasks, task);
}

/* Executor Job: Construct the self tiling scan */
static void
ConstructSelfTilingScanQuery(PlanTask *plan, char * query_string, MultiPhaseExecutor *multiPhaseExecutor)
{
    ExecutorTask *task = (ExecutorTask *) palloc0(sizeof(ExecutorTask));
    task->taskType = SelfTilingScan;
    if (plan->tbl1->catalogFilter == NULL)
    {
        task->catalog_filtered = plan->tbl2->catalogFilter;
        task->numCores = plan->tbl2->catalogTableInfo.numTiles;
    }
    else
    {
        task->catalog_filtered = plan->tbl1->catalogFilter;
        task->numCores = plan->tbl1->catalogTableInfo.numTiles;
    }
    task->taskQuery = makeStringInfo();
    StringInfo key = makeStringInfo();
    appendStringInfo(key, "WHERE %s.%s = %s.%s AND ",
                     plan->tbl1->alias->aliasname, Var_Catalog_Tile_Key,
                     plan->tbl2->alias->aliasname, Var_Catalog_Tile_Key);
    appendStringInfo(task->taskQuery, "%s",replaceWord(query_string,
                                                        "where", key->data));
    multiPhaseExecutor->tasks = lappend(multiPhaseExecutor->tasks, task);
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
        multiPhaseExecutor->dataReshuffled = true;
    }
    else
        multiPhaseExecutor->dataReshuffled = false;
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
}

/* Executor Job: Index the reshuffled data */
static void
IndexReshuffledData(Rte *reshuffledTable, MultiPhaseExecutor *multiPhaseExecutor)
{
    int spi_result;
    StringInfo val = makeStringInfo();
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }
    if (reshuffledTable->RteType == STRte)
    {
        STMultirelation *stMultirelation = (STMultirelation *)reshuffledTable->rte;
        appendStringInfo(val, "CREATE INDEX %s_%s_idx on %s.%s USING %s(%s);",
                         stMultirelation->catalogTableInfo.reshuffledTable,
                         stMultirelation->col, Var_Schema, stMultirelation->catalogTableInfo.reshuffledTable,
                         Var_Spatiotemporal_Index, stMultirelation->col);
    }
    else if (reshuffledTable->RteType == CitusRte)
    {
        CitusRteNode *citusRteNode = (CitusRteNode *)reshuffledTable->rte;
        appendStringInfo(val, "CREATE INDEX %s_%s_idx on %s.%s USING %s(%s);",
                         citusRteNode->reshuffledTable,
                         citusRteNode->col, Var_Schema, citusRteNode->reshuffledTable,
                         Var_Spatiotemporal_Index, citusRteNode->col);

    }
    spi_result = SPI_execute(val->data, false, 1);
    if (spi_result == SPI_OK_UTILITY)
    {
        multiPhaseExecutor->indexCreated = true;
    }
    else
        multiPhaseExecutor->indexCreated = false;

    /*
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
     */

}

/* Executor Job: GeneralScan */
static GeneralScan *
ConstructGeneralQuery(DistributedSpatiotemporalQueryPlan *distPlan, MultiPhaseExecutor *multiPhaseExecutor)
{
    GeneralScan *generalScan = (GeneralScan *) palloc0(sizeof(GeneralScan));
    generalScan->length = 0;
    generalScan->query_string = makeStringInfo();
    ListCell *cell = NULL;

    /* Loop through current plan strategies involved in this query */
    foreach(cell, distPlan->strategies)
    {
        StrategyType strategy = (StrategyType) lfirst_int(cell);
        if (strategy == NonColocation)
        {
            if (generalScan->length > 0)
                appendStringInfo(generalScan->query_string, "%s", " UNION ");
            appendStringInfo(generalScan->query_string, "%s",
                             taskQuery(multiPhaseExecutor->tasks, NeighborTilingScan));
            generalScan->length++;
        }
        else if (strategy == Colocation)
        {
            if (generalScan->length > 0)
                appendStringInfo(generalScan->query_string, "%s", " UNION ");
            appendStringInfo(generalScan->query_string, "%s",
                             taskQuery(multiPhaseExecutor->tasks, SelfTilingScan));
            generalScan->length++;
        }
        else if (strategy == PredicatePushDown)
        {
            if (generalScan->length > 0)
                appendStringInfo(generalScan->query_string, "%s", " UNION ");
            appendStringInfo(generalScan->query_string, "%s",
                             taskQuery(multiPhaseExecutor->tasks, PushDownScan));
            generalScan->length++;
        }
        else
            elog(ERROR, "The query executor could not identify the planner strategy");
    }
    /* Loop though the post processing tasks */
    if (generalScan->length == 0)
        appendStringInfo(generalScan->query_string,"%s",
                         distPlan->postProcessing->worker);
    foreach(cell, multiPhaseExecutor->coordTasks)
    {
        ExecutorTask *task = (ExecutorTask *) lfirst(cell);
        if (task->taskType == FINALScan)
        {
            StringInfo temp = makeStringInfo();
            appendStringInfo(temp, "%s", change_sentence(task->taskQuery->data,
                                                     "intermediate", generalScan->query_string->data));
            resetStringInfo(generalScan->query_string);
            appendStringInfo(generalScan->query_string, "%s", temp->data);
        }
    }
    generalScan->query = ParseQueryString(generalScan->query_string->data,
                                          NULL, 0);
    return generalScan;
}

/* Executor Job: Construct the self tiling scan */
static void
ConstructPredicatePushDownQuery(PlanTask *plan, char * query_string, MultiPhaseExecutor *multiPhaseExecutor)
{
    ExecutorTask *task = (ExecutorTask *) palloc0(sizeof(ExecutorTask));
    task->taskType = PushDownScan;
    task->catalog_filtered = plan->tbl1->catalogFilter;
    task->numCores = plan->tbl1->catalogTableInfo.numTiles;
    task->taskQuery = makeStringInfo();
    appendStringInfo(task->taskQuery, "%s",query_string);
    multiPhaseExecutor->tasks = lappend(multiPhaseExecutor->tasks, task);
}

extern Datum
GetTaskType(ExecutorTask *task)
{
    if (task->taskType == NeighborTilingScan)
        return CStringGetDatum("Neighbor Scan");
    else if (task->taskType == SelfTilingScan)
        return CStringGetDatum("Self Tiling Scan");
}

static void
ConstructPostProcessingPhase(CoordinatorLevelOperator *coordOp, MultiPhaseExecutor *multiPhaseExecutor)
{
    if (list_length(coordOp->intermediateOp) > 0)
    {
        ExecutorTask *task = ProcessIntermediateTasks(coordOp->intermediateOp);
        multiPhaseExecutor->coordTasks = lappend(multiPhaseExecutor->coordTasks, task);
    }
    if (list_length(coordOp->finalOp) > 0)
    {
        ExecutorTask *task = ProcessFinalTasks(coordOp->finalOp);
        multiPhaseExecutor->coordTasks = lappend(multiPhaseExecutor->coordTasks, task);
    }
}

