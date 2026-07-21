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
#include <ctype.h>
#include "executor/executor_tasks.h"
#include "executor/multi_phase_executor.h"
#include <distributed/multi_join_order.h>
#include <distributed/multi_executor.h>
#include <distributed/distribution_column.h>
#include <catalog/namespace.h>
#include <access/xact.h>
#include "utils/planner_utils.h"
#include "utils/helper_functions.h"
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
static char *EliminateShapeSegmentDuplicates(char *query_string, bool hasDistributedAggregate);
static char *StripOrderByAliasQualifiers(char *orderByText, Query *parse);



/*
 * QueryExecutor drives a distributed query end-to-end: runs every planned
 * strategy via RunQueryExecutor(), then stitches the resulting per-strategy
 * tasks into the single SQL statement returned as a GeneralScan.
 */
extern GeneralScan *
QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain)
{
    MultiPhaseExecutor *multiPhaseExecutor = RunQueryExecutor(distPlan, explain);
    GeneralScan *generalScan = ConstructGeneralQuery(distPlan, multiPhaseExecutor);
    return generalScan;
}

/*
 * RunQueryExecutor walks distPlan's chosen PlanTask strategies in order and
 * materializes each into ExecutorTasks: NonColocation triggers a reshuffle
 * of one side into a temporary colocated table followed by a neighbor scan,
 * Colocation performs a direct self-tiling (tile-key equi-join) scan, and
 * PredicatePushDown runs the predicate as-is on the worker. `explain`
 * suppresses the actual reshuffle/data-modifying SPI calls so EXPLAIN can
 * describe the plan without side effects.
 */
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


/*
 * ColocateRte makes a plain Citus-distributed `other` table colocated with
 * `base`: it materializes other's data into a fresh reshuffled table
 * distributed on base's distribution column/shard count, then rebalances
 * base's tiles into it so both sides can be scanned tile-by-tile without
 * cross-node data movement. Returns false if `other` is not a CitusRte.
 */
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
        /* Citus rejects colocate_with for range-distributed tables
         * ("colocate_with option is not supported for append / range
         * distributed tables"), so this stays "default"; physical
         * co-location with base is instead done after the fact by
         * colocate_shards() in create_reshuffled_multirelation, which
         * explicitly moves each shard onto base's matching-tile node. */
        char *parentRelationName = "default";

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
        CreateDistributedTable(reshuffled_table_oid,
                               ColumnToColumnName(base->catalogTableInfo.table_oid, (Node *) distributionColumn),
                               DISTRIBUTE_BY_RANGE, shardCount, true,
                               parentRelationName);

        RearrangeTiles(base->catalogTableInfo.table_oid, base->catalogTableInfo.numTiles,
                       citusRteNode->reshuffledTable);
        return true;
    }
    return false;

}

/*
 * createReshuffledTable is the STRte counterpart of ColocateRte(): it
 * materializes `other`'s data into a fresh table distributed to match
 * `base`'s shard count, then rebalances base's tiles into it so both
 * spatiotemporal relations can be joined tile-by-tile.
 */
extern bool
createReshuffledTable(STMultirelation *base, STMultirelation *other)
{
    SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
                    PGC_S_OVERRIDE);
    /* Prepare the reshuffled table Query */
    char *reshuffled_table = get_rel_name(other->catalogTableInfo.table_oid);
    Var *distributionColumn = DistPartitionKey(other->catalogTableInfo.table_oid);
    int shardCount = ShardIntervalCount(base->catalogTableInfo.table_oid);
    /* Citus rejects colocate_with for range-distributed tables
     * ("colocate_with option is not supported for append / range
     * distributed tables"), so this stays "default"; physical
     * co-location with base is instead done after the fact by
     * colocate_shards() in create_reshuffled_multirelation, which
     * explicitly moves each shard onto base's matching-tile node. */
    char *parentRelationName = "default";

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
    CreateDistributedTable(reshuffled_table_oid,
                           ColumnToColumnName(other->catalogTableInfo.table_oid, (Node *) distributionColumn),
                           DISTRIBUTE_BY_RANGE, shardCount, true,
                           parentRelationName);

    RearrangeTiles(base->catalogTableInfo.table_oid, base->catalogTableInfo.numTiles,
                   other->catalogTableInfo.reshuffledTable);
    return true;
}

/*
 * DropReshuffledTableIfExists drops the temporary per-query reshuffled
 * table from the extension's schema. The surrounding
 * commit/start-transaction pair runs the DDL in its own transaction so it
 * takes effect immediately and is visible to the CREATE that follows it,
 * rather than staying pending inside the planner's outer transaction.
 */
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

/*
 * CreateReshuffledTableIfNotExists creates reshuffled_table as a `LIKE
 * org_table` copy in the extension's schema, optionally adding the tile-key
 * column (for tables that will be reshuffled/rebalanced across tiles). Runs
 * in its own commit/start-transaction bracket for the same reason as
 * DropReshuffledTableIfExists.
 */
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

/*
 * ConstructNeighborScanQuery builds the task that scans `tbl` (the
 * reshuffled/colocated side) tile-by-tile against `base`'s tiling scheme,
 * widening the tile set searched (catalog_filtered) whenever the catalog
 * filter found more matching candidates than there are tiles.
 */
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
                         DatumGetCString(AddTilingKey(stMultirelation->catalogTableInfo, tbl->alias, base->alias, query_string)));
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

/*
 * ConstructSelfTilingScanQuery builds the task for a Colocation-strategy
 * join: since both tables share the same tiling, the join reduces to
 * adding a `tile_key = tile_key` equality (replacing the query's `WHERE`)
 * so each tile only ever matches its own counterpart tile, avoiding any
 * cross-tile data transfer.
 */
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

/*
 * ReshuffleData runs the INSERT ... SELECT that copies base's rows into the
 * reshuffled/colocated table (see createReshuffledTable/ColocateRte),
 * recording success in multiPhaseExecutor->dataReshuffled.
 */
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

/*
 * IndexReshuffledData creates the spatiotemporal (GIST) index on the newly
 * reshuffled table's distribution column, needed before it can be scanned
 * efficiently in the neighbor-scan phase.
 */
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

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }

}

/*
 * EliminateShapeSegmentDuplicates removes shape-segmentation duplicates from
 * query_string by wrapping it as `SELECT DISTINCT * FROM (query_string) AS
 * x`, deduping on whatever the query actually projects. Only applied when
 * no distributed aggregate is involved (see hasDistributedAggregate at the
 * call site) -- for an aggregate like count(*), the duplicates are already
 * consumed before this outer DISTINCT would ever see them, so this is left
 * as a no-op (returns NULL) for that case rather than risking a rewrite
 * Citus' planner may reject for non-colocated repartition joins.
 */
static char *
EliminateShapeSegmentDuplicates(char *query_string, bool hasDistributedAggregate)
{
    if (hasDistributedAggregate)
        return NULL;

    char *innerQuery = replaceWord(query_string, ";", " ");
    StringInfo dedupedQuery = makeStringInfo();
    appendStringInfo(dedupedQuery, "SELECT DISTINCT * FROM (%s) AS dedup_result", innerQuery);
    return dedupedQuery->data;
}

/*
 * StripOrderByAliasQualifiers removes every "alias." qualifier belonging to
 * one of parse's own FROM-clause range table entries from orderByText. The
 * final ORDER BY is re-applied (see the sortClause handling in
 * ConstructGeneralQuery below) outside a "SELECT * FROM (...) AS
 * ordered_result" wrap whose only visible columns are the wrapped
 * subquery's own unqualified output column names -- a table-qualified
 * reference copied verbatim from the original query text (e.g. "p.pointid")
 * is not in scope out there and fails with "missing FROM-clause entry for
 * table \"p\"" (confirmed on a BerlinMOD Q4-style query: `... ORDER BY
 * p.PointId, v.Licence` against trips_Nt joined with two reference tables).
 * orderByText is already lowercased (it's sliced out of
 * distPlan->org_query_string, itself lowercased in place by
 * RunQueryExecutor), so the qualifiers built here are lowered to match.
 */
static char *
StripOrderByAliasQualifiers(char *orderByText, Query *parse)
{
    char *result = orderByText;
    ListCell *cell;
    foreach(cell, parse->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(cell);
        if (rte->rtekind != RTE_RELATION || !rte->inFromCl)
            continue;
        StringInfo qualifier = makeStringInfo();
        appendStringInfo(qualifier, "%s.", rte->eref->aliasname);
        char *lowerQualifier = toLower(qualifier->data);
        while (strstr(result, lowerQualifier) != NULL)
            result = change_sentence(result, lowerQualifier, "");
    }
    return result;
}

/*
 * ConstructGeneralQuery assembles the final SQL text to execute: it unions
 * together the worker-phase task query for each strategy used in the plan
 * (NonColocation -> neighbor scan, Colocation -> self-tiling scan,
 * PredicatePushDown -> push-down scan), then, if a coordinator-phase
 * (FINALScan) task exists, splices that union in as its `intermediate`
 * subquery so the coordinator can post-process the combined worker output.
 */
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
    /* NonColocation/Colocation strategies reshuffle onto tiles built from a
     * spatiotemporal shape, which can place the same row's shape-segmented
     * copy in more than one tile so a boundary-crossing match isn't missed
     * -- see checkQueryType's dupRemOperator->active assignment. That makes
     * the worker-phase join above contain the same logical match
     * (base_row1, base_row2) more than once. This has to be resolved here,
     * on the flat two-table query before the coordinator/aggregate-rewriter
     * wrapping below nests it inside a "select sum(...) from (...) as fQ"
     * subquery -- once that wrapping happens, ExtractRangeTableEntryList
     * sees three range table entries (the subquery plus its two inner
     * tables) instead of the two this rewrite expects, and any duplicate
     * rows have already been consumed by the inner aggregate anyway. */
    if (distPlan->postProcessing->coordinatorLevelOperator->dupRemOperator->active)
    {
        bool hasDistributedAggregate = list_length(distPlan->postProcessing->distfuns) > 0;
        char *deduped = EliminateShapeSegmentDuplicates(generalScan->query_string->data,
                                                        hasDistributedAggregate);
        if (deduped != NULL)
        {
            resetStringInfo(generalScan->query_string);
            appendStringInfo(generalScan->query_string, "%s", deduped);
        }
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
    /*
     * A strategy like PredicatePushDown can push the *entire* original
     * query text (GROUP BY/aggregate/ORDER BY and all) down to run
     * independently per relevant tile/shard, then concatenate (UNION) each
     * task's own output above -- any ORDER BY embedded in that pushed-down
     * text only sorts *within* one task's own result, so the final
     * concatenated result is a sequence of independently-sorted runs, not
     * one globally sorted result (confirmed: a query combining a CTE,
     * cross-table aggregation, and ORDER BY returned rows grouped by
     * originating shard instead of by the ORDER BY key). Re-applying the
     * original top-level ORDER BY once more, over the fully assembled
     * result, fixes this regardless of how many tasks/strategies
     * contributed to it -- and is a harmless no-op wrap for any query shape
     * that was already correctly ordered.
     */
    if (distPlan->query->sortClause != NIL)
    {
        /* org_query_string was already lowercased in place by
         * RunQueryExecutor above. FindTopLevelKeywordToken (not
         * FindKeywordToken) is required here, not just for the whitespace
         * tolerance, but because this is the *whole* query's text -- a CTE
         * body can itself contain "order by"/"group by"/etc at a nested
         * paren depth, which must not be mistaken for the outermost
         * query's own trailing ORDER BY. */
        char *orderByPos = FindTopLevelKeywordToken(distPlan->org_query_string, "order by");
        if (orderByPos != NULL)
        {
            char *orderByText = pstrdup(orderByPos);
            size_t len = strlen(orderByText);
            while (len > 0 && (orderByText[len - 1] == ';' || isspace((unsigned char) orderByText[len - 1])))
                orderByText[--len] = '\0';

            orderByText = StripOrderByAliasQualifiers(orderByText, distPlan->query);

            StringInfo wrapped = makeStringInfo();
            appendStringInfo(wrapped, "SELECT * FROM (%s) AS ordered_result %s",
                             generalScan->query_string->data, orderByText);
            resetStringInfo(generalScan->query_string);
            appendStringInfo(generalScan->query_string, "%s", wrapped->data);
        }
    }
    generalScan->query = ParseQueryString(generalScan->query_string->data,
                                          NULL, 0);
    return generalScan;
}

/*
 * ConstructPredicatePushDownQuery builds the task for the PredicatePushDown
 * strategy: the original query text is run as-is (no rewriting), since the
 * predicate can be fully evaluated on the worker without any coordinator
 * merge step.
 */
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

/*
 * GetTaskType renders task's ExecTaskType as a human-readable label for
 * EXPLAIN output. Every ExecTaskType that ExplainPlanStrategies can
 * actually iterate over (multiPhaseExecutor->tasks -- never the separate
 * coordTasks list, so INTERMEDIATEScan/FINALScan don't reach here today)
 * needs a case; falling off the end without returning left this Datum
 * uninitialized, and the caller's DatumGetCString/appendStringInfo("%s")
 * would then dereference whatever garbage pointer was left in the return
 * register -- e.g. for a PushDownScan task (a single distributed table
 * joined against reference tables only, no self-join), crashing EXPLAIN
 * outright.
 */
extern Datum
GetTaskType(ExecutorTask *task)
{
    if (task->taskType == NeighborTilingScan)
        return CStringGetDatum("Neighbor Scan");
    else if (task->taskType == SelfTilingScan)
        return CStringGetDatum("Self Tiling Scan");
    else if (task->taskType == PushDownScan)
        return CStringGetDatum("Push Down Scan");
    else if (task->taskType == INTERMEDIATEScan)
        return CStringGetDatum("Intermediate Scan");
    else if (task->taskType == FINALScan)
        return CStringGetDatum("Final Scan");
    else
        return CStringGetDatum("Unknown Scan");
}

/*
 * ConstructPostProcessingPhase builds the coordinator-phase tasks (see
 * ProcessIntermediateTasks/ProcessFinalTasks) for any distributed
 * aggregates recorded in coordOp, so they get merged into the final query
 * by ConstructGeneralQuery().
 */
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

