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
#include <ctype.h>
#include "executor/executor_tasks.h"
#include "executor/multi_phase_executor.h"
#include <distributed/multi_join_order.h>
#include <distributed/multi_executor.h>
#include <distributed/distribution_column.h>
#include <distributed/metadata_cache.h>
#include <catalog/namespace.h>
#include <access/xact.h>
#include "utils/planner_utils.h"
#include "utils/helper_functions.h"
#include "planner/planner_strategies.h"
#include <optimizer/optimizer.h>
#include <portability/instr_time.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <commands/dbcommands.h>
#include <access/tupdesc.h>
#include <executor/spi.h>
#include <miscadmin.h>
#include <nodes/nodeFuncs.h>

/* dmdb.parallel_same_tile_scan support: fixed dblink connection/temp table
 * names, see DispatchSameTileQueryAsync/FetchAsyncSameTileResult. */
#define Var_Async_SameTile_Conn "dmdb_async_sametile_conn"
#define Var_Async_SameTile_Result_Table "dmdb_async_sametile_result"

static void ConstructNeighborScanQuery(Rte *tbl, char * query_string, STMultirelation *base,
                                       MultiPhaseExecutor *multiPhaseExecutor);
static void ConstructSelfTilingScanQuery(PlanTask *plan, char *query_string,
                                         MultiPhaseExecutor *multiPhaseExecutor, float distance,
                                         Query *parsedQuery, bool explain);
static char *BuildSelfTilingScanQueryText(PlanTask *plan, char *query_string, float distance,
                                          Query *parsedQuery, bool explain);
static bool ColumnExistsOnTable(Oid relationId, const char *columnName);
static char *BuildBboxProxyCondition(PlanTask *plan, float distance);
static char *BuildSidePrefilterQueryText(PlanTask *plan, char *query_string, Query *parsedQuery, bool explain);
static bool HasStboxExpressionIndex(Oid relationId, const char *columnName);
static bool HasTrajectoryExpressionIndex(Oid relationId, const char *columnName);
static char *InjectStboxIntersectionPrefilter(char *query_string, Query *parsedQuery, bool explain);
static void DispatchSameTileQueryAsync(const char *queryText);
static char *FetchAsyncSameTileResult(const char *queryText, Query *queryDesc);
static bool StrategiesInclude(List *strategies, StrategyType type);
static void ReshuffleData(char * query_string, MultiPhaseExecutor *multiPhaseExecutor);
extern bool createReshuffledTable(STMultirelation *base, STMultirelation *other, bool explain);
static void DistributeReshuffledTable(int numTiles, char *tileKey, char *reshuffledTable);
static void ConstructPredicatePushDownQuery(PlanTask *plan, char * query_string,
                                            MultiPhaseExecutor *multiPhaseExecutor);
static GeneralScan *ConstructGeneralQuery(DistributedSpatiotemporalQueryPlan *distPlan,
                                           MultiPhaseExecutor *multiPhaseExecutor);
static void IndexReshuffledData(Rte *reshuffledTable, MultiPhaseExecutor *multiPhaseExecutor);
static int64 GetLiveRowCount(const char *tableName);
static bool ReshuffleCacheIsFresh(Oid baseTableOid, const char *baseTableName, const char *reshuffledTableName, double cacheDistance);
static void RecordReshuffleCache(Oid baseTableOid, const char *baseTableName, const char *reshuffledTableName, double cacheDistance);
static void ConstructPostProcessingPhase(CoordinatorLevelOperator *coordOp, MultiPhaseExecutor *multiPhaseExecutor);
static char *EliminateShapeSegmentDuplicates(char *query_string, bool hasDistributedAggregate);
static char *StripOrderByAliasQualifiers(char *orderByText, Query *parse);
static char *BuildPositionalOrderBy(Query *parse, const char *orderByText);
static char *FindEarliestSortModifier(const char *chunk);
static char *StripTrailingOrderBy(const char *queryText);



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

    /* dmdb.use_stbox_expr_prefilter: see InjectStboxIntersectionPrefilter's
     * comment. Applied here, once, on the original query text -- before any
     * strategy-specific task construction reads org_query_string -- so it
     * benefits whichever strategy(ies) end up handling the query, not just
     * the self-join Colocation path the other bbox-related GUCs are scoped
     * to. */
    distPlan->org_query_string = InjectStboxIntersectionPrefilter(
        distPlan->org_query_string, distPlan->query, explain);

    /* dmdb.parallel_same_tile_scan (debug/experimental, off by default):
     * when a query needs BOTH a Colocation (same-tile) and NonColocation
     * (reshuffle) strategy, the same-tile branch only ever reads the base
     * table -- it has no data dependency on the reshuffle build at all, so
     * there's no correctness reason the two need to run sequentially. With
     * this on, the Colocation branch below fires its query asynchronously
     * (via dblink) instead of building its task immediately, and the
     * NonColocation branch fetches that result once its own reshuffle work
     * finishes, building the SelfTilingScan task from the already-computed
     * answer instead of running the same-tile scan a second time as part of
     * the final UNION. See DispatchSameTileQueryAsync/FetchAsyncSameTileResult.
     * Colocation is always planned before NonColocation for this query shape
     * (ProcessPredicateClause adds Colocation to distPlan->strategies first),
     * so distPlan->strategyPlans -- built in that same order -- is guaranteed
     * to reach the Colocation branch before the NonColocation branch below
     * needs to know whether a dispatch is pending. */
    const char *parallelFlag = GetConfigOption("dmdb.parallel_same_tile_scan", true, false);
    bool parallelSameTileOn = (parallelFlag != NULL && strcmp(parallelFlag, "on") == 0);
    bool sameTileDispatchedAsync = false;
    char *sameTileQueryTextForFetch = NULL;

    /* dmdb.use_bbox_proxy_filter (debug/experimental, off by default): see
     * BuildBboxProxyCondition's comment. Only meaningful for a genuine
     * distance predicate (eDwithin) -- -1 signals "not applicable" the same
     * way DistanceReshufflingPlan's own distPlan->distance sentinel does. */
    float sameTileDistance = (distPlan->predicatesList != NULL &&
                              distPlan->predicatesList->predicateType == DISTANCE)
                             ? distPlan->predicatesList->predicateInfo->distancePredicate->distance
                             : -1.0;

    /* Loop through all strategies */
    ListCell *cell = NULL;
    foreach(cell, distPlan->strategyPlans)
    {
        PlanTask *task = (PlanTask *) lfirst(cell);
        if (task->type == NonColocation)
        {
            instr_time phaseStart, phaseEnd;
            /* Neighbor Scan */

            /* Base table oid/name and destination reshuffled-table name,
             * needed up front to check the reuse cache before paying for
             * any rebuild -- same lookups createReshuffledTable/ColocateRte
             * do internally, just surfaced here too. */
            Oid baseTableOid = InvalidOid;
            char *baseTableName = NULL;
            char *reshuffledTableName = NULL;
            if (distPlan->reshuffledTable->RteType == STRte)
            {
                STMultirelation *other = (STMultirelation *) distPlan->reshuffledTable->rte;
                baseTableOid = other->catalogTableInfo.table_oid;
                baseTableName = get_rel_name(baseTableOid);
                reshuffledTableName = other->catalogTableInfo.reshuffledTable;
            }
            else if (distPlan->reshuffledTable->RteType == CitusRte)
            {
                CitusRteNode *citusRteNode = (CitusRteNode *) distPlan->reshuffledTable->rte;
                baseTableOid = ((RangeTblEntry *) lfirst(citusRteNode->rangeTableCell))->relid;
                baseTableName = get_rel_name(baseTableOid);
                reshuffledTableName = citusRteNode->reshuffledTable;
            }

            /* DistanceReshufflingPlan bakes the query's own distance
             * threshold into which rows the reshuffled table gets -- a
             * different threshold against the same base table needs a
             * genuinely different reshuffled table, so it must be part of
             * the cache key (see pg_dist_spatiotemporal_reshuffle_cache's
             * comment). -1 is the sentinel for a non-distance predicate,
             * whose reshuffling doesn't depend on any distance value. */
            double cacheDistance = (distPlan->predicatesList != NULL &&
                                    distPlan->predicatesList->predicateType == DISTANCE)
                                   ? (double) distPlan->distance : -1.0;

            if (!explain && OidIsValid(baseTableOid) &&
                ReshuffleCacheIsFresh(baseTableOid, baseTableName, reshuffledTableName, cacheDistance))
            {
                multiPhaseExecutor->tableCreated = true;
                multiPhaseExecutor->dataReshuffled = true;
                elog(INFO, "Reusing existing reshuffled table %s (base table %s row count unchanged)",
                    reshuffledTableName, baseTableName);
            }
            else
            {
                INSTR_TIME_SET_CURRENT(phaseStart);
                if (distPlan->reshuffledTable->RteType == STRte)
                    multiPhaseExecutor->tableCreated = createReshuffledTable( distPlan->reshuffled_table_base,
                                                                              (STMultirelation *)distPlan->reshuffledTable->rte,
                                                                              explain);
                else if (distPlan->reshuffledTable->RteType == CitusRte)
                    multiPhaseExecutor->tableCreated = ColocateRte( distPlan->reshuffled_table_base,
                                                                              distPlan->reshuffledTable, explain);
                INSTR_TIME_SET_CURRENT(phaseEnd);
                INSTR_TIME_SUBTRACT(phaseEnd, phaseStart);
                elog(INFO, "TIMING createReshuffledTable/ColocateRte: %.3f ms", INSTR_TIME_GET_MILLISEC(phaseEnd));
                if (multiPhaseExecutor->tableCreated)
                {
                    if (!explain)
                    {
                        INSTR_TIME_SET_CURRENT(phaseStart);
                        ReshuffleData(distPlan->reshuffling_query, multiPhaseExecutor);
                        INSTR_TIME_SET_CURRENT(phaseEnd);
                        INSTR_TIME_SUBTRACT(phaseEnd, phaseStart);
                        elog(INFO, "TIMING ReshuffleData (data copy INSERT): %.3f ms",
                            INSTR_TIME_GET_MILLISEC(phaseEnd));
                        if (multiPhaseExecutor->dataReshuffled && OidIsValid(baseTableOid))
                            RecordReshuffleCache(baseTableOid, baseTableName, reshuffledTableName, cacheDistance);
                    }
                    INSTR_TIME_SET_CURRENT(phaseStart);
                    IndexReshuffledData(distPlan->reshuffledTable, multiPhaseExecutor);
                    INSTR_TIME_SET_CURRENT(phaseEnd);
                    INSTR_TIME_SUBTRACT(phaseEnd, phaseStart);
                    elog(INFO, "TIMING IndexReshuffledData: %.3f ms", INSTR_TIME_GET_MILLISEC(phaseEnd));
                }
            }
            if (multiPhaseExecutor->tableCreated)
            {
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

            /* The reshuffle work above (whichever branch: cache-hit or full
             * rebuild) is done -- if the same-tile branch was dispatched
             * asynchronously earlier, its query has been running the whole
             * time; fetch its (by now likely-finished, or nearly so) result
             * and build the SelfTilingScan task from that instead of the
             * scan running a second time. */
            if (sameTileDispatchedAsync)
            {
                instr_time fetchStart, fetchEnd;
                INSTR_TIME_SET_CURRENT(fetchStart);
                char *sameTileResultTable = FetchAsyncSameTileResult(sameTileQueryTextForFetch, distPlan->query);
                INSTR_TIME_SET_CURRENT(fetchEnd);
                INSTR_TIME_SUBTRACT(fetchEnd, fetchStart);
                elog(INFO, "TIMING FetchAsyncSameTileResult (remaining same-tile wait + fetch): %.3f ms",
                    INSTR_TIME_GET_MILLISEC(fetchEnd));
                ExecutorTask *sameTileTask = (ExecutorTask *) palloc0(sizeof(ExecutorTask));
                sameTileTask->taskType = SelfTilingScan;
                sameTileTask->taskQuery = makeStringInfo();
                appendStringInfo(sameTileTask->taskQuery, "SELECT * FROM %s", sameTileResultTable);
                multiPhaseExecutor->tasks = lappend(multiPhaseExecutor->tasks, sameTileTask);
                sameTileDispatchedAsync = false;
            }
        }
        else if (task->type == Colocation)
        {
            /* Self Tiling Scan -- dispatched asynchronously instead of built
             * immediately when dmdb.parallel_same_tile_scan is on and this
             * query also needs a NonColocation (reshuffle) strategy; see the
             * comment above the loop. `explain` never dispatches (it must
             * stay side-effect-free, and doesn't execute anything anyway). */
            if (!explain && parallelSameTileOn &&
                StrategiesInclude(distPlan->strategies, NonColocation))
            {
                instr_time dispatchStart, dispatchEnd;
                sameTileQueryTextForFetch = BuildSelfTilingScanQueryText(task, distPlan->org_query_string,
                                                                          sameTileDistance, distPlan->query,
                                                                          explain);
                INSTR_TIME_SET_CURRENT(dispatchStart);
                DispatchSameTileQueryAsync(sameTileQueryTextForFetch);
                INSTR_TIME_SET_CURRENT(dispatchEnd);
                INSTR_TIME_SUBTRACT(dispatchEnd, dispatchStart);
                elog(INFO, "TIMING DispatchSameTileQueryAsync (send only): %.3f ms",
                    INSTR_TIME_GET_MILLISEC(dispatchEnd));
                sameTileDispatchedAsync = true;
            }
            else
            {
                ConstructSelfTilingScanQuery(task, distPlan->org_query_string,
                                             multiPhaseExecutor, sameTileDistance, distPlan->query, explain);
            }
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
 *
 * Under EXPLAIN (explain=true), this used to run the full drop/recreate/
 * distribute/colocate_shards() sequence unconditionally -- real DDL and
 * cross-node shard placement moves, on every single call, even just to
 * describe a plan. If citusRteNode->reshuffledTable already exists as a
 * genuine Citus-distributed table (i.e. some earlier call, EXPLAIN or real,
 * already built it), EXPLAIN now reuses it as-is instead of paying that
 * cost again -- reproduced directly: an EXPLAIN on a self-join distance
 * query took 300+ ms (and, under lock contention from a concurrent real
 * run, multiple minutes) purely from this rebuild, for a plan that never
 * even looks at the reshuffled table's actual row contents.
 */
extern bool
ColocateRte(STMultirelation *base, Rte *other, bool explain)
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

        if (explain)
        {
            Oid existingOid = get_relname_relid(citusRteNode->reshuffledTable,
                                                get_namespace_oid(Var_Schema, false));
            if (OidIsValid(existingOid) && LookupCitusTableCacheEntry(existingOid) != NULL)
                return true;
        }

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
 *
 * See ColocateRte's comment above for why `explain` short-circuits this
 * into reusing an already-built reshuffled table instead of rebuilding it.
 */
extern bool
createReshuffledTable(STMultirelation *base, STMultirelation *other, bool explain)
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

    if (explain)
    {
        Oid existingOid = get_relname_relid(other->catalogTableInfo.reshuffledTable,
                                            get_namespace_oid(Var_Schema, false));
        if (OidIsValid(existingOid) && LookupCitusTableCacheEntry(existingOid) != NULL)
            return true;
    }

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
ConstructSelfTilingScanQuery(PlanTask *plan, char * query_string, MultiPhaseExecutor *multiPhaseExecutor,
                             float distance, Query *parsedQuery, bool explain)
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
    appendStringInfo(task->taskQuery, "%s", BuildSelfTilingScanQueryText(plan, query_string, distance,
                                                                         parsedQuery, explain));
    multiPhaseExecutor->tasks = lappend(multiPhaseExecutor->tasks, task);
}

/*
 * BuildSelfTilingScanQueryText builds just the SQL text ConstructSelfTilingScanQuery
 * would use for its task, without constructing the ExecutorTask itself -- shared
 * with the async-dispatch path below (dmdb.parallel_same_tile_scan), which needs
 * the query text alone to hand to dblink_send_query before the rest of the plan
 * (numCores/catalog_filtered) is relevant, since that path never builds a normal
 * ExecutorTask for this branch at all.
 */
static char *
BuildSelfTilingScanQueryText(PlanTask *plan, char *query_string, float distance, Query *parsedQuery,
                             bool explain)
{
    /* dmdb.use_side_prefilter_materialization: see BuildSidePrefilterQueryText's
     * comment. Applied first, on the original query text -- the tile_key/
     * bbox-proxy injection below operates on whatever this returns (itself
     * unchanged when the GUC is off or the query's shape doesn't qualify),
     * so it doesn't need to know anything about this restructuring. */
    query_string = BuildSidePrefilterQueryText(plan, query_string, parsedQuery, explain);

    /* query_string is already lowercased by RunQueryExecutor before any
     * task gets built, so a plain substring search against lowercased
     * alias names is enough here -- no need for AST-level access. If the
     * query's own WHERE clause already has this exact tile_key equality
     * (a user can write either "tbl1.tile_key = tbl2.tile_key" or the
     * operands reversed), injecting it again produced a harmless-but-
     * confusing "tile_key = tile_key AND tile_key = tile_key" in every
     * generated task's query text -- same condition twice changes nothing
     * about the result, but reads as if the query ran twice. */
    char *forwardEquality = psprintf("%s.%s = %s.%s", plan->tbl1->alias->aliasname,
                                     Var_Catalog_Tile_Key, plan->tbl2->alias->aliasname,
                                     Var_Catalog_Tile_Key);
    char *reverseEquality = psprintf("%s.%s = %s.%s", plan->tbl2->alias->aliasname,
                                     Var_Catalog_Tile_Key, plan->tbl1->alias->aliasname,
                                     Var_Catalog_Tile_Key);
    bool tileKeyAlreadyPresent = (strstr(query_string, forwardEquality) != NULL ||
                                  strstr(query_string, reverseEquality) != NULL);

    /* dmdb.use_bbox_proxy_filter: see BuildBboxProxyCondition's comment. */
    char *bboxCondition = BuildBboxProxyCondition(plan, distance);
    bool bboxAlreadyPresent = bboxCondition != NULL && strstr(query_string, bboxCondition) != NULL;

    if (tileKeyAlreadyPresent && (bboxCondition == NULL || bboxAlreadyPresent))
        return query_string;

    StringInfo key = makeStringInfo();
    appendStringInfoString(key, "WHERE ");
    if (!tileKeyAlreadyPresent)
        appendStringInfo(key, "%s.%s = %s.%s AND ",
                         plan->tbl1->alias->aliasname, Var_Catalog_Tile_Key,
                         plan->tbl2->alias->aliasname, Var_Catalog_Tile_Key);
    if (bboxCondition != NULL && !bboxAlreadyPresent)
        appendStringInfo(key, "%s AND ", bboxCondition);

    /* A plain replaceWord(query_string, "where", ...) here would splice into
     * the FIRST "where" found anywhere in the text -- fine as long as
     * query_string has no other "where" ahead of its own top-level one, but
     * BuildSidePrefilterQueryText (above) can now prepend a WITH clause
     * whose CTE bodies have their own "where exists (...)" clauses, which
     * always sort earlier in the text. Locating the TOP-LEVEL "where" (via
     * FindTopLevelKeywordToken, paren-depth aware -- a CTE body is always
     * wrapped in parens) and splicing there instead avoids injecting into
     * the wrong, nested WHERE clause regardless of whether the
     * prefilter rewrite ran. */
    char *loweredForSplice = toLower(query_string);
    char *topLevelWhere = FindTopLevelKeywordToken(loweredForSplice, "where");
    if (topLevelWhere == NULL)
        return query_string;
    size_t whereOffset = topLevelWhere - loweredForSplice;

    StringInfo result = makeStringInfo();
    appendBinaryStringInfo(result, query_string, whereOffset);
    appendStringInfo(result, "%s", key->data);
    appendStringInfo(result, "%s", query_string + whereOffset + strlen("where"));
    return result->data;
}

/*
 * ColumnExistsOnTable is a small pg_attribute lookup used by
 * BuildBboxProxyCondition to check for a companion bbox column before
 * relying on it -- returns false (rather than erroring) for a column that
 * doesn't exist, since the bbox proxy is opt-in and only some tables carry
 * the companion column.
 */
static bool
ColumnExistsOnTable(Oid relationId, const char *columnName)
{
    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    StringInfo query = makeStringInfo();
    appendStringInfo(query,
        "SELECT 1 FROM pg_attribute WHERE attrelid = %u AND attname = %s AND NOT attisdropped",
        relationId, quote_literal_cstr(columnName));
    spi_result = SPI_execute(query->data, true, 1);

    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
        elog(ERROR, "Could not disconnect from database using SPI");

    return found;
}

/*
 * BuildBboxProxyCondition (dmdb.use_bbox_proxy_filter, off by default):
 * the same-tile scan's own GIST index on the spatiotemporal join column
 * (e.g. "trip") has to detoast/decompress the actual stored trajectory to
 * evaluate "trip && expandspace(...)", even for candidates the index
 * ultimately rejects -- trajectories in this dataset average ~33KB
 * compressed (up to ~140KB), so a probe that finds almost nothing still
 * pays a real decompression cost (measured directly via EXPLAIN
 * (ANALYZE, BUFFERS): one shard's inner Index Scan touched 145,115 buffer
 * pages, ~1,116 per outer-loop probe, to find a final ~57 candidates
 * across 130 probes -- 1048 of the query's 1053ms). A companion
 * "<col>_bbox stbox" column (small, fixed-size, never toasted) with its
 * own GIST index does the same candidate-finding job without ever
 * touching the trajectory itself; eDwithin (already present in the
 * query's own WHERE clause) still does the real, correctness-bearing
 * check on the few survivors. Verified on the same shard/query: buffer
 * hits 145,115 -> 43,023 (3.4x fewer), 1053ms -> 625ms (1.7x faster).
 *
 * Returns NULL (no condition to add) if the GUC is off, distance is the
 * -1 "not a distance predicate" sentinel, tbl1 has no distCol, or the
 * "<distCol>_bbox" companion column doesn't exist on tbl1's table --
 * conservative by design, since this is opt-in and only wired up for
 * tables that actually carry the companion column (currently just
 * trips_sf05_6t, added for end-to-end validation).
 */
static char *
BuildBboxProxyCondition(PlanTask *plan, float distance)
{
    const char *bboxProxyFlag = GetConfigOption("dmdb.use_bbox_proxy_filter", true, false);
    if (bboxProxyFlag == NULL || strcmp(bboxProxyFlag, "on") != 0)
        return NULL;

    if (distance < 0)
        return NULL;

    char *distCol = plan->tbl1->catalogTableInfo.distCol;
    if (distCol == NULL)
        return NULL;

    char *bboxCol = psprintf("%s_bbox", distCol);
    if (!ColumnExistsOnTable(plan->tbl1->catalogTableInfo.table_oid, bboxCol))
        return NULL;

    return psprintf("%s.%s && expandspace(%s.%s, %f)",
                    plan->tbl2->alias->aliasname, bboxCol,
                    plan->tbl1->alias->aliasname, bboxCol, distance);
}

/*
 * ColumnSqlTypeName returns columnName's own SQL type name on relationId
 * (e.g. "tgeompoint", "geometry"), or NULL if the column doesn't exist.
 * Used by InjectStboxIntersectionPrefilter's tgeompoint-vs-geometry check
 * instead of DistributedColumnType: that lookup only covers a table
 * registered in pg_dist_spatiotemporal_tables (the tiled/distributed side),
 * so it can't tell a plain PostGIS geometry column on an ordinary Citus
 * reference table apart from any other DIFFTYPE case -- checking the
 * column's actual declared type instead works for either side regardless
 * of registration.
 */
static char *
ColumnSqlTypeName(Oid relationId, const char *columnName)
{
    /* Saved so the value can be copied back into the caller's own context
     * below, before SPI_finish() tears down the context SPI_connect()
     * switched CurrentMemoryContext to -- SPI_getvalue's return value lives
     * in that context and doesn't survive SPI_finish(); without copying it
     * out first, the caller's second ColumnSqlTypeName call (resolving the
     * *other* side of the same conjunct) can reuse and overwrite that same
     * freed memory before this value is ever read, silently corrupting it
     * to the second call's own result (reproduced: t.Trip's type came back
     * as "geometry(Point,3857)", p.Geom's own type, once both calls'
     * results were compared side by side in the same log line). */
    MemoryContext callerContext = CurrentMemoryContext;

    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    StringInfo query = makeStringInfo();
    appendStringInfo(query,
        "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
        "WHERE a.attrelid = %u AND a.attname = %s AND NOT a.attisdropped",
        relationId, quote_literal_cstr(columnName));
    spi_result = SPI_execute(query->data, true, 1);

    char *typeName = NULL;
    if (spi_result == SPI_OK_SELECT && SPI_processed > 0)
    {
        char *spiValue = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        MemoryContext spiContext = MemoryContextSwitchTo(callerContext);
        typeName = pstrdup(spiValue);
        MemoryContextSwitchTo(spiContext);
    }

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
        elog(ERROR, "Could not disconnect from database using SPI");

    return typeName;
}

/*
 * HasStboxExpressionIndex checks pg_indexes for a GIST index on relationId
 * whose definition mentions "stbox(<columnName>)" -- used by
 * InjectStboxIntersectionPrefilter to only inject the proxy condition when
 * there's actually an index for Postgres to use it with. Text-matches the
 * index definition (via pg_get_indexdef) rather than parsing it properly,
 * matching this file's existing convention (ColumnExistsOnTable etc.) of
 * simple catalog lookups over deeper introspection.
 */
static bool
HasStboxExpressionIndex(Oid relationId, const char *columnName)
{
    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    char *needle = psprintf("stbox(%s)", columnName);
    StringInfo query = makeStringInfo();
    appendStringInfo(query,
        "SELECT 1 FROM pg_index i WHERE i.indrelid = %u "
        "AND pg_get_indexdef(i.indexrelid) ILIKE %s",
        relationId, quote_literal_cstr(psprintf("%%%s%%", needle)));
    spi_result = SPI_execute(query->data, true, 1);

    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
        elog(ERROR, "Could not disconnect from database using SPI");

    return found;
}

/*
 * HasTrajectoryExpressionIndex is HasStboxExpressionIndex's counterpart for
 * a GIST index on trajectory(<columnName>) -- used by
 * InjectStboxIntersectionPrefilter's tgeompoint-vs-geometry case (see its
 * own comment) to only inject "trajectory(tgeompointCol) && geomCol" when
 * there's actually an index to satisfy it with.
 */
static bool
HasTrajectoryExpressionIndex(Oid relationId, const char *columnName)
{
    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    char *needle = psprintf("trajectory(%s)", columnName);
    StringInfo query = makeStringInfo();
    appendStringInfo(query,
        "SELECT 1 FROM pg_index i WHERE i.indrelid = %u "
        "AND pg_get_indexdef(i.indexrelid) ILIKE %s",
        relationId, quote_literal_cstr(psprintf("%%%s%%", needle)));
    spi_result = SPI_execute(query->data, true, 1);

    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
        elog(ERROR, "Could not disconnect from database using SPI");

    return found;
}

/*
 * InjectStboxIntersectionPrefilter (dmdb.use_stbox_expr_prefilter, off by
 * default): eIntersects(tgeompoint, geometry) already gets an automatic
 * "trip && stbox(geom)" index condition from Postgres' own support-function
 * machinery, using the tgeompoint column's own GIST index -- but that index
 * has to detoast/decompress the actual stored trajectory during its own
 * internal comparisons even for a pure bbox check (trajectories here
 * average ~33KB compressed, up to ~140KB), the same class of cost this
 * session's other bbox-proxy work found and fixed elsewhere. Rather than a
 * materialized companion column, this adds an explicit
 * "stbox(<col1>) && stbox(<col2>)" conjunct that Postgres can instead
 * satisfy with a GIST *expression* index on stbox(<col1>) -- no extra
 * column on the base table at all, just an index whose keys are computed at
 * index-build time from a light SELECT, not re-derived from the raw
 * (compressed, toasted) column on every probe. Verified on a distributed
 * spatiotemporal table joined against a reference table on an eIntersects
 * predicate (single-shard EXPLAIN ANALYZE): 1,578,849 buffer hits /
 * 11.2s -> 858,724 buffer hits (via a materialized stbox column, for
 * reference) / 8,332,586 buffer hits and 8.3s via this exact expression-
 * index approach -- ~1.4x faster than the current index either way; the
 * expression index is a little more expensive than a materialized column
 * (GIST for these types is lossy, so the heap recheck still needs the raw
 * column), but needs no schema change beyond the index itself.
 *
 * When one argument is a tgeompoint column and the other is a plain
 * geometry column (e.g. eIntersects(t.Trip, p.Geom)), "trajectory(t.Trip)
 * && p.Geom" is used instead of "stbox(t.Trip) && stbox(p.Geom)": the
 * geometry side already has its own ordinary (non-expression) GIST index
 * that a geometry-vs-geometry && can use directly, and PostGIS' own
 * geometry-vs-geometry && is not lossy the way MobilityDB's stbox
 * comparison is, so the heap recheck this still requires is cheaper -- both
 * conditions are still added purely as bbox proxies alongside the real
 * eIntersects call, same as the stbox case. Only applies given a matching
 * GIST expression index on trajectory(<tgeompointCol>)
 * (HasTrajectoryExpressionIndex); the plain stbox(...) && stbox(...) form
 * above is used whenever this doesn't apply (both arguments the same type,
 * e.g. a tgeompoint-vs-tgeompoint self join, or no matching index).
 *
 * Only ever ADDS a redundant-but-correct conjunct alongside the original
 * eIntersects call, never replaces it -- eIntersects remains the real,
 * correctness-bearing check. Purely text-based, like this file's other
 * query-shape rewrites: splits the WHERE clause into top-level conjuncts
 * (SplitTopLevelConjuncts) and, for any conjunct that's exactly a bare
 * "eintersects(arg1, arg2)" call, resolves arg1/arg2 (only handles the
 * simple "alias.column" form -- bails out on anything more complex) via
 * parsedQuery's own range table to find each argument's underlying table,
 * and only injects "stbox(arg1) && stbox(arg2)" if at least one side
 * actually has a matching stbox(...) GIST expression index
 * (HasStboxExpressionIndex) -- conservative by design, consistent with
 * this file's other opt-in rewrites: no index, no injection, since the
 * bare function call would just be extra unindexed work otherwise.
 */
static char *
InjectStboxIntersectionPrefilter(char *query_string, Query *parsedQuery, bool explain)
{
    /* Skipped under EXPLAIN, same reasoning as BuildSidePrefilterQueryText's
     * own explain skip: EXPLAIN just needs a representative, correct task
     * query to display, not the fastest one, and this session already hit
     * one real duplication bug from a rewritten same-tile task query
     * interacting badly with ExplainPlanStrategies -- skipping any of
     * these opt-in rewrites under EXPLAIN sidesteps that whole class of
     * risk without touching real query execution at all. */
    if (explain)
        return query_string;

    const char *flag = GetConfigOption("dmdb.use_stbox_expr_prefilter", true, false);
    if (flag == NULL || strcmp(flag, "on") != 0)
        return query_string;

    if (parsedQuery == NULL || query_string == NULL)
        return query_string;

    char *lowered = toLower(query_string);
    char *whereToken = FindKeywordToken(lowered, "where");
    if (whereToken == NULL)
        return query_string;
    size_t whereBodyOffset = (whereToken - lowered) + strlen("where");

    char *afterWhere = lowered + whereBodyOffset;
    char *whereEnd = NULL;
    const char *trailingKeywords[] = {"order by", "group by", "limit", NULL};
    for (int i = 0; trailingKeywords[i] != NULL; i++)
    {
        char *found = FindTopLevelKeywordToken(afterWhere, trailingKeywords[i]);
        if (found != NULL && (whereEnd == NULL || found < whereEnd))
            whereEnd = found;
    }
    size_t whereBodyLen = (whereEnd != NULL) ? (size_t) (whereEnd - afterWhere) : strlen(afterWhere);

    char *whereBodyOriginal = query_string + whereBodyOffset;
    char *whereText = TrimmedSubstring(whereBodyOriginal, whereBodyOriginal + whereBodyLen);

    List *conjuncts = SplitTopLevelConjuncts(whereText);
    List *injectedConditions = NIL;

    ListCell *conjunctCell;
    foreach(conjunctCell, conjuncts)
    {
        char *conjunct = (char *) lfirst(conjunctCell);
        char *conjunctLower = toLower(conjunct);

        char *call = FindIdentifierToken(conjunctLower, "eintersects");
        if (call == NULL || call != conjunctLower)
            continue; /* not a bare "eintersects(...)" conjunct -- skip */

        char *openParen = strchr(conjunct, '(');
        if (openParen == NULL)
            continue;
        size_t conjunctLen = strlen(conjunct);
        if (conjunct[conjunctLen - 1] != ')')
            continue; /* something trails the call (a cast, another AND...) -- too complex, skip */

        char *argsText = TrimmedSubstring(openParen + 1, conjunct + conjunctLen - 1);
        List *args = SplitTopLevelCommas(argsText);
        if (list_length(args) != 2)
            continue;

        char *arg1 = (char *) linitial(args);
        char *arg2 = (char *) lsecond(args);

        char *dot1 = strchr(arg1, '.');
        char *dot2 = strchr(arg2, '.');
        if (dot1 == NULL || dot2 == NULL)
            continue; /* not plain "alias.column" -- too complex, skip */

        char *alias1 = TrimmedSubstring(arg1, dot1);
        char *col1 = TrimmedSubstring(dot1 + 1, arg1 + strlen(arg1));
        char *alias2 = TrimmedSubstring(arg2, dot2);
        char *col2 = TrimmedSubstring(dot2 + 1, arg2 + strlen(arg2));

        Oid relid1 = InvalidOid, relid2 = InvalidOid;
        ListCell *rteCell;
        foreach(rteCell, parsedQuery->rtable)
        {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);
            if (rte->rtekind != RTE_RELATION)
                continue;
            if (strcmp(rte->eref->aliasname, alias1) == 0)
                relid1 = rte->relid;
            if (strcmp(rte->eref->aliasname, alias2) == 0)
                relid2 = rte->relid;
        }
        if (!OidIsValid(relid1) || !OidIsValid(relid2))
            continue;

        char *sqlType1 = ColumnSqlTypeName(relid1, col1);
        char *sqlType2 = ColumnSqlTypeName(relid2, col2);
        bool isTgeompoint1 = sqlType1 != NULL && strcmp(sqlType1, "tgeompoint") == 0;
        bool isTgeompoint2 = sqlType2 != NULL && strcmp(sqlType2, "tgeompoint") == 0;
        bool isGeometry1 = sqlType1 != NULL && strncmp(sqlType1, "geometry", strlen("geometry")) == 0;
        bool isGeometry2 = sqlType2 != NULL && strncmp(sqlType2, "geometry", strlen("geometry")) == 0;

        char *condition = NULL;
        if (isTgeompoint1 && isGeometry2 && HasTrajectoryExpressionIndex(relid1, col1))
            condition = psprintf("trajectory(%s) && %s", arg1, arg2);
        else if (isTgeompoint2 && isGeometry1 && HasTrajectoryExpressionIndex(relid2, col2))
            condition = psprintf("trajectory(%s) && %s", arg2, arg1);
        else if (HasStboxExpressionIndex(relid1, col1) || HasStboxExpressionIndex(relid2, col2))
            condition = psprintf("stbox(%s) && stbox(%s)", arg1, arg2);
        else
            continue;

        if (strstr(query_string, condition) == NULL)
            injectedConditions = lappend(injectedConditions, condition);
    }

    if (injectedConditions == NIL)
        return query_string;

    StringInfo prefix = makeStringInfo();
    appendStringInfoString(prefix, "where ");
    ListCell *condCell;
    foreach(condCell, injectedConditions)
        appendStringInfo(prefix, "%s and ", (char *) lfirst(condCell));

    char *loweredForSplice = toLower(query_string);
    char *topLevelWhere = FindTopLevelKeywordToken(loweredForSplice, "where");
    if (topLevelWhere == NULL)
        return query_string;
    size_t spliceOffset = topLevelWhere - loweredForSplice;

    StringInfo result = makeStringInfo();
    appendBinaryStringInfo(result, query_string, spliceOffset);
    appendStringInfo(result, "%s", prefix->data);
    appendStringInfo(result, "%s", query_string + spliceOffset + strlen("where"));
    return result->data;
}

/*
 * BuildSidePrefilterQueryText (dmdb.use_side_prefilter_materialization, off
 * by default, revert with SET ... = off): the same-tile scan's heavy
 * per-pair operation (eDwithin, eIntersects, or any other registered
 * distance/intersection predicate -- this function has no idea which one
 * it is, and doesn't need to) runs once per candidate pair, but only ONE
 * side of the self-join gets pre-filtered by whatever reference-table join
 * the query itself uses to restrict which rows matter (e.g.
 * "t1.vehicleid = v1.vehicleid AND v1.vehicletype = 'truck'") -- the join
 * order Postgres picks applies that filter to shrink the OUTER loop's row
 * count, but the INNER side's candidate search still scans the WHOLE base
 * table, paying the full spatial/distance cost for rows the reference-table
 * filter would have excluded anyway (verified: on trips_sf05_6t, only 23 of
 * 447 vehicles are trucks, but the inner index scan considered all 2339
 * rows of the tile, not just the ~130 truck-owned ones).
 *
 * Materializing each self-join side's OWN reference-table-filtered row set
 * as a CTE first, then self-joining those two (much smaller) sets, makes
 * BOTH sides benefit: verified on trips_sf05_6t (single shard, 2339 rows),
 * 778ms -> 74.5ms (10.4x faster), buffer hits ~51,879 -> ~3,600 (14x
 * fewer). Generic by construction: this only ever looks at the self-join's
 * own two aliases plus whatever OTHER reference tables the query joins them
 * to -- it never inspects the cross-side predicate itself, so it applies
 * equally to any heavy per-pair operation, not just eDwithin.
 *
 * The original WHERE clause is left completely untouched -- this only adds
 * a WITH prefix and repoints the self-join's own FROM-clause table
 * references at the two new CTEs by name; the reference-table joins/filters
 * the query already had keep running exactly as before (now over the much
 * smaller CTE output instead of the full base table), so there's no risk
 * of dropping a condition the original SELECT list's projection needs.
 * Verified this doesn't cost anything extra: the "redundant" reference-
 * table filter still evaluated in the outer query is a cheap indexed
 * lookup against a handful of already-pre-filtered rows.
 *
 * Detection is deliberately conservative -- pattern-matches the exact shape
 * this rewrite is provably safe for, and returns query_string unchanged (no
 * error, no partial rewrite) for anything else:
 *   - every WHERE-clause conjunct must, by itself, reference only (a) both
 *     self-join aliases (kept as-is, e.g. tile_key=/vehicleid</the cross-
 *     side predicate itself), (b) one self-join alias plus at most one
 *     OTHER (reference) table alias -- a candidate for that side's
 *     pre-filter, or (c) only one other alias alone (e.g.
 *     "v1.vehicletype = 'truck'"), resolved to whichever side that alias
 *     was already linked to via (b);
 *   - each self-join side may be linked to at most ONE other-table alias
 *     this way (two different reference tables on one side isn't handled);
 *   - a conjunct matching none of the above (e.g. mentioning two different
 *     other-table aliases at once, or an alias this function can't place)
 *     aborts the whole rewrite for this query.
 *
 * Extension-local only: this is pure text construction inside
 * DistributedMobilityDB's own query-building code, gated by its own GUC --
 * it doesn't touch Citus/PostgreSQL planning, catalogs, or behavior for any
 * query this extension isn't itself constructing. Deliberately avoids
 * Postgres' deparse machinery (deparse_expression/deparse_context_for_*)
 * for extracting conjunct text -- an earlier feature in this codebase
 * (RewriteReplicatedAggregateQuery, distributed_mobilitydb_planner.c)
 * segfaulted the backend calling into that machinery from this same
 * nested position inside our own already-executing planner_hook; this
 * works with the query's own source text instead (SplitTopLevelConjuncts),
 * using the parsed Query only for safe, read-only lookups (range table
 * aliases/relids), never expression deparsing.
 */
static char *
BuildSidePrefilterQueryText(PlanTask *plan, char *query_string, Query *parsedQuery, bool explain)
{
    /* Skipped entirely under EXPLAIN: ExplainOneTask's own local-EXPLAIN
     * dispatch (ExplainOnHostingWorker) renders a WITH-CTE-wrapped same-
     * tile task query correctly in isolation (confirmed directly), but the
     * full "distributed_mobilitydb_planner_hook -> ExplainPlanStrategies"
     * path duplicates the entire "Distributed Spatiotemporal Planner"
     * section when the same-tile task text has this shape (reproduced:
     * Self Tiling Scan section appears twice, identical shard/Task Count
     * both times -- looks like one render getting emitted twice rather
     * than two distinct tasks, root cause not yet isolated). EXPLAIN's own
     * purpose is just to show a *representative* task query, not the
     * fastest one, so skipping this rewrite there -- falling back to the
     * plain (still fully correct, just not CTE-shrunk) same-tile query --
     * sidesteps the bug without touching real query execution at all. */
    if (explain)
        return query_string;

    const char *prefilterFlag = GetConfigOption("dmdb.use_side_prefilter_materialization", true, false);
    if (prefilterFlag == NULL || strcmp(prefilterFlag, "on") != 0)
        return query_string;

    if (parsedQuery == NULL)
        return query_string;

    char *tbl1Alias = plan->tbl1->alias->aliasname;
    char *tbl2Alias = plan->tbl2->alias->aliasname;

    char *lowered = toLower(query_string);
    char *whereToken = FindKeywordToken(lowered, "where");
    if (whereToken == NULL)
        return query_string;
    size_t whereBodyOffset = (whereToken - lowered) + strlen("where");

    char *afterWhere = lowered + whereBodyOffset;
    char *whereEnd = NULL;
    const char *trailingKeywords[] = {"order by", "group by", "limit", NULL};
    for (int i = 0; trailingKeywords[i] != NULL; i++)
    {
        char *found = FindTopLevelKeywordToken(afterWhere, trailingKeywords[i]);
        if (found != NULL && (whereEnd == NULL || found < whereEnd))
            whereEnd = found;
    }
    size_t whereBodyLen = (whereEnd != NULL) ? (size_t) (whereEnd - afterWhere) : strlen(afterWhere);

    char *whereBodyOriginal = query_string + whereBodyOffset;
    char *whereText = TrimmedSubstring(whereBodyOriginal, whereBodyOriginal + whereBodyLen);

    List *conjuncts = SplitTopLevelConjuncts(whereText);
    if (list_length(conjuncts) < 2)
        return query_string;

    /* Enumerate "other" (non-self-join) relation aliases from the parsed
     * range table -- a plain, safe AST read (no deparsing/replanning). */
    List *otherAliases = NIL;
    ListCell *rteCell;
    foreach(rteCell, parsedQuery->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);
        if (rte->rtekind != RTE_RELATION)
            continue;
        char *aliasName = rte->eref->aliasname;
        if (strcmp(aliasName, tbl1Alias) == 0 || strcmp(aliasName, tbl2Alias) == 0)
            continue;
        otherAliases = lappend(otherAliases, aliasName);
    }
    if (otherAliases == NIL)
        return query_string;

    char *tbl1AuxAlias = NULL;
    char *tbl2AuxAlias = NULL;
    List *tbl1SideConjuncts = NIL;
    List *tbl2SideConjuncts = NIL;
    List *unresolvedConjuncts = NIL;

    ListCell *cCell;
    foreach(cCell, conjuncts)
    {
        char *conjunct = (char *) lfirst(cCell);
        char *conjunctLower = toLower(conjunct);
        bool mentionsTbl1 = FindIdentifierToken(conjunctLower, tbl1Alias) != NULL;
        bool mentionsTbl2 = FindIdentifierToken(conjunctLower, tbl2Alias) != NULL;

        char *mentionedAux = NULL;
        int auxMentionCount = 0;
        ListCell *aCell;
        foreach(aCell, otherAliases)
        {
            char *alias = (char *) lfirst(aCell);
            if (FindIdentifierToken(conjunctLower, alias) != NULL)
            {
                mentionedAux = alias;
                auxMentionCount++;
            }
        }
        if (auxMentionCount > 1)
            return query_string;

        if (mentionsTbl1 && mentionsTbl2)
            continue; /* cross-side (tile_key=, the spatial/distance predicate, ...) -- leave as-is */
        else if (mentionsTbl1 && mentionedAux != NULL)
        {
            if (tbl1AuxAlias != NULL && strcmp(tbl1AuxAlias, mentionedAux) != 0)
                return query_string;
            tbl1AuxAlias = mentionedAux;
            tbl1SideConjuncts = lappend(tbl1SideConjuncts, conjunct);
        }
        else if (mentionsTbl2 && mentionedAux != NULL)
        {
            if (tbl2AuxAlias != NULL && strcmp(tbl2AuxAlias, mentionedAux) != 0)
                return query_string;
            tbl2AuxAlias = mentionedAux;
            tbl2SideConjuncts = lappend(tbl2SideConjuncts, conjunct);
        }
        else if (mentionsTbl1 || mentionsTbl2)
            continue; /* one side alone, no aux table -- leave as-is */
        else if (mentionedAux != NULL)
            unresolvedConjuncts = lappend(unresolvedConjuncts, conjunct);
        else
            return query_string; /* unrecognized shape -- bail out conservatively */
    }

    foreach(cCell, unresolvedConjuncts)
    {
        char *conjunct = (char *) lfirst(cCell);
        char *conjunctLower = toLower(conjunct);
        char *mentionedAux = NULL;
        ListCell *aCell;
        foreach(aCell, otherAliases)
        {
            char *alias = (char *) lfirst(aCell);
            if (FindIdentifierToken(conjunctLower, alias) != NULL)
            {
                mentionedAux = alias;
                break;
            }
        }
        if (mentionedAux != NULL && tbl1AuxAlias != NULL && strcmp(mentionedAux, tbl1AuxAlias) == 0)
            tbl1SideConjuncts = lappend(tbl1SideConjuncts, conjunct);
        else if (mentionedAux != NULL && tbl2AuxAlias != NULL && strcmp(mentionedAux, tbl2AuxAlias) == 0)
            tbl2SideConjuncts = lappend(tbl2SideConjuncts, conjunct);
        else
            return query_string; /* aux alias never linked to either side -- bail out */
    }

    if (tbl1SideConjuncts == NIL && tbl2SideConjuncts == NIL)
        return query_string;

    StringInfo ctes = makeStringInfo();
    StringInfo rewritten = makeStringInfo();
    appendStringInfoString(rewritten, query_string);

    List *sides[2] = {tbl1SideConjuncts, tbl2SideConjuncts};
    char *sideAliases[2] = {tbl1Alias, tbl2Alias};
    char *auxAliases[2] = {tbl1AuxAlias, tbl2AuxAlias};
    Oid tableOids[2] = {plan->tbl1->catalogTableInfo.table_oid, plan->tbl2->catalogTableInfo.table_oid};

    for (int side = 0; side < 2; side++)
    {
        if (sides[side] == NIL)
            continue;

        Oid auxOid = InvalidOid;
        foreach(rteCell, parsedQuery->rtable)
        {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);
            if (rte->rtekind == RTE_RELATION && strcmp(rte->eref->aliasname, auxAliases[side]) == 0)
            {
                auxOid = rte->relid;
                break;
            }
        }
        if (!OidIsValid(auxOid))
            return query_string;

        char *baseTable = get_rel_name(tableOids[side]);
        char *auxTable = get_rel_name(auxOid);
        char *sideAlias = sideAliases[side];

        StringInfo conjunctText = makeStringInfo();
        bool first = true;
        ListCell *ccc;
        foreach(ccc, sides[side])
        {
            if (!first)
                appendStringInfoString(conjunctText, " and ");
            first = false;
            appendStringInfoString(conjunctText, (char *) lfirst(ccc));
        }
        appendStringInfo(ctes, "%s_filtered as materialized (select %s.* from %s %s "
                               "where exists (select 1 from %s %s where %s)), ",
                         sideAlias, sideAlias, baseTable, sideAlias,
                         auxTable, auxAliases[side], conjunctText->data);

        char *fromPattern = psprintf("%s %s", baseTable, sideAlias);
        char *fromReplacement = psprintf("%s_filtered %s", sideAlias, sideAlias);
        char *afterFix = replaceWord(rewritten->data, fromPattern, fromReplacement);
        resetStringInfo(rewritten);
        appendStringInfoString(rewritten, afterFix);
    }

    /* Strip the trailing ", " left after the last CTE definition. */
    ctes->data[ctes->len - 2] = '\0';

    StringInfo finalQuery = makeStringInfo();
    appendStringInfo(finalQuery, "with %s %s", ctes->data, rewritten->data);
    return finalQuery->data;
}

/* StrategiesInclude returns whether type is among strategies -- local copy of
 * distributed_mobilitydb_planner.c's own (static, not shared) helper of the
 * same name; trivial enough not to warrant exporting that one instead. */
static bool
StrategiesInclude(List *strategies, StrategyType type)
{
    ListCell *cell = NULL;
    foreach(cell, strategies)
    {
        if ((StrategyType) lfirst_int(cell) == type)
            return true;
    }
    return false;
}

/*
 * DispatchSameTileQueryAsync fires queryText on a separate dblink connection
 * without waiting for it -- used so the same-tile (Colocation) branch can start
 * running concurrently with the reshuffle build the NonColocation branch is
 * about to do, instead of waiting for the reshuffle to finish first. The two
 * branches are otherwise fully independent: same-tile only ever reads the base
 * table, never the table the reshuffle is building.
 *
 * dblink connections are backend-local and keyed by name -- Var_Async_SameTile_Conn
 * is a fixed name since only one such dispatch is ever in flight per query
 * (RunQueryExecutor handles at most one Colocation+NonColocation pair). A stale
 * connection under that name from an earlier ERROR (which would skip the normal
 * disconnect) is disconnected first, best-effort, before reconnecting.
 */
static void
DispatchSameTileQueryAsync(const char *queryText)
{
    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    /* Disconnect a stale connection under this name (left behind by an
     * earlier call that errored before reaching its own disconnect) only if
     * one actually exists -- checking via dblink_get_connections() first
     * means dblink_disconnect is never called on a name that isn't open,
     * avoiding the need to catch-and-discard an expected error here (a bare
     * PG_TRY/PG_CATCH around SPI_execute doesn't unwind resource
     * ownership/snapshots the way a real subtransaction would, and produced
     * "snapshot reference leak" warnings when tried). */
    StringInfo disconnectStale = makeStringInfo();
    appendStringInfo(disconnectStale,
        "SELECT dblink_disconnect('%s') WHERE '%s' = ANY(COALESCE(dblink_get_connections(), ARRAY[]::text[]))",
        Var_Async_SameTile_Conn, Var_Async_SameTile_Conn);
    SPI_execute(disconnectStale->data, false, 0);

    char *dbName = get_database_name(MyDatabaseId);
    char *userName = GetUserNameFromId(GetUserId(), false);
    char *connInfo = psprintf("dbname=%s user=%s", dbName, userName);

    StringInfo connectQuery = makeStringInfo();
    appendStringInfo(connectQuery, "SELECT dblink_connect('%s', %s)",
                     Var_Async_SameTile_Conn, quote_literal_cstr(connInfo));
    spi_result = SPI_execute(connectQuery->data, false, 1);
    if (spi_result != SPI_OK_SELECT)
        elog(WARNING, "DispatchSameTileQueryAsync: dblink_connect failed (code %d)", spi_result);

    /* Leads with Var_Same_Tile_Dispatch_Marker so the planner_hook on the
     * receiving dblink connection recognizes this as an already-resolved
     * task query and doesn't re-plan it from scratch -- see that marker's
     * comment (distributed_mobilitydb_planner.h) for the concrete failure
     * this avoids. */
    char *markedQueryText = psprintf("%s %s", Var_Same_Tile_Dispatch_Marker, queryText);
    StringInfo sendQuery = makeStringInfo();
    appendStringInfo(sendQuery, "SELECT dblink_send_query('%s', %s)",
                     Var_Async_SameTile_Conn, quote_literal_cstr(markedQueryText));
    spi_result = SPI_execute(sendQuery->data, false, 1);
    if (spi_result != SPI_OK_SELECT)
        elog(WARNING, "DispatchSameTileQueryAsync: dblink_send_query failed (code %d)", spi_result);

    SPI_finish();
}

/*
 * FetchAsyncSameTileResult blocks until the query DispatchSameTileQueryAsync
 * sent has finished, materializes its result into a fresh temp table, and
 * returns that table's name -- the caller (RunQueryExecutor) uses it as the
 * SelfTilingScan task's whole query text, selecting everything from that
 * temp table by name, so the already-computed answer is read back rather
 * than the same-tile scan running a second time as part of the final UNION.
 *
 * dblink_get_result needs the result's column names/types spelled out
 * explicitly -- there's no way to ask it to infer them. An earlier version
 * of this function derived them by locally selecting from the same query
 * text with a zero-row limit, to resolve a real TupleDesc -- measured
 * directly to be a genuine correctness/performance
 * problem, not just a naive inefficiency: this extension's own planner
 * hook intercepts that probe query too (it still matches the self-join
 * pattern the hook looks for), and doesn't know to treat an outer LIMIT 0
 * as license to skip real work, so the probe silently re-ran the full
 * same-tile computation a second time -- roughly doubling the query's
 * total cost (measured: ~23s vs. the ~13-16s a correctly-parallel version
 * should take). Deriving column defs from queryDesc's own already-parsed
 * target list instead needs no SQL execution or re-entry into the planner
 * at all: BuildSelfTilingScanQueryText only ever rewrites the WHERE
 * clause, so the same-tile query's SELECT list is always identical to the
 * original query's.
 */
static char *
FetchAsyncSameTileResult(const char *queryText, Query *queryDesc)
{
    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    StringInfo colDefs = makeStringInfo();
    ListCell *tlCell = NULL;
    bool first = true;
    foreach(tlCell, queryDesc->targetList)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(tlCell);
        if (tle->resjunk)
            continue;
        if (!first)
            appendStringInfoString(colDefs, ", ");
        first = false;
        char *colName = tle->resname != NULL ? tle->resname : psprintf("col%d", tle->resno);
        appendStringInfo(colDefs, "%s %s", quote_identifier(colName),
                         format_type_be(exprType((Node *) tle->expr)));
    }

    char *tempTableName = pstrdup(Var_Async_SameTile_Result_Table);
    StringInfo dropQuery = makeStringInfo();
    appendStringInfo(dropQuery, "DROP TABLE IF EXISTS %s", tempTableName);
    SPI_execute(dropQuery->data, false, 0);

    StringInfo fetchQuery = makeStringInfo();
    appendStringInfo(fetchQuery, "SELECT * INTO TEMP %s FROM dblink_get_result('%s') AS t(%s)",
                     tempTableName, Var_Async_SameTile_Conn, colDefs->data);
    spi_result = SPI_execute(fetchQuery->data, false, 0);
    if (spi_result != SPI_OK_SELINTO)
        elog(WARNING, "FetchAsyncSameTileResult: dblink_get_result fetch failed (code %d)", spi_result);

    StringInfo disconnectQuery = makeStringInfo();
    appendStringInfo(disconnectQuery, "SELECT dblink_disconnect('%s')", Var_Async_SameTile_Conn);
    SPI_execute(disconnectQuery->data, false, 1);

    SPI_finish();
    return tempTableName;
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
    const char *logQueryFlag = GetConfigOption("dmdb.log_final_query", true, false);
    if (logQueryFlag != NULL && strcmp(logQueryFlag, "on") == 0)
        elog(INFO, "RESHUFFLE QUERY: %s", query_string);
    const char *explainFlag = GetConfigOption("dmdb.explain_reshuffle", true, false);
    if (explainFlag != NULL && strcmp(explainFlag, "on") == 0)
    {
        StringInfo explainQuery = makeStringInfo();
        appendStringInfo(explainQuery, "EXPLAIN (ANALYZE, TIMING OFF) %s", query_string);
        spi_result = SPI_execute(explainQuery->data, false, 0);
        if (spi_result == SPI_OK_UTILITY)
        {
            for (uint64 i = 0; i < SPI_processed; i++)
                elog(INFO, "PLAN: %s", SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
            multiPhaseExecutor->dataReshuffled = true;
        }
        else
            multiPhaseExecutor->dataReshuffled = false;
    }
    else
    {
        spi_result = SPI_execute(query_string, false, 1);
        if (spi_result == SPI_OK_INSERT)
            multiPhaseExecutor->dataReshuffled = true;
        else
            multiPhaseExecutor->dataReshuffled = false;
    }
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
}

/*
 * GetLiveRowCount returns tableName's current row count. Against a
 * Citus-distributed table this is a fast parallel per-shard COUNT(*), not
 * a full data scan, so it's cheap to call on every real query even for a
 * large base table.
 */
static int64
GetLiveRowCount(const char *tableName)
{
    int spi_result;
    int64 count = -1;
    StringInfo query = makeStringInfo();

    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    appendStringInfo(query, "SELECT count(*) FROM %s", tableName);
    spi_result = SPI_execute(query->data, true, 1);
    if (spi_result == SPI_OK_SELECT && SPI_processed == 1)
    {
        /* Text-based, not SPI_getbinval+DatumGetInt64: Citus rewrites a
         * distributed count(*) as SUM(per-shard count) at the coordinator,
         * and SUM(bigint) returns numeric, not bigint -- DatumGetInt64
         * against a numeric Datum's varlena header reads garbage (caught
         * directly: an 8-byte count came back as 100413546030920).
         * SPI_getvalue goes through the actual type's output function
         * first, so it's correct regardless of which type comes back. */
        char *text = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        if (text != NULL)
            count = strtoll(text, NULL, 10);
    }

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
        elog(ERROR, "Could not disconnect from database using SPI");
    return count;
}

/*
 * ReshuffleCacheIsFresh reports whether the already-built reshuffled table
 * for (baseTableOid, cacheDistance) can be reused as-is instead of paying
 * the full drop/recreate/copy/index rebuild again (see the
 * pg_dist_spatiotemporal_reshuffle_cache catalog table's own comment for
 * why a row-count fingerprint, why distance is part of the key, and what
 * it does/doesn't catch). Requires all of: a cache row for (baseTableOid,
 * cacheDistance) whose recorded reshuffled_table still matches
 * reshuffledTableName, the base table's live row count still matching what
 * was cached, and the reshuffled table still existing as a genuine
 * Citus-distributed relation (guards against it having been dropped
 * manually since the cache row was written).
 */
static bool
ReshuffleCacheIsFresh(Oid baseTableOid, const char *baseTableName, const char *reshuffledTableName, double cacheDistance)
{
    int spi_result;
    bool fresh = false;
    int64 cachedCount = -1;
    char *cachedTable = NULL;
    StringInfo query = makeStringInfo();

    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    appendStringInfo(query,
                     "SELECT reshuffled_table, base_row_count "
                     "FROM pg_dist_spatiotemporal_reshuffle_cache "
                     "WHERE base_table_oid = %u AND distance = %.17g", baseTableOid, cacheDistance);
    spi_result = SPI_execute(query->data, true, 1);
    if (spi_result == SPI_OK_SELECT && SPI_processed == 1)
    {
        bool isnull;
        Datum tableDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            cachedTable = pstrdup(TextDatumGetCString(tableDatum));
        Datum countDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        if (!isnull)
            cachedCount = DatumGetInt64(countDatum);
    }

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
        elog(ERROR, "Could not disconnect from database using SPI");

    if (cachedTable != NULL && cachedCount >= 0 && strcmp(cachedTable, reshuffledTableName) == 0)
    {
        Oid reshuffledOid = get_relname_relid(reshuffledTableName, get_namespace_oid(Var_Schema, false));
        if (OidIsValid(reshuffledOid) && LookupCitusTableCacheEntry(reshuffledOid) != NULL)
            fresh = (GetLiveRowCount(baseTableName) == cachedCount);
    }
    return fresh;
}

/*
 * RecordReshuffleCache upserts the cache row for (baseTableOid,
 * cacheDistance) right after a real rebuild finishes, so the next real
 * query against the same unchanged base table and the same distance
 * threshold can skip the rebuild via ReshuffleCacheIsFresh above.
 */
static void
RecordReshuffleCache(Oid baseTableOid, const char *baseTableName, const char *reshuffledTableName, double cacheDistance)
{
    int spi_result;
    int64 rowCount = GetLiveRowCount(baseTableName);
    StringInfo query = makeStringInfo();

    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
        elog(ERROR, "Could not connect to database using SPI");

    appendStringInfo(query,
                     "INSERT INTO pg_dist_spatiotemporal_reshuffle_cache "
                     "(base_table_oid, distance, reshuffled_table, base_row_count, cached_at) "
                     "VALUES (%u, %.17g, %s, " INT64_FORMAT ", now()) "
                     "ON CONFLICT (base_table_oid, distance) DO UPDATE SET "
                     "reshuffled_table = EXCLUDED.reshuffled_table, "
                     "base_row_count = EXCLUDED.base_row_count, "
                     "cached_at = now()",
                     baseTableOid, cacheDistance, quote_literal_cstr(reshuffledTableName), rowCount);
    spi_result = SPI_execute(query->data, false, 1);
    if (spi_result != SPI_OK_INSERT)
        elog(WARNING, "Could not record reshuffle cache for base table oid %u", baseTableOid);

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
        elog(ERROR, "Could not disconnect from database using SPI");
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
    /* IF NOT EXISTS: under EXPLAIN, createReshuffledTable()/ColocateRte() may
     * reuse an already-built reshuffled table (see their comments) rather
     * than dropping and recreating it -- that table already has this same
     * index from whichever earlier call actually built it, so a plain
     * CREATE INDEX would fail on "relation ..._idx already exists". */
    if (reshuffledTable->RteType == STRte)
    {
        STMultirelation *stMultirelation = (STMultirelation *)reshuffledTable->rte;
        appendStringInfo(val, "CREATE INDEX IF NOT EXISTS %s_%s_idx on %s.%s USING %s(%s); ANALYZE %s.%s;",
                         stMultirelation->catalogTableInfo.reshuffledTable,
                         stMultirelation->col, Var_Schema, stMultirelation->catalogTableInfo.reshuffledTable,
                         Var_Spatiotemporal_Index, stMultirelation->col,
                         Var_Schema, stMultirelation->catalogTableInfo.reshuffledTable);
    }
    else if (reshuffledTable->RteType == CitusRte)
    {
        CitusRteNode *citusRteNode = (CitusRteNode *)reshuffledTable->rte;
        appendStringInfo(val, "CREATE INDEX IF NOT EXISTS %s_%s_idx on %s.%s USING %s(%s); ANALYZE %s.%s;",
                         citusRteNode->reshuffledTable,
                         citusRteNode->col, Var_Schema, citusRteNode->reshuffledTable,
                         Var_Spatiotemporal_Index, citusRteNode->col,
                         Var_Schema, citusRteNode->reshuffledTable);

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
 * query_string by wrapping it in an outer SELECT DISTINCT over everything
 * it projects, deduping on whatever the query actually projects. Only applied when
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
 * FindEarliestSortModifier returns a pointer to the earliest whitespace-
 * bounded occurrence of "asc", "desc", or "nulls" in chunk (chunk must
 * already be lowercased), or NULL if none of those appear -- used to find
 * where a single ORDER BY item's core expression ends and its direction/
 * nulls-ordering modifier (if any) begins.
 */
static char *
FindEarliestSortModifier(const char *chunk)
{
    char *asc = FindKeywordToken(chunk, "asc");
    char *desc = FindKeywordToken(chunk, "desc");
    char *nulls = FindKeywordToken(chunk, "nulls");
    char *best = NULL;
    if (asc != NULL && (best == NULL || asc < best))
        best = asc;
    if (desc != NULL && (best == NULL || desc < best))
        best = desc;
    if (nulls != NULL && (best == NULL || nulls < best))
        best = nulls;
    return best;
}

/*
 * BuildPositionalOrderBy rewrites orderByText (still carrying its leading
 * "order by" keyword, already-lowercased) into the equivalent
 * ordinal-position form, matching each comma-separated item against
 * parse's own parsed sortClause/targetList by position rather than by
 * name.
 *
 * This exists because the alternative -- textually stripping each item's
 * "alias." qualifier so it resolves against the outer SELECT-star wrap's
 * unqualified output columns (StripOrderByAliasQualifiers)
 * -- can't handle two ORDER BY items that happen to share the same bare
 * column name once unqualified, e.g. two aliases of the same CTE each
 * ordering by a same-named column: stripping both qualifiers produces two
 * identical, now-ambiguous column references against two identically-named
 * output columns (reproduced directly). A 1-based ordinal position is
 * unambiguous regardless of how many output columns share a name.
 *
 * Returns NULL (letting the caller fall back to StripOrderByAliasQualifiers)
 * if the item count doesn't match sortClause's, or any item turns out to
 * sort by a resjunk target entry (not part of the visible output columns,
 * so no ordinal position of it is meaningful) -- deliberately conservative,
 * matching this codebase's existing convention of bailing out to the
 * pre-existing (im)perfect behavior rather than risking a wrong rewrite.
 */
static char *
BuildPositionalOrderBy(Query *parse, const char *orderByText)
{
    const char *itemsStart = orderByText + strlen("order by");
    while (isspace((unsigned char) *itemsStart))
        itemsStart++;

    List *chunks = SplitTopLevelCommas(itemsStart);
    if (list_length(chunks) != list_length(parse->sortClause))
        return NULL;

    StringInfo result = makeStringInfo();
    appendStringInfoString(result, "order by ");

    ListCell *chunkCell = list_head(chunks);
    ListCell *sortCell;
    bool first = true;
    foreach(sortCell, parse->sortClause)
    {
        SortGroupClause *sortGroupClause = (SortGroupClause *) lfirst(sortCell);
        TargetEntry *targetEntry = get_sortgroupclause_tle(sortGroupClause, parse->targetList);
        if (targetEntry == NULL || targetEntry->resjunk)
            return NULL;

        char *chunk = (char *) lfirst(chunkCell);
        char *modifier = FindEarliestSortModifier(chunk);

        if (!first)
            appendStringInfoString(result, ", ");
        appendStringInfo(result, "%d", targetEntry->resno);
        if (modifier != NULL)
            appendStringInfo(result, " %s", modifier);

        first = false;
        chunkCell = lnext(chunks, chunkCell);
    }
    return result->data;
}

/*
 * Strips "alias." qualifiers (from parse's own FROM-clause RTEs) out of
 * orderByText. The final ORDER BY gets re-applied outside an outer
 * SELECT-star wrap whose only visible columns are unqualified, so a
 * copied-verbatim alias-qualified column reference fails with "missing
 * FROM-clause entry" otherwise. orderByText is already lowercased, so qualifiers
 * built here are too. Fallback for when BuildPositionalOrderBy can't be used
 * (item-count mismatch or a resjunk sort target).
 */
static char *
StripOrderByAliasQualifiers(char *orderByText, Query *parse)
{
    char *result = orderByText;
    ListCell *cell;
    foreach(cell, parse->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(cell);
        /*
         * A top-level FROM entry aliasing one of the query's own CTEs is
         * RTE_CTE, not RTE_RELATION -- excluding it here left its qualifier
         * unstripped, surviving into the outer SELECT-star/ORDER BY wrap
         * where it's out of scope ("missing FROM-clause entry", reproduced
         * on any query that orders
         * by a CTE-aliased column at the top level). Both kinds need the
         * same treatment: strip any top-level FROM alias's qualifier, not
         * just a real relation's.
         */
        if ((rte->rtekind != RTE_RELATION && rte->rtekind != RTE_CTE) || !rte->inFromCl)
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
 * StripTrailingOrderBy returns a copy of queryText with its own top-level
 * trailing ORDER BY clause (if any) cut off -- a bare ORDER BY is only
 * legal on the last arm of a UNION (or requires wrapping that arm in
 * parens), so combining several strategies' task queries as-is produced
 * "... order by x UNION select ... order by x", a syntax error. Uses a
 * lowercased scratch copy purely to locate the keyword case-insensitively
 * (FindTopLevelKeywordToken requires already-lowercased input); the
 * returned copy preserves queryText's original casing up to the cut point,
 * since lowering is a 1:1, length-preserving byte mapping so the same
 * offset applies to both.
 */
static char *
StripTrailingOrderBy(const char *queryText)
{
    char *result = pstrdup(queryText);
    char *lowered = toLower(pstrdup(queryText));
    char *orderByPos = FindTopLevelKeywordToken(lowered, "order by");
    if (orderByPos != NULL)
    {
        size_t offset = orderByPos - lowered;
        result[offset] = '\0';
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

    /* Loop through current plan strategies involved in this query. Each
     * task's own text is built from the original query (including its
     * trailing ORDER BY, if any, e.g. a self-join distance query needing
     * both a same-tile Colocation scan and a reshuffled-tile NonColocation
     * scan) -- StripTrailingOrderBy() drops that here since a bare ORDER BY
     * is only legal on the very last arm of a UNION (or requires wrapping
     * the arm in parens): an ORDER BY on a non-final UNION arm is a
     * syntax error, reproduced directly running a distance self-join that
     * needed both strategies. The query's real ORDER BY is re-applied once,
     * correctly, over the fully combined result further below.
     *
     * Each branch is also wrapped in parens: a task's own text can start
     * with its own leading WITH clause (a query built on top of its own
     * outer CTE), and a WITH clause is only legal introducing a whole
     * statement, or a
     * *parenthesized* arm of a UNION -- a bare, unparenthesized UNION arm
     * starting with its own WITH clause is a syntax error, reproduced
     * directly combining a Colocation and a NonColocation task for a
     * CTE-based query. A
     * standalone (non-CTE) branch or a single-strategy query (no UNION at
     * all) is unaffected either way -- wrapping a plain SELECT in parens is
     * always valid SQL, whether or not it ends up combined with anything. */
    foreach(cell, distPlan->strategies)
    {
        StrategyType strategy = (StrategyType) lfirst_int(cell);
        if (strategy == NonColocation)
        {
            if (generalScan->length > 0)
                appendStringInfo(generalScan->query_string, "%s", " UNION ");
            appendStringInfo(generalScan->query_string, "(%s)",
                             StripTrailingOrderBy(taskQuery(multiPhaseExecutor->tasks, NeighborTilingScan)));
            generalScan->length++;
        }
        else if (strategy == Colocation)
        {
            if (generalScan->length > 0)
                appendStringInfo(generalScan->query_string, "%s", " UNION ");
            appendStringInfo(generalScan->query_string, "(%s)",
                             StripTrailingOrderBy(taskQuery(multiPhaseExecutor->tasks, SelfTilingScan)));
            generalScan->length++;
        }
        else if (strategy == PredicatePushDown)
        {
            if (generalScan->length > 0)
                appendStringInfo(generalScan->query_string, "%s", " UNION ");
            appendStringInfo(generalScan->query_string, "(%s)",
                             StripTrailingOrderBy(taskQuery(multiPhaseExecutor->tasks, PushDownScan)));
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
     * wrapping below nests it inside an outer aggregate subquery -- once
     * that wrapping happens, ExtractRangeTableEntryList
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

            char *positional = BuildPositionalOrderBy(distPlan->query, orderByText);
            orderByText = positional != NULL ? positional :
                          StripOrderByAliasQualifiers(orderByText, distPlan->query);

            /* If the visible output columns include a duplicate name (e.g.
             * two unaliased projected columns that are both literally named
             * the same thing), explicitly rename "ordered_result"'s
             * own columns to guaranteed-unique synthetic names ("c1", "c2",
             * ...) instead of letting it inherit those duplicate names.
             * Even with this ORDER BY already rewritten to ordinal positions
             * (BuildPositionalOrderBy, right above) to avoid resolving by
             * name here, Citus' OWN distributed_planner() -- called on this
             * wrapped query right after QueryExecutor() returns it, to
             * build the coordinator-side merge/sort step for gathering
             * sorted results from every worker -- reconstructs its own
             * equivalent ORDER BY internally while splitting the query into
             * worker/master halves, and does so by column NAME, not
             * position (reproduced: "ORDER BY licence is ambiguous" still
             * fires even with a purely ordinal "order by 1, 2" here, since
             * that ambiguity is regenerated one level down in Citus' own
             * planner, entirely outside this function's control). Only
             * applied when a real conflict exists -- unconditionally
             * renaming every query's output columns to "c1"/"c2" would trade
             * this rare-but-real bug for a much more visible regression
             * (reproduced: a query that already explicitly aliased its own
             * output, e.g. "AS licence1, AS licence2", lost those meaningful
             * names in favor of "c1"/"c2" for every single query, not just
             * ambiguous ones). */
            List *seenNames = NIL;
            bool hasDuplicateName = false;
            int visibleColumnCount = 0;
            ListCell *targetCell;
            foreach(targetCell, distPlan->query->targetList)
            {
                TargetEntry *targetEntry = (TargetEntry *) lfirst(targetCell);
                if (targetEntry->resjunk)
                    continue;
                visibleColumnCount++;
                if (targetEntry->resname == NULL)
                    continue;
                ListCell *seenCell;
                foreach(seenCell, seenNames)
                {
                    if (strcmp((char *) lfirst(seenCell), targetEntry->resname) == 0)
                    {
                        hasDuplicateName = true;
                        break;
                    }
                }
                seenNames = lappend(seenNames, targetEntry->resname);
            }

            StringInfo wrapped = makeStringInfo();
            if (hasDuplicateName && visibleColumnCount > 0)
            {
                StringInfo columnAliases = makeStringInfo();
                for (int i = 1; i <= visibleColumnCount; i++)
                {
                    if (i > 1)
                        appendStringInfoString(columnAliases, ", ");
                    appendStringInfo(columnAliases, "c%d", i);
                }
                appendStringInfo(wrapped, "SELECT * FROM (%s) AS ordered_result(%s) %s",
                                 generalScan->query_string->data, columnAliases->data, orderByText);
            }
            else
            {
                appendStringInfo(wrapped, "SELECT * FROM (%s) AS ordered_result %s",
                                 generalScan->query_string->data, orderByText);
            }
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

