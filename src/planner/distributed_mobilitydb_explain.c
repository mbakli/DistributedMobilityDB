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

#include "planner/distributed_mobilitydb_explain.h"

#include "commands/explain.h"
#include <utils/lsyscache.h>
#include <distributed/multi_explain.h>
#include "distributed/listutils.h"
#include "distributed/distributed_planner.h"
#include "utils/helper_functions.h"
#include "utils/planner_utils.h"
#include "catalog/nodes.h"
#include "planner/planner_strategies.h"
#include "catalog/table_ops.h"
#include <tcop/tcopprot.h>
#include <executor/spi.h>
#include <utils/builtins.h>


static Node *SpatiotemporalExecutorCreateScan(CustomScan *scan);
static void SpatiotemporalPreExecutionScan(SpatiotemporalScanState *scanState);
static void SpatiotemporalExplainScan(CustomScanState *node, List *ancestors, struct ExplainState *es);
static void ExplainWorkerPlan(PlannedStmt *plannedstmt, DestReceiver *dest, ExplainState *es,
                              const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv,
                              const instr_time *planduration);
static void InitializeDistributedQueryExplain(DistributedQueryExplain *distributedQueryExplain,
                                              ExplainState *es, const char *queryString);
static void ExplainQueryType(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es);
static char * getQueryType(List *strategies);

static void ExplainQueryPlan(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es,
                                       int indent_group);
static void ExplainPlanStrategies(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es, int indent_group);
static void ExplainOneTask(ExecutorTask *task, STMultirelation *base, ExplainState *es, int indent_group);
static char * GetLocalQuery(char *query_string, Oid base, ExecTaskType taskType, int rand_tile);
static char * GetPhysicalTileQuery(char *query_string, ExecTaskType taskType, int rand_tile,
                                   TaskNode *taskNode);
static char * ExplainOnHostingWorker(char *physicalQuery, TaskNode *taskNode);

/* create custom scan method for the spatiotemporal executor */
CustomScanMethods SpatiotemporalExecutorMethod = {
        "DistributedSpatiotemporalQueryPlanner",
        SpatiotemporalExecutorCreateScan
};

/*
 * Define executor methods for the different executor types.
 */
static CustomExecMethods SpatiotemporalExecutorMethods = {
        .CustomName = "SpatiotemporalExecutorScan",
        .ExplainCustomScan = SpatiotemporalExplainScan
};

/*
 * spatiotemporal_explain is the executor hook that is called when
 * postgres wants to explain a query.
 */
extern void
distributed_mobilitydb_explain(Query *query, int cursorOptions, IntoClause *into,
                       ExplainState *es, const char *queryString, ParamListInfo params,
                       QueryEnvironment *queryEnv)
{
    DistributedSpatiotemporalQueryPlan *distPlan = (DistributedSpatiotemporalQueryPlan *)
            palloc0(sizeof(DistributedSpatiotemporalQueryPlan));
    DistributedQueryExplain *curDistributedQueryExplain = (DistributedQueryExplain *)
            palloc0(sizeof(DistributedQueryExplain));
    InitializeDistributedQueryExplain(curDistributedQueryExplain, es, queryString);
    /* Plan a copy: planning mutates the Query tree in place, and `query`
     * itself must stay pristine in case we fall back to CitusExplainOneQuery
     * below, which plans it again from scratch. */
    PlannedStmt *result = distributed_mobilitydb_planner_internal(copyObject(query),
                                                          curDistributedQueryExplain->query_string,
                                                          cursorOptions, params,
                                                          distPlan, true);

    /* If our custom planning bailed out, the query was already handled by
     * Citus/Postgres directly and distPlan was never fully populated -
     * delegate the explain output to Citus and skip our custom section. */
    if (result != NULL)
    {
        CitusExplainOneQuery(query,cursorOptions,into,es,queryString,params,queryEnv);
        return;
    }

    /* Explain using Distributed MobilityDB  */
    ExplainOpenGroup("DistributedQueryExplain", "Distributed Query", true, es);
    ExplainQueryType(distPlan,es);
    ExplainQueryParameters(distPlan, es, 2);
    ExplainQueryPlan(distPlan, es, 2);
    ExplainCloseGroup("DistributedQueryExplain", "Distributed Query", true, es);

}
/*
 * Let PostgreSQL know about the custom scan nodes.
 */
void
RegisterSpatiotemporalPlanMethods(void)
{
    RegisterCustomScanMethods(&SpatiotemporalExecutorMethod);
}

/* SpatiotemporalExecutorCreateScan is the CustomScanMethods callback that builds the scan's execution state. */
static Node *
SpatiotemporalExecutorCreateScan(CustomScan *scan)
{
    SpatiotemporalScanState *scanState = palloc0(sizeof(SpatiotemporalScanState));

    scanState->customScanState.ss.ps.type = T_CustomScanState;
    scanState->distributedSpatiotemporalPlan = GetSpatiotemporalDistributedPlan(scan);
    scanState->customScanState.methods = &SpatiotemporalExecutorMethods;
    scanState->finishedPreScan = false;
    scanState->finishedRemoteScan = false;

    return (Node *) scanState;
}

/*
 * Initialize the distributed query explain
 */
static void
InitializeDistributedQueryExplain(DistributedQueryExplain *distributedQueryExplain,
                                  ExplainState *es, const char *queryString)
{
    StringInfo tmp = makeStringInfo();
    appendStringInfo(tmp, "%s", replaceWord(toLower((char *)queryString), "explain ", ""));
    distributedQueryExplain->query_string = tmp->data;
}

/*
 * Explain the query type that can be one of the following: noncolocated, colocated, range, knn, other
 */
static void ExplainQueryType(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es)
{
    StringInfo temp = makeStringInfo();
    appendStringInfo(temp, "(Query Type: %s)", getQueryType(distPlan->strategies));
    ExplainPropertyText("Distributed Spatiotemporal Planner", temp->data, es);
}

/* getQueryType renders the combination of chosen strategies as a short human-readable label. */
static char * getQueryType(List *strategies)
{
    ListCell *cell = NULL;
    bool colocated = false;
    bool range = false;
    bool knn = false;
    foreach(cell, strategies)
    {
        StrategyType strategy = (StrategyType)lfirst_int(cell);
        if (strategy == NonColocation)
            return "Non Colocated";
        else if (strategy == Colocation)
            colocated = true;
        else if (strategy == TileScanRebalancer)
            range = true;
        else if (strategy == KNN)
            knn = true;
    }
    if (colocated && range)
        return "Colocated - Range";
    else if (range && knn)
        return "Range - Knn";
    else if (colocated)
        return "Colocated";
    else if (range)
        return "Range";
    else if (knn)
        return "Knn";
    else
        return "Other";
}


extern bool IsReshufflingRequired(List *strategies)
{
    ListCell *cell = NULL;
    foreach(cell, strategies)
    {
        StrategyType strategy = (StrategyType) lfirst_int(cell);
        if (strategy == NonColocation)
            return true;
    }
    return false;
}

/*
 * SpatiotemporalExplainScan is a custom scan explain callback function which is used to
 * print explain information
 */
void
SpatiotemporalExplainScan(CustomScanState *node, List *ancestors, struct ExplainState *es)
{
    SpatiotemporalScanState *scanState = (SpatiotemporalScanState *) node;
    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan = scanState->distributedSpatiotemporalPlan;
    ExplainOpenGroup("Distributed Query", "Distributed Query", true, es);
    ExplainPropertyText("Distributed Spatiotemporal Query Plan", "Distributed Spatiotemporal Query Plan", es);
    ExplainCloseGroup("Distributed Query", "Distributed Query", true, es);
}

static void ExplainQueryPlan(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es,
                             int indent_group)
{
    appendStringInfo(es->str, "-> Query Plan:\n");
    es->indent = indent_group;
    if (distPlan->postProcessing->coordinatorLevelOperator->dupRemOperator->active)
    {
        appendStringInfoSpaces(es->str, es->indent * indent_group);
        appendStringInfo(es->str, "Remove Duplicates:\n");
    }

    if (distPlan->postProcessing->coordinatorLevelOperator->mergeOperator->active)
    {
        appendStringInfoSpaces(es->str, es->indent * indent_group);
        appendStringInfo(es->str, "Merge:\n");
    }

    if (distPlan->postProcessing->workerLevelOperator->collectOperator->active)
    {
        appendStringInfoSpaces(es->str, es->indent * indent_group);
        appendStringInfo(es->str, "Collect:\n");
    }

    if (list_length(distPlan->strategies) > 0)
    {
        appendStringInfoSpaces(es->str, es->indent * indent_group);
        es->indent -= indent_group;
        ExplainPlanStrategies(distPlan, es, indent_group + 2);
    }
}

/*
 * ExplainPlanStrategies re-runs the executor in explain-only mode
 * (RunQueryExecutor with explain=true, so no data-modifying SPI calls
 * happen) to obtain the per-strategy tasks, then prints one representative
 * task per task type via ExplainOneTask — showing the plan for a single
 * tile stands in for all `candidates` tiles that would actually run.
 */
static void
ExplainPlanStrategies(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es, int indent_group)
{
    MultiPhaseExecutor *multiPhaseExecutor = RunQueryExecutor(distPlan, true);
    ListCell *cell = NULL;
    foreach(cell, multiPhaseExecutor->tasks)
    {
        ExecutorTask *task = (ExecutorTask *) lfirst(cell);
        appendStringInfoSpaces(es->str, es->indent * indent_group + 2);
        appendStringInfo(es->str, "-> %s:\n", DatumGetCString(GetTaskType(task)));
        appendStringInfoSpaces(es->str, es->indent * indent_group + 8);
        appendStringInfo(es->str, "Task Count: %d\n", task->catalog_filtered->candidates);
        appendStringInfoSpaces(es->str, es->indent * indent_group + 8);
        appendStringInfo(es->str, "Tasks Shown: One of %d\n", task->catalog_filtered->candidates);
        appendStringInfoSpaces(es->str, es->indent * indent_group + 10);
        appendStringInfo(es->str, "-> Task:\n");
        ExplainOneTask(task, distPlan->reshuffled_table_base, es, indent_group);
        es->indent -= 5;
    }
}
/*
 * ExplainOneTask prints the plan for a single, randomly-chosen tile of
 * `task`. Since colocate_shards() (see create_reshuffled_multirelation)
 * physically moves each reshuffled shard onto the same worker as its
 * matching-tile shard of the base table, a tile's join is ordinarily a
 * genuine single-node operation -- so we first try GetPhysicalTileQuery(),
 * which substitutes each logical table name for its concrete shard name at
 * the chosen tile, then ExplainOnHostingWorker() dispatches a real local
 * EXPLAIN of that physical query to the one worker hosting it, returning
 * Postgres' own plan with no Citus wrapper at all. If the tiles turn out not
 * to be co-located (a stale/partial reshuffle, or a straggler placement),
 * GetPhysicalTileQuery() returns NULL and we fall back to planning the
 * logical query (pinned to the tile via a tile_key filter, GetLocalQuery())
 * through Citus' distributed_planner() directly (bypassing this extension's
 * own planner_hook, since these tables are already-registered distributed
 * spatiotemporal tables and would otherwise recurse back into our own
 * planning here), which shows the real cross-node repartition instead of a
 * misleading local-only plan.
 */
static void
ExplainOneTask(ExecutorTask *task, STMultirelation *base,ExplainState *es, int indent_group)
{
    int rand_tile = GetRandTileNum(base);
    TaskNode *taskNode = GetShardHostNode(base->catalogTableInfo.table_oid, rand_tile);

    appendStringInfoSpaces(es->str, es->indent * indent_group + 12);
    appendStringInfo(es->str, "Node: host=%s ", DatumToString(taskNode->node, TEXTOID));
    appendStringInfo(es->str, "port=%d ", taskNode->port);
    appendStringInfo(es->str, "dbname=%s\n", DatumGetCString(GetDBName()));

    /*
     * Prefer the physical, tile-substituted query (real shard table names)
     * so "Query:" shows one concrete tile example instead of the logical
     * table name. Only available when every referenced table's rand_tile
     * shard is actually co-located on taskNode; GetPhysicalTileQuery
     * returns NULL otherwise (no single physical tile name to show when the
     * tables aren't co-located), in which case we fall back to the logical
     * query pinned by a tile_key filter (GetLocalQuery) for display.
     *
     * What's printed is deliberately independent of whether
     * ExplainOnHostingWorker's *live* local EXPLAIN round-trip below
     * succeeds: that dispatch can fail for reasons unrelated to
     * co-location (e.g. connection/transaction state while already
     * mid-EXPLAIN of a repartition query), and discarding a correctly
     * substituted physicalQuery just because the live probe happened to
     * fail would defeat the point of showing it at all. When the live
     * probe does fail, we still need *some* query to hand to Citus'
     * distributed_planner() for the fallback plan below -- that must be
     * the logical query (physical shard tables aren't known to the
     * planner), so GetLocalQuery's output is always computed too.
     */
    char *physicalQuery = GetPhysicalTileQuery(task->taskQuery->data, task->taskType,
                                               rand_tile, taskNode);
    char *logicalQuery = GetLocalQuery(task->taskQuery->data, base->catalogTableInfo.table_oid,
                                       task->taskType, rand_tile);
    char *displayQuery = physicalQuery != NULL ? physicalQuery : logicalQuery;

    appendStringInfoSpaces(es->str, es->indent * indent_group + 12);
    appendStringInfo(es->str, "Query: %s\n", displayQuery);
    appendStringInfoSpaces(es->str, es->indent * indent_group + 12);

    char *localPlanText = physicalQuery != NULL ?
        ExplainOnHostingWorker(physicalQuery, taskNode) : NULL;
    if (localPlanText != NULL)
    {
        es->indent += 6;
        appendStringInfoSpaces(es->str, es->indent * indent_group);
        appendStringInfo(es->str, "%s\n", localPlanText);
        es->indent -= 6;
        return;
    }

    Query *parse = ParseQueryString(logicalQuery, NULL, 0);
    PlannedStmt *plan = distributed_planner(parse, logicalQuery, 0, NULL);
    instr_time planduration;
    INSTR_TIME_SET_ZERO(planduration);
    es->indent += 6;
    DestReceiver *tupleStoreDest = CreateTuplestoreDestReceiver();
    ExplainWorkerPlan(plan, tupleStoreDest, es, logicalQuery, NULL, NULL,
                      &planduration);
    ExplainEndOutput(es);
}

/*
 * GetLocalQuery pins query_string to one representative tile by adding a
 * literal `<alias>.tile_key = rand_tile` predicate for every range table
 * entry that actually has a tile_key column, so Citus' shard pruning
 * narrows each such table down to the single matching shard instead of
 * planning across all of them.
 */
static char *
GetLocalQuery(char *query_string, Oid base, ExecTaskType taskType, int rand_tile)
{
    if (query_string == NULL)
        return NULL;

    Query *query = ParseQueryString(query_string, NULL, 0);
    List *rangeTableList = ExtractRangeTableEntryList(query);
    StringInfo tileKeyConditions = makeStringInfo();

    ListCell *rangeTableCell = NULL;
    foreach(rangeTableCell, rangeTableList)
    {
        RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
        /* Only tables tiled by this extension's own machinery (a
         * distributed spatiotemporal table, or a plain table reshuffled
         * to be colocated with one) actually have a tile_key column --
         * unconditionally pinning every range table entry (as this used
         * to) added "alias.tile_key = N" for reference tables too (e.g.
         * vehicles_ref/points_ref), which have no such column at all,
         * producing "column v.tile_key does not exist" instead of a plan. */
        if (!IsDistributedSpatiotemporalTable(rangeTableEntry->relid) &&
            !IsReshuffledTable(rangeTableEntry->relid))
            continue;
        appendStringInfo(tileKeyConditions, "%s.%s = %d AND ", rangeTableEntry->eref->aliasname,
                         Var_Catalog_Tile_Key, rand_tile);
    }

    if (tileKeyConditions->len == 0)
        return query_string;

    /*
     * Inserted right after the query's own WHERE keyword rather than
     * appended at the very end -- appending unconditionally landed these
     * AND-joined conditions after a trailing ORDER BY whenever the task
     * query had one (e.g. Q16's per-tile query), silently folding them
     * into the ORDER BY expression list instead of the WHERE clause:
     * "ORDER BY ..., l2.licence AND t1.tile_key = 5 AND ..." parses as one
     * AND-expression whose left operand is l2.licence (text), producing
     * "argument of AND must be type boolean, not type text" instead of
     * pinning the query to one tile.
     */
    StringInfo key = makeStringInfo();
    appendStringInfo(key, "WHERE %s", tileKeyConditions->data);
    return replaceWord(query_string, "where", key->data);
}

/*
 * GetPhysicalTileQuery checks that every table referenced by query_string
 * has its rand_tile shard physically co-located on taskNode; if so, it
 * substitutes each logical table name for its concrete shard-qualified name
 * (one tile example, e.g. trips_passenger_6t -> trips_passenger_6t_102008)
 * and returns the resulting query text. Returns NULL if any table's
 * matching tile lives elsewhere, signalling the caller to fall back to a
 * Citus-routed explain instead, since there's then no single physical tile
 * name that represents the whole query.
 */
static char *
GetPhysicalTileQuery(char *query_string, ExecTaskType taskType, int rand_tile, TaskNode *taskNode)
{
    char *targetNode = DatumToString(taskNode->node, TEXTOID);
    Query *query = ParseQueryString(query_string, NULL, 0);
    List *rangeTableList = ExtractRangeTableEntryList(query);
    StringInfo physicalQuery = makeStringInfo();
    appendStringInfo(physicalQuery, "%s", query_string);

    ListCell *rangeTableCell = NULL;
    foreach(rangeTableCell, rangeTableList)
    {
        RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

        TaskNode *rteNode = GetShardHostNode(rangeTableEntry->relid, rand_tile);
        if (rteNode->node == (Datum) 0 || rteNode->port != taskNode->port ||
            strcmp(DatumToString(rteNode->node, TEXTOID), targetNode) != 0)
        {
            return NULL;
        }

        StringInfo tableName = makeStringInfo();
        if (IsReshuffledTable(rangeTableEntry->relid))
            appendStringInfo(tableName, "%s.%s", Var_Schema, get_rel_name(rangeTableEntry->relid));
        else
            appendStringInfo(tableName, "%s ", get_rel_name(rangeTableEntry->relid));

        char *shardName = GetRandomTileId(rangeTableEntry->relid, taskType, rand_tile);
        char *rewritten = replaceWord(physicalQuery->data, tableName->data, shardName);
        resetStringInfo(physicalQuery);
        appendStringInfo(physicalQuery, "%s", rewritten);
    }

    return physicalQuery->data;
}

/*
 * ExplainOnHostingWorker runs `EXPLAIN (FORMAT JSON)` for physicalQuery
 * (already tile-substituted by GetPhysicalTileQuery) directly on taskNode
 * via run_command_on_workers() (JSON format always returns exactly one row,
 * which run_command_on_workers() requires), returning the pretty-printed
 * plan. Returns NULL if the dispatch itself fails.
 */
static char *
ExplainOnHostingWorker(char *physicalQuery, TaskNode *taskNode)
{
    char *targetNode = DatumToString(taskNode->node, TEXTOID);

    StringInfo explainCommand = makeStringInfo();
    appendStringInfo(explainCommand, "EXPLAIN (FORMAT JSON) %s", physicalQuery);

    int spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    StringInfo dispatchQuery = makeStringInfo();
    appendStringInfo(dispatchQuery,
                     "SELECT jsonb_pretty(result::jsonb) FROM run_command_on_workers(%s) "
                     "WHERE nodename = %s AND nodeport = %d AND success",
                     quote_literal_cstr(explainCommand->data),
                     quote_literal_cstr(targetNode), taskNode->port);
    spi_result = SPI_execute(dispatchQuery->data, true, 1);

    char *planText = NULL;
    if (spi_result == SPI_OK_SELECT && SPI_processed > 0)
    {
        planText = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    }

    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }

    return planText;
}

static void
ExplainWorkerPlan(PlannedStmt *plannedstmt, DestReceiver *dest, ExplainState *es,
                  const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv,
                  const instr_time *planduration)
{
    QueryDesc  *queryDesc;
    instr_time	starttime;
    double		totaltime = 0;
    int			eflags;
    int			instrument_option = 0;

    Assert(plannedstmt->commandType != CMD_UTILITY);

    if (es->analyze && es->timing)
        instrument_option |= INSTRUMENT_TIMER;
    else if (es->analyze)
        instrument_option |= INSTRUMENT_ROWS;

    if (es->buffers)
        instrument_option |= INSTRUMENT_BUFFERS;
#if PG_VERSION_NUM >= PG_VERSION_13
    if (es->wal)
        instrument_option |= INSTRUMENT_WAL;
#endif
    /*
     * We always collect timing for the entire statement, even when node-level
     * timing is off, so we don't look at es->timing here.  (We could skip
     * this if !es->summary, but it's hardly worth the complication.)
     */
    INSTR_TIME_SET_CURRENT(starttime);

    /*
     * Use a snapshot with an updated command ID to ensure this query sees
     * results of any previously executed queries.
     */
    PushActiveSnapshot(GetTransactionSnapshot());
    UpdateActiveSnapshotCommandId();

    /* Create a QueryDesc for the query */
    queryDesc = CreateQueryDesc(plannedstmt, queryString,
                                GetActiveSnapshot(), InvalidSnapshot,
                                dest, params, queryEnv, instrument_option);

    /* Select execution options */
    if (es->analyze)
        eflags = 0;				/* default run-to-completion flags */
    else
        eflags = EXEC_FLAG_EXPLAIN_ONLY;

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, eflags);

    /* Execute the plan for statistics if asked for */
    if (es->analyze)
    {
        ScanDirection dir = ForwardScanDirection;

        /* run the plan */
        ExecutorRun(queryDesc, dir, 0L, true);

        /* run cleanup too */
        ExecutorFinish(queryDesc);
    }

    ExplainOpenGroup("Query", NULL, true, es);

    /* Create textual dump of plan tree */
    ExplainPrintPlan(es, queryDesc);

    if (es->summary && planduration)
    {
        double		plantime = INSTR_TIME_GET_DOUBLE(*planduration);

        ExplainPropertyFloat("Planning Time", "ms", 1000.0 * plantime, 3, es);
    }

    /* Print info about runtime of triggers */
    if (es->analyze)
        ExplainPrintTriggers(es, queryDesc);

    /*
     * Print info about JITing. Tied to es->costs because we don't want to
     * display this in regression tests, as it'd cause output differences
     * depending on build options.  Might want to separate that out from COSTS
     * at a later stage.
     */
    if (es->costs)
        ExplainPrintJITSummary(es, queryDesc);

    /*
     * Close down the query and free resources.  Include time for this in the
     * total execution time (although it should be pretty minimal).
     */
    INSTR_TIME_SET_CURRENT(starttime);

    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);

    PopActiveSnapshot();
    /* We need a CCI just in case query expanded to multiple plans */
    if (es->analyze)
        CommandCounterIncrement();

    //totaltime += elapsed_time(&starttime);

    /*
     * We only report execution time if we actually ran the query (that is,
     * the user specified ANALYZE), and if summary reporting is enabled (the
     * user can set SUMMARY OFF to not have the timing information included in
     * the output).  By default, ANALYZE sets SUMMARY to true.
     */
    if (es->summary && es->analyze)
        ExplainPropertyFloat("Execution Time", "ms", 1000.0 * totaltime, 3,
                             es);

    //*executionDurationMillisec = totaltime * 1000;

    ExplainCloseGroup("Query", NULL, true, es);
}
