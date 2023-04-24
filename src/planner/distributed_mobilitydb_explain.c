#include "planner/distributed_mobilitydb_explain.h"

#include "commands/explain.h"
#include <utils/lsyscache.h>
#include <distributed/multi_explain.h>
#include "distributed/listutils.h"
#include "catalog/nodes.h"
#include "planner/planner_strategies.h"


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
    PlannedStmt *result = distributed_mobilitydb_planner_internal(curDistributedQueryExplain->query,
                                                          curDistributedQueryExplain->query_string,
                                                          cursorOptions, params,
                                                          distPlan, true);

    /* Delegating it to Citus*/
    if (result != NULL)
        CitusExplainOneQuery(query,cursorOptions,into,es,queryString,params,queryEnv);

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
    distributedQueryExplain->query_string = palloc((strlen(tmp->data) + 1) * sizeof (char));
    distributedQueryExplain->query_string = tmp->data;
    distributedQueryExplain->query = ParseQueryString(tmp->data, NULL, 0);
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
static void
ExplainOneTask(ExecutorTask *task, STMultirelation *base,ExplainState *es, int indent_group)
{
    int rand_tile = GetRandTileNum(base);

    appendStringInfoSpaces(es->str, es->indent * indent_group + 12);
    TaskNode *taskNode = GetNodeInfo();
    appendStringInfo(es->str, "Node: host=%s ", DatumToString(taskNode->node, TEXTOID));
    appendStringInfo(es->str, "port=%d ", taskNode->port);
    appendStringInfo(es->str, "dbname=%s\n", DatumGetCString(taskNode->db));
    appendStringInfoSpaces(es->str, es->indent * indent_group + 12);

    char *OneTileQuery = GetLocalQuery(task->taskQuery->data, base->catalogTableInfo.table_oid,
                                       task->taskType, rand_tile);
    Query *parse = ParseQueryString(OneTileQuery, NULL, 0);
    PlannedStmt *plan = pg_plan_query_compat(parse, NULL, 0, NULL);
    instr_time planduration;
    INSTR_TIME_SET_ZERO(planduration);
    es->indent += 6;
    DestReceiver *tupleStoreDest = CreateTuplestoreDestReceiver();
    ExplainWorkerPlan(plan, tupleStoreDest, es, OneTileQuery, NULL, NULL,
                      &planduration);
    ExplainEndOutput(es);
}

static char *
GetLocalQuery(char *query_string, Oid base, ExecTaskType taskType, int rand_tile)
{

    if (query_string != NULL)
    {
        Query *query = ParseQueryString(query_string, NULL, 0);
        ListCell *rangeTableCell = NULL;
        List *rangeTableList = ExtractRangeTableEntryList(query);
        StringInfo final_query = makeStringInfo();
        StringInfo replace = makeStringInfo();
        appendStringInfo(final_query, "%s", query_string);
        RangeTblEntry *reshuffledTableEntry;
        StringInfo tileId = makeStringInfo();
        foreach(rangeTableCell, rangeTableList)
        {
            RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
            // Get random tile key
            appendStringInfo(tileId, "%s", GetRandomTileId(rangeTableEntry->relid, taskType, rand_tile));

            StringInfo t = makeStringInfo();
            if (IsReshuffledTable(rangeTableEntry->relid))
                appendStringInfo(t, "%s.%s", Var_Schema, get_rel_name(rangeTableEntry->relid));
            else
                appendStringInfo(t, "%s ", get_rel_name(rangeTableEntry->relid));
            // Replace it in the query string
            appendStringInfo(replace, "%s ", replaceWord(final_query->data, t->data,
                                                         tileId->data));
            resetStringInfo(t);
            resetStringInfo(final_query);
            appendStringInfo(final_query, "%s", replace->data);
            resetStringInfo(replace);
            resetStringInfo(tileId);
        }

        return final_query->data;

    }
    return 0;
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
