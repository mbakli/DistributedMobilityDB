#include "postgres.h"
#include "commands/explain.h"
#include "distributed/citus_custom_scan.h"
#include "planner/spatiotemporal_explain.h"
#include "general/catalog_management.h"
#include <utils/lsyscache.h>
#include <distributed/multi_explain.h>
#include "distributed/listutils.h"
#include "planner/planner_strategies.h"

static Node *SpatiotemporalExecutorCreateScan(CustomScan *scan);
static void SpatiotemporalPreExecutionScan(SpatiotemporalScanState *scanState);
static void SpatiotemporalExplainScan(CustomScanState *node, List *ancestors, struct ExplainState *es);
static void ExplainWorkerPlan(PlannedStmt *plannedstmt, DestReceiver *dest, ExplainState *es,
                              const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv,
                              const instr_time *planduration, double *executionDurationMillisec);
static void InitializeDistributedQueryExplain(DistributedQueryExplain *distributedQueryExplain,
                                              ExplainState *es, const char *queryString);
static void ExplainQueryType(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es);
static char * getQueryType(List *strategies);
static void ExplainQueryParameters(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es, int indent_group);
static void ExplainMainPredicate(PredicateType predicateType, PredicateInfo * predicateInfo,
                                 ExplainState *es, int indent_group);
static void ExplainDistributedTables(SpatiotemporalTables *tablesList, ExplainState *es, int indent_group);
static void ExplainReshufflingPlanInfo(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es,
                                       int indent_group);
static void ExplainQueryPlan(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es,
                                       int indent_group);
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
void
spatiotemporal_explain(Query *query, int cursorOptions, IntoClause *into,
                       ExplainState *es, const char *queryString, ParamListInfo params,
                       QueryEnvironment *queryEnv)
{

}
/*
 * Let PostgreSQL know about the custom scan nodes.
 */
void
RegisterSpatiotemporalPlanMethods(void)
{
    RegisterCustomScanMethods(&SpatiotemporalExecutorMethod);
}

/*
 * AdaptiveExecutorCreateScan creates the scan state for the adaptive executor.
 */
static Node *
SpatiotemporalExecutorCreateScan(CustomScan *scan)
{
    SpatiotemporalScanState *scanState = palloc0(sizeof(SpatiotemporalScanState));

    scanState->customScanState.ss.ps.type = T_CustomScanState;
    scanState->distributedSpatiotemporalPlan = GetSpatiotemporalDistributedPlan(scan);
    scanState->customScanState.methods = &SpatiotemporalExecutorMethods;
    //scanState->PreExecScan = &SpatiotemporalPreExecutionScan;

    scanState->finishedPreScan = false;
    scanState->finishedRemoteScan = false;

    return (Node *) scanState;
}