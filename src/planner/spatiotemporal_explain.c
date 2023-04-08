#include "postgres.h"
#include "commands/explain.h"
#include "distributed/citus_custom_scan.h"
#include "planner/spatiotemporal_explain.h"
#include "general/catalog_management.h"
#include <utils/lsyscache.h>
#include <distributed/multi_explain.h>
#include "distributed/listutils.h"

static Node *SpatiotemporalExecutorCreateScan(CustomScan *scan);
static void SpatiotemporalPreExecutionScan(SpatiotemporalScanState *scanState);
void SpatiotemporalExplainScan(CustomScanState *node, List *ancestors, struct ExplainState *es);
static void ExplainWorkerPlan(PlannedStmt *plannedstmt, DestReceiver *dest, ExplainState *es,
                              const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv,
                              const instr_time *planduration, double *executionDurationMillisec);

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


