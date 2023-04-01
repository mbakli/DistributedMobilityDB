#include "planner/spatiotemporal_planner.h"
#include "distributed/distributed_planner.h"
#include "general/metadata_management.h"
#include "distributed/multi_executor.h"
#include "executor/spatiotemporal_executor.h"
#include "planner/query_semantic_check.h"
#include "nodes/nodeFuncs.h"
#include "distributed_functions/distributed_function.h"
#include "planner/post_processing.h"


PlannedStmt *
spatiotemporal_planner(Query *parse, const char *query_string, int cursorOptions,
                       ParamListInfo boundParams)
{
    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan = (DistributedSpatiotemporalQueryPlan *)
            palloc0(sizeof(DistributedSpatiotemporalQueryPlan));
    return spatiotemporal_planner_internal(parse, query_string, cursorOptions, boundParams,
                                           distributedSpatiotemporalPlan, false);
}

PlannedStmt *
spatiotemporal_planner_internal(Query *parse, const char *query_string, int cursorOptions,
                                ParamListInfo boundParams,
                                DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan, bool explain)
{
    PlannedStmt *result = NULL;
    return result;
}