#ifndef SPATIOTEMPORAL_PLANNER_H
#define SPATIOTEMPORAL_PLANNER_H

#include "postgres.h"
#include "optimizer/planner.h"
#include "post_processing.h"
#include "general/helper_functions.h"
#include "distributed_functions/coordinator_operations.h"
#include "distributed_functions/worker_operations.h"

typedef struct SpatiotemporalTableCatalog
{
    int num_tiles;
    char *dist_col;
} SpatiotemporalTableCatalog;


typedef struct SpatiotemporalTable
{
    ListCell *rangeTableCell;
    SpatiotemporalTableCatalog catalogTableInfo;
} SpatiotemporalTable;

typedef struct PostProcessing
{
    List *functions; // DistributedFunction;
    bool group_by_required;

} PostProcessing;

/*
 * DistributedSpatiotemporalQueryPlan contains all information necessary to execute a
 * distributed spatiotemporal query.
 */
typedef struct DistributedSpatiotemporalQueryPlan
{
    /* reshuffled relation of one or more tables */
    RangeTblEntry *reshuffledTable;
    /* The base table for reshuffling */
    RangeTblEntry *reshuffled_table_base;
    /* which spatiotemporal relations are accessed by this distributed plan */
    List *spatiotemporalTableList;
    PostProcessing *post_processing;
    /* The spatial reference identifier for the query tables */
    int srid;
    /* Determines the query processing type: 1 for spatial, 2 for temporal, 3 for spatotemporal, 4 for other */
    int query_type;
    /* Determines whether the plan is either co-located or not */
    bool colocatedQuery;
    bool nonColocatedQuery;
    bool knn_query;
    bool activate_post_processing_phase;
    Oid *tables_oid_cached;
    /* Distance value if exists which means that the required will need some of the data to be reshuffled before
     * executing the query
     */
    float distance;
    /* It is just for visualization in the explain command */
    char * queryType;
    char * joinType;
} DistributedSpatiotemporalQueryPlan;

extern PlannedStmt * spatiotemporal_planner(Query *parse, const char *query_string, int cursorOptions,
                                            ParamListInfo boundParams);
extern PlannedStmt *spatiotemporal_planner_internal(Query *parse, const char *query_string, int cursorOptions,
                                                    ParamListInfo boundParams,
                                                    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan,
                                                    bool explain);
extern DistributedSpatiotemporalQueryPlan *GetSpatiotemporalDistributedPlan(CustomScan *customScan);
extern SpatiotemporalTableCatalog *GetSpatiotemporalCatalogTableInfo(RangeTblEntry *rangeTableEntry);
extern void createReshufflingTablePlan(DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan);

#endif /* SPATIOTEMPORAL_PLANNER_H */
