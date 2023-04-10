#ifndef SPATIOTEMPORAL_PLANNER_H
#define SPATIOTEMPORAL_PLANNER_H

#include "postgres.h"
#include "optimizer/planner.h"
#include "partitioning/multirelation.h"
#include "post_processing/post_processing.h"
#include "general/helper_functions.h"
#include "distributed_functions/coordinator_operations.h"
#include "distributed_functions/worker_operations.h"
#include "predicate_management.h"


/* Filter Operation */
typedef struct FilterOp
{
    char *queryPredicate;
} FilterOp;

/*
 * DistributedSpatiotemporalQueryPlan contains all information necessary to execute a
 * distributed spatiotemporal query.
 */
typedef struct DistributedSpatiotemporalQueryPlan
{
    /* reshuffled relation of one or more tables */
    SpatiotemporalTable *reshuffledTable;
    /* The base table for reshuffling */
    SpatiotemporalTable *reshuffled_table_base;
    /* which spatiotemporal relations are accessed by this distributed plan */
    SpatiotemporalTables *tablesList;
    Predicates *predicatesList;
    Query *query;
    bool queryContainsReshuffledTable;
    char *reshuffling_query;
    ShapeType shapeType;
    /* The spatial reference identifier for the query tables */
    int srid;
    /* Determines the query processing type: 1 for spatial, 2 for temporal, 3 for spatotemporal, 4 for other */
    int query_type;
    char * joining_col;
    /* Determines the plan strategy: colocated, non-colocated, tile scan rebalancer, filter and predicate pushdown */
    List *strategies;
    bool activate_post_processing_phase;
    Oid *tables_oid_cached;
    /* Distance value if exists which means that the required will need some of the data to be reshuffled before
     * executing the query
     */
    float distance;
    /* Catalog query string */
    char *catalog_query_string;
    /* It is just for visualization in the explain command */
    char *org_query_string;
    char * queryType;
    char * joinType;
    PostProcessing *postProcessing;
} DistributedSpatiotemporalQueryPlan;

/* Filter Operation */
typedef struct GeneralScan
{
    Query *query;
    StringInfo query_string;
    int length;
} GeneralScan;

extern PlannedStmt * spatiotemporal_planner(Query *parse, const char *query_string, int cursorOptions,
                                            ParamListInfo boundParams);
extern PlannedStmt *spatiotemporal_planner_internal(Query *parse, const char *query_string, int cursorOptions,
                                                    ParamListInfo boundParams,
                                                    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan,
                                                    bool explain);
extern DistributedSpatiotemporalQueryPlan *GetSpatiotemporalDistributedPlan(CustomScan *customScan);
extern SpatiotemporalTableCatalog *GetSpatiotemporalCatalogTableInfo(RangeTblEntry *rangeTableEntry);
#endif /* SPATIOTEMPORAL_PLANNER_H */
