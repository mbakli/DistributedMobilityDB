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

#ifndef SPATIOTEMPORAL_PLANNER_H
#define SPATIOTEMPORAL_PLANNER_H

#include "postgres.h"
#include "optimizer/planner.h"
#include "multirelation/multirelation_utils.h"
#include "post_processing/post_processing.h"
#include "utils/helper_functions.h"
#include "predicate_management.h"
#include "nodes/makefuncs.h"
#include "catalog/table_ops.h"
#include "general/rte.h"


/*
 * DistributedSpatiotemporalQueryPlan contains all information necessary to execute a
 * distributed spatiotemporal query.
 */
typedef struct DistributedSpatiotemporalQueryPlan
{
    /* reshuffled relation of one or more tables */
    Rte *reshuffledTable;
    /* The base table for reshuffling */
    STMultirelation *reshuffled_table_base;
    /* which spatiotemporal relations are accessed by this distributed plan */
    STMultirelations *tablesList;
    Predicates *predicatesList;
    Query *query;
    bool queryContainsReshuffledTable;
    char *reshuffling_query;
    ShapeType shapeType;
    /* The spatial reference identifier for the query tables */
    int srid;
    /* Determines the query processing type: 1 for spatial, 2 for temporal, 3 for spatotemporal, 4 for other */
    char * joining_col;
    /* Determines the plan strategy: colocated, non-colocated, tile scan rebalancer, filter and predicate pushdown */
    List *strategies;
    List *strategyPlans;
    bool activate_post_processing_phase;
    /* Distance value if exists which means that the required will need some of the data to be reshuffled before
     * executing the query
     */
    float distance;
    bool activate_rewriter;
    /* Catalog query string */
    char *catalog_query_string;
    /* It is just for visualization in the explain command */
    char *org_query_string;
    Datum range_bbox;
    PostProcessing *postProcessing;
    /*
     * Set when RewriteSegmentedDistFuncCalls rewrote the query (bare
     * distributed-function call over a segmented table -> explicit grouped
     * aggregate) and handed off to Citus' own planner directly. The
     * EXPLAIN hook treats a non-NULL PlannedStmt from
     * distributed_mobilitydb_planner_internal as "our custom planning
     * bailed out, re-explain the original query" -- which is wrong here,
     * since this *is* the plan that actually runs; without this, EXPLAIN
     * would show the original bare (ungrouped) query shape instead of what
     * was really executed.
     */
    char *segmentedRewriteQuery;
    /*
     * Set alongside segmentedRewriteQuery: one line per rewritten function
     * naming its registered worker/combiner/final ops (from
     * pg_dist_spatiotemporal_dist_functions) and the op actually applied in
     * the rewrite -- for EXPLAIN to show *why* the rewrite looks the way it
     * does in this extension's own worker/combiner/final vocabulary,
     * without asserting anything about replication vs. true segmentation.
     */
    char *segmentedRewriteExplainNotes;
    /*
     * Set by ProcessQueryPredicates whenever a query (outer query, a CTE, or
     * a FROM-clause subquery) has a WHERE clause, regardless of whether any
     * conjunct turns out to be a registered spatiotemporal predicate. Lets
     * getQueryType tell apart, once no strategy ends up chosen, "Filtered
     * Scan" (WHERE present but nothing spatial in it, e.g. WHERE
     * vehicleid = 5 -- still can't prune tiles) from "Full Scan" (no WHERE
     * clause at all).
     */
    bool hasWhereClause;
} DistributedSpatiotemporalQueryPlan;

/* Filter Operation */
typedef struct GeneralScan
{
    Query *query;
    StringInfo query_string;
    int length;
} GeneralScan;

/*
 * planner_hook entry point: intercepts queries touching distributed
 * spatiotemporal tables and produces a custom-scan plan wrapping a
 * DistributedSpatiotemporalQueryPlan; falls back to the standard planner
 * otherwise.
 */
extern PlannedStmt * distributed_mobilitydb_planner(Query *parse, const char *query_string, int cursorOptions,
                                            ParamListInfo boundParams);

/* Shared planning logic behind distributed_mobilitydb_planner(), also used by EXPLAIN. */
extern PlannedStmt *distributed_mobilitydb_planner_internal(Query *parse, const char *query_string, int cursorOptions,
                                                            ParamListInfo boundParams,
                                                            DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan,
                                                            bool explain);

/* Retrieves the DistributedSpatiotemporalQueryPlan stashed on a planned CustomScan node. */
extern DistributedSpatiotemporalQueryPlan *GetSpatiotemporalDistributedPlan(CustomScan *customScan);

/* Loads the pg_dist_spatiotemporal_tables catalog row for rangeTableEntry's relation. */
extern STMultirelationCatalog *GetSpatiotemporalCatalogTableInfo(RangeTblEntry *rangeTableEntry);


#endif /* SPATIOTEMPORAL_PLANNER_H */
