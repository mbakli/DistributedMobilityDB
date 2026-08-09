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

#ifndef QUERY_SEMANTICS_CHECK_H
#define QUERY_SEMANTICS_CHECK_H
#include "post_processing/post_processing.h"
#include "multirelation/multirelation_utils.h"

/* Scans the SELECT targetlist for distributed aggregates and records them into postProcessing. */
extern void analyseSelectClause(List *targetList, PostProcessing *postProcessing);

/*
 * Rewrites a bare distributed-function call over a segmented table into a
 * grouped-by-trip aggregate query; NULL if nothing to rewrite. On success,
 * *explainNotesOut is set to a human-readable, newline-joined description
 * of each rewritten function's own worker/combiner/final ops and the op
 * actually applied (for EXPLAIN); *postProcessingNotesOut is set to a
 * newline-joined list of any plain scalar function(s) composed *around*
 * one of those calls (e.g. numinstants in `numinstants(cumulativeLength
 * (trip))`), kept separate so EXPLAIN can show them under their own
 * heading rather than folding them into a registered function's own
 * worker/combiner/final description; either may be left untouched (NULL)
 * on a NULL return, and *postProcessingNotesOut may stay NULL even on
 * success if no call in the query was composed with an outer function.
 */
extern char *RewriteSegmentedDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList,
                                           char **explainNotesOut, char **postProcessingNotesOut);

/*
 * Rewrites a bare distributed-function call used as a WHERE-clause filter
 * (e.g. `WHERE length(trip) > 5000`) over a single segmented distributed
 * spatiotemporal table into a two-level query that filters on the
 * function's properly-combined value instead of evaluating it per-fragment;
 * NULL if nothing to rewrite. See the .c file for the full rationale and
 * scope limits.
 */
extern char *RewriteWhereClauseDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList);

/*
 * Rewrites a query with an explicit aggregate over a registered distributed
 * function applied to a replicated (isMobilityDB, segmented) table's column
 * -- e.g. `SUM(length(atTime(t.Trip, p.Period))) ... GROUP BY ...` -- into a
 * two-level dedupe/aggregate query, so a trip replicated across N tiles
 * contributes to the aggregate once instead of N times. NULL if nothing to
 * rewrite.
 */
extern char *RewriteReplicatedAggregateQuery(Query *parse, const char *query_string, STMultirelations *tablesList);

/*
 * Same problem as RewriteReplicatedAggregateQuery, but for a query whose
 * aggregate-over-distributed-function lives inside one of its own CTEs
 * (e.g. `WITH x AS (SELECT ... SUM(length(atTime(...))) ... GROUP BY ...)
 * SELECT ... FROM x`) rather than at the top level; NULL if no CTE needed
 * rewriting (or there are no CTEs at all).
 */
extern char *RewriteReplicatedAggregateInCTEs(Query *parse, const char *query_string, STMultirelations *tablesList);

/* Analyses fromExpr's predicates against tbl's catalog to derive its candidate-tile filter. */
extern CatalogFilter *AnalyseCatalog(STMultirelation *tbl, FromExpr * fromExpr);

#endif /* QUERY_SEMANTICS_CHECK_H */
