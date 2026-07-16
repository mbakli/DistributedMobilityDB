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

#ifndef QUERY_SEMANTICS_CHECK_H
#define QUERY_SEMANTICS_CHECK_H
#include "post_processing/post_processing.h"
#include "multirelation/multirelation_utils.h"

/* Scans the SELECT targetlist for distributed aggregates and records them into postProcessing. */
extern void analyseSelectClause(List *targetList, PostProcessing *postProcessing);

/* Rewrites a bare distributed-function call over a segmented table into a grouped-by-trip aggregate query; NULL if nothing to rewrite. */
extern char *RewriteSegmentedDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList);

/* Analyses fromExpr's predicates against tbl's catalog to derive its candidate-tile filter. */
extern CatalogFilter *AnalyseCatalog(STMultirelation *tbl, FromExpr * fromExpr);

#endif /* QUERY_SEMANTICS_CHECK_H */
