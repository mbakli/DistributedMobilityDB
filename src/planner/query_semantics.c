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

#include "postgres.h"
#include <distributed/multi_logical_planner.h>
#include "planner/query_semantics.h"
#include "utils/planner_utils.h"


/*
 * analyseSelectClause analyses the select clause and
 * detects the distributed functions, segmented objects, etc
 */
extern void
analyseSelectClause(List *targetList, PostProcessing *postProcessing)
{
    ListCell *targetEntryCell = NULL;
    foreach(targetEntryCell, targetList)
    {
        /* Process the input functions */
        TargetEntry *targetEntry = lfirst(targetEntryCell);
        if (targetEntry->resname != NULL && IsDistFunc(targetEntry))
        {
            // Add the distributed function to the list of the post processing operations
            DistributedFunction *dist_function = addDistributedFunction(targetEntry);
            postProcessing->distfuns = lappend(postProcessing->distfuns,
                                                         dist_function);
        }
    }
}

/*
 * AnalyseCatalog walks fromExpr's WHERE clauses looking for spatiotemporal
 * predicates on `tbl` and, for each one found, folds its arguments into a
 * CatalogFilter (via AddCatalogFilterInfo) describing which of tbl's tiles
 * can possibly satisfy the query — this is what lets the planner narrow a
 * scan down to a handful of candidate tiles instead of all of them.
 */
extern CatalogFilter *
AnalyseCatalog(STMultirelation *tbl, FromExpr * fromExpr)
{
    CatalogFilter *catalogFilter = (CatalogFilter *) palloc0(sizeof(CatalogFilter));
    List *clauseList = WhereClauseList(fromExpr);
    ListCell *clauseCell = NULL;
    /* Iterate over the where clause conditions */
    foreach(clauseCell, clauseList) {
        Node *clause = (Node *) lfirst(clauseCell);
        if (!NodeIsEqualsOpExpr(clause))
        {
            Oid predicateOid;
            List *predicateArgs;
            /*
             * MobilityDB/PostGIS predicates such as eDwithin(...) or
             * ST_Intersects(...) parse as FuncExpr, not OpExpr; extract the
             * callable oid/args uniformly instead of assuming OpExpr.
             */
            bool hasPredicate = GetPredicateOidAndArgs(clause, &predicateOid, &predicateArgs);

            if (hasPredicate && predicateOid > 0 && list_length(predicateArgs) > 2)
            {
                /* Assumption that the first and the second argument are of type:
                 * Temporal-only (e.g., Period, timestamptz, etc)
                 * Spatial-only (e.g., point, linestring, polygon, etc)
                 * Spatiotemporal (e.g., instant, sequence, sequenceset)
                 * */
                if (IsIntersectionOperation(predicateOid))
                {
                    ListCell *arg;
                    foreach(arg, predicateArgs) {
                        AddCatalogFilterInfo(tbl->catalogTableInfo, catalogFilter, (Node *) lfirst(arg),
                                             INTERSECTION, false);
                    }
                }
                else if (IsDistanceOperation(predicateOid))
                {
                    ListCell *arg;
                    foreach(arg, predicateArgs) {
                        AddCatalogFilterInfo(tbl->catalogTableInfo, catalogFilter, (Node *) lfirst(arg),
                                             DISTANCE, false);
                    }
                }
            }
            else if (hasPredicate && predicateOid > 0 && list_length(predicateArgs) == 2)
            {
                /* Assumptions:
                 * (1) Both arguments are of type spatiotemporal, or
                 * (2) The first or the second argument is static */
                if (IsIntersectionOperation(predicateOid))
                {
                    /* Check if one of the arguments is static */
                    ListCell *arg;
                    foreach(arg, predicateArgs)
                    {
                        Node *node = (Node *) lfirst(arg);
                        if (IsA(node, Const))
                            AddCatalogFilterInfo(tbl->catalogTableInfo, catalogFilter, node,
                                                 RANGE, true);
                        else
                            AddCatalogFilterInfo(tbl->catalogTableInfo, catalogFilter, node,
                                                 INTERSECTION, false);
                    }
                }
                else if (IsDistanceOperation(predicateOid))
                {
                    /* Check if one of the arguments is static */
                    ListCell *arg;
                    foreach(arg, predicateArgs)
                    {
                        Node *node = (Node *) lfirst(arg);
                        if (IsA(node, Const))
                            AddCatalogFilterInfo(tbl->catalogTableInfo, catalogFilter, node, RANGE,
                                                 true);
                        else
                            AddCatalogFilterInfo(tbl->catalogTableInfo, catalogFilter, node, DISTANCE,
                                                 false);
                    }
                }
            }
            else
                AddCatalogFilterInfo(tbl->catalogTableInfo, catalogFilter, NULL,
                                     OTHER, false);
        }
    }
    return catalogFilter;
}

