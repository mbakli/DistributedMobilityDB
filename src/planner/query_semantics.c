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
#include <ctype.h>
#include <distributed/multi_logical_planner.h>
#include <nodes/parsenodes.h>
#include <parser/parsetree.h>
#include <utils/lsyscache.h>
#include "planner/query_semantics.h"
#include "utils/planner_utils.h"
#include "utils/helper_functions.h"
#include "distributed_functions/distributed_function.h"
#include "general/rte.h"


/*
 * analyseSelectClause analyses the select clause and
 * detects the distributed functions, segmented objects, etc
 *
 * The worker/combiner/final rewrite this feeds (RewriterDistFuncs ->
 * ProcessIntermediateTasks/ProcessFinalTasks) can only build a SELECT list
 * out of the registered functions themselves -- it has no way to also carry
 * along a plain pass-through column such as a join key. So if the target
 * list mixes a registered function (e.g. length()) with any other column,
 * treating that function as distributed collapses the whole result down to
 * one aggregated row and silently drops every other column (e.g.
 * "SELECT t1.tripid, t2.tripid, length(t2.trip) FROM ... WHERE eIntersects(...)"
 * returned only length). Only engage the distributed-function rewrite when
 * every projected column is itself a registered function, matching the
 * single-aggregate queries (e.g. "SELECT sum(length(Trip)) FROM trips_50t")
 * it's actually designed for; otherwise leave the functions as plain
 * per-row calls pushed down with the rest of the query.
 */
extern void
analyseSelectClause(List *targetList, PostProcessing *postProcessing)
{
    ListCell *targetEntryCell = NULL;
    foreach(targetEntryCell, targetList)
    {
        TargetEntry *targetEntry = lfirst(targetEntryCell);
        if (targetEntry->resjunk)
            continue;
        if (targetEntry->resname == NULL || !IsDistFunc(targetEntry))
            return;
    }
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
 * RewriteSegmentedDistFuncCalls detects a bare (non-aggregate) call to a
 * registered distributed function -- e.g. `length(trip)`, no sum()/
 * aggregate wrapper -- over a single shape-segmented distributed
 * spatiotemporal table, and returns a rewritten query string turning it
 * into an ordinary SQL grouped aggregate, e.g.:
 *
 *   select length(trip) from trips_50t
 *   -> select sum(length(trip)) as length from trips_50t group by tripid  -- PostGIS linestring/polygon
 *   -> select max(length(trip)) as length from trips_50t group by tripid  -- MobilityDB tgeompoint
 *
 * so Citus' own native distributed GROUP BY/aggregate pushdown -- already
 * proven correct for `sum(length(trip)) as length` without a GROUP BY --
 * combines each trip's per-tile fragments into one row per trip, instead
 * of every row silently collapsing into a single global total (see
 * IsDistFunc's comment for why a bare call collides with that mechanism in
 * the first place, and why it must not be the one to handle this case).
 * Which combining op is used (the registered "final" op, e.g. sum, vs a
 * plain duplicate-collapsing max) depends on whether the table's tiles
 * genuinely hold disjoint fragments or full duplicate copies -- see the
 * isMobilityDB check below for why those aren't the same thing despite
 * both being flagged "segmented" in the catalog.
 *
 * GROUP BY doesn't require its key to be projected, so groupCol is never
 * added to the SELECT list on its own -- if the caller wants it in the
 * output (e.g. `select tripid, numinstants(trip) from t where tripid=312`,
 * to tell which trip a row belongs to when running an unfiltered query
 * over many trips), it must already be there in the original target list;
 * an explicit reference to it is recognized and passed through as-is
 * rather than triggering a bail-out. Any *other* plain column reference
 * isn't something this rewrite knows how to group by safely, so it still
 * bails out (falls back to the pre-existing per-fragment-pushdown
 * behavior) in that case.
 *
 * Deliberately scoped to a single table with no join, and a target list
 * made up solely of bare distributed-function calls (plus, optionally, an
 * explicit groupCol reference) over that table's own spatiotemporal
 * column: that covers the reported bug ("select length(trip) from any
 * distributed table") without the complexity of a general
 * arbitrary-target-list/join rewrite. Returns NULL when there's nothing to
 * rewrite (not a single-table query, table isn't segmented, no groupCol on
 * record, the target list isn't purely bare distfunc calls over that
 * column, or the query has no textual " from " to anchor on) -- the caller
 * should keep using the original query_string in that case.
 */
extern char *
RewriteSegmentedDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList)
{
    if (tablesList == NULL || tablesList->length != 1)
        return NULL;

    /*
     * tablesList->tables holds Rte wrappers (see general/rte.h), tagged by
     * RteType, not STMultirelation directly -- a plain Citus table or a
     * local (non-distributed) table produces the very same list shape.
     * Only STRte actually wraps an STMultirelation with tiling catalog
     * info; anything else has nothing for this rewrite to key off.
     */
    Rte *rteNode = (Rte *) linitial(tablesList->tables);
    if (rteNode->RteType != STRte)
        return NULL;

    STMultirelation *table = (STMultirelation *) rteNode->rte;
    STMultirelationCatalog *catalog = &table->catalogTableInfo;
    if (!catalog->segmentation || catalog->groupCol == NULL)
        return NULL;

    StringInfo selectList = makeStringInfo();
    bool foundAny = false;

    ListCell *targetEntryCell = NULL;
    foreach(targetEntryCell, parse->targetList)
    {
        TargetEntry *targetEntry = lfirst(targetEntryCell);
        if (targetEntry->resjunk)
            continue;

        /*
         * A plain reference to the table's own group column (e.g. writing
         * `SELECT tripid, numinstants(trip) FROM ...` explicitly, instead
         * of relying on some implicit projection) is passed through as-is
         * -- GROUP BY doesn't require its key to be projected at all, so
         * this rewrite never adds groupCol to the SELECT list on its own;
         * it only appears in the output if the caller's own target list
         * already asked for it, in whatever position/alias they used. Any
         * *other* plain column reference isn't something this rewrite
         * knows how to group by safely, so it bails out.
         */
        if (IsA(targetEntry->expr, Var))
        {
            Var *var = (Var *) targetEntry->expr;
            RangeTblEntry *varRte = rt_fetch(var->varno, parse->rtable);
            char *varColName = get_attname(varRte->relid, var->varattno, false);
            if (varColName == NULL || strcasecmp(varColName, catalog->groupCol) != 0)
                return NULL;

            if (selectList->len > 0)
                appendStringInfoString(selectList, ", ");
            appendStringInfo(selectList, "%s as %s", catalog->groupCol,
                             targetEntry->resname != NULL ? targetEntry->resname : catalog->groupCol);
            continue;
        }

        if (!IsA(targetEntry->expr, FuncExpr))
            return NULL;

        FuncExpr *funcExpr = (FuncExpr *) targetEntry->expr;
        if (list_length(funcExpr->args) != 1 || !IsA(linitial(funcExpr->args), Var))
            return NULL;

        Var *arg = (Var *) linitial(funcExpr->args);
        RangeTblEntry *rte = rt_fetch(arg->varno, parse->rtable);
        char *argColName = get_attname(rte->relid, arg->varattno, false);
        if (argColName == NULL || strcasecmp(argColName, catalog->distCol) != 0)
            return NULL;

        char *funcName = get_func_name(funcExpr->funcid);
        char *finalOp = LookupDistFuncFinalOp(funcName);
        if (finalOp == NULL)
            return NULL;

        /*
         * shape_segmentation.sql's ST_Intersection-based clipping (safe to
         * SUM back together) only applies to its 'linestring'/
         * 'multilinestring'/'polygon'/'multipolygon' branches -- static
         * PostGIS geometry. Its 'sequence'/'sequenceset' branch (every
         * MobilityDB tgeompoint trajectory table this extension has, since
         * a moving point's shape type is never one of those PostGIS
         * types) does no clipping at all: it just re-packs the *whole*,
         * unsplit trip into every tile whose bbox it overlaps (`WHERE
         * distCol && bbox_with_srid` is a bbox-overlap test, not a cut).
         * Confirmed empirically: every "fragment" of a multi-tile trip
         * carries identical numinstants/startTimestamp/endTimestamp/
         * length -- full duplicates, not disjoint partial pieces. Summing
         * those would silently multiply the true value by however many
         * tiles the trip happens to touch. The registered final op (sum
         * for length -- correct for genuine partial-fragment reconstruction)
         * doesn't apply here; any of several duplicate-but-identical rows
         * is already the whole, correct answer, so MAX (or MIN, or any
         * other order-independent pick) collapses them to one row without
         * altering the value.
         */
        char *combiningOp = catalog->isMobilityDB ? "max" : finalOp;

        if (selectList->len > 0)
            appendStringInfoString(selectList, ", ");
        appendStringInfo(selectList, "%s(%s(%s)) as %s", combiningOp, funcName, catalog->distCol,
                         targetEntry->resname != NULL ? targetEntry->resname : funcName);
        foundAny = true;
    }

    if (!foundAny)
        return NULL;

    char *lowered = toLower((char *) query_string);
    char *fromKeyword = strstr(lowered, " from ");
    if (fromKeyword == NULL)
        return NULL;
    size_t fromOffset = (fromKeyword - lowered) + 1; /* skip the leading space " from " matched on */

    /*
     * ORDER BY/HAVING can reference the *original*, un-rewritten expression
     * (e.g. "ORDER BY length(trip)"), which would need the same rewriting
     * treatment as the SELECT list to remain valid against the new GROUP
     * BY -- safer to bail out and let the caller fall back to the
     * pre-existing (imperfect but non-erroring) per-fragment pushdown than
     * to risk emitting broken or silently-wrong SQL by trying to patch an
     * arbitrary ORDER BY/HAVING expression via text substitution.
     */
    char *loweredTail = lowered + fromOffset;
    if (strstr(loweredTail, " order by ") != NULL || strstr(loweredTail, " having ") != NULL)
        return NULL;

    /*
     * LIMIT/OFFSET don't reference columns, so relocating them is safe:
     * GROUP BY must come before them, not after -- find the earliest of
     * the two (if any) in the tail and split there, so the rewritten query
     * becomes "... FROM ... WHERE ... GROUP BY groupcol LIMIT/OFFSET ..."
     * instead of appending GROUP BY unconditionally at the very end, which
     * produced invalid SQL ("... LIMIT 2 GROUP BY tripid" -> syntax error).
     */
    char *splitPos = NULL;
    const char *boundaryMarkers[] = { " limit ", " offset " };
    for (int i = 0; i < 2; i++)
    {
        char *found = strstr(loweredTail, boundaryMarkers[i]);
        if (found != NULL && (splitPos == NULL || found < splitPos))
            splitPos = found;
    }

    char *core;
    char *suffix;
    if (splitPos != NULL)
    {
        size_t coreLen = splitPos - loweredTail;
        core = palloc(coreLen + 1);
        memcpy(core, query_string + fromOffset, coreLen);
        core[coreLen] = '\0';
        suffix = pstrdup(query_string + fromOffset + coreLen);
    }
    else
    {
        core = pstrdup(query_string + fromOffset);
        suffix = "";
    }

    /* Strip a trailing ";"/whitespace from whichever piece ends up last, so the constructed query stays valid SQL. */
    char *last = (suffix[0] != '\0') ? suffix : core;
    size_t lastLen = strlen(last);
    while (lastLen > 0 && (last[lastLen - 1] == ';' || isspace((unsigned char) last[lastLen - 1])))
        last[--lastLen] = '\0';

    StringInfo newQuery = makeStringInfo();
    appendStringInfo(newQuery, "select %s %s group by %s %s",
                     selectList->data, core, catalog->groupCol, suffix);
    return newQuery->data;
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

