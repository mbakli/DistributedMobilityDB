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

#include "postgres.h"
#include <ctype.h>
#include <distributed/multi_logical_planner.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <parser/parsetree.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <optimizer/optimizer.h>
#include "planner/query_semantics.h"
#include "utils/planner_utils.h"
#include "utils/helper_functions.h"
#include "distributed_functions/distributed_function.h"
#include "general/rte.h"

/*
 * TargetEntryReferencesGroupCol reports whether te's expression is a plain
 * Var referencing replicatedTable's own groupCol column (e.g. writing
 * `t.tripid` explicitly in the SELECT list) -- used by
 * RewriteReplicatedAggregateQuery to avoid projecting the row identifier
 * its inner DISTINCT needs a *second* time under the same alias when the
 * caller's own target list already asked for it. Postgres allows
 * constructing a subquery whose output has two same-named columns, but
 * referencing that name from the enclosing query is then ambiguous --
 * exactly the shape produced by unconditionally prepending groupCol
 * without checking whether it's already there.
 */
static bool
TargetEntryReferencesGroupCol(TargetEntry *te, Query *parse, STMultirelation *replicatedTable)
{
    if (!IsA(te->expr, Var))
        return false;
    Var *var = (Var *) te->expr;
    RangeTblEntry *varRte = rt_fetch(var->varno, parse->rtable);
    if (varRte->relid != replicatedTable->catalogTableInfo.table_oid)
        return false;
    char *varColName = get_attname(varRte->relid, var->varattno, false);
    return varColName != NULL &&
           strcasecmp(varColName, replicatedTable->catalogTableInfo.groupCol) == 0;
}

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
 *   -> select sum(length(trip)) as length from trips_50t group by tripid
 *
 * so Citus' own native distributed GROUP BY/aggregate pushdown -- already
 * proven correct for `sum(length(trip)) as length` without a GROUP BY --
 * combines each trip's per-tile fragments into one row per trip, instead
 * of every row silently collapsing into a single global total (see
 * IsDistFunc's comment for why a bare call collides with that mechanism in
 * the first place, and why it must not be the one to handle this case).
 *
 * Always wrapped in the table's registered final op (e.g. sum) and grouped
 * by groupCol, regardless of catalog->isMobilityDB -- this used to branch
 * on that flag, using a plain unwrapped call + DISTINCT ON instead on the
 * theory that a segmented MobilityDB table's tiles hold full duplicate
 * copies of each trip rather than disjoint fragments. Empirically false
 * for at least trips_9t (isMobilityDB=true, segmentation=true): its tiles
 * hold genuinely disjoint, clipped fragments -- sum(length(trip)) GROUP BY
 * tripid over it reproduces the true untiled length, while DISTINCT ON
 * would silently keep one arbitrary fragment's partial value instead. This
 * function's only prerequisite (catalog->segmentation being true, checked
 * below) is exactly the condition under which the final op is a real
 * aggregate over real partial values, so it's always correct to use it.
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
RewriteSegmentedDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList,
                              char **explainNotesOut)
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
    StringInfo explainNotes = makeStringInfo();
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

        char *combinerOp = LookupDistFuncCombinerOp(funcName);
        if (explainNotes->len > 0)
            appendStringInfoString(explainNotes, "\n");
        appendStringInfo(explainNotes, "%s(%s): worker=%s, combiner=%s, final=%s",
                         funcName, catalog->distCol, funcName,
                         combinerOp != NULL ? combinerOp : "(none)", finalOp);

        if (selectList->len > 0)
            appendStringInfoString(selectList, ", ");
        /*
         * Always wrapped in the registered final op (e.g. sum) -- NOT
         * conditioned on catalog->isMobilityDB the way this used to read.
         * The MobilityDB branch this replaced assumed a segmented
         * MobilityDB table's tiles hold full duplicate copies of each trip
         * (shape_segmentation.sql's sequence/sequenceset branch doing a
         * bbox-overlap repack with no clipping), so a plain unwrapped call
         * plus DISTINCT ON was enough to dedupe. Empirically false for at
         * least trips_9t (isMobilityDB=true, segmentation=true): its tiles
         * hold genuinely disjoint, clipped fragments -- sum(length(trip))
         * GROUP BY tripid over it reproduces the true untiled length
         * (confirmed against the source table to available floating-point
         * precision), while DISTINCT ON silently picked one arbitrary
         * fragment's partial value instead of the trip's real one. This
         * function's only prerequisite for reaching here at all is
         * catalog->segmentation being true (checked above), which is
         * exactly the condition under which the registered final op is a
         * real aggregate over real partial values -- so it's always
         * correct to wrap in it, regardless of isMobilityDB.
         */
        appendStringInfo(selectList, "%s(%s(%s)) as %s", finalOp, funcName, catalog->distCol,
                         targetEntry->resname != NULL ? targetEntry->resname : funcName);
        foundAny = true;
    }

    if (!foundAny)
        return NULL;

    char *lowered = toLower((char *) query_string);
    char *fromKeyword = FindKeywordToken(lowered, "from");
    if (fromKeyword == NULL)
        return NULL;
    size_t fromOffset = fromKeyword - lowered;

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
    if (FindKeywordToken(loweredTail, "order by") != NULL || FindKeywordToken(loweredTail, "having") != NULL)
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
    const char *boundaryMarkers[] = { "limit", "offset" };
    for (int i = 0; i < 2; i++)
    {
        char *found = FindKeywordToken(loweredTail, boundaryMarkers[i]);
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
    if (explainNotesOut != NULL)
        *explainNotesOut = explainNotes->data;
    return newQuery->data;
}

/*
 * ReplicatedAggregateSearch is expression_tree_walker's context for
 * ReplicatedAggregateWalker: accumulates whether a registered distributed
 * function call and a replicated table's Var were both found somewhere in
 * the same expression subtree.
 */
typedef struct ReplicatedAggregateSearch
{
    Query *parse;
    STMultirelations *tablesList;
    bool foundDistFunc;
    STMultirelation *replicatedTable;
} ReplicatedAggregateSearch;

/*
 * ReplicatedAggregateWalker recurses through an Aggref argument expression
 * (e.g. `length(atTime(t.Trip, p.Period))`) looking for (a) a call to a
 * function registered in pg_dist_spatiotemporal_dist_functions and (b) a
 * Var belonging to a table whose tiles hold full replicated copies rather
 * than disjoint fragments (isMobilityDB && segmentation -- see
 * RewriteSegmentedDistFuncCalls's comment for why those aren't the same as
 * a genuinely segmented table). Both found anywhere in the same subtree
 * means the aggregate's result will be silently multiplied by however many
 * tiles a row happens to touch -- the join/explicit-aggregate
 * generalization of the bug RewriteSegmentedDistFuncCalls fixes for the
 * single-table case.
 */
static bool
ReplicatedAggregateWalker(Node *node, ReplicatedAggregateSearch *search)
{
    if (node == NULL)
        return false;

    if (IsA(node, FuncExpr))
    {
        FuncExpr *funcExpr = (FuncExpr *) node;
        char *funcName = get_func_name(funcExpr->funcid);
        if (funcName != NULL && LookupDistFuncFinalOp(funcName) != NULL)
            search->foundDistFunc = true;
    }
    else if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varno > 0 && (int) var->varno <= list_length(search->parse->rtable))
        {
            RangeTblEntry *rte = rt_fetch(var->varno, search->parse->rtable);
            if (rte->rtekind == RTE_RELATION)
            {
                ListCell *cell;
                foreach(cell, search->tablesList->tables)
                {
                    Rte *rteNode = (Rte *) lfirst(cell);
                    if (rteNode->RteType != STRte)
                        continue;
                    STMultirelation *table = (STMultirelation *) rteNode->rte;
                    if (table->catalogTableInfo.table_oid == rte->relid &&
                        table->catalogTableInfo.isMobilityDB &&
                        table->catalogTableInfo.segmentation)
                    {
                        search->replicatedTable = table;
                        break;
                    }
                }
            }
        }
    }
    return expression_tree_walker(node, ReplicatedAggregateWalker, (void *) search);
}

/* TrimmedSubstring returns a newly palloc'd, whitespace-trimmed copy of the text spanning [start, end). */
static char *
TrimmedSubstring(const char *start, const char *end)
{
    while (start < end && isspace((unsigned char) *start))
        start++;
    while (end > start && isspace((unsigned char) *(end - 1)))
        end--;
    size_t len = end - start;
    char *result = palloc(len + 1);
    memcpy(result, start, len);
    result[len] = '\0';
    return result;
}

/*
 * SplitTopLevelCommas splits text on commas that are not nested inside
 * parentheses, returning a List of palloc'd, whitespace-trimmed C-string
 * chunks in left-to-right order -- used to break a SELECT list's text into
 * one chunk per target-list entry without misreading a comma inside a
 * nested function call (e.g. `atTime(t.Trip, p.Period)`) as a top-level
 * separator.
 */
static List *
SplitTopLevelCommas(const char *text)
{
    List *chunks = NIL;
    int depth = 0;
    const char *chunkStart = text;
    const char *p = text;
    for (; *p; p++)
    {
        if (*p == '(')
            depth++;
        else if (*p == ')')
            depth--;
        else if (*p == ',' && depth == 0)
        {
            chunks = lappend(chunks, TrimmedSubstring(chunkStart, p));
            chunkStart = p + 1;
        }
    }
    chunks = lappend(chunks, TrimmedSubstring(chunkStart, p));
    return chunks;
}

/*
 * RewriteWhereClauseDistFuncCalls detects a bare (non-aggregate) call to a
 * registered distributed function used as a WHERE-clause filter -- e.g.
 * `WHERE length(trip) > 5000` -- over a single shape-segmented distributed
 * spatiotemporal table. Evaluating that per-fragment, the way a plain
 * pushdown would, is wrong: a trip split across several tiles has each
 * fragment see only part of the trajectory, so filtering each fragment
 * independently can both wrongly admit a trip whose *complete* value
 * doesn't actually pass the filter (e.g. one short fragment happens to
 * exceed a length threshold on its own) and wrongly exclude one whose
 * complete value does (no single fragment alone crosses the threshold, only
 * their sum) -- the same fragmentation problem RewriteSegmentedDistFuncCalls
 * fixes for the SELECT-list case, just triggered from the WHERE clause
 * instead.
 *
 * Rewritten into a two-level query: an inner query projecting the caller's
 * original plain (non-aggregate) target-list columns plus the function's
 * properly *combined* value (grouped by the table's own row identifier,
 * same worker/combiner/final technique as RewriteSegmentedDistFuncCalls),
 * and an outer query that filters on that combined value -- moving the
 * filter from a per-row WHERE to a post-combination one, effectively a
 * HAVING -- then re-projects the caller's original target list (so the
 * combined-value column never reaches the client), applying any of the
 * caller's own aggregates (e.g. count(*)) in the outer query, over the
 * now-correctly-filtered rows. E.g.:
 *
 *   select tripid from trips_50t where length(trip) > 5000
 *   -> select tripid from (
 *        select tripid as tripid, sum(length(trip)) as __dmdb_wc_filter
 *        from trips_50t group by tripid
 *      ) as dmdb_wc_sub where __dmdb_wc_filter > 5000::numeric
 *
 *   select count(*) from trips_50t where length(trip) > 5000
 *   -> select count(*) as count from (
 *        select sum(length(trip)) as __dmdb_wc_filter
 *        from trips_50t group by tripid
 *      ) as dmdb_wc_sub where __dmdb_wc_filter > 5000::numeric
 *
 *   select count(distinct tripid) from trips_50t where length(trip) > 5000
 *   -> select count(distinct tripid) as count from (
 *        select tripid as tripid, sum(length(trip)) as __dmdb_wc_filter
 *        from trips_50t group by tripid
 *      ) as dmdb_wc_sub where __dmdb_wc_filter > 5000::numeric
 *
 * A caller's own aggregate is carried into the outer query as-is, and any
 * column it references (other than distCol -- see below) is pulled out
 * and projected into the inner query too, so the outer reference resolves;
 * safe despite the inner query's own GROUP BY groupCol, since groupCol is
 * the table's own primary key and Postgres's functional-dependency rule
 * lets any other same-table column be projected without its own
 * aggregation or GROUP BY entry.
 *
 * Deliberately narrow, matching RewriteSegmentedDistFuncCalls's own scope
 * limits: single table, no join; the *entire* WHERE clause must be exactly
 * one comparison between a bare distfunc call over the table's distCol and
 * a constant (a combined WHERE clause would need its other conjuncts
 * relocated into the inner query's own WHERE, which this first version
 * doesn't attempt -- it bails out to NULL instead of risking a partially
 * wrong rewrite); no pre-existing GROUP BY/ORDER BY/HAVING (this rewrite
 * introduces its own); a caller's aggregate referencing distCol itself
 * (e.g. `count(distinct trip)`) bails out too -- the inner query's per-row
 * value for that column is one fragment's own value, not the trip's real
 * combined one, and this rewrite has no second combination layer to fix
 * that up. Returns NULL when the query doesn't match this shape -- the
 * caller falls back to whatever handling the query would otherwise get.
 */
extern char *
RewriteWhereClauseDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList)
{
    if (tablesList == NULL || tablesList->length != 1)
        return NULL;
    if (parse->groupClause != NIL || parse->sortClause != NIL || parse->havingQual != NULL)
        return NULL;

    Rte *rteNode = (Rte *) linitial(tablesList->tables);
    if (rteNode->RteType != STRte)
        return NULL;

    STMultirelation *table = (STMultirelation *) rteNode->rte;
    STMultirelationCatalog *catalog = &table->catalogTableInfo;
    if (!catalog->segmentation || catalog->groupCol == NULL)
        return NULL;

    List *whereClauseList = WhereClauseList(parse->jointree);
    if (list_length(whereClauseList) != 1)
        return NULL;

    Node *clause = (Node *) linitial(whereClauseList);
    if (!IsA(clause, OpExpr))
        return NULL;

    OpExpr *opExpr = (OpExpr *) clause;
    if (list_length(opExpr->args) != 2)
        return NULL;

    char *opName = get_opname(opExpr->opno);
    if (opName == NULL)
        return NULL;
    bool isComparisonOp = strcmp(opName, "=") == 0 || strcmp(opName, "<>") == 0 ||
                          strcmp(opName, "<") == 0 || strcmp(opName, "<=") == 0 ||
                          strcmp(opName, ">") == 0 || strcmp(opName, ">=") == 0;
    if (!isComparisonOp)
        return NULL;

    Node *leftArg = (Node *) linitial(opExpr->args);
    Node *rightArg = (Node *) lsecond(opExpr->args);

    FuncExpr *funcExpr;
    Const *constArg;
    bool funcOnLeft;
    if (IsA(leftArg, FuncExpr) && IsA(rightArg, Const))
    {
        funcExpr = (FuncExpr *) leftArg;
        constArg = (Const *) rightArg;
        funcOnLeft = true;
    }
    else if (IsA(rightArg, FuncExpr) && IsA(leftArg, Const))
    {
        funcExpr = (FuncExpr *) rightArg;
        constArg = (Const *) leftArg;
        funcOnLeft = false;
    }
    else
        return NULL;

    if (constArg->constisnull)
        return NULL;

    if (list_length(funcExpr->args) != 1 || !IsA(linitial(funcExpr->args), Var))
        return NULL;

    Var *arg = (Var *) linitial(funcExpr->args);
    RangeTblEntry *rte = rt_fetch(arg->varno, parse->rtable);
    char *argColName = get_attname(rte->relid, arg->varattno, false);
    if (argColName == NULL || strcasecmp(argColName, catalog->distCol) != 0)
        return NULL;

    char *funcName = get_func_name(funcExpr->funcid);
    if (funcName == NULL)
        return NULL;
    char *finalOp = LookupDistFuncFinalOp(funcName);
    if (finalOp == NULL)
        return NULL;

    /*
     * Reproduced textually via the type's own output function plus an
     * explicit cast, rather than trying to locate the constant's original
     * source text in query_string -- robust regardless of how the constant
     * was written (5000, '5000', a parameter that already got folded to a
     * Const, etc.); the explicit ::type cast avoids the rewritten literal
     * being parsed back with a different inferred type than the original
     * (same reasoning as the STBOX-literal casts used elsewhere in this
     * extension's dynamic SQL).
     */
    char *constText = DatumToString(constArg->constvalue, constArg->consttype);
    char *constTypeName = format_type_be(constArg->consttype);
    char *constTextWithCast = psprintf("%s::%s", constText, constTypeName);

    /*
     * Locate the SELECT list's own text span, same technique as
     * RewriteReplicatedAggregateQuery: skip leading whitespace, require
     * "select" (no DISTINCT/ALL -- not handled), everything up to " from ".
     */
    char *lowered = toLower((char *) query_string);
    char *cursor = lowered;
    while (isspace((unsigned char) *cursor))
        cursor++;
    if (strncmp(cursor, "select", 6) != 0)
        return NULL;
    cursor += 6;
    while (isspace((unsigned char) *cursor))
        cursor++;
    if (strncmp(cursor, "distinct", 8) == 0 || strncmp(cursor, "all ", 4) == 0)
        return NULL;
    size_t selectListStart = cursor - lowered;

    char *fromKeyword = FindKeywordToken(lowered + selectListStart, "from");
    if (fromKeyword == NULL)
        return NULL;
    size_t selectListEnd = fromKeyword - lowered;

    char *selectListText = TrimmedSubstring(query_string + selectListStart, query_string + selectListEnd);
    List *chunks = SplitTopLevelCommas(selectListText);

    StringInfo innerSelectList = makeStringInfo();
    StringInfo outerSelectList = makeStringInfo();
    /* Column names (C strings) already projected by innerSelectList, so an
     * aggregate referencing the same column twice (or a column a plain
     * target entry already projects under its own alias) doesn't get
     * projected again under a second, redundant alias. */
    List *innerProjectedCols = NIL;

    ListCell *tlCell;
    ListCell *chunkCell = list_head(chunks);
    foreach(tlCell, parse->targetList)
    {
        TargetEntry *te = lfirst(tlCell);
        if (te->resjunk)
            continue;
        /* Chunk count not matching the target list means something about
         * this query's shape wasn't anticipated -- bail out rather than
         * risk pairing the wrong chunk with the wrong entry. */
        if (chunkCell == NULL || te->resname == NULL)
            return NULL;
        char *chunkText = (char *) lfirst(chunkCell);
        chunkCell = lnext(chunks, chunkCell);

        /*
         * An aggregate belongs only in the outer query, over the inner
         * subquery's already-combined-and-filtered rows -- e.g. count(*)
         * needs to count qualifying trips, not run per-tripid-group inside
         * the inner query alongside its own GROUP BY. Any actual column it
         * references (count(tripid), count(distinct tripid), ...) needs to
         * be available from the inner subquery's own output first, though,
         * or the outer reference won't resolve -- pulled out of the
         * aggregate's arguments (recursing through any nested expression,
         * not just a bare Var) and projected into the inner query,
         * deduplicated against what's already there. Safe to add
         * unconditionally despite the inner query's own GROUP BY groupCol:
         * groupCol is the table's own primary key, so Postgres's
         * functional-dependency rule lets any other same-table column be
         * projected without its own aggregation or GROUP BY entry.
         *
         * The one column this can't safely handle is distCol itself (e.g.
         * `count(distinct trip)`): the inner query's per-row value for it
         * is one fragment's own value, not the trip's real combined one,
         * and this rewrite has no second combination layer to fix that up
         * -- bail out rather than risk a plausible-looking wrong answer.
         */
        if (IsA(te->expr, Aggref))
        {
            Aggref *aggref = (Aggref *) te->expr;
            List *aggVars = pull_var_clause((Node *) aggref->args,
                                            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS);
            ListCell *varCell;
            foreach(varCell, aggVars)
            {
                Var *var = (Var *) lfirst(varCell);
                if (var->varno <= 0 || (int) var->varno > list_length(parse->rtable))
                    return NULL;
                RangeTblEntry *varRte = rt_fetch(var->varno, parse->rtable);
                if (varRte->rtekind != RTE_RELATION)
                    return NULL;
                char *varColName = get_attname(varRte->relid, var->varattno, false);
                if (varColName == NULL)
                    return NULL;
                if (strcasecmp(varColName, catalog->distCol) == 0)
                    return NULL;

                bool alreadyProjected = false;
                ListCell *projCell;
                foreach(projCell, innerProjectedCols)
                {
                    if (strcasecmp((char *) lfirst(projCell), varColName) == 0)
                    {
                        alreadyProjected = true;
                        break;
                    }
                }
                if (!alreadyProjected)
                {
                    if (innerSelectList->len > 0)
                        appendStringInfoString(innerSelectList, ", ");
                    appendStringInfo(innerSelectList, "%s as %s", varColName, varColName);
                    innerProjectedCols = lappend(innerProjectedCols, varColName);
                }
            }

            if (outerSelectList->len > 0)
                appendStringInfoString(outerSelectList, ", ");
            appendStringInfo(outerSelectList, "%s as %s", chunkText, te->resname);
            continue;
        }

        if (innerSelectList->len > 0)
            appendStringInfoString(innerSelectList, ", ");
        appendStringInfo(innerSelectList, "%s as %s", chunkText, te->resname);
        innerProjectedCols = lappend(innerProjectedCols, te->resname);
        if (outerSelectList->len > 0)
            appendStringInfoString(outerSelectList, ", ");
        appendStringInfoString(outerSelectList, te->resname);
    }
    if (chunkCell != NULL || outerSelectList->len == 0)
        return NULL;

    /*
     * Always wrap in the registered final op and GROUP BY groupCol -- NOT
     * conditioned on catalog->isMobilityDB the way RewriteSegmentedDistFunc
     * Calls's SELECT-list rewrite is (its doc comment claims a
     * segmented+isMobilityDB table's tiles hold full duplicate copies, so a
     * plain unwrapped call plus DISTINCT ON suffices). Empirically false for
     * at least trips_9t (isMobilityDB=true, segmentation=true): its tiles
     * hold genuinely disjoint, clipped fragments -- sum(length(trip)) GROUP
     * BY tripid over it reproduces the true untiled length (confirmed
     * against the source table to the available floating-point precision),
     * while any single fragment's raw length() is far smaller. A DISTINCT
     * ON here would silently filter on one arbitrary fragment's partial
     * value instead of the trip's real one. RewriteSegmentedDistFuncCalls's
     * own DISTINCT ON path likely has the same bug for this same data
     * shape, but that's a pre-existing SELECT-list issue left alone here --
     * out of scope for this WHERE-clause rewrite.
     */
    StringInfo filterExpr = makeStringInfo();
    appendStringInfo(filterExpr, "%s(%s(%s))", finalOp, funcName, catalog->distCol);

    /*
     * Preserve the FROM clause text verbatim (same "find ' from '"
     * technique used throughout this file), stopping at the query's own
     * WHERE keyword -- the single predicate this rewrite already extracted
     * (confirmed above to be the *entire* WHERE clause) is what's being
     * relocated, so nothing of it needs to survive into the inner query.
     */
    size_t fromOffset = fromKeyword - lowered;
    char *loweredTail = lowered + fromOffset;
    char *whereKeyword = FindKeywordToken(loweredTail, "where");
    if (whereKeyword == NULL)
        return NULL;
    size_t fromTextLen = whereKeyword - loweredTail;
    char *fromText = palloc(fromTextLen + 1);
    memcpy(fromText, query_string + fromOffset, fromTextLen);
    fromText[fromTextLen] = '\0';
    while (fromTextLen > 0 && (fromText[fromTextLen - 1] == ';' || isspace((unsigned char) fromText[fromTextLen - 1])))
        fromText[--fromTextLen] = '\0';

    /*
     * innerSelectList can be empty -- e.g. `SELECT count(*) FROM ... WHERE
     * length(trip) > 5000` has no plain column, only the aggregate (routed
     * to outerSelectList above) -- in which case a leading ", " would
     * produce invalid SQL ("select , sum(...) as __dmdb_wc_filter ...").
     */
    StringInfo innerQuery = makeStringInfo();
    if (innerSelectList->len > 0)
        appendStringInfo(innerQuery, "select %s, %s as __dmdb_wc_filter %s group by %s",
                         innerSelectList->data, filterExpr->data, fromText, catalog->groupCol);
    else
        appendStringInfo(innerQuery, "select %s as __dmdb_wc_filter %s group by %s",
                         filterExpr->data, fromText, catalog->groupCol);

    StringInfo outerQuery = makeStringInfo();
    appendStringInfo(outerQuery, "select %s from (%s) as dmdb_wc_sub where %s %s %s",
                     outerSelectList->data, innerQuery->data,
                     funcOnLeft ? "__dmdb_wc_filter" : constTextWithCast,
                     opName,
                     funcOnLeft ? constTextWithCast : "__dmdb_wc_filter");
    return outerQuery->data;
}

/*
 * RewriteReplicatedAggregateQuery detects an explicit aggregate (e.g.
 * `SUM(length(atTime(t.Trip, p.Period)))`) whose argument references both
 * a registered distributed function and a replicated table's column, and
 * rewrites the query into a two-level shape: an inner query that
 * DISTINCT-deduplicates on the replicated table's own row identifier
 * (groupCol) before computing the aggregate's argument once per row, and
 * an outer query that applies the original aggregate/GROUP BY over that
 * already-deduplicated result, e.g.:
 *
 *   select l.licence, p.periodid, p.period, sum(length(atTime(t.trip, p.period))) as dist
 *   from trips_16t t, licences1 l, periods1 p
 *   where t.vehicleid = l.vehicleid and t.trip && p.period
 *   group by l.licence, p.periodid, p.period
 *   ->
 *   select licence, periodid, period, sum(agg_input) as dist
 *   from (
 *     select distinct tripid, l.licence as licence, p.periodid as periodid, p.period as period,
 *       length(atTime(t.trip, p.period)) as agg_input
 *     from trips_16t t, licences1 l, periods1 p
 *     where t.vehicleid = l.vehicleid and t.trip && p.period
 *   ) as dedup_agg
 *   group by licence, periodid, period
 *
 * Without this, a row belonging to a trip replicated across N tiles
 * contributes to the SUM N times (once per tile) instead of once, silently
 * multiplying the result -- confirmed empirically comparing BerlinMOD Q8
 * against trips_16t vs. the non-distributed source table `trips` (~4.3x
 * inflated, matching how many tiles the affected trips' bboxes touch).
 *
 * Deliberately narrow: only triggers when there's a GROUP BY (an
 * ungrouped, single-aggregate query is already handled correctly by
 * Citus' own native aggregate pushdown, same as `sum(length(trip)) as
 * length` without a GROUP BY); only the first qualifying Aggref found is
 * rewritten (a target list combining several different replicated
 * aggregates isn't handled); SELECT DISTINCT/ALL, HAVING isn't supported
 * (its own expression could reference the pre-rewrite raw aggregate,
 * unsafe to carry over as-is); and an ORDER BY referencing anything other
 * than a plain target-list entry, or containing DESC, bails out rather
 * than risk getting the rebuilt ORDER BY wrong. All these return NULL,
 * falling back to the pre-existing (still-incorrect-for-this-case)
 * behavior rather than risking broken or silently-wrong SQL.
 *
 * Expression text is extracted by splitting the SELECT list's own text on
 * top-level commas (SplitTopLevelCommas) and matching each resulting chunk
 * positionally against parse->targetList -- guaranteed to line up 1:1 in
 * the same left-to-right order by SQL's own parsing rules -- rather than
 * deparsing each target entry's expression via a locally-planned
 * statement: calling standard_planner() from this nested position inside
 * our own already-executing planner_hook segfaulted the backend
 * (reproduced reliably on this exact query), so this avoids calling into
 * Postgres' planner internals at all.
 */
extern char *
RewriteReplicatedAggregateQuery(Query *parse, const char *query_string, STMultirelations *tablesList)
{
    if (parse->groupClause == NIL)
        return NULL;

    TargetEntry *aggTargetEntry = NULL;
    STMultirelation *replicatedTable = NULL;

    ListCell *tlCell;
    foreach(tlCell, parse->targetList)
    {
        TargetEntry *te = lfirst(tlCell);
        if (te->resjunk || !IsA(te->expr, Aggref))
            continue;

        Aggref *aggref = (Aggref *) te->expr;
        ReplicatedAggregateSearch search;
        search.parse = parse;
        search.tablesList = tablesList;
        search.foundDistFunc = false;
        search.replicatedTable = NULL;

        ListCell *argCell;
        foreach(argCell, aggref->args)
        {
            TargetEntry *argTe = (TargetEntry *) lfirst(argCell);
            ReplicatedAggregateWalker((Node *) argTe->expr, &search);
        }

        if (search.foundDistFunc && search.replicatedTable != NULL)
        {
            aggTargetEntry = te;
            replicatedTable = search.replicatedTable;
            break;
        }
    }

    if (aggTargetEntry == NULL || replicatedTable->catalogTableInfo.groupCol == NULL)
        return NULL;

    Aggref *aggref = (Aggref *) aggTargetEntry->expr;
    if (list_length(aggref->args) != 1)
        return NULL;

    char *aggFuncName = get_func_name(aggref->aggfnoid);
    if (aggFuncName == NULL)
        return NULL;

    /*
     * Locate the SELECT list's own text span: skip leading whitespace,
     * require the query to start with "select" (bail out on anything
     * before it, e.g. a leading CTE -- not handled by this rewrite), skip
     * DISTINCT/ALL (not handled either), then take everything up to the
     * query's own " from " keyword.
     */
    char *lowered = toLower((char *) query_string);
    char *cursor = lowered;
    while (isspace((unsigned char) *cursor))
        cursor++;
    if (strncmp(cursor, "select", 6) != 0)
        return NULL;
    cursor += 6;
    while (isspace((unsigned char) *cursor))
        cursor++;
    if (strncmp(cursor, "distinct", 8) == 0 || strncmp(cursor, "all ", 4) == 0)
        return NULL;
    size_t selectListStart = cursor - lowered;

    char *fromKeyword = FindKeywordToken(lowered + selectListStart, "from");
    if (fromKeyword == NULL)
        return NULL;
    size_t selectListEnd = fromKeyword - lowered;

    char *selectListText = TrimmedSubstring(query_string + selectListStart, query_string + selectListEnd);
    List *chunks = SplitTopLevelCommas(selectListText);

    StringInfo innerSelectList = makeStringInfo();
    StringInfo outerSelectList = makeStringInfo();
    StringInfo groupByList = makeStringInfo();
    char *aggArgText = NULL;

    /*
     * groupCol is always needed in the inner DISTINCT to safely disambiguate
     * two different trips that happen to produce identical output on every
     * *other* projected column (e.g. same licence/period/length by
     * coincidence) -- but only added here if the caller's own target list
     * doesn't already project it explicitly (e.g. `SELECT t.tripid, ...`);
     * otherwise the inner subquery would end up with two same-named output
     * columns, making that name ambiguous from the outer query.
     */
    bool groupColProjected = false;
    foreach(tlCell, parse->targetList)
    {
        TargetEntry *te = lfirst(tlCell);
        if (te->resjunk || te == aggTargetEntry)
            continue;
        if (TargetEntryReferencesGroupCol(te, parse, replicatedTable))
        {
            groupColProjected = true;
            break;
        }
    }
    if (!groupColProjected)
        appendStringInfo(innerSelectList, "%s", replicatedTable->catalogTableInfo.groupCol);

    ListCell *chunkCell = list_head(chunks);
    foreach(tlCell, parse->targetList)
    {
        TargetEntry *te = lfirst(tlCell);
        if (te->resjunk)
            continue;
        /* Chunk count not matching the target list means something about
         * this query's shape wasn't anticipated -- bail out rather than
         * risk pairing the wrong chunk with the wrong entry. */
        if (chunkCell == NULL)
            return NULL;
        char *chunkText = (char *) lfirst(chunkCell);
        chunkCell = lnext(chunks, chunkCell);

        if (te == aggTargetEntry)
        {
            /* chunkText looks like "<aggFuncName>( ... ) [AS alias]" -- an
             * explicit alias on the aggregate itself (as opposed to relying
             * on its resname) leaves trailing text after the wrapper's own
             * closing paren, so its end can't just be assumed to be the
             * chunk's last character; walk paren depth from the opening '('
             * to find the *matching* close instead (the argument itself
             * nests parens, e.g. length(atTime(t.Trip, p.Period))), then
             * ignore anything after it -- the alias is already known via
             * te->resname regardless of what this chunk spells it as. */
            size_t nameLen = strlen(aggFuncName);
            char *chunkLower = toLower(chunkText);
            if (strncmp(chunkLower, aggFuncName, nameLen) != 0)
                return NULL;
            char *afterName = chunkText + nameLen;
            while (isspace((unsigned char) *afterName))
                afterName++;
            if (*afterName != '(')
                return NULL;
            char *argStart = afterName + 1;
            char *scan = argStart;
            int depth = 1;
            while (*scan != '\0' && depth > 0)
            {
                if (*scan == '(')
                    depth++;
                else if (*scan == ')')
                    depth--;
                if (depth > 0)
                    scan++;
            }
            if (depth != 0)
                return NULL;
            aggArgText = TrimmedSubstring(argStart, scan);
            continue;
        }

        if (te->resname == NULL)
            return NULL;

        if (innerSelectList->len > 0)
            appendStringInfoString(innerSelectList, ", ");
        appendStringInfo(innerSelectList, "%s as %s", chunkText, te->resname);
        if (outerSelectList->len > 0)
            appendStringInfoString(outerSelectList, ", ");
        appendStringInfoString(outerSelectList, te->resname);
        if (groupByList->len > 0)
            appendStringInfoString(groupByList, ", ");
        appendStringInfoString(groupByList, te->resname);
    }
    /* Leftover chunks, or the aggregate's own chunk never matched, means
     * the chunk count didn't line up with the target list -- bail out
     * rather than risk a silently wrong rewrite. */
    if (chunkCell != NULL || aggArgText == NULL)
        return NULL;
    appendStringInfo(innerSelectList, ", %s as agg_input", aggArgText);

    /*
     * Preserve the FROM/WHERE clause text verbatim (same "find ' from '"
     * technique as RewriteSegmentedDistFuncCalls), up to the query's own
     * GROUP BY keyword -- that clause's *column list* is discarded, since
     * it's already been rebuilt above from the target list's own
     * (resname-aliased) expressions, avoiding any ambiguity between a
     * GROUP BY entry written as `l.Licence` and its output alias
     * `licence`.
     */
    size_t fromOffset = fromKeyword - lowered;

    char *loweredTail = lowered + fromOffset;
    char *groupByKeyword = FindKeywordToken(loweredTail, "group by");
    if (groupByKeyword == NULL)
        return NULL;
    size_t fromWhereLen = groupByKeyword - loweredTail;

    char *fromWhereText = palloc(fromWhereLen + 1);
    memcpy(fromWhereText, query_string + fromOffset, fromWhereLen);
    fromWhereText[fromWhereLen] = '\0';

    if (FindKeywordToken(groupByKeyword, "having") != NULL)
        return NULL;

    /*
     * ORDER BY, if present, is rebuilt from parse->sortClause (matching
     * each SortGroupClause's tleSortGroupRef back to the target entry it
     * sorts by, then using that entry's own alias) rather than carried
     * over as text -- the original text can reference a target entry via
     * its *input* expression (e.g. `ORDER BY l.Licence`), which won't
     * resolve against the rewritten outer query's plain-aliased columns
     * (`licence`). DESC (or any other case this rebuild can't faithfully
     * reproduce) bails out rather than risk silently reordering results
     * wrong; LIMIT/OFFSET don't reference columns at all, so are safe to
     * carry over as plain text.
     */
    StringInfo outerSuffix = makeStringInfo();
    if (parse->sortClause != NIL)
    {
        char *afterGroupBy = groupByKeyword + strlen("group by");
        char *orderByPos = FindKeywordToken(afterGroupBy, "order by");
        if (orderByPos == NULL)
            return NULL;
        char *orderByEnd = NULL;
        char *limitPos = FindKeywordToken(orderByPos, "limit");
        char *offsetPos = FindKeywordToken(orderByPos, "offset");
        if (limitPos != NULL && (orderByEnd == NULL || limitPos < orderByEnd))
            orderByEnd = limitPos;
        if (offsetPos != NULL && (orderByEnd == NULL || offsetPos < orderByEnd))
            orderByEnd = offsetPos;
        size_t orderByTextLen = orderByEnd != NULL ? (size_t) (orderByEnd - orderByPos) : strlen(orderByPos);
        char *orderByText = palloc(orderByTextLen + 1);
        memcpy(orderByText, orderByPos, orderByTextLen);
        orderByText[orderByTextLen] = '\0';
        if (strstr(orderByText, "desc") != NULL)
            return NULL;

        StringInfo rebuiltOrderBy = makeStringInfo();
        ListCell *sortCell;
        foreach(sortCell, parse->sortClause)
        {
            SortGroupClause *sortClause = (SortGroupClause *) lfirst(sortCell);
            TargetEntry *matchedTe = NULL;
            foreach(tlCell, parse->targetList)
            {
                TargetEntry *te = lfirst(tlCell);
                if (!te->resjunk && te->ressortgroupref == sortClause->tleSortGroupRef)
                {
                    matchedTe = te;
                    break;
                }
            }
            if (matchedTe == NULL || matchedTe->resname == NULL)
                return NULL;
            if (rebuiltOrderBy->len > 0)
                appendStringInfoString(rebuiltOrderBy, ", ");
            appendStringInfoString(rebuiltOrderBy, matchedTe->resname);
        }
        appendStringInfo(outerSuffix, " order by %s", rebuiltOrderBy->data);
        if (orderByEnd != NULL)
            appendStringInfo(outerSuffix, " %s", orderByEnd);
    }
    else
    {
        char *afterGroupBy = groupByKeyword + strlen("group by");
        char *limitPos = FindKeywordToken(afterGroupBy, "limit");
        char *offsetPos = FindKeywordToken(afterGroupBy, "offset");
        char *tailStart = NULL;
        if (limitPos != NULL && (tailStart == NULL || limitPos < tailStart))
            tailStart = limitPos;
        if (offsetPos != NULL && (tailStart == NULL || offsetPos < tailStart))
            tailStart = offsetPos;
        if (tailStart != NULL)
            appendStringInfo(outerSuffix, " %s", tailStart);
    }

    /* Strip a trailing ";"/whitespace so the constructed query stays valid SQL. */
    if (outerSuffix->len > 0)
    {
        size_t suffixLen = outerSuffix->len;
        while (suffixLen > 0 && (outerSuffix->data[suffixLen - 1] == ';' ||
                                 isspace((unsigned char) outerSuffix->data[suffixLen - 1])))
            suffixLen--;
        outerSuffix->data[suffixLen] = '\0';
    }
    else
    {
        size_t fwLen = strlen(fromWhereText);
        while (fwLen > 0 && (fromWhereText[fwLen - 1] == ';' || isspace((unsigned char) fromWhereText[fwLen - 1])))
            fwLen--;
        fromWhereText[fwLen] = '\0';
    }

    StringInfo innerQuery = makeStringInfo();
    /* fromWhereText already starts with "from " (see fromOffset above), so
     * no literal "from" is added here -- doing so produced "... from from
     * trips_16t ..." as invalid SQL. */
    appendStringInfo(innerQuery, "select distinct %s %s", innerSelectList->data, fromWhereText);

    StringInfo outerQuery = makeStringInfo();
    appendStringInfo(outerQuery, "select %s, %s(agg_input) as %s from (%s) as dedup_agg group by %s%s",
                     outerSelectList->data, aggFuncName, aggTargetEntry->resname,
                     innerQuery->data, groupByList->data, outerSuffix->data);
    return outerQuery->data;
}

/*
 * FindCTEBodySpan locates cteName's own body text -- the span strictly
 * between its "AS (" and the matching close paren -- within query_string.
 * Uses the same whitespace-tolerant (FindKeywordToken) and paren-depth
 * (walked manually here, since the open paren itself is what's being
 * searched for, not a keyword) techniques used throughout this file.
 * Returns true and sets *bodyStart/*bodyEnd (pointers into query_string;
 * [start, end) exclusive of the parens themselves) on success; false if
 * "<cteName> AS (" couldn't be located, or its parens are unbalanced.
 */
static bool
FindCTEBodySpan(const char *query_string, const char *cteName, const char **bodyStart, const char **bodyEnd)
{
    char *lowered = toLower((char *) query_string);
    char *loweredCteName = toLower((char *) cteName);
    char *cursor = lowered;
    while ((cursor = FindKeywordToken(cursor, loweredCteName)) != NULL)
    {
        char *afterName = cursor + strlen(loweredCteName);
        while (isspace((unsigned char) *afterName))
            afterName++;
        /* Only accept "as" immediately following the name (mere whitespace
         * in between, nothing else) -- a bare cteName match elsewhere in
         * the string (e.g. inside the CTE's own body, referencing itself,
         * or in a comment) isn't this CTE's own "<name> AS (" definition. */
        bool isAsKeyword = strncmp(afterName, "as", 2) == 0 &&
                            (isspace((unsigned char) afterName[2]) || afterName[2] == '(');
        if (isAsKeyword)
        {
            char *afterAs = afterName + 2;
            while (isspace((unsigned char) *afterAs))
                afterAs++;
            if (*afterAs == '(')
            {
                char *scan = afterAs + 1;
                int depth = 1;
                while (*scan != '\0' && depth > 0)
                {
                    if (*scan == '(')
                        depth++;
                    else if (*scan == ')')
                        depth--;
                    if (depth > 0)
                        scan++;
                }
                if (depth == 0)
                {
                    *bodyStart = query_string + ((afterAs + 1) - lowered);
                    *bodyEnd = query_string + (scan - lowered);
                    return true;
                }
            }
        }
        cursor++;
    }
    return false;
}

/*
 * RewriteReplicatedAggregateInCTEs generalizes RewriteReplicatedAggregateQuery
 * to reach *inside* a query's own CTEs -- e.g.
 *   WITH Distances AS (
 *     SELECT p.PeriodId, p.Period, t.VehicleId,
 *       SUM(length(atTime(t.Trip, p.Period))) AS Dist
 *     FROM trips_16t t, periods_ref p
 *     WHERE t.Trip && p.Period
 *     GROUP BY p.PeriodId, p.Period, t.VehicleId
 *   )
 *   SELECT PeriodId, Period, MAX(Dist) AS MaxDist FROM Distances
 *   GROUP BY PeriodId, Period ORDER BY PeriodId
 * has the exact same replication-overcounting problem as BerlinMOD Q8
 * (SUM(length(atTime(...))) over a replicated table, per-tile-copy
 * overcounted instead of deduplicated) -- just one level down, inside the
 * CTE, rather than at the top level. RewriteReplicatedAggregateQuery only
 * ever looked at the top-level query's own targetList/groupClause, so it
 * never saw this shape at all (the top level's own aggregate, MAX(Dist),
 * takes a plain Var on the CTE's *output* column -- no distributed
 * function call in sight from up there).
 *
 * For each of parse's CTEs (in order, so a later CTE's own body span is
 * always located within the *already-rewritten* text if an earlier one
 * changed), locates that CTE's own body text (FindCTEBodySpan) and runs
 * the existing single-query rewrite against it (cte->ctequery is a
 * complete, independent Query, so RewriteReplicatedAggregateQuery's
 * existing logic -- which was already written generically against
 * whatever Query/text pair it's handed, not hardcoded to the top level --
 * applies unchanged); if it rewrote anything, splices the rewritten body
 * text back in place of the original and keeps going. Returns the final
 * combined query_string if any CTE was rewritten, NULL if none needed it
 * (or parse has no CTEs at all).
 */
extern char *
RewriteReplicatedAggregateInCTEs(Query *parse, const char *query_string, STMultirelations *tablesList)
{
    if (parse->cteList == NIL)
        return NULL;

    char *currentQueryString = NULL;
    bool rewroteAny = false;

    ListCell *cteCell;
    foreach(cteCell, parse->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
        if (!IsA(cte->ctequery, Query))
            continue;
        Query *cteQuery = (Query *) cte->ctequery;

        const char *base = currentQueryString != NULL ? currentQueryString : query_string;
        const char *bodyStart, *bodyEnd;
        if (!FindCTEBodySpan(base, cte->ctename, &bodyStart, &bodyEnd))
            continue;

        char *cteBodyText = TrimmedSubstring(bodyStart, bodyEnd);
        char *rewrittenBody = RewriteReplicatedAggregateQuery(cteQuery, cteBodyText, tablesList);
        if (rewrittenBody == NULL)
            continue;

        StringInfo combined = makeStringInfo();
        appendBinaryStringInfo(combined, base, bodyStart - base);
        appendStringInfoString(combined, rewrittenBody);
        appendStringInfoString(combined, bodyEnd);
        currentQueryString = combined->data;
        rewroteAny = true;
    }
    return rewroteAny ? currentQueryString : NULL;
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

