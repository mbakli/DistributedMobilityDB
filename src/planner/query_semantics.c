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
 *   -> select sum(length(trip)) as length from trips_50t group by tripid              -- genuinely segmented (PostGIS linestring/polygon)
 *   -> select distinct on (tripid) length(trip) as length from trips_50t order by tripid  -- replicated (MobilityDB tgeompoint)
 *
 * so Citus' own native distributed GROUP BY/aggregate pushdown -- already
 * proven correct for `sum(length(trip)) as length` without a GROUP BY --
 * combines each trip's per-tile fragments into one row per trip, instead
 * of every row silently collapsing into a single global total (see
 * IsDistFunc's comment for why a bare call collides with that mechanism in
 * the first place, and why it must not be the one to handle this case).
 *
 * Which shape is used depends on whether the table's tiles genuinely hold
 * disjoint fragments or full duplicate copies -- see the isMobilityDB
 * check below for why those aren't the same thing despite both being
 * flagged "segmented" in the catalog. A genuinely segmented table's final
 * op (the registered combining function, e.g. sum) is a real aggregate
 * over real partial values, so GROUP BY + that op is correct. A
 * replicated table has nothing to combine -- every "fragment" already is
 * the complete, correct answer -- so no distributed-function machinery
 * applies there at all; the plain, ordinary function call is kept as-is
 * and DISTINCT ON just removes the duplicate rows.
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
         * shape_segmentation.sql's ST_Intersection-based clipping only
         * applies to its 'linestring'/'multilinestring'/'polygon'/
         * 'multipolygon' branches -- static PostGIS geometry. Its
         * 'sequence'/'sequenceset' branch (every MobilityDB tgeompoint
         * trajectory table this extension has, since a moving point's
         * shape type is never one of those PostGIS types) does no
         * clipping at all: it just re-packs the *whole*, unsplit trip
         * into every tile whose bbox it overlaps (`WHERE distCol &&
         * bbox_with_srid` is a bbox-overlap test, not a cut). Confirmed
         * empirically: every "fragment" of a multi-tile trip carries
         * identical numinstants/startTimestamp/endTimestamp/length -- full
         * duplicates, not disjoint partial pieces.
         *
         * A genuinely segmented table's registered final op (e.g. sum) is
         * a real aggregate over real partial values, so wrap the call in
         * it as usual. A replicated (MobilityDB) table has nothing to
         * combine -- summing duplicates would multiply the true value by
         * however many tiles a trip touches -- so the plain, ordinary
         * function call is projected completely unwrapped; the DISTINCT
         * ON built into the final query (below) removes the duplicate
         * rows without needing any aggregate at all.
         */
        if (catalog->isMobilityDB)
            appendStringInfo(selectList, "%s(%s) as %s", funcName, catalog->distCol,
                             targetEntry->resname != NULL ? targetEntry->resname : funcName);
        else
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
    if (catalog->isMobilityDB)
        /*
         * DISTINCT ON (groupcol) ... ORDER BY groupcol removes the
         * duplicate replica rows without wrapping anything in an
         * aggregate -- Postgres only requires ORDER BY to *start* with
         * the DISTINCT ON expression(s), not that they also appear in the
         * SELECT list, so groupcol still isn't forced into the output
         * unless the caller's own target list already asked for it.
         */
        appendStringInfo(newQuery, "select distinct on (%s) %s %s order by %s %s",
                         catalog->groupCol, selectList->data, core, catalog->groupCol, suffix);
    else
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

