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

static char *BuildRecombinedFuncCall(const char *finalOp, const char *combinerOp, const char *funcName, const char *distCol);
static bool ExtractDistFuncCore(Node *expr, List *rtable, STMultirelationCatalog *catalog,
                                char **coreFuncName, char **coreFinalOp, char **coreCombinerOp,
                                List **wrapperFuncNames);

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
 * one aggregated row and silently drops every other column (e.g. a
 * distance-predicate join projecting both sides' row identifiers alongside
 * a registered function call returned only the function's own result,
 * dropping the identifiers). Only engage the distributed-function rewrite
 * when every projected column is itself a registered function, matching
 * the single-aggregate queries it's actually designed for; otherwise leave
 * the functions as plain
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
 * ExtractDistFuncCore walks a chain of nested unary function calls (e.g.
 * `numinstants(cumulativelength(trip))`) down to its innermost call, looking
 * for a registered distributed function invoked directly on catalog's own
 * distCol at the bottom of the chain. Every function call encountered along
 * the way down that isn't itself that bottom call is just a plain scalar
 * wrapper meant to run against the bottom call's *combined* result, not a
 * distributed function of its own -- e.g. numinstants() above only ever
 * needs to see cumulativeLength's one true, already-reconstructed value,
 * never a per-fragment partial one, so it must be applied after
 * recombination rather than distributed itself.
 *
 * On success returns true and sets coreFuncName/coreFinalOp/coreCombinerOp
 * to the bottom call's own registration (as looked up via
 * LookupDistFuncFinalOp/LookupDistFuncCombinerOp) and wrapperFuncNames to
 * the outer function names in innermost-to-outermost order -- i.e. the
 * order to re-wrap them around the recombined core call in. Returns false
 * (leaving the out-params untouched) for anything else: not a FuncExpr, more
 * than one argument anywhere in the chain, or a Var at the bottom that isn't
 * distCol or isn't itself registered.
 */
static bool
ExtractDistFuncCore(Node *expr, List *rtable, STMultirelationCatalog *catalog,
                    char **coreFuncName, char **coreFinalOp, char **coreCombinerOp,
                    List **wrapperFuncNames)
{
    if (!IsA(expr, FuncExpr))
        return false;

    FuncExpr *funcExpr = (FuncExpr *) expr;
    if (list_length(funcExpr->args) != 1)
        return false;

    char *funcName = get_func_name(funcExpr->funcid);
    Node *arg = (Node *) linitial(funcExpr->args);

    if (IsA(arg, Var))
    {
        Var *var = (Var *) arg;
        RangeTblEntry *rte = rt_fetch(var->varno, rtable);
        char *argColName = get_attname(rte->relid, var->varattno, false);
        if (argColName == NULL || strcasecmp(argColName, catalog->distCol) != 0)
            return false;

        char *finalOp = LookupDistFuncFinalOp(funcName);
        char *combinerOp = LookupDistFuncCombinerOp(funcName);
        if (finalOp == NULL && combinerOp == NULL)
            return false;

        *coreFuncName = funcName;
        *coreFinalOp = finalOp;
        *coreCombinerOp = combinerOp;
        *wrapperFuncNames = NIL;
        return true;
    }

    if (!ExtractDistFuncCore(arg, rtable, catalog, coreFuncName, coreFinalOp, coreCombinerOp, wrapperFuncNames))
        return false;

    *wrapperFuncNames = lappend(*wrapperFuncNames, funcName);
    return true;
}

/*
 * RewriteSegmentedDistFuncCalls detects a bare (non-aggregate) call to a
 * registered distributed function -- e.g. `length(trip)`, no sum()/
 * aggregate wrapper -- over a single shape-segmented distributed
 * spatiotemporal table, and returns a rewritten query string turning it
 * into an ordinary SQL grouped aggregate: the bare call is wrapped in the
 * table's registered final op and a GROUP BY on the table's row identifier
 * is added, so Citus' own native distributed GROUP BY/aggregate pushdown --
 * already proven correct for a single aggregate with no GROUP BY --
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
 * for a genuinely-clipped table (isMobilityDB=true, segmentation=true): its tiles
 * hold genuinely disjoint, clipped fragments -- sum(length(trip)) GROUP BY
 * tripid over it reproduces the true untiled length, while DISTINCT ON
 * would silently keep one arbitrary fragment's partial value instead. This
 * function's only prerequisite (catalog->segmentation being true, checked
 * below) is exactly the condition under which the final op is a real
 * aggregate over real partial values, so it's always correct to use it.
 *
 * GROUP BY doesn't require its key to be projected, so groupCol is never
 * added to the SELECT list on its own -- if the caller wants it in the
 * output, to tell which trip a row belongs to when running an unfiltered
 * query over many trips, it must already be there in the original target
 * list;
 * an explicit reference to it is recognized and passed through as-is
 * rather than triggering a bail-out. Any *other* plain column reference
 * isn't something this rewrite knows how to group by safely, so it still
 * bails out (falls back to the pre-existing per-fragment-pushdown
 * behavior) in that case.
 *
 * Deliberately scoped to a single table with no join, and a target list
 * made up solely of bare distributed-function calls (plus, optionally, an
 * explicit groupCol reference) over that table's own spatiotemporal
 * column: that covers a bare distributed-function call over any
 * distributed table without the complexity of a general
 * arbitrary-target-list/join rewrite. Returns NULL when there's nothing to
 * rewrite (not a single-table query, table isn't segmented, no groupCol on
 * record, the target list isn't purely bare distfunc calls over that
 * column, or the query has no textual " from " to anchor on) -- the caller
 * should keep using the original query_string in that case.
 */
extern char *
RewriteSegmentedDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList,
                              char **explainNotesOut, char **postProcessingNotesOut)
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
    List *postProcessingFuncNames = NIL;
    bool foundAny = false;

    ListCell *targetEntryCell = NULL;
    foreach(targetEntryCell, parse->targetList)
    {
        TargetEntry *targetEntry = lfirst(targetEntryCell);
        if (targetEntry->resjunk)
            continue;

        /*
         * A plain reference to the table's own group column, projected
         * explicitly alongside a distributed-function call rather than
         * relying on some implicit projection, is passed through as-is
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

        char *funcName = NULL;
        char *finalOp = NULL;
        char *combinerOp = NULL;
        List *wrapperFuncNames = NIL;
        if (!ExtractDistFuncCore((Node *) targetEntry->expr, parse->rtable, catalog,
                                 &funcName, &finalOp, &combinerOp, &wrapperFuncNames))
            return NULL;

        /*
         * Always recombined via the registered final op -- NOT
         * conditioned on catalog->isMobilityDB the way this used to read.
         * The MobilityDB branch this replaced assumed a segmented
         * MobilityDB table's tiles hold full duplicate copies of each trip
         * (shape_segmentation.sql's sequence/sequenceset branch doing a
         * bbox-overlap repack with no clipping), so a plain unwrapped call
         * plus DISTINCT ON was enough to dedupe. Empirically false for a
         * genuinely-clipped table (isMobilityDB=true, segmentation=true): its tiles
         * hold genuinely disjoint, clipped fragments -- sum(length(trip))
         * GROUP BY tripid over it reproduces the true untiled length
         * (confirmed against the source table to available floating-point
         * precision), while DISTINCT ON silently picked one arbitrary
         * fragment's partial value instead of the trip's real one. This
         * function's only prerequisite for reaching here at all is
         * catalog->segmentation being true (checked above), which is
         * exactly the condition under which the registered final op is a
         * real recombination over real partial values -- so it's always
         * correct to recombine with it, regardless of isMobilityDB. See
         * BuildRecombinedFuncCall for why the composition order itself
         * depends on which kind of final op this particular function is
         * registered with.
         *
         * wrapperFuncNames holds any plain scalar functions composed
         * *around* the registered call (e.g. numinstants() around
         * cumulativeLength()) -- applied here, in order, around the
         * already-recombined core call, so they only ever see the trip's
         * one true combined value rather than a per-fragment partial one.
         * They're deliberately kept out of this call's own EXPLAIN line
         * below (and out of the "Distributed functions" heading
         * altogether): they're not registered distributed functions
         * themselves and have no worker/combiner/final of their own to
         * report -- folding them in there would misleadingly read as part
         * of the *registered* function's own registration. Collected into
         * postProcessingFuncNames instead, for its own "Post processing
         * functions" heading further down.
         */
        char *recombined = BuildRecombinedFuncCall(finalOp, combinerOp, funcName, catalog->distCol);
        ListCell *wrapperCell;
        foreach(wrapperCell, wrapperFuncNames)
        {
            char *wrapperFuncName = (char *) lfirst(wrapperCell);
            recombined = psprintf("%s(%s)", wrapperFuncName, recombined);

            bool alreadyListed = false;
            ListCell *seenCell;
            foreach(seenCell, postProcessingFuncNames)
            {
                if (strcmp((char *) lfirst(seenCell), wrapperFuncName) == 0)
                {
                    alreadyListed = true;
                    break;
                }
            }
            if (!alreadyListed)
                postProcessingFuncNames = lappend(postProcessingFuncNames, wrapperFuncName);
        }

        if (explainNotes->len > 0)
            appendStringInfoString(explainNotes, "\n");
        appendStringInfo(explainNotes, "%s(%s): worker=%s, combiner=%s, final=%s",
                         funcName, catalog->distCol, funcName,
                         combinerOp != NULL ? combinerOp : "(none)",
                         finalOp != NULL ? finalOp : "(none)");

        if (selectList->len > 0)
            appendStringInfoString(selectList, ", ");
        appendStringInfo(selectList, "%s as %s", recombined,
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
    if (postProcessingNotesOut != NULL && postProcessingFuncNames != NIL)
    {
        StringInfo postProcessingNotes = makeStringInfo();
        ListCell *nameCell;
        foreach(nameCell, postProcessingFuncNames)
        {
            if (postProcessingNotes->len > 0)
                appendStringInfoString(postProcessingNotes, ", ");
            appendStringInfoString(postProcessingNotes, (char *) lfirst(nameCell));
        }
        *postProcessingNotesOut = postProcessingNotes->data;
    }
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
        if (funcName != NULL &&
            (LookupDistFuncFinalOp(funcName) != NULL || LookupDistFuncCombinerOp(funcName) != NULL))
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

/*
 * TrimmedSubstring/SplitTopLevelCommas now live in utils/helper_functions.c
 * (promoted from here so multi_phase_executor.c's positional ORDER BY
 * rewrite could reuse them too -- same "make future text-parsing fixes
 * reusable across files" pattern FindKeywordToken/FindTopLevelKeywordToken
 * already followed).
 */

/*
 * BuildRecombinedFuncCall composes a registered distributed function's
 * per-fragment calls back into one call over distCol's true, complete
 * value. Three compositions exist, chosen by what's actually registered
 * for funcName (LookupDistFuncFinalOp/LookupDistFuncCombinerOp), so this
 * applies uniformly to any current or future registered function without
 * any hardcoded per-function list:
 *
 * combinerOp set (a real Postgres aggregate, e.g. via CREATE AGGREGATE with
 * its own SFUNC/COMBINEFUNC/FINALFUNC) -- the aggregate itself already
 * knows how to recombine per-fragment partial state into the true value
 * internally, so this just invokes it directly: "combinerOp(distCol)".
 * finalOp is irrelevant here (typically NULL) and never consulted. Needed
 * for a function whose correct recombination isn't a single op applied to
 * independently-meaningful per-fragment results, and isn't just "reconstruct
 * the source first" either -- e.g. a cumulative distance-over-time profile,
 * whose per-fragment partial results have to be time-ordered and offset by
 * every earlier fragment's own running total, not just concatenated or
 * merged as-is.
 *
 * combinerOp unset, finalOp like sum/min/max/bool_or -- combines
 * independently-correct per-fragment RESULTS: each fragment's own call
 * already returns a meaningful partial answer (e.g. a fragment's own
 * partial path length), and finalOp reduces those partial answers into the
 * true total. Evaluating the function on each fragment first and combining
 * its results afterward is exactly right: "finalOp(funcName(distCol))".
 *
 * combinerOp unset, finalOp "merge" -- the function itself has no
 * meaningful per-fragment partial answer at all; it needs the *source*
 * fragments reconstructed into the complete original value *before* it can
 * run correctly. Evaluating such a function on each fragment independently
 * and only merging the *results* afterward silently loses whatever it
 * would have computed right at each tile boundary, since neither fragment
 * alone ever sees both sides of it (a derivative-like computation over the
 * boundary instant, for instance). For these, the source column itself is
 * reconstructed first and the function runs once over that whole,
 * reassembled value: "funcName(merge(distCol))".
 */
static char *
BuildRecombinedFuncCall(const char *finalOp, const char *combinerOp, const char *funcName, const char *distCol)
{
    if (combinerOp != NULL)
        return psprintf("%s(%s)", combinerOp, distCol);
    if (strcmp(finalOp, "merge") == 0)
        return psprintf("%s(merge(%s))", funcName, distCol);
    return psprintf("%s(%s(%s))", finalOp, funcName, distCol);
}

/*
 * WrapDistColReference returns a copy of exprText with its first
 * identifier-boundary occurrence of distCol replaced by "merge(distCol)"
 * -- e.g. "trip" becomes "merge(trip)", "numinstants(trip)" becomes
 * "numinstants(merge(trip))" -- so a caller expression that needs distCol's
 * real, complete value (not one tile's own fragment) gets it by having the
 * merge() reconstruction substituted in place of the bare reference, then
 * runs unchanged over that reconstructed value. Returns NULL if distCol
 * isn't found (bounded) anywhere in exprText.
 *
 * The search runs against a lowercased scratch copy purely to locate the
 * identifier case-insensitively (FindIdentifierToken requires already-
 * lowercased input); the returned copy preserves exprText's original
 * casing outside of the substituted span, since lowering is a 1:1,
 * length-preserving byte mapping so the same offset applies to both.
 */
static char *
WrapDistColReference(const char *exprText, const char *distCol)
{
    char *lowered = toLower((char *) exprText);
    char *loweredDistCol = toLower((char *) distCol);
    char *pos = FindIdentifierToken(lowered, loweredDistCol);
    if (pos == NULL)
        return NULL;
    size_t offset = pos - lowered;
    size_t distColLen = strlen(distCol);
    StringInfo result = makeStringInfo();
    appendBinaryStringInfo(result, exprText, offset);
    appendStringInfo(result, "merge(%.*s)", (int) distColLen, exprText + offset);
    appendStringInfoString(result, exprText + offset + distColLen);
    return result->data;
}

/*
 * A single matched WHERE-clause conjunct: a bare call to a registered
 * distributed function over the table's own distCol, compared against a
 * constant. RewriteWhereClauseDistFuncCalls collects one of these per
 * top-level AND-conjunct before building anything, so the whole rewrite
 * can still bail out cleanly if a later conjunct doesn't match.
 */
typedef struct DistFuncFilterMatch
{
    char *funcName;
    char *finalOp;    /* NULL when combinerOp is set instead -- see BuildRecombinedFuncCall */
    char *combinerOp;
    char *opName;
    Const *constArg;
    bool funcOnLeft;
} DistFuncFilterMatch;

/*
 * RewriteWhereClauseDistFuncCalls detects one or more bare (non-aggregate)
 * calls to registered distributed functions used as WHERE-clause filters --
 * e.g. a distributed length function compared against a numeric threshold,
 * possibly ANDed together with other such filters -- over a single
 * shape-segmented distributed spatiotemporal table.
 *
 * A function registered with an additive final op (sum, min, max, bool_or)
 * has a per-fragment result that's already independently meaningful --
 * evaluating it per-fragment, the way a plain pushdown would, is wrong on
 * its own: a trip split across several tiles has each fragment see only
 * part of the trajectory, so filtering each fragment independently can
 * both wrongly admit a trip whose *complete* value doesn't actually pass
 * the filter and wrongly exclude one whose complete value does -- the same
 * fragmentation problem RewriteSegmentedDistFuncCalls fixes for the
 * SELECT-list case, just triggered from the WHERE clause instead. This
 * rewrite recombines those into a proper per-group value first (via each
 * function's own registered final op -- see BuildRecombinedFuncCall's own
 * comment for why the composition differs by final op) and filters on
 * that instead.
 *
 * A function registered with final op "merge" is deliberately left alone
 * here, evaluated per-fragment exactly as a plain pushdown already would:
 * unlike the additive case, reconstructing every fragment for every group
 * just to filter on it is a real, measured cross-shard cost (one merge per
 * distinct row identifier), and per-fragment filtering is accurate enough
 * for this purpose even though the function's actual *value* still needs
 * full reconstruction (which the SELECT-list case, RewriteSegmentedDistFunc
 * Calls, still always does). See the matching loop below for exactly where
 * this split happens.
 *
 * Rewritten into a two-level query: an inner query projecting the caller's
 * original plain (non-aggregate) target-list columns plus each matched
 * function's properly recombined value, one column per match (grouped by
 * the table's own row identifier, same worker/combiner/final technique as
 * RewriteSegmentedDistFuncCalls), and an outer query that filters on all of
 * those recombined values, ANDed together -- moving the filter from a
 * per-row WHERE to a post-combination one, effectively a HAVING -- then
 * re-projects the caller's original target list (so the recombined-value
 * columns never reach the client), applying any of the caller's own
 * aggregates in the outer query, over the now-correctly-filtered rows: a
 * bare filtered projection, a plain count, and a distinct count of the row
 * identifier are all handled the same way, each getting its own
 * outer-query shape appropriate to what it originally asked for. A
 * caller's own aggregate is carried into the outer query as-is, and any
 * column it references (other than distCol -- see below) is pulled out
 * and projected into the inner query too, so the outer reference resolves;
 * safe despite the inner query's own GROUP BY groupCol, since groupCol is
 * the table's own primary key and Postgres's functional-dependency rule
 * lets any other same-table column be projected without its own
 * aggregation or GROUP BY entry.
 *
 * Deliberately narrow, matching RewriteSegmentedDistFuncCalls's own scope
 * limits: single table, no join; every top-level WHERE-clause conjunct must
 * independently be exactly one comparison between a bare distfunc call over
 * the table's distCol and a constant -- any conjunct that isn't recognized
 * as that shape (a plain, non-distfunc filter mixed in, for instance) bails
 * the whole rewrite out to NULL rather than risk relocating it incorrectly;
 * no pre-existing GROUP BY/ORDER BY/HAVING (this rewrite introduces its
 * own); a caller's aggregate referencing distCol itself bails out too --
 * the inner query's per-row value for that column is one fragment's own
 * value, not the trip's real combined one, and this rewrite has no second
 * combination layer to fix that up. Returns NULL when the query doesn't
 * match this shape -- the caller falls back to whatever handling the query
 * would otherwise get.
 */
extern char *
RewriteWhereClauseDistFuncCalls(Query *parse, const char *query_string, STMultirelations *tablesList)
{
    if (tablesList == NULL || tablesList->length != 1)
        return NULL;
    /* A pre-existing GROUP BY/HAVING would conflict with the GROUP BY
     * groupCol this rewrite introduces itself further below -- ORDER BY
     * doesn't have that problem (handled separately, near the end, once
     * the outermost query it needs to attach to actually exists) so it's
     * not bailed out here. */
    if (parse->groupClause != NIL || parse->havingQual != NULL)
        return NULL;

    Rte *rteNode = (Rte *) linitial(tablesList->tables);
    if (rteNode->RteType != STRte)
        return NULL;

    STMultirelation *table = (STMultirelation *) rteNode->rte;
    STMultirelationCatalog *catalog = &table->catalogTableInfo;
    if (!catalog->segmentation || catalog->groupCol == NULL)
        return NULL;

    List *whereClauseList = WhereClauseList(parse->jointree);
    if (whereClauseList == NIL)
        return NULL;

    /*
     * Locate the SELECT list's own text span, same technique as
     * RewriteReplicatedAggregateQuery: skip leading whitespace, require
     * "select" (no DISTINCT/ALL -- not handled), everything up to " from ".
     * Done before the WHERE-clause matching below (moved up from this
     * rewrite's earlier, single-conjunct version) since locating the WHERE
     * clause's own text span next needs fromKeyword already known, to
     * scope the search past it the same way the final query assembly
     * further down already does.
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

    /*
     * Locate the WHERE clause's own text span (scoped past fromKeyword,
     * same as the final query assembly further down) and split it into
     * per-conjunct text, positionally paired against whereClauseList's own
     * parsed conjuncts below -- same "split text, pair positionally
     * against the parsed list" technique the SELECT list's chunks above
     * already use. Needed so a conjunct left as a plain per-fragment
     * pushdown (the finalOp == "merge" case below) can be relocated into
     * the inner query's own WHERE clause using its *original* text, rather
     * than trying to deparse the parsed OpExpr back into SQL by hand
     * (deparsing from this nested position has crashed the backend before
     * -- see RewriteReplicatedAggregateQuery's own doc comment).
     */
    size_t whereSearchOffset = fromKeyword - lowered;
    char *whereKeywordEarly = FindKeywordToken(lowered + whereSearchOffset, "where");
    if (whereKeywordEarly == NULL)
        return NULL;
    size_t whereBodyOffset = (whereKeywordEarly - lowered) + strlen("where");
    char *afterWhere = lowered + whereBodyOffset;
    char *whereBodyEnd = FindTopLevelKeywordToken(afterWhere, "order by");
    size_t whereBodyLen = (whereBodyEnd != NULL) ? (size_t) (whereBodyEnd - afterWhere) : strlen(afterWhere);
    char *whereText = TrimmedSubstring(query_string + whereBodyOffset, query_string + whereBodyOffset + whereBodyLen);
    size_t whereTextLen = strlen(whereText);
    while (whereTextLen > 0 && (whereText[whereTextLen - 1] == ';' || isspace((unsigned char) whereText[whereTextLen - 1])))
        whereText[--whereTextLen] = '\0';
    List *whereConjunctTexts = SplitTopLevelConjuncts(whereText);
    if (list_length(whereConjunctTexts) != list_length(whereClauseList))
        return NULL;

    /*
     * One entry per top-level AND-conjunct, positionally paired against
     * whereConjunctTexts above. A conjunct that doesn't match "bare
     * distfunc call over distCol, compared against a constant" at all (a
     * plain filter mixed in, a comparison against something other than a
     * constant, etc.) bails the entire rewrite out, consistent with this
     * function's existing conservative philosophy elsewhere. A conjunct
     * that *does* match but whose function is registered with a "merge"
     * final op is deliberately left as a plain per-fragment pushdown
     * (passthroughConjunctTexts) instead of being pulled into the
     * recombination machinery below -- unlike an additive final op (sum,
     * min, max, bool_or), where a per-fragment result is already
     * meaningful and simply needs combining, a "merge" function's
     * per-fragment *filtering* usefulness doesn't require full source
     * reconstruction the way computing its actual *value* does (the
     * SELECT-list case, RewriteSegmentedDistFuncCalls, still always
     * reconstructs first for that reason) -- and reconstructing every
     * fragment for every group just to filter is a real, measured cost
     * (cross-shard, one merge per distinct row identifier) this rewrite
     * shouldn't force onto a query that doesn't need it.
     */
    List *matches = NIL;
    List *passthroughConjunctTexts = NIL;
    ListCell *clauseCell = list_head(whereClauseList);
    ListCell *textCell = list_head(whereConjunctTexts);
    while (clauseCell != NULL)
    {
        Node *clause = (Node *) lfirst(clauseCell);
        char *conjunctText = (char *) lfirst(textCell);

        if (!IsA(clause, OpExpr))
            return NULL;

        OpExpr *opExpr = (OpExpr *) clause;
        if (list_length(opExpr->args) != 2)
            return NULL;

        char *opName = get_opname(opExpr->opno);
        if (opName == NULL)
            return NULL;
        /*
         * Not restricted to plain scalar comparisons: a registered
         * function returning a temporal value (e.g. a distributed speed
         * function producing a temporal float) is ordinarily compared via
         * MobilityDB's own "ever"/"always" operators rather than a plain
         * scalar one -- both families are accepted here uniformly; the AST
         * shape check below (bare distfunc call vs. a constant) is what
         * actually bounds this rewrite's scope, not the specific operator
         * used.
         */
        bool isComparisonOp = strcmp(opName, "=") == 0 || strcmp(opName, "<>") == 0 ||
                              strcmp(opName, "<") == 0 || strcmp(opName, "<=") == 0 ||
                              strcmp(opName, ">") == 0 || strcmp(opName, ">=") == 0 ||
                              strcmp(opName, "?=") == 0 || strcmp(opName, "?<>") == 0 ||
                              strcmp(opName, "?<") == 0 || strcmp(opName, "?<=") == 0 ||
                              strcmp(opName, "?>") == 0 || strcmp(opName, "?>=") == 0 ||
                              strcmp(opName, "%=") == 0 || strcmp(opName, "%<>") == 0 ||
                              strcmp(opName, "%<") == 0 || strcmp(opName, "%<=") == 0 ||
                              strcmp(opName, "%>") == 0 || strcmp(opName, "%>=") == 0;
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
        char *combinerOp = LookupDistFuncCombinerOp(funcName);
        if (finalOp == NULL && combinerOp == NULL)
            return NULL;

        /*
         * Only the plain "merge" final op (no registered combinerOp) is
         * left as a per-fragment passthrough -- see this function's own
         * doc comment. A registered combinerOp (a real Postgres aggregate,
         * e.g. cumulativeLength) never is: unlike a bare merge() of the
         * source, its own worker/combine/final recombination is the only
         * way to get a meaningful value out of it at all, so per-fragment
         * filtering on it isn't just cheaper-but-approximate the way
         * per-fragment speed filtering is -- it's simply wrong.
         */
        if (combinerOp == NULL && strcmp(finalOp, "merge") == 0)
        {
            passthroughConjunctTexts = lappend(passthroughConjunctTexts, conjunctText);
        }
        else
        {
            DistFuncFilterMatch *match = (DistFuncFilterMatch *) palloc(sizeof(DistFuncFilterMatch));
            match->funcName = funcName;
            match->finalOp = finalOp;
            match->combinerOp = combinerOp;
            match->opName = opName;
            match->constArg = constArg;
            match->funcOnLeft = funcOnLeft;
            matches = lappend(matches, match);
        }

        clauseCell = lnext(whereClauseList, clauseCell);
        textCell = lnext(whereConjunctTexts, textCell);
    }
    if (matches == NIL)
        return NULL;

    StringInfo innerSelectList = makeStringInfo();
    StringInfo outerSelectList = makeStringInfo();
    /* Column names (C strings) already projected by innerSelectList, so an
     * aggregate referencing the same column twice (or a column a plain
     * target entry already projects under its own alias) doesn't get
     * projected again under a second, redundant alias. */
    List *innerProjectedCols = NIL;
    /*
     * A target-list entry that plainly (non-aggregate) selects distCol
     * itself, alongside a distfunc-based WHERE-clause filter, can't be
     * satisfied by the two-level filtering query below the way
     * any other column can: that query's per-row distCol value is only one
     * tile's own fragment, never the row's true, complete value, and there
     * is no fixing that up inside it. Such an entry is deferred out of the
     * filtering query (mergeAliases records the alias(es) it needs to come
     * back under) and satisfied by a third query layer instead: re-fetch
     * every fragment for each qualifying groupCol value from the base
     * table and run MobilityDB's own merge() aggregate over distCol,
     * GROUP BY groupCol -- the same "reconstruct the true value from its
     * tiles" idea sum(length(trip)) GROUP BY groupCol already applies to a
     * derived scalar above, just applied to distCol itself instead of a
     * number computed from it.
     */
    bool needsMerge = false;
    StringInfo finalSelectList = makeStringInfo();

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
         * A target-list entry -- aggregate or plain -- referencing distCol
         * itself (directly or nested inside some other expression) can't be
         * satisfied by the two-level filtering query below: its own per-row
         * distCol value is only one tile's own fragment of a segmented row,
         * never the row's true combined value.
         *
         * A bare or wrapped plain reference to distCol itself -- the common
         * case: retrieving distCol's own true value, or something computed
         * from it, for the rows that matched a distfunc-based filter -- is
         * handled below by textually substituting merge(distCol) for
         * distCol's own reference inside te's expression text
         * (WrapDistColReference) and deferring that into a third query
         * layer (see needsMerge below) -- this used to fall into the
         * ordinary plain-entry branch further down, which projected te's
         * text into the inner query as a bare column with no GROUP BY entry
         * or aggregate wrapping it, producing an invalid "column ... must
         * appear in the GROUP BY clause" query. An aggregate referencing
         * distCol itself still bails out: the merge() layer only reconstructs
         * distCol's own complete value, it doesn't know how to run an
         * aggregate over that reconstruction in the same pass.
         */
        List *entryVars = pull_var_clause((Node *) te->expr,
                                          PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS);
        bool referencesDistCol = false;
        ListCell *entryVarCell;
        foreach(entryVarCell, entryVars)
        {
            Var *var = (Var *) lfirst(entryVarCell);
            if (var->varno <= 0 || (int) var->varno > list_length(parse->rtable))
                return NULL;
            RangeTblEntry *varRte = rt_fetch(var->varno, parse->rtable);
            if (varRte->rtekind != RTE_RELATION)
                return NULL;
            char *varColName = get_attname(varRte->relid, var->varattno, false);
            if (varColName == NULL)
                return NULL;
            if (strcasecmp(varColName, catalog->distCol) == 0)
                referencesDistCol = true;
        }
        if (referencesDistCol)
        {
            if (IsA(te->expr, Aggref))
                return NULL;
            char *wrapped = WrapDistColReference(chunkText, catalog->distCol);
            if (wrapped == NULL)
                return NULL;
            needsMerge = true;
            if (finalSelectList->len > 0)
                appendStringInfoString(finalSelectList, ", ");
            appendStringInfo(finalSelectList, "%s as %s", wrapped, te->resname);
            continue;
        }

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
                RangeTblEntry *varRte = rt_fetch(var->varno, parse->rtable);
                char *varColName = get_attname(varRte->relid, var->varattno, false);

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
            if (finalSelectList->len > 0)
                appendStringInfoString(finalSelectList, ", ");
            appendStringInfo(finalSelectList, "%s as %s", chunkText, te->resname);
            continue;
        }

        if (innerSelectList->len > 0)
            appendStringInfoString(innerSelectList, ", ");
        appendStringInfo(innerSelectList, "%s as %s", chunkText, te->resname);
        innerProjectedCols = lappend(innerProjectedCols, te->resname);
        if (outerSelectList->len > 0)
            appendStringInfoString(outerSelectList, ", ");
        appendStringInfoString(outerSelectList, te->resname);
        /* finalSelectList re-derives this column fresh from the base table
         * (fromText) in the merge query below rather than reusing te's
         * chunkText verbatim -- safe since it's plain, non-distCol, and
         * functionally dependent on groupCol just like it already is in the
         * filtering query's own GROUP BY groupCol. */
        if (finalSelectList->len > 0)
            appendStringInfoString(finalSelectList, ", ");
        appendStringInfo(finalSelectList, "%s as %s", chunkText, te->resname);
    }
    if (chunkCell != NULL || (outerSelectList->len == 0 && !needsMerge))
        return NULL;

    /*
     * needsMerge's filtering query only needs groupCol as a correlation
     * key for the merge query's WHERE ... IN below -- add it if no target
     * entry already projected it (the common case of projecting distCol
     * alone, which has nothing else in outerSelectList at all).
     */
    if (needsMerge)
    {
        bool groupColAlreadyProjected = false;
        ListCell *projCell;
        foreach(projCell, innerProjectedCols)
        {
            if (strcasecmp((char *) lfirst(projCell), catalog->groupCol) == 0)
            {
                groupColAlreadyProjected = true;
                break;
            }
        }
        if (!groupColAlreadyProjected)
        {
            if (innerSelectList->len > 0)
                appendStringInfoString(innerSelectList, ", ");
            appendStringInfo(innerSelectList, "%s as %s", catalog->groupCol, catalog->groupCol);
            if (outerSelectList->len > 0)
                appendStringInfoString(outerSelectList, ", ");
            appendStringInfoString(outerSelectList, catalog->groupCol);
        }
    }

    /*
     * Always recombined via each match's own registered final op and
     * GROUP BY groupCol -- NOT conditioned on catalog->isMobilityDB the way
     * RewriteSegmentedDistFuncCalls's SELECT-list rewrite is (its doc
     * comment claims a segmented+isMobilityDB table's tiles hold full
     * duplicate copies, so a plain unwrapped call plus DISTINCT ON
     * suffices). Empirically false for a genuinely-clipped table
     * (isMobilityDB=true, segmentation=true): its tiles hold genuinely
     * disjoint, clipped fragments -- sum(length(trip)) GROUP BY tripid over
     * it reproduces the true untiled length (confirmed against the source
     * table to the available floating-point precision), while any single
     * fragment's raw length() is far smaller. A DISTINCT ON here would
     * silently filter on one arbitrary fragment's partial value instead of
     * the trip's real one. RewriteSegmentedDistFuncCalls's own DISTINCT ON
     * path likely has the same bug for this same data shape, but that's a
     * pre-existing SELECT-list issue left alone here -- out of scope for
     * this WHERE-clause rewrite. One recombined column per match, each
     * under its own synthetic alias so the outer WHERE below can AND them
     * together independently.
     */
    StringInfo innerFilterCols = makeStringInfo();
    ListCell *matchCell;
    int matchIdx = 0;
    foreach(matchCell, matches)
    {
        DistFuncFilterMatch *match = (DistFuncFilterMatch *) lfirst(matchCell);
        if (innerFilterCols->len > 0)
            appendStringInfoString(innerFilterCols, ", ");
        appendStringInfo(innerFilterCols, "%s as __dmdb_wc_filter_%d",
                         BuildRecombinedFuncCall(match->finalOp, match->combinerOp, match->funcName, catalog->distCol),
                         matchIdx);
        matchIdx++;
    }

    /*
     * Preserve the FROM clause text verbatim (same "find ' from '"
     * technique used throughout this file), stopping at the query's own
     * WHERE keyword -- every conjunct this rewrite pulled into
     * recombination (matches) is what's being relocated into the outer
     * filter below, so none of *those* need to survive into the inner
     * query; any passthrough conjunct (passthroughConjunctTexts) is
     * spliced back into the inner query's own WHERE right below instead,
     * unchanged, so it's still evaluated per-fragment exactly as a plain
     * pushdown would.
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

    StringInfo innerWhereClause = makeStringInfo();
    if (passthroughConjunctTexts != NIL)
    {
        appendStringInfoString(innerWhereClause, " where ");
        ListCell *ptCell;
        bool firstPassthrough = true;
        foreach(ptCell, passthroughConjunctTexts)
        {
            if (!firstPassthrough)
                appendStringInfoString(innerWhereClause, " and ");
            appendStringInfoString(innerWhereClause, (char *) lfirst(ptCell));
            firstPassthrough = false;
        }
    }

    /*
     * innerSelectList can be empty -- e.g. a bare count(*) alongside a
     * distfunc-based WHERE filter has no plain column, only the aggregate
     * (routed to outerSelectList above) -- in which case a leading ", "
     * would produce invalid SQL (a comma with nothing before it).
     */
    StringInfo innerQuery = makeStringInfo();
    if (innerSelectList->len > 0)
        appendStringInfo(innerQuery, "select %s, %s %s%s group by %s",
                         innerSelectList->data, innerFilterCols->data, fromText,
                         innerWhereClause->data, catalog->groupCol);
    else
        appendStringInfo(innerQuery, "select %s %s%s group by %s",
                         innerFilterCols->data, fromText,
                         innerWhereClause->data, catalog->groupCol);

    /*
     * Each match's recombined column is filtered against its own original
     * constant/operator, ANDed together -- reproducing exactly what the
     * original WHERE clause's own top-level AND already expressed, just
     * evaluated post-recombination instead of per-fragment.
     */
    StringInfo outerWhere = makeStringInfo();
    matchIdx = 0;
    foreach(matchCell, matches)
    {
        DistFuncFilterMatch *match = (DistFuncFilterMatch *) lfirst(matchCell);
        /*
         * Reproduced textually via the type's own output function plus an
         * explicit cast, rather than trying to locate the constant's
         * original source text in query_string -- robust regardless of how
         * the constant was written (5000, '5000', a parameter that already
         * got folded to a Const, etc.); the explicit ::type cast avoids the
         * rewritten literal being parsed back with a different inferred
         * type than the original (same reasoning as the STBOX-literal
         * casts used elsewhere in this extension's dynamic SQL).
         */
        char *constText = DatumToString(match->constArg->constvalue, match->constArg->consttype);
        char *constTypeName = format_type_be(match->constArg->consttype);
        char *constTextWithCast = psprintf("%s::%s", constText, constTypeName);
        char *filterCol = psprintf("__dmdb_wc_filter_%d", matchIdx);

        if (outerWhere->len > 0)
            appendStringInfoString(outerWhere, " and ");
        appendStringInfo(outerWhere, "%s %s %s",
                         match->funcOnLeft ? filterCol : constTextWithCast,
                         match->opName,
                         match->funcOnLeft ? constTextWithCast : filterCol);
        matchIdx++;
    }

    StringInfo outerQuery = makeStringInfo();
    appendStringInfo(outerQuery, "select %s from (%s) as dmdb_wc_sub where %s",
                     outerSelectList->data, innerQuery->data, outerWhere->data);

    /*
     * ORDER BY (LIMIT/OFFSET too, if present) is carried over as raw text
     * from the original query rather than rebuilt from parse->sortClause
     * the way RewriteSegmentedDistFuncCalls does it further below -- that
     * rebuild exists there because its own rewritten query's FROM clause
     * differs from the original, so a sort key's original text may no
     * longer resolve and has to be re-targeted at the rewrite's own
     * aliases. This rewrite's outermost query (outerQuery/finalQuery)
     * always queries fromText verbatim -- the exact same FROM-clause text,
     * same table, same alias if any, as the original query -- so any
     * column reference in the original ORDER BY resolves identically
     * there, whether or not that column made it into the SELECT list
     * (Postgres already allows ordering a GROUP BY query by any column
     * functionally dependent on the grouping key, e.g. ordering by the
     * table's own row identifier when groupCol is that same identifier --
     * the common case this fixes: reproduced directly with a distfunc-based
     * WHERE filter alongside an ORDER BY on the row identifier, which
     * previously bailed out of this rewrite entirely on sortClause != NIL
     * and fell back to RewriteSegmentedDistFuncCalls's WHERE-clause-blind
     * handling instead, silently dropping trips whose complete value
     * passes the filter but whose every individual fragment doesn't -- the
     * same wrong-results class of bug this whole rewrite exists to fix,
     * just re-opened by the sortClause bail-out). A sort key referencing
     * distCol itself has no obviously correct single interpretation here
     * (ordering by a whole reconstructed trajectory?) and bails out rather
     * than guess.
     */
    StringInfo orderBySuffix = makeStringInfo();
    if (parse->sortClause != NIL)
    {
        char *orderByKeyword = FindTopLevelKeywordToken(whereKeyword, "order by");
        if (orderByKeyword == NULL)
            return NULL;
        size_t orderByOffset = orderByKeyword - lowered;
        char *orderByText = pstrdup(query_string + orderByOffset);
        size_t orderByLen = strlen(orderByText);
        while (orderByLen > 0 && (orderByText[orderByLen - 1] == ';' || isspace((unsigned char) orderByText[orderByLen - 1])))
            orderByText[--orderByLen] = '\0';
        char *loweredOrderBy = toLower(orderByText);
        if (FindIdentifierToken(loweredOrderBy, toLower(catalog->distCol)) != NULL)
            return NULL;
        appendStringInfo(orderBySuffix, " %s", orderByText);
    }

    if (!needsMerge)
    {
        appendStringInfoString(outerQuery, orderBySuffix->data);
        return outerQuery->data;
    }

    /*
     * outerQuery above only determines *which* groupCol values qualify --
     * it was never asked to project distCol's real value (see the
     * referencesDistCol check earlier). This third layer re-fetches every
     * fragment for each qualifying groupCol value straight from the base
     * table and merges them back into distCol's true, complete value,
     * finally satisfying the caller's original target list -- finalSelectList
     * already holds every entry (both the plain columns re-derived fresh
     * here, functionally dependent on groupCol just like they already were
     * in outerQuery, and each deferred distCol reference rewritten to its
     * merge()-wrapped form by WrapDistColReference above).
     */
    StringInfo finalQuery = makeStringInfo();
    appendStringInfo(finalQuery, "select %s %s where %s in (select %s from (%s) as dmdb_wc_qualifying) group by %s",
                     finalSelectList->data,
                     fromText,
                     catalog->groupCol, catalog->groupCol, outerQuery->data,
                     catalog->groupCol);
    appendStringInfoString(finalQuery, orderBySuffix->data);
    return finalQuery->data;
}

/*
 * RewriteReplicatedAggregateQuery detects an explicit aggregate (e.g. a sum
 * of a registered distributed function's result) whose argument references
 * both that registered distributed function and a replicated table's
 * column, and rewrites the query into a two-level shape: an inner query
 * that DISTINCT-deduplicates on the replicated table's own row identifier
 * (groupCol) before computing the aggregate's argument once per row, and
 * an outer query that applies the original aggregate/GROUP BY over that
 * already-deduplicated result.
 *
 * Without this, a row belonging to a trip replicated across N tiles
 * contributes to the SUM N times (once per tile) instead of once, silently
 * multiplying the result -- confirmed empirically comparing this predicate
 * shape against a distributed table vs. the non-distributed source table
 * (~4.3x inflated, matching how many tiles the affected trips' bboxes
 * touch).
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
     * doesn't already project it explicitly; otherwise the inner subquery
     * would end up with two same-named output
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

        /*
         * Any *other* aggregate in the target list (e.g. a plain count
         * alongside the replicated one this rewrite is handling) can't be
         * treated as a pass-through column the way a plain Var can: the code
         * below embeds te's raw SQL text into the inner query's own
         * SELECT DISTINCT list, which has no GROUP BY at all --
         * mixing an aggregate into that list produces invalid SQL ("column
         * ... must appear in the GROUP BY clause or be used in an aggregate
         * function", reproduced directly against a real segmented
         * MobilityDB table). This
         * function's own doc comment already scopes it to "only the first
         * qualifying Aggref found is rewritten" -- bailing out here when a
         * second, unrelated aggregate is present is consistent with that,
         * and with this function's existing philosophy of falling back to
         * the pre-existing (imperfect but non-erroring) behavior rather than
         * risking broken SQL.
         */
        if (IsA(te->expr, Aggref))
            return NULL;

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
     * Trips ..." as invalid SQL. */
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
 * to reach *inside* a query's own CTEs: a CTE whose own body computes a
 * replicated-table aggregate (the same shape RewriteReplicatedAggregateQuery
 * handles at the top level -- an aggregate over a registered distributed
 * function's result, referencing a replicated table's column) has the exact
 * same replication-overcounting problem, just one level down, inside the
 * CTE, rather than at the top level, even when the outer query's own
 * aggregate (e.g. a MAX over the CTE's own output column) has no
 * distributed function call in sight from up there.
 * RewriteReplicatedAggregateQuery only
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

