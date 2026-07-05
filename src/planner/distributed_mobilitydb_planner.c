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

#include <liblwgeom.h>
#include "distributed/distributed_planner.h"
#include "planner/query_semantics.h"
#include "distributed/multi_executor.h"
#include "general/general_types.h"
#include "distributed_functions/distributed_function.h"
#include "executor/multi_phase_executor.h"
#include "planner/planner_strategies.h"
#include "catalog/table_ops.h"
#include "optimizer/planner.h"
#include "catalog/table_ops.h"
#include "multirelation/multirelation_utils.h"
#include "post_processing/post_processing.h"
#include "utils/helper_functions.h"
#include "distributed_functions/coordinator_operations.h"
#include "distributed_functions/worker_operations.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "general/spatiotemporal_processing.h"
#include "general/rte.h"


static void analyzeDistributedSpatiotemporalTables(List *rangeTableList,
                                       DistributedSpatiotemporalQueryPlan *distPlan);
static void PlanInitialization(DistributedSpatiotemporalQueryPlan *distPlan);
static void checkQueryType(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan);
static void ProcessQueryPredicates(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan);
static void ProcessPredicateClause(DistributedSpatiotemporalQueryPlan *distPlan, Node *clause);
static bool SelectListPredicateWalker(Node *node, DistributedSpatiotemporalQueryPlan *distPlan);
static void AnalyseSelectListPredicates(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan);
static bool needsDistributedSpatiotemporalPlanning(DistributedSpatiotemporalQueryPlan *distPlan);
static bool StrategiesInclude(List *strategies, StrategyType type);
static PlannedStmt * EarlyQueryCheck(Query *parse, const char *query_string, int cursorOptions,
                                     ParamListInfo boundParams);

/*
 * distributed_mobilitydb_planner is the planner_hook entry point (see
 * _PG_init in shared_library_init.c): it allocates a fresh
 * DistributedSpatiotemporalQueryPlan and delegates the actual work to
 * distributed_mobilitydb_planner_internal(), which is also reused directly
 * by the EXPLAIN hook.
 */
PlannedStmt *
distributed_mobilitydb_planner(Query *parse, const char *query_string, int cursorOptions,
                       ParamListInfo boundParams)
{
    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan = (DistributedSpatiotemporalQueryPlan *)
            palloc0(sizeof(DistributedSpatiotemporalQueryPlan));
    return distributed_mobilitydb_planner_internal(parse, query_string, cursorOptions, boundParams,
                                           distributedSpatiotemporalPlan, false);
}

/*
 * distributed_mobilitydb_planner_internal drives the full planning
 * pipeline for a query: bail out early (via EarlyQueryCheck/Citus'
 * distributed_planner) when no distributed spatiotemporal table is
 * involved; otherwise inventory the query's tables
 * (analyzeDistributedSpatiotemporalTables), classify its predicates
 * (checkQueryType) to pick execution strategies, rewrite any distributed
 * aggregate calls (RewriterDistFuncs), run each chosen strategy's plan
 * function, and finally hand the rewritten query off to QueryExecutor()
 * and Citus' planner. `explain` skips the actual execution step so EXPLAIN
 * can report the plan without running it.
 */
PlannedStmt *
distributed_mobilitydb_planner_internal(Query *parse, const char *query_string, int cursorOptions,
                                ParamListInfo boundParams,
                                DistributedSpatiotemporalQueryPlan *distPlan, bool explain)
{
    PlannedStmt *result = NULL;
    bool needsSpatiotemporalPlanning = false;
    PlanInitialization(distPlan);
    result = EarlyQueryCheck(parse, query_string, cursorOptions, boundParams);
    if (result != NULL)
        return result;
    /* Get info about the user query */
    List *rangeTableList = ExtractRangeTableEntryList(parse);
    /* Copy the parse tree for later use */
    distPlan->query = parse;
    if (query_string == NULL)
    {
        return distributed_planner(parse, query_string, cursorOptions, boundParams);
    }
    analyzeDistributedSpatiotemporalTables(rangeTableList, distPlan);

    if (distPlan->tablesList->length == 0 )
    {
        return distributed_planner(parse, query_string, cursorOptions, boundParams);
    }

    /* Initialize the post processing phase */
    distPlan->postProcessing = InitializePostProcessing();
    analyseSelectClause(parse->targetList, distPlan->postProcessing);
    if (query_string!= NULL && distPlan->tablesList->length > 0 && !distPlan->queryContainsReshuffledTable)
    {
        checkQueryType(parse, distPlan);
        needsSpatiotemporalPlanning = needsDistributedSpatiotemporalPlanning(distPlan);
        /* NonColocation/Colocation strategies join tiles that were built by
         * reshuffling on a spatiotemporal shape (see
         * analyzeDistributedSpatiotemporalTables/shapesegmented), which can
         * legitimately place the same row's shape-segmented copy in more
         * than one tile so a boundary-crossing match isn't missed by any
         * single tile. That means the coordinator-level union of per-tile
         * results can contain the same logical match more than once, so
         * these strategies need a final deduplication pass. */
        if (StrategiesInclude(distPlan->strategies, NonColocation) ||
            StrategiesInclude(distPlan->strategies, Colocation))
        {
            distPlan->postProcessing->coordinatorLevelOperator->dupRemOperator->active = true;
        }
    }

    /* Query rewriter */
    if (list_length(distPlan->postProcessing->distfuns) > 0 )
        RewriterDistFuncs(parse, distPlan->postProcessing, query_string);
    if (needsSpatiotemporalPlanning && list_length(distPlan->strategies) == 0
            && list_length(distPlan->postProcessing->distfuns) == 0)
    {
        /* needsDistributedSpatiotemporalPlanning() can return true purely
         * from tablesList->diffCount > 1, even when checkQueryType() found
         * no registered intersection/distance predicate to build a strategy
         * for (e.g. a plain `@>` "contains" clause against a reference
         * table isn't one of those). With no strategy and no distributed
         * function, there is nothing for the custom executor below to
         * build a plan from -- it would otherwise fall into
         * ConstructGeneralQuery's `generalScan->length == 0` branch and use
         * postProcessing->worker, which is NULL here, producing "(null)"
         * as the query text. Let Citus plan the query directly instead. */
        return distributed_planner(parse, query_string, cursorOptions, boundParams);
    }
    if (needsSpatiotemporalPlanning)
    {
        if (!distPlan->activate_rewriter)
        {
            /* Keep the original query string for later */
            distPlan->org_query_string = palloc((strlen(query_string) + 1 ) * sizeof (char));
            strcpy(distPlan->org_query_string, replaceWord(toLower((char *)query_string), ";", " "));
        }
        else
        {
            distPlan->org_query_string = palloc((strlen(distPlan->postProcessing->worker) + 1 ) * sizeof (char));
            strcpy(distPlan->org_query_string, distPlan->postProcessing->worker);
        }
        ListCell *cell = NULL;
        foreach(cell, distPlan->strategies)
        {
            StrategyType type = (StrategyType) lfirst_int(cell);
            if (type == Colocation)
                ColocationStrategyPlan(distPlan);
            else if (type == NonColocation)
                NonColocationStrategyPlan(distPlan);
            else if (type == TileScanRebalancer)
                TileScanRebalanceStrategyPlan(distPlan);
            else if (type == PredicatePushDown)
                PredicatePushDownStrategyPlan(distPlan);
            else
                ereport(ERROR, (errmsg("This query is not supported yet!")));
        }
        /* Initialize the post processing phase */
        if (distPlan->activate_post_processing_phase)
            PostProcessingQuery(distPlan->postProcessing, distPlan->strategies);
        /* Executor */
        GeneralScan *generalScan = (GeneralScan *) palloc0(sizeof(GeneralScan));
        if (!explain)
        {
            generalScan = QueryExecutor(distPlan, explain);
            result = distributed_planner(generalScan->query, generalScan->query_string->data,
                                         cursorOptions, boundParams);
        }
        else
            return result;
    }
    else
        result = distributed_planner(parse, query_string, cursorOptions, boundParams);
    return result;
}

/*
 * GetDistributedPlan returns the associated DistributedPlan for a CustomScan.
 * Callers should only read from the returned data structure, since it may be
 * the plan of a prepared statement and may therefore be reused.
 */
DistributedSpatiotemporalQueryPlan *
GetSpatiotemporalDistributedPlan(CustomScan *customScan)
{
    Assert(list_length(customScan->custom_private) == 1);

    Node *node = (Node *) linitial(customScan->custom_private);
    Assert(CitusIsA(node, DistributedSpatiotemporalQueryPlan));

    DistributedSpatiotemporalQueryPlan *distPlan = (DistributedSpatiotemporalQueryPlan *) node;

    return distPlan;
}

/*
 * analyzeDistributedSpatiotemporalTables gets a list of range table entries
 * and detects the spatiotemporal distributed relation range
 * table entry in the list.
 */
static void
analyzeDistributedSpatiotemporalTables(List *rangeTableList,
                                       DistributedSpatiotemporalQueryPlan *distPlan)
{
    ListCell *rangeTableCell = NULL;
    Oid curr_relid = -1;
    /* diffCount/simCount need to know whether relid has appeared ANYWHERE
     * earlier in the range table, not just in the immediately preceding
     * entry -- comparing only to curr_relid miscounted a self-join like
     * "Trips t1, Licences1 l1, Trips t2" as three different tables instead
     * of recognizing t2 as a repeat of t1, since t2 is compared against
     * l1's relid rather than t1's. */
    List *seenRelids = NIL;
    bool shapeType;
    List *rtes = NIL;
    foreach(rangeTableCell, rangeTableList)
    {
        RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
        if (rangeTableEntry->rtekind != RTE_RELATION) {
            continue;
        }
        /* A view reference (e.g. Licences1, a view over Licences) expands
         * in the range table into the view's own subquery entry (rtekind
         * != RTE_RELATION, already skipped above), PostgreSQL's internal
         * rule-system "old"/"new" placeholder entries for that view, and
         * the view's real underlying base table -- none of which the user
         * actually joined except the last. inFromCl is false for exactly
         * those non-user-visible placeholders (verified: "old"/"new" have
         * inFromCl=0, the real base table has inFromCl=1), so without this
         * check every view reference was triple-counted as extra distinct
         * tables, inflating diffCount/length enough to make e.g. a query
         * with one distributed table plus reference tables look like it
         * had several more distributed tables than it really did. */
        if (!rangeTableEntry->inFromCl) {
            continue;
        }
        if (IsDistributedSpatiotemporalTable(rangeTableEntry->relid))
        {
            if(IsReshuffledTable(rangeTableEntry->relid))
            {
                distPlan->queryContainsReshuffledTable = true;
                return;
            }
            shapeType = DistributedColumnType(rangeTableEntry->relid);
            if (shapeType == SPATIAL || shapeType == SPATIOTEMPORAL)
            {
                /* distPlan->shapeType drives which bbox flavor (MobilityDB
                 * STBOX vs PostGIS geometry) the reshuffling plan builds; it
                 * was previously never assigned here, so it stayed at its
                 * palloc0 zero value (SPATIAL) even for tgeompoint columns,
                 * making cross-table distance/intersection joins on
                 * spatiotemporal columns build a PostGIS-only reshuffling
                 * query that can't compare against a tgeompoint column. */
                distPlan->shapeType = shapeType;
                STMultirelation *spatiotemporal_table = GetMultirelationInfo(rangeTableEntry, shapeType);
                if (curr_relid != rangeTableEntry->relid)
                    distPlan->joining_col = spatiotemporal_table->col;


                spatiotemporal_table->catalogFilter = AnalyseCatalog(spatiotemporal_table,
                                                                     distPlan->query->jointree);
                distPlan->reshuffled_table_base = spatiotemporal_table;
                Rte *rteNode = GetRteNode((Node *) spatiotemporal_table, STRte, rangeTableEntry->alias);
                rtes = lappend(rtes , rteNode);
                distPlan->tablesList->stCount++;
            }
        }
        else
        {
            /* Table is not distributed using any of the following dimensions:
             * 1D (Temporal Tiling)
             * 2D (Spatial Tiling)
             * 3D (Spatiotemporal)
             * */

            if (LookupCitusTableCacheEntry(rangeTableEntry->relid) != NULL)
            {
                char partitioningMethod = PartitionMethodViaCatalog (rangeTableEntry->relid);
                /* A reference table is replicated to every node, so joining
                 * it alongside a distributed table needs no repartitioning.
                 * Its reported partitioning method alone doesn't identify it
                 * uniquely, so check its table type explicitly instead. */
                bool isReferenceTable = IsCitusTableType(rangeTableEntry->relid, REFERENCE_TABLE);
                if (partitioningMethod == DISTRIBUTE_BY_HASH || partitioningMethod == DISTRIBUTE_BY_RANGE
                        || isReferenceTable)
                {
                    /* Citus table processing */
                    CitusRteNode *citusNode = GetCitusRteInfo(rangeTableEntry,partitioningMethod);
                    citusNode->rangeTableCell = rangeTableCell;
                    /* length is already incremented unconditionally for
                     * every range table entry below (after this if/else) --
                     * incrementing it here too double-counted every Citus
                     * table entry, inflating effectiveLength enough that a
                     * query with one distributed table plus reference
                     * tables was never recognized as an effective
                     * single-table case. */
                    Rte *rteNode = GetRteNode((Node *) citusNode, CitusRte, rangeTableEntry->alias);
                    rtes = lappend(rtes , rteNode);
                    distPlan->tablesList->nonStCount++;
                    if (isReferenceTable)
                        distPlan->tablesList->refCount++;
                }
                else
                    elog(ERROR, "The %s table is not distributed using one of the supported partitioning methods",
                         get_rel_name(rangeTableEntry->relid));
            }
            else
            {
                /* Local table processing */
                LocalRteNode *localNode = GetLocalRteInfo(rangeTableEntry);
                Rte *rteNode = GetRteNode((Node *) localNode, LocalRte, rangeTableEntry->alias);
                rtes = lappend(rtes , rteNode);
            }
        }
        if (!list_member_oid(seenRelids, rangeTableEntry->relid))
        {
            distPlan->tablesList->diffCount++;
            seenRelids = lappend_oid(seenRelids, rangeTableEntry->relid);
        }
        else
            distPlan->tablesList->simCount++;

        curr_relid = rangeTableEntry->relid;
        distPlan->tablesList->length++;
    }

    distPlan->tablesList->tables = rtes;
}

/*
 * EarlyQueryCheck short-circuits planning for queries that don't touch any
 * distributed spatiotemporal SELECT-able table: it returns a plan produced
 * by Citus' standard distributed_planner() immediately, or NULL to let
 * distributed_mobilitydb_planner_internal() continue with the
 * spatiotemporal-aware planning path.
 */
static PlannedStmt *
EarlyQueryCheck(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *result = NULL;
    ListCell *rangeTableCell = NULL;
    bool res = false;
    if (query_string == NULL)
        return result;
    /* parse->rtable only holds the OUTER query's own range table -- a query
     * that references its distributed spatiotemporal table exclusively
     * inside a CTE (e.g. "WITH Temp AS (SELECT ... FROM trips_16t t1,
     * trips_16t t2 ...) SELECT ... FROM Temp") has just an RTE_CTE entry
     * here, so this loop never saw the real table and always deferred such
     * queries straight to Citus' own planner -- which then rejects a
     * same-table self-join baked inside the CTE outright, since it has no
     * way to push it down or materialize-and-rejoin it the way it can for a
     * CTE referenced (self-joined) from outside. ExtractRangeTableEntryList
     * recurses into CTEs/subqueries so the table is actually found here,
     * letting the query into our own planning pipeline instead. */
    List *rangeTableList = ExtractRangeTableEntryList(parse);
    foreach(rangeTableCell, rangeTableList) {
        RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
        if (rangeTableEntry->rtekind != RTE_RELATION)
            continue;
        if (IsDistributedSpatiotemporalTable(rangeTableEntry->relid) && parse->commandType == CMD_SELECT)
        {
            /* at least one distriubted table */
            res = true;
        }
        /*if(IsReshuffledTable(rangeTableEntry->relid)) {
            res = false;
            break;
        }*/
    }
    if (res)
        return result;
    else
        result = distributed_planner(parse, query_string, cursorOptions, boundParams);
    return result;
}

/* PlanInitialization initializes the distributed plan */
static void
PlanInitialization(DistributedSpatiotemporalQueryPlan *distPlan)
{
    distPlan->tablesList = (STMultirelations *) palloc0(sizeof(STMultirelations));
    distPlan->tablesList->diffCount = 0;
    distPlan->tablesList->simCount = 0;
    distPlan->tablesList->nonStCount = 0;
    distPlan->queryContainsReshuffledTable = false;
    distPlan->tablesList->tables = NIL;
    distPlan->postProcessing =  (PostProcessing *) palloc0(sizeof(PostProcessing));
    distPlan->postProcessing->distfuns = NIL;
    distPlan->postProcessing->coordinatorLevelOperator = (CoordinatorLevelOperator *)
            palloc0(sizeof(CoordinatorLevelOperator));
    distPlan->postProcessing->workerLevelOperator = (WorkerLevelOperator *)
            palloc0(sizeof(WorkerLevelOperator));
    distPlan->tablesList->length = 0;
    distPlan->predicatesList = palloc0(sizeof(PredicateInfo));
    distPlan->predicatesList->predicateInfo = palloc0(sizeof(PredicateInfo));
    distPlan->strategies = NIL;
}

/*
 * checkQueryType analyses the query parameters to plan ahead the query type
 */
static void
checkQueryType(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan)
{
    ProcessQueryPredicates(parse, distPlan);

    /* A self-join whose spatiotemporal predicate lives entirely inside a
     * CTE's own definition (e.g. Q10: "WITH Temp AS (SELECT ...
     * whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0)) ... FROM trips_16t t1, ...,
     * trips_16t t2, ... ) SELECT ... FROM Temp") is invisible to the scan
     * above, since parse->jointree/parse->targetList only cover the OUTER
     * query -- the outer query here just references "Temp" once, with no
     * spatiotemporal predicate of its own. Scan each CTE's own query the
     * same way so its self-join still gets a strategy chosen. */
    ListCell *cteCell;
    foreach(cteCell, parse->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
        if (!IsA(cte->ctequery, Query))
            continue;
        ProcessQueryPredicates((Query *) cte->ctequery, distPlan);
    }
    /* TODO: The rest is excluded for now and will be added after testing the main features */
}

/*
 * ProcessQueryPredicates scans a single query's WHERE clause and, if that
 * doesn't already pick a strategy, its SELECT list, for a registered
 * intersection/distance predicate. Called once for the outer query and once
 * per CTE by checkQueryType, since a CTE's own self-join is otherwise never
 * visible from the outer query's jointree/targetList.
 */
static void
ProcessQueryPredicates(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan)
{
    List *whereClauseList = WhereClauseList(parse->jointree);
    ListCell *clauseCell = NULL;
    if (whereClauseList == NIL && parse->hasSubLinks)
    {
        /* TODO: subquery is excluded for now */
        ereport(ERROR, (errmsg("A sub query is not supported yet in Distributed MobilityDB!")));
    }
    /* Iterate over the where clause conditions */
    foreach(clauseCell, whereClauseList)
    {
        Node *clause = (Node *) lfirst(clauseCell);
        ProcessPredicateClause(distPlan, clause);
    }
    /* A self-join whose only spatiotemporal computation lives in the SELECT
     * list (e.g. MIN(nearestapproachdistance(t1.Trip, t2.Trip)), with no
     * spatiotemporal predicate in the WHERE clause at all) never reaches the
     * loop above, since WhereClauseList only sees WHERE-clause conjuncts --
     * leaving no strategy chosen and Citus rejecting the resulting
     * unconditioned self cross-join. Only run this fallback scan when the
     * WHERE clause didn't already pick a strategy, so existing queries are
     * unaffected. */
    if (list_length(distPlan->strategies) == 0)
    {
        AnalyseSelectListPredicates(parse, distPlan);
    }
}

/*
 * ProcessPredicateClause inspects a single predicate node (either a
 * WHERE-clause conjunct or a spatiotemporal function call found inside the
 * SELECT list) and, if it's a registered intersection/distance operation,
 * chooses the strategy needed to plan it.
 */
static void
ProcessPredicateClause(DistributedSpatiotemporalQueryPlan *distPlan, Node *clause)
{
    if (NodeIsEqualsOpExpr(clause))
        return;

    /* Reference tables are already replicated to every node, so a join
     * against one never needs the NonColocation strategy's reshuffle --
     * Citus can push the predicate down to each shard directly. Subtracting
     * refCount here means a query joining one distributed spatiotemporal
     * table with any number of reference tables is treated the same as a
     * genuine single-table query below. */
    int effectiveDiffCount = distPlan->tablesList->diffCount - distPlan->tablesList->refCount;
    int effectiveLength = distPlan->tablesList->length - distPlan->tablesList->refCount;

    Oid predicateOid;
    List *predicateArgs;

    /*
     * MobilityDB/PostGIS join predicates such as eDwithin(...) or
     * ST_Intersects(...) parse as FuncExpr, not OpExpr -- casting
     * blindly to OpExpr (as this used to) silently failed to
     * recognize them (or worse, read OpExpr-shaped fields out of a
     * FuncExpr node), so joins using them fell through to Citus'
     * own planner, which rejects any join not on distribution
     * columns.
     */
    if (!(GetPredicateOidAndArgs(clause, &predicateOid, &predicateArgs) &&
          predicateOid > 0 && list_length(predicateArgs) >= 2))
        return;

    if (IsIntersectionOperation(predicateOid))
    {
        if (effectiveDiffCount > 1)
        {
            /* Intersection join between two distinct tables: must colocate them first. */
            AddStrategy(distPlan, NonColocation);
        }
        else if (effectiveLength == 1)
        {
            /* Single-table intersection: decide between rebalancing tiles to fit the
             * query's search box or simply pushing the predicate to each worker. */
            Datum rangeBox = get_query_range(distPlan->tablesList, clause);
            if (!IsDatumEmpty(rangeBox) &&
                CheckTileRebalancerActivation(distPlan->tablesList, clause, rangeBox))
            {
                AddStrategy(distPlan, TileScanRebalancer);
                distPlan->range_bbox = rangeBox;
            }
            else
                AddStrategy(distPlan, PredicatePushDown);
        }
        else
        {
            /* By default: multiple references to the same colocated table (self-join). */
            AddStrategy(distPlan, Colocation);
        }
    }
    else if (IsDistanceOperation(predicateOid))
    {
        if (distPlan->tablesList->simCount >= 1)
            AddStrategy(distPlan, Colocation);
        /* The NonColocation strategy is triggered by default until the analysis
         * changes it -- except when the only "different" tables besides one
         * distributed spatiotemporal table are reference tables (refCount > 0
         * guards this so behavior is untouched whenever no reference table is
         * involved), which Citus can push the predicate down to directly with
         * no reshuffle needed. */
        if (effectiveDiffCount > 1 || distPlan->tablesList->refCount == 0)
        {
            AddStrategy(distPlan, NonColocation);
        }
        else if (effectiveLength == 1)
        {
            AddStrategy(distPlan, PredicatePushDown);
        }
        if(distPlan->predicatesList->predicateType == DISTANCE)
        {
            ereport(ERROR, (errmsg("Currently, we do not support using more than "
                                   "one distance operation in the same query !")));
        }
        distPlan->predicatesList->predicateInfo->distancePredicate = (DistancePredicate *)palloc0(
                sizeof(DistancePredicate));
        distPlan->predicatesList->predicateInfo->distancePredicate = analyseDistancePredicate(clause);
        distPlan->predicatesList->predicateType = DISTANCE;
    }
    else
    {
        ListCell *arg;
        foreach(arg, predicateArgs)
        {
            Node *node = (Node *) lfirst(arg);
            if (!IsA(node, Const))
                continue;
            Oid arg_oid = ((Const *)node)->consttype;
            if (IsDistanceOperation(arg_oid))
            {
                if (distPlan->tablesList->simCount >= 1)
                    AddStrategy(distPlan, Colocation);
                distPlan->predicatesList->predicateInfo->distancePredicate =
                        analyseDistancePredicate(node);
                AddStrategy(distPlan, NonColocation);
                distPlan->predicatesList->predicateType = DISTANCE;
            }
        }
    }
}

/*
 * SelectListPredicateWalker recurses through a SELECT-list expression (e.g.
 * into an Aggref's arguments) looking for a registered spatiotemporal
 * predicate function/operator. A matched node is handed to
 * ProcessPredicateClause and not recursed into further, since a registered
 * predicate's own arguments (plain columns) never nest another one.
 */
static bool
SelectListPredicateWalker(Node *node, DistributedSpatiotemporalQueryPlan *distPlan)
{
    if (node == NULL)
        return false;

    if (IsA(node, FuncExpr) || IsA(node, OpExpr))
    {
        Oid predicateOid;
        List *predicateArgs;
        if (GetPredicateOidAndArgs(node, &predicateOid, &predicateArgs) &&
            predicateOid > 0 && list_length(predicateArgs) >= 2 &&
            (IsIntersectionOperation(predicateOid) || IsDistanceOperation(predicateOid)))
        {
            ProcessPredicateClause(distPlan, node);
            return false;
        }
    }
    return expression_tree_walker(node, SelectListPredicateWalker, (void *) distPlan);
}

/*
 * AnalyseSelectListPredicates scans the SELECT list's target entries for a
 * registered spatiotemporal predicate function used inside an aggregate
 * (e.g. MIN(nearestapproachdistance(t1.Trip, t2.Trip))), so a self-join
 * whose only spatiotemporal computation lives in the SELECT list still gets
 * a strategy chosen.
 */
static void
AnalyseSelectListPredicates(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan)
{
    ListCell *cell;
    foreach(cell, parse->targetList)
    {
        TargetEntry *targetEntry = (TargetEntry *) lfirst(cell);
        SelectListPredicateWalker((Node *) targetEntry->expr, distPlan);
    }
}

/*
 * needsDistributedSpatiotemporalPlanning gets the parse tree and the number of distributed spatiotemporal
 * tables and returns true if the query needs the spatiotemporal planner.
 *
 * A prior revision required tablesList->length > 1 before even considering
 * distPlan->strategies, so a single-table query -- even one checkQueryType()
 * had already assigned a PredicatePushDown strategy to -- always fell
 * through to Citus' plain distributed_planner() and never actually engaged
 * this extension's own planning/execution path (or its EXPLAIN output).
 * diffCount > 1 on its own already implies more than one table, so it does
 * not need the length > 1 guard either.
 */
static bool
needsDistributedSpatiotemporalPlanning(DistributedSpatiotemporalQueryPlan *distPlan)
{
    bool res = false;
    if ((list_length(distPlan->strategies) > 0
            || distPlan->tablesList->diffCount > 1
            || list_length(distPlan->postProcessing->distfuns) > 0)
            && !distPlan->queryContainsReshuffledTable)
        res = true;
    distPlan->activate_post_processing_phase = res;
    return res;
}

/* StrategiesInclude returns whether type is among distPlan's chosen strategies. */
static bool
StrategiesInclude(List *strategies, StrategyType type)
{
    ListCell *cell = NULL;
    foreach(cell, strategies)
    {
        if ((StrategyType) lfirst_int(cell) == type)
            return true;
    }
    return false;
}


