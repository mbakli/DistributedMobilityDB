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
#include "general/spatiotemporal_processing.h"
#include "general/rte.h"


static void analyzeDistributedSpatiotemporalTables(List *rangeTableList,
                                       DistributedSpatiotemporalQueryPlan *distPlan);
static void PlanInitialization(DistributedSpatiotemporalQueryPlan *distPlan);
static void checkQueryType(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan);
static bool needsDistributedSpatiotemporalPlanning(DistributedSpatiotemporalQueryPlan *distPlan);
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
    }

    /* Query rewriter */
    if (list_length(distPlan->postProcessing->distfuns) > 0 )
        RewriterDistFuncs(parse, distPlan->postProcessing, query_string);
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
    bool shapeType;
    List *rtes = NIL;
    foreach(rangeTableCell, rangeTableList)
    {
        RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
        if (rangeTableEntry->rtekind != RTE_RELATION) {
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
                if (partitioningMethod == DISTRIBUTE_BY_HASH || partitioningMethod == DISTRIBUTE_BY_RANGE)
                {
                    /* Citus table processing */
                    CitusRteNode *citusNode = GetCitusRteInfo(rangeTableEntry,partitioningMethod);
                    citusNode->rangeTableCell = rangeTableCell;
                    distPlan->tablesList->length++;
                    Rte *rteNode = GetRteNode((Node *) citusNode, CitusRte, rangeTableEntry->alias);
                    rtes = lappend(rtes , rteNode);
                    distPlan->tablesList->nonStCount++;
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
        if (curr_relid != rangeTableEntry->relid)
        {
            distPlan->tablesList->diffCount++;
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
    foreach(rangeTableCell, parse->rtable) {
        RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
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
    // extract where clause qualifiers and verify we can plan for them

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

        if (!NodeIsEqualsOpExpr(clause))
        {
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
            if (GetPredicateOidAndArgs(clause, &predicateOid, &predicateArgs) &&
                predicateOid > 0 && list_length(predicateArgs) >= 2)
            {
                if (IsIntersectionOperation(predicateOid))
                {
                    if (distPlan->tablesList->diffCount > 1)
                    {
                        /* Intersection join between two distinct tables: must colocate them first. */
                        AddStrategy(distPlan, NonColocation);
                    }
                    else if (distPlan->tablesList->length == 1)
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
                    /* The NonColocation strategy is triggered by default until the analysis changes it */
                    if (distPlan->tablesList->simCount >= 1)
                        AddStrategy(distPlan, Colocation);
                    AddStrategy(distPlan, NonColocation);
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
        }
    }
    /* TODO: The rest is excluded for now and will be added after testing the main features */
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


