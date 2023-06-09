/*-------------------------------------------------------------------------
 *
 * distributed_mobilitydb_planner.c
 *	  General Distributed MobilityDB planner code.
 *
 *-------------------------------------------------------------------------
 */
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

/* Distributed spatiotemporal planner */
PlannedStmt *
distributed_mobilitydb_planner(Query *parse, const char *query_string, int cursorOptions,
                       ParamListInfo boundParams)
{
    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan = (DistributedSpatiotemporalQueryPlan *)
            palloc0(sizeof(DistributedSpatiotemporalQueryPlan));
    return distributed_mobilitydb_planner_internal(parse, query_string, cursorOptions, boundParams,
                                           distributedSpatiotemporalPlan, false);
}

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
        return distributed_planner(parse, query_string, cursorOptions, boundParams);
    analyzeDistributedSpatiotemporalTables(rangeTableList, distPlan);

    if (distPlan->tablesList->length == 0 )
        return distributed_planner(parse, query_string, cursorOptions, boundParams);

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
            OpExpr *opExpr = (OpExpr *) clause;
            if (opExpr->opno > 0 && list_length(opExpr->args) >= 2)
            {
                if (IsIntersectionOperation(opExpr->opno))
                {
                    if (distPlan->tablesList->diffCount > 1)
                    {
                        AddStrategy(distPlan, NonColocation);
                    }
                    else if (distPlan->tablesList->length == 1)
                    {
                        Datum rangeBox = get_query_range(distPlan->tablesList, opExpr);
                        if (!IsDatumEmpty(rangeBox) &&
                            CheckTileRebalancerActivation(distPlan->tablesList, opExpr, rangeBox))
                        {
                            AddStrategy(distPlan, TileScanRebalancer);
                            distPlan->range_bbox = rangeBox;
                        }
                        else
                            AddStrategy(distPlan, PredicatePushDown);
                    }
                    else
                    {
                        /* By default */
                        AddStrategy(distPlan, Colocation);
                    }
                }
                else if (IsDistanceOperation(opExpr->opno))
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
                    foreach(arg, opExpr->args)
                    {
                        Node *node = (Node *) lfirst(arg);
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
 */
static bool
needsDistributedSpatiotemporalPlanning(DistributedSpatiotemporalQueryPlan *distPlan)
{
    bool res = false;
    if (((distPlan->tablesList->length > 1 && (
            list_length(distPlan->strategies) > 0 || distPlan->tablesList->diffCount > 1))
            || list_length(distPlan->postProcessing->distfuns) > 0)
            && !distPlan->queryContainsReshuffledTable)
        res = true;
    distPlan->activate_post_processing_phase = res;
    return res;
}


