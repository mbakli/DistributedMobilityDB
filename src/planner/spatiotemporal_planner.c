/*-------------------------------------------------------------------------
 *
 * spatiotemporal_planner.c
 *	  General Distributed MobilityDB planner code.
 *
 *-------------------------------------------------------------------------
 */
#include "distributed/distributed_planner.h"
#include "general/catalog_management.h"
#include "distributed/multi_executor.h"
#include "nodes/nodeFuncs.h"
#include "distributed_functions/distributed_function.h"
#include "planner/planner_utils.h"
#include "executor/multi_phase_executor.h"
#include "planner/planner_strategies.h"
#include "planner/spatiotemporal_planner.h"

static void analyzeDistributedSpatiotemporalTables(List *rangeTableList,
                                       DistributedSpatiotemporalQueryPlan *distPlan);
static void PlanInitialization(DistributedSpatiotemporalQueryPlan *distPlan);
static void analyseSelectClause(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan);
static void checkQueryType(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan);
static bool needsDistributedSpatiotemporalPlanning(DistributedSpatiotemporalQueryPlan *distPlan);
static void AddStrategy(DistributedSpatiotemporalQueryPlan *distPlan, PlannerStrategy strategy);
static PlannedStmt * EarlyQueryCheck(Query *parse, const char *query_string, int cursorOptions,
                                     ParamListInfo boundParams);

/* Distributed spatiotemporal planner */
PlannedStmt *
spatiotemporal_planner(Query *parse, const char *query_string, int cursorOptions,
                       ParamListInfo boundParams)
{
    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan = (DistributedSpatiotemporalQueryPlan *)
            palloc0(sizeof(DistributedSpatiotemporalQueryPlan));
    return spatiotemporal_planner_internal(parse, query_string, cursorOptions, boundParams,
                                           distributedSpatiotemporalPlan, false);
}

PlannedStmt *
spatiotemporal_planner_internal(Query *parse, const char *query_string, int cursorOptions,
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
    analyzeDistributedSpatiotemporalTables(rangeTableList, distPlan);
    if (distPlan->tablesList->length == 0 || query_string == NULL)
        return distributed_planner(parse, query_string, cursorOptions, boundParams);
    analyseSelectClause(parse, distPlan);
    if (query_string!= NULL && distPlan->tablesList->length > 0 && !distPlan->queryContainsReshuffledTable)
    {
        /* TODO: Excluded for now and will be added after testing the main features */
        // analyseOrderByClause(parse, distPlan);
        checkQueryType(parse, distPlan);
        needsSpatiotemporalPlanning = needsDistributedSpatiotemporalPlanning(distPlan);
    }
    if (needsSpatiotemporalPlanning)
    {
        /* Keep the original query string for later */
        distPlan->org_query_string = palloc((strlen(query_string) + 1) * sizeof (char));
        strcpy(distPlan->org_query_string, toLower((char *)query_string));
        ListCell *cell = NULL;
        foreach(cell, distPlan->strategies)
        {
            PlannerStrategy plannerStrategy = (PlannerStrategy) lfirst_int(cell);
            if (plannerStrategy == Colocation)
                ColocationStrategyPlan(distPlan);
            else if (plannerStrategy == NonColocation)
                NonColocationStrategyPlan(distPlan);
            else if (plannerStrategy == TileScanRebalancer)
                TileScanRebalanceStrategyPlan(distPlan);
            else
                ereport(ERROR, (errmsg("This query is not supported yet!")));
        }
        /* Plan the post processing phase */
        if (distPlan->activate_post_processing_phase)
            distPlan->postProcessing = PostProcessingQuery(distPlan->strategies);
        /* Executor */
        GeneralScan *generalScan = QueryExecutor(distPlan, explain);
        result = distributed_planner(generalScan->query, generalScan->query_string->data,
                                     cursorOptions, boundParams);
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
                                       DistributedSpatiotemporalQueryPlan *distPlan) {
    /* TODO: Find a way to stop the function if it is not a spatiotemporal query */
    ListCell *rangeTableCell = NULL;
    Oid curr_relid = -1;
    bool shapeType;
    List *spatiotemporal_tables = NIL;

    foreach(rangeTableCell, rangeTableList) {
        RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
        if (rangeTableEntry->rtekind != RTE_RELATION) {
            continue;
        }
        if (IsDistributedSpatiotemporalTable(rangeTableEntry->relid)) {
            if(IsReshuffledTable(rangeTableEntry->relid))
            {
                distPlan->queryContainsReshuffledTable = true;
                return;
            }
            shapeType = DistributedColumnType(rangeTableEntry->relid);
            if (shapeType == SPATIAL || shapeType == SPATIOTEMPORAL)
            {
                SpatiotemporalTable *spatiotemporal_table =
                        (SpatiotemporalTable *) palloc0(sizeof(SpatiotemporalTable));
                spatiotemporal_table->shapeType = shapeType;
                spatiotemporal_table->col = GetSpatiotemporalCol(rangeTableEntry->relid);
                spatiotemporal_table->localIndex = GetLocalIndex(rangeTableEntry->relid,
                                                                 spatiotemporal_table->col);
                spatiotemporal_table->catalogTableInfo = GetTilingSchemeInfo(rangeTableEntry->relid);
                if (curr_relid != rangeTableEntry->relid)
                {
                    distPlan->tablesList->numDiffTables++;
                    distPlan->joining_col = spatiotemporal_table->col;
                }
                else
                    distPlan->tablesList->numSimTables++;
                curr_relid = rangeTableEntry->relid;
                spatiotemporal_tables = lappend(spatiotemporal_tables , spatiotemporal_table);
                distPlan->tablesList->length++;
            }
        }
    }
    distPlan->tablesList->tables = spatiotemporal_tables;
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
            res = true;
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
    distPlan->tablesList = (SpatiotemporalTables *) palloc0(sizeof(SpatiotemporalTables));
    distPlan->tablesList->numDiffTables = 0;
    distPlan->tablesList->numSimTables = 0;
    distPlan->queryContainsReshuffledTable = false;
    distPlan->tablesList->tables = NIL;
    distPlan->postProcessing =  (PostProcessing *) palloc0(sizeof(PostProcessing));
    distPlan->postProcessing->distfuns = NIL;
    distPlan->tablesList->length = 0;
    distPlan->predicatesList = palloc0(sizeof(PredicateInfo));
    distPlan->predicatesList->predicateInfo = palloc0(sizeof(PredicateInfo));
    distPlan->strategies = NIL;
}

/*
 * analyseSelectClause analyses the select clause and
 * detects the distributed functions, segmented objects, etc
 */
static void
analyseSelectClause(Query *parse, DistributedSpatiotemporalQueryPlan *distPlan)
{
    List *targetList = parse->targetList;
    ListCell *targetEntryCell = NULL;
    foreach(targetEntryCell, targetList)
    {
        /* Process the input functions */
        TargetEntry *targetEntry = lfirst(targetEntryCell);
        if (targetEntry->resname != NULL && IsDistFunc(targetEntry))
        {
            // Add the distributed function to the list of the post processing operations
            DistributedFunction *dist_function = addDistributedFunction(targetEntry);
            distPlan->postProcessing->distfuns = lappend(distPlan->postProcessing->distfuns,
                                                          dist_function);
        }
    }
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
                    if (distPlan->tablesList->numDiffTables > 1)
                    {
                        AddStrategy(distPlan, NonColocation);
                    }
                    else if (distPlan->tablesList->length == 1)
                    {
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
                    AddStrategy(distPlan, NonColocation);
                    if(distPlan->predicatesList->predicateType == DISTANCE)
                    {
                        ereport(ERROR, (errmsg("Currently, we do not support using more than one distance operation!")));
                    }
                    distPlan->predicatesList->predicateInfo->distancePredicate = (DistancePredicate *)palloc0(sizeof(DistancePredicate));
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
                            distPlan->predicatesList->predicateInfo->distancePredicate = analyseDistancePredicate(node);
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
    if (distPlan->tablesList->length > 1 && (
            list_length(distPlan->strategies) > 0 || distPlan->tablesList->numDiffTables > 1)
            && !distPlan->queryContainsReshuffledTable)
        res = true;
    distPlan->activate_post_processing_phase = res;
    return res;
}

void AddStrategy(DistributedSpatiotemporalQueryPlan *distPlan, PlannerStrategy strategy)
{
    distPlan->strategies = lappend(distPlan->strategies, (Datum *)strategy);
}