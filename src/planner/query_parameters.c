#include "postgres.h"
#include <commands/explain.h>
#include "planner/predicate_management.h"
#include "multirelation/multirelation_utils.h"
#include "planner/distributed_mobilitydb_planner.h"
#include "planner/distributed_mobilitydb_explain.h"
#include "general/rte.h"

static void ExplainMainPredicate(PredicateType predicateType, PredicateInfo * predicateInfo,
                                 ExplainState *es, int indent_group);
static void ExplainDistributedTables(STMultirelations *tablesList, ExplainState *es, int indent_group);
static void ExplainReshufflingPlanInfo(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es,
                                       int indent_group);

/*
 * Explain the query parameters: distributed tables, tiling methods, distributed index, etc
 */
extern void ExplainQueryParameters(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es, int indent_group)
{
    ExplainOpenGroup("QueryParameters", "Query Parameters", true, es);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    es->indent += indent_group;
    appendStringInfo(es->str, "-> Query Parameters: \n");
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    appendStringInfo(es->str, "Joining column: %s\n", distPlan->joining_col);
    ExplainMainPredicate(distPlan->predicatesList->predicateType, distPlan->predicatesList->predicateInfo,
                         es, indent_group);

    ExplainDistributedTables(distPlan->tablesList, es, indent_group);
    if (IsReshufflingRequired(distPlan->strategies))
        ExplainReshufflingPlanInfo(distPlan, es, indent_group);
    ExplainCloseGroup("QueryParameters", "Query Parameters", true, es);
}

static void ExplainMainPredicate(PredicateType predicateType, PredicateInfo * predicateInfo,
                                 ExplainState *es, int indent_group)
{
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    if (predicateType == DISTANCE)
    {
        appendStringInfo(es->str, "Query distance:%.3f\n", predicateInfo->distancePredicate->distance);
        appendStringInfoSpaces(es->str, es->indent * indent_group);
        appendStringInfo(es->str, "Main predicate: Distance-based\n");
    }
    else if (predicateType == INTERSECTION)
    {
        appendStringInfo(es->str, "Main predicate: Intersection-based\n");
    }
}

static void ExplainDistributedTables(STMultirelations *tablesList, ExplainState *es, int indent_group)
{
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    appendStringInfo(es->str, "-> Distributed Tables:%d\n", tablesList->length);
    es->indent += indent_group;
    ExplainOpenGroup("TablesInfo", "Distributed Tables Info", true, es);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    appendStringInfo(es->str, "Number of similar tables: %d\n", tablesList->simCount);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    if (tablesList->diffCount > 1)
        appendStringInfo(es->str, "Number of different tables: %d\n", tablesList->diffCount);
    else
        appendStringInfo(es->str, "Number of different tables: %d\n", 0);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    ListCell *rangeTableCell = NULL;
    char * check = NULL;
    foreach(rangeTableCell, tablesList->tables)
    {
        Rte *rteNode = (Rte *) lfirst(rangeTableCell);
        if (rteNode->RteType == STRte)
        {
            STMultirelation *spatiotemporalTable = (STMultirelation *) rteNode->rte;
            char * relname = get_rel_name(spatiotemporalTable->catalogTableInfo.table_oid);
            if (check == NULL)
            {
                check = palloc((strlen(relname) + 1) * sizeof (char));
                strcpy(check, relname);
            }

            if (strcasecmp(check, relname) == 0)
            {
                indent_group -= 2;
                continue;
            }
            else
            {
                check = palloc((strlen(relname) + 1) * sizeof (char));
                strcpy(check, relname);
            }
            appendStringInfo(es->str, "-> Table: %s\n", relname);
            es->indent += indent_group;

            appendStringInfoSpaces(es->str, es->indent * indent_group);
            appendStringInfo(es->str, "Global Index: %s\n", spatiotemporalTable->catalogTableInfo.tiling_method);
            appendStringInfoSpaces(es->str, es->indent * indent_group);
            appendStringInfo(es->str, "Local Index: %s\n", spatiotemporalTable->localIndex);
            appendStringInfoSpaces(es->str, es->indent * indent_group);
            appendStringInfo(es->str, "Number of tiles: %d\n",spatiotemporalTable->catalogTableInfo.numTiles);
            es->indent -= 2;
            appendStringInfoSpaces(es->str, es->indent * indent_group);
        }
    }
    ExplainCloseGroup("TablesInfo", "Distributed Tables Info", true, es);
}

static void ExplainReshufflingPlanInfo(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es,
                                       int indent_group)
{
    STMultirelation *stReshuffled = NULL;
    CitusRteNode *citusReshuffled = NULL;

    if(distPlan->reshuffledTable->RteType == STRte)
        stReshuffled = (STMultirelation *)distPlan->reshuffledTable->rte;
    else if (distPlan->reshuffledTable->RteType == CitusRte)
        citusReshuffled = (CitusRteNode *)distPlan->reshuffledTable->rte;
    es->indent = 0;
    ExplainOpenGroup("Reshuffling", "Reshuffling", true, es);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    appendStringInfo(es->str, "-> Data Reshuffling:\n");
    es->indent += 6;
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    appendStringInfo(es->str, "-> Required: Yes\n");
    es->indent += indent_group;
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    ExplainOpenGroup("ReshufflingTables", "ReshufflingTables", true, es);
    appendStringInfo(es->str, "-> Reshuffled tables:\n");
    es->indent += indent_group;

    appendStringInfoSpaces(es->str, es->indent * indent_group);
    if(distPlan->reshuffledTable->RteType == STRte)
        appendStringInfo(es->str, "-> Table: %s\n", get_rel_name(
                stReshuffled->catalogTableInfo.table_oid));
    else if (distPlan->reshuffledTable->RteType == CitusRte)
        appendStringInfo(es->str, "-> Table: %s\n", get_rel_name(
                ((RangeTblEntry *) lfirst(citusReshuffled->rangeTableCell))->relid));

    es->indent += indent_group;
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    if(distPlan->reshuffledTable->RteType == STRte)
        appendStringInfo(es->str, "-> Reshuffling column: %s\n", stReshuffled->col);
    else if (distPlan->reshuffledTable->RteType == CitusRte)
        appendStringInfo(es->str, "-> Reshuffling column: %s\n", citusReshuffled->col);
    ExplainCloseGroup("ReshufflingTables", "ReshufflingTables", true, es);
    ExplainCloseGroup("Reshuffling", "Reshuffling", true, es);
}

