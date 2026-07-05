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
#include <commands/explain.h>
#include <distributed/pg_dist_partition.h>
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

/* ExplainMainPredicate prints the query's dominant predicate kind (distance threshold or intersection). */
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

/*
 * ExplainDistributedTables prints each distinct spatiotemporal table
 * touched by the query along with its tiling method, local index, and tile
 * count; repeated references to the same table (self-joins) are skipped
 * after the first.
 *
 * The header counts distinguish genuinely-distributed (tiled) spatiotemporal
 * tables from replicated reference tables -- tablesList->length/diffCount
 * count every range-table entry or distinct relid regardless of kind, so a
 * query joining one tiled table against several reference tables (e.g. a
 * self-join on trips_16t plus 4 reference-table references) used to print
 * as "Distributed Tables:6" / "different tables: 4", reading as if several
 * genuinely-sharded tables were involved instead of one.
 */
static void ExplainDistributedTables(STMultirelations *tablesList, ExplainState *es, int indent_group)
{
    List *seenDistributedRelids = NIL;
    List *seenReferenceRelids = NIL;
    int distributedOccurrences = 0;
    int referenceOccurrences = 0;
    ListCell *countCell = NULL;
    foreach(countCell, tablesList->tables)
    {
        Rte *rteNode = (Rte *) lfirst(countCell);
        if (rteNode->RteType == STRte)
        {
            STMultirelation *st = (STMultirelation *) rteNode->rte;
            distributedOccurrences++;
            if (!list_member_oid(seenDistributedRelids, st->catalogTableInfo.table_oid))
                seenDistributedRelids = lappend_oid(seenDistributedRelids, st->catalogTableInfo.table_oid);
        }
        else if (rteNode->RteType == CitusRte)
        {
            CitusRteNode *citusRte = (CitusRteNode *) rteNode->rte;
            /* Reference tables report Citus' "none" partition method
             * ('n', DISTRIBUTE_BY_NONE) -- the same value plain Citus
             * local tables report, but a CitusRteNode only ever exists for
             * a hash/range-distributed table or a reference table (see
             * analyzeDistributedSpatiotemporalTables), so "not hash, not
             * range" reliably means "reference table" here. */
            if (citusRte->partitionMethod != DISTRIBUTE_BY_HASH &&
                citusRte->partitionMethod != DISTRIBUTE_BY_RANGE)
            {
                Oid relid = ((RangeTblEntry *) lfirst(citusRte->rangeTableCell))->relid;
                referenceOccurrences++;
                if (!list_member_oid(seenReferenceRelids, relid))
                    seenReferenceRelids = lappend_oid(seenReferenceRelids, relid);
            }
        }
    }

    int distinctDistributedCount = list_length(seenDistributedRelids);

    appendStringInfoSpaces(es->str, es->indent * indent_group);
    appendStringInfo(es->str, "-> Distributed Tables:%d\n", distinctDistributedCount);
    es->indent += indent_group;
    ExplainOpenGroup("TablesInfo", "Distributed Tables Info", true, es);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    /* "Similar"/"different" here are scoped to genuinely-distributed (tiled)
     * tables only -- self-joins (e.g. trips_16t as both t1 and t2) count as
     * "similar", and distinct distributed tables (e.g. two different tiled
     * tables joined together) count as "different". Reference tables are
     * reported separately below, never folded into either count. */
    appendStringInfo(es->str, "Number of similar tables: %d\n",
                     distributedOccurrences - distinctDistributedCount);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    appendStringInfo(es->str, "Number of different tables: %d\n", distinctDistributedCount);
    appendStringInfoSpaces(es->str, es->indent * indent_group);
    if (referenceOccurrences > 0)
        appendStringInfo(es->str, "Replicated (reference) tables: %d (%d reference%s)\n",
                         list_length(seenReferenceRelids), referenceOccurrences,
                         referenceOccurrences == 1 ? "" : "s");
    else
        appendStringInfo(es->str, "Replicated (reference) tables: 0\n");
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
            appendStringInfo(es->str, "Local Index: %s\n",
                             spatiotemporalTable->localIndex ? spatiotemporalTable->localIndex : "none");
            appendStringInfoSpaces(es->str, es->indent * indent_group);
            appendStringInfo(es->str, "Number of tiles: %d\n",spatiotemporalTable->catalogTableInfo.numTiles);
            es->indent -= 2;
            appendStringInfoSpaces(es->str, es->indent * indent_group);
        }
    }
    ExplainCloseGroup("TablesInfo", "Distributed Tables Info", true, es);
}

/*
 * ExplainReshufflingPlanInfo prints which table gets reshuffled for a
 * NonColocation-strategy query and which column drives the reshuffle,
 * shown only when IsReshufflingRequired() is true.
 */
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

