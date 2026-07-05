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
#include "planner/planner_strategies.h"
#include "general/rte.h"
#include <utils/lsyscache.h>
#include <executor/spi.h>
#include <distributed/multi_executor.h>

static void planReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan);
static void chooseReshuffledTable(DistributedSpatiotemporalQueryPlan *distPlan);
static void createReshufflingTablePlan(DistributedSpatiotemporalQueryPlan *distPlan);
static Datum GetReshufflingAlias(Oid oid);
static void DistanceReshufflingPlan(DistributedSpatiotemporalQueryPlan *distPlan);
static void OtherReshufflingPlan(DistributedSpatiotemporalQueryPlan *distPlan);
static void ConstructReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan);
static char *getReshuffledColumns(DistributedSpatiotemporalQueryPlan *distPlan, Oid oid);
static void PlanReshufflingStRtes(DistributedSpatiotemporalQueryPlan *distPlan);
static void PlanReshufflingNonStRteWithStRte(DistributedSpatiotemporalQueryPlan *distPlan,
                                             ReshufflingRte *rte_node);
static void createReshufflingPlanForNonstRte(DistributedSpatiotemporalQueryPlan *distPlan);
static void ConstructReshufflingQueryForNonstRte(DistributedSpatiotemporalQueryPlan *distPlan);

/*
 * NonColocationStrategyPlan handles a join between tables that don't
 * already share the same tiling scheme: it first builds a reshuffling plan
 * (planReshufflingQuery) that will copy one side into a colocated
 * temporary table at execution time, then records a single NonColocation
 * PlanTask joining the (still-to-be-created) reshuffled table against the
 * base spatiotemporal table.
 */
extern void
NonColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* Create a reshuffling plan */
    planReshufflingQuery(distPlan);
    PlanTask * strategy = (PlanTask *) palloc0(sizeof(PlanTask));
    STMultirelation *reshuffledTable = (STMultirelation *)distPlan->reshuffledTable->rte;
    /* Strategy type */
    strategy->type = NonColocation;
    strategy->tbl1 = distPlan->reshuffled_table_base;
    strategy->tbl2 = reshuffledTable;
    strategy->tileKey = (Datum) reshuffledTable->catalogTableInfo.tileKey;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}

/*
 * planReshufflingQuery picks which reshuffling path applies to the query:
 * joining a spatiotemporal table against a plain Citus table
 * (PlanReshufflingNonStRteWithStRte) or joining two spatiotemporal tables
 * with different tiling schemes (PlanReshufflingStRtes). Joins among only
 * plain Citus tables are not yet handled here.
 */
static void
planReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan)
{
    if (distPlan->tablesList->stCount > 0 && distPlan->tablesList->nonStCount > 0)
    {
        /* At least one tiling scheme exists */
        ReshufflingRte *rte_node = GetReshufflingRte(distPlan->tablesList);
        PlanReshufflingNonStRteWithStRte(distPlan, rte_node);
    }
    else if (distPlan->tablesList->stCount > 0)
    {
        /* No tiling scheme */
        PlanReshufflingStRtes(distPlan);
    }
    else if (distPlan->tablesList->nonStCount > 0)
    {
        /* No tiling scheme */
        //PlanReshufflingNonStRte(distPlan->tablesList);
    }
}

/*
 * PlanReshufflingNonStRteWithStRte plans reshuffling the query's plain
 * Citus/local table (`distPlan->reshuffledTable`) to match rte_node's
 * spatiotemporal table's tiling scheme, which becomes the join's base
 * table. A LocalRte may in principle be broadcast instead of reshuffled
 * (see CheckBroadcastingPossibility), but that path is not yet implemented.
 */
static void
PlanReshufflingNonStRteWithStRte(DistributedSpatiotemporalQueryPlan *distPlan, ReshufflingRte *rte_node)
{
    ListCell *rangeTableCell = NULL;
    /* Set base table for other relations */
    distPlan->reshuffled_table_base = rte_node->stMultirelation;
    foreach(rangeTableCell, distPlan->tablesList->tables)
    {
        Rte * rteNode = (Rte *) lfirst(rangeTableCell);
        /* TODO(known bug, tracked separately -- not fixed here): Rte.RteType
         * is declared `bool` in include/general/rte.h but the RteType enum
         * it holds has three values (STRte=0, CitusRte=1, LocalRte=2);
         * storing LocalRte truncates to the same bool value CitusRte
         * produces, so `== LocalRte` (comparing against the int literal 2)
         * can never be true here. Needs Rte.RteType changed to the real
         * enum type plus an audit of every ->RteType comparison in the
         * codebase before it's safe to fix. */
        // cppcheck-suppress compareBoolExpressionWithInt
        if (rteNode->RteType == CitusRte || rteNode->RteType == LocalRte)
            distPlan->reshuffledTable = rteNode;
        else if (rteNode->RteType == STRte){
        }
        else
            elog(ERROR, "The reshuffling node is not identified!");
    }
    /* Add the alias name */
    if (distPlan->reshuffledTable->RteType == CitusRte)
    {
        CitusRteNode *citusNode = (CitusRteNode *)distPlan->reshuffledTable->rte;
        citusNode->reshuffledTable = DatumGetCString(GetReshufflingAlias(
                ((RangeTblEntry *)lfirst(citusNode->rangeTableCell))->relid));
        createReshufflingPlanForNonstRte(distPlan);
    }
    // cppcheck-suppress compareBoolExpressionWithInt -- see TODO above on the same known Rte.RteType bug
    else if (distPlan->reshuffledTable->RteType == LocalRte)
    {
        /* The rte can be either broadcasted or partitioned using the same tiling scheme of the given
         * spatiotemporal multirelation
         * */
        LocalRteNode *localNode = (LocalRteNode *)distPlan->reshuffledTable->rte;
        localNode->refCandidate = CheckBroadcastingPossibility(localNode);
        localNode->reshuffledTable = DatumGetCString(GetReshufflingAlias(
                ((RangeTblEntry *)lfirst(localNode->rangeTableCell))->relid));
        if (localNode->refCandidate)
        {
            /* TODO: there is a bug that needs to be fixed */
            //createReshufflingPlanForRefRte(distPlan);
        }
        else
            createReshufflingPlanForNonstRte(distPlan);
    }

    ConstructReshufflingQueryForNonstRte(distPlan);
}

/*
 * PlanReshufflingStRtes plans reshuffling for a join between two
 * spatiotemporal tables with different tiling schemes: pick which one
 * becomes the base and which gets reshuffled (chooseReshuffledTable),
 * build the catalog query that computes the new tile assignment
 * (createReshufflingTablePlan), then wrap it into the INSERT that performs
 * the reshuffle (ConstructReshufflingQuery).
 */
static void
PlanReshufflingStRtes(DistributedSpatiotemporalQueryPlan *distPlan)
{
    chooseReshuffledTable(distPlan);
    createReshufflingTablePlan(distPlan);
    ConstructReshufflingQuery(distPlan);
}

/*
 * chooseReshuffledTable picks the table with the most tiles as the base
 * (reshuffled_table_base) and the other as the one to be reshuffled
 * (reshuffledTable) — reshuffling the smaller side is cheaper. The cost
 * function is a placeholder (tile count only) until further testing.
 */
static void
chooseReshuffledTable(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* TODO: Add the cost function after testing the main features */
    ListCell *rangeTableCell = NULL;
    int base_tbl = 0;
    foreach(rangeTableCell, distPlan->tablesList->tables)
    {
        Rte * rteNode = (Rte *) lfirst(rangeTableCell);
        if (rteNode->RteType == STRte)
        {
            STMultirelation * spatiotemporalTable = (STMultirelation *) rteNode->rte;
            if ( spatiotemporalTable->catalogTableInfo.numTiles > base_tbl)
            {
                base_tbl = spatiotemporalTable->catalogTableInfo.numTiles;
                distPlan->reshuffled_table_base = spatiotemporalTable;
            }
            else
                distPlan->reshuffledTable = rteNode;
        }
    }
    if (distPlan->reshuffledTable->RteType == STRte)
    {
        STMultirelation *reshuffledTable = (STMultirelation *)distPlan->reshuffledTable->rte;
        reshuffledTable->catalogTableInfo.reshuffledTable = (char *)GetReshufflingAlias(
                reshuffledTable->catalogTableInfo.table_oid);
    }
}

/*
 * ConstructReshufflingQueryForNonstRte builds the INSERT that copies the
 * plain Citus table's rows into its reshuffled counterpart, tagging each
 * row with the id of the spatiotemporal tile whose bounding box it falls
 * into (computed by the catalog_query_string CTE built in
 * createReshufflingPlanForNonstRte).
 */
static void
ConstructReshufflingQueryForNonstRte(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* Add the reshuffling query */
    StringInfo reshufflingQuery = makeStringInfo();

    if (distPlan->reshuffledTable->RteType == CitusRte)
    {
        CitusRteNode *reshuffledTable = (CitusRteNode *)distPlan->reshuffledTable->rte;
        /* Copy structure of the second table into the reshuffled table */
        char *reshuffledTableColumns = getReshuffledColumns(distPlan,
                                                            ((RangeTblEntry *)lfirst(reshuffledTable->rangeTableCell))->relid);
        /* Generate the final CTE for the reshuffling data*/
        appendStringInfo(reshufflingQuery, "WITH TEMP AS (%s) "
                                           "INSERT INTO %s.%s "
                                           "SELECT %s, C.id "
                                           "FROM %s T, TEMP C "
                                           "WHERE T.%s && C.reshufflingBbox;",
                         distPlan->catalog_query_string, Var_Schema,
                         reshuffledTable->reshuffledTable,
                         reshuffledTableColumns,
                         get_rel_name(((RangeTblEntry *)lfirst(reshuffledTable->rangeTableCell))->relid),
                         reshuffledTable->col
        );
    }
    distPlan->reshuffling_query = palloc((strlen(reshufflingQuery->data) + 1) * sizeof (char));
    strcpy(distPlan->reshuffling_query, reshufflingQuery->data);
}

/* GetReshufflingAlias returns the temporary reshuffled-table name for oid, i.e. "<table>_reshuffled". */
static Datum
GetReshufflingAlias(Oid oid)
{
    StringInfo tableName = makeStringInfo();
    appendStringInfo(tableName, "%s%s", get_rel_name(oid), Var_Temp_Reshuffled);
    return PointerGetDatum(tableName->data);
}

/* Create the reshuffling table plan */
static void
createReshufflingTablePlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    if (distPlan->predicatesList->predicateType == DISTANCE)
    {
        /* This plan requires expanding the bounding box of each tile using the given distance */
        DistanceReshufflingPlan(distPlan);
    }
    else
    {
        OtherReshufflingPlan(distPlan);
    }
}

/*
 * createReshufflingPlanForNonstRte builds the catalog query that maps each
 * tile of the base spatiotemporal table to its bounding box, used to
 * assign the plain Citus table's rows to a tile in
 * ConstructReshufflingQueryForNonstRte.
 */
static void
createReshufflingPlanForNonstRte(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* Prepare reshuffling plan */
    StringInfo catalogQuery = makeStringInfo();
    char bbox_col[30];
    if (distPlan->shapeType == SPATIOTEMPORAL)
        strcpy(bbox_col, Var_MobilityDB_BBOX);
    else if (distPlan->shapeType == SPATIAL)
        strcpy(bbox_col, Var_PostGIS_BBOX);
    // Prepare the select clause
    appendStringInfo(catalogQuery, "SELECT S.%s id, S.%s as reshufflingBbox ",
                     Var_Catalog_Tile_Key, bbox_col);
    // Prepare the from clause
    appendStringInfo(catalogQuery, "FROM %s T, %s S ",
                     Var_Dist_Tables, Var_Table_Tiles);
    // Prepare the where clause
    appendStringInfo(catalogQuery, "WHERE T.tbloid=%d AND T.id = S.table_id ",
                     distPlan->reshuffled_table_base->catalogTableInfo.table_oid);
    // Keep the catalog query in the distributed plan
    distPlan->catalog_query_string = palloc((strlen(catalogQuery->data) + 1) * sizeof (char));
    strcpy(distPlan->catalog_query_string, catalogQuery->data);
}
/*
 * DistanceReshufflingPlan builds the catalog query that pairs each tile of
 * the base table with every tile of the other table whose bounding box
 * intersects the base tile's box expanded by the query's distance
 * threshold — i.e. every tile pair that could possibly contain a match for
 * an eDwithin-style predicate.
 */
static void
DistanceReshufflingPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    StringInfo catalogQuery = makeStringInfo();
    char bbox_col[30];
    char expand_op[30];
    STMultirelation *reshuffledTable = (STMultirelation *)distPlan->reshuffledTable->rte;
    if (distPlan->shapeType == SPATIOTEMPORAL)
    {
        strcpy(bbox_col, Var_MobilityDB_BBOX);
        strcpy(expand_op, Var_MobilityDB_Expand);
        // Prepare the select clause
        appendStringInfo(catalogQuery, "SELECT S1.%s id1, S2.%s id2, "
                                       "S2.%s * %s(S1.%s, %f) reshufflingBbox ",
                         Var_Catalog_Tile_Key, Var_Catalog_Tile_Key, bbox_col,
                         expand_op, bbox_col, distPlan->distance);
    }
    else
    {
        strcpy(bbox_col, Var_PostGIS_BBOX);
        strcpy(expand_op, Var_PostGIS_Expand);
        // Prepare the select clause
        appendStringInfo(catalogQuery, "SELECT S1.%s id1, S2.%s id2, "
                                       "st_intersection(S2.%s, %s(S1.%s, %f)) reshufflingBbox ",
                         Var_Catalog_Tile_Key, Var_Catalog_Tile_Key, bbox_col,
                         expand_op, bbox_col,
                         distPlan->predicatesList->predicateInfo->distancePredicate->distance);
    }
    // Prepare the from clause
    appendStringInfo(catalogQuery, "FROM %s T1, %s S1, %s T2, %s S2 ",
                     Var_Dist_Tables, Var_Table_Tiles, Var_Dist_Tables, Var_Table_Tiles);
    // Prepare the where clause
    appendStringInfo(catalogQuery, "WHERE T1.tableName='%s' AND t2.tableName='%s' AND T1.id = S1.table_id "
                                   "AND T2.id = S2.table_id "
                                   "%s AND S2.%s && %s(S1.%s,%f) ORDER BY id1,id2 ",
                     get_rel_name(distPlan->reshuffled_table_base->catalogTableInfo.table_oid),
                     get_rel_name(reshuffledTable->catalogTableInfo.table_oid),
                     (reshuffledTable->catalogTableInfo.table_oid ==
                      distPlan->reshuffled_table_base->catalogTableInfo.table_oid) ? " AND S1.id < S2.id ":"",
                     bbox_col, expand_op, bbox_col,
                     distPlan->distance);
    // Keep the catalog query in the distributed plan
    distPlan->catalog_query_string = palloc((strlen(catalogQuery->data) + 1) * sizeof (char));
    strcpy(distPlan->catalog_query_string, catalogQuery->data);
}

/*
 * OtherReshufflingPlan is DistanceReshufflingPlan()'s counterpart for
 * non-distance (e.g. intersection) predicates: it pairs tiles whose
 * bounding boxes directly intersect, with no distance expansion.
 */
static void
OtherReshufflingPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    STMultirelation *reshuffledTable = (STMultirelation *)distPlan->reshuffledTable->rte;
    StringInfo catalogQuery = makeStringInfo();
    char bbox_col[30];
    if (distPlan->shapeType == SPATIOTEMPORAL)
    {
        strcpy(bbox_col, Var_MobilityDB_BBOX);
        // Prepare the select clause
        appendStringInfo(catalogQuery, "SELECT S1.%s id1, S2.%s id2, "
                                       "S2.%s * S1.%s as reshufflingBbox ",
                         Var_Catalog_Tile_Key, Var_Catalog_Tile_Key, bbox_col, bbox_col);
    }
    else
    {
        strcpy(bbox_col, Var_PostGIS_BBOX);
        // Prepare the select clause
        appendStringInfo(catalogQuery, "SELECT S1.%s id1, S2.%s id2, "
                                       "st_intersection(S2.%s, S1.%s) reshufflingBbox ",
                         Var_Catalog_Tile_Key, Var_Catalog_Tile_Key, bbox_col, bbox_col);
    }
    // Prepare the from clause
    appendStringInfo(catalogQuery, "FROM %s T1, %s S1, %s T2, %s S2 ",
                     Var_Dist_Tables, Var_Table_Tiles, Var_Dist_Tables, Var_Table_Tiles);
    // Prepare the where clause
    appendStringInfo(catalogQuery, "WHERE T1.tbloid=%d AND t2.tbloid=%d AND T1.id = S1.table_id AND "
                                   "T2.id = S2.table_id "
                                   "%s AND S2.%s && S1.%s ORDER BY id1,id2 ",
                     distPlan->reshuffled_table_base->catalogTableInfo.table_oid,
                     reshuffledTable->catalogTableInfo.table_oid,
                     (reshuffledTable->catalogTableInfo.table_oid ==
                      distPlan->reshuffled_table_base->catalogTableInfo.table_oid) ?
                     " AND S1.id < S2.id ":"",
                     bbox_col, bbox_col);
    // Keep the catalog query in the distributed plan
    distPlan->catalog_query_string = palloc((strlen(catalogQuery->data) + 1) * sizeof (char));
    strcpy(distPlan->catalog_query_string, catalogQuery->data);
}

/*
 * CheckBroadcastingPossibility would determine whether localNode is small
 * enough to broadcast to every worker instead of reshuffling it into
 * tiles. Not yet implemented (always returns false); see the TODO in
 * PlanReshufflingNonStRteWithStRte().
 */
extern bool
CheckBroadcastingPossibility(LocalRteNode *localNode)
{
    return false;
}

/*
 * ConstructReshufflingQuery builds the INSERT that copies the chosen
 * table's rows into its reshuffled counterpart, tagging each row with the
 * id of the base table's tile it was paired with (from the tile-pair CTE
 * built by DistanceReshufflingPlan/OtherReshufflingPlan).
 */
static void
ConstructReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan)
{
    STMultirelation *reshuffledTable = (STMultirelation *)distPlan->reshuffledTable->rte;
    /* Add the reshuffling query */
    StringInfo reshufflingQuery = makeStringInfo();
    /* Copy structure of the second table into the reshuffled table */
    char *reshuffledTableColumns = getReshuffledColumns(distPlan, reshuffledTable->catalogTableInfo.table_oid);
    /* Generate the final CTE for the reshuffling data*/
    appendStringInfo(reshufflingQuery, "WITH TEMP AS (%s) "
                                       "INSERT INTO %s.%s "
                                       "SELECT %s, C.id1 "
                                       "FROM %s T, TEMP C "
                                       "WHERE T.tile_key = C.id2 AND T.%s && C.reshufflingBbox;",
                     distPlan->catalog_query_string, Var_Schema,
                     reshuffledTable->catalogTableInfo.reshuffledTable,
                     reshuffledTableColumns,
                     get_rel_name(reshuffledTable->catalogTableInfo.table_oid),
                     reshuffledTable->catalogTableInfo.distCol
    );
    distPlan->reshuffling_query = palloc((strlen(reshufflingQuery->data) + 1) * sizeof (char));
    strcpy(distPlan->reshuffling_query, reshufflingQuery->data);
}

/* getReshuffledColumns returns oid's column list (excluding tile_key) as a comma-separated, quoted string. */
static char *
getReshuffledColumns(DistributedSpatiotemporalQueryPlan *distPlan, Oid oid)
{
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "SELECT array_to_string(array_agg(concat('\"',column_name,'\"'))::text[], "
                                   "',') FROM information_schema.columns WHERE table_schema = 'public' AND "
                                   "table_name  = '%s' and column_name not in ('tile_key') ",
                     get_rel_name(oid));

    int spi_result;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }
    /* Execute the query, noting the readonly status of this SQL */
    spi_result = SPI_execute(catalogQuery->data, false, 1);
    /* Read back the PROJ text */
    if (spi_result == SPI_OK_SELECT)
    {
        char * reshuffledTableColumns = DatumGetCString(SPI_getvalue(SPI_tuptable->vals[0],
                                                                     SPI_tuptable->tupdesc,
                                                                     1));
        spi_result = SPI_finish();
        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
        return reshuffledTableColumns;
    }
    elog(ERROR, "Could not read column list for relation %u", oid);
}

/*
 * ColocationStrategyPlan records a Colocation PlanTask for the query's two
 * self-joined spatiotemporal (STRte) range-table entries, joined on their
 * shared tile key — no data movement needed since both are already tiled
 * the same way.
 */
extern void
ColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    PlanTask * strategy = (PlanTask *) palloc0(sizeof(PlanTask));
    strategy->type = Colocation;
    /* TODO: Add the other cases */
    /* This is a self-join, so the two tables to join are the two STRte
     * entries specifically -- not just whichever entries happen to be
     * first/second in tablesList->tables. Any reference or plain Citus
     * tables also joined in the same query (CitusRte/LocalRte entries)
     * can end up interleaved with them (e.g. "Trips t1, Licences1 l1,
     * Trips t2" puts l1 at index 1), and blindly casting one of those to
     * STMultirelation* read garbage through the wrong struct layout. */
    ListCell *rangeTableCell = NULL;
    STMultirelation *stTables[2] = {NULL, NULL};
    int stTableCount = 0;
    foreach(rangeTableCell, distPlan->tablesList->tables)
    {
        Rte *rteNode = (Rte *) lfirst(rangeTableCell);
        if (rteNode->RteType == STRte && stTableCount < 2)
        {
            stTables[stTableCount] = (STMultirelation *) rteNode->rte;
            stTableCount++;
        }
    }
    if (stTableCount < 2)
        ereport(ERROR, (errmsg("Colocation strategy requires two spatiotemporal tables to self-join")));
    strategy->tbl1 = stTables[0];
    strategy->tbl2 = stTables[1];
    strategy->tileKey = (Datum) Var_Catalog_Tile_Key;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}

/* TileScanRebalanceStrategyPlan is not yet implemented; disabled alongside CheckTileRebalancerActivation(). */
extern void
TileScanRebalanceStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* To be added later */
}

/*
 * GetReshufflingRte returns the single spatiotemporal table among rtes as a
 * ReshufflingRte base candidate, or NULL if there is more than one (in
 * which case chooseReshuffledTable() must be used instead).
 */
extern ReshufflingRte *GetReshufflingRte(STMultirelations *rtes)
{
    ListCell *rangeTableCell = NULL;
    List *stMultirelations = NULL;
    foreach(rangeTableCell, rtes->tables)
    {
        Rte * rteNode = (Rte *) lfirst(rangeTableCell);
        if (rteNode->RteType == STRte)
        {
            STMultirelation * spatiotemporalTable = (STMultirelation *) rteNode->rte;
            stMultirelations = lappend(stMultirelations, spatiotemporalTable);
        }
    }
    if (list_length(stMultirelations) == 1)
    {
        ReshufflingRte *rte = (ReshufflingRte *) palloc0(sizeof(ReshufflingRte));
        rte->stMultirelation = (STMultirelation *) list_nth(stMultirelations, 0);
        return rte;
    }
    else
    {
        /* To be added: if we have more than one STMultirelation, call the choose reshuffling function  */
        return NULL;
    }
}

/*
 * PredicatePushDownStrategyPlan records a PredicatePushDown PlanTask: the
 * query's predicate can be evaluated entirely on the worker with no
 * coordinator-side merge, since it applies to a single table's own tiles.
 */
extern void
PredicatePushDownStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    PlanTask * strategy = (PlanTask *) palloc0(sizeof(PlanTask));
    strategy->type = PredicatePushDown;
    /* tablesList->tables holds Rte wrappers (STRte/CitusRte/LocalRte), not
     * bare STMultirelation pointers -- list_nth(...)[0]/[1] cast directly
     * to STMultirelation* (as this used to) read a Citus/local reference
     * table's Rte wrapper as if it were the spatiotemporal table's own
     * struct whenever one of the query's other tables sorted before it, a
     * type confusion that left task->catalog_filtered pointing at garbage
     * and crashed EXPLAIN's "Task Count: %d" (task->catalog_filtered->
     * candidates). Find the actual STRte entry instead; PredicatePushDown
     * only ever needs the one spatiotemporal table its predicate pushes
     * down onto (see ConstructPredicatePushDownQuery, which only reads
     * tbl1), so tbl2 is left unset.
     */
    ListCell *rangeTableCell = NULL;
    STMultirelation *stTable = NULL;
    foreach(rangeTableCell, distPlan->tablesList->tables)
    {
        Rte *rteNode = (Rte *) lfirst(rangeTableCell);
        if (rteNode->RteType == STRte)
        {
            stTable = (STMultirelation *) rteNode->rte;
            break;
        }
    }
    if (stTable == NULL)
        ereport(ERROR, (errmsg("PredicatePushDown strategy requires a spatiotemporal table")));
    strategy->tbl1 = stTable;
    strategy->tileKey = (Datum) Var_Catalog_Tile_Key;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}

/* AddStrategy appends `type` to distPlan's list of chosen StrategyTypes, if
 * not already present. checkQueryType calls this once per registered
 * predicate clause, and a query can have more than one predicate that maps
 * to the same strategy between the same table pair (e.g. Q16's two
 * ST_Intersects clauses plus an aDisjoint clause all resolve to Colocation)
 * -- appending unconditionally queued the same strategy's plan/task/query
 * multiple times, and ConstructGeneralQuery's UNION of one query per
 * strategies-list entry then UNIONed several copies of a query that already
 * has its own trailing ORDER BY, which Postgres rejects outright. */
extern
void AddStrategy(DistributedSpatiotemporalQueryPlan *distPlan, StrategyType type)
{
    ListCell *cell;
    foreach(cell, distPlan->strategies)
    {
        if ((StrategyType) lfirst_int(cell) == type)
            return;
    }
    distPlan->strategies = lappend(distPlan->strategies, (Datum *)type);
}