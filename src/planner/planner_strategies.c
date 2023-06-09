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

/* Non colocated Query Plan */
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

/* Reshuffling Query Plan */
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

/* Reshuffling Query Plan with spatiotemporal multirelations */
static void
PlanReshufflingNonStRteWithStRte(DistributedSpatiotemporalQueryPlan *distPlan, ReshufflingRte *rte_node)
{
    ListCell *rangeTableCell = NULL;
    /* Set base table for other relations */
    distPlan->reshuffled_table_base = rte_node->stMultirelation;
    foreach(rangeTableCell, distPlan->tablesList->tables)
    {
        Rte * rteNode = (Rte *) lfirst(rangeTableCell);
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

/* Reshuffling Query Plan with spatiotemporal multirelations */
static void
PlanReshufflingStRtes(DistributedSpatiotemporalQueryPlan *distPlan)
{
    chooseReshuffledTable(distPlan);
    createReshufflingTablePlan(distPlan);
    ConstructReshufflingQuery(distPlan);
}
/* Choose the base and reshuffling tables */
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

extern bool
CheckBroadcastingPossibility(LocalRteNode *localNode)
{
    return false;
}

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
}

extern void
ColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    PlanTask * strategy = (PlanTask *) palloc0(sizeof(PlanTask));
    strategy->type = Colocation;
    /* TODO: Add the other cases */
    strategy->tbl1 = (STMultirelation *) ((Rte *)list_nth(distPlan->tablesList->tables, 0))->rte;
    strategy->tbl2 = (STMultirelation *) ((Rte *)list_nth(distPlan->tablesList->tables, 1))->rte;
    strategy->tileKey = (Datum) Var_Catalog_Tile_Key;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}

extern void
TileScanRebalanceStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* To be added later */
}

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

extern void
PredicatePushDownStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    PlanTask * strategy = (PlanTask *) palloc0(sizeof(PlanTask));
    strategy->type = PredicatePushDown;
    strategy->tbl1 = (STMultirelation *) list_nth(distPlan->tablesList->tables, 0);
    strategy->tbl2 = (STMultirelation *) list_nth(distPlan->tablesList->tables, 1);
    strategy->tileKey = (Datum) Var_Catalog_Tile_Key;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}
extern
void AddStrategy(DistributedSpatiotemporalQueryPlan *distPlan, StrategyType type)
{
    distPlan->strategies = lappend(distPlan->strategies, (Datum *)type);
}