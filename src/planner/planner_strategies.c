#include "postgres.h"
#include "planner/planner_strategies.h"
#include <utils/lsyscache.h>
#include <executor/spi.h>
#include <distributed/multi_executor.h>

static void planReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan);
static void chooseReshuffledTable(DistributedSpatiotemporalQueryPlan *distPlan);
static void createReshufflingTablePlan(DistributedSpatiotemporalQueryPlan *distPlan);
static void DistanceReshufflingPlan(DistributedSpatiotemporalQueryPlan *distPlan);
static void OtherReshufflingPlan(DistributedSpatiotemporalQueryPlan *distPlan);
static void ConstructReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan);
static char *getReshuffledColumns(DistributedSpatiotemporalQueryPlan *distPlan);

/* Non colocated Query Plan */
extern void
NonColocationStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* Create a reshuffling plan */
    planReshufflingQuery(distPlan);
    PlanTask * strategy = (PlanTask *) palloc0(sizeof(PlanTask));
    /* Strategy type */
    strategy->type = NonColocation;
    strategy->tbl1 = distPlan->reshuffled_table_base;
    strategy->tbl2 = distPlan->reshuffledTable;
    strategy->tileKey = (Datum) distPlan->reshuffledTable->catalogTableInfo.tileKey;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}

/* Reshuffling Query Plan */
static void
planReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan)
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
        SpatiotemporalTable * spatiotemporalTable = (SpatiotemporalTable *) lfirst(rangeTableCell);
        if ( spatiotemporalTable->catalogTableInfo.numTiles > base_tbl)
        {
            base_tbl = spatiotemporalTable->catalogTableInfo.numTiles;
            distPlan->reshuffled_table_base = (SpatiotemporalTable *) lfirst(rangeTableCell);
        }
        else
            distPlan->reshuffledTable = (SpatiotemporalTable *) lfirst(rangeTableCell);
    }
    StringInfo tableName = makeStringInfo();
    appendStringInfo(tableName, "%s%s",
                     get_rel_name(distPlan->reshuffledTable->catalogTableInfo.table_oid), Var_Temp_Reshuffled);
    distPlan->reshuffledTable->catalogTableInfo.reshuffledTable = tableName->data;
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
DistanceReshufflingPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    StringInfo catalogQuery = makeStringInfo();
    char bbox_col[30];
    char expand_op[30];
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
                     get_rel_name(distPlan->reshuffledTable->catalogTableInfo.table_oid),
                     (distPlan->reshuffledTable->catalogTableInfo.table_oid ==
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
                     distPlan->reshuffledTable->catalogTableInfo.table_oid,
                     (distPlan->reshuffledTable->catalogTableInfo.table_oid ==
                      distPlan->reshuffled_table_base->catalogTableInfo.table_oid) ?
                     " AND S1.id < S2.id ":"",
                     bbox_col, bbox_col);
    // Keep the catalog query in the distributed plan
    distPlan->catalog_query_string = palloc((strlen(catalogQuery->data) + 1) * sizeof (char));
    strcpy(distPlan->catalog_query_string, catalogQuery->data);
}

static void
ConstructReshufflingQuery(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* Add the reshuffling query */
    StringInfo reshufflingQuery = makeStringInfo();
    /* Copy structure of the second table into the reshuffled table */
    char *reshuffledTableColumns = getReshuffledColumns(distPlan);
    /* Generate the final CTE for the reshuffling data*/
    appendStringInfo(reshufflingQuery, "WITH TEMP AS (%s) "
                                       "INSERT INTO %s.%s "
                                       "SELECT %s, C.id1 "
                                       "FROM %s T, TEMP C "
                                       "WHERE T.tile_key = C.id2 AND T.%s && C.reshufflingBbox;",
                     distPlan->catalog_query_string, Var_Schema,
                     distPlan->reshuffledTable->catalogTableInfo.reshuffledTable,
                     reshuffledTableColumns,
                     get_rel_name(distPlan->reshuffledTable->catalogTableInfo.table_oid),
                     distPlan->reshuffledTable->catalogTableInfo.distCol
    );
    distPlan->reshuffling_query = palloc((strlen(reshufflingQuery->data) + 1) * sizeof (char));
    strcpy(distPlan->reshuffling_query, reshufflingQuery->data);
}

static char *
getReshuffledColumns(DistributedSpatiotemporalQueryPlan *distPlan)
{
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "SELECT array_to_string(array_agg(concat('\"',column_name,'\"'))::text[], "
                                   "',') FROM information_schema.columns WHERE table_schema = 'public' AND "
                                   "table_name  = '%s' and column_name not in ('tile_key') ",
                     get_rel_name(distPlan->reshuffledTable->catalogTableInfo.table_oid));
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
    strategy->tbl1 = (SpatiotemporalTable *) list_nth(distPlan->tablesList->tables, 0);
    strategy->tbl2 = (SpatiotemporalTable *) list_nth(distPlan->tablesList->tables, 1);
    strategy->tileKey = (Datum) Var_Catalog_Tile_Key;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}

extern void
TileScanRebalanceStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    /* To be added later */
}

extern void
PredicatePushDownStrategyPlan(DistributedSpatiotemporalQueryPlan *distPlan)
{
    PlanTask * strategy = (PlanTask *) palloc0(sizeof(PlanTask));
    strategy->type = PredicatePushDown;
    strategy->tbl1 = (SpatiotemporalTable *) list_nth(distPlan->tablesList->tables, 0);
    strategy->tbl2 = (SpatiotemporalTable *) list_nth(distPlan->tablesList->tables, 1);
    strategy->tileKey = (Datum) Var_Catalog_Tile_Key;
    distPlan->strategyPlans = lappend(distPlan->strategyPlans, strategy);
}
extern
void AddStrategy(DistributedSpatiotemporalQueryPlan *distPlan, StrategyType type)
{
    distPlan->strategies = lappend(distPlan->strategies, (Datum *)type);
}