#include "general/catalog_management.h"
#include "distributed/metadata_cache.h"

static HeapTuple PgSpatiotemporalJoinOperationTupleViaCatalog(Oid operationId, bool distance);
static Datum *PgDistSpatiotemporalTableTupleViaCatalog(Oid relationId);

/* constants for columnar.options */
#define Anum_pg_spatiotemporal_join_operations_oid 3
#define Anum_pg_spatiotemporal_join_operations_distance 4
#define Anum_pg_dist_spatiotemporal_tables_oid 2
#define Anum_pg_dist_spatiotemporal_table_name 3
#define Natts_pg_dist_spatiotemporal_tables 9


/*
 * PgSpatiotemporalJoinOperationTupleViaCatalog is a helper function that searches
 * pg_spatiotemporal_join_operations for the given operationId. The caller is responsible
 * for ensuring that the returned heap tuple is valid before accessing
 * its fields.
 */

static HeapTuple
PgSpatiotemporalJoinOperationTupleViaCatalog(Oid operationId, bool distance)
{
    const int scanKeyCount = 2;
    ScanKeyData scanKey[2];
    bool indexOK = false;
    Relation pgSpatiotemporalOperation = table_open(RelationId("pg_spatiotemporal_join_operations"), AccessShareLock);

    ScanKeyInit(&scanKey[0], Anum_pg_spatiotemporal_join_operations_oid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(operationId));

    ScanKeyInit(&scanKey[1], Anum_pg_spatiotemporal_join_operations_distance,
                BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(distance));

    SysScanDesc scanDescriptor = systable_beginscan(pgSpatiotemporalOperation,
                                                    ExtensionNameIndexId,
                                                    indexOK, NULL, scanKeyCount, scanKey);

    HeapTuple partitionTuple = systable_getnext(scanDescriptor);

    if (HeapTupleIsValid(partitionTuple))
    {
        /* callers should have the tuple in their memory contexts */
        partitionTuple = heap_copytuple(partitionTuple);
    }

    systable_endscan(scanDescriptor);
    table_close(pgSpatiotemporalOperation, AccessShareLock);

    return partitionTuple;
}

/*
 * IsDistanceOperation returns whether the given operation is a
 * distance operation or not.
 *
 * It does so by searching pg_spatiotemporal_join_operations.
 */
bool
IsDistanceOperation(Oid operationId)
{
    HeapTuple heapTuple = PgSpatiotemporalJoinOperationTupleViaCatalog(operationId, true);

    bool heapTupleIsValid = HeapTupleIsValid(heapTuple);

    if (heapTupleIsValid)
    {
        heap_freetuple(heapTuple);
    }
    return heapTupleIsValid;
}

/*
 * IsIntersectionOperation returns whether the given operation is an
 * intersection operation or not.
 *
 * It does so by searching pg_spatiotemporal_join_operations.
 */
bool
IsIntersectionOperation(Oid operationId)
{
    HeapTuple heapTuple = PgSpatiotemporalJoinOperationTupleViaCatalog(operationId, false);

    bool heapTupleIsValid = HeapTupleIsValid(heapTuple);

    if (heapTupleIsValid)
    {
        heap_freetuple(heapTuple);
    }
    return heapTupleIsValid;
}

/*
 * IsReshuffledTable returns whether relationId is a reshuffled relation or not.
 */

bool
IsReshuffledTable(Oid relationId)
{
    return strstr(get_rel_name(relationId), "reshuffled") != NULL;
}

/*
 * DistributedColumnType returns the distributed column type
 * Needs some improvement
 */
int
DistributedColumnType(Oid relationId)
{
    int spi_result;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    /* Execute the query, noting the readonly status of this SQL */
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "select distcoltype from pg_dist_spatiotemporal_tables WHERE tableName= '%s' ",
                         get_rel_name(relationId));
    spi_result = SPI_execute(catalogQuery->data, false, 1);
    /* Read back the PROJ text */
    if (spi_result == SPI_OK_SELECT)
    {
        const char * columnType = DatumGetCString(SPI_getvalue(SPI_tuptable->vals[0],
                                                               SPI_tuptable->tupdesc,
                                                               1));
        spi_result = SPI_finish();

        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
        if (strcmp(columnType, "geometry") == 0)
            return SPATIAL;
        else
            return SPATIOTEMPORAL;
    }
    return DIFFTYPE;
}

/* return oid of any catalog relation */
extern Oid
RelationId(const char *relationName)
{
    return get_relname_relid(relationName, PG_CATALOG_NAMESPACE);
}

char *
GetSpatiotemporalCol(Oid relationId)
{
    int spi_result;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    /* Execute the query, noting the readonly status of this SQL */
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "select distcol from pg_dist_spatiotemporal_tables WHERE tableName= '%s' ",
                     get_rel_name(relationId));

    spi_result = SPI_execute(catalogQuery->data, true, 1);
    /* Read back the PROJ text */
    if (spi_result == SPI_OK_SELECT)
    {
        char * distcol = DatumGetCString(SPI_getvalue(SPI_tuptable->vals[0],
                                                      SPI_tuptable->tupdesc,
                                                      1));
        spi_result = SPI_finish();

        if (spi_result != SPI_OK_FINISH)
        {
            elog(ERROR, "Could not disconnect from database using SPI");
        }
        return distcol;
    }
    return NULL;
}

/*
 * IsDistributedTable returns whether relationId is a distributed relation or not.
 */

extern bool
IsDistributedSpatiotemporalTable(Oid relationId)
{
    // TODO: I have to see first whether the table is spatiotemporal distributed or not.
    return LookupCitusTableCacheEntry(relationId) != NULL;
}