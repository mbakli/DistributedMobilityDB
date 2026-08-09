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
 * Copyright (c) 2020-2026 Mohamed Bakli <mohamed_bakli@hotmail.com>
 *
 *****************************************************************************/

#include "catalog/table_ops.h"
#include "general/general_types.h"
#include "utils/helper_functions.h"
#include <distributed/metadata_cache.h>
#include <utils/fmgroids.h>
#include "catalog/pg_namespace.h"
#include "utils/planner_utils.h"
#include <distributed/metadata_utility.h>
#include <catalog/pg_extension.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <executor/spi.h>
#include <access/genam.h>

static Datum *PgDistSpatiotemporalTableTupleViaCatalog(Oid relationId);

/*
 * PgSpatiotemporalJoinOperationTupleViaCatalog is a helper function that searches
 * pg_spatiotemporal_join_operations for the given operationId. The caller is responsible
 * for ensuring that the returned heap tuple is valid before accessing
 * its fields.
 */

extern HeapTuple
PgSpatiotemporalJoinOperationTupleViaCatalog(Oid operationId, bool distance)
{
    const int scanKeyCount = 2;
    ScanKeyData scanKey[2];
    bool indexOK = false;
    Relation pgSpatiotemporalOperation = table_open(RelationId("pg_spatiotemporal_join_operations"),
                                                    AccessShareLock);

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
    table_close(pgSpatiotemporalOperation, NoLock);

    return partitionTuple;
}

/*
 * DistributedColumnType returns the distributed column type
 * Needs some improvement
 */
extern int
DistributedColumnType(Oid relationId)
{
    int spi_result;
    int result = DIFFTYPE;
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
    /*
     * Read back the PROJ text -- SPI_OK_SELECT only means the query ran as a SELECT, not that it
     * matched any rows (e.g. a stale/mismatched relationId<->tableName lookup). SPI_processed must
     * be checked before indexing SPI_tuptable->vals[0]; without it, a zero-row result reads past
     * the end of an empty array. Reproduced directly under gdb: SIGSEGV inside SPI_getvalue,
     * called from here with a zero-row SPI_tuptable, on every query against a table this lookup
     * failed to match.
     */
    if (spi_result == SPI_OK_SELECT && SPI_processed > 0)
    {
        const char *columnType = (char *) DatumGetCString(SPI_getvalue(SPI_tuptable->vals[0],
                                                                        SPI_tuptable->tupdesc,
                                                                        1));
        if (strcmp(columnType, "geometry") == 0)
            result = SPATIAL;
        else if (strcmp(columnType, "tgeompoint") == 0)
            result = SPATIOTEMPORAL;
    }
    /* Always paired with SPI_connect() above, regardless of which branch was taken -- the
     * previous version only called this inside the SPI_OK_SELECT branch, leaking the SPI
     * connection on any other result status. */
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
    return result;
}

/*
 * IsReshuffledTable returns whether relationId is a reshuffled relation or not.
 */

bool
IsReshuffledTable(Oid relationId)
{
    return strstr(get_rel_name(relationId), "reshuffled") != NULL;
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
    bool isNull = false;
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
    /*
     * A stale/mismatched relationId<->tableName lookup legitimately returns zero rows here --
     * SPI_tuptable->vals[0] must not be read in that case (SIGSEGV inside SPI_getbinval,
     * reproduced under gdb, reached from here via analyzeDistributedSpatiotemporalTables ->
     * GetMultirelationInfo on every query against a table this lookup failed to match).
     *
     * SPI_copytuple (unlike DatumToString/SPI_getvalue) allocates in the context that was
     * current before SPI_connect(), so row/rowDescriptor stay valid past SPI_finish() --
     * rendering the actual C string via DatumToString is deferred until after SPI_finish()
     * below (mirroring GetRandomTileId/GetDBName in nodes.c) so it isn't palloc'd inside the
     * SPI-owned context SPI_finish() is about to delete. Calling DatumToString before
     * SPI_finish() (as this used to) returned a pointer into memory freed the instant
     * SPI_finish() ran -- reproduced in practice: multirelation->col (this function's result,
     * stored by GetMultirelationInfo) read back as garbage bytes by the time it was used much
     * later at execution time in IndexReshuffledData, formatted straight into a CREATE INDEX
     * statement ("CREATE INDEX ..._idx on ... USING GIST(<garbage>)").
     */
    HeapTuple row = NULL;
    TupleDesc rowDescriptor = NULL;
    Datum distcol = (Datum) 0;
    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);
    if (found)
    {
        row = SPI_copytuple(SPI_tuptable->vals[0]);
        rowDescriptor = SPI_tuptable->tupdesc;
        distcol = SPI_getbinval(row, rowDescriptor, 1, &isNull);
        found = !isNull;
    }
    /* Always paired with SPI_connect() above -- the previous version returned NULL directly on a
     * non-SPI_OK_SELECT result without ever calling this, leaking the SPI connection. */
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
    if (!found)
        return NULL;
    return DatumToString(distcol, TEXTOID);
}

/*
 * IsDistributedTable returns whether relationId is a distributed relation or not.
 */

extern bool
IsDistributedSpatiotemporalTable(Oid relationId)
{
    // TODO: I have to see first whether the table is spatiotemporal distributed or not.
    if (LookupCitusTableCacheEntry(relationId) != NULL)
    {
        ScanKeyData scanKey[1];
        bool indexOK = false;
        Relation multirelation = table_open(MTSRelationId(), RowExclusiveLock);
        ScanKeyInit(&scanKey[0], Anum_MTS_oid + 1,
                    BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(relationId));

        SysScanDesc scanDescriptor = systable_beginscan(multirelation,
                                                        DistPlacementPlacementidIndexId(),
                                                        indexOK,
                                                        NULL, 1, scanKey);

        HeapTuple heapTuple = systable_getnext(scanDescriptor);

        bool heapTupleIsValid = HeapTupleIsValid(heapTuple);
        systable_endscan(scanDescriptor);
        table_close(multirelation, NoLock);
        return heapTupleIsValid;
    }
    return false;
}

/*
 * GetLocalIndex returns the name of the (non-unique) index defined on
 * relationId's `col`, used to accelerate per-tile scans on workers.
 */
extern char *
GetLocalIndex(Oid relationId, char * col)
{
    /* col is NULL for any table with no shape/geometry column (GetShapeCol's
     * legitimate "no such column" result) -- there can be no shape index to
     * look up in that case. Without this check, the NULL flowed straight
     * into the query text below via %s, which glibc renders as the literal
     * string "(null)" rather than a real column name, producing a query
     * that always fails ("attname = '(null)'" never matches). */
    if (col == NULL)
        return NULL;

    int spi_result;
    bool isNull = false;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    /* Execute the query, noting the readonly status of this SQL */
    StringInfo catalogQuery = makeStringInfo();
    /* Schema-qualified: get_rel_name() alone returns the bare table name,
     * so the '%s'::regclass cast below silently failed to resolve any
     * table living outside search_path (e.g. dist_mobilitydb.*) with
     * "relation ... does not exist" -- even though the table exists. */
    char *qualifiedName = quote_qualified_identifier(
        get_namespace_name(get_rel_namespace(relationId)), get_rel_name(relationId));
    appendStringInfo(catalogQuery, "select ic.relname as index_name\n"
                                   "        from pg_index ix\n"
                                   "join pg_class ic on ix.indexrelid = ic.oid\n"
                                   "join pg_attribute a on a.attrelid = ic.oid\n"
                                   "where ix.indrelid = '%s'::regclass\n"
                                   "        and attname = '%s'\n"
                                   "        and not ix.indisunique;",
                     qualifiedName, col);

    spi_result = SPI_execute(catalogQuery->data, true, 1);
    /* A table with no qualifying (non-unique) index legitimately returns zero
     * rows here -- reading SPI_tuptable->vals[0] in that case dereferences
     * past the (empty) result set.
     *
     * ic.relname is Postgres' `name` type: a fixed NAMEDATALEN-byte,
     * NUL-padded array with no varlena length header, stored by reference --
     * unlike a genuine text/varlena Datum, SPI_getbinval's raw pointer for
     * it already IS a plain C string, which is why the original
     * `result = (char *) localIndex;` raw cast happened to render correctly.
     * It's still rendered through DatumToString(..., NAMEOID) here (not
     * TEXTOID -- textout expects a varlena length header this data doesn't
     * have, and would misread it) purely so the extraction follows the same
     * deferred-until-after-SPI_finish() pattern as GetSpatiotemporalCol
     * above, rather than relying on the caller knowing `name`'s raw Datum
     * already happens to be directly usable.
     */
    HeapTuple row = NULL;
    TupleDesc rowDescriptor = NULL;
    Datum localIndex = (Datum) 0;
    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);
    if (found)
    {
        row = SPI_copytuple(SPI_tuptable->vals[0]);
        rowDescriptor = SPI_tuptable->tupdesc;
        localIndex = SPI_getbinval(row, rowDescriptor, 1, &isNull);
        found = !isNull;
    }
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
    if (!found)
        return NULL;
    return DatumToString(localIndex, NAMEOID);
}

/*
 * GetShapeCol returns the name of the geometry/shape column that
 * relationId is distributed on, via the getDistributedCol() SQL helper.
 */
char *
GetShapeCol(Oid relationId)
{
    int spi_result;
    bool isNull = false;
    /* Connect */
    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }

    /* Execute the query, noting the readonly status of this SQL */
    StringInfo catalogQuery = makeStringInfo();
    appendStringInfo(catalogQuery, "select getDistributedCol('%s');",
                     get_rel_name(relationId));

    spi_result = SPI_execute(catalogQuery->data, true, 1);
    /* Read back the PROJ text. getDistributedCol() legitimately returns
     * NULL for a plain (non-spatiotemporal) table, e.g. a reference table
     * joined alongside a distributed one -- calling DatumToString on that
     * NULL Datum crashed instead of just reporting "no shape column".
     *
     * DatumToString itself is deferred until after SPI_finish() below (see
     * the matching comment in GetSpatiotemporalCol above) -- calling it
     * while still connected palloc's its rendered string inside the
     * SPI-owned context that SPI_finish() deletes, leaving a dangling
     * pointer as this function's result. */
    HeapTuple row = NULL;
    TupleDesc rowDescriptor = NULL;
    Datum distcol = (Datum) 0;
    bool found = (spi_result == SPI_OK_SELECT && SPI_processed > 0);
    if (found)
    {
        row = SPI_copytuple(SPI_tuptable->vals[0]);
        rowDescriptor = SPI_tuptable->tupdesc;
        distcol = SPI_getbinval(row, rowDescriptor, 1, &isNull);
        found = !isNull;
    }
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
    if (!found)
        return NULL;
    return DatumToString(distcol, TEXTOID);
}