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
#include <distributed/multi_logical_planner.h>
#include "utils/planner_utils.h"
#include "utils/helper_functions.h"
#include "catalog/pg_dist_spatiotemporal_dist_functions.h"

/*
 * GetTilingSchemeInfo loads relationId's pg_dist_spatiotemporal_tables row
 * into an in-memory STMultirelationCatalog: its tiling method/type/
 * granularity, tile count, distribution column, and related metadata.
 */
extern STMultirelationCatalog
GetTilingSchemeInfo(Oid relationId)
{
    /* groupCol/internalType/reshuffledTable aren't read from
     * pg_dist_spatiotemporal_tables here (they're filled in by other code
     * paths later) -- zero-initializing means they default to a safe NULL
     * instead of whatever garbage was already on the stack, which
     * previously clobbered the zeroed STMultirelation this gets copied
     * into (see GetMultirelationInfo, which palloc0's its multirelation
     * before this overwrites catalogTableInfo wholesale). */
    STMultirelationCatalog catalog = {0};
    Datum datumArray[Natts_MTS];
    bool isNullArray[Natts_MTS];
    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation pgMTS = table_open(MTSRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_MTS_oid + 1,
                BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(relationId));

    SysScanDesc scanDescriptor = systable_beginscan(pgMTS,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);

    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    heap_deform_tuple(heapTuple, RelationGetDescr(pgMTS), datumArray,
                      isNullArray);
    catalog.table_oid = DatumGetInt32(datumArray[Anum_MTS_oid]);
    catalog.numTiles = DatumGetInt32(datumArray[Anum_MTS_numTiles]);
    catalog.tiling_method = DatumToString(PointerGetDatum(datumArray[Anum_MTS_method]), TEXTOID);
    catalog.tiling_type = DatumToString(PointerGetDatum(datumArray[Anum_MTS_type]), TEXTOID);
    /* TODO: Store it as enum*/
    catalog.granularity = (strcasecmp(DatumToString(PointerGetDatum(datumArray[Anum_MTS_granularity]), TEXTOID), "shape-based") == 0) ?
            SHAPE_BASED : POINT_BASED;
    catalog.disjointTiles = DatumGetBool(datumArray[Anum_MTS_disjointTiles]);
    catalog.isMobilityDB = DatumGetBool(datumArray[Anum_MTS_isMobilityDB]);
    catalog.distCol = DatumToString(PointerGetDatum(datumArray[Anum_MTS_distCol]), TEXTOID);
    catalog.distColType = DatumToString(PointerGetDatum(datumArray[Anum_MTS_distColType]), TEXTOID);
    /* tilekey is a varchar column: DatumGetCString would misread its varlena
     * length header as leading string bytes (this previously corrupted
     * every generated tile-key join predicate with stray control bytes). */
    catalog.tileKey = DatumToString(PointerGetDatum(datumArray[Anum_MTS_tileKey]), TEXTOID);
    catalog.segmentation = DatumGetBool(datumArray[Anum_MTS_segmentation]);
    catalog.srid = DatumGetInt32(datumArray[Anum_MTS_srid]);
    /* groupcol is NULL for rows written before this column existed (or for
     * a source table with no usable primary key -- see getGroupCol) --
     * leave catalog.groupCol as NULL rather than misreading the varlena
     * header as string data (the same hazard tileKey's comment above
     * warns about). */
    catalog.groupCol = isNullArray[Anum_MTS_groupCol] ? NULL :
        DatumToString(PointerGetDatum(datumArray[Anum_MTS_groupCol]), TEXTOID);
    systable_endscan(scanDescriptor);
    table_close(pgMTS, NoLock);
    return catalog;
}

/* return oid of the MTS */
extern Oid
MTSRelationId()
{
    return RelationId(Var_Dist_Tables);
}

/* return oid of the worker nodes */
extern Oid
DistNodeId()
{
    return RelationId("pg_dist_node");
}

/* return oid of the MTS tiles */
extern Oid
MTSTilesRelationId()
{
    return RelationId(Var_Table_Tiles);
}


/* return oid of the distributed functions */
extern Oid
DisFuncRelationId()
{
    return RelationId(Tbl_Dist_Functions);
}

/*
 * AddCatalogFilterInfo records into catalogFilter which of tbl's tiles a
 * predicate node of predType could match. A NULL node (no analysable
 * predicate) conservatively assumes every tile is a candidate. Narrowing
 * the candidate count below numTiles (e.g. via a real bounding-box lookup)
 * is left as future work — see the Tile Scan Rebalancer TODO below.
 */
extern void
AddCatalogFilterInfo(STMultirelationCatalog tbl, CatalogFilter *catalogFilter, Node *node,
                     PredicateType predType, bool IsConst)
{
    if (node == NULL)
    {
        catalogFilter->candidates = tbl.numTiles;
        catalogFilter->tileExpand = false;
    }
    else
    {
        /* To be added after testing the other strategies: Tile Scan Rebalancer Job */
        if (predType == DISTANCE)
            catalogFilter->tileExpand = true;
        else
            catalogFilter->tileExpand = false;
        catalogFilter->candidates = tbl.numTiles;
        catalogFilter->predicate = (Datum) predType;
    }
}