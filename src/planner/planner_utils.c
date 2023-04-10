#include "planner/planner_utils.h"

extern SpatiotemporalTableCatalog
GetTilingSchemeInfo(Oid relationId)
{
    SpatiotemporalTableCatalog catalog;
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
    catalog.granularity = (strcasecmp(DatumGetCString(datumArray[Anum_MTS_granularity]), "shape-based") == 0) ?
            SHAPE_BASED : POINT_BASED;
    catalog.disjointTiles = DatumGetBool(datumArray[Anum_MTS_disjointTiles]);
    catalog.isMobilityDB = DatumGetBool(datumArray[Anum_MTS_isMobilityDB]);
    catalog.distCol = DatumToString(PointerGetDatum(datumArray[Anum_MTS_distCol]), TEXTOID);
    catalog.distColType = DatumToString(PointerGetDatum(datumArray[Anum_MTS_distColType]), TEXTOID);
    catalog.tileKey = DatumGetCString(datumArray[Anum_MTS_tileKey]);
    catalog.segmentation = DatumGetBool(datumArray[Anum_MTS_segmentation]);
    catalog.srid = DatumGetInt32(datumArray[Anum_MTS_srid]);
    systable_endscan(scanDescriptor);
    table_close(pgMTS, NoLock);
    return catalog;
}

/* return oid of the MTS */
extern Oid
MTSRelationId()
{
    return RelationId("pg_dist_spatiotemporal_tables");
}

/* return oid of the MTS tiles */
extern Oid
MTSTilesRelationId()
{
    return RelationId("pg_dist_spatiotemporal_tiles");
}

/* return oid of the distributed functions */
extern Oid
DisFuncRelationId()
{
    return RelationId("pg_dist_spatiotemporal_dist_functions");
}
