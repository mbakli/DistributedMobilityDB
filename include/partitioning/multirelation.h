#ifndef MULTIRELATION_H
#define MULTIRELATION_H

#include "partitioning/tiling_utils.h"
#include "distributed/listutils.h"

typedef struct SpatiotemporalTableCatalog
{
    Oid table_oid;
    char *tiling_type;
    char *tiling_method;
    int numTiles;
    char *groupCol;
    bool isMobilityDB;
    bool disjointTiles;
    char *internalType;
    char *distColType;
    char *tileKey;
    TilingGranularity granularity;
    bool segmentation;
    int srid;
    char * distCol;
    char *reshuffledTable;
} SpatiotemporalTableCatalog;

/* Spatiotemporal Table Information */
typedef struct SpatiotemporalTable
{
    ListCell *rangeTableCell;
    SpatiotemporalTableCatalog catalogTableInfo;
    bool spatiotemporal_distributed;
    ShapeType shapeType;
    char *col;
    int srid;
    char *localIndex;
} SpatiotemporalTable;

typedef struct SpatiotemporalTables
{
    List *tables;
    int numDiffTables;
    int numSimTables;
    int length;
} SpatiotemporalTables;

extern bool IsDistributedSpatiotemporalTable(Oid relationId);
extern char *GetLocalIndex(Oid relationId, char * col);
#endif /* MULTIRELATION_H */
