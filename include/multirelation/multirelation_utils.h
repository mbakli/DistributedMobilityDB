#ifndef MULTIRELATION_H
#define MULTIRELATION_H

#include "multirelation/tiling_utils.h"
#include "distributed/listutils.h"

typedef struct STMultirelationCatalog
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
} STMultirelationCatalog;

/* Catalog Filter */
typedef struct CatalogFilter
{
    Datum predicate;
    int candidates;
    bool tileExpand;
    float expandValue;
} CatalogFilter;

/* Spatiotemporal Table Information */
typedef struct STMultirelation
{
    ListCell *rangeTableCell;
    STMultirelationCatalog catalogTableInfo;
    bool spatiotemporal_distributed;
    ShapeType shapeType;
    char *col;
    int srid;
    char *localIndex;
    Alias *alias;
    CatalogFilter *catalogFilter;
} STMultirelation;

typedef struct STMultirelations
{
    List *tables;
    int diffCount;
    int simCount;
    int stCount;
    int nonStCount;
    int length;
} STMultirelations;



extern bool IsDistributedSpatiotemporalTable(Oid relationId);
extern STMultirelation *GetMultirelationInfo(RangeTblEntry *rangeTableEntry, ShapeType type);
extern char *GetLocalIndex(Oid relationId, char * col);
extern char GetReshufflingType(RangeTblEntry *rangeTableEntry);
#endif /* MULTIRELATION_H */
