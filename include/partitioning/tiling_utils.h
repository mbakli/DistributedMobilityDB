#ifndef MD_TILING_H
#define MD_TILING_H
#include "postgres.h"

/* In-memory representation of a typed tuple in pg_dist_spatiotemporal_tiles. */
typedef struct TileSize
{
    int tileIndex;
    uint64 tileKey;
    int numShapes;      /* Number of Shapes */
    int numPoints;      /* Number of contained Points */
    Datum minValue;     /* min value for the tile */
    Datum maxValue;     /* max value for the tile */
    Datum spatial_bbox;
    Datum spatiotemporal_bbox;
    char *nodeName;
    char *relation;
} TileSize;

/* Tiling Granularity */
typedef enum TilingGranularity
{
    POINT_BASED,
    SHAPE_BASED
} TilingGranularity;

/* Shape Type */
typedef enum ShapeType
{
    SPATIAL,
    SPATIOTEMPORAL,
    DIFFTYPE
} ShapeType;

/* In-memory representation of a typed tuple in pg_dist_spatiotemporal_tables. */
typedef struct MTS
{
    int id;
    Oid table_oid;
    TilingGranularity tilingGranularity;
    ShapeType tilingType;
    int tiles_count;
    bool disjoint_tiles;
    char *distcol;
    char *tilekey;
    bool shapeSegmented;
} MTS;


#endif /* MD_TILING_H */
