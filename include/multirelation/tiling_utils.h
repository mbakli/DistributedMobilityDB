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

#ifndef MD_TILING_H
#define MD_TILING_H
#include "postgres.h"
#include "general/general_types.h"

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
