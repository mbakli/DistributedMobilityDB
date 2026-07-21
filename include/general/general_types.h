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

#ifndef GENERAL_TYPES_H
#define GENERAL_TYPES_H

#include "postgres.h"

/*
 * ShapeType
 *
 * Classifies a distributed column/relation by the kind of tiling it needs:
 *   SPATIAL       - PostGIS geometry only (e.g. point, polygon)
 *   SPATIOTEMPORAL - MobilityDB temporal type (e.g. tgeompoint)
 *   DIFFTYPE       - relation is not distributed on a spatial/temporal column
 */
typedef enum ShapeType
{
    SPATIAL,
    SPATIOTEMPORAL,
    DIFFTYPE
} ShapeType;

/* Schema created by the extension to hold internal catalog tables. */
#define Var_Schema "dist_mobilitydb"
/* Index method used for spatial/spatiotemporal (bbox) tile indexes. */
#define Var_Spatiotemporal_Index "GIST"
/* Index method used for scalar (non-spatial) tile indexes. */
#define Var_BTREE_Index "BTREE"


#endif /* GENERAL_TYPES_H */
