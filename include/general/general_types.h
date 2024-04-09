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
 * Copyright (c) 2023-2024 Mohamed Bakli <mohamed_bakli@hotmail.com>
 *
 *****************************************************************************/

#ifndef GENERAL_TYPES_H
#define GENERAL_TYPES_H

#include "postgres.h"

/* Shape Type */
typedef enum ShapeType
{
    SPATIAL,
    SPATIOTEMPORAL,
    DIFFTYPE
} ShapeType;

#define Var_Schema "dist_mobilitydb"
#define Var_Spatiotemporal_Index "GIST"
#define Var_BTREE_Index "BTREE"


#endif /* GENERAL_TYPES_H */
