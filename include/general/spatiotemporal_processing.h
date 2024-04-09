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

#ifndef SPATIOTEMPORAL_PROCESSING_H
#define SPATIOTEMPORAL_PROCESSING_H
#include "postgres.h"

#define S_BOX_PTR(c)  ( (GBOX *) DatumGetPointer(c) )
#define DATUM_GET_SBOX(c)  ( S_BOX_PTR(c) )
#define BOX_GET_DATUM(c)  ( PointerGetDatum(c) )
#define ST_BOX_PTR(c)  ( (STBOX *) DatumGetPointer(c) )
#define DATUM_GET_STBOX(c)  ( ST_BOX_PTR(c) )
#endif /* SPATIOTEMPORAL_PROCESSING_H */
