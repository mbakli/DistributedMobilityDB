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

#ifndef TABLE_OPS_H
#define TABLE_OPS_H

#include "postgres.h"



/* constants for columnar.options */
#define Anum_pg_spatiotemporal_join_operations_oid 3
#define Anum_pg_spatiotemporal_join_operations_distance 4

extern Datum * GetDistributedTableMetadata(Oid relationId);
extern bool IsReshuffledTable(Oid relationId);
extern int DistributedColumnType(Oid relationId);
extern  char * GetSpatiotemporalCol(Oid relationId);
extern char * GetGlobalIndexInfo(Oid relid);
extern Oid RelationId(const char *relationName);
extern bool IsDistributedSpatiotemporalTable(Oid relationId);
char *GetShapeCol(Oid relationId);

#endif /* TABLE_OPS_H */
