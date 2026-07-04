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

/* Full pg_dist_spatiotemporal_tables row for relationId, as a Datum array. */
extern Datum * GetDistributedTableMetadata(Oid relationId);

/* True if relationId's tiles have been rebalanced/reshuffled since creation. */
extern bool IsReshuffledTable(Oid relationId);

/* PostgreSQL type oid of the column driving relationId's distribution. */
extern int DistributedColumnType(Oid relationId);

/* Name of the spatiotemporal (MobilityDB) column used to distribute relationId. */
extern  char * GetSpatiotemporalCol(Oid relationId);

/* Serialized global (cross-tile) index metadata for relid. */
extern char * GetGlobalIndexInfo(Oid relid);

/* Resolves a relation oid from its (possibly schema-qualified) name. */
extern Oid RelationId(const char *relationName);

/* True if relationId is registered in pg_dist_spatiotemporal_tables. */
extern bool IsDistributedSpatiotemporalTable(Oid relationId);

/* Name of the geometry/shape column used to distribute relationId. */
char *GetShapeCol(Oid relationId);

#endif /* TABLE_OPS_H */
