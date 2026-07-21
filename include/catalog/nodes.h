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

#ifndef NODESS_H
#define NODESS_H
#include "postgres.h"
#include "executor/executor_tasks.h"

/* Name of the database the current backend is connected to. */
extern Datum GetDBName();

/* Coordinator/worker node info (host, port, role) for the local backend. */
extern TaskNode *GetNodeInfo();

/* Node (host, port) actually hosting relationId's shard for rand_tile. */
extern TaskNode *GetShardHostNode(Oid relationId, int rand_tile);

/* Table id of the tile assigned to a randomly-picked worker for relationId. */
extern char* GetRandomTileId(Oid relationId, ExecTaskType taskType, int rand_tile);

/* Physical shard name for a Citus reference table's single (every-node-replicated) shard. */
extern char* GetReferenceTableShardName(Oid relationId);

/* Looks up the tiling method used to distribute relationId; -1 if not distributed. */
extern int TilingSearch(Oid relationId);

/* Number of tiles the given distributed relation was split into. */
extern int GetNumTiles(Oid relationId);
#endif /* NODESS_H */
