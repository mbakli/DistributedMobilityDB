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

#ifndef TILE_TASKS_H
#define TILE_TASKS_H

#include "postgres.h"
#include <nodes/pg_list.h>
#include "executor_tasks.h"
#include "general/rte.h"

/* Builds the SQL text for the given tasks, specialized for taskType. */
extern char *taskQuery (List *tasks, ExecTaskType taskType);

/* Rebalances relid's data into numTiles tiles, materializing into reshuffledTable. */
extern void RearrangeTiles(Oid relid, int numTiles, char *reshuffledTable);

/* Appends the tile-key equi-join predicate between alias and otherAlias for a distributed (spatiotemporal) relation to query_string. */
extern Datum AddTilingKey(STMultirelationCatalog tblCatalog, Alias *alias, Alias *otherAlias, char * query_string);

/* Appends the tile-key projection for a non-spatiotemporal (plain) rte to query_string. */
extern Datum AddNonStRteTilingKey(Rte *tbl, Alias *alias ,char * query_string);

/* Picks a random tile number for rte, used to sample/estimate during planning. */
extern int GetRandTileNum(STMultirelation *rte);
#endif /* TILE_TASKS_H */
