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

#ifndef MULTIRELATION_H
#define MULTIRELATION_H

#include "multirelation/tiling_utils.h"
#include "distributed/listutils.h"

/*
 * STMultirelationCatalog
 *
 * In-memory copy of a table's pg_dist_spatiotemporal_tables catalog row:
 * how it was tiled (tiling_type/tiling_method/numTiles/granularity/
 * disjointTiles), which column drives the distribution (distCol and its
 * types), and the reshuffledTable to use if its tiles were rebalanced.
 */
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

/*
 * CatalogFilter
 *
 * Describes how a query predicate narrows down the set of tiles a task
 * needs to touch: `predicate` is the filter expression, `candidates` the
 * resulting tile count, and tileExpand/expandValue record whether the
 * search region had to be grown (e.g. for kNN/distance predicates) and by
 * how much.
 */
typedef struct CatalogFilter
{
    Datum predicate;
    int candidates;
    bool tileExpand;
    float expandValue;
} CatalogFilter;

/* Spatiotemporal Table Information: a distributed relation as seen by the planner/executor. */
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

/*
 * STMultirelations
 *
 * A query's full set of range-table entries (`tables`), tallied by kind
 * (stCount = spatiotemporal, nonStCount = plain, simCount/diffCount =
 * entries sharing/not sharing the same shape type) so the planner can
 * decide which join/execution strategy applies.
 */
typedef struct STMultirelations
{
    List *tables;
    int diffCount;
    int simCount;
    int stCount;
    int nonStCount;
    int length;
    /* Count of nonStCount entries that are Citus reference tables -- these
     * are already replicated to every node, so they never need reshuffling
     * and shouldn't count as a "different distributed table" when deciding
     * whether a join needs the NonColocation strategy. */
    int refCount;
} STMultirelations;

/* True if relationId is registered as a distributed spatiotemporal table. */
extern bool IsDistributedSpatiotemporalTable(Oid relationId);

/* Builds the STMultirelation describing rangeTableEntry, expected to hold shape `type`. */
extern STMultirelation *GetMultirelationInfo(RangeTblEntry *rangeTableEntry, ShapeType type);

/* Name of the local (per-tile) index defined on relationId's `col`. */
extern char *GetLocalIndex(Oid relationId, char * col);

/* Determines whether/how rangeTableEntry's tiles need reshuffling before use. */
extern char GetReshufflingType(RangeTblEntry *rangeTableEntry);
#endif /* MULTIRELATION_H */
