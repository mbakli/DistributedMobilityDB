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

#ifndef RTE_H
#define RTE_H

#include "postgres.h"
#include "general_types.h"
#include "multirelation/multirelation_utils.h"
#include <nodes/primnodes.h>
#include <nodes/parsenodes.h>

/*
 * Rte
 *
 * A wrapper around a query range-table entry that tags it with which kind
 * of underlying node (`rte`) it holds, as identified by RteType.
 */
typedef struct Rte
{
    Node *rte;
    bool RteType;
    Alias *alias;
} Rte;

/*
 * RteType
 *
 *   STRte    - a spatiotemporal/distributed table (see STMultirelation)
 *   CitusRte - a plain Citus-distributed table
 *   LocalRte - a regular, non-distributed local table
 */
typedef enum RteType
{
    STRte,
    CitusRte,
    LocalRte
} RteType;

/* Metadata for a range-table entry backed by a Citus-distributed table. */
typedef struct CitusRteNode
{
    ListCell *rangeTableCell;
    char partitionMethod;
    ShapeType shapeType;
    int srid;
    char *col;
    char *localIndex;
    char *reshuffledTable;
} CitusRteNode;

/* Metadata for a range-table entry backed by a plain local (non-distributed) table. */
typedef struct LocalRteNode
{
    ListCell *rangeTableCell;
    bool refCandidate; /* Candidate to be broadcasted */
    int srid;
    char *col;
    char *localIndex;
    char *reshuffledTable;
} LocalRteNode;

/* Wraps an STMultirelation that needs its tiles reshuffled/rebalanced before use. */
typedef struct ReshufflingRte
{
    STMultirelation *stMultirelation;
} ReshufflingRte;

/* Builds an Rte wrapper around `node`, tagged with rteType and its query alias. */
extern Rte *GetRteNode(Node * node, RteType rteType, Alias *alias);

/* Extracts Citus distribution metadata for rangeTableEntry (given its partitionMethod). */
extern CitusRteNode *GetCitusRteInfo(RangeTblEntry *rangeTableEntry, char partitionMethod);

/* Extracts local-table metadata for rangeTableEntry. */
extern LocalRteNode * GetLocalRteInfo(RangeTblEntry *rangeTableEntry);

#endif /* RTE_H */
