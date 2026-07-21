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


#include "postgres.h"
#include "general/rte.h"
#include "catalog/table_ops.h"
#include "multirelation/multirelation_utils.h"

/* GetRteNode wraps `node` into a generic Rte, tagged with its RteType and query alias. */
extern Rte *
GetRteNode(Node * node, RteType rteType, Alias *alias)
{
    Rte *rteNode = (Rte *) palloc0(sizeof(Rte));
    rteNode->rte = node;
    rteNode->RteType = rteType;
    rteNode->alias = alias;
    return rteNode;
}

/* GetCitusRteInfo collects the shape column and local index for a plain Citus-distributed range-table entry. */
extern CitusRteNode *
GetCitusRteInfo(RangeTblEntry *rangeTableEntry, char partitionMethod)
{
    CitusRteNode *citusRteNode = (CitusRteNode *) palloc0(sizeof(CitusRteNode));
    citusRteNode->col = GetShapeCol(rangeTableEntry->relid);
    //citusRteNode->shapeType = GetShapeType(rangeTableEntry->relid);
    citusRteNode->localIndex = GetLocalIndex(rangeTableEntry->relid,
                                             citusRteNode->col);
    citusRteNode->partitionMethod = partitionMethod;
    return citusRteNode;
}

/* GetLocalRteInfo collects the shape column and local index for a plain (non-distributed) local range-table entry. */
extern LocalRteNode *
GetLocalRteInfo(RangeTblEntry *rangeTableEntry)
{
    LocalRteNode *localRteNode = (LocalRteNode *) palloc0(sizeof(LocalRteNode));
    localRteNode->col = GetShapeCol(rangeTableEntry->relid);
    localRteNode->localIndex = GetLocalIndex(rangeTableEntry->relid,
                                             localRteNode->col);
    return localRteNode;
}