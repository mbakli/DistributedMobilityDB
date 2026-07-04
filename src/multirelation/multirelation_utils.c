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

#include "postgres.h"
#include "multirelation/multirelation_utils.h"
#include "catalog/table_ops.h"
#include "utils/planner_utils.h"

/*
 * GetMultirelationInfo builds the STMultirelation describing rangeTableEntry
 * as a distributed table of the given shape `type`: its spatiotemporal
 * column, query alias, local per-tile index, and tiling scheme catalog info.
 */
extern STMultirelation *
GetMultirelationInfo(RangeTblEntry *rangeTableEntry, ShapeType type)
{
    STMultirelation *multirelation = (STMultirelation *) palloc0(sizeof(STMultirelation));
    multirelation->shapeType = type;
    multirelation->col = GetSpatiotemporalCol(rangeTableEntry->relid);
    multirelation->alias = rangeTableEntry->alias;
    multirelation->localIndex = GetLocalIndex(rangeTableEntry->relid,
                                              multirelation->col);
    multirelation->catalogTableInfo = GetTilingSchemeInfo(rangeTableEntry->relid);
    return multirelation;
}
