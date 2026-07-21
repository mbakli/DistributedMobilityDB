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

#ifndef PLANNER_UTILS_H
#define PLANNER_UTILS_H
#include "postgres.h"
#include "multirelation/multirelation_utils.h"
#include "planner/predicate_management.h"
#include <access/skey.h>
#include <access/genam.h>
#include <utils/fmgroids.h>
#include <access/table.h>
#include <access/relation.h>
#include <utils/rel.h>





/* constants for tiles.options */
#define Natts_MTS 15
#define Anum_MTS_oid 1
#define Anum_MTS_numTiles 3
#define Anum_MTS_method 4
#define Anum_MTS_type 5
#define Anum_MTS_granularity 6
#define Anum_MTS_disjointTiles 7
#define Anum_MTS_isMobilityDB 8
#define Anum_MTS_distCol 9
#define Anum_MTS_distColType 10
#define Anum_MTS_tileKey 11
#define Anum_MTS_segmentation 12
#define Anum_MTS_srid 13
/* groupcol was appended as the table's 15th (0-indexed 14th) column --
 * it did not exist when this catalog table was first designed, so unlike
 * the constants above (which match physical column order), this one can't
 * be slotted in without renumbering every constant after it. Previously
 * defined as 5, colliding with Anum_MTS_type -- vestigial from a groupCol
 * column that was never actually added to the table, so this was always
 * dead/wrong (GetTilingSchemeInfo never read it). */
#define Anum_MTS_groupCol 14

/* MobilityDB and PostGIS variables */
#define Var_MobilityDB_BBOX "mobdb_bbox"
#define Var_MobilityDB_Expand "expandSpace"
#define Var_PostGIS_BBOX "postgis_bbox"
#define Var_PostGIS_Expand "st_expand"

/* Catalog */
#define Var_Catalog_Tile_Key "tile_key"
#define Var_Dist_Tables "pg_dist_spatiotemporal_tables"
#define Var_Table_Tiles "pg_dist_spatiotemporal_tiles"



/* Loads relationId's tiling scheme (method/type/granularity/etc.) from pg_dist_spatiotemporal_tables. */
extern STMultirelationCatalog GetTilingSchemeInfo(Oid relationId);

/* Oid of the pg_dist_spatiotemporal_tables catalog relation. */
extern Oid MTSRelationId();

/* Oid of the pg_dist_spatiotemporal_tiles catalog relation. */
extern Oid MTSTilesRelationId();

/* Oid of the pg_dist_spatiotemporal_dist_functions catalog relation. */
extern Oid DisFuncRelationId();

/* Oid of the distributed-node catalog relation. */
extern Oid DistNodeId();

/* Derives catalogFilter's candidate tiles/expansion for tbl from a predicate node of predType. */
extern void AddCatalogFilterInfo(STMultirelationCatalog tbl, CatalogFilter *catalogFilter, Node *node,
                                 PredicateType predType, bool IsConst);
#endif /* PLANNER_UTILS_H */
