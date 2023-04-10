#ifndef PLANNER_UTILS_H
#define PLANNER_UTILS_H
#include "postgres.h"
#include "partitioning/multirelation.h"

#include "general/catalog_management.h"
#include <access/skey.h>
#include <access/genam.h>
#include <utils/fmgroids.h>
#include <access/table.h>
#include <access/relation.h>
#include <utils/rel.h>

/* constants for tiles.options */
#define Natts_MTS 14
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
#define Anum_MTS_groupCol 5

/* MobilityDB and PostGIS variables */
#define Var_MobilityDB_BBOX "mobdb_bbox"
#define Var_MobilityDB_Expand "expandSpace"
#define Var_PostGIS_BBOX "postgis_bbox"
#define Var_PostGIS_Expand "st_expand"

/* Catalog */
#define Var_Catalog_Tile_Key "tile_key"
#define Var_Dist_Tables "pg_dist_spatiotemporal_tables"
#define Var_Table_Tiles "pg_dist_spatiotemporal_tiles"

#define Var_Schema "dist_mobilitydb"

extern SpatiotemporalTableCatalog GetTilingSchemeInfo(Oid relationId);
extern Oid MTSRelationId();
extern Oid MTSTilesRelationId();
extern Oid DisFuncRelationId();
#endif /* PLANNER_UTILS_H */
