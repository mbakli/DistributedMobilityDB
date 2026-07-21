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

#ifndef PG_DIST_SPATIOTEMPORAL_TILES_H
#define PG_DIST_SPATIOTEMPORAL_TILES_H

/* ----------------
 *  pg_dist_spatiotemporal_tiles: Compiler constants
 *
 *  Catalog table with one row per tile produced for a distributed table
 *  (table_id references pg_dist_spatiotemporal_tables). Stores the tile's
 *  key and its bounding boxes in both MobilityDB (mobdb_bbox) and PostGIS
 *  (postgis_bbox) representations, plus per-tile cardinality used by the
 *  planner for load balancing (num_shapes, num_points).
 * ----------------
 */

#define Natts_pg_dist_spatiotemporal_tiles 7
#define Anum_pg_dist_spatiotemporal_tiles_id 1
#define Anum_pg_dist_spatiotemporal_tiles_table_id 2
#define Anum_pg_dist_spatiotemporal_tiles_tile_key 3
#define Anum_pg_dist_spatiotemporal_tiles_mobdb_bbox 4
#define Anum_pg_dist_spatiotemporal_tiles_postgis_bbox 5
#define Anum_pg_dist_spatiotemporal_tiles_num_shapes 6
#define Anum_pg_dist_spatiotemporal_tiles_num_points 7


#endif /* PG_DIST_SPATIOTEMPORAL_TILES_H */
