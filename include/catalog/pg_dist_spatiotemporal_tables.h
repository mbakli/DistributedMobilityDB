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

#ifndef PG_DIST_SPATIOTEMPORAL_TABLES_H
#define PG_DIST_SPATIOTEMPORAL_TABLES_H

/* ----------------
 *  pg_dist_spatiotemporal_tables: Compiler constants
 * ----------------
 */

#define Natts_pg_dist_spatiotemporal_tables 14
#define Anum_pg_dist_spatiotemporal_tables_id 1
#define Anum_pg_dist_spatiotemporal_tables_tbloid 2
#define Anum_pg_dist_spatiotemporal_tables_tablename 3
#define Anum_pg_dist_spatiotemporal_tables_numtiles 4
#define Anum_pg_dist_spatiotemporal_tables_tilingmethod 5
#define Anum_pg_dist_spatiotemporal_tables_tilingtype 6
#define Anum_pg_dist_spatiotemporal_tables_granularity 7
#define Anum_pg_dist_spatiotemporal_tables_disjoint 8
#define Anum_pg_dist_spatiotemporal_tables_ismobilitydb 9
#define Anum_pg_dist_spatiotemporal_tables_distcol 10
#define Anum_pg_dist_spatiotemporal_tables_distcoltype 11
#define Anum_pg_dist_spatiotemporal_tables_tilekey 12
#define Anum_pg_dist_spatiotemporal_tables_shapesegmented 13
#define Anum_pg_dist_spatiotemporal_tables_srid 14

#endif /* PG_DIST_SPATIOTEMPORAL_TABLES_H */
