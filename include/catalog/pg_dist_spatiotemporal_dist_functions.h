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

#ifndef PG_DIST_SPATIOTEMPORAL_DIST_FUNCTIONS_H
#define PG_DIST_SPATIOTEMPORAL_DIST_FUNCTIONS_H

/* ----------------
 *  pg_dist_spatiotemporal_dist_functions: Compiler constants
 *
 *  Catalog table that maps a distributable aggregate/function (id) to the
 *  three-phase function set used to run it across the cluster: the
 *  per-tile `worker` function, the `combiner` that merges partial results,
 *  and the `final` function that produces the end result. `sexec_id` ties
 *  the row to the pg_execution_run entry for the query that registered it.
 * ----------------
 */

#define Tbl_Dist_Functions "pg_dist_spatiotemporal_dist_functions"
#define Natts_pg_dist_spatiotemporal_dist_functions 5
#define Anum_pg_dist_spatiotemporal_dist_functions_id 1
#define Anum_pg_dist_spatiotemporal_dist_functions_worker 2
#define Anum_pg_dist_spatiotemporal_dist_functions_combiner 3
#define Anum_pg_dist_spatiotemporal_dist_functions_final 4
#define Anum_pg_dist_spatiotemporal_dist_functions_sexec_id 5

#endif /* PG_DIST_SPATIOTEMPORAL_DIST_FUNCTIONS_H */
