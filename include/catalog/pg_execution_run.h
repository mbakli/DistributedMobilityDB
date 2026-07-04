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

#ifndef PG_EXECUTION_RUN_H
#define PG_EXECUTION_RUN_H

/* ----------------
 *  pg_execution_run: Compiler constants
 *
 *  Catalog table that assigns a unique execution id (id) to each
 *  distributed query run, paired with its serialized executor state
 *  (sexec) so distributed_functions rows can be traced back to the run
 *  that produced them.
 * ----------------
 */

#define Natts_pg_execution_run 2
#define Anum_pg_execution_run_id 1
#define Anum_pg_execution_run_sexec 2

#endif /* PG_EXECUTION_RUN_H */
