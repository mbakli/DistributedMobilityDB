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
#include "miscadmin.h"
#include "utils/elog.h"
#include "commands/explain.h"
#include "planner/distributed_mobilitydb_planner.h"
#include "planner/distributed_mobilitydb_explain.h"
/* PostgreSQL Includes */
PG_MODULE_MAGIC;
void _PG_init(void);

/* shared library initialization function */
void
_PG_init(void)
{
    elog(LOG, "Init started: Distributed MobilityDB!");
    ereport(NOTICE, (errmsg("_PG_init")));

    /* Register the spatiotemporal plan methods */
    RegisterSpatiotemporalPlanMethods();

    /* intercept planner */
    planner_hook = distributed_mobilitydb_planner;

    ExplainOneQuery_hook = distributed_mobilitydb_explain;
}