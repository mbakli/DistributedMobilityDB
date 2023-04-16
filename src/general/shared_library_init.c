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