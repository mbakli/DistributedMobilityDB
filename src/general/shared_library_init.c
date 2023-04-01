#include "postgres.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "commands/explain.h"
#include "planner/spatiotemporal_planner.h"
#include "planner/spatiotemporal_explain.h"


/* shared library initialization function */
void
_PG_init(void)
{
    elog(LOG, "Init started: Distributed MobilityDB!");
    ereport(NOTICE, (errmsg("_PG_init")));

    /* Register the spatiotemporal plan methods */
    RegisterSpatiotemporalPlanMethods();

    /* intercept planner */
    planner_hook = spatiotemporal_planner;

    ExplainOneQuery_hook = spatiotemporal_explain;

}