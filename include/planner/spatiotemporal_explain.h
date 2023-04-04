#ifndef SPATIOTEMPORAL_EXPLAIN_H
#define SPATIOTEMPORAL_EXPLAIN_H

#include "postgres.h"
#include "spatiotemporal_planner.h"
#include <distributed/multi_executor.h>
#include "executor/tstoreReceiver.h"
#include "libpq-fe.h"
#include "miscadmin.h"

void spatiotemporal_explain(Query *query, int cursorOptions, IntoClause *into,
                            ExplainState *es, const char *queryString, ParamListInfo params,
                            QueryEnvironment *queryEnv);
void RegisterSpatiotemporalPlanMethods(void);

typedef struct SpatiotemporalScanState
{
    CustomScanState customScanState;  /* underlying custom scan node */

    /* function that gets called before postgres starts its execution */
    bool finishedPreScan;          /* flag to check if the pre scan is finished */
    //void (*PreExecScan)(struct SpatiotemporalScanState *scanState);

    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan; /* distributed execution plan */
    //SpatiotemporalExecutorType executorType;   /* distributed executor type */
    bool finishedRemoteScan;          /* flag to check if remote scan is finished */
    Tuplestorestate *tuplestorestate; /* tuple store to store distributed results */
} SpatiotemporalScanState;
#endif /* SPATIOTEMPORAL_EXPLAIN_H */
