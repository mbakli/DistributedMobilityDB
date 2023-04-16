#ifndef SPATIOTEMPORAL_EXPLAIN_H
#define SPATIOTEMPORAL_EXPLAIN_H

#include "postgres.h"
#include "dist_mobilitydb_planner.h"
#include <distributed/multi_executor.h>
#include "executor/tstoreReceiver.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "catalog/nodes.h"
#include "executor/multi_phase_executor.h"

extern void spatiotemporal_explain(Query *query, int cursorOptions, IntoClause *into,
                            ExplainState *es, const char *queryString, ParamListInfo params,
                            QueryEnvironment *queryEnv);
extern void RegisterSpatiotemporalPlanMethods(void);
extern bool IsReshufflingRequired(List *strategies);

typedef struct SpatiotemporalScanState
{
    CustomScanState customScanState;  /* underlying custom scan node */

    /* function that gets called before postgres starts its execution */
    bool finishedPreScan;          /* flag to check if the pre scan is finished */
    void (*PreExecScan)(struct SpatiotemporalScanState *scanState);

    DistributedSpatiotemporalQueryPlan *distributedSpatiotemporalPlan; /* distributed execution plan */
    bool finishedRemoteScan;          /* flag to check if remote scan is finished */
    Tuplestorestate *tuplestorestate; /* tuple store to store distributed results */
} SpatiotemporalScanState;


typedef struct DistributedQueryExplain
{
    Query *query;
    char *query_string;

}DistributedQueryExplain;

extern void ExplainQueryParameters(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es, int indent_group);

#endif /* SPATIOTEMPORAL_EXPLAIN_H */
