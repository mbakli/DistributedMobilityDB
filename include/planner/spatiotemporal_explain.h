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

#endif /* SPATIOTEMPORAL_EXPLAIN_H */
