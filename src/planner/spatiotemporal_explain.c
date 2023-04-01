#include "postgres.h"
#include "commands/explain.h"
#include "distributed/citus_custom_scan.h"
#include "planner/spatiotemporal_explain.h"
#include "general/metadata_management.h"
#include <utils/lsyscache.h>
#include <distributed/multi_explain.h>
#include "distributed/listutils.h"

/*
 * spatiotemporal_explain is the executor hook that is called when
 * postgres wants to explain a query.
 */
void
spatiotemporal_explain(Query *query, int cursorOptions, IntoClause *into,
                       ExplainState *es, const char *queryString, ParamListInfo params,
                       QueryEnvironment *queryEnv)
{

}