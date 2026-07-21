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

#ifndef SPATIOTEMPORAL_EXPLAIN_H
#define SPATIOTEMPORAL_EXPLAIN_H

#include "postgres.h"
#include "distributed_mobilitydb_planner.h"
#include <distributed/multi_executor.h>
#include "executor/tstoreReceiver.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "catalog/nodes.h"
#include "executor/multi_phase_executor.h"

/* EXPLAIN hook: planned and runs query, then prints the distributed plan/tasks. */
extern void distributed_mobilitydb_explain(Query *query, int cursorOptions, IntoClause *into,
                            ExplainState *es, const char *queryString, ParamListInfo params,
                            QueryEnvironment *queryEnv);

/* Registers the custom-scan callbacks (Begin/Exec/End/Explain) for the spatiotemporal scan node. */
extern void RegisterSpatiotemporalPlanMethods(void);

/* True if any of `strategies` requires reshuffling tile data before execution. */
extern bool IsReshufflingRequired(List *strategies);

/* Custom-scan execution state for a distributed spatiotemporal query. */
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


/* The (possibly rewritten) SQL text of the query being explained. */
typedef struct DistributedQueryExplain
{
    char *query_string;

}DistributedQueryExplain;

/* Writes distPlan's strategy, tasks and parameters into the EXPLAIN output (es). */
extern void ExplainQueryParameters(DistributedSpatiotemporalQueryPlan *distPlan, ExplainState *es, int indent_group);

#endif /* SPATIOTEMPORAL_EXPLAIN_H */
