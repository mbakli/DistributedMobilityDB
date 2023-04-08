#ifndef SPATIOTEMPORAL_EXECUTOR_H
#define SPATIOTEMPORAL_EXECUTOR_H

#include "planner/spatiotemporal_planner.h"
#include <utils/snapmgr.h>
#include <executor/spi.h>
#include <utils/lsyscache.h>


/*
 * MultiPhaseExecutor
 */
typedef struct MultiPhaseExecutor
{
    DistributedSpatiotemporalQueryPlan distPlan;
    StringInfo neighborScanQuery;
    StringInfo selfTilingScanQuery;
    int numCores;
    bool dataReshuffled;
    bool table_created;
} MultiPhaseExecutor;

extern GeneralScan * QueryExecutor(DistributedSpatiotemporalQueryPlan *distPlan, bool explain);
extern void DropReshuffledTableIfExists(char * reshuffled_table);
extern void CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table);
#endif /* SPATIOTEMPORAL_EXECUTOR_H */
