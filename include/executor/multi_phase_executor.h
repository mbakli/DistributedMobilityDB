#ifndef SPATIOTEMPORAL_EXECUTOR_H
#define SPATIOTEMPORAL_EXECUTOR_H

#include "planner/spatiotemporal_planner.h"

extern void createReshuffledTable(DistributedSpatiotemporalQueryPlan *distPlan);
extern void DropReshuffledTableIfExists(char * reshuffled_table);
extern void CreateReshuffledTableIfNotExists(char * reshuffled_table, char * org_table);
#endif /* SPATIOTEMPORAL_EXECUTOR_H */
