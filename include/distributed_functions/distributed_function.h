#ifndef DISTRIBUTED_FUNCTION_H
#define DISTRIBUTED_FUNCTION_H

#include "coordinator_operations.h"
#include "worker_operations.h"
#include "optimizer/planner.h"

typedef struct DistributedFunction
{
    CoordinatorOperation *coordinatorOperation;
    WorkerOperation *workerOperation;
    TargetEntry *org_operation;
} DistributedFunction;

extern DistributedFunction *addDistributedFunction(TargetEntry *operation);

#endif /* DISTRIBUTED_FUNCTION_H */
