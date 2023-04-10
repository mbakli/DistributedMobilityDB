#ifndef DISTRIBUTED_FUNCTION_H
#define DISTRIBUTED_FUNCTION_H

#include "coordinator_operations.h"
#include "worker_operations.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"


/* constants for distributed functions.options */
#define Natts_DistFun 5
#define Anum_DistFun_worker 2
#define Anum_DistFun_combiner 3
#define Anum_DistFun_final 4

typedef struct DistributedFunction
{
    CoordinatorOperation *coordinatorOperation;
    WorkerOperation *workerOperation;
    TargetEntry *org_operation;
} DistributedFunction;

extern DistributedFunction *addDistributedFunction(TargetEntry *operation);
extern bool IsDistFunc(TargetEntry *targetEntry);
#endif /* DISTRIBUTED_FUNCTION_H */
