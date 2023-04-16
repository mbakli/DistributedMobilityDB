#ifndef DISTRIBUTED_FUNCTION_H
#define DISTRIBUTED_FUNCTION_H

#include "postgres.h"
#include "coordinator_operations.h"
#include "worker_operations.h"
#include <nodes/makefuncs.h>

/* constants for distributed functions.options */
#define Natts_DistFun 5
#define Anum_DistFun_worker 2
#define Anum_DistFun_combiner 3
#define Anum_DistFun_final 4


typedef struct QOperation
{
    Datum op;
    Alias *alias;
    Datum col;
} QOperation;

typedef struct DistributedFunction
{
    CoordinatorOperation *coordinatorOp;
    WorkerOperation *workerOp;
    TargetEntry *targetEntry;
} DistributedFunction;

extern DistributedFunction *addDistributedFunction(TargetEntry *operation);
extern bool IsDistFunc(TargetEntry *targetEntry);
extern QOperation * AddQOperation(Datum des, Datum cur);
#endif /* DISTRIBUTED_FUNCTION_H */
