#include "postgres.h"
#include "distributed_functions/distributed_function.h"

/* addDistributedFunction is a function used in the post processing phase, where each function is defined using
 * three operations: worker, intermediate, coordinator.
 * */
extern DistributedFunction *
addDistributedFunction(TargetEntry *targetEntry)
{
    DistributedFunction *dist_function = palloc0(sizeof(DistributedFunction));
    return dist_function;
}