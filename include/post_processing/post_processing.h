#ifndef POST_PROCESSING_H
#define POST_PROCESSING_H
#include "postgres.h"
#include "distributed/listutils.h"

typedef struct CollectOperator
{
    bool active;

} CollectOperator;

typedef struct MergeOperator
{
    bool active;

} MergeOperator;

typedef struct WorkerLevelOperator
{
    CollectOperator *collectOperator;
} WorkerLevelOperator;

typedef struct RemDupOperator
{
    bool active;
} RemDupOperator;

typedef struct CoordinatorLevelOperator
{
    MergeOperator *mergeOperator;
    RemDupOperator * dupRemOperator;
} CoordinatorLevelOperator;



typedef struct PostProcessing
{
    CoordinatorLevelOperator *coordinatorLevelOperator;
    WorkerLevelOperator  *workerLevelOperator;
    List *distfuns; // DistributedFunction;
    bool group_by_required;
    Datum *genPrimKey;
} PostProcessing;

extern PostProcessing *PostProcessingQuery(List *strategies);
#endif /* POST_PROCESSING_H */
