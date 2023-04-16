#ifndef POST_PROCESSING_H
#define POST_PROCESSING_H
#include "postgres.h"
#include "distributed/listutils.h"
#include "distributed_functions/distributed_function.h"

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
    List *intermediateOp;
    List *finalOp;
} CoordinatorLevelOperator;


typedef struct PostProcessing
{
    CoordinatorLevelOperator *coordinatorLevelOperator;
    WorkerLevelOperator  *workerLevelOperator;
    List *distfuns; // DistributedFunction;
    bool group_by_required;
    Datum *genPrimKey;
    char * worker;
    Datum intermediate;
    Datum final;
} PostProcessing;

extern void PostProcessingQuery(PostProcessing *postProcessing,List *strategies);
extern PostProcessing *InitializePostProcessing();
extern void RewriterDistFuncs(Query *parse, PostProcessing *postProcessing, const char *query_string);
#endif /* POST_PROCESSING_H */
