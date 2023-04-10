#include "post_processing/post_processing.h"
#include "planner/planner_strategies.h"

/* Post Processing Query */
extern PostProcessing *
PostProcessingQuery(List *strategies)
{
    PostProcessing *postProcessing = (PostProcessing *) palloc0(sizeof(PostProcessing));
    postProcessing->coordinatorLevelOperator =
            (CoordinatorLevelOperator *) palloc0(sizeof(CoordinatorLevelOperator));
    postProcessing->coordinatorLevelOperator->dupRemOperator =
            (RemDupOperator *) palloc0(sizeof(RemDupOperator));
    postProcessing->coordinatorLevelOperator->mergeOperator =
            (MergeOperator *) palloc0(sizeof(MergeOperator));
    postProcessing->workerLevelOperator =
            (WorkerLevelOperator *) palloc0(sizeof(WorkerLevelOperator));
    postProcessing->workerLevelOperator->collectOperator =
            (CollectOperator *) palloc0(sizeof(CollectOperator));
    ListCell *cell = NULL;
    if (list_length(strategies) > 0)
    {
        postProcessing->coordinatorLevelOperator->mergeOperator->active = true;
        postProcessing->workerLevelOperator->collectOperator->active = true;
    }
    return postProcessing;
}

