#include "post_processing/post_processing.h"
#include "planner/planner_strategies.h"


/* Initialize Post Processing  */
extern PostProcessing *
InitializePostProcessing()
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
    return postProcessing;
}

/* Post Processing Query */
extern void
PostProcessingQuery(PostProcessing *postProcessing, List *strategies)
{
    /* The collect and merge operators are needed for all strategies */
    if (list_length(strategies) > 0)
    {
        postProcessing->coordinatorLevelOperator->mergeOperator->active = true;
        postProcessing->workerLevelOperator->collectOperator->active = true;
    }
    /* The other operations will be added internally based on the query semantics */
}

/* Early rewriting for the distributed functions */

extern void
RewriterDistFuncs(Query *parse, PostProcessing *postProcessing, const char *query_string)
{
    /* Currently, we do not support using alias names for the function results */
    ListCell *cell = NULL;
    postProcessing->coordinatorLevelOperator->intermediateOp = NIL;
    postProcessing->coordinatorLevelOperator->finalOp = NIL;
    StringInfo worker = makeStringInfo();
    appendStringInfo(worker, "%s", replaceWord(toLower((char *)query_string), ";", " "));
    /* Add them in a list and parse them in the executor */
    foreach(cell, postProcessing->distfuns)
    {
        DistributedFunction *func = (DistributedFunction *) lfirst(cell);
        /* For now add the worker function to the original query */
        StringInfo queryOp = makeStringInfo();
        appendStringInfo(queryOp, "%s", replaceWord(worker->data,
                                                    func->targetEntry->resname,
                                                    DatumToString(func->workerOp->op,
                                                                  TEXTOID)));
        resetStringInfo(worker);
        appendStringInfo(worker, "%s", queryOp->data);
        resetStringInfo(queryOp);
        /* Add the combiner in the intermediate query */
        if (!IsDatumEmpty(func->coordinatorOp->intermediate_op))
        {
            QOperation *qOp = AddQOperation(func->coordinatorOp->intermediate_op,
                                            func->workerOp->op);
            postProcessing->coordinatorLevelOperator->intermediateOp = lappend(
                    postProcessing->coordinatorLevelOperator->intermediateOp, qOp);
            resetStringInfo(queryOp);
        }
        /* Add the final in the intermediate query */
        if (!IsDatumEmpty(func->coordinatorOp->final_op))
        {
            QOperation *qOp = AddQOperation(func->coordinatorOp->final_op,
                                            func->workerOp->op);
            postProcessing->coordinatorLevelOperator->finalOp = lappend(
                    postProcessing->coordinatorLevelOperator->finalOp, qOp);
            resetStringInfo(queryOp);
        }
    }
    postProcessing->worker = worker->data;
}