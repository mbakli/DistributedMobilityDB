#include "postgres.h"
#include "utils/planner_utils.h"
#include <distributed/metadata_cache.h>
#include "distributed_functions/distributed_function.h"
#include "general/general_types.h"
#include "utils/builtins.h"


/* addDistributedFunction is a function used in the post processing phase, where each function is defined using
 * three operations: worker, intermediate, coordinator.
 * */
extern DistributedFunction *
addDistributedFunction(TargetEntry *targetEntry)
{
    DistributedFunction *dist_function = (DistributedFunction *)palloc0(sizeof(DistributedFunction));
    dist_function->coordinatorOp = (CoordinatorOperation *) palloc0(sizeof(CoordinatorOperation));
    dist_function->workerOp = (WorkerOperation *) palloc0(sizeof (WorkerOperation));
    dist_function->targetEntry = targetEntry;
    Datum datumArray[Natts_DistFun];
    bool isNullArray[Natts_DistFun];
    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation distFuns = table_open(DisFuncRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_DistFun_worker,
                BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(dist_function->targetEntry->resname));

    SysScanDesc scanDescriptor = systable_beginscan(distFuns,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    heap_deform_tuple(heapTuple, RelationGetDescr(distFuns), datumArray,
                      isNullArray);

    dist_function->workerOp->op = PointerGetDatum(datumArray[Anum_DistFun_worker - 1]);
    dist_function->coordinatorOp->final_op = PointerGetDatum(datumArray[Anum_DistFun_final - 1]);
    dist_function->coordinatorOp->intermediate_op = PointerGetDatum(datumArray[Anum_DistFun_combiner - 1]);

    systable_endscan(scanDescriptor);
    table_close(distFuns, NoLock);

    return dist_function;
}

extern QOperation *
AddQOperation(Datum des, Datum cur)
{
    QOperation *qOp = (QOperation *) palloc0(sizeof(QOperation));
    qOp->op = des;
    qOp->alias = makeAlias(DatumToString(cur, TEXTOID), NIL);
    qOp->col = cur;
    return qOp;
}

extern bool
IsDistFunc(TargetEntry *targetEntry)
{
    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation distFuns = table_open(DisFuncRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_DistFun_worker,
                BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(targetEntry->resname));

    SysScanDesc scanDescriptor = systable_beginscan(distFuns,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);

    HeapTuple heapTuple = systable_getnext(scanDescriptor);

    bool heapTupleIsValid = HeapTupleIsValid(heapTuple);
    systable_endscan(scanDescriptor);
    table_close(distFuns, AccessShareLock);
    return heapTupleIsValid;
}

