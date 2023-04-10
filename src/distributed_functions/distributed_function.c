#include "postgres.h"
#include "distributed_functions/distributed_function.h"
#include "planner/planner_utils.h"

static void GetDistFuncInfo(DistributedFunction *dist_function);
/* addDistributedFunction is a function used in the post processing phase, where each function is defined using
 * three operations: worker, intermediate, coordinator.
 * */
extern DistributedFunction *
addDistributedFunction(TargetEntry *targetEntry)
{
    DistributedFunction *dist_function = (DistributedFunction *)palloc0(sizeof(DistributedFunction));
    dist_function->coordinatorOperation = (CoordinatorOperation *) palloc0(sizeof(CoordinatorOperation));
    dist_function->workerOperation = (WorkerOperation *) palloc0(sizeof (WorkerOperation));
    dist_function->org_operation = targetEntry;
    Datum datumArray[Natts_DistFun];
    bool isNullArray[Natts_DistFun];
    ScanKeyData scanKey[1];
    bool indexOK = false;
    Relation distFuns = table_open(DisFuncRelationId(), RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_DistFun_worker,
                BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(dist_function->org_operation->resname));

    SysScanDesc scanDescriptor = systable_beginscan(distFuns,
                                                    DistPlacementPlacementidIndexId(),
                                                    indexOK,
                                                    NULL, 1, scanKey);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    heap_deform_tuple(heapTuple, RelationGetDescr(distFuns), datumArray,
                      isNullArray);

    dist_function->workerOperation->op = PointerGetDatum(datumArray[Anum_DistFun_worker - 1]);
    dist_function->coordinatorOperation->final_op = PointerGetDatum(datumArray[Anum_DistFun_final - 1]);
    dist_function->coordinatorOperation->intermediate_op = PointerGetDatum(datumArray[Anum_DistFun_combiner - 1]);
    systable_endscan(scanDescriptor);
    table_close(distFuns, NoLock);
    return dist_function;
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
