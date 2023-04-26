#include "postgres.h"
#include "general/rte.h"
#include "catalog/table_ops.h"
#include "multirelation/multirelation_utils.h"

extern Rte *
GetRteNode(Node * node, RteType rteType, Alias *alias)
{
    Rte *rteNode = (Rte *) palloc0(sizeof(Rte));
    rteNode->rte = node;
    rteNode->RteType = rteType;
    rteNode->alias = alias;
    return rteNode;
}

extern CitusRteNode *
GetCitusRteInfo(RangeTblEntry *rangeTableEntry, char partitionMethod)
{
    CitusRteNode *citusRteNode = (CitusRteNode *) palloc0(sizeof(CitusRteNode));
    citusRteNode->col = GetShapeCol(rangeTableEntry->relid);
    //citusRteNode->shapeType = GetShapeType(rangeTableEntry->relid);
    citusRteNode->localIndex = GetLocalIndex(rangeTableEntry->relid,
                                             citusRteNode->col);
    citusRteNode->partitionMethod = partitionMethod;
    return citusRteNode;
}

extern LocalRteNode *
GetLocalRteInfo(RangeTblEntry *rangeTableEntry)
{
    LocalRteNode *localRteNode = (LocalRteNode *) palloc0(sizeof(LocalRteNode));
    localRteNode->col = GetShapeCol(rangeTableEntry->relid);
    localRteNode->localIndex = GetLocalIndex(rangeTableEntry->relid,
                                             localRteNode->col);
    return localRteNode;
}