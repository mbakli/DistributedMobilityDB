#include "postgres.h"
#include "multirelation/multirelation_utils.h"
#include "catalog/table_ops.h"
#include "utils/planner_utils.h"

extern STMultirelation *
GetMultirelationInfo(RangeTblEntry *rangeTableEntry, ShapeType type)
{
    STMultirelation *multirelation = (STMultirelation *) palloc0(sizeof(STMultirelation));
    multirelation->shapeType = type;
    multirelation->col = GetSpatiotemporalCol(rangeTableEntry->relid);
    multirelation->alias = rangeTableEntry->alias;
    multirelation->localIndex = GetLocalIndex(rangeTableEntry->relid,
                                              multirelation->col);
    multirelation->catalogTableInfo = GetTilingSchemeInfo(rangeTableEntry->relid);
    return multirelation;
}



extern char
GetReshufflingType(RangeTblEntry *rangeTableEntry)
{
    //char partitioningMethod =
}
