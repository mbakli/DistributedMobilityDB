#ifndef QUERY_SEMANTICS_CHECK_H
#define QUERY_SEMANTICS_CHECK_H
#include "post_processing/post_processing.h"
#include "multirelation/multirelation_utils.h"

extern void analyseSelectClause(List *targetList, PostProcessing *postProcessing);
extern CatalogFilter *AnalyseCatalog(SpatiotemporalTable *tbl, FromExpr * fromExpr);

#endif /* QUERY_SEMANTICS_CHECK_H */
