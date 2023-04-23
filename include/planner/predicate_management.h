#ifndef PREDICATE_MANAGMENT_H
#define PREDICATE_MANAGMENT_H
#include "postgres.h"
#include "multirelation/multirelation_utils.h"
#include <nodes/nodes.h>
#include <access/htup_details.h>

typedef enum PredicateType
{
    INTERSECTION,
    DISTANCE,
    RANGE,
    OTHER
} PredicateType;

typedef struct RangePredicate
{
    char *op;
    Datum bbox;
} RangePredicate;

typedef struct DistancePredicate
{
    char *op;
    float distance;
} DistancePredicate;

typedef struct IntersectionPredicate
{
    char *op;
} IntersectionPredicate;


typedef struct PredicateInfo
{
    RangePredicate *rangePredicate;
    DistancePredicate *distancePredicate;
    IntersectionPredicate *intersectionPredicate;
} PredicateInfo;


typedef struct Predicates
{
    PredicateType predicateType;
    PredicateInfo *predicateInfo;
} Predicates;

extern DistancePredicate *analyseDistancePredicate(Node *clause);
extern HeapTuple PgSpatiotemporalJoinOperationTupleViaCatalog(Oid operationId, bool distance);
extern bool IsDistanceOperation(Oid operationId);
extern bool IsIntersectionOperation(Oid operationId);
extern Datum get_query_range(STMultirelations *tbls, OpExpr *opExpr);
extern bool CheckTileRebalancerActivation(STMultirelations *tbls, OpExpr *opExpr, Datum box);
#endif /* PREDICATE_MANAGMENT_H */
