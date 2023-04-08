#ifndef PREDICATE_MANAGMENT_H
#define PREDICATE_MANAGMENT_H
#include <nodes/nodes.h>


typedef enum PredicateType
{
    INTERSECTION,
    DISTANCE,
    RANGE
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

#endif /* PREDICATE_MANAGMENT_H */
