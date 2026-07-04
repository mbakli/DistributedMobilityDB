/*****************************************************************************
 *
 * DistributedMobilityDB - Large-Scale Spatial, Temporal, and Spatiotemporal Data Management Within PostgreSQL
 * https://github.com/mbakli/DistributedMobilityDB
 *
 * DistributedMobilityDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Copyright (c) 2023-2024 Mohamed Bakli <mohamed_bakli@hotmail.com>
 *
 *****************************************************************************/

#ifndef PREDICATE_MANAGMENT_H
#define PREDICATE_MANAGMENT_H
#include "postgres.h"
#include "multirelation/multirelation_utils.h"
#include <nodes/nodes.h>
#include <access/htup_details.h>

/* Classifies a WHERE-clause predicate by the kind of spatial/temporal test it performs. */
typedef enum PredicateType
{
    INTERSECTION,
    DISTANCE,
    RANGE,
    OTHER
} PredicateType;

/* A bounding-box range predicate (e.g. `&&`), with its operator and query box. */
typedef struct RangePredicate
{
    char *op;
    Datum bbox;
} RangePredicate;

/* A distance predicate (e.g. eDwithin), with its operator and distance threshold. */
typedef struct DistancePredicate
{
    char *op;
    float distance;
} DistancePredicate;

/* An intersection predicate (e.g. ST_Intersects/eIntersects). */
typedef struct IntersectionPredicate
{
    char *op;
} IntersectionPredicate;

/* Holds the parsed detail for whichever PredicateType a Predicates entry represents. */
typedef struct PredicateInfo
{
    RangePredicate *rangePredicate;
    DistancePredicate *distancePredicate;
    IntersectionPredicate *intersectionPredicate;
} PredicateInfo;

/* One analysed predicate from the query's WHERE clause. */
typedef struct Predicates
{
    PredicateType predicateType;
    PredicateInfo *predicateInfo;
} Predicates;

/* Extracts the operator and distance threshold from a distance-predicate clause. */
extern DistancePredicate *analyseDistancePredicate(Node *clause);

/* Looks up the pg_spatiotemporal_join_operations catalog tuple for a distance/intersection operator. */
extern HeapTuple PgSpatiotemporalJoinOperationTupleViaCatalog(Oid operationId, bool distance);

/* True if operationId is a registered distance operator (e.g. eDwithin). */
extern bool IsDistanceOperation(Oid operationId);

/* True if operationId is a registered intersection operator (e.g. eIntersects). */
extern bool IsIntersectionOperation(Oid operationId);

/*
 * Extracts the callable identifier and argument list from a WHERE-clause
 * predicate node, regardless of whether MobilityDB/PostGIS exposed it as an
 * infix operator (OpExpr, e.g. `&&`) or a plain function call (FuncExpr,
 * e.g. eDwithin(...), ST_Intersects(...)) -- both forms appear in practice
 * and pg_spatiotemporal_join_operations is keyed by either an operator or a
 * function oid. Returns false (leaving *oid/*args unset) for any other node
 * type.
 */
extern bool GetPredicateOidAndArgs(Node *clause, Oid *oid, List **args);

/* Computes the query's search bounding box from tbls and the range/distance predicate clause. */
extern Datum get_query_range(STMultirelations *tbls, Node *clause);

/* True if clause's search box spans enough tiles of tbls to warrant rebalancing first. */
extern bool CheckTileRebalancerActivation(STMultirelations *tbls, Node *clause, Datum box);
#endif /* PREDICATE_MANAGMENT_H */
