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

#ifndef DISTRIBUTED_FUNCTION_H
#define DISTRIBUTED_FUNCTION_H

#include "postgres.h"
#include "coordinator_operations.h"
#include "worker_operations.h"
#include <nodes/makefuncs.h>

/* constants for distributed functions.options */
#define Natts_DistFun 5
#define Anum_DistFun_worker 2
#define Anum_DistFun_combiner 3
#define Anum_DistFun_final 4

/*
 * QOperation
 *
 * A single query operation (e.g. an aggregate call) being rewritten for
 * distributed execution: `op` is the operation's Datum representation,
 * `col` the column/argument it applies to, and `alias` the output alias
 * it must be projected under in the rewritten targetlist.
 */
typedef struct QOperation
{
    Datum op;
    Alias *alias;
    Datum col;
} QOperation;

/*
 * DistributedFunction
 *
 * Binds a query targetlist entry to the worker/coordinator function pair
 * (looked up via pg_dist_spatiotemporal_dist_functions) that implements it
 * across the distributed plan.
 */
typedef struct DistributedFunction
{
    CoordinatorOperation *coordinatorOp;
    WorkerOperation *workerOp;
    TargetEntry *targetEntry;
} DistributedFunction;

/* Resolves and attaches the worker/coordinator functions for a targetlist entry. */
extern DistributedFunction *addDistributedFunction(TargetEntry *operation);

/* True if targetEntry's expression is a registered distributable function. */
extern bool IsDistFunc(TargetEntry *targetEntry);

/* Looks up workerFuncName's registered "final" combining op (e.g. "sum" for "length"), or NULL if unregistered. */
extern char *LookupDistFuncFinalOp(const char *workerFuncName);

/* Looks up workerFuncName's registered "combiner" op, or NULL if it has none (the common case today). */
extern char *LookupDistFuncCombinerOp(const char *workerFuncName);

/* Builds a QOperation pairing a distributed op (des) with its argument column (cur). */
extern QOperation * AddQOperation(Datum des, Datum cur);
#endif /* DISTRIBUTED_FUNCTION_H */
