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

#ifndef WORKER_OPERATIONS_H
#define WORKER_OPERATIONS_H

/*
 * WorkerOperation
 *
 * The worker-side half of a distributed aggregate/expression: `op` is the
 * per-tile computation run on each worker before its partial result is
 * shipped back to the coordinator for combination (see CoordinatorOperation).
 */
typedef struct WorkerOperation
{
    Datum op;
} WorkerOperation;

#endif /* WORKER_OPERATIONS_H */
