#ifndef COORDINATOR_OPERATIONS_H
#define COORDINATOR_OPERATIONS_H

typedef struct CoordinatorOperation
{
    Datum intermediate_op;
    Datum final_op;
} CoordinatorOperation;

#endif /* COORDINATOR_OPERATIONS_H */
