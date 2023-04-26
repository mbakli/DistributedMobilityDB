#ifndef RTE_H
#define RTE_H

#include "postgres.h"
#include "general_types.h"
#include "multirelation/multirelation_utils.h"
#include <nodes/primnodes.h>
#include <nodes/parsenodes.h>

/* Generic Relation */
typedef struct Rte
{
    Node *rte;
    bool RteType;
    Alias *alias;
} Rte;

/* Rte Types */
typedef enum RteType
{
    STRte,
    CitusRte,
    LocalRte
} RteType;

/* Citus Relation */
typedef struct CitusRteNode
{
    ListCell *rangeTableCell;
    char partitionMethod;
    ShapeType shapeType;
    int srid;
    char *col;
    char *localIndex;
    char *reshuffledTable;
} CitusRteNode;

/* Local Relation */
typedef struct LocalRteNode
{
    ListCell *rangeTableCell;
    bool refCandidate; /* Candidate to be broadcasted */
    int srid;
    char *col;
    char *localIndex;
    char *reshuffledTable;
} LocalRteNode;

/* Reshuffling Relation */
typedef struct ReshufflingRte
{
    STMultirelation *stMultirelation;
} ReshufflingRte;


extern Rte *GetRteNode(Node * node, RteType rteType, Alias *alias);
extern CitusRteNode *GetCitusRteInfo(RangeTblEntry *rangeTableEntry, char partitionMethod);
extern LocalRteNode * GetLocalRteInfo(RangeTblEntry *rangeTableEntry);

#endif /* RTE_H */
