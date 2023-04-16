#ifndef TABLE_OPS_H
#define TABLE_OPS_H

#include "postgres.h"



/* constants for columnar.options */
#define Anum_pg_spatiotemporal_join_operations_oid 3
#define Anum_pg_spatiotemporal_join_operations_distance 4

extern Datum * GetDistributedTableMetadata(Oid relationId);
extern bool IsReshuffledTable(Oid relationId);
extern int DistributedColumnType(Oid relationId);
extern  char * GetSpatiotemporalCol(Oid relationId);
extern char * GetGlobalIndexInfo(Oid relid);
extern Oid RelationId(const char *relationName);
extern bool IsDistributedSpatiotemporalTable(Oid relationId);

#endif /* TABLE_OPS_H */
