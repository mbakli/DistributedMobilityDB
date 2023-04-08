#ifndef METADATA_MANAGEMENT_H
#define METADATA_MANAGEMENT_H

#include "postgres.h"
#include "access/genam.h"
#include "utils/fmgroids.h"
#include "catalog/pg_namespace.h"
#include <utils/lsyscache.h>
#include <catalog/pg_extension.h>
#include "libpq-fe.h"
#include "executor/spi.h"
#include <utils/snapmgr.h>
#include "partitioning/tiling_utils.h"

extern bool IsDistanceOperation(Oid operationId);
extern bool IsIntersectionOperation(Oid operationId);
extern Datum * GetDistributedTableMetadata(Oid relationId);
extern bool IsReshuffledTable(Oid relationId);
extern int DistributedColumnType(Oid relationId);
extern  char * GetSpatiotemporalCol(Oid relationId);
extern char * GetGlobalIndexInfo(Oid relid);
extern Oid RelationId(const char *relationName);
extern char* GetRandomTileId(Oid relationId);
extern int TilingSearch(Oid relationId);
extern int GetNumTiles(Oid relationId);
extern bool IsDistributedSpatiotemporalTable(Oid relationId);


#endif /* METADATA_MANAGEMENT_H */
