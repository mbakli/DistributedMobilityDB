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
#include "planner/spatiotemporal_planner.h"
#include <utils/snapmgr.h>

extern bool IsDistributedSpatiotemporalTable(Oid relationId);
extern bool IsDistanceOperation(Oid operationId);
extern bool IsIntersectionOperation(Oid operationId);
extern Datum * GetDistributedTableMetadata(Oid relationId);
extern bool IsReshuffledTable(Oid relationId);
extern bool DistributedColumnType(Oid relationId);
extern  char * GetSpatiotemporalCol(Oid relationId);
extern char * GetGlobalIndexInfo(Oid relid);
extern char* GetRandomTileId(Oid relationId);
extern int TilingSearch(Oid relationId);
extern int GetNumTiles(Oid relationId);

#endif /* METADATA_MANAGEMENT_H */
