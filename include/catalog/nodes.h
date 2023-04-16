#ifndef NODESS_H
#define NODESS_H
#include "postgres.h"
#include "executor/executor_tasks.h"

extern Datum GetDBName();
extern TaskNode *GetNodeInfo();
extern char* GetRandomTileId(Oid relationId, ExecTaskType taskType);
extern int TilingSearch(Oid relationId);
extern int GetNumTiles(Oid relationId);
#endif /* NODESS_H */
