#ifndef TILE_TASKS_H
#define TILE_TASKS_H

#include "postgres.h"
#include <nodes/pg_list.h>
#include "executor_tasks.h"
#include "general/rte.h"

extern char *taskQuery (List *tasks, ExecTaskType taskType);
extern void RearrangeTiles(Oid relid, int numTiles, char *reshuffledTable);
extern Datum AddTilingKey(STMultirelationCatalog tblCatalog, Alias *alias , char * query_string);
extern Datum AddNonStRteTilingKey(Rte *tbl, Alias *alias ,char * query_string);
extern int GetRandTileNum(STMultirelation *rte);
#endif /* TILE_TASKS_H */
