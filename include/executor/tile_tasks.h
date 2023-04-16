#ifndef TILE_TASKS_H
#define TILE_TASKS_H

#include "postgres.h"
#include <nodes/pg_list.h>
#include "executor_tasks.h"

extern char *taskQuery (List *tasks, ExecTaskType taskType);
extern void RearrangeTiles(Oid relid, int numTiles, char *reshuffledTable);
extern Datum AddTilingKey(SpatiotemporalTableCatalog tblCatalog, Alias *alias , char * query_string);

#endif /* TILE_TASKS_H */
