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

#ifndef HELPER_FUNCTIONS_H
#define HELPER_FUNCTIONS_H
#include "postgres.h"
#include <nodes/pg_list.h>

// #include "meos.h"
// #include "meos_internal.h"


extern List *ListSpatiotemporalOperations(Node * equals);
extern char *replaceWord( char* s,  char* oldW,  char* newW);
extern char *extract_between(const char *str, const char *p1, const char *p2);
extern char *change_sentence (char *sentence, char *find, char *replace);
extern char *toLower(char *str);
extern bool IsDatumEmpty(Datum val);
#endif /* HELPER_FUNCTIONS_H */
