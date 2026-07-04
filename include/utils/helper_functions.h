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


/* Flattens a boolean expression tree of spatiotemporal predicates into a list of OpExprs. */
extern List *ListSpatiotemporalOperations(Node * equals);

/* Returns a copy of s with the first occurrence of oldW replaced by newW. */
extern char *replaceWord( char* s,  char* oldW,  char* newW);

/* Returns the substring of str strictly between markers p1 and p2. */
extern char *extract_between(const char *str, const char *p1, const char *p2);

/* Returns a copy of sentence with every occurrence of find replaced by replace. */
extern char *change_sentence (char *sentence, char *find, char *replace);

/* Returns a lowercased copy of str. */
extern char *toLower(char *str);

/* True if val is a NULL/zero Datum (no value set). */
extern bool IsDatumEmpty(Datum val);
#endif /* HELPER_FUNCTIONS_H */
