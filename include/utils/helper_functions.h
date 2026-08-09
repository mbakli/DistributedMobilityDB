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
 * Copyright (c) 2020-2026 Mohamed Bakli <mohamed_bakli@hotmail.com>
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

/*
 * Finds the first case-insensitive, whitespace-bounded occurrence of
 * `keyword` in `lowered` (already-lowercased haystack, already-lowercase
 * keyword) -- tolerates newlines/tabs/multiple spaces around it, unlike a
 * plain strstr(haystack, " keyword "). Returns a pointer to the start of
 * the keyword itself, or NULL if not found.
 */
extern char *FindKeywordToken(const char *lowered, const char *keyword);

/*
 * Like FindKeywordToken, but only matches an occurrence at paren-depth 0
 * (not nested inside a subquery/CTE's own parenthesized definition) -- for
 * locating a keyword that belongs to a whole query's outermost SELECT.
 */
extern char *FindTopLevelKeywordToken(const char *lowered, const char *keyword);

/*
 * Finds the first case-insensitive occurrence of `identifier` in `lowered`
 * bounded by non-identifier characters (or string start/end) on both sides
 * -- unlike FindKeywordToken, which only tolerates whitespace boundaries,
 * this also treats punctuation immediately adjacent to the identifier
 * (parens, commas, dots, operators, ...) as a valid boundary, needed to
 * locate a bare column reference embedded in an expression like
 * "numinstants(trip)" or "trip,othercol" where nothing separates it from
 * what follows by whitespace. Also guards against matching inside a longer
 * identifier that merely contains `identifier` as a substring (e.g.
 * "roundtrip" when searching for "trip"). `lowered` must already be
 * lowercased; `identifier` must already be lowercase.
 */
extern char *FindIdentifierToken(const char *lowered, const char *identifier);

/* True if val is a NULL/zero Datum (no value set). */
extern bool IsDatumEmpty(Datum val);

/* Renders datum (of the given PostgreSQL type oid) as a palloc'd C string via its type's output function. */
extern char *DatumToString(Datum datum, Oid typeoid);

/* Runs query via SPI, erroring out if its result status doesn't match expectedSpiOk (an SPI_OK_* constant). */
extern void ExecuteQueryViaSPI(char *query, int expectedSpiOk);

/* Returns a newly palloc'd, whitespace-trimmed copy of the text spanning [start, end). */
extern char *TrimmedSubstring(const char *start, const char *end);

/*
 * Splits text on commas that are not nested inside parentheses, returning a
 * List of palloc'd, whitespace-trimmed C-string chunks in left-to-right
 * order -- e.g. breaking a SELECT/ORDER BY list's text into one chunk per
 * entry without misreading a comma inside a nested function call (e.g.
 * `atTime(t.Trip, p.Period)`) as a top-level separator.
 */
extern List *SplitTopLevelCommas(const char *text);

/*
 * Splits text on top-level " and " keywords (case-insensitive, not nested
 * inside parentheses, whitespace-bounded) -- e.g. breaking a WHERE clause's
 * text into one chunk per top-level conjunct, the way SplitTopLevelCommas
 * breaks a SELECT list into one chunk per entry.
 */
extern List *SplitTopLevelConjuncts(const char *text);
#endif /* HELPER_FUNCTIONS_H */
