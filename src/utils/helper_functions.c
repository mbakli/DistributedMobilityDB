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

#include "utils/helper_functions.h"
#include <ctype.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <executor/spi.h>

/* DatumToString renders datum as a palloc'd C string via typeoid's output function. */
extern char *
DatumToString(Datum datum, Oid typeoid)
{
    Oid typoutput;
    bool typisvarlena;

    getTypeOutputInfo(typeoid, &typoutput, &typisvarlena);
    return OidOutputFunctionCall(typoutput, datum);
}

/* ExecuteQueryViaSPI runs query via SPI, erroring out if the result status doesn't match expectedSpiOk. */
extern void
ExecuteQueryViaSPI(char *query, int expectedSpiOk)
{
    int spi_result;

    spi_result = SPI_connect();
    if (spi_result != SPI_OK_CONNECT)
    {
        elog(ERROR, "Could not connect to database using SPI");
    }
    spi_result = SPI_execute(query, false, 0);
    if (spi_result != expectedSpiOk)
    {
        elog(ERROR, "SPI_execute failed for query: %s", query);
    }
    spi_result = SPI_finish();
    if (spi_result != SPI_OK_FINISH)
    {
        elog(ERROR, "Could not disconnect from database using SPI");
    }
}

/*
 * replaceWord replaces the first occurrence of oldW in s with newW and
 * returns the result as a freshly palloc'd string (s itself is left
 * untouched). A prior revision built the result into a buffer sized off
 * strlen(s) and copied it back into s in place -- since callers routinely
 * replace a bare table name with a longer schema-qualified one (e.g.
 * "trips" -> "dist_mobilitydb.trips_reshuffled"), newW is frequently longer
 * than oldW, which overflowed that buffer (and s itself, which is sized for
 * the original text) and corrupted the stack.
 */
extern
char* replaceWord( char* s,  char* oldW,  char* newW)
{
    size_t sLen = strlen(s);
    size_t oldWLen = strlen(oldW);
    size_t newWLen = strlen(newW);
    char *bstr = (char *) palloc0(sLen + newWLen + 1);
    size_t i = 0;
    size_t bpos = 0;
    int occurance = 0;

    while (i < sLen)
    {
        if (occurance == 0 && oldWLen > 0 && strncmp(s + i, oldW, oldWLen) == 0)
        {
            memcpy(bstr + bpos, newW, newWLen);
            bpos += newWLen;
            occurance++;
            i += oldWLen;
        }
        else
        {
            bstr[bpos++] = s[i++];
        }
    }
    bstr[bpos] = '\0';
    return bstr;
}

/* TrimmedSubstring returns a newly palloc'd, whitespace-trimmed copy of the text spanning [start, end). */
extern char *
TrimmedSubstring(const char *start, const char *end)
{
    while (start < end && isspace((unsigned char) *start))
        start++;
    while (end > start && isspace((unsigned char) *(end - 1)))
        end--;
    size_t len = end - start;
    char *result = palloc(len + 1);
    memcpy(result, start, len);
    result[len] = '\0';
    return result;
}

/*
 * SplitTopLevelCommas splits text on commas that are not nested inside
 * parentheses, returning a List of palloc'd, whitespace-trimmed C-string
 * chunks in left-to-right order -- used to break a SELECT/ORDER BY list's
 * text into one chunk per entry without misreading a comma inside a nested
 * function call (e.g. `atTime(t.Trip, p.Period)`) as a top-level separator.
 */
extern List *
SplitTopLevelCommas(const char *text)
{
    List *chunks = NIL;
    int depth = 0;
    const char *chunkStart = text;
    const char *p = text;
    for (; *p; p++)
    {
        if (*p == '(')
            depth++;
        else if (*p == ')')
            depth--;
        else if (*p == ',' && depth == 0)
        {
            chunks = lappend(chunks, TrimmedSubstring(chunkStart, p));
            chunkStart = p + 1;
        }
    }
    chunks = lappend(chunks, TrimmedSubstring(chunkStart, p));
    return chunks;
}

/*
 * SplitTopLevelConjuncts splits text on " and " keywords that are not
 * nested inside parentheses, the same left-to-right/whitespace-trimmed
 * chunking style as SplitTopLevelCommas but keyword- rather than
 * character-based -- mirrors FindTopLevelKeywordToken's paren-depth and
 * whitespace-boundary checks so "sandwich" or a parenthesized OR-group
 * isn't misread as (or split at) a top-level "and".
 */
extern List *
SplitTopLevelConjuncts(const char *text)
{
    List *chunks = NIL;
    int depth = 0;
    const char *chunkStart = text;
    const char *p = text;
    while (*p != '\0')
    {
        if (*p == '(')
        {
            depth++;
            p++;
        }
        else if (*p == ')')
        {
            depth--;
            p++;
        }
        else if (depth == 0 && strncmp(p, "and", 3) == 0 &&
                 (p == text || isspace((unsigned char) p[-1])) &&
                 isspace((unsigned char) p[3]))
        {
            chunks = lappend(chunks, TrimmedSubstring(chunkStart, p));
            p += 3;
            chunkStart = p;
        }
        else
        {
            p++;
        }
    }
    chunks = lappend(chunks, TrimmedSubstring(chunkStart, p));
    return chunks;
}

/*
 * extract_between returns a newly allocated copy of the substring of str
 * found strictly between markers p1 and p2, or NULL if either marker isn't
 * found (or allocation fails).
 */
extern
char * extract_between(const char *str, const char *p1, const char *p2) {
    const char *i1 = strstr(str, p1);
    if (i1 != NULL) {
        const size_t pl1 = strlen(p1);
        const char *i2 = strstr(i1 + pl1, p2);
        if (i2 != NULL) {
            /* Found both markers, extract text. */
            const size_t mlen = i2 - (i1 + pl1);
            char *ret = malloc(mlen + 1);
            if (ret != NULL) {
                memcpy(ret, i1 + pl1, mlen);
                ret[mlen] = '\0';
                return ret;
            }
        }
    }
    return NULL;
}

/* change_sentence returns a newly allocated copy of sentence with the first occurrence of find replaced by replace. */
extern char *
change_sentence (char *sentence, char *find, char *replace)
{
    char *dest = malloc (strlen(sentence)-strlen(find)+strlen(replace)+1);
    char *ptr;

    strcpy (dest, sentence);

    ptr = strstr (dest, find);
    if (ptr)
    {
        memmove (ptr+strlen(replace), ptr+strlen(find), strlen(ptr+strlen(find))+1);
        strncpy (ptr, replace, strlen(replace));
    }

    return dest;
}

/* toLower returns a newly allocated, lowercased copy of str. */
extern
char *toLower(char *str)
{
    size_t len = strlen(str);
    char *str_l = calloc(len+1, sizeof(char));

    for (size_t i = 0; i < len; ++i) {
        str_l[i] = tolower((unsigned char)str[i]);
    }
    return str_l;
}

/*
 * FindKeywordToken finds the first case-insensitive occurrence of `keyword`
 * in `lowered` that is bounded by whitespace on both sides (or string
 * start/end), returning a pointer to the start of the keyword itself. Unlike
 * a plain strstr(haystack, " keyword "), this tolerates any whitespace
 * (newlines, tabs, multiple spaces) around the keyword, not just a single
 * literal space -- multi-line-formatted SQL (e.g. a line break before a
 * keyword, routine when queries are written across several lines) silently
 * fails to match on an exact-space search. `lowered` must already be
 * lowercased; `keyword` must already be lowercase and contain no
 * leading/trailing space of its own (internal spaces, e.g. "group by", are
 * fine).
 */
extern char *
FindKeywordToken(const char *lowered, const char *keyword)
{
    size_t keywordLen = strlen(keyword);
    const char *cursor = lowered;
    while ((cursor = strstr(cursor, keyword)) != NULL)
    {
        bool precededByBoundary = (cursor == lowered) || isspace((unsigned char) cursor[-1]);
        bool followedByBoundary = isspace((unsigned char) cursor[keywordLen]);
        if (precededByBoundary && followedByBoundary)
            return (char *) cursor;
        cursor++;
    }
    return NULL;
}

/*
 * FindTopLevelKeywordToken behaves like FindKeywordToken, but only matches
 * an occurrence sitting at paren-depth 0 -- i.e. not nested inside a
 * subquery/CTE's own parenthesized definition. Needed when scanning a
 * *whole* query's text (which may itself contain nested SELECTs, e.g. a
 * CTE body) for a keyword belonging to the outermost/final SELECT, such as
 * a trailing ORDER BY/LIMIT that applies to the query as a whole.
 */
/*
 * FindIdentifierToken finds the first case-insensitive occurrence of
 * `identifier` in `lowered` bounded by non-identifier characters (or
 * string start/end) on both sides -- see the header comment for why this
 * differs from FindKeywordToken.
 */
extern char *
FindIdentifierToken(const char *lowered, const char *identifier)
{
    size_t idLen = strlen(identifier);
    const char *cursor = lowered;
    while ((cursor = strstr(cursor, identifier)) != NULL)
    {
        bool precededByBoundary = (cursor == lowered) ||
            !(isalnum((unsigned char) cursor[-1]) || cursor[-1] == '_');
        bool followedByBoundary =
            !(isalnum((unsigned char) cursor[idLen]) || cursor[idLen] == '_');
        if (precededByBoundary && followedByBoundary)
            return (char *) cursor;
        cursor++;
    }
    return NULL;
}

extern char *
FindTopLevelKeywordToken(const char *lowered, const char *keyword)
{
    size_t keywordLen = strlen(keyword);
    int depth = 0;
    const char *cursor = lowered;
    while (*cursor != '\0')
    {
        if (*cursor == '(')
            depth++;
        else if (*cursor == ')')
            depth--;
        else if (depth == 0 && strncmp(cursor, keyword, keywordLen) == 0)
        {
            bool precededByBoundary = (cursor == lowered) || isspace((unsigned char) cursor[-1]);
            bool followedByBoundary = isspace((unsigned char) cursor[keywordLen]);
            if (precededByBoundary && followedByBoundary)
                return (char *) cursor;
        }
        cursor++;
    }
    return NULL;
}

/* IsDatumEmpty reports whether val is the zero/unset Datum (i.e. no value was assigned). */
extern bool
IsDatumEmpty(Datum val)
{
    if (val == 0)
        return true;
    return false;
}