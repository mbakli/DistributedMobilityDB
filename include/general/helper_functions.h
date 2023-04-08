#ifndef HELPER_FUNCTIONS_H
#define HELPER_FUNCTIONS_H
#include "postgres.h"
#include <nodes/pg_list.h>

extern List * ListSpatiotemporalOperations(Node * equals);
extern char* replaceWord( char* s,  char* oldW,  char* newW);
extern char * extract_between(const char *str, const char *p1, const char *p2);
extern char *toLower(char *str);
#endif /* HELPER_FUNCTIONS_H */
