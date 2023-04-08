#include "general/helper_functions.h"

extern
char* replaceWord( char* s,  char* oldW,  char* newW)
{
    char bstr[strlen(s)];
    memset(bstr,0,sizeof(bstr));
    int i;
    int occurance = 0;
    for(i = 0;i < strlen(s);i++){
        if(!strncmp(s+i,oldW,strlen(oldW)) && occurance == 0){
            strcat(bstr,newW);
            occurance++;
            i += strlen(oldW) - 1;
        }else{
            strncat(bstr,s + i,1);
        }
    }

    strcpy(s,bstr);
    return s;
}

extern
char * extract_between(const char *str, const char *p1, const char *p2) {
    const char *i1 = strstr(str, p1);
    if (i1 != NULL) {
        const size_t pl1 = strlen(p1);
        const char *i2 = strstr(i1 + pl1, p2);
        if (p2 != NULL) {
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
}

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
