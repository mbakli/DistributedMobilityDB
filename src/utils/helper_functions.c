#include "utils/helper_functions.h"

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

extern bool
IsDatumEmpty(Datum val)
{
    if (val == 0)
        return true;
    return false;
}