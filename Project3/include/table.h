#ifndef __TABLE_H__
#define __TABLE_H__

#include <stdbool.h>

#define MAX_TABLE 10
#define NAME_LENGTH 20

typedef struct table_info
{
    int fd;
    char pathname[NAME_LENGTH + 1];
    bool is_opened;
}table_info;

#endif