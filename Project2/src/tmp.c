#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "file.h"
#include "bpt.h"

// initialize datafile
void init_table(int fd)
{
    page_t * p;
    header_page_t *hp;

    hp = (header_page_t *)malloc(sizeof(header_page_t));
    hp->free_page_num = 0;
    hp->root_page_num = 0;
    hp->page_num = 1;

    p = (page_t *)hp;

    file_write_page(0, p);
}

int open_table(char * pathname)
{
    fd = open(pathname, O_RDWR | O_CREAT | O_EXCL, 0644);

    if(fd != -1)
    {
        init_table(fd);
    }
    else if((fd == -1) && (EEXIST == errno)) //data file already exists
    {
        fd = open(pathname, O_RDWR);
    }
    
    return fd;
}

int db_insert(int64_t key, char * value)
{
    int tableId;
    char * dataFile = "pathofdataFile";
    tableId = open_table(dataFile);


    close(tableId);
}

int db_find(int64_t key, char * ret_val)
{
    int tableId;
    char * dataFile = "pathofdataFile";
    tableId = open_table(dataFile);


    close(tableId);


}

int db_delete(int64_t key)
{
    int tableId;
    char * dataFile = "pathofdataFile";
    tableId = open_table(dataFile);


    close(tableId);


}