#include <stdint.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "file.h"
#include "bpt.h"

void init_table(int fd)
{
    page_t p;
    int header_page_num;

    header_page_num = file_alloc_page();
    p.pagenum = header_page_num;
    //initialize p's element..
    
    file_write_page(header_page_num, &p);
}

int open_table(char * pathname)
{
    int fd;

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