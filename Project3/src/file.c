#include "file.h"
#include "table.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

extern table_info tables[MAX_TABLE + 1];
extern int table_num;

// make a 10 free pages of free page list
// and return the first free page number
pagenum_t file_make_free_page(int table_id)
{
    int i;
    free_page_t tmp[FREE_PAGE_NUM];
    header_page_t header;
    pagenum_t start_free_page_num, page_num;

    file_read_page(table_id, 0, (page_t*)&header);

    page_num = header.page_num;
    start_free_page_num = page_num;

    for(i = 0; i < FREE_PAGE_NUM; i++)
    {
        if(i == FREE_PAGE_NUM - 1)
        {
            tmp[i].next_free_page_num = 0;
        }
        else
        {
            tmp[i].next_free_page_num = page_num + 1;
            page_num++;
        }       
    }

    header.free_page_num = start_free_page_num;
    header.page_num += FREE_PAGE_NUM;
    file_write_page(table_id, 0, (page_t*)&header);

    for(i = 0; i < FREE_PAGE_NUM; i++)
    {
        file_write_page(table_id, start_free_page_num, (page_t *)&tmp[i]);
        printf("free page(#%ld) is created!!!!\n", start_free_page_num);
        start_free_page_num++;
    }
    return header.free_page_num;
}

// first, find free pages from free page list.
// if free pages don't exist, extend the file and make new free pages
// and allocate first free page in free page list
pagenum_t file_alloc_page(int table_id)
{
    header_page_t header;
    free_page_t tmp;
    pagenum_t free_page_num;

    file_read_page(table_id, 0, (page_t *)&header);

    free_page_num = header.free_page_num;

    if(free_page_num == 0)
    {
        printf("there are no free pages. make a free page list,,\n");
        //make 10 new free pages
        free_page_num = file_make_free_page(table_id);
    }

    printf("get free pages from free page list,,\n");

    //get free pages from free page list
    file_read_page(table_id, free_page_num, (page_t *)&tmp);

    // must read header page again! because in file_make_free_page(), header information is changed!!
    file_read_page(table_id, 0, (page_t *)&header);
    header.free_page_num = tmp.next_free_page_num;
    file_write_page(table_id, 0, (page_t *)&header);

    return free_page_num;
}

// make pagenum page a free page
void file_free_page(int table_id, pagenum_t pagenum)
{
    free_page_t tmp;
    header_page_t header;
    pagenum_t first_free_page_num;

    file_read_page(table_id, pagenum, (page_t *)&tmp);
    file_read_page(table_id, 0, (page_t *)&header);

    first_free_page_num = header.free_page_num;
    tmp.next_free_page_num = first_free_page_num; // garbage data will be overwritten
    header.free_page_num = pagenum;

    file_write_page(table_id, 0, (page_t *)&header);
    file_write_page(table_id, pagenum, (page_t *)&tmp);

}

// load on-disk page information into in-memory page
// through this function, directly access to disk to read a page
void file_read_page(int table_id, pagenum_t pagenum, page_t * dest)
{
    int ret, fd;
    
    fd = tables[table_id].fd;
    ret = pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);

    if(ret == PAGE_SIZE)
    {
        return;
    }
    else if(ret == -1)
    {
        printf("pread() failed\n");
        printf("file_read_page() failed\n");
        printf("error: %s\n", strerror(errno));
        exit(0);
    }
    else if(ret == 0)
    {
        printf("EOF\n");
        printf("file_read_page() failed\n");
        exit(0);
    }
    else
    {
        printf("read less or more than %d\n", PAGE_SIZE);
        printf("file_read_page() failed\n");
        exit(0);
    }
}

// load in-memory page information into on-disk page
// through this function, directly access to disk to write a page
void file_write_page(int table_id, pagenum_t pagenum, const page_t * src)
{
    int ret,fd;

    fd = tables[table_id].fd;
    ret = pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);

    if(ret == PAGE_SIZE)
    {
        fsync(fd);
        return;
    }
    else if(ret == -1)
    {
        printf("pwrite() failed\n");
        printf("file_write_page() failed\n");
        printf("error: %s\n", strerror(errno));
        exit(0);
    }   
}