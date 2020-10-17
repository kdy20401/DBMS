#include "file.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <errno.h>
#include <string.h>

#define FREE_PAGE_NUM 10
extern uint64_t fd;

// make a 10 free pages of free page list
// and return the first free page number
pagenum_t make_free_page(header_page_t * header)
{
    int i;
    free_page_t tmp[10];
    int start_free_page_num, page_num;

    page_num = header->page_num;
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

    header->free_page_num = start_free_page_num;
    header->page_num += FREE_PAGE_NUM;
    file_write_page(0, (page_t*)header);

    for(i = 0; i < FREE_PAGE_NUM; i++)
    {
        printf("free page(#%d) is created!!!!\n", start_free_page_num);
        file_write_page(start_free_page_num, (page_t *)&tmp[i]);
        start_free_page_num++;
    }

    return header->free_page_num;
}
// first, find free pages from free page list.
// if free pages don't exist, extend the file and make new free pages
// and allocate first free page in free page list
pagenum_t file_alloc_page()
{
    header_page_t header;
    pagenum_t free_page_num, page_num;

    file_read_page(0, (page_t *)&header);

    free_page_num = header.free_page_num; // 2
    page_num = header.page_num; // 11

    if(free_page_num == 0)
    {
        printf("there are no free pages. make a free page list,,\n");
        //make 10 new free pages
        free_page_num = make_free_page(&header);
    }
    

    printf("get free pages from free page list,,\n");
    //get free pages from free page list
    free_page_t tmp;
    int next_free_page;

    file_read_page(free_page_num, (page_t *)&tmp);
    next_free_page = tmp.next_free_page_num;
    header.free_page_num = next_free_page;
    file_write_page(0, (page_t *)&header);

    return free_page_num;
}

// make pagenum page a free page
void file_free_page(pagenum_t pagenum)
{
    free_page_t tmp;
    header_page_t header;
    pagenum_t first_free_page_num;

    file_read_page(pagenum, (page_t *)&tmp);
    file_read_page(0, (page_t *)&header);

    // when try to free the page which have already been freed.
    if(pagenum == header.free_page_num)
    {
        return;
    }

    first_free_page_num = header.free_page_num;
    tmp.next_free_page_num = first_free_page_num; // garbage data will be overwritten
    header.free_page_num = pagenum;

    file_write_page(0, (page_t *)&header);
    file_write_page(pagenum, (page_t *)&tmp);
}

// load on-disk page information into in-memory page
void file_read_page(pagenum_t pagenum, page_t * dest)
{
    int ret;

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
void file_write_page(pagenum_t pagenum, const page_t * src)
{
    int ret;

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
