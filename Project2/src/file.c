#include "../include/file.h"
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
        printf("free page(#%d) is created!\n", start_free_page_num);
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

    // header = (header_page_t *)malloc(sizeof(PAGE_SIZE));
    // if(header == NULL)
    // {
        // printf("malloc() failed\n");
        // exit(0);
    // }
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
    
    // tmp = (free_page_t *)malloc(sizeof(PAGE_SIZE));
    // if(tmp == NULL)
    // {
        // printf("malloc() failed\n");
        // printf("file_alloc_page() failed");
        // exit(0);
    // }

    file_read_page(free_page_num, (page_t *)&tmp); //tmp->2
    next_free_page = tmp.next_free_page_num; //next_free_page=3
    header.free_page_num = next_free_page; // 3
    file_write_page(0, (page_t *)&header);

    //for test
    file_read_page(0, (page_t *)&header);
    //

    return free_page_num;
}

// make pagenum page a free page
void file_free_page(pagenum_t pagenum)
{
    page_t * tmp; //contains the page which will become a free page
    header_page_t * head;
    int first_free_page_num;

    tmp = (page_t *)malloc(sizeof(PAGE_SIZE));
    if(tmp == NULL)
    {
        printf("malloc() failed\n");
        printf("file_free_page() failed\n");
        exit(0);
    }
    head = (header_page_t *)malloc(sizeof(PAGE_SIZE));
    if(head == NULL)
    {
        printf("malloc() failed\n");
        printf("file_free_page() failed\n");
        exit(0);
    }

    file_read_page(pagenum, tmp);
    file_read_page(0, (page_t *)head);

    first_free_page_num = head->free_page_num;
    ((free_page_t *)tmp)->next_free_page_num = first_free_page_num; // use memset for erasing garbage data?
    head->free_page_num = pagenum;

    file_write_page(0, (page_t *)head);
    file_write_page(pagenum, tmp);
    
    free(tmp);
    free(head);
}

// load on-disk page information into in-memory page
void file_read_page(pagenum_t pagenum, page_t * dest)
{
    int ret;

    if((ret = pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE)) == -1)
    {
        printf("pread() failed\n");
        printf("file_read_page() failed\n");
        printf("error: %s\n", strerror(errno));
        exit(0);
    }
}

// load in-memory page information into on-disk page
void file_write_page(pagenum_t pagenum, const page_t * src)
{
    int ret;

    if((ret = pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE)) == -1)
    {
        printf("pwrite() failed\n");
        printf("file_write_page() failed\n");
        printf("error: %s\n", strerror(errno));
        exit(0);
    }   
}
