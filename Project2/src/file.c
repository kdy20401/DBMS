#include "file.h"
#include <stdlib.h>
#include <unistd.h>
#define FREE_PAGE_NUM 10

// first, find free pages from free page list.
// if free pages don't exist, extend the file and make new free pages
pagenum_t file_alloc_page()
{
    header_page_t * header;
    int free_page_num, page_num;

    header = (header_page_t *)malloc(sizeof(PAGE_SIZE));
    if(header == NULL)
    {
        printf("malloc() failed\n");
        exit(0);
    }
    file_read_page(0, (page_t *)header);

    free_page_num = header->free_page_num;
    page_num = header->page_num;

    if(free_page_num == 0)
    {
        //make 10 new free pages
        int i;
        free_page_t ** tmp;
        int start_free_page_num;

        start_free_page_num = page_num;
        free_page_num = page_num;

        tmp = (free_page_t **)malloc(FREE_PAGE_NUM * sizeof(PAGE_SIZE));
        if(tmp == NULL)
        {
            printf("malloc() failed\n");
            printf("file_alloc_page() failed");
            exit(0);
        }

        for(i = 0; i < FREE_PAGE_NUM; i++)
        {
            tmp[i] = (free_page_t *)malloc(sizeof(PAGE_SIZE));
            if(tmp[i] == NULL)
            {
                printf("malloc() failed\n");
                printf("file_alloc_page() failed");
                exit(0);
            }
            if(i == 9)
            {
                tmp[i]->next_free_page_num = 0;
            }
            else
            {
                tmp[i]->next_free_page_num = page_num + 1;
            }       
        }

        header->free_page_num = start_free_page_num;
        header->page_num += FREE_PAGE_NUM;
        file_write_page(0, header);

        for(i = 0; i < FREE_PAGE_NUM; i++)
        {
            file_write_page(start_free_page_num, (page_t *)tmp[i]);
            start_free_page_num++;
        }
    }
    
    //get free pages from free page list
    free_page_t * tmp;
    int next_free_page;
    
    tmp = (free_page_t *)malloc(sizeof(PAGE_SIZE));
    if(tmp == NULL)
    {
        printf("malloc() failed\n");
        printf("file_alloc_page() failed");
        exit(0);
    }
    file_read_page(free_page_num, (page_t *)tmp);
    next_free_page = tmp->next_free_page_num;
    header->free_page_num = next_free_page;
    file_write_page(0, header);
    sync();

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
    sync();
    
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
        exit(0);
    }
    sync();
}
