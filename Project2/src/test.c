#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

typedef uint64_t pagenum_t;

#define HEADER 0
#define FREE 1
#define INTERNAL 2
#define LEAF 3
#define PAGE_SIZE 4096

//leaf record
typedef struct record1 {
    int64_t key;
    char value[120];
}record1;

//internal record(?)
typedef struct record2 {
    int64_t key;
    pagenum_t pagenum;
}record2;

typedef struct page_t {
}page_t;

typedef struct {
    page_t p;
    pagenum_t free_page_num;
    pagenum_t root_page_num;
    pagenum_t page_num;
    int reserved[1018];
}header_page_t;

typedef struct {
    page_t p;
    pagenum_t next_free_page_num;
    int reserved[1022];
}free_page_t;

typedef struct {
    page_t p;
    pagenum_t parent_page_num;
    uint32_t isLeaf;
    uint32_t num_key;
    int reserved[26];
    pagenum_t leftdownmost_page_num;
    record2 records[248];
}internal_page_t;

typedef struct {
    page_t p;
    pagenum_t parent_page_num;
    uint32_t isLeaf;
    uint32_t num_key;
    int reserved[26];
    pagenum_t right_sibling_page_num;
    record1 records[31];
}leaf_page_t;


extern uint64_t fd;

pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t * dest);
void file_write_page(pagenum_t pagenum, const page_t * src);
void read_page(page_t * dest, int fd);



int main(void)
{
    printf("107\n");
    
    free_page_t * fp = (free_page_t *)malloc(sizeof(PAGE_SIZE));
    if(fp == NULL)
    {
        printf("malloc() failed\n");
    }
    fp->next_free_page_num = 5;

    page_t * p = (page_t *)malloc(sizeof(PAGE_SIZE));
    if(p == NULL)
    {
        printf("malloc() failed\n");
    }

    // free_page_t * p = (free_page_t *)malloc(sizeof(PAGE_SIZE));
    // if(p == NULL)
    // {
        // printf("malloc() failed\n");
    // }

    int fd, ret;
    fd = open("test1.bin", O_RDWR | O_CREAT, 0644);
    
    if(fd == -1)
    {
        printf("open() failed\n");
    }
    if((ret = write(fd, fp, PAGE_SIZE)) == -1)
    {
        printf("write() failed\n");
    }

    read_page(p, fd);
    // read_page((page_t *)p, fd);

    printf("free page num : %ld\n", ((free_page_t *)p)->next_free_page_num);
    // printf("free page num : %ld\n", p->next_free_page_num);

    close(fd);
    free(fp);
    free(p);



    // 
    // fp = (free_page_t *)malloc(sizeof(free_page_t));
    // fp->next_free_page_num = 4;

    // t = (page_t *)fp;
    // printf("next free page = %ld\n", ((free_page_t *)t)->next_free_page_num);

    return 0;
}

void read_page(page_t * dest, int fd)
{
    // printf("107\n");
    int ret;

    ret = lseek(fd, 0, SEEK_SET);

    if((ret = read(fd, dest, PAGE_SIZE)) == -1) //?
    {
        printf("read() failed\n");
    }
}