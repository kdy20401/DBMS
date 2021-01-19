#ifndef _FILE_H_
#define _FILE_H_

#include <stdint.h>

/* FILE MANAGER */
#define FREE_PAGE_NUM 10
uint64_t fd;

// file manager API
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t * dest);
void file_write_page(pagenum_t pagenum, const page_t * src);

pagenum_t make_free_page(header_page_t * header); // helper function for file_alloc_page()

/* PAGE TYPE*/
#define PAGE_SIZE 4096
#define MAX_RECORD 31
#define MAX_ENTRY 248

typedef uint64_t pagenum_t;


//leaf record
typedef struct record {
    int64_t key;
    char value[120];
}record;

//internal entry
typedef struct entry {
    int64_t key;
    pagenum_t pagenum;
}entry;

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
    pagenum_t leftmostdown_page_num;
    entry entries[248];
}internal_page_t;

typedef struct {
    page_t p;
    pagenum_t parent_page_num;
    uint32_t isLeaf;
    uint32_t num_key;
    int reserved[26];
    pagenum_t right_sibling_page_num;
    record records[31];
}leaf_page_t;

#endif