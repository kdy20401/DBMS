#ifndef __PTYPE_H__
#define __PTYPE_H__

#include <stdint.h>

#define PAGE_SIZE 4096
#define MAX_RECORD 31
#define MAX_ENTRY 248

// type for representing page number
typedef uint64_t pagenum_t;

// leaf record
typedef struct record
{
    int64_t key;
    char value[120];
}record;

//internal entry
typedef struct entry
{
    int64_t key;
    pagenum_t pagenum;
}entry;

// like a parent class
typedef struct page_t
{

}page_t;

typedef struct
{
    page_t p;
    pagenum_t free_page_num;
    pagenum_t root_page_num;
    pagenum_t page_num;
    int reserved[1018];
}header_page_t;

typedef struct
{
    page_t p;
    pagenum_t next_free_page_num;
    int reserved[1022];
}free_page_t;

typedef struct
{
    page_t p;
    pagenum_t parent_page_num;
    uint32_t isLeaf;
    uint32_t num_key;
    int reserved[26];
    pagenum_t leftmostdown_page_num;
    entry entries[MAX_ENTRY];
    // entry entries[MAX_ENTRY];
    // entry entries_reserved[248 - MAX_ENTRY];
}internal_page_t;

typedef struct
{
    page_t p;
    pagenum_t parent_page_num;
    uint32_t isLeaf;
    uint32_t num_key;
    int reserved[26];
    pagenum_t right_sibling_page_num;
    record records[MAX_RECORD];
    // record records[MAX_RECORD];
    // record records_reserved[31 - MAX_RECORD];
}leaf_page_t;

#endif