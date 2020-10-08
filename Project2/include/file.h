#include <stdint.h>

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