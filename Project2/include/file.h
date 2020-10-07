#include <stdint.h>

typedef uint64_t pagenum_t;

typedef struct page_t {
    pagenum_t pagenum;
}page_t;

uint64_t fd;

pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t * dest);
void file_write_page(pagenum_t pagenum, const page_t * src);