#ifndef __FILE_H__
#define __FILE_H__

#include "ptype.h"

#define FREE_PAGE_NUM 10 // when free page doesn't exist, make 10 new free pages

pagenum_t file_alloc_page(int table_id);
pagenum_t file_make_free_page(int table_id);
void file_free_page(int table_id, pagenum_t pagenum);
void file_read_page(int table_id, pagenum_t pagenum, page_t * dest);
void file_write_page(int table_id, pagenum_t pagenum, const page_t * src);

#endif