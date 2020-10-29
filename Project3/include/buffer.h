#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "ptype.h"
#include <stdbool.h>

typedef struct frame
{
    page_t * page;
    int table_id;
    pagenum_t page_num;
    bool is_dirty;
    bool is_pinned;
    struct frame * next;
    struct frame * prev;
}frame;

typedef struct bucket
{
    // pagenum_t page_num;
    frame * fptr;
    struct bucket * next;
}bucket;

typedef struct hashtable
{
    int table_size;
    bucket ** directory;
}hashtable;

hashtable hash_table;

frame ** buffer; // buffer
int frame_num;

frame * head; // header of LRU list
frame * tail;

// hash functions
void init_hash(int size);
void shutdown_hash();
int hash(pagenum_t key, int hashtable_size);
frame * hash_find(int table_id, pagenum_t key, hashtable * table);
void hash_insert(frame * fptr, hashtable * table);
void hash_delete(frame * fptr, hashtable * table);

//buffer manager API
int init_db(int buf_num);
int shutdown_db(void);
void show_buffer_info();
void show_lru_list();
void init_lru_list(int buf_num);
frame * get_frame_from_lru_list();
void update_lru_list(frame * fptr);

pagenum_t buf_make_free_page(int table_id);
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);
void buf_read_page(int table_id, pagenum_t page_num, page_t * dest);
void buf_write_page(int table_id, pagenum_t page_num, page_t * src);
void buf_unpin_page(int table_id, pagenum_t page_num);

#endif