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

// hash table for searching frame in buffer quickly
typedef struct bucket
{
    frame * fptr;
    struct bucket * next;
}bucket;

typedef struct hashtable
{
    int table_size;
    bucket ** directory;
}hashtable;

hashtable hash_table;

// buffer
frame ** buffer;
int frame_num;

// head and tail of LRU list
frame * head;
frame * tail;

/* buffer manager API */
pagenum_t buf_make_free_page(int table_id);
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);
void buf_read_page(int table_id, pagenum_t page_num, page_t * dest);
void buf_write_page(int table_id, pagenum_t page_num, page_t * src);
void buf_unpin_page(int table_id, pagenum_t page_num);

// functions for buffer and LRU list
int init_db(int buf_num);
int shutdown_db(void);
void init_lru_list(int buf_num);
frame * get_frame_from_lru_list();
void update_lru_list(frame * fptr);

// hash functions
void init_hash(int size);
void shutdown_hash();
int hash(pagenum_t key, int hashtable_size);
frame * hash_find(int table_id, pagenum_t key, hashtable * table);
void hash_insert(frame * fptr, hashtable * table);
void hash_delete(frame * fptr, hashtable * table);

// util functions
void show_buffer_info();
void show_lru_list();

#endif