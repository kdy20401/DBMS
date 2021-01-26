#ifndef __DISKBPT_H__
#define __DISKBPT_H__

#include "ptype.h"

// using a queue to print the entire tree
typedef struct page
{
    pagenum_t page_num;
    struct page * next;
}page;
page * p_queue;

/* operation API with transaction */
int db_find(int table_id, int64_t key, char * ret_val, int trx_id);
int db_update(int table_id, int64_t key, char * values, int trx_id);

// find
int search_routingIndex(internal_page_t * internal, int64_t key);
int search_recordIndex(leaf_page_t * leaf, int64_t key);
pagenum_t find_leaf_page(int table_id, int64_t key);

// update
void insert_into_rollback_list(int table_id, int64_t key, char * org_value, int trx_id);

// insert
int get_left_index(int table_id, pagenum_t left_page_num);
void insert_into_internal(int table_id, internal_page_t * left, pagenum_t left_index, int key, internal_page_t * right, pagenum_t right_page_num);
void insert_into_internal_after_splitting(int table_id, internal_page_t * old, pagenum_t old_page_num, int left_index, int key, pagenum_t right_page_num);
void insert_into_parent(int table_id, internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num);
void insert_into_leaf_after_splitting(int table_id, leaf_page_t * leaf, pagenum_t leaf_page_num, int64_t key, char * value);
void insert_into_leaf(int table_id, pagenum_t leaf_page_num, int64_t key, char * value);
void insert_into_new_root(int table_id, internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num);
void start_new_tree(int table_id, int64_t key, char * value);
int db_insert(int table_id, int64_t key, char * value);

// delete
int get_neighbor_page_index(int table_id, pagenum_t page_num);
pagenum_t get_left_sibling_leaf_page_num(int table_id, pagenum_t n);
void delayed_merge(int table_id, pagenum_t parent, pagenum_t neighbor, pagenum_t n, int neighbor_index, int64_t k_prime);
void adjust_root_page(int table_id, header_page_t * header, pagenum_t root_page_num);
pagenum_t remove_entry_from_page(int table_id, pagenum_t n, int64_t key);
void delete_entry(int table_id, pagenum_t n, int64_t key); 
int db_delete(int table_id, int64_t key);

// original functinos for db_find() that do not offer transaction
pagenum_t nt_find_leaf_page(int table_id, int64_t key);
int nt_db_find(int table_id, int64_t key, char * ret_val);

// util functions
void show_leaf_page_keys(int table_id);
void show_free_page_list(int table_id);

// functions for printing disk based b+ tree
void enqueue(pagenum_t page_num);
pagenum_t dequeue();
int path_to_root(int table_id, pagenum_t pagenum);
void print_tree(int table_id);

// functions for creating and removing table
void init_table(int table_id);
int open_table(char * pathname);
int get_table_id(char * pathname);
int close_table(int table_id);

#endif