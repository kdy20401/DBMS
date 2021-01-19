#ifndef __DISKBPT_H__
#define __DISKBPT_H__

#include "file.h"

/* DB Operation API */
int open_table(char * pathname);
int db_find(int64_t key, char * ret_val);
int db_insert(int64_t key, char * value);
int db_delete(int64_t key);

// page type used for queue to traverse a whole tree
typedef struct node
{
    pagenum_t page_num;
    struct node * next;
}node;
node * queue;

void show_leaf_page_keys();
void show_free_page_list();

void enqueue(pagenum_t page_num);
pagenum_t dequeue();
int path_to_root(pagenum_t pagenum);
void print_tree(void);

void close_table(void);
void init_table(int fd);

// helper function for db_find() 
pagenum_t find_leaf_page(int64_t key);

// helper function for db_insert() 
int get_left_index(pagenum_t left_page_num);
void insert_into_internal(internal_page_t * left, pagenum_t left_index, int key, internal_page_t * right, pagenum_t right_page_num);
void insert_into_internal_after_splitting(internal_page_t * old, pagenum_t old_page_num, int left_index, int key, pagenum_t right_page_num);
void insert_into_parent(internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num);
void insert_into_leaf_after_splitting(leaf_page_t * leaf, pagenum_t leaf_page_num, int64_t key, char * value);
void insert_into_leaf(pagenum_t leaf_page_num, int64_t key, char * value);
void insert_into_new_root(internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num);
void start_new_tree(int64_t key, char * value);

// helper function for db_delete() 
int get_neighbor_page_index(pagenum_t page_num);
pagenum_t get_left_sibling_leaf_page_num(pagenum_t n);
void delayed_merge(pagenum_t parent, pagenum_t neighbor, pagenum_t n, int neighbor_index, int64_t k_prime);
void adjust_root_page(header_page_t * header, pagenum_t root_page_num);
pagenum_t remove_entry_from_page(pagenum_t n, int64_t key);
void delete_entry(pagenum_t n, int64_t key);

#endif
