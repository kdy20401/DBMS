#ifndef __DISKBPT_H__
#define __DISKBPT_H__


#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

#include "../include/file.h"

typedef struct node
{
    pagenum_t page_num;
    struct node * next;
}node;

node * queue;

void show_free_page_list();
int open_table(char * pathname);
void close_table(void);
void init_table(int fd);

void show_leaf_page_keys();
int db_find(int64_t key, char * ret_val);
pagenum_t find_leaf_page(int64_t key);
int path_to_root(pagenum_t pagenum);
void enqueue(pagenum_t page_num);
pagenum_t dequeue();

int db_insert(int64_t key, char * value);

int db_delete(int64_t key);

void print_tree(header_page_t * header);
#endif
