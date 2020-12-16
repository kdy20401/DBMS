#ifndef __TRX_H__
#define __TRX_H__

#include "lock_table.h"
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>

typedef struct rollback_t
{
    int table_id;
    int64_t key;
    char value_saved[120];
    struct rollback_t * next;
}rollback_t;

typedef struct trx_node
{
    int trx_id;
    bool checked;
    lock_t * head;
    lock_t * tail;
    rollback_t * rollback_list;
    pthread_mutex_t trx_latch;
    struct trx_node * next;
}trx_node;

typedef struct trx_table
{
    trx_node * header;
    int trx_num;
}trx_table_t;

pthread_mutex_t trx_manager_latch;
trx_table_t trx_table;

int global_trx_id;

void acquire_trx_manager_latch();
void release_trx_manager_latch();
void acquire_trx_latch(int trx_id);
void release_trx_latch(int trx_id);

trx_node * create_trx_node(int trx_id);
void insert_into_trx_table(trx_node * node);
void remove_from_trx_table(trx_node * target);
trx_node * find_trx_node(int trx_id);
void insert_into_trx_lock_list(lock_t * lock_obj);

void remove_rollback_list(trx_node * node);
void insert_into_rollback_list(int table_id, int64_t key, char * org_value, int trx_id);
void rollback_db_update(int table_id, int64_t key, char * org_value);
void rollback(trx_node * node);

/* transaction APIs */
int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);

#endif /* __TRX_H__ */
