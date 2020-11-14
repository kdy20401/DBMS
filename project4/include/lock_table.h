#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>

#define LOCK_TABLE_SIZE 11

typedef struct bucket bucket;
typedef struct lock_t lock_t;

struct lock_t {
	bucket * sentinel;
	pthread_cond_t cond;
	lock_t * prev;
	lock_t * next;
};

struct bucket
{
    int table_id;
    int64_t key;
    lock_t * head;
    lock_t * tail;
    struct bucket * next;
};

typedef struct lock_table_t
{
    bucket ** directory;
    int table_size;
}lock_table_t;

pthread_mutex_t lock_table_latch;
lock_table_t lock_table;

int hash(int table_id, int64_t key);
bucket * hash_find(int table_id, int64_t key);
bucket * hash_insert(int table_id, int64_t key);

/* APIs for lock table */
int init_lock_table();
lock_t * create_lock(bucket * sentinel);
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
