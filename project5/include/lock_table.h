#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>

#define LOCK_TABLE_SIZE 111

// lock mode
#define SHARED 0
#define EXCLUSIVE 1

#define WAITING 1
#define WORKING 2

typedef struct node
{
    int trx_id;
    struct node * next;
}node;

typedef struct queue
{
	node * front;
	node * rear;
}queue;

queue w_queue; // for finding a deadlock

typedef struct lt_bucket lt_bucket;
typedef struct lock_t lock_t;

struct lock_t
{
	lt_bucket * sentinel;
	pthread_cond_t cond;
	lock_t * prev;
	lock_t * next;
    int lock_mode;
    int trx_id;
    lock_t * trx_next;
    int status;
};

struct lt_bucket
{
    int table_id;
    int64_t key;
    lock_t * head;
    lock_t * tail;
    struct lt_bucket * next;
};

typedef struct lock_table_t
{
    lt_bucket ** directory;
    int table_size;
}lock_table_t;

pthread_mutex_t lock_table_latch;
lock_table_t lock_table;

void acquire_lock_table_latch();
void release_lock_table_latch();


int init_lock_table();
lock_t * create_lock(lt_bucket * sentinel, int trx_id, int lock_mode);
void insert_into_record_lock_list(lt_bucket * sentinel, lock_t * lock_obj);
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t * lock_obj);

/* lock table is implemented by hash table */
int lock_table_hash(int table_id, int64_t key);
lt_bucket * lock_table_find(int table_id, int64_t key);
lt_bucket * lock_table_insert(int table_id, int64_t key);

/* APIs for detecting a deadlock */
void init_wait_queue();
void w_enqueue(int trx_id);
int w_dequeue();
void destroy_wait_queue();
void uncheck_trx_nodes();
bool is_deadlock(lock_t * suspect_lock);



#endif /* __LOCK_TABLE_H__ */
