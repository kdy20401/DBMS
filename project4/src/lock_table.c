#include <lock_table.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

int hash(int table_id, int64_t key)
{
	return ((table_id + 1) * key) % lock_table.table_size;
}

bucket * hash_find(int table_id, int64_t key)
{
	bucket * p;

	p = lock_table.directory[hash(table_id, key)]; //dummy
	p = p->next;

	while(p != NULL)
	{
		if(p->table_id == table_id && p->key == key)
		{
			break;
		}
		p = p->next;
	}

	return p;
}

bucket * hash_insert(int table_id, int64_t key)
{
	bucket * dummy;
	bucket * new_bucket;

	dummy = lock_table.directory[hash(table_id, key)];
	new_bucket = (bucket *)malloc(sizeof(bucket));

	if(new_bucket == NULL)
	{
		// printf("malloc() failed\n");
		// printf("hash_insert() failed\n");
		return NULL;
	}

	new_bucket->table_id = table_id;
	new_bucket->key = key;
	new_bucket->head = NULL;
	new_bucket->tail = NULL;
	new_bucket->next = dummy->next;
	dummy->next = new_bucket;
	
	return new_bucket;
}

int init_lock_table()
{
	// 1. initialize lock table
	lock_table.table_size = LOCK_TABLE_SIZE;
	lock_table.directory = (bucket **)malloc(sizeof(bucket *) * lock_table.table_size);

	if(lock_table.directory == NULL)
	{
		// printf("malloc() failed.\n");
		// printf("init_lock_table() failed.\n");
		return -1;
	}

	for(int i = 0; i < lock_table.table_size; i++)
	{
		lock_table.directory[i] = (bucket *)malloc(sizeof(bucket));

		if(lock_table.directory[i] == NULL)
		{
			// printf("malloc() failed\n");
			// printf("init_lock_table() failed\n");
			return -1;
		}

		// make dummy buckets
		lock_table.directory[i]->table_id = -1;
		lock_table.directory[i]->key = -1;
		lock_table.directory[i]->head = NULL;
		lock_table.directory[i]->tail = NULL;
		lock_table.directory[i]->next = NULL;
	}

	// 2. initialize mutex
	pthread_mutex_init(&lock_table_latch, NULL);

	return 0;
}

lock_t * create_lock(bucket * sentinel)
{
	lock_t * lock_obj = (lock_t *)malloc(sizeof(lock_t));

	if(lock_obj == NULL)
	{
		// printf("malloc() failed\n");
		// printf("lock_acquire() failed\n");
		return NULL;
	}

	if(pthread_cond_init(&(lock_obj->cond), NULL) != 0)
	{
		// printf("pthread_cond_init() failed\n");
		return NULL;
	}
	lock_obj->sentinel = sentinel;
	lock_obj->prev = NULL;
	lock_obj->next = NULL;

	return lock_obj;
}

lock_t * lock_acquire(int table_id, int64_t key)
{
	bucket * p;
	lock_t * lock_obj;

	if(pthread_mutex_lock(&lock_table_latch) != 0)
	{
		return NULL;
	}
	// printf("thread %ld tries to ACQUIRE lock on (%d, %ld)\n", pthread_self() % 10, table_id, key);

	p = hash_find(table_id, key);

	// if bucket doesn't exist in hash table
	if(p == NULL)
	{
		// make a new bucket and add to hash table
		p = hash_insert(table_id, key);
		if(p == NULL)
		{
			return NULL;
		}
	}

	// create a lock object
	lock_obj = create_lock(p);
	if(lock_obj == NULL)
	{
		return NULL;
	}

	if(p->head == NULL)
	{
		p->head = lock_obj;
		p->tail = lock_obj;
	}
	else
	{
		lock_obj->prev = p->tail;
		p->tail->next = lock_obj;
		p->tail = lock_obj;

		// printf("thread %ld at (%d, %ld) goes to sleep,,,,\n", pthread_self() % 10, table_id, key);
		if(pthread_cond_wait(&(lock_obj->cond), &lock_table_latch) != 0)
		{
			// printf("pthread_cond_wait() failed\n");
			return NULL;
		}
		// printf("thread %ld sleeping at (%d, %ld) wakes up!!!\n", pthread_self() % 10, table_id, key);		
	}

	if(pthread_mutex_unlock(&lock_table_latch) != 0)
	{
		return NULL;
	}
	// printf("thread %ld ACQUIRE lock on (%d, %ld) successfully!\n", pthread_self() % 10, table_id, key);
	return lock_obj;
}

int lock_release(lock_t * lock_obj)
{
	if(pthread_mutex_lock(&lock_table_latch) != 0)
	{
		return -1;
	}
	
	// printf("thread %ld tries to RELEASE lock on (%d, %ld)\n", pthread_self() % 10, lock_obj->sentinel->table_id, lock_obj->sentinel->key);
	lock_obj->sentinel->head = lock_obj->next;

	if(lock_obj->sentinel->head == NULL)
	{
		lock_obj->sentinel->tail = NULL;
	}
	else
	{
		// printf("sending wake up signal~\n");
		if(pthread_cond_signal(&(lock_obj->next->cond)) != 0)
		{
			// printf("pthread_cond_signal() failed\n");
			return -1;
		}
	}
	// printf("thread %ld RELEASE lock on (%d, %ld) successfully!\n", pthread_self() % 10, lock_obj->sentinel->table_id, lock_obj->sentinel->key);
	free(lock_obj);

	if(pthread_mutex_unlock(&lock_table_latch) != 0)
	{
		return -1;
	}

	return 0;
}
