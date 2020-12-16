#include "trx.h"
#include "lock_table.h"
#include "buffer.h"
#include "bpt.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

void acquire_lock_table_latch()
{
	if(pthread_mutex_lock(&lock_table_latch) != 0)
	{
		printf("pthread_mutex_lock() failed\n");
		exit(0);
	}
}

void release_lock_table_latch()
{
	if(pthread_mutex_unlock(&lock_table_latch) != 0)
	{
		printf("pthread_mutex_unlock() failed\n");
		exit(0);
	}
}

int init_lock_table()
{
	// 1. initialize lock table
	lock_table.table_size = LOCK_TABLE_SIZE;
	lock_table.directory = (lt_bucket **)malloc(sizeof(lt_bucket *) * lock_table.table_size);

	if(lock_table.directory == NULL)
	{
		printf("malloc() failed.\n");
		printf("init_lock_table() failed.\n");
		return -1;
	}

	for(int i = 0; i < lock_table.table_size; i++)
	{
		lock_table.directory[i] = (lt_bucket *)malloc(sizeof(lt_bucket));

		if(lock_table.directory[i] == NULL)
		{
			printf("malloc() failed\n");
			printf("init_lock_table() failed\n");
			return -1;
		}

		// make dummy buckets
		lock_table.directory[i]->table_id = -1;
		lock_table.directory[i]->key = -1;
		lock_table.directory[i]->head = NULL;
		lock_table.directory[i]->tail = NULL;
		lock_table.directory[i]->next = NULL;
	}

	// 2. initialize lock table latch
	pthread_mutex_init(&lock_table_latch, NULL);

	// 3. initialize transaction table and latch
	trx_table.header = NULL;
	trx_table.trx_num = 0;
	pthread_mutex_init(&trx_manager_latch, NULL);
	global_trx_id = 0;

	return 0;
}

lock_t * create_lock(lt_bucket * sentinel, int trx_id, int lock_mode)
{
	lock_t * lock_obj = (lock_t *)malloc(sizeof(lock_t));

	if(lock_obj == NULL)
	{
		printf("malloc() failed\n");
		printf("create_lock() failed\n");
		return NULL;
	}

	if(pthread_cond_init(&(lock_obj->cond), NULL) != 0)
	{
		printf("pthread_cond_init() failed\n");
		return NULL;
	}
	lock_obj->sentinel = sentinel;
	lock_obj->prev = NULL;
	lock_obj->next = NULL;
	lock_obj->trx_id = trx_id;
	lock_obj->lock_mode = lock_mode;
	lock_obj->trx_next = NULL;
	lock_obj->status = 0;

	return lock_obj;
}

void insert_into_record_lock_list(lt_bucket * sentinel, lock_t * lock_obj)
{
	lock_t * pred;

	// if record lock list is empty
	if(sentinel->head == NULL && sentinel->tail == NULL)
	{
		sentinel->head = lock_obj;
		sentinel->tail = lock_obj;
		lock_obj->status = WORKING;
		return;
	}

	// arrange record lock list
	lock_obj->prev = sentinel->tail;
	sentinel->tail->next = lock_obj;
	sentinel->tail = lock_obj;
	pred = lock_obj->prev;


	// // when one transaction can execute only one opration(db_find() or db_update()) to one record
	// if(pred->status == WAITING)
	// {
	// 	lock_obj->status = WAITING;
	// }
	// else if(pred->status == WORKING)
	// {
	// 	if(pred->lock_mode == SHARED && lock_obj->lock_mode == SHARED)
	// 	{
	// 		lock_obj->status = WORKING;
	// 	}
	// 	else
	// 	{
	// 		lock_obj->status = WAITING;
	// 	}
	// }

	// // when one transaction can execute two or more operations(db_find() or db_update()) to one record
	// if(pred->status == WAITING)
	// {
	// 	lock_obj->status = WAITING;
	// }
	// else
	// {
	// 	if(pred->lock_mode == EXCLUSIVE)
	// 	{
	// 		if(pred->trx_id == lock_obj->trx_id)
	// 		{
	// 			lock_obj->status = WORKING;
	// 		}
	// 		else
	// 		{
	// 			lock_obj->status = WAITING;
	// 		}
	// 	}
	// 	else
	// 	{
	// 		if(pred->trx_id == lock_obj->trx_id)
	// 		{
	// 			if(lock_obj->lock_mode == SHARED)
	// 			{
	// 				lock_obj->status = WORKING;
	// 			}
	// 			else
	// 			{
	// 				while(pred != NULL)
	// 				{
	// 					if(pred->trx_id != lock_obj->trx_id)
	// 					{
	// 						lock_obj->status = WAITING;
	// 						break;
	// 					}
	// 					pred = pred->prev;
	// 				}
	// 				if(pred == NULL)
	// 				{
	// 					lock_obj->status = WORKING;
	// 				}
	// 			}
	// 		}
	// 		else
	// 		{
	// 			if(lock_obj->lock_mode == SHARED)
	// 			{
	// 				while(pred != NULL)
	// 				{
	// 					if(pred->lock_mode == EXCLUSIVE)
	// 					{
	// 						lock_obj->status = WAITING;
	// 						break;
	// 					}
	// 					pred = pred->prev;
	// 				}

	// 				if(pred == NULL)
	// 				{
	// 					lock_obj->status = WORKING;
	// 				}
	// 			}
	// 			else
	// 			{
	// 				lock_obj->status = WAITING;
	// 			}
	// 		}
	// 	}
	// }

	// original code used to test project6
	if(pred->status == WAITING)
	{
		lock_obj->status = WAITING;
	}
	else if(pred->status == WORKING)
	{
		if(pred->trx_id == lock_obj->trx_id)
		{
			lock_obj->status = WORKING;	
		}
		else if(lock_obj->lock_mode == SHARED)
		{
			// when lock_obj's transaction is different from predecessor's transactioin
			// the only case lock_obj can acquire a lock directly is when all predecessor locks are S locks
			// simultaneously, lock_obj is S lock 
			while(pred != NULL)
			{
				if(pred->lock_mode == EXCLUSIVE)
				{
					lock_obj->status = WAITING;
					return;
				}
				pred = pred->prev;
			}
			lock_obj->status = WORKING;
		}
		else if(lock_obj->lock_mode == EXCLUSIVE)
		{
			lock_obj->status = WAITING;
		}
	}
}

int lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode, lock_t ** ret_lock)
{
	acquire_lock_table_latch();

	lt_bucket * b;
	lock_t * lockObj;
	trx_node * node;

	b = lock_table_find(table_id, key);

	// if bucket doesn't exist in hash table
	if(b == NULL)
	{
		// make a new bucket and add to hash table
		b = lock_table_insert(table_id, key);
	}

	// create a lock object, add to record lock list and decide whether wait or work
	lockObj = create_lock(b, trx_id, lock_mode);
	insert_into_record_lock_list(b, lockObj);

	acquire_trx_manager_latch();
	insert_into_trx_lock_list(lockObj);
	*ret_lock = lockObj;

	// acquired a lock!
	if(lockObj->status == WORKING)
	{	
		release_lock_table_latch();
		release_trx_manager_latch();
		return ACQUIRED;
	}
	// waiting for acquiring a lock
	else if(lockObj->status == WAITING)
	{
		if(is_deadlock(lockObj))
		{
			release_lock_table_latch();
			release_trx_manager_latch();
			return DEADLOCK;
		}
		else
		{
			// before releasing latches(page_latch, trx_manager, lock_table) acquire trx_latch
			node = find_trx_node(lockObj->trx_id);
			pthread_mutex_lock(&(node->trx_latch));
			return NEED_TO_WAIT;
			// after return, lock_wait() will be called
		}		
	}
}

void lock_wait(lock_t * lock_obj)
{
	trx_node * node;
	
	node = find_trx_node(lock_obj->trx_id);
	release_lock_table_latch();
	release_trx_manager_latch();
	// printf("trx %d's lock attached to key %ld in table %d goes to sleep for acquiring a lock,,,,,,,\n",
		// lock_obj->trx_id, lock_obj->sentinel->key, lock_obj->sentinel->table_id);

	pthread_cond_wait(&(lock_obj->cond), &(node->trx_latch));
	
	// after wake up, release transaction latch
	pthread_mutex_unlock(&(node->trx_latch));
	lock_obj->status = WORKING;
}

int lock_release(lock_t * lock_obj)
{
	lt_bucket * sentinel;
	lock_t *succ, *succsucc;
	int succ_trx_id, flag;

	sentinel = lock_obj->sentinel;

	// arrange record lock list
	if(sentinel->head == lock_obj && sentinel->tail == lock_obj)
	{
		sentinel->head = NULL;
		sentinel->tail = NULL;
	}
	else if(sentinel->head == lock_obj)
	{
		sentinel->head = lock_obj->next;
		sentinel->head->prev = NULL;
	}
	else if(sentinel->tail == lock_obj)
	{
		sentinel->tail = lock_obj->prev;
		sentinel->tail->next = NULL;
	}
	else
	{
		lock_obj->prev->next = lock_obj->next;
		lock_obj->next->prev = lock_obj->prev;
	}

	// // when one transaction can execute one opration(db_find() or db_update()) to one record
	// pred = lock_obj->prev;
	// succ = lock_obj->next;

	// if(pred == NULL && succ != NULL)
	// {
	// 	if(succ->lock_mode == EXCLUSIVE)
	// 	{
	// 		acquire_trx_latch(succ->trx_id);
	// 		pthread_cond_signal(&(succ->cond));
	// 		release_trx_latch(succ->trx_id);
	// 	}
	// 	else if(succ->lock_mode == SHARED && succ->status == WAITING)
	// 	{
	// 		while(succ != NULL)
	// 		{
	// 			if(succ->lock_mode == SHARED)
	// 			{
	// 				acquire_trx_latch(succ->trx_id);
	// 				pthread_cond_signal(&(succ->cond));
	// 				release_trx_latch(succ->trx_id);
	// 			}
	// 			else
	// 			{
	// 				break;
	// 			}
	// 			succ = succ->next;
	// 		}
	// 	}
	// }

	// when one transaction can execute two or more operations(db_find() or db_update()) to one record
	succ = lock_obj->next;
	if(succ == NULL)
	{
		free(lock_obj);
		return 0;
	}

	if(succ->lock_mode == SHARED)
	{
		if(lock_obj->prev == NULL)
		{
			if(succ->status == WAITING)
			{
				while(succ != NULL)
				{
					if(succ->lock_mode == SHARED)
					{
						acquire_trx_latch(succ->trx_id);
						pthread_cond_signal(&(succ->cond));
						release_trx_latch(succ->trx_id);
					}
					else
					{
						break;
					}
					succ = succ->next;
				}
			}
			else
			{
				lock_t * succsucc;
				succ_trx_id = succ->trx_id;
				succsucc = succ->next;

				while(succsucc != NULL && succsucc->trx_id == succ->trx_id)
				{
					if(succsucc->lock_mode == EXCLUSIVE)
					{
						acquire_trx_latch(succsucc->trx_id);
						pthread_cond_signal(&(succsucc->cond));
						release_trx_latch(succsucc->trx_id);
						break;
					}
					succsucc = succsucc->next;
				}
			}
		}
	}
	else if(succ->lock_mode == EXCLUSIVE && succ->status == WAITING)
	{
		if(lock_obj->prev == NULL)
		{
			acquire_trx_latch(succ->trx_id);
			pthread_cond_signal(&(succ->cond));
			release_trx_latch(succ->trx_id);
		}
		else
		{
			if(lock_obj->prev->trx_id == succ->trx_id)
			{
				if(lock_obj->prev->status == WORKING && lock_obj->status == WORKING && succ->status == WAITING)
				{
					acquire_trx_latch(succ->trx_id);
					pthread_cond_signal(&(succ->cond));
					release_trx_latch(succ->trx_id);
				}
			}
		}
	}

    // // explanation of the condition after ||
	// // basically, if both predecessor and successor exist, don't wakes up the successor
	// // because the predecessor will wakes it up. however, by strict two phase locking,
	// // the situation where predecessor cannot wake up the successor exists.
	// // which is, both predecessor and successor belong to the same transaction
	
	// flag = 0;

	// if(lock_obj->next != NULL)
	// {
	// 	if((lock_obj->prev == NULL) || ((lock_obj->prev != NULL) && (lock_obj->prev->trx_id == lock_obj->next->trx_id)))
	// 	{
	// 		succ = lock_obj->next;
	// 		succ_trx_id = succ->trx_id;

	// 		// wakes up all sleeping record locks
	// 		// idea1 : regardless of transaction, can wake up all S locks in a row
	// 		// idea2 : can wake up all S locks or X locks in a row belong to one transaction
	// 		if(succ->lock_mode == EXCLUSIVE)
	// 		{
	// 			while(succ != NULL && succ->status == WAITING)
	// 			{
	// 				if(succ->trx_id == succ_trx_id)
	// 				{
	// 					// printf("wake up trx %d's lock attached to key %ld in table %d\n", succ->trx_id, succ->sentinel->key, succ->sentinel->table_id);
	// 					acquire_trx_latch(succ->trx_id);
	// 					pthread_cond_signal(&(succ->cond));
	// 					release_trx_latch(succ->trx_id);
	// 				}
	// 				else
	// 				{
	// 					break;
	// 				}
	// 				succ = succ->next;
	// 			}
	// 		}
	// 		else if(succ->lock_mode == SHARED)
	// 		{
	// 			// wakes up all successive S locks
	// 			// if those S locks' transaction is not one, flag is set to 1
	// 			while(succ != NULL && succ->status == WAITING && succ->lock_mode == SHARED)
	// 			{
	// 				// printf("wake up trx %d's lock attached to key %ld in table %d\n", succ->trx_id, succ->sentinel->key, succ->sentinel->table_id);
	// 				acquire_trx_latch(succ->trx_id);
	// 				pthread_cond_signal(&(succ->cond));
	// 				release_trx_latch(succ->trx_id);

	// 				if(succ->trx_id != succ_trx_id)
	// 				{
	// 					flag = 1;
	// 				}
	// 				succ = succ->next;
	// 			}

	// 			// idea2
	// 			if((succ != NULL) && (succ->lock_mode == EXCLUSIVE) && (flag == 0))
	// 			{
	// 				while(succ != NULL)
	// 				{

	// 					if(succ->trx_id == succ_trx_id)
	// 					{
	// 						// printf("wake up trx %d's lock attached to key %ld in table %d\n", succ->trx_id, succ->sentinel->key, succ->sentinel->table_id);
	// 						acquire_trx_latch(succ->trx_id);
	// 						pthread_cond_signal(&(succ->cond));
	// 						release_trx_latch(succ->trx_id);
	// 					}
	// 					else
	// 					{
	// 						break;
	// 					}
	// 					succ = succ->next;
	// 				}
	// 			}
	// 		}
	// 	}
	// }
	// // if not, nothing to do. predecessor will wake up successor

	free(lock_obj);
	return 0;
}

int lock_table_hash(int table_id, int64_t key)
{
	return ((table_id + 1) * key) % lock_table.table_size;
}

lt_bucket * lock_table_find(int table_id, int64_t key)
{
	lt_bucket * p;

	p = lock_table.directory[lock_table_hash(table_id, key)]; //dummy
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

lt_bucket * lock_table_insert(int table_id, int64_t key)
{
	lt_bucket * dummy;
	lt_bucket * new_bucket;

	dummy = lock_table.directory[lock_table_hash(table_id, key)];
	new_bucket = (lt_bucket *)malloc(sizeof(lt_bucket));

	if(new_bucket == NULL)
	{
		printf("malloc() failed\n");
		printf("lock_table_insert() failed\n");
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

void init_wait_queue()
{
	w_queue.front = NULL;
	w_queue.rear = NULL;
}

void w_enqueue(int trx_id)
{
	node * new_node;

	new_node = (node *)malloc(sizeof(node));
	if(new_node == NULL)
	{
		printf("enqueue() failed\n");
		exit(0);
	}
	new_node->trx_id = trx_id;
	new_node->next = NULL;

	if(w_queue.front == NULL)
	{
		w_queue.front = new_node;
		w_queue.rear = new_node;
	}
	else
	{
		w_queue.rear->next = new_node;
		w_queue.rear = new_node;
	}	
}

int w_dequeue()
{
	node * tmp_node;
	int tmp_value;

	if(w_queue.front == NULL)
	{
		return -1;
	}
	else
	{
		tmp_node = w_queue.front;
		tmp_value = tmp_node->trx_id;
		w_queue.front = tmp_node->next;
		free(tmp_node);
		return tmp_value;	
	}
}

void destroy_wait_queue()
{
	node * p;
	node * q;

	p = NULL;
	q = w_queue.front;

	while(q != NULL)
	{
		p = q;
		q = q->next;
		free(p);
	}

	w_queue.front = NULL;
	w_queue.rear = NULL;
}

void uncheck_trx_nodes()
{
	trx_node * node;

	node = trx_table.header;

	while(node != NULL)
	{
		node->checked = false;
		node = node->next;
	}
}

bool is_deadlock(lock_t * suspect_lock)
{
	int start_trx_id;
	int trx_id;
	int checked_trx_num;
	int total_trx_num;
	trx_node * node;
	lock_t * lock;
	lock_t * prev_lock;

	checked_trx_num = 0;
	total_trx_num = trx_table.trx_num;
	start_trx_id = suspect_lock->trx_id;

	init_wait_queue();
	prev_lock = suspect_lock->prev;
	
	// insert transactions to queue which start_trx waits for
	while(prev_lock != NULL)
	{	
		if(prev_lock->trx_id != start_trx_id)
		{
			// printf("enqueue trx %d\n", prev_lock->trx_id);
			w_enqueue(prev_lock->trx_id);
		}
		prev_lock = prev_lock->prev;
	}

	// printf("lock object of trx %d, let's traverse wait-for graph!\n", suspect_lock->trx_id);
	while((trx_id = w_dequeue()) != -1)
	{
		// if checked all transaction except suspect transaction 
		if(checked_trx_num == total_trx_num - 1)
		{
			break;
		}

		node = find_trx_node(trx_id);

		if(node->checked == true)
		{
			continue;
		}

		checked_trx_num++;
		node->checked = true;
		lock = node->head;

		while(lock != NULL)
		{
			if(lock->status == WAITING)
			{
				prev_lock = lock->prev;

				while(prev_lock != NULL)
				{
					if(prev_lock->trx_id == start_trx_id)
					{
						// printf("trx %d incurs deadlock!!!!!!\n", suspect_lock->trx_id);
						uncheck_trx_nodes();
						destroy_wait_queue();
						return true;
					}
					else if(prev_lock->trx_id != lock->trx_id)
					{
						w_enqueue(prev_lock->trx_id);
					}
					prev_lock = prev_lock->prev;
				}
			}
			lock = lock->trx_next;
		}
	}
	// printf("deadlock not detected,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,\n");

	uncheck_trx_nodes();

	return false;
}

