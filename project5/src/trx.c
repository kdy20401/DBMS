#include "trx.h"
#include "lock_table.h"
#include "buffer.h"
#include "bpt.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void acquire_trx_manager_latch()
{
	if(pthread_mutex_lock(&trx_manager_latch) != 0)
	{
		printf("acquire_trx_manager_latch() failed\n");
		exit(0);
	}
}

void release_trx_manager_latch()
{
	if(pthread_mutex_unlock(&trx_manager_latch) != 0)
	{
		printf("release_trx_manager_latch() failed\n");
		exit(0);
	}
}

trx_node * create_trx_node(int trx_id)
{
	trx_node * new_node;

	new_node = (trx_node *)malloc(sizeof(trx_node));
	if(new_node == NULL)
	{
		printf("malloc() failed\n");
		printf("insert_trx_table() failed\n");
		return NULL;
	}
	
	new_node->trx_id = trx_id;
	new_node->head = NULL;
	new_node->tail = NULL;
	new_node->checked = false;
	new_node->next = NULL;
	new_node->rollback_list = NULL;

	return new_node;
}

void insert_into_trx_table(trx_node * node)
{
	if(trx_table.header == NULL)
	{
		trx_table.header = node;
	}
	else
	{
		node->next = trx_table.header;
		trx_table.header = node;
	}
}

// free rollback_list of transaction node
// and remove transaction node from table
void remove_from_trx_table(trx_node * target)
{
	trx_node * prev;

	remove_rollback_list(target);

	prev = trx_table.header;

	if(prev == target)
	{
		trx_table.header = target->next;
		free(target);
		return;
	}

	while(prev->next != target)
	{
		prev = prev->next;
	}

	prev->next = target->next;
	free(target);
}

trx_node * find_trx_node(int trx_id)
{
	trx_node * target;

	target = trx_table.header;

	while(target != NULL)
	{
		if(target->trx_id == trx_id)
		{
			return target;
		}

		target = target->next;
	}

	// printf("there is no trx %d node in the table\n", trx_id);
	return NULL;
}

void insert_into_trx_lock_list(lock_t * lock_obj)
{
	trx_node * target;

	target = find_trx_node(lock_obj->trx_id);

	if(target->head == NULL && target->tail == NULL)
	{
		target->head = lock_obj;
		target->tail = lock_obj;
	}
	else
	{
		target->tail->trx_next = lock_obj;
		target->tail = lock_obj;
	}
}

void remove_rollback_list(trx_node * node)
{
	rollback_t * head;
	rollback_t * p;

	head = node->rollback_list;
	p = head;

	while(head != NULL)
	{
		p = p->next;
		free(head);
		head = p;
	}
}

void insert_into_rollback_list(int table_id, int64_t key, char * org_value, int trx_id)
{
	trx_node * node;
	rollback_t * rb_node;

	node = find_trx_node(trx_id);

	rb_node = (rollback_t *)malloc(sizeof(rollback_t));
	if(rb_node == NULL)
	{
		printf("malloc() failed");
		printf("insert_into_rollback_list() failed\n");
		exit(0);
	}

	rb_node->table_id = table_id;
	rb_node->key = key;
	strcpy(rb_node->value_saved, org_value);

	rb_node->next = node->rollback_list;
	node->rollback_list = rb_node;
}

void rollback_db_update(int table_id, int64_t key, char * org_value)
{
	// printf("rollback : key %ld original value %s in table %d\n", key, org_value, table_id);
    pagenum_t leaf_page_num;
    leaf_page_t leaf;
	frame * fptr;

	leaf_page_num = find_leaf_page(table_id, key);

	fptr = buf_read_page_trx(table_id, leaf_page_num, (page_t *)&leaf);

	for(int i = 0; i < leaf.num_key; i++)
	{
		if(leaf.records[i].key == key)
		{
			strcpy(leaf.records[i].value, org_value);
			break;
		}
	}

	buf_write_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
	release_page_latch(fptr);
}

void rollback(trx_node * node)
{	
	rollback_t * rb_node;

	rb_node = node->rollback_list;

	// acquire_buffer_latch(); -> not needed
	while(rb_node != NULL)
	{
		rollback_db_update(rb_node->table_id, rb_node->key, rb_node->value_saved);
		rb_node = rb_node->next;
	}
	// release_buffer_latch(); -> not needed
	// trx_abort(rollback ,,) is executed having lock table and transaction manager latch
	// so following transactions cannot access to dirty data
}

void trx_abort(int trx_id)
{
	// printf("abort start\n");
	trx_node * t;
	lock_t * p;
	lock_t * q;

	// printf("find_trx_node,,\n");
	t = find_trx_node(trx_id);

	// roll back all data
	// printf("rollback,,,\n");
	rollback(t);

	// release all locks
	p = NULL;
	q = t->head;

	// printf("release locks,,\n");
	while(q != NULL)
	{
		p = q;
		q = q->trx_next;
		// if(p->lock_mode == SHARED)
		// {
		// 	printf("trx %d SHARED lock is released\n", p->trx_id);
		// }
		// else if(p->lock_mode == EXCLUSIVE)
		// {
		// 	printf("trx %d EXCLUSIVE lock is released\n", p->trx_id);
		// }
		lock_release(p);
	}

	// printf("remove_from_trx_table,,,\n");
	remove_from_trx_table(t);
	// printf("trx_abort finished\n");
}

int trx_begin()
{
	acquire_trx_manager_latch();
	
	trx_node * node;
	int trx_id;

	trx_id = ++global_trx_id;
	trx_table.trx_num++;

	if(trx_id < 1)
	{
		release_trx_manager_latch();
		return 0;
	}
	
	node = create_trx_node(trx_id);
	if(node == NULL)
	{
		release_trx_manager_latch();
		return 0;
	}

	insert_into_trx_table(node);

	release_trx_manager_latch();

    // printf("trx %d begin\n", trx_id);

	return trx_id;
}

// trx_commit() modifies record lock list and transaction table.
// so should acquire lock table latch and transaction manager latch.
// follow the order to prevent deadlock : lock table latch -> transaction manager latch
// how about move acquire and release lock table latch into lock_release()?
int trx_commit(int trx_id)
{
	acquire_trx_manager_latch();
	// acquire_lock_table_latch();

	trx_node * target;
	lock_t *p, *q;

	target = find_trx_node(trx_id);
	
	// the transaction node is already removed by trx_abort
	// so release all latches before return error code
	if(target == NULL)
	{
		// release_lock_table_latch();
		release_trx_manager_latch();
		return 0;
	}

	p = NULL;
	q = target->head;

	while(q != NULL)
	{
		p = q;
		q = q->trx_next;
		lock_release(p);
	}

	remove_from_trx_table(target);
	trx_table.trx_num--;

	// release_lock_table_latch();
	release_trx_manager_latch();
    // printf("trx %d commit\n", trx_id);

	return trx_id;
}