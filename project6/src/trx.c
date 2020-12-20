#include "trx.h"
#include "lock_table.h"
#include "buffer.h"
#include "bpt.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

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

void acquire_trx_latch(int trx_id)
{
	trx_node * node;

	node = find_trx_node(trx_id);

	if(pthread_mutex_lock(&(node->trx_latch)) != 0)
	{
		printf("acquire_trx_latch() failed\n");
		exit(0);
	}
}

void release_trx_latch(int trx_id)
{
	trx_node * node;

	node = find_trx_node(trx_id);

	if(pthread_mutex_unlock(&(node->trx_latch)) != 0)
	{
		printf("release_trx_latch() failed\n");
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
	new_node->rollback_list = NULL;
	new_node->lastLSN = 0;
	pthread_mutex_init(&(new_node->trx_latch), NULL);
	new_node->next = NULL;

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
	int i;

	// unlike db_update(), existence check is not needed.
	// record information which is represented by this function's parameters
	// is from the rollback list of transaction node. it means that
	// the result of db_update() operation is already reflected to buffer or disk
	leaf_page_num = find_leaf_page(table_id, key);
	fptr = buf_read_page_trx(table_id, leaf_page_num, (page_t *)&leaf);

	i = search_recordIndex(&leaf, key);
	strcpy(leaf.records[i].value, org_value);

	buf_write_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
	release_page_latch(fptr);
}

void rollback(trx_node * node)
{	
	rollback_t * rb_node;
	int ret;

	rb_node = node->rollback_list; 

	while(rb_node != NULL)
	{
		rollback_db_update(rb_node->table_id, rb_node->key, rb_node->value_saved);
		rb_node = rb_node->next;
	}
}

int trx_abort(int trx_id)
{
	// printf("abort start\n");
	trx_node * node;
	lock_t *p, *q;

	// printf("find_trx_node,,\n");
	acquire_trx_manager_latch();
	node = find_trx_node(trx_id);
	release_trx_manager_latch();

	if(node == NULL)
	{
		// already aborted or error in find_trx_node()
		return 0;
	}

	// roll back all data
	// printf("rollback,,,\n");

	// the new flow of acquiring a latch
	// : acquiring a page latch -> acquiring a lock table latch
	// so doing rollback() while holding a lock table latch will incur deadlatch
	// before trx_abort(), release lock table latch and transaction manager latch
	// it doesn't matter if another transaction acquires those latches and do his work
	// because it must wait for X lock ahead of it.
	acquire_log_buffer_latch();
	
	bcrlog_t rollbackLog;
	rollbackLog.log_size = BCR_LOG_SIZE;
	rollbackLog.LSN = (logBufferTail == 0) ? 0 : logBufferTail;	
	rollbackLog.prevLSN = node->lastLSN;
	rollbackLog.trx_id = trx_id;
	rollbackLog.type = ROLLBACK;
	logBufferTail += BCR_LOG_SIZE;
	memcpy(logBuffer + (rollbackLog.LSN - flushedLSN), (void *)&rollbackLog, BCR_LOG_SIZE);
	
	release_log_buffer_latch();

	rollback(node);

	// release all locks
	p = NULL;
	q = node->head;

	acquire_lock_table_latch();
	acquire_trx_manager_latch();
	// printf("release locks,,\n");
	while(q != NULL)
	{
		p = q;
		q = q->trx_next;
		// if(p->lock_mode == SHARED)
		// {
			// printf("trx %d SHARED lock is released\n", p->trx_id);
		// }
		// else if(p->lock_mode == EXCLUSIVE)
		// {
			// printf("trx %d EXCLUSIVE lock is released\n", p->trx_id);
		// }
		lock_release(p);
	}

	// remove transaction node
	remove_from_trx_table(node);
	release_trx_manager_latch();
	release_lock_table_latch();
	
	// printf("trx_abort finished\n");
	return trx_id;
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

	acquire_log_buffer_latch();

	bcrlog_t beginLog;
	beginLog.log_size = BCR_LOG_SIZE;
	beginLog.LSN = (logBufferTail == 0) ? 0 : logBufferTail;	
	beginLog.prevLSN = node->lastLSN;
	beginLog.trx_id = trx_id;
	beginLog.type = BEGIN;
	logBufferTail += BCR_LOG_SIZE;
	memcpy(logBuffer + (beginLog.LSN - flushedLSN), (void *)&beginLog, BCR_LOG_SIZE);

	release_log_buffer_latch();

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
	// printf("trx%d commit start\n", trx_id);
	acquire_lock_table_latch();
	acquire_trx_manager_latch();

	trx_node * node;
	lock_t *p, *q;

	node = find_trx_node(trx_id);

	// the transaction node is already removed by trx_abort
	// so release all latches before return error code
	if(node == NULL)
	{
		release_trx_manager_latch();
		release_lock_table_latch();
		// printf("trx%d commit end (ABORTED)\n", trx_id);

		return 0;
	}

	p = NULL;
	q = node->head;

	while(q != NULL)
	{
		p = q;
		q = q->trx_next;
		// if(p->lock_mode == SHARED)
		// {
			// printf("trx %d SHARED lock is released\n", p->trx_id);
		// }
		// else if(p->lock_mode == EXCLUSIVE)
		// {
			// printf("trx %d EXCLUSIVE lock is released\n", p->trx_id);
		// }
		lock_release(p);
	}

	acquire_log_buffer_latch();

	bcrlog_t commitLog;
	commitLog.log_size = BCR_LOG_SIZE;
	commitLog.LSN = (logBufferTail == 0) ? 0 : logBufferTail;
	commitLog.prevLSN = node->lastLSN;
	commitLog.trx_id = trx_id;
	commitLog.type = BEGIN;
	logBufferTail += BCR_LOG_SIZE;
	memcpy(logBuffer + (commitLog.LSN - flushedLSN), (void *)&commitLog, BCR_LOG_SIZE);
	flush_log_buffer();

	release_log_buffer_latch();

	remove_from_trx_table(node);
	trx_table.trx_num--;

	release_trx_manager_latch();
	release_lock_table_latch();

    // printf("trx %d commit\n", trx_id);
	return trx_id;
}

void acquire_log_buffer_latch()
{
	if(pthread_mutex_lock(&log_buffer_latch) != 0)
	{
		printf("acquire_log_buffer_latch() failed\n");
		exit(0);
	}
}

void release_log_buffer_latch()
{
	if(pthread_mutex_unlock(&log_buffer_latch) != 0)
	{
		printf("release_log_buffer_latch() failed\n");
		exit(0);
	}
}

void flush_log_buffer()
{
	int ret;
	int log_size;

	log_size = logBufferTail - flushedLSN;

	ret = pwrite(logDB, logBuffer, log_size, flushedLSN);

	if(ret == log_size)
	{
		fsync(logDB);
	}
	else if(ret == -1)
	{
        printf("pwrite() failed\n");
        printf("flush_log_buffer() failed\n");
        printf("error: %s\n", strerror(errno));
	}

	flushedLSN += log_size;
}

// void create_log(trx_node * node, int type)
// {
// 	switch (type)
// 	{
// 		case BEGIN:
// 			bcrlog_t beginLog;
// 			beginLog.log_size = BCR_LOG_SIZE;

// 			if(logBufferTail == 0)
// 			{
// 				beginLog.LSN = 0;
// 			}
// 			else
// 			{
// 				beginLog.LSN = logBufferTail;
// 			}

// 			beginLog.prevLSN = node->lastLSN;
// 			beginLog.trx_id = trx_id;
// 			beginLog.type = BEGIN;
// 			logBufferTail += BCR_LOG_SIZE;

// 			memcpy(logBuffer + beginLog.LSN, (void *)&beginLog, BCR_LOG_SIZE);
// 			break;
// 		case UPDATE:

// 			break;
// 		case COMMIT:
// 			bcrlog_t updateLog;
// 			updateLog.log_size = BCR_LOG_SIZE;

// 			if(logBufferTail == 0)
// 			{
// 				updateLog.LSN = 0;
// 			}
// 			else
// 			{
// 				updateLog.LSN = logBufferTail;
// 			}

// 			updateLog.prevLSN = node->lastLSN;
// 			updateLog.trx_id = trx_id;
// 			updateLog.type = UPDATE;
// 			logBufferTail += BCR_LOG_SIZE;

// 			memcpy(logBuffer + updateLog.LSN, (void *)updateLog, BCR_LOG_SIZE);
// 			break;
// 		case ROLLBACK:
// 			break;
// 		case COMPENSATE:
// 			break;

// 		default:
// 			break;
// 		}
// }

int init_log(char * log_path)
{
	// allocate log buffer
	logBuffer = (char *)malloc(sizeof(LOG_BUF_SIZE));
	
	if(logBuffer == NULL)
	{
		printf("malloc() failed\n");
		printf("init_log() failed\n");
		return -1;
	}

	// initialize log manater mutex
	pthread_mutex_init(&log_buffer_latch, NULL);

	logDB = open(log_path, O_RDWR | O_CREAT, 0644);
	if(logDB == -1)
	{
		printf("open() failed\n");
		printf("init_log() failed\n");
		return -1;
	}

	logBufferTail = 0;
	flushedLSN = 0;
	return 0;
}

void analysis(char * log_msgpath)
{
	FILE * fp;
	int LSN, size, type, offset, ret, trx_id, max_trx_id;
	bcrlog_t log_header, bcrLog;
	updateLog_t updateLog;
	compensateLog_t compensateLog;

	fp = fopen(log_msgpath, "w+");
	if(fp == NULL)
	{
		printf("fopen() failed\n");
		printf("analysis() failed\n");
	}
	
	max_trx_id = -1;
	offset = 0;

	fprintf(fp, "[ANALYSIS] Analysis path start\n");

	ret = pread(logDB, (void *)&log_header, BCR_LOG_SIZE, offset);

	while(ret > 0)
	{
		type = log_header.type;

		if(type == BEGIN)
		{
			pread(logDB, (void *)&bcrLog, BCR_LOG_SIZE, offset);
			trx_id = bcrLog.trx_id;
			max_trx_id = (trx_id > max_trx_id) ? trx_id : max_trx_id;
			loser[trx_id] = true;
			offset += BCR_LOG_SIZE;

		}
		else if(type == UPDATE)
		{
			pread(logDB, (void *)&updateLog, UPDATE_LOG_SIZE, offset);
			trx_id = updateLog.trx_id;
			max_trx_id = (trx_id > max_trx_id) ? trx_id : max_trx_id;
			offset += UPDATE_LOG_SIZE;
		}
		else if(type == COMMIT)
		{
			pread(logDB, (void *)&bcrLog, BCR_LOG_SIZE, offset);
			trx_id = bcrLog.trx_id;
			max_trx_id = (trx_id > max_trx_id) ? trx_id : max_trx_id;
			loser[trx_id] = false;
			winner[trx_id] = true;
			offset += BCR_LOG_SIZE;

		}
		else if(type == ROLLBACK)
		{
			pread(logDB, (void *)&bcrLog, BCR_LOG_SIZE, offset);
			trx_id = bcrLog.trx_id;

			offset += BCR_LOG_SIZE;

		}
		else // COMPENSATE
		{
			pread(logDB, (void *)&compensateLog, COMPENSATE_LOG_SIZE, offset);
			trx_id = compensateLog.trx_id;
			offset += COMPENSATE_LOG_SIZE;
		}
		
		ret = pread(logDB, (void *)&log_header, BCR_LOG_SIZE, offset);

	}

	fprintf(fp, "[ANALYSIS] Analysis success.");
	fprintf(fp, " Winner: ");

	for(int i = 1; i <= max_trx_id; i++)
	{
		if(winner[i])
		{
			fprintf(fp, "%d ", i);
		}
	}
	fprintf(fp, ", Loser: ");

	for(int i = 1; i <= max_trx_id; i++)
	{
		if(loser[i])
		{
			fprintf(fp, "%d ", i);
		}
	}
	fprintf(fp, "\n");
}

