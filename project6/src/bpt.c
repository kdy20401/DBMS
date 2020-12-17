
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice, 
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice, 
 *  this list of conditions and the following disclaimer in the documentation 
 *  and/or other materials provided with the distribution.
 
 *  3. Neither the name of the copyright holder nor the names of its 
 *  contributors may be used to endorse or promote products derived from this 
 *  software without specific prior written permission.
 
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 *  POSSIBILITY OF SUCH DAMAGE.
 
 *  Author:  Amittai Aviram 
 *    http://www.amittai.com
 *    amittai.aviram@gmail.edu or afa13@columbia.edu
 *  Original Date:  26 June 2010
 *  Last modified: 17 June 2016
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *  
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 * 
 * 
 */

/*
 * diskbpt.c which is a disk-based b+ tree is based on the bpt.c
 * above annotations contains information of bpt.c (e.g. copyright,,) 
*/
#include "table.h"

#include "trx.h"
#include "lock_table.h"
#include "buffer.h"
#include "bpt.h"
#include "file.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

table_info tables[MAX_TABLE + 1]; // to maintain unique table id
int table_num; // the number of table opened at execution time

// initialize table
// make header page and make free page list
void init_table(int table_id)
{
    header_page_t header;

    header.free_page_num = 0;
    header.root_page_num = 0;
    header.page_num = 1;
    buf_write_page(table_id, 0, (page_t *)&header);
    buf_make_free_page(table_id);
}

// return the unique table id corresponding pathname
// when open the data faile again, change the fd
// binary search?
int get_table_id(char * pathname)
{
    int i;

    for(i = 1; i <= MAX_TABLE; i++)
    {
        if(strcmp(tables[i].pathname, pathname) == 0)
        {
            return i;
        }
    }

    // this is the case of opening an existing datfile created at before program execution time
    if(i == MAX_TABLE + 1)
    {
        return -1;
    }
}

// after opening a table, return it's unique table id
int open_table(char * pathname)
{
    int fd, len, table_id;
    char str[3]; // str = {'some character', '\0', '\0'}

    if(strlen(pathname) > 20)
    {
        printf("data file's name is too long! it should be less than or equal to 20.\n");
        return -1;
    }

    if(table_num == MAX_TABLE)
    {
        return - 1;
    }

    strncpy(str, pathname + 4, strlen(pathname) - 4);
    table_id = atoi(str);
    
    if(table_id > 10 || table_id < 1)
    {
        return - 1;
    }

    fd = open(pathname, O_RDWR | O_CREAT | O_EXCL, 0644);
    
    // when data file is newly created
    if(fd != -1)
    {
        table_num++;
        tables[table_id].fd = fd;
        tables[table_id].is_opened = true;
        strcpy(tables[table_id].pathname, pathname);
        init_table(table_id);
    }
    // when data file already exists
    else if(fd == -1 && EEXIST == errno)
    {
        if(tables[table_id].is_opened)
        {
            return -1;
        }

        table_num++;
        fd = open(pathname, O_RDWR);
        tables[table_id].fd = fd;
        tables[table_id].is_opened = true;
        // printf("datafile (table id = %d) is opened!\n", table_id);
    }
    else
    {
        return -1;
    }
    
    return table_id;
}

// todo : how to effectively implement -> use one more hash table?
// before closing table, flush all dirty pages in this table to disk
int close_table(int table_id)
{
    // printf("close table!!\n");
    if(table_id > table_num)
    {
        // printf("table with id %d has never been created!\n", table_id);
        return -1;
    }

    if(tables[table_id].is_opened == false)
    {
        // printf("table with id %d is already closed!\n", table_id);
        return -1;
    }

    int i;
    for(i = 0; i < frame_num; i++)
    {
        if(buffer[i]->table_id == table_id)
        {
            if(buffer[i]->is_dirty)
            {
                file_write_page(table_id, buffer[i]->page_num, buffer[i]->page);
                buffer[i]->is_dirty = false;
            }
        }
    }

    tables[table_id].is_opened = false;
    close(tables[table_id].fd);
    table_num--;
    return 0;
}

void show_leaf_page_keys(int table_id)
{
    header_page_t header;
    internal_page_t internal_page;
    leaf_page_t leaf_page;
    int i;
    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);
    
    if(header.root_page_num == 0)
    {
        printf("Empty tree.\n");
        return ;
    }

    buf_read_page(table_id, header.root_page_num, (page_t *)&internal_page);
    buf_unpin_page(table_id, header.root_page_num);

    if(!!internal_page.isLeaf)
    {
        buf_read_page(table_id, header.root_page_num, (page_t *)&leaf_page);
        buf_unpin_page(table_id, header.root_page_num);

        for(i = 0; i < leaf_page.num_key; i++)
        {
            printf("%ld ", leaf_page.records[i].key);
        }
        return ;
    }
    else
    {
        pagenum_t next_page_num;

        // find left most leaf page
        while(internal_page.isLeaf != 1)
        {
            next_page_num = internal_page.leftmostdown_page_num;
            buf_read_page(table_id, next_page_num, (page_t *)&internal_page);
            buf_unpin_page(table_id, next_page_num);
        }

        // until right sibling page doesn't exist, print keys and follow link
        while(next_page_num != 0)
        {
            buf_read_page(table_id, next_page_num, (page_t *)&leaf_page);
            buf_unpin_page(table_id, next_page_num);
            printf("page(%ld):", next_page_num);
            for(i = 0; i < leaf_page.num_key; i++)
            {
                printf("%ld ", leaf_page.records[i].key);
            }
            printf("| ");
            next_page_num = leaf_page.right_sibling_page_num;
        }
        return;
    }
    
}
void show_free_page_list(int table_id)
{
    header_page_t header;
    free_page_t free;
    pagenum_t first, next;

    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);

    first = header.free_page_num;
    next = first;

    printf("free page list : %ld", first);
    while(next != 0)
    {
        buf_read_page(table_id, next, (page_t *)&free);
        buf_unpin_page(table_id, next);
        next = free.next_free_page_num;
        printf("% ld", next);
    }
    printf("\n");
}

void enqueue(pagenum_t page_num)
{
    page * new_page;
    page * c;

    new_page = (page *)malloc(sizeof(page));
    if(new_page == NULL)
    {
        printf("malloc() failed\n");
        printf("enqueue() failed\n");
        exit(0);
    }
    new_page->page_num = page_num;

    if(p_queue == NULL)
    {
        p_queue = new_page;
        p_queue->next = NULL;
    }
    else
    {
        c = p_queue;
        while(c->next != NULL)
        {
            c = c->next;
        }
        c->next = new_page;
        new_page->next = NULL;
    }
}

pagenum_t dequeue()
{
    int page_num;

    if(p_queue == NULL)
    {
        return -1;
    }

    page * n = p_queue;
    p_queue = p_queue->next;
    page_num = n->page_num;
    free(n);
    return page_num;
}


int path_to_root(int table_id, pagenum_t pagenum)
{
    int length = 0;
    pagenum_t parent_page_num;
    internal_page_t internal;

    parent_page_num = pagenum;

    // root itself
    if(parent_page_num == 0)
    {
        return length;
    }
    else
    {
        while(parent_page_num != 0)
        {
            buf_read_page(table_id, parent_page_num, (page_t *)&internal);
            buf_unpin_page(table_id, parent_page_num);
            parent_page_num = internal.parent_page_num;
            length++;
        }
        return length;
    }
}

void print_tree(int table_id)
{
    pagenum_t n;
    internal_page_t tmp;
    internal_page_t parent;
    leaf_page_t tmp1;
    header_page_t header;

    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);

    int i = 0;
    int rank = 0;
    int new_rank = 0;

    if(header.root_page_num == 0)
    {
        printf("Empty tree.\n");
        return;
    }

    p_queue = NULL;
    enqueue(header.root_page_num);

    while(p_queue != NULL)
    {
        n = dequeue();
        buf_read_page(table_id, n, (page_t *)&tmp);
        buf_unpin_page(table_id, n);
        
        if(!!tmp.isLeaf) //leaf page -> read page again using leaf page struct
        {
            buf_read_page(table_id, n, (page_t *)&tmp1);
            buf_unpin_page(table_id, n);

            if(tmp1.parent_page_num != 0)
            {
                buf_read_page(table_id, tmp1.parent_page_num, (page_t *)&parent);
                buf_unpin_page(table_id, tmp1.parent_page_num);

                if(n == parent.leftmostdown_page_num)
                {
                    new_rank = path_to_root(table_id, tmp1.parent_page_num);
                    if(new_rank != rank)
                    {
                        rank = new_rank;
                        printf("\n");
                    }
                }
            }
            for(i = 0; i < tmp1.num_key; i++)
            {
                printf("%ld ", tmp1.records[i].key);
            }
            printf("| ");
        }
        else // internal page
        {
            //when n is the left most internal page(excpet root)
            if (tmp.parent_page_num != 0)
            {
                buf_read_page(table_id, tmp.parent_page_num, (page_t *)&parent);
                buf_unpin_page(table_id, tmp.parent_page_num);

                if(n == parent.leftmostdown_page_num)
                {   
                    new_rank = path_to_root(table_id, tmp.parent_page_num);
                    if (new_rank != rank)
                    {
                        rank = new_rank;
                        printf("\n");
                    }
                }
            }

            for (i = 0; i < tmp.num_key; i++)
            {
                printf("%ld ", tmp.entries[i].key);
            }
            
            enqueue(tmp.leftmostdown_page_num);
            for (i = 0; i < tmp.num_key; i++)
            {
                enqueue(tmp.entries[i].pagenum);
            }
            printf("| ");
        }
    }
}

// find a possible leaf page containing a key
// if there is a possible leaf page, return the leaf page num
// else return -1 
pagenum_t nt_find_leaf_page(int table_id, int64_t key)
{   
    // printf("finding a leaf page,,,\n");
    header_page_t header;
    internal_page_t root;
    pagenum_t root_page_num, leaf_page_num, next;
    int i;

    buf_read_page(table_id, 0, (page_t *)&header);

    // there is no root
    if(header.root_page_num == 0)
    {
        // printf("there is no root\n");
        return -1;
    }

    root_page_num = header.root_page_num; 
    buf_read_page(table_id, root_page_num, (page_t *)&root);
    
    // root == leaf
    if(!!(root.isLeaf))
    {
        leaf_page_num = root_page_num;
    }
    else
    {
        while(!(root.isLeaf))
        {
            if(key < root.entries[0].key)
            {
                next = root.leftmostdown_page_num;
                buf_read_page(table_id, next, (page_t *)&root);
                continue;
            }

            i = 0;
            while(i < root.num_key && root.entries[i].key <= key)
            {
                i++;
            }
            next = root.entries[--i].pagenum;
            buf_read_page(table_id, next, (page_t *)&root);
        }

        leaf_page_num = next;
    }
    return leaf_page_num;
}

// select index k when traversing B+ tree to find target key if
// entries[k].key <= target key < entries[k + 1].key
int search_routingIndex(internal_page_t * internal, int64_t key)
{
    int left, mid, right, num_key;

    num_key = internal->num_key;
    left = 0;
    right = num_key - 1;

    while(left <= right)
    {
        mid = (left + right) / 2;

        if(key < internal->entries[mid].key)
        {
            right = mid - 1;
        }
        else
        {
            // when key can be found by following the right most index
            if(mid + 1 >= internal->num_key)
            {
                return mid;
            }

            if(key < internal->entries[mid + 1].key)
            {
                return mid;
            }
            else
            {
                left = mid + 1;
            }
        }
    }

    // leftmost_down page_num
    if(left > right)
    {
        return -1;
    }
}

int search_recordIndex(leaf_page_t * leaf, int64_t key)
{
    int left, mid, right;

    left = 0;
    right = leaf->num_key - 1;

    while(left <= right)
    {
        mid = (left + right) / 2;

        if(key == leaf->records[mid].key)
        {
            return mid;
        }
        else if(key < leaf->records[mid].key)
        {
            right = mid - 1;
        }
        else
        {
            left = mid + 1;
        }
    }

    return -1;
}

// while traversing B+ tree index to find leaf page,
// protect buffer(e.g., LRU list) by acquiring buffer latch - page latch
// and releasing buffer latch - page latch
pagenum_t find_leaf_page(int table_id, int64_t key)
{   
    // printf("finding a leaf page,,,\n");
    header_page_t header;
    internal_page_t root;
    pagenum_t root_page_num, leaf_page_num, next;
    int i;
    frame * fptr;

    fptr = buf_read_page_trx(table_id, 0, (page_t *)&header);
    release_page_latch(fptr);

    // there is no root
    if(header.root_page_num == 0)
    {
        // printf("there is no root\n");
        return -1;
    }

    root_page_num = header.root_page_num;
    fptr = buf_read_page_trx(table_id, root_page_num, (page_t *)&root);
    release_page_latch(fptr);

    // root == leaf
    if(!!(root.isLeaf))
    {
        leaf_page_num = root_page_num;
    }
    else
    {
        while(!(root.isLeaf))
        {
            i = search_routingIndex(&root, key);
            if(i == -1)
            {
                next = root.leftmostdown_page_num;
            }
            else
            {
                next = root.entries[i].pagenum;
            }
            fptr = buf_read_page_trx(table_id, next, (page_t *)&root);
            release_page_latch(fptr);
        }
        leaf_page_num = next;
    }
    return leaf_page_num;
}

// db_find() which does not provide transaction
int nt_db_find(int table_id, int64_t key, char * ret_val)
{
    leaf_page_t leaf;
    pagenum_t leaf_page_num;
    int i;

    leaf_page_num = nt_find_leaf_page(table_id, key);

    if(leaf_page_num == -1)
    {
        return -1;
    }

    buf_read_page(table_id, leaf_page_num, (page_t *)&leaf);

    for(i = 0; i < leaf.num_key; i++)
    {
        if(leaf.records[i].key == key)
        {   
            strcpy(ret_val, leaf.records[i].value);
            return 0;
        }
    }
    if(i == leaf.num_key)
    {
        return -1;
    }
}

int db_find(int table_id, int64_t key, char * ret_val, int trx_id)
{
    // printf("trx %d tries to find key %ld in table %d\n", trx_id, key, table_id);
    pagenum_t leaf_page_num;
    leaf_page_t leaf;
    lock_t * record_lock;
    frame * fptr;
    int i, ret;

    // find a leaf page number containing the target record
    leaf_page_num = find_leaf_page(table_id, key);

    // there is no root page
    if(leaf_page_num == -1)
    {
        // transaction who call this function must be aborted
        return -1;
    }

    // read page to check whether target record exists.
    // while holding a page latch, check if the target record exists
    fptr = buf_read_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
    i = search_recordIndex(&leaf, key);

    // there is no record with key
    if(i == -1)
    {
        // transaction who call this function must be aborted
        release_page_latch(fptr);
        return -1;
    }

    // after identifying the existence of target record, try to acquire a record lock holding a page latch
    // because in general case, the record can be moved to another page by structure modification
    ret = lock_acquire(table_id, key, trx_id, SHARED, &record_lock);

    if(ret == ACQUIRED)
    {
        // leaf page was already loaded to memory to check the existence of target record.
        // in this process, page latch is acquired. after, the record lock is directly acquired
        // without waiting. so just use that leaf page because the target record is still in that page.

    	// if(record_lock->lock_mode == SHARED)
		// {
			// printf("trx %d acquired a S lock!!\n", trx_id);
		// }
		// else
		// {
			// printf("trx %d acquired a X lock!!\n", trx_id);
		// }
        release_trx_manager_latch();
        strcpy(ret_val, leaf.records[i].value);
        release_page_latch(fptr);
    }
    else if(ret == DEADLOCK)
    {
        // in general design, thread doesn't release page latch before going to acquire a record lock.
	    // there is a special situation when thread1 who is going to acquire buffer latch to 
	    // rollback record changes. if, another thread2 exists who already acquired buffer latch
	    // and is waiting for acquiring the page latch (acquired by thread1),
	    // thread1 cannot acquire a buffer latch because thread2 is holding it.
	    // so must release page latch before trx_abort()
        release_page_latch(fptr);
        trx_abort(trx_id);
        // printf("trx %d's find() is aborted at key %ld in table %d\n", trx_id, key, table_id);
        return -1;
    }
    else if(ret == NEED_TO_WAIT)
    {
        // printf("trx %d fails to acquire a lock,, get ready to sleep,,\n", record_lock->trx_id);

        // after acquired transaction latch, release all latches and go to sleep
        release_page_latch(fptr);
        lock_wait(record_lock);

	    // if(record_lock->lock_mode == SHARED)
	    // {
	    	// printf("trx %d wakes up and acquired a S lock!\n", trx_id);
	    // }
	    // else
	    // {
	    	// printf("trx %d wakes up and acquired a X lock!\n", trx_id);
	    // }

        // after wakes up and acquires a record lock simultaneously,
        // find a page again because the page might have been evicted from buffer pool.
        // (in general situation, the record might have been moved to another page
        // because of structure modification. so should retraverse index)
        fptr = buf_read_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
        i = search_recordIndex(&leaf, key);
        strcpy(ret_val, leaf.records[i].value);
        release_page_latch(fptr);
    }

    // printf("trx %d find success at key %ld, value %s in table %d\n", trx_id, key, ret_val, table_id);
    return 0;
}


int db_update(int table_id, int64_t key, char * values, int trx_id)
{
    // printf("trx %d tries to update key %ld in table %d\n", trx_id, key, table_id);
    pagenum_t leaf_page_num;
    leaf_page_t leaf;
    frame * fptr;
    lock_t * record_lock;
    trx_node * node;
    char org_value[120];
    int i, ret;

    // find a leaf page number containing the target record
    leaf_page_num = find_leaf_page(table_id, key);

    if(leaf_page_num == -1)
    {
        // transaction who call this function must be aborted
        return -1;
    }

    // read page to check whether target record exists
    // while holding a page latch, check if the target record exists
    fptr = buf_read_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
    i = search_recordIndex(&leaf, key);

    if(i == -1)
    {
        release_page_latch(fptr);
        return -1;
    }

    // after finding the target record, try to acquire a record lock while holding a page latch
    // because in general case, the record can be moved to another page by structure modification
    ret = lock_acquire(table_id, key, trx_id, EXCLUSIVE, &record_lock);

    if(ret == ACQUIRED)
    {

        // write log buffer ahead 
        acquire_log_buffer_latch();

        updateLog_t updateLog;
	    updateLog.log_size = UPDATE_LOG_SIZE;
	    updateLog.LSN = (logBufferTail == 0) ? 0 : logBufferTail;
        node = find_trx_node(trx_id);
        release_trx_manager_latch();
	    updateLog.prevLSN = node->lastLSN;
	    updateLog.trx_id = trx_id;
	    updateLog.type = UPDATE;
        updateLog.table_id = table_id;
        updateLog.pagenum = leaf_page_num;
        updateLog.offset = ((leaf_page_num * PAGE_SIZE) + (128 + 4 * i + 8)) % PAGE_SIZE;
        updateLog.dataLength = 120;
        strcpy(updateLog.oldImage, leaf.records[i].value);
        strcpy(updateLog.newImage, values);
	    logBufferTail += UPDATE_LOG_SIZE;
	    memcpy(logBuffer + (updateLog.LSN - flushedLSN), (void *)&updateLog, UPDATE_LOG_SIZE);
	    
        release_log_buffer_latch();

        // leaf page was already loaded to memory to check the existence of target record.
        // in this process, page latch is acquired. after, the record lock is directly acquired
        // without waiting. so just use that leaf page because the target record is still in that page.

        // if(record_lock->lock_mode == SHARED)
		// {
			// printf("trx %d acquired a S lock!!\n", trx_id);
		// }
		// else if(record_lock->lock_mode == EXCLUSIVE)
		// {
			// printf("trx %d acquired a X lock!!\n", trx_id);
		// }

        strcpy(org_value, leaf.records[i].value); 
        strcpy(leaf.records[i].value, values);
        buf_write_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
        release_page_latch(fptr);
    }
    else if(ret == DEADLOCK)
    {
        // in general design, thread doesn't release page latch before going to acquire a record lock.
	    // there is a special situation when thread1 who is going to acquire buffer latch to 
	    // rollback record changes. if, another thread2 exists who already acquired buffer latch
	    // and is waiting for acquiring the page latch (acquired by thread1),
	    // thread1 cannot acquire a buffer latch because thread2 is holding it.
	    // so must release page latch before trx_abort()
        release_page_latch(fptr);
        trx_abort(trx_id);
        // printf("trx %d's update() is aborted at key %ld in table %d\n", trx_id, key, table_id);
        return -1;
    }
    else if(ret == NEED_TO_WAIT)
    {
        // printf("trx %d fails to acquire a lock,, get ready to sleep,,\n", record_lock->trx_id);

        // after acquired transaction latch, release all latches and go to sleep
        release_page_latch(fptr);
        lock_wait(record_lock);

        // if(record_lock->lock_mode == SHARED)
	    // {
	    	// printf("trx %d wakes up and acquired a S lock!\n", trx_id);
	    // }
	    // else if(record_lock->lock_mode == EXCLUSIVE)
	    // {
	    	// printf("trx %d wakes up and acquired a X lock!\n", trx_id);
	    // }

        // after wakes up and acquires a record lock simultaneously,
        // find a page again because the page might have been evicted from buffer pool.
        // (in general situation, the record might have been moved to another page
        // because of structure modification. so should retraverse index)
        fptr = buf_read_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
        i = search_recordIndex(&leaf, key);

        acquire_trx_manager_latch();
        acquire_log_buffer_latch();

        updateLog_t updateLog;
	    updateLog.log_size = UPDATE_LOG_SIZE;
	    updateLog.LSN = (logBufferTail == 0) ? 0 : logBufferTail;
        node = find_trx_node(trx_id);
        release_trx_manager_latch();
	    updateLog.prevLSN = node->lastLSN;
	    updateLog.trx_id = trx_id;
	    updateLog.type = UPDATE;
        updateLog.table_id = table_id;
        updateLog.pagenum = leaf_page_num;
        updateLog.offset = ((leaf_page_num * PAGE_SIZE) + (128 + 4 * i + 8)) % PAGE_SIZE;
        updateLog.dataLength = 120;
        strcpy(updateLog.oldImage, leaf.records[i].value);
        strcpy(updateLog.newImage, values);
	    logBufferTail += UPDATE_LOG_SIZE;
	    memcpy(logBuffer + (updateLog.LSN - flushedLSN), (void *)&updateLog, UPDATE_LOG_SIZE);
	    
        release_log_buffer_latch();

        strcpy(org_value, leaf.records[i].value); 
        strcpy(leaf.records[i].value, values);
        buf_write_page_trx(table_id, leaf_page_num, (page_t *)&leaf);
        release_page_latch(fptr);
    }

    // save original value of record to transaction node in transaction table
    acquire_trx_manager_latch();
    insert_into_rollback_list(table_id, key, org_value, trx_id);
    release_trx_manager_latch();

    // printf("trx %d update() success at key %ld value to %s in table %d\n", trx_id, key, values, table_id);
    return 0;
}

void start_new_tree(int table_id, int64_t key, char * value)
{
    leaf_page_t root;
    header_page_t header;
    pagenum_t root_page_num;

    root.isLeaf = 1;
    root.num_key = 1;
    root.parent_page_num = 0;
    root.records[0].key = key;
    strcpy(root.records[0].value, value);
    
    root.right_sibling_page_num = 0;

    // printf("allocating page,,\n");
    root_page_num = buf_alloc_page(table_id);

    buf_read_page(table_id, 0, (page_t *)&header); // why not to just write? -> overwrite problem
    buf_unpin_page(table_id, 0);
    header.root_page_num = root_page_num;
    buf_write_page(table_id, 0, (page_t *)&header);

    buf_write_page(table_id, root_page_num, (page_t *)&root);
    // printf("root is written in page %ld\n", header.root_page_num);
}

void insert_into_leaf(int table_id, pagenum_t leaf_page_num, int64_t key, char * value)
{
    leaf_page_t leaf;
    int i, insertion_point;

    buf_read_page(table_id, leaf_page_num, (page_t *)&leaf);
    buf_unpin_page(table_id, leaf_page_num);

    // printf("keys in the leaf :\n");
    // for(i = 0; i < leaf.num_key; i++)
    // {
    //     printf("%ld ", leaf.records[i].key);
    // }
    // printf("\n");

    insertion_point = 0;
    while(insertion_point < leaf.num_key && leaf.records[insertion_point].key < key)
    {
        insertion_point++;
    }

    for(i = leaf.num_key; i > insertion_point; i--)
    {
        leaf.records[i].key = leaf.records[i - 1].key;
        strcpy(leaf.records[i].value, leaf.records[i - 1].value);
    }
    leaf.records[insertion_point].key = key;
    strcpy(leaf.records[insertion_point].value, value);
    leaf.num_key++;


    // printf("keys in the leaf after inserting :\n");
    // for(int i = 0; i < leaf.num_key; i++)
    // {
    //     printf("%ld ", leaf.records[i].key);
    // }
    // printf("\n");

    buf_write_page(table_id, leaf_page_num, (page_t *)&leaf);
}

void insert_into_internal_after_splitting(int table_id, internal_page_t * old, pagenum_t old_page_num, int left_index, int key, pagenum_t right_page_num)
{
    int i, j, split, k_prime;
    internal_page_t new;
    pagenum_t new_page_num;
    entry tmp[MAX_ENTRY + 1];

    // create tmp array which contains key and pagenum in order,
    // including new key and pagenum
    for(i = 0, j = 0; i < old->num_key; i++, j++)
    {
        if(j == left_index + 1)
        {
            j++;
        }
        tmp[j].key = old->entries[i].key;
        tmp[j].pagenum = old->entries[i].pagenum;
    }
    tmp[left_index + 1].key = key;
    tmp[left_index + 1].pagenum = right_page_num;


    // split tmp in tmp[0~split-1], tmp[split], tmp[split+1 ~ 248]
    // as a result,     old          k_prime          new

    split = (MAX_ENTRY + 1) / 2;
    new.isLeaf = 0;
    new.num_key = 0;
    new.parent_page_num = old->parent_page_num;
    new.leftmostdown_page_num = tmp[split].pagenum;

    old->num_key = 0;
    for(i = 0; i < split; i++)
    {
        old->entries[i].key = tmp[i].key;
        old->entries[i].pagenum = tmp[i].pagenum;
        old->num_key++;
    }
    k_prime = tmp[split].key;
    // when splitting in internal page, unlike in leaf page, raise the middle key(k_prime) up and
    // do not leave the middle key in the new page so, ++i
    for(++i, j = 0; i < MAX_ENTRY + 1; i++, j++) 
    {
        new.entries[j].key = tmp[i].key;
        new.entries[j].pagenum = tmp[i].pagenum;
        new.num_key++;
    }

    // change the parent_page_num of new page's children pages
    // because their parent changed from old to new
    pagenum_t tmp_page_num;
    internal_page_t tmp_page;

    new_page_num = buf_alloc_page(table_id);

    buf_read_page(table_id, new.leftmostdown_page_num, (page_t *)&tmp_page);
    buf_unpin_page(table_id, new.leftmostdown_page_num);
    tmp_page.parent_page_num = new_page_num;
    buf_write_page(table_id, new.leftmostdown_page_num, (page_t *)&tmp_page);

    for(i = 0; i < new.num_key; i++)
    {
        buf_read_page(table_id, new.entries[i].pagenum, (page_t *)&tmp_page);
        buf_unpin_page(table_id, new.entries[i].pagenum);
        tmp_page.parent_page_num = new_page_num;
        buf_write_page(table_id, new.entries[i].pagenum, (page_t *)&tmp_page);
    }

    // write in disk
    buf_write_page(table_id, old_page_num, (page_t*)old);
    buf_write_page(table_id, new_page_num, (page_t *)&new);

    return insert_into_parent(table_id, old, old_page_num, k_prime, &new, new_page_num);
}

// core dump?
void insert_into_internal(int table_id, internal_page_t * left, pagenum_t left_index, int key, internal_page_t * right, pagenum_t right_page_num)
{
    int i;
    internal_page_t parent;

    buf_read_page(table_id, left->parent_page_num, (page_t *)&parent);
    buf_unpin_page(table_id, left->parent_page_num);

    for(i = parent.num_key; i > left_index + 1; i--)
    {
        parent.entries[i].key = parent.entries[i - 1].key;
        parent.entries[i].pagenum = parent.entries[i - 1].pagenum;
    }
    parent.entries[left_index + 1].key = key;
    parent.entries[left_index + 1].pagenum = right_page_num;
    parent.num_key++;

    buf_write_page(table_id, left->parent_page_num, (page_t *)&parent);
}

// return the index of parent's record2[] which points to child page(left_page)
// so in insert_into_internal(), upcomming element should be inserted at (left_index + 1)
int get_left_index(int table_id, pagenum_t left_page_num)
{
    internal_page_t left, parent;
    int left_index;

    buf_read_page(table_id, left_page_num, (page_t *)&left);
    buf_unpin_page(table_id, left_page_num);
    buf_read_page(table_id, left.parent_page_num, (page_t *)&parent);
    buf_unpin_page(table_id, left.parent_page_num);
    
    left_index = 0;

    if(left_page_num == parent.leftmostdown_page_num)
    {
        return -1;
    }

    //left_index < parent.num_key && needed? -> no
    while(parent.entries[left_index].pagenum != left_page_num)
    {
        left_index++;
    }
    return left_index;
}

void insert_into_new_root(int table_id, internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num)
{
    header_page_t header;
    internal_page_t new_root;
    pagenum_t new_root_page_num;

    new_root.isLeaf = 0;
    new_root.leftmostdown_page_num = left_page_num;
    new_root.num_key = 1;
    new_root.parent_page_num = 0;
    new_root.entries[0].key = key;
    new_root.entries[0].pagenum = right_page_num;

    new_root_page_num = buf_alloc_page(table_id);
    buf_write_page(table_id, new_root_page_num, (page_t *)&new_root);

    // printf("new root page(#%ld) is created!\n", new_root_page_num);
    
    // change the header's information
    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);
    header.root_page_num = new_root_page_num;
    buf_write_page(table_id, 0, (page_t *)&header);

    // change the child's parent to root
    left->parent_page_num = new_root_page_num;
    right->parent_page_num = new_root_page_num;
    buf_write_page(table_id, left_page_num, (page_t *)left);
    buf_write_page(table_id, right_page_num, (page_t *)right);
}



void insert_into_parent(int table_id, internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num)
{
    // printf("insert %d into parent!\n", key);
    int left_index;
    internal_page_t parent;
    
    if(left->parent_page_num == 0)
    {
        // printf("insert into new root,,\n");
        return insert_into_new_root(table_id, left, left_page_num, key, right, right_page_num);
    }

    buf_read_page(table_id, left->parent_page_num, (page_t *)&parent);
    buf_unpin_page(table_id, left->parent_page_num);

    left_index = get_left_index(table_id, left_page_num);

    if(parent.num_key < MAX_ENTRY)
    {
        // insert into internal page
        return insert_into_internal(table_id, left, left_index, key, right, right_page_num);
        
    }
    else
    {
        // insert into internal page after splitting
        // printf("split propagation!!!!\n");
        return insert_into_internal_after_splitting(table_id, &parent, left->parent_page_num, left_index, key, right_page_num);
    }
    

}

void insert_into_leaf_after_splitting(int table_id, leaf_page_t * leaf, pagenum_t leaf_page_num, int64_t key, char * value)
{
    int insertion_point, i, j, split;
    int new_leaf_page_num_key;
    int new_key; // which is going to parent page;
    pagenum_t new_leaf_page_num;
    record temp_arr[MAX_RECORD + 1];
    record new_record[MAX_RECORD];

    leaf_page_t new_leaf;

    // make temp_arr which is a record array containing new key and
    insertion_point = 0;
    while(insertion_point < leaf->num_key && leaf->records[insertion_point].key < key)
    {
        insertion_point++;
    }
    for(i = 0, j = 0; i < leaf->num_key; i++, j++)
    {
        if(j == insertion_point)
        {
            j++;
        }
        temp_arr[j].key = leaf->records[i].key;
        strcpy(temp_arr[j].value, leaf->records[i].value);
    }
    temp_arr[insertion_point].key = key;
    strcpy(temp_arr[insertion_point].value, value);

    // split into leaf and new leaf
    leaf->num_key = 0;
    split = (MAX_RECORD + 1) / 2; // when the number of max record is odd, split the node in the exact half
    for(i = 0; i < split; i++)
    {
        leaf->records[i].key = temp_arr[i].key;
        strcpy(leaf->records[i].value, temp_arr[i].value);
        leaf->num_key++;
    }

    new_leaf_page_num_key = 0;
    for(j = 0; i < MAX_RECORD + 1; i++, j++)
    {
        new_record[j].key = temp_arr[i].key;
        strcpy(new_record[j].value, temp_arr[i].value);
        new_leaf_page_num_key++;
    }
    new_key = new_record[0].key;


    //allocate new leaf page and initialize
    new_leaf_page_num = buf_alloc_page(table_id);
    new_leaf.isLeaf = 1;
    new_leaf.num_key = new_leaf_page_num_key;
    new_leaf.parent_page_num = leaf->parent_page_num; // this is 0 -> unwanted -> no matter
    new_leaf.right_sibling_page_num = leaf->right_sibling_page_num;
    for(i = 0; i < new_leaf.num_key; i++)
    {
        new_leaf.records[i].key = new_record[i].key;
        strcpy(new_leaf.records[i].value, new_record[i].value);
    }
    buf_write_page(table_id, new_leaf_page_num, (page_t *)&new_leaf);

    //change old leaf page's right sibling
    leaf->right_sibling_page_num = new_leaf_page_num;
    buf_write_page(table_id, leaf_page_num, (page_t *)leaf);

    // printf("after leaf split,\n old_leaf's key num : %d\n new_leaf's key num : %d\n", leaf->num_key, new_leaf.num_key);
    return insert_into_parent(table_id, (internal_page_t *)leaf, leaf_page_num, new_key, (internal_page_t *)&new_leaf, new_leaf_page_num);
}

// insert record(key, value) into right page
// return 0 when sucess otherwise, return -1
int db_insert(int table_id, int64_t key, char * value)
{
    pagenum_t leaf_page_num;
    header_page_t header;
    leaf_page_t leaf;
    char tmp[120];

    // doesn't allow duplicate keys
    if(nt_db_find(table_id, key, tmp) != -1)
    {
        // printf("oops, the key already exists in the tree!\n");
        return -1;
    }

    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);

    // Case: the tree does not exist yet
    if(header.root_page_num == 0)
    {
        // start a new tree.
        // printf("start_new_tree,,\n");
        start_new_tree(table_id, key, value);
        // printf("root page is created!\n");
    }
    else
    {
        leaf_page_num = nt_find_leaf_page(table_id, key);
        buf_read_page(table_id, leaf_page_num, (page_t *)&leaf);
        buf_unpin_page(table_id, leaf_page_num);
        // printf("leaf page number : %ld\n", leaf_page_num);

        // Case: leaf has room for records
        if(leaf.num_key < MAX_RECORD)
        {
            //insert into leaf
            // printf("current leaf's key number : %d\n", leaf.num_key);
            // printf("insert into leaf,,\n");
            insert_into_leaf(table_id, leaf_page_num, key, value);
        }
        // Case: leaf has no room for records -> split
        else
        {
            // printf("splitting occurs!!\n");
            insert_into_leaf_after_splitting(table_id, &leaf, leaf_page_num, key, value);
        }
    }
    return 0;
}

// after deletion entry from root page, adjust root page information
void adjust_root_page(int table_id, header_page_t * header, pagenum_t root_page_num)
{
    internal_page_t root, new_root;
    pagenum_t new_root_page_num;

    buf_read_page(table_id, root_page_num, (page_t *)&root);
    buf_unpin_page(table_id, root_page_num);
    
    // Case: root is nonempty -> nothing to be done
    if(root.num_key > 0)
    {
        // printf("root is nonempty. just exit.\n");
        return ;
    }

    // Case: root is empty but has a child
    if(!root.isLeaf)
    {   
        new_root_page_num = root.leftmostdown_page_num;
        
        header->root_page_num = new_root_page_num;
        // printf("new root page %ld is born!\n", new_root_page_num);
        buf_write_page(table_id, 0, (page_t *)header);

        buf_read_page(table_id, new_root_page_num, (page_t *)&new_root);
        buf_unpin_page(table_id, new_root_page_num);
        new_root.parent_page_num = 0;
        buf_write_page(table_id, new_root_page_num, (page_t *)&new_root);
        buf_free_page(table_id, root_page_num);
    }
    // Case root is empty but has no child
    else
    {
        // printf("root page's records are all deleted\n");
        // printf("tree becomes empty\n");
        header->root_page_num = 0;
        buf_write_page(table_id, 0, (page_t *)header);
        buf_free_page(table_id, root_page_num);
    }
}

// remove an entry with key in page n and shift other records for tidiness
// and return the page number where deletion occured
pagenum_t remove_entry_from_page(int table_id, pagenum_t n, int64_t key)
{
    internal_page_t internal;
    leaf_page_t leaf;
    int deletion_point, i;

    buf_read_page(table_id, n, (page_t *)&leaf);
    buf_unpin_page(table_id, n);

    // when remove entry from internal page
    if(!leaf.isLeaf)
    {   
        // printf("remove entry with key %ld from internal page!\n", key);

        // because entry type is different, read page again with internal_page_t type
        buf_read_page(table_id, n, (page_t *)&internal);
        buf_unpin_page(table_id, n);

        // internal page has an one more special key which points to the left most page
        // so this condition means that removing that key value from page 
        if(internal.num_key == 0)
        {
            // printf("internal page's keys are already deleted!, and the only left is a leftmostpage.\n");
            internal.leftmostdown_page_num = 0; //
            buf_write_page(table_id, n, (page_t *)&internal); //
            buf_free_page(table_id, internal.leftmostdown_page_num); //
            return n;
        }

        for(i = 0; i < internal.num_key; i++)
        {
            if(internal.entries[i].key == key)
            {
                deletion_point = i;
                break;
            }
        }

        for(i = deletion_point + 1; i < internal.num_key; i++)
        {   
            internal.entries[i - 1].key = internal.entries[i].key;
            internal.entries[i - 1].pagenum = internal.entries[i].pagenum;
        }
        internal.num_key--;

        buf_write_page(table_id, n, (page_t *)&internal);
    }
    // when remove an entry from leaf page
    else
    {
        // printf("remove entry with key %ld from leaf page!\n", key);
        for(i = 0; i < leaf.num_key; i++)
        {
            if(leaf.records[i].key == key)
            {
                deletion_point = i;
                break;
            }
        }

        // not really remove data, change num_key
        for(i = deletion_point + 1; i < leaf.num_key; i++)
        {
            leaf.records[i - 1].key = leaf.records[i].key;
            strcpy(leaf.records[i - 1].value, leaf.records[i].value);
        }
        leaf.num_key--;
        buf_write_page(table_id, n, (page_t *)&leaf);
    }

    return n;
}

// get neighbor page's index from the parent page
// return -2 when the current page is the leftmost page
// return -1 when the current page is the right next to the leftmost page
int get_neighbor_page_index(int table_id, pagenum_t page_num)
{
    pagenum_t parent_page_number;
    internal_page_t n, parent;
    int i;

    buf_read_page(table_id, page_num, (page_t *)&n);
    buf_unpin_page(table_id, page_num);

    parent_page_number = n.parent_page_num;
    buf_read_page(table_id, parent_page_number, (page_t *)&parent);
    buf_unpin_page(table_id, parent_page_number);

    if(page_num == parent.leftmostdown_page_num)
    {
        return -2;
    }

    for(i = 0; i < parent.num_key; i++)
    {
        if(parent.entries[i].pagenum == page_num)
        {
            return i - 1;
        }
    }
}

// this function is used when the right subtree's left most leaf page is deleted,
// to link the left subtree's right most leaf page to next leaf page.
pagenum_t get_left_sibling_leaf_page_num(int table_id, pagenum_t n)
{
    // printf("lets's find left sibling leaf page!\n");
    leaf_page_t start_page;
    internal_page_t parent_page, grandparent_page;
    pagenum_t parent_page_num, grandparent_page_num;
    int i, index;

    buf_read_page(table_id, n, (page_t *)&start_page);
    buf_unpin_page(table_id, n);

    parent_page_num = start_page.parent_page_num;

    while(1)
    {
        buf_read_page(table_id, parent_page_num, (page_t *)&parent_page);
        buf_unpin_page(table_id, parent_page_num);

        grandparent_page_num = parent_page.parent_page_num;

        if(grandparent_page_num == 0)
        {
            // printf("the leaf page is the left most page in the tree! nothing to do,,\n");
            return -1;
        }
        buf_read_page(table_id, grandparent_page_num, (page_t *)&grandparent_page);
        buf_unpin_page(table_id, grandparent_page_num);

        if(grandparent_page.leftmostdown_page_num != parent_page_num)
        {
            break;
        }
        else
        {
            parent_page_num = grandparent_page_num;
        }
    }

    // printf("grand parent page : %ld. let's move down\n", grandparent_page_num);

    for(i = 0; i < grandparent_page.num_key; i++)
    {
        if(grandparent_page.entries[i].pagenum == parent_page_num)
        {
            index = i;
            break;
        }
    }

    leaf_page_t target_page;
    internal_page_t neighbor_parent_page, child_page;
    pagenum_t neighbor_parent_page_num, child_page_num;

    child_page.isLeaf = 0;

    if(index == 0)
    {
        neighbor_parent_page_num = grandparent_page.leftmostdown_page_num;
    }
    else
    {
        neighbor_parent_page_num = grandparent_page.entries[index - 1].pagenum;
    }

    
    while(!child_page.isLeaf)
    {
        buf_read_page(table_id, neighbor_parent_page_num, (page_t *)&neighbor_parent_page);
        buf_unpin_page(table_id, neighbor_parent_page_num);

        if(neighbor_parent_page.num_key == 0)
        {
            child_page_num = neighbor_parent_page.leftmostdown_page_num;
        }
        else
        {
            index = neighbor_parent_page.num_key - 1;
            child_page_num = neighbor_parent_page.entries[index].pagenum;
        }
        
        buf_read_page(table_id, child_page_num, (page_t *)&child_page);
        buf_unpin_page(table_id, child_page_num);

        neighbor_parent_page_num = child_page_num;
    }
    
    // printf("the target leaf page num : %ld\n", child_page_num);
    return child_page_num;
}

void delayed_merge(int table_id, pagenum_t parent, pagenum_t neighbor, pagenum_t n, int neighbor_index, int64_t k_prime)
{

    pagenum_t left_sibling_leaf_page_num;
    internal_page_t parent_page;
    internal_page_t internal_n, internal_neighbor, internal_tmp;
    leaf_page_t leaf_n, leaf_neighbor;
    leaf_page_t left_sibling_leaf_page;
    int neighbor_insertion_index;

    buf_read_page(table_id, parent, (page_t *)&parent_page);
    buf_unpin_page(table_id, parent);

    buf_read_page(table_id, n, (page_t*)&internal_n);
    buf_unpin_page(table_id, n);

    // case : internal page
    if(!internal_n.isLeaf)
    {
        // don't merge page n with neighbor until all elements are deleted
        if(internal_n.leftmostdown_page_num != 0)
        {
            // printf("internal page's leftmost page is still alive! stop merging,,\n");
            return;
        }
        else
        {
            // case 1 : n is the left most internal page
            if(neighbor_index == -2)
            {
                // case 1-1 : n is the last child page
                if(parent_page.num_key == 0)
                {
                    // printf("special case : deleting the the last remaining page\n ");
                    parent_page.leftmostdown_page_num = 0;
                    buf_write_page(table_id, parent, (page_t *)&parent_page);
                }
                // case 1-2 : because of the structure of internal page,
                // before removing entry from parent page, move the first entry's pagenum to the leftmostdown_page_num
                else
                {
                    // printf("special case : deleting the left most internal page -> copy!\n");
                    parent_page.leftmostdown_page_num = neighbor;
                    buf_write_page(table_id, parent, (page_t *)&parent_page);
                }
                
            }
            //when n is not the left most internal page, just free that page
            buf_free_page(table_id, n);
            // printf("while merging, internal page %ld is freed!\n", n);

        }
    }
    // case: leaf page
    else
    {
        // special case : page n is the leftmost page
        if(neighbor_index == -2)
        {
            // page n is the last child page
            if(parent_page.num_key == 0)
            {
                parent_page.leftmostdown_page_num = 0;
                buf_write_page(table_id, parent, (page_t *)&parent_page);
            }
            else
            {
                // printf("special case : deleting the left most leaf page -> copy!\n");
                parent_page.leftmostdown_page_num = neighbor;
                buf_write_page(table_id, parent, (page_t *)&parent_page);
            }

            // should link left subtree's right most leaf page to n's neighbor leaf page
            buf_read_page(table_id, n, (page_t *)&leaf_n);
            buf_unpin_page(table_id, n);
            left_sibling_leaf_page_num = get_left_sibling_leaf_page_num(table_id, n);

            // printf("left sibling leaf page num : %ld\n", left_sibling_leaf_page_num);
            if(left_sibling_leaf_page_num != -1)
            {
                // printf("special case! : link leaf page(%ld) to leaf page(%ld)\n", left_sibling_leaf_page_num, leaf_n.right_sibling_page_num);
                buf_read_page(table_id, left_sibling_leaf_page_num, (page_t * )&left_sibling_leaf_page);
                buf_unpin_page(table_id, left_sibling_leaf_page_num);
                left_sibling_leaf_page.right_sibling_page_num = leaf_n.right_sibling_page_num;
                buf_write_page(table_id, left_sibling_leaf_page_num, (page_t * )&left_sibling_leaf_page);
            }
        }
        else
        {
            buf_read_page(table_id, n, (page_t *)&leaf_n);
            buf_unpin_page(table_id, n);

            buf_read_page(table_id, neighbor, (page_t *)&leaf_neighbor);
            buf_unpin_page(table_id, neighbor);

            leaf_neighbor.right_sibling_page_num = leaf_n.right_sibling_page_num;
            buf_write_page(table_id, neighbor, (page_t *)&leaf_neighbor);
        }
        buf_free_page(table_id, n);
        // printf("while merging, leaf page %ld is freed!\n", n);
    }
    delete_entry(table_id, parent, k_prime);
}

// delete entry with key in the page n.
// if page's entrys are all deleted, modify b+ tree structure using delayed merged.
void delete_entry(int table_id, pagenum_t n, int64_t key) 
{
    // printf("delete entry with key %ld in page %ld,,\n", key, n);
    header_page_t header;
    internal_page_t parent;
    leaf_page_t page;
    pagenum_t page_num, neighbor_page_num;
    int neighbor_page_index;
    int k_prime_index;
    int64_t k_prime;

    page_num = remove_entry_from_page(table_id, n, key);

    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);

    // Case: deletion from the root
    if(page_num == header.root_page_num)
    {
        // printf("deletion occured in root page %ld! adjust root,,,\n", page_num);
        return adjust_root_page(table_id, &header, page_num);
    }

    // it doesn't matter reading page with leaf_page_t type because
    // leaf page and internal page share the same header information(parent_page_num, isLeaf, num_key,,)
    buf_read_page(table_id, page_num, (page_t *)&page);
    buf_unpin_page(table_id, page_num);

    // Case: deletion below the root and the number of records becomes 0 -> delayed merge
    if(page.num_key == 0)
    {
        // printf("deletion occured in below root page! merge start,,\n");

        neighbor_page_index = get_neighbor_page_index(table_id, page_num);

        if(neighbor_page_index == -2 || neighbor_page_index == -1)
        {
            k_prime_index = 0;
        }
        else
        {
            k_prime_index = neighbor_page_index + 1;
        }
        buf_read_page(table_id, page.parent_page_num, (page_t *)&parent);
        buf_unpin_page(table_id, page.parent_page_num);

        k_prime = parent.entries[k_prime_index].key;

        // when n is the leftmost page
        if(neighbor_page_index == -2)
        {
            neighbor_page_num = parent.entries[0].pagenum;
        }
        // when n is the right page of the leftmost page
        else if(neighbor_page_index == -1)
        {
            neighbor_page_num = parent.leftmostdown_page_num;
        }
        else
        {
            neighbor_page_num = parent.entries[neighbor_page_index].pagenum;
        }
        
        delayed_merge(table_id, page.parent_page_num ,neighbor_page_num, page_num, neighbor_page_index, k_prime);
    }
}
// delete record with key. if success, return 0
// else return -1. using delayed merge
int db_delete(int table_id, int64_t key)
{
    pagenum_t leaf_page_num;
    char tmp[120];

    if(nt_db_find(table_id, key, tmp) == -1)
    {
        // printf("there is no record with key %ld\n", key);
        return -1;
    }

    leaf_page_num = nt_find_leaf_page(table_id, key);
    
    delete_entry(table_id, leaf_page_num, key);

    return 0;
}