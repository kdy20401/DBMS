#include "table.h"
#include "lock_table.h"
#include "buffer.h"
#include "file.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>

extern table_info tables[MAX_TABLE + 1];
extern int table_num;

// initialize hash table
void init_hash(int size)
{
    int i;
    hash_table.table_size = size;
    hash_table.directory = (bucket **)malloc(sizeof(bucket *) * hash_table.table_size);
    
    if(hash_table.directory == NULL)
    {
        printf("malloc() failed\n");
        printf("init_hash() failed\n");
        exit(0) ;
    }
    //make dummy node
    for(i = 0; i < hash_table.table_size; i++)
    {
        hash_table.directory[i] = (bucket *)malloc(sizeof(bucket));
        if(hash_table.directory[i] == NULL)
        {
            printf("malloc() failed\n");
            printf("making dummy nodes in hash table failed\n");
            printf("init_hash() failed\n");
            exit(0);
        }
        hash_table.directory[i]->fptr = NULL;
        hash_table.directory[i]->next = NULL;
    }

    // printf("hash table of size %d is made!\n", hash_table.table_size);
}

// hash function. key is the page number
int hash(pagenum_t page_num, int hashtable_size)
{
    return page_num % hashtable_size;
}

// find the bucket containing a frame pointer which has corresponding table id and page number
// if the bucket exists, return the frame pointer
frame * hash_find(int table_id, pagenum_t page_num, hashtable * table)
{
    bucket * dummy;
    bucket * p;
    frame * fptr;

    dummy = table->directory[hash(page_num, table->table_size)];
    p = dummy->next;

    while(p != NULL)
    {   
        fptr = p->fptr;

        if(fptr->table_id == table_id && fptr->page_num == page_num)
        {
            return fptr;   
        }
        p = p->next;
    }
    
    return NULL;
}

// make a new bucket containing a frame pointer and insert to hash table
void hash_insert(frame * fptr, hashtable * table)
{
    bucket * new_bucket;
    bucket * dummy;
    pagenum_t page_num;

    new_bucket = (bucket *)malloc(sizeof(bucket));
    if(new_bucket == NULL)
    {
        printf("malloc() failed\n");
        printf("hash_insert() failed\n");
        exit(0);
    }
    new_bucket->fptr = fptr;
    page_num = fptr->page_num;

    dummy = table->directory[hash(page_num, table->table_size)];
    new_bucket->next = dummy->next;
    dummy->next = new_bucket;
}

// find the bucket containing a corresponding frame pointer and delete it from hash table
void hash_delete(frame * fptr, hashtable * table)
{
    bucket * p;
    bucket * q;
    int table_id;
    pagenum_t page_num;

    table_id = fptr->table_id;
    page_num = fptr->page_num;

    q = table->directory[hash(page_num, table->table_size)]; //dummy
    p = q->next;

    while(p != NULL)
    {
        if(p->fptr->table_id == table_id && p->fptr->page_num == page_num)
        {
            q->next = p->next;
            free(p);
            break;
        }
        q = p;
        p = p->next;
    }
}

void shutdown_lock_table()
{
	lt_bucket * p;
	lt_bucket * q;

	for(int i = 0; i < lock_table.table_size; i++)
	{
		q = lock_table.directory[i];
		
		while(q != NULL)
		{
			p = q;
			q = q->next;
			free(p);
		}
	}
	
	free(lock_table.directory);
}

// remove hash table
void shutdown_hash()
{
    bucket * p;
    bucket * q;

    // deallocate every buckets in each directory rows
    for(int i = 0; i < hash_table.table_size; i++)
    {
        q = hash_table.directory[i];

        while(q != NULL)
        {
            p = q;
            q = q->next;
            free(p);
        }
    }

    free(hash_table.directory);
}

// when initializing buffer, also make LRU list
// to easily maintain the LRU list
void init_lru_list(int frame_num)
{
    int i;

    lru_list_head = buffer[0];
    lru_list_tail = buffer[frame_num - 1];
    
    if(frame_num == 1)
    {
        return;
    }

    buffer[0]->next = buffer[1];
    
    for(i = 1; i < frame_num - 1; i++)
    {
        buffer[i]->next = buffer[i + 1];
        buffer[i]->prev = buffer[i - 1];
    }
    buffer[frame_num - 1]->prev = buffer[frame_num - 2];
}

// create a buffer which contains buf_num number of frames, initialize frame information and buffer manager latch.
// create a hash table which helps to find a frame fast which contains target page. also initialize LRU list

int init_db(int buf_num)
{
    if(buf_num <= 0)
    {
        return -1;
    }

    int i;

    frame_num = buf_num;
    pthread_mutex_init(&buffer_manager_latch, NULL);

    // create a buffer
    buffer = (frame **)malloc(sizeof(frame *) * frame_num);
    if(buffer == NULL)
    {
        printf("malloc() failed\n");
        printf("failed to create buffer!\n");
        return -1;
    }

    for(i = 0; i < frame_num; i++)
    {
        buffer[i] = (frame *)malloc(sizeof(frame));
        
        if(buffer[i] == NULL)
        {
            printf("malloc() failed\n");
            printf("failed to create frame!\n");
            return -1;
        }
        buffer[i]->page = (page_t *)malloc(PAGE_SIZE);
        buffer[i]->page_num = -1;
        buffer[i]->table_id = -1;
        buffer[i]->is_dirty = false;
        pthread_mutex_init(&(buffer[i]->page_latch), NULL);
        buffer[i]->next = NULL;
        buffer[i]->prev = NULL;        
    }

    // create a hash table and initialize LRU list
    init_hash(frame_num);
    init_lru_list(frame_num);

    // initialize lock table and transaction table
    init_lock_table();
    return 0;
}

// flush all dirty pages in the buffer to disk
// deallocate memory used for buffer
int shutdown_db(void)
{
    int i;
    
    // remove buffer
    for(i = 0; i < frame_num; i++)
    {   
        if(buffer[i]->is_dirty)
        {
            file_write_page(buffer[i]->table_id, buffer[i]->page_num, buffer[i]->page);
        }
        free(buffer[i]->page);
        free(buffer[i]);
    }
    free(buffer);

    // remove hash table
    shutdown_hash();

    // remove lock table
    shutdown_lock_table();

    //if table exists which is not closed yet, close that table
    for(i = 1; i <= table_num; i++)
    {
        if(tables[i].is_opened)
        {
            close(tables[i].fd);
        }
    }

    return 0;
}

void show_buffer_info()
{
    int i;
    for(i = 0; i < frame_num; i++)
    {
        printf("frame%d : table_id = %d, page_num = %ld\n", i, buffer[i]->table_id, buffer[i]->page_num);
    }
}

void show_lru_list()
{
    frame * p;

    p = lru_list_head;

    while(p != NULL)
    {
        printf("%ld ", p->page_num);
        p = p->next;
    }
    printf("\n");
}

// by least recently used policy,
// the frame used right before should be placed in the head
void update_lru_list(frame * fptr)
{
    // if corressponding frame is the head of the LRU list,
    // nothing to do
    if(fptr == lru_list_head)
    {
        return;
    }

    if(fptr != lru_list_tail)
    {
        fptr->prev->next = fptr->next;
        fptr->next->prev = fptr->prev;
    }
    else
    {
        fptr->prev->next = NULL;
        lru_list_tail = fptr->prev;
    }
    fptr->prev = NULL;
    fptr->next = lru_list_head;
    fptr->next->prev = fptr;
    lru_list_head = fptr;
}

// make a 10 free pages of free page list
// and return the first free page number
// same function as file_make_free_page but to maintain layered architecture,
// use buffer manager API's and include it to buffer managers API
pagenum_t buf_make_free_page(int table_id)
{
    int i;
    free_page_t tmp[FREE_PAGE_NUM];
    header_page_t header;
    pagenum_t start_free_page_num, page_num;

    buf_read_page(table_id, 0, (page_t*)&header);
    buf_unpin_page(table_id, 0);

    page_num = header.page_num;
    start_free_page_num = page_num;

    for(i = 0; i < FREE_PAGE_NUM; i++)
    {
        if(i == FREE_PAGE_NUM - 1)
        {
            tmp[i].next_free_page_num = 0;
        }
        else
        {
            tmp[i].next_free_page_num = page_num + 1;
            page_num++;
        }       
    }

    header.free_page_num = start_free_page_num;
    header.page_num += FREE_PAGE_NUM;
    buf_write_page(table_id, 0, (page_t *)&header);

    for(i = 0; i < FREE_PAGE_NUM; i++)
    {
        buf_write_page(table_id, start_free_page_num, (page_t *)&tmp[i]);
        // printf("free page(#%ld) is created!!!!\n", start_free_page_num);
        start_free_page_num++;
    }

    return header.free_page_num;
}

// first, find free pages from free page list.
// if free pages don't exist, extend the file and make new free pages
// and allocate first free page in free page list.
// same function as file_alloc_page but to maintain layered architecture,
// use buffer manager API's and include it to buffer managers API
pagenum_t buf_alloc_page(int table_id)
{
    header_page_t header;
    free_page_t tmp;
    pagenum_t free_page_num;

    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);

    free_page_num = header.free_page_num;

    if(free_page_num == 0)
    {
        // printf("there are no free pages. make a free page list,,\n");
        //make 10 new free pages
        free_page_num = buf_make_free_page(table_id);
    }

    //get free pages from free page list
    buf_read_page(table_id, free_page_num, (page_t *)&tmp);
    buf_unpin_page(table_id, free_page_num);

    // must read header page again! because in buf_make_free_page(), header information is changed!!
    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);

    header.free_page_num = tmp.next_free_page_num;
    buf_write_page(table_id, 0, (page_t *)&header);

    return free_page_num;
}

// make pagenum page a free page.
// same function as file_free_page but to maintain layered architecture,
// use buffer manager APIs and include it to buffer managers API
void buf_free_page(int table_id, pagenum_t pagenum)
{
    free_page_t tmp;
    header_page_t header;
    pagenum_t first_free_page_num;

    buf_read_page(table_id, pagenum, (page_t *)&tmp);
    buf_unpin_page(table_id, pagenum);

    buf_read_page(table_id, 0, (page_t *)&header);
    buf_unpin_page(table_id, 0);

    first_free_page_num = header.free_page_num;
    tmp.next_free_page_num = first_free_page_num; // garbage data will be overwritten
    header.free_page_num = pagenum;

    buf_write_page(table_id, 0, (page_t *)&header);
    buf_write_page(table_id, pagenum, (page_t *)&tmp);
}

void buf_read_page(int table_id, pagenum_t page_num, page_t * dest)
{
    frame * fptr;

    fptr = hash_find(table_id, page_num, &hash_table);

    if(fptr != NULL)
    {
        memcpy(dest, fptr->page, PAGE_SIZE);
    }
    else
    {
        fptr = lru_list_tail;

        if(fptr->page_num != -1)
        {
            hash_delete(fptr, &hash_table);
        }

        if(fptr->is_dirty == true)
        {
            file_write_page(fptr->table_id, fptr->page_num, fptr->page);
        }

        file_read_page(table_id, page_num, fptr->page);
        memcpy(dest, fptr->page, PAGE_SIZE);
        fptr->is_dirty = false;
        fptr->page_num = page_num;
        fptr->table_id = table_id;

        hash_insert(fptr, &hash_table);
    }
}
frame * buf_read_page_trx(int table_id, pagenum_t page_num, page_t * dest)
{
    acquire_buffer_latch();

    frame * fptr;

    //use a hash table to find the frame which contains the target page
    fptr = hash_find(table_id, page_num, &hash_table);

    //if the page is in the buffer pool
    if(fptr != NULL)
    {
        acquire_page_latch(fptr);
        memcpy(dest, fptr->page, PAGE_SIZE);
    }
    //if the page is not in the pool, get page from the disk!
    else
    {
        // eviction
        fptr = lru_list_tail;
        acquire_page_latch(fptr);

        // delete existing frame information in hash table
        // this condition is for the initial circumstance
        if(fptr->page_num != -1)
        {
            hash_delete(fptr, &hash_table);
        }

        if(fptr->is_dirty == true)
        {
            file_write_page(fptr->table_id, fptr->page_num, fptr->page);
        }

        file_read_page(table_id, page_num, fptr->page);
        memcpy(dest, fptr->page, PAGE_SIZE);
        fptr->is_dirty = false;
        fptr->page_num = page_num;
        fptr->table_id = table_id;

        hash_insert(fptr, &hash_table);
    }

    //update LRU list
    update_lru_list(fptr);

    release_buffer_latch();

    return fptr;
}

// this buf_read_page_trx1() function is same as buf_read_page_trx()
// but doesn't acquire buffer latch and page latch. this function is only used to rollback
void buf_read_page_trx1(int table_id, pagenum_t page_num, page_t * dest)
{
    frame * fptr;

    //use a hash table to find the frame which contains the target page
    fptr = hash_find(table_id, page_num, &hash_table);

    //if the page is in the buffer pool
    if(fptr != NULL)
    {
        memcpy(dest, fptr->page, PAGE_SIZE);
    }
    //if the page is not in the pool, get page from the disk!
    else
    {
        // eviction
        fptr = lru_list_tail;

        // delete existing frame information in hash table
        // this condition is for the initial circumstance
        if(fptr->page_num != -1)
        {
            hash_delete(fptr, &hash_table);
        }

        if(fptr->is_dirty == true)
        {
            file_write_page(fptr->table_id, fptr->page_num, fptr->page);
        }

        file_read_page(table_id, page_num, fptr->page);
        memcpy(dest, fptr->page, PAGE_SIZE);
        fptr->is_dirty = false;
        fptr->page_num = page_num;
        fptr->table_id = table_id;

        hash_insert(fptr, &hash_table);
    }

    // this function is used to rollback, so i think updating LRU is meaningless.
    // however, including update_lru_list() incurs segmentation fault -> why?
    // update_lru_list(fptr);
}

void buf_write_page(int table_id, pagenum_t page_num, page_t * src)
{
    frame * fptr;

    fptr = hash_find(table_id, page_num, &hash_table);

    if(fptr != NULL)
    {
        memcpy(fptr->page, src, PAGE_SIZE);
        fptr->is_dirty = true;
    }
    else
    {
        fptr = lru_list_tail;
        
        if(fptr->page_num != -1)
        {
            hash_delete(fptr, &hash_table);
        }

        if(fptr->is_dirty == true)
        {
            file_write_page(fptr->table_id, fptr->page_num, fptr->page);
        }

        memcpy(fptr->page, src, PAGE_SIZE);
        fptr->is_dirty = true;
        fptr->page_num = page_num;
        fptr->table_id = table_id;

        hash_insert(fptr, &hash_table);
    }
}

void buf_write_page_trx(int table_id, pagenum_t page_num, page_t * src)
{
    frame * fptr;

    //use a hash table to find the frame which contains the target page
    fptr = hash_find(table_id, page_num, &hash_table);

    // target page is in the buffer pool
    if(fptr != NULL)
    {
        memcpy(fptr->page, src, PAGE_SIZE);
        fptr->is_dirty = true;
    }
    // target page is not in the buffer pool
    else
    {
        fptr = lru_list_tail;

        // this condition is for the initial circumstance
        if(fptr->page_num != -1)
        {
            hash_delete(fptr, &hash_table);
        }

        if(fptr->is_dirty == true)
        {
            file_write_page(fptr->table_id, fptr->page_num, fptr->page);
        }

        memcpy(fptr->page, src, PAGE_SIZE);
        fptr->is_dirty = true;
        fptr->page_num = page_num;
        fptr->table_id = table_id;

        hash_insert(fptr, &hash_table);
    }

    // don't update lru list
    // in most cases, buf_write_page_trx() is executed after buf_read_page_trx()
    // so LRU list is already updated
    // update_lru_list(fptr);
    
}

void buf_unpin_page(int table_id, pagenum_t page_num)
{

}

void acquire_buffer_latch()
{
    if(pthread_mutex_lock(&buffer_manager_latch) != 0)
    {
        printf("acquire_buffer_latch() failed\n");
        exit(0);
    }
}

void release_buffer_latch()
{
    if(pthread_mutex_unlock(&buffer_manager_latch) != 0)
    {
        printf("release_buffer_latch() failed\n");
        exit(0);
    }
}

void acquire_page_latch(frame * fptr)
{   
    if(pthread_mutex_lock(&(fptr->page_latch)) != 0)
    {
        printf("acquire_page_latch() failed\n");
        exit(0);
    }
}

void release_page_latch(frame * fptr)
{
    if(pthread_mutex_unlock(&(fptr->page_latch)) != 0)
    {
        printf("release_page_latch() failed\n");
        exit(0);
    }
}