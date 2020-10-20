
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
#include "diskbpt.h"

#include <stdio.h>
#include <stdlib.h>


#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>


extern uint64_t table_id; 
extern int tables[MAX_TABLE + 1];

void show_leaf_page_keys()
{
    header_page_t header;
    internal_page_t internal_page;
    leaf_page_t leaf_page;
    int i;
    file_read_page(0, (page_t *)&header);
    
    if(header.root_page_num == 0)
    {
        printf("Empty tree.\n");
        return ;
    }

    file_read_page(header.root_page_num, (page_t *)&internal_page);

    if(!!internal_page.isLeaf)
    {
        file_read_page(header.root_page_num, (page_t *)&leaf_page);
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
            file_read_page(next_page_num, (page_t *)&internal_page);
        }

        // until right sibling page doesn't exist, print keys and follow link
        while(next_page_num != 0)
        {
            file_read_page(next_page_num, (page_t *)&leaf_page);
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
void show_free_page_list()
{
    header_page_t header;
    free_page_t free;

    file_read_page(0, (page_t *)&header);

    pagenum_t first = header.free_page_num;
    pagenum_t next = first;
    printf("free page list : %ld", next);
    while(next != 0)
    {
        file_read_page(next, (page_t *)&free);
        next = free.next_free_page_num;
        printf("% ld", next);
    }
    printf("\n");
}

void enqueue(pagenum_t page_num)
{
    node * new_node;
    node * c;

    new_node = (node *)malloc(sizeof(node));
    if(new_node == NULL)
    {
        printf("malloc() failed\n");
        printf("enqueue() failed\n");
        exit(0);
    }
    new_node->page_num = page_num;

    if(queue == NULL)
    {
        queue = new_node;
        queue->next = NULL;
    }
    else
    {
        c = queue;
        while(c->next != NULL)
        {
            c = c->next;
        }
        c->next = new_node;
        new_node->next = NULL;
    }
}

pagenum_t dequeue()
{
    int page_num;

    if(queue == NULL)
    {
        return -1;
    }

    node * n = queue;
    queue = queue->next;
    page_num = n->page_num;
    free(n);
    return page_num;
}


int path_to_root(pagenum_t pagenum)
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
            // printf("parent %ld\n", parent_page_num);
            file_read_page(parent_page_num, (page_t *)&internal);
            parent_page_num = internal.parent_page_num;
            length++;
        }
        return length;
    }
}

void print_tree(void)
{
    pagenum_t n;
    internal_page_t tmp;
    internal_page_t parent;
    leaf_page_t tmp1;
    header_page_t header;

    file_read_page(0, (page_t *)&header);
    int i = 0;
    int rank = 0;
    int new_rank = 0;

    if(header.root_page_num == 0)
    {
        printf("Empty tree.\n");
        return;
    }

    // file_read_page(header->root_page_num, (page_t *)&root);
    queue = NULL;
    enqueue(header.root_page_num);

    while(queue != NULL)
    {
        n = dequeue();
        file_read_page(n, (page_t *)&tmp);

        if(!!tmp.isLeaf) //leaf page -> read page again using leaf page struct
        {

            file_read_page(n, (page_t *)&tmp1); 
            if(tmp1.parent_page_num != 0)
            {
                file_read_page(tmp1.parent_page_num, (page_t *)&parent);
                if(n == parent.leftmostdown_page_num)
                {
                    new_rank = path_to_root(tmp1.parent_page_num);
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
                file_read_page(tmp.parent_page_num, (page_t *)&parent);
                if(n == parent.leftmostdown_page_num)
                {   
                    new_rank = path_to_root(tmp.parent_page_num);
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

void close_table(int table_id)
{
    close(tables[table_id]);
}

// initialize datafile
void init_table(int table_id)
{
    header_page_t header;

    header.free_page_num = 0;
    header.root_page_num = 0;
    header.page_num = 1;
    file_write_page(0, (page_t *)&header);
    make_free_page(&header);
}

// return the unique table id corresponding fd
// binary search?
int get_table_id(int fd)
{
    int i;
    for(i = 1; i <= MAX_TABLE; i++)
    {
        if(tables[i] == fd)
        {
            return i;
        }
    }
}

// after opening a table, return it's unique table id
int open_table(char * pathname)
{
    int fd, table_id;

    if(strlen(pathname) > 20)
    {
        printf("data file's name is too long! it should be less than or equal to 20.\n");
        return -1;
    }

    fd = open(pathname, O_RDWR | O_CREAT | O_EXCL, 0644);

    if(fd != -1)
    {
        table_id++;
        if(table_id > 10)
        {
            printf("table number exceeds 10,,!\n");
            close(fd);
            return -1;
        }
        printf("datafile (table id = %ld) is newly created!\n", fd);
        tables[table_id] = fd;
        init_table(table_id);
    }
    else if((fd == -1) && (EEXIST == errno)) //data file already exists
    {
        fd = open(pathname, O_RDWR);
        printf("datafile (table id = %ld) is opened!\n", fd);
        table_id = get_table_id(fd);
    }
    
    return table_id;
}

// find a possible leaf page containing a key
// if there is a possible leaf page, return the leaf page num
// else return -1 
pagenum_t find_leaf_page(int64_t key)
{   
    header_page_t header;
    internal_page_t root;
    pagenum_t root_page_num, leaf_page_num, next;
    int i;

    file_read_page(0, (page_t *)&header);

    // there is no root
    if(header.root_page_num == 0)
    {
        printf("there is no root\n");
        return -1;
    }

    root_page_num = header.root_page_num; 

    file_read_page(root_page_num, (page_t *)&root);

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
                file_read_page(next, (page_t *)&root);
                continue;
            }

            i = 0;
            while(i < root.num_key && root.entries[i].key <= key)
            {
                i++;
            }
            next = root.entries[--i].pagenum;
            file_read_page(next, (page_t *)&root);
        }

        leaf_page_num = next;
    }
    return leaf_page_num;
}

// find a record with key and copy to ret_val
// return 0 when success, -1 when there is no record with key 
int db_find(int64_t key, char * ret_val)
{
    leaf_page_t leaf;
    int leaf_page_num, i;

    // printf("finding a leaf,,\n");
    leaf_page_num = find_leaf_page(key);

    if(leaf_page_num == -1)
    {
        return -1;
    }

    file_read_page(leaf_page_num, (page_t *)&leaf);

    printf("finding a location of record in the leaf,,\n");
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

void start_new_tree(int64_t key, char * value)
{
    leaf_page_t root;
    header_page_t header;
    int root_page_num;

    root.isLeaf = 1;
    root.num_key = 1;
    root.parent_page_num = 0;
    root.records[0].key = key;
    strcpy(root.records[0].value, value);
    root.right_sibling_page_num = 0;

    // printf("allocating page,,\n");
    root_page_num = file_alloc_page();

    file_read_page(0, (page_t *)&header); // why not to just write? -> overwrite problem
    header.root_page_num = root_page_num;
    file_write_page(0, (page_t *)&header);

    file_write_page(root_page_num, (page_t *)&root);
    printf("root is written in page %ld\n", header.root_page_num);
    
}

void insert_into_leaf(pagenum_t leaf_page_num, int64_t key, char * value)
{
    leaf_page_t leaf;
    int i, insertion_point;

    file_read_page(leaf_page_num, (page_t *)&leaf);

    // printf("keys in the leaf :\n");
    for(i = 0; i < leaf.num_key; i++)
    {
        printf("%ld ", leaf.records[i].key);
    }
    printf("\n");
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
    for(int i = 0; i < leaf.num_key; i++)
    {
        printf("%ld ", leaf.records[i].key);
    }
    printf("\n");

    file_write_page(leaf_page_num, (page_t *)&leaf);
}

void insert_into_internal_after_splitting(internal_page_t * old, pagenum_t old_page_num, int left_index, int key, pagenum_t right_page_num)
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

    new_page_num = file_alloc_page();

    file_read_page(new.leftmostdown_page_num, (page_t *)&tmp_page);
    tmp_page.parent_page_num = new_page_num;
    file_write_page(new.leftmostdown_page_num, (page_t *)&tmp_page);

    for(i = 0; i < new.num_key; i++)
    {
        file_read_page(new.entries[i].pagenum, (page_t *)&tmp_page);
        tmp_page.parent_page_num = new_page_num;
        file_write_page(new.entries[i].pagenum, (page_t *)&tmp_page);
    }

    // write in disk
    file_write_page(old_page_num, (page_t*)old);
    file_write_page(new_page_num, (page_t *)&new);

    return insert_into_parent(old, old_page_num, k_prime, &new, new_page_num);
}

// core dump?
void insert_into_internal(internal_page_t * left, pagenum_t left_index, int key, internal_page_t * right, pagenum_t right_page_num)
{
    int i;
    internal_page_t parent;

    file_read_page(left->parent_page_num, (page_t *)&parent);
    for(i = parent.num_key; i > left_index + 1; i--)
    {
        parent.entries[i].key = parent.entries[i - 1].key;
        parent.entries[i].pagenum = parent.entries[i - 1].pagenum;
    }
    parent.entries[left_index + 1].key = key;
    parent.entries[left_index + 1].pagenum = right_page_num;
    parent.num_key++;

    file_write_page(left->parent_page_num, (page_t *)&parent);
}

// return the index of parent's record2[] which points to child page(left_page)
// so in insert_into_internal(), upcomming element should be inserted at (left_index + 1)
int get_left_index(pagenum_t left_page_num)
{
    internal_page_t left, parent;
    int left_index;

    file_read_page(left_page_num, (page_t *)&left);
    file_read_page(left.parent_page_num, (page_t *)&parent);
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

void insert_into_new_root(internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num)
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

    new_root_page_num = file_alloc_page();
    file_write_page(new_root_page_num, (page_t *)&new_root);

    printf("new root page(#%ld) is created!\n", new_root_page_num);
    
    // change the header's information
    file_read_page(0, (page_t *)&header);
    header.root_page_num = new_root_page_num;
    file_write_page(0, (page_t *)&header);

    // change the child's parent to root
    left->parent_page_num = new_root_page_num;
    right->parent_page_num = new_root_page_num;
    file_write_page(left_page_num, (page_t *)left);
    file_write_page(right_page_num, (page_t *)right);
}



void insert_into_parent(internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num)
{
    // printf("insert %d into parent!\n", key);

    int left_index;
    internal_page_t parent;
    
    if(left->parent_page_num == 0)
    {
        printf("insert into new root,,\n");
        return insert_into_new_root(left, left_page_num, key, right, right_page_num);
    }

    file_read_page(left->parent_page_num, (page_t *)&parent);

    left_index = get_left_index(left_page_num);

    if(parent.num_key < MAX_ENTRY)
    {
        // insert into internal page
        return insert_into_internal(left, left_index, key, right, right_page_num);
        
    }
    else
    {
        // insert into internal page after splitting
        // printf("split propagation!!!!\n");
        return insert_into_internal_after_splitting(&parent, left->parent_page_num, left_index, key, right_page_num);
    }
    

}

void insert_into_leaf_after_splitting(leaf_page_t * leaf, pagenum_t leaf_page_num, int64_t key, char * value)
{
    int insertion_point, i, j, split;
    int new_leaf_page_num_key, new_leaf_page_right_sibling_page;
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
    new_leaf_page_num = file_alloc_page();
    new_leaf.isLeaf = 1;
    new_leaf.num_key = new_leaf_page_num_key;
    new_leaf.parent_page_num = leaf->parent_page_num; // this is 0 -> unwanted -> no matter
    new_leaf.right_sibling_page_num = leaf->right_sibling_page_num;
    for(i = 0; i < new_leaf.num_key; i++)
    {
        new_leaf.records[i].key = new_record[i].key;
        strcpy(new_leaf.records[i].value, new_record[i].value);
    }
    file_write_page(new_leaf_page_num, (page_t *)&new_leaf);

    //change old leaf page's right sibling
    leaf->right_sibling_page_num = new_leaf_page_num;
    file_write_page(leaf_page_num, (page_t *)leaf);

    // printf("after leaf split,\n old_leaf's key num : %d\n new_leaf's key num : %d\n", leaf->num_key, new_leaf.num_key);
    return insert_into_parent((internal_page_t *)leaf, leaf_page_num, new_key, (internal_page_t *)&new_leaf, new_leaf_page_num);
}

// insert record(key, value) into right page
// return 0 when sucess otherwise, return -1
int db_insert(int64_t key, char * value)
{
    pagenum_t leaf_page_num;
    header_page_t header;
    leaf_page_t leaf;

    // doesn't allow duplicate keys
    if(db_find(key, value) != -1)
    {
        printf("oops, the key already exists in the tree!\n");
        return -1;
    }

    file_read_page(0, (page_t *)&header);

    // Case: the tree does not exist yet
    if(header.root_page_num == 0)
    {
        // start a new tree.
        // printf("start_new_tree,,\n");
        start_new_tree(key, value);
        // printf("root node is created!\n");
    }
    else
    {
        leaf_page_num = find_leaf_page(key);
        file_read_page(leaf_page_num, (page_t *)&leaf);
        // printf("leaf page number : %ld\n", leaf_page_num);

        // Case: leaf has room for records
        if(leaf.num_key < MAX_RECORD)
        {
            //insert into leaf
            // printf("current leaf's key number : %d\n", leaf.num_key);
            // printf("insert into leaf,,\n");
            insert_into_leaf(leaf_page_num, key, value);

            return 0;
        }
        // Case: leaf has no room for records -> split
        else
        {
            // printf("splitting occurs!!\n");
            insert_into_leaf_after_splitting(&leaf, leaf_page_num, key, value);
        }
    }
    return 0;
}

// after deletion entry from root page, adjust root page information
void adjust_root_page(header_page_t * header, pagenum_t root_page_num)
{
    internal_page_t root, new_root;
    pagenum_t new_root_page_num;

    file_read_page(root_page_num, (page_t *)&root);

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
        file_write_page(0, (page_t *)header);

        file_read_page(new_root_page_num, (page_t *)&new_root);
        new_root.parent_page_num = 0;
        file_write_page(new_root_page_num, (page_t *)&new_root);
        file_free_page(root_page_num);
    }
    // Case root is empty but has no child
    else
    {
        // printf("root page's records are all deleted\n");
        // printf("tree becomes empty\n");
        header->root_page_num = 0;
        file_write_page(0, (page_t *)header);
        file_free_page(root_page_num);
    }
}

// remove an entry with key in page n and shift other records for tidiness
// and return the page number where deletion occured
pagenum_t remove_entry_from_page(pagenum_t n, int64_t key)
{
    internal_page_t internal;
    leaf_page_t leaf;
    int deletion_point, i;

    file_read_page(n, (page_t *)&leaf);

    // when remove entry from internal page
    if(!leaf.isLeaf)
    {   
        // printf("remove entry with key %ld from internal page!\n", key);

        // because entry type is different, read page again with internal_page_t type
        file_read_page(n, (page_t *)&internal);

        // internal page has an one more special key which points to the left most page
        // so this condition means that removing that key value from page 
        if(internal.num_key == 0)
        {
            // printf("internal page's keys are already deleted!, and the only left is a leftmostpage.\n");
            internal.leftmostdown_page_num = 0; //
            file_write_page(n, (page_t *)&internal); //
            file_free_page(internal.leftmostdown_page_num); //
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

        file_write_page(n, (page_t *)&internal);
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
        file_write_page(n, (page_t *)&leaf);
    }

    return n;
}

// get neighbor page's index from the parent page
// return -2 when the current page is the leftmost page
// return -1 when the current page is the right next to the leftmost page
int get_neighbor_page_index(pagenum_t page_num)
{
    pagenum_t parent_page_number;
    internal_page_t parent;
    internal_page_t n;
    int i;

    file_read_page(page_num, (page_t *)&n);
    parent_page_number = n.parent_page_num;
    file_read_page(parent_page_number, (page_t *)&parent);

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
pagenum_t get_left_sibling_leaf_page_num(pagenum_t n)
{
    // printf("lets's find left sibling leaf page!\n");
    leaf_page_t start_page;
    internal_page_t parent_page, grandparent_page;
    pagenum_t parent_page_num, grandparent_page_num;
    int i, index;

    file_read_page(n, (page_t *)&start_page);
    parent_page_num = start_page.parent_page_num;

    while(1)
    {
        file_read_page(parent_page_num, (page_t *)&parent_page);
        grandparent_page_num = parent_page.parent_page_num;

        if(grandparent_page_num == 0)
        {
            // printf("the leaf page is the left most page in the tree! nothing to do,,\n");
            return -1;
        }
        file_read_page(grandparent_page_num, (page_t *)&grandparent_page);

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
        file_read_page(neighbor_parent_page_num, (page_t *)&neighbor_parent_page);
        if(neighbor_parent_page.num_key == 0)
        {
            child_page_num = neighbor_parent_page.leftmostdown_page_num;
        }
        else
        {
            index = neighbor_parent_page.num_key - 1;
            child_page_num = neighbor_parent_page.entries[index].pagenum;
        }
        
        file_read_page(child_page_num, (page_t *)&child_page);
        neighbor_parent_page_num = child_page_num;
    }
    
    // printf("the target leaf page num : %ld\n", child_page_num);
    return child_page_num;
}

void delayed_merge(pagenum_t parent, pagenum_t neighbor, pagenum_t n, int neighbor_index, int64_t k_prime)
{

    internal_page_t parent_page;
    internal_page_t internal_n, internal_neighbor, internal_tmp;
    leaf_page_t leaf_n, leaf_neighbor;
    int neighbor_insertion_index;

    file_read_page(parent, (page_t *)&parent_page);
    file_read_page(n, (page_t*)&internal_n);

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
                    file_write_page(parent, (page_t *)&parent_page);
                }
                // case 1-2 : because of the structure of internal page,
                // before removing entry from parent page, move the first pagenum to the leftmostdown_page_num
                else
                {
                    // printf("special case : deleting the left most internal page -> copy!\n");
                    parent_page.leftmostdown_page_num = parent_page.entries[0].pagenum;
                    file_write_page(parent, (page_t *)&parent_page);
                }
                
            }
            //when n is not the left most internal page, just free that page
            
            file_free_page(n);
            // printf("while merging, internal page %ld is freed!\n", n);

        }
    }
    // case: leaf page
    else
    {
        // special case : page n is the leftmost page
        if(neighbor_index == -2)
        {
            // for delay merge
            if(parent_page.num_key == 0)
            {
                parent_page.leftmostdown_page_num = 0;
                file_write_page(parent, (page_t *)&parent_page);
                
                //how to connect left subtree's right most leaf page
                // to right subtree's left most leaf page?
            }
            else
            {
                // printf("special case : deleting the left most leaf page -> copy!\n");
                parent_page.leftmostdown_page_num = neighbor;
                file_write_page(parent, (page_t *)&parent_page);
                // n = neighbor;     
            }

            pagenum_t left_sibling_leaf_page_num;
            leaf_page_t left_sibling_leaf_page;
            file_read_page(n, (page_t *)&leaf_n);
            left_sibling_leaf_page_num = get_left_sibling_leaf_page_num(n);
            // printf("left sibling leaf page num : %ld\n", left_sibling_leaf_page_num);
            if(left_sibling_leaf_page_num != -1)
            {
                // printf("special case! : link leaf page(%ld) to leaf page(%ld)\n", left_sibling_leaf_page_num, leaf_n.right_sibling_page_num);
                file_read_page(left_sibling_leaf_page_num, (page_t * )&left_sibling_leaf_page);
                left_sibling_leaf_page.right_sibling_page_num = leaf_n.right_sibling_page_num;
                file_write_page(left_sibling_leaf_page_num, (page_t * )&left_sibling_leaf_page);
            }
            
        }
        else
        {
            file_read_page(n, (page_t *)&leaf_n);
            file_read_page(neighbor, (page_t *)&leaf_neighbor);
            leaf_neighbor.right_sibling_page_num = leaf_n.right_sibling_page_num;
            file_write_page(neighbor, (page_t *)&leaf_neighbor);
        }
        file_free_page(n);
        // printf("while merging, leaf page %ld is freed!\n", n);

    }
    delete_entry(parent, k_prime);
}

// delete entry with key in the page n.
// if page's entrys are all deleted, modify b+ tree structure using delayed merged.
void delete_entry(pagenum_t n, int64_t key) 
{
    // printf("delete entry with key %ld in page %ld,,\n", key, n);
    
    header_page_t header;
    internal_page_t parent;
    leaf_page_t page;
    pagenum_t page_num, neighbor_page_num;
    int neighbor_page_index;
    int k_prime_index;
    int64_t k_prime;

    page_num = remove_entry_from_page(n, key);

    file_read_page(0, (page_t *)&header);

    // Case: deletion from the root
    if(page_num == header.root_page_num)
    {
        // printf("deletion occured in root page %ld! adjust root,,,\n", page_num);
        adjust_root_page(&header, page_num);
        return ;
    }

    // it doesn't matter reading page with leaf_page_t type because
    // leaf page and internal page share the same header information
    file_read_page(page_num, (page_t *)&page);

    // Case: deletion below the root and the number of records becomes 0 -> delayed merge
    if(page.num_key == 0)
    {
        // printf("deletion occured in below root page! merge start,,\n");

        neighbor_page_index = get_neighbor_page_index(page_num);

        if(neighbor_page_index == -2 || neighbor_page_index == -1)
        {
            k_prime_index = 0;
        }
        else
        {
            k_prime_index = neighbor_page_index + 1;
        }
        file_read_page(page.parent_page_num, (page_t *)&parent);
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
        
        delayed_merge(page.parent_page_num ,neighbor_page_num, page_num, neighbor_page_index, k_prime);

    }
}
// delete record with key. if success, return 0
// else return -1. using delayed merge
int db_delete(int64_t key)
{
    pagenum_t leaf_page_num;
    char tmp[120];

    if(db_find(key, tmp) == -1)
    {
        printf("there is no record with key %ld\n", key);
        return -1;
    }

    leaf_page_num = find_leaf_page(key);
    
    delete_entry(leaf_page_num, key);

    return 0;
}