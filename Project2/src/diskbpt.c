// #include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

// #include <sys/types.h>
// #include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "../include/diskbpt.h"

extern uint64_t fd; 
/*
new function
int find_leaf_page(int key)
void init_table(int fd)
void close_table(void)

vo. enqueue(pagenum_t page_num)
pagenum_t dequeue()
int path_to_root(interna(internal_page_t *)l_page_t * root, internal_page_t * child)
void print_tree(header_page_t * header)


*/

void insert_into_parent(internal_page_t * left, pagenum_t left_page_num, int key, internal_page_t * right, pagenum_t right_page_num);

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

// new_rank = path_to_root(&root, (internal_page_t *)&tmp1);

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
            file_read_page(parent_page_num, (page_t *)&internal);
            parent_page_num = internal.parent_page_num;
            length++;
        }
        return length;
    }
}

void print_tree(header_page_t * header)
{
    pagenum_t n;
    // internal_page_t root;
    internal_page_t tmp;
    internal_page_t parent;
    leaf_page_t tmp1;

    int i = 0;
    int rank = 0;
    int new_rank = 0;

    if(header->root_page_num == 0)
    {
        printf("Empty tree.\n");
        return;
    }

    // file_read_page(header->root_page_num, (page_t *)&root);
    queue = NULL;
    enqueue(header->root_page_num);

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
                printf("%ld ", tmp.records[i].key);
            }
            
            enqueue(tmp.leftmostdown_page_num);
            for (i = 0; i < tmp.num_key; i++)
            {
                enqueue(tmp.records[i].pagenum);
            }
            printf("| ");
        }
    }
}

void close_table(void)
{
    close(fd);
}

// initialize datafile
void init_table(int fd)
{
    header_page_t header;

    // header = (header_page_t *)malloc(sizeof(PAGE_SIZE));
    // if(header == NULL)
    // {
        // printf("malloc() failed\n");
        // printf("init_table() failed\n");
        // exit(0);
    // }
    header.free_page_num = 0;
    header.root_page_num = 0;
    header.page_num = 1;
    file_write_page(0, (page_t *)&header);
    make_free_page(&header);
}

int open_table(char * pathname)
{
    fd = open(pathname, O_RDWR | O_CREAT | O_EXCL | O_SYNC, 0644);

    if(fd != -1)
    {
        printf("datafile is newly created!\n");
        init_table(fd);
    }
    else if((fd == -1) && (EEXIST == errno)) //data file already exists
    {
        fd = open(pathname, O_RDWR | O_SYNC);
    }
    
    return fd;
}

// find a possible leaf page containing a key
// if there is a possible leaf page, return the leaf page num
// else if when there is a no page, return -1 
pagenum_t find_leaf_page(int key)
{   
    header_page_t header;
    internal_page_t root;
    pagenum_t root_page_num, leaf_page_num, next;
    int i;

    file_read_page(0, (page_t *)&header);

    // there is no root
    if(header.root_page_num == 0)
    {
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
            if(key < root.records[0].key)
            {
                next = root.leftmostdown_page_num;
                file_read_page(next, (page_t *)&root);
                continue;
            }

            i = 0;
            while(i < root.num_key && root.records[i].key <= key)
            {
                i++;
            }
            next = root.records[--i].pagenum;
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

    printf("finding a leaf,,\n");
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

    printf("allocating page,,\n");
    root_page_num = file_alloc_page();

    file_read_page(0, (page_t *)&header); // why not to just write? -> overwrite problem
    header.root_page_num = root_page_num;
    file_write_page(0, (page_t *)&header);

    file_write_page(root_page_num, (page_t *)&root);
    printf("root is written in page %ld\n", header.root_page_num);
    
    //for test
    header_page_t header1;
    file_read_page(0, (page_t *)&header1);
}

void insert_into_leaf(pagenum_t leaf_page_num, int64_t key, char * value)
{
    leaf_page_t leaf;
    int i, insertion_point;

    file_read_page(leaf_page_num, (page_t *)&leaf);

    printf("keys in the leaf :\n");
    for(int i = 0; i < leaf.num_key; i++)
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

    // printf("new key : %ld\n", leaf.records[insertion_point].key);
    // printf("new record : %s\n", leaf.records[insertion_point].value);

    printf("keys in the leaf after inserting :\n");
    for(int i = 0; i < leaf.num_key; i++)
    {
        printf("%ld ", leaf.records[i].key);
    }
    printf("\n");

    file_write_page(leaf_page_num, (page_t *)&leaf);
}

void insert_into_node_after_splitting(internal_page_t * old, pagenum_t old_page_num, int left_index, int key, pagenum_t right_page_num)
{
    int i, j, split, k_prime;
    internal_page_t new;
    pagenum_t new_page_num;
    // record2 tmp[249];
    record2 tmp[3];

    // create tmp array which contains key and pagenum in order,
    // including new key and pagenum
    for(i = 0, j = 0; i < old->num_key; i++, j++)
    {
        if(j == left_index + 1)
        {
            j++;
        }
        tmp[j].key = old->records[i].key;
        tmp[j].pagenum = old->records[i].pagenum;
    }
    tmp[left_index + 1].key = key;
    tmp[left_index + 1].pagenum = right_page_num;


    // split tmp in tmp[0~split-1], tmp[split], tmp[split+1 ~ 248]
    // as a result,     old          k_prime          new

    // split = 249 / 2;
    split = 3 / 2;
    new.isLeaf = 0;
    new.num_key = 0;
    new.parent_page_num = old->parent_page_num;
    new.leftmostdown_page_num = tmp[split].pagenum;

    old->num_key = 0;
    for(i = 0; i < split; i++)
    {
        old->records[i].key = tmp[i].key;
        old->records[i].pagenum = tmp[i].pagenum;
        old->num_key++;
    }
    k_prime = tmp[split].key;
    for(++i, j = 0; i < 3; i++, j++)
    {
        new.records[j].key = tmp[i].key;
        new.records[j].pagenum = tmp[i].pagenum;
        new.num_key++;
    }

    // change parent page number of new page's children pages
    // because their parent changed from old to new

    pagenum_t tmp_page_num;
    internal_page_t tmp_page;

    new_page_num = file_alloc_page();

    file_read_page(new.leftmostdown_page_num, (page_t *)&tmp_page);
    tmp_page.parent_page_num = new_page_num;
    file_write_page(new.leftmostdown_page_num, (page_t *)&tmp_page);

    for(i = 0; i < new.num_key; i++)
    {
        file_read_page(new.records[i].pagenum, (page_t *)&tmp_page);
        tmp_page.parent_page_num = new_page_num;
        file_write_page(new.records[i].pagenum, (page_t *)&tmp_page);
    }

    // write in disk
    file_write_page(old_page_num, (page_t*)old);
    file_write_page(new_page_num, (page_t *)&new);

    return insert_into_parent(old, old_page_num, k_prime, &new, new_page_num);
}

// core dump?
void insert_into_node(internal_page_t * left, pagenum_t left_index, int key, internal_page_t * right, pagenum_t right_page_num)
{
    int i;
    internal_page_t parent;

    file_read_page(left->parent_page_num, (page_t *)&parent);
    for(i = parent.num_key; i > left_index + 1; i--)
    {
        parent.records[i].key = parent.records[i - 1].key;
        parent.records[i].pagenum = parent.records[i - 1].pagenum;
    }
    parent.records[left_index + 1].key = key;
    parent.records[left_index + 1].pagenum = right_page_num;
    parent.num_key++;

    file_write_page(left->parent_page_num, (page_t *)&parent);
}

// return the index of parent's record2[] which points to child page(left_page)
// so in insert_into_node(), upcomming element should be inserted at (left_index + 1)
int get_left_index(internal_page_t * left, pagenum_t left_page_num)
{
    internal_page_t parent;
    int left_index;

    file_read_page(left->parent_page_num, (page_t *)&parent);
    left_index = 0;

    if(left_page_num == parent.leftmostdown_page_num)
    {
        return -1;
    }

    //left_index < parent.num_key && needed? -> maybe not
    while(parent.records[left_index].pagenum != left_page_num)
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
    new_root.records[0].key = key;
    new_root.records[0].pagenum = right_page_num;

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
    printf("insert %d into parent!\n", key);

    int left_index;
    internal_page_t parent;
    
    if(left->parent_page_num == 0)
    {
        printf("insert into new root,,\n");
        return insert_into_new_root(left, left_page_num, key, right, right_page_num);
    }

    file_read_page(left->parent_page_num, (page_t *)&parent);

    left_index = get_left_index(left, left_page_num);

    //if(parent.num_key < 248)
    if(parent.num_key < 2)
    {
        // insert into node
        return insert_into_node(left, left_index, key, right, right_page_num);
        //return insert_into_node(left, left_index, key, right);
        
    }
    else
    {
        printf("split propagation!!!!\n");
        return insert_into_node_after_splitting(&parent, left->parent_page_num, left_index, key, right_page_num);
        // insert into node after splitting
    }
    

}

void insert_into_leaf_after_splitting(leaf_page_t * leaf, pagenum_t leaf_page_num, int64_t key, char * value)
{
    int insertion_point, i, j, split;
    int new_leaf_page_num_key, new_leaf_page_right_sibling_page;
    int new_key; // which is going to parent page;
    pagenum_t new_leaf_page_num;
    // record1 temp_arr[32];
    // record1 new_record[31];
    record1 temp_arr[4];
    record1 new_record[3];

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
    // split = 16;
    split = 2;
    for(i = 0; i < split; i++)
    {
        leaf->records[i].key = temp_arr[i].key;
        strcpy(leaf->records[i].value, temp_arr[i].value);
        leaf->num_key++;
    }

    new_leaf_page_num_key = 0;
    for(j = 0; j < split; i++, j++)
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

    printf("after leaf split,\n old_leaf's key num : %d\n new_leaf's key num : %d\n", leaf->num_key, new_leaf.num_key);
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
        printf("start_new_tree,,\n");
        start_new_tree(key, value);
        printf("root node is created!\n");
        return 0;
    }
    else
    {
        leaf_page_num = find_leaf_page(key);
        file_read_page(leaf_page_num, (page_t *)&leaf);
        printf("leaf page number : %ld\n", leaf_page_num);

        // Case: leaf has room for records
        // if(leaf.num_key < 31)
        if(leaf.num_key < 3)
        {
            //insert into leaf
            printf("current leaf's key number : %d\n", leaf.num_key);
            printf("insert into leaf,,\n");
            insert_into_leaf(leaf_page_num, key, value);

            //tmp
            file_read_page(leaf_page_num, (page_t *)&leaf);
            printf("after writing in the disk, keys are :\n");
            for(int i = 0; i < leaf.num_key; i++)
            {
                printf("%ld ", leaf.records[i].key);
            }
            printf("\n");
            return 0;
        }
        // Case: leaf has no room for records -> split
        else
        {
            printf("splitting occurs!!\n");
            insert_into_leaf_after_splitting(&leaf, leaf_page_num, key, value);
        }
    }
}

int db_delete(int64_t key)
{

}