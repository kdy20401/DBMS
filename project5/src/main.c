#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>

#define MAX_OPNUM 3
#define TRHEAD_NUM 5
#define MAX_RECORD 2
#define DB_FIND 0
#define DB_UPDATE 1

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;

// total_transaction = total_commit + abort_find + abort_update
int total_transaction = 0;
int total_commit = 0;
int total_abort = 0;
int commit_update = 0;
int abort_find = 0;
int abort_update = 0;

void * find_or_update_func(void * arg)
{
    int64_t key;
    char value[120];
    char ret_val[120];
    int table_id;
    int op_num;
    int db_api;

    table_id = *((int *)arg);
    op_num = MAX_OPNUM;

    int trx_id = trx_begin();
    printf("trx %d begin\n", trx_id);

    pthread_mutex_lock(&mutex);
    total_transaction++;
    pthread_mutex_unlock(&mutex);

    sprintf(value, "trx%d", trx_id);
    strcat(value, "\0");

    for(int i = 0; i < op_num; i++)
    {
        db_api = rand() % 2;
        // table_id = (rand() % 2) + 1;

        if(db_api == DB_UPDATE)
        {
            key = (rand() % (MAX_RECORD)) + 1;

            // printf("trx %d tries to update key %ld in table %d\n", trx_id, key, table_id);
            if (db_update(table_id, key, value, trx_id) == -1)
            {
                // printf("trx %d's update() is aborted at key %ld in table %d\n", trx_id, key, table_id);
                pthread_mutex_lock(&mutex);
                abort_update++;
                pthread_mutex_unlock(&mutex);
                pthread_exit(NULL);
            }
            // printf("trx %d update() success at key %ld value to %s in table %d\n", trx_id, key, value, table_id);
        }
        else if(db_api == DB_FIND)
        {
            key = (rand() % (MAX_RECORD)) + 1;

            // printf("trx %d tries to find key %ld in table %d\n", trx_id, key, table_id);

            if (db_find(table_id, key, ret_val, trx_id) == -1)
            {
                // printf("trx %d's find() is aborted at key %ld in table %d\n", trx_id, key, table_id);
                pthread_mutex_lock(&mutex);
                abort_find++;
                pthread_mutex_unlock(&mutex);
                pthread_exit(NULL);
            }
            // printf("trx %d find success at key %ld, value %s in table %d\n", trx_id, key, ret_val, table_id);
        }
    }

    trx_id = trx_commit(trx_id);
    printf("trx %d commit\n", trx_id);
    pthread_mutex_lock(&mutex);
    total_commit++;
    pthread_mutex_unlock(&mutex);

}

void * find_func(void* argv)
{
    int table_id;
    int64_t key;
    char value[120];
    int op_num;

    table_id = 1;
    op_num = 2;

    int trx_id = trx_begin();

    printf("trx %d begin\n", trx_id);

    for(int i = 0; i < op_num; i++)
    {
        key = (rand() % (MAX_RECORD)) + 1;

        printf("trx %d tries to find key %ld in table %d\n", trx_id, key, table_id);

        if (db_find(table_id, key, value, trx_id) == -1)
        {
            printf("trx %d's find() is aborted at key %ld in table %d\n", trx_id, key, table_id);
            pthread_mutex_lock(&mutex);
            abort_find++;
            pthread_mutex_unlock(&mutex);
            pthread_exit(NULL);
        }
        printf("trx %d find success at key %ld, value %s in table %d\n", trx_id, key, value, table_id);
    }

    trx_id = trx_commit(trx_id);
    printf("trx %d commit\n", trx_id);

}

void * update_func(void * argv)
{
    int64_t key;
    char value[120];
    int table_id;
    int op_num;

    table_id = 1;
    op_num = 2;
    int trx_id = trx_begin();

    printf("trx %d begin\n", trx_id);

    sprintf(value, "trx%d", trx_id);
    value[4] = '\0';

    for(int i = 0; i < op_num; i++)
    {
        key = (rand() % (MAX_RECORD)) + 1;

        printf("trx %d tries to update key %ld in table %d\n", trx_id, key, table_id);
        if (db_update(1, key, value, trx_id) != 0)
        {
            printf("trx %d's update() is aborted at key %ld in table %d\n", trx_id, key, table_id);
            pthread_mutex_lock(&mutex1);
            abort_update++;
            pthread_mutex_unlock(&mutex1);
            pthread_exit(NULL);
        }
        printf("trx %d update() success at key %ld value to %s in table %d\n", trx_id, key, value, table_id);

    }

    trx_id = trx_commit(trx_id);
    printf("trx %d commit\n", trx_id);

    pthread_mutex_lock(&mutex1);
    commit_update++;
    pthread_mutex_unlock(&mutex1);
}


int main(int argc, char* argv[])
{
    srand(time(NULL));

    int t1;
    void * arg;
    char value1[120];
    char value2[120];
    
    if(init_db(100) == -1)
    {
        printf("init_db() failed\n");
        return 0;
    }


    t1 = open_table("c.db");
    arg = (void *)&t1;

    for(int i = 1; i <= MAX_RECORD; i++)
    {
        db_delete(t1, i);
    }
    printf("db_delete() finished\n");

    for(int i = 1; i <= MAX_RECORD; i++)
    {
        db_insert(t1, i, "1");
    }
    printf("db_insert() finished\n");
    print_tree(t1);
    printf("\n\n");

    pthread_t thread_id[TRHEAD_NUM];

    clock_t start = clock();

    for(int i = 0; i < TRHEAD_NUM; i++)
    {
        pthread_create(&thread_id[i], 0, find_or_update_func, arg);
    }

    for(int i = 0; i < TRHEAD_NUM; i++)
    {
        pthread_join(thread_id[i], NULL);
    }

    clock_t end = clock();

    printf("\n");
    printf("total_transaction : %d\n", total_transaction);
    printf("total_commit : %d\n", total_commit);
    printf("abort_find : %d\n", abort_find);
    printf("abort_update : %d\n\n\n", abort_update);
    printf("Time: %lf\n", (double)(end - start)/CLOCKS_PER_SEC);

    int total_abort;
    total_abort = abort_find + abort_update;

    if(total_transaction == total_commit + total_abort)
    {
        printf("transactions ends correctly\n");
    }
    else
    {
        printf("there is correctness problem with transactions\n");
    }

    nt_db_find(t1, 1, value1);
    nt_db_find(t1, 2, value2);
    printf("key 1 value %s\nkey 2 value %s\n", value1, value2);
    shutdown_db();
    return 0;
}