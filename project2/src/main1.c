#include "bpt.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char ** argv)
{
    int table_id;
    char instruction;
    int64_t key;
    char value[120];
    char datafile[20];

    // initialize buffer and hash table
    printf("first, open a datafile (o datafile)\n");
    printf("> ");
    scanf("%s", datafile);
    table_id = open_table(datafile);

    clock_t start = clock();
    for(int i = 0; i < 5000; i++)
    {
        scanf("%ld %s", &key, value);
        db_insert(key, value);
    }
    clock_t end = clock();

    printf("Time: %lf\n", (double)(end - start)/CLOCKS_PER_SEC);

    return 0;
}