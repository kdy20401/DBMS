#include <stdio.h>
#include <stdlib.h>
#include "../include/diskbpt.h"


int main(int argc, char ** argv)
{
    int ret;
    char instruction;
    int64_t key;
    char value[120];
    header_page_t header, tmp;

    open_table("datafile.bin");
    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction)
        {
            case 'i':
                scanf("%ld %s", &key, value);
                db_insert(key, value);
                file_read_page(0, (page_t *)&header);
                // print_tree(&header);
                break;
            case 'f':
                scanf("%ld", &key);
                if(db_find(key, value) == 0)
                {
                    printf("key : %ld, value : %s\n", key, value);
                }
                else
                {
                    printf("record with key %ld doesn't exist in the tree\n", key);
                }
                break;
            case 'q':
                while (getchar() != (int)'\n');
                return 0;
                break;
            case 't':
                break;
            case 'p':
                file_read_page(0, (page_t *)&header);
                print_tree(&header);
                break;
            case 'x':
                remove("datafile.bin");
                open_table("datafile.bin");
                break;
            case 'd':
                scanf("%ld", &key);
                if(db_delete(key) == 0)
                {
                    printf("test_main : record with key %ld is deleted\n", key);
                }
                else
                {
                    printf("test_main : record with key %ld doesn't exist in the tree\n", key);
                }
                break;
            case 's':
                show_leaf_page_keys();
                break;
            default:
                break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    close_table();
    return 0;
}