#include <stdio.h>
#include <stdlib.h>

int main()
{
    int t1,t2;
    char value[20];

	// init_db(1000);
	t1 = open_table("a.db");
	t2 = open_table("b.db");
	printf("open_table fin\n");

	for (int i = 0; i < 2000; ++i) {
		if(db_insert(t1, i, "1") != 0)
        {
            printf("db_insert() failed\n");
        }
        if(db_insert(t2, i, "1") != 0)
        {
            printf("db_insert() failed\n");
        }
	}
    printf("db insert() fin\n");

	shutdown_db();
    printf("shutdown_db fin\n");
    return 0;
}

// int main(int argc, char ** argv)
// {
//     int table_id;
//     char instruction;
//     int64_t key;
//     char value[120];
//     char datafile[20];

//     // initialize buffer and hash table
//     init_db(1000);

//     printf("first, open a datafile (o datafile)\n");
//     printf("> ");
//     while (scanf("%c", &instruction) != EOF)
//     {
//         switch (instruction)
//         {
//             case 'o':
//                 scanf("%s", datafile);
//                 table_id = open_table(datafile);
//                 if(table_id == -1)
//                 {
//                     printf("open_table() failed.\n");
//                 }
//                 break;
//             case 'c':
//                 scanf("%d", &table_id);
//                 close_table(table_id);
//                 break;
//             case 'i':
//                 scanf("%d %ld %s", &table_id, &key, value);
//                 db_insert(table_id, key, value);
//                 break;
//             case 'f':
//                 scanf("%d %ld", &table_id, &key);
//                 if(db_find(table_id, key, value) == 0)
//                 {
//                     printf("key : %ld, value : %s\n", key, value);
//                 }
//                 else
//                 {
//                     printf("record with key %ld doesn't exist in the tree\n", key);
//                 }
//                 break;
//             case 'q':
//                 while (getchar() != (int)'\n');
                
//                 if(shutdown_db() != 0)
//                 {
//                     printf("shutdown_db() failed\n");
//                 }
//                 return 0;
//                 break;
//             case 'p':
//                 scanf("%d", &table_id);
//                 print_tree(table_id);
//                 break;
//             case 'x':
//                 remove(datafile);
//                 return 0;
//                 break;
//             case 'd':
//                 scanf("%d %ld", &table_id, &key);
//                 if(db_delete(table_id, key) == 0)
//                 {
//                     printf("test_main : record with key %ld is deleted\n", key);
//                 }
//                 else
//                 {
//                     printf("test_main : record with key %ld doesn't exist in the tree\n", key);
//                 }
//                 break;
//             case 's':
//                 scanf("%d", &table_id);
//                 show_leaf_page_keys(table_id);
//                 break;
//             case 'b':
//                 show_buffer_info();
//                 break;
//             case 'l':
//                 show_lru_list();
//             default:
//                 break;
//         }
//         while (getchar() != (int)'\n');
//         printf("> ");
//     }
//     return 0;
// }

