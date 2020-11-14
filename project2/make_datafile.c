#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

int main()
{
    srand(time(NULL));
    FILE *fp;
    char key[10];
    char record[7];
    size_t len;
     //i 1 deok

    fp = fopen("datafile.bin", "w+");
    if(fp == NULL)
    {
        printf("file open failed()\n");
        exit(0);
    }
    for(int i = 1; i <= 5000; i++)
    {
        char input[30] = "";
        sprintf(key, "%d", i);
        strcat(input, key);
        len = strlen(key);
        input[len] = '\0';

        record[0] = ' ';
        record[1] = rand() % 26 + 'a';
        record[2] = rand() % 26 + 'a';
        record[3] = rand() % 26 + 'a';
        record[4] = rand() % 26 + 'a';
        record[5] = rand() % 26 + 'a';
        record[6] = '\0';
        strcat(input, record);
        fputs(input, fp);
        fputs("\n", fp);
    }

    fclose(fp);
    return 0;
}