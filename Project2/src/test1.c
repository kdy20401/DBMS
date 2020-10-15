#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>

int main()
{
    char a[120];
    strcpy(a, "hello!");
    printf("%s\n", a);
    strcpy(a, "");
    printf("%s\n", a);
    printf("sizeof a : %ld", sizeof(a));

    return 0;
}