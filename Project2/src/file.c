#include "file.h"
#include <stdlib.h>
#include <unistd.h>

//ssize_t read (int fd, void *buf, size_t len)
//off_t lseek(int fd, off_t offset, int whence)
extern uint64_t fd;

// first, find free pages from free page list.
// if free pages don't exist, extend the file and make new free pages
pagenum_t file_alloc_page()
{
    page_t buf;
    int ret;

    //make offset to 0..

    ret = read(fd, &buf, sizeof(buf));

    if(ret == -1)
    {
        printf("read() failed\n");
    }


}
void file_free_page(pagenum_t pagenum)
{

}
void file_read_page(pagenum_t pagenum, page_t * dest)
{

}
void file_write_page(pagenum_t pagenum, const page_t * src)
{

}
