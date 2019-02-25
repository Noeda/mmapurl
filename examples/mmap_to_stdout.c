/*
 * This example program memory maps an S3 object and copies its entire
 * contents to stdout, by using write() syscall directly on the pointer.
 */

#include "mmapurl.h"
#include <sys/mman.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

int main(int argc, char** argv)
{
    if (argc != 2) {
        fprintf(stderr, "Exactly one argument expected: S3 URL\n");
        return -1;
    }

    int err;
    size_t sz;

    const void* ptr = mmap_s3(argv[1], &sz, &err);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "Mapping failed: %s\n", mmap_s3_errstr(err));
        return -1;
    }

    size_t cursor = 0;
    while(cursor < sz) {
        ssize_t written = write(1, &((char*) ptr)[cursor], sz - cursor);
        if (written == -1) {
            fprintf(stderr, "write() failed: %s\n", strerror(errno));
            return -1;
        }
        cursor += written;
    }

    munmap_s3(ptr);
    return 0;
}
