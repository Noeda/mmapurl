#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// These are possible errors that can happen with mmap_s3
#define MMAP_S3_OK               0    // everything went okay
#define MMAP_S3_ERRNO            1    // some syscall failed, use "errno" to check how
#define MMAP_S3_NOT_FOUND        4    // bucket or key not found
#define MMAP_S3_PERMISSION_ERROR 5    // we are not allowed to read from S3
#define MMAP_S3_INVALID_S3URL    7    // the S3 url is invalid

// These errors are defined but they should never happen; only if our
// library is buggy or S3 is not conforming to its protocol in some way.
#define MMAP_S3_IOERROR          2    // I/O error while downloading
#define MMAP_S3_CONTENT_LENGTH_NOT_RETURNED 3 // No content length returned from S3
#define MMAP_S3_NO_BODY_RETURNED 6    // S3 GET request didn't include body
#define MMAP_S3_UNKNOWN          7    // Some error happened we have not categorized.

// Memory maps an S3 object. Returns a pointer to it or MAP_FAILED. Reading
// from the pointer will trigger downloads from S3 on-demand.
//
// If 'err' is not NULL, an error code of MMAP_S3_* is filled into it if
// something bad happens.
//
// If 'sz' is not NULL, it will be filled with the size of the mapping. You
// probably really want to use it or you won't know how far returned
// pointer will be valid.
//
// If any errors happen while you are touching the returned pages, the
// program will call abort().
//
// The returned pointer is read-only.
//
// Unmap the region with munmap_s3().
const void* mmap_s3(const char* s3url, size_t* sz, int* err);

// Unmaps a region previously mapped with mmap_s3().
//
// Returns -1 if the pointer is unrecognized and then does nothing.
// Otherwise returns 0 and the memory is released.
//
// Note: this call may take some time to complete because it has to stop
// threads that are potentially quite busy. Usually it's just hundreds of
// milliseconds.
int munmap_s3(const void* ptr);

// Takes an error code and turns it into a string that can be displayed.
const char* mmap_s3_errstr(int err);

#ifdef __cplusplus
} // extern "C"
#endif
