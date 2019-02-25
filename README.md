mmapurl
-------

This is a library that can memory map S3 URLs.

```
size_t sz;
int err;
void* ptr = mmap_s3("s3://path/to/file", &sz, &err);
// Now do stuff with ptr
```

The implementation is based on [Linux userfaultfd](http://man7.org/linux/man-pages/man2/userfaultfd.2.html) feature.

When you touch the pages that have been memory mapped, it will trigger a
download from S3 for the pages you touched. Only pages you actually read will
be downloaded.

# Documentation

For C API, I recommend looking inside `src/mmapurl.h` in this repository which
is commented. There are only three functions: `mmap_s3`, `munmap_s3` and
`mmap_s3_errstr`. You can also look at `examples/mmap_to_stdout.c` to see the
code in use.

The Rust API is not as well documented but this example should get you started:

```rust
extern crate mmapurl;

use mmapurl::{mmap_with_userfault, MMap, MMapS3};

fn main() {
    let mmapped: MMap<MMapS3> = mmapurl::mmap_with_userfault("s3://path/to/file".to_owned()).unwrap();
    let slice: &[u8] = mmapped.as_slice();
    // Do stuff with 'slice'.
}
```

The memory mapping is automatically unmapped with `Drop` traits so you
shouldn't be able to shoot yourself in the foot easily.

# Install

## Prerequisites

* mmapurl will only work on 64-bit Linux. (because it uses Linux-specific APIs).
* userfaultfd must be enabled. You can try `zcat /proc/config.gz|grep USERFAULTFD` to check this.

## Compilation

This library is written in Rust but it exposes a C API.

You will need Rust ecosystem to compile this. Get yourself a working
[Rust compiler and cargo](https://www.rust-lang.org/tools/install).

As of writing of this `cargo` does not really deal with installing C-compatible
libraries. I have included a crude `Makefile` that will compile the library and
optionally install it.

To compile and install C-compatible library and its header files, invoke two
`make`s:

    make
    make install PREFIX=/usr/local

If you are just going to use this from Rust then this is just a simple cargo
package you can use like any other Rust package.

## Using it

If you have the library installed, you can now try using it. I recommend
checking that the example C program works:

    cd examples
    gcc -O3 mmap_to_stdout.c -o mmap_to_stdout -lmmapurl
    ./mmap_to_stdout s3://path/to/some/file > file

## Heuristics

This library implements some heuristics to read larger pieces if it detects
sequential-like reading.

Pages are evicted if too many have been loaded at once: this makes sure memory
will not grow unboundedly even if the S3 object is enormous.

## Caveats

  * Performance is not great. Currently, this library will not initiate
    background downloads: all downloads are done synchronously. When a page
    that has not been populated is being read, the thread trying to do the read
    will be put to sleep, download is initiated and thread will resume only
    after download is complete. This downside may be mitigated in a future
    version.

  * If anything goes wrong with downloading from S3 *after* memory mapping has
    been established, the library will call `abort()`. For example, network
    connection going down will also kill your application. You should not use
    this library in anything that must not die in conditions like this.
