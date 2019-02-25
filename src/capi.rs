// This module implements a C API for the S3 mapper.

use crate::userfaultfd::{mmap_with_userfault, MMap};
use crate::userfaultfd_s3::{MMapS3, S3Failure};
use libc::{c_char, c_int, c_void, size_t};
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::sync::RwLock;

// Keep in sync with mmapurl.h
const MMAP_S3_OK: c_int = 0;
const MMAP_S3_ERRNO: c_int = 1;
const MMAP_S3_IOERROR: c_int = 2;
const MMAP_S3_CONTENT_LENGTH_NOT_RETURNED: c_int = 3;
const MMAP_S3_NOT_FOUND: c_int = 4;
const MMAP_S3_PERMISSION_ERROR: c_int = 5;
const MMAP_S3_NO_BODY_RETURNED: c_int = 6;
const MMAP_S3_INVALID_S3URL: c_int = 7;
const MMAP_S3_UNKNOWN: c_int = 8;

const MMAP_S3_OK_STR: &'static [u8] = b"MMAP_S3_OK\0";
const MMAP_S3_ERRNO_STR: &'static [u8] = b"MMAP_S3_ERRNO\0";
const MMAP_S3_IOERROR_STR: &'static [u8] = b"MMAP_S3_IOERROR\0";
const MMAP_S3_CONTENT_LENGTH_NOT_RETURNED_STR: &'static [u8] =
    b"MMAP_S3_CONTENT_LENGTH_NOT_RETURNED\0";
const MMAP_S3_NOT_FOUND_STR: &'static [u8] = b"MMAP_S3_NOT_FOUND\0";
const MMAP_S3_PERMISSION_ERROR_STR: &'static [u8] = b"MMAP_S3_PERMISSION_ERROR\0";
const MMAP_S3_NO_BODY_RETURNED_STR: &'static [u8] = b"MMAP_S3_NO_BODY_RETURNED\0";
const MMAP_S3_INVALID_S3URL_STR: &'static [u8] = b"MMAP_S3_INVALID_S3URL\0";
const MMAP_S3_UNKNOWN_STR: &'static [u8] = b"MMAP_S3_UNKNOWN\0";

lazy_static! {
    // We need to keep track of pointers we have mapped so munmap_s3 knows which MMap handles
    // correspond to which pointers.
    static ref mmapped_s3s: RwLock<BTreeMap<u64, MMap<MMapS3>>> = RwLock::new(BTreeMap::new());
}

#[no_mangle]
pub extern "C" fn mmap_s3(url: *const c_char, sz: *mut size_t, err: *mut c_int) -> *const c_void {
    unsafe {
        let mut sz: *mut size_t = sz;
        let mut err: *mut c_int = err;
        let mut err_n: c_int = 0;
        let mut sz_n: size_t = 0;

        if sz as *const size_t == std::ptr::null::<size_t>() {
            sz = &mut sz_n;
        }

        if err as *const c_int == std::ptr::null::<c_int>() {
            err = &mut err_n;
        }

        *sz = 0;
        *err = MMAP_S3_OK;
        let s3url = CStr::from_ptr(url);
        let s3url = match s3url.to_str() {
            Err(_) => {
                *err = MMAP_S3_INVALID_S3URL;
                return libc::MAP_FAILED;
            }
            Ok(s3urlstr) => s3urlstr,
        }
        .to_owned();

        let result: Result<MMap<MMapS3>, Result<c_int, S3Failure>> = mmap_with_userfault(s3url);
        match result {
            Ok(mmapped) => {
                let mut mmapped_pointers = mmapped_s3s.write().unwrap();
                *sz = mmapped.len();
                let ptr = mmapped.as_ptr();
                mmapped_pointers.insert(ptr as u64, mmapped);
                return ptr;
            }
            Err(Ok(_errno)) => {
                *err = MMAP_S3_ERRNO;
            }
            Err(Err(s3failure)) => match s3failure {
                S3Failure::InvalidS3Url => *err = MMAP_S3_INVALID_S3URL,
                S3Failure::ContentLengthNotReturned => *err = MMAP_S3_CONTENT_LENGTH_NOT_RETURNED,
                S3Failure::NoBodyReturned => *err = MMAP_S3_NO_BODY_RETURNED,
                S3Failure::S3NotFound => *err = MMAP_S3_NOT_FOUND,
                S3Failure::S3PermissionError => *err = MMAP_S3_PERMISSION_ERROR,
                S3Failure::IOError => *err = MMAP_S3_IOERROR,
                _ => *err = MMAP_S3_UNKNOWN,
            },
        };
        libc::MAP_FAILED
    }
}

#[no_mangle]
pub extern "C" fn munmap_s3(ptr: *const c_void) -> c_int {
    let mut mmapped_pointers = mmapped_s3s.write().unwrap();
    if !mmapped_pointers.contains_key(&(ptr as u64)) {
        return -1;
    }

    mmapped_pointers.remove(&(ptr as u64));
    return 0;
}

#[no_mangle]
pub extern "C" fn mmap_s3_errstr(err: c_int) -> *const c_char {
    match err {
        MMAP_S3_OK => MMAP_S3_OK_STR,
        MMAP_S3_ERRNO => MMAP_S3_ERRNO_STR,
        MMAP_S3_IOERROR => MMAP_S3_IOERROR_STR,
        MMAP_S3_CONTENT_LENGTH_NOT_RETURNED => MMAP_S3_CONTENT_LENGTH_NOT_RETURNED_STR,
        MMAP_S3_NOT_FOUND => MMAP_S3_NOT_FOUND_STR,
        MMAP_S3_PERMISSION_ERROR => MMAP_S3_PERMISSION_ERROR_STR,
        MMAP_S3_NO_BODY_RETURNED => MMAP_S3_NO_BODY_RETURNED_STR,
        MMAP_S3_INVALID_S3URL => MMAP_S3_INVALID_S3URL_STR,
        _ => MMAP_S3_UNKNOWN_STR,
    }
    .as_ptr() as *const c_char
}
