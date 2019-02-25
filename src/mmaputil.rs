use libc::{c_int, c_void, size_t};
use std::slice;

lazy_static! {
    pub static ref PAGESIZE_U64: u64 = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    pub static ref PAGESIZE_USIZE: usize = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
}

pub fn round_up_to_pagesize(nbytes: size_t) -> size_t {
    if nbytes % *PAGESIZE_USIZE == 0 {
        return nbytes;
    }
    nbytes + (*PAGESIZE_USIZE - (nbytes % *PAGESIZE_USIZE))
}

pub fn round_down_to_pagesize(nbytes: size_t) -> size_t {
    if nbytes % *PAGESIZE_USIZE == 0 {
        return nbytes;
    }
    nbytes - (nbytes % *PAGESIZE_USIZE)
}

// This is a small struct+trait combination to make it easy to quickly ask for a bit of mmapped
// space.
pub struct MMapPages {
    pub vehicle_page: *const c_void,
    pub mmapped_size: u64,
    pub do_unmap: bool,
}

impl MMapPages {
    pub fn new(nbytes: u64) -> Self {
        let nbytes = round_up_to_pagesize(nbytes as usize);
        let vehicle_page = unsafe {
            libc::mmap(
                std::ptr::null::<*const c_void>() as *mut c_void,
                nbytes as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if vehicle_page == libc::MAP_FAILED {
            let err: c_int = unsafe { *libc::__errno_location() };
            panic!(format!("Failed mmap() while handling a userfault. {}", err));
        }
        MMapPages {
            vehicle_page,
            mmapped_size: nbytes as u64,
            do_unmap: true,
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.vehicle_page as *mut u8, self.mmapped_size as usize)
        }
    }
}

impl Drop for MMapPages {
    fn drop(&mut self) {
        if self.do_unmap {
            unsafe {
                libc::munmap(self.vehicle_page as *mut c_void, self.mmapped_size as usize);
            }
        }
    }
}
