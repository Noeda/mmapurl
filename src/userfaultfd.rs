use crate::mmaputil::{round_down_to_pagesize, round_up_to_pagesize, MMapPages, PAGESIZE_USIZE};
use libc::{c_int, c_long, c_void, size_t};
use rayon::ThreadPoolBuilder;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::mem;
use std::slice;
use std::sync::{Arc, RwLock};
use std::thread::{spawn, JoinHandle};

static NR_USERFAULTFD: c_long = 323;
static O_CLOEXEC: c_int = 0o2000000;
static O_NONBLOCK: c_int = 0o0004000;

// ioctl IDs
static UFFDIO_API: c_int = -1072125377;
static UFFDIO_REGISTER: c_int = -1071601152;
static UFFDIO_COPY: c_int = -1071076861;

static UFFD_API: u64 = 0xAA;
static UFFDIO_REGISTER_MODE_MISSING: u64 = 0x1;

static UFFD_EVENT_PAGEFAULT: u8 = 18;

const USERFAULT_MSG_SZ: isize = 32;

const MAX_CONCURRENT_WORKERS: usize = 16;

#[derive(Debug)]
pub struct MMap<M> {
    thread_handle: Option<JoinHandle<()>>,
    ptr_u64: u64,
    die: Arc<RwLock<bool>>,
    sz: size_t,
    sz_unrounded: size_t,
    ufd: c_int,
    mmap_state: M,
}

pub trait MMapHandler
where
    Self: Sized + Clone + 'static,
{
    type Argument;
    type Failure: Debug;
    type PageIterator: IntoIterator<Item = MMapPages>;

    fn new(arg: Self::Argument) -> Result<(Self, usize), Self::Failure>;
    fn handle_userfault(
        self,
        offset: u64,
    ) -> Result<(Self::PageIterator, BTreeSet<usize>), Self::Failure>;
}

#[repr(C)]
struct uffdio_api {
    api: u64,
    features: u64,
    ioctls: u64,
}

#[repr(C)]
struct uffdio_register {
    start: u64,
    len: u64,
    mode: u64,
    ioctls: u64,
}

#[repr(C)]
struct uffdio_copy {
    dst: u64,
    src: u64,
    len: u64,
    mode: u64,
    copy: u64,
}

impl uffdio_copy {
    fn new() -> Self {
        uffdio_copy {
            dst: 0,
            src: 0,
            len: 0,
            mode: 0,
            copy: 0,
        }
    }
}

#[derive(Copy, Clone)]
#[repr(packed)]
#[allow(non_camel_case_types)]
#[allow(dead_code)]
struct uffd_msg {
    event: u8,
    reserved1: u8,
    reserved2: u16,
    reserved3: u32,
    flags: u64,
    address: u64,
    padding: u64,
}

impl uffd_msg {
    fn new() -> Self {
        uffd_msg {
            event: 0,
            reserved1: 0,
            reserved2: 0,
            reserved3: 0,
            flags: 0,
            address: 0,
            padding: 0,
        }
    }
}

impl uffdio_register {
    fn new() -> Self {
        uffdio_register {
            start: 0,
            len: 0,
            mode: 0,
            ioctls: 0,
        }
    }
}

impl uffdio_api {
    fn new() -> Self {
        uffdio_api {
            api: UFFD_API,
            features: 0,
            ioctls: 0,
        }
    }
}

impl<M> Drop for MMap<M> {
    fn drop(&mut self) {
        {
            let mut die_val = self.die.write().unwrap();
            *die_val = true;
        };
        let handle = self.thread_handle.take();
        match handle {
            Some(handle) => {
                handle.join().unwrap();
            }
            None => (),
        }
        unsafe {
            libc::close(self.ufd);
            libc::munmap(self.ptr_u64 as *mut c_void, self.sz);
        }
    }
}

pub fn mmap_with_userfault<M: MMapHandler + Send>(
    arg: M::Argument,
) -> Result<MMap<M>, Result<c_int, M::Failure>> {
    let (mmap_state, nbytes) = match M::new(arg) {
        Err(fail) => return Err(Err(fail)),
        Ok((mmap_state, nbytes)) => (mmap_state, nbytes),
    };

    let nbytes_unrounded = nbytes;
    let nbytes = if nbytes == 0 { 1 } else { nbytes };
    let nbytes = round_up_to_pagesize(nbytes);

    let ufd: c_int = unsafe { libc::syscall(NR_USERFAULTFD, O_CLOEXEC | O_NONBLOCK) as c_int };
    if ufd == -1 {
        let err: c_int = unsafe { *libc::__errno_location() };
        return Err(Ok(err));
    }
    let uapi = uffdio_api::new();
    if unsafe { libc::ioctl(ufd, UFFDIO_API as u64, &uapi) } == -1 {
        let err: c_int = unsafe { *libc::__errno_location() };
        unsafe {
            libc::close(ufd);
        };
        return Err(Ok(err));
    }

    let ptr = unsafe {
        libc::mmap(
            std::ptr::null::<*const c_void>() as *mut c_void,
            nbytes,
            libc::PROT_READ,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        let err: c_int = unsafe { *libc::__errno_location() };
        unsafe {
            libc::close(ufd);
        };
        return Err(Ok(err));
    }

    let mut register = uffdio_register::new();
    register.start = ptr as u64;
    register.len = nbytes as u64;
    register.mode = UFFDIO_REGISTER_MODE_MISSING;

    if unsafe { libc::ioctl(ufd, UFFDIO_REGISTER as u64, &register) } == -1 {
        let err: c_int = unsafe { *libc::__errno_location() };
        unsafe {
            libc::munmap(ptr as *mut c_void, nbytes);
            libc::close(ufd);
        }
        return Err(Ok(err));
    }

    let die = Arc::new(RwLock::new(false));
    let die_thread = die.clone();
    // The Wrapper is a dance to send a pointer to a thread.
    // Rust resists sending pointers to threads without some rituals.
    let mmap_state_cloned = mmap_state.clone();
    let ptr_u64: u64 = ptr as u64;
    let thread_handle = spawn(move || {
        run_userfault_handler(ufd, die_thread, mmap_state_cloned, ptr_u64);
    });
    Ok(MMap {
        thread_handle: Some(thread_handle),
        ptr_u64,
        die: die,
        sz: nbytes,
        sz_unrounded: nbytes_unrounded,
        ufd,
        mmap_state,
    })
}

fn run_userfault_handler<M: MMapHandler + Send>(
    ufd: c_int,
    die: Arc<RwLock<bool>>,
    mmap_state: M,
    ptr_u64: u64,
) {
    let tpool = ThreadPoolBuilder::new()
        .num_threads(MAX_CONCURRENT_WORKERS)
        .build()
        .unwrap();

    tpool.scope(move |scope| run_userfault_handler_scoped(ufd, die, scope, mmap_state, ptr_u64));
}

fn run_userfault_handler_scoped<M: MMapHandler + Send>(
    ufd: c_int,
    die: Arc<RwLock<bool>>,
    scope: &rayon::Scope,
    mmap_state: M,
    ptr_u64: u64,
) {
    let mut userfault_msg = uffd_msg::new();

    let mut pfd = libc::pollfd {
        fd: ufd,
        events: libc::POLLIN,
        revents: 0,
    };

    loop {
        // Check if we are supposed to die.
        {
            let die_val = die.read().unwrap();
            if *die_val == true {
                return;
            }
        }

        // Poll for events. We specify timeout of 100ms and try again if timeout was reached. This
        // makes sure we check for the die flag in at most 100ms if it gets set by MMap Drop trait
        // and is waiting for our thread to go down.
        pfd.fd = ufd;
        pfd.events = libc::POLLIN;
        pfd.revents = 0;
        let pollresult = unsafe { libc::poll(&mut pfd, 1, 100) };
        if pollresult == -1 {
            let err: c_int = unsafe { *libc::__errno_location() };
            if err == libc::EINTR {
                continue;
            }
            // According to linux man apge, EAGAIN sometimes can happen on other POSIXes and
            // portable applications probably should treat it like EINTR.
            // It happens if Kernel is out resources.
            if err == libc::EAGAIN {
                continue;
            }
            // In any other condition...well we are in a bad state and no easy way to recover so
            // just panic.
            panic!(format!(
                "Unexpectedly received unrecoverable error from poll() syscall: {}",
                err
            ));
        }
        // timeout
        if pollresult == 0 {
            continue;
        }
        // pollresult should only be 1.
        // Check for strange results
        if pollresult != 1 {
            panic!(format!("Unexpectedly received return value {} from poll() syscall. I only expect 1 for successful poll() call.", pollresult));
        }

        // Check that this is a type of poll event we care about.
        if ((pfd.revents | libc::POLLIN) == 0) && ((pfd.revents | libc::POLLERR) == 0) {
            continue;
        }

        loop {
            let userfault_msg_ptr: *mut uffd_msg = &mut userfault_msg;
            let nread = unsafe {
                libc::read(
                    ufd,
                    userfault_msg_ptr as *mut c_void,
                    USERFAULT_MSG_SZ as usize,
                )
            };
            if nread == -1 {
                let err: c_int = unsafe { *libc::__errno_location() };
                if err == libc::EINTR {
                    continue;
                }
                panic!(format!("Unexpected result from read() syscall {}", err));
            }
            if nread == 0 {
                panic!(format!("Unexpected EOF from userfaultfd."));
            }
            if nread != USERFAULT_MSG_SZ {
                panic!(format!(
                    "Unexpected read size from read() syscall, expected {} bytes, got {} bytes.",
                    USERFAULT_MSG_SZ, nread
                ));
            }
            break;
        }

        // Check that the userfault message is what we expect
        if userfault_msg.event != UFFD_EVENT_PAGEFAULT {
            panic!(format!(
                "Unexpected userfaultfd event type, we only expect UFFD_EVENT_PAGEFAULT"
            ));
        }

        // If we are at this point, we have successfully received a request to fill in some page.
        // We send the request to our thread pool to deal with.
        let mmap_state_cloned = mmap_state.clone();
        scope.spawn(move |_scope| pagefault_handle(ufd, userfault_msg, mmap_state_cloned, ptr_u64));
    }
}

fn pagefault_handle<M: MMapHandler + Send>(ufd: c_int, msg: uffd_msg, mmap_state: M, ptr_u64: u64) {
    let offset_ptr = round_down_to_pagesize(msg.address as usize) as u64;
    let offset = offset_ptr - ptr_u64;
    let (pages, evictions) = mmap_state.handle_userfault(offset).unwrap();

    let mut uffdio_copy = uffdio_copy::new();
    for page in pages {
        loop {
            uffdio_copy.src = page.vehicle_page as u64;
            uffdio_copy.dst = offset_ptr;
            uffdio_copy.len = page.mmapped_size;
            uffdio_copy.mode = 0;
            uffdio_copy.copy = 0;
            if unsafe { libc::ioctl(ufd, UFFDIO_COPY as u64, &uffdio_copy) } == -1 {
                let err: c_int = unsafe { *libc::__errno_location() };
                if err == libc::EAGAIN {
                    continue;
                }
                // EEXIST isn't even documented as possible return from UFFDIO_COPY.
                //
                // I think you get this when the pages are already loaded. Seems like tests pass (all
                // data is there properly) even when you get this so I'm hoping really hard it's fine
                // to ignore EEXIST.
                if err == libc::EEXIST {
                    break;
                }
                panic!(format!(
                    "Unexpected error from ioctl() syscall while copying page with userfaultfd. {}",
                    err
                ));
            }
        }
    }

    for eviction_page in evictions.into_iter() {
        let evict_offset = eviction_page * *PAGESIZE_USIZE + ptr_u64 as usize;
        let ret = unsafe {
            libc::madvise(
                evict_offset as *mut c_void,
                *PAGESIZE_USIZE,
                libc::MADV_DONTNEED,
            )
        };
        if ret == -1 {
            let err: c_int = unsafe { *libc::__errno_location() };
            panic!(format!(
                "Unexpected error from madvise() with MADV_FREE. {}",
                err
            ));
        }
    }
}

impl<M> MMap<M> {
    pub fn as_ptr<T>(&self) -> *const T {
        self.ptr_u64 as *const T
    }

    pub fn as_slice<T>(&self) -> &[T] {
        let typesize = mem::size_of::<T>();
        let rem = self.sz_unrounded % typesize;
        if rem != 0 {
            panic!(format!("MMap::as_slice called with type parameter that does not evenly divide the file size. File size is {}, type size is {}, remainder is {}.", self.sz_unrounded, typesize, rem));
        }
        unsafe { slice::from_raw_parts(self.ptr_u64 as *const T, self.sz_unrounded / typesize) }
    }

    pub fn len(&self) -> usize {
        self.sz_unrounded
    }
}
