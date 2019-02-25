#![feature(bind_by_move_pattern_guards)]

#[macro_use]
extern crate lazy_static;
extern crate libc;
extern crate rand;
extern crate regex;

mod capi;
mod heuristics;
mod mmaputil;
mod userfaultfd;
mod userfaultfd_dummy;
mod userfaultfd_s3;

pub use crate::userfaultfd::mmap_with_userfault;
pub use crate::userfaultfd_dummy::MMapDummy;
pub use crate::userfaultfd_s3::MMapS3;
