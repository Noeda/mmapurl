/* This module implements a dummy userfaultfd handler, to be used for testing/debugging purposes.
 *
 * The dummy fills the memory with bytes that are predictable by their offset from start of
 * mapping.
 *
 * byte = (offset * 13) & 0xFF
 *
 * It uses the same read-ahead heuristics as userfaultfd_s3.
 */

use crate::heuristics::PageHeuristics;
use crate::mmaputil::{round_up_to_pagesize, MMapPages, PAGESIZE_U64, PAGESIZE_USIZE};
use crate::userfaultfd::MMapHandler;
use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct MMapDummy {
    state: Arc<RwLock<MMapDummyState>>,
    sz: usize,
}

struct MMapDummyState {
    heuristics: PageHeuristics,
}

pub struct DummyPageIterator {
    base_page: MMapPages,
    cursor: usize,
    offset: usize,
}

impl Iterator for DummyPageIterator {
    type Item = MMapPages;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.base_page.mmapped_size as usize {
            let slice = self.base_page.as_mut_slice();
            for i in self.cursor..self.cursor + *PAGESIZE_USIZE {
                slice[i] = (((i + self.offset) * 13) & 0xFF) as u8;
            }
            let ret = unsafe {
                MMapPages {
                    vehicle_page: self.base_page.vehicle_page.add(self.cursor),
                    mmapped_size: *PAGESIZE_U64,
                    do_unmap: false,
                }
            };
            self.cursor += *PAGESIZE_USIZE;
            Some(ret)
        } else {
            None
        }
    }
}

impl MMapHandler for MMapDummy {
    type Argument = usize;
    type Failure = ();
    type PageIterator = DummyPageIterator;

    fn new(size: Self::Argument) -> Result<(Self, usize), Self::Failure> {
        Ok((
            MMapDummy {
                sz: size,
                state: Arc::new(RwLock::new(MMapDummyState {
                    heuristics: PageHeuristics::new(),
                })),
            },
            size,
        ))
    }

    fn handle_userfault(
        self,
        offset: u64,
    ) -> Result<(Self::PageIterator, BTreeSet<usize>), Self::Failure> {
        let offset = offset as usize;
        let actual_read_sz = {
            let mut stw = self.state.write().unwrap();
            stw.heuristics.readahead_heuristic(offset, *PAGESIZE_USIZE)
        };

        let len = round_up_to_pagesize(if offset + actual_read_sz > self.sz {
            self.sz - offset
        } else {
            actual_read_sz
        });

        let page = MMapPages::new(len as u64);

        let evictions = {
            let mut stw = self.state.write().unwrap();
            stw.heuristics.mark_pages_as_read(
                offset / *PAGESIZE_USIZE,
                (offset + page.mmapped_size as usize) / *PAGESIZE_USIZE,
            );
            stw.heuristics.evict_pages_if_needed2()
        };

        Ok((
            DummyPageIterator {
                base_page: page,
                cursor: 0,
                offset,
            },
            evictions,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::userfaultfd::*;
    use rand::{seq::SliceRandom, thread_rng};

    fn expect_byte(byte: u8, offset: usize) {
        assert_eq!(byte, ((offset * 13) & 0xFF) as u8);
    }

    #[test]
    fn one_page_test() {
        let mmapped: MMap<MMapDummy> = mmap_with_userfault(4096).unwrap();
        let slice = mmapped.as_slice();
        assert_eq!(slice.len(), 4096);
        for i in 0..slice.len() {
            expect_byte(slice[i], i);
        }
    }

    #[test]
    fn t32000_page_test() {
        let mmapped: MMap<MMapDummy> = mmap_with_userfault(4096 * 32000).unwrap();
        let slice = mmapped.as_slice();
        assert_eq!(slice.len(), 4096 * 32000);
        for i in 0..slice.len() {
            expect_byte(slice[i], i);
        }
    }

    #[test]
    fn t32000_page_test_interleaved() {
        let mmapped: MMap<MMapDummy> = mmap_with_userfault(4096 * 32000).unwrap();
        let slice = mmapped.as_slice();
        assert_eq!(slice.len(), 4096 * 32000);
        for i in 0..slice.len() {
            expect_byte(slice[i], i);
        }
        for i in 0..slice.len() {
            expect_byte(slice[i], i);
        }
        for i in 0..slice.len() {
            expect_byte(slice[i], i);
        }
    }

    #[test]
    fn t32000_page_test_randomly_loaded() {
        let mmapped: MMap<MMapDummy> = mmap_with_userfault(4096 * 32000).unwrap();
        let slice = mmapped.as_slice();
        let mut pages = Vec::with_capacity(32000);
        for page in 0..32000 {
            pages.push(page);
        }
        pages.shuffle(&mut thread_rng());

        for page in pages {
            for i in (page * 4096)..(page + 1) * 4096 {
                expect_byte(slice[i], i);
            }
        }
    }

    #[test]
    fn zero_page_test() {
        let mmapped: MMap<MMapDummy> = mmap_with_userfault(0).unwrap();
        let slice: &[u8] = mmapped.as_slice();
        assert_eq!(slice.len(), 0);
    }
}
