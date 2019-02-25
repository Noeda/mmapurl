// This module implements all logic related to read-ahead reading and booting out old pages.
//
// This module has been written to be completely agnostic of actual page loading gritty details.
//
/*
 * We slice the S3 file into slices, on two levels. Lower level is sliced to 256 kilobytes. Higher
 * level is sliced to 256*128 = ~32 megabytes.
 *
 * We track manually which pages have been downloaded. If we get a request that would fill a slice,
 * we instead do a bigger read that will read ahead rather than just the page that was requested.
 *
 * For 256kb sized slices, we read ahead 16 extra sizes (~4 megabytes).
 * For 32MB sized slices, we read ahead 2 extra slices (~64 megabytes).
 *
 * All these numbers are configurable by tuning the knobs below.
 */

use crate::mmaputil::PAGESIZE_USIZE;
use std::cmp;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// These are multiplied by page which is 4096 bytes.
const LEVEL1_SLICE_SIZE: usize = 64; // 262144 (~256kb)
const LEVEL2_SLICE_SIZE: usize = 8192; // 33554432 (~32mb)
const LEVEL1_READAHEAD: usize = 16;
const LEVEL2_READAHEAD: usize = 2;
const MAX_LOADED_PAGES: usize = 32768; // ~128 megabytes, should be larger than level2 readahead
const NUM_PAGES_TO_GO_BELOW_MAX_LOADED_PAGES_ON_EVICT: usize = 500; // How many pages to go below MAX_LOADED_PAGES when too many pages have been loaded. 500 = ~2 megabytes.

pub struct PageHeuristics {
    level1slices: BTreeMap<usize, Slice>,
    level2slices: BTreeMap<usize, Slice>,

    evict_queue: VecDeque<usize>,
}

impl PageHeuristics {
    pub fn new() -> Self {
        PageHeuristics {
            level1slices: BTreeMap::new(),
            level2slices: BTreeMap::new(),
            evict_queue: VecDeque::new(),
        }
    }

    // This records that some pages have been read.
    // The range is not inclusive so 'end_page' itself is not included.
    pub fn mark_pages_as_read(&mut self, start_page: usize, end_page: usize) {
        for pagenum in start_page..end_page {
            self.evict_queue.push_back(pagenum);

            let slice1num = pagenum / LEVEL1_SLICE_SIZE;
            let slice2num = pagenum / LEVEL2_SLICE_SIZE;
            let slice1page = pagenum % LEVEL1_SLICE_SIZE;
            let slice2page = pagenum % LEVEL2_SLICE_SIZE;

            {
                let slice1entry = self.level1slices.entry(slice1num);
                let s1e = slice1entry.or_insert_with(|| Slice::new(LEVEL1_SLICE_SIZE));
                s1e.add_page(slice1page);
            }
            {
                let slice2entry = self.level2slices.entry(slice2num);
                let s2e = slice2entry.or_insert_with(|| Slice::new(LEVEL2_SLICE_SIZE));
                s2e.add_page(slice2page);
            }
        }
    }

    // Checks how many pages have been loaded if it's too many, evicts pages.
    //
    // It puts the pages it wants to evict in the given BTreeSet.
    pub fn evict_pages_if_needed(&mut self, evictions: &mut BTreeSet<usize>) {
        if self.evict_queue.len() > MAX_LOADED_PAGES {
            // evict so that we are 100 pages below maximum
            for _ in 0..(self.evict_queue.len() - MAX_LOADED_PAGES
                + NUM_PAGES_TO_GO_BELOW_MAX_LOADED_PAGES_ON_EVICT)
            {
                let page_evict = self.evict_queue.pop_front().unwrap();
                let slice1num = page_evict / LEVEL1_SLICE_SIZE;
                let slice2num = page_evict / LEVEL2_SLICE_SIZE;
                let slice1page = page_evict % LEVEL1_SLICE_SIZE;
                let slice2page = page_evict % LEVEL2_SLICE_SIZE;
                {
                    let slice1entry = self.level1slices.entry(slice1num);
                    let s1e = slice1entry.or_insert_with(|| Slice::new(LEVEL1_SLICE_SIZE));
                    s1e.remove_page(slice1page);
                }
                {
                    let slice2entry = self.level2slices.entry(slice2num);
                    let s2e = slice2entry.or_insert_with(|| Slice::new(LEVEL2_SLICE_SIZE));
                    s2e.remove_page(slice2page);
                }

                evictions.insert(page_evict);
            }
        }
    }

    // Same as evict_pages_if_needed but returns a BTreeSet instead of mutating one.
    pub fn evict_pages_if_needed2(&mut self) -> BTreeSet<usize> {
        let mut evictions = BTreeSet::new();
        self.evict_pages_if_needed(&mut evictions);
        evictions
    }

    // Checks if we can do a read-ahead heuristic.
    //
    // You give this function the offset and the number of bytes you want to read at minimum (which is usually 1
    // page, so 4096 bytes) and this function may or may not tell you to read a lot more.
    //
    // Caution: the heuristics doesn't know how large the actual underlying resource is so this can
    // tell you to read more data than there is. Just check the value against actual size and cap
    // it off as needed. As long as you use mark_pages_as_read() with the actual pages you read the
    // heuristics will be in good staet.
    pub fn readahead_heuristic(&mut self, offset: usize, actual_read_sz: usize) -> usize {
        let mut actual_read_sz = actual_read_sz;
        let slice1num = offset / *PAGESIZE_USIZE / LEVEL1_SLICE_SIZE;
        let slice2num = offset / *PAGESIZE_USIZE / LEVEL2_SLICE_SIZE;
        let slice1page = (offset / *PAGESIZE_USIZE) % LEVEL1_SLICE_SIZE;
        let slice2page = (offset / *PAGESIZE_USIZE) % LEVEL2_SLICE_SIZE;
        // Would we fill a small slice?
        {
            let slice1entry = self.level1slices.entry(slice1num);
            let s1e = slice1entry.or_insert_with(|| Slice::new(LEVEL1_SLICE_SIZE));
            if s1e.would_fill(slice1page) {
                actual_read_sz = extend_readahead1(offset, *PAGESIZE_USIZE);
            }
        }

        // Would we fill a big slice?
        {
            let slice2entry = self.level2slices.entry(slice2num);
            let s2e = slice2entry.or_insert_with(|| Slice::new(LEVEL2_SLICE_SIZE));
            if s2e.would_fill(slice2page) {
                actual_read_sz = cmp::max(
                    actual_read_sz,
                    LEVEL2_READAHEAD * LEVEL2_SLICE_SIZE * *PAGESIZE_USIZE,
                );
                actual_read_sz = roundup_slice1(offset, actual_read_sz);
            }
        }
        actual_read_sz
    }
}

struct Slice {
    loaded_pages: BTreeSet<usize>,
    num_pages: usize,
}

impl Slice {
    fn new(num_pages: usize) -> Self {
        Slice {
            loaded_pages: BTreeSet::new(),
            num_pages,
        }
    }

    fn remove_page(&mut self, page_number: usize) {
        self.loaded_pages.remove(&page_number);
    }

    fn add_page(&mut self, page_number: usize) {
        self.loaded_pages.insert(page_number);
    }

    fn would_fill(&self, page_number: usize) -> bool {
        if self.loaded_pages.contains(&page_number) {
            return false;
        }

        if self.loaded_pages.len() + 1 == self.num_pages {
            return true;
        }

        false
    }
}

// Given an offset and a read size, extend the read size until level1 read-ahead is met.
//
// Twist: extend the read-ahead so that if we read the next page, we immediately trigger a second
// read-ahead. We do this by extending the read-ahead so that we almost completely read the next
// slice in full as well.
//
// If level2 slice lines up well we may extend to that instead.
fn extend_readahead1(offset: usize, minsz: usize) -> usize {
    // this is the value if we just straight up extend with level1 readahead size
    let actual_read_sz = LEVEL1_READAHEAD * LEVEL1_SLICE_SIZE * *PAGESIZE_USIZE;
    // but what if we extended to next level2 boundary? (so next read will trigger level2
    // read-ahead)
    let minsz_page = (offset + minsz - 1) / *PAGESIZE_USIZE;
    let minsz_level2_page = minsz_page % LEVEL2_SLICE_SIZE;
    if minsz_level2_page == LEVEL2_SLICE_SIZE - 2 {
        return minsz;
    } else if minsz_level2_page < LEVEL2_SLICE_SIZE - 2 {
        let missing_pages = (LEVEL2_SLICE_SIZE - 2) - minsz_level2_page;
        let new_sz = minsz + missing_pages * *PAGESIZE_USIZE;
        // Round to level2 boundary if the amount of reading would be less than level1 readahead
        if new_sz <= LEVEL1_READAHEAD * LEVEL1_SLICE_SIZE * *PAGESIZE_USIZE {
            return new_sz;
        }
    }
    roundup_slice1(offset, actual_read_sz)
}

fn roundup_slice1(offset: usize, sz: usize) -> usize {
    let final_page = (offset + sz - 1) / *PAGESIZE_USIZE;
    let level1_page = final_page % LEVEL1_SLICE_SIZE;
    if level1_page == LEVEL1_SLICE_SIZE - 2 {
        sz
    } else if level1_page == LEVEL1_SLICE_SIZE - 1 {
        sz + (LEVEL1_SLICE_SIZE - 1) * *PAGESIZE_USIZE
    } else {
        let missing_pages = (LEVEL1_SLICE_SIZE - 2) - level1_page;
        sz + missing_pages * *PAGESIZE_USIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundup_slice1_tests() {
        // 1 page at 0th offset should get extended to just below 1 slice size.
        assert_eq!(roundup_slice1(0, 4096), 4096 * (LEVEL1_SLICE_SIZE - 1));
        // 1 page at 1th offset should get extended to just below 1 slice size, minus the one page
        //   we have with offset.
        assert_eq!(roundup_slice1(4096, 4096), 4096 * (LEVEL1_SLICE_SIZE - 2));
        // Non-page aligned read
        assert_eq!(roundup_slice1(1111, 4096), 4096 * (LEVEL1_SLICE_SIZE - 2));

        // After one slice read
        assert_eq!(
            roundup_slice1(LEVEL1_SLICE_SIZE * 4096, 4096),
            4096 * (LEVEL1_SLICE_SIZE - 1)
        );
    }
}
