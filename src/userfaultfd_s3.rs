/* This module implements S3 handler for the userfaultfd mmapper.
 *
 * There is a simple read-ahead heuristic to make it so that scans are reasonably fast. Look into
 * heuristics.rs for more details on that.
 *
 * TODO items:
 *   * When a large read-ahead is initiated, no pages are returned to the user until the entire
 *     read is done. I noticed when I changed this to return pages immediately, things overall will
 *     slow down rather than speed up. Possible solutions: return N pages at a time.
 *
 *   * In scenarios with multiple threads, I think it's possible to have multiple S3 downloads
 *     going on at the same time on the same file with overlapping regions. This is wasted bandwidth. Possible solutions: keep track of which downloads are going on and if there is an overlapping one, wait for the overlapping download to complete while downloading only the non-overlapping part. Return pages when both downloads are complete.
 *
 */

use crate::heuristics::PageHeuristics;
use crate::mmaputil::{round_up_to_pagesize, MMapPages, PAGESIZE_USIZE};
use crate::userfaultfd::MMapHandler;
use libc::c_void;
use regex::Regex;
use rusoto_core::{region::ParseRegionError, Region};
use rusoto_s3::{
    GetBucketLocationError, GetBucketLocationRequest, GetObjectError, GetObjectRequest,
    HeadObjectError, HeadObjectRequest, S3Client, S3,
};
use std::cmp;
use std::collections::BTreeSet;
use std::convert::From;
use std::io;
use std::io::Read;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

lazy_static! {
    // this splits s3 url to bucket and key
    static ref s3split_re: Regex = Regex::new("^s3://([^/]+)/(.+)$").unwrap();
}

fn get_some_s3client() -> S3Client {
    S3Client::new(Region::UsEast1)
}

fn get_s3client_by_location(location: Option<String>) -> Result<S3Client, ParseRegionError> {
    match location {
        None => Ok(get_some_s3client()),
        Some(region_name) => {
            let region = Region::from_str(&region_name)?;
            Ok(S3Client::new(region))
        }
    }
}

fn split_s3_url(url: &str) -> Option<(String, String)> {
    match s3split_re.captures(url) {
        None => None,
        Some(capts) => Some((capts[1].to_owned(), capts[2].to_owned())),
    }
}

#[derive(Clone)]
pub struct MMapS3 {
    state: Arc<RwLock<MMapS3State>>,
}

struct MMapS3State {
    bucket_name: String,
    key_name: String,
    s3client: S3Client,
    s3objectsize: usize,
    heuristics: PageHeuristics,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub enum S3Failure {
    InvalidS3Url,             // Cannot parse S3 url
    ContentLengthNotReturned, // We did HEAD request on S3 object but it didn't return content size.
    NoBodyReturned,           // We did a GET request but there is no body.
    ParseRegionError,         // We can't understand the bucket's region name.
    S3NotFound,               // Object not found
    S3PermissionError,        // 403 from S3
    IOError,                  // I/O error while downloading from S3
    Unknown,                  // Error we can't quite categorize
    PartialRead,              // We made a GET request but the returned body seems incomplete
}

impl From<GetBucketLocationError> for S3Failure {
    fn from(gble: GetBucketLocationError) -> Self {
        match gble {
            GetBucketLocationError::Unknown(resp) if resp.status.as_u16() == 403 => {
                S3Failure::S3PermissionError
            }
            GetBucketLocationError::Unknown(resp) if resp.status.as_u16() == 404 => {
                S3Failure::S3NotFound
            }
            _ => S3Failure::Unknown,
        }
    }
}

impl From<ParseRegionError> for S3Failure {
    fn from(_: ParseRegionError) -> Self {
        S3Failure::ParseRegionError
    }
}

impl From<HeadObjectError> for S3Failure {
    fn from(hoe: HeadObjectError) -> Self {
        match hoe {
            HeadObjectError::NoSuchKey(_) => S3Failure::S3NotFound,
            HeadObjectError::Unknown(resp) if resp.status.as_u16() == 403 => {
                S3Failure::S3PermissionError
            }
            HeadObjectError::Unknown(resp) if resp.status.as_u16() == 404 => S3Failure::S3NotFound,
            _ => S3Failure::Unknown,
        }
    }
}

impl From<GetObjectError> for S3Failure {
    fn from(goe: GetObjectError) -> Self {
        match goe {
            GetObjectError::NoSuchKey(_) => S3Failure::S3NotFound,
            GetObjectError::Unknown(resp) if resp.status.as_u16() == 403 => {
                S3Failure::S3PermissionError
            }
            GetObjectError::Unknown(resp) if resp.status.as_u16() == 404 => S3Failure::S3NotFound,
            err => {
                println!("{:?}", err);
                S3Failure::Unknown
            }
        }
    }
}

impl From<io::Error> for S3Failure {
    fn from(_: io::Error) -> Self {
        S3Failure::IOError
    }
}

impl MMapHandler for MMapS3 {
    type Argument = String;
    type Failure = S3Failure;
    type PageIterator = Vec<MMapPages>;

    fn new(url: Self::Argument) -> Result<(Self, usize), Self::Failure> {
        let (bucket_name, key_name) = match split_s3_url(&url) {
            None => return Err(S3Failure::InvalidS3Url),
            Some((bucket_name, key_name)) => (bucket_name, key_name),
        };

        let s3client = get_some_s3client();
        let location = s3client
            .get_bucket_location(GetBucketLocationRequest {
                bucket: bucket_name.clone(),
            })
            .sync()?;

        let s3client = get_s3client_by_location(location.location_constraint)?;

        // We need to know the size of the S3 file to know how much memory to map. So we do a HEAD
        // request for it.

        let mut hor = HeadObjectRequest::default();
        hor.bucket = bucket_name.clone();
        hor.key = key_name.clone();

        let hob = s3client.head_object(hor).sync()?;

        let content_length = match hob.content_length {
            None => return Err(S3Failure::ContentLengthNotReturned),
            Some(cl) => cl,
        };

        // MMapping 0 bytes doesn't work so round it up one byte.
        let content_length = if content_length == 0 {
            1
        } else {
            content_length
        };

        Ok((
            MMapS3 {
                state: Arc::new(RwLock::new(MMapS3State {
                    s3client,
                    bucket_name,
                    key_name,
                    s3objectsize: content_length as usize,
                    heuristics: PageHeuristics::new(),
                })),
            },
            content_length as usize,
        ))
    }

    fn handle_userfault(
        self,
        offset: u64,
    ) -> Result<(Self::PageIterator, BTreeSet<usize>), Self::Failure> {
        let offset = offset as usize;
        // Figure out how much we should actually read.
        // This will be just pagesize if we don't do any read-ahead.
        let actual_read_sz = {
            let mut stw = self.state.write().unwrap();
            stw.heuristics.readahead_heuristic(offset, *PAGESIZE_USIZE)
        };

        assert!((actual_read_sz % *PAGESIZE_USIZE) == 0);

        let page = {
            let st = self.state.read().unwrap();
            // Don't read more data than there is in the S3 object.
            let len = if offset + actual_read_sz > st.s3objectsize as usize {
                st.s3objectsize - offset
            } else {
                actual_read_sz
            };
            // TODO: there's quite a bit of copying involved when downloading from S3.
            // There are probably some clever ways to download directly to mmapped pages.
            //
            // In here we have 'data' in its own vector, which we copy.
            let data: Vec<u8> = fetch_range(
                &st.s3client,
                st.bucket_name.clone(),
                st.key_name.clone(),
                offset as usize,
                len,
            )?;
            let page = MMapPages::new(cmp::min(
                round_up_to_pagesize(len) as u64,
                actual_read_sz as u64,
            ));
            unsafe {
                libc::memcpy(
                    page.vehicle_page as *mut c_void,
                    data.as_ptr() as *const c_void,
                    len,
                );
            };
            page
        };

        let mut evictions = BTreeSet::new();

        // Mark the pages as read.
        {
            let mut stw = self.state.write().unwrap();
            stw.heuristics.mark_pages_as_read(
                offset / *PAGESIZE_USIZE,
                (offset + page.mmapped_size as usize) / *PAGESIZE_USIZE,
            );
            stw.heuristics.evict_pages_if_needed(&mut evictions);

            // do we have too many pages loaded? evict pages if need to.
        }
        Ok((vec![page], evictions))
    }
}

// This is a utility function that fetches a range of bytes from an S3 object.
fn fetch_range(
    s3client: &S3Client,
    bucket: String,
    key: String,
    offset: usize,
    len: usize,
) -> Result<Vec<u8>, S3Failure> {
    let mut gob = GetObjectRequest::default();
    gob.bucket = bucket;
    gob.key = key;
    gob.range = Some(format!("bytes={}-{}", offset, offset + len - 1));

    let result = s3client.get_object(gob).sync()?;
    let body = match result.body {
        None => return Err(S3Failure::NoBodyReturned),
        Some(body) => body,
    };

    let mut v = Vec::new();
    body.into_blocking_read().read_to_end(&mut v)?;
    if v.len() != len {
        return Err(S3Failure::PartialRead);
    }
    Ok(v)
}
