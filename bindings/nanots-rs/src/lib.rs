//! # nanots-rs
//! 
//! Rust bindings for the nanots time series database.
//! 
//! This crate provides safe, idiomatic Rust wrappers around the nanots C++ library.
//! 
//! ## Example
//! 
//! ```no_run
//! use nanots_rs::{Writer, Reader};
//! 
//! # fn main() -> nanots_rs::Result<()> {
//! // Allocate a new database file
//! Writer::allocate_file("data.nts", 1024 * 1024, 100)?;
//! 
//! // Write some data
//! let writer = Writer::new("data.nts", false)?;
//! let context = writer.create_context("sensor_data", "temperature readings")?;
//! writer.write(&context, b"hello world", 1234567890, 0)?;
//! 
//! // Read it back
//! let reader = Reader::new("data.nts")?;
//! reader.read("sensor_data", 0, i64::MAX, |data, flags, timestamp, block_seq| {
//!     println!("Read {} bytes at timestamp {}", data.len(), timestamp);
//! })?;
//! # Ok(())
//! # }
//! ```

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::slice;

// Error codes matching the C enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ErrorCode {
    Ok = 0,
    CantOpen = 1,
    Schema = 2,
    NoFreeBlocks = 3,
    InvalidBlockSize = 4,
    DuplicateStreamTag = 5,
    UnableToCreateSegment = 6,
    UnableToCreateSegmentBlock = 7,
    NonMonotonicTimestamp = 8,
    RowSizeTooBig = 9,
    UnableToAllocateFile = 10,
    InvalidArgument = 11,
    Unknown = 12,
}

impl ErrorCode {
    fn from_c(code: u32) -> Self {
        match code {
            0 => ErrorCode::Ok,
            1 => ErrorCode::CantOpen,
            2 => ErrorCode::Schema,
            3 => ErrorCode::NoFreeBlocks,
            4 => ErrorCode::InvalidBlockSize,
            5 => ErrorCode::DuplicateStreamTag,
            6 => ErrorCode::UnableToCreateSegment,
            7 => ErrorCode::UnableToCreateSegmentBlock,
            8 => ErrorCode::NonMonotonicTimestamp,
            9 => ErrorCode::RowSizeTooBig,
            10 => ErrorCode::UnableToAllocateFile,
            11 => ErrorCode::InvalidArgument,
            _ => ErrorCode::Unknown,
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            ErrorCode::Ok => "Success",
            ErrorCode::CantOpen => "Cannot open file",
            ErrorCode::Schema => "Schema error",
            ErrorCode::NoFreeBlocks => "No free blocks available",
            ErrorCode::InvalidBlockSize => "Invalid block size",
            ErrorCode::DuplicateStreamTag => "Duplicate stream tag",
            ErrorCode::UnableToCreateSegment => "Unable to create segment",
            ErrorCode::UnableToCreateSegmentBlock => "Unable to create segment block",
            ErrorCode::NonMonotonicTimestamp => "Non-monotonic timestamp",
            ErrorCode::RowSizeTooBig => "Row size too big",
            ErrorCode::UnableToAllocateFile => "Unable to allocate file",
            ErrorCode::InvalidArgument => "Invalid argument",
            ErrorCode::Unknown => "Unknown error",
        };
        write!(f, "{}", msg)
    }
}

impl std::error::Error for ErrorCode {}

/// Result type for nanots operations
pub type Result<T> = std::result::Result<T, ErrorCode>;

// C struct representations
#[repr(C)]
#[derive(Clone)]
pub struct ContiguousSegment {
    pub segment_id: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
}

#[repr(C)]
struct FrameInfo {
    data: *const u8,
    size: usize,
    flags: u8,
    timestamp: i64,
    block_sequence: i64,
}

// Opaque handle types
enum WriterHandle {}
enum WriteContextHandle {}
enum ReaderHandle {}
enum IteratorHandle {}

type WriterPtr = *mut WriterHandle;
type WriteContextPtr = *mut WriteContextHandle;
type ReaderPtr = *mut ReaderHandle;
type IteratorPtr = *mut IteratorHandle;

// C callback type
type ReadCallback = extern "C" fn(
    data: *const u8,
    size: usize,
    flags: u8,
    timestamp: i64,
    block_sequence: i64,
    user_data: *mut c_void,
);

// External C functions
extern "C" {
    fn nanots_writer_allocate_file(
        file_name: *const c_char,
        block_size: u32,
        n_blocks: u32,
    ) -> u32;
    
    fn nanots_writer_create(file_name: *const c_char, auto_reclaim: c_int) -> WriterPtr;
    fn nanots_writer_destroy(writer: WriterPtr);
    fn nanots_writer_create_context(
        writer: WriterPtr,
        stream_tag: *const c_char,
        metadata: *const c_char,
    ) -> WriteContextPtr;
    fn nanots_write_context_destroy(context: WriteContextPtr);
    fn nanots_writer_write(
        writer: WriterPtr,
        context: WriteContextPtr,
        data: *const u8,
        size: usize,
        timestamp: i64,
        flags: u8,
    ) -> u32;
    fn nanots_writer_free_blocks(
        writer: WriterPtr,
        stream_tag: *const c_char,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> u32;
    
    fn nanots_reader_create(file_name: *const c_char) -> ReaderPtr;
    fn nanots_reader_destroy(reader: ReaderPtr);
    fn nanots_reader_read(
        reader: ReaderPtr,
        stream_tag: *const c_char,
        start_timestamp: i64,
        end_timestamp: i64,
        callback: ReadCallback,
        user_data: *mut c_void,
    ) -> u32;
    fn nanots_reader_query_contiguous_segments(
        reader: ReaderPtr,
        stream_tag: *const c_char,
        start_timestamp: i64,
        end_timestamp: i64,
        segments: *mut *mut ContiguousSegment,
        count: *mut usize,
    ) -> u32;
    fn nanots_free_contiguous_segments(segments: *mut ContiguousSegment);
    
    fn nanots_iterator_create(file_name: *const c_char, stream_tag: *const c_char) -> IteratorPtr;
    fn nanots_iterator_destroy(iterator: IteratorPtr);
    fn nanots_iterator_valid(iterator: IteratorPtr) -> c_int;
    fn nanots_iterator_get_current_frame(iterator: IteratorPtr, frame_info: *mut FrameInfo) -> u32;
    fn nanots_iterator_next(iterator: IteratorPtr) -> u32;
    fn nanots_iterator_prev(iterator: IteratorPtr) -> u32;
    fn nanots_iterator_find(iterator: IteratorPtr, timestamp: i64) -> u32;
    fn nanots_iterator_reset(iterator: IteratorPtr) -> u32;
    fn nanots_iterator_current_block_sequence(iterator: IteratorPtr) -> i64;
    
    fn nanots_reader_query_stream_tags_start(
        reader: ReaderPtr,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> u32;
    fn nanots_reader_query_stream_tags_next(reader: ReaderPtr) -> *const c_char;
}

/// Writer for nanots database
pub struct Writer {
    ptr: WriterPtr,
}

impl Writer {
    /// Create a new writer for the specified file
    pub fn new(file_name: &str, auto_reclaim: bool) -> Result<Self> {
        let c_file_name = CString::new(file_name).map_err(|_| ErrorCode::InvalidArgument)?;
        let ptr = unsafe { nanots_writer_create(c_file_name.as_ptr(), auto_reclaim as c_int) };
        if ptr.is_null() {
            Err(ErrorCode::CantOpen)
        } else {
            Ok(Writer { ptr })
        }
    }

    /// Create a write context for a specific stream
    pub fn create_context(&self, stream_tag: &str, metadata: &str) -> Result<WriteContext> {
        let c_stream_tag = CString::new(stream_tag).map_err(|_| ErrorCode::InvalidArgument)?;
        let c_metadata = CString::new(metadata).map_err(|_| ErrorCode::InvalidArgument)?;
        let ptr = unsafe {
            nanots_writer_create_context(self.ptr, c_stream_tag.as_ptr(), c_metadata.as_ptr())
        };
        if ptr.is_null() {
            Err(ErrorCode::UnableToCreateSegment)
        } else {
            Ok(WriteContext { ptr })
        }
    }

    /// Write data to the database
    pub fn write(&self, context: &WriteContext, data: &[u8], timestamp: i64, flags: u8) -> Result<()> {
        let result = unsafe {
            nanots_writer_write(self.ptr, context.ptr, data.as_ptr(), data.len(), timestamp, flags)
        };
        let error_code = ErrorCode::from_c(result);
        if error_code == ErrorCode::Ok {
            Ok(())
        } else {
            Err(error_code)
        }
    }

    /// Free blocks in a time range for a stream
    pub fn free_blocks(&self, stream_tag: &str, start_timestamp: i64, end_timestamp: i64) -> Result<()> {
        let c_stream_tag = CString::new(stream_tag).map_err(|_| ErrorCode::InvalidArgument)?;
        let result = unsafe {
            nanots_writer_free_blocks(self.ptr, c_stream_tag.as_ptr(), start_timestamp, end_timestamp)
        };
        let error_code = ErrorCode::from_c(result);
        if error_code == ErrorCode::Ok {
            Ok(())
        } else {
            Err(error_code)
        }
    }

    /// Allocate a new database file
    pub fn allocate_file(file_name: &str, block_size: u32, n_blocks: u32) -> Result<()> {
        let c_file_name = CString::new(file_name).map_err(|_| ErrorCode::InvalidArgument)?;
        let result = unsafe { nanots_writer_allocate_file(c_file_name.as_ptr(), block_size, n_blocks) };
        let error_code = ErrorCode::from_c(result);
        if error_code == ErrorCode::Ok {
            Ok(())
        } else {
            Err(error_code)
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        unsafe { nanots_writer_destroy(self.ptr) }
    }
}

unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

/// Write context for a specific stream
pub struct WriteContext {
    ptr: WriteContextPtr,
}

impl Drop for WriteContext {
    fn drop(&mut self) {
        unsafe { nanots_write_context_destroy(self.ptr) }
    }
}

/// Reader for nanots database
pub struct Reader {
    ptr: ReaderPtr,
}

impl Reader {
    /// Create a new reader for the specified file
    pub fn new(file_name: &str) -> Result<Self> {
        let c_file_name = CString::new(file_name).map_err(|_| ErrorCode::InvalidArgument)?;
        let ptr = unsafe { nanots_reader_create(c_file_name.as_ptr()) };
        if ptr.is_null() {
            Err(ErrorCode::CantOpen)
        } else {
            Ok(Reader { ptr })
        }
    }

    /// Read data from a stream in a time range
    pub fn read<F>(&self, stream_tag: &str, start_timestamp: i64, end_timestamp: i64, mut callback: F) -> Result<()>
    where
        F: FnMut(&[u8], u8, i64, i64),
    {
        let c_stream_tag = CString::new(stream_tag).map_err(|_| ErrorCode::InvalidArgument)?;
        
        extern "C" fn c_callback(
            data: *const u8,
            size: usize,
            flags: u8,
            timestamp: i64,
            block_sequence: i64,
            user_data: *mut c_void,
        ) {
            let callback = unsafe { &mut *(user_data as *mut &mut dyn FnMut(&[u8], u8, i64, i64)) };
            let data_slice = unsafe { slice::from_raw_parts(data, size) };
            callback(data_slice, flags, timestamp, block_sequence);
        }

        let mut callback_ref: &mut dyn FnMut(&[u8], u8, i64, i64) = &mut callback;
        let user_data = &mut callback_ref as *mut _ as *mut c_void;

        let result = unsafe {
            nanots_reader_read(self.ptr, c_stream_tag.as_ptr(), start_timestamp, end_timestamp, c_callback, user_data)
        };
        
        let error_code = ErrorCode::from_c(result);
        if error_code == ErrorCode::Ok {
            Ok(())
        } else {
            Err(error_code)
        }
    }

    /// Query contiguous segments in a time range
    pub fn query_contiguous_segments(&self, stream_tag: &str, start_timestamp: i64, end_timestamp: i64) -> Result<Vec<ContiguousSegment>> {
        let c_stream_tag = CString::new(stream_tag).map_err(|_| ErrorCode::InvalidArgument)?;
        let mut segments_ptr: *mut ContiguousSegment = ptr::null_mut();
        let mut count: usize = 0;

        let result = unsafe {
            nanots_reader_query_contiguous_segments(
                self.ptr, c_stream_tag.as_ptr(), start_timestamp, end_timestamp, &mut segments_ptr, &mut count
            )
        };

        let error_code = ErrorCode::from_c(result);
        if error_code != ErrorCode::Ok {
            return Err(error_code);
        }

        if segments_ptr.is_null() || count == 0 {
            return Ok(Vec::new());
        }

        let segments = unsafe { slice::from_raw_parts(segments_ptr, count) }.to_vec();
        unsafe { nanots_free_contiguous_segments(segments_ptr) };
        Ok(segments)
    }

    /// Query stream tags in a time range
    pub fn query_stream_tags(&self, start_timestamp: i64, end_timestamp: i64) -> Result<Vec<String>> {
        let result = unsafe {
            nanots_reader_query_stream_tags_start(self.ptr, start_timestamp, end_timestamp)
        };
        
        let error_code = ErrorCode::from_c(result);
        if error_code != ErrorCode::Ok {
            return Err(error_code);
        }

        let mut tags = Vec::new();
        loop {
            let tag_ptr = unsafe { nanots_reader_query_stream_tags_next(self.ptr) };
            if tag_ptr.is_null() {
                break;
            }
            
            let c_str = unsafe { std::ffi::CStr::from_ptr(tag_ptr) };
            match c_str.to_str() {
                Ok(tag) => tags.push(tag.to_string()),
                Err(_) => return Err(ErrorCode::InvalidArgument),
            }
        }
        
        Ok(tags)
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        unsafe { nanots_reader_destroy(self.ptr) }
    }
}

unsafe impl Send for Reader {}
unsafe impl Sync for Reader {}

/// Iterator for nanots database
pub struct Iterator {
    ptr: IteratorPtr,
}

impl Iterator {
    /// Create a new iterator for a specific stream
    pub fn new(file_name: &str, stream_tag: &str) -> Result<Self> {
        let c_file_name = CString::new(file_name).map_err(|_| ErrorCode::InvalidArgument)?;
        let c_stream_tag = CString::new(stream_tag).map_err(|_| ErrorCode::InvalidArgument)?;
        let ptr = unsafe { nanots_iterator_create(c_file_name.as_ptr(), c_stream_tag.as_ptr()) };
        if ptr.is_null() {
            Err(ErrorCode::CantOpen)
        } else {
            Ok(Iterator { ptr })
        }
    }

    /// Check if the iterator is at a valid position
    pub fn is_valid(&self) -> bool {
        unsafe { nanots_iterator_valid(self.ptr) != 0 }
    }

    /// Get the current frame
    pub fn current_frame(&self) -> Result<Frame> {
        let mut frame_info = FrameInfo {
            data: ptr::null(),
            size: 0,
            flags: 0,
            timestamp: 0,
            block_sequence: 0,
        };

        let result = unsafe { nanots_iterator_get_current_frame(self.ptr, &mut frame_info) };
        let error_code = ErrorCode::from_c(result);
        if error_code != ErrorCode::Ok {
            return Err(error_code);
        }

        let data = if frame_info.data.is_null() || frame_info.size == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(frame_info.data, frame_info.size) }.to_vec()
        };

        Ok(Frame {
            data,
            flags: frame_info.flags,
            timestamp: frame_info.timestamp,
            block_sequence: frame_info.block_sequence,
        })
    }

    /// Move to the next frame
    pub fn next(&mut self) -> Result<()> {
        let result = unsafe { nanots_iterator_next(self.ptr) };
        let error_code = ErrorCode::from_c(result);
        if error_code == ErrorCode::Ok {
            Ok(())
        } else {
            Err(error_code)
        }
    }

    /// Move to the previous frame
    pub fn prev(&mut self) -> Result<()> {
        let result = unsafe { nanots_iterator_prev(self.ptr) };
        let error_code = ErrorCode::from_c(result);
        if error_code == ErrorCode::Ok {
            Ok(())
        } else {
            Err(error_code)
        }
    }

    /// Find the first frame at or after the given timestamp
    pub fn find(&mut self, timestamp: i64) -> Result<bool> {
        let result = unsafe { nanots_iterator_find(self.ptr, timestamp) };
        let error_code = ErrorCode::from_c(result);
        Ok(error_code == ErrorCode::Ok)
    }

    /// Reset to the first frame
    pub fn reset(&mut self) -> Result<()> {
        let result = unsafe { nanots_iterator_reset(self.ptr) };
        let error_code = ErrorCode::from_c(result);
        if error_code == ErrorCode::Ok {
            Ok(())
        } else {
            Err(error_code)
        }
    }

    /// Get the current block sequence number
    pub fn current_block_sequence(&self) -> i64 {
        unsafe { nanots_iterator_current_block_sequence(self.ptr) }
    }
}

impl Drop for Iterator {
    fn drop(&mut self) {
        unsafe { nanots_iterator_destroy(self.ptr) }
    }
}

/// Frame data from the database
#[derive(Debug, Clone)]
pub struct Frame {
    pub data: Vec<u8>,
    pub flags: u8,
    pub timestamp: i64,
    pub block_sequence: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_display() {
        assert_eq!(ErrorCode::Ok.to_string(), "Success");
        assert_eq!(ErrorCode::CantOpen.to_string(), "Cannot open file");
    }

    #[test]
    fn test_error_code_from_c() {
        assert_eq!(ErrorCode::from_c(0), ErrorCode::Ok);
        assert_eq!(ErrorCode::from_c(1), ErrorCode::CantOpen);
        assert_eq!(ErrorCode::from_c(999), ErrorCode::Unknown);
    }
}
