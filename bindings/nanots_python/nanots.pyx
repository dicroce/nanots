# nanots.pyx
from libc.stdint cimport uint8_t, uint32_t, int64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from libcpp cimport bool
import os

# External C declarations
cdef extern from "nanots.h":
    ctypedef struct nanots_writer_handle
    ctypedef nanots_writer_handle* nanots_writer_t
    
    ctypedef struct nanots_write_context_handle
    ctypedef nanots_write_context_handle* nanots_write_context_t
    
    ctypedef struct nanots_reader_handle
    ctypedef nanots_reader_handle* nanots_reader_t
    
    ctypedef struct nanots_iterator_handle
    ctypedef nanots_iterator_handle* nanots_iterator_t
    
    ctypedef enum nanots_ec_t:
        NANOTS_EC_OK = 0
        NANOTS_EC_CANT_OPEN = 1
        NANOTS_EC_SCHEMA = 2
        NANOTS_EC_NO_FREE_BLOCKS = 3
        NANOTS_EC_INVALID_BLOCK_SIZE = 4
        NANOTS_EC_DUPLICATE_STREAM_TAG = 5
        NANOTS_EC_UNABLE_TO_CREATE_SEGMENT = 6
        NANOTS_EC_UNABLE_TO_CREATE_SEGMENT_BLOCK = 7
        NANOTS_EC_NON_MONOTONIC_TIMESTAMP = 8
        NANOTS_EC_ROW_SIZE_TOO_BIG = 9
        NANOTS_EC_UNABLE_TO_ALLOCATE_FILE = 10
        NANOTS_EC_INVALID_ARGUMENT = 11
        NANOTS_EC_UNKNOWN = 12
        NANOTS_EC_NOT_FOUND = 13
    
    ctypedef struct nanots_contiguous_segment_t:
        int segment_id
        int64_t start_timestamp
        int64_t end_timestamp
    
    ctypedef struct nanots_frame_info_t:
        const uint8_t* data
        size_t size
        uint8_t flags
        int64_t timestamp
        int64_t block_sequence
    
    # Function declarations
    nanots_writer_t nanots_writer_create(const char* file_name, int auto_reclaim)
    void nanots_writer_destroy(nanots_writer_t writer)
    nanots_write_context_t nanots_writer_create_context(nanots_writer_t writer,
                                                        const char* stream_tag,
                                                        const char* metadata)
    void nanots_write_context_destroy(nanots_write_context_t context)
    nanots_ec_t nanots_writer_write(nanots_writer_t writer,
                                    nanots_write_context_t context,
                                    const uint8_t* data,
                                    size_t size,
                                    int64_t timestamp,
                                    uint8_t flags)
    nanots_ec_t nanots_writer_free_blocks(nanots_writer_t writer,
                                          const char* stream_tag,
                                          int64_t start_timestamp,
                                          int64_t end_timestamp)
    nanots_ec_t nanots_writer_allocate_file(const char* file_name,
                                            uint32_t block_size,
                                            uint32_t n_blocks)
    
    nanots_reader_t nanots_reader_create(const char* file_name)
    void nanots_reader_destroy(nanots_reader_t reader)
    ctypedef void (*nanots_read_callback_t)(const uint8_t* data,
                                           size_t size,
                                           uint8_t flags,
                                           int64_t timestamp,
                                           int64_t block_sequence,
                                           const char* metadata,
                                           void* user_data)
    
    nanots_ec_t nanots_reader_read(nanots_reader_t reader,
                                   const char* stream_tag,
                                   int64_t start_timestamp,
                                   int64_t end_timestamp,
                                   nanots_read_callback_t callback,
                                   void* user_data)
    
    nanots_ec_t nanots_reader_query_contiguous_segments(
        nanots_reader_t reader,
        const char* stream_tag,
        int64_t start_timestamp,
        int64_t end_timestamp,
        nanots_contiguous_segment_t** segments,
        size_t* count)
    void nanots_free_contiguous_segments(nanots_contiguous_segment_t* segments)
    
    nanots_ec_t nanots_reader_query_stream_tags_start(nanots_reader_t reader,
                                                      int64_t start_timestamp,
                                                      int64_t end_timestamp)
    const char* nanots_reader_query_stream_tags_next(nanots_reader_t reader)
    
    nanots_iterator_t nanots_iterator_create(const char* file_name,
                                             const char* stream_tag)
    void nanots_iterator_destroy(nanots_iterator_t iterator)
    int nanots_iterator_valid(nanots_iterator_t iterator)
    nanots_ec_t nanots_iterator_get_current_frame(
        nanots_iterator_t iterator,
        nanots_frame_info_t* frame_info)
    nanots_ec_t nanots_iterator_next(nanots_iterator_t iterator)
    nanots_ec_t nanots_iterator_prev(nanots_iterator_t iterator)
    nanots_ec_t nanots_iterator_find(nanots_iterator_t iterator,
                                     int64_t timestamp)
    nanots_ec_t nanots_iterator_reset(nanots_iterator_t iterator)
    int64_t nanots_iterator_current_block_sequence(nanots_iterator_t iterator)
    const char* nanots_iterator_current_metadata(nanots_iterator_t iterator)

# Python exceptions
class NanoTSError(Exception):
    pass

class CantOpenError(NanoTSError):
    pass

class SchemaError(NanoTSError):
    pass

class NoFreeBlocksError(NanoTSError):
    pass

class InvalidBlockSizeError(NanoTSError):
    pass

class DuplicateStreamTagError(NanoTSError):
    pass

class UnableToCreateSegmentError(NanoTSError):
    pass

class UnableToCreateSegmentBlockError(NanoTSError):
    pass

class NonMonotonicTimestampError(NanoTSError):
    pass

class RowSizeTooBigError(NanoTSError):
    pass

class UnableToAllocateFileError(NanoTSError):
    pass

class InvalidArgumentError(NanoTSError):
    pass

class NotFoundError(NanoTSError):
    pass

# Helper function to check results and raise appropriate exceptions
cdef void _check_result(nanots_ec_t result):
    if result == NANOTS_EC_OK:
        return
    elif result == NANOTS_EC_CANT_OPEN:
        raise CantOpenError("Cannot open file")
    elif result == NANOTS_EC_SCHEMA:
        raise SchemaError("Schema error")
    elif result == NANOTS_EC_NO_FREE_BLOCKS:
        raise NoFreeBlocksError("No free blocks available")
    elif result == NANOTS_EC_INVALID_BLOCK_SIZE:
        raise InvalidBlockSizeError("Invalid block size")
    elif result == NANOTS_EC_DUPLICATE_STREAM_TAG:
        raise DuplicateStreamTagError("Duplicate stream tag")
    elif result == NANOTS_EC_UNABLE_TO_CREATE_SEGMENT:
        raise UnableToCreateSegmentError("Unable to create segment")
    elif result == NANOTS_EC_UNABLE_TO_CREATE_SEGMENT_BLOCK:
        raise UnableToCreateSegmentBlockError("Unable to create segment block")
    elif result == NANOTS_EC_NON_MONOTONIC_TIMESTAMP:
        raise NonMonotonicTimestampError("Non-monotonic timestamp")
    elif result == NANOTS_EC_ROW_SIZE_TOO_BIG:
        raise RowSizeTooBigError("Row size too big")
    elif result == NANOTS_EC_UNABLE_TO_ALLOCATE_FILE:
        raise UnableToAllocateFileError("Unable to allocate file")
    elif result == NANOTS_EC_INVALID_ARGUMENT:
        raise InvalidArgumentError("Invalid argument")
    elif result == NANOTS_EC_NOT_FOUND:
        raise NotFoundError("Not found")
    else:
        raise NanoTSError(f"Unknown error: {result}")

# Utility function to create database files
def allocate_file(str filename, uint32_t block_size, uint32_t n_blocks):
    """Allocate a new nanots database file."""
    cdef bytes filename_bytes = filename.encode('utf-8')
    cdef nanots_ec_t result = nanots_writer_allocate_file(filename_bytes, block_size, n_blocks)
    _check_result(result)

# Write Context wrapper
cdef class WriteContext:
    cdef nanots_write_context_t _context
    cdef object _parent_writer  # Keep reference to prevent GC
    
    def __cinit__(self):
        self._context = NULL
        self._parent_writer = None
    
    def __dealloc__(self):
        if self._context != NULL:
            nanots_write_context_destroy(self._context)

# Writer wrapper
cdef class Writer:
    cdef nanots_writer_t _writer
    cdef str _filename
    
    def __cinit__(self, str filename, bint auto_reclaim=True):
        self._filename = filename
        cdef bytes filename_bytes = filename.encode('utf-8')
        self._writer = nanots_writer_create(filename_bytes, 1 if auto_reclaim else 0)
        if self._writer == NULL:
            raise NanoTSError("Failed to create writer")
    
    def __dealloc__(self):
        if self._writer != NULL:
            nanots_writer_destroy(self._writer)
    
    def create_context(self, str stream_tag, str metadata=""):
        """Create a write context for a specific stream."""
        cdef WriteContext context = WriteContext()
        cdef bytes stream_tag_bytes = stream_tag.encode('utf-8')
        cdef bytes metadata_bytes = metadata.encode('utf-8')
        
        context._context = nanots_writer_create_context(
            self._writer, stream_tag_bytes, metadata_bytes)
        context._parent_writer = self  # Keep reference
        
        if context._context == NULL:
            raise NanoTSError("Failed to create write context")
        return context
    
    def write(self, WriteContext context, bytes data, int64_t timestamp, uint8_t flags=0):
        """Write data to the database."""
        cdef nanots_ec_t result = nanots_writer_write(
            self._writer, context._context, data, len(data), timestamp, flags)
        _check_result(result)
    
    def free_blocks(self, str stream_tag, int64_t start_timestamp, int64_t end_timestamp):
        """Free blocks for a time range in a stream."""
        cdef bytes stream_tag_bytes = stream_tag.encode('utf-8')
        cdef nanots_ec_t result = nanots_writer_free_blocks(
            self._writer, stream_tag_bytes, start_timestamp, end_timestamp)
        _check_result(result)

# Reader wrapper
cdef class Reader:
    cdef nanots_reader_t _reader
    cdef str _filename
    
    def __cinit__(self, str filename):
        self._filename = filename
        cdef bytes filename_bytes = filename.encode('utf-8')
        self._reader = nanots_reader_create(filename_bytes)
        if self._reader == NULL:
            raise NanoTSError("Failed to create reader")
    
    def __dealloc__(self):
        if self._reader != NULL:
            nanots_reader_destroy(self._reader)
    
    def read(self, str stream_tag, int64_t start_timestamp, int64_t end_timestamp):
        """Read data from the database, returning a list of frames."""
        frames = []
        
        def callback(data, size, flags, timestamp, block_sequence, metadata):
            # Copy data to Python bytes object
            frame_data = data[:size]  # This creates a copy
            frames.append({
                'data': frame_data,
                'timestamp': timestamp,
                'flags': flags,
                'block_sequence': block_sequence,
                'metadata': metadata
            })
        
        # Store callback in a place where the C code can find it
        cdef object callback_ref = callback
        
        # For now, we'll use the simpler iterator approach
        # The callback approach requires more complex Cython code
        cdef Iterator iterator = Iterator(self._filename, stream_tag)
        iterator.find(start_timestamp)
        
        while iterator.valid():
            frame = iterator.get_current_frame()
            if frame['timestamp'] > end_timestamp:
                break
            frames.append(frame)
            iterator.next()
        
        return frames
    
    def query_contiguous_segments(self, str stream_tag, int64_t start_timestamp, int64_t end_timestamp):
        """Query contiguous segments in a time range."""
        cdef bytes stream_tag_bytes = stream_tag.encode('utf-8')
        cdef nanots_contiguous_segment_t* segments = NULL
        cdef size_t count = 0
        
        cdef nanots_ec_t result = nanots_reader_query_contiguous_segments(
            self._reader, stream_tag_bytes, start_timestamp, end_timestamp, &segments, &count)
        _check_result(result)
        
        # Convert to Python list
        segment_list = []
        for i in range(count):
            segment_list.append({
                'segment_id': segments[i].segment_id,
                'start_timestamp': segments[i].start_timestamp,
                'end_timestamp': segments[i].end_timestamp
            })
        
        # Free the C memory
        nanots_free_contiguous_segments(segments)
        return segment_list
    
    def query_stream_tags(self, int64_t start_timestamp, int64_t end_timestamp):
        """Query all stream tags that exist in the given time range."""
        cdef nanots_ec_t result = nanots_reader_query_stream_tags_start(
            self._reader, start_timestamp, end_timestamp)
        _check_result(result)
        
        # Collect all stream tags
        stream_tags = []
        cdef const char* tag_ptr
        
        while True:
            tag_ptr = nanots_reader_query_stream_tags_next(self._reader)
            if tag_ptr == NULL:
                break
            # Convert C string to Python string
            stream_tags.append(tag_ptr.decode('utf-8'))
        
        return stream_tags

# Iterator wrapper
cdef class Iterator:
    cdef nanots_iterator_t _iterator
    cdef str _filename
    cdef str _stream_tag
    
    def __cinit__(self, str filename, str stream_tag):
        self._filename = filename
        self._stream_tag = stream_tag
        cdef bytes filename_bytes = filename.encode('utf-8')
        cdef bytes stream_tag_bytes = stream_tag.encode('utf-8')
        self._iterator = nanots_iterator_create(filename_bytes, stream_tag_bytes)
        if self._iterator == NULL:
            raise NanoTSError("Failed to create iterator")
    
    def __dealloc__(self):
        if self._iterator != NULL:
            nanots_iterator_destroy(self._iterator)
    
    def valid(self):
        """Check if iterator is at a valid position."""
        return nanots_iterator_valid(self._iterator) != 0
    
    def get_current_frame(self):
        """Get the current frame data."""
        if not self.valid():
            raise NanoTSError("Iterator not at valid position")
        
        cdef nanots_frame_info_t frame_info
        cdef nanots_ec_t result = nanots_iterator_get_current_frame(
            self._iterator, &frame_info)
        _check_result(result)
        
        # Copy the data to a Python bytes object
        cdef bytes data = frame_info.data[:frame_info.size]
        
        return {
            'data': data,
            'timestamp': frame_info.timestamp,
            'flags': frame_info.flags,
            'block_sequence': frame_info.block_sequence,
            'metadata': self.current_metadata()
        }
    
    def next(self):
        """Move to next frame."""
        cdef nanots_ec_t result = nanots_iterator_next(self._iterator)
        _check_result(result)
    
    def prev(self):
        """Move to previous frame."""
        cdef nanots_ec_t result = nanots_iterator_prev(self._iterator)
        _check_result(result)
    
    def find(self, int64_t timestamp):
        """Find frame at or after given timestamp."""
        cdef nanots_ec_t result = nanots_iterator_find(self._iterator, timestamp)
        _check_result(result)
    
    def reset(self):
        """Reset iterator to beginning."""
        cdef nanots_ec_t result = nanots_iterator_reset(self._iterator)
        _check_result(result)
    
    def current_block_sequence(self):
        """Get current block sequence number."""
        return nanots_iterator_current_block_sequence(self._iterator)
    
    def current_metadata(self):
        """Get current block metadata."""
        cdef const char* metadata_ptr = nanots_iterator_current_metadata(self._iterator)
        if metadata_ptr == NULL:
            return ""
        return metadata_ptr.decode('utf-8')
    
    def __iter__(self):
        """Make iterator iterable."""
        return self
    
    def iter_all(self):
        """Iterate from beginning regardless of current position."""
        self.reset()
        return self
    
    def __next__(self):
        """Python iterator protocol."""
        if not self.valid():
            raise StopIteration
        
        frame = self.get_current_frame()
        self.next()
        return frame
