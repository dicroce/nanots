package nanots

/*
#cgo CPPFLAGS: -I../../amalgamated_src -DNANOTS_BUILD -DSQLITE_THREADSAFE=1 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_RTREE
#cgo linux CPPFLAGS: -DLINUX -DSQLITE_OS_UNIX=1
#cgo darwin CPPFLAGS: -DMACOS -DSQLITE_OS_UNIX=1
#cgo windows CPPFLAGS: -DWIN32 -D_WIN32 -DSQLITE_OS_WIN=1
#cgo CXXFLAGS: -std=c++17 -O2
#cgo LDFLAGS: -lstdc++ -lm
#cgo linux LDFLAGS: -lpthread -ldl
#cgo darwin LDFLAGS: -lpthread

#include <stdlib.h>
#include <stdint.h>
#include "../../amalgamated_src/nanots.h"

// Callback bridge for Go
extern void goReadCallback(const uint8_t* data, size_t size, uint8_t flags,
                          int64_t timestamp, int64_t block_sequence,
                          const char* metadata, void* user_data);
*/
import "C"
import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

// ErrorCode represents nanots error codes
type ErrorCode int

const (
	ErrOK                      ErrorCode = C.NANOTS_EC_OK
	ErrCantOpen                ErrorCode = C.NANOTS_EC_CANT_OPEN
	ErrSchema                  ErrorCode = C.NANOTS_EC_SCHEMA
	ErrNoFreeBlocks            ErrorCode = C.NANOTS_EC_NO_FREE_BLOCKS
	ErrInvalidBlockSize        ErrorCode = C.NANOTS_EC_INVALID_BLOCK_SIZE
	ErrDuplicateStreamTag      ErrorCode = C.NANOTS_EC_DUPLICATE_STREAM_TAG
	ErrUnableToCreateSegment   ErrorCode = C.NANOTS_EC_UNABLE_TO_CREATE_SEGMENT
	ErrUnableToCreateSegmentBlock ErrorCode = C.NANOTS_EC_UNABLE_TO_CREATE_SEGMENT_BLOCK
	ErrNonMonotonicTimestamp   ErrorCode = C.NANOTS_EC_NON_MONOTONIC_TIMESTAMP
	ErrRowSizeTooBig           ErrorCode = C.NANOTS_EC_ROW_SIZE_TOO_BIG
	ErrUnableToAllocateFile    ErrorCode = C.NANOTS_EC_UNABLE_TO_ALLOCATE_FILE
	ErrInvalidArgument         ErrorCode = C.NANOTS_EC_INVALID_ARGUMENT
	ErrUnknown                 ErrorCode = C.NANOTS_EC_UNKNOWN
	ErrNotFound                ErrorCode = C.NANOTS_EC_NOT_FOUND
)

// Error represents a nanots error with an error code
type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("nanots error %d: %s", e.Code, e.Message)
}

// newError creates an Error from an error code
func newError(code ErrorCode) error {
	if code == ErrOK {
		return nil
	}
	var msg string
	switch code {
	case ErrCantOpen:
		msg = "cannot open file"
	case ErrSchema:
		msg = "schema error"
	case ErrNoFreeBlocks:
		msg = "no free blocks available"
	case ErrInvalidBlockSize:
		msg = "invalid block size"
	case ErrDuplicateStreamTag:
		msg = "duplicate stream tag"
	case ErrUnableToCreateSegment:
		msg = "unable to create segment"
	case ErrUnableToCreateSegmentBlock:
		msg = "unable to create segment block"
	case ErrNonMonotonicTimestamp:
		msg = "non-monotonic timestamp"
	case ErrRowSizeTooBig:
		msg = "row size too big"
	case ErrUnableToAllocateFile:
		msg = "unable to allocate file"
	case ErrInvalidArgument:
		msg = "invalid argument"
	case ErrNotFound:
		msg = "not found"
	default:
		msg = "unknown error"
	}
	return &Error{Code: code, Message: msg}
}

// AllocateFile creates a new fixed-size nanots database file.
//
// The file is pre-allocated to hold exactly nBlocks blocks and will never
// grow. For an on-demand-growing file, use AllocateGrowableFile.
func AllocateFile(fileName string, blockSize, nBlocks uint32) error {
	cFileName := C.CString(fileName)
	defer C.free(unsafe.Pointer(cFileName))

	ec := C.nanots_writer_allocate_file(cFileName, C.uint32_t(blockSize), C.uint32_t(nBlocks))
	return newError(ErrorCode(ec))
}

// AllocateGrowableFile creates a new growable nanots database file.
//
// The file starts at just the 64KB header and is extended on demand using a
// BoltDB-style doubling strategy (capped at 1 GiB per grow). Pass maxBlocks=0
// for unbounded growth (grow until the disk is full).
func AllocateGrowableFile(fileName string, blockSize, maxBlocks uint32) error {
	cFileName := C.CString(fileName)
	defer C.free(unsafe.Pointer(cFileName))

	ec := C.nanots_writer_allocate_growable_file(cFileName, C.uint32_t(blockSize), C.uint32_t(maxBlocks))
	return newError(ErrorCode(ec))
}

// Writer provides write operations for nanots database
type Writer struct {
	handle C.nanots_writer_t
	mu     sync.Mutex
}

// NewWriter creates a new Writer for the specified file
func NewWriter(fileName string, autoReclaim bool) (*Writer, error) {
	cFileName := C.CString(fileName)
	defer C.free(unsafe.Pointer(cFileName))

	autoReclaimInt := 0
	if autoReclaim {
		autoReclaimInt = 1
	}

	handle := C.nanots_writer_create(cFileName, C.int(autoReclaimInt))
	if handle == nil {
		return nil, errors.New("failed to create writer")
	}

	w := &Writer{handle: handle}
	runtime.SetFinalizer(w, (*Writer).Close)
	return w, nil
}

// Close releases the writer resources
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.handle != nil {
		C.nanots_writer_destroy(w.handle)
		w.handle = nil
	}
	return nil
}

// WriteContext represents a write context for a specific stream
type WriteContext struct {
	handle C.nanots_write_context_t
}

// CreateWriteContext creates a new write context for the specified stream tag and metadata
func (w *Writer) CreateWriteContext(streamTag, metadata string) (*WriteContext, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.handle == nil {
		return nil, errors.New("writer is closed")
	}

	cStreamTag := C.CString(streamTag)
	defer C.free(unsafe.Pointer(cStreamTag))
	cMetadata := C.CString(metadata)
	defer C.free(unsafe.Pointer(cMetadata))

	handle := C.nanots_writer_create_context(w.handle, cStreamTag, cMetadata)
	if handle == nil {
		return nil, errors.New("failed to create write context")
	}

	wc := &WriteContext{handle: handle}
	runtime.SetFinalizer(wc, (*WriteContext).Close)
	return wc, nil
}

// Close releases the write context resources
func (wc *WriteContext) Close() error {
	if wc.handle != nil {
		C.nanots_write_context_destroy(wc.handle)
		wc.handle = nil
	}
	return nil
}

// Write writes data to the database with the specified timestamp and flags
func (w *Writer) Write(ctx *WriteContext, data []byte, timestamp int64, flags uint8) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.handle == nil {
		return errors.New("writer is closed")
	}
	if ctx.handle == nil {
		return errors.New("write context is closed")
	}

	var dataPtr *C.uint8_t
	if len(data) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}

	ec := C.nanots_writer_write(
		w.handle,
		ctx.handle,
		dataPtr,
		C.size_t(len(data)),
		C.int64_t(timestamp),
		C.uint8_t(flags),
	)
	return newError(ErrorCode(ec))
}

// FreeBlocks frees blocks in the specified time range for the given stream tag
func FreeBlocks(fileName, streamTag string, startTimestamp, endTimestamp int64) error {
	cFileName := C.CString(fileName)
	defer C.free(unsafe.Pointer(cFileName))
	cStreamTag := C.CString(streamTag)
	defer C.free(unsafe.Pointer(cStreamTag))

	ec := C.nanots_writer_free_blocks(
		cFileName,
		cStreamTag,
		C.int64_t(startTimestamp),
		C.int64_t(endTimestamp),
	)
	return newError(ErrorCode(ec))
}

// Reader provides read operations for nanots database
type Reader struct {
	handle C.nanots_reader_t
	mu     sync.Mutex
}

// NewReader creates a new Reader for the specified file
func NewReader(fileName string) (*Reader, error) {
	cFileName := C.CString(fileName)
	defer C.free(unsafe.Pointer(cFileName))

	handle := C.nanots_reader_create(cFileName)
	if handle == nil {
		return nil, errors.New("failed to create reader")
	}

	r := &Reader{handle: handle}
	runtime.SetFinalizer(r, (*Reader).Close)
	return r, nil
}

// Close releases the reader resources
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handle != nil {
		C.nanots_reader_destroy(r.handle)
		r.handle = nil
	}
	return nil
}

// Frame represents a single data frame
type Frame struct {
	Data          []byte
	Timestamp     int64
	BlockSequence int64
	Flags         uint8
	Metadata      string
}

// ReadCallback is called for each frame during a read operation
type ReadCallback func(frame Frame) error

var (
	callbackMu    sync.Mutex
	callbackStore = make(map[uintptr]ReadCallback)
	callbackID    uintptr
)

// Read reads data from the specified stream in the given time range
func (r *Reader) Read(streamTag string, startTimestamp, endTimestamp int64, callback ReadCallback) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handle == nil {
		return errors.New("reader is closed")
	}

	cStreamTag := C.CString(streamTag)
	defer C.free(unsafe.Pointer(cStreamTag))

	// Store the callback
	callbackMu.Lock()
	callbackID++
	id := callbackID
	callbackStore[id] = callback
	callbackMu.Unlock()

	defer func() {
		callbackMu.Lock()
		delete(callbackStore, id)
		callbackMu.Unlock()
	}()

	ec := C.nanots_reader_read(
		r.handle,
		cStreamTag,
		C.int64_t(startTimestamp),
		C.int64_t(endTimestamp),
		C.nanots_read_callback_t(C.goReadCallback),
		unsafe.Pointer(id),
	)
	return newError(ErrorCode(ec))
}

// QueryStreamTags returns all stream tags in the specified time range
func (r *Reader) QueryStreamTags(startTimestamp, endTimestamp int64) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handle == nil {
		return nil, errors.New("reader is closed")
	}

	ec := C.nanots_reader_query_stream_tags_start(
		r.handle,
		C.int64_t(startTimestamp),
		C.int64_t(endTimestamp),
	)
	if err := newError(ErrorCode(ec)); err != nil {
		return nil, err
	}

	var tags []string
	for {
		cTag := C.nanots_reader_query_stream_tags_next(r.handle)
		if cTag == nil {
			break
		}
		tags = append(tags, C.GoString(cTag))
	}

	return tags, nil
}

// ContiguousSegment represents a contiguous segment of data
type ContiguousSegment struct {
	SegmentID      int64
	StartTimestamp int64
	EndTimestamp   int64
}

// QueryContiguousSegments returns all contiguous segments for the specified stream in the given time range
func (r *Reader) QueryContiguousSegments(streamTag string, startTimestamp, endTimestamp int64) ([]ContiguousSegment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handle == nil {
		return nil, errors.New("reader is closed")
	}

	cStreamTag := C.CString(streamTag)
	defer C.free(unsafe.Pointer(cStreamTag))

	var segments *C.nanots_contiguous_segment_t
	var count C.size_t

	ec := C.nanots_reader_query_contiguous_segments(
		r.handle,
		cStreamTag,
		C.int64_t(startTimestamp),
		C.int64_t(endTimestamp),
		&segments,
		&count,
	)
	if err := newError(ErrorCode(ec)); err != nil {
		return nil, err
	}

	if segments != nil {
		defer C.nanots_free_contiguous_segments(segments)
	}

	result := make([]ContiguousSegment, int(count))
	segmentSlice := unsafe.Slice(segments, int(count))
	for i, seg := range segmentSlice {
		result[i] = ContiguousSegment{
			SegmentID:      int64(seg.segment_id),
			StartTimestamp: int64(seg.start_timestamp),
			EndTimestamp:   int64(seg.end_timestamp),
		}
	}

	return result, nil
}

// Iterator provides sequential access to frames in a stream
type Iterator struct {
	handle C.nanots_iterator_t
	mu     sync.Mutex
}

// NewIterator creates a new Iterator for the specified file and stream tag
func NewIterator(fileName, streamTag string) (*Iterator, error) {
	cFileName := C.CString(fileName)
	defer C.free(unsafe.Pointer(cFileName))
	cStreamTag := C.CString(streamTag)
	defer C.free(unsafe.Pointer(cStreamTag))

	handle := C.nanots_iterator_create(cFileName, cStreamTag)
	if handle == nil {
		return nil, errors.New("failed to create iterator")
	}

	it := &Iterator{handle: handle}
	runtime.SetFinalizer(it, (*Iterator).Close)
	return it, nil
}

// Close releases the iterator resources
func (it *Iterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle != nil {
		C.nanots_iterator_destroy(it.handle)
		it.handle = nil
	}
	return nil
}

// Valid returns true if the iterator is at a valid position
func (it *Iterator) Valid() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return false
	}
	return C.nanots_iterator_valid(it.handle) != 0
}

// FrameInfo represents information about the current frame
type FrameInfo struct {
	Data          []byte
	Timestamp     int64
	BlockSequence int64
	Flags         uint8
}

// Current returns the current frame information
func (it *Iterator) Current() (FrameInfo, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return FrameInfo{}, errors.New("iterator is closed")
	}

	var cFrame C.nanots_frame_info_t
	ec := C.nanots_iterator_get_current_frame(it.handle, &cFrame)
	if err := newError(ErrorCode(ec)); err != nil {
		return FrameInfo{}, err
	}

	// Copy the data to Go slice
	data := C.GoBytes(unsafe.Pointer(cFrame.data), C.int(cFrame.size))

	return FrameInfo{
		Data:          data,
		Timestamp:     int64(cFrame.timestamp),
		BlockSequence: int64(cFrame.block_sequence),
		Flags:         uint8(cFrame.flags),
	}, nil
}

// Next moves the iterator to the next frame
func (it *Iterator) Next() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return errors.New("iterator is closed")
	}

	ec := C.nanots_iterator_next(it.handle)
	return newError(ErrorCode(ec))
}

// Prev moves the iterator to the previous frame
func (it *Iterator) Prev() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return errors.New("iterator is closed")
	}

	ec := C.nanots_iterator_prev(it.handle)
	return newError(ErrorCode(ec))
}

// Find seeks to the first frame with timestamp >= the specified timestamp
func (it *Iterator) Find(timestamp int64) error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return errors.New("iterator is closed")
	}

	ec := C.nanots_iterator_find(it.handle, C.int64_t(timestamp))
	return newError(ErrorCode(ec))
}

// Reset moves the iterator to the first frame
func (it *Iterator) Reset() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return errors.New("iterator is closed")
	}

	ec := C.nanots_iterator_reset(it.handle)
	return newError(ErrorCode(ec))
}

// CurrentBlockSequence returns the current block sequence number
func (it *Iterator) CurrentBlockSequence() int64 {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return 0
	}
	return int64(C.nanots_iterator_current_block_sequence(it.handle))
}

// CurrentMetadata returns the metadata for the current stream
func (it *Iterator) CurrentMetadata() string {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.handle == nil {
		return ""
	}
	cMetadata := C.nanots_iterator_current_metadata(it.handle)
	if cMetadata == nil {
		return ""
	}
	return C.GoString(cMetadata)
}
