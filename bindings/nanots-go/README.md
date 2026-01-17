# NanoTS Go Bindings

Go bindings for [NanoTS](https://github.com/dicroce/nanots), a high-performance embedded time-series database.

## Overview

NanoTS is an ultra-fast, embedded time-series database designed for real-time streaming applications. These Go bindings provide idiomatic Go access to NanoTS's powerful features.

### Key Features

- **High Performance**: 113,000+ writes/sec with 8.83μs write latency on SSD
- **Embedded Architecture**: Single-file database, no server required (like SQLite)
- **Multi-Stream Support**: Store different data streams in the same database file
- **Bidirectional Iteration**: Efficient forward/backward navigation with timestamp-based seeking
- **Memory-Mapped Storage**: Lock-free data structures with memory-mapped files
- **Crash Recovery**: Automatic detection and recovery from unexpected shutdowns
- **Cross-Platform**: Works on Linux, Windows, and macOS

## Installation

### Prerequisites

Before installing the Go bindings, you need to build the NanoTS C++ library:

```bash
# From the root nanots directory
mkdir -p build
cd build
cmake ..
make
sudo make install
```

### Installing the Go Package

```bash
go get github.com/nanots/nanots-go
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/nanots/nanots-go"
)

func main() {
    // 1. Allocate a new database file
    err := nanots.AllocateFile("data.nts", 1024*1024, 10)
    if err != nil {
        log.Fatal(err)
    }

    // 2. Write data
    writer, err := nanots.NewWriter("data.nts", false)
    if err != nil {
        log.Fatal(err)
    }
    defer writer.Close()

    ctx, err := writer.CreateWriteContext("sensor/temp", `{"unit": "celsius"}`)
    if err != nil {
        log.Fatal(err)
    }
    defer ctx.Close()

    err = writer.Write(ctx, []byte("22.5"), 1000, 0)
    if err != nil {
        log.Fatal(err)
    }

    // 3. Read data
    reader, err := nanots.NewReader("data.nts")
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    err = reader.Read("sensor/temp", 0, 9223372036854775807, func(frame nanots.Frame) error {
        fmt.Printf("Data: %s, Timestamp: %d\n", string(frame.Data), frame.Timestamp)
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

## API Reference

### File Allocation

```go
// AllocateFile creates a new NanoTS database file
func AllocateFile(fileName string, blockSize, nBlocks uint32) error
```

**Parameters:**
- `fileName`: Path to the database file
- `blockSize`: Size of each block in bytes (e.g., 1024*1024 for 1MB)
- `nBlocks`: Number of blocks to allocate

### Writer

The `Writer` type provides write operations for the database.

```go
// Create a new writer
writer, err := nanots.NewWriter(fileName string, autoReclaim bool) (*Writer, error)
defer writer.Close()

// Create a write context for a specific stream
ctx, err := writer.CreateWriteContext(streamTag, metadata string) (*WriteContext, error)
defer ctx.Close()

// Write data to the stream
err = writer.Write(ctx *WriteContext, data []byte, timestamp int64, flags uint8) error
```

**Writer Methods:**
- `NewWriter(fileName, autoReclaim)`: Creates a new writer instance
  - `autoReclaim`: If true, automatically reclaim blocks when space is low
- `CreateWriteContext(streamTag, metadata)`: Creates a write context for a stream
  - `streamTag`: Unique identifier for the stream
  - `metadata`: JSON metadata associated with the stream
- `Write(ctx, data, timestamp, flags)`: Writes a frame to the database
  - `ctx`: Write context for the target stream
  - `data`: Byte slice containing the frame data
  - `timestamp`: Unix timestamp (milliseconds or custom unit)
  - `flags`: User-defined flags (0-255)
- `Close()`: Releases writer resources

**Static Functions:**
- `FreeBlocks(fileName, streamTag, startTimestamp, endTimestamp)`: Frees blocks in a time range

### Reader

The `Reader` type provides read operations for the database.

```go
// Create a new reader
reader, err := nanots.NewReader(fileName string) (*Reader, error)
defer reader.Close()

// Read data with a callback
err = reader.Read(streamTag string, startTime, endTime int64, callback ReadCallback) error

// Query available stream tags
tags, err := reader.QueryStreamTags(startTime, endTime int64) ([]string, error)

// Query contiguous segments
segments, err := reader.QueryContiguousSegments(streamTag string, startTime, endTime int64) ([]ContiguousSegment, error)
```

**Reader Methods:**
- `NewReader(fileName)`: Creates a new reader instance
- `Read(streamTag, startTime, endTime, callback)`: Reads frames in time range
  - Callback signature: `func(frame Frame) error`
  - Returns any error from the callback or read operation
- `QueryStreamTags(startTime, endTime)`: Returns all stream tags with data in time range
- `QueryContiguousSegments(streamTag, startTime, endTime)`: Returns contiguous data segments
- `Close()`: Releases reader resources

### Iterator

The `Iterator` type provides sequential access to frames in a stream.

```go
// Create a new iterator
iter, err := nanots.NewIterator(fileName, streamTag string) (*Iterator, error)
defer iter.Close()

// Check if iterator is valid
if iter.Valid() {
    // Get current frame
    frame, err := iter.Current() (FrameInfo, error)
}

// Navigate
err = iter.Next()  // Move to next frame
err = iter.Prev()  // Move to previous frame
err = iter.Find(timestamp int64)  // Seek to timestamp >= specified
err = iter.Reset()  // Go to first frame

// Get metadata
blockSeq := iter.CurrentBlockSequence() int64
metadata := iter.CurrentMetadata() string
```

**Iterator Methods:**
- `NewIterator(fileName, streamTag)`: Creates a new iterator for a stream
- `Valid()`: Returns true if iterator is at a valid position
- `Current()`: Returns the current frame information
- `Next()`: Moves to the next frame
- `Prev()`: Moves to the previous frame
- `Find(timestamp)`: Seeks to first frame with timestamp >= specified value
- `Reset()`: Moves to the first frame in the stream
- `CurrentBlockSequence()`: Returns the block sequence number
- `CurrentMetadata()`: Returns the stream metadata
- `Close()`: Releases iterator resources

### Data Types

```go
// Frame represents a data frame from a read operation
type Frame struct {
    Data          []byte  // Frame data
    Timestamp     int64   // Frame timestamp
    BlockSequence int64   // Block sequence number
    Flags         uint8   // User-defined flags
    Metadata      string  // Stream metadata
}

// FrameInfo represents frame information from an iterator
type FrameInfo struct {
    Data          []byte  // Frame data
    Timestamp     int64   // Frame timestamp
    BlockSequence int64   // Block sequence number
    Flags         uint8   // User-defined flags
}

// ContiguousSegment represents a contiguous segment of data
type ContiguousSegment struct {
    SegmentID      int64  // Segment identifier
    StartTimestamp int64  // First timestamp in segment
    EndTimestamp   int64  // Last timestamp in segment
}

// Error represents a NanoTS error
type Error struct {
    Code    ErrorCode  // Error code
    Message string     // Error message
}
```

### Error Codes

```go
const (
    ErrOK                      ErrorCode = 0
    ErrCantOpen                ErrorCode = 1
    ErrSchema                  ErrorCode = 2
    ErrNoFreeBlocks            ErrorCode = 3
    ErrInvalidBlockSize        ErrorCode = 4
    ErrDuplicateStreamTag      ErrorCode = 5
    ErrUnableToCreateSegment   ErrorCode = 6
    ErrUnableToCreateSegmentBlock ErrorCode = 7
    ErrNonMonotonicTimestamp   ErrorCode = 8
    ErrRowSizeTooBig           ErrorCode = 9
    ErrUnableToAllocateFile    ErrorCode = 10
    ErrInvalidArgument         ErrorCode = 11
    ErrUnknown                 ErrorCode = 12
    ErrNotFound                ErrorCode = 13
)
```

## Usage Examples

### Writing Time-Series Data

```go
// Allocate database
err := nanots.AllocateFile("metrics.nts", 1024*1024, 100)
if err != nil {
    log.Fatal(err)
}

// Create writer
writer, err := nanots.NewWriter("metrics.nts", false)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

// Create contexts for different metrics
cpuCtx, _ := writer.CreateWriteContext("cpu/usage", `{"host": "server1"}`)
memCtx, _ := writer.CreateWriteContext("mem/usage", `{"host": "server1"}`)
defer cpuCtx.Close()
defer memCtx.Close()

// Write metrics
timestamp := time.Now().UnixMilli()
writer.Write(cpuCtx, []byte("45.2"), timestamp, 0)
writer.Write(memCtx, []byte("2048"), timestamp, 0)
```

### Reading with Time Range

```go
reader, err := nanots.NewReader("metrics.nts")
if err != nil {
    log.Fatal(err)
}
defer reader.Close()

// Read last hour of data
endTime := time.Now().UnixMilli()
startTime := endTime - (60 * 60 * 1000) // 1 hour ago

err = reader.Read("cpu/usage", startTime, endTime, func(frame nanots.Frame) error {
    fmt.Printf("CPU: %s%% at %d\n", string(frame.Data), frame.Timestamp)
    return nil
})
```

### Iterating Through Data

```go
iter, err := nanots.NewIterator("metrics.nts", "cpu/usage")
if err != nil {
    log.Fatal(err)
}
defer iter.Close()

// Start from beginning
iter.Reset()

// Iterate forward
for iter.Valid() {
    frame, err := iter.Current()
    if err != nil {
        break
    }

    fmt.Printf("CPU: %s%% at %d\n", string(frame.Data), frame.Timestamp)

    if err := iter.Next(); err != nil {
        break
    }
}
```

### Seeking to Specific Time

```go
iter, err := nanots.NewIterator("metrics.nts", "cpu/usage")
if err != nil {
    log.Fatal(err)
}
defer iter.Close()

// Find data at or after specific timestamp
targetTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
err = iter.Find(targetTime)
if err != nil {
    log.Fatal(err)
}

if iter.Valid() {
    frame, _ := iter.Current()
    fmt.Printf("Found: %s at %d\n", string(frame.Data), frame.Timestamp)
}
```

### Managing Multiple Streams

```go
writer, _ := nanots.NewWriter("data.nts", false)
defer writer.Close()

// Create multiple stream contexts
streams := map[string]*nanots.WriteContext{
    "sensor1": writer.CreateWriteContext("sensor1", `{"type": "temperature"}`),
    "sensor2": writer.CreateWriteContext("sensor2", `{"type": "pressure"}`),
    "sensor3": writer.CreateWriteContext("sensor3", `{"type": "humidity"}`),
}
defer func() {
    for _, ctx := range streams {
        ctx.Close()
    }
}()

// Write to different streams
timestamp := time.Now().UnixMilli()
writer.Write(streams["sensor1"], []byte("22.5"), timestamp, 0)
writer.Write(streams["sensor2"], []byte("1013.25"), timestamp, 0)
writer.Write(streams["sensor3"], []byte("65.0"), timestamp, 0)

// Query all available streams
reader, _ := nanots.NewReader("data.nts")
defer reader.Close()

tags, _ := reader.QueryStreamTags(0, 9223372036854775807)
fmt.Printf("Available streams: %v\n", tags)
```

## Building and Testing

### Building the Bindings

```bash
cd bindings/nanots-go
go build
```

### Running Tests

```bash
go test -v
```

### Running the Example

```bash
cd examples
go run basic_usage.go
```

## Performance Considerations

1. **Block Size**: Choose block size based on your use case
   - Larger blocks (2-4MB): Better for sequential writes
   - Smaller blocks (512KB-1MB): Better for random access

2. **Auto Reclaim**: Enable for long-running processes
   - Automatically frees old blocks when space is low
   - Slight performance overhead

3. **Batch Writes**: Write multiple frames in sequence
   - Keep write context alive for multiple writes
   - Reduces context creation overhead

4. **Iterator Caching**: Reuse iterators when possible
   - Iterators cache block data
   - More efficient than creating new iterators

5. **Memory Mapping**: Database uses memory-mapped I/O
   - Ensure sufficient virtual memory
   - OS caches frequently accessed blocks

## Thread Safety

- `Writer`, `Reader`, and `Iterator` types are **not** thread-safe
- Each goroutine should have its own instances
- Use synchronization primitives (mutexes, channels) for concurrent access
- Multiple readers can access the same file simultaneously (from different instances)

## License

NanoTS and its Go bindings are distributed under the terms specified in the main NanoTS repository.

## Contributing

Contributions are welcome! Please submit issues and pull requests to the main NanoTS repository.

## Links

- [NanoTS GitHub Repository](https://github.com/dicroce/nanots)
- [Python Bindings](../nanots_python/)
- [Rust Bindings](../nanots-rs/)

## Support

For questions and support:
- Open an issue on GitHub
- Check the main NanoTS documentation
- Review the examples in the `examples/` directory
