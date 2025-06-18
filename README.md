# NanoTS - High-Performance, embedded Time-Series Database

A lightweight, high-performance, embedded (like sqlite) time-series database optimized for real-time streaming applications like video, finance, and IoT sensor data.

## Key Features

- **Ultra-fast writes**: 8.83μs per write on SSD, 300μs on spinning disk
- **Memory-mapped storage**: Lock free storage data structure on memory mapped file allows for maximum throughput.
- **Configurable durability**: Trade-off between performance and data safety with configurable block sizes
- **Crash recovery**: Automatic detection and recovery from unexpected shutdowns
- **Preallocated storage**: Preallocated storage files avoid filesystem fragmentation and administration hassles. In auto-recyle mode you always have the latest data.
- **Multiple streams**: Store different data streams in the same database file
- **Iterator interface**: Efficient navigation with bidirectional iteration and timestamp-based seeking
- **Cross Platform**: Currently works on Linux, Windows and MacOS.

## Performance

NanoTS is designed for high-throughput, low-latency applications:

- **113,000+ writes/second per stream** sustained on SSD
- **3,300+ writes/second** on spinning disk
- **Sub-microsecond reads** via memory mapping
- **Efficient seeks** using binary search on timestamps

## Architecture

### Storage Layout

NanoTS uses a hybrid approach combining SQLite for metadata with memory-mapped binary files for data:

```
┌─────────────┬─────────────┬─────────────┬─────────────┐
│File Header  │   Block 1   │   Block 2   │   Block N   │
│   (64KB)    │   (1MB+)    │   (1MB+)    │   (1MB+)    │
└─────────────┴─────────────┴─────────────┴─────────────┘
```
Block size is configurable and tunable for different applications.

Each block contains:
- **Block header**: Metadata and frame count
- **Frame index**: Timestamp → offset mappings for fast seeks
- **Frame data**: Variable-size frames with headers and payload

### Durability Guarantees

- **Frame-level atomicity**: Individual frames are written atomically
- **Block-level durability**: In worst case, lose at most one block of data during crash
- **Configurable trade-offs**: Smaller blocks = better durability, larger blocks = better performance

## Quick Start

### Writing Data

```cpp
#include "nanots.h"

// Create database with 1MB blocks, 16 total blocks
nanots_writer::allocate("video.nts", 1024*1024, 16);

// Open the db in auto recycle mode (once full, new data will re-use the oldest blocks).
nanots_writer db("video.nts", true);

// Create write context for a stream
auto wctx = db.create_write_context("camera_1", "stream metadata");

// Write frames
uint8_t frame_data[] = {/* video frame bytes */};
db.write(wctx, frame_data, sizeof(frame_data), timestamp_us, flags);

```

### Reading Data

```cpp
#include "nanots.h"

// Create iterator for a stream
nanots_iterator iter("video.nts", "camera_1");

// Iterate through all frames
while (iter.valid()) {
    auto& frame = *iter;
    process_frame(frame.data, frame.size, frame.timestamp, frame.flags);
    ++iter;
}

// Or seek to specific timestamp
if (iter.find(target_timestamp)) {
    // Found first frame >= target_timestamp
    auto& frame = *iter;
    // ... process frame
}
```

### Backward Iteration

```cpp
// Start from end and go backward
iter.find(end_timestamp);
while (iter.valid()) {
    auto& frame = *iter;
    process_frame(frame.data, frame.size, frame.timestamp, frame.flags);
    --iter;  // Go to previous frame
}
```

## Configuration

### Block Size Selection

Block size affects the durability/performance trade-off:

```cpp
// High durability, slightly slower
nanots_writer::allocate("data.nts", 64 * 1024, 32);        // 64KB blocks

// Balanced
nanots_writer::allocate("data.nts", 1024 * 1024, 16);      // 1MB blocks  

// High performance, larger potential data loss
nanots_writer::allocate("data.nts", 16 * 1024 * 1024, 8);  // 16MB blocks

// What I use for h.264 video streams
nanots_writer::allocate("data.nts", 10 * 1024 * 1024, 100); // 100 10mb blocks
```

## Use Cases

### Video Streaming
- Store video frames with microsecond timestamps
- Write dozens of streams simultaneously while reading from all of them.
- Seek to specific time positions for playback
- Handle variable frame sizes efficiently

### IoT Sensor Data
- High-frequency sensor readings
- Efficient storage of numeric telemetry
- Time-based queries and analysis

### Financial Data
- Trade tick data with microsecond precision
- Market data replay systems
- Low-latency historical queries

## Advanced Features

### Crash Recovery

NanoTS automatically detects and recovers from crashes:

```cpp
// On startup, validates all blocks and recovers partial writes
nanots_writer db("data.nts");
// Database is automatically validated and ready to use
```

### Multiple Streams

Store different data types in the same database:

```cpp
auto video_ctx = db.create_write_context("video", "H.264 stream");
auto audio_ctx = db.create_write_context("audio", "AAC stream");
auto sensor_ctx = db.create_write_context("sensors", "IMU data");

// Each stream has independent iterators
nanots_iterator video_iter("data.nts", "video");
nanots_iterator audio_iter("data.nts", "audio");
```

### Block Recycling

Automatic management of storage space:

```cpp
// Enable automatic reclaim of oldest blocks when space runs out
nanots_writer("file.nts", true);
// When blocks are full, oldest finalized blocks are automatically recycled
```

## Performance Tips

1. **Use appropriate block sizes**: Larger blocks for higher throughput, smaller for better durability
2. **Batch writes when possible**: Future bulk write API will provide even better performance
3. **Use SSD storage**: 30-40x faster than spinning disks for random access patterns
4. **Size your block pool**: More blocks = less frequent recycling = better performance
5. **Align frame sizes**: Consider your typical frame sizes when choosing block sizes

## Requirements

- **C++17 or later**
- **Memory mapping support** (Linux/Windows/macOS)
- **File system**: Any POSIX-compliant or Windows NTFS

## Thread Safety

- **Single writer per stream**: Each write context should be used from one thread
- **Multiple readers**: Iterators are thread-safe for reading
- **Cross-stream concurrent access**: Different streams can be accessed concurrently

## Limitations

- **Single file per database**: All blocks stored in one file (with separate SQLite DB)
- **Write-once semantics**: Frames cannot be modified after writing
- **Platform-specific alignment**: Windows requires 64KB block alignment
- **Memory usage**: Each loaded block consumes virtual address space

## Building

The easiest way to use NanoTS is to copy the 4 source files from amalgamation_src/ into your project source directory and add them to your build. Then you can include "nanots.h" and start writing.

That said the repo is a normal CMake project that builds NanoTS as a static lib and you can link against that if you prefer.

## License

NanoTS is licensed under the Apache 2.0 license.

## Contributing

Happy to look at PR's from forks.

## Support

Feel free to contact me for support: dicroce@gmail.com
