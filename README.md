# NanoTS - High-Performance, embedded Time-Series Database

A lightweight, high-performance, embedded (like sqlite) time-series database optimized for real-time streaming applications like video, finance, and IoT sensor data.

**I think what makes NanoTS special in comparison to anything else is how disconnected reads are from writes. You should be able to have as many simultaneous readers as you want. In some ways, its almost more of an in memory datastructure than a storage system.**

**Note: If you plan to use nanots in an embedded system where you want to keep the newest data but overwriting the oldest is OK make sure you checkout the auto_reclaim feature.**

## Key Features

- **Ultra-fast writes**: 8.83μs per write on SSD, 300μs on spinning disk
- **Memory-mapped storage**: Lock free storage data structure on memory mapped file allows for maximum throughput.
- **Configurable durability**: Trade-off between performance and data safety with configurable block sizes
- **Crash recovery**: Automatic detection and recovery from unexpected shutdowns
- **Two storage modes**: *Preallocated* (fixed-size files, no surprises on disk usage — ideal for surveillance/embedded) or *Growable* (file extends on demand using BoltDB-style doubling, capped at 1 GiB per grow).
- **Multiple streams**: Store different data streams in the same database file
- **Iterator interface**: Efficient navigation with bidirectional iteration and seeking by timestamp or secondary key
- **Optional secondary key**: Each stream can store a strictly-monotonic `int64` *secondary key* alongside the timestamp (e.g. an external sequence number or exchange-supplied trade ID) and be binary-searched on it
- **Cross Platform**: Currently works on Linux, Windows and MacOS.

## Performance

NanoTS is designed for high-throughput, low-latency applications:

- **113,000+ writes/second per stream** sustained on SSD
- **3,300+ writes/second** on spinning disk
- **Sub-microsecond reads** via memory mapping
- **Efficient seeks** using binary search on timestamps (or on the optional secondary key)

Benchmarks are tracked in a dedicated repo: [nanots_bench](https://github.com/dicroce/nanots_bench). See [RESULTS.md](https://github.com/dicroce/nanots_bench/blob/main/RESULTS.md) for current numbers.

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
- **Frame index**: Timestamp → offset mappings for fast seeks (and an optional secondary key per entry for direct seek-by-key)
- **Frame data**: Variable-size frames with headers and payload

### On-Disk Format Version

The current on-disk format is **v2**. Every nanots file begins with the
4-byte magic `"NTS\0"` at offset 0, followed by `uint16 format_version`
(currently `2`) and `uint16 header_size`. The header also reserves
`uint32 flags` and `uint64 feature_bits` for future format-extension knobs,
so adding new capabilities should not require breaking the format again.

**v2 is not backwards-compatible with v1.** v1 files cannot be opened by a
v2 build — there is no migration tool. Migrate by re-writing data into a
freshly-allocated v2 file.

### Durability Guarantees

- **Frame-level atomicity**: Individual frames are written atomically
- **Block-level durability**: In worst case, lose at most one block of data during crash
- **Configurable trade-offs**: Smaller blocks = better durability, larger blocks = better performance

## Quick Start

### Writing Data

```cpp
#include "nanots.h"

// Option A: Preallocated — 1MB blocks, 16 total blocks. File is the full
// size up front; never grows. Best for fixed-budget storage (surveillance,
// embedded).
nanots_writer::allocate("video.nts", 1024*1024, 16);

// Option B: Growable — file starts at just the 64KB header and extends on
// demand. Pass 0 for max_blocks to grow until disk-full.
nanots_writer::allocate_growable("video.nts", 1024*1024, 0);

// Open the db in auto recycle mode (once full, new data will re-use the oldest blocks).
nanots_writer db("video.nts", true);

// Create write context for a stream
auto wctx = db.create_write_context("camera_1", "stream metadata");

// Every write() takes (flags, timestamp[, secondary_key]). The secondary
// key is optional per stream and defaults to NANOTS_SEC_KEY_UNSET — i.e.
// "this stream has no secondary key." The first write to any given stream
// tag locks the stream as either "keyed" or "unkeyed"; subsequent writes
// must match.
uint8_t frame_data[] = {/* video frame bytes */};
db.write(wctx, frame_data, sizeof(frame_data), flags, timestamp_us);

// Or — for a keyed stream — pass any strictly-increasing int64 as the
// secondary key (any value except INT64_MIN, which is the "unset" sentinel):
auto trade_ctx = db.create_write_context("trades", "BTC-USD ticks");
db.write(trade_ctx, frame_data, sizeof(frame_data),
         flags, timestamp_us, /*sequence_no=*/exchange_trade_id);

```

### Reading Data

```cpp
#include "nanots.h"

// Create iterator for a stream
nanots_iterator iter("video.nts", "camera_1");

// Iterate through all frames. Every frame exposes `timestamp`, `flags`,
// `secondary_key` (NANOTS_SEC_KEY_UNSET on unkeyed streams), and
// `block_sequence`.
while (iter.valid()) {
    auto& frame = *iter;
    process_frame(frame.data, frame.size,
                  frame.timestamp, frame.secondary_key, frame.flags);
    ++iter;
}

// Seek by timestamp (works on any stream):
if (iter.find(target_timestamp)) {
    auto& frame = *iter;
    // ... process frame
}

// Or — on keyed streams — seek by the secondary key:
if (iter.has_secondary_key() && iter.find_by_secondary_key(target_sequence)) {
    auto& frame = *iter;
    // frame.secondary_key >= target_sequence
}
```

### Backward Iteration

```cpp
// Start from end and go backward
iter.find(end_timestamp);
while (iter.valid()) {
    auto& frame = *iter;
    process_frame(frame.data, frame.size,
                  frame.timestamp, frame.secondary_key, frame.flags);
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

### Preallocated vs. Growable

NanoTS supports two storage modes, both using the same on-disk block layout:

```cpp
// Preallocated: file is fully allocated up front and never changes size.
// Predictable disk usage, no fragmentation, fast steady-state writes.
nanots_writer::allocate("data.nts", 1024 * 1024, 100);

// Growable: file starts as just a 64KB header and is extended on demand.
// File size grows BoltDB-style (doubles each time, capped at 1 GiB per
// grow event). Pass max_blocks > 0 to bound the maximum file size.
nanots_writer::allocate_growable("data.nts", 1024 * 1024, /*max_blocks=*/0);
```

Pick **preallocated** when you care about predictable disk usage (surveillance
systems, embedded devices, anything where surprise disk-full failures are
unacceptable). Pick **growable** when you want the file to fit the data —
useful for general time-series workloads where you don't know the data
volume up front.

Growable mode can be combined with `auto_reclaim`: grow until the cap (or
disk-full) is reached, then start recycling the oldest blocks.

Internally, growable files are marked by `n_blocks == 0` in the file header.
Existing preallocated files are unaffected and continue to open as before.

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
- Seek by exchange-supplied trade/sequence ID via the [secondary key](#secondary-key), independent of timestamp

## Advanced Features

### Crash Recovery

NanoTS automatically detects and recovers from crashes:

```cpp
// On startup, validates all blocks and recovers partial writes
nanots_writer db("data.nts");
// Database is automatically validated and ready to use
```

### Secondary Key

Each stream can optionally store an `int64` *secondary key* alongside the
timestamp on every frame. Typical use cases: an external monotonic sequence
number, an exchange-supplied trade ID, or any second ordering you'd like to
seek by without redundantly encoding it in the timestamp.

**Stream-level choice (locked on first write).** The first write to a stream
tag fixes that stream as either *keyed* or *unkeyed*. Mixing yields
`NANOTS_EC_SECONDARY_KEY_MISMATCH`:

```cpp
auto wctx = db.create_write_context("trades", "BTC-USD");

// First write picks the mode: this one's keyed.
db.write(wctx, data, len, /*flags=*/0, ts1, /*sec_key=*/100);

// OK — same mode, strictly-greater key.
db.write(wctx, data, len, 0, ts2, /*sec_key=*/101);

// Throws NANOTS_EC_SECONDARY_KEY_MISMATCH — stream is keyed.
db.write(wctx, data, len, 0, ts3);  // omitted sec_key defaults to UNSET

// Throws NANOTS_EC_NON_MONOTONIC_SECONDARY_KEY — key must strictly increase.
db.write(wctx, data, len, 0, ts4, /*sec_key=*/50);
```

For an *unkeyed* stream, just omit the `secondary_key` argument — it defaults
to `NANOTS_SEC_KEY_UNSET` (which is `INT64_MIN`). Passing any other value —
including `0` — counts as a real secondary key and will lock the stream as
keyed.

**Reading.** Iterators on keyed streams can binary-search by secondary key:

```cpp
nanots_iterator iter("data.nts", "trades");
if (iter.has_secondary_key()) {
    iter.find_by_secondary_key(target_sequence_number);
    while (iter.valid()) {
        auto& frame = *iter;
        // frame.secondary_key is the value the writer passed
        ++iter;
    }
}
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
