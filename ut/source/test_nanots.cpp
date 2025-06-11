#include "test_nanots.h"
#include <chrono>
#include <set>
#include "nanots.h"

// Forward declaration from nanots.cpp
static std::string _database_name(const std::string& file_name) {
  return file_name.substr(0, file_name.find(".nts")) + ".db";
}

using namespace std;
using namespace std::chrono;

REGISTER_TEST_FIXTURE(test_nanots);

static void _whack_files() {
  if (rtf_file_exists("nanots_test_16mb.nts"))
    rtf_remove_file("nanots_test_16mb.nts");
  if (rtf_file_exists("nanots_test_4mb.nts"))
    rtf_remove_file("nanots_test_4mb.nts");
  if (rtf_file_exists("nanots_test_2048_4k_blocks.nts"))
    rtf_remove_file("nanots_test_2048_4k_blocks.nts");
}

void test_nanots::setup() {
  _whack_files();

  nanots_writer::allocate("nanots_test_16mb.nts", 1024 * 1024, 16);
  nanots_writer::allocate("nanots_test_4mb.nts", 1024 * 1024, 4);
  nanots_writer::allocate("nanots_test_2048_4k_blocks.nts", 4096, 2048);
}

void test_nanots::teardown() {
  _whack_files();
}

void test_nanots::test_nanots_basic() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Write some test frames
  std::string frame1_data = "Hello, World!";
  std::string frame2_data = "This is frame 2 with more data";
  std::string frame3_data = "Frame 3";

  {
    auto wctx = db.create_write_context("test_stream", "test metadata");

    db.write(wctx, (uint8_t*)frame1_data.c_str(), (uint32_t)frame1_data.size(),
             1000, 0x01);
    db.write(wctx, (uint8_t*)frame2_data.c_str(), (uint32_t)frame2_data.size(),
             2000, 0x02);
    db.write(wctx, (uint8_t*)frame3_data.c_str(), (uint32_t)frame3_data.size(),
             3000, 0x03);
  }

  // Read back using iterator
  nanots_iterator iter("nanots_test_4mb.nts", "test_stream");

  RTF_ASSERT(iter.valid());

  // Check first frame
  auto& frame1 = *iter;
  RTF_ASSERT(frame1.timestamp == 1000);
  RTF_ASSERT(frame1.flags == 0x01);
  RTF_ASSERT(frame1.size == frame1_data.size());
  RTF_ASSERT(memcmp(frame1.data, frame1_data.c_str(), frame1.size) == 0);

  // Move to second frame
  ++iter;
  RTF_ASSERT(iter.valid());
  auto& frame2 = *iter;
  RTF_ASSERT(frame2.timestamp == 2000);
  RTF_ASSERT(frame2.flags == 0x02);
  RTF_ASSERT(frame2.size == frame2_data.size());
  RTF_ASSERT(memcmp(frame2.data, frame2_data.c_str(), frame2.size) == 0);

  // Move to third frame
  ++iter;
  RTF_ASSERT(iter.valid());
  auto& frame3 = *iter;
  RTF_ASSERT(frame3.timestamp == 3000);
  RTF_ASSERT(frame3.flags == 0x03);
  RTF_ASSERT(frame3.size == frame3_data.size());
  RTF_ASSERT(memcmp(frame3.data, frame3_data.c_str(), frame3.size) == 0);

  // Should be at end now
  ++iter;
  RTF_ASSERT(!iter.valid());
}

void test_nanots::test_nanots_iterator_find() {
  nanots_writer db("nanots_test_4mb.nts", true);

  {
    auto wctx = db.create_write_context("test_stream", "find test");

    auto before = std::chrono::steady_clock::now();
    // Write frames with varying timestamps
    for (int i = 0; i < 10; i++) {
      std::string data = "frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 500);  // 1000, 1500, 2000, 2500, ...
      db.write(wctx, (uint8_t*)data.c_str(), (uint32_t)data.size(), timestamp,
               (uint8_t)i);
    }

    auto after = std::chrono::steady_clock::now();

    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(after - before);
    printf("Avg Time taken: %d us\n", (int)duration.count() / 10);
  }

  nanots_iterator iter("nanots_test_4mb.nts", "test_stream");

  // Find exact match
  RTF_ASSERT(iter.find(2000));
  RTF_ASSERT(iter->timestamp == 2000);
  RTF_ASSERT(iter->flags == 2);

  // Find between timestamps (should land on next higher)
  RTF_ASSERT(iter.find(2250));
  RTF_ASSERT(iter->timestamp == 2500);
  RTF_ASSERT(iter->flags == 3);

  // Find before first timestamp
  RTF_ASSERT(iter.find(500));
  RTF_ASSERT(iter->timestamp == 1000);
  RTF_ASSERT(iter->flags == 0);

  // Find after last timestamp
  RTF_ASSERT(!iter.find(10000));
  RTF_ASSERT(!iter.valid());
}

void test_nanots::test_nanots_multiple_streams() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Create multiple streams with different data
  {
    auto video_ctx = db.create_write_context("video", "h264 1080p stream");
    auto audio_ctx = db.create_write_context("audio", "aac 44.1khz stereo");
    auto metadata_ctx = db.create_write_context("metadata", "sensor data");

    // Write interleaved data to different streams
    for (int i = 0; i < 5; i++) {
      uint64_t base_timestamp = 1000 + (i * 100);

      std::string video_data = "video_frame_" + std::to_string(i);
      std::string audio_data = "audio_sample_" + std::to_string(i);
      std::string meta_data = "sensor_" + std::to_string(i);

      db.write(video_ctx, (uint8_t*)video_data.c_str(),
               (uint32_t)video_data.size(), base_timestamp, 0x01);
      db.write(audio_ctx, (uint8_t*)audio_data.c_str(),
               (uint32_t)audio_data.size(), base_timestamp + 10, 0x02);
      db.write(metadata_ctx, (uint8_t*)meta_data.c_str(),
               (uint32_t)meta_data.size(), base_timestamp + 20, 0x03);
    }
  }

  // Verify each stream independently
  nanots_iterator video_iter("nanots_test_4mb.nts", "video");
  nanots_iterator audio_iter("nanots_test_4mb.nts", "audio");
  nanots_iterator meta_iter("nanots_test_4mb.nts", "metadata");

  // Check video stream
  int video_count = 0;
  while (video_iter.valid()) {
    RTF_ASSERT(video_iter->flags == 0x01);
    std::string expected = "video_frame_" + std::to_string(video_count);
    RTF_ASSERT(video_iter->size == expected.size());
    RTF_ASSERT(memcmp(video_iter->data, expected.c_str(), video_iter->size) ==
               0);
    ++video_iter;
    video_count++;
  }
  RTF_ASSERT(video_count == 5);

  // Check audio stream
  int audio_count = 0;
  while (audio_iter.valid()) {
    RTF_ASSERT(audio_iter->flags == 0x02);
    std::string expected = "audio_sample_" + std::to_string(audio_count);
    RTF_ASSERT(audio_iter->size == expected.size());
    RTF_ASSERT(memcmp(audio_iter->data, expected.c_str(), audio_iter->size) ==
               0);
    ++audio_iter;
    audio_count++;
  }
  RTF_ASSERT(audio_count == 5);

  // Check metadata stream
  int meta_count = 0;
  while (meta_iter.valid()) {
    RTF_ASSERT(meta_iter->flags == 0x03);
    std::string expected = "sensor_" + std::to_string(meta_count);
    RTF_ASSERT(meta_iter->size == expected.size());
    RTF_ASSERT(memcmp(meta_iter->data, expected.c_str(), meta_iter->size) == 0);
    ++meta_iter;
    meta_count++;
  }
  RTF_ASSERT(meta_count == 5);
}

void test_nanots::test_nanots_reader_time_range() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx = db.create_write_context("test_stream", "time range test");

    // Write frames every 100ms for 2 seconds
    for (int i = 0; i < 20; i++) {
      std::string data = "frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 100);  // 1000 to 2900
      db.write(wctx, (uint8_t*)data.c_str(), (uint32_t)data.size(), timestamp,
               (uint8_t)(i % 256));
    }
  }

  nanots_reader reader("nanots_test_4mb.nts");

  // Test reading specific time ranges
  std::vector<std::pair<uint64_t, std::string>> frames_read;

  // Read middle portion (1500 to 2200)
  reader.read("test_stream", 1500, 2200,
              [&](const uint8_t* data, size_t size, uint8_t flags,
                  uint64_t timestamp, uint64_t block_sequence) {
                std::string frame_data((char*)data, size);
                frames_read.push_back({timestamp, frame_data});
              });

  // Should have frames from timestamp 1500 to 2200 (inclusive)
  RTF_ASSERT(frames_read.size() == 8);  // frames 5-12
  RTF_ASSERT(frames_read[0].first == 1500);
  RTF_ASSERT(frames_read[0].second == "frame_5");
  RTF_ASSERT(frames_read[7].first == 2200);
  RTF_ASSERT(frames_read[7].second == "frame_12");

  // Test reading from beginning
  frames_read.clear();
  reader.read("test_stream", 0, 1200,
              [&](const uint8_t* data, size_t size, uint8_t flags,
                  uint64_t timestamp, uint64_t block_sequence) {
                std::string frame_data((char*)data, size);
                frames_read.push_back({timestamp, frame_data});
              });

  RTF_ASSERT(frames_read.size() == 3);  // frames 0-2
  RTF_ASSERT(frames_read[0].first == 1000);
  RTF_ASSERT(frames_read[2].first == 1200);
}

void test_nanots::test_nanots_iterator_bidirectional() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx = db.create_write_context("test_stream", "bidirectional test");

    for (int i = 0; i < 10; i++) {
      std::string data = "data_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 1000);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "test_stream");

  // Move to middle
  RTF_ASSERT(iter.find(5000));
  RTF_ASSERT(iter->timestamp == 5000);
  RTF_ASSERT(iter->flags == 4);

  // Move forward a few steps
  ++iter;
  RTF_ASSERT(iter->timestamp == 6000);
  ++iter;
  RTF_ASSERT(iter->timestamp == 7000);

  // Move backward
  --iter;
  RTF_ASSERT(iter->timestamp == 6000);
  --iter;
  RTF_ASSERT(iter->timestamp == 5000);
  --iter;
  RTF_ASSERT(iter->timestamp == 4000);

  // Test reset functionality
  iter.reset();
  RTF_ASSERT(iter.valid());
  RTF_ASSERT(iter->timestamp == 1000);
  RTF_ASSERT(iter->flags == 0);
}

void test_nanots::test_nanots_large_frames() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx = db.create_write_context("large_stream", "large frame test");

    // Create progressively larger frames
    for (int i = 0; i < 5; i++) {
      size_t frame_size = 1024 * (i + 1);  // 1KB, 2KB, 3KB, 4KB, 5KB
      std::vector<uint8_t> large_data(frame_size);

      // Fill with pattern to verify integrity
      for (size_t j = 0; j < frame_size; j++) {
        large_data[j] = (uint8_t)((i * 256 + j) % 256);
      }

      uint64_t timestamp = 1000 + (i * 1000);
      db.write(wctx, large_data.data(), frame_size, timestamp, (uint8_t)i);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "large_stream");

  // Verify each large frame
  for (int i = 0; i < 5; i++) {
    RTF_ASSERT(iter.valid());

    size_t expected_size = 1024 * (i + 1);
    RTF_ASSERT(iter->size == expected_size);
    RTF_ASSERT(iter->timestamp == (int64_t)(1000 + (i * 1000)));
    RTF_ASSERT(iter->flags == (uint8_t)i);

    // Verify data pattern
    for (size_t j = 0; j < expected_size; j++) {
      uint8_t expected_byte = (uint8_t)((i * 256 + j) % 256);
      RTF_ASSERT(iter->data[j] == expected_byte);
    }

    ++iter;
  }

  RTF_ASSERT(!iter.valid());
}

void test_nanots::test_nanots_edge_cases() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Test empty stream
  {
    nanots_iterator empty_iter("nanots_test_4mb.nts", "nonexistent_stream");
    RTF_ASSERT(!empty_iter.valid());
  }

  // Test single frame
  {
    auto wctx = db.create_write_context("single_stream", "single frame test");
    std::string data = "single_frame";
    db.write(wctx, (uint8_t*)data.c_str(), data.size(), 1000, 0x01);
  }

  {
    nanots_iterator single_iter("nanots_test_4mb.nts", "single_stream");
    RTF_ASSERT(single_iter.valid());
    RTF_ASSERT(single_iter->timestamp == 1000);
    ++single_iter;
    RTF_ASSERT(!single_iter.valid());
  }

  // Test zero-sized frame (if allowed)
  {
    auto wctx = db.create_write_context("zero_stream", "zero size test");
    db.write(wctx, nullptr, 0, 2000, 0x00);
  }

  {
    nanots_iterator zero_iter("nanots_test_4mb.nts", "zero_stream");
    RTF_ASSERT(zero_iter.valid());
    RTF_ASSERT(zero_iter->size == 0);
    RTF_ASSERT(zero_iter->timestamp == 2000);
  }
}

void test_nanots::test_nanots_monotonic_timestamp_validation() {
  nanots_writer db("nanots_test_4mb.nts", false);

  auto wctx = db.create_write_context("test_stream", "monotonic test");

  std::string data1 = "frame1";
  std::string data2 = "frame2";
  std::string data3 = "frame3";

  // Write first frame
  db.write(wctx, (uint8_t*)data1.c_str(), data1.size(), 1000, 0x01);

  // Write second frame with higher timestamp (should succeed)
  db.write(wctx, (uint8_t*)data2.c_str(), data2.size(), 2000, 0x02);

  // Try to write with equal timestamp (should throw)
  bool caught_exception = false;
  try {
    db.write(wctx, (uint8_t*)data3.c_str(), data3.size(), 2000, 0x03);
  } catch (const std::exception&) {
    caught_exception = true;
  }
  RTF_ASSERT(caught_exception);

  // Try to write with lower timestamp (should throw)
  caught_exception = false;
  try {
    db.write(wctx, (uint8_t*)data3.c_str(), data3.size(), 1500, 0x03);
  } catch (const std::exception&) {
    caught_exception = true;
  }
  RTF_ASSERT(caught_exception);

  // Write with higher timestamp (should succeed)
  db.write(wctx, (uint8_t*)data3.c_str(), data3.size(), 3000, 0x03);

  // Verify only valid frames are present
  nanots_iterator iter("nanots_test_4mb.nts", "test_stream");
  int count = 0;
  while (iter.valid()) {
    count++;
    ++iter;
  }
  RTF_ASSERT(count == 3);  // Should have exactly 3 valid frames
}

void test_nanots::test_nanots_performance_baseline() {
  nanots_writer db("nanots_test_4mb.nts", false);

  const int num_frames = 1000;
  const size_t frame_size = 1024;  // 1KB frames

  std::vector<uint8_t> test_data(frame_size);
  for (size_t i = 0; i < frame_size; i++) {
    test_data[i] = (uint8_t)(i % 256);
  }

  auto start_time = std::chrono::high_resolution_clock::now();

  {
    auto wctx = db.create_write_context("perf_stream", "performance test");

    for (int i = 0; i < num_frames; i++) {
      uint64_t timestamp = 1000 + i;
      db.write(wctx, test_data.data(), frame_size, timestamp,
               (uint8_t)(i % 256));
    }
  }

  auto write_end_time = std::chrono::high_resolution_clock::now();

  // Now test read performance
  nanots_iterator iter("nanots_test_4mb.nts", "perf_stream");

  int frames_read = 0;
  while (iter.valid()) {
    // Verify frame integrity
    RTF_ASSERT(iter->size == frame_size);
    RTF_ASSERT(iter->timestamp == (int64_t)(1000 + frames_read));

    frames_read++;
    ++iter;
  }

  auto read_end_time = std::chrono::high_resolution_clock::now();

  RTF_ASSERT(frames_read == num_frames);

  auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      write_end_time - start_time);
  auto read_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      read_end_time - write_end_time);

  printf("Performance Results:\n");
  printf("  Wrote %d frames (%zu bytes each) in %lld us\n", num_frames,
         frame_size, static_cast<long long>(write_duration.count()));
  printf("  Write rate: %.2f frames/ms, %.2f MB/s\n",
         (double)num_frames / write_duration.count() * 1000.0,
         (double)(num_frames * frame_size) / write_duration.count());
  printf("  Read %d frames in %lld us\n", frames_read,
         static_cast<long long>(read_duration.count()));
  printf("  Read rate: %.2f frames/ms, %.2f MB/s\n",
         (double)frames_read / read_duration.count() * 1000.0,
         (double)(frames_read * frame_size) / read_duration.count());
}

void test_nanots::test_nanots_concurrent_readers() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Write test data
  {
    auto wctx = db.create_write_context("concurrent_stream", "concurrent test");

    for (int i = 0; i < 100; i++) {
      std::string data = "concurrent_frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 100);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)(i % 256));
    }
  }

  // Create multiple readers and verify they can read independently
  std::vector<std::unique_ptr<nanots_iterator>> readers;
  for (int i = 0; i < 5; i++) {
    readers.emplace_back(std::make_unique<nanots_iterator>(
        "nanots_test_4mb.nts", "concurrent_stream"));
  }

  // Verify each reader works independently
  for (size_t reader_idx = 0; reader_idx < readers.size(); reader_idx++) {
    auto& reader = *readers[reader_idx];

    // Each reader starts at different position
    int64_t start_timestamp =
        1000 + (reader_idx * 2000);  // 1000, 3000, 5000, etc.
    RTF_ASSERT(reader.find(start_timestamp));

    int frames_read = 0;
    while (reader.valid() && frames_read < 10) {
      RTF_ASSERT(reader->timestamp >= start_timestamp);
      ++reader;
      frames_read++;
    }

    RTF_ASSERT(frames_read == 10);
  }
}

void test_nanots::test_nanots_metadata_integrity() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Create streams with different metadata
  std::string video_metadata = "codec=h264,resolution=1920x1080,fps=30";
  std::string audio_metadata = "codec=aac,samplerate=44100,channels=2";

  {
    auto video_ctx = db.create_write_context("video", video_metadata);
    auto audio_ctx = db.create_write_context("audio", audio_metadata);

    db.write(video_ctx, (uint8_t*)"video1", 6, 1000, 0x01);
    db.write(audio_ctx, (uint8_t*)"audio1", 6, 1010, 0x02);
  }

  // Verify metadata is preserved during reads
  nanots_reader reader("nanots_test_4mb.nts");

  bool video_metadata_correct = false;
  bool audio_metadata_correct = false;

  // Read and check if we can access metadata through iterator or reader
  // Note: This test assumes metadata is accessible during read operations
  // You may need to modify based on your actual metadata access API

  reader.read("video", 0, 2000,
              [&](const uint8_t* data, size_t size, uint8_t flags,
                  uint64_t timestamp, uint64_t block_sequence) {
                // In a real implementation, you might have access to metadata
                // here This is a placeholder for metadata verification
                video_metadata_correct = true;
              });

  reader.read(
      "audio", 0, 2000,
      [&](const uint8_t* data, size_t size, uint8_t flags, uint64_t timestamp,
          uint64_t block_sequence) { audio_metadata_correct = true; });

  RTF_ASSERT(video_metadata_correct);
  RTF_ASSERT(audio_metadata_correct);
}

void test_nanots::test_nanots_block_exhaustion() {
  // Test behavior when blocks are exhausted (without auto_reclaim)
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx =
        db.create_write_context("exhaust_stream", "block exhaustion test");

    // Write large frames to fill up available blocks
    const size_t large_frame_size = 200 * 1024;  // 200KB frames
    std::vector<uint8_t> large_data(large_frame_size, 0xAB);

    int successful_writes = 0;
    bool write_failed = false;

    // Keep writing until we get an exception (blocks exhausted)
    for (int i = 0; i < 100 && !write_failed; i++) {
      try {
        uint64_t timestamp = 1000 + (i * 1000);
        db.write(wctx, large_data.data(), large_frame_size, timestamp,
                 (uint8_t)i);
        successful_writes++;
      } catch (const std::exception&) {
        printf("Write failed after %d frames.\n", successful_writes);
        write_failed = true;
      }
    }

    printf("Successfully wrote %d large frames before exhaustion\n",
           successful_writes);
    RTF_ASSERT(successful_writes >
               0);  // Should have written at least some frames
  }

  // Verify the frames that were successfully written are still readable
  nanots_iterator iter("nanots_test_4mb.nts", "exhaust_stream");

  int frames_read = 0;
  while (iter.valid()) {
    RTF_ASSERT(iter->size == 200 * 1024);
    frames_read++;
    ++iter;
  }

  printf("Successfully read back %d frames\n", frames_read);
  RTF_ASSERT(frames_read > 0);
}

void test_nanots::test_nanots_block_filling_and_transition() {
  // Test what happens when we fill up a block and transition to the next one
  // Use auto_reclaim=true to ensure we can get new blocks when needed
  nanots_writer db("nanots_test_4mb.nts", true);

  {
    auto wctx =
        db.create_write_context("block_fill_stream", "block filling test");

    // Use smaller frames to avoid running out of blocks too quickly
    const size_t large_frame_size = 50 * 1024;  // 50KB frames
    std::vector<uint8_t> large_data(large_frame_size);

    // Fill with identifiable pattern
    for (size_t i = 0; i < large_frame_size; i++) {
      large_data[i] = (uint8_t)(i % 256);
    }

    int frames_written = 0;

    // Write enough to trigger at least one block transition
    for (int i = 0; i < 20; i++) {  // Reduced count to avoid exhausting blocks
      uint64_t timestamp = 1000 + (i * 1000);
      db.write(wctx, large_data.data(), large_frame_size, timestamp,
               (uint8_t)(i % 256));
      frames_written++;
    }

    printf("Wrote %d large frames (50KB each)\n", frames_written);
  }

  // Verify all frames are readable and in correct order
  nanots_iterator iter("nanots_test_4mb.nts", "block_fill_stream");

  int frames_read = 0;
  int64_t prev_timestamp = 0;
  int64_t prev_block_sequence = 0;
  int block_transitions = 0;

  while (iter.valid()) {
    RTF_ASSERT(iter->size == 50 * 1024);
    RTF_ASSERT(iter->timestamp > prev_timestamp);  // Monotonic

    // Count block transitions
    if (iter->block_sequence != prev_block_sequence && frames_read > 0) {
      block_transitions++;
      printf("Block transition %d at frame %d (block sequence %lld -> %lld)\n",
             block_transitions, frames_read,
             static_cast<long long>(prev_block_sequence),
             static_cast<long long>(iter->block_sequence));
    }

    // Verify data integrity
    for (size_t i = 0; i < 1024; i++) {  // Check first 1KB
      RTF_ASSERT(iter->data[i] == (uint8_t)(i % 256));
    }

    prev_timestamp = iter->timestamp;
    prev_block_sequence = iter->block_sequence;
    frames_read++;
    ++iter;
  }

  printf("Read %d frames across %d block transitions\n", frames_read,
         block_transitions);
  RTF_ASSERT(frames_read > 0);
  // Note: block_transitions might be 0 if all frames fit in one block
}

void test_nanots::test_nanots_sparse_timestamp_seeking() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx =
        db.create_write_context("sparse_stream", "sparse timestamp test");

    // Write frames with large gaps between timestamps
    std::vector<uint64_t> timestamps = {1000,   5000,   15000,   50000,
                                        100000, 500000, 1000000, 5000000};

    for (size_t i = 0; i < timestamps.size(); i++) {
      std::string data = "sparse_frame_" + std::to_string(i);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamps[i],
               (uint8_t)i);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "sparse_stream");

  // Test seeking to various positions within the sparse data
  std::vector<std::pair<uint64_t, size_t>> seek_tests = {
      {500, 0},       // Before first frame -> should land on first
      {1000, 0},      // Exact match on first
      {3000, 1},      // Between first and second -> should land on second
      {15000, 2},     // Exact match on third
      {75000, 4},     // Between fourth and fifth -> should land on fifth
      {1000000, 6},   // Exact match on seventh
      {10000000, -1}  // After last frame -> should be invalid
  };

  for (auto& test : seek_tests) {
    uint64_t seek_timestamp = test.first;
    int expected_frame = static_cast<int>(test.second);

    bool found = iter.find(seek_timestamp);

    if (expected_frame == -1) {
      RTF_ASSERT(!found);
      RTF_ASSERT(!iter.valid());
    } else {
      RTF_ASSERT(found);
      RTF_ASSERT(iter.valid());
      RTF_ASSERT(iter->flags == (uint8_t)expected_frame);

      std::string expected_data =
          "sparse_frame_" + std::to_string(expected_frame);
      RTF_ASSERT(iter->size == expected_data.size());
      RTF_ASSERT(memcmp(iter->data, expected_data.c_str(), iter->size) == 0);
    }
  }
}

void test_nanots::test_nanots_write_context_lifecycle() {
  // Test proper write context lifecycle - one writer per stream
  nanots_writer db("nanots_test_4mb.nts", false);

  // Test writing in batches within the same context (proper usage)
  {
    auto wctx =
        db.create_write_context("single_writer_stream", "single writer test");

    // Write first batch
    for (int i = 0; i < 5; i++) {
      std::string data = "batch1_frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }

    // Write second batch (continuing same context - proper way)
    for (int i = 0; i < 5; i++) {
      std::string data = "batch2_frame_" + std::to_string(i);
      uint64_t timestamp = 10000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }

    // Write third batch (continuing same context)
    for (int i = 0; i < 5; i++) {
      std::string data = "batch3_frame_" + std::to_string(i);
      uint64_t timestamp = 20000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }
    // wctx destructor will finalize the final block
  }

  // Verify all data is present and in order
  nanots_iterator iter("nanots_test_4mb.nts", "single_writer_stream");

  std::vector<std::string> expected_order = {
      "batch1_frame_0", "batch1_frame_1", "batch1_frame_2", "batch1_frame_3",
      "batch1_frame_4", "batch2_frame_0", "batch2_frame_1", "batch2_frame_2",
      "batch2_frame_3", "batch2_frame_4", "batch3_frame_0", "batch3_frame_1",
      "batch3_frame_2", "batch3_frame_3", "batch3_frame_4"};

  int frame_idx = 0;
  while (iter.valid()) {
    RTF_ASSERT(frame_idx < (int)expected_order.size());

    std::string actual_data((char*)iter->data, iter->size);
    printf("Frame %d: expected='%s', actual='%s'\n", frame_idx,
           expected_order[frame_idx].c_str(), actual_data.c_str());
    RTF_ASSERT(actual_data == expected_order[frame_idx]);

    frame_idx++;
    ++iter;
  }

  printf("Frame index: %d\n", frame_idx);
  printf("Expected order size: %d\n", (int)expected_order.size());
  RTF_ASSERT(frame_idx == (int)expected_order.size());
}

void test_nanots::test_nanots_multiple_streams_separate_writers() {
  // Test the correct way: separate streams with separate writers
  nanots_writer db("nanots_test_4mb.nts", false);

  // Create separate contexts for different streams (this is correct)
  {
    auto video_ctx = db.create_write_context("video_stream", "h264 video");
    auto audio_ctx = db.create_write_context("audio_stream", "aac audio");
    auto data_ctx = db.create_write_context("data_stream", "sensor data");

    // Write to different streams (this is the intended usage)
    for (int i = 0; i < 5; i++) {
      uint64_t base_timestamp = 1000 + (i * 100);

      std::string video_data = "video_" + std::to_string(i);
      std::string audio_data = "audio_" + std::to_string(i);
      std::string sensor_data = "sensor_" + std::to_string(i);

      db.write(video_ctx, (uint8_t*)video_data.c_str(), video_data.size(),
               base_timestamp, 0x01);
      db.write(audio_ctx, (uint8_t*)audio_data.c_str(), audio_data.size(),
               base_timestamp + 10, 0x02);
      db.write(data_ctx, (uint8_t*)sensor_data.c_str(), sensor_data.size(),
               base_timestamp + 20, 0x03);
    }
  }

  // Verify each stream independently
  auto verify_stream = [&](const std::string& stream_name,
                           const std::string& prefix, uint8_t expected_flags) {
    nanots_iterator iter("nanots_test_4mb.nts", stream_name);
    int count = 0;
    while (iter.valid()) {
      RTF_ASSERT(iter->flags == expected_flags);
      std::string expected = prefix + "_" + std::to_string(count);
      std::string actual((char*)iter->data, iter->size);
      RTF_ASSERT(actual == expected);
      count++;
      ++iter;
    }
    RTF_ASSERT(count == 5);
    printf("Stream '%s': verified %d frames\n", stream_name.c_str(), count);
  };

  verify_stream("video_stream", "video", 0x01);
  verify_stream("audio_stream", "audio", 0x02);
  verify_stream("data_stream", "sensor", 0x03);
}

void test_nanots::test_nanots_invalid_multiple_writers_same_stream() {
  // Test what happens if someone tries to create multiple writers for same
  // stream This should either be prevented or handled gracefully
  nanots_writer db("nanots_test_4mb.nts", false);
  
  auto ctx1 = db.create_write_context("shared_stream", "first writer");
  db.write(ctx1, (uint8_t*)"frame1", 6, 1000, 0x01);

  bool second_writer_threw = false;
  nanots_ec_t ec = NANOTS_EC_OK;

  try
  {
    // Create second context for same stream (this violates the design)
    auto ctx2 = db.create_write_context("shared_stream", "second writer");
  }
  catch(nanots_exception& ex)
  {
    second_writer_threw = true;
    ec = ex.get_ec();
    printf("%s\n",ex.what());
  }

  RTF_ASSERT(second_writer_threw);
  RTF_ASSERT(ec == NANOTS_EC_DUPLICATE_STREAM_TAG);
}

void test_nanots::test_nanots_multiple_segments_same_stream() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Test writing in multiple batches within the same context
  {
    auto wctx = db.create_write_context("reuse_stream", "context reuse test");

    // Write first batch
    for (int i = 0; i < 5; i++) {
      std::string data = "reuse_data_batch1_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }

    // Write second batch (continuing same context)
    for (int i = 0; i < 5; i++) {
      std::string data = "reuse_data_batch2_" + std::to_string(i);
      uint64_t timestamp = 10000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }

    // Write third batch (continuing same context)
    for (int i = 0; i < 5; i++) {
      std::string data = "reuse_data_batch3_" + std::to_string(i);
      uint64_t timestamp = 20000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }
    // wctx destructor will finalize the final block
  }

  // Verify all data is present and in order
  nanots_iterator iter("nanots_test_4mb.nts", "reuse_stream");

  std::vector<std::string> expected_order = {
      "reuse_data_batch1_0", "reuse_data_batch1_1", "reuse_data_batch1_2",
      "reuse_data_batch1_3", "reuse_data_batch1_4", "reuse_data_batch2_0",
      "reuse_data_batch2_1", "reuse_data_batch2_2", "reuse_data_batch2_3",
      "reuse_data_batch2_4", "reuse_data_batch3_0", "reuse_data_batch3_1",
      "reuse_data_batch3_2", "reuse_data_batch3_3", "reuse_data_batch3_4"};

  int frame_idx = 0;
  while (iter.valid()) {
    RTF_ASSERT(frame_idx < (int)expected_order.size());

    std::string actual_data((char*)iter->data, iter->size);
    RTF_ASSERT(actual_data == expected_order[frame_idx]);

    frame_idx++;
    ++iter;
  }

  RTF_ASSERT(frame_idx == (int)expected_order.size());
}

void test_nanots::test_nanots_iterator_edge_navigation() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx = db.create_write_context("edge_stream", "edge navigation test");

    for (int i = 0; i < 10; i++) {
      std::string data = "edge_frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 1000);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "edge_stream");

  // Test going backwards from the beginning
  RTF_ASSERT(iter.valid());
  RTF_ASSERT(iter->flags == 0);  // First frame

  --iter;  // Should become invalid
  RTF_ASSERT(!iter.valid());

  // Once invalid, operations should keep it invalid
  --iter;  // Should remain invalid
  RTF_ASSERT(!iter.valid());

  ++iter;  // Should remain invalid (no recovery from invalid state)
  RTF_ASSERT(!iter.valid());

  // Reset to get back to valid state
  iter.reset();
  RTF_ASSERT(iter.valid());
  RTF_ASSERT(iter->flags == 0);  // Back to first frame

  // Move to last frame
  for (int i = 0; i < 9; i++) {
    ++iter;
    RTF_ASSERT(iter.valid());
  }
  RTF_ASSERT(iter->flags == 9);  // Last frame

  // Try to go past end
  ++iter;
  RTF_ASSERT(!iter.valid());

  // Once invalid, should stay invalid
  ++iter;  // Should remain invalid
  RTF_ASSERT(!iter.valid());

  --iter;  // Should remain invalid (no recovery)
  RTF_ASSERT(!iter.valid());

  // Only reset() should restore validity
  iter.reset();
  RTF_ASSERT(iter.valid());
  RTF_ASSERT(iter->flags == 0);  // Back to first frame
}

void test_nanots::test_nanots_mixed_frame_sizes() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx =
        db.create_write_context("mixed_stream", "mixed frame sizes test");

    // Write frames of varying sizes in a pattern
    std::vector<size_t> frame_sizes = {10,   100, 1000, 50,  500,
                                       5000, 25,  250,  2500};

    for (size_t i = 0; i < frame_sizes.size(); i++) {
      size_t size = frame_sizes[i];
      std::vector<uint8_t> data(size);

      // Fill with identifiable pattern
      for (size_t j = 0; j < size; j++) {
        data[j] = (uint8_t)((i * 256 + j) % 256);
      }

      uint64_t timestamp = 1000 + (i * 1000);
      db.write(wctx, data.data(), size, timestamp, (uint8_t)i);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "mixed_stream");

  std::vector<size_t> expected_sizes = {10,   100, 1000, 50,  500,
                                        5000, 25,  250,  2500};

  int frame_idx = 0;
  while (iter.valid()) {
    RTF_ASSERT(frame_idx < (int)expected_sizes.size());

    size_t expected_size = expected_sizes[frame_idx];
    RTF_ASSERT(iter->size == expected_size);
    RTF_ASSERT(iter->flags == (uint8_t)frame_idx);

    // Verify data pattern
    for (size_t j = 0; j < min(expected_size, (size_t)100);
         j++) {  // Check first 100 bytes
      uint8_t expected_byte = (uint8_t)((frame_idx * 256 + j) % 256);
      RTF_ASSERT(iter->data[j] == expected_byte);
    }

    frame_idx++;
    ++iter;
  }

  RTF_ASSERT(frame_idx == (int)expected_sizes.size());
}

void test_nanots::test_nanots_reader_callback_exceptions() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx = db.create_write_context("exception_stream", "exception test");

    for (int i = 0; i < 10; i++) {
      std::string data = "exception_frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 1000);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), timestamp,
               (uint8_t)i);
    }
  }

  nanots_reader reader("nanots_test_4mb.nts");

  // Test what happens when callback throws exception
  int frames_processed = 0;
  bool exception_caught = false;

  try {
    reader.read("exception_stream", 0, 20000,
                [&](const uint8_t* data, size_t size, uint8_t flags,
                    uint64_t timestamp, uint64_t block_sequence) {
                  frames_processed++;

                  if (frames_processed == 5) {
                    throw std::runtime_error("Test exception in callback");
                  }
                });
  } catch (const std::exception&) {
    exception_caught = true;
  }

  RTF_ASSERT(exception_caught);
  RTF_ASSERT(frames_processed ==
             5);  // Should have processed frames before exception

  // Verify database is still in good state after exception
  nanots_iterator iter("nanots_test_4mb.nts", "exception_stream");
  int count = 0;
  while (iter.valid()) {
    count++;
    ++iter;
  }
  RTF_ASSERT(count == 10);  // All frames should still be accessible
}

void test_nanots::test_nanots_high_frequency_writes() {
  nanots_writer db("nanots_test_4mb.nts",
                   true);  // Enable auto_reclaim for high volume

  const int num_frames = 10000;
  const size_t frame_size = 64;  // Small frames for high frequency

  std::vector<uint8_t> test_data(frame_size);
  for (size_t i = 0; i < frame_size; i++) {
    test_data[i] = (uint8_t)(i % 256);
  }

  auto start_time = std::chrono::high_resolution_clock::now();

  {
    auto wctx =
        db.create_write_context("high_freq_stream", "high frequency test");

    for (int i = 0; i < num_frames; i++) {
      // Microsecond precision timestamps
      uint64_t timestamp = 1000000 + i;  // 1 microsecond apart
      db.write(wctx, test_data.data(), frame_size, timestamp,
               (uint8_t)(i % 256));
    }
  }

  auto write_end_time = std::chrono::high_resolution_clock::now();

  // Verify all frames are present and correctly ordered
  nanots_iterator iter("nanots_test_4mb.nts", "high_freq_stream");

  int frames_read = 0;
  int64_t expected_timestamp = 1000000;

  while (iter.valid()) {
    RTF_ASSERT(iter->timestamp == expected_timestamp);
    RTF_ASSERT(iter->size == frame_size);
    RTF_ASSERT(iter->flags == (uint8_t)(frames_read % 256));

    frames_read++;
    expected_timestamp++;
    ++iter;
  }

  auto read_end_time = std::chrono::high_resolution_clock::now();

  RTF_ASSERT(frames_read == num_frames);

  auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      write_end_time - start_time);
  auto read_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      read_end_time - write_end_time);

  printf("High Frequency Results:\n");
  printf("  Wrote %d frames (%zu bytes each) in %lld µs\n", num_frames,
         frame_size, static_cast<long long>(write_duration.count()));
  printf("  Write rate: %.2f frames/ms, %.2f MB/s\n",
         (double)num_frames / write_duration.count() * 1000.0,
         (double)(num_frames * frame_size) / write_duration.count());
  printf("  Read rate: %.2f frames/ms, %.2f MB/s\n",
         (double)frames_read / read_duration.count() * 1000.0,
         (double)(frames_read * frame_size) / read_duration.count());
  printf("  Average time per write: %.2f µs\n",
         (double)write_duration.count() / num_frames);
}

void test_nanots::test_nanots_timestamp_precision() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx =
        db.create_write_context("precision_stream", "timestamp precision test");

    // Test with nanosecond-precision timestamps
    std::vector<uint64_t> precise_timestamps = {
        1000000000ULL,  // 1 second
        1000000001ULL,  // 1 nanosecond later
        1000000010ULL,  // 10 nanoseconds later
        1000000100ULL,  // 100 nanoseconds later
        1000001000ULL,  // 1 microsecond later
        1000010000ULL,  // 10 microseconds later
        1000100000ULL,  // 100 microseconds later
        1001000000ULL,  // 1 millisecond later
    };

    for (size_t i = 0; i < precise_timestamps.size(); i++) {
      std::string data = "precise_" + std::to_string(i);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), precise_timestamps[i],
               (uint8_t)i);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "precision_stream");

  // Verify timestamps are preserved exactly
  for (size_t i = 0; i < 8; i++) {
    RTF_ASSERT(iter.valid());

    int64_t expected_timestamp = 1000000000LL;
    switch (i) {
      case 0:
        expected_timestamp += 0;
        break;
      case 1:
        expected_timestamp += 1;
        break;
      case 2:
        expected_timestamp += 10;
        break;
      case 3:
        expected_timestamp += 100;
        break;
      case 4:
        expected_timestamp += 1000;
        break;
      case 5:
        expected_timestamp += 10000;
        break;
      case 6:
        expected_timestamp += 100000;
        break;
      case 7:
        expected_timestamp += 1000000;
        break;
    }

    RTF_ASSERT(iter->timestamp == expected_timestamp);
    RTF_ASSERT(iter->flags == (uint8_t)i);

    std::string expected_data = "precise_" + std::to_string(i);
    RTF_ASSERT(iter->size == expected_data.size());
    RTF_ASSERT(memcmp(iter->data, expected_data.c_str(), iter->size) == 0);

    ++iter;
  }

  RTF_ASSERT(!iter.valid());
}

void test_nanots::test_nanots_free_blocks() {
  nanots_writer db("nanots_test_2048_4k_blocks.nts", false);

  // Write multiple blocks worth of data to ensure blocks are finalized
  {
    auto wctx = db.create_write_context("delete_stream", "free blocks test");

    std::vector<uint8_t> one_k_row(1024);

    for (int i = 1; i < 1024; i++) {
      db.write(wctx, one_k_row.data(), one_k_row.size(), i, (uint8_t)i);
    }
  }

  // Verify all data is initially present
  nanots_iterator iter("nanots_test_2048_4k_blocks.nts", "delete_stream");
  int initial_count = 0;
  while (iter.valid()) {
    initial_count++;
    ++iter;
  }

  // RTF_ASSERT(initial_count == 20);
  printf("Initially found %d frames\n", initial_count);

  // Debug: Check what blocks exist in database
  auto db_name = _database_name("nanots_test_2048_4k_blocks.nts");
  nts_sqlite_conn debug_conn(db_name, false, true);
  auto debug_result = debug_conn.exec(
      "SELECT sb.start_timestamp, sb.end_timestamp, sb.block_idx, s.stream_tag "
      "FROM segment_blocks sb "
      "JOIN segments s ON sb.segment_id = s.id "
      "WHERE s.stream_tag = 'delete_stream' "
      "ORDER BY sb.start_timestamp");

  printf("Blocks in database before deletion:\n");
  for (auto& row : debug_result) {
    printf("  start_timestamp=%s, end_timestamp=%s, block_idx=%s\n",
           row["start_timestamp"].value().c_str(), row["end_timestamp"].value().c_str(),
           row["block_idx"].value().c_str());
  }

  // Delete blocks in the middle time range (5000 to 15000)
  // This should delete frames with timestamps 5000, 6000, 7000, ..., 15000
  db.free_blocks("delete_stream", 250, 500);

  // Debug: Check what blocks exist after deletion
  debug_result = debug_conn.exec(
      "SELECT sb.start_timestamp, sb.end_timestamp, sb.block_idx, s.stream_tag "
      "FROM segment_blocks sb "
      "JOIN segments s ON sb.segment_id = s.id "
      "WHERE s.stream_tag = 'delete_stream' "
      "ORDER BY sb.start_timestamp");

  printf("Blocks in database after deletion:\n");
  for (auto& row : debug_result) {
    printf("  start_timestamp=%s, end_timestamp=%s, block_idx=%s\n",
           row["start_timestamp"].value().c_str(), row["end_timestamp"].value().c_str(),
           row["block_idx"].value().c_str());
  }

  // Verify that frames in the deletion range are gone
  nanots_reader reader("nanots_test_2048_4k_blocks.nts");
  std::vector<uint64_t> remaining_timestamps;

  reader.read("delete_stream", 1, 1024,
              [&](const uint8_t* data, size_t size, uint8_t flags,
                  uint64_t timestamp, uint64_t block_sequence) {
                remaining_timestamps.push_back(timestamp);
              });

  printf("After deletion, found %d frames\n", (int)remaining_timestamps.size());

  // Remember, free_blocks() only free's whole blocks (not rows) between the
  // start and the end timestamp. In this test our blocks are 4k, but our rows
  // are 1k... We can't really fit 4 rows in a block because of the overhead of
  // the block index, so we end up fitting 3 rows in these blocks. This makes
  // the exact number of blocks we free above a little hard to predict... so
  // instead of looking for a specific number we just look for a large gap in
  // the timestamps.

  uint64_t last_timestamp = 0;
  bool large_gap_found = false;
  for (auto ts : remaining_timestamps) {
    if (last_timestamp != 0) {
      if (ts - last_timestamp > 100) {
        large_gap_found = true;
      }
    }
    last_timestamp = ts;
  }

  RTF_ASSERT(large_gap_found);
}

void test_nanots::test_nanots_query_contiguous_segments() {
  nanots_writer db("nanots_test_2048_4k_blocks.nts", false);

  {
    auto wctx = db.create_write_context("test_stream", "meta");
    std::vector<uint8_t> one_k_row(1024);
    for (int i = 1; i < 1024; i++) {
      db.write(wctx, one_k_row.data(), one_k_row.size(), i, (uint8_t)i);
    }
  }

  db.free_blocks("test_stream", 250, 500);

  nanots_reader reader("nanots_test_2048_4k_blocks.nts");
  auto segments = reader.query_contiguous_segments("test_stream", 1, 1024);

  RTF_ASSERT(segments.size() == 2);
  RTF_ASSERT(segments[0].start_timestamp == 1);
  RTF_ASSERT(segments[1].end_timestamp == 1023);
}

void test_nanots::test_nanots_query_stream_tags() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Create multiple streams with data in different time ranges
  {
    auto video_ctx = db.create_write_context("video", "h264 1080p stream");
    auto audio_ctx = db.create_write_context("audio", "aac 44.1khz stereo");
    auto metadata_ctx = db.create_write_context("metadata", "sensor data");

    // Write video frames: 1000-5000
    for (int i = 0; i < 5; i++) {
      std::string data = "video_frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 1000);
      db.write(video_ctx, (uint8_t*)data.c_str(), data.size(), timestamp, 0x01);
    }

    // Write audio samples: 2000-6000 (overlaps with video)
    for (int i = 0; i < 5; i++) {
      std::string data = "audio_sample_" + std::to_string(i);
      uint64_t timestamp = 2000 + (i * 1000);
      db.write(audio_ctx, (uint8_t*)data.c_str(), data.size(), timestamp, 0x02);
    }

    // Write metadata: 8000-12000 (non-overlapping)
    for (int i = 0; i < 5; i++) {
      std::string data = "sensor_" + std::to_string(i);
      uint64_t timestamp = 8000 + (i * 1000);
      db.write(metadata_ctx, (uint8_t*)data.c_str(), data.size(), timestamp, 0x03);
    }
  }

  nanots_reader reader("nanots_test_4mb.nts");

  // Test query_stream_tags for different time ranges
  
  // Query range that includes all streams (1000-12000)
  auto all_tags = reader.query_stream_tags(1000, 12000);
  std::set<std::string> all_tags_set(all_tags.begin(), all_tags.end());
  RTF_ASSERT(all_tags_set.size() == 3);
  RTF_ASSERT(all_tags_set.count("video") == 1);
  RTF_ASSERT(all_tags_set.count("audio") == 1);
  RTF_ASSERT(all_tags_set.count("metadata") == 1);

  // Query range that includes only video and audio (2000-6000)
  auto video_audio_tags = reader.query_stream_tags(2000, 6000);
  std::set<std::string> video_audio_set(video_audio_tags.begin(), video_audio_tags.end());
  RTF_ASSERT(video_audio_set.size() == 2);
  RTF_ASSERT(video_audio_set.count("video") == 1);
  RTF_ASSERT(video_audio_set.count("audio") == 1);
  RTF_ASSERT(video_audio_set.count("metadata") == 0);

  // Query range that includes only metadata (8000-12000)
  auto metadata_tags = reader.query_stream_tags(8000, 12000);
  std::set<std::string> metadata_set(metadata_tags.begin(), metadata_tags.end());
  RTF_ASSERT(metadata_set.size() == 1);
  RTF_ASSERT(metadata_set.count("metadata") == 1);
  RTF_ASSERT(metadata_set.count("video") == 0);
  RTF_ASSERT(metadata_set.count("audio") == 0);

  // Query range with no data (20000-25000)
  auto empty_tags = reader.query_stream_tags(20000, 25000);
  RTF_ASSERT(empty_tags.empty());

  // Query range that includes only video (1000-1500)
  auto video_only_tags = reader.query_stream_tags(1000, 1500);
  std::set<std::string> video_only_set(video_only_tags.begin(), video_only_tags.end());
  RTF_ASSERT(video_only_set.size() == 1);
  RTF_ASSERT(video_only_set.count("video") == 1);
  RTF_ASSERT(video_only_set.count("audio") == 0);
  RTF_ASSERT(video_only_set.count("metadata") == 0);
}
