#include "test_nanots.h"
#include <atomic>
#include <chrono>
#include <memory>
#include <set>
#include <thread>
#include <vector>
#include <inttypes.h>
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

  for (const char* name :
       {"nanots_growable_basic", "nanots_growable_doubling", "nanots_growable_cap",
        "nanots_growable_concurrent"}) {
    std::string nts = std::string(name) + ".nts";
    std::string db  = std::string(name) + ".db";
    std::string shm = db + "-shm";
    std::string wal = db + "-wal";
    if (rtf_file_exists(nts.c_str())) rtf_remove_file(nts.c_str());
    if (rtf_file_exists(db.c_str())) rtf_remove_file(db.c_str());
    if (rtf_file_exists(shm.c_str())) rtf_remove_file(shm.c_str());
    if (rtf_file_exists(wal.c_str())) rtf_remove_file(wal.c_str());
  }
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

    db.write(wctx, (uint8_t*)frame1_data.c_str(), (uint32_t)frame1_data.size(), 0x01,
             1000);
    db.write(wctx, (uint8_t*)frame2_data.c_str(), (uint32_t)frame2_data.size(), 0x02,
             2000);
    db.write(wctx, (uint8_t*)frame3_data.c_str(), (uint32_t)frame3_data.size(), 0x03,
             3000);
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
      db.write(wctx, (uint8_t*)data.c_str(), (uint32_t)data.size(),
               (uint8_t)i, timestamp);
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

// find() caches the range of the block it selects. Walking into another block
// must update that range; otherwise a later seek into the old range takes the
// fast path but searches the iterator's new physical block.
void test_nanots::test_nanots_iterator_reseek_after_block_transition() {
  nanots_writer db("nanots_test_2048_4k_blocks.nts", false);

  constexpr int N = 12;
  {
    auto wctx = db.create_write_context("reseek", "");
    // allocate() rounds this fixture's requested 4 KiB blocks to NanoTS's
    // 64 KiB minimum. A 16 KiB payload guarantees several transitions.
    std::vector<uint8_t> row(16 * 1024, 0x5A);
    for (int i = 0; i < N; ++i) {
      db.write(wctx, row.data(), row.size(), static_cast<uint32_t>(i),
               1000 + (i * 10), 500 + i);
    }
  }

  nanots_iterator iter("nanots_test_2048_4k_blocks.nts", "reseek");
  RTF_ASSERT(iter.find(1000, 500));
  RTF_ASSERT(iter->timestamp == 1000);
  RTF_ASSERT(iter->secondary_key == 500);

  const int64_t first_block = iter->block_sequence;
  while (iter.valid() && iter->block_sequence == first_block)
    ++iter;

  RTF_ASSERT(iter.valid());
  RTF_ASSERT(iter->block_sequence != first_block);

  // This used to search the current (second) block using the stale cached
  // range of the first block, returning the second block's first frame.
  RTF_ASSERT(iter.find(1000, 500));
  RTF_ASSERT(iter->timestamp == 1000);
  RTF_ASSERT(iter->secondary_key == 500);
  RTF_ASSERT(iter->block_sequence == first_block);

  // Exercise the other half of the fast-path bug: adjacent blocks can share
  // a timestamp when secondary keys provide the ordering. Updating only the
  // cached timestamps would still search the wrong block here.
  {
    auto wctx = db.create_write_context("reseek_composite", "");
    std::vector<uint8_t> row(16 * 1024, 0xA5);
    for (int i = 0; i < N; ++i)
      db.write(wctx, row.data(), row.size(), static_cast<uint32_t>(i),
               2000, i);
  }

  nanots_iterator composite_iter(
      "nanots_test_2048_4k_blocks.nts", "reseek_composite");
  RTF_ASSERT(composite_iter.find(2000, 0));
  const int64_t composite_first_block = composite_iter->block_sequence;
  while (composite_iter.valid() &&
         composite_iter->block_sequence == composite_first_block)
    ++composite_iter;

  RTF_ASSERT(composite_iter.valid());
  RTF_ASSERT(composite_iter->block_sequence != composite_first_block);
  RTF_ASSERT(composite_iter.find(2000, 0));
  RTF_ASSERT(composite_iter->timestamp == 2000);
  RTF_ASSERT(composite_iter->secondary_key == 0);
  RTF_ASSERT(composite_iter->block_sequence == composite_first_block);
}

// A mapped block is live. An iterator must refresh the block's committed
// index count when an operation reaches the end of its cached prefix.
void test_nanots::test_nanots_iterator_sees_appends_in_loaded_block() {
  nanots_writer db("nanots_test_4mb.nts", false);
  auto wctx = db.create_write_context("live_tail", "");

  const std::string first = "first";
  db.write(wctx, reinterpret_cast<const uint8_t*>(first.data()), first.size(),
           1, 100);

  nanots_iterator iter("nanots_test_4mb.nts", "live_tail");
  RTF_ASSERT(iter.find(100));
  RTF_ASSERT(iter->timestamp == 100);

  const int64_t block_sequence = iter->block_sequence;
  const std::string second = "second";
  db.write(wctx, reinterpret_cast<const uint8_t*>(second.data()), second.size(),
           2, 200);

  // find() must extend its binary-search range past the one-entry prefix the
  // iterator observed when it first mapped this block.
  RTF_ASSERT(iter.find(200));
  RTF_ASSERT(iter->timestamp == 200);
  RTF_ASSERT(iter->block_sequence == block_sequence);

  const std::string third = "third";
  db.write(wctx, reinterpret_cast<const uint8_t*>(third.data()), third.size(),
           3, 300);

  // seek_end() must use the current committed count, not the cached count.
  RTF_ASSERT(iter.seek_end());
  RTF_ASSERT(iter->timestamp == 300);
  RTF_ASSERT(iter->block_sequence == block_sequence);

  const std::string fourth = "fourth";
  db.write(wctx, reinterpret_cast<const uint8_t*>(fourth.data()), fourth.size(),
           4, 400);

  // Advancing from the formerly-known tail must discover the new entry in
  // the same block instead of looking for a next block and becoming invalid.
  ++iter;
  RTF_ASSERT(iter.valid());
  RTF_ASSERT(iter->timestamp == 400);
  RTF_ASSERT(iter->block_sequence == block_sequence);

  // seek_end() must also notice that the live tail rolled into a block that
  // did not exist in the iterator's cached block list.
  nanots_writer rolling_db("nanots_test_2048_4k_blocks.nts", false);
  auto rolling_ctx = rolling_db.create_write_context("live_tail_rollover", "");
  std::vector<uint8_t> row(16 * 1024, 0x6B);
  rolling_db.write(rolling_ctx, row.data(), row.size(), 0, 1000);

  nanots_iterator rolling_iter(
      "nanots_test_2048_4k_blocks.nts", "live_tail_rollover");
  const int64_t rolling_first_block = rolling_iter->block_sequence;
  for (int i = 1; i < 12; ++i)
    rolling_db.write(rolling_ctx, row.data(), row.size(),
                     static_cast<uint32_t>(i), 1000 + i);

  RTF_ASSERT(rolling_iter.seek_end());
  RTF_ASSERT(rolling_iter->timestamp == 1011);
  RTF_ASSERT(rolling_iter->block_sequence != rolling_first_block);
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
               (uint32_t)video_data.size(), 0x01, base_timestamp);
      db.write(audio_ctx, (uint8_t*)audio_data.c_str(),
               (uint32_t)audio_data.size(), 0x02, base_timestamp + 10);
      db.write(metadata_ctx, (uint8_t*)meta_data.c_str(),
               (uint32_t)meta_data.size(), 0x03, base_timestamp + 20);
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
    
    // Test metadata access through iterator
    RTF_ASSERT(meta_iter.current_metadata() == "sensor data");
    
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
      db.write(wctx, (uint8_t*)data.c_str(), (uint32_t)data.size(),
               (uint8_t)(i % 256), timestamp);
    }
  }

  nanots_reader reader("nanots_test_4mb.nts");

  // Test reading specific time ranges
  std::vector<std::pair<uint64_t, std::string>> frames_read;

  // Read middle portion (1500 to 2200)
  reader.read("test_stream", 1500, NANOTS_SEC_KEY_UNSET, 2200, INT64_MAX,
              [&](const uint8_t* data, size_t size, uint32_t flags,
                  int64_t timestamp, int64_t /*secondary_key*/,
                  int64_t block_sequence, const std::string& metadata) {
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
  reader.read("test_stream", 0, NANOTS_SEC_KEY_UNSET, 1200, INT64_MAX,
              [&](const uint8_t* data, size_t size, uint32_t flags,
                  int64_t timestamp, int64_t /*secondary_key*/,
                  int64_t block_sequence, const std::string& metadata) {
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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
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
      db.write(wctx, large_data.data(), frame_size, (uint8_t)i, timestamp);
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
    db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x01, 1000);
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
    db.write(wctx, nullptr, 0, 0x00, 2000);
  }

  {
    nanots_iterator zero_iter("nanots_test_4mb.nts", "zero_stream");
    RTF_ASSERT(zero_iter.valid());
    RTF_ASSERT(zero_iter->size == 0);
    RTF_ASSERT(zero_iter->timestamp == 2000);
  }
}

void test_nanots::test_nanots_cross_segment_iteration() {
  // Create multiple segments by writing data in separate contexts
  nanots_writer db("nanots_test_4mb.nts", false);

  // Write first segment
  {
    auto wctx = db.create_write_context("cross_segment_stream", "segment 1");
    for (int i = 0; i < 5; i++) {
      std::string data = "seg1_frame_" + std::to_string(i);
      uint64_t timestamp = 1000 + (i * 100);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x01, timestamp);
    }
  }

  // Write second segment (simulating restart)
  {
    auto wctx = db.create_write_context("cross_segment_stream", "segment 2");
    for (int i = 0; i < 5; i++) {
      std::string data = "seg2_frame_" + std::to_string(i);
      uint64_t timestamp = 2000 + (i * 100);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x02, timestamp);
    }
  }

  // Write third segment
  {
    auto wctx = db.create_write_context("cross_segment_stream", "segment 3");
    for (int i = 0; i < 5; i++) {
      std::string data = "seg3_frame_" + std::to_string(i);
      uint64_t timestamp = 3000 + (i * 100);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x03, timestamp);
    }
  }

  // Test forward iteration across segments
  {
    nanots_iterator iter("nanots_test_4mb.nts", "cross_segment_stream");
    RTF_ASSERT(iter.valid());

    // Verify we can iterate through all frames across segments
    std::vector<std::string> frames_read;
    std::vector<int64_t> timestamps_read;
    while (iter.valid()) {
      std::string frame_data((char*)iter->data, iter->size);
      frames_read.push_back(frame_data);
      timestamps_read.push_back(iter->timestamp);
      ++iter;
    }

    RTF_ASSERT(frames_read.size() == 15); // 5 frames per segment * 3 segments
    
    // Verify first segment frames
    for (int i = 0; i < 5; i++) {
      RTF_ASSERT(frames_read[i] == "seg1_frame_" + std::to_string(i));
      RTF_ASSERT(timestamps_read[i] == 1000 + (i * 100));
    }
    
    // Verify second segment frames  
    for (int i = 0; i < 5; i++) {
      RTF_ASSERT(frames_read[5 + i] == "seg2_frame_" + std::to_string(i));
      RTF_ASSERT(timestamps_read[5 + i] == 2000 + (i * 100));
    }
    
    // Verify third segment frames
    for (int i = 0; i < 5; i++) {
      RTF_ASSERT(frames_read[10 + i] == "seg3_frame_" + std::to_string(i));
      RTF_ASSERT(timestamps_read[10 + i] == 3000 + (i * 100));
    }
  }

  // Test backward iteration across segments
  {
    nanots_iterator iter("nanots_test_4mb.nts", "cross_segment_stream");
    
    // Find a timestamp in the middle segment
    RTF_ASSERT(iter.find(2200));
    RTF_ASSERT(iter->timestamp == 2200);
    
    // Move backward to previous segment
    for (int i = 0; i < 3; i++) {
      --iter;
      RTF_ASSERT(iter.valid());
    }
    
    // Should now be at last frame of first segment
    RTF_ASSERT(iter->timestamp == 1400);
    std::string frame_data((char*)iter->data, iter->size);
    RTF_ASSERT(frame_data == "seg1_frame_4");
    
    // Continue moving backward within first segment
    --iter;
    RTF_ASSERT(iter->timestamp == 1300);
  }

  // Test find across segments
  {
    nanots_iterator iter("nanots_test_4mb.nts", "cross_segment_stream");
    
    // Find in first segment
    RTF_ASSERT(iter.find(1200));
    RTF_ASSERT(iter->timestamp == 1200);
    
    // Find in second segment
    RTF_ASSERT(iter.find(2100));
    RTF_ASSERT(iter->timestamp == 2100);
    
    // Find in third segment
    RTF_ASSERT(iter.find(3300));
    RTF_ASSERT(iter->timestamp == 3300);
    
    // Find with timestamp between segments
    RTF_ASSERT(iter.find(1500)); // After first segment, should find first frame of second segment
    RTF_ASSERT(iter->timestamp == 2000);
  }

  // Test metadata changes across segments
  {
    nanots_iterator iter("nanots_test_4mb.nts", "cross_segment_stream");
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter.current_metadata() == "segment 1");
    
    // Move to second segment
    for (int i = 0; i < 5; i++) {
      ++iter;
    }
    RTF_ASSERT(iter.current_metadata() == "segment 2");
    
    // Move to third segment
    for (int i = 0; i < 5; i++) {
      ++iter;
    }
    RTF_ASSERT(iter.current_metadata() == "segment 3");
  }
}

void test_nanots::test_nanots_monotonic_timestamp_validation() {
  nanots_writer db("nanots_test_4mb.nts", false);

  auto wctx = db.create_write_context("test_stream", "monotonic test");

  std::string data1 = "frame1";
  std::string data2 = "frame2";
  std::string data3 = "frame3";

  // Write first frame
  db.write(wctx, (uint8_t*)data1.c_str(), data1.size(), 0x01, 1000);

  // Write second frame with higher timestamp (should succeed)
  db.write(wctx, (uint8_t*)data2.c_str(), data2.size(), 0x02, 2000);

  // Try to write with equal timestamp (should throw)
  bool caught_exception = false;
  try {
    db.write(wctx, (uint8_t*)data3.c_str(), data3.size(), 0x03, 2000);
  } catch (const std::exception&) {
    caught_exception = true;
  }
  RTF_ASSERT(caught_exception);

  // Try to write with lower timestamp (should throw)
  caught_exception = false;
  try {
    db.write(wctx, (uint8_t*)data3.c_str(), data3.size(), 0x03, 1500);
  } catch (const std::exception&) {
    caught_exception = true;
  }
  RTF_ASSERT(caught_exception);

  // Write with higher timestamp (should succeed)
  db.write(wctx, (uint8_t*)data3.c_str(), data3.size(), 0x03, 3000);

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
      db.write(wctx, test_data.data(), frame_size,
               (uint8_t)(i % 256), timestamp);
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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)(i % 256), timestamp);
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

    db.write(video_ctx, (uint8_t*)"video1", 6, 0x01, 1000);
    db.write(audio_ctx, (uint8_t*)"audio1", 6, 0x02, 1010);
  }

  // Verify metadata is preserved during reads
  nanots_reader reader("nanots_test_4mb.nts");

  bool video_metadata_correct = false;
  bool audio_metadata_correct = false;

  // Read and check if we can access metadata through iterator or reader
  // Note: This test assumes metadata is accessible during read operations
  // You may need to modify based on your actual metadata access API

  reader.read("video", 0, NANOTS_SEC_KEY_UNSET, 2000, INT64_MAX,
              [&](const uint8_t* data, size_t size, uint32_t flags,
                  int64_t timestamp, int64_t /*secondary_key*/,
                  int64_t block_sequence, const std::string& metadata) {
                // Now we can actually verify the metadata
                if (metadata == "codec=h264,resolution=1920x1080,fps=30") {
                  video_metadata_correct = true;
                }
              });

  reader.read(
      "audio", 0, NANOTS_SEC_KEY_UNSET, 2000, INT64_MAX,
      [&](const uint8_t* data, size_t size, uint32_t flags, int64_t timestamp,
          int64_t /*secondary_key*/,
                  int64_t block_sequence, const std::string& metadata) { 
            if (metadata == "codec=aac,samplerate=44100,channels=2") {
              audio_metadata_correct = true;
            }
          });

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
        db.write(wctx, large_data.data(), large_frame_size,
                 (uint8_t)i, timestamp);
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
      db.write(wctx, large_data.data(), large_frame_size,
               (uint8_t)(i % 256), timestamp);
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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamps[i]);
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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
    }

    // Write second batch (continuing same context - proper way)
    for (int i = 0; i < 5; i++) {
      std::string data = "batch2_frame_" + std::to_string(i);
      uint64_t timestamp = 10000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
    }

    // Write third batch (continuing same context)
    for (int i = 0; i < 5; i++) {
      std::string data = "batch3_frame_" + std::to_string(i);
      uint64_t timestamp = 20000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
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

      db.write(video_ctx, (uint8_t*)video_data.c_str(), video_data.size(), 0x01,
               base_timestamp);
      db.write(audio_ctx, (uint8_t*)audio_data.c_str(), audio_data.size(), 0x02,
               base_timestamp + 10);
      db.write(data_ctx, (uint8_t*)sensor_data.c_str(), sensor_data.size(), 0x03,
               base_timestamp + 20);
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

void test_nanots::test_nanots_concurrent_writers_claim_distinct_blocks() {
  constexpr int writer_count = 16;

  auto run_concurrent_claims = [&](const std::string& path) {
    // Use separate writer instances to exercise SQLite's cross-connection
    // allocator. Every writer targets a distinct stream, as supported by the
    // public threading contract.
    std::vector<std::unique_ptr<nanots_writer>> writers;
    std::vector<std::unique_ptr<write_context>> contexts;
    writers.reserve(writer_count);
    contexts.reserve(writer_count);

    for (int i = 0; i < writer_count; ++i) {
      writers.emplace_back(std::make_unique<nanots_writer>(path, false));
      contexts.emplace_back(std::make_unique<write_context>(
          writers.back()->create_write_context(
              "concurrent_writer_" + std::to_string(i), "concurrent claim")));
    }

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    // Each thread owns one distinct element; avoid vector<bool>'s shared packed
    // storage, which would itself introduce a test-only data race.
    std::vector<int> write_succeeded(writer_count, 0);
    threads.reserve(writer_count);

    for (int i = 0; i < writer_count; ++i) {
      threads.emplace_back([&, i]() {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire))
          std::this_thread::yield();

        try {
          std::string payload = "writer_payload_" + std::to_string(i);
          writers[i]->write(*contexts[i],
                            reinterpret_cast<const uint8_t*>(payload.data()),
                            payload.size(), 0, 1000 + i);
          write_succeeded[i] = 1;
        } catch (...) {
          write_succeeded[i] = 0;
        }
      });
    }

    while (ready.load(std::memory_order_acquire) != writer_count)
      std::this_thread::yield();
    start.store(true, std::memory_order_release);

    for (auto& thread : threads)
      thread.join();

    for (int i = 0; i < writer_count; ++i)
      RTF_ASSERT(write_succeeded[i]);

    // Finalize every block before opening readers.
    contexts.clear();
    writers.clear();

    for (int i = 0; i < writer_count; ++i) {
      nanots_iterator iter(path, "concurrent_writer_" + std::to_string(i));
      RTF_ASSERT(iter.valid());
      std::string expected = "writer_payload_" + std::to_string(i);
      std::string actual(reinterpret_cast<const char*>(iter->data), iter->size);
      RTF_ASSERT(actual == expected);
    }
  };

  // Cover both the free-list claim and the COUNT+grow+INSERT path.
  run_concurrent_claims("nanots_test_16mb.nts");
  nanots_writer::allocate_growable("nanots_growable_concurrent.nts", 65536,
                                   writer_count);
  run_concurrent_claims("nanots_growable_concurrent.nts");
}

void test_nanots::test_nanots_invalid_multiple_writers_same_stream() {
  // Test what happens if someone tries to create multiple writers for same
  // stream This should either be prevented or handled gracefully
  nanots_writer db("nanots_test_4mb.nts", false);
  
  auto ctx1 = db.create_write_context("shared_stream", "first writer");
  db.write(ctx1, (uint8_t*)"frame1", 6, 0x01, 1000);

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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
    }

    // Write second batch (continuing same context)
    for (int i = 0; i < 5; i++) {
      std::string data = "reuse_data_batch2_" + std::to_string(i);
      uint64_t timestamp = 10000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
    }

    // Write third batch (continuing same context)
    for (int i = 0; i < 5; i++) {
      std::string data = "reuse_data_batch3_" + std::to_string(i);
      uint64_t timestamp = 20000 + (i * 1000);
      printf("Writing frame %s\n", data.c_str());
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
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
      db.write(wctx, data.data(), size, (uint8_t)i, timestamp);
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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, timestamp);
    }
  }

  nanots_reader reader("nanots_test_4mb.nts");

  // Test what happens when callback throws exception
  int frames_processed = 0;
  bool exception_caught = false;

  try {
    reader.read("exception_stream", 0, NANOTS_SEC_KEY_UNSET, 20000, INT64_MAX,
                [&](const uint8_t* data, size_t size, uint32_t flags,
                    int64_t timestamp, int64_t /*secondary_key*/,
                  int64_t block_sequence, const std::string& metadata) {
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
      db.write(wctx, test_data.data(), frame_size,
               (uint8_t)(i % 256), timestamp);
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
      db.write(wctx, (uint8_t*)data.c_str(), data.size(),
               (uint8_t)i, precise_timestamps[i]);
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
      db.write(wctx, one_k_row.data(), one_k_row.size(), (uint8_t)i, i);
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
  nanots_writer::free_blocks("nanots_test_2048_4k_blocks.nts", "delete_stream", 250, NANOTS_SEC_KEY_UNSET, 500, INT64_MAX);

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

  reader.read("delete_stream", 1, NANOTS_SEC_KEY_UNSET, 1024, INT64_MAX,
              [&](const uint8_t* data, size_t size, uint32_t flags,
                  int64_t timestamp, int64_t /*secondary_key*/,
                  int64_t block_sequence, const std::string& metadata) {
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
      db.write(wctx, one_k_row.data(), one_k_row.size(), (uint8_t)i, i);
    }
  }

  nanots_writer::free_blocks("nanots_test_2048_4k_blocks.nts", "test_stream", 250, NANOTS_SEC_KEY_UNSET, 500, INT64_MAX);

  nanots_reader reader("nanots_test_2048_4k_blocks.nts");
  auto segments = reader.query_contiguous_segments("test_stream", 1, NANOTS_SEC_KEY_UNSET, 1024, INT64_MAX);

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
      db.write(video_ctx, (uint8_t*)data.c_str(), data.size(), 0x01, timestamp);
    }

    // Write audio samples: 2000-6000 (overlaps with video)
    for (int i = 0; i < 5; i++) {
      std::string data = "audio_sample_" + std::to_string(i);
      uint64_t timestamp = 2000 + (i * 1000);
      db.write(audio_ctx, (uint8_t*)data.c_str(), data.size(), 0x02, timestamp);
    }

    // Write metadata: 8000-12000 (non-overlapping)
    for (int i = 0; i < 5; i++) {
      std::string data = "sensor_" + std::to_string(i);
      uint64_t timestamp = 8000 + (i * 1000);
      db.write(metadata_ctx, (uint8_t*)data.c_str(), data.size(), 0x03, timestamp);
    }
  }

  nanots_reader reader("nanots_test_4mb.nts");

  // Test query_stream_tags for different time ranges
  
  // Query range that includes all streams (1000-12000)
  auto all_tags = reader.query_stream_tags(1000, NANOTS_SEC_KEY_UNSET, 12000, INT64_MAX);
  std::set<std::string> all_tags_set(all_tags.begin(), all_tags.end());
  RTF_ASSERT(all_tags_set.size() == 3);
  RTF_ASSERT(all_tags_set.count("video") == 1);
  RTF_ASSERT(all_tags_set.count("audio") == 1);
  RTF_ASSERT(all_tags_set.count("metadata") == 1);

  // Query range that includes only video and audio (2000-6000)
  auto video_audio_tags = reader.query_stream_tags(2000, NANOTS_SEC_KEY_UNSET, 6000, INT64_MAX);
  std::set<std::string> video_audio_set(video_audio_tags.begin(), video_audio_tags.end());
  RTF_ASSERT(video_audio_set.size() == 2);
  RTF_ASSERT(video_audio_set.count("video") == 1);
  RTF_ASSERT(video_audio_set.count("audio") == 1);
  RTF_ASSERT(video_audio_set.count("metadata") == 0);

  // Query range that includes only metadata (8000-12000)
  auto metadata_tags = reader.query_stream_tags(8000, NANOTS_SEC_KEY_UNSET, 12000, INT64_MAX);
  std::set<std::string> metadata_set(metadata_tags.begin(), metadata_tags.end());
  RTF_ASSERT(metadata_set.size() == 1);
  RTF_ASSERT(metadata_set.count("metadata") == 1);
  RTF_ASSERT(metadata_set.count("video") == 0);
  RTF_ASSERT(metadata_set.count("audio") == 0);

  // Query range with no data (20000-25000)
  auto empty_tags = reader.query_stream_tags(20000, NANOTS_SEC_KEY_UNSET, 25000, INT64_MAX);
  RTF_ASSERT(empty_tags.empty());

  // Query range that includes only video (1000-1500)
  auto video_only_tags = reader.query_stream_tags(1000, NANOTS_SEC_KEY_UNSET, 1500, INT64_MAX);
  std::set<std::string> video_only_set(video_only_tags.begin(), video_only_tags.end());
  RTF_ASSERT(video_only_set.size() == 1);
  RTF_ASSERT(video_only_set.count("video") == 1);
  RTF_ASSERT(video_only_set.count("audio") == 0);
  RTF_ASSERT(video_only_set.count("metadata") == 0);
}

void test_nanots::test_nanots_progressive_block_deletion() {
  // Create a file with small blocks to make it easier to have many blocks
  const size_t block_size = 8 * 1024;  // 8KB blocks
  const size_t num_blocks = 1024;  // Plenty of blocks
  
  nanots_writer::allocate("nanots_test_progressive_deletion.nts", block_size, num_blocks);
  nanots_writer db("nanots_test_progressive_deletion.nts", false);
  
  // Write data with small frames to fill many blocks
  const size_t frame_size = 512;  // 512 byte frames
  const int total_frames = 2000;  // Write 2000 frames
  const uint64_t start_timestamp = 10000;
  const uint64_t end_timestamp = start_timestamp + (total_frames - 1) * 10;
  
  printf("Writing %d frames of %zu bytes each (timestamps %" PRIu64 " to %" PRIu64 ")\n", 
         total_frames, frame_size, start_timestamp, end_timestamp);
  
  {
    auto wctx = db.create_write_context("test_stream", "progressive deletion test");
    std::vector<uint8_t> frame_data(frame_size, 0xAB);
    
    // Write frames with sequential timestamps starting at 10000
    for (int i = 0; i < total_frames; i++) {
      uint64_t timestamp = start_timestamp + i * 10;  // Space timestamps by 10
      db.write(wctx, frame_data.data(), frame_size, (uint8_t)(i % 256), timestamp);
    }
  }
  
  // Verify initial state - should have 1 contiguous segment
  {
    nanots_reader reader("nanots_test_progressive_deletion.nts");
    auto segments = reader.query_contiguous_segments("test_stream", start_timestamp, NANOTS_SEC_KEY_UNSET, end_timestamp, INT64_MAX);
    
    RTF_ASSERT(segments.size() == 1);
    RTF_ASSERT(segments[0].start_timestamp == start_timestamp);
    RTF_ASSERT(segments[0].end_timestamp == end_timestamp);
    
    printf("\nInitial state: %zu contiguous segment(s)\n", segments.size());
    printf("  Segment 0: [%" PRId64 ", %" PRId64 "]\n", 
           segments[0].start_timestamp, segments[0].end_timestamp);
  }
  
  // Free a large window in the middle - guaranteed to delete at least one complete block
  uint64_t first_delete_start = 15000;  // Somewhere in the middle
  uint64_t first_delete_end = 18000;    // Large enough window to encompass multiple blocks
  
  printf("\nFreeing large window: [%" PRIu64 ", %" PRIu64 "]\n", 
         first_delete_start, first_delete_end);
  nanots_writer::free_blocks("nanots_test_progressive_deletion.nts", "test_stream", first_delete_start, NANOTS_SEC_KEY_UNSET, first_delete_end, INT64_MAX);
  
  // Should now have 2 contiguous segments
  {
    nanots_reader reader("nanots_test_progressive_deletion.nts");
    auto segments = reader.query_contiguous_segments("test_stream", start_timestamp, NANOTS_SEC_KEY_UNSET, end_timestamp, INT64_MAX);
    
    RTF_ASSERT(segments.size() == 2);
    RTF_ASSERT(segments[0].start_timestamp == start_timestamp);
    RTF_ASSERT(segments[1].end_timestamp == end_timestamp);
    
    printf("After first deletion: %zu contiguous segment(s)\n", segments.size());
    for (size_t i = 0; i < segments.size(); i++) {
      printf("  Segment %zu: [%" PRId64 ", %" PRId64 "]\n", i,
             segments[i].start_timestamp, segments[i].end_timestamp);
    }
  }
  
  // Free another large window earlier in the timeline
  uint64_t second_delete_start = 11000;
  uint64_t second_delete_end = 13000;
  
  printf("\nFreeing second window: [%" PRIu64 ", %" PRIu64 "]\n", 
         second_delete_start, second_delete_end);
  nanots_writer::free_blocks("nanots_test_progressive_deletion.nts", "test_stream", second_delete_start, NANOTS_SEC_KEY_UNSET, second_delete_end, INT64_MAX);
  
  {
    nanots_reader reader("nanots_test_progressive_deletion.nts");
    auto segments = reader.query_contiguous_segments("test_stream", start_timestamp, NANOTS_SEC_KEY_UNSET, end_timestamp, INT64_MAX);
    
    // Should have at least 2 segments, possibly 3 if the windows don't overlap
    RTF_ASSERT(segments.size() >= 2);
    
    printf("After second deletion: %zu contiguous segment(s)\n", segments.size());
    for (size_t i = 0; i < segments.size(); i++) {
      printf("  Segment %zu: [%" PRId64 ", %" PRId64 "]\n", i,
             segments[i].start_timestamp, segments[i].end_timestamp);
    }
  }
  
  // Free another large window later in the timeline
  uint64_t third_delete_start = 22000;
  uint64_t third_delete_end = 25000;
  
  printf("\nFreeing third window: [%" PRIu64 ", %" PRIu64 "]\n", 
         third_delete_start, third_delete_end);
  nanots_writer::free_blocks("nanots_test_progressive_deletion.nts", "test_stream", third_delete_start, NANOTS_SEC_KEY_UNSET, third_delete_end, INT64_MAX);
  
  {
    nanots_reader reader("nanots_test_progressive_deletion.nts");
    auto segments = reader.query_contiguous_segments("test_stream", start_timestamp, NANOTS_SEC_KEY_UNSET, end_timestamp, INT64_MAX);
    
    // Should have multiple segments now
    RTF_ASSERT(segments.size() >= 2);
    
    printf("After third deletion: %zu contiguous segment(s)\n", segments.size());
    for (size_t i = 0; i < segments.size(); i++) {
      printf("  Segment %zu: [%" PRId64 ", %" PRId64 "]\n", i,
             segments[i].start_timestamp, segments[i].end_timestamp);
    }
  }
  
  // Test edge case: try to free a very small range that won't encompass any complete blocks
  printf("\nTrying to free tiny range (should not free anything)\n");
  uint64_t tiny_start = 10050;
  uint64_t tiny_end = 10060;   // Only 10 timestamp units - won't encompass a full block
  
  printf("Attempting to free tiny range [%" PRIu64 ", %" PRIu64 "]\n", 
         tiny_start, tiny_end);
  
  // Get current segment count
  nanots_reader reader_before("nanots_test_progressive_deletion.nts");
  auto segments_before = reader_before.query_contiguous_segments("test_stream", start_timestamp, NANOTS_SEC_KEY_UNSET, end_timestamp, INT64_MAX);
  size_t count_before = segments_before.size();
  
  nanots_writer::free_blocks("nanots_test_progressive_deletion.nts", "test_stream", tiny_start, NANOTS_SEC_KEY_UNSET, tiny_end, INT64_MAX);
  
  // Check that segment count hasn't changed
  nanots_reader reader_after("nanots_test_progressive_deletion.nts");
  auto segments_after = reader_after.query_contiguous_segments("test_stream", start_timestamp, NANOTS_SEC_KEY_UNSET, end_timestamp, INT64_MAX);
  
  RTF_ASSERT(segments_after.size() == count_before);
  printf("After tiny range free attempt: still %zu contiguous segment(s) (unchanged)\n", segments_after.size());
  
  // Test querying a deleted range
  {
    nanots_reader reader("nanots_test_progressive_deletion.nts");
    auto segments = reader.query_contiguous_segments("test_stream", first_delete_start, NANOTS_SEC_KEY_UNSET, first_delete_end, INT64_MAX);
    
    // Should return empty or very few segments since most of this range was deleted
    printf("\nQuery in first deleted range [%" PRIu64 ", %" PRIu64 "]: %zu segment(s)\n", 
           first_delete_start, first_delete_end, segments.size());
    
    // The range might have some remaining data at the edges, so we don't assert == 0
    // but we can assert it's fewer segments than we started with
    RTF_ASSERT(segments.size() <= 2);
  }
}

void test_nanots::test_nanots_iterator_block_transition_flag_search() {
  // Use smaller block database to ensure block transitions happen sooner
  nanots_writer db("nanots_test_2048_4k_blocks.nts", false);
  
  const size_t frame_size = 500;  // 500 byte frames to fill blocks faster
  const int total_frames = 200;   // Write enough frames to guarantee block transitions
  
  printf("Writing %d frames of %zu bytes each to force block transitions\n", 
         total_frames, frame_size);
  
  {
    auto wctx = db.create_write_context("block_transition_stream", "block transition flag test");
    std::vector<uint8_t> frame_data(frame_size, 0xCD);
    
    for (int i = 0; i < total_frames; i++) {
      uint64_t timestamp = 10000 + (i * 100);  // Timestamps: 10000, 10100, 10200, ...
      
      // Every 20th row has flags = 1, others have flags = 0
      // But skip the flagged frame at the block boundary to force cross-block search
      uint32_t flags = (i % 20 == 0 && i != 120) ? 1 : 0;
      
      // Fill frame with unique pattern to verify integrity
      for (size_t j = 0; j < frame_size; j++) {
        frame_data[j] = (uint8_t)((i + j) % 256);
      }
      
      db.write(wctx, frame_data.data(), frame_size, flags, timestamp);
      
      if (flags == 1) {
        printf("  Wrote flagged frame %d at timestamp %" PRIu64 "\n", i, timestamp);
      }
    }
  }
  
  printf("Finished writing frames\n");
  
  // Verify we have multiple blocks by checking block sequences
  nanots_iterator iter("nanots_test_2048_4k_blocks.nts", "block_transition_stream");
  
  std::set<int64_t> block_sequences;
  int frame_count = 0;
  
  while (iter.valid()) {
    block_sequences.insert(iter->block_sequence);
    frame_count++;
    ++iter;
  }
  
  printf("Found %d frames across %zu different blocks\n", 
         frame_count, block_sequences.size());
  RTF_ASSERT(block_sequences.size() > 1);  // Must have multiple blocks
  RTF_ASSERT(frame_count == total_frames);
  
  // Find the exact block transition point by scanning through frames
  iter.reset();
  int64_t first_block_sequence = -1;
  uint64_t block_transition_timestamp = 0;
  int frames_in_first_block = 0;
  
  while (iter.valid()) {
    if (first_block_sequence == -1) {
      first_block_sequence = iter->block_sequence;
    }
    
    if (iter->block_sequence != first_block_sequence) {
      // Found the transition point
      block_transition_timestamp = iter->timestamp;
      printf("Block transition found at timestamp %" PRIu64 " (frame %d), from block %" PRId64 " to %" PRId64 "\n", 
             iter->timestamp, frames_in_first_block, first_block_sequence, iter->block_sequence);
      break;
    }
    
    frames_in_first_block++;
    ++iter;
  }
  
  RTF_ASSERT(block_transition_timestamp > 0);  // Must have found a transition
  
  // Position iterator just into the second block, past the boundary frame
  // Since we removed the flag from frame 120, backing up should go to frame 100 in block 0
  uint64_t search_timestamp = block_transition_timestamp + (5 * 100);  // 5 frames past transition
  
  printf("\nSearching for timestamp %" PRIu64 " (5 frames past block transition)\n", search_timestamp);
  
  iter.reset();
  bool found = iter.find(search_timestamp);
  RTF_ASSERT(found);
  RTF_ASSERT(iter.valid());
  
  printf("Found frame at timestamp %" PRId64 " in block sequence %" PRId64 "\n", 
         iter->timestamp, iter->block_sequence);
  
  // Now walk backward using -- until we find a frame with flags == 1
  int steps_backward = 0;
  bool found_flagged = false;
  
  printf("Walking backward from timestamp %" PRId64 " looking for flags=1\n", 
         iter->timestamp);
  
  while (iter.valid()) {
    printf("  Step %d: timestamp=%" PRId64 ", flags=%u, block_sequence=%" PRId64 "\n",
           steps_backward, iter->timestamp, iter->flags, iter->block_sequence);
    
    if (iter->flags == 1) {
      found_flagged = true;
      printf("  Found flagged frame at timestamp %" PRId64 " after %d backward steps!\n", 
             iter->timestamp, steps_backward);
      break;
    }
    
    --iter;
    steps_backward++;
    
    // Safety check to prevent infinite loop
    if (steps_backward > 100) {
      printf("  Stopped after 100 steps to prevent infinite loop\n");
      break;
    }
  }
  
  RTF_ASSERT(found_flagged);  // Must find a flagged frame
  RTF_ASSERT(iter->flags == 1);  // Verify we're on the correct frame
  
  // Verify this is indeed one of our expected flagged frames (every 20th)
  // Calculate which frame index this should be based on timestamp
  int64_t frame_index = (iter->timestamp - 10000) / 100;
  RTF_ASSERT(frame_index % 20 == 0);  // Should be a multiple of 20
  
  printf("Successfully found flagged frame at index %" PRId64 " (timestamp %" PRId64 ")\n", 
         frame_index, iter->timestamp);
  
  // Verify frame data integrity
  for (size_t j = 0; j < min(frame_size, (size_t)100); j++) {
    uint8_t expected_byte = (uint8_t)((frame_index + j) % 256);
    RTF_ASSERT(iter->data[j] == expected_byte);
  }
  
  printf("Frame data integrity verified\n");
}

void test_nanots::test_nanots_iterator_performance_benchmark() {
  nanots_writer db("nanots_test_16mb.nts", false);
  
  const int num_rows = 2000;
  const size_t row_size = 1024;  // 1KB rows
  const uint64_t start_timestamp = 1000000;
  
  printf("Writing %d rows of %zu bytes each for performance test\n", num_rows, row_size);
  
  // Step 1: Write rows with every 30th row having flags=1
  {
    auto wctx = db.create_write_context("perf_test_stream", "iterator performance test");
    std::vector<uint8_t> row_data(row_size);
    
    for (int i = 0; i < num_rows; i++) {
      uint64_t timestamp = start_timestamp + i;
      uint32_t flags = (i % 30 == 0) ? 1 : 0;
      
      // Fill row with pattern for verification
      for (size_t j = 0; j < row_size; j++) {
        row_data[j] = (uint8_t)((i + j) % 256);
      }
      
      db.write(wctx, row_data.data(), row_size, flags, timestamp);
    }
  }
  
  printf("Finished writing %d rows\n", num_rows);
  
  // Step 2: Get current time in microseconds
  auto start_time = std::chrono::high_resolution_clock::now();
  
  // Static buffer for memcpy (step 7)
  static uint8_t static_buffer[1024];
  
  // Step 3: Loop 100 times
  const int num_iterations = 100;
  int successful_finds = 0;
  
  for (int iteration = 0; iteration < num_iterations; iteration++) {
    // Step 4: Compute random timestamp in inserted range
    uint64_t random_timestamp = start_timestamp + (rand() % num_rows);
    
    // Step 5: Declare iterator and do find
    nanots_iterator iter("nanots_test_16mb.nts", "perf_test_stream");
    bool found = iter.find(random_timestamp);
    
    if (!found || !iter.valid()) {
      printf("Warning: Failed to find timestamp %" PRIu64 " in iteration %d\n", 
             random_timestamp, iteration);
      continue;
    }
    
    // Step 6: Backup iterator until we see flags=1
    int backup_steps = 0;
    while (iter.valid() && iter->flags != 1) {
      --iter;
      backup_steps++;
      
      // Safety check to prevent going too far back
      if (backup_steps > 100) {
        printf("Warning: Too many backup steps in iteration %d\n", iteration);
        break;
      }
    }
    
    if (iter.valid() && iter->flags == 1) {
      // Step 7: memcpy the row data out to static buffer
      size_t copy_size = min(iter->size, sizeof(static_buffer));
      memcpy(static_buffer, iter->data, copy_size);
      successful_finds++;
    }
  }
  // Step 7 (end loop)
  
  // Get current time (step 8)
  auto end_time = std::chrono::high_resolution_clock::now();
  
  // Step 8: Print timing summary
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
  
  printf("\n=== Iterator Performance Benchmark Results ===\n");
  printf("Total time: %lld microseconds\n", static_cast<long long>(duration.count()));
  printf("Number of iterations: %d\n", num_iterations);
  printf("Successful finds: %d\n", successful_finds);
  printf("Average time per iteration: %.2f microseconds\n", 
         (double)duration.count() / num_iterations);
  printf("Average time per successful find: %.2f microseconds\n",
         successful_finds > 0 ? (double)duration.count() / successful_finds : 0.0);
  printf("==============================================\n");
  
  // Verify at least most finds were successful
  RTF_ASSERT(successful_finds >= num_iterations * 0.9);  // At least 90% success rate
}

void test_nanots::test_nanots_iterator_seek_end() {
  // --- Case 1: single segment, seek_end lands on last written frame ---
  {
    nanots_writer db("nanots_test_4mb.nts", false);
    auto wctx = db.create_write_context("seek_end_stream", "seek_end test");
    for (int i = 0; i < 10; i++) {
      std::string data = "frame_" + std::to_string(i);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), (uint8_t)i, 1000 + i * 100);
    }
  }

  {
    nanots_iterator iter("nanots_test_4mb.nts", "seek_end_stream");
    RTF_ASSERT(iter.seek_end());
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->timestamp == 1900);
    RTF_ASSERT(iter->flags == 9);
    std::string expected = "frame_9";
    RTF_ASSERT(iter->size == expected.size());
    RTF_ASSERT(memcmp(iter->data, expected.c_str(), iter->size) == 0);

    // Can navigate backwards from the end
    --iter;
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->timestamp == 1800);
    RTF_ASSERT(iter->flags == 8);

    // Going forward from last frame becomes invalid
    iter.seek_end();
    ++iter;
    RTF_ASSERT(!iter.valid());
  }

  // --- Case 2: multiple segments, seek_end lands in the last segment ---
  {
    nanots_writer db("nanots_test_16mb.nts", false);

    {
      auto wctx = db.create_write_context("seek_end_multi", "segment 1");
      for (int i = 0; i < 5; i++) {
        std::string data = "seg1_" + std::to_string(i);
        db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x01, 1000 + i * 100);
      }
    }

    {
      auto wctx = db.create_write_context("seek_end_multi", "segment 2");
      for (int i = 0; i < 5; i++) {
        std::string data = "seg2_" + std::to_string(i);
        db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x02, 2000 + i * 100);
      }
    }

    {
      auto wctx = db.create_write_context("seek_end_multi", "segment 3");
      for (int i = 0; i < 5; i++) {
        std::string data = "seg3_" + std::to_string(i);
        db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x03, 3000 + i * 100);
      }
    }
  }

  {
    nanots_iterator iter("nanots_test_16mb.nts", "seek_end_multi");
    RTF_ASSERT(iter.seek_end());
    RTF_ASSERT(iter.valid());
    // Last frame is in segment 3
    RTF_ASSERT(iter->timestamp == 3400);
    RTF_ASSERT(iter->flags == 0x03);
    std::string expected = "seg3_4";
    RTF_ASSERT(iter->size == expected.size());
    RTF_ASSERT(memcmp(iter->data, expected.c_str(), iter->size) == 0);
  }

  // --- Case 3: empty stream returns false and invalidates the iterator ---
  {
    nanots_iterator iter("nanots_test_4mb.nts", "nonexistent_stream");
    RTF_ASSERT(!iter.seek_end());
    RTF_ASSERT(!iter.valid());
  }
}

// File starts empty (header only). Writing frames must extend the file and
// read-back must work exactly like a preallocated file.
void test_nanots::test_nanots_growable_basic() {
  const char* nts = "nanots_growable_basic.nts";
  const uint32_t block_size = 64 * 1024;

  nanots_writer::allocate_growable(nts, block_size, 0);

  // File should start at just the header (64KB), no blocks yet.
  RTF_ASSERT(file_size(nts) == 64 * 1024);

  {
    nanots_writer db(nts, false);
    RTF_ASSERT(db.is_growable());

    auto wctx = db.create_write_context("grow_stream", "growable test");

    // Write frames small enough that several fit in one block, large enough
    // to force several blocks total. Block payload area is roughly
    // (block_size - header - index_overhead). Write ~10 blocks' worth.
    const size_t frame_size = 4 * 1024;
    std::vector<uint8_t> data(frame_size, 0x5A);

    for (int i = 0; i < 200; i++) {
      db.write(wctx, data.data(), frame_size, (uint8_t)(i & 0xff), 1000 + i * 100);
    }
  }

  // File should have grown beyond the bare header.
  uint64_t after_size = file_size(nts);
  printf("growable_basic: file size after writes = %llu bytes\n",
         (unsigned long long)after_size);
  RTF_ASSERT(after_size > 64 * 1024);
  // Must still be header + integer multiple of block_size.
  RTF_ASSERT(((after_size - 64 * 1024) % block_size) == 0);

  // Reopen and read everything back.
  {
    nanots_iterator iter(nts, "grow_stream");
    int n = 0;
    int64_t last_ts = 0;
    while (iter.valid()) {
      RTF_ASSERT(iter->size == 4 * 1024);
      RTF_ASSERT(iter->timestamp > last_ts);
      last_ts = iter->timestamp;
      n++;
      ++iter;
    }
    RTF_ASSERT(n == 200);
  }
}

// Verify the doubling growth pattern: file size after each grow event should
// be roughly 2x the previous size (until the 1 GiB-per-grow cap kicks in).
void test_nanots::test_nanots_growable_doubling() {
  const char* nts = "nanots_growable_doubling.nts";
  const uint32_t block_size = 64 * 1024;

  nanots_writer::allocate_growable(nts, block_size, 0);

  nanots_writer db(nts, false);
  auto wctx = db.create_write_context("doubling", "");

  // Each frame is just under block-payload size so each frame ends up in
  // its own block. That makes every write a grow-or-recycle event.
  const size_t frame_size = block_size / 2;
  std::vector<uint8_t> data(frame_size, 0xCD);

  std::vector<uint64_t> sizes;
  sizes.push_back(file_size(nts));
  for (int i = 0; i < 8; i++) {
    db.write(wctx, data.data(), frame_size, 0, 1000 + i);
    sizes.push_back(file_size(nts));
  }

  printf("growable_doubling: sizes (bytes): ");
  for (auto s : sizes) printf("%llu ", (unsigned long long)s);
  printf("\n");

  // After the first write we have 1 block. After subsequent grows we should
  // see 2, 4, 8, ... blocks (until we stop forcing growth).
  // Each entry should monotonically increase.
  for (size_t i = 1; i < sizes.size(); i++) {
    RTF_ASSERT(sizes[i] >= sizes[i - 1]);
  }
  // First write should have grown the file by exactly one block.
  RTF_ASSERT(sizes[1] == sizes[0] + block_size);
  // Second write should double (add another block, total 2).
  RTF_ASSERT(sizes[2] == sizes[0] + 2 * block_size);
  // Third write should double again (total 4).
  RTF_ASSERT(sizes[3] == sizes[0] + 4 * block_size);
}

// max_blocks cap: once we hit the cap, further writes throw (no auto_reclaim).
void test_nanots::test_nanots_growable_max_cap() {
  const char* nts = "nanots_growable_cap.nts";
  const uint32_t block_size = 64 * 1024;
  const uint32_t cap = 3;

  nanots_writer::allocate_growable(nts, block_size, cap);

  nanots_writer db(nts, false);
  auto wctx = db.create_write_context("capped", "");

  const size_t frame_size = block_size / 2;
  std::vector<uint8_t> data(frame_size, 0x77);

  int wrote = 0;
  bool threw = false;
  for (int i = 0; i < 20 && !threw; i++) {
    try {
      db.write(wctx, data.data(), frame_size, 0, 1000 + i);
      wrote++;
    } catch (const nanots_exception& e) {
      RTF_ASSERT(e.get_ec() == NANOTS_EC_NO_FREE_BLOCKS);
      threw = true;
    }
  }

  printf("growable_max_cap: wrote %d frames before hitting cap of %u blocks\n",
         wrote, cap);
  RTF_ASSERT(threw);
  // We wrote a frame per block (each frame is block_size/2 → next write
  // forces grow), so we should have managed exactly `cap` writes.
  RTF_ASSERT(wrote == (int)cap);

  uint64_t final_size = file_size(nts);
  RTF_ASSERT(final_size == 64 * 1024 + (uint64_t)cap * block_size);
}

// --- Secondary key tests --------------------------------------------------

// Basic: write frames with a secondary key, read them back, verify the key
// round-trips through both the iterator and the reader.
void test_nanots::test_nanots_secondary_key_basic() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx = db.create_write_context("sk_stream", "secondary key test");
    for (int i = 0; i < 10; i++) {
      std::string data = "frame_" + std::to_string(i);
      // sec_key advances by 7 each frame, distinct from timestamp pattern.
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0x01,
               1000 + i * 100, /*sec_key=*/100 + i * 7);
    }
  }

  // Iterator round-trip.
  nanots_iterator iter("nanots_test_4mb.nts", "sk_stream");
  int i = 0;
  while (iter.valid()) {
    RTF_ASSERT(iter->timestamp == 1000 + i * 100);
    RTF_ASSERT(iter->secondary_key == 100 + i * 7);
    ++iter;
    i++;
  }
  RTF_ASSERT(i == 10);

  // Reader callback round-trip.
  nanots_reader reader("nanots_test_4mb.nts");
  int j = 0;
  reader.read("sk_stream", 0, NANOTS_SEC_KEY_UNSET, 100000, INT64_MAX,
              [&](const uint8_t* data, size_t size, uint32_t flags,
                  int64_t timestamp, int64_t sec_key,
                  int64_t block_seq, const std::string& meta) {
                RTF_ASSERT(timestamp == 1000 + j * 100);
                RTF_ASSERT(sec_key == 100 + j * 7);
                j++;
              });
  RTF_ASSERT(j == 10);
}

// Composite find() with both args: exact composite match, between-key lands
// on next-higher composite, etc.
void test_nanots::test_nanots_secondary_key_find() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Write 10 frames with strictly-increasing timestamps and sec_keys.
  // Composite is just lex on (ts, sk).
  {
    auto wctx = db.create_write_context("sk_find", "");
    for (int i = 0; i < 10; i++) {
      std::string data = "sk_" + std::to_string(i);
      db.write(wctx, (uint8_t*)data.c_str(), data.size(), 0,
               1000 + i * 100, /*sec_key=*/1000 + i * 500);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "sk_find");

  // Exact composite match: (1200, 2000) is frame 2.
  RTF_ASSERT(iter.find(1200, 2000));
  RTF_ASSERT(iter->timestamp == 1200);
  RTF_ASSERT(iter->secondary_key == 2000);

  // Between composites: (1200, 2001) → next composite (1300, 2500).
  RTF_ASSERT(iter.find(1200, 2001));
  RTF_ASSERT(iter->timestamp == 1300);
  RTF_ASSERT(iter->secondary_key == 2500);

  // Before first composite.
  RTF_ASSERT(iter.find(0, NANOTS_SEC_KEY_UNSET));
  RTF_ASSERT(iter->timestamp == 1000);
  RTF_ASSERT(iter->secondary_key == 1000);

  // Past last composite.
  RTF_ASSERT(!iter.find(99999, 99999));
  RTF_ASSERT(!iter.valid());

  // Find with default sec_key (= UNSET): lands on first frame at the given
  // timestamp (smallest sec_key wins).
  RTF_ASSERT(iter.find(1500));  // = find(1500, NANOTS_SEC_KEY_UNSET)
  RTF_ASSERT(iter->timestamp == 1500);
}

// Composite monotonicity: (ts, sk) must strictly increase across writes.
// Ties on ts are fine if sk strictly increases; ts may jump as long as the
// composite still moves forward.
void test_nanots::test_nanots_secondary_key_monotonic() {
  nanots_writer db("nanots_test_4mb.nts", false);
  auto wctx = db.create_write_context("sk_mono", "");

  db.write(wctx, (uint8_t*)"a", 1, 0, 1000, /*sec_key=*/100);
  // Same ts, greater sk: allowed.
  db.write(wctx, (uint8_t*)"b", 1, 0, 1000, /*sec_key=*/200);
  // Greater ts, smaller sk: allowed (ts wins in lex order).
  db.write(wctx, (uint8_t*)"c", 1, 0, 2000, /*sec_key=*/50);

  // Smaller ts: rejected.
  bool threw = false;
  nanots_ec_t ec = NANOTS_EC_OK;
  try {
    db.write(wctx, (uint8_t*)"d", 1, 0, 1500, /*sec_key=*/999);
  } catch (const nanots_exception& e) {
    threw = true;
    ec = e.get_ec();
  }
  RTF_ASSERT(threw);
  RTF_ASSERT(ec == NANOTS_EC_NON_MONOTONIC_TIMESTAMP);

  // Same composite (ts, sk) — rejected (strict).
  threw = false;
  try {
    db.write(wctx, (uint8_t*)"e", 1, 0, 2000, /*sec_key=*/50);
  } catch (const nanots_exception& e) {
    threw = true;
    ec = e.get_ec();
  }
  RTF_ASSERT(threw);
  RTF_ASSERT(ec == NANOTS_EC_NON_MONOTONIC_TIMESTAMP);

  // Same ts, equal sk — rejected (strict on the composite).
  threw = false;
  try {
    db.write(wctx, (uint8_t*)"f", 1, 0, 2000, /*sec_key=*/50);
  } catch (const nanots_exception& e) {
    threw = true;
    ec = e.get_ec();
  }
  RTF_ASSERT(threw);
  RTF_ASSERT(ec == NANOTS_EC_NON_MONOTONIC_TIMESTAMP);

  // Same ts, larger sk — OK.
  db.write(wctx, (uint8_t*)"g", 1, 0, 2000, /*sec_key=*/51);
}

// A file with no v2 magic at offset 0 must be rejected on open. We synthesise
// the legacy v1 header (block_size at offset 0, n_blocks at offset 4) and
// verify the writer ctor throws NANOTS_EC_BAD_MAGIC.
void test_nanots::test_nanots_v1_file_rejected() {
  const char* nts = "nanots_legacy_v1.nts";
  // Whack any leftovers.
  if (rtf_file_exists(nts)) rtf_remove_file(nts);
  std::string db_name = "nanots_legacy_v1.db";
  if (rtf_file_exists(db_name.c_str())) rtf_remove_file(db_name.c_str());

  // Create a tiny file that looks like a legacy v1 header: 4 byte block_size,
  // 4 byte n_blocks. Padded out to 64KB.
  {
    auto f = nts_file::open(nts, "w+");
    std::vector<uint8_t> hdr(FILE_HEADER_BLOCK_SIZE, 0);
    *(uint32_t*)(hdr.data() + 0) = 64 * 1024;  // block_size
    *(uint32_t*)(hdr.data() + 4) = 1;          // n_blocks
    fwrite(hdr.data(), 1, hdr.size(), f);
    // One empty block so the file has a sensible size.
    std::vector<uint8_t> blk(64 * 1024, 0);
    fwrite(blk.data(), 1, blk.size(), f);
    f.close();
  }

  // nanots_writer ctor must reject it.
  bool threw = false;
  nanots_ec_t ec = NANOTS_EC_OK;
  try {
    nanots_writer w(nts, false);
  } catch (const nanots_exception& e) {
    threw = true;
    ec = e.get_ec();
  }
  RTF_ASSERT(threw);
  RTF_ASSERT(ec == NANOTS_EC_BAD_MAGIC);

  // nanots_reader ctor must reject it too.
  threw = false;
  try {
    nanots_reader r(nts);
  } catch (const nanots_exception& e) {
    threw = true;
    ec = e.get_ec();
  }
  RTF_ASSERT(threw);
  RTF_ASSERT(ec == NANOTS_EC_BAD_MAGIC);

  // Cleanup.
  if (rtf_file_exists(nts)) rtf_remove_file(nts);
  if (rtf_file_exists(db_name.c_str())) rtf_remove_file(db_name.c_str());
}

// Force many frames so the stream spans multiple blocks, then exercise
// composite find() across block boundaries and verify ++/-- around the
// boundary.
void test_nanots::test_nanots_secondary_key_cross_blocks() {
  // 4 KB blocks → ~3 frames per block at 1 KB rows; 64 frames = ~22 blocks.
  nanots_writer db("nanots_test_2048_4k_blocks.nts", false);

  const int N = 64;
  {
    auto wctx = db.create_write_context("sk_cross", "");
    std::vector<uint8_t> row(1024, 0xCC);
    for (int i = 0; i < N; i++) {
      // Distinctive: timestamp and sec_key advance at *different* rates.
      db.write(wctx, row.data(), row.size(), 0,
               /*ts=*/1000 + i * 10,
               /*sec_key=*/500 + i * 73);
    }
  }

  nanots_iterator iter("nanots_test_2048_4k_blocks.nts", "sk_cross");

  // Confirm we actually filled multiple blocks.
  std::set<int64_t> blocks_seen;
  iter.reset();
  while (iter.valid()) {
    blocks_seen.insert(iter->block_sequence);
    ++iter;
  }
  printf("sk_cross blocks: %zu\n", blocks_seen.size());
  RTF_ASSERT(blocks_seen.size() > 1);

  // Walk every frame via composite find() and verify we land on the right one.
  for (int i = 0; i < N; i++) {
    int64_t ts = 1000 + i * 10;
    int64_t sk = 500 + i * 73;
    RTF_ASSERT(iter.find(ts, sk));
    RTF_ASSERT(iter->secondary_key == sk);
    RTF_ASSERT(iter->timestamp == ts);
  }

  // Find between two composites → next higher composite. Frame 5 is
  // (1050, 865); frame 6 is (1060, 938). (1050, 866) is between them in
  // lex order.
  RTF_ASSERT(iter.find(1050, 866));
  RTF_ASSERT(iter->timestamp == 1060);
  RTF_ASSERT(iter->secondary_key == 938);
}

// After a composite find(), ++ and -- must walk by physical order.
void test_nanots::test_nanots_secondary_key_bidirectional() {
  nanots_writer db("nanots_test_4mb.nts", false);

  const int N = 20;
  {
    auto wctx = db.create_write_context("sk_bidi", "");
    for (int i = 0; i < N; i++) {
      std::string d = "bidi_" + std::to_string(i);
      db.write(wctx, (uint8_t*)d.c_str(), d.size(), 0,
               /*ts=*/1000 + i, /*sec_key=*/100 + i * 11);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "sk_bidi");

  // Land in the middle by composite.
  RTF_ASSERT(iter.find(1010, 100 + 10 * 11));
  RTF_ASSERT(iter->secondary_key == 100 + 10 * 11);
  RTF_ASSERT(iter->timestamp == 1010);

  // Walk forward.
  for (int i = 11; i < N; i++) {
    ++iter;
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->secondary_key == 100 + i * 11);
  }
  ++iter;
  RTF_ASSERT(!iter.valid());

  // Reseek and walk backward.
  RTF_ASSERT(iter.find(1005, 100 + 5 * 11));
  for (int i = 4; i >= 0; i--) {
    --iter;
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->secondary_key == 100 + i * 11);
  }
  --iter;
  RTF_ASSERT(!iter.valid());
}

// Multiple write contexts (= multiple segments) on the same stream. find()
// and iteration must cross segment boundaries cleanly.
void test_nanots::test_nanots_secondary_key_multi_segment() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Segment 1.
  {
    auto wctx = db.create_write_context("sk_segs", "s1");
    for (int i = 0; i < 5; i++) {
      std::string d = "s1_" + std::to_string(i);
      db.write(wctx, (uint8_t*)d.c_str(), d.size(), 0,
               1000 + i, /*sec_key=*/100 + i);
    }
  }
  {
    auto wctx = db.create_write_context("sk_segs", "s2");
    for (int i = 0; i < 5; i++) {
      std::string d = "s2_" + std::to_string(i);
      db.write(wctx, (uint8_t*)d.c_str(), d.size(), 0,
               2000 + i, /*sec_key=*/200 + i);
    }
  }
  {
    auto wctx = db.create_write_context("sk_segs", "s3");
    for (int i = 0; i < 5; i++) {
      std::string d = "s3_" + std::to_string(i);
      db.write(wctx, (uint8_t*)d.c_str(), d.size(), 0,
               3000 + i, /*sec_key=*/300 + i);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "sk_segs");

  // Find in segment 1.
  RTF_ASSERT(iter.find(1002, 102));
  RTF_ASSERT(iter->secondary_key == 102);
  RTF_ASSERT(iter.current_metadata() == "s1");

  // Find in segment 2.
  RTF_ASSERT(iter.find(2001, 201));
  RTF_ASSERT(iter->secondary_key == 201);
  RTF_ASSERT(iter.current_metadata() == "s2");

  // Find in segment 3.
  RTF_ASSERT(iter.find(3004, 304));
  RTF_ASSERT(iter->secondary_key == 304);
  RTF_ASSERT(iter.current_metadata() == "s3");

  // Find between segments — composite (1500, 0) lands on first frame of
  // segment 2 (the first composite >= (1500, 0)).
  RTF_ASSERT(iter.find(1500, 0));
  RTF_ASSERT(iter->secondary_key == 200);
  RTF_ASSERT(iter.current_metadata() == "s2");

  // Walk all 15 frames in composite order from before-the-first.
  RTF_ASSERT(iter.find(0));
  int seen = 0;
  while (iter.valid()) {
    seen++;
    ++iter;
  }
  RTF_ASSERT(seen == 15);
}

// Two streams in the same file: one keyed, one unkeyed (always passes UNSET
// for sec_key, so composite degenerates to ts-monotonic).
void test_nanots::test_nanots_secondary_key_mixed_streams_same_file() {
  nanots_writer db("nanots_test_4mb.nts", false);
  {
    auto keyed   = db.create_write_context("keyed", "k");
    auto unkeyed = db.create_write_context("unkeyed", "u");
    for (int i = 0; i < 5; i++) {
      db.write(keyed, (uint8_t*)"k", 1, 0, 1000 + i, /*sec_key=*/10 + i);
      db.write(unkeyed, (uint8_t*)"u", 1, 0, 2000 + i);
    }
  }

  // Keyed stream: composite find works.
  {
    nanots_iterator it("nanots_test_4mb.nts", "keyed");
    RTF_ASSERT(it.find(1002, 12));
    RTF_ASSERT(it->secondary_key == 12);
    RTF_ASSERT(it->timestamp == 1002);
  }

  // Unkeyed stream: every frame's sec_key reads as UNSET; find by ts works
  // normally (sec_key defaults to UNSET so composite = (ts, UNSET)).
  {
    nanots_iterator it("nanots_test_4mb.nts", "unkeyed");
    RTF_ASSERT(it.valid());
    RTF_ASSERT(it->secondary_key == NANOTS_SEC_KEY_UNSET);
    RTF_ASSERT(it.find(2003));
    RTF_ASSERT(it->timestamp == 2003);
    RTF_ASSERT(it->secondary_key == NANOTS_SEC_KEY_UNSET);
  }
}

// Sparse composites: huge gaps must still binary-search correctly.
void test_nanots::test_nanots_secondary_key_sparse() {
  nanots_writer db("nanots_test_4mb.nts", false);

  std::vector<int64_t> keys = {
      1, 100, 10000, 1000000, 100000000, 10000000000LL,
      1000000000000LL, 100000000000000LL, 1000000000000000LL,
  };
  {
    auto wctx = db.create_write_context("sk_sparse", "");
    for (size_t i = 0; i < keys.size(); i++) {
      std::string d = "sp_" + std::to_string(i);
      db.write(wctx, (uint8_t*)d.c_str(), d.size(), 0,
               /*ts=*/1000 + (int64_t)i, /*sec_key=*/keys[i]);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "sk_sparse");

  // Exact composite matches.
  for (size_t i = 0; i < keys.size(); i++) {
    int64_t ts = 1000 + (int64_t)i;
    RTF_ASSERT(iter.find(ts, keys[i]));
    RTF_ASSERT(iter->secondary_key == keys[i]);
    RTF_ASSERT(iter->timestamp == ts);
  }

  // Between-composite seeks: (ts=1000, sk=50) is between frame 0 (1000,1)
  // and frame 1 (1001,100). Next higher composite is frame 1.
  RTF_ASSERT(iter.find(1000, 50));
  RTF_ASSERT(iter->secondary_key == 100);

  // (ts=1003, sk=999999): frame 3 is (1003, 1000000), so lower_bound is
  // frame 3.
  RTF_ASSERT(iter.find(1003, 999999));
  RTF_ASSERT(iter->secondary_key == 1000000);

  // Below the smallest composite → first frame.
  RTF_ASSERT(iter.find(0));
  RTF_ASSERT(iter->secondary_key == 1);

  // Above the largest composite → invalid.
  RTF_ASSERT(!iter.find(99999, 9999999999999999LL));
  RTF_ASSERT(!iter.valid());
}

// Secondary keys are int64, signed. Negative keys and values close to
// INT64_MAX must work. INT64_MIN is reserved as the "unset" sentinel and is
// off-limits as a real key.
void test_nanots::test_nanots_secondary_key_extreme_values() {
  nanots_writer db("nanots_test_4mb.nts", false);

  // Each frame gets a unique timestamp AND a wildly-varying sec_key. The
  // composite is strictly increasing because ts is.
  std::vector<int64_t> keys = {
      INT64_MIN + 1,        // smallest legal sec_key
      -1000000000000LL,
      -1,
      0,
      1,
      1000000000000LL,
      INT64_MAX - 1,
      INT64_MAX,
  };
  {
    auto wctx = db.create_write_context("sk_extreme", "");
    for (size_t i = 0; i < keys.size(); i++) {
      std::string d = "ex_" + std::to_string(i);
      db.write(wctx, (uint8_t*)d.c_str(), d.size(), 0,
               /*ts=*/1000 + (int64_t)i, keys[i]);
    }
  }

  nanots_iterator iter("nanots_test_4mb.nts", "sk_extreme");

  // Round-trip each composite.
  for (size_t i = 0; i < keys.size(); i++) {
    int64_t ts = 1000 + (int64_t)i;
    RTF_ASSERT(iter.find(ts, keys[i]));
    RTF_ASSERT(iter->secondary_key == keys[i]);
    RTF_ASSERT(iter->timestamp == ts);
  }

  // find(ts) with no sec_key argument defaults to UNSET (= INT64_MIN). For
  // frame 0 at ts=1000, the composite is (1000, INT64_MIN+1); lower_bound
  // for (1000, INT64_MIN) finds frame 0.
  RTF_ASSERT(iter.find(1000));
  RTF_ASSERT(iter->secondary_key == INT64_MIN + 1);

  // Backward from first frame → invalid.
  --iter;
  RTF_ASSERT(!iter.valid());

  // Forward from last frame → invalid.
  RTF_ASSERT(iter.find(1007, INT64_MAX));
  ++iter;
  RTF_ASSERT(!iter.valid());
}

// The reader callback must surface the secondary key for keyed streams,
// and NANOTS_SEC_KEY_UNSET for unkeyed streams.
void test_nanots::test_nanots_secondary_key_reader_callback() {
  nanots_writer db("nanots_test_4mb.nts", false);
  {
    auto k = db.create_write_context("rk_keyed", "");
    for (int i = 0; i < 4; i++) {
      db.write(k, (uint8_t*)"x", 1, 0, 1000 + i, /*sec_key=*/500 + i);
    }
    auto u = db.create_write_context("rk_unkeyed", "");
    for (int i = 0; i < 4; i++) {
      db.write(u, (uint8_t*)"y", 1, 0, 2000 + i);
    }
  }

  nanots_reader reader("nanots_test_4mb.nts");

  // Keyed: every callback should see the right sec_key.
  std::vector<int64_t> seen_keyed;
  reader.read("rk_keyed", 0, NANOTS_SEC_KEY_UNSET, 100000, INT64_MAX,
              [&](const uint8_t* data, size_t size, uint32_t flags,
                  int64_t ts, int64_t sk,
                  int64_t block_seq, const std::string& meta) {
                seen_keyed.push_back(sk);
              });
  RTF_ASSERT(seen_keyed.size() == 4);
  for (int i = 0; i < 4; i++) {
    RTF_ASSERT(seen_keyed[i] == 500 + i);
  }

  // Unkeyed: every callback should see NANOTS_SEC_KEY_UNSET.
  std::vector<int64_t> seen_unkeyed;
  reader.read("rk_unkeyed", 0, NANOTS_SEC_KEY_UNSET, 100000, INT64_MAX,
              [&](const uint8_t* data, size_t size, uint32_t flags,
                  int64_t ts, int64_t sk,
                  int64_t block_seq, const std::string& meta) {
                seen_unkeyed.push_back(sk);
              });
  RTF_ASSERT(seen_unkeyed.size() == 4);
  for (auto sk : seen_unkeyed) {
    RTF_ASSERT(sk == NANOTS_SEC_KEY_UNSET);
  }
}

// Composite semantics: timestamps may repeat as long as sec_key strictly
// increases within the repeat. Matches the financial-data scenario where
// the exchange-supplied timestamp isn't unique but a sequence number is.
void test_nanots::test_nanots_composite_duplicate_timestamps() {
  nanots_writer db("nanots_test_4mb.nts", false);

  {
    auto wctx = db.create_write_context("dup_ts", "");
    // 3 frames at ts=1000 with strictly-increasing sec_keys, then a few at
    // ts=2000 with their own monotonic sec_keys.
    db.write(wctx, (uint8_t*)"a", 1, 0, 1000, /*sk=*/1);
    db.write(wctx, (uint8_t*)"b", 1, 0, 1000, /*sk=*/2);
    db.write(wctx, (uint8_t*)"c", 1, 0, 1000, /*sk=*/3);
    db.write(wctx, (uint8_t*)"d", 1, 0, 2000, /*sk=*/10);
    db.write(wctx, (uint8_t*)"e", 1, 0, 2000, /*sk=*/20);
  }

  // Iterate and verify all 5 frames come back in composite order.
  nanots_iterator iter("nanots_test_4mb.nts", "dup_ts");
  struct expected { int64_t ts; int64_t sk; char ch; };
  std::vector<expected> want = {
      {1000, 1, 'a'}, {1000, 2, 'b'}, {1000, 3, 'c'},
      {2000, 10, 'd'}, {2000, 20, 'e'},
  };
  size_t i = 0;
  while (iter.valid()) {
    RTF_ASSERT(i < want.size());
    RTF_ASSERT(iter->timestamp == want[i].ts);
    RTF_ASSERT(iter->secondary_key == want[i].sk);
    RTF_ASSERT(iter->size == 1);
    RTF_ASSERT(*iter->data == (uint8_t)want[i].ch);
    ++iter;
    i++;
  }
  RTF_ASSERT(i == 5);

  // Composite find lands exactly: (1000, 2) → frame 'b'.
  RTF_ASSERT(iter.find(1000, 2));
  RTF_ASSERT(*iter->data == 'b');

  // Within a ts run, find() with default sec_key lands on the first of the
  // run — frame 'a' for ts=1000.
  RTF_ASSERT(iter.find(1000));
  RTF_ASSERT(*iter->data == 'a');
  RTF_ASSERT(iter->secondary_key == 1);
}

// find(ts) with default sec_key (=UNSET=INT64_MIN) lands on the first frame
// at that timestamp regardless of sec_key, because UNSET is the smallest
// possible sec_key in lex order.
void test_nanots::test_nanots_composite_find_lands_on_first_at_ts() {
  nanots_writer db("nanots_test_4mb.nts", false);
  {
    auto wctx = db.create_write_context("first_at_ts", "");
    db.write(wctx, (uint8_t*)"x", 1, 0, 100, /*sk=*/-50);
    db.write(wctx, (uint8_t*)"y", 1, 0, 100, /*sk=*/0);
    db.write(wctx, (uint8_t*)"z", 1, 0, 100, /*sk=*/50);
    db.write(wctx, (uint8_t*)"q", 1, 0, 200, /*sk=*/-100);
  }

  nanots_iterator iter("nanots_test_4mb.nts", "first_at_ts");

  // find(100) lands on the first frame at ts=100 (which has the smallest
  // sk=-50).
  RTF_ASSERT(iter.find(100));
  RTF_ASSERT(iter->timestamp == 100);
  RTF_ASSERT(iter->secondary_key == -50);

  // find(150) — no frame at ts=150 — lands on first frame with ts >= 150.
  RTF_ASSERT(iter.find(150));
  RTF_ASSERT(iter->timestamp == 200);
  RTF_ASSERT(iter->secondary_key == -100);

  // find(100, -50) is the same as find(100) here: exact match on the first
  // frame.
  RTF_ASSERT(iter.find(100, -50));
  RTF_ASSERT(iter->secondary_key == -50);

  // find(100, 0) lands on the middle frame.
  RTF_ASSERT(iter.find(100, 0));
  RTF_ASSERT(iter->secondary_key == 0);

  // find(100, 1) lands on the next-greater composite — (100, 50).
  RTF_ASSERT(iter.find(100, 1));
  RTF_ASSERT(iter->secondary_key == 50);

  // find(100, 51) skips past the ts=100 run entirely to (200, -100).
  RTF_ASSERT(iter.find(100, 51));
  RTF_ASSERT(iter->timestamp == 200);
  RTF_ASSERT(iter->secondary_key == -100);
}

// A frame whose padded size exactly fills the space left in a block used to
// throw std::bad_optional_access from nanots_writer::write(). The fit check
// compared index_end against new_block_ofs, but new_block_ofs doubles as a
// "no room, roll over" sentinel equal to index_end, so a legitimate exact fit
// on the first write into an empty block was misread as "no room" and sent
// into the finalize path -- which dereferences wctx.last_timestamp before any
// write has set it. Padding rounds the frame up to 8 bytes, so the whole top
// 8 payload sizes collapse onto that offset, not just the documented maximum.
void test_nanots::test_nanots_exact_max_size_frame() {
  // allocate() rounds the block size up to a 64k boundary; 1 MiB is already
  // a multiple of 64k, so the effective block size is what we asked for.
  const size_t block_size = 1024 * 1024;
  const size_t max_size =
      block_size - (FRAME_HEADER_SIZE + INDEX_ENTRY_SIZE + BLOCK_HEADER_SIZE);

  nanots_writer db("nanots_test_16mb.nts", false);

  // Every size in the padding window must be writable as the first frame of a
  // fresh block, and must read back intact.
  for (size_t k = 0; k < 8; k++) {
    const size_t size = max_size - k;
    const std::string tag = "exact_max_" + std::to_string(k);

    std::vector<uint8_t> data(size);
    for (size_t j = 0; j < size; j++)
      data[j] = (uint8_t)((j + k) % 256);

    {
      auto wctx = db.create_write_context(tag, "exact max size");
      RTF_ASSERT_NO_THROW(db.write(wctx, data.data(), size, 0, 1000));
    }

    nanots_iterator iter("nanots_test_16mb.nts", tag);
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->size == size);
    RTF_ASSERT(iter->timestamp == 1000);
    RTF_ASSERT(iter->data[0] == (uint8_t)(k % 256));
    RTF_ASSERT(iter->data[size - 1] == (uint8_t)((size - 1 + k) % 256));
    ++iter;
    RTF_ASSERT(!iter.valid());
  }

  // An exact-fit frame leaves a block with zero usable space, so the next
  // write must roll over to a new block rather than overlap the first frame.
  {
    std::vector<uint8_t> big(max_size, 0xAB);
    std::vector<uint8_t> tiny(16, 0xCD);

    {
      auto wctx = db.create_write_context("exact_max_rollover", "rollover");
      RTF_ASSERT_NO_THROW(db.write(wctx, big.data(), big.size(), 0, 1000));
      RTF_ASSERT_NO_THROW(db.write(wctx, tiny.data(), tiny.size(), 0, 2000));
    }

    nanots_iterator iter("nanots_test_16mb.nts", "exact_max_rollover");
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->size == max_size);
    RTF_ASSERT(iter->timestamp == 1000);
    RTF_ASSERT(iter->data[0] == 0xAB);
    RTF_ASSERT(iter->data[max_size - 1] == 0xAB);
    const int64_t first_block_sequence = iter->block_sequence;

    ++iter;
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->size == 16);
    RTF_ASSERT(iter->timestamp == 2000);
    RTF_ASSERT(iter->data[0] == 0xCD);
    RTF_ASSERT(iter->data[15] == 0xCD);
    // The two frames must live in different blocks -- an exact fit leaves no
    // usable space, so overlapping the first frame is the failure to catch.
    RTF_ASSERT(iter->block_sequence != first_block_sequence);

    ++iter;
    RTF_ASSERT(!iter.valid());
  }

  // Oversized frames are still rejected.
  {
    auto wctx = db.create_write_context("exact_max_toobig", "too big");
    std::vector<uint8_t> toobig(max_size + 1, 0x11);
    RTF_ASSERT_THROWS(db.write(wctx, toobig.data(), toobig.size(), 0, 1000),
                      nanots_exception);
  }
}

// _validate_blocks() reclaims blocks left unfinalized by a crash (end_timestamp
// still NULL), keeping them only if it can find a valid frame. Its bounds check
// used to reserve an (n_valid_indexes + 1)'th index slot, one more than the
// block actually occupies, so a frame packed flush against the index region was
// judged invalid. When it was the only frame the entire block was freed, losing
// committed data. A max-size frame lands exactly on that boundary.
void test_nanots::test_nanots_unfinalized_block_with_max_frame_survives() {
  const size_t block_size = 1024 * 1024;
  const size_t max_size =
      block_size - (FRAME_HEADER_SIZE + INDEX_ENTRY_SIZE + BLOCK_HEADER_SIZE);

  auto db_name = _database_name("nanots_test_4mb.nts");

  // A control frame (comfortably clear of the index region) and the max-size
  // frame (flush against it) must both survive recovery.
  struct { const char* tag; size_t size; } cases[] = {
    {"unfinalized_small", 1000},
    {"unfinalized_max", max_size},
  };

  for (const auto& c : cases) {
    std::vector<uint8_t> data(c.size, 0x5A);
    data[0] = 0x01;
    data[c.size - 1] = 0x02;

    {
      nanots_writer db("nanots_test_4mb.nts", false);
      auto wctx = db.create_write_context(c.tag, "unfinalized");
      db.write(wctx, data.data(), data.size(), 0, 1000);
    }

    // Simulate dying before the block was finalized. A clean shutdown runs
    // ~write_context(), which stamps end_timestamp; a crash does not.
    {
      nts_sqlite_conn conn(db_name, true, true);
      auto stmt = conn.prepare(
          "UPDATE segment_blocks SET end_timestamp = NULL, "
          "end_secondary_key = NULL WHERE segment_id IN "
          "(SELECT id FROM segments WHERE stream_tag = ?);");
      stmt.bind(1, std::string(c.tag)).exec_no_result();
    }

    // Opening a writer runs _validate_blocks() over every unfinalized block.
    { nanots_writer recover("nanots_test_4mb.nts", false); }

    int n = 0;
    size_t got_size = 0;
    uint8_t first = 0, last = 0;
    nanots_reader reader("nanots_test_4mb.nts");
    reader.read(c.tag, 0, NANOTS_SEC_KEY_UNSET, INT64_MAX, INT64_MAX,
                [&](const uint8_t* d, size_t size, uint32_t /*flags*/,
                    int64_t /*timestamp*/, int64_t /*secondary_key*/,
                    int64_t /*block_sequence*/, const std::string& /*metadata*/) {
                  n++;
                  got_size = size;
                  first = d[0];
                  last = d[size - 1];
                });

    RTF_ASSERT(n == 1);
    RTF_ASSERT(got_size == c.size);
    RTF_ASSERT(first == 0x01);
    RTF_ASSERT(last == 0x02);
  }
}

// Timestamp zero is real data. While a block is live its catalog end is NULL;
// after finalization the same column may legitimately contain integer zero.
void test_nanots::test_nanots_timestamp_zero_and_null_open_block() {
  const char* file_name = "nanots_test_4mb.nts";
  const std::string db_name = _database_name(file_name);

  nanots_writer db(file_name, false);
  {
    auto wctx = db.create_write_context("zero_ts", "zero timestamp");
    const std::string data = "epoch";
    db.write(wctx, reinterpret_cast<const uint8_t*>(data.data()), data.size(),
             0x42, 0);

    nts_sqlite_conn conn(db_name, false, false);
    auto rows = conn.exec(
        "SELECT end_timestamp, end_secondary_key FROM segment_blocks sb "
        "JOIN segments s ON s.id = sb.segment_id "
        "WHERE s.stream_tag = 'zero_ts';");
    RTF_ASSERT(rows.size() == 1);
    RTF_ASSERT(!rows[0]["end_timestamp"].has_value());
    RTF_ASSERT(!rows[0]["end_secondary_key"].has_value());

    // Both read APIs must understand a live block with a NULL end.
    nanots_iterator iter(file_name, "zero_ts");
    RTF_ASSERT(iter.valid());
    RTF_ASSERT(iter->timestamp == 0);
    RTF_ASSERT(iter->flags == 0x42);

    nanots_reader reader(file_name);
    int count = 0;
    reader.read("zero_ts", 0, NANOTS_SEC_KEY_UNSET, 0, INT64_MAX,
                [&](const uint8_t*, size_t, uint32_t, int64_t timestamp,
                    int64_t, int64_t, const std::string&) {
                  RTF_ASSERT(timestamp == 0);
                  count++;
                });
    RTF_ASSERT(count == 1);
    auto tags = reader.query_stream_tags(
        0, NANOTS_SEC_KEY_UNSET, 0, INT64_MAX);
    RTF_ASSERT(tags.size() == 1);
    RTF_ASSERT(tags[0] == "zero_ts");
    auto segments = reader.query_contiguous_segments(
        "zero_ts", 0, NANOTS_SEC_KEY_UNSET, 0, INT64_MAX);
    RTF_ASSERT(segments.size() == 1);
    RTF_ASSERT(segments[0].start_timestamp == 0);
    // The public C/C++ result structs retain their historical open-end value;
    // the catalog and internal C++ state no longer rely on that sentinel.
    RTF_ASSERT(segments[0].end_timestamp == 0);
  }

  // Finalization changes NULL to the actual endpoint, which is still zero.
  {
    nts_sqlite_conn conn(db_name, false, false);
    auto rows = conn.exec(
        "SELECT end_timestamp FROM segment_blocks sb "
        "JOIN segments s ON s.id = sb.segment_id "
        "WHERE s.stream_tag = 'zero_ts';");
    RTF_ASSERT(rows.size() == 1);
    RTF_ASSERT(rows[0]["end_timestamp"].has_value());
    RTF_ASSERT(std::stoll(rows[0]["end_timestamp"].value()) == 0);
  }

  // A finalized zero-timestamp block is deletable; it is no longer confused
  // with the formerly-zero open-block sentinel.
  nanots_writer::free_blocks(file_name, "zero_ts", 0,
                             NANOTS_SEC_KEY_UNSET, 0, INT64_MAX);
  nanots_iterator after_free(file_name, "zero_ts");
  RTF_ASSERT(!after_free.valid());
}

// A v2 catalog cannot distinguish an open block from a finalized block ending
// at timestamp zero. The v3 migration resolves both from the committed indexes.
void test_nanots::test_nanots_catalog_v2_to_v3_migration() {
  const char* file_name = "nanots_test_4mb.nts";
  const std::string db_name = _database_name(file_name);

  {
    nanots_writer db(file_name, false);
    {
      auto wctx = db.create_write_context("migrate_zero", "v2 zero");
      const uint8_t value = 0x10;
      db.write(wctx, &value, 1, 0, 0);
    }
    {
      auto wctx = db.create_write_context("migrate_positive", "v2 open");
      const uint8_t value = 0x20;
      db.write(wctx, &value, 1, 0, 4242);
    }
  }

  // Recreate the v2 ambiguity for both rows. For migrate_zero this represents
  // a finalized endpoint of zero; for migrate_positive it represents a block
  // left open by a crash.
  {
    nts_sqlite_conn conn(db_name, true, true);
    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& tx) {
      tx.exec(
          "UPDATE segment_blocks SET end_timestamp = 0, "
          "end_secondary_key = 0 WHERE segment_id IN "
          "(SELECT id FROM segments WHERE stream_tag IN "
          "('migrate_zero', 'migrate_positive')); ");
      tx.exec("PRAGMA user_version = 2;");
    });
  }

  // Opening any NanoTS writer performs and completes the catalog migration.
  { nanots_writer migrated(file_name, false); }

  {
    nts_sqlite_conn conn(db_name, false, false);
    auto version = conn.exec("PRAGMA user_version;");
    RTF_ASSERT(version.size() == 1);
    RTF_ASSERT(std::stoi(version[0].begin()->second.value()) == 3);

    auto rows = conn.exec(
        "SELECT s.stream_tag, sb.end_timestamp, sb.end_secondary_key "
        "FROM segment_blocks sb JOIN segments s ON s.id = sb.segment_id "
        "WHERE s.stream_tag IN ('migrate_zero', 'migrate_positive') "
        "ORDER BY s.stream_tag;");
    RTF_ASSERT(rows.size() == 2);
    for (const auto& row : rows) {
      RTF_ASSERT(row.at("end_timestamp").has_value());
      RTF_ASSERT(row.at("end_secondary_key").has_value());
      const auto& tag = row.at("stream_tag").value();
      const int64_t end = std::stoll(row.at("end_timestamp").value());
      if (tag == "migrate_zero")
        RTF_ASSERT(end == 0);
      else if (tag == "migrate_positive")
        RTF_ASSERT(end == 4242);
      else
        RTF_ASSERT(false);
    }
  }

  // Reader-only applications must also be able to perform the first upgrade.
  // Downgrade the simulated catalog once more and prove that constructing a
  // reader migrates and recovers it before issuing any query.
  {
    nts_sqlite_conn conn(db_name, true, true);
    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& tx) {
      tx.exec(
          "UPDATE segment_blocks SET end_timestamp = 0, "
          "end_secondary_key = 0 WHERE segment_id IN "
          "(SELECT id FROM segments WHERE stream_tag IN "
          "('migrate_zero', 'migrate_positive')); ");
      tx.exec("PRAGMA user_version = 2;");
    });
  }

  {
    nanots_reader reader(file_name);
    int count = 0;
    reader.read("migrate_positive", 4242, NANOTS_SEC_KEY_UNSET,
                4242, INT64_MAX,
                [&](const uint8_t*, size_t, uint32_t, int64_t timestamp,
                    int64_t, int64_t, const std::string&) {
                  RTF_ASSERT(timestamp == 4242);
                  count++;
                });
    RTF_ASSERT(count == 1);
  }

  {
    nts_sqlite_conn conn(db_name, false, false);
    auto version = conn.exec("PRAGMA user_version;");
    RTF_ASSERT(std::stoi(version[0].begin()->second.value()) == 3);
    auto open_rows = conn.exec(
        "SELECT 1 FROM segment_blocks WHERE end_timestamp IS NULL;");
    RTF_ASSERT(open_rows.empty());
  }

  nanots_iterator zero(file_name, "migrate_zero");
  RTF_ASSERT(zero.valid());
  RTF_ASSERT(zero->timestamp == 0);
  nanots_iterator positive(file_name, "migrate_positive");
  RTF_ASSERT(positive.valid());
  RTF_ASSERT(positive->timestamp == 4242);
}
