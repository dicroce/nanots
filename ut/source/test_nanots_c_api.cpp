#include "test_nanots_c_api.h"
#include <cstring>
#include <string>
#include <vector>
#include "nanots.h"


using namespace std;

REGISTER_TEST_FIXTURE(test_nanots_c_api);

static void _whack_c_api_files() {
  if (rtf_file_exists("nanots_c_api_test.nts"))
    rtf_remove_file("nanots_c_api_test.nts");
  if (rtf_file_exists("nanots_c_api_test.db"))
    rtf_remove_file("nanots_c_api_test.db");
}

void test_nanots_c_api::setup() {
  _whack_c_api_files();

  // Allocate test file using C API
  nanots_ec_t result =
      nanots_writer_allocate_file("nanots_c_api_test.nts", 1024 * 1024, 4);
  RTF_ASSERT(result == NANOTS_EC_OK);
}

void test_nanots_c_api::teardown() {
  _whack_c_api_files();
}

void test_nanots_c_api::test_c_api_basic_write_read() {
  // Test writer creation
  nanots_writer_t writer = nanots_writer_create("nanots_c_api_test.nts", 0);
  RTF_ASSERT(writer != nullptr);

  // Test write context creation
  nanots_write_context_t context =
      nanots_writer_create_context(writer, "test_stream", "test metadata");
  RTF_ASSERT(context != nullptr);

  // Write test data
  const char* data1 = "Hello, C API!";
  const char* data2 = "Second frame";
  const char* data3 = "Third frame with more data";

  nanots_ec_t result = nanots_writer_write(
      writer, context, (const uint8_t*)data1, strlen(data1), 1000, 0x01);
  RTF_ASSERT(result == NANOTS_EC_OK);

  result = nanots_writer_write(writer, context, (const uint8_t*)data2,
                               strlen(data2), 2000, 0x02);
  RTF_ASSERT(result == NANOTS_EC_OK);

  result = nanots_writer_write(writer, context, (const uint8_t*)data3,
                               strlen(data3), 3000, 0x03);
  RTF_ASSERT(result == NANOTS_EC_OK);

  // Clean up writer resources
  nanots_write_context_destroy(context);
  nanots_writer_destroy(writer);
  // Test reader functionality
  nanots_reader_t reader = nanots_reader_create("nanots_c_api_test.nts");
  RTF_ASSERT(reader != nullptr);
  // Callback data collection
  struct callback_data {
    vector<string> frames;
    vector<uint8_t> flags;
    vector<int64_t> timestamps;
    vector<int64_t> block_sequences;
  } cb_data;

  // Define callback function
  auto callback = [](const uint8_t* data, size_t size, uint8_t flags,
                     int64_t timestamp, int64_t block_sequence,
                     void* user_data) {
    callback_data* cb = static_cast<callback_data*>(user_data);
    cb->frames.emplace_back(reinterpret_cast<const char*>(data), size);
    cb->flags.push_back(flags);
    cb->timestamps.push_back(timestamp);
    cb->block_sequences.push_back(block_sequence);
  };

  // Read data back - use a smaller end timestamp to avoid potential UINT64_MAX
  // binding issues
  result =
      nanots_reader_read(reader, "test_stream", 0, 10000, callback, &cb_data);
  RTF_ASSERT(result == NANOTS_EC_OK);

  // Verify read data
  RTF_ASSERT(cb_data.frames.size() == 3);
  RTF_ASSERT(cb_data.frames[0] == "Hello, C API!");
  RTF_ASSERT(cb_data.frames[1] == "Second frame");
  RTF_ASSERT(cb_data.frames[2] == "Third frame with more data");
  RTF_ASSERT(cb_data.flags[0] == 0x01);
  RTF_ASSERT(cb_data.flags[1] == 0x02);
  RTF_ASSERT(cb_data.flags[2] == 0x03);
  RTF_ASSERT(cb_data.timestamps[0] == 1000);
  RTF_ASSERT(cb_data.timestamps[1] == 2000);
  RTF_ASSERT(cb_data.timestamps[2] == 3000);

  nanots_reader_destroy(reader);
}

void test_nanots_c_api::test_c_api_iterator_functionality() {
  // Create writer and write test data
  nanots_writer_t writer = nanots_writer_create("nanots_c_api_test.nts", 0);
  RTF_ASSERT(writer != nullptr);

  nanots_write_context_t context =
      nanots_writer_create_context(writer, "iter_stream", "iterator test");
  RTF_ASSERT(context != nullptr);

  // Write multiple frames
  for (int i = 0; i < 5; i++) {
    string data = "Frame " + to_string(i);
    nanots_ec_t result =
        nanots_writer_write(writer, context, (const uint8_t*)data.c_str(),
                            data.size(), 1000 + i * 100, (uint8_t)i);
    RTF_ASSERT(result == NANOTS_EC_OK);
  }

  nanots_write_context_destroy(context);
  nanots_writer_destroy(writer);

  // Test iterator
  nanots_iterator_t iterator =
      nanots_iterator_create("nanots_c_api_test.nts", "iter_stream");
  RTF_ASSERT(iterator != nullptr);

  // Test validity
  RTF_ASSERT(nanots_iterator_valid(iterator) == 1);

  // Test forward iteration
  int frame_count = 0;
  nanots_frame_info_t frame_info;

  while (nanots_iterator_valid(iterator)) {
    nanots_ec_t result =
        nanots_iterator_get_current_frame(iterator, &frame_info);
    RTF_ASSERT(result == NANOTS_EC_OK);

    string expected_data = "Frame " + to_string(frame_count);
    string actual_data(reinterpret_cast<const char*>(frame_info.data),
                       frame_info.size);
    RTF_ASSERT(actual_data == expected_data);
    RTF_ASSERT(frame_info.flags == frame_count);
    RTF_ASSERT(frame_info.timestamp == (int64_t)(1000 + frame_count * 100));

    frame_count++;
    if (frame_count < 5) {
      result = nanots_iterator_next(iterator);
      RTF_ASSERT(result == NANOTS_EC_OK);
    } else {
      break;
    }
  }
  RTF_ASSERT(frame_count == 5);

  // Test backward iteration
  frame_count = 4;
  while (nanots_iterator_valid(iterator) && frame_count >= 0) {
    nanots_ec_t result =
        nanots_iterator_get_current_frame(iterator, &frame_info);
    RTF_ASSERT(result == NANOTS_EC_OK);

    string expected_data = "Frame " + to_string(frame_count);
    string actual_data(reinterpret_cast<const char*>(frame_info.data),
                       frame_info.size);
    RTF_ASSERT(actual_data == expected_data);

    if (frame_count > 0) {
      result = nanots_iterator_prev(iterator);
      RTF_ASSERT(result == NANOTS_EC_OK);
    }
    frame_count--;
  }

  // Test find functionality
  nanots_ec_t result = nanots_iterator_find(iterator, 1200);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(nanots_iterator_valid(iterator) == 1);

  result = nanots_iterator_get_current_frame(iterator, &frame_info);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(frame_info.timestamp == 1200);

  string actual_data(reinterpret_cast<const char*>(frame_info.data),
                     frame_info.size);
  RTF_ASSERT(actual_data == "Frame 2");

  // Test reset
  result = nanots_iterator_reset(iterator);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(nanots_iterator_valid(iterator) == 1);

  result = nanots_iterator_get_current_frame(iterator, &frame_info);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(frame_info.timestamp == 1000);

  nanots_iterator_destroy(iterator);
}

void test_nanots_c_api::test_c_api_contiguous_segments() {
  // Create writer and write test data
  nanots_writer_t writer = nanots_writer_create("nanots_c_api_test.nts", 0);
  RTF_ASSERT(writer != nullptr);

  nanots_write_context_t context =
      nanots_writer_create_context(writer, "segment_stream", "segment test");
  RTF_ASSERT(context != nullptr);

  // Write some data
  for (int i = 0; i < 3; i++) {
    string data = "Segment data " + to_string(i);
    nanots_ec_t result =
        nanots_writer_write(writer, context, (const uint8_t*)data.c_str(),
                            data.size(), 1000 + i * 100, 0);
    RTF_ASSERT(result == NANOTS_EC_OK);
  }

  nanots_write_context_destroy(context);
  nanots_writer_destroy(writer);

  // Test contiguous segments query
  nanots_reader_t reader = nanots_reader_create("nanots_c_api_test.nts");
  RTF_ASSERT(reader != nullptr);

  nanots_contiguous_segment_t* segments = nullptr;
  size_t count = 0;

  nanots_ec_t result = nanots_reader_query_contiguous_segments(
      reader, "segment_stream", 0, 10000, &segments, &count);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(count > 0);
  RTF_ASSERT(segments != nullptr);
  RTF_ASSERT(segments[0].start_timestamp <= 1000);
  RTF_ASSERT(segments[0].end_timestamp >= 1200);

  // Clean up
  nanots_free_contiguous_segments(segments);
  nanots_reader_destroy(reader);
}

void test_nanots_c_api::test_c_api_error_handling() {
  // Test invalid handles
  RTF_ASSERT(nanots_iterator_valid(nullptr) == 0);
  RTF_ASSERT(nanots_iterator_current_block_sequence(nullptr) == 0);

  nanots_frame_info_t frame_info;
  RTF_ASSERT(nanots_iterator_get_current_frame(nullptr, &frame_info) ==
             NANOTS_EC_INVALID_ARGUMENT);
  RTF_ASSERT(nanots_iterator_next(nullptr) == NANOTS_EC_INVALID_ARGUMENT);
  RTF_ASSERT(nanots_iterator_prev(nullptr) == NANOTS_EC_INVALID_ARGUMENT);
  RTF_ASSERT(nanots_iterator_find(nullptr, 1000) == NANOTS_EC_INVALID_ARGUMENT);
  RTF_ASSERT(nanots_iterator_reset(nullptr) == NANOTS_EC_INVALID_ARGUMENT);

  // Test null pointer parameters
  nanots_contiguous_segment_t* segments = nullptr;
  size_t count = 0;
  RTF_ASSERT(nanots_reader_query_contiguous_segments(nullptr, "test", 0, 100,
                                                     &segments, &count) ==
             NANOTS_EC_INVALID_ARGUMENT);

  // Test monotonic timestamp validation
  nanots_writer_t writer = nanots_writer_create("nanots_c_api_test.nts", 0);
  RTF_ASSERT(writer != nullptr);

  nanots_write_context_t context =
      nanots_writer_create_context(writer, "error_stream", "error test");
  RTF_ASSERT(context != nullptr);

  const char* data = "test";

  // Write first frame
  nanots_ec_t result = nanots_writer_write(
      writer, context, (const uint8_t*)data, strlen(data), 2000, 0);
  RTF_ASSERT(result == NANOTS_EC_OK);

  // Try to write frame with earlier timestamp (should fail)
  result = nanots_writer_write(writer, context, (const uint8_t*)data,
                               strlen(data), 1000, 0);
  RTF_ASSERT(result == NANOTS_EC_NON_MONOTONIC_TIMESTAMP);

  nanots_write_context_destroy(context);
  nanots_writer_destroy(writer);
}

void test_nanots_c_api::test_c_api_multiple_streams() {
  nanots_writer_t writer = nanots_writer_create("nanots_c_api_test.nts", 0);
  RTF_ASSERT(writer != nullptr);

  // Create contexts for multiple streams
  nanots_write_context_t context1 =
      nanots_writer_create_context(writer, "stream1", "metadata1");
  nanots_write_context_t context2 =
      nanots_writer_create_context(writer, "stream2", "metadata2");
  RTF_ASSERT(context1 != nullptr);
  RTF_ASSERT(context2 != nullptr);

  // Write to both streams
  const char* data1 = "Stream 1 data";
  const char* data2 = "Stream 2 data";

  nanots_ec_t result = nanots_writer_write(
      writer, context1, (const uint8_t*)data1, strlen(data1), 1000, 1);
  RTF_ASSERT(result == NANOTS_EC_OK);

  result = nanots_writer_write(writer, context2, (const uint8_t*)data2,
                               strlen(data2), 1100, 2);
  RTF_ASSERT(result == NANOTS_EC_OK);

  nanots_write_context_destroy(context1);
  nanots_write_context_destroy(context2);
  nanots_writer_destroy(writer);

  // Read from both streams separately
  nanots_reader_t reader = nanots_reader_create("nanots_c_api_test.nts");
  RTF_ASSERT(reader != nullptr);

  // Test stream1
  vector<string> stream1_data;
  auto callback1 = [](const uint8_t* data, size_t size, uint8_t flags,
                      int64_t timestamp, int64_t block_sequence,
                      void* user_data) {
    vector<string>* frames = static_cast<vector<string>*>(user_data);
    frames->emplace_back(reinterpret_cast<const char*>(data), size);
  };

  result =
      nanots_reader_read(reader, "stream1", 0, 10000, callback1, &stream1_data);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(stream1_data.size() == 1);
  RTF_ASSERT(stream1_data[0] == "Stream 1 data");

  // Test stream2
  vector<string> stream2_data;
  result =
      nanots_reader_read(reader, "stream2", 0, 10000, callback1, &stream2_data);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(stream2_data.size() == 1);
  RTF_ASSERT(stream2_data[0] == "Stream 2 data");

  nanots_reader_destroy(reader);
}

void test_nanots_c_api::test_c_api_query_stream_tags() {
  nanots_writer_t writer = nanots_writer_create("nanots_c_api_test.nts", 0);
  RTF_ASSERT(writer != nullptr);

  // Create multiple streams with different time ranges
  nanots_write_context_t video_ctx =
      nanots_writer_create_context(writer, "video", "h264 video");
  nanots_write_context_t audio_ctx =
      nanots_writer_create_context(writer, "audio", "aac audio");
  nanots_write_context_t metadata_ctx =
      nanots_writer_create_context(writer, "metadata", "sensor data");

  RTF_ASSERT(video_ctx != nullptr);
  RTF_ASSERT(audio_ctx != nullptr);
  RTF_ASSERT(metadata_ctx != nullptr);

  // Write video frames: 1000-5000
  for (int i = 0; i < 5; i++) {
    string data = "video_frame_" + to_string(i);
    int64_t timestamp = 1000 + (i * 1000);
    nanots_ec_t result = nanots_writer_write(
        writer, video_ctx, (const uint8_t*)data.c_str(), data.size(), timestamp, 0x01);
    RTF_ASSERT(result == NANOTS_EC_OK);
  }

  // Write audio samples: 2000-6000 (overlaps with video)
  for (int i = 0; i < 5; i++) {
    string data = "audio_sample_" + to_string(i);
    int64_t timestamp = 2000 + (i * 1000);
    nanots_ec_t result = nanots_writer_write(
        writer, audio_ctx, (const uint8_t*)data.c_str(), data.size(), timestamp, 0x02);
    RTF_ASSERT(result == NANOTS_EC_OK);
  }

  // Write metadata: 8000-12000 (non-overlapping)
  for (int i = 0; i < 5; i++) {
    string data = "sensor_" + to_string(i);
    int64_t timestamp = 8000 + (i * 1000);
    nanots_ec_t result = nanots_writer_write(
        writer, metadata_ctx, (const uint8_t*)data.c_str(), data.size(), timestamp, 0x03);
    RTF_ASSERT(result == NANOTS_EC_OK);
  }

  nanots_write_context_destroy(video_ctx);
  nanots_write_context_destroy(audio_ctx);
  nanots_write_context_destroy(metadata_ctx);
  nanots_writer_destroy(writer);

  // Test query_stream_tags with callback
  nanots_reader_t reader = nanots_reader_create("nanots_c_api_test.nts");
  RTF_ASSERT(reader != nullptr);

  // Test callback for collecting stream tags
  vector<string> collected_tags;
  auto stream_tag_callback = [](const char* stream_tag, void* user_data) {
    vector<string>* tags = static_cast<vector<string>*>(user_data);
    tags->push_back(string(stream_tag));
  };

  // Query range that includes all streams (1000-12000)
  collected_tags.clear();
  nanots_ec_t result = nanots_reader_query_stream_tags(
      reader, 1000, 12000, stream_tag_callback, &collected_tags);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(collected_tags.size() == 3);
  
  // Convert to set for easier checking (order may vary)
  set<string> all_tags_set(collected_tags.begin(), collected_tags.end());
  RTF_ASSERT(all_tags_set.count("video") == 1);
  RTF_ASSERT(all_tags_set.count("audio") == 1);
  RTF_ASSERT(all_tags_set.count("metadata") == 1);

  // Query range that includes only video and audio (2000-6000)
  collected_tags.clear();
  result = nanots_reader_query_stream_tags(
      reader, 2000, 6000, stream_tag_callback, &collected_tags);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(collected_tags.size() == 2);
  
  set<string> video_audio_set(collected_tags.begin(), collected_tags.end());
  RTF_ASSERT(video_audio_set.count("video") == 1);
  RTF_ASSERT(video_audio_set.count("audio") == 1);
  RTF_ASSERT(video_audio_set.count("metadata") == 0);

  // Query range that includes only metadata (8000-12000)
  collected_tags.clear();
  result = nanots_reader_query_stream_tags(
      reader, 8000, 12000, stream_tag_callback, &collected_tags);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(collected_tags.size() == 1);
  RTF_ASSERT(collected_tags[0] == "metadata");

  // Query range with no data (20000-25000)
  collected_tags.clear();
  result = nanots_reader_query_stream_tags(
      reader, 20000, 25000, stream_tag_callback, &collected_tags);
  RTF_ASSERT(result == NANOTS_EC_OK);
  RTF_ASSERT(collected_tags.empty());

  // Test error handling - null callback
  result = nanots_reader_query_stream_tags(reader, 1000, 2000, nullptr, &collected_tags);
  RTF_ASSERT(result == NANOTS_EC_INVALID_ARGUMENT);

  // Test error handling - null reader
  result = nanots_reader_query_stream_tags(nullptr, 1000, 2000, stream_tag_callback, &collected_tags);
  RTF_ASSERT(result == NANOTS_EC_INVALID_ARGUMENT);

  nanots_reader_destroy(reader);
}