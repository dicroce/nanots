
#include "framework.h"

class test_nanots : public test_fixture {
 public:
  RTF_FIXTURE(test_nanots);
  TEST(test_nanots::test_nanots_basic);
  TEST(test_nanots::test_nanots_iterator_find);
  TEST(test_nanots::test_nanots_multiple_streams);
  TEST(test_nanots::test_nanots_reader_time_range);
  TEST(test_nanots::test_nanots_iterator_bidirectional);
  TEST(test_nanots::test_nanots_large_frames);
  TEST(test_nanots::test_nanots_edge_cases);
  TEST(test_nanots::test_nanots_monotonic_timestamp_validation);
  TEST(test_nanots::test_nanots_performance_baseline);
  TEST(test_nanots::test_nanots_concurrent_readers);
  TEST(test_nanots::test_nanots_metadata_integrity);
  TEST(test_nanots::test_nanots_block_exhaustion);
  TEST(test_nanots::test_nanots_block_filling_and_transition);
  TEST(test_nanots::test_nanots_sparse_timestamp_seeking);
  TEST(test_nanots::test_nanots_write_context_lifecycle);
  TEST(test_nanots::test_nanots_multiple_streams_separate_writers);
  TEST(test_nanots::test_nanots_invalid_multiple_writers_same_stream);
  TEST(test_nanots::test_nanots_multiple_segments_same_stream);
  TEST(test_nanots::test_nanots_iterator_edge_navigation);
  TEST(test_nanots::test_nanots_mixed_frame_sizes);
  TEST(test_nanots::test_nanots_reader_callback_exceptions);
  TEST(test_nanots::test_nanots_high_frequency_writes);
  TEST(test_nanots::test_nanots_timestamp_precision);
  TEST(test_nanots::test_nanots_free_blocks);
  TEST(test_nanots::test_nanots_query_contiguous_segments);
  TEST(test_nanots::test_nanots_query_stream_tags);
  TEST(test_nanots::test_nanots_progressive_block_deletion);
  TEST(test_nanots::test_nanots_iterator_block_transition_flag_search);
  RTF_FIXTURE_END();

  virtual ~test_nanots() throw() {}

  virtual void setup();
  virtual void teardown();

  void test_nanots_basic();
  void test_nanots_iterator_find();
  void test_nanots_basic_read();
  void test_nanots_multiple_streams();
  void test_nanots_reader_time_range();
  void test_nanots_iterator_bidirectional();
  void test_nanots_large_frames();
  void test_nanots_edge_cases();
  void test_nanots_monotonic_timestamp_validation();
  void test_nanots_performance_baseline();
  void test_nanots_concurrent_readers();
  void test_nanots_metadata_integrity();
  void test_nanots_block_exhaustion();
  void test_nanots_block_filling_and_transition();
  void test_nanots_sparse_timestamp_seeking();
  void test_nanots_write_context_lifecycle();
  void test_nanots_multiple_streams_separate_writers();
  void test_nanots_invalid_multiple_writers_same_stream();
  void test_nanots_multiple_segments_same_stream();
  void test_nanots_iterator_edge_navigation();
  void test_nanots_mixed_frame_sizes();
  void test_nanots_reader_callback_exceptions();
  void test_nanots_high_frequency_writes();
  void test_nanots_timestamp_precision();
  void test_nanots_free_blocks();
  void test_nanots_query_contiguous_segments();
  void test_nanots_query_stream_tags();
  void test_nanots_progressive_block_deletion();
  void test_nanots_iterator_block_transition_flag_search();
};
