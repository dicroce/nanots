
#ifndef test_nanots_ebr_h
#define test_nanots_ebr_h

#include "framework.h"

class test_nanots_ebr : public test_fixture {
 public:
  RTF_FIXTURE(test_nanots_ebr);
    TEST(test_nanots_ebr::test_basic_auto_reclaim_no_readers);
    TEST(test_nanots_ebr::test_reader_pins_then_releases);
    TEST(test_nanots_ebr::test_inactive_slot_does_not_pin);
    TEST(test_nanots_ebr::test_dead_heartbeat_does_not_pin);
    TEST(test_nanots_ebr::test_iterator_op_advances_epoch);
    TEST(test_nanots_ebr::test_stress_concurrent_writer_readers);
    TEST(test_nanots_ebr::test_ts_index_refresh_finds_new_blocks);
    TEST(test_nanots_ebr::test_blocked_reclaim_retires_only_one_block);
  RTF_FIXTURE_END();

  virtual ~test_nanots_ebr() throw() {}

  virtual void setup();
  virtual void teardown();

  void test_basic_auto_reclaim_no_readers();
  void test_reader_pins_then_releases();
  void test_inactive_slot_does_not_pin();
  void test_dead_heartbeat_does_not_pin();
  void test_iterator_op_advances_epoch();
  void test_stress_concurrent_writer_readers();
  void test_ts_index_refresh_finds_new_blocks();
  void test_blocked_reclaim_retires_only_one_block();
};

#endif
