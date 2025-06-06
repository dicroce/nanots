#include "framework.h"

class test_nanots_c_api : public test_fixture {
 public:
  RTF_FIXTURE(test_nanots_c_api);
  TEST(test_nanots_c_api::test_c_api_basic_write_read);
  TEST(test_nanots_c_api::test_c_api_iterator_functionality);
  TEST(test_nanots_c_api::test_c_api_contiguous_segments);
  TEST(test_nanots_c_api::test_c_api_error_handling);
  TEST(test_nanots_c_api::test_c_api_multiple_streams);
  RTF_FIXTURE_END();

  virtual ~test_nanots_c_api() throw() {}

  virtual void setup();
  virtual void teardown();

  void test_c_api_basic_write_read();
  void test_c_api_iterator_functionality();
  void test_c_api_contiguous_segments();
  void test_c_api_error_handling();
  void test_c_api_multiple_streams();
};