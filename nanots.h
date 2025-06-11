
#ifndef NANOTS_H
#define NANOTS_H

#include "utils.h"

extern "C" {

enum nanots_ec_t {
  NANOTS_EC_OK = 0,
  NANOTS_EC_CANT_OPEN = 1,
  NANOTS_EC_SCHEMA = 2,
  NANOTS_EC_NO_FREE_BLOCKS = 3,
  NANOTS_EC_INVALID_BLOCK_SIZE = 4,
  NANOTS_EC_DUPLICATE_STREAM_TAG = 5,
  NANOTS_EC_UNABLE_TO_CREATE_SEGMENT = 6,
  NANOTS_EC_UNABLE_TO_CREATE_SEGMENT_BLOCK = 7,
  NANOTS_EC_NON_MONOTONIC_TIMESTAMP = 8,
  NANOTS_EC_ROW_SIZE_TOO_BIG = 9,
  NANOTS_EC_UNABLE_TO_ALLOCATE_FILE = 10,
  NANOTS_EC_INVALID_ARGUMENT = 11,
  NANOTS_EC_UNKNOWN = 12
};

}

class nanots_exception : public std::exception {
public:
  nanots_exception(nanots_ec_t ec, const std::string& message, const std::string& file, int line) : _ec(ec), _message(message), _file(file), _line(line) {}
  nanots_exception(const nanots_exception& other) = default;
  nanots_exception& operator=(const nanots_exception& other) = default;
  nanots_exception(nanots_exception&& other) noexcept = default;
  nanots_exception& operator=(nanots_exception&& other) noexcept = default;
  nanots_ec_t get_ec() const { return _ec; }
  const char* what() const noexcept override {
    _formatted_message = format_s("%s:%d: %d(%s)", _file.c_str(), _line, _ec, _message.c_str());
    return _formatted_message.c_str();
  }
private:
  nanots_ec_t _ec;
  std::string _message;
  mutable std::string _formatted_message;
  std::string _file;
  int _line;
};

#define FILE_HEADER_BLOCK_SIZE 65536
// 8 + 4 + 4 bytes (with padding)
#define BLOCK_HEADER_SIZE 16
// 8 + 8 bytes
#define INDEX_ENTRY_SIZE 16
// 16 + 1 + 4 bytes
#define FRAME_HEADER_SIZE 21
#define FRAME_UUID_OFFSET 0
#define FRAME_SIZE_OFFSET 16
#define FRAME_FLAGS_OFFSET 20

struct block_header {
  int64_t block_start_timestamp{0};
  uint32_t n_valid_indexes{0};
  uint32_t reserved{0};
};

struct index_entry {
  int64_t timestamp;
  uint32_t offset;
};

struct block {
  int64_t id{0};
  int64_t idx{0};
};

struct segment {
  int64_t id{0};
  std::string stream_tag;
  std::string metadata;
  int64_t sequence{0};
};

struct segment_block {
  int64_t id{0};
  int64_t segment_id{0};
  int64_t sequence{0};
  int64_t block_id{0};
  int64_t block_idx{0};
  int64_t start_timestamp{0};
  int64_t end_timestamp{0};
  uint8_t uuid[16];
};

struct write_context final {
  write_context() = default;
  write_context(const write_context&) = delete;
  write_context& operator=(const write_context&) = delete;
  write_context(write_context&& other) noexcept = default;
  write_context& operator=(write_context&& other) noexcept = default;

  ~write_context();

  std::string metadata;
  std::string stream_tag;
  std::optional<int64_t> last_timestamp;
  std::optional<segment> current_segment;
  std::optional<segment_block> current_block;
  nts_file file;
  nts_memory_map mm;
  std::string file_name;
};

class nanots_writer {
 public:
  nanots_writer(const std::string& file_name, bool auto_reclaim = false);
  nanots_writer(const nanots_writer&) = delete;
  nanots_writer(nanots_writer&&) = default;
  nanots_writer& operator=(const nanots_writer&) = delete;
  nanots_writer& operator=(nanots_writer&&) = default;
  ~nanots_writer() = default;

  write_context create_write_context(const std::string& stream_tag,
                                     const std::string& metadata);

  void write(write_context& wctx,
             const uint8_t* data,
             size_t size,
             int64_t timestamp,
             uint8_t flags);

  void free_blocks(const std::string& stream_tag,
                   int64_t start_timestamp,
                   int64_t end_timestamp);

  static void allocate(const std::string& file_name,
                       uint32_t block_size,
                       uint32_t n_blocks);

 private:
  std::string _file_name;
  uint64_t _file_size;
  nts_file _file;
  nts_memory_map _file_header_mm;
  uint8_t* _file_header_p;
  uint32_t _block_size;
  uint32_t _n_blocks;
  bool _auto_reclaim;
  std::set<std::string> _active_stream_tags;
};

struct contiguous_segment {
  int64_t segment_id{0};
  int64_t start_timestamp{0};
  int64_t end_timestamp{0};
};

class nanots_reader {
 public:
  nanots_reader(const std::string& file_name);
  nanots_reader(const nanots_reader&) = delete;
  nanots_reader(nanots_reader&&) = default;
  nanots_reader& operator=(const nanots_reader&) = delete;
  nanots_reader& operator=(nanots_reader&&) = default;
  ~nanots_reader() = default;

  void read(
      const std::string& stream_tag,
      int64_t start_timestamp,
      int64_t end_timestamp,
      const std::function<
          void(const uint8_t*, size_t, uint8_t, int64_t, int64_t)>& callback);
  
  std::vector<std::string> query_stream_tags(int64_t start_timestamp, int64_t end_timestamp);

  std::vector<contiguous_segment> query_contiguous_segments(
      const std::string& stream_tag,
      int64_t start_timestamp,
      int64_t end_timestamp);

 private:
  std::string _file_name;
  nts_file _file;
  uint32_t _block_size;
  uint32_t _n_blocks;
};

struct frame_info {
  const uint8_t* data{nullptr};
  size_t size{0};
  uint8_t flags{0};
  int64_t timestamp{0};
  int64_t block_sequence{0};
};

struct block_info {
  int64_t block_idx{0};
  int64_t block_sequence{0};
  std::string metadata;
  std::string uuid_hex;
  int64_t start_timestamp{0};
  int64_t end_timestamp{0};

  // Loaded block data
  nts_memory_map mm;
  uint8_t* block_p{nullptr};
  uint32_t n_valid_indexes{0};
  uint8_t uuid[16];
  bool is_loaded{false};
};

class nanots_iterator {
 public:
  nanots_iterator(const std::string& file_name, const std::string& stream_tag);
  nanots_iterator(const nanots_iterator&) = delete;
  nanots_iterator(nanots_iterator&&) = default;
  nanots_iterator& operator=(const nanots_iterator&) = delete;
  nanots_iterator& operator=(nanots_iterator&&) = default;
  ~nanots_iterator() = default;

  // Iterator interface
  bool valid() const { return _valid; }
  const frame_info& operator*() const { return _current_frame; }
  const frame_info* operator->() const { return &_current_frame; }

  // Navigation
  nanots_iterator& operator++();  // Move to next frame
  nanots_iterator& operator--();  // Move to previous frame
  bool find(int64_t timestamp);  // Find first frame >= timestamp
  void reset();                   // Go to first frame

  // Utility
  int64_t current_block_sequence() const { return _current_block_sequence; }

 private:
  block_info* _get_block_by_sequence(int64_t sequence);
  block_info* _get_first_block();
  block_info* _get_next_block(int64_t current_sequence);
  block_info* _get_prev_block(int64_t current_sequence);
  block_info* _find_block_for_timestamp(int64_t timestamp);

  bool _load_block_data(block_info& block);
  bool _load_current_frame();

  std::string _file_name;
  std::string _stream_tag;
  nts_file _file;
  uint32_t _block_size;

  // Current position
  int64_t _current_block_sequence;
  size_t _current_frame_idx;

  // Cache of visited blocks (sequence -> block_info)
  std::unordered_map<int64_t, block_info> _block_cache;

  // Cached current frame
  frame_info _current_frame;
  bool _valid;
  bool _initialized;
};

#ifdef __cplusplus
extern "C" {
#endif

typedef struct nanots_writer_handle* nanots_writer_t;
typedef struct nanots_write_context_handle* nanots_write_context_t;
typedef struct nanots_reader_handle* nanots_reader_t;
typedef struct nanots_iterator_handle* nanots_iterator_t;

typedef struct {
  int64_t segment_id;
  int64_t start_timestamp;
  int64_t end_timestamp;
} nanots_contiguous_segment_t;

typedef struct {
  const uint8_t* data;
  size_t size;
  uint8_t flags;
  int64_t timestamp;
  int64_t block_sequence;
} nanots_frame_info_t;

typedef void (*nanots_read_callback_t)(const uint8_t* data,
                                       size_t size,
                                       uint8_t flags,
                                       int64_t timestamp,
                                       int64_t block_sequence,
                                       void* user_data);

nanots_ec_t nanots_writer_allocate_file(const char* file_name, uint32_t block_size, uint32_t n_blocks);

// writer
nanots_writer_t nanots_writer_create(const char* file_name, int auto_reclaim);

void nanots_writer_destroy(nanots_writer_t writer);

nanots_write_context_t nanots_writer_create_context(nanots_writer_t writer,
                                                    const char* stream_tag,
                                                    const char* metadata);

void nanots_write_context_destroy(nanots_write_context_t context);

nanots_ec_t nanots_writer_write(nanots_writer_t writer,
                                    nanots_write_context_t context,
                                    const uint8_t* data,
                                    size_t size,
                                    int64_t timestamp,
                                    uint8_t flags);

nanots_ec_t nanots_writer_free_blocks(nanots_writer_t writer,
                                      const char* stream_tag,
                                      int64_t start_timestamp,
                                      int64_t end_timestamp);

// reader
nanots_reader_t nanots_reader_create(const char* file_name);

void nanots_reader_destroy(nanots_reader_t reader);

nanots_ec_t nanots_reader_read(nanots_reader_t reader,
                               const char* stream_tag,
                               int64_t start_timestamp,
                               int64_t end_timestamp,
                               nanots_read_callback_t callback,
                               void* user_data);

nanots_ec_t nanots_reader_query_contiguous_segments(
    nanots_reader_t reader,
    const char* stream_tag,
    int64_t start_timestamp,
    int64_t end_timestamp,
    nanots_contiguous_segment_t** segments,
    size_t* count);

void nanots_free_contiguous_segments(nanots_contiguous_segment_t* segments);

nanots_ec_t nanots_reader_query_stream_tags_start(nanots_reader_t reader,
                                                  int64_t start_timestamp,
                                                  int64_t end_timestamp);

const char* nanots_reader_query_stream_tags_next(nanots_reader_t reader);

// iterator
nanots_iterator_t nanots_iterator_create(const char* file_name,
                                         const char* stream_tag);
void nanots_iterator_destroy(nanots_iterator_t iterator);

int nanots_iterator_valid(nanots_iterator_t iterator);

nanots_ec_t nanots_iterator_get_current_frame(
    nanots_iterator_t iterator,
    nanots_frame_info_t* frame_info);

nanots_ec_t nanots_iterator_next(nanots_iterator_t iterator);

nanots_ec_t nanots_iterator_prev(nanots_iterator_t iterator);

nanots_ec_t nanots_iterator_find(nanots_iterator_t iterator,
                                     int64_t timestamp);

nanots_ec_t nanots_iterator_reset(nanots_iterator_t iterator);

int64_t nanots_iterator_current_block_sequence(nanots_iterator_t iterator);

#ifdef __cplusplus
}
#endif

#endif
