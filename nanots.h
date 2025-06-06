
#ifndef NANOTS_H
#define NANOTS_H

#include "utils.h"

// TODO: Keep track of active stream_tag's and disallow multiple writers for any
// single stream_tag.

#define FILE_HEADER_BLOCK_SIZE 65536
// 8 + 4 + 4 bytes (with padding)
#define BLOCK_HEADER_SIZE 16
// 8 + 4 bytes
#define INDEX_ENTRY_SIZE 12
// 16 + 1 + 4 bytes
#define FRAME_HEADER_SIZE 21
#define FRAME_UUID_OFFSET 0
#define FRAME_FLAGS_OFFSET 16
#define FRAME_SIZE_OFFSET 17

struct block_header {
  uint64_t block_start_ts{0};
  uint32_t n_valid_indexes{0};
  uint32_t reserved{0};
};

struct index_entry {
  uint64_t timestamp;
  uint32_t offset;
};

struct block {
  int id{0};
  int idx{0};
};

struct segment {
  int id{0};
  std::string stream_tag;
  std::string metadata;
  int sequence{0};
};

struct segment_block {
  int id{0};
  int segment_id{0};
  int sequence{0};
  int block_id{0};
  int block_idx{0};
  uint64_t start_ts{0};
  uint64_t end_ts{0};
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
  std::optional<uint64_t> last_ts;
  std::optional<segment> current_segment;
  std::optional<segment_block> current_block;
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
             uint64_t timestamp,
             uint8_t flags);

  void free_blocks(const std::string& stream_tag,
                   uint64_t start_ts,
                   uint64_t end_ts);

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
};

struct contiguous_segment {
  int segment_id{0};
  uint64_t start_ts{0};
  uint64_t end_ts{0};
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
      uint64_t start_ts,
      uint64_t end_ts,
      const std::function<
          void(const uint8_t*, size_t, uint8_t, uint64_t, uint64_t)>& callback);

  std::vector<contiguous_segment> query_contiguous_segments(
      const std::string& stream_tag,
      uint64_t start_ts,
      uint64_t end_ts);

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
  uint64_t timestamp{0};
  uint64_t block_sequence{0};
};

struct block_info {
  int block_idx{0};
  uint64_t block_sequence{0};
  std::string metadata;
  std::string uuid_hex;
  uint64_t start_ts{0};
  uint64_t end_ts{0};

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
  bool find(uint64_t timestamp);  // Find first frame >= timestamp
  void reset();                   // Go to first frame

  // Utility
  uint64_t current_block_sequence() const { return _current_block_sequence; }

 private:
  block_info* _get_block_by_sequence(uint64_t sequence);
  block_info* _get_first_block();
  block_info* _get_next_block(uint64_t current_sequence);
  block_info* _get_prev_block(uint64_t current_sequence);
  block_info* _find_block_for_timestamp(uint64_t timestamp);

  bool _load_block_data(block_info& block);
  bool _load_current_frame();

  std::string _file_name;
  std::string _stream_tag;
  nts_file _file;
  uint32_t _block_size;

  // Current position
  uint64_t _current_block_sequence;
  size_t _current_frame_idx;

  // Cache of visited blocks (sequence -> block_info)
  std::unordered_map<uint64_t, block_info> _block_cache;

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

typedef enum {
  NANOTS_OK = 0,
  NANOTS_ERROR = -1,
  NANOTS_INVALID_HANDLE = -2,
  NANOTS_INVALID_TIMESTAMP = -3,
  NANOTS_FRAME_TOO_LARGE = -4,
  NANOTS_NO_FREE_BLOCKS = -5
} nanots_result_t;

typedef struct {
  int segment_id;
  uint64_t start_ts;
  uint64_t end_ts;
} nanots_contiguous_segment_t;

typedef struct {
  const uint8_t* data;
  size_t size;
  uint8_t flags;
  uint64_t timestamp;
  uint64_t block_sequence;
} nanots_frame_info_t;

typedef void (*nanots_read_callback_t)(const uint8_t* data,
                                       size_t size,
                                       uint8_t flags,
                                       uint64_t timestamp,
                                       uint64_t block_sequence,
                                       void* user_data);

nanots_writer_t nanots_writer_create(const char* file_name, int auto_reclaim);

void nanots_writer_destroy(nanots_writer_t writer);

nanots_write_context_t nanots_writer_create_context(nanots_writer_t writer,
                                                    const char* stream_tag,
                                                    const char* metadata);

void nanots_write_context_destroy(nanots_write_context_t context);

nanots_result_t nanots_writer_write(nanots_writer_t writer,
                                    nanots_write_context_t context,
                                    const uint8_t* data,
                                    size_t size,
                                    uint64_t timestamp,
                                    uint8_t flags);

nanots_result_t nanots_writer_free_blocks(nanots_writer_t writer,
                                          const char* stream_tag,
                                          uint64_t start_ts,
                                          uint64_t end_ts);

nanots_result_t nanots_writer_allocate_file(const char* file_name,
                                            uint32_t block_size,
                                            uint32_t n_blocks);

nanots_reader_t nanots_reader_create(const char* file_name);

void nanots_reader_destroy(nanots_reader_t reader);

nanots_result_t nanots_reader_read(nanots_reader_t reader,
                                   const char* stream_tag,
                                   uint64_t start_ts,
                                   uint64_t end_ts,
                                   nanots_read_callback_t callback,
                                   void* user_data);

nanots_result_t nanots_reader_query_contiguous_segments(
    nanots_reader_t reader,
    const char* stream_tag,
    uint64_t start_ts,
    uint64_t end_ts,
    nanots_contiguous_segment_t** segments,
    size_t* count);

void nanots_free_segments(nanots_contiguous_segment_t* segments);

nanots_iterator_t nanots_iterator_create(const char* file_name,
                                         const char* stream_tag);
void nanots_iterator_destroy(nanots_iterator_t iterator);

int nanots_iterator_valid(nanots_iterator_t iterator);

nanots_result_t nanots_iterator_get_current_frame(
    nanots_iterator_t iterator,
    nanots_frame_info_t* frame_info);

nanots_result_t nanots_iterator_next(nanots_iterator_t iterator);

nanots_result_t nanots_iterator_prev(nanots_iterator_t iterator);

nanots_result_t nanots_iterator_find(nanots_iterator_t iterator,
                                     uint64_t timestamp);

nanots_result_t nanots_iterator_reset(nanots_iterator_t iterator);

uint64_t nanots_iterator_current_block_sequence(nanots_iterator_t iterator);

#ifdef __cplusplus
}
#endif

#endif
