// Amalgamated header file
// Generated automatically - do not edit

#ifndef NANOTS_AMALGAMATED_H
#define NANOTS_AMALGAMATED_H

#include "sqlite3.h"


#ifndef UTILS_H
#define UTILS_H

#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>


#ifdef _WIN32
#include <Rpc.h>
#include <Windows.h>
#include <io.h>

#define F_OK 0
#else
  #include <fcntl.h>
  #include <sys/mman.h>
  #include <sys/stat.h>
  #include <unistd.h>
#endif

// String utilities...
std::string format_s(const char* fmt, ...);
std::string format_s(const char* fmt, va_list& args);
std::string convert_utf16_string_to_multi_byte_string(const uint16_t* str);
std::string convert_utf16_string_to_multi_byte_string(const uint16_t* str,
                                                      size_t length);
std::vector<uint16_t> convert_multi_byte_string_to_utf16_string(
    const std::string& str);
std::string convert_utf32_string_to_multi_byte_string(const uint32_t* str);
std::string convert_utf32_string_to_multi_byte_string(const uint32_t* str,
                                                      size_t length);
std::vector<uint32_t> convert_multi_byte_string_to_utf32_string(
    const std::string& str);
std::string convert_wide_string_to_multi_byte_string(const wchar_t* str);
std::string convert_wide_string_to_multi_byte_string(const wchar_t* str,
                                                     size_t length);
std::wstring convert_multi_byte_string_to_wide_string(const std::string& str);

// SQLite raii
struct sqlite3;
struct sqlite3_stmt;
class nts_sqlite_stmt;

class nts_sqlite_conn final {
 public:
  nts_sqlite_conn(const std::string& fileName,
                  bool rw = true,
                  bool wal = false);
  nts_sqlite_conn(const nts_sqlite_conn&) = delete;
  nts_sqlite_conn(nts_sqlite_conn&& obj) noexcept;

  ~nts_sqlite_conn() noexcept;

  nts_sqlite_conn& operator=(const nts_sqlite_conn&) = delete;
  nts_sqlite_conn& operator=(nts_sqlite_conn&&) noexcept;

  std::vector<std::map<std::string, std::optional<std::string>>> exec(
      const std::string& query) const;

  std::string last_insert_id() const;

  nts_sqlite_stmt prepare(const std::string& query) const;

  friend class nts_sqlite_stmt;

 private:
  void _clear() noexcept;

  sqlite3* _db;
  bool _rw;
};

class nts_sqlite_stmt final {
 public:
  nts_sqlite_stmt(sqlite3* db, const std::string& query);
  nts_sqlite_stmt(const nts_sqlite_stmt&) = delete;
  nts_sqlite_stmt(nts_sqlite_stmt&& obj) noexcept;
  ~nts_sqlite_stmt() noexcept;

  nts_sqlite_stmt& operator=(const nts_sqlite_stmt&) = delete;
  nts_sqlite_stmt& operator=(nts_sqlite_stmt&&) noexcept;

  // Bind methods for different types
  nts_sqlite_stmt& bind(int index, int value);
  nts_sqlite_stmt& bind(int index, int64_t value);
  nts_sqlite_stmt& bind(int index, uint64_t value);
  nts_sqlite_stmt& bind(int index, double value);
  nts_sqlite_stmt& bind(int index, const std::string& value);
  nts_sqlite_stmt& bind(int index, const char* value);
  nts_sqlite_stmt& bind_null(int index);

  // Execute and get results
  std::vector<std::map<std::string, std::optional<std::string>>> exec();

  // Execute without expecting results (INSERT, UPDATE, DELETE)
  void exec_no_result();

  // Reset for reuse with different parameters
  void reset();

 private:
  void _clear() noexcept;

  sqlite3_stmt* _stmt;
  sqlite3* _db;
};

template <typename T>
void nts_sqlite_transaction(const nts_sqlite_conn& db, T t) {
  db.exec("BEGIN");
  try {
    t(db);
    db.exec("COMMIT");
  } catch (const std::exception& ex) {
    db.exec("ROLLBACK");
    throw ex;
  } catch (...) {
    db.exec("ROLLBACK");
    throw;
  }
}

// File raii
class nts_file final {
 public:
  nts_file() : _f(nullptr) {}
  nts_file(const nts_file&) = delete;
  nts_file(nts_file&& obj) noexcept : _f(std::move(obj._f)) {
    obj._f = nullptr;
  }
  ~nts_file() noexcept {
    if (_f)
      fclose(_f);
  }
  nts_file& operator=(nts_file&& obj) noexcept {
    if (_f)
      fclose(_f);
    _f = std::move(obj._f);
    obj._f = nullptr;
    return *this;
  }
  operator FILE*() const { return _f; }
  static nts_file open(const std::string& path, const std::string& mode) {
#ifdef _WIN32
#pragma warning(push)
#pragma warning(disable : 4996)
    nts_file obj;
    obj._f = _fsopen(path.c_str(), mode.c_str(), _SH_DENYNO);
    if (!obj._f)
      throw std::runtime_error("Unable to open: " + path);
    return obj;
#pragma warning(pop)
#else
    nts_file obj;
    obj._f = fopen(path.c_str(), mode.c_str());
    if (!obj._f)
      throw std::runtime_error("Unable to open: " + path);
    return obj;
#endif
  }
  void close() {
    if (_f) {
      fclose(_f);
      _f = nullptr;
    }
  }

 private:
  FILE* _f;
};

// File utilities
bool file_exists(const std::string& path);
int filenum(FILE* f);
uint64_t file_size(const std::string& fileName);
int fallocate(FILE* file, uint64_t size);
void remove_file(const std::string& path);

// returns pointer to first element between start and end which does not compare
// less than target
template <typename CMP>
uint8_t* lower_bound_bytes(uint8_t* start,
                           uint8_t* end,
                           uint8_t* target,
                           size_t elementSize,
                           CMP cmp) {
  size_t N = (end - start) / elementSize;
  size_t low = 0, high = N;

  while (low < high) {
    size_t mid = low + (high - low) / 2;
    uint8_t* mid_elem = start + mid * elementSize;

    // If mid_elem is >= target, move left
    if (cmp(mid_elem, target) >= 0)
      high = mid;
    else
      low = mid + 1;
  }

  return start + low * elementSize;
}

#ifdef _WIN32
#define FULL_MEM_BARRIER MemoryBarrier
#else
#define FULL_MEM_BARRIER __sync_synchronize
#endif

class nts_memory_map {
 public:
  enum Flags {
    NMM_TYPE_FILE = 0x01,
    NMM_TYPE_ANON = 0x02,
    NMM_SHARED = 0x04,
    NMM_PRIVATE = 0x08,
    NMM_FIXED = 0x10
  };

  enum Protection {
    NMM_PROT_NONE = 0x00,
    NMM_PROT_READ = 0x01,
    NMM_PROT_WRITE = 0x02,
    NMM_PROT_EXEC = 0x04
  };

  enum Advice {
    NMM_ADVICE_NORMAL = 0x00,
    NMM_ADVICE_RANDOM = 0x01,
    NMM_ADVICE_SEQUENTIAL = 0x02,
    NMM_ADVICE_WILLNEED = 0x04,
    NMM_ADVICE_DONTNEED = 0x08
  };

  nts_memory_map();

  nts_memory_map(int fd,
                 int64_t offset,
                 uint32_t len,
                 uint32_t prot,
                 uint32_t flags);

  nts_memory_map(const nts_memory_map&) = delete;

  nts_memory_map(nts_memory_map&& other)
      :
#ifdef _WIN32
        _fileHandle(std::move(other._fileHandle)),
        _mapHandle(std::move(other._mapHandle)),
#endif
        _mem(std::move(other._mem)),
        _length(std::move(other._length)) {
#ifdef _WIN32
    other._fileHandle = INVALID_HANDLE_VALUE;
    other._mapHandle = INVALID_HANDLE_VALUE;
#endif
    other._mem = nullptr;
    other._length = 0;
  }

  virtual ~nts_memory_map() noexcept;

  nts_memory_map& operator=(const nts_memory_map& other) = delete;

  nts_memory_map& operator=(nts_memory_map&& other) noexcept {
    if (this != &other) {
      _clear();

#ifdef _WIN32
      _fileHandle = std::move(other._fileHandle);
      other._fileHandle = INVALID_HANDLE_VALUE;
      _mapHandle = std::move(other._mapHandle);
      other._mapHandle = INVALID_HANDLE_VALUE;
#endif
      _mem = std::move(other._mem);
      other._mem = nullptr;
      _length = std::move(other._length);
      other._length = 0;
    }

    return *this;
  }

  inline void* map() const { return _mem; }

  inline uint32_t length() const { return _length; }

  inline bool mapped() const { return _mem != nullptr; }

  void advise(int advice, void* addr = nullptr, size_t length = 0) const;

  void flush(void* addr = nullptr, size_t length = 0, bool now = true);

 private:
  void _clear() noexcept;
#ifdef _WIN32
  int _GetWinProtFlags(int flags) const;
  int _GetWinAccessFlags(int flags) const;
#else
  int _GetPosixProtFlags(int prot) const;
  int _GetPosixAccessFlags(int flags) const;
  int _GetPosixAdvice(int advice) const;
#endif

#ifdef _WIN32
  HANDLE _fileHandle;
  HANDLE _mapHandle;
#endif
  void* _mem;
  uint32_t _length;
};

// UUID utilities
void generate_entropy_id(uint8_t* id);
std::string generate_entropy_id();
std::string entropy_id_to_s(const uint8_t* id);
void s_to_entropy_id(const std::string& idS, uint8_t* id);

#endif



#ifndef NANOTS_H
#define NANOTS_H


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
  int64_t block_start_timestamp{0};
  uint32_t n_valid_indexes{0};
  uint32_t reserved{0};
};

struct index_entry {
  int64_t timestamp;
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
};

struct contiguous_segment {
  int segment_id{0};
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
  int block_idx{0};
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
                                    int64_t timestamp,
                                    uint8_t flags);

nanots_result_t nanots_writer_free_blocks(nanots_writer_t writer,
                                          const char* stream_tag,
                                          int64_t start_timestamp,
                                          int64_t end_timestamp);

nanots_result_t nanots_writer_allocate_file(const char* file_name,
                                            uint32_t block_size,
                                            uint32_t n_blocks);

nanots_reader_t nanots_reader_create(const char* file_name);

void nanots_reader_destroy(nanots_reader_t reader);

nanots_result_t nanots_reader_read(nanots_reader_t reader,
                                   const char* stream_tag,
                                   int64_t start_timestamp,
                                   int64_t end_timestamp,
                                   nanots_read_callback_t callback,
                                   void* user_data);

nanots_result_t nanots_reader_query_contiguous_segments(
    nanots_reader_t reader,
    const char* stream_tag,
    int64_t start_timestamp,
    int64_t end_timestamp,
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
                                     int64_t timestamp);

nanots_result_t nanots_iterator_reset(nanots_iterator_t iterator);

int64_t nanots_iterator_current_block_sequence(nanots_iterator_t iterator);

#ifdef __cplusplus
}
#endif

#endif


#endif // NANOTS_AMALGAMATED_H