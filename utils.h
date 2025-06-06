
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
