// Amalgamated C++ source file
// Generated automatically - do not edit

#include "nanots.h"

// Utils implementation


std::string format_s(const char* fmt, ...) {
  va_list args;
  va_start(args, fmt);
  const std::string result = format_s(fmt, args);
  va_end(args);
  return result;
}

std::string format_s(const char* fmt, va_list& args) {
  va_list newargs;
  va_copy(newargs, args);

  int chars_written = vsnprintf(nullptr, 0, fmt, newargs);
  int len = chars_written + 1;

  std::vector<char> str(len);

  va_end(newargs);

  va_copy(newargs, args);
  vsnprintf(&str[0], len, fmt, newargs);

  va_end(newargs);

  return std::string(&str[0]);
}

std::string convert_utf16_string_to_multi_byte_string(const uint16_t* str) {
  return convert_utf16_string_to_multi_byte_string(str, (size_t)-1);
}

std::string convert_utf16_string_to_multi_byte_string(const uint16_t* str,
                                                      size_t length) {
  std::string out;
  if (str == NULL)
    return out;
  unsigned int codepoint = 0;
  for (size_t i = 0; i < length && *str != 0; ++i, ++str) {
    if (*str >= 0xd800 && *str <= 0xdbff)
      codepoint = ((*str - 0xd800) << 10) + 0x10000;
    else {
      if (*str >= 0xdc00 && *str <= 0xdfff)
        codepoint |= *str - 0xdc00;
      else
        codepoint = *str;

      if (codepoint <= 0x7f)
        out.append(1, static_cast<char>(codepoint));
      else if (codepoint <= 0x7ff) {
        out.append(1, static_cast<char>(0xc0 | ((codepoint >> 6) & 0x1f)));
        out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
      } else if (codepoint <= 0xffff) {
        out.append(1, static_cast<char>(0xe0 | ((codepoint >> 12) & 0x0f)));
        out.append(1, static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
        out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
      } else {
        out.append(1, static_cast<char>(0xf0 | ((codepoint >> 18) & 0x07)));
        out.append(1, static_cast<char>(0x80 | ((codepoint >> 12) & 0x3f)));
        out.append(1, static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
        out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
      }
      codepoint = 0;
    }
  }
  return out;
}

std::vector<uint16_t> convert_multi_byte_string_to_utf16_string(
    const std::string& str) {
  std::vector<uint16_t> out;
  if (str.empty())
    return out;
  char* place = const_cast<char*>(str.c_str());
  unsigned int codepoint = 0;
  int following = 0;
  for (; *place != 0; ++place) {
    unsigned char ch = *place;
    if (ch <= 0x7f) {
      codepoint = ch;
      following = 0;
    } else if (ch <= 0xbf) {
      if (following > 0) {
        codepoint = (codepoint << 6) | (ch & 0x3f);
        --following;
      }
    } else if (ch <= 0xdf) {
      codepoint = ch & 0x1f;
      following = 1;
    } else if (ch <= 0xef) {
      codepoint = ch & 0x0f;
      following = 2;
    } else {
      codepoint = ch & 0x07;
      following = 3;
    }
    if (following == 0) {
      if (codepoint > 0xffff) {
        out.push_back(static_cast<wchar_t>(0xd800 + (codepoint >> 10)));
        out.push_back(static_cast<wchar_t>(0xdc00 + (codepoint & 0x03ff)));
      } else
        out.push_back(static_cast<wchar_t>(codepoint));
      codepoint = 0;
    }
  }
  return out;
}

std::string convert_utf32_string_to_multi_byte_string(const uint32_t* str) {
  return convert_utf32_string_to_multi_byte_string(str, (size_t)-1);
}

std::string convert_utf32_string_to_multi_byte_string(const uint32_t* str,
                                                      size_t length) {
  std::string out;
  if (str == NULL)
    return out;

  size_t i = 0;
  for (wchar_t* temp = (wchar_t*)str; i < length && *temp != 0; ++temp, ++i) {
    unsigned int codepoint = *temp;

    if (codepoint <= 0x7f)
      out.append(1, static_cast<char>(codepoint));
    else if (codepoint <= 0x7ff) {
      out.append(1, static_cast<char>(0xc0 | ((codepoint >> 6) & 0x1f)));
      out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
    } else if (codepoint <= 0xffff) {
      out.append(1, static_cast<char>(0xe0 | ((codepoint >> 12) & 0x0f)));
      out.append(1, static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
      out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
    } else {
      out.append(1, static_cast<char>(0xf0 | ((codepoint >> 18) & 0x07)));
      out.append(1, static_cast<char>(0x80 | ((codepoint >> 12) & 0x3f)));
      out.append(1, static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
      out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
    }
  }
  return out;
}

std::vector<uint32_t> convert_multi_byte_string_to_utf32_string(
    const std::string& str) {
  std::vector<uint32_t> out;

  wchar_t codepoint = 0;
  int following = 0;
  for (char* temp = const_cast<char*>(str.c_str()); *temp != 0; ++temp) {
    unsigned char ch = *temp;
    if (ch <= 0x7f) {
      codepoint = ch;
      following = 0;
    } else if (ch <= 0xbf) {
      if (following > 0) {
        codepoint = (codepoint << 6) | (ch & 0x3f);
        --following;
      }
    } else if (ch <= 0xdf) {
      codepoint = ch & 0x1f;
      following = 1;
    } else if (ch <= 0xef) {
      codepoint = ch & 0x0f;
      following = 2;
    } else {
      codepoint = ch & 0x07;
      following = 3;
    }
    if (following == 0) {
      out.push_back(codepoint);
      codepoint = 0;
    }
  }
  return out;
}

std::string convert_wide_string_to_multi_byte_string(const wchar_t* str) {
#ifdef _WIN32
  std::string result(convert_utf16_string_to_multi_byte_string((uint16_t*)str));
#else
  std::string result(convert_utf32_string_to_multi_byte_string((uint32_t*)str));
#endif
  return result;
}

std::string convert_wide_string_to_multi_byte_string(const wchar_t* str,
                                                     size_t length) {
#ifdef _WIN32
  std::string result(
      convert_utf16_string_to_multi_byte_string((uint16_t*)str, length));
#else
  std::string result(
      convert_utf32_string_to_multi_byte_string((uint32_t*)str, length));
#endif
  return result;
}

std::wstring convert_multi_byte_string_to_wide_string(const std::string& str) {
#ifdef _WIN32
  std::vector<uint16_t> converted =
      convert_multi_byte_string_to_utf16_string(str);
  std::wstring result(converted.begin(), converted.end());
#else
  std::vector<uint32_t> converted =
      convert_multi_byte_string_to_utf32_string(str);
  std::wstring result(converted.begin(), converted.end());
#endif
  return result;
}

static const int DEFAULT_NUM_OPEN_RETRIES = 5;
static const int BASE_SLEEP_MICROS = 500000;
static const int BUSY_TIMEOUT_MILLIS = 2000;

nts_sqlite_conn::nts_sqlite_conn(const std::string& fileName, bool rw, bool wal)
    : _db(nullptr), _rw(rw) {
  int numRetries = DEFAULT_NUM_OPEN_RETRIES;
  int ret = 0;
  while (numRetries > 0) {
    int flags = SQLITE_OPEN_NOMUTEX;
    if (_rw) {
      flags |= SQLITE_OPEN_READWRITE |
               SQLITE_OPEN_CREATE;  // Only add CREATE for R/W
    } else {
      flags |= SQLITE_OPEN_READONLY;  // No CREATE for read-only
    }

    // ret = sqlite3_open_v2(fileName.c_str(), &_db, flags,
    // (embeddedvfs)?"embedded":nullptr);
    ret = sqlite3_open_v2(fileName.c_str(), &_db, flags, nullptr);
    if (ret == SQLITE_OK) {
      sqlite3_busy_timeout(_db, BUSY_TIMEOUT_MILLIS);

      if (wal)
        exec("PRAGMA journal_mode=WAL;");

      return;
    }
    if (_db != nullptr)
      _clear();
    std::this_thread::sleep_for(std::chrono::microseconds(
        ((DEFAULT_NUM_OPEN_RETRIES - numRetries) + 1) * BASE_SLEEP_MICROS));
    --numRetries;
  }

  throw std::runtime_error("Unable to open SQLite database.");
}

nts_sqlite_conn::nts_sqlite_conn(nts_sqlite_conn&& obj) noexcept
    : _db(std::move(obj._db)), _rw(std::move(obj._rw)) {
  obj._db = nullptr;
  obj._rw = false;
}

nts_sqlite_conn::~nts_sqlite_conn() noexcept {
  _clear();
}

nts_sqlite_conn& nts_sqlite_conn::operator=(nts_sqlite_conn&& obj) noexcept {
  _clear();

  _db = std::move(obj._db);
  obj._db = nullptr;

  _rw = std::move(obj._rw);
  obj._rw = false;

  return *this;
}

std::vector<std::map<std::string, std::optional<std::string>>>
nts_sqlite_conn::exec(const std::string& query) const {
  std::vector<std::map<std::string, std::optional<std::string>>> results;

  sqlite3_stmt* stmt = nullptr;

  int rc = sqlite3_prepare_v3(_db, query.c_str(), (int)query.length(), 0, &stmt,
                              nullptr);
  if (rc != SQLITE_OK)
    throw std::runtime_error(format_s("sqlite3_prepare_v2(%s) failed with: %s",
                                      query.c_str(), sqlite3_errmsg(_db)));
  if (stmt == NULL)
    throw std::runtime_error(
        "sqlite3_prepare_v2() succeeded but returned NULL statement.");

  try {
    bool done = false;
    while (!done) {
      rc = sqlite3_step(stmt);

      if (rc == SQLITE_DONE)
        done = true;
      else if (rc == SQLITE_ROW) {
        int columnCount = sqlite3_column_count(stmt);

        std::map<std::string, std::optional<std::string>> row;

        for (int i = 0; i < columnCount; ++i) {
          std::optional<std::string> val;

          switch (sqlite3_column_type(stmt, i)) {
            case SQLITE_INTEGER:
              val = std::to_string(sqlite3_column_int64(stmt, i));
              break;
            case SQLITE_FLOAT:
              val = std::to_string(sqlite3_column_double(stmt, i));
              break;
            case SQLITE_NULL:
              break;
            case SQLITE_TEXT:
            default: {
              const char* tp = (const char*)sqlite3_column_text(stmt, i);
              if (tp && (*tp != '\0'))
                val = std::string(tp);
            } break;
          }

          row[sqlite3_column_name(stmt, i)] = val;
        }

        results.push_back(row);
      } else {
        throw std::runtime_error(format_s("Query (%s) to db failed. Cause: %s",
                                          query.c_str(), sqlite3_errmsg(_db)));
      }
    }

    sqlite3_finalize(stmt);
  } catch (...) {
    sqlite3_finalize(stmt);
    throw;
  }

  return results;
}

std::string nts_sqlite_conn::last_insert_id() const {
  if (!_db)
    throw std::runtime_error(
        "Cannot last_insert_id() on moved out instance of nts_sqlite_conn.");

  return std::to_string(sqlite3_last_insert_rowid(_db));
}

nts_sqlite_stmt nts_sqlite_conn::prepare(const std::string& query) const {
  return nts_sqlite_stmt(_db, query);
}

void nts_sqlite_conn::_clear() noexcept {
  if (_db) {
    sqlite3_close(_db);
    _db = nullptr;
  }
}

nts_sqlite_stmt::nts_sqlite_stmt(sqlite3* db, const std::string& query)
    : _stmt(nullptr), _db(db) {
  int rc = sqlite3_prepare_v2(_db, query.c_str(), (int)query.length(), &_stmt,
                              nullptr);
  if (rc != SQLITE_OK)
    throw std::runtime_error(format_s("sqlite3_prepare_v2(%s) failed with: %s",
                                      query.c_str(), sqlite3_errmsg(_db)));
  if (_stmt == nullptr)
    throw std::runtime_error(
        "sqlite3_prepare_v2() succeeded but returned NULL statement.");
}

nts_sqlite_stmt::nts_sqlite_stmt(nts_sqlite_stmt&& obj) noexcept
    : _stmt(std::move(obj._stmt)), _db(std::move(obj._db)) {
  obj._stmt = nullptr;
  obj._db = nullptr;
}

nts_sqlite_stmt::~nts_sqlite_stmt() noexcept {
  _clear();
}

nts_sqlite_stmt& nts_sqlite_stmt::operator=(nts_sqlite_stmt&& obj) noexcept {
  _clear();

  _stmt = std::move(obj._stmt);
  obj._stmt = nullptr;

  _db = std::move(obj._db);
  obj._db = nullptr;

  return *this;
}

nts_sqlite_stmt& nts_sqlite_stmt::bind(int index, int value) {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot bind() on moved out instance of nts_sqlite_stmt.");

  int rc = sqlite3_bind_int(_stmt, index, value);
  if (rc != SQLITE_OK)
    throw std::runtime_error(
        format_s("sqlite3_bind_int() failed with: %s", sqlite3_errmsg(_db)));

  return *this;
}

nts_sqlite_stmt& nts_sqlite_stmt::bind(int index, int64_t value) {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot bind() on moved out instance of nts_sqlite_stmt.");

  int rc = sqlite3_bind_int64(_stmt, index, value);
  if (rc != SQLITE_OK)
    throw std::runtime_error(
        format_s("sqlite3_bind_int64() failed with: %s", sqlite3_errmsg(_db)));

  return *this;
}

nts_sqlite_stmt& nts_sqlite_stmt::bind(int index, uint64_t value) {
  // Cast to int64_t since SQLite doesn't have unsigned 64-bit
  return bind(index, static_cast<int64_t>(value));
}

nts_sqlite_stmt& nts_sqlite_stmt::bind(int index, double value) {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot bind() on moved out instance of nts_sqlite_stmt.");

  int rc = sqlite3_bind_double(_stmt, index, value);
  if (rc != SQLITE_OK)
    throw std::runtime_error(
        format_s("sqlite3_bind_double() failed with: %s", sqlite3_errmsg(_db)));

  return *this;
}

nts_sqlite_stmt& nts_sqlite_stmt::bind(int index, const std::string& value) {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot bind() on moved out instance of nts_sqlite_stmt.");

  // SQLITE_TRANSIENT makes SQLite copy the string
  int rc = sqlite3_bind_text(_stmt, index, value.c_str(), (int)value.length(),
                             SQLITE_TRANSIENT);
  if (rc != SQLITE_OK)
    throw std::runtime_error(
        format_s("sqlite3_bind_text() failed with: %s", sqlite3_errmsg(_db)));

  return *this;
}

nts_sqlite_stmt& nts_sqlite_stmt::bind(int index, const char* value) {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot bind() on moved out instance of nts_sqlite_stmt.");

  if (value == nullptr)
    return bind_null(index);

  int rc = sqlite3_bind_text(_stmt, index, value, -1, SQLITE_TRANSIENT);
  if (rc != SQLITE_OK)
    throw std::runtime_error(
        format_s("sqlite3_bind_text() failed with: %s", sqlite3_errmsg(_db)));

  return *this;
}

nts_sqlite_stmt& nts_sqlite_stmt::bind_null(int index) {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot bind_null() on moved out instance of nts_sqlite_stmt.");

  int rc = sqlite3_bind_null(_stmt, index);
  if (rc != SQLITE_OK)
    throw std::runtime_error(
        format_s("sqlite3_bind_null() failed with: %s", sqlite3_errmsg(_db)));

  return *this;
}

std::vector<std::map<std::string, std::optional<std::string>>>
nts_sqlite_stmt::exec() {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot exec() on moved out instance of nts_sqlite_stmt.");

  std::vector<std::map<std::string, std::optional<std::string>>> results;

  bool done = false;
  while (!done) {
    int rc = sqlite3_step(_stmt);

    if (rc == SQLITE_DONE)
      done = true;
    else if (rc == SQLITE_ROW) {
      int columnCount = sqlite3_column_count(_stmt);

      std::map<std::string, std::optional<std::string>> row;

      for (int i = 0; i < columnCount; ++i) {
        std::optional<std::string> val;

        switch (sqlite3_column_type(_stmt, i)) {
          case SQLITE_INTEGER:
            val = std::to_string(sqlite3_column_int64(_stmt, i));
            break;
          case SQLITE_FLOAT:
            val = std::to_string(sqlite3_column_double(_stmt, i));
            break;
          case SQLITE_NULL:
            break;
          case SQLITE_TEXT:
          default: {
            const char* tp = (const char*)sqlite3_column_text(_stmt, i);
            if (tp && (*tp != '\0'))
              val = std::string(tp);
          } break;
        }

        row[sqlite3_column_name(_stmt, i)] = val;
      }

      results.push_back(row);
    } else {
      throw std::runtime_error(
          format_s("Statement execution failed: %s", sqlite3_errmsg(_db)));
    }
  }

  return results;
}

void nts_sqlite_stmt::exec_no_result() {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot exec_no_result() on moved out instance of nts_sqlite_stmt.");

  int rc = sqlite3_step(_stmt);
  if (rc != SQLITE_DONE)
    throw std::runtime_error(
        format_s("Statement execution failed: %s", sqlite3_errmsg(_db)));
}

void nts_sqlite_stmt::reset() {
  if (!_stmt)
    throw std::runtime_error(
        "Cannot reset() on moved out instance of nts_sqlite_stmt.");

  sqlite3_reset(_stmt);
  sqlite3_clear_bindings(_stmt);
}

void nts_sqlite_stmt::_clear() noexcept {
  if (_stmt) {
    sqlite3_finalize(_stmt);
    _stmt = nullptr;
  }
  _db = nullptr;
}

bool file_exists(const std::string& path) {
#ifdef _WIN32
  return (_access(path.c_str(), F_OK) == 0);
#else
  return (access(path.c_str(), F_OK) == 0);
#endif
}

int filenum(FILE* f) {
#ifdef _WIN32
  return _fileno(f);
#else
  return ::fileno(f);
#endif
}

uint64_t file_size(const std::string& fileName) {
#ifdef _WIN32
  struct __stat64 sfi;
  if (_wstat64(convert_multi_byte_string_to_wide_string(fileName).data(),
               &sfi) == 0)
    return sfi.st_size;
  throw std::runtime_error("Unable to stat: " + fileName);
#else
  struct stat sfi;
  if (::stat(fileName.c_str(), &sfi) == 0)
    return sfi.st_size;
  throw std::runtime_error("Unable to stat: " + fileName);
#endif
}

int fallocate(FILE* file, uint64_t size) {
#ifdef _WIN32
  LARGE_INTEGER li;
  li.QuadPart = size;
  BOOL ok = SetFilePointerEx((HANDLE)_get_osfhandle(filenum(file)), li,
                                nullptr, FILE_BEGIN);
  if (!ok)
    return -1;

  if (!SetEndOfFile((HANDLE)_get_osfhandle(filenum(file))))
    return -1;

  return 0;
#elif defined(__APPLE__)
  // macOS: Use fcntl with F_PREALLOCATE for actual space allocation
  fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, (off_t)size, 0};
  int fd = filenum(file);
  
  // Try contiguous allocation first
  int result = fcntl(fd, F_PREALLOCATE, &store);
  if (result == -1) {
    // Fall back to non-contiguous allocation
    store.fst_flags = F_ALLOCATEALL;
    result = fcntl(fd, F_PREALLOCATE, &store);
  }
  
  if (result == -1)
    return -1;
    
  // Set the file size
  return ftruncate(fd, size);
#else
  return posix_fallocate64(filenum(file), 0, size);
#endif
}

void remove_file(const std::string& path) {
#ifdef _WIN32
  if (DeleteFileA(path.c_str()) == 0)
    throw std::runtime_error("Unable to remove file: " + path);
#else
  if (unlink(path.c_str()) != 0)
    throw std::runtime_error("Unable to remove file: " + path);
#endif
}

static const uint32_t MAX_MAPPING_LEN = 1048576000;

nts_memory_map::nts_memory_map()
    :
#ifdef _WIN32
      _fileHandle(INVALID_HANDLE_VALUE),
      _mapHandle(INVALID_HANDLE_VALUE),
#endif
      _mem(nullptr),
      _length(0) {
}

nts_memory_map::nts_memory_map(int fd,
                               int64_t offset,
                               uint32_t len,
                               uint32_t prot,
                               uint32_t flags)
    :
#ifdef _WIN32
      _fileHandle(INVALID_HANDLE_VALUE),
      _mapHandle(INVALID_HANDLE_VALUE),
#endif
      _mem(NULL),
      _length(len) {
  if (fd <= 0)
    throw std::runtime_error("Attempting to memory map a bad file descriptor.");

  if ((len == 0) || (len > MAX_MAPPING_LEN))
    throw std::runtime_error(
        "Attempting to memory map more than 1gb is invalid.");

  if (!(flags & NMM_TYPE_FILE) && !(flags & NMM_TYPE_ANON))
    throw std::runtime_error(
        "A mapping must be either a file mapping, or an "
        "anonymous mapping (neither was specified).");

  if (flags & NMM_FIXED)
    throw std::runtime_error("nts_memory_map does not support fixed mappings.");

#ifdef _WIN32
  int protFlags = _GetWinProtFlags(prot);
  int accessFlags = _GetWinAccessFlags(prot);

  if (fd != -1)
    _fileHandle = (HANDLE)_get_osfhandle(fd);

  if (_fileHandle == INVALID_HANDLE_VALUE) {
    if (!(flags & NMM_TYPE_ANON))
      throw std::runtime_error(
          "An invalid fd was passed and this is not an anonymous mapping.");
  } else {
    if (!DuplicateHandle(GetCurrentProcess(), _fileHandle, GetCurrentProcess(),
                         &_fileHandle, 0, FALSE, DUPLICATE_SAME_ACCESS))
      throw std::runtime_error(
          "Unable to duplicate the provided fd file handle.");

    _mapHandle = CreateFileMapping(_fileHandle, NULL, protFlags, 0, 0, NULL);
    if (_mapHandle == 0)
      throw std::runtime_error("Unable to create file mapping");

    uint64_t ofs = (uint64_t)offset;

    _mem = MapViewOfFile(_mapHandle, accessFlags, (DWORD)(ofs >> 32),
                         (DWORD)(ofs & 0x00000000FFFFFFFF), len);
    if (_mem == NULL) {
      DWORD lastError = GetLastError();
      throw std::runtime_error(
          format_s("Unable to complete file mapping: %lu", lastError));
    }
  }
#else
  _mem = mmap(NULL, _length, _GetPosixProtFlags(prot),
              _GetPosixAccessFlags(flags), fd, offset);

  if (_mem == MAP_FAILED)
    throw std::runtime_error("Unable to complete file mapping");
#endif
}

nts_memory_map::~nts_memory_map() noexcept {
  _clear();
}

void nts_memory_map::advise(int advice, void* addr, size_t length) const {
#ifndef _WIN32
  int posixAdvice = _GetPosixAdvice(advice);

  int err = madvise((addr) ? addr : _mem, (length > 0) ? length : _length,
                    posixAdvice);

  if (err != 0)
    throw std::runtime_error("Unable to apply memory mapping advice.");
#endif
}

void nts_memory_map::flush(void* addr, size_t length, bool now) {
#ifndef _WIN32
  int err = msync((addr) ? addr : _mem, (length > 0) ? length : _length,
                  (now) ? MS_SYNC : MS_ASYNC);

  if (err != 0)
    throw std::runtime_error("Unable to sync memory mapped file.");
#else
  void*  flush_addr = (addr)   ? addr   : _mem;
  size_t flush_len  = (length) ? length : _length;

  if (!FlushViewOfFile(flush_addr, flush_len))
    throw std::runtime_error("Unable to sync memory mapped file.");

  if (now) {
    // NtFlushVirtualMemory is the Windows equivalent of msync(MS_SYNC): it
    // syncs only the dirty pages in [flush_addr, flush_addr+flush_len] to
    // durable storage, unlike FlushFileBuffers which flushes the entire file.
    using PFN_NtFlushVirtualMemory = NTSTATUS(NTAPI*)(HANDLE, PVOID*, PSIZE_T,
                                                      PIO_STATUS_BLOCK);
    static const auto nt_flush_vm = []() -> PFN_NtFlushVirtualMemory {
      HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
      if (!ntdll) return nullptr;
      return reinterpret_cast<PFN_NtFlushVirtualMemory>(
          GetProcAddress(ntdll, "NtFlushVirtualMemory"));
    }();

    if (nt_flush_vm) {
      IO_STATUS_BLOCK iosb{};
      PVOID  base = flush_addr;
      SIZE_T size = flush_len;
      nt_flush_vm(GetCurrentProcess(), &base, &size, &iosb);
    } else {
      if (!FlushFileBuffers(_fileHandle))
        throw std::runtime_error("Unable to flush file handle.");
    }
  }
#endif
}

void nts_memory_map::_clear() noexcept {
#ifdef _WIN32
  if (_mem != nullptr) {
    UnmapViewOfFile(_mem);
    _mem = nullptr;
  }
  if (_mapHandle != INVALID_HANDLE_VALUE) {
    CloseHandle(_mapHandle);
    _mapHandle = INVALID_HANDLE_VALUE;
  }
  if (_fileHandle != INVALID_HANDLE_VALUE) {
    CloseHandle(_fileHandle);
    _fileHandle = INVALID_HANDLE_VALUE;
  }
#else
  if (_mem != nullptr) {
    munmap(_mem, _length);
    _mem = nullptr;
  }
#endif
}

#ifdef _WIN32

int nts_memory_map::_GetWinProtFlags(int flags) const {
  int prot = 0;

  if (flags & NMM_PROT_READ) {
    if (flags & NMM_PROT_WRITE)
      prot = (flags & NMM_PROT_EXEC) ? PAGE_EXECUTE_READWRITE : PAGE_READWRITE;
    else
      prot = (flags & NMM_PROT_EXEC) ? PAGE_EXECUTE_READ : PAGE_READONLY;
  } else if (flags & NMM_PROT_WRITE)
    prot = (flags & NMM_PROT_EXEC) ? PAGE_EXECUTE_READ : PAGE_WRITECOPY;
  else if (flags & NMM_PROT_EXEC)
    prot = PAGE_EXECUTE_READ;

  return prot;
}

int nts_memory_map::_GetWinAccessFlags(int flags) const {
  int access = 0;

  if (flags & NMM_PROT_READ) {
    if (flags & NMM_PROT_WRITE)
      access = FILE_MAP_WRITE;
    else
      access = (flags & NMM_PROT_EXEC) ? FILE_MAP_EXECUTE : FILE_MAP_READ;
  } else if (flags & NMM_PROT_WRITE)
    access = FILE_MAP_COPY;
  else if (flags & NMM_PROT_EXEC)
    access = FILE_MAP_EXECUTE;

  return access;
}

#else

int nts_memory_map::_GetPosixProtFlags(int prot) const {
  int osProtFlags = 0;

  if (prot & NMM_PROT_READ)
    osProtFlags |= PROT_READ;
  if (prot & NMM_PROT_WRITE)
    osProtFlags |= PROT_WRITE;
  if (prot & NMM_PROT_EXEC)
    osProtFlags |= PROT_EXEC;

  return osProtFlags;
}

int nts_memory_map::_GetPosixAccessFlags(int flags) const {
  int osFlags = 0;

  if (flags & NMM_TYPE_FILE)
    osFlags |= MAP_FILE;
  if (flags & NMM_TYPE_ANON)
    osFlags |= MAP_ANONYMOUS;
  if (flags & NMM_SHARED)
    osFlags |= MAP_SHARED;
  if (flags & NMM_PRIVATE)
    osFlags |= MAP_PRIVATE;
  if (flags & NMM_FIXED)
    osFlags |= MAP_FIXED;

  return osFlags;
}

int nts_memory_map::_GetPosixAdvice(int advice) const {
  int posixAdvice = 0;

  if (advice & NMM_ADVICE_RANDOM)
    posixAdvice |= MADV_RANDOM;
  if (advice & NMM_ADVICE_SEQUENTIAL)
    posixAdvice |= MADV_SEQUENTIAL;
  if (advice & NMM_ADVICE_WILLNEED)
    posixAdvice |= MADV_WILLNEED;
  if (advice & NMM_ADVICE_DONTNEED)
    posixAdvice |= MADV_DONTNEED;

  return posixAdvice;
}

#endif

void generate_entropy_id(uint8_t* id) {
  thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<unsigned int> dis(0, 255);

  // Generate 16 random bytes (128 bits of entropy)
  for (int i = 0; i < 16; i++)
    id[i] = static_cast<uint8_t>(dis(gen));
}

std::string generate_entropy_id() {
  uint8_t id[16];
  generate_entropy_id(id);
  return entropy_id_to_s(id);
}

std::string entropy_id_to_s(const uint8_t* id) {
  std::stringstream ss;
  ss << std::hex << std::setfill('0');

  // Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  for (int i = 0; i < 4; i++)
    ss << std::setw(2) << static_cast<int>(id[i]);
  ss << "-";
  for (int i = 4; i < 6; i++)
    ss << std::setw(2) << static_cast<int>(id[i]);
  ss << "-";
  for (int i = 6; i < 8; i++)
    ss << std::setw(2) << static_cast<int>(id[i]);
  ss << "-";
  for (int i = 8; i < 10; i++)
    ss << std::setw(2) << static_cast<int>(id[i]);
  ss << "-";
  for (int i = 10; i < 16; i++)
    ss << std::setw(2) << static_cast<int>(id[i]);

  return ss.str();
}

void s_to_entropy_id(const std::string& idS, uint8_t* id) {
  // Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  std::string cleaned = idS;

  // Remove dashes
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '-'),
                cleaned.end());

  // Convert hex string to bytes
  for (int i = 0; i < 16 && i * 2 < (int)cleaned.length(); i++) {
    std::string byteStr = cleaned.substr(i * 2, 2);
    id[i] = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
  }
}


// Nanots implementation

/* NANOTS */


#include <cctype>
#include <filesystem>

std::mutex current_stream_tags_lok;
std::set<std::string> current_stream_tags;

// The SQLite catalog schema is versioned independently from the binary .nts
// format. Version 3 makes an open segment_block explicit with SQL NULL instead
// of overloading the valid timestamp value zero.
static constexpr int NANOTS_CATALOG_VERSION = 3;

// ---------------------------------------------------------------------------
// Epoch-Based Reclamation registry implementation
// ---------------------------------------------------------------------------

namespace {

int64_t _now_us() {
  using namespace std::chrono;
  return duration_cast<microseconds>(
             steady_clock::now().time_since_epoch())
      .count();
}

// Path-keyed table of registries. Both writers and iterators look up here
// at construction time. weak_ptr means entries naturally evaporate when the
// last writer and iterator on a file are destroyed.
std::mutex                                                    g_registry_table_mu;
std::map<std::string, std::weak_ptr<nanots_epoch_registry>>   g_registry_table;

std::string _registry_key(const std::string& file_path) {
  namespace fs = std::filesystem;

  std::error_code ec;
  fs::path normalized = fs::weakly_canonical(fs::path(file_path), ec);
  if (ec) {
    ec.clear();
    normalized = fs::absolute(fs::path(file_path), ec);
    if (ec) normalized = fs::path(file_path);
  }

  std::string key = normalized.lexically_normal().generic_string();
#ifdef _WIN32
  // Windows paths are case-insensitive. weakly_canonical() normalizes aliases
  // and dot components, but it does not promise a canonical letter case.
  std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
#endif
  return key;
}

}  // namespace

nanots_epoch_registry::Slot& nanots_epoch_registry::slot(uint32_t id) {
  // The slot's address is stable once allocated (unique_ptr in a vector).
  // We still take the lock so concurrent grows are safe.
  std::lock_guard<std::mutex> g(_slots_mu);
  return *_slots[id];
}

uint32_t nanots_epoch_registry::acquire_slot() {
  std::lock_guard<std::mutex> g(_slots_mu);

  // First try to reuse an INACTIVE slot.
  for (size_t i = 0; i < _slots.size(); ++i) {
    if (_slots[i]->epoch.load(std::memory_order_relaxed) == INACTIVE) {
      // Mark acquired with a temporary 0 epoch; caller will overwrite via
      // op_begin().
      _slots[i]->epoch.store(0, std::memory_order_relaxed);
      return static_cast<uint32_t>(i);
    }
  }

  // No free slot; grow.
  _slots.emplace_back(std::make_unique<Slot>());
  _slots.back()->epoch.store(0, std::memory_order_relaxed);
  return static_cast<uint32_t>(_slots.size() - 1);
}

void nanots_epoch_registry::release_slot(uint32_t id) {
  std::lock_guard<std::mutex> g(_slots_mu);
  if (id < _slots.size()) {
    _slots[id]->epoch.store(INACTIVE, std::memory_order_release);
  }
}

bool nanots_epoch_registry::can_recycle(uint64_t retired_epoch,
                                        int64_t  now_us) const {
  (void)now_us;  // retained API parameter; elapsed time is not proof of safety
  std::lock_guard<std::mutex> g(_slots_mu);
  for (const auto& slot_ptr : _slots) {
    uint64_t e = slot_ptr->epoch.load(std::memory_order_acquire);
    if (e == INACTIVE) continue;
    if (e > retired_epoch) continue;
    return false;  // active reader still pinning this retire
  }
  return true;
}

std::shared_ptr<nanots_epoch_registry>
nanots_epoch_registry::get_or_create(const std::string& file_path) {
  const std::string key = _registry_key(file_path);
  std::lock_guard<std::mutex> g(g_registry_table_mu);

  auto it = g_registry_table.find(key);
  if (it != g_registry_table.end()) {
    if (auto sp = it->second.lock()) return sp;
    // weak_ptr expired; fall through and create a new one.
  }

  auto sp = std::make_shared<nanots_epoch_registry>();
  g_registry_table[key] = sp;
  return sp;
}

// ---------------------------------------------------------------------------
// nanots_slot_guard
// ---------------------------------------------------------------------------

nanots_slot_guard::nanots_slot_guard(
    std::shared_ptr<nanots_epoch_registry> registry)
    : _registry(std::move(registry)) {
  if (_registry) {
    _slot_id = _registry->acquire_slot();
    op_begin();  // publish initial epoch + diagnostic heartbeat
  }
}

nanots_slot_guard::~nanots_slot_guard() { _release(); }

nanots_slot_guard::nanots_slot_guard(nanots_slot_guard&& other) noexcept
    : _registry(std::move(other._registry)), _slot_id(other._slot_id) {
  other._slot_id = UINT32_MAX;
}

nanots_slot_guard& nanots_slot_guard::operator=(
    nanots_slot_guard&& other) noexcept {
  if (this != &other) {
    _release();
    _registry      = std::move(other._registry);
    _slot_id       = other._slot_id;
    other._slot_id = UINT32_MAX;
  }
  return *this;
}

void nanots_slot_guard::op_begin() {
  if (_slot_id == UINT32_MAX) return;
  auto& s = _registry->slot(_slot_id);
  uint64_t e = _registry->global_epoch_load();
  s.epoch.store(e, std::memory_order_release);
  s.heartbeat_us.store(_now_us(), std::memory_order_relaxed);
}

void nanots_slot_guard::_release() noexcept {
  if (_slot_id != UINT32_MAX && _registry) {
    _registry->release_slot(_slot_id);
    _slot_id = UINT32_MAX;
  }
  _registry.reset();
}

// ---------------------------------------------------------------------------

static uint32_t _round_to_64k_boundary(uint32_t requested_size) {
  const uint32_t BOUNDARY = 65536;  // 64KB

  if (requested_size == 0)
    return BOUNDARY;  // Minimum size is 64KB

  // Round up to next multiple of 65536
  return ((requested_size + BOUNDARY - 1) / BOUNDARY) * BOUNDARY;
}

static bool _validate_frame_header(const uint8_t* frame_p,
                                   const uint8_t* expected_uuid,
                                   uint32_t* flags_out,
                                   uint32_t* size_out,
                                   int64_t* sec_key_out) {
  if (memcmp(frame_p + FRAME_UUID_OFFSET, expected_uuid, 16) != 0)
    return false;

  if (sec_key_out)
    *sec_key_out = *(int64_t*)(frame_p + FRAME_SECKEY_OFFSET);

  if (size_out)
    *size_out = *(uint32_t*)(frame_p + FRAME_SIZE_OFFSET);

  if (flags_out)
    *flags_out = *(uint32_t*)(frame_p + FRAME_FLAGS_OFFSET);

  return true;
}

// The writer publishes an index entry by incrementing this counter only after
// its frame and index bytes are complete. Readers use an acquire load so
// observing the new count also makes those preceding writes visible.
static uint32_t _load_committed_index_count(uint32_t* valid_counter) {
#ifdef _WIN32
  // The mapping may be read-only, so an interlocked read-modify-write (even
  // one that stores the same value) is not permitted. Aligned 32-bit reads
  // are atomic on supported Windows targets; the full barrier supplies the
  // acquire ordering paired with the writer's interlocked increment.
  uint32_t observed =
      *reinterpret_cast<volatile uint32_t*>(valid_counter);
  MemoryBarrier();
  return observed;
#else
  return __atomic_load_n(valid_counter, std::memory_order_acquire);
#endif
}

static std::string _database_name(const std::string& file_name) {
  return file_name.substr(0, file_name.find(".nts")) + ".db";
}

static void _free_block(nts_sqlite_conn& conn, int sb_id, int block_id) {
  nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
    auto stmt = conn.prepare("DELETE FROM segment_blocks WHERE id = ?");
    stmt.bind(1, sb_id).exec_no_result();
    stmt = conn.prepare("UPDATE blocks SET status = 'free' WHERE id = ?");
    stmt.bind(1, block_id).exec_no_result();
  });
}

static bool _is_valid_frame_at_index(uint8_t* block_p, uint32_t block_size,
                                     int index, uint32_t n_valid_indexes,
                                     const uint8_t* uuid) {
  uint8_t* index_p = block_p + BLOCK_HEADER_SIZE + (index * INDEX_ENTRY_SIZE);
  uint64_t offset = *(uint64_t*)(index_p + INDEX_ENTRY_OFFSET_OFFSET);

  // A zero offset identifies an unused/zeroed entry. Timestamp zero is valid;
  // n_valid_indexes is the commit boundary for the index array.
  if (offset == 0) {
    return false;
  }

  // Check frame offset bounds. The index region actually in use is exactly
  // the n_valid_indexes committed entries: nanots_writer::write() places the
  // frame at index i no lower than BLOCK_HEADER_SIZE + ((i + 1) *
  // INDEX_ENTRY_SIZE) — the end of that frame's own slot — and the valid
  // counter is only incremented once both the frame and its index entry are
  // written. Reserving an extra (n_valid_indexes + 1)'th slot here rejected
  // frames the writer legitimately placed flush against the index region,
  // which cost the whole block on recovery. That is reachable whenever the
  // last frame packs tightly, most easily with a max-size frame (its offset
  // lands exactly on the region end).
  uint32_t index_region_end = BLOCK_HEADER_SIZE + (n_valid_indexes * INDEX_ENTRY_SIZE);
  if (offset < index_region_end || offset > block_size - FRAME_HEADER_SIZE) {
    return false;
  }

  // Validate frame header
  uint32_t frame_size = 0;
  if (!_validate_frame_header(block_p + offset, uuid, nullptr, &frame_size, nullptr)) {
    return false;
  }

  // Check if frame size fits within block
  if (frame_size > block_size - offset - FRAME_HEADER_SIZE) {
    return false;
  }

  return true;
}

static void _validate_blocks(const std::string& file_name) {
  auto f = nts_file::open(file_name, "r+");

  if (!f)
    throw nanots_exception(NANOTS_EC_CANT_OPEN, "Unable to open file.", __FILE__, __LINE__);

  uint32_t block_size = 0;

  {
    nts_memory_map mm(
        filenum(f), 0, FILE_HEADER_BLOCK_SIZE,
        nts_memory_map::NMM_PROT_READ | nts_memory_map::NMM_PROT_WRITE,
        nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

    uint8_t* hp = (uint8_t*)mm.map();
    if (memcmp(hp + FILE_HEADER_MAGIC_OFFSET, NANOTS_FILE_MAGIC, NANOTS_FILE_MAGIC_LEN) != 0)
      throw nanots_exception(NANOTS_EC_BAD_MAGIC, "Not a nanots v2 file (bad magic).", __FILE__, __LINE__);
    uint16_t version = *(uint16_t*)(hp + FILE_HEADER_VERSION_OFFSET);
    if (version != NANOTS_FORMAT_VERSION)
      throw nanots_exception(NANOTS_EC_BAD_VERSION, "Unsupported nanots format version.", __FILE__, __LINE__);
    block_size = *(uint32_t*)(hp + FILE_HEADER_BLOCK_SIZE_OFFSET);
  }

  auto db_name = _database_name(file_name);
  nts_sqlite_conn conn(db_name, true, true);
  
  std::vector<std::map<std::string, std::optional<std::string>>> rowsToProcess;
  
  bool doneValidating = false;
  while(!doneValidating) {
    if(rowsToProcess.empty()) {
      rowsToProcess = conn.exec(
          "SELECT sb.id, sb.block_idx, sb.block_id, sb.uuid, s.stream_tag "
          "FROM segment_blocks sb "
          "JOIN segments s ON sb.segment_id = s.id "
          "WHERE sb.end_timestamp IS NULL");
      
      if(rowsToProcess.empty()) {
        doneValidating = true;
        continue;
      }
    } else {
      auto row = rowsToProcess.back();
      rowsToProcess.pop_back();

      int sb_id = std::stoi(row["id"].value());
      int block_id = std::stoi(row["block_id"].value());
      int block_idx = std::stoi(row["block_idx"].value());
      std::string uuid_hex = row["uuid"].value();

      uint8_t uuid[16];
      s_to_entropy_id(uuid_hex, uuid);

      nts_memory_map mm(
          filenum(f), FILE_HEADER_BLOCK_SIZE + (block_idx * block_size),
          block_size,
          nts_memory_map::NMM_PROT_READ | nts_memory_map::NMM_PROT_WRITE,
          nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

      uint8_t* block_p = (uint8_t*)mm.map();

      auto valid_counter = (uint32_t*)(block_p + 8);

      uint32_t n_valid_indexes =
          _load_committed_index_count(valid_counter);

      if((n_valid_indexes * INDEX_ENTRY_SIZE) >= (block_size / 2) || n_valid_indexes == 0) {
        _free_block(conn, sb_id, block_id);
        rowsToProcess.clear();
        continue;
      }

      // Find the last valid frame by searching backwards from the end
      int last_valid = -1;
      for (int i = n_valid_indexes - 1; i >= 0; i--) {
        if (_is_valid_frame_at_index(block_p, block_size, i, n_valid_indexes, uuid)) {
          last_valid = i;
          break;
        }
      }

      if(last_valid < 0) {
        _free_block(conn, sb_id, block_id);
        rowsToProcess.clear();
        continue;
      } else {
        // Truncating corrupt block
        *(uint32_t*)(block_p + 8) = last_valid + 1;
        mm.flush(mm.map(), block_size, true);

        nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
          uint8_t* last_index_p =
              block_p + BLOCK_HEADER_SIZE + (last_valid * INDEX_ENTRY_SIZE);
          int64_t actual_last_timestamp = *(int64_t*)(last_index_p + INDEX_ENTRY_TS_OFFSET);
          int64_t actual_last_sk = *(int64_t*)(last_index_p + INDEX_ENTRY_SECKEY_OFFSET);
          auto stmt = conn.prepare(
              "UPDATE segment_blocks SET end_timestamp = ?, end_secondary_key = ? "
              "WHERE block_idx = ? AND uuid = ?");
          stmt.bind(1, actual_last_timestamp)
              .bind(2, actual_last_sk)
              .bind(3, block_idx)
              .bind(4, uuid_hex)
              .exec_no_result();
        });
      }
    }
  }
}

static void _recover_orphaned_blocks(const nts_sqlite_conn& conn) {
  // Startup recovery. A block is "orphaned" when it is marked 'used' or
  // 'reserved' in the catalog but no segment_block references it. This happens
  // when an auto-reclaim recycle is interrupted: _db_reclaim_oldest_used_block()
  // deletes the segment_block and marks the block 'reserved' for reuse through
  // EBR, but the in-memory limbo/ready lists that would return it to 'free' are
  // lost on a crash or hard exit. The block is then stranded forever: it has no
  // segment_block, so it can never be selected for reclaim again, and it is not
  // 'free', so it can never be handed to a writer. Eventually every block ends
  // up stranded and writes fail with NANOTS_EC_NO_FREE_BLOCKS. (Files written
  // by older builds may also hold 'used' orphans, from when the maintenance
  // task still flipped aged reclaim victims to 'used'; those are recovered here
  // too, hence the status != 'free' predicate rather than just 'reserved'.)
  //
  // The writer constructor runs before any block is handed out, so at this
  // point any non-'free' block lacking a segment_block is genuinely orphaned
  // and safe to reclaim. Blocks with a segment_block (finalized or the
  // currently-open one, already validated by _validate_blocks) are preserved.
  //
  // KNOWN LIMITATION (narrow, in-process only): this sweep frees orphans
  // unconditionally, which is correct at a true process start (any prior-process
  // orphan has no live reader pinning it — readers only ever pin blocks they
  // located via a segment_block, and an orphan has none). The one unsafe case is
  // reconstructing a writer *within a still-running process* that also has a live
  // reader: if a now-destroyed writer retired a block (deleting its segment_block)
  // while that reader was mid-read and still pinning the block's bytes, this sweep
  // could free it and hand it to a writer to overwrite under the reader. A precise
  // guard isn't possible here because the per-block retired_epoch died with the
  // previous writer's in-memory limbo. Closing it would require parking retired
  // victims in the shared (per-file) epoch registry so they survive writer churn
  // and can be drained through can_recycle(); the ~nanots_writer drain already
  // narrows the window to blocks under active read at the exact destroy/construct
  // boundary. The supported lifecycle — one writer opened at process start — never
  // hits this.
  nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
    conn.exec(
        "UPDATE blocks SET status = 'free', reserved_at = NULL WHERE "
        "status != 'free' AND id NOT IN (SELECT block_id FROM segment_blocks);");
  });
}

static int _get_db_version(const nts_sqlite_conn& conn) {
  auto result = conn.exec("PRAGMA user_version;");
  if (result.empty())
    throw nanots_exception(NANOTS_EC_SCHEMA, "Unable to query database version.", __FILE__, __LINE__);

  auto row = result.front();

  return std::stoi(row.begin()->second.value());
}

static void _set_db_version(const nts_sqlite_conn& conn, int version) {
  conn.exec("PRAGMA user_version=" + std::to_string(version) + ";");
}

static void _upgrade_db(const nts_sqlite_conn& conn,
                        const std::string& file_name) {
  auto current_version = _get_db_version(conn);

  // The v2 binary format is still current, but pre-v2 catalog schemas remain
  // unsupported. Fresh catalogs are stamped directly to the latest version.
  if (current_version != 0 && current_version < 2) {
    throw nanots_exception(NANOTS_EC_BAD_VERSION,
                           "Legacy nanots catalog (pre-v2) is not supported.",
                           __FILE__, __LINE__);
  }

  if (current_version > NANOTS_CATALOG_VERSION) {
    throw nanots_exception(NANOTS_EC_BAD_VERSION,
                           "Nanots catalog is newer than this library.",
                           __FILE__, __LINE__);
  }

  switch (current_version) {
    case 0: {
      nts_sqlite_transaction(
          conn, true, [&](const nts_sqlite_conn& conn) {
            _set_db_version(conn, NANOTS_CATALOG_VERSION);
          });
    }
      break;
    case 2: {
      // Version 2 used end_timestamp == 0 for both an open block and a
      // finalized block whose last frame really occurred at timestamp zero.
      // Convert every ambiguous row to the v3 open representation first, then
      // let block recovery inspect the committed on-disk index and restore its
      // actual end composite. Keep user_version at 2 until recovery succeeds:
      // an interrupted migration is therefore idempotent on the next open.
      nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
        conn.exec(
            "UPDATE segment_blocks "
            "SET end_timestamp = NULL, end_secondary_key = NULL "
            "WHERE end_timestamp = 0;");
      });

      _validate_blocks(file_name);

      nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
        _set_db_version(conn, NANOTS_CATALOG_VERSION);
      });
      break;
    }
    default:
      break;
  };
}

// Readers normally keep the catalog read-only. They only reopen it writable
// when an actual migration is required, so an already-current database remains
// usable from a read-only deployment.
static void _upgrade_db_for_reader(const std::string& file_name) {
  auto db_name = _database_name(file_name);
  {
    nts_sqlite_conn conn(db_name, false, false);
    if (_get_db_version(conn) == NANOTS_CATALOG_VERSION)
      return;
  }

  nts_sqlite_conn conn(db_name, true, true);
  _upgrade_db(conn, file_name);
}

static std::optional<block> _db_reclaim_oldest_used_block(
    const nts_sqlite_conn& conn) {
  // Find oldest finalized segment_block. Open blocks have a NULL end.
  auto result = conn.exec(
      "SELECT sb.block_id, b.idx, sb.id as segment_block_id, b.status "
      "FROM segment_blocks sb "
      "JOIN blocks b ON sb.block_id = b.id "
      "WHERE sb.end_timestamp IS NOT NULL AND (b.status = 'used' OR b.status = 'reserved') "
      "ORDER BY sb.end_timestamp ASC, b.reserved_at ASC "
      "LIMIT 1");

  if (result.empty())
    return std::nullopt;

  auto row = result.front();
  int64_t block_id = std::stoll(row["block_id"].value());
  int64_t segment_block_id = std::stoll(row["segment_block_id"].value());

  // Delete the segment_block entry (trigger will clean up empty segments)
  auto stmt = conn.prepare("DELETE FROM segment_blocks WHERE id = ?");
  stmt.bind(1, segment_block_id).exec_no_result();

  // Mark block as reserved
  stmt = conn.prepare(
      "UPDATE blocks SET status = 'reserved', reserved_at = CURRENT_TIMESTAMP "
      "WHERE id = ?");
  stmt.bind(1, block_id).exec_no_result();

  return block{block_id, std::stoll(row["idx"].value())};
}

static std::optional<block> _db_get_free_block(const nts_sqlite_conn& conn) {
  auto result =
      conn.exec("SELECT id, idx FROM blocks WHERE status = 'free' LIMIT 1;");

  if (result.empty())
    return std::nullopt;

  auto row = result.front();
  int64_t block_id = std::stoll(row["id"].value());

  auto stmt =
      conn.prepare("UPDATE blocks SET status = 'reserved' WHERE id = ?");
  stmt.bind(1, block_id).exec_no_result();

  return block{block_id, std::stoll(row["idx"].value())};
}

// (Block acquisition logic moved to nanots_writer::_acquire_writable_block,
//  which composes _db_get_free_block / _grow_blocks / _db_reclaim_oldest_used_block
//  with the EBR limbo/ready machinery.)

static std::optional<segment> _db_create_segment(const nts_sqlite_conn& conn,
                                                 const std::string& stream_tag,
                                                 const std::string& metadata) {
  auto stmt = conn.prepare(
      "INSERT INTO segments (stream_tag, metadata) VALUES (?, ?)");
  stmt.bind(1, stream_tag).bind(2, metadata).exec_no_result();

  segment s;
  s.id = std::stoll(conn.last_insert_id());
  s.stream_tag = stream_tag;
  s.metadata = metadata;
  s.sequence = 0;
  return s;
}

static std::optional<segment_block> _db_create_segment_block(
    const nts_sqlite_conn& conn,
    int64_t segment_id,
    int64_t sequence,
    int64_t block_id,
    int64_t block_idx,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    const uint8_t* uuid) {
  auto stmt = conn.prepare(
      "INSERT INTO segment_blocks ("
      "segment_id, "
      "sequence, "
      "block_id, "
      "block_idx, "
      "start_timestamp, "
      "end_timestamp, "
      "start_secondary_key, "
      "end_secondary_key, "
      "uuid"
      ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

  auto hex_uuid = entropy_id_to_s(uuid);

  stmt.bind(1, segment_id)
      .bind(2, sequence)
      .bind(3, block_id)
      .bind(4, block_idx)
      .bind(5, start_timestamp)
      .bind_null(6)
      .bind(7, start_secondary_key)
      .bind_null(8)
      .bind(9, hex_uuid)
      .exec_no_result();

  struct segment_block sb;
  sb.id = std::stoll(conn.last_insert_id());
  sb.segment_id = segment_id;
  sb.sequence = sequence;
  sb.block_id = block_id;
  sb.block_idx = block_idx;
  sb.start_timestamp = start_timestamp;
  sb.start_secondary_key = start_secondary_key;
  memcpy(sb.uuid, uuid, 16);

  return sb;
}

static void _db_finalize_block(const nts_sqlite_conn& conn,
                               int64_t segment_block_id,
                               int64_t timestamp,
                               int64_t secondary_key) {
  auto stmt = conn.prepare(
      "UPDATE segment_blocks SET end_timestamp = ?, end_secondary_key = ? "
      "WHERE id = ?");
  stmt.bind(1, timestamp)
      .bind(2, secondary_key)
      .bind(3, segment_block_id)
      .exec_no_result();
}

static void _db_trans_finalize_reserved_blocks(const nts_sqlite_conn& conn) {
  // Promote aged 'reserved' writer slots to 'used'. A block can be 'reserved'
  // for two distinct reasons, and only one of them may be finalized here:
  //
  //   1. A live writer slot — it has a segment_block row. After the grace
  //      window it becomes 'used' (the block now holds committed data). This
  //      is the only transition this maintenance task performs.
  //
  //   2. An auto-reclaim victim — _db_reclaim_oldest_used_block() deleted its
  //      segment_block and marked it 'reserved' to recycle it through EBR, then
  //      parked it in the in-memory limbo/ready lists. Such a block has no
  //      segment_block row, and this task deliberately leaves it untouched:
  //
  //        - Flipping it to 'used' would orphan it forever — with no
  //          segment_block, _db_reclaim_oldest_used_block() can never select it
  //          again and it is never 'free' to hand out. That is the wedge bug
  //          (NANOTS_EC_NO_FREE_BLOCKS) this code path used to cause.
  //        - Flipping it to 'free' is unsafe: the victim may still be pinned by
  //          a live reader. EBR keeps a reader pinned until its iterator or
  //          reader operation releases the slot; a reader that pinned the
  //          block before its segment_block was deleted may still be
  //          dereferencing its bytes. A later _db_get_free_block() would then
  //          hand the block to a writer that overwrites it under the reader,
  //          breaking the core reads-disconnected-from-writes guarantee.
  //
  //   Reclaim victims are recycled the safe way instead: while the process
  //   lives, _scan_limbo() promotes a victim to the ready list only once
  //   can_recycle() confirms no reader can still be pinning it, and the next
  //   acquire hands the block back to a writer (which re-creates its
  //   segment_block). Victims still parked in limbo/ready when the process
  //   exits are returned to 'free' at the next open by
  //   _recover_orphaned_blocks(), which runs in the writer constructor.
  conn.exec(
      "UPDATE blocks SET status = 'used' WHERE status = 'reserved' AND "
      "reserved_at < datetime('now', '-10 seconds') AND "
      "id IN (SELECT block_id FROM segment_blocks);");
}

static void _recycle_block(write_context& wctx, int64_t timestamp) {
  uint8_t* p = (uint8_t*)wctx.mm.map();

  // write the new timestamp
  *(int64_t*)p = timestamp;
  p += sizeof(int64_t);

  uint32_t old_n_valid_indexes = *(uint32_t*)p;

  // zero out the n_valid_indexes
  auto valid_counter = (uint32_t*)p;

#ifdef _WIN32
  std::atomic_thread_fence(std::memory_order_release);
  *reinterpret_cast<volatile uint32_t*>(valid_counter) = 0;
#else
  __atomic_store_n(valid_counter, 0, std::memory_order_release);
#endif

  p += sizeof(uint32_t);

  // zero out the reserved field
  *(uint32_t*)p = 0;
  p += sizeof(uint32_t);

  memset(p, 0, INDEX_ENTRY_SIZE * old_n_valid_indexes);

  // IMPORTANT: Sync immediately to ensure zeros are on disk
  // This prevents seeing old index entries after a crash
  wctx.mm.flush(wctx.mm.map(),
                BLOCK_HEADER_SIZE + (INDEX_ENTRY_SIZE * old_n_valid_indexes),
                true);
}

write_context::~write_context() {
  std::lock_guard<std::mutex> g(current_stream_tags_lok);
  std::string key = file_name + ":" + stream_tag;
  current_stream_tags.erase(key);

  if (last_timestamp && current_block) {
    mm.flush(mm.map(), _block_size, true);

    auto db_name = _database_name(file_name);
    nts_sqlite_conn conn(db_name, true, true);

    int64_t end_sk = last_secondary_key.value_or(NANOTS_SEC_KEY_UNSET);

    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
      _db_finalize_block(conn, current_block->id, last_timestamp.value(), end_sk);
      // This is a maintenance task that needs to be done periodically.
      _db_trans_finalize_reserved_blocks(conn);
    });
  }
}

nanots_writer::nanots_writer(const std::string& file_name, bool auto_reclaim)
    : _file_name(file_name),
      _file_size(file_size(file_name)),
      _file(nts_file::open(file_name, "r+")),
      _file_header_mm(
          filenum(_file),
          0,
          FILE_HEADER_BLOCK_SIZE,
          nts_memory_map::NMM_PROT_READ | nts_memory_map::NMM_PROT_WRITE,
          nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED),
      _file_header_p((uint8_t*)_file_header_mm.map()),
      _block_size(*(uint32_t*)(_file_header_p + FILE_HEADER_BLOCK_SIZE_OFFSET)),
      _n_blocks(*(uint32_t*)(_file_header_p + FILE_HEADER_N_BLOCKS_OFFSET)),
      _max_blocks(*(uint32_t*)(_file_header_p + FILE_HEADER_MAX_BLOCKS_OFFSET)),
      _auto_reclaim(auto_reclaim),
      _epoch(nanots_epoch_registry::get_or_create(file_name)) {
  // Validate v2 magic + version up front. Legacy v1 files (which had
  // block_size at offset 0) are rejected by design.
  if (memcmp(_file_header_p + FILE_HEADER_MAGIC_OFFSET,
             NANOTS_FILE_MAGIC, NANOTS_FILE_MAGIC_LEN) != 0)
    throw nanots_exception(NANOTS_EC_BAD_MAGIC,
                           "Not a nanots v2 file (bad magic). Legacy v1 files are not supported.",
                           __FILE__, __LINE__);
  uint16_t version = *(uint16_t*)(_file_header_p + FILE_HEADER_VERSION_OFFSET);
  if (version != NANOTS_FORMAT_VERSION)
    throw nanots_exception(NANOTS_EC_BAD_VERSION,
                           "Unsupported nanots format version.",
                           __FILE__, __LINE__);

  if (_block_size < 4096 || _block_size > 1024 * 1024 * 1024)
    throw nanots_exception(NANOTS_EC_INVALID_BLOCK_SIZE, "Invalid block size in file header.", __FILE__, __LINE__);

  auto db_name = _database_name(_file_name);
  nts_sqlite_conn db(db_name, true, true);
  _upgrade_db(db, _file_name);
  _validate_blocks(_file_name);
  _recover_orphaned_blocks(db);
}

nanots_writer::~nanots_writer() {
  // Drain the EBR limbo/ready lists back to the free pool before this writer
  // disappears. These lists are in-memory only; anything still parked in them
  // when we go away is a block we retired (its segment_block deleted, status
  // 'reserved') but never got to hand back to a writer. Without this drain such
  // blocks survive as orphaned 'reserved' rows and are only reclaimed on the
  // next open by _recover_orphaned_blocks() — a leak for the lifetime of the
  // process. Draining here returns the EBR-cleared ones to 'free' immediately.
  //
  // Safety: we free ONLY blocks that have cleared EBR (migrated into _ready by
  // _scan_limbo, i.e. can_recycle() is true — no reader can still be pinning
  // their bytes). Entries still sitting in _limbo are pinned by a live reader;
  // we deliberately leave those 'reserved' and let _recover_orphaned_blocks()
  // reclaim them at the next open, once those readers are gone.
  if (!_auto_reclaim)
    return;

  std::vector<int64_t> freeable;
  {
    _scan_limbo();  // migrate any now-clear limbo entries into _ready
    std::lock_guard<std::mutex> g(_limbo_mu);
    for (const auto& e : _ready)
      freeable.push_back(e.block_id);
    _ready.clear();
    // Whatever remains in _limbo is still reader-pinned; leave it 'reserved'.
  }

  if (freeable.empty())
    return;

  // A destructor must not throw. If the catalog is momentarily unavailable at
  // shutdown the blocks simply stay 'reserved' and are recovered on next open.
  try {
    nts_sqlite_conn conn(_database_name(_file_name), true, true);
    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
      // The predicate mirrors _recover_orphaned_blocks: only flip a block that
      // is genuinely a retired victim (reserved, no segment_block). Belt and
      // suspenders — a _ready entry is always exactly that.
      auto stmt = conn.prepare(
          "UPDATE blocks SET status = 'free', reserved_at = NULL WHERE id = ? "
          "AND status = 'reserved' AND id NOT IN "
          "(SELECT block_id FROM segment_blocks);");
      for (int64_t id : freeable) {
        stmt.reset();
        stmt.bind(1, id).exec_no_result();
      }
    });
  } catch (...) {
    // Swallow: see note above.
  }
}

std::optional<block> nanots_writer::_grow_blocks(const nts_sqlite_conn& conn) {
  // Current physical block count comes from sqlite (authoritative).
  auto count_rows = conn.exec("SELECT COUNT(*) AS c FROM blocks;");
  int64_t current_count = std::stoll(count_rows.front()["c"].value());

  // Honor the cap.
  if (_max_blocks > 0 && current_count >= _max_blocks)
    return std::nullopt;

  // BoltDB-style: double the current count, capped at 1 GiB per grow.
  // First-ever grow allocates exactly 1 block.
  const int64_t cap_bytes = 1024LL * 1024LL * 1024LL;
  int64_t cap_blocks = std::max<int64_t>(1, cap_bytes / _block_size);

  int64_t to_add = (current_count == 0)
                       ? 1
                       : std::min<int64_t>(current_count, cap_blocks);

  // Don't exceed _max_blocks if one is set.
  if (_max_blocks > 0)
    to_add = std::min<int64_t>(to_add, int64_t(_max_blocks) - current_count);

  if (to_add <= 0)
    return std::nullopt;

  // Extend the file. fallocate is idempotent on size — if a previous grow
  // partially succeeded (file extended but sqlite rolled back), we just
  // re-extend to the same or larger size, no harm done.
  uint64_t new_size = FILE_HEADER_BLOCK_SIZE +
                      static_cast<uint64_t>(current_count + to_add) * _block_size;

  if (fallocate(_file, new_size) < 0)
    throw nanots_exception(NANOTS_EC_UNABLE_TO_ALLOCATE_FILE,
                           "Unable to grow file.", __FILE__, __LINE__);

  _file_size = new_size;

  // Insert new block rows. First one is 'reserved' (the caller's slot);
  // the rest are 'free' for future writes.
  auto stmt = conn.prepare("INSERT INTO blocks (idx, status) VALUES (?, ?)");
  int64_t first_new_idx = current_count;
  for (int64_t i = 0; i < to_add; i++) {
    int64_t idx = current_count + i;
    stmt.reset();
    stmt.bind(1, idx).bind(2, std::string(i == 0 ? "reserved" : "free")).exec_no_result();
  }

  int64_t first_new_id = std::stoll(conn.last_insert_id()) - (to_add - 1);
  return block{first_new_id, first_new_idx};
}

write_context nanots_writer::create_write_context(const std::string& stream_tag,
                                                  const std::string& metadata) {

  std::string key = _file_name + ":" + stream_tag;

  std::lock_guard<std::mutex> g(current_stream_tags_lok);
  if(current_stream_tags.find(key) != current_stream_tags.end())
    throw nanots_exception(NANOTS_EC_DUPLICATE_STREAM_TAG, "Only one current writer per active stream tag.", __FILE__, __LINE__);

  write_context wctx;
  wctx.metadata = metadata;
  wctx.stream_tag = stream_tag;
  wctx.file_name = _file_name;
  wctx._block_size = _block_size;

  // Segment creation is deferred to the first write(): only then do we know
  // whether the caller intends this stream to carry a secondary key, which
  // is recorded permanently on the segments row.

  current_stream_tags.insert(key);

  return wctx;
}

void nanots_writer::_scan_limbo() {
  int64_t now = _now_us();
  std::lock_guard<std::mutex> g(_limbo_mu);
  // Front-of-deque ordering by retired_epoch is monotonic (every retire
  // bumps global_epoch), so once the front is blocked, everything behind
  // it is too.
  while (!_limbo.empty()) {
    const auto& front = _limbo.front();
    if (!_epoch->can_recycle(front.retired_epoch, now)) break;
    _ready.push_back(front);
    _limbo.pop_front();
  }
}

block nanots_writer::_acquire_writable_block(const nts_sqlite_conn& conn) {
  auto take_ready = [&]() -> std::optional<block> {
    std::lock_guard<std::mutex> g(_limbo_mu);
    if (!_ready.empty()) {
      auto e = _ready.front();
      _ready.pop_front();
      return block{e.block_id, e.block_idx};
    }
    return std::nullopt;
  };

  // 1. Existing retired blocks may have become safe since the previous write.
  // Scan before touching the catalog so we never strand a reusable victim or
  // retire additional history unnecessarily.
  _scan_limbo();
  if (auto ready = take_ready())
    return *ready;

  // 2. Claim a truly free block, or grow the file, while holding SQLite's
  // write lock. The SELECT+UPDATE in _db_get_free_block and the
  // COUNT+extend+INSERT sequence in _grow_blocks must be one serialized
  // catalog operation; otherwise two writer instances can select the same
  // physical block (or both grow starting at the same block index).
  std::optional<block> available;
  nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& tx) {
    available = _db_get_free_block(tx);
    if (!available && is_growable())
      available = _grow_blocks(tx);
  });
  if (available)
    return *available;

  // 4. Auto-reclaim. One acquisition may retire at most one catalog block.
  // If a prior victim is still waiting in limbo, all later retirement epochs
  // would be pinned by the same reader, so deleting more history cannot help.
  if (_auto_reclaim) {
    _scan_limbo();
    if (auto ready = take_ready())
      return *ready;

    {
      std::lock_guard<std::mutex> g(_limbo_mu);
      if (_limbo.empty()) {
        // Keep this writer's empty-check, retirement and enqueue together so
        // concurrent write contexts cannot both create a first pending victim.
        // SQLite's transaction still serializes victim choice across distinct
        // nanots_writer instances.
        std::optional<block> victim;
        nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& tx) {
          victim = _db_reclaim_oldest_used_block(tx);
        });

        if (victim) {
          uint64_t retired_epoch = _epoch->global_epoch_bump();
          _limbo.push_back({victim->id, victim->idx, retired_epoch});
        }
      }
    }

    // Give an active reader the same short opportunity to advance that the old
    // loop provided, but only rescan the existing victim—never retire another.
    constexpr int MAX_RETRIES = 100;
    for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
      _scan_limbo();
      if (auto ready = take_ready())
        return *ready;
      std::this_thread::yield();
    }
  }

  throw nanots_exception(NANOTS_EC_NO_FREE_BLOCKS, "Unable to get free block.",
                         __FILE__, __LINE__);
}

void nanots_writer::write(write_context& wctx,
                          const uint8_t* data,
                          size_t size,
                          uint32_t flags,
                          int64_t timestamp,
                          int64_t secondary_key) {
  // Composite (timestamp, secondary_key) must be strictly greater than the
  // previous frame's composite. When the caller doesn't pass a sec_key, the
  // default is NANOTS_SEC_KEY_UNSET (= INT64_MIN); for a stream that never
  // passes one this degenerates to "timestamp must strictly increase."
  if (wctx.last_timestamp) {
    int64_t last_ts = wctx.last_timestamp.value();
    int64_t last_sk = wctx.last_secondary_key.value_or(NANOTS_SEC_KEY_UNSET);
    bool composite_greater =
        (timestamp > last_ts) ||
        (timestamp == last_ts && secondary_key > last_sk);
    if (!composite_greater) {
      throw nanots_exception(
          NANOTS_EC_NON_MONOTONIC_TIMESTAMP,
          "Composite (timestamp, secondary_key) is not strictly greater than "
          "the previous frame's composite.",
          __FILE__, __LINE__);
    }
  }

  if (size >
      _block_size - (FRAME_HEADER_SIZE + INDEX_ENTRY_SIZE + BLOCK_HEADER_SIZE))
    throw nanots_exception(NANOTS_EC_ROW_SIZE_TOO_BIG, "Frame size is too large. Use a much larger block size.", __FILE__, __LINE__);

  // First write to this context: lazily create the segment row.
  if (!wctx.current_segment) {
    nts_sqlite_conn conn(_database_name(_file_name), true, true);
    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
      wctx.current_segment = _db_create_segment(
          conn, wctx.stream_tag, wctx.metadata);
      if (!wctx.current_segment)
        throw nanots_exception(NANOTS_EC_UNABLE_TO_CREATE_SEGMENT, "Unable to create segment.", __FILE__, __LINE__);
    });
  }

  if (!wctx.current_block) {
    nts_sqlite_conn conn(_database_name(_file_name), true, true);

    // Acquire a physical block under EBR. The returned block is always safe
    // to overwrite: it is either a freshly-free block, a newly-grown block,
    // or a retired block that has cleared EBR safety. Acquisition uses short
    // internal IMMEDIATE transactions to serialize each catalog claim; it
    // does not hold a SQLite transaction while waiting on readers.
    block phys = _acquire_writable_block(conn);

    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
      uint8_t uuid[16];
      generate_entropy_id(uuid);

      wctx.current_block = _db_create_segment_block(
          conn, wctx.current_segment->id, wctx.current_segment->sequence,
          phys.id, phys.idx, timestamp, secondary_key, uuid);

      if (!wctx.current_block)
        throw nanots_exception(NANOTS_EC_UNABLE_TO_CREATE_SEGMENT_BLOCK, "Unable to create segment block.", __FILE__, __LINE__);

      wctx.current_segment->sequence++;
    });

    wctx.file = nts_file::open(_file_name, "r+");

    wctx.mm = nts_memory_map(
        filenum(wctx.file),
        FILE_HEADER_BLOCK_SIZE + (wctx.current_block->block_idx * _block_size),
        _block_size,
        nts_memory_map::NMM_PROT_READ | nts_memory_map::NMM_PROT_WRITE,
        nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

    _recycle_block(wctx, timestamp);
  }

  uint8_t* block_p = (uint8_t*)wctx.mm.map();

  uint32_t n_valid_indexes = *(uint32_t*)(block_p + 8);

  uint64_t index_end =
      BLOCK_HEADER_SIZE + ((n_valid_indexes + 1) * INDEX_ENTRY_SIZE);

  // Calculate padded frame size for 8-byte alignment (required for ARM compatibility)
  uint32_t total_frame_size = (uint32_t)(FRAME_HEADER_SIZE + size);
  uint32_t padded_frame_size = (total_frame_size + 7) & ~7;  // Round up to multiple of 8

  uint64_t new_block_ofs = (uint64_t)(_block_size - padded_frame_size);
  bool fits = (new_block_ofs >= index_end);

  if (n_valid_indexes > 0) {
    uint8_t* last_index_p = block_p + BLOCK_HEADER_SIZE +
                            ((n_valid_indexes - 1) * INDEX_ENTRY_SIZE);
    uint64_t last_frame_offset = *(uint64_t*)(last_index_p + INDEX_ENTRY_OFFSET_OFFSET);
    if (last_frame_offset >= padded_frame_size) {
      uint64_t candidate_ofs = last_frame_offset - padded_frame_size;
      fits = (candidate_ofs >= index_end);
      new_block_ofs = fits ? candidate_ofs : index_end;
    } else {
      fits = false;
      new_block_ofs = index_end;  // Force rollover to new block
    }
  }

  if (!fits) {
    nts_sqlite_conn conn(_database_name(_file_name), true, true);

    wctx.mm.flush(wctx.mm.map(), _block_size, true);

    int64_t end_sk = wctx.last_secondary_key.value_or(NANOTS_SEC_KEY_UNSET);
    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
      _db_finalize_block(conn, wctx.current_block->id,
                         wctx.last_timestamp.value(), end_sk);
    });

    wctx.current_block = std::nullopt;
    wctx.mm = nts_memory_map();

    return write(wctx, data, size, flags, timestamp, secondary_key);
  }

  uint8_t* frame_p = block_p + new_block_ofs;
  memcpy(frame_p + FRAME_UUID_OFFSET, wctx.current_block->uuid, 16);
  *(int64_t*)(frame_p + FRAME_SECKEY_OFFSET) = secondary_key;
  *(uint32_t*)(frame_p + FRAME_SIZE_OFFSET) = (uint32_t)size;
  *(uint32_t*)(frame_p + FRAME_FLAGS_OFFSET) = flags;
  memcpy(frame_p + FRAME_HEADER_SIZE, data, size);

  uint8_t* index_p =
      block_p + BLOCK_HEADER_SIZE + (n_valid_indexes * INDEX_ENTRY_SIZE);
  *(int64_t*)(index_p + INDEX_ENTRY_TS_OFFSET) = timestamp;
  *(int64_t*)(index_p + INDEX_ENTRY_SECKEY_OFFSET) = secondary_key;
  *(uint64_t*)(index_p + INDEX_ENTRY_OFFSET_OFFSET) = new_block_ofs;
  *(uint64_t*)(index_p + INDEX_ENTRY_RESERVED_OFFSET) = 0;

  auto valid_counter = (uint32_t*)(block_p + 8);

#ifdef _WIN32
  _InterlockedIncrement(reinterpret_cast<volatile long*>(valid_counter));
#else
  __atomic_fetch_add(valid_counter, 1, std::memory_order_release);
#endif

  wctx.last_timestamp = timestamp;
  // Track the secondary key unconditionally so the composite monotonicity
  // check on the next write has the right "last" value (including UNSET).
  wctx.last_secondary_key = secondary_key;
}

void nanots_writer::free_blocks(const std::string& file_name,
                                const std::string& stream_tag,
                                int64_t start_timestamp,
                                int64_t start_secondary_key,
                                int64_t end_timestamp,
                                int64_t end_secondary_key) {
  auto db_name = _database_name(file_name);
  nts_sqlite_conn conn(db_name, true, true);
  _upgrade_db(conn, file_name);

  nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
    // Find blocks fully contained in the composite window
    // [(start_ts, start_sk), (end_ts, end_sk)]:
    //   block_start >= window_start:
    //     start_ts > ? OR (start_ts = ? AND start_sk >= ?)
    //   block_end <= window_end:
    //     end_ts < ? OR (end_ts = ? AND end_sk <= ?)
    // Plus end_timestamp IS NOT NULL to skip the currently-open block.
    auto stmt = conn.prepare(
        "SELECT sb.id as segment_block_id, sb.block_id "
        "FROM segment_blocks sb "
        "JOIN segments s ON sb.segment_id = s.id "
        "WHERE s.stream_tag = ? "
        "AND (sb.start_timestamp > ? OR "
        "     (sb.start_timestamp = ? AND sb.start_secondary_key >= ?)) "
        "AND (sb.end_timestamp < ? OR "
        "     (sb.end_timestamp = ? AND sb.end_secondary_key <= ?)) "
        "AND sb.end_timestamp IS NOT NULL");
    auto blocks_to_delete =
        stmt.bind(1, stream_tag)
            .bind(2, start_timestamp).bind(3, start_timestamp).bind(4, start_secondary_key)
            .bind(5, end_timestamp).bind(6, end_timestamp).bind(7, end_secondary_key)
            .exec();

    for (auto& block_row : blocks_to_delete) {
      int64_t segment_block_id = std::stoll(block_row["segment_block_id"].value());
      int64_t block_id = std::stoll(block_row["block_id"].value());

      // Remove segment_block entry (trigger will clean up empty segments)
      stmt = conn.prepare("DELETE FROM segment_blocks WHERE id = ?");
      stmt.bind(1, segment_block_id).exec_no_result();

      // Mark block as free
      stmt = conn.prepare("UPDATE blocks SET status = 'free' WHERE id = ?");
      stmt.bind(1, block_id).exec_no_result();
    }
  });
}

void nanots_writer::allocate_growable(const std::string& file_name,
                                      uint32_t block_size,
                                      uint32_t max_blocks) {
  // Growable mode is signalled by n_blocks == 0 in the file header.
  allocate(file_name, block_size, 0);

  if (max_blocks > 0) {
    auto f = nts_file::open(file_name, "r+");
    nts_memory_map mm(
        filenum(f), 0, 4096,
        nts_memory_map::NMM_PROT_READ | nts_memory_map::NMM_PROT_WRITE,
        nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);
    uint8_t* p = (uint8_t*)mm.map();
    *(uint32_t*)(p + FILE_HEADER_MAX_BLOCKS_OFFSET) = max_blocks;
    mm.flush(mm.map(), FILE_HEADER_USED_BYTES);
  }
}

void nanots_writer::allocate(const std::string& file_name,
                             uint32_t block_size,
                             uint32_t n_blocks) {
  // Windows MapViewOfFile() requires mapped regions to start and end on 64k
  // boundaires. Our file header size is 65536, SO if the block size is a
  // multiple of 65536 then block start and end on 64k boundaries.
  block_size = _round_to_64k_boundary(block_size);

  // n_blocks == 0 ⇒ growable mode: file starts at just the header and grows
  // on demand. Otherwise pre-allocate the full file up front.
  uint64_t file_size = FILE_HEADER_BLOCK_SIZE + static_cast<uint64_t>(n_blocks) * block_size;

  {
    auto f = nts_file::open(file_name, "w+");

    if (fallocate(f, file_size) < 0)
      throw nanots_exception(NANOTS_EC_UNABLE_TO_ALLOCATE_FILE, "Unable to allocate file.", __FILE__, __LINE__);
  }

  {
    auto f = nts_file::open(file_name, "r+");

    nts_memory_map mm(
        filenum(f), 0, 4096,
        nts_memory_map::NMM_PROT_READ | nts_memory_map::NMM_PROT_WRITE,
        nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

    uint8_t* p = (uint8_t*)mm.map();

    // Write the v2 file header. Zero the first 64 bytes first so we have a
    // clean slate (fallocate / SetEndOfFile leave indeterminate bytes here).
    memset(p, 0, 64);
    memcpy(p + FILE_HEADER_MAGIC_OFFSET, NANOTS_FILE_MAGIC, NANOTS_FILE_MAGIC_LEN);
    *(uint16_t*)(p + FILE_HEADER_VERSION_OFFSET) = (uint16_t)NANOTS_FORMAT_VERSION;
    *(uint16_t*)(p + FILE_HEADER_HSIZE_OFFSET)   = (uint16_t)FILE_HEADER_USED_BYTES;
    *(uint32_t*)(p + FILE_HEADER_BLOCK_SIZE_OFFSET) = block_size;
    *(uint32_t*)(p + FILE_HEADER_N_BLOCKS_OFFSET)   = n_blocks;
    *(uint32_t*)(p + FILE_HEADER_MAX_BLOCKS_OFFSET) = 0;
    *(uint32_t*)(p + FILE_HEADER_FLAGS_OFFSET)      = 0;
    *(uint64_t*)(p + FILE_HEADER_FEATURES_OFFSET)   = 0;

    mm.flush(mm.map(), 64);
  }

  auto db_name = _database_name(file_name);

  if (file_exists(db_name))
    remove_file(db_name);

  nts_sqlite_conn db(db_name.c_str(), true, true);

  std::string query =
      "CREATE TABLE blocks ("
      "id INTEGER PRIMARY KEY AUTOINCREMENT, "
      "idx INTEGER, "
      "status STRING, "
      "reserved_at DATETIME DEFAULT CURRENT_TIMESTAMP"
      ");";
  db.exec(query);

  query =
      "CREATE TABLE segments ("
      "id INTEGER PRIMARY KEY AUTOINCREMENT, "
      "stream_tag STRING, "
      "metadata STRING"
      ");";
  db.exec(query);

  query =
      "CREATE TABLE segment_blocks ("
      "id INTEGER PRIMARY KEY AUTOINCREMENT, "
      "segment_id INTEGER, "
      "sequence INTEGER, "
      "block_id INTEGER, "
      "block_idx INTEGER, "
      "start_timestamp INTEGER, "
      "end_timestamp INTEGER, "
      "start_secondary_key INTEGER, "
      "end_secondary_key INTEGER, "
      "uuid STRING, "
      "FOREIGN KEY (segment_id) REFERENCES segments(id)"
      ");";
  db.exec(query);

  query =
      "CREATE TRIGGER delete_empty_segments "
      "AFTER DELETE ON segment_blocks "
      "BEGIN "
      "DELETE FROM segments "
      "WHERE id = OLD.segment_id "
      "AND NOT EXISTS ( "
      "SELECT 1 FROM segment_blocks "
      "WHERE segment_id = OLD.segment_id "
      "); "
      "END;";
  db.exec(query);

  query =
      "CREATE INDEX idx_segment_blocks_segment_id ON "
      "segment_blocks(segment_id);";
  db.exec(query);

  query =
      "CREATE INDEX idx_segment_blocks_time_range ON segment_blocks(start_timestamp);";
  db.exec(query);

  query =
      "CREATE INDEX idx_segment_blocks_sec_key ON "
      "segment_blocks(segment_id, start_secondary_key);";
  db.exec(query);

  query = "CREATE INDEX idx_segments_stream_tag ON segments(stream_tag);";
  db.exec(query);

  query = "CREATE INDEX idx_blocks_status ON blocks(status);";
  db.exec(query);

  nts_sqlite_transaction(db, true, [n_blocks](const nts_sqlite_conn& conn) {
    auto stmt =
        conn.prepare("INSERT INTO blocks (idx, status) VALUES (?, 'free')");
    for (uint32_t i = 0; i < n_blocks; i++) {
      stmt.bind(1, static_cast<int>(i)).exec_no_result();
      stmt.reset();
    }
  });

  _upgrade_db(db, file_name);
}

nanots_reader::nanots_reader(const std::string& file_name)
    : _file_name(file_name),
      _file(nts_file::open(file_name, "r")),
      _block_size(),
      _n_blocks(),
      _slot_guard(nanots_epoch_registry::get_or_create(file_name)) {
  auto mm = nts_memory_map(
      filenum(_file), 0, FILE_HEADER_BLOCK_SIZE, nts_memory_map::NMM_PROT_READ,
      nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

  auto header_p = (uint8_t*)mm.map();

  if (memcmp(header_p + FILE_HEADER_MAGIC_OFFSET,
             NANOTS_FILE_MAGIC, NANOTS_FILE_MAGIC_LEN) != 0)
    throw nanots_exception(NANOTS_EC_BAD_MAGIC,
                           "Not a nanots v2 file (bad magic).",
                           __FILE__, __LINE__);
  uint16_t version = *(uint16_t*)(header_p + FILE_HEADER_VERSION_OFFSET);
  if (version != NANOTS_FORMAT_VERSION)
    throw nanots_exception(NANOTS_EC_BAD_VERSION,
                           "Unsupported nanots format version.",
                           __FILE__, __LINE__);

  _block_size = *(uint32_t*)(header_p + FILE_HEADER_BLOCK_SIZE_OFFSET);
  _n_blocks   = *(uint32_t*)(header_p + FILE_HEADER_N_BLOCKS_OFFSET);

  _upgrade_db_for_reader(_file_name);
}

// Lexicographic compare on (timestamp, secondary_key). Target is a 16-byte
// buffer containing [target_ts (int64), target_sk (int64)]. Used for the
// composite binary search inside a block.
static int _compare_index_entry_composite(uint8_t* index_entry_p,
                                          uint8_t* target_p) {
  int64_t entry_ts = *(int64_t*)(index_entry_p + INDEX_ENTRY_TS_OFFSET);
  int64_t target_ts = *(int64_t*)(target_p + 0);
  if (entry_ts != target_ts) return entry_ts < target_ts ? -1 : 1;

  int64_t entry_sk = *(int64_t*)(index_entry_p + INDEX_ENTRY_SECKEY_OFFSET);
  int64_t target_sk = *(int64_t*)(target_p + sizeof(int64_t));
  if (entry_sk != target_sk) return entry_sk < target_sk ? -1 : 1;
  return 0;
}

void nanots_reader::read(
    const std::string& stream_tag,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key,
    const std::function<
        void(const uint8_t*, size_t, uint32_t, int64_t, int64_t, int64_t, const std::string&)>& callback) {
  // EBR critical section spans the entire read(): the writer must not
  // overwrite any block whose bytes the callback might dereference.
  nanots_op_scope _op(_slot_guard);

  nts_sqlite_conn db(_database_name(_file_name), false, true);

  // Composite overlap: include a block whose lex range
  // [(start_ts, start_sk), (end_ts, end_sk)] intersects the requested
  // window. Two lex comparisons:
  //   block_start <= window_end:
  //     start_ts < ?  OR  (start_ts = ? AND start_sk <= ?)
  //   block_end >= window_start (or the block is still open):
  //     end_ts IS NULL OR end_ts > ? OR (end_ts = ? AND end_sk >= ?)
  auto stmt = db.prepare(
      "SELECT "
      "s.metadata as metadata, "
      "sb.sequence as block_sequence, "
      "sb.block_idx as block_idx, "
      "sb.start_timestamp as block_start_timestamp, "
      "sb.end_timestamp as block_end_timestamp, "
      "sb.uuid as uuid "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE s.stream_tag = ? "
      "AND (sb.start_timestamp < ? OR "
      "     (sb.start_timestamp = ? AND sb.start_secondary_key <= ?)) "
      "AND (sb.end_timestamp IS NULL OR sb.end_timestamp > ? OR "
      "     (sb.end_timestamp = ? AND sb.end_secondary_key >= ?)) "
      "ORDER BY sb.sequence ASC;");
  auto results =
      stmt.bind(1, stream_tag)
          .bind(2, end_timestamp).bind(3, end_timestamp).bind(4, end_secondary_key)
          .bind(5, start_timestamp).bind(6, start_timestamp).bind(7, start_secondary_key)
          .exec();

  bool need_binary_search = true;

  for (auto& row : results) {
    std::string metadata = (row["metadata"])?row["metadata"].value():std::string();
    int64_t block_sequence = std::stoll(row["block_sequence"].value());
    int64_t block_idx = std::stoll(row["block_idx"].value());
    std::string uuid_hex = row["uuid"].value();

    uint8_t uuid[16];
    s_to_entropy_id(uuid_hex, uuid);

    auto mm = nts_memory_map(
        filenum(_file), FILE_HEADER_BLOCK_SIZE + (block_idx * _block_size),
        _block_size, nts_memory_map::NMM_PROT_READ,
        nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

    auto block_p = (uint8_t*)mm.map();

    auto valid_counter = (uint32_t*)(block_p + 8);

    uint32_t n_valid_indexes =
        _load_committed_index_count(valid_counter);

    uint8_t* index_start = block_p + BLOCK_HEADER_SIZE;
    uint8_t* index_end = index_start + (n_valid_indexes * INDEX_ENTRY_SIZE);

    int64_t start_index = 0;

    if (need_binary_search) {
      // Composite lower_bound at (start_timestamp, start_secondary_key).
      int64_t target[2] = {start_timestamp, start_secondary_key};
      uint8_t* first_entry =
          lower_bound_bytes(index_start, index_end, (uint8_t*)target,
                            INDEX_ENTRY_SIZE, _compare_index_entry_composite);

      start_index = (first_entry - index_start) / INDEX_ENTRY_SIZE;
      need_binary_search = false;
    }

    // Iterate through frames in this block
    for (size_t i = start_index; i < n_valid_indexes; i++) {
      uint8_t* index_p = block_p + BLOCK_HEADER_SIZE + (i * INDEX_ENTRY_SIZE);
      int64_t timestamp = *(int64_t*)(index_p + INDEX_ENTRY_TS_OFFSET);
      int64_t sk_index  = *(int64_t*)(index_p + INDEX_ENTRY_SECKEY_OFFSET);
      uint64_t offset   = *(uint64_t*)(index_p + INDEX_ENTRY_OFFSET_OFFSET);

      // Check if we've passed the end composite.
      if (timestamp > end_timestamp ||
          (timestamp == end_timestamp && sk_index > end_secondary_key))
        return;  // All done!

      // Validate frame header
      uint32_t flags;
      uint32_t frame_size;
      int64_t sec_key;
      if (!_validate_frame_header(block_p + offset, uuid, &flags,
                                  &frame_size, &sec_key)) {
        // Log warning? Skip corrupted frame
        continue;
      }

      // Callback with frame data
      callback(block_p + offset + FRAME_HEADER_SIZE, (size_t)frame_size, flags,
               timestamp, sec_key, block_sequence, metadata);
    }
  }
}

std::vector<std::string> nanots_reader::query_stream_tags(
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key) {
  nts_sqlite_conn db(_database_name(_file_name), false, true);

  // Composite overlap: a block contributes if its lex range overlaps the
  // requested window. Same predicate as nanots_reader::read.
  auto stmt = db.prepare(
      "SELECT DISTINCT s.stream_tag "
      "FROM segments s "
      "JOIN segment_blocks sb ON s.id = sb.segment_id "
      "WHERE (sb.start_timestamp < ? OR "
      "       (sb.start_timestamp = ? AND sb.start_secondary_key <= ?)) "
      "AND (sb.end_timestamp IS NULL OR sb.end_timestamp > ? OR "
      "     (sb.end_timestamp = ? AND sb.end_secondary_key >= ?));");
  auto results =
      stmt.bind(1, end_timestamp).bind(2, end_timestamp).bind(3, end_secondary_key)
          .bind(4, start_timestamp).bind(5, start_timestamp).bind(6, start_secondary_key)
          .exec();

  std::vector<std::string> stream_tags;

  for (auto& row : results) {
      stream_tags.push_back(row["stream_tag"].value());
  }

  return stream_tags;
}

std::vector<contiguous_segment> nanots_reader::query_contiguous_segments(
    const std::string& stream_tag,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key) {
  nts_sqlite_conn db(_database_name(_file_name), false, true);

  // Create a grouping key by subtracting sequence from row number; within
  // a stream blocks are composite-monotonic, so contiguous-by-sequence is
  // also contiguous-by-composite. For each group we want the first block's
  // (start_ts, start_sk) and the last block's (end_ts, end_sk).
  //
  // Composite overlap predicate matches nanots_reader::read.
  auto stmt = db.prepare(
    "WITH contiguous_groups AS ( "
    "  SELECT "
    "    sb.segment_id, "
    "    sb.sequence, "
    "    sb.start_timestamp, "
    "    sb.start_secondary_key, "
    "    sb.end_timestamp, "
    "    sb.end_secondary_key, "
    "    ROW_NUMBER() OVER (PARTITION BY sb.segment_id ORDER BY sb.sequence) "
    "      - sb.sequence AS group_key "
    "  FROM segment_blocks sb "
    "  JOIN segments s ON sb.segment_id = s.id "
    "  WHERE s.stream_tag = ? "                       /* bind(1) */
    "    AND (sb.start_timestamp < ? OR "             /* bind(2,3,4) — end composite */
    "         (sb.start_timestamp = ? AND sb.start_secondary_key <= ?)) "
    "    AND (sb.end_timestamp IS NULL OR sb.end_timestamp > ? OR "  /* bind(5,6,7) — start composite */
    "         (sb.end_timestamp = ? AND sb.end_secondary_key >= ?)) "
    "), "
    "region_boundaries AS ( "
    "  SELECT "
    "    segment_id, "
    "    group_key, "
    "    FIRST_VALUE(start_timestamp) "
    "      OVER w AS region_start_ts, "
    "    FIRST_VALUE(start_secondary_key) "
    "      OVER w AS region_start_sk, "
    "    LAST_VALUE(end_timestamp) "
    "      OVER w_full AS region_end_ts, "
    "    LAST_VALUE(end_secondary_key) "
    "      OVER w_full AS region_end_sk, "
    "    COUNT(*) OVER (PARTITION BY segment_id, group_key) AS block_count "
    "  FROM contiguous_groups "
    "  WINDOW "
    "    w AS (PARTITION BY segment_id, group_key ORDER BY sequence), "
    "    w_full AS (PARTITION BY segment_id, group_key ORDER BY sequence "
    "               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
    ") "
    "SELECT DISTINCT "
    "  segment_id, "
    "  region_start_ts, "
    "  region_start_sk, "
    "  region_end_ts, "
    "  region_end_sk, "
    "  block_count "
    "FROM region_boundaries "
    "ORDER BY segment_id, region_start_ts;"
  );
  auto results =
      stmt.bind(1, stream_tag)
          .bind(2, end_timestamp).bind(3, end_timestamp).bind(4, end_secondary_key)
          .bind(5, start_timestamp).bind(6, start_timestamp).bind(7, start_secondary_key)
          .exec();

  std::vector<contiguous_segment> segments;

  for (auto& row : results) {
    contiguous_segment segment;
    segment.segment_id = std::stoll(row["segment_id"].value());
    segment.start_timestamp = std::stoll(row["region_start_ts"].value());
    // Preserve the existing public API convention for a live/open region;
    // internally and in SQLite the state is now represented explicitly.
    segment.end_timestamp = row["region_end_ts"].has_value()
        ? std::stoll(row["region_end_ts"].value()) : 0;
    auto& s_sk = row["region_start_sk"];
    auto& e_sk = row["region_end_sk"];
    segment.start_secondary_key = s_sk.has_value()
        ? std::stoll(s_sk.value()) : NANOTS_SEC_KEY_UNSET;
    segment.end_secondary_key = e_sk.has_value()
        ? std::stoll(e_sk.value()) : NANOTS_SEC_KEY_UNSET;
    segments.push_back(segment);
  }

  return segments;
}

nanots_iterator::nanots_iterator(const std::string& file_name,
                                 const std::string& stream_tag)
    : _file_name(file_name),
      _stream_tag(stream_tag),
      _file(nts_file::open(file_name, "r")),
      _current_block_sequence(0),
      _current_segment_id(0),
      _current_frame_idx(0),
      _current_block_start_ts(0),
      _current_block_end_ts(std::nullopt),
      _current_block_start_sk(NANOTS_SEC_KEY_UNSET),
      _current_block_end_sk(std::nullopt),
      _valid(false),
      _initialized(false),
      _slot_guard(nanots_epoch_registry::get_or_create(file_name)) {
  // Read block size + validate magic/version from file header.
  auto header_mm = nts_memory_map(
      filenum(_file), 0, FILE_HEADER_BLOCK_SIZE, nts_memory_map::NMM_PROT_READ,
      nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

  auto header_p = (uint8_t*)header_mm.map();

  if (memcmp(header_p + FILE_HEADER_MAGIC_OFFSET,
             NANOTS_FILE_MAGIC, NANOTS_FILE_MAGIC_LEN) != 0)
    throw nanots_exception(NANOTS_EC_BAD_MAGIC,
                           "Not a nanots v2 file (bad magic).",
                           __FILE__, __LINE__);
  uint16_t version = *(uint16_t*)(header_p + FILE_HEADER_VERSION_OFFSET);
  if (version != NANOTS_FORMAT_VERSION)
    throw nanots_exception(NANOTS_EC_BAD_VERSION,
                           "Unsupported nanots format version.",
                           __FILE__, __LINE__);

  _block_size = *(uint32_t*)(header_p + FILE_HEADER_BLOCK_SIZE_OFFSET);

  _upgrade_db_for_reader(_file_name);

  // Initialize to first frame if stream exists
  reset();
}

nts_sqlite_conn& nanots_iterator::_ensure_db_connection() {
  if (!_db_conn.has_value()) {
    auto db_name = _database_name(_file_name);
    _db_conn.emplace(db_name, false, true);
  }
  return _db_conn.value();
}

block_info* nanots_iterator::_get_block_by_segment_and_sequence(int64_t segment_id, int64_t sequence) {
  std::string cache_key = std::to_string(segment_id) + ":" + std::to_string(sequence);
  auto it = _block_cache.find(cache_key);
  if (it != _block_cache.end()) {
    return &it->second;
  }

  // Query database for this specific block
  if (!_stmt_get_block_by_id.has_value())
    _stmt_get_block_by_id.emplace(_ensure_db_connection().prepare(
        "SELECT "
        "s.metadata as metadata, "
        "sb.segment_id as segment_id, "
        "sb.sequence as block_sequence, "
        "sb.block_idx as block_idx, "
        "sb.start_timestamp as start_timestamp, "
        "sb.end_timestamp as end_timestamp, "
        "sb.start_secondary_key as start_secondary_key, "
        "sb.end_secondary_key as end_secondary_key, "
        "sb.uuid as uuid "
        "FROM segments s "
        "JOIN segment_blocks sb ON sb.segment_id = s.id "
        "WHERE sb.segment_id = ? AND sb.sequence = ?"));

  auto& stmt = *_stmt_get_block_by_id;
  stmt.reset();
  auto results = stmt.bind(1, segment_id).bind(2, sequence).exec();

  if (results.empty()) {
    return nullptr;
  }

  auto& row = results[0];
  block_info block;
  block.block_idx = std::stoll(row["block_idx"].value());
  block.block_sequence = std::stoll(row["block_sequence"].value());
  block.segment_id = std::stoll(row["segment_id"].value());
  // Metadata may legitimately be the empty string, which the sqlite wrapper
  // reports as a missing optional.
  block.metadata = row["metadata"].has_value() ? row["metadata"].value() : std::string();
  block.uuid_hex = row["uuid"].value();
  block.start_timestamp = std::stoll(row["start_timestamp"].value());
  auto& end_ts = row["end_timestamp"];
  block.end_timestamp = end_ts.has_value()
      ? std::optional<int64_t>(std::stoll(end_ts.value())) : std::nullopt;
  auto& sk_start = row["start_secondary_key"];
  block.start_secondary_key = sk_start.has_value()
      ? std::stoll(sk_start.value()) : NANOTS_SEC_KEY_UNSET;
  auto& sk_end = row["end_secondary_key"];
  block.end_secondary_key = sk_end.has_value()
      ? std::optional<int64_t>(std::stoll(sk_end.value())) : std::nullopt;

  auto result = _block_cache.emplace(cache_key, std::move(block));
  return &result.first->second;
}

// ---------------------------------------------------------------------------
// Timestamp index helpers
// ---------------------------------------------------------------------------

void nanots_iterator::_ensure_ts_index() {
  if (!_ts_index_filled) _refresh_ts_index();
}

void nanots_iterator::_refresh_ts_index() {
  auto& db = _ensure_db_connection();
  // Single query for the entire stream's block list. Blocks within a stream
  // have strictly monotonic start_ts (writer enforces non-decreasing
  // timestamps via NANOTS_EC_NON_MONOTONIC_TIMESTAMP), so the result is
  // already in start_ts order — no sort needed.
  auto stmt = db.prepare(
      "SELECT sb.segment_id, sb.sequence, "
      "       sb.start_timestamp, sb.end_timestamp, "
      "       sb.start_secondary_key, sb.end_secondary_key "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE s.stream_tag = ? "
      "ORDER BY s.id ASC, sb.sequence ASC");
  auto results = stmt.bind(1, _stream_tag).exec();

  _ts_index.clear();
  _ts_index.reserve(results.size());
  for (auto& row : results) {
    BlockRange br;
    br.segment_id = std::stoll(row["segment_id"].value());
    br.sequence   = std::stoll(row["sequence"].value());
    br.start_ts   = std::stoll(row["start_timestamp"].value());
    auto& e_ts = row["end_timestamp"];
    br.end_ts = e_ts.has_value()
        ? std::optional<int64_t>(std::stoll(e_ts.value())) : std::nullopt;
    auto& s_sk = row["start_secondary_key"];
    auto& e_sk = row["end_secondary_key"];
    br.start_sk = s_sk.has_value() ? std::stoll(s_sk.value())
                                   : NANOTS_SEC_KEY_UNSET;
    br.end_sk = e_sk.has_value()
        ? std::optional<int64_t>(std::stoll(e_sk.value())) : std::nullopt;
    _ts_index.push_back(br);
  }
  _ts_index_filled = true;
}


size_t nanots_iterator::_ts_index_find(int64_t ts, int64_t sk) const {
  // Composite-key search across blocks. Each block holds a sorted run of
  // (ts, sk) tuples; the block's lex range is [(start_ts, start_sk),
  // (end_ts, end_sk)]. We want the block whose range covers the target
  // composite, or — failing that — the first block whose start composite
  // is >= the target.
  using P = std::pair<int64_t, int64_t>;
  auto br_start = [](const BlockRange& br) { return P{br.start_ts, br.start_sk}; };
  auto cmp_lt = [](P a, P b) {
    return a.first != b.first ? a.first < b.first : a.second < b.second;
  };

  P target{ts, sk};
  // First block whose start composite > target.
  auto it = std::upper_bound(_ts_index.begin(), _ts_index.end(), target,
      [&](P t, const BlockRange& br) { return cmp_lt(t, br_start(br)); });

  if (it != _ts_index.begin()) {
    auto prev = std::prev(it);
    P prev_start = br_start(*prev);
    bool ge_start = !cmp_lt(target, prev_start);  // target >= prev_start
    bool open_block = !prev->end_ts.has_value();
    P prev_end{prev->end_ts.value_or(0),
               prev->end_sk.value_or(NANOTS_SEC_KEY_UNSET)};
    bool le_end = open_block || !cmp_lt(prev_end, target);  // target <= prev_end
    if (ge_start && le_end) {
      return static_cast<size_t>(std::distance(_ts_index.begin(), prev));
    }
  }

  // Fallback: first block whose start composite >= target.
  if (it != _ts_index.end()) {
    return static_cast<size_t>(std::distance(_ts_index.begin(), it));
  }
  return _ts_index.size();
}

bool nanots_iterator::_ts_index_pos_is_valid() const {
  return _ts_index_pos < _ts_index.size() &&
         _ts_index[_ts_index_pos].segment_id == _current_segment_id &&
         _ts_index[_ts_index_pos].sequence   == _current_block_sequence;
}

bool nanots_iterator::_ts_index_relocate() {
  for (size_t i = 0; i < _ts_index.size(); ++i) {
    if (_ts_index[i].segment_id == _current_segment_id &&
        _ts_index[i].sequence   == _current_block_sequence) {
      _ts_index_pos = i;
      return true;
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Block navigation (uses _ts_index for in-memory lookup, falls back to
// SQL refresh on miss).
// ---------------------------------------------------------------------------

block_info* nanots_iterator::_get_first_block() {
  _ensure_ts_index();
  if (_ts_index.empty()) {
    _refresh_ts_index();
    if (_ts_index.empty()) return nullptr;
  }
  const auto& br = _ts_index.front();
  auto* block = _get_block_by_segment_and_sequence(br.segment_id, br.sequence);
  if (block) _ts_index_pos = 0;
  return block;
}

block_info* nanots_iterator::_get_last_block() {
  _ensure_ts_index();
  if (_ts_index.empty()) {
    _refresh_ts_index();
    if (_ts_index.empty()) return nullptr;
  }
  size_t pos = _ts_index.size() - 1;
  const auto& br = _ts_index[pos];
  auto* block = _get_block_by_segment_and_sequence(br.segment_id, br.sequence);
  if (block) _ts_index_pos = pos;
  return block;
}

block_info* nanots_iterator::_get_next_block() {
  _ensure_ts_index();

  // Verify our cached position; relocate if stale (e.g. after refresh).
  if (!_ts_index_pos_is_valid() && !_ts_index_relocate()) {
    _refresh_ts_index();
    if (!_ts_index_relocate()) return nullptr;
  }

  // Try advancing within the current snapshot.
  if (_ts_index_pos + 1 < _ts_index.size()) {
    const auto& br = _ts_index[_ts_index_pos + 1];
    auto* block = _get_block_by_segment_and_sequence(br.segment_id, br.sequence);
    if (block) {
      _ts_index_pos++;
      return block;
    }
  }

  // Past the cached end OR cache-stale at the tail: refresh and retry once.
  _refresh_ts_index();
  if (!_ts_index_relocate()) return nullptr;
  if (_ts_index_pos + 1 >= _ts_index.size()) return nullptr;
  const auto& br = _ts_index[_ts_index_pos + 1];
  auto* block = _get_block_by_segment_and_sequence(br.segment_id, br.sequence);
  if (block) _ts_index_pos++;
  return block;
}

block_info* nanots_iterator::_get_prev_block() {
  _ensure_ts_index();

  if (!_ts_index_pos_is_valid() && !_ts_index_relocate()) {
    _refresh_ts_index();
    if (!_ts_index_relocate()) return nullptr;
  }

  if (_ts_index_pos == 0) return nullptr;
  const auto& br = _ts_index[_ts_index_pos - 1];
  auto* block = _get_block_by_segment_and_sequence(br.segment_id, br.sequence);
  if (block) _ts_index_pos--;
  return block;
}

block_info* nanots_iterator::_find_block_for_composite(int64_t timestamp,
                                                       int64_t sec_key) {
  _ensure_ts_index();

  // Helper: given a position in _ts_index, look up its block_info. Returns
  // nullptr if the block was reclaimed (catalog row gone) since we snapshotted.
  auto try_at = [&](size_t pos) -> block_info* {
    if (pos >= _ts_index.size()) return nullptr;
    const auto& br = _ts_index[pos];
    auto* b = _get_block_by_segment_and_sequence(br.segment_id, br.sequence);
    if (b) _ts_index_pos = pos;
    return b;
  };

  // First attempt: snapshot we already have.
  size_t pos = _ts_index_find(timestamp, sec_key);
  if (auto* b = try_at(pos)) return b;

  // Miss or stale: refresh and try once more. This catches:
  //   (a) writer added blocks past the cached end
  //   (b) writer reclaimed the block we matched and the new (segment, sequence)
  //       at that idx isn't in our cache
  _refresh_ts_index();
  pos = _ts_index_find(timestamp, sec_key);
  return try_at(pos);
}

bool nanots_iterator::_load_block_data(block_info& block) {
  if (block.is_loaded)
    return true;

  // Memory map the block
  block.mm = nts_memory_map(
      filenum(_file), FILE_HEADER_BLOCK_SIZE + (block.block_idx * _block_size),
      _block_size, nts_memory_map::NMM_PROT_READ,
      nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

  block.block_p = (uint8_t*)block.mm.map();

  // Convert UUID std::string to bytes
  s_to_entropy_id(block.uuid_hex, block.uuid);

  block.is_loaded = true;
  return _refresh_committed_index_count(block);
}

bool nanots_iterator::_refresh_committed_index_count(block_info& block) {
  if (!block.is_loaded || !block.block_p)
    return false;

  auto* valid_counter = reinterpret_cast<uint32_t*>(block.block_p + 8);
  uint32_t observed = _load_committed_index_count(valid_counter);
  uint64_t max_indexes =
      (_block_size - BLOCK_HEADER_SIZE) / INDEX_ENTRY_SIZE;

  // A live block's committed prefix can only grow. A decrease means the
  // physical block was recycled unexpectedly; an oversized count is corrupt.
  if (observed < block.n_valid_indexes || observed > max_indexes)
    return false;

  block.n_valid_indexes = observed;
  return true;
}

bool nanots_iterator::_load_current_frame() {
  auto* block = _get_block_by_segment_and_sequence(_current_segment_id, _current_block_sequence);
  if (!block) {
    _valid = false;
    return false;
  }

  if (!_load_block_data(*block)) {
    _valid = false;
    return false;
  }

  if (_current_frame_idx >= block->n_valid_indexes) {
    _valid = false;
    return false;
  }

  // Get frame info from index
  uint8_t* index_p = block->block_p + BLOCK_HEADER_SIZE +
                     (_current_frame_idx * INDEX_ENTRY_SIZE);
  int64_t timestamp = *(int64_t*)(index_p + INDEX_ENTRY_TS_OFFSET);
  uint64_t offset   = *(uint64_t*)(index_p + INDEX_ENTRY_OFFSET_OFFSET);

  // Validate frame header
  uint32_t flags;
  uint32_t frame_size;
  int64_t sec_key;
  if (!_validate_frame_header(block->block_p + offset, block->uuid, &flags,
                              &frame_size, &sec_key)) {
    _valid = false;
    return false;
  }

  // Set up current frame
  _current_frame.data = block->block_p + offset + FRAME_HEADER_SIZE;
  _current_frame.size = frame_size;
  _current_frame.flags = flags;
  _current_frame.timestamp = timestamp;
  _current_frame.secondary_key = sec_key;
  _current_frame.block_sequence = block->block_sequence;

  _valid = true;
  return true;
}

void nanots_iterator::_select_block(const block_info& block,
                                    size_t frame_idx) {
  _current_segment_id = block.segment_id;
  _current_block_sequence = block.block_sequence;
  _current_frame_idx = frame_idx;
  _current_block_start_ts = block.start_timestamp;
  _current_block_end_ts = block.end_timestamp;
  _current_block_start_sk = block.start_secondary_key;
  _current_block_end_sk = block.end_secondary_key;
}

nanots_iterator& nanots_iterator::operator++() {
  nanots_op_scope _op(_slot_guard);
  if (!_valid)
    return *this;

  auto* current_block = _get_block_by_segment_and_sequence(_current_segment_id, _current_block_sequence);
  if (!current_block || !_load_block_data(*current_block)) {
    _valid = false;
    return *this;
  }

  _current_frame_idx++;

  // The mapped block is live. Only pay for a synchronized counter read when
  // advancing would leave the committed prefix we previously observed.
  if (_current_frame_idx >= current_block->n_valid_indexes &&
      !current_block->end_timestamp.has_value()) {
    if (!_refresh_committed_index_count(*current_block)) {
      _valid = false;
      return *this;
    }
  }

  // If the refreshed count still has no next frame, move to the next block.
  if (_current_frame_idx >= current_block->n_valid_indexes) {
    auto* next_block = _get_next_block();
    if (!next_block) {
      _valid = false;
      return *this;
    }

    _select_block(*next_block, 0);
  }

  _load_current_frame();
  return *this;
}

nanots_iterator& nanots_iterator::operator--() {
  nanots_op_scope _op(_slot_guard);
  if (!_valid)
    return *this;

  if (_current_frame_idx == 0) {
    // Need to go to previous block
    auto* prev_block = _get_prev_block();
    if (!prev_block) {
      _valid = false;
      return *this;
    }

    if (!_load_block_data(*prev_block)) {
      _valid = false;
      return *this;
    }

    _select_block(
        *prev_block,
        (prev_block->n_valid_indexes > 0) ? prev_block->n_valid_indexes - 1 : 0);
  } else
    _current_frame_idx--;

  _load_current_frame();
  return *this;
}

bool nanots_iterator::find(int64_t timestamp, int64_t secondary_key) {
  nanots_op_scope _op(_slot_guard);
  block_info* block = nullptr;

  // Fast path: if the target is within the already-loaded block's range
  // (composite), skip the SQL lookup entirely.
  using composite_key = std::pair<int64_t, int64_t>;
  const composite_key target_key{timestamp, secondary_key};
  const composite_key current_start{
      _current_block_start_ts, _current_block_start_sk};
  const composite_key current_end{
      _current_block_end_ts.value_or(0),
      _current_block_end_sk.value_or(NANOTS_SEC_KEY_UNSET)};
  if (_valid && _current_block_end_ts.has_value() &&
      _current_block_end_sk.has_value() &&
      target_key >= current_start && target_key <= current_end) {
    block = _get_block_by_segment_and_sequence(_current_segment_id, _current_block_sequence);
  }

  if (!block)
    block = _find_block_for_composite(timestamp, secondary_key);

  if (!block) {
    _valid = false;
    return false;
  }

  if (!_load_block_data(*block)) {
    _valid = false;
    return false;
  }

  _select_block(*block, 0);

  int64_t target[2] = {timestamp, secondary_key};
  auto find_in_committed_prefix = [&]() {
    uint8_t* index_start = block->block_p + BLOCK_HEADER_SIZE;
    uint8_t* index_end =
        index_start + (block->n_valid_indexes * INDEX_ENTRY_SIZE);
    uint8_t* found_entry =
        lower_bound_bytes(index_start, index_end, (uint8_t*)target,
                          INDEX_ENTRY_SIZE, _compare_index_entry_composite);
    _current_frame_idx =
        (found_entry - index_start) / INDEX_ENTRY_SIZE;
  };

  find_in_committed_prefix();

  // A miss at the cached end of an open block may simply mean the writer
  // appended since we last observed its committed count. Refresh on demand
  // and repeat the binary search only if the prefix grew.
  if (_current_frame_idx >= block->n_valid_indexes &&
      !block->end_timestamp.has_value()) {
    uint32_t cached_count = block->n_valid_indexes;
    if (!_refresh_committed_index_count(*block)) {
      _valid = false;
      return false;
    }
    if (block->n_valid_indexes > cached_count)
      find_in_committed_prefix();
  }

  // If we didn't find it in this block, try next block
  if (_current_frame_idx >= block->n_valid_indexes) {
    auto* next_block = _get_next_block();
    if (!next_block) {
      _valid = false;
      return false;
    }

    _select_block(*next_block, 0);
  }

  return _load_current_frame();
}

void nanots_iterator::reset() {
  nanots_op_scope _op(_slot_guard);
  auto* first_block = _get_first_block();
  if (!first_block) {
    _valid = false;
    return;
  }

  _select_block(*first_block, 0);
  _load_current_frame();
}

bool nanots_iterator::seek_end() {
  nanots_op_scope _op(_slot_guard);
  // A writer may have rolled the live tail into a block created after this
  // iterator cached the stream's block list.
  _refresh_ts_index();
  auto* block = _get_last_block();
  if (!block) {
    _valid = false;
    return false;
  }

  if (!_load_block_data(*block)) {
    _valid = false;
    return false;
  }

  if (!block->end_timestamp.has_value() &&
      !_refresh_committed_index_count(*block)) {
    _valid = false;
    return false;
  }

  if (block->n_valid_indexes == 0) {
    _valid = false;
    return false;
  }

  _select_block(*block, block->n_valid_indexes - 1);
  return _load_current_frame();
}

const std::string& nanots_iterator::current_metadata() const {
  std::string cache_key = std::to_string(_current_segment_id) + ":" + std::to_string(_current_block_sequence);
  auto it = _block_cache.find(cache_key);
  if (it != _block_cache.end()) {
    return it->second.metadata;
  }
  
  static std::string empty_string;
  return empty_string;
}

extern "C" {

struct nanots_writer_handle {
  nanots_writer* writer;
  nanots_writer_handle(nanots_writer* w) : writer(w) {}
  ~nanots_writer_handle() { delete writer; }
};

struct nanots_write_context_handle {
  write_context context;
  nanots_write_context_handle(write_context&& c) : context(std::move(c)) {}
};

struct nanots_reader_handle {
  nanots_reader* reader;
  std::vector<std::string> cached_stream_tags;
  size_t stream_tags_iterator;
  nanots_reader_handle(nanots_reader* r) : reader(r), stream_tags_iterator(0) {}
  ~nanots_reader_handle() { delete reader; }
};

struct nanots_iterator_handle {
  nanots_iterator* iterator;
  nanots_iterator_handle(nanots_iterator* i) : iterator(i) {}
  ~nanots_iterator_handle() { delete iterator; }
};

nanots_ec_t nanots_writer_allocate_file(const char* file_name, uint32_t block_size, uint32_t n_blocks) {
  nanots_ec_t ec = nanots_ec_t::NANOTS_EC_OK;
  try {
    nanots_writer::allocate(std::string(file_name), block_size, n_blocks);
  } catch (const nanots_exception& e) {
    ec = e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_writer_allocate_file: %s\n", e.what());
    ec = NANOTS_EC_UNKNOWN;
  } catch (...) {
    ec = NANOTS_EC_UNKNOWN;
  }
  if(ec != NANOTS_EC_OK) {
    fprintf(stderr,"Error in nanots_writer_allocate_file: %d\n", ec);
  }
  return ec;
}

nanots_ec_t nanots_writer_allocate_growable_file(const char* file_name, uint32_t block_size, uint32_t max_blocks) {
  nanots_ec_t ec = nanots_ec_t::NANOTS_EC_OK;
  try {
    nanots_writer::allocate_growable(std::string(file_name), block_size, max_blocks);
  } catch (const nanots_exception& e) {
    ec = e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_writer_allocate_growable_file: %s\n", e.what());
    ec = NANOTS_EC_UNKNOWN;
  } catch (...) {
    ec = NANOTS_EC_UNKNOWN;
  }
  if(ec != NANOTS_EC_OK) {
    fprintf(stderr,"Error in nanots_writer_allocate_growable_file: %d\n", ec);
  }
  return ec;
}

nanots_writer_t nanots_writer_create(const char* file_name, int auto_reclaim) {
  try {
    auto* writer = new nanots_writer(std::string(file_name), auto_reclaim != 0);
    return new nanots_writer_handle(writer);
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_writer_create: %s\n", e.what());
    return nullptr;
  } catch (...) {
    return nullptr;
  }
}

void nanots_writer_destroy(nanots_writer_t writer) {
  delete writer;
}

nanots_write_context_t nanots_writer_create_context(nanots_writer_t writer,
                                                    const char* stream_tag,
                                                    const char* metadata) {
  if (!writer || !writer->writer) {
    return nullptr;
  }

  try {
    auto context = writer->writer->create_write_context(std::string(stream_tag),
                                                        std::string(metadata));
    return new nanots_write_context_handle(std::move(context));
  } catch (const nanots_exception& e) {
    fprintf(stderr,"Error in nanots_writer_create_context: %d", e.get_ec());
    return nullptr;
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_writer_create_context: %s\n", e.what());
    return nullptr;
  } catch (...) {
    return nullptr;
  }
}

void nanots_write_context_destroy(nanots_write_context_t context) {
  delete context;
}

nanots_ec_t nanots_writer_write(nanots_writer_t writer,
                                nanots_write_context_t context,
                                const uint8_t* data,
                                size_t size,
                                uint32_t flags,
                                int64_t timestamp,
                                int64_t secondary_key) {
  if (!writer || !writer->writer) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }
  if (!context) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    writer->writer->write(context->context, data, size,
                          flags, timestamp, secondary_key);
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_writer_write: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_writer_write\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_ec_t nanots_writer_free_blocks(const char* file_name,
                                          const char* stream_tag,
                                          int64_t start_timestamp,
                                          int64_t start_secondary_key,
                                          int64_t end_timestamp,
                                          int64_t end_secondary_key) {
  if (!file_name || !stream_tag) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    nanots_writer::free_blocks(std::string(file_name), std::string(stream_tag),
                               start_timestamp, start_secondary_key,
                               end_timestamp, end_secondary_key);
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_writer_free_blocks: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_writer_free_blocks\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_reader_t nanots_reader_create(const char* file_name) {
  try {
    auto* reader = new nanots_reader(std::string(file_name));
    return new nanots_reader_handle(reader);
  } catch (const nanots_exception& e) {
    fprintf(stderr,"Exception in nanots_reader_create: %d", e.get_ec());
    return nullptr;
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_reader_create: %s\n", e.what());
    return nullptr;
  } catch (...) {
    return nullptr;
  }
}

void nanots_reader_destroy(nanots_reader_t reader) {
  delete reader;
}

struct nanots_callback_context {
  nanots_read_callback_t callback;
  void* user_data;
};

nanots_ec_t nanots_reader_read(nanots_reader_t reader,
                               const char* stream_tag,
                               int64_t start_timestamp,
                               int64_t start_secondary_key,
                               int64_t end_timestamp,
                               int64_t end_secondary_key,
                               nanots_read_callback_t callback,
                               void* user_data) {
  if (!reader || !reader->reader) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }
  if (!callback) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    nanots_callback_context ctx{callback, user_data};
    reader->reader->read(std::string(stream_tag),
                         start_timestamp, start_secondary_key,
                         end_timestamp, end_secondary_key,
                         [&ctx](const uint8_t* data, size_t size, uint32_t flags,
                                int64_t timestamp, int64_t secondary_key,
                                int64_t block_sequence, const std::string& metadata) {
                           ctx.callback(data, size, flags, timestamp, secondary_key,
                                        block_sequence, metadata.c_str(), ctx.user_data);
                         });
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_reader_read: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_reader_read\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_ec_t nanots_reader_query_contiguous_segments(
    nanots_reader_t reader,
    const char* stream_tag,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key,
    nanots_contiguous_segment_t** segments,
    size_t* count) {
  if (!reader || !reader->reader) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }
  if (!segments || !count) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    auto cpp_segments = reader->reader->query_contiguous_segments(
        std::string(stream_tag),
        start_timestamp, start_secondary_key,
        end_timestamp, end_secondary_key);

    *count = cpp_segments.size();
    if (*count == 0) {
      *segments = nullptr;
      return NANOTS_EC_OK;
    }

    *segments = (nanots_contiguous_segment_t*)malloc(
        *count * sizeof(nanots_contiguous_segment_t));
    if (!*segments) {
      return NANOTS_EC_UNKNOWN;
    }

    for (size_t i = 0; i < *count; i++) {
      (*segments)[i].segment_id = cpp_segments[i].segment_id;
      (*segments)[i].start_timestamp = cpp_segments[i].start_timestamp;
      (*segments)[i].start_secondary_key = cpp_segments[i].start_secondary_key;
      (*segments)[i].end_timestamp = cpp_segments[i].end_timestamp;
      (*segments)[i].end_secondary_key = cpp_segments[i].end_secondary_key;
    }

    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_reader_query_contiguous_segments: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_reader_query_contiguous_segments\n");
    return NANOTS_EC_UNKNOWN;
  }
}

void nanots_free_contiguous_segments(nanots_contiguous_segment_t* segments) {
  free(segments);
}

nanots_ec_t nanots_reader_query_stream_tags_start(nanots_reader_t reader,
                                                  int64_t start_timestamp,
                                                  int64_t start_secondary_key,
                                                  int64_t end_timestamp,
                                                  int64_t end_secondary_key) {
  if (!reader || !reader->reader) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    reader->cached_stream_tags = reader->reader->query_stream_tags(
        start_timestamp, start_secondary_key,
        end_timestamp, end_secondary_key);
    reader->stream_tags_iterator = 0;
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_reader_query_stream_tags_start: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_reader_query_stream_tags_start\n");
    return NANOTS_EC_UNKNOWN;
  }
}

const char* nanots_reader_query_stream_tags_next(nanots_reader_t reader) {
  if (!reader || !reader->reader) {
    return nullptr;
  }

  if (reader->stream_tags_iterator >= reader->cached_stream_tags.size()) {
    return nullptr;
  }

  const char* result = reader->cached_stream_tags[reader->stream_tags_iterator].c_str();
  reader->stream_tags_iterator++;
  return result;
}

nanots_iterator_t nanots_iterator_create(const char* file_name,
                                         const char* stream_tag) {
  try {
    auto* iterator =
        new nanots_iterator(std::string(file_name), std::string(stream_tag));
    return new nanots_iterator_handle(iterator);
  } catch (const nanots_exception& e) {
    fprintf(stderr,"Error in nanots_iterator_create: %d", e.get_ec());
    return nullptr;
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_create: %s\n", e.what());
    return nullptr;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_create\n");
    return nullptr;
  }
}

void nanots_iterator_destroy(nanots_iterator_t iterator) {
  delete iterator;
}

int nanots_iterator_valid(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return 0;
  }
  return iterator->iterator->valid() ? 1 : 0;
}

nanots_ec_t nanots_iterator_get_current_frame(
    nanots_iterator_t iterator,
    nanots_frame_info_t* frame_info) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }
  if (!frame_info) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }
  if (!iterator->iterator->valid()) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    const auto& frame = iterator->iterator->operator*();
    frame_info->data = frame.data;
    frame_info->size = frame.size;
    frame_info->flags = frame.flags;
    frame_info->timestamp = frame.timestamp;
    frame_info->secondary_key = frame.secondary_key;
    frame_info->block_sequence = frame.block_sequence;
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_get_current_frame: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_get_current_frame\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_ec_t nanots_iterator_next(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    ++(*iterator->iterator);
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_next: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_next\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_ec_t nanots_iterator_prev(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    --(*iterator->iterator);
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_prev: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_prev\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_ec_t nanots_iterator_find(nanots_iterator_t iterator,
                                 int64_t timestamp,
                                 int64_t secondary_key) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    bool found = iterator->iterator->find(timestamp, secondary_key);
    return found ? NANOTS_EC_OK : NANOTS_EC_INVALID_ARGUMENT;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_find: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_find\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_ec_t nanots_iterator_reset(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    iterator->iterator->reset();
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_reset: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_reset\n");
    return NANOTS_EC_UNKNOWN;
  }
}

nanots_ec_t nanots_iterator_seek_end(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    iterator->iterator->seek_end();
    return NANOTS_EC_OK;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_seek_end: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_seek_end\n");
    return NANOTS_EC_UNKNOWN;
  }
}

int64_t nanots_iterator_current_block_sequence(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return 0;
  }

  try {
    return iterator->iterator->current_block_sequence();
  } catch (const nanots_exception& e) {
    fprintf(stderr,"nanots_exception in nanots_iterator_current_block_sequence: %d\n", e.get_ec());
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_current_block_sequence: %s\n", e.what());
  } catch (...) {
    fprintf(stderr,"Unknown exception in nanots_iterator_current_block_sequence\n");
  }
  
  return 0;
}

const char* nanots_iterator_current_metadata(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return nullptr;
  }

  try {
    const std::string& metadata = iterator->iterator->current_metadata();
    return metadata.c_str();
  } catch (const nanots_exception& e) {
    fprintf(stderr,"nanots_exception in nanots_iterator_current_metadata: %d\n", e.get_ec());
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_current_metadata: %s\n", e.what());
  } catch (...) {
    fprintf(stderr,"Unknown exception in nanots_iterator_current_metadata\n");
  }
  
  return nullptr;
}

}

/* NANOTS */
