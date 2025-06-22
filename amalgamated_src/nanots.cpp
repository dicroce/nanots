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
  if (!FlushViewOfFile((addr) ? addr : _mem, (length > 0) ? length : _length))
    throw std::runtime_error("Unable to sync memory mapped file.");

  if (now) {
    if (!FlushFileBuffers(_fileHandle))
      throw std::runtime_error("Unable to flush file handle.");
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
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<unsigned int> dis(0, 255);

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


std::mutex current_stream_tags_lok;
std::set<std::string> current_stream_tags;

static uint32_t _round_to_64k_boundary(uint32_t requested_size) {
  const uint32_t BOUNDARY = 65536;  // 64KB

  if (requested_size == 0)
    return BOUNDARY;  // Minimum size is 64KB

  // Round up to next multiple of 65536
  return ((requested_size + BOUNDARY - 1) / BOUNDARY) * BOUNDARY;
}

static bool _validate_frame_header(const uint8_t* frame_p,
                                   const uint8_t* expected_uuid,
                                   uint8_t* flags_out,
                                   uint32_t* size_out) {
  if (memcmp(frame_p + FRAME_UUID_OFFSET, expected_uuid, 16) != 0)
    return false;

  if (size_out)
    *size_out = *(uint32_t*)(frame_p + FRAME_SIZE_OFFSET);

  if (flags_out)
    *flags_out = *(frame_p + FRAME_FLAGS_OFFSET);

  return true;
}

static std::string _database_name(const std::string& file_name) {
  return file_name.substr(0, file_name.find(".nts")) + ".db";
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

    block_size = *(uint32_t*)mm.map();
  }

  auto db_name = _database_name(file_name);
  nts_sqlite_conn conn(db_name, true, true);

  auto result = conn.exec(
      "SELECT sb.id, sb.block_idx, sb.uuid, s.stream_tag "
      "FROM segment_blocks sb "
      "JOIN segments s ON sb.segment_id = s.id "
      "WHERE sb.end_timestamp = 0");

  for (auto& row : result) {
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

    #ifdef _WIN32
        uint32_t n_valid_indexes = *reinterpret_cast<volatile uint32_t*>(valid_counter);
        _ReadWriteBarrier(); // compiler barrier (not mem)
    #else
        uint32_t n_valid_indexes = __atomic_load_n(valid_counter, std::memory_order_acquire);
    #endif
    
    // Scan backwards to find last valid frame

    int last_valid = -1;
    for (int i = n_valid_indexes - 1; i >= 0; i--) {
      uint8_t* index_p = block_p + BLOCK_HEADER_SIZE + (i * INDEX_ENTRY_SIZE);
      int64_t timestamp = *(int64_t*)index_p;
      uint64_t offset = *(uint64_t*)(index_p + 8);

      if (timestamp == 0 || offset == 0)
        continue;  // Skip zeroed entries

      uint32_t index_region_end =
          BLOCK_HEADER_SIZE + ((n_valid_indexes + 1) * INDEX_ENTRY_SIZE);

      // Frame must be after all possible index entries and have room for header
      if (offset < index_region_end || offset > block_size - FRAME_HEADER_SIZE)
        continue;

      uint32_t frame_size = 0;
      if (_validate_frame_header(block_p + offset, uuid, nullptr,
                                 &frame_size)) {
        // Basic sanity check on size
        if (frame_size > block_size - offset - FRAME_HEADER_SIZE)
          continue;

        last_valid = i;
        break;
      }
    }

    if (last_valid >= 0) {
      nts_sqlite_transaction(conn, [&](const nts_sqlite_conn& conn) {
        uint8_t* last_index_p =
            block_p + BLOCK_HEADER_SIZE + (last_valid * INDEX_ENTRY_SIZE);
        int64_t actual_last_timestamp = *(int64_t*)last_index_p;
        auto stmt = conn.prepare(
            "UPDATE segment_blocks SET end_timestamp = ? WHERE block_idx = ? AND uuid "
            "= ?");
        stmt.bind(1, actual_last_timestamp)
            .bind(2, block_idx)
            .bind(3, uuid_hex)
            .exec_no_result();
      });
    }

    // Fix n_valid_indexes if needed
    if (last_valid + 1 != (int)n_valid_indexes) {
      // Truncating corrupt block
      *(uint32_t*)(block_p + 8) = last_valid + 1;
      mm.flush(mm.map(), block_size, true);
    }
  }
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

static void _upgrade_db(const nts_sqlite_conn& conn) {
  auto current_version = _get_db_version(conn);

  switch (current_version) {
    case 0: {
      nts_sqlite_transaction(
          conn, [&](const nts_sqlite_conn& conn) { _set_db_version(conn, 1); });
    }
      [[fallthrough]];
    default:
      break;
  };
}

static std::optional<block> _db_reclaim_oldest_used_block(
    const nts_sqlite_conn& conn) {
  // Find oldest finalized segment_block (end_timestamp != 0)
  auto result = conn.exec(
      "SELECT sb.block_id, b.idx, sb.id as segment_block_id, b.status "
      "FROM segment_blocks sb "
      "JOIN blocks b ON sb.block_id = b.id "
      "WHERE sb.end_timestamp != 0 AND (b.status = 'used' OR b.status = 'reserved') "
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

static std::optional<block> _db_get_block(const nts_sqlite_conn& conn,
                                          bool auto_reclaim) {
  auto result =
      conn.exec("SELECT id, idx FROM blocks WHERE status = 'free' LIMIT 1;");

  if (!result.empty()) {
    auto row = result.front();
    int64_t block_id = std::stoll(row["id"].value());

    auto stmt =
        conn.prepare("UPDATE blocks SET status = 'reserved' WHERE id = ?");
    stmt.bind(1, block_id).exec_no_result();

    return block{block_id, std::stoll(row["idx"].value())};
  }

  if (auto_reclaim)
    return _db_reclaim_oldest_used_block(conn);
  else
    throw nanots_exception(NANOTS_EC_NO_FREE_BLOCKS, "Unable to get free block.", __FILE__, __LINE__);
}

static std::optional<segment> _db_create_segment(const nts_sqlite_conn& conn,
                                                 const std::string& stream_tag,
                                                 const std::string& metadata) {
  auto stmt =
      conn.prepare("INSERT INTO segments (stream_tag, metadata) VALUES (?, ?)");
  stmt.bind(1, stream_tag).bind(2, metadata).exec_no_result();

  return segment{std::stoll(conn.last_insert_id()), stream_tag, metadata, 0};
}

static std::optional<segment_block> _db_create_segment_block(
    const nts_sqlite_conn& conn,
    int64_t segment_id,
    int64_t sequence,
    int64_t block_id,
    int64_t block_idx,
    int64_t start_timestamp,
    int64_t end_timestamp,
    const uint8_t* uuid) {
  auto stmt = conn.prepare(
      "INSERT INTO segment_blocks ("
      "segment_id, "
      "sequence, "
      "block_id, "
      "block_idx, "
      "start_timestamp, "
      "end_timestamp, "
      "uuid"
      ") VALUES (?, ?, ?, ?, ?, ?, ?)");

  auto hex_uuid = entropy_id_to_s(uuid);

  stmt.bind(1, segment_id)
      .bind(2, sequence)
      .bind(3, block_id)
      .bind(4, block_idx)
      .bind(5, start_timestamp)
      .bind(6, end_timestamp)
      .bind(7, hex_uuid)
      .exec_no_result();

  struct segment_block sb;
  sb.id = std::stoll(conn.last_insert_id());
  sb.segment_id = segment_id;
  sb.sequence = sequence;
  sb.block_id = block_id;
  sb.block_idx = block_idx;
  sb.start_timestamp = start_timestamp;
  sb.end_timestamp = end_timestamp;
  memcpy(sb.uuid, uuid, 16);

  return sb;
}

static void _db_finalize_block(const nts_sqlite_conn& conn,
                               int64_t segment_block_id,
                               int64_t timestamp) {
  auto stmt = conn.prepare("UPDATE segment_blocks SET end_timestamp = ? WHERE id = ?");
  stmt.bind(1, timestamp).bind(2, segment_block_id).exec_no_result();
}

static void _db_trans_finalize_reserved_blocks(const nts_sqlite_conn& conn) {
  // Set status to 'used' for all blocks whose status is 'reserved' and
  // reserved_at is older than 10 seconds.
  auto query =
      "UPDATE blocks SET status = 'used' WHERE status = 'reserved' AND "
      "reserved_at < datetime('now', '-10 seconds');";
  conn.exec(query);
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
  current_stream_tags.erase(stream_tag);

  if (last_timestamp && current_block) {
    auto db_name = _database_name(file_name);
    nts_sqlite_conn conn(db_name, true, true);

    nts_sqlite_transaction(conn, [&](const nts_sqlite_conn& conn) {
      _db_finalize_block(conn, current_block->id, last_timestamp.value());
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
      _block_size(*(uint32_t*)_file_header_p),
      _n_blocks(*(uint32_t*)(_file_header_p + sizeof(uint32_t))),
      _auto_reclaim(auto_reclaim),
      _active_stream_tags() {
  if (_block_size < 4096 || _block_size > 1024 * 1024 * 1024)
    throw nanots_exception(NANOTS_EC_INVALID_BLOCK_SIZE, "Invalid block size in file header.", __FILE__, __LINE__);

  auto db_name = _database_name(_file_name);
  nts_sqlite_conn db(db_name, true, true);
  _upgrade_db(db);
  _validate_blocks(_file_name);
}

write_context nanots_writer::create_write_context(const std::string& stream_tag,
                                                  const std::string& metadata) {

  std::lock_guard<std::mutex> g(current_stream_tags_lok);
  if(current_stream_tags.find(stream_tag) != current_stream_tags.end())
    throw nanots_exception(NANOTS_EC_DUPLICATE_STREAM_TAG, "Only one current writer per active stream tag.", __FILE__, __LINE__);

  write_context wctx;
  wctx.metadata = metadata;
  wctx.stream_tag = stream_tag;
  wctx.file_name = _file_name;

  auto db_name = _database_name(_file_name);

  nts_sqlite_conn conn(db_name.c_str(), true, true);

  if(_active_stream_tags.find(stream_tag) != _active_stream_tags.end())
    throw nanots_exception(NANOTS_EC_DUPLICATE_STREAM_TAG, "Stream tag already exists.", __FILE__, __LINE__);

  nts_sqlite_transaction(conn, [&](const nts_sqlite_conn& conn) {
    wctx.current_segment = _db_create_segment(conn, stream_tag, metadata);
    if (!wctx.current_segment)
      throw nanots_exception(NANOTS_EC_UNABLE_TO_CREATE_SEGMENT, "Unable to create segment.", __FILE__, __LINE__);
  });

  current_stream_tags.insert(stream_tag);

  return wctx;
}

void nanots_writer::write(write_context& wctx,
                          const uint8_t* data,
                          size_t size,
                          int64_t timestamp,
                          uint8_t flags) {
  if (wctx.last_timestamp && timestamp <= wctx.last_timestamp.value())
    throw nanots_exception(NANOTS_EC_NON_MONOTONIC_TIMESTAMP, "Timestamp is not monotonic.", __FILE__, __LINE__);

  if (size >
      _block_size - (FRAME_HEADER_SIZE + INDEX_ENTRY_SIZE + BLOCK_HEADER_SIZE))
    throw nanots_exception(NANOTS_EC_ROW_SIZE_TOO_BIG, "Frame size is too large. Use a much larger block size.", __FILE__, __LINE__);

  if (!wctx.current_block) {
    nts_sqlite_conn conn(_database_name(_file_name), true, true);

    nts_sqlite_transaction(conn, [&](const nts_sqlite_conn& conn) {
      auto block = _db_get_block(conn, _auto_reclaim);
      if (!block)
        throw nanots_exception(NANOTS_EC_NO_FREE_BLOCKS, "Unable to get free block.", __FILE__, __LINE__);

      uint8_t uuid[16];
      generate_entropy_id(uuid);

      wctx.current_block = _db_create_segment_block(
          conn, wctx.current_segment->id, wctx.current_segment->sequence,
          block->id, block->idx, timestamp, 0, uuid);

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

  if (n_valid_indexes > 0) {
    uint8_t* last_index_p = block_p + BLOCK_HEADER_SIZE +
                            ((n_valid_indexes - 1) * INDEX_ENTRY_SIZE);
    uint64_t last_frame_offset = *(uint64_t*)(last_index_p + 8);
    if (last_frame_offset >= padded_frame_size) {
      uint64_t candidate_ofs = last_frame_offset - padded_frame_size;
      new_block_ofs = (candidate_ofs >= index_end) ? candidate_ofs : index_end;
    } else {
      new_block_ofs = index_end;  // Force rollover to new block
    }
  }

  if (index_end >= new_block_ofs) {
    nts_sqlite_conn conn(_database_name(_file_name), true, true);

    wctx.mm.flush(wctx.mm.map(), _block_size, true);

    nts_sqlite_transaction(conn, [&](const nts_sqlite_conn& conn) {
      _db_finalize_block(conn, wctx.current_block->id, wctx.last_timestamp.value());
    });

    wctx.current_block = std::nullopt;
    wctx.mm = nts_memory_map();

    return write(wctx, data, size, timestamp, flags);
  }

  uint8_t* frame_p = block_p + new_block_ofs;
  memcpy(frame_p, wctx.current_block->uuid, 16);
  *(uint32_t*)(frame_p + 16) = (uint32_t)size;
  *(frame_p + 20) = flags;
  memcpy(frame_p + FRAME_HEADER_SIZE, data, size);

  uint8_t* index_p =
      block_p + BLOCK_HEADER_SIZE + (n_valid_indexes * INDEX_ENTRY_SIZE);
  *(int64_t*)index_p = timestamp;
  *(uint64_t*)(index_p + 8) = new_block_ofs;

  auto valid_counter = (uint32_t*)(block_p + 8);

#ifdef _WIN32
  _InterlockedIncrement(reinterpret_cast<volatile long*>(valid_counter));
#else
  __atomic_fetch_add(valid_counter, 1, std::memory_order_release);
#endif

  wctx.last_timestamp = timestamp;
}

void nanots_writer::free_blocks(const std::string& stream_tag,
                                int64_t start_timestamp,
                                int64_t end_timestamp) {
  auto db_name = _database_name(_file_name);
  nts_sqlite_conn conn(db_name, true, true);

  nts_sqlite_transaction(conn, [&](const nts_sqlite_conn& conn) {
    // Find blocks that fall entirely within the deletion time range
    auto stmt = conn.prepare(
        "SELECT sb.id as segment_block_id, sb.block_id "
        "FROM segment_blocks sb "
        "JOIN segments s ON sb.segment_id = s.id "
        "WHERE s.stream_tag = ? "
        "AND sb.start_timestamp >= ? "
        "AND sb.end_timestamp <= ? "
        "AND sb.end_timestamp != 0");
    auto blocks_to_delete =
        stmt.bind(1, stream_tag).bind(2, start_timestamp).bind(3, end_timestamp).exec();

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

void nanots_writer::allocate(const std::string& file_name,
                             uint32_t block_size,
                             uint32_t n_blocks) {
  // Windows MapViewOfFile() requires mapped regions to start and end on 64k
  // boundaires. Our file header size is 65536, SO if the block size is a
  // multiple of 65536 then block start and end on 64k boundaries.
  block_size = _round_to_64k_boundary(block_size);

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

    // write a file header
    *(uint32_t*)p = block_size;
    p += sizeof(uint32_t);
    *(uint32_t*)p = n_blocks;
    p += sizeof(uint32_t);

    mm.flush(mm.map(), 8);
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
      "metadata STRING "
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

  query = "CREATE INDEX idx_segments_stream_tag ON segments(stream_tag);";
  db.exec(query);

  query = "CREATE INDEX idx_blocks_status ON blocks(status);";
  db.exec(query);

  nts_sqlite_transaction(db, [n_blocks](const nts_sqlite_conn& conn) {
    auto stmt =
        conn.prepare("INSERT INTO blocks (idx, status) VALUES (?, 'free')");
    for (uint32_t i = 0; i < n_blocks; i++) {
      stmt.bind(1, static_cast<int>(i)).exec_no_result();
      stmt.reset();
    }
  });

  _upgrade_db(db);
}

nanots_reader::nanots_reader(const std::string& file_name)
    : _file_name(file_name),
      _file(nts_file::open(file_name, "r")),
      _block_size(),
      _n_blocks() {
  auto mm = nts_memory_map(
      filenum(_file), 0, FILE_HEADER_BLOCK_SIZE, nts_memory_map::NMM_PROT_READ,
      nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

  auto header_p = (uint8_t*)mm.map();

  _block_size = *(uint32_t*)header_p;

  _n_blocks = (*(uint32_t*)(header_p + sizeof(uint32_t)));
}

static int _compare_index_entry_timestamp(uint8_t* index_entry_p,
                                          uint8_t* target_timestamp_p) {
  int64_t entry_timestamp = *(int64_t*)index_entry_p;
  int64_t target_timestamp = *(int64_t*)target_timestamp_p;

  if (entry_timestamp < target_timestamp)
    return -1;
  if (entry_timestamp > target_timestamp)
    return 1;
  return 0;
}

void nanots_reader::read(
    const std::string& stream_tag,
    int64_t start_timestamp,
    int64_t end_timestamp,
    const std::function<
        void(const uint8_t*, size_t, uint8_t, int64_t, int64_t, const std::string&)>& callback) {
  nts_sqlite_conn db(_database_name(_file_name), false, true);

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
      "AND sb.start_timestamp <= ? "
      "AND (sb.end_timestamp >= ? OR sb.end_timestamp = 0) "
      "ORDER BY sb.sequence ASC;");
  auto results =
      stmt.bind(1, stream_tag).bind(2, end_timestamp).bind(3, start_timestamp).exec();

  bool need_binary_search = true;

  for (auto& row : results) {
    std::string metadata = row["metadata"].value();
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

#ifdef _WIN32
    uint32_t n_valid_indexes = *reinterpret_cast<volatile uint32_t*>(valid_counter);
    _ReadWriteBarrier(); // compiler barrier (not mem)
#else
    uint32_t n_valid_indexes = __atomic_load_n(valid_counter, std::memory_order_acquire);
#endif

    uint8_t* index_start = block_p + BLOCK_HEADER_SIZE;
    uint8_t* index_end = index_start + (n_valid_indexes * INDEX_ENTRY_SIZE);

    int64_t start_index = 0;

    if (need_binary_search) {
      uint8_t* first_entry =
          lower_bound_bytes(index_start, index_end, (uint8_t*)&start_timestamp,
                            INDEX_ENTRY_SIZE, _compare_index_entry_timestamp);

      start_index = (first_entry - index_start) / INDEX_ENTRY_SIZE;
      need_binary_search = false;
    }

    // Iterate through frames in this block
    for (size_t i = start_index; i < n_valid_indexes; i++) {
      uint8_t* index_p = block_p + BLOCK_HEADER_SIZE + (i * INDEX_ENTRY_SIZE);
      int64_t timestamp = *(int64_t*)index_p;
      uint64_t offset = *(uint64_t*)(index_p + 8);

      // Check if we've passed the end time
      if (timestamp > end_timestamp)
        return;  // All done!

      // Validate frame header
      uint8_t flags;
      uint32_t frame_size;
      if (!_validate_frame_header(block_p + offset, uuid, &flags,
                                  &frame_size)) {
        // Log warning? Skip corrupted frame
        continue;
      }

      // Callback with frame data
      callback(block_p + offset + FRAME_HEADER_SIZE, (size_t)frame_size, flags,
               timestamp, block_sequence, metadata);
    }
  }
}

std::vector<std::string> nanots_reader::query_stream_tags(int64_t start_timestamp, int64_t end_timestamp) {
  nts_sqlite_conn db(_database_name(_file_name), false, true);

  auto stmt = db.prepare(
      "SELECT DISTINCT s.stream_tag "
      "FROM segments s "
      "JOIN segment_blocks sb ON s.id = sb.segment_id "
      "WHERE sb.start_timestamp <= ? AND (sb.end_timestamp >= ? OR sb.end_timestamp = 0);");
  auto results =
      stmt.bind(1, end_timestamp).bind(2, start_timestamp).exec();

  std::vector<std::string> stream_tags;

  for (auto& row : results) {
      stream_tags.push_back(row["stream_tag"].value());
  }

  return stream_tags;
}

std::vector<contiguous_segment> nanots_reader::query_contiguous_segments(
    const std::string& stream_tag,
    int64_t start_timestamp,
    int64_t end_timestamp) {
  nts_sqlite_conn db(_database_name(_file_name), false, true);

  // Create a grouping key by subtracting sequence from row number
  // Contiguous sequences will have the same group_key

  auto stmt = db.prepare(
      "WITH contiguous_groups AS ( "
      "SELECT "
      "sb.segment_id, "
      "sb.sequence, "
      "sb.start_timestamp, "
      "sb.end_timestamp, "
      "ROW_NUMBER() "
      "OVER (PARTITION BY sb.segment_id ORDER BY sb.sequence) - sb.sequence AS "
      "group_key "
      "FROM segment_blocks sb "
      "JOIN segments s ON sb.segment_id = s.id "
      "WHERE sb.start_timestamp <= ? "
      "AND (sb.end_timestamp >= ? OR sb.end_timestamp = 0) "
      "AND s.stream_tag = ? "
      "), "
      "region_boundaries AS ( "
      "SELECT "
      "segment_id, "
      "group_key, "
      "MIN(start_timestamp) AS region_start, "
      "MAX(end_timestamp) AS region_end, "
      "COUNT(*) AS block_count "
      "FROM contiguous_groups "
      "GROUP BY segment_id, group_key "
      ") "
      "SELECT "
      "segment_id, "
      "region_start, "
      "region_end, "
      "block_count "
      "FROM region_boundaries "
      "ORDER BY segment_id, region_start;");
  auto results =
      stmt.bind(1, end_timestamp).bind(2, start_timestamp).bind(3, stream_tag).exec();

  std::vector<contiguous_segment> segments;

  for (auto& row : results) {
    contiguous_segment segment;
    segment.segment_id = std::stoll(row["segment_id"].value());
    segment.start_timestamp = std::stoll(row["region_start"].value());
    segment.end_timestamp = std::stoll(row["region_end"].value());
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
      _valid(false),
      _initialized(false) {
  // Read block size from file header
  auto header_mm = nts_memory_map(
      filenum(_file), 0, FILE_HEADER_BLOCK_SIZE, nts_memory_map::NMM_PROT_READ,
      nts_memory_map::NMM_TYPE_FILE | nts_memory_map::NMM_SHARED);

  auto header_p = (uint8_t*)header_mm.map();
  _block_size = *(uint32_t*)header_p;

  // Initialize to first frame if stream exists
  reset();
}

block_info* nanots_iterator::_get_block_by_segment_and_sequence(int64_t segment_id, int64_t sequence) {
  std::string cache_key = std::to_string(segment_id) + ":" + std::to_string(sequence);
  auto it = _block_cache.find(cache_key);
  if (it != _block_cache.end()) {
    return &it->second;
  }

  // Query database for this specific block
  auto db_name = _database_name(_file_name);
  nts_sqlite_conn db(db_name, false, true);

  auto stmt = db.prepare(
      "SELECT "
      "s.metadata as metadata, "
      "sb.segment_id as segment_id, "
      "sb.sequence as block_sequence, "
      "sb.block_idx as block_idx, "
      "sb.start_timestamp as start_timestamp, "
      "sb.end_timestamp as end_timestamp, "
      "sb.uuid as uuid "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE sb.segment_id = ? AND sb.sequence = ?");

  auto results = stmt.bind(1, segment_id).bind(2, sequence).exec();

  if (results.empty()) {
    return nullptr;
  }

  auto& row = results[0];
  block_info block;
  block.block_idx = std::stoll(row["block_idx"].value());
  block.block_sequence = std::stoll(row["block_sequence"].value());
  block.segment_id = std::stoll(row["segment_id"].value());
  block.metadata = row["metadata"].value();
  block.uuid_hex = row["uuid"].value();
  block.start_timestamp = std::stoll(row["start_timestamp"].value());
  block.end_timestamp = std::stoll(row["end_timestamp"].value());
  
  auto result = _block_cache.emplace(cache_key, std::move(block));
  return &result.first->second;
}

block_info* nanots_iterator::_get_first_block() {
  auto db_name = _database_name(_file_name);
  nts_sqlite_conn db(db_name, false, true);

  auto stmt = db.prepare(
      "SELECT sb.segment_id, sb.sequence "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE s.stream_tag = ? "
      "ORDER BY s.id ASC, sb.sequence ASC "
      "LIMIT 1");

  auto results = stmt.bind(1, _stream_tag).exec();

  if (results.empty())
    return nullptr;

  int64_t segment_id = std::stoll(results[0]["segment_id"].value());
  int64_t sequence = std::stoll(results[0]["sequence"].value());
  return _get_block_by_segment_and_sequence(segment_id, sequence);
}

block_info* nanots_iterator::_get_next_block() {
  auto db_name = _database_name(_file_name);
  nts_sqlite_conn db(db_name, false, true);

  // First try to find next block within the same segment
  auto stmt = db.prepare(
      "SELECT sb.id, sb.sequence "
      "FROM segment_blocks sb "
      "WHERE sb.segment_id = ? AND sb.sequence > ? "
      "ORDER BY sb.sequence ASC "
      "LIMIT 1");

  auto results = stmt.bind(1, _current_segment_id).bind(2, _current_block_sequence).exec();

  if (!results.empty()) {
    // Found next block in same segment
    int64_t next_sequence = std::stoll(results[0]["sequence"].value());
    return _get_block_by_segment_and_sequence(_current_segment_id, next_sequence);
  }

  // No more blocks in current segment, look for first block in next segment
  stmt = db.prepare(
      "SELECT sb.segment_id, sb.sequence "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE s.stream_tag = ? "
      "AND s.id > ? "
      "ORDER BY s.id ASC, sb.sequence ASC "
      "LIMIT 1");

  results = stmt.bind(1, _stream_tag).bind(2, _current_segment_id).exec();

  if (results.empty())
    return nullptr;

  int64_t next_segment_id = std::stoll(results[0]["segment_id"].value());
  int64_t next_sequence = std::stoll(results[0]["sequence"].value());
  return _get_block_by_segment_and_sequence(next_segment_id, next_sequence);
}

block_info* nanots_iterator::_get_prev_block() {
  auto db_name = _database_name(_file_name);
  nts_sqlite_conn db(db_name, false, true);

  // First try to find previous block within the same segment
  auto stmt = db.prepare(
      "SELECT sb.id, sb.sequence "
      "FROM segment_blocks sb "
      "WHERE sb.segment_id = ? AND sb.sequence < ? "
      "ORDER BY sb.sequence DESC "
      "LIMIT 1");

  auto results = stmt.bind(1, _current_segment_id).bind(2, _current_block_sequence).exec();

  if (!results.empty()) {
    // Found previous block in same segment
    int64_t prev_sequence = std::stoll(results[0]["sequence"].value());
    return _get_block_by_segment_and_sequence(_current_segment_id, prev_sequence);
  }

  // No previous blocks in current segment, look for last block in previous segment
  stmt = db.prepare(
      "SELECT sb.segment_id, sb.sequence "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE s.stream_tag = ? "
      "AND s.id < ? "
      "ORDER BY s.id DESC, sb.sequence DESC "
      "LIMIT 1");

  results = stmt.bind(1, _stream_tag).bind(2, _current_segment_id).exec();

  if (results.empty())
    return nullptr;

  int64_t prev_segment_id = std::stoll(results[0]["segment_id"].value());
  int64_t prev_sequence = std::stoll(results[0]["sequence"].value());
  return _get_block_by_segment_and_sequence(prev_segment_id, prev_sequence);
}

block_info* nanots_iterator::_find_block_for_timestamp(int64_t timestamp) {
  auto db_name = _database_name(_file_name);
  nts_sqlite_conn db(db_name, false, true);

  // First try to find block that contains the timestamp
  auto stmt = db.prepare(
      "SELECT sb.segment_id, sb.sequence "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE s.stream_tag = ? "
      "AND sb.start_timestamp <= ? "
      "AND (sb.end_timestamp >= ? OR sb.end_timestamp = 0) "
      "ORDER BY s.id ASC, sb.sequence ASC "
      "LIMIT 1");

  auto results =
      stmt.bind(1, _stream_tag).bind(2, timestamp).bind(3, timestamp).exec();

  if(!results.empty())
  {
    int64_t segment_id = std::stoll(results[0]["segment_id"].value());
    int64_t sequence = std::stoll(results[0]["sequence"].value());
    return _get_block_by_segment_and_sequence(segment_id, sequence);
  }

  // If no block contains the timestamp, find the first block with start_timestamp >=
  // timestamp. This explicitly allows a find() before the first timsestamp to still find the first block.
  stmt = db.prepare(
      "SELECT sb.segment_id, sb.sequence "
      "FROM segments s "
      "JOIN segment_blocks sb ON sb.segment_id = s.id "
      "WHERE s.stream_tag = ? "
      "AND sb.start_timestamp >= ? "
      "ORDER BY s.id ASC, sb.sequence ASC "
      "LIMIT 1");

  results = stmt.bind(1, _stream_tag).bind(2, timestamp).exec();

  if (results.empty())
    return nullptr;  // No blocks at all, or timestamp is after everything

  int64_t segment_id = std::stoll(results[0]["segment_id"].value());
  int64_t sequence = std::stoll(results[0]["sequence"].value());
  return _get_block_by_segment_and_sequence(segment_id, sequence);
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

  auto valid_counter = (uint32_t*)(block.block_p + 8);

#ifdef _WIN32
    block.n_valid_indexes = *reinterpret_cast<volatile uint32_t*>(valid_counter);
    _ReadWriteBarrier(); // compiler barrier (not mem)
#else
    block.n_valid_indexes = __atomic_load_n(valid_counter, std::memory_order_acquire);
#endif

  // Convert UUID std::string to bytes
  s_to_entropy_id(block.uuid_hex, block.uuid);

  block.is_loaded = true;
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
  int64_t timestamp = *(int64_t*)index_p;
  uint64_t offset = *(uint64_t*)(index_p + 8);

  // Validate frame header
  uint8_t flags;
  uint32_t frame_size;
  if (!_validate_frame_header(block->block_p + offset, block->uuid, &flags,
                              &frame_size)) {
    _valid = false;
    return false;
  }

  // Set up current frame
  _current_frame.data = block->block_p + offset + FRAME_HEADER_SIZE;
  _current_frame.size = frame_size;
  _current_frame.flags = flags;
  _current_frame.timestamp = timestamp;
  _current_frame.block_sequence = block->block_sequence;

  _valid = true;
  return true;
}

nanots_iterator& nanots_iterator::operator++() {
  if (!_valid)
    return *this;

  auto* current_block = _get_block_by_segment_and_sequence(_current_segment_id, _current_block_sequence);
  if (!current_block || !_load_block_data(*current_block)) {
    _valid = false;
    return *this;
  }

  _current_frame_idx++;

  // If we've gone past the end of current block, move to next block
  if (_current_frame_idx >= current_block->n_valid_indexes) {
    auto* next_block = _get_next_block();
    if (!next_block) {
      _valid = false;
      return *this;
    }

    _current_segment_id = next_block->segment_id;
    _current_block_sequence = next_block->block_sequence;
    _current_frame_idx = 0;
  }

  _load_current_frame();
  return *this;
}

nanots_iterator& nanots_iterator::operator--() {
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

    _current_segment_id = prev_block->segment_id;
    _current_block_sequence = prev_block->block_sequence;
    _current_frame_idx =
        (prev_block->n_valid_indexes > 0) ? prev_block->n_valid_indexes - 1 : 0;
  } else
    _current_frame_idx--;

  _load_current_frame();
  return *this;
}

bool nanots_iterator::find(int64_t timestamp) {
  auto* block = _find_block_for_timestamp(timestamp);
  if (!block) {
    _valid = false;
    return false;
  }

  if (!_load_block_data(*block)) {
    _valid = false;
    return false;
  }

  _current_segment_id = block->segment_id;
  _current_block_sequence = block->block_sequence;

  // Binary search within the block
  uint8_t* index_start = block->block_p + BLOCK_HEADER_SIZE;
  uint8_t* index_end =
      index_start + (block->n_valid_indexes * INDEX_ENTRY_SIZE);

  uint8_t* found_entry =
      lower_bound_bytes(index_start, index_end, (uint8_t*)&timestamp,
                        INDEX_ENTRY_SIZE, _compare_index_entry_timestamp);

  _current_frame_idx = (found_entry - index_start) / INDEX_ENTRY_SIZE;

  // If we didn't find it in this block, try next block
  if (_current_frame_idx >= block->n_valid_indexes) {
    auto* next_block = _get_next_block();
    if (!next_block) {
      _valid = false;
      return false;
    }

    _current_segment_id = next_block->segment_id;
    _current_block_sequence = next_block->block_sequence;
    _current_frame_idx = 0;
  }

  // Get frame info from index
  uint8_t* index_p = block->block_p + BLOCK_HEADER_SIZE +
                     (_current_frame_idx * INDEX_ENTRY_SIZE);
  int64_t found_timestamp = *(int64_t*)index_p;
  uint64_t offset = *(uint64_t*)(index_p + 8);

  // Validate frame header
  uint8_t flags;
  uint32_t frame_size;
  if (!_validate_frame_header(block->block_p + offset, block->uuid, &flags,
                              &frame_size)) {
    _valid = false;
    return false;
  }

  // Set up current frame
  _current_frame.data = block->block_p + offset + FRAME_HEADER_SIZE;
  _current_frame.size = frame_size;
  _current_frame.flags = flags;
  _current_frame.timestamp = found_timestamp;
  _current_frame.block_sequence = block->block_sequence;

  _valid = true;
  return true;
}

void nanots_iterator::reset() {
  auto* first_block = _get_first_block();
  if (!first_block) {
    _valid = false;
    return;
  }

  _current_segment_id = first_block->segment_id;
  _current_block_sequence = first_block->block_sequence;
  _current_frame_idx = 0;
  _load_current_frame();
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
                                int64_t timestamp,
                                uint8_t flags) {
  if (!writer || !writer->writer) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }
  if (!context) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    writer->writer->write(context->context, data, size, timestamp, flags);
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

nanots_ec_t nanots_writer_free_blocks(nanots_writer_t writer,
                                          const char* stream_tag,
                                          int64_t start_timestamp,
                                          int64_t end_timestamp) {
  if (!writer || !writer->writer) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    writer->writer->free_blocks(std::string(stream_tag), start_timestamp, end_timestamp);
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
                               int64_t end_timestamp,
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
    reader->reader->read(std::string(stream_tag), start_timestamp, end_timestamp,
                         [&ctx](const uint8_t* data, size_t size, uint8_t flags,
                                int64_t timestamp, int64_t block_sequence, const std::string& metadata) {
                           ctx.callback(data, size, flags, timestamp,
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
    int64_t end_timestamp,
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
        std::string(stream_tag), start_timestamp, end_timestamp);

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
      (*segments)[i].end_timestamp = cpp_segments[i].end_timestamp;
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
                                                  int64_t end_timestamp) {
  if (!reader || !reader->reader) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    reader->cached_stream_tags = reader->reader->query_stream_tags(start_timestamp, end_timestamp);
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
                                     int64_t timestamp) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    bool found = iterator->iterator->find(timestamp);
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
