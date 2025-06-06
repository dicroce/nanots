
#include "utils.h"
#include "sqlite3.h"

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
  auto moved = SetFilePointerEx((HANDLE)_get_osfhandle(filenum(file)), li,
                                nullptr, FILE_BEGIN);
  if (moved == INVALID_SET_FILE_POINTER)
    return -1;
  SetEndOfFile((HANDLE)_get_osfhandle(filenum(file)));
  return 0;
  // return ( _chsize_s( filenum( file ), size ) == 0) ? 0 : -1;
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
