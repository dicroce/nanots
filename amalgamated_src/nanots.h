// Amalgamated header file
// Generated automatically - do not edit

#ifndef NANOTS_AMALGAMATED_H
#define NANOTS_AMALGAMATED_H

#include "sqlite3.h"


#ifndef UTILS_H
#define UTILS_H

// Export macro for DLL/shared library
#if defined(_WIN32)
  #if defined(NANOTS_BUILDING_DLL)
    // Building the DLL
    #define NANOTS_API __declspec(dllexport)
  #elif defined(NANOTS_USING_DLL)
    // Using the DLL from another project
    #define NANOTS_API __declspec(dllimport)
  #else
    // Using static library
    #define NANOTS_API
  #endif
#elif defined(__GNUC__) || defined(__clang__)
  // On GCC/Clang, explicitly mark nanots symbols as visible
  // (SQLite symbols will remain hidden due to -fvisibility=hidden)
  #define NANOTS_API __attribute__((visibility("default")))
#else
  #define NANOTS_API
#endif

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
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <algorithm>

#ifdef _WIN32
#include <Rpc.h>
#include <Windows.h>
#include <winternl.h>
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

class NANOTS_API nts_sqlite_conn final {
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

class NANOTS_API nts_sqlite_stmt final {
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
void nts_sqlite_transaction(const nts_sqlite_conn& db, bool immediate, T t) {
  if (immediate) {
    db.exec("BEGIN IMMEDIATE");
  } else {
    db.exec("BEGIN");
  }
  try {
    t(db);
    db.exec("COMMIT");
  } catch (...) {
    db.exec("ROLLBACK");
    throw;
  }
}

// File raii
class NANOTS_API nts_file final {
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

class NANOTS_API nts_memory_map {
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
  NANOTS_EC_UNKNOWN = 12,
  NANOTS_EC_NOT_FOUND = 13,
  NANOTS_EC_BAD_MAGIC = 14,
  NANOTS_EC_BAD_VERSION = 15
};

}

class NANOTS_API nanots_exception : public std::exception {
public:
    nanots_exception(
       nanots_ec_t ec,
       const std::string& message,
       const std::string& file,
       int line
    ) : _ec(ec), _message(message), _file(file), _line(line) {}
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

// --- On-disk format version 2 -------------------------------------------
// File magic: ASCII "NTS\0". Legacy v1 files had block_size (always a
// multiple of 65536) at offset 0, which can never collide with this magic
// in its low byte ('N' = 0x4E), so the sniff is unambiguous.
#define NANOTS_FILE_MAGIC "NTS\0"
#define NANOTS_FILE_MAGIC_LEN 4
#define NANOTS_FORMAT_VERSION 2

#define FILE_HEADER_BLOCK_SIZE 65536
// File header field offsets (within the first 64 KB; rest is reserved):
//   0  : magic[4]                = "NTS\0"
//   4  : format_version (u16)    = 2
//   6  : header_size    (u16)    = bytes actually used (>= 32)
//   8  : block_size     (u32)
//   12 : n_blocks       (u32; 0 = growable)
//   16 : max_blocks     (u32; growable cap, 0 = unbounded)
//   20 : flags          (u32; reserved, currently 0)
//   24 : feature_bits   (u64; reserved for future per-file feature flags,
//                       currently zero)
//   32 : reserved (zeroed)
#define FILE_HEADER_MAGIC_OFFSET     0
#define FILE_HEADER_VERSION_OFFSET   4
#define FILE_HEADER_HSIZE_OFFSET     6
#define FILE_HEADER_BLOCK_SIZE_OFFSET 8
#define FILE_HEADER_N_BLOCKS_OFFSET  12
#define FILE_HEADER_MAX_BLOCKS_OFFSET 16
#define FILE_HEADER_FLAGS_OFFSET     20
#define FILE_HEADER_FEATURES_OFFSET  24
#define FILE_HEADER_USED_BYTES       32

// 8 + 4 + 4 bytes. block_feature_bits (was 'reserved') is repurposed as
// per-block feature flags for future use; currently always 0.
#define BLOCK_HEADER_SIZE 16

// v2 index entry: 8 + 8 + 8 + 8 = 32 bytes.
//   0  : timestamp     (int64)
//   8  : secondary_key (int64; NANOTS_SEC_KEY_UNSET if the writer didn't
//                       supply one — used as the composite tiebreaker)
//   16 : offset        (uint64; byte offset of frame in block)
//   24 : reserved      (uint64; zeroed)
#define INDEX_ENTRY_SIZE 32
#define INDEX_ENTRY_TS_OFFSET      0
#define INDEX_ENTRY_SECKEY_OFFSET  8
#define INDEX_ENTRY_OFFSET_OFFSET 16
#define INDEX_ENTRY_RESERVED_OFFSET 24

// v2 frame header: 16 + 8 + 4 + 4 = 32 bytes (8-byte aligned, payload at +32).
//   0  : uuid[16]
//   16 : secondary_key (int64; NANOTS_SEC_KEY_UNSET if the writer didn't
//                       supply one)
//   24 : size          (uint32)
//   28 : flags         (uint32 — widened from u8 in v2)
#define FRAME_HEADER_SIZE 32
#define FRAME_UUID_OFFSET        0
#define FRAME_SECKEY_OFFSET     16
#define FRAME_SIZE_OFFSET       24
#define FRAME_FLAGS_OFFSET      28

// Default tiebreaker value for callers that don't supply a real secondary
// key. INT64_MIN is the smallest possible int64, so under the composite
// (timestamp, secondary_key) ordering this acts as "smaller than any real
// key" — i.e. a stream that always passes this sentinel sees the classic
// "timestamp strictly monotonic" rule. Any other int64 (including 0) is a
// real key and participates in the tiebreaker.
#define NANOTS_SEC_KEY_UNSET INT64_MIN

struct block_header {
    int64_t block_start_timestamp{0};
    uint32_t n_valid_indexes{0};
    uint32_t block_feature_bits{0};
};

struct index_entry {
    int64_t timestamp{0};
    int64_t secondary_key{NANOTS_SEC_KEY_UNSET};
    uint64_t offset{0};
    uint64_t reserved{0};
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
    // SQL NULL while the block is open. Timestamp zero is valid data and
    // must not double as an occupancy/finalization sentinel.
    std::optional<int64_t> end_timestamp;
    int64_t start_secondary_key{NANOTS_SEC_KEY_UNSET};
    std::optional<int64_t> end_secondary_key;
    uint8_t uuid[16];
};

// ---------------------------------------------------------------------------
// Epoch-Based Reclamation (EBR) registry.
// ---------------------------------------------------------------------------
//
// One registry per file (in-process, path-keyed singleton). Shared by every
// nanots_writer and nanots_iterator opened on that file. EBR lets the writer
// recycle physical blocks safely even when readers are concurrently iterating:
// the writer defers byte-overwriting reclaim of a block until no reader's slot
// can still be holding pointers into that block's bytes.
//
// Hot-path cost: one atomic load + two atomic stores per iterator operation;
// one atomic increment per writer retire.
//
// Within a single process only. Cross-process readers are not protected.
//
class NANOTS_API nanots_epoch_registry {
public:
    static constexpr uint64_t INACTIVE = UINT64_MAX;

    struct Slot {
      std::atomic<uint64_t> epoch{INACTIVE};
      std::atomic<int64_t> heartbeat_us{0};
    };

    // Hot-path operations.
    uint64_t global_epoch_load() const {
      return _global_epoch.load(std::memory_order_acquire);
    }

    uint64_t global_epoch_bump() {
      return _global_epoch.fetch_add(1, std::memory_order_release);
    }

    // Returns true if no active slot's epoch is <= retired_epoch. An active
    // slot remains pinned until its owner explicitly releases it; elapsed time
    // cannot prove that mapped frame data is no longer in use. now_us is kept
    // in the signature for source and binary compatibility and is ignored.
    bool can_recycle(uint64_t retired_epoch, int64_t now_us) const;

    // Accessor for slot_guard.
    Slot& slot(uint32_t id);

    // Acquire/release; called by nanots_slot_guard.
    uint32_t acquire_slot();
    void release_slot(uint32_t id);

    // Path-keyed singleton accessor. Equivalent paths to the same file return
    // the same registry for the lifetime of any caller holding the shared_ptr.
    static std::shared_ptr<nanots_epoch_registry> get_or_create(
        const std::string& file_path
    );

private:
    std::atomic<uint64_t> _global_epoch{1};
    mutable std::mutex _slots_mu;
    std::vector<std::unique_ptr<Slot>> _slots;
};

// Retained for source compatibility. Heartbeat age is diagnostic only and is
// never used to infer that an active reader is safe to ignore.
constexpr int64_t NANOTS_HEARTBEAT_TIMEOUT_US = 60LL * 1000 * 1000;  // 60s

// ---------------------------------------------------------------------------
// RAII wrapper for slot acquisition + publishing.
// ---------------------------------------------------------------------------
//
// On construction: acquires a slot from the registry and publishes the current
// global_epoch + a diagnostic heartbeat. On destruction: sets the slot to
// INACTIVE and releases it. Pairing the acquire and the release in one object means an
// exception thrown anywhere in iterator construction or operation still leaves
// the slot in a clean state.
//
// Move-only.
//
class NANOTS_API nanots_slot_guard {
public:
    nanots_slot_guard() = default;
    explicit nanots_slot_guard(std::shared_ptr<nanots_epoch_registry> registry);
    ~nanots_slot_guard();

    nanots_slot_guard(const nanots_slot_guard&)            = delete;
    nanots_slot_guard& operator=(const nanots_slot_guard&) = delete;
    nanots_slot_guard(nanots_slot_guard&& other) noexcept;
    nanots_slot_guard& operator=(nanots_slot_guard&& other) noexcept;

    // Publishes the latest global_epoch and updates the diagnostic heartbeat.
    // Call at the start of every iterator operation. No-op if !active().
    void op_begin();

    // True if this guard owns a slot.
    bool active() const { return _slot_id != UINT32_MAX; }

private:
    void _release() noexcept;

    std::shared_ptr<nanots_epoch_registry> _registry;
    uint32_t _slot_id{UINT32_MAX};
};

// ---------------------------------------------------------------------------
// Scoped op marker.
// ---------------------------------------------------------------------------
//
// Stack-only RAII wrapper that marks the start of one iterator operation.
// Construction publishes the current global_epoch + diagnostic heartbeat;
// destruction is intentionally a no-op (the slot retains the published epoch
// until the next op or the iterator is destroyed — that's what protects the
// Frame::data validity contract). The point of this type is to make "this
// scope is one iterator op" syntactically explicit at every call site, and to
// reserve a hook for any future op-exit work (latency counters, debug
// assertions, etc.) without churning all the call sites again.
//
class NANOTS_API nanots_op_scope {
public:
    explicit nanots_op_scope(nanots_slot_guard& guard) : _guard(guard) {
      _guard.op_begin();
    }
    ~nanots_op_scope() = default;

    nanots_op_scope(const nanots_op_scope&)            = delete;
    nanots_op_scope& operator=(const nanots_op_scope&) = delete;
    nanots_op_scope(nanots_op_scope&&)                 = delete;
    nanots_op_scope& operator=(nanots_op_scope&&)      = delete;

private:
    nanots_slot_guard& _guard;
};

struct NANOTS_API write_context final {
    write_context() = default;
    write_context(const write_context&) = delete;
    write_context& operator=(const write_context&) = delete;
    write_context(write_context&& other) = default;
    write_context& operator=(write_context&& other) = default;

    ~write_context();

    std::string metadata;
    std::string stream_tag;
    std::optional<int64_t> last_timestamp;
    std::optional<int64_t> last_secondary_key;
    std::optional<segment> current_segment;
    std::optional<segment_block> current_block;
    nts_file file;
    nts_memory_map mm;
    std::string file_name;
    uint32_t _block_size{0};
};

class NANOTS_API nanots_writer {
 public:
    nanots_writer(const std::string& file_name, bool auto_reclaim = false);
    nanots_writer(const nanots_writer&) = delete;
    nanots_writer(nanots_writer&&) = default;
    nanots_writer& operator=(const nanots_writer&) = delete;
    nanots_writer& operator=(nanots_writer&&) = default;
    ~nanots_writer();

    write_context create_write_context(
        const std::string& stream_tag,
        const std::string& metadata
    );

    // Frames are ordered by the COMPOSITE (timestamp, secondary_key) — that
    // pair must be strictly greater than the previous frame's composite. So:
    //   - timestamp may repeat; secondary_key must then strictly increase
    //   - if timestamp strictly increases, secondary_key may be anything
    // Default secondary_key = NANOTS_SEC_KEY_UNSET (= INT64_MIN) — the smallest
    // possible value, equivalent to "no tiebreaker." A stream that always
    // writes with the default gets the classic "timestamp strictly monotonic"
    // behavior because (t, MIN) > (last_t, MIN) iff t > last_t.
    void write(
        write_context& wctx,
        const uint8_t* data,
        size_t size,
        uint32_t flags,
        int64_t timestamp,
        int64_t secondary_key = NANOTS_SEC_KEY_UNSET
    );

    // Free every block in this stream that falls entirely within the
    // composite window [(start_ts, start_sk), (end_ts, end_sk)]. Pass
    // NANOTS_SEC_KEY_UNSET for start_secondary_key and INT64_MAX for
    // end_secondary_key to ignore the sec_key axis (the classic
    // timestamp-only deletion).
    static void free_blocks(
        const std::string& file_name,
        const std::string& stream_tag,
        int64_t start_timestamp,
        int64_t start_secondary_key,
        int64_t end_timestamp,
        int64_t end_secondary_key
    );

    // Preallocated: fixed-size file with n_blocks slots, never grows.
    // Pass n_blocks == 0 to create a growable file (see allocate_growable).
    static void allocate(
        const std::string& file_name,
        uint32_t block_size,
        uint32_t n_blocks
    );

    // Growable: file starts at just the header and extends on demand.
    // max_blocks == 0 means unbounded (grow until disk is full).
    static void allocate_growable(
        const std::string& file_name,
        uint32_t block_size,
        uint32_t max_blocks = 0
    );

    bool is_growable() const { return _n_blocks == 0; }

    // Grow the file by some number of blocks (BoltDB-style: doubles up to a
    // 1 GiB-per-grow cap, then linear). Caller must already hold a sqlite
    // IMMEDIATE transaction. Returns the first new block (status='reserved');
    // any extras are inserted as 'free'. Returns nullopt if max_blocks cap
    // is already reached. Public only so _db_get_block can call it; treat as
    // internal.
    std::optional<block> _grow_blocks(const nts_sqlite_conn& conn);

 private:

    // EBR limbo: blocks the writer has retired (catalog ops done, but bytes
    // not yet overwritten). _limbo entries that have cleared EBR safety get
    // migrated to _ready, ready to be consumed by the next acquire.
    struct LimboEntry {
        int64_t  block_id;
        int64_t  block_idx;
        uint64_t retired_epoch;
    };

    // Top-level block acquisition under EBR. Tries (1) ready queue, (2) free
    // block, (3) growable extension, (4) one retire-into-limbo followed by
    // bounded rescans of that same victim. Returned block is always safe to
    // pass through
    // _recycle_block: either truly free (already zeroed), freshly grown, or
    // an EBR-cleared retired block.
    block _acquire_writable_block(const nts_sqlite_conn& conn);

    // Move limbo entries that have cleared EBR safety into the ready queue.
    // Caller must NOT hold _limbo_mu.
    void _scan_limbo();

    std::string _file_name;
    uint64_t _file_size;
    nts_file _file;
    nts_memory_map _file_header_mm;
    uint8_t* _file_header_p;
    uint32_t _block_size;
    uint32_t _n_blocks;       // 0 == growable mode (sentinel)
    uint32_t _max_blocks;     // growable cap; 0 == unbounded; ignored if !growable
    bool _auto_reclaim;
    std::set<std::string> _active_stream_tags;

    // EBR state. _epoch is shared with every iterator and writer on this file.
    std::shared_ptr<nanots_epoch_registry> _epoch;
    std::deque<LimboEntry> _limbo;
    std::deque<LimboEntry> _ready;
    std::mutex _limbo_mu;
};

struct contiguous_segment {
    int64_t segment_id{0};
    int64_t start_timestamp{0};
    int64_t start_secondary_key{NANOTS_SEC_KEY_UNSET};
    int64_t end_timestamp{0};
    int64_t end_secondary_key{NANOTS_SEC_KEY_UNSET};
};

class NANOTS_API nanots_reader {
 public:
    nanots_reader(const std::string& file_name);
    nanots_reader(const nanots_reader&) = delete;
    nanots_reader(nanots_reader&&) = default;
    nanots_reader& operator=(const nanots_reader&) = delete;
    nanots_reader& operator=(nanots_reader&&) = default;
    ~nanots_reader() = default;

    // Stream all frames whose composite (timestamp, secondary_key) falls
    // within [(start_ts, start_sk), (end_ts, end_sk)] inclusive. Pass
    // NANOTS_SEC_KEY_UNSET for start_secondary_key and INT64_MAX for
    // end_secondary_key to ignore the sec_key axis.
    //
    // Callback signature is:
    //   (data, size, flags, timestamp, secondary_key, block_sequence, metadata)
    // `secondary_key` is NANOTS_SEC_KEY_UNSET for frames whose writer didn't
    // supply one.
    void read(
        const std::string& stream_tag,
        int64_t start_timestamp,
        int64_t start_secondary_key,
        int64_t end_timestamp,
        int64_t end_secondary_key,
        const std::function<
            void(const uint8_t*, size_t, uint32_t, int64_t, int64_t, int64_t, const std::string&)>& callback
    );

    // Distinct stream tags that have any block overlapping the composite
    // window. Use NANOTS_SEC_KEY_UNSET / INT64_MAX for sec_key bounds to
    // ignore that axis.
    std::vector<std::string> query_stream_tags(
        int64_t start_timestamp,
        int64_t start_secondary_key,
        int64_t end_timestamp,
        int64_t end_secondary_key
    );

    // Contiguous runs of blocks for this stream within the composite window.
    // Use NANOTS_SEC_KEY_UNSET / INT64_MAX for sec_key bounds to ignore
    // that axis.
    std::vector<contiguous_segment> query_contiguous_segments(
        const std::string& stream_tag,
        int64_t start_timestamp,
        int64_t start_secondary_key,
        int64_t end_timestamp,
        int64_t end_secondary_key
    );

 private:
    std::string _file_name;
    nts_file _file;
    uint32_t _block_size;
    uint32_t _n_blocks;

    // EBR slot. Acquired on construction, released on destruction. read() opens
    // a critical section for its duration so the writer cannot overwrite block
    // bytes while the callback is dereferencing them.
    nanots_slot_guard _slot_guard;
};

struct frame_info {
    const uint8_t* data{nullptr};
    size_t size{0};
    uint32_t flags{0};
    int64_t timestamp{0};
    int64_t secondary_key{NANOTS_SEC_KEY_UNSET};
    int64_t block_sequence{0};
};

struct block_info {
    int64_t block_idx{0};
    int64_t block_sequence{0};
    int64_t segment_id{0};
    std::string metadata;
    std::string uuid_hex;
    int64_t start_timestamp{0};
    std::optional<int64_t> end_timestamp;
    int64_t start_secondary_key{NANOTS_SEC_KEY_UNSET};
    std::optional<int64_t> end_secondary_key;

    // Loaded block data
    nts_memory_map mm;
    uint8_t* block_p{nullptr};
    uint32_t n_valid_indexes{0};
    uint8_t uuid[16];
    bool is_loaded{false};
};

class NANOTS_API nanots_iterator {
 public:
    nanots_iterator(
        const std::string& file_name,
        const std::string& stream_tag
    );
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

    // Find the first frame whose composite (timestamp, secondary_key) is >=
    // the requested composite. Pass `secondary_key` to seek to a specific
    // tiebroken position; omit it (defaults to NANOTS_SEC_KEY_UNSET) to land
    // on the first frame at the requested timestamp regardless of sec_key —
    // because INT64_MIN is the smallest possible sec_key.
    bool find(int64_t timestamp,
              int64_t secondary_key = NANOTS_SEC_KEY_UNSET
    );

    bool seek_end();               // Go to last frame
    void reset();                   // Go to first frame

    // Utility
    int64_t current_block_sequence() const { return _current_block_sequence; }
    const std::string& current_metadata() const;

 private:
    // In-memory index of all blocks in this stream, sorted by start_ts
    // (equivalently by (segment_id, sequence) since both are monotonic for a
    // given stream). Built lazily on first navigation operation; refreshed when
    // a stale entry is detected or when navigation runs off the cached end.
    // Replaces per-op SQL queries in _find_block_for_timestamp /
    // _get_first/last/next/prev_block with in-memory binary search.
    struct BlockRange {
      int64_t start_ts;
      std::optional<int64_t> end_ts;  // nullopt = open block
      int64_t segment_id;
      int64_t sequence;
      int64_t start_sk;     // NANOTS_SEC_KEY_UNSET if stream has no sec key
      std::optional<int64_t> end_sk;
    };

    block_info* _get_block_by_segment_and_sequence(int64_t segment_id, int64_t sequence);
    block_info* _get_first_block();
    block_info* _get_last_block();
    block_info* _get_next_block();
    block_info* _get_prev_block();
    block_info* _find_block_for_composite(int64_t timestamp, int64_t sec_key);

    // Build (or rebuild) the timestamp index from SQL. _ensure_ts_index is the
    // lazy entry point used by navigation methods.
    void _ensure_ts_index();
    void _refresh_ts_index();

    // Returns the position in _ts_index of the block matching ts, or
    // _ts_index.size() if none. Matches the old SQL semantics: first prefers a
    // block that covers ts (start_ts <= ts <= end_ts, or has no end); otherwise
    // returns the first block with start_ts >= ts.
    // Composite lower_bound: returns the position of the first block whose
    // (start_ts, start_sk) is >= (ts, sk), or the block that covers it.
    size_t _ts_index_find(int64_t ts, int64_t sk) const;

    // True if _ts_index_pos matches (current_segment_id, current_block_sequence).
    bool _ts_index_pos_is_valid() const;

    // Linear search _ts_index for (current_segment_id, current_block_sequence)
    // and update _ts_index_pos. Returns true on success.
    bool _ts_index_relocate();

    bool _load_block_data(block_info& block);
    bool _refresh_committed_index_count(block_info& block);
    bool _load_current_frame();
    void _select_block(const block_info& block, size_t frame_idx);

    std::string _file_name;
    std::string _stream_tag;
    nts_file _file;
    uint32_t _block_size;

    // Current position
    int64_t _current_block_sequence;
    int64_t _current_segment_id;
    size_t _current_frame_idx;
    int64_t _current_block_start_ts;
    std::optional<int64_t> _current_block_end_ts;
    int64_t _current_block_start_sk;
    std::optional<int64_t> _current_block_end_sk;

    // Cache of visited blocks (segment_id:sequence -> block_info)
    // Using string key for simplicity: "segment_id:sequence"
    std::unordered_map<std::string, block_info> _block_cache;

    // Timestamp index. See BlockRange above. _ts_index_pos tracks the position
    // in _ts_index corresponding to (_current_segment_id, _current_block_sequence)
    // so operator++/-- can advance via array indexing instead of SQL.
    std::vector<BlockRange> _ts_index;
    bool _ts_index_filled{false};
    size_t _ts_index_pos{0};

    // Cached current frame
    frame_info _current_frame;
    bool _valid;
    bool _initialized;

    // Lazily-initialized database connection for read operations
    std::optional<nts_sqlite_conn> _db_conn;
    nts_sqlite_conn& _ensure_db_connection();

    // Cached prepared statements (lazily initialized on first use)
    std::optional<nts_sqlite_stmt> _stmt_get_block_by_id;
    std::optional<nts_sqlite_stmt> _stmt_get_first_block;
    std::optional<nts_sqlite_stmt> _stmt_get_last_block;
    std::optional<nts_sqlite_stmt> _stmt_next_in_segment;
    std::optional<nts_sqlite_stmt> _stmt_next_cross_segment;
    std::optional<nts_sqlite_stmt> _stmt_prev_in_segment;
    std::optional<nts_sqlite_stmt> _stmt_prev_cross_segment;
    std::optional<nts_sqlite_stmt> _stmt_find_block_containing;
    std::optional<nts_sqlite_stmt> _stmt_find_block_ge;

    // EBR slot. Acquired on construction, released on destruction. Each
    // navigation operation (find, ++, --, reset, seek_end) calls op_begin()
    // to publish the current global epoch + diagnostic heartbeat. This permits
    // the writer to know it's safe to physically recycle a block.
    nanots_slot_guard _slot_guard;
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
  int64_t start_secondary_key;
  int64_t end_timestamp;
  int64_t end_secondary_key;
} nanots_contiguous_segment_t;

typedef struct {
  const uint8_t* data;
  size_t size;
  uint32_t flags;
  int64_t timestamp;
  int64_t secondary_key;
  int64_t block_sequence;
} nanots_frame_info_t;

typedef void (*nanots_read_callback_t)(
    const uint8_t* data,
    size_t size,
    uint32_t flags,
    int64_t timestamp,
    int64_t secondary_key,
    int64_t block_sequence,
    const char* metadata,
    void* user_data
);

NANOTS_API nanots_ec_t nanots_writer_allocate_file(
    const char* file_name,
    uint32_t block_size,
    uint32_t n_blocks
);

NANOTS_API nanots_ec_t nanots_writer_allocate_growable_file(
    const char* file_name,
    uint32_t block_size,
    uint32_t max_blocks
);

// writer
NANOTS_API nanots_writer_t nanots_writer_create(
    const char* file_name,
    int auto_reclaim
);

NANOTS_API void nanots_writer_destroy(nanots_writer_t writer);

NANOTS_API nanots_write_context_t nanots_writer_create_context(
    nanots_writer_t writer,
    const char* stream_tag,
    const char* metadata
);

NANOTS_API void nanots_write_context_destroy(nanots_write_context_t context);

// Pass NANOTS_SEC_KEY_UNSET (INT64_MIN) for `secondary_key` to write into
// a stream that doesn't use a tiebreaker. Frames are ordered by the
// composite (timestamp, secondary_key) which must be strictly monotonic.
//
// Argument order mirrors the C++ method: flags, then timestamp, then the
// secondary key. The C ABI has no defaults — pass all 7 args.
NANOTS_API nanots_ec_t nanots_writer_write(
    nanots_writer_t writer,
    nanots_write_context_t context,
    const uint8_t* data,
    size_t size,
    uint32_t flags,
    int64_t timestamp,
    int64_t secondary_key
);

// Free blocks fully contained in the composite window
// [(start_ts, start_sk), (end_ts, end_sk)]. Use NANOTS_SEC_KEY_UNSET and
// INT64_MAX for the sec_key bounds to ignore that axis.
NANOTS_API nanots_ec_t nanots_writer_free_blocks(
    const char* file_name,
    const char* stream_tag,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key
);

// reader
NANOTS_API nanots_reader_t nanots_reader_create(const char* file_name);

NANOTS_API void nanots_reader_destroy(nanots_reader_t reader);

// Stream frames whose composite (timestamp, secondary_key) lies within
// [(start_ts, start_sk), (end_ts, end_sk)]. Use NANOTS_SEC_KEY_UNSET and
// INT64_MAX for the sec_key bounds to ignore that axis.
NANOTS_API nanots_ec_t nanots_reader_read(
    nanots_reader_t reader,
    const char* stream_tag,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key,
    nanots_read_callback_t callback,
    void* user_data
);

NANOTS_API nanots_ec_t nanots_reader_query_contiguous_segments(
    nanots_reader_t reader,
    const char* stream_tag,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key,
    nanots_contiguous_segment_t** segments,
    size_t* count
);

NANOTS_API void nanots_free_contiguous_segments(nanots_contiguous_segment_t* segments);

NANOTS_API nanots_ec_t nanots_reader_query_stream_tags_start(
    nanots_reader_t reader,
    int64_t start_timestamp,
    int64_t start_secondary_key,
    int64_t end_timestamp,
    int64_t end_secondary_key
);

NANOTS_API const char* nanots_reader_query_stream_tags_next(nanots_reader_t reader);

// iterator
NANOTS_API nanots_iterator_t nanots_iterator_create(
    const char* file_name,
    const char* stream_tag
);

NANOTS_API void nanots_iterator_destroy(nanots_iterator_t iterator);

NANOTS_API int nanots_iterator_valid(nanots_iterator_t iterator);

NANOTS_API nanots_ec_t nanots_iterator_get_current_frame(
    nanots_iterator_t iterator,
    nanots_frame_info_t* frame_info
);

NANOTS_API nanots_ec_t nanots_iterator_next(nanots_iterator_t iterator);

NANOTS_API nanots_ec_t nanots_iterator_prev(nanots_iterator_t iterator);

// Find the first frame whose composite (timestamp, secondary_key) is >= the
// requested composite. Pass NANOTS_SEC_KEY_UNSET for `secondary_key` to land
// on the first frame at the requested timestamp.
NANOTS_API nanots_ec_t nanots_iterator_find(
    nanots_iterator_t iterator,
    int64_t timestamp,
    int64_t secondary_key
);

NANOTS_API nanots_ec_t nanots_iterator_reset(nanots_iterator_t iterator);

NANOTS_API nanots_ec_t nanots_iterator_seek_end(nanots_iterator_t iterator);

NANOTS_API int64_t nanots_iterator_current_block_sequence(nanots_iterator_t iterator);

NANOTS_API const char* nanots_iterator_current_metadata(nanots_iterator_t iterator);

#ifdef __cplusplus
}
#endif

#endif


#endif // NANOTS_AMALGAMATED_H