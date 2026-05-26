
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
  NANOTS_EC_UNKNOWN = 12,
  NANOTS_EC_NOT_FOUND = 13,
  NANOTS_EC_BAD_MAGIC = 14,
  NANOTS_EC_BAD_VERSION = 15
  // Reserved 16-17 (previously SECONDARY_KEY_MISMATCH and
  // NON_MONOTONIC_SECONDARY_KEY). Composite-key semantics fold both into
  // NANOTS_EC_NON_MONOTONIC_TIMESTAMP, which now means "composite (ts,
  // sec_key) is not strictly greater than the previous frame's composite."
};

}

class NANOTS_API nanots_exception : public std::exception {
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
//   24 : feature_bits   (u64; bit 0 = secondary_key bit reserved for future
//                       per-file features. Per-stream secondary-key opt-in
//                       is recorded in segments.has_secondary_key.)
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
//   8  : secondary_key (int64; NANOTS_SEC_KEY_UNSET if the stream has none)
//   16 : offset        (uint64; byte offset of frame in block)
//   24 : reserved      (uint64; zeroed)
#define INDEX_ENTRY_SIZE 32
#define INDEX_ENTRY_TS_OFFSET      0
#define INDEX_ENTRY_SECKEY_OFFSET  8
#define INDEX_ENTRY_OFFSET_OFFSET 16
#define INDEX_ENTRY_RESERVED_OFFSET 24

// v2 frame header: 16 + 8 + 4 + 4 = 32 bytes (8-byte aligned, payload at +32).
//   0  : uuid[16]
//   16 : secondary_key (int64; NANOTS_SEC_KEY_UNSET if the stream has none)
//   24 : size          (uint32)
//   28 : flags         (uint32 — widened from u8 in v2)
#define FRAME_HEADER_SIZE 32
#define FRAME_UUID_OFFSET        0
#define FRAME_SECKEY_OFFSET     16
#define FRAME_SIZE_OFFSET       24
#define FRAME_FLAGS_OFFSET      28

// Sentinel for "no secondary key on this frame". Streams either always have
// one or never have one; the choice is fixed at the first write into the
// stream and stored in segments.has_secondary_key.
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
  int64_t end_timestamp{0};
  int64_t start_secondary_key{NANOTS_SEC_KEY_UNSET};
  int64_t end_secondary_key{NANOTS_SEC_KEY_UNSET};
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
// Hot-path cost: one atomic load + one atomic store per iterator operation;
// one atomic increment per writer retire.
//
// Within a single process only. Cross-process readers are not protected.
//
class NANOTS_API nanots_epoch_registry {
 public:
  static constexpr uint64_t INACTIVE = UINT64_MAX;

  struct Slot {
    std::atomic<uint64_t> epoch{INACTIVE};
    std::atomic<int64_t>  heartbeat_us{0};
  };

  // Hot-path operations.
  uint64_t global_epoch_load() const {
    return _global_epoch.load(std::memory_order_acquire);
  }
  uint64_t global_epoch_bump() {
    return _global_epoch.fetch_add(1, std::memory_order_release);
  }

  // Returns true if no active slot's epoch is <= retired_epoch. Slots whose
  // heartbeat is older than NANOTS_HEARTBEAT_TIMEOUT_US are treated as dead
  // and ignored.
  bool can_recycle(uint64_t retired_epoch, int64_t now_us) const;

  // Accessor for slot_guard.
  Slot& slot(uint32_t id);

  // Acquire/release; called by nanots_slot_guard.
  uint32_t acquire_slot();
  void     release_slot(uint32_t id);

  // Path-keyed singleton accessor. Same path string returns the same registry
  // for the lifetime of any caller holding the shared_ptr.
  static std::shared_ptr<nanots_epoch_registry> get_or_create(
      const std::string& file_path);

 private:
  std::atomic<uint64_t>               _global_epoch{1};
  mutable std::mutex                  _slots_mu;
  std::vector<std::unique_ptr<Slot>>  _slots;
};

// Heartbeat timeout: a slot whose last heartbeat is older than this is
// treated as dead by can_recycle().
constexpr int64_t NANOTS_HEARTBEAT_TIMEOUT_US = 60LL * 1000 * 1000;  // 60s

// ---------------------------------------------------------------------------
// RAII wrapper for slot acquisition + publishing.
// ---------------------------------------------------------------------------
//
// On construction: acquires a slot from the registry and publishes the current
// global_epoch + heartbeat. On destruction: sets the slot to INACTIVE and
// releases it. Pairing the acquire and the release in one object means an
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

  // Publishes the latest global_epoch to our slot and updates the heartbeat.
  // Call at the start of every iterator operation. No-op if !active().
  void op_begin();

  // True if this guard owns a slot.
  bool active() const { return _slot_id != UINT32_MAX; }

 private:
  void _release() noexcept;

  std::shared_ptr<nanots_epoch_registry> _registry;
  uint32_t                                _slot_id{UINT32_MAX};
};

// ---------------------------------------------------------------------------
// Scoped op marker.
// ---------------------------------------------------------------------------
//
// Stack-only RAII wrapper that marks the start of one iterator operation.
// Construction publishes the current global_epoch + heartbeat to the slot;
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
  ~nanots_writer() = default;

  write_context create_write_context(const std::string& stream_tag,
                                     const std::string& metadata);

  // Frames are ordered by the COMPOSITE (timestamp, secondary_key) — that
  // pair must be strictly greater than the previous frame's composite. So:
  //   - timestamp may repeat; secondary_key must then strictly increase
  //   - if timestamp strictly increases, secondary_key may be anything
  // Default secondary_key = NANOTS_SEC_KEY_UNSET (= INT64_MIN) — the smallest
  // possible value, equivalent to "no tiebreaker." A stream that always
  // writes with the default gets the classic "timestamp strictly monotonic"
  // behavior because (t, MIN) > (last_t, MIN) iff t > last_t.
  void write(write_context& wctx,
             const uint8_t* data,
             size_t size,
             uint32_t flags,
             int64_t timestamp,
             int64_t secondary_key = NANOTS_SEC_KEY_UNSET);

  static void free_blocks(const std::string& file_name,
                          const std::string& stream_tag,
                          int64_t start_timestamp,
                          int64_t end_timestamp);

  // Preallocated: fixed-size file with n_blocks slots, never grows.
  // Pass n_blocks == 0 to create a growable file (see allocate_growable).
  static void allocate(const std::string& file_name,
                       uint32_t block_size,
                       uint32_t n_blocks);

  // Growable: file starts at just the header and extends on demand.
  // max_blocks == 0 means unbounded (grow until disk is full).
  static void allocate_growable(const std::string& file_name,
                                uint32_t block_size,
                                uint32_t max_blocks = 0);

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

  // Cap on outstanding limbo entries. If a long-stalled reader prevents
  // any limbo entry from clearing, retire calls throw rather than grow
  // unboundedly.
  static constexpr size_t LIMBO_MAX_ENTRIES = 1024;

  // Top-level block acquisition under EBR. Tries (1) ready queue, (2) free
  // block, (3) growable extension, (4) retire-into-limbo + ready queue with
  // bounded retries. Returned block is always safe to pass through
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
  std::deque<LimboEntry>                 _limbo;
  std::deque<LimboEntry>                 _ready;
  std::mutex                             _limbo_mu;
};

struct contiguous_segment {
  int64_t segment_id{0};
  int64_t start_timestamp{0};
  int64_t end_timestamp{0};
};

class NANOTS_API nanots_reader {
 public:
  nanots_reader(const std::string& file_name);
  nanots_reader(const nanots_reader&) = delete;
  nanots_reader(nanots_reader&&) = default;
  nanots_reader& operator=(const nanots_reader&) = delete;
  nanots_reader& operator=(nanots_reader&&) = default;
  ~nanots_reader() = default;

  // Callback signature is:
  //   (data, size, flags, timestamp, secondary_key, block_sequence, metadata)
  // `secondary_key` is NANOTS_SEC_KEY_UNSET for frames whose writer didn't
  // supply one.
  void read(
      const std::string& stream_tag,
      int64_t start_timestamp,
      int64_t end_timestamp,
      const std::function<
          void(const uint8_t*, size_t, uint32_t, int64_t, int64_t, int64_t, const std::string&)>& callback);
  
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
  int64_t end_timestamp{0};
  int64_t start_secondary_key{NANOTS_SEC_KEY_UNSET};
  int64_t end_secondary_key{NANOTS_SEC_KEY_UNSET};

  // Loaded block data
  nts_memory_map mm;
  uint8_t* block_p{nullptr};
  uint32_t n_valid_indexes{0};
  uint8_t uuid[16];
  bool is_loaded{false};
};

class NANOTS_API nanots_iterator {
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

  // Find the first frame whose composite (timestamp, secondary_key) is >=
  // the requested composite. Pass `secondary_key` to seek to a specific
  // tiebroken position; omit it (defaults to NANOTS_SEC_KEY_UNSET) to land
  // on the first frame at the requested timestamp regardless of sec_key —
  // because INT64_MIN is the smallest possible sec_key.
  bool find(int64_t timestamp,
            int64_t secondary_key = NANOTS_SEC_KEY_UNSET);

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
    int64_t end_ts;       // 0 = open block (currently being written)
    int64_t segment_id;
    int64_t sequence;
    int64_t start_sk;     // NANOTS_SEC_KEY_UNSET if stream has no sec key
    int64_t end_sk;       // 0 boundary semantics same as end_ts
  };

  block_info* _get_block_by_segment_and_sequence(int64_t segment_id, int64_t sequence);
  block_info* _get_first_block();
  block_info* _get_last_block();
  block_info* _get_next_block();
  block_info* _get_prev_block();
  block_info* _find_block_for_composite(int64_t timestamp, int64_t sec_key);

  // Build (or rebuild) the timestamp index from SQL. _ensure_ts_index is the
  // lazy entry point used by navigation methods.
  void   _ensure_ts_index();
  void   _refresh_ts_index();

  // Returns the position in _ts_index of the block matching ts, or
  // _ts_index.size() if none. Matches the old SQL semantics: first prefers a
  // block that covers ts (start_ts <= ts <= end_ts, or end_ts == 0); otherwise
  // returns the first block with start_ts >= ts.
  // Composite lower_bound: returns the position of the first block whose
  // (start_ts, start_sk) is >= (ts, sk), or the block that covers it.
  size_t _ts_index_find(int64_t ts, int64_t sk) const;

  // True if _ts_index_pos matches (current_segment_id, current_block_sequence).
  bool   _ts_index_pos_is_valid() const;

  // Linear search _ts_index for (current_segment_id, current_block_sequence)
  // and update _ts_index_pos. Returns true on success.
  bool   _ts_index_relocate();

  bool _load_block_data(block_info& block);
  bool _load_current_frame();

  std::string _file_name;
  std::string _stream_tag;
  nts_file _file;
  uint32_t _block_size;

  // Current position
  int64_t _current_block_sequence;
  int64_t _current_segment_id;
  size_t _current_frame_idx;
  int64_t _current_block_start_ts;
  int64_t _current_block_end_ts;

  // Cache of visited blocks (segment_id:sequence -> block_info)
  // Using string key for simplicity: "segment_id:sequence"
  std::unordered_map<std::string, block_info> _block_cache;

  // Timestamp index. See BlockRange above. _ts_index_pos tracks the position
  // in _ts_index corresponding to (_current_segment_id, _current_block_sequence)
  // so operator++/-- can advance via array indexing instead of SQL.
  std::vector<BlockRange> _ts_index;
  bool                    _ts_index_filled{false};
  size_t                  _ts_index_pos{0};

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
  // to publish the current global epoch + heartbeat. This is what permits
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
  int64_t end_timestamp;
} nanots_contiguous_segment_t;

typedef struct {
  const uint8_t* data;
  size_t size;
  uint32_t flags;
  int64_t timestamp;
  int64_t secondary_key;
  int64_t block_sequence;
} nanots_frame_info_t;

typedef void (*nanots_read_callback_t)(const uint8_t* data,
                                       size_t size,
                                       uint32_t flags,
                                       int64_t timestamp,
                                       int64_t secondary_key,
                                       int64_t block_sequence,
                                       const char* metadata,
                                       void* user_data);

NANOTS_API nanots_ec_t nanots_writer_allocate_file(const char* file_name, uint32_t block_size, uint32_t n_blocks);

NANOTS_API nanots_ec_t nanots_writer_allocate_growable_file(const char* file_name, uint32_t block_size, uint32_t max_blocks);

// writer
NANOTS_API nanots_writer_t nanots_writer_create(const char* file_name, int auto_reclaim);

NANOTS_API void nanots_writer_destroy(nanots_writer_t writer);

NANOTS_API nanots_write_context_t nanots_writer_create_context(nanots_writer_t writer,
                                                    const char* stream_tag,
                                                    const char* metadata);

NANOTS_API void nanots_write_context_destroy(nanots_write_context_t context);

// Pass NANOTS_SEC_KEY_UNSET (INT64_MIN) for `secondary_key` to write into
// a stream without a secondary key. The first write to a stream fixes the
// choice; mixing yields NANOTS_EC_SECONDARY_KEY_MISMATCH.
//
// Argument order mirrors the C++ method: flags, then timestamp, then the
// optional secondary key. The C ABI has no defaults — pass all 7 args.
NANOTS_API nanots_ec_t nanots_writer_write(nanots_writer_t writer,
                                    nanots_write_context_t context,
                                    const uint8_t* data,
                                    size_t size,
                                    uint32_t flags,
                                    int64_t timestamp,
                                    int64_t secondary_key);

NANOTS_API nanots_ec_t nanots_writer_free_blocks(const char* file_name,
                                      const char* stream_tag,
                                      int64_t start_timestamp,
                                      int64_t end_timestamp);

// reader
NANOTS_API nanots_reader_t nanots_reader_create(const char* file_name);

NANOTS_API void nanots_reader_destroy(nanots_reader_t reader);

NANOTS_API nanots_ec_t nanots_reader_read(nanots_reader_t reader,
                               const char* stream_tag,
                               int64_t start_timestamp,
                               int64_t end_timestamp,
                               nanots_read_callback_t callback,
                               void* user_data);

NANOTS_API nanots_ec_t nanots_reader_query_contiguous_segments(
    nanots_reader_t reader,
    const char* stream_tag,
    int64_t start_timestamp,
    int64_t end_timestamp,
    nanots_contiguous_segment_t** segments,
    size_t* count);

NANOTS_API void nanots_free_contiguous_segments(nanots_contiguous_segment_t* segments);

NANOTS_API nanots_ec_t nanots_reader_query_stream_tags_start(nanots_reader_t reader,
                                                  int64_t start_timestamp,
                                                  int64_t end_timestamp);

NANOTS_API const char* nanots_reader_query_stream_tags_next(nanots_reader_t reader);

// iterator
NANOTS_API nanots_iterator_t nanots_iterator_create(const char* file_name,
                                         const char* stream_tag);
NANOTS_API void nanots_iterator_destroy(nanots_iterator_t iterator);

NANOTS_API int nanots_iterator_valid(nanots_iterator_t iterator);

NANOTS_API nanots_ec_t nanots_iterator_get_current_frame(
    nanots_iterator_t iterator,
    nanots_frame_info_t* frame_info);

NANOTS_API nanots_ec_t nanots_iterator_next(nanots_iterator_t iterator);

NANOTS_API nanots_ec_t nanots_iterator_prev(nanots_iterator_t iterator);

// Find the first frame whose composite (timestamp, secondary_key) is >= the
// requested composite. Pass NANOTS_SEC_KEY_UNSET for `secondary_key` to land
// on the first frame at the requested timestamp.
NANOTS_API nanots_ec_t nanots_iterator_find(nanots_iterator_t iterator,
                                            int64_t timestamp,
                                            int64_t secondary_key);

NANOTS_API nanots_ec_t nanots_iterator_reset(nanots_iterator_t iterator);

NANOTS_API nanots_ec_t nanots_iterator_seek_end(nanots_iterator_t iterator);

NANOTS_API int64_t nanots_iterator_current_block_sequence(nanots_iterator_t iterator);

NANOTS_API const char* nanots_iterator_current_metadata(nanots_iterator_t iterator);

#ifdef __cplusplus
}
#endif

#endif
