
/* NANOTS */

#include "nanots.h"

std::mutex current_stream_tags_lok;
std::set<std::string> current_stream_tags;

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
  std::lock_guard<std::mutex> g(_slots_mu);
  for (const auto& slot_ptr : _slots) {
    uint64_t e = slot_ptr->epoch.load(std::memory_order_acquire);
    if (e == INACTIVE) continue;
    if (e > retired_epoch) continue;

    // Slot is at-or-before the retire. Check liveness via heartbeat.
    int64_t hb = slot_ptr->heartbeat_us.load(std::memory_order_relaxed);
    if (now_us - hb > NANOTS_HEARTBEAT_TIMEOUT_US) continue;  // dead, ignore

    return false;  // active reader still pinning this retire
  }
  return true;
}

std::shared_ptr<nanots_epoch_registry>
nanots_epoch_registry::get_or_create(const std::string& file_path) {
  std::lock_guard<std::mutex> g(g_registry_table_mu);

  auto it = g_registry_table.find(file_path);
  if (it != g_registry_table.end()) {
    if (auto sp = it->second.lock()) return sp;
    // weak_ptr expired; fall through and create a new one.
  }

  auto sp = std::make_shared<nanots_epoch_registry>();
  g_registry_table[file_path] = sp;
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
    op_begin();  // publish initial epoch + heartbeat
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
  int64_t timestamp = *(int64_t*)(index_p + INDEX_ENTRY_TS_OFFSET);
  uint64_t offset = *(uint64_t*)(index_p + INDEX_ENTRY_OFFSET_OFFSET);

  // Skip zeroed entries
  if (timestamp == 0 || offset == 0) {
    return false;
  }

  // Check frame offset bounds
  uint32_t index_region_end = BLOCK_HEADER_SIZE + ((n_valid_indexes + 1) * INDEX_ENTRY_SIZE);
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
          "WHERE sb.end_timestamp = 0");
      
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

      #ifdef _WIN32
          uint32_t n_valid_indexes = *reinterpret_cast<volatile uint32_t*>(valid_counter);
          _ReadWriteBarrier(); // compiler barrier (not mem)
      #else
          uint32_t n_valid_indexes = __atomic_load_n(valid_counter, std::memory_order_acquire);
      #endif

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

  // v2-only build: any pre-v2 database is rejected. (Fresh DBs created by
  // allocate() are stamped to version 2 below.)
  if (current_version != 0 && current_version < 2) {
    throw nanots_exception(NANOTS_EC_BAD_VERSION,
                           "Legacy nanots catalog (pre-v2) is not supported.",
                           __FILE__, __LINE__);
  }

  switch (current_version) {
    case 0: {
      nts_sqlite_transaction(
          conn, true, [&](const nts_sqlite_conn& conn) { _set_db_version(conn, 2); });
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

// Look up whether any existing segment for this stream tag uses the secondary
// key. Returns nullopt if no segment exists yet for this stream (caller's
// choice will become the canonical one), or the 0/1 value otherwise.
static std::optional<int> _db_lookup_stream_has_secondary_key(
    const nts_sqlite_conn& conn, const std::string& stream_tag) {
  auto stmt = conn.prepare(
      "SELECT has_secondary_key FROM segments WHERE stream_tag = ? LIMIT 1");
  auto rows = stmt.bind(1, stream_tag).exec();
  if (rows.empty()) return std::nullopt;
  return std::stoi(rows.front()["has_secondary_key"].value());
}

static std::optional<segment> _db_create_segment(const nts_sqlite_conn& conn,
                                                 const std::string& stream_tag,
                                                 const std::string& metadata,
                                                 bool has_secondary_key) {
  auto stmt = conn.prepare(
      "INSERT INTO segments (stream_tag, metadata, has_secondary_key) VALUES (?, ?, ?)");
  stmt.bind(1, stream_tag)
      .bind(2, metadata)
      .bind(3, has_secondary_key ? 1 : 0)
      .exec_no_result();

  segment s;
  s.id = std::stoll(conn.last_insert_id());
  s.stream_tag = stream_tag;
  s.metadata = metadata;
  s.sequence = 0;
  s.has_secondary_key = has_secondary_key;
  return s;
}

static std::optional<segment_block> _db_create_segment_block(
    const nts_sqlite_conn& conn,
    int64_t segment_id,
    int64_t sequence,
    int64_t block_id,
    int64_t block_idx,
    int64_t start_timestamp,
    int64_t end_timestamp,
    int64_t start_secondary_key,
    int64_t end_secondary_key,
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
      .bind(6, end_timestamp)
      .bind(7, start_secondary_key)
      .bind(8, end_secondary_key)
      .bind(9, hex_uuid)
      .exec_no_result();

  struct segment_block sb;
  sb.id = std::stoll(conn.last_insert_id());
  sb.segment_id = segment_id;
  sb.sequence = sequence;
  sb.block_id = block_id;
  sb.block_idx = block_idx;
  sb.start_timestamp = start_timestamp;
  sb.end_timestamp = end_timestamp;
  sb.start_secondary_key = start_secondary_key;
  sb.end_secondary_key = end_secondary_key;
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
  _upgrade_db(db);
  _validate_blocks(_file_name);
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
  // 1. Try the ready queue (limbo entries that have already cleared EBR).
  {
    std::lock_guard<std::mutex> g(_limbo_mu);
    if (!_ready.empty()) {
      auto e = _ready.front();
      _ready.pop_front();
      return block{e.block_id, e.block_idx};
    }
  }

  // 2. Try a truly free block.
  if (auto b = _db_get_free_block(conn)) {
    return *b;
  }

  // 3. Growable: extend the file.
  if (is_growable()) {
    if (auto b = _grow_blocks(conn)) {
      return *b;
    }
  }

  // 4. Auto-reclaim: retire blocks into limbo and consume from ready.
  if (_auto_reclaim) {
    constexpr int MAX_RETRIES = 100;
    for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
      // Cap check before retiring.
      {
        std::lock_guard<std::mutex> g(_limbo_mu);
        if (_limbo.size() >= LIMBO_MAX_ENTRIES) {
          throw nanots_exception(
              NANOTS_EC_NO_FREE_BLOCKS,
              "EBR limbo cap exceeded; pinned readers blocking reclaim.",
              __FILE__, __LINE__);
        }
      }

      // Retire one block (SQL: delete its segment_block, mark reserved).
      auto victim = _db_reclaim_oldest_used_block(conn);
      if (!victim) {
        // Nothing finalized to retire (either no blocks at all, or every
        // remaining block is still being actively written). Fall through
        // to the throw below.
        break;
      }

      uint64_t retired_epoch = _epoch->global_epoch_bump();
      {
        std::lock_guard<std::mutex> g(_limbo_mu);
        _limbo.push_back({victim->id, victim->idx, retired_epoch});
      }

      // Try to migrate cleared entries to ready and consume one.
      _scan_limbo();
      {
        std::lock_guard<std::mutex> g(_limbo_mu);
        if (!_ready.empty()) {
          auto e = _ready.front();
          _ready.pop_front();
          return block{e.block_id, e.block_idx};
        }
      }

      // No ready block yet — readers haven't advanced their epochs. Yield
      // briefly and retry.
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
  if (wctx.last_timestamp && timestamp <= wctx.last_timestamp.value())
    throw nanots_exception(NANOTS_EC_NON_MONOTONIC_TIMESTAMP, "Timestamp is not monotonic.", __FILE__, __LINE__);

  if (size >
      _block_size - (FRAME_HEADER_SIZE + INDEX_ENTRY_SIZE + BLOCK_HEADER_SIZE))
    throw nanots_exception(NANOTS_EC_ROW_SIZE_TOO_BIG, "Frame size is too large. Use a much larger block size.", __FILE__, __LINE__);

  // First write to this context: lazily create (or attach to) the segment
  // row. The very first write to a brand-new stream tag fixes the choice of
  // whether the stream stores a secondary key on every frame.
  if (!wctx.current_segment) {
    bool wants_sec_key = (secondary_key != NANOTS_SEC_KEY_UNSET);

    nts_sqlite_conn conn(_database_name(_file_name), true, true);
    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
      auto existing = _db_lookup_stream_has_secondary_key(conn, wctx.stream_tag);
      if (existing.has_value()) {
        bool existing_yes = (*existing != 0);
        if (existing_yes != wants_sec_key) {
          throw nanots_exception(
              NANOTS_EC_SECONDARY_KEY_MISMATCH,
              "Stream's secondary-key mode is fixed by its first write and "
              "this write does not match.", __FILE__, __LINE__);
        }
      }
      wctx.current_segment = _db_create_segment(
          conn, wctx.stream_tag, wctx.metadata, wants_sec_key);
      if (!wctx.current_segment)
        throw nanots_exception(NANOTS_EC_UNABLE_TO_CREATE_SEGMENT, "Unable to create segment.", __FILE__, __LINE__);
    });
  } else {
    // Subsequent writes: enforce the per-stream choice that was locked in
    // on the first write.
    bool stream_wants = wctx.current_segment->has_secondary_key;
    bool this_write_provides = (secondary_key != NANOTS_SEC_KEY_UNSET);
    if (stream_wants != this_write_provides) {
      throw nanots_exception(
          NANOTS_EC_SECONDARY_KEY_MISMATCH,
          "Stream's secondary-key mode is fixed and this write does not match.",
          __FILE__, __LINE__);
    }
  }

  if (secondary_key != NANOTS_SEC_KEY_UNSET &&
      wctx.last_secondary_key.has_value() &&
      secondary_key <= wctx.last_secondary_key.value()) {
    throw nanots_exception(NANOTS_EC_NON_MONOTONIC_SECONDARY_KEY,
                           "Secondary key is not monotonic.",
                           __FILE__, __LINE__);
  }

  if (!wctx.current_block) {
    nts_sqlite_conn conn(_database_name(_file_name), true, true);

    // Acquire a physical block under EBR. The returned block is always safe
    // to overwrite: it is either a freshly-free block, a newly-grown block,
    // or a retired block that has cleared EBR safety. Block acquisition
    // happens outside the transaction below because retiring may need
    // multiple sub-transactions and yields while waiting on readers.
    block phys = _acquire_writable_block(conn);

    nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
      uint8_t uuid[16];
      generate_entropy_id(uuid);

      wctx.current_block = _db_create_segment_block(
          conn, wctx.current_segment->id, wctx.current_segment->sequence,
          phys.id, phys.idx, timestamp, 0, secondary_key, 0, uuid);

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
    uint64_t last_frame_offset = *(uint64_t*)(last_index_p + INDEX_ENTRY_OFFSET_OFFSET);
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
  if (secondary_key != NANOTS_SEC_KEY_UNSET)
    wctx.last_secondary_key = secondary_key;
}

void nanots_writer::free_blocks(const std::string& file_name,
                                const std::string& stream_tag,
                                int64_t start_timestamp,
                                int64_t end_timestamp) {
  auto db_name = _database_name(file_name);
  nts_sqlite_conn conn(db_name, true, true);

  nts_sqlite_transaction(conn, true, [&](const nts_sqlite_conn& conn) {
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
      "metadata STRING, "
      "has_secondary_key INTEGER NOT NULL DEFAULT 0"
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

  _upgrade_db(db);
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
}

static int _compare_index_entry_timestamp(uint8_t* index_entry_p,
                                          uint8_t* target_timestamp_p) {
  int64_t entry_timestamp = *(int64_t*)(index_entry_p + INDEX_ENTRY_TS_OFFSET);
  int64_t target_timestamp = *(int64_t*)target_timestamp_p;

  if (entry_timestamp < target_timestamp)
    return -1;
  if (entry_timestamp > target_timestamp)
    return 1;
  return 0;
}

static int _compare_index_entry_secondary_key(uint8_t* index_entry_p,
                                              uint8_t* target_sk_p) {
  int64_t entry_sk = *(int64_t*)(index_entry_p + INDEX_ENTRY_SECKEY_OFFSET);
  int64_t target_sk = *(int64_t*)target_sk_p;

  if (entry_sk < target_sk)
    return -1;
  if (entry_sk > target_sk)
    return 1;
  return 0;
}

void nanots_reader::read(
    const std::string& stream_tag,
    int64_t start_timestamp,
    int64_t end_timestamp,
    const std::function<
        void(const uint8_t*, size_t, uint32_t, int64_t, int64_t, int64_t, const std::string&)>& callback) {
  // EBR critical section spans the entire read(): the writer must not
  // overwrite any block whose bytes the callback might dereference.
  nanots_op_scope _op(_slot_guard);

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
      int64_t timestamp = *(int64_t*)(index_p + INDEX_ENTRY_TS_OFFSET);
      uint64_t offset   = *(uint64_t*)(index_p + INDEX_ENTRY_OFFSET_OFFSET);

      // Check if we've passed the end time
      if (timestamp > end_timestamp)
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
    "  SELECT "
    "    sb.segment_id, "
    "    sb.sequence, "
    "    sb.start_timestamp, "
    "    sb.end_timestamp, "
    "    ROW_NUMBER() OVER (PARTITION BY sb.segment_id ORDER BY sb.sequence) "
    "      - sb.sequence AS group_key "
    "  FROM segment_blocks sb "
    "  JOIN segments s ON sb.segment_id = s.id "
    "  WHERE sb.start_timestamp <= ? "                    /* bind(1) = window_end    */
    "    AND (sb.end_timestamp >= ? OR sb.end_timestamp = 0) " /* bind(2) = window_start */
    "    AND s.stream_tag = ? "                           /* bind(3) = stream_tag    */
    "), "
    "region_boundaries AS ( "
    "  SELECT "
    "    segment_id, "
    "    group_key, "
    "    MIN(start_timestamp) AS region_start, "
    "    CASE "
    "      WHEN MIN(end_timestamp) = 0 THEN 0 "
    "      ELSE MAX(end_timestamp) "
    "    END AS region_end, "
    "    COUNT(*) AS block_count "
    "  FROM contiguous_groups "
    "  GROUP BY segment_id, group_key "
    ") "
    "SELECT "
    "  segment_id, "
    "  region_start, "
    "  region_end, "
    "  block_count "
    "FROM region_boundaries "
    "ORDER BY segment_id, region_start;"
  );
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
      _current_block_start_ts(0),
      _current_block_end_ts(0),
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

  // Initialize to first frame if stream exists
  reset();
}

bool nanots_iterator::has_secondary_key() {
  if (_has_secondary_key_cache >= 0) return _has_secondary_key_cache != 0;
  auto& db = _ensure_db_connection();
  auto stmt = db.prepare(
      "SELECT has_secondary_key FROM segments WHERE stream_tag = ? LIMIT 1");
  auto rows = stmt.bind(1, _stream_tag).exec();
  int v = 0;
  if (!rows.empty()) {
    auto& cell = rows.front()["has_secondary_key"];
    if (cell.has_value()) v = std::stoi(cell.value());
  }
  _has_secondary_key_cache = v;
  return v != 0;
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
        "s.has_secondary_key as has_secondary_key, "
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
  block.end_timestamp = std::stoll(row["end_timestamp"].value());
  auto& hsk = row["has_secondary_key"];
  block.has_secondary_key = hsk.has_value() && std::stoi(hsk.value()) != 0;
  auto& sk_start = row["start_secondary_key"];
  block.start_secondary_key = sk_start.has_value()
      ? std::stoll(sk_start.value()) : NANOTS_SEC_KEY_UNSET;
  auto& sk_end = row["end_secondary_key"];
  block.end_secondary_key = sk_end.has_value()
      ? std::stoll(sk_end.value()) : NANOTS_SEC_KEY_UNSET;

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
    br.end_ts     = std::stoll(row["end_timestamp"].value());
    auto& s_sk = row["start_secondary_key"];
    auto& e_sk = row["end_secondary_key"];
    br.start_sk = s_sk.has_value() ? std::stoll(s_sk.value())
                                   : NANOTS_SEC_KEY_UNSET;
    br.end_sk   = e_sk.has_value() ? std::stoll(e_sk.value())
                                   : NANOTS_SEC_KEY_UNSET;
    _ts_index.push_back(br);
  }
  _ts_index_filled = true;
}

size_t nanots_iterator::_ts_index_find_by_sk(int64_t sk) const {
  // Prefer a block that covers sk.
  auto it = std::upper_bound(_ts_index.begin(), _ts_index.end(), sk,
      [](int64_t k, const BlockRange& br) { return k < br.start_sk; });
  if (it != _ts_index.begin()) {
    auto prev = std::prev(it);
    if (sk >= prev->start_sk &&
        (prev->end_sk == 0 || sk <= prev->end_sk)) {
      return static_cast<size_t>(std::distance(_ts_index.begin(), prev));
    }
  }
  if (it != _ts_index.end()) {
    return static_cast<size_t>(std::distance(_ts_index.begin(), it));
  }
  return _ts_index.size();
}

size_t nanots_iterator::_ts_index_find(int64_t ts) const {
  // Prefer a block that covers ts.
  auto it = std::upper_bound(_ts_index.begin(), _ts_index.end(), ts,
      [](int64_t t, const BlockRange& br) { return t < br.start_ts; });
  if (it != _ts_index.begin()) {
    auto prev = std::prev(it);
    if (ts >= prev->start_ts &&
        (prev->end_ts == 0 || ts <= prev->end_ts)) {
      return static_cast<size_t>(std::distance(_ts_index.begin(), prev));
    }
  }
  // Fallback: first block with start_ts >= ts (matches old SQL semantics).
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

block_info* nanots_iterator::_find_block_for_timestamp(int64_t timestamp) {
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
  size_t pos = _ts_index_find(timestamp);
  if (auto* b = try_at(pos)) return b;

  // Miss or stale: refresh and try once more. This catches:
  //   (a) writer added blocks past the cached end
  //   (b) writer reclaimed the block we matched and the new (segment, sequence)
  //       at that idx isn't in our cache
  _refresh_ts_index();
  pos = _ts_index_find(timestamp);
  return try_at(pos);
}

block_info* nanots_iterator::_find_block_for_secondary_key(int64_t sec_key) {
  _ensure_ts_index();

  auto try_at = [&](size_t pos) -> block_info* {
    if (pos >= _ts_index.size()) return nullptr;
    const auto& br = _ts_index[pos];
    auto* b = _get_block_by_segment_and_sequence(br.segment_id, br.sequence);
    if (b) _ts_index_pos = pos;
    return b;
  };

  size_t pos = _ts_index_find_by_sk(sec_key);
  if (auto* b = try_at(pos)) return b;

  _refresh_ts_index();
  pos = _ts_index_find_by_sk(sec_key);
  return try_at(pos);
}

bool nanots_iterator::find_by_secondary_key(int64_t sec_key) {
  nanots_op_scope _op(_slot_guard);

  if (!has_secondary_key()) {
    _valid = false;
    return false;
  }

  block_info* block = _find_block_for_secondary_key(sec_key);
  if (!block) {
    _valid = false;
    return false;
  }

  if (!_load_block_data(*block)) {
    _valid = false;
    return false;
  }

  _current_block_start_ts = block->start_timestamp;
  _current_block_end_ts   = block->end_timestamp;
  _current_segment_id     = block->segment_id;
  _current_block_sequence = block->block_sequence;

  uint8_t* index_start = block->block_p + BLOCK_HEADER_SIZE;
  uint8_t* index_end   = index_start + (block->n_valid_indexes * INDEX_ENTRY_SIZE);

  uint8_t* found_entry =
      lower_bound_bytes(index_start, index_end, (uint8_t*)&sec_key,
                        INDEX_ENTRY_SIZE,
                        _compare_index_entry_secondary_key);

  _current_frame_idx = (found_entry - index_start) / INDEX_ENTRY_SIZE;

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

  return _load_current_frame();
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
  nanots_op_scope _op(_slot_guard);
  block_info* block = nullptr;

  // Fast path: if the target is within the already-loaded block's range, skip
  // the SQL lookup entirely and go straight to the binary search.
  if (_valid && _current_block_end_ts != 0 &&
      timestamp >= _current_block_start_ts && timestamp <= _current_block_end_ts) {
    block = _get_block_by_segment_and_sequence(_current_segment_id, _current_block_sequence);
  }

  if (!block)
    block = _find_block_for_timestamp(timestamp);

  if (!block) {
    _valid = false;
    return false;
  }

  if (!_load_block_data(*block)) {
    _valid = false;
    return false;
  }

  _current_block_start_ts = block->start_timestamp;
  _current_block_end_ts   = block->end_timestamp;

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
  int64_t found_timestamp = *(int64_t*)(index_p + INDEX_ENTRY_TS_OFFSET);
  uint64_t offset = *(uint64_t*)(index_p + INDEX_ENTRY_OFFSET_OFFSET);

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
  _current_frame.timestamp = found_timestamp;
  _current_frame.secondary_key = sec_key;
  _current_frame.block_sequence = block->block_sequence;

  _valid = true;
  return true;
}

void nanots_iterator::reset() {
  nanots_op_scope _op(_slot_guard);
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

bool nanots_iterator::seek_end() {
  nanots_op_scope _op(_slot_guard);
  auto* block = _get_last_block();
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
  _current_frame_idx = block->n_valid_indexes - 1;
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
                                          int64_t end_timestamp) {
  if (!file_name || !stream_tag) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    nanots_writer::free_blocks(std::string(file_name), std::string(stream_tag), start_timestamp, end_timestamp);
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

nanots_ec_t nanots_iterator_find_by_secondary_key(nanots_iterator_t iterator,
                                                  int64_t secondary_key) {
  if (!iterator || !iterator->iterator) {
    return NANOTS_EC_INVALID_ARGUMENT;
  }

  try {
    bool found = iterator->iterator->find_by_secondary_key(secondary_key);
    return found ? NANOTS_EC_OK : NANOTS_EC_INVALID_ARGUMENT;
  } catch (const nanots_exception& e) {
    return e.get_ec();
  } catch (const std::exception& e) {
    fprintf(stderr,"Exception in nanots_iterator_find_by_secondary_key: %s\n", e.what());
    return NANOTS_EC_UNKNOWN;
  } catch (...) {
    fprintf(stderr,"Exception in nanots_iterator_find_by_secondary_key\n");
    return NANOTS_EC_UNKNOWN;
  }
}

int nanots_iterator_has_secondary_key(nanots_iterator_t iterator) {
  if (!iterator || !iterator->iterator) {
    return 0;
  }
  try {
    return iterator->iterator->has_secondary_key() ? 1 : 0;
  } catch (...) {
    return 0;
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
