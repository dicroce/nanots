
#include "test_nanots_ebr.h"
#include "nanots.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

REGISTER_TEST_FIXTURE(test_nanots_ebr);

namespace {

constexpr const char* EBR_FILE = "nanots_test_ebr.nts";
constexpr uint32_t    EBR_BLOCK_SIZE = 65536;   // 64 KB
constexpr uint32_t    EBR_N_BLOCKS   = 8;

int64_t _now_us_test() {
  using namespace std::chrono;
  return duration_cast<microseconds>(
             steady_clock::now().time_since_epoch())
      .count();
}

void _whack_ebr_files() {
  for (const char* suffix : {".nts", ".db", ".db-shm", ".db-wal"}) {
    std::string p = std::string("nanots_test_ebr") + suffix;
    if (rtf_file_exists(p)) rtf_remove_file(p);
  }
}

// Build a payload where every 8-byte word equals the timestamp. A reader can
// verify data integrity by checking that all words in the payload match the
// frame's reported timestamp; a corrupted/torn read would produce mismatches.
void _fill_payload_with_ts(std::vector<uint8_t>& buf, int64_t ts) {
  size_t n = buf.size() / sizeof(int64_t);
  auto* p = reinterpret_cast<int64_t*>(buf.data());
  for (size_t i = 0; i < n; ++i) p[i] = ts;
  // Tail bytes (if not aligned) are left at whatever they were.
}

bool _verify_payload_matches_ts(const uint8_t* data, size_t size, int64_t ts) {
  size_t n = size / sizeof(int64_t);
  auto* p = reinterpret_cast<const int64_t*>(data);
  for (size_t i = 0; i < n; ++i) {
    if (p[i] != ts) return false;
  }
  return true;
}

}  // namespace

void test_nanots_ebr::setup() {
  _whack_ebr_files();
  nanots_writer::allocate(EBR_FILE, EBR_BLOCK_SIZE, EBR_N_BLOCKS);
}

void test_nanots_ebr::teardown() {
  _whack_ebr_files();
}

// ---------------------------------------------------------------------------
// Test: auto_reclaim works the same as before when no readers are active.
// ---------------------------------------------------------------------------
void test_nanots_ebr::test_basic_auto_reclaim_no_readers() {
  // Smaller frame size so we get a few frames per block; with 8 blocks of
  // 64 KB each, we want to fill+reclaim several times.
  constexpr size_t FRAME_SIZE = 4096;
  constexpr int    N_FRAMES   = 2000;   // ~16× the block pool

  std::vector<uint8_t> payload(FRAME_SIZE, 0xAB);

  nanots_writer writer(EBR_FILE, /*auto_reclaim=*/true);
  auto wctx = writer.create_write_context("stream_a", "meta");

  for (int i = 0; i < N_FRAMES; ++i) {
    int64_t ts = 1000 + i * 100;
    _fill_payload_with_ts(payload, ts);
    RTF_ASSERT_NO_THROW(
        writer.write(wctx, payload.data(), payload.size(), ts, 0));
  }
  // If we got here, auto_reclaim worked. With no readers, every retire
  // should clear limbo immediately and migrate to ready.
}

// ---------------------------------------------------------------------------
// Test: an active iterator pins reclaim; destroying it allows reclaim.
// ---------------------------------------------------------------------------
//
// Directly probes the registry: after an iterator does an op, can_recycle()
// for retire epochs >= that op's epoch should return false. After the
// iterator is destroyed (slot goes INACTIVE), can_recycle should return true.
//
void test_nanots_ebr::test_reader_pins_then_releases() {
  // Write a few frames so the iterator has something to point at.
  {
    nanots_writer writer(EBR_FILE, /*auto_reclaim=*/false);
    auto wctx = writer.create_write_context("stream_a", "meta");
    std::vector<uint8_t> payload(256, 0xCD);
    for (int i = 0; i < 32; ++i) {
      int64_t ts = 1000 + i * 100;
      _fill_payload_with_ts(payload, ts);
      writer.write(wctx, payload.data(), payload.size(), ts, 0);
    }
  }

  auto registry = nanots_epoch_registry::get_or_create(EBR_FILE);
  uint64_t epoch_before_iter = registry->global_epoch_load();

  {
    nanots_iterator iter(EBR_FILE, "stream_a");
    RTF_ASSERT(iter.valid());

    // The iterator's op_begin during construction published the current
    // global_epoch. Any future retire bumps the global_epoch, so a retire
    // happening now would be at a strictly later epoch than what the
    // iterator's slot holds — and can_recycle should return false.
    uint64_t simulated_retire_epoch = registry->global_epoch_bump();
    RTF_ASSERT(simulated_retire_epoch >= epoch_before_iter);

    bool can = registry->can_recycle(simulated_retire_epoch, _now_us_test());
    RTF_ASSERT(can == false);  // iterator's slot pins this retire
  }

  // Iterator destroyed → slot INACTIVE. Same retire epoch should now be
  // reclaimable.
  uint64_t retire_epoch = registry->global_epoch_load() - 1;
  RTF_ASSERT(registry->can_recycle(retire_epoch, _now_us_test()) == true);
}

// ---------------------------------------------------------------------------
// Test: an INACTIVE slot does not pin reclaim.
// ---------------------------------------------------------------------------
//
// Ensures that slots which have never been populated, or which were released,
// don't accidentally block reclaim. Specifically this guards against the slot
// pool growing without an iterator (which would default-construct slots with
// epoch=INACTIVE and never block).
//
void test_nanots_ebr::test_inactive_slot_does_not_pin() {
  auto registry = nanots_epoch_registry::get_or_create(EBR_FILE);

  // With no iterators alive, any retire should be reclaimable immediately.
  uint64_t e = registry->global_epoch_bump();
  RTF_ASSERT(registry->can_recycle(e, _now_us_test()) == true);

  // Even with many bumps, still reclaimable.
  for (int i = 0; i < 100; ++i) e = registry->global_epoch_bump();
  RTF_ASSERT(registry->can_recycle(e, _now_us_test()) == true);
}

// ---------------------------------------------------------------------------
// Test: a slot whose heartbeat is stale is treated as dead.
// ---------------------------------------------------------------------------
//
// Simulates a reader that "died" — its slot still has a non-INACTIVE epoch
// but its heartbeat is far in the past. The writer's reclaim should treat
// this slot as dead and proceed.
//
void test_nanots_ebr::test_dead_heartbeat_does_not_pin() {
  // Write a few frames so the iterator can find something.
  {
    nanots_writer writer(EBR_FILE, false);
    auto wctx = writer.create_write_context("stream_a", "meta");
    std::vector<uint8_t> payload(256, 0xEF);
    for (int i = 0; i < 8; ++i) {
      writer.write(wctx, payload.data(), payload.size(), 1000 + i * 100, 0);
    }
  }

  auto registry = nanots_epoch_registry::get_or_create(EBR_FILE);

  nanots_iterator iter(EBR_FILE, "stream_a");
  RTF_ASSERT(iter.valid());

  uint64_t retire_epoch = registry->global_epoch_bump();

  // Sanity: live iterator pins this retire.
  RTF_ASSERT(registry->can_recycle(retire_epoch, _now_us_test()) == false);

  // Now forcibly age the iterator's heartbeat far past the timeout.
  // (Public API: we walk slots from index 0; the iterator owns the first one.)
  registry->slot(0).heartbeat_us.store(0, std::memory_order_relaxed);

  // can_recycle treats now-vs-heartbeat as the liveness signal. now_us is
  // huge (microseconds since steady_clock epoch); heartbeat is 0; diff is
  // way past timeout. Slot should be ignored.
  RTF_ASSERT(registry->can_recycle(retire_epoch, _now_us_test()) == true);
}

// ---------------------------------------------------------------------------
// Test: each iterator op advances the slot's published epoch.
// ---------------------------------------------------------------------------
//
// This is what allows the writer to make reclaim progress when a long-lived
// iterator continues to do operations: between ops, the writer's retires
// might be stuck, but as the iterator calls another op, the slot advances
// to the current global epoch, freeing up older retires.
//
void test_nanots_ebr::test_iterator_op_advances_epoch() {
  {
    nanots_writer writer(EBR_FILE, false);
    auto wctx = writer.create_write_context("stream_a", "meta");
    std::vector<uint8_t> payload(256, 0x11);
    for (int i = 0; i < 8; ++i) {
      writer.write(wctx, payload.data(), payload.size(), 1000 + i * 100, 0);
    }
  }

  auto registry = nanots_epoch_registry::get_or_create(EBR_FILE);
  nanots_iterator iter(EBR_FILE, "stream_a");

  uint64_t e_after_ctor = registry->slot(0).epoch.load(std::memory_order_acquire);

  // Bump the global epoch.
  for (int i = 0; i < 5; ++i) registry->global_epoch_bump();

  uint64_t e_after_bumps = registry->global_epoch_load();
  RTF_ASSERT(e_after_bumps > e_after_ctor);

  // Iterator's slot is still at e_after_ctor (hasn't done an op).
  RTF_ASSERT_EQUAL(registry->slot(0).epoch.load(std::memory_order_acquire),
                   e_after_ctor);

  // Do an iterator op — should advance the slot.
  iter.reset();

  uint64_t e_after_op = registry->slot(0).epoch.load(std::memory_order_acquire);
  RTF_ASSERT(e_after_op >= e_after_bumps);
}

// ---------------------------------------------------------------------------
// Stress test: concurrent writer + readers with auto_reclaim.
// ---------------------------------------------------------------------------
//
// Payload integrity check: every frame's payload is the frame's timestamp
// repeated. Any torn read (writer overwriting bytes the reader is observing)
// would produce a payload that doesn't match the timestamp. If EBR is broken
// or absent, this should detect the corruption.
//
void test_nanots_ebr::test_stress_concurrent_writer_readers() {
  constexpr size_t FRAME_SIZE   = 1024;
  constexpr int    N_READERS    = 4;
  constexpr int    RUN_SECONDS  = 2;   // keep total test time bounded

  std::atomic<bool>     stop{false};
  std::atomic<uint64_t> frames_written{0};
  std::atomic<uint64_t> frames_read{0};
  std::atomic<uint64_t> integrity_failures{0};
  std::atomic<int64_t>  max_written_ts{0};

  // Producer thread.
  std::thread writer_thread([&]() {
    nanots_writer writer(EBR_FILE, /*auto_reclaim=*/true);
    auto wctx = writer.create_write_context("stream_a", "stress");
    std::vector<uint8_t> payload(FRAME_SIZE, 0);

    int64_t ts = 1000;
    while (!stop.load(std::memory_order_relaxed)) {
      _fill_payload_with_ts(payload, ts);
      try {
        writer.write(wctx, payload.data(), payload.size(), ts, 0);
        frames_written.fetch_add(1, std::memory_order_relaxed);
        max_written_ts.store(ts, std::memory_order_relaxed);
        ts += 100;
      } catch (const nanots_exception&) {
        // Limbo cap might fire under extreme reader pressure; tolerate it.
        std::this_thread::yield();
      }
    }
  });

  // Consumer threads. Each runs nanots_reader::read() over the latest
  // committed timestamp range. Verifies every frame's payload matches its ts.
  std::vector<std::thread> reader_threads;
  reader_threads.reserve(N_READERS);
  for (int r = 0; r < N_READERS; ++r) {
    reader_threads.emplace_back([&, r]() {
      nanots_reader reader(EBR_FILE);
      while (!stop.load(std::memory_order_relaxed)) {
        int64_t hi = max_written_ts.load(std::memory_order_relaxed);
        if (hi < 1000) {
          std::this_thread::yield();
          continue;
        }
        int64_t lo = hi > 100000 ? hi - 100000 : 1000;
        try {
          reader.read("stream_a", lo, hi,
              [&](const uint8_t* data, size_t size, uint8_t /*flags*/,
                  int64_t ts, int64_t /*block_seq*/,
                  const std::string& /*meta*/) {
                if (!_verify_payload_matches_ts(data, size, ts)) {
                  integrity_failures.fetch_add(1, std::memory_order_relaxed);
                }
                frames_read.fetch_add(1, std::memory_order_relaxed);
              });
        } catch (const nanots_exception&) {
          // Stream might be empty or the requested range may have been
          // recycled out from under us — tolerate.
        }
        (void)r;
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
  stop.store(true, std::memory_order_relaxed);

  writer_thread.join();
  for (auto& t : reader_threads) t.join();

  // Must have made meaningful progress. Throughput is naturally throttled
  // by EBR under reader pressure (writer waits for reader slots to advance
  // before recycling), so these thresholds are conservative — the real
  // assertion is integrity below.
  RTF_ASSERT(frames_written.load() > 100);
  RTF_ASSERT(frames_read.load() > 100);

  // Load-bearing assertion: every frame's payload-vs-timestamp invariant
  // held across every observed read. If EBR were broken (writer overwriting
  // bytes a reader was dereferencing), this would be non-zero.
  RTF_ASSERT_EQUAL(integrity_failures.load(), 0u);
}

// ---------------------------------------------------------------------------
// Test: ts_index refresh picks up new blocks written after construction.
// ---------------------------------------------------------------------------
//
// An iterator builds its timestamp index lazily on the first navigation op.
// After that snapshot, a writer may append more blocks to the stream. A
// subsequent find() for a timestamp in the newly-written range must trigger
// a tail refresh and locate the block.
//
void test_nanots_ebr::test_ts_index_refresh_finds_new_blocks() {
  // Small payloads so two phases of writes fit comfortably in the 8-block
  // pool without needing auto_reclaim.
  std::vector<uint8_t> payload(512, 0x55);

  // Phase 1: write enough frames to fill multiple blocks.
  {
    nanots_writer writer(EBR_FILE, /*auto_reclaim=*/false);
    auto wctx = writer.create_write_context("stream_a", "meta");
    for (int i = 0; i < 32; ++i) {
      int64_t ts = 1000 + i * 100;
      _fill_payload_with_ts(payload, ts);
      writer.write(wctx, payload.data(), payload.size(), ts, 0);
    }
  }

  // Open iterator; do a find() to force ts_index build.
  nanots_iterator iter(EBR_FILE, "stream_a");
  RTF_ASSERT(iter.find(1000));
  RTF_ASSERT_EQUAL(iter->timestamp, (int64_t)1000);

  // Phase 2: append more frames at later timestamps. The iterator's snapshot
  // doesn't know about these.
  {
    nanots_writer writer(EBR_FILE, /*auto_reclaim=*/false);
    auto wctx = writer.create_write_context("stream_a", "meta");
    for (int i = 32; i < 64; ++i) {
      int64_t ts = 1000 + i * 100;
      _fill_payload_with_ts(payload, ts);
      writer.write(wctx, payload.data(), payload.size(), ts, 0);
    }
  }

  // find() for a NEW timestamp. Without refresh, this would return false;
  // with refresh, the iterator detects the miss past the cached end, refreshes
  // its ts_index, and locates the block.
  int64_t new_ts = 1000 + 50 * 100;
  RTF_ASSERT(iter.find(new_ts));
  RTF_ASSERT_EQUAL(iter->timestamp, new_ts);

  // operator++ across the boundary should also work via refresh.
  RTF_ASSERT(iter.find(1000 + 30 * 100));  // last frame of original batch
  ++iter;
  RTF_ASSERT(iter.valid());
  ++iter;
  RTF_ASSERT(iter.valid());
  // We should now be reading into the post-refresh region.
  RTF_ASSERT(iter->timestamp > 1000 + 31 * 100);
}
