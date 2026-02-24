## Next Steps (Production Readiness) — Ordered by Complexity

### 0. ~~MVCC GC: Committed Transactions Never Retired~~ FIXED
   - `mvcc_commit_txn()` now calls `txn_map_remove` + `epoch_gc_retire` (same as abort)
   - `mvcc_get_txn_state()` returns COMMITTED for removed entries (txn_id < next_txn_id)
   - Result: active_count stays bounded (~9 during stress test), retired count > 0

### 1. ~~Page Corruption Handling~~ FIXED
   - Checksum verification already existed (`page_verify_checksum`)
   - Test added: corrupt page on disk via pwrite, verify `PAGE_ERROR_CORRUPTION` on read
   - Test verifies repair path: write new valid page, read back succeeds

### 2. ~~Snapshot Timeout Enforcement~~ FIXED
   - Added `created_at` timestamp (CLOCK_MONOTONIC) to `mvcc_snapshot_t`
   - Added `snapshot_timeout_ms` to manager (default 0 = disabled)
   - `mvcc_expire_snapshots()` force-closes expired snapshots, called during periodic GC
   - Tests: basic expiration, selective (old vs new), disabled timeout, 100 snapshots bulk expire

### 3. ~~Fsync Retry Logic~~ FIXED
   - `fsync_with_retry()` in page_manager with 3x exponential backoff
   - `db_health_t` state machine (OK → DEGRADED → FAILING)
   - `page_manager_get_health()` API
   - Test: health transitions on bad fd

### 4. ~~Error Handling & Return Codes~~ FIXED
   - Added `db_error.h` with 13-value `db_error_t` enum (superset of `page_result_t`)
   - Thread-local error trace stack: `DB_ERROR(code, fmt, ...)` macro captures `__FILE__`, `__LINE__`, `__func__`
   - 8-frame deep trace: frame[0] = root cause, frame[depth-1] = outermost caller
   - `db_error_trace_print(FILE*)` for formatted diagnostics
   - Instrumented 28 error sites across 6 files (insert/delete/commit/abort/flush, mvcc, wal)
   - Non-breaking: all `bool` signatures unchanged, backward-compat `db_set_last_error()` preserved
   - Tests: 10 tests (backward compat, single/multi-frame traces, overflow, print format, 16-thread isolation)

### 5. Crash Recovery Tests (1-2 days) 🔴 CRITICAL
   - Test: Kill process mid-write, verify data on restart
   - Test: Corrupt WAL segment, verify graceful failure
   - Test: Power loss simulation (truncated writes)
   - Validates: Durability guarantee actually works

### 6. Checkpointing Implementation (2-3 days) 🔴 CRITICAL
   - Background checkpoint thread
   - Test: Verify checkpoint reduces recovery time
   - Test: Concurrent checkpoint + writes don't corrupt
   - Prevents: Unbounded WAL growth

### 7. Iterator Support (3-4 days) — HARDEST
   - Implement persistent iterator with page pinning
   - Test: Iterate 1M entries without running out of memory
   - Test: Iterator + concurrent writes maintain consistency
   - Required for: Ethereum state scanning

### 8. Dead Code Cleanup
   - Remove `db_compaction.h` (entire unimplemented header)
   - Remove 10 unused functions (data_art_cow_node, data_art_snapshot, data_art_load, etc.)
   - Remove 4 dead static functions (shrink_node16_to_node4, shrink_node48_to_node16, etc.)
   - Remove debug printfs in `mem_art.c`, duplicate log lines in `wal.c`
   - Remove unused includes (`<stdio.h>` in search.c, `<assert.h>` in delete.c)

Total: ~2 weeks to production-ready
