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

### 3. Fsync Retry Logic (1 day)
   - Retry fsync failures 3x with exponential backoff
   - Add health monitoring (db_health_t)
   - Test: Inject ENOSPC, verify graceful degradation

### 4. Error Handling & Return Codes (1-2 days)
   - Define db_error_t enum (replace bool/-1)
   - Add get_last_error() API for detailed messages
   - Distinguish: "disk full" vs "corruption" vs "I/O error"
   - Mechanical but touches many files

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
