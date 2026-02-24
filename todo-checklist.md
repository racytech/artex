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

### 5. ~~Crash Recovery Tests~~ FIXED
   - End-to-end recovery: insert 100 keys → close → reopen → `data_art_recover(tree, 0)` → all 100 verified
   - Truncated WAL: `truncate()` last entry mid-payload → 59/60 keys recovered, partial entry skipped
   - Corrupted WAL: `pwrite()` garbage into last entry → CRC32 mismatch detected, 19/20 keys recovered
   - Checkpoint in WAL: 50 inserts + checkpoint + 30 inserts → full replay recovers all 80 keys
   - Bug fixed: `data_art_recover` deadlocked (WAL re-logging during replay) — now disables WAL+MVCC during replay
   - Bug fixed: `data_art_flush` false failure — `buffer_pool_flush_all` returns int, not bool

### 6. ~~Checkpointing Implementation~~ FIXED
   - `checkpoint_manager_t` with background pthread (`pthread_cond_timedwait` loop)
   - Checkpoint sequence: `wal_should_checkpoint()` → `data_art_checkpoint()` → `wal_checkpoint_completed()` → `wal_truncate()`
   - `checkpoint_manager_force()` for on-demand checkpoint with blocking wait
   - Tests: start/stop lifecycle, force checkpoint, concurrent writes + checkpoint (1000 keys, 4 checkpoints), WAL truncation (4→2 segments)
   - Result: WAL growth bounded, old segments automatically cleaned up

### 7. ~~Iterator Support~~ FIXED
   - Stack-based DFS iterator yielding leaves in lexicographic key order
   - Captures committed root atomically at creation (snapshot isolation under concurrent writes)
   - Each `next()` resets TLS arena, reloads nodes from `node_ref_t` — no persistent page pinning needed
   - Overflow value reading supported via `data_art_read_overflow_value()`
   - Tests: empty tree, single key, sorted order (100 keys), large scale (10,000 keys), concurrent writes, overflow values

### 8. Dead Code Cleanup
   - Remove `db_compaction.h` (entire unimplemented header)
   - Remove 10 unused functions (data_art_cow_node, data_art_snapshot, data_art_load, etc.)
   - Remove 4 dead static functions (shrink_node16_to_node4, shrink_node48_to_node16, etc.)
   - Remove debug printfs in `mem_art.c`, duplicate log lines in `wal.c`
   - Remove unused includes (`<stdio.h>` in search.c, `<assert.h>` in delete.c)

Total: ~2 weeks to production-ready
