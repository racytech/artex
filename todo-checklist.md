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

### 8. ~~Delete Replay Test~~ FIXED
   - Added test_delete_replay_recovery to test_crash_recovery.c
   - Insert 50 keys → delete keys 10-29 → close → reopen → recover from WAL → verify
   - 70 entries replayed (50 inserts + 20 deletes), tree size = 30, deleted keys confirmed absent
   - Validates `WAL_ENTRY_DELETE` replay path in `apply_wal_entry()`

### 9. ~~Transaction-Aware WAL Replay~~ FIXED
   - Two-pass recovery: scan WAL for BEGIN/COMMIT markers, compute uncommitted set, then replay skipping their entries
   - Auto-commit operations (no BEGIN_TXN in WAL) always applied; explicit txns without COMMIT_TXN are skipped
   - Handles corruption gracefully: scan proceeds with whatever markers collected before error
   - Tests: uncommitted txn skipped (20 auto-commit present, 10 uncommitted absent), mixed committed + uncommitted (15 present, 5 absent)
   - All 23/23 tests pass, 7/7 crash recovery tests pass

### 10. Metadata Persistence (2-3 days) 🔴 CRITICAL
   - `page_manager_create()` always resets `next_page_id = 1` — no allocator state survives restart
   - Persist to `$db_path/metadata.bin`: `next_page_id`, free list counts, `last_checkpoint_lsn`
   - Write metadata atomically during checkpoint (after flush, before WAL truncate)
   - Read on startup in `page_manager_create()` before WAL replay
   - Enables incremental recovery from last checkpoint instead of full replay from LSN 0
   - Test: insert keys → checkpoint → close → reopen → verify `next_page_id` restored, no page ID collisions

### 11. Page Reuse (1-2 days) 🔴 CRITICAL
   - `data_art_release_page()` is a no-op (data_art_core.c:322) — freed pages never returned to allocator
   - `page_manager_alloc()` always increments `next_page_id` instead of reusing free list
   - Database grows monotonically: delete 9,000 keys + insert 1,000 = 11,000 pages (should be ~1,000)
   - Fix: make `release_page()` call `page_manager_free()` (epoch GC already ensures safety)
   - Fix: `page_manager_alloc()` should check free lists first before allocating new page IDs
   - Test: insert → delete → insert → verify page count stays bounded

### 12. Iterator Seek / Range Queries (1-2 days)
   - Add `data_art_iterator_seek(iter, key)` — position iterator at first key >= given key
   - Enables efficient range scans: seek to start key, iterate until end key
   - Required for Ethereum: scan all storage slots for a specific account prefix
   - Builds on existing iterator DFS stack — just needs initial descent to target key

### 13. Batch Insert (1-2 days)
   - `data_art_insert_batch(tree, keys[], values[], count)` — atomic multi-key insert
   - Wrap in single WAL transaction (BEGIN → N inserts → COMMIT) for durability
   - Reduces fsync overhead: one fsync per batch instead of per-key
   - Ethereum use case: block state updates (hundreds of key changes per block)

### 14. Database Compaction (3-5 days)
   - Rewrite live pages into contiguous files, discard dead pages
   - Requires: iterate all live pages, copy to new file, swap atomically
   - Header `db_compaction.h` exists as stub — needs full implementation
   - Reclaims space from long-running databases with heavy delete workloads
   - Could run as background thread similar to checkpoint_manager

### 15. Torn Page Detection (1 day)
   - Currently pages have CRC32 checksum — detects corruption but can't distinguish torn write from bit-flip
   - Add page LSN or write counter to detect partial writes (half-written page after power loss)
   - Double-write buffer approach: write page to temp location first, then to final location
   - Low priority: WAL replay already recovers from most scenarios

### 16. Online Backup / Snapshot Export (2-3 days)
   - Export consistent snapshot of entire database to a directory
   - Use existing snapshot isolation: capture committed root, iterate all keys, write to new files
   - Enables point-in-time backups without stopping the database
   - Could also support incremental backup (only pages changed since last backup)

### 17. Prefix Iteration (1 day)
   - `data_art_iterator_create_prefix(tree, prefix, prefix_len)` — iterate keys sharing a prefix
   - ART naturally supports this: descend to prefix node, then iterate subtree
   - Ethereum use case: enumerate all storage keys for a specific contract address
   - Builds on iterator + seek — just adds early termination when prefix diverges

### 18. Dead Code Cleanup
   - Remove `db_compaction.h` (entire unimplemented header)
   - Remove unused functions (data_art_cow_node, data_art_snapshot, data_art_load, etc.)
   - Remove dead static functions in `data_art_node_ops.c` (shrink stubs already implemented in data_art_delete.c)
   - Remove debug printfs in `mem_art.c`, duplicate log lines in `wal.c`
   - Remove unused includes (`<stdio.h>` in search.c, `<assert.h>` in delete.c)

Total: ~2-3 weeks to production-ready (items 8-11 are critical path, 12-17 are nice-to-have)
