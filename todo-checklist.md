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

### 10. ~~Metadata Persistence~~ FIXED
   - `metadata.bin` written atomically (tmp + fsync + rename) during checkpoint
   - Format: magic + version + next_page_id + last_checkpoint_lsn + CRC32 checksum (32 bytes)
   - `page_manager_create()` loads metadata.bin on startup, restores `next_page_id`
   - Existing data files (pages_XXXXX.dat) reopened automatically on startup via directory scan
   - Fixed bug: `data_art_checkpoint()` was passing `nodes_allocated` (wrong) instead of actual `next_page_id`
   - WAL checkpoint entry also restores `next_page_id` during replay (fallback if metadata.bin missing)
   - Tests: metadata survives restart (next_page_id=105 preserved), WAL-only fallback (deleted metadata.bin, restored from checkpoint entry)

### 11. ~~Page Reuse~~ SUPERSEDED by Append-Only
   - Originally: free lists recycled page IDs, deferred free for MVCC safety
   - **Now**: append-only storage — `page_manager_free()` marks pages dead (`PAGE_GC_FLAG_DEAD`), `page_manager_alloc()` always increments `next_page_id`. Space reclaimed by future compaction.
   - Page index persisted as `pages.idx` (atomic save/load), metadata v2 persists append cursor
   - `page_index_clear()` for full WAL replay recovery
   - Result: 25/25 tests pass

### 12. ~~Iterator Seek / Range Queries~~ FIXED
   - `data_art_iterator_seek(iter, key, key_len)` positions iterator at first key >= target
   - Targeted descent from root: compares partial prefixes and finds ceiling children at each level
   - `find_child_ge()` helper handles all 4 node types (Node4/16: linear scan, Node48/256: byte scan)
   - Tests: exact match, between-keys, before-all/past-all boundaries, range scan (300 keys from sorted[100..399]), empty tree
   - Result: 11/11 iterator tests pass, 23/23 full suite

### 13. ~~Batch Insert~~ TEMP FIX
   - `data_art_insert_batch(tree, keys[], key_lens[], values[], value_lens[], count)` — atomic multi-key insert
   - `data_art_batch(tree, ops[], count)` — mixed insert+delete batch via `data_art_batch_op_t`
   - Both wrap existing txn API: `begin_txn` → buffer ops → `commit_txn` (single fsync)
   - Atomicity: any failure aborts entire batch (no partial application)
   - Tests: basic 100-key batch, atomicity rollback, mixed insert+delete (50-20+30=60), WAL recovery, empty batch
   - Result: 10/10 txn_buffer tests pass, 23/23 full suite
   - **Needs optimization**: commit path acquires/releases write_lock N times (once per op), publishes root N times, allocates N auto-commit MVCC txns. A dedicated batch path should hold the lock once, apply all mutations, and publish root once.

### 14. ~~Prefix Iteration~~ FIXED
   - `data_art_iterator_create_prefix(tree, prefix, prefix_len)` — iterate keys sharing a prefix
   - Seeks to first key >= prefix, then stops when prefix diverges in `next()`
   - NULL/empty prefix falls back to full iteration
   - Tests: basic (3 prefixes, verify only target prefix returned), no match, all match, empty prefix
   - Result: 15/15 iterator tests pass, 23/23 full suite

### 15. ~~Torn Page Detection~~ FIXED
   - Repurposed unused `flags` field as `write_counter` in `page_header_t` (no header size change)
   - Write counter incremented on every `page_manager_write()`, mirrored at page tail (last 4 bytes, `PAGE_TAIL_MARKER_OFFSET`)
   - On read: CRC fails → check header counter vs tail counter → `PAGE_ERROR_TORN_WRITE` if mismatch, `PAGE_ERROR_CORRUPTION` if match
   - Also catches edge case: CRC passes but counters mismatch (torn boundary at exact CRC-neutral point)
   - `PAGE_DATA_SIZE` constant (4028 bytes) documents usable data area excluding tail marker
   - Tests: simulated torn write (half-page pwrite), write counter increment verification (5 writes → counter=5)
   - Result: all 23/23 tests pass

### 16. ~~Online Backup / Snapshot Export~~ FIXED
   - Binary backup format: 32B header + sequential key-value entries + 16B footer with CRC32-C checksums
   - `db_backup_export()` — creates iterator (snapshot isolation), writes all entries with running CRC
   - `db_backup_import()` — validates header/footer/checksums, batch inserts in groups of 1000
   - `db_backup_info()` — reads header metadata (entry_count, timestamp, key_size) without importing
   - Added incremental CRC32-C API (`compute_crc32_begin/update/finish`) to crc32.h
   - Flags field encodes key_size for import validation (bits 16-31)
   - Tests: empty tree, single key, 1000 keys, 8KB overflow values, metadata, corrupted header/data, truncated file, invalid paths
   - Result: 9/9 backup tests pass, 24/24 full suite

### 17. ~~Page Compression (LZ4/ZSTD)~~ FIXED
   - Transparent compression in page_manager write/read paths — callers unchanged
   - `page_compress()`/`page_decompress()` helpers with `#ifdef HAVE_LZ4`/`HAVE_ZSTD` guards
   - `page_manager_write()` auto-compresses when enabled, skips if savings < 10%
   - `page_manager_read()` auto-decompresses based on `compression_type` in page index
   - `page_manager_set_compression(pm, type)` for runtime configuration
   - `page_manager_write_compressed(pm, page, type)` for explicit tier selection
   - `compression_type_t` enum: NONE=0, LZ4=1, ZSTD_5=2, ZSTD_19=3
   - LZ4 enabled by default — patterned data: 98.4% savings (4096 → 64 bytes)
   - Tests: 12 tests (84 assertions) — round-trip, skip logic, mixed reads, persistence, corruption detection
   - Result: 25/25 tests pass
   - **Future**: background compactor for tier promotion based on `last_access_time`

### 18. ~~Database Compaction~~ FIXED
   - In-place defragmentation: moves live pages forward within each data file, ftruncate() to reclaim space
   - No temporary files or extra disk space needed (critical for 100GB-1TB+ Ethereum state)
   - Pages only move to lower offsets (write cursor always behind read cursor)
   - Crash-safe via compaction journal (`compaction.journal`): records move plan, idempotent replay on recovery
   - `db_compaction_recover()` handles all crash states (mid-move, post-truncate, post-index-save)
   - Compressed pages copied as raw bytes (no decompress/recompress)
   - Dead-at-end optimization: no moves needed when dead pages are only at file tail (just truncate)
   - `db_compaction_analyze()` for read-only fragmentation analysis, `db_compaction_is_recommended()` (>30%)
   - Tests: 16 tests (171 assertions) — analyze, in-place moves, interleaved dead, journal recovery, persistence
   - Result: 26/26 tests pass

### 19. Batch Insert Optimization
   - Current TEMP FIX: commit path acquires/releases write_lock N times, publishes root N times
   - Dedicated batch path: hold lock once, apply all mutations to tree, publish root once
   - Single MVCC txn for entire batch instead of N auto-commit txns
   - Expected: 5-10x throughput improvement for large batches

### 20. Write Batching / Group Commit
   - Currently every `commit_txn` does its own fsync (~5ms each)
   - Group commit: buffer multiple transactions, single fsync for the group
   - Amortize disk latency across many commits (critical for high-throughput workloads)
   - WAL already uses write buffer — extend with commit coalescing

### 21. Bloom Filters for Negative Lookups
   - Negative lookups (key not found) currently traverse tree to disk
   - Per-page or per-level bloom filter avoids I/O for keys that definitely don't exist
   - Important for Ethereum where misses are common (state trie probing)
   - Space-efficient: ~10 bits per key for <1% false positive rate

### 22. Incremental Backup
   - Current backup exports all keys every time
   - Delta backup: only keys/pages changed since last backup (tracked via LSN or write_counter)
   - Much faster for large databases with small daily changes
   - Could use WAL LSN watermark to identify changed pages

### 23. Monitoring / Metrics Export
   - Basic stats exist (pages_read/written, bytes, cache hits) but no time-series
   - Add latency histograms for read/write/commit operations
   - Cache hit rate over sliding window
   - Export interface for Prometheus or similar monitoring systems

### 24. File Pre-allocation
   - Data files currently grow on demand via pwrite
   - Pre-allocate with `fallocate()` to reduce filesystem fragmentation
   - Improves sequential write throughput and reduces metadata overhead
   - Allocate in chunks (e.g. 64MB) when file grows past current allocation

### 25. State Pruning Policy
   - MVCC + page GC infrastructure exists but no policy layer
   - Prune state versions older than N snapshots/checkpoints
   - Ethereum-specific: keep only last N blocks of state, reclaim the rest
   - Ties into compaction: pruning marks pages dead, compaction reclaims space

### 26. Merkle Proof Generation
   - If ART replaces Merkle Patricia Trie, need state root computation and inclusion/exclusion proofs
   - Compute hash of each node (bottom-up), root hash = state root
   - Proof = path from root to leaf with sibling hashes at each level
   - Could be a separate layer on top of the ART (hash-augmented nodes)

### 27. Encryption at Rest
   - No data encryption on disk currently
   - Page-level encryption: encrypt before write, decrypt after read
   - Key management: master key + per-file or per-page derived keys
   - Transparent to buffer pool and upper layers

### 28. CLI (Command-Line Interface)
   - Combined admin + client tool for interacting with the database
   - **Key-value operations**: `get <key>`, `put <key> <value>`, `delete <key>`, `scan [prefix]`
   - **Transaction support**: `begin`, `commit`, `abort` for multi-key atomic updates
   - **Admin commands**: `stats` (page manager, buffer pool, MVCC), `compact` (trigger compaction), `checkpoint` (force checkpoint), `backup <path>` / `restore <path>`
   - **Inspection**: `info` (database metadata, compression ratio, dead bytes), `pages [page_id]` (page index details), `health` (fsync health state)
   - **Interactive REPL mode** + single-command mode (`artdb get mykey`)
   - Opens database from a path argument: `artdb --db /path/to/db`
   - Read-only mode: `artdb --db /path/to/db --readonly`
   - Builds on all existing APIs: data_art, page_manager, buffer_pool, checkpoint_manager, db_backup

### 29. Dead Code Cleanup
   - Remove unused functions (data_art_cow_node, data_art_snapshot, data_art_load, etc.)
   - Remove dead static functions in `data_art_node_ops.c` (shrink stubs already implemented in data_art_delete.c)
   - Remove debug printfs in `mem_art.c`, duplicate log lines in `wal.c`
   - Remove unused includes (`<stdio.h>` in search.c, `<assert.h>` in delete.c)
   - Remove `page_manager_replace_data_fd()` (leftover from copy-based compaction, unused now)
