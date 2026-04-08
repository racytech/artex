# Flat Storage Integration Guide

## Starting Point
Branch `flat-storage-dev-0` at commit `ee29581` (mpt_store/bloom_filter/node_cache removed).
Baseline: 95,412 tests pass (8 expected failures).

## Files to Create
Already exist as untracked:
- `database/include/storage_flat.h` — flat file API
- `database/src/storage_flat.c` — implementation (mmap'd sorted key-value regions)
- `database/tests/test_storage_flat.c` — correctness + benchmarks

## Files to Modify

### 1. CMakeLists.txt
- Add `storage_flat` library: `add_library(storage_flat STATIC database/src/storage_flat.c)`
- Add test: `add_executable(test_storage_flat ...)`
- Link state to storage_flat: `target_link_libraries(state art_database hashed_art storage_flat code_store common)`
- Comment out `test_eviction`

### 2. state/include/account.h
- Remove `#include "hashed_art.h"` (hart no longer in resource_t)
- Replace resource_t fields:
  ```c
  // REMOVE: evict_count, storage (hart_t*), evict_offset, lru_prev, lru_next, acct_idx
  // ADD:
  uint32_t  stor_count;    /* live entries in flat file */
  uint64_t  stor_offset;   /* byte offset in flat file */
  uint32_t  stor_cap;      /* allocated capacity (entries) */
  uint32_t  _pad;
  ```

### 3. state/include/state.h
- Remove eviction API: `state_set_evict_path`, `state_set_evict_threshold`, `state_set_evict_budget`, `state_evict_cold_storage`, `state_compact_evict_file`, `state_set_lru_capacity`, `state_trim_storage`
- Add: `void state_set_storage_path(state_t *s, const char *dir);`
- Simplify state_stats_t: remove `stor_in_memory`, `stor_evicted`, `stor_arena_bytes`, `stor_reloads`, `stor_reload_ms`

### 4. state/src/state.c — THE BIG ONE

#### Includes
- Replace `#include "hashed_art.h"` → keep (still used for acct_index)
- Add `#include "storage_flat.h"`
- Remove `<fcntl.h>`, `<time.h>` (eviction I/O)

#### Constants
- Remove: `STOR_KEY_SIZE`, `STOR_VAL_SIZE`, `STOR_INIT_CAP`, `WHALE_THRESHOLD`
- Add: `#define DIRTY_STOR_KEY_SIZE 52`

#### Callbacks
- KEEP `stor_value_encode` — needed for hart_root_hash at snapshot time
- Signature: `static uint32_t stor_value_encode(key[32], leaf_val, rlp_out, ctx)` → `rlp_be(val, 32, rlp_out)`

#### struct state
Remove:
- `resource_list`, `resource_count`, `resource_cap`
- `evict_fd`, `evict_path[512]`, `evict_file_size`, `evict_threshold`, `evict_budget`
- `lru_head`, `lru_tail`, `lru_count`, `lru_capacity`
- `stor_arena_total`, `stor_in_memory`, `stor_evicted`, `stor_reload_count`, `stor_reload_ms`

Add:
- `storage_flat_t *stor_flat;`
- `char stor_flat_path[512];`
- `mem_art_t dirty_storage;` — (addr[20]+slot_hash[32]) → value_be[32]
- `dirty_vec_t dirty_stor_keys;` — list of 52-byte keys for iteration

#### Forward declarations
- Remove: `state_reload_storage`, `lru_touch`, `lru_push_front`, `lru_evict_tail`, `evict_ensure_fd`, `evict_one`
- Add: `static void commit_dirty_storage(state_t *s);`

#### get_resource — simplify
```c
static resource_t *get_resource(const state_t *s, const account_t *a) {
    if (!a->resource_idx) return NULL;
    return &((state_t *)s)->resources[a->resource_idx];
}
```
CRITICAL: remove ALL storage prefetch/LRU code. Old code had `r->storage` checks that compile but crash with new resource_t layout.

#### ensure_resource
- Remove `r->acct_idx = ...` line

#### DELETE entirely
- `resource_list_add`
- `ensure_storage`
- `lru_remove`, `lru_push_front`, `lru_touch`, `lru_evict_tail`
- `destroy_resource_storage`
- `state_reload_storage`
- ALL eviction functions at end of file

#### ADD: dirty_stor_put
```c
static void dirty_stor_put(state_t *s, const uint8_t addr[20],
                           const uint8_t slot_hash[32], const uint8_t val_be[32]) {
    uint8_t dkey[DIRTY_STOR_KEY_SIZE];
    memcpy(dkey, addr, 20);
    memcpy(dkey + 20, slot_hash, 32);
    mem_art_insert(&s->dirty_storage, dkey, DIRTY_STOR_KEY_SIZE, val_be, 32);
    dirty_push(&s->dirty_stor_keys, dkey, DIRTY_STOR_KEY_SIZE);
}
```

#### storage_read — rewrite
```c
static uint256_t storage_read(const state_t *s, const account_t *a,
                               const uint8_t slot_hash[32]) {
    // 1. Check dirty buffer (mem_art)
    // 2. Check ACCT_STORAGE_CLEARED → return 0
    // 3. Check flat file via storage_flat_get(stor_flat, r->stor_offset, r->stor_count, slot_hash, val)
    // 4. Return uint256_from_bytes(val, 32)
}
```

#### state_create
- Remove `evict_fd = -1`, `evict_threshold = 50000`
- Add `mem_art_init(&s->dirty_storage)`
- Create default flat storage on /dev/shm:
  ```c
  char tmp[64];
  snprintf(tmp, sizeof(tmp), "/dev/shm/artex_stor_%d.dat", (int)getpid());
  s->stor_flat = storage_flat_create(tmp);
  ```

#### state_destroy
- Remove `destroy_resource_storage` calls, `free(s->resource_list)`, `close(s->evict_fd)`
- Add cleanup: `mem_art_destroy(&s->dirty_storage)`, `dirty_free(&s->dirty_stor_keys)`
- Add flat storage cleanup: `storage_flat_sync`, `storage_flat_destroy`, `unlink` if temp

#### state_set_storage_raw
- Remove `ensure_storage`, hart operations
- Use `ensure_resource` + `dirty_stor_put` + `acct_set_flag(STORAGE_DIRTY)`

#### state_set_storage (SSTORE path)
- Replace hart write section with: `ensure_resource(s, a)` + `dirty_stor_put`

#### state_has_storage
- Just check `storage_root != EMPTY` (remove `r->storage && hart_size > 0`)

#### JE_STORAGE revert
- Use `dirty_stor_put` instead of hart insert/delete

#### JE_CREATE journal entry
- ADD fields: `stor_offset`, `stor_count`, `stor_cap` to create struct in journal
- SAVE them when creating journal entry in state_create_account
- RESTORE them on revert, plus restore dirty buffer from flat file:
  ```c
  if (r->stor_count > 0 && s->stor_flat) {
      const uint8_t *entries = storage_flat_region(s->stor_flat, r->stor_offset);
      if (entries) {
          for (uint32_t j = 0; j < r->stor_count; j++)
              dirty_stor_put(s, a->addr.bytes,
                             entries + j * STOR_ENTRY_SIZE,
                             entries + j * STOR_ENTRY_SIZE + 32);
      }
  }
  ```

#### state_create_account (CREATE2)
- Save `had_dirty_storage = acct_has_flag(a, ACCT_STORAGE_DIRTY)` BEFORE setting flags
- Set flags with `(a->flags & ACCT_MPT_DIRTY)` preserved
- DON'T free flat file region (journal revert needs it)
- Zero dirty buffer entries ONLY if `had_dirty_storage` is true (O(N) scan but only for accounts with prior dirty storage — rare for fresh CREATEs)
- KEEP `ACCT_STORAGE_CLEARED` set (commit_dirty_storage needs it to free old flat file region)

#### state_commit_tx (self-destruct)
- Remove `destroy_resource_storage`
- Zero dirty buffer entries (O(N) scan, acceptable — self-destruct is rare)
- KEEP `ACCT_STORAGE_CLEARED` set
- DON'T free flat file region inline

#### state_clear_prestate_dirty
- Call `commit_dirty_storage(s)`
- Compute storage roots for prestate accounts via temp harts from flat file

#### state_get_stats
- Remove all storage arena/eviction/reload stats

#### ADD: commit_dirty_storage (before compute_root_ex)
- Sort dirty_stor_keys by (addr, slot_hash)
- For each account:
  - If ACCT_STORAGE_CLEARED: free old flat file region, reset offset/count/cap
  - Collect deduplicated dirty entries from mem_art
  - storage_flat_merge(old_offset, old_count, old_cap, dirty_entries, ...)
  - Update r->stor_offset/count/cap
  - Clear ACCT_STORAGE_CLEARED
- Clear dirty buffer

#### state_compute_root_ex
- Call `commit_dirty_storage(s)` BEFORE the dirty account loop (when compute_hash=true)
- Inline storage root computation in the first dirty loop:
  - If STORAGE_DIRTY and compute_hash:
    - If ACCT_STORAGE_CLEARED: free flat file region, reset to 0
    - If stor_count == 0: EMPTY_STORAGE_ROOT
    - Else: rebuild temp hart from flat file, hart_root_hash, destroy hart
  - If STORAGE_DIRTY and !compute_hash: set storage_roots_stale
- Clear flags: `ACCT_MPT_DIRTY | ACCT_BLOCK_DIRTY | ACCT_STORAGE_DIRTY | ACCT_STORAGE_CLEARED`
- Handle stale roots (no-validate → validate transition): rebuild ALL storage roots from flat file

#### state_invalidate_all
- Remove per-resource hart_invalidate_all loop (just keep acct_index invalidation)

#### state_compact
- Remove `destroy_resource_storage` in dead resource cleanup

#### ADD: state_set_storage_path
```c
void state_set_storage_path(state_t *s, const char *dir) {
    if (s->stor_flat) { storage_flat_destroy(s->stor_flat); s->stor_flat = NULL; }
    snprintf(s->stor_flat_path, sizeof(s->stor_flat_path), "%s/storage_flat.dat", dir);
    s->stor_flat = storage_flat_open(s->stor_flat_path);
    if (!s->stor_flat) s->stor_flat = storage_flat_create(s->stor_flat_path);
}
```

#### state_save
- Read storage from flat file: `storage_flat_region(stor_flat, r->stor_offset)`
- Write inline: `stor_count` + entries

#### state_load
- Reset flat file at start (destroy + recreate)
- For each account with stor_count > 0:
  - `storage_flat_alloc` → region
  - Read entries from file using pread loop (fread to mmap fails for large reads)
  - Set r->stor_offset/count/cap
- Sync flat file after load

### 5. state/src/evm_state.c
- Replace eviction wrappers with no-ops
- Wire `evm_state_set_storage_path` → `state_set_storage_path`

### 6. evm/include/evm_state.h
- Remove eviction API declarations
- Update `evm_state_set_storage_path` signature (no capacity_hint)

### 7. tools/chain_replay.c
- Remove `--storage-budget`, `--storage-cache`, `--no-evict` CLI options
- Replace eviction setup with: `evm_state_set_storage_path(sync_get_state(sync), data_dir)`
- Simplify stats printing (remove storage arena/reload stats)

### 8. sync/src/sync.c
- Remove eviction trigger block
- Remove `last_evict_ms`, `last_evict_block` fields
- Remove `evm_state_compact_evict_file` call in compaction

### 9. sync/include/sync.h
- Remove `evict_interval`, `no_evict` from sync_config_t

## Key Bugs Found and Fixed

### 1. ACCT_STORAGE_CLEARED flag lifecycle
- KEEP the flag set after CREATE2/self-destruct — commit_dirty_storage needs it to free old flat file region
- Clear it in commit_dirty_storage after processing
- Clear it in the inline root computation loop

### 2. Dirty buffer zeroing on CREATE2
- Must zero pre-CREATE2 dirty entries to prevent stale values in storage_read
- Guard with `had_dirty_storage` (saved BEFORE setting new flags) to avoid O(N) scan on fresh accounts
- Self-destruct always scans (rare, acceptable)

### 3. Journal revert for JE_CREATE
- Save r->stor_offset/count/cap in journal entry
- On revert: restore them + re-read flat file entries into dirty buffer
- DON'T free flat file region in state_create_account (revert needs it)

### 4. fread to mmap fails for large reads
- Use pread loop instead of fread for large storage entry reads in state_load

### 5. storage_flat offset 0 sentinel
- data_size starts at ENTRY_SIZE (64), not 0 — offset 0 means "no region"

### 6. NUM_SIZE_CLASSES must be >= 25
- Whale accounts can have 4.8M+ entries → need class 23 (2^23 = 8M entries)

### 7. Initial mmap size
- Use 64KB (not 256MB) — tests create/destroy thousands of states
- posix_fallocate on tmpfs is extremely slow — use ftruncate only
- 1.5x growth factor (not 2x)

## Verification
1. `ctest --test-dir build` — 16 unit tests
2. `build/test_runner_batch integration_tests/fixtures/state_tests` — 44,041
3. `build/test_runner_batch integration_tests/fixtures/blockchain_tests` — 47,589
4. `build/test_runner_batch ~/workspace/ethereum-tests/GeneralStateTests` — 2,642
5. `build/test_runner_batch ~/workspace/ethereum-tests/BlockchainTests` — 1,140 (8 expected failures)
6. `build/test_runner_batch integration_tests/fixtures/blockchain_tests/constantinople/eip1014_create2/test_recreate.json` — 12/12 (CREATE2 recreate regression test)

## Known Issue
- Block 14023585: gas mismatch -36414. Deterministic. Root cause: likely a failed CREATE that reverts, leaving stale dirty buffer entries. The JE_CREATE revert restores flat file region + re-reads entries, but there may be dirty buffer entries from the same window (not in flat file) that aren't properly restored by JE_STORAGE reverts. Needs dump-prestate debugging to isolate the specific tx.
