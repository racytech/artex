# Storage Hart Eviction — Design Document

## Problem

At block 14M, storage harts consume 45GB+ RAM for ~6M storage accounts.
Most accounts are cold — accessed once and never touched again. The hot
working set (accounts accessed in the last few thousand blocks) is much
smaller, likely 100K-300K accounts. Without memory control, RSS grows
unbounded until the machine swaps.

## Current Implementation (state-dev-4)

- **Eviction**: every 10K blocks, scan all accounts. If
  `current_block - last_access_block > 50K`, write hart entries to a flat
  file and free the hart.
- **Reload**: lazy on first access — `pread` from eviction file, rebuild hart.
- **Tracking**: `last_access_block` updated on writes (`ensure_account_h`)
  and reload (`state_reload_storage`), but NOT on reads (`storage_read`).

### Problems

1. No memory budget — evicts by age only, storage can grow to 40GB+.
2. Read-heavy contracts (DEXes, oracles, price feeds) never update
   `last_access_block` via SLOAD, so they look cold and get evicted
   even when actively read every block.
3. Eviction file grows unbounded — append-only, stale entries from
   re-evicted accounts accumulate.
4. Eviction scan walks all 150M+ accounts even though only ~5M have
   storage resources.

## Proposed Design

### 1. Memory Budget

Add a configurable storage memory budget:

```
--storage-budget 16384   (MB, default: 0 = unlimited)
```

Stored in `state_t`:
```c
size_t evict_budget;  /* target max stor_arena_bytes, 0 = unlimited */
```

At eviction time:
- If `stor_arena_bytes <= budget`: do nothing
- If `stor_arena_bytes > budget`: evict coldest accounts until under budget

No fixed block-age threshold. The budget determines how much stays in RAM.
With a 16GB budget, ~400K-800K hot accounts stay (depending on average
storage size), and everything else lives on disk.

### 2. Accurate Access Tracking

Update `last_access_block` on storage reads, not just writes:

```c
// In storage_read(), before hart_get:
if (a->last_access_block != s->current_block)
    a->last_access_block = s->current_block;
```

The `!= current_block` check avoids redundant writes — within a single
block, multiple SLOADs to the same account only write once. Cost: one
branch per SLOAD (predicted taken), one store on first access per block.

This ensures read-heavy contracts stay hot. A Uniswap pair contract
that gets SLOAD'd every block will never be evicted.

### 3. Eviction Algorithm

When `stor_arena_bytes > budget`:

**Option A — Threshold scan (simple, current approach extended)**
1. Start with threshold = 50K blocks
2. Scan accounts, evict those colder than threshold
3. If still over budget, halve threshold and scan again
4. If under budget, done

Pro: simple. Con: multiple passes if budget is tight.

**Option B — Sorted eviction (one pass, precise)**
1. Collect all accounts with in-memory storage into a temp array
   `(account_idx, last_access_block, arena_size)`
2. Sort by `last_access_block` ascending (coldest first)
3. Evict from coldest until `stor_arena_bytes <= budget`

Pro: precise, single pass after sort. Con: allocates temp array
(~5M entries × 16 bytes = 80MB). Acceptable at eviction time.

**Option C — Approximate top-K (no allocation)**
1. Scan accounts, compute age percentile from `last_access_block`
2. Two-pass: first pass finds the age cutoff that hits the budget,
   second pass evicts accounts older than cutoff

Pro: no allocation. Con: two passes, slightly complex.

**Recommendation: Option A** for simplicity. The threshold adapts
automatically — if budget is tight, it evicts more aggressively. If
budget is generous, most accounts stay hot.

### 4. Eviction File Compaction

The eviction file is append-only. When an account is evicted, reloaded,
modified, and re-evicted, the old entries become stale. Over time, the
file grows larger than necessary.

**Compaction trigger**: at snapshot save time (every 1M blocks).

**Algorithm**:
1. Create a new temporary file
2. Walk all resources with `evict_count > 0`
3. Read entries from old file, write to new file
4. Update `evict_offset` in each resource
5. Replace old file with new file

**Cost**: proportional to evicted data size. At 16GB budget with maybe
4M evicted accounts × 100 slots × 64 bytes = ~25GB of evicted data.
Sequential read + write at NVMe speed: ~10-15 seconds. Acceptable at
snapshot boundaries.

### 5. Integration Points

**`storage_read` (state.c:~480)**
```c
// Add access tracking
if (a->last_access_block != s->current_block)
    a->last_access_block = s->current_block;
```

**`state_evict_cold_storage` (state.c:~1660)**
Replace fixed threshold with budget-based logic:
```c
if (s->evict_budget > 0) {
    state_stats_t ss = state_get_stats(s);
    if (ss.stor_arena_bytes <= s->evict_budget) return 0;
    // evict until under budget
}
```

**`sync_execute_block` (sync.c)**
- Pass budget from config: `evm_state_set_evict_budget(state, budget_bytes)`
- Eviction trigger unchanged (every 10K blocks at checkpoint boundaries)

**`chain_replay.c`**
- Parse `--storage-budget N` argument
- Pass to state via `evm_state_set_evict_budget`

**Snapshot save time**
- After `state_save`, compact the eviction file

### 6. CLI Interface

```
--storage-budget N    Target storage memory in MB (default: 0 = unlimited)
                      Recommended: 8192-16384 (8-16GB)
```

### 7. Expected Behavior

With `--storage-budget 16384` at block 14M:

| Metric | Before (no budget) | After (16GB budget) |
|--------|-------------------|-------------------|
| stor_arena | 45GB+ growing | ~16GB stable |
| RSS | 75-80GB | ~45-50GB |
| In-memory harts | 3-6M | 400K-800K |
| Evicted harts | varies | 5-6M |
| Eviction file | grows unbounded | compacted at snapshots |
| Eviction time | 2-3s every 10K blks | <1s (fewer to evict) |

### 8. Implementation Order

1. Add `last_access_block` update in `storage_read`
2. Add `evict_budget` field and `--storage-budget` CLI flag
3. Change eviction to budget-based (Option A: adaptive threshold)
4. Add eviction file compaction at snapshot time
5. Test with chain replay from genesis
