# storage_hart — single-pool slab allocator

## Goal
Eliminate the two dominant costs of today's arena-growing storage_hart pool:
1. **Relocation memcpy** on grow — a 1 GB arena that grows to 1.5 GB triggers a 1 GB copy.
2. **Fragmentation** — each relocated arena leaves a hole; over a 1M-block run this accumulates to ~60–90 GB dead space ([project_hart_freelist_waste](../.claude/projects/-home-racytech-workspace-art/memory/project_hart_freelist_waste.md)).

## What today's pool actually looks like
Each account's `storage_hart_t` owns a contiguous arena `{arena_offset, arena_cap, arena_used}` inside the single shared `storage_hart_pool_t`. Within one arena, nodes are dense. The problem is growth.

```
arena_ensure(sh, needed):
  if arena_used + needed <= arena_cap:          return (fits)
  if arena is at pool tail:                     bump pool->data_size — fast path
  else:
      new_off = pool_alloc(new_cap)
      memcpy(pool+new_off, pool+old_off, used)  ← the cost
      pool_free(old_off, old_cap)               ← leaves a hole
      sh->arena_offset = new_off
```

Growth is 1.5× until 1 GB, then +256 MB linear. Whale contracts relocate many times over a long replay. Cumulative bytes copied is measured in TBs.

```
pool over time:
  [A_v1 FREED][B][C][A_v2 FREED][D][A_v3 live][E][B' live][...]
```

## Proposed shape: single pool + per-account slab chains
One MAP_ANONYMOUS | MAP_PRIVATE region that hands out fixed-size slabs. Each hart owns a linked list of slabs (append-only, never relocated). Growth appends a new slab; old slabs keep their content and their refs forever (until account destruct).

### Slab sizes (5 classes)
```
HART_SLAB_MIN = 512 B
HART_SLAB_MAX = 8192 B = 8 KB
Classes:        512, 1024, 2048, 4096, 8192  (geometric)
```
Pool hands out only slabs. Individual ART node allocation happens *inside* a slab (see below).

### Per-account slab chain
```c
struct hart_slab_t {
    hart_pool_ref_t head_ref;    // active slab — bump target
    uint32_t        head_used;   // bytes consumed in head slab
    uint32_t        head_cap;    // head slab capacity
    hart_pool_ref_t chain_ref;   // previous closed slab (linked list)
};
```

- **First alloc** reserves a 512 B slab from the pool.
- **Fits in head** → bump `head_used`.
- **Doesn't fit** → close head (append to chain via `chain_ref`), reserve a new larger slab (geometric: 512 → 1 K → 2 K → 4 K → 8 K, capped at 8 K; whales keep appending 8 K slabs).
- **Account destruct** → walk the chain, push every slab onto the pool's size-class freelist.

Each slab carries a 16 B header `{chain_ref, cap}` at offset 0. Payload starts at +16.

### Pool-level freelist (cross-account reuse)
One LIFO stack per slab class (5 stacks). On slab release, push onto its class's list. On new slab request, pop from the class's list first; bump the pool cursor only on miss. Strict class match (no best-fit split) for v1.

### Intra-hart node freelist (within a slab chain)
Each `storage_hart_t` keeps a per-size-class freelist for node-level reuse. Step is 128 B, 16 classes (covers all ART node sizes up to 2 KB).

Decision: **128 B step, per-size-class** (not per-type). A freed node4 (~64 B) and a leaf alloc (~80 B) both land in class 0, which fixes the cross-type waste. Cost: 16 × 4 B heads = 64 B per hart (vs 20 B for the old per-type scheme); ~2 GB extra state at 50M harts.

Free-list next pointers are embedded in the first 4 bytes of the freed node's payload.

## Why only one pool
Earlier drafts proposed a separate large-tier pool (Pool L) for oversized allocations. Dropped because:
- Every allocation in storage_hart fits in a slab (max node is ~2 KB, max slab is 8 KB).
- Whale accounts chain more 8 KB slabs; they don't need a second pool.
- Code blobs live in `code_store`, not hart_pool. SSZ buffers are short-lived, stack/heap.
- A second pool adds a ref-tag bit and duplicate lifecycle paths with no consumer.

If a future feature genuinely needs an oversized allocator, it can be added shaped around that consumer's needs.

## Memory projections

| | today (arena grow) | single-pool slab |
|---|---|---|
| Live data | ~40 GB | ~40 GB |
| Dead holes / freelist | 60–90 GB | 5–10 GB |
| Relocation buffer spike | up to 2× live during grow | **none** |
| VmData high-water | 128 GB | ~50–60 GB |
| Cumulative memcpy during replay | many TB | **0** |

## Major-fault budget
Current 33M major-fault run breaks down roughly:
- ~15M: arena relocation touches (read old + write new) during grow
- ~15M: first-time cold-account SLOAD
- ~3M: heap / metadata / other

Slab design eliminates the first bucket entirely. Expected after change: **~15–20M major faults**, ~2× throughput improvement on heavy-swap workloads. The cold-SLOAD bucket is fundamentally a RAM-sizing issue and doesn't go away until physical RAM grows or the hot set shrinks.

## Interface
See [hart_pool.h](../database/include/hart_pool.h). Key signatures:

```c
hart_pool_t    *hart_pool_create(void);              // MAP_ANONYMOUS, no fd
void            hart_pool_destroy(hart_pool_t *);

hart_pool_ref_t hart_pool_alloc(hart_pool_t *, hart_slab_t *slab,
                                 uint32_t bytes, uint32_t *out_cap);
void            hart_pool_free_slabs(hart_pool_t *, hart_slab_t *);

void           *hart_pool_ptr  (const hart_pool_t *, hart_pool_ref_t);

hart_pool_stats_t hart_pool_stats(const hart_pool_t *);
```

`hart_pool_ref_t` is `uint64_t` — a byte offset into the pool. `0` is reserved as NULL.

## Preserving SLOAD warming (critical)
The last pool refactor broke +2000 gas on block 24030731 TX 223 because slot identity (the key used by the slot-hash warming cache, commit `85091b8`) drifted when nodes moved under free/reuse.

**Invariant for this design:** warming-cache keys must derive from `(resource_idx, hashed_key)`, never from a pool ref. Slabs are never moved, so a ref IS stable for its node's lifetime — but when a slot is deleted and later re-created, the freelist may hand the same ref to a different logical slot. Cache must not key on refs.

**Audit before reapplying:**
1. Inspect every `slot_hash_cache_get`/`put` call for pool-ref leakage into keys.
2. On account destruct, evict all `(old_resource_idx, *)` entries before the resource slot is reused.
3. Regression test: run `setCustodian` tx from block 24030731 in isolation (minimal alloc + `evm_t8n`), assert gas = 26116.

## Rollout plan
1. Implement `hart_pool.c` behind a compile flag. Skeleton in place at [hart_pool.c](../database/src/hart_pool.c).
2. Unit tests (`test_hart_pool.c`): alloc/free/reuse within each slab class, chain growth, freelist hit/miss, stats counters.
3. Port `storage_hart.c` ops to slab-aware allocation behind the flag. Keep single-arena path alive for bisect.
4. `state_save`/`state_load` stay unchanged — they walk leaves via `storage_hart_foreach` and rebuild via `storage_hart_put`; slab metadata is transient.
5. Run `setCustodian` regression test. **BLOCKER** for reapply.
6. Run 23M→24M chain_replay; compare VmData high-water, major-fault count, blk/s.
7. Flip default, remove old path.

## Complementary optimization (pursue in same PR)
- **Inline storage root in `account_t`** — reuse the dead `last_access_block` slot (TODO in [account.h](../state/include/account.h)). Saves one indirection per SLOAD. Zero size cost.

## Open questions
- `MADV_HUGEPAGE` on the pool to pin hot slabs on 2 MB pages? Defer — measure after slab migration first.
- Slab compaction pass at checkpoint boundaries (copy live nodes out of sparse slabs, free the source)? Only if benches show intra-slab waste is material.
- Should account-hart arena (`acct_index`) migrate to this allocator too? Its access pattern differs from storage (index lookup vs ART walk). Defer.

## References
- [project_pool_size_classes](../.claude/projects/-home-racytech-workspace-art/memory/project_pool_size_classes.md)
- [project_hart_freelist_waste](../.claude/projects/-home-racytech-workspace-art/memory/project_hart_freelist_waste.md)
- [project_osaka_gas_mismatch](../.claude/projects/-home-racytech-workspace-art/memory/project_osaka_gas_mismatch.md)
- [account.h TODO](../state/include/account.h)
