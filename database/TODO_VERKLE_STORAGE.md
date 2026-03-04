# Verkle Storage Migration: hash_store -> art_store

## Problem

verkle_flat uses three hash_store instances (value_store, commit_store, slot_store)
backed by mmap'd sharded files. At 3500+ keys per block, commit_block causes 35,000+
random page faults, spending 85% of wall time in kernel I/O.

Measured: `user: 2m58s` vs `sys: 16m47s` — computation is fast, I/O is the bottleneck.

## Solution

Replace hash_store with art_store (compact_art index + flat data file) for leaf data,
and use compact_art directly for internal commitments.

## Architecture

```
Current:                          Target:
  value_store  (hash_store)         stem_store (art_store)
  commit_store (hash_store)           key=31B stem
  slot_store   (hash_store)           record=8320B (C1+C2+commit+bitmap+256 values)
                                    internals (compact_art, RAM-only)
                                      key=32B, value=32B
                                    slot_store eliminated
```

## Stem Record Layout (8320 bytes)

```
offset  size   field
0       32     C1 (serialized banderwagon point)
32      32     C2 (serialized banderwagon point)
64      32     leaf_commitment (serialized banderwagon point)
96      32     presence_bitmap (256 bits, 1 = suffix has value)
128     8192   values[256] (32 bytes per suffix, ordered 0-255)
```

## Migration Steps

### Step 1: Stem Store

Replace value_store + commit_store leaf portion with one art_store:
```c
art_store_t *stem_store;  // key=31B stem -> 8320B record
```

**Call site changes in verkle_flat.c:**
- `hash_store_get(value_store, key)` -> read from stem record `values[suffix]`
- `hash_store_put(value_store, key, value)` -> modify stem record, pwrite back
- `vcs_get_leaf(stem)` -> art_store_get stem record, deserialize C1/C2/commit
- `vcs_put_leaf(stem, c1, c2, commit)` -> serialize into record, art_store_put

**Key benefit:** One pread per stem (gets all values + commitments) vs N separate
hash_store lookups. process_stem() becomes: read record -> modify in RAM -> write record.

### Step 2: Internal Commitments

Replace commit_store internal portion with compact_art directly:
```c
compact_art_t internals;  // key=32B [depth+1||path||zeros] -> value=32B commitment
```

**Why in-memory:** Internal nodes at all depths total < 100K entries. At 32+32=64 bytes
per entry in compact_art, that's < 7 MB of RAM. Depth 0-2 (root + first two levels) are
accessed on every block and stay in cache.

**Persistence:** On clean shutdown, serialize all internals to a file. On startup, either
reload or use a small art_store(key=32B, record=32B) if persistence is needed.

### Step 3: Eliminate Slot Store

The slot_store maps (depth, path, slot) -> occupant_stem for collision detection.
With the stem index in compact_art, use `compact_art_iterator_seek()`:

```c
// To check if slot S at depth D with path P is occupied:
uint8_t seek_key[31];
memset(seek_key, 0, 31);
memcpy(seek_key, path, D);
seek_key[D] = S;

compact_art_iterator_t *it = compact_art_iterator_create(&stem_store->index);
compact_art_iterator_seek(it, seek_key);
if (compact_art_iterator_next(it)) {
    const uint8_t *found = compact_art_iterator_key(it);
    if (memcmp(found, seek_key, D + 1) == 0) {
        // Slot is occupied, found stem = found[0..30]
    }
}
```

### Step 4: Struct Changes

```c
typedef struct {
    art_store_t   *stem_store;      // key=31B -> 8320B record
    compact_art_t  internals;       // key=32B -> value=32B (RAM)

    // Unchanged:
    vf_change_t  *changes;
    uint32_t      change_count, change_cap;
    vf_undo_t    *undos;
    uint32_t      undo_count, undo_cap;
    vf_commit_undo_t *commit_undos;
    uint32_t          cu_count, cu_cap;
    vf_block_t   *blocks;
    uint32_t      block_count, block_cap;
    bool          block_active;
} verkle_flat_t;
```

### Step 5: Undo System

- **Value undo:** unchanged — key[32] + old_value[32] + had_value
- **Commitment undo:** store old C1/C2/commit (96B) per stem
- **Internal undo:** store old commitment (32B) per internal path
- **Slot undo:** not needed (slot_store eliminated)

### Step 6: Optimize commit_block Flow

With stem records, the commit_block flow becomes:

```
For each stem group (sorted):
  1. art_store_get(stem_store, stem) -> record  [1 pread]
  2. For each modified suffix:
     - old_val = record.values[suffix]
     - Compute delta, update C1/C2 incrementally
     - record.values[suffix] = new_val
     - record.presence[suffix] = 1
  3. Update record.leaf_commitment
  4. art_store_put(stem_store, stem, record)     [1 pwrite]
  5. Queue propagation delta

Propagate internals (all in RAM via compact_art):
  - Zero disk I/O for commitment tree traversal
```

## Expected Improvements

| Metric | hash_store | art_store |
|--------|-----------|-----------|
| Ops per stem | 2 + 2×N (reads+writes) | 2 (1 read + 1 write) |
| I/O type | Random mmap page faults | Targeted pread/pwrite |
| Internal commits | Disk (random mmap) | RAM (compact_art) |
| RAM overhead | ~0 (mmap only) | ~120 MB (2M stems in ART) |
| Page faults/block | 35,000+ | ~3,500 (1 per stem) |
| Kernel time | 85% (16m47s sys) | <10% (estimated) |

## Testing

1. All existing verkle tests must pass with new backend
2. Cross-validation against go-verkle must still match
3. Scenario 20 (3500+ keys) should complete in seconds, not minutes
4. Stress test: verkle_flat with art_store at 10K+ blocks
