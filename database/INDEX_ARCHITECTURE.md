# Index Architecture: compact_art + Value Files

## Overview

Separate the index from the data. The ART tree lives entirely in RAM as a
lookup structure. Actual values live on disk in two files: one for state
(fixed-size slots), one for contract code (variable-size, append-only).

```
compact_art (RAM)                    Disk
┌──────────────────────┐      ┌─────────────────────┐
│ key: 32B keccak256   │      │ state.dat            │
│ val: 4B packed ref   │─────→│ fixed 64B slots      │
│                      │      │ [slot0][slot1][...]   │
│ 500M keys × 64 B/key │      ├─────────────────────┤
│ = ~31 GB RAM         │      │ code.dat             │
│                      │─────→│ append-only log      │
└──────────────────────┘      │ [code0][code1][...]  │
                              └─────────────────────┘
                              code_index (RAM)
                              ┌─────────────────────┐
                              │ entry[i] = {off,len} │
                              │ 50M × 12B = 600 MB  │
                              └─────────────────────┘
```

## compact_art: In-Memory Index

Space-efficient ART storing fixed-size keys and values. Optimizations over
mem_art (~97 B/key → ~64 B/key at 300M keys):

1. **Arena allocator** — bump allocator with 64 MB mmap'd slabs, no malloc overhead
2. **Tagged pointers** — LSB marks leaf, no type field in leaf struct
3. **Fixed-size leaf** — zero-header, just `key[32] + value[4]` = 36 bytes
4. **Compact inner nodes** — `uint8_t type/num_children/partial_len`
5. **Node32** — fills gap between Node16 and Node48, 2x SSE lookup

### Benchmark Results

| Keys | B/key | RSS | Notes |
|------|-------|-----|-------|
| 10M | 73 B | 700 MB | Early — small node overhead dominates |
| 100M | 73 B | 6.8 GB | Stabilizing |
| 200M | 63 B | 11.7 GB | Node32 absorbing depth-3 nodes |
| 300M | 64 B | 17.9 GB | Stable plateau |
| 500M | ~63 B | ~31 GB | Extrapolated — fits 32 GB RAM |

### API

```c
compact_art_t tree;
compact_art_init(&tree, 32, 4);             // 32B keys, 4B values
compact_art_insert(&tree, key, &ref);       // key[32], ref = packed 4B
const void *v = compact_art_get(&tree, key); // returns ptr to 4B value
compact_art_delete(&tree, key);
compact_art_destroy(&tree);
```

The tree does not interpret the 4-byte value. It is opaque to the index.

## 4-Byte Packed Reference

Every leaf stores a 4-byte value that encodes a reference to on-disk data.
Bit 31 selects which store:

```
┌───────────────────────────────────────────┐
│ bit 31 │ bits 30..0                       │
├────────┼──────────────────────────────────┤
│   0    │ state slot index (2^31 = 2B)     │
│   1    │ code entry index (2^31 = 2B)     │
└────────┴──────────────────────────────────┘
```

Application-level encoding/decoding:

```c
// Encode
uint32_t state_ref = slot_index;                  // bit 31 = 0
uint32_t code_ref  = code_index | 0x80000000;     // bit 31 = 1

// Decode
uint32_t ref = *(uint32_t *)compact_art_get(&tree, key);
if (ref & 0x80000000) {
    uint32_t code_idx = ref & 0x7FFFFFFF;         // → code store
} else {
    uint32_t slot_idx = ref;                       // → state store
}
```

## State Store: state.dat

Fixed-size 64-byte slots. Each slot holds one state entry (account or
storage value) with a 2-byte length prefix.

### Slot Layout (64 bytes)

```
┌──────────┬────────────────────────────────────────────┬─────────┐
│ len (2B) │ value data (up to 62 bytes)                │ padding │
└──────────┴────────────────────────────────────────────┴─────────┘
```

- `len`: uint16_t, actual value size in bytes (0 = deleted/empty)
- `value`: raw value bytes
- `padding`: zero-filled to 64 bytes

### Properties

| Property | Value |
|----------|-------|
| Slot size | 64 bytes |
| Max slots | 2^31 = 2,147,483,648 |
| Max file size | 2^31 × 64 = 128 GB |
| Max value per slot | 62 bytes |
| Addressing | `byte_offset = slot_index × 64` |

### Ethereum Value Sizes

| Type | Size | Fits in 64B slot? |
|------|------|-------------------|
| Storage slot | 32 bytes | Yes |
| Account (nonce+balance+storageRoot+codeHash) | ~100 bytes | No — see below |
| Account (RLP-encoded, typical) | 40-80 bytes | Mostly |

For accounts exceeding 62 bytes: use 2 consecutive slots (128 bytes usable
with a multi-slot flag), or increase slot size to 128 bytes. At 128B slots:
- Max file = 256 GB, max value = 126 bytes — all accounts fit
- Trade-off: 32-byte storage values waste 75% of slot space

Decision: **start with 64B slots**. Most values fit. Accounts that don't
fit can use a continuation slot (next slot, flagged by a reserved len value
like `0xFFFF`). Measure real distribution before increasing slot size.

### Read Path

```c
uint32_t ref = *(uint32_t *)compact_art_get(&tree, key);
if (!(ref & 0x80000000)) {
    uint64_t offset = (uint64_t)ref * SLOT_SIZE;
    uint8_t slot[SLOT_SIZE];
    pread(state_fd, slot, SLOT_SIZE, offset);
    uint16_t len = *(uint16_t *)slot;
    // value is at slot + 2, len bytes
}
```

One memory lookup (compact_art) + one pread. No indirection table.

### Write Path (during merge)

```c
// Allocate next free slot
uint32_t slot_idx = next_free_slot++;
uint64_t offset = (uint64_t)slot_idx * SLOT_SIZE;

// Write slot
uint8_t slot[SLOT_SIZE] = {0};
uint16_t len = value_len;
memcpy(slot, &len, 2);
memcpy(slot + 2, value, value_len);
pwrite(state_fd, slot, SLOT_SIZE, offset);

// Update index
compact_art_insert(&tree, key, &slot_idx);
```

### Deleted Entries

When a key is deleted:
1. `compact_art_delete(&tree, key)` removes from index
2. The slot in state.dat becomes garbage (unreachable)
3. Reclaimed during compaction (see Compaction section)

Alternatively, maintain a free list of reclaimed slots for reuse.

## Code Store: code.dat + code_index

Contract bytecode is write-once, variable-size (up to 24 KB per EIP-170),
and keyed by `keccak256(bytecode)` — content-addressed.

### code.dat Layout

Append-only log of raw bytecode:

```
[code_0 bytes...][code_1 bytes...][code_2 bytes...]
^                ^                ^
offset 0         offset len_0     offset len_0+len_1
```

No framing or length prefix in the file — lengths are tracked in the index.

### code_index (in-memory array)

```c
typedef struct {
    uint64_t offset;     // byte offset in code.dat
    uint32_t length;     // bytecode length
} code_entry_t;

code_entry_t *code_entries;  // realloc'd array
uint32_t code_count;         // number of entries
```

Memory: 50M contracts × 12 bytes = 600 MB. In practice, Ethereum mainnet
has ~60M contracts as of 2025, many sharing identical bytecodes (proxies).

### Read Path

```c
uint32_t ref = *(uint32_t *)compact_art_get(&tree, key);
if (ref & 0x80000000) {
    uint32_t idx = ref & 0x7FFFFFFF;
    code_entry_t *e = &code_entries[idx];
    uint8_t *buf = malloc(e->length);
    pread(code_fd, buf, e->length, e->offset);
}
```

One memory lookup (compact_art) + one array lookup (L1 cache) + one pread.

### Write Path

```c
// Append bytecode to code.dat
uint64_t offset = lseek(code_fd, 0, SEEK_END);
write(code_fd, bytecode, bytecode_len);

// Add to code_index
uint32_t idx = code_count++;
code_entries[idx] = (code_entry_t){ offset, bytecode_len };

// Insert into compact_art with code flag
uint32_t ref = idx | 0x80000000;
compact_art_insert(&tree, code_hash, &ref);
```

### Deduplication

Contract code is content-addressed (`keccak256(bytecode)`). Before
appending, check `compact_art_get(&tree, code_hash)`:
- If found: code already exists, skip write
- If not found: append and insert

This is free — no extra data structure needed.

## Lifecycle

### Startup

```
1. Open state.dat and code.dat
2. Load code_index from code_index.dat (or rebuild from code.dat)
3. Rebuild compact_art from state index file (or scan state.dat)
4. Re-execute K blocks from blockchain to rebuild write buffer
5. Ready to serve reads
```

### During Block Execution

```
Read:
  1. Check write buffer (mem_art / hash map)
  2. If not found: compact_art_get(key) → ref → pread from state/code

Write:
  1. Insert into write buffer (memory-only, never touches disk)
```

### Checkpoint (every K blocks)

```
1. Freeze write buffer
2. Merge write buffer into state.dat:
   - For each insert/update in buffer:
     a. Allocate new slot in state.dat (or reuse freed slot)
     b. pwrite value to slot
     c. compact_art_insert(key, new_slot_index)
   - For each delete in buffer:
     a. compact_art_delete(key)
     b. Mark old slot as free
3. Persist code.dat and code_index.dat (if new contracts deployed)
4. Persist compact_art state index (for fast restart — see below)
5. fsync all files
6. Clear write buffer
```

### Crash Recovery

Two options:

**Option A: Scan state.dat to rebuild index** (simple, slow startup)
- Sequential scan: read each slot, check if live (need a bitmap or scan log)
- Problem: can't tell which slots are live without the index
- Need a slot allocation bitmap persisted alongside state.dat

**Option B: Persist compact_art index** (fast startup, recommended)
- Periodically dump the compact_art contents to an index file
- Format: sorted list of `(key[32], ref[4])` pairs — same as SSTable
- On startup: sequential scan of index file → rebuild compact_art
- 500M entries × 36B = 18 GB file, sequential read at 2 GB/s ≈ 9 seconds

**Option C: Checkpoint includes index snapshot** (best)
- At checkpoint: walk compact_art iterator, write `(key, ref)` pairs
- Atomic: write to temp file, fsync, rename
- Recovery: read index file → populate compact_art, re-execute K blocks

Option C is recommended. The index file doubles as crash recovery AND
startup acceleration.

### Index File Format

```
Header (4096 bytes, page-aligned):
  magic:        "ARTIDX01" (8 bytes)
  version:      uint32
  num_entries:  uint64
  key_size:     uint32 (32)
  value_size:   uint32 (4)
  checksum:     uint32 (CRC32 of entries)
  padding to 4096

Entries (sorted by key, starting at offset 4096):
  [32B key][4B ref] [32B key][4B ref] [32B key][4B ref] ...

No per-entry framing needed — fixed 36-byte stride.
```

## Compaction

Over time, state.dat accumulates dead slots (deleted or updated keys whose
old slots are unreachable). Compaction reclaims this space.

### Strategy: Rebuild

```
1. Create state_new.dat
2. Walk compact_art in sorted order (iterator)
3. For each live key:
   a. Read value from old state.dat
   b. Write to next slot in state_new.dat
   c. Update compact_art with new slot index
4. fsync state_new.dat
5. Write new index file
6. Atomic rename state_new.dat → state.dat
7. Delete old file
```

This is a full rewrite — acceptable if done infrequently (e.g., weekly or
when dead space exceeds a threshold).

### Incremental Alternative

Maintain a free list of dead slots. When writing new entries during merge,
reuse free slots first, only extend the file when the free list is empty.
No separate compaction pass needed.

## Memory Budget (500M keys)

| Component | Size |
|-----------|------|
| compact_art index | ~31 GB |
| code_index array | ~0.6 GB |
| Write buffer (mem_art, K=100 blocks) | ~0.5 GB |
| OS / process overhead | ~0.3 GB |
| **Total RAM** | **~32.4 GB** |

Tight for 32 GB. Options to reduce:
- Smaller write buffer (K=50 blocks): saves ~0.25 GB
- Compress partial[] in cold inner nodes: saves ~1-2 GB
- 4-byte key prefix in leaf instead of full 32B key (with hash collision
  handling): saves ~14 GB but adds complexity

For 64 GB RAM: plenty of headroom, no optimizations needed.

## Disk Budget (500M keys)

| Component | Size |
|-----------|------|
| state.dat (500M × 64B slots) | 32 GB |
| code.dat (60M contracts × ~3 KB avg) | ~180 GB |
| index file (500M × 36B) | 18 GB |
| code_index.dat (60M × 12B) | 720 MB |
| **Total disk** | **~231 GB** |

NVMe SSD with 500 GB is sufficient.

## Read/Write Performance

### Reads

| Path | Latency | Notes |
|------|---------|-------|
| Write buffer hit | ~100 ns | Memory only |
| compact_art + state pread | ~5-10 us | 1 memory lookup + 1 SSD read |
| compact_art + code pread | ~10-50 us | Depends on code size |
| Page cache hit | ~200 ns | Hot state stays cached by kernel |

### Writes

| Path | Latency | Notes |
|------|---------|-------|
| Buffer insert | ~100-200 ns | Memory only, never blocks |
| Merge (amortized) | ~0 ns/write | Bulk sequential I/O every K blocks |

### Merge Throughput

Merge K=100 blocks ≈ 500K state changes:
- Write 500K × 64B slots = 32 MB sequential write → <100 ms on NVMe
- Update 500K compact_art entries → <500 ms in memory
- Total merge: <1 second for 100 blocks (~20 minutes of chain)

## Files

| File | Type | Description |
|------|------|-------------|
| `compact_art.h` | Header | Index API and types |
| `compact_art.c` | Source | Index implementation |
| `state_store.h` | Header | State file operations (TODO) |
| `state_store.c` | Source | Fixed-slot state file (TODO) |
| `code_store.h` | Header | Code file operations (TODO) |
| `code_store.c` | Source | Append-only code log (TODO) |
| `index_file.h` | Header | Index persistence (TODO) |
| `index_file.c` | Source | Write/load compact_art snapshot (TODO) |
