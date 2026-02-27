# Data Layer Design: compact_art + Value Files

## Overview

compact_art is a pure in-memory index: `key[32] → ref[4]`. It tells you
*where* data lives, not *what* the data is. The storage layer holds actual
values in two files on disk, with a write buffer absorbing all mutations
in memory.

```
                    Block Execution
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
    Write Buffer     compact_art     state.dat
    (hash map)       (RAM index)     (pread)
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │ key→val  │    │ key→ref  │    │ 64B slots│
    │ tombstone│    │ ~2-4 GB  │    │ pread()  │
    │ memory   │    │ 60 B/key │    │ 32-64 GB │
    └──────────┘    └──────────┘    └──────────┘
          │                               ▲
          └───── merge every K blocks ────┘
                 (sorted, sequential)
```

## Components

### compact_art (RAM)

In-memory ART mapping 32-byte keccak256 keys to 4-byte packed references.
Bit 31 of the reference selects the store:

```
bit 31 = 0  →  state slot index (state.dat)
bit 31 = 1  →  code entry index (code.dat)
```

At 300M keys with COMPACT_MAX_PREFIX=8: ~60 B/key, ~17 GB RAM.

### state.dat (disk)

Fixed-size 64-byte slots. Each slot holds one state entry with a 2-byte
length prefix:

```
┌──────────┬────────────────────────────────────────────┬─────────┐
│ len (2B) │ value data (up to 62 bytes)                │ padding │
└──────────┴────────────────────────────────────────────┴─────────┘
```

Addressing: `byte_offset = slot_index × 64`. One pread per lookup.

### code.dat (disk)

Append-only log of raw contract bytecode. Content-addressed by
`keccak256(bytecode)` — deduplication is free via compact_art lookup
before append.

### Write Buffer (RAM)

In-memory hash map: `key[32] → (value, len, tombstone)`. All writes
during block execution go here. Never touches disk until merge.

## Read Path

```
1. Check write buffer       → hit = 100ns, done
2. compact_art_get(key)     → ref (memory lookup)
3. pread(state_fd, slot, 64, ref * 64)
   - page cache hit:  ~200ns
   - page cache miss: ~5-10μs (NVMe random read)
```

One memory lookup + one pread. No indirection table, no deserialization.

## Write Path

```
1. buffer_put(key, value)   → O(1), always in memory, ~100ns
2. Deletion: buffer_put(key, TOMBSTONE)
```

Writes never touch disk during block execution. This is why write
throughput is constant regardless of database size.

## Merge (every K blocks)

Every K blocks (~100 = ~20 minutes of chain), the write buffer is
flushed to disk:

```
1. Freeze write buffer
2. For each entry in buffer (sorted by key):
   - Insert/update: allocate slot in state.dat, pwrite value,
     compact_art_insert(key, new_slot_ref)
   - Delete: compact_art_delete(key), mark old slot as free
3. fsync state.dat
4. Persist index snapshot (for fast restart)
5. Clear write buffer
```

Sorting the buffer keys before merge produces sequential access patterns
when walking compact_art, which means sequential I/O on disk. NVMe
handles sequential writes at 2-3 GB/s.

Merge cost for K=100 blocks (~500K state changes):
- Write 500K × 64B slots = 32 MB sequential → <100ms on NVMe
- Update 500K compact_art entries → <500ms in memory
- Total: <1 second for ~20 minutes of chain

## Crash Recovery

No WAL. The blockchain is the recovery log:

```
1. Open last checkpoint (index snapshot + state.dat)
2. Rebuild compact_art from index file
3. Ask consensus layer for blocks since checkpoint
4. Re-execute K blocks (deterministic, seconds)
5. Done
```

Recovery cost at K=100: re-execute ~20 minutes of chain. In practice
this takes seconds because state is in page cache from the previous run.

## Why pread, Not mmap

mmap gives zero-copy reads (pointer dereference instead of syscall), but
has a fatal weakness at scale: when state.dat exceeds RAM, random access
triggers page faults (~0.5ms each). The kernel controls eviction via LRU
and has no knowledge of which pages are hot for the workload. Throughput
collapses.

This was the lesson from the persistent ART on db-dev-2/3 (see
WRITE_SCALABILITY.md). When the mmap'd file exceeded 32 GB RAM, write
throughput dropped from 80 Kkeys/s to 15-20 Kkeys/s due to page cache
thrashing.

pread avoids this by:
- Giving the kernel a clear "read this one slot" signal (better readahead decisions)
- Not mapping the entire file into the address space
- Allowing an optional application-level cache with explicit eviction control

## Performance at Scale

### Why This Design Is Stable

| Concern | Why it's handled |
|---------|-----------------|
| Write throughput | Constant — always memory (buffer absorbs everything) |
| Read throughput | Hot data stays in page cache (Ethereum has strong temporal locality) |
| Merge cost | Amortized over K blocks, sequential I/O |
| Crash safety | Blockchain-as-WAL — no fsync per commit |
| Cold reads | Bounded by NVMe IOPS (~1M/s), but rare during normal execution |

### Expected Latencies

| Path | Latency | Notes |
|------|---------|-------|
| Write buffer insert | ~100ns | Memory only |
| Read: buffer hit | ~100ns | Memory only |
| Read: page cache hit | ~200ns | compact_art lookup + pread (cached) |
| Read: page cache miss | ~5-10μs | NVMe random read |
| Merge (amortized) | ~0ns/write | Bulk sequential I/O every K blocks |

### Disk Budget (500M keys)

| File | Size |
|------|------|
| state.dat (500M × 64B) | 32 GB |
| code.dat (~60M contracts × ~3 KB avg) | ~180 GB |
| index snapshot (500M × 36B) | 18 GB |
| Total | ~230 GB |

NVMe SSD with 500 GB is sufficient.

## Account Encoding (TODO)

Ethereum accounts use a two-slot layout in state.dat:

```
Slot 1 (always):  [1B flags] [8B nonce] [balance bytes...]
Slot 2 (if code): [32B code_hash] [30B reserved]
```

Flags byte:
- bit 0: `has_code` — second slot exists
- bits 1-4: `balance_len` (0-32, compact big-endian, no leading zeros)

If `has_code` is set, the last 4 bytes of slot 1 data hold the slot index
of slot 2:

```
Slot 1 with code: [1B flags][8B nonce][N bal bytes][4B slot2_id]
  max: 1 + 8 + 32 + 4 = 45B — fits in 62B
```

EOAs (externally owned accounts, ~70% of all accounts) use a single slot.
Contract accounts use two slots. The second slot is allocated independently
via `state_store_alloc()` — no adjacent-slot pairing required, which keeps
the allocator and free list simple.

Read path for contracts: decode slot 1 flags → extract slot2_id →
`pread` slot 2. Two syscalls instead of one, but contract reads are
followed by bytecode execution (thousands of opcodes), so the extra pread
is negligible. Hot contracts stay in page cache.

## Optional: Application-Level Read Cache

If page cache hit rate drops under pressure (other processes competing
for memory, archive node queries, initial sync), add a fixed-size LRU
cache for hot slots:

```
Read path with cache:
  1. Check write buffer
  2. Check slot cache (hash map, e.g., 2M entries = 128 MB)
  3. compact_art_get → ref → pread → insert into cache
```

This decouples performance from kernel page cache behavior. The
application controls eviction and can prioritize:
- Recently executed accounts
- High-frequency contracts (DEXs, bridges)
- Storage slots read repeatedly within a block

Only add this if measurements show page cache misses becoming a
bottleneck. During normal block execution, the kernel page cache is
sufficient.

## Implementation Order

```
Priority   Component          Effort    Status
──────────────────────────────────────────────
1          compact_art        Done      60 B/key at 300M
2          state_store        Medium    state.dat + slot allocator
3          write_buffer       Medium    hash map + tombstones
4          merge              Medium    sorted walk + sequential I/O
5          index_snapshot     Low       persist/load compact_art
6          code_store         Low       append-only + code_index
7          read_cache         Low       only if needed
```
