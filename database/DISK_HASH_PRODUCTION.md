# disk_hash — Production Hardening

## Current State

**What works:**
- Bucket-based open hashing with 4096-byte page-aligned pread/pwrite
- Overflow bucket chains when primary bucket fills
- Tombstone deletion with slot reuse on insert
- Batch API (batch_get, batch_put) with argsort by bucket for sequential I/O
- Persistence across close/reopen via on-disk header
- Zero RAM: ~48 bytes in-memory struct, no index, no heap allocations
- Stress-tested at 1M+ keys (6 phases, fail-fast)

**What's missing for production:**
- Crash safety (stale header, unsafe overflow write order)
- Thread safety (zero synchronization)
- I/O robustness (short reads/writes not retried)
- Iterator (no way to scan all entries)

---

## 1. Crash Safety (DONE)

### Analysis

Three crash windows exist in the current implementation:

**Window A — Overflow allocation** (`disk_hash.c` put path):

Current write order during overflow:
1. `alloc_overflow` writes zeroed page at `new_id`
2. Parent bucket's `overflow_id` updated to point to `new_id`
3. Data written into `new_id`

If crash between (2) and (3): parent points to an empty page. Not corrupt
(reads as empty bucket with count=0), but the inserted entry is lost.

**Window B — Header staleness:**

`entry_count` and `overflow_count` are only flushed to disk on explicit
`disk_hash_sync()`. Crash between mutations = stale metadata on reopen.
The data pages themselves are correct, but the count is wrong.

**Window C — Single-page torn writes (non-issue):**

4096-byte page-aligned pwrite is atomic on Linux ext4/xfs/btrfs. The kernel
writes filesystem blocks atomically. No torn writes possible for our
page-aligned, page-sized I/O. No action needed.

### Solution

**Fix A — Reorder overflow writes (data-first):**

New write order:
1. Write data page (entry + bucket header) at `new_id`
2. Update parent bucket's `overflow_id` to point to `new_id`

Crash after (1) but before (2): orphaned page with valid data. Harmless —
wastes 4KB of disk space. The entry is lost but the table is consistent.

Crash after (2): parent correctly points to populated overflow page. Correct.

**Fix B — Dirty flag + recovery scan:**

Add a `dirty` flag (1 byte) in the header's reserved area:
- Set `dirty = 1` on first mutation after open/sync (single pwrite to header page)
- Clear `dirty = 0` on `disk_hash_sync()`
- On `disk_hash_open()`: if `dirty == 1`, run recovery scan

Recovery scan: iterate all bucket pages (primary + overflow), count occupied
slots to reconstruct `entry_count`. Also count overflow pages to reconstruct
`overflow_count`. Time complexity: O(total_buckets).

At 2B entries (~43M primary buckets + overflow): sequential read of ~175 GB.
On NVMe (~3 GB/s sequential read): ~60 seconds. On SATA SSD: ~3 minutes.
Acceptable — this only runs after unclean shutdown.

### What we don't need

| Technique | Why not |
|-----------|---------|
| Write-ahead log (WAL) | Page-aligned atomic writes make WAL unnecessary |
| Journal | Same — no multi-page atomic transactions needed |
| Double-write buffer | Overkill; no torn writes with page-aligned I/O |
| fsync per operation | Too expensive; caller controls durability via sync() |

---

## 2. Thread Safety (DONE)

### Access Pattern

Verkle workload: **single writer + multiple concurrent readers**.
- Writer: block execution thread (put, delete, batch_put)
- Readers: RPC query threads (get, batch_get), background checkpoint (batch_get)

### Solution: `pthread_rwlock_t`

Add one `pthread_rwlock_t` to the `disk_hash` struct:

| Operation | Lock |
|-----------|------|
| `get`, `contains`, `batch_get` | `pthread_rwlock_rdlock` |
| `put`, `delete`, `batch_put`, `sync` | `pthread_rwlock_wrlock` |

Readers never block each other. Writer waits for active readers to drain, then
has exclusive access. Struct grows by ~56 bytes (platform-dependent). Still
negligible — total ~104 bytes in memory.

### Why not bucket-level locks

| Approach | RAM overhead at 2B keys | Benefit |
|----------|------------------------|---------|
| Single rwlock | 56 bytes | Full read concurrency |
| Per-bucket rwlock | ~56 × 43M = ~2.4 GB | Write concurrency (not needed) |
| Striped locks (1024) | ~56 KB | Partial write concurrency |

Single rwlock is the right choice because:
- The single-writer pattern has zero write contention to optimize
- Readers already get full concurrency through rwlock
- Zero-RAM is a core design goal — per-bucket locks defeat it
- Striped locks add complexity for a benefit we don't need

---

## 3. I/O Robustness

### Current Issues

- `write_header` casts pwrite return to `(void)` — errors silently ignored
- `alloc_overflow` same issue
- `read_bucket` handles short reads (zero-fills) but doesn't retry on EINTR

### Solution

Wrap pread/pwrite with retry helpers:

```c
static ssize_t pread_full(int fd, void *buf, size_t count, off_t offset) {
    size_t total = 0;
    while (total < count) {
        ssize_t n = pread(fd, (char *)buf + total, count - total, offset + total);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;  /* EOF */
        total += (size_t)n;
    }
    return (ssize_t)total;
}
```

Same for pwrite. All call sites already check `read_bucket`/`write_bucket`
return values and propagate failures.

---

## 4. Iterator

### Use Cases

- **Recovery scan**: count occupied slots after unclean shutdown
- **Migration**: export all entries to a new table (e.g., after resize)
- **Debugging**: dump table contents
- **Monitoring**: compute actual load factor, tombstone ratio

### API

```c
typedef struct {
    uint64_t bucket_id;
    uint32_t slot_idx;
} disk_hash_iter_t;

void disk_hash_iter_init(disk_hash_iter_t *it);
bool disk_hash_iter_next(disk_hash_t *dh, disk_hash_iter_t *it,
                          uint8_t *key, void *record);
```

Walks buckets sequentially: primary 0..bucket_count-1, then overflow region.
Within each bucket page, scans slots 0..slots_per_bucket-1. Skips empty and
tombstone slots. Yields key + record for occupied slots.

**Note**: iterator does not follow overflow chains from primary buckets. It
scans the entire address space linearly. This is simpler and gives sequential
I/O, which is what we want for recovery and migration.

Caller is responsible for holding appropriate lock (rdlock for read-only scan,
wrlock if mutating during iteration).

---

## 5. Stats

Computed from in-memory fields. Zero I/O, zero allocation:

```c
typedef struct {
    uint64_t entry_count;       /* live entries */
    uint64_t capacity;          /* bucket_count * slots_per_bucket */
    uint64_t bucket_count;      /* primary buckets */
    uint64_t overflow_count;    /* overflow buckets allocated */
    double   load_factor;       /* entry_count / capacity */
} disk_hash_stats_t;

void disk_hash_get_stats(const disk_hash_t *dh, disk_hash_stats_t *out);
```

---

## 6. Non-Goals

These are explicitly out of scope to keep the implementation simple:

| Feature | Reason |
|---------|--------|
| Online resize | Pre-size at creation; Ethereum scale is known (~2B keys) |
| Cross-process locking | Single process with multiple threads |
| Page checksums | Filesystem/block device handles data integrity |
| Compaction | Tombstones are reused on insert; Ethereum state rarely deletes |
| mmap | pread/pwrite with kernel page cache is already optimal; mmap adds TLB pressure at 175 GB |

---

## 7. Implementation Priority

| # | Feature | Complexity | Impact | Status |
|---|---------|-----------|--------|--------|
| 1 | Crash safety | Medium | Correctness after power loss | DONE |
| 2 | Thread safety | Low | Concurrent reads during block execution | DONE |
| 3 | I/O robustness | Low | Reliability under I/O pressure | |
| 4 | Iterator | Low | Enables recovery + migration | |
| 5 | Stats | Trivial | Monitoring / debugging | |

---

## 8. Testing

### Existing
- `test_disk_hash` — 12 unit tests (CRUD, batch, tombstone, persistence, overflow, scale)
- `test_stress_disk_hash` — 6-phase stress test at configurable scale (default 1M)

### New tests (DONE)
- `test_crash_disk_hash` — 4 scenarios × N rounds: fork + kill -9 + recovery,
  baseline key survival verification across all scenarios
- `test_mt_disk_hash` — 1 writer thread + N reader threads, safe-range integrity,
  hot-range consistency checks, fail-fast on any corruption
