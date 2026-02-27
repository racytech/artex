# Checkpoint Design: Crash Recovery and Index Persistence

## Problem

compact_art lives entirely in memory. On crash, it's gone. state.dat
is on disk but might be mid-merge (partially written). We need to
recover to a known-good state without a WAL.

The key insight: **state.dat can be ahead of the checkpoint**. It may
contain extra slots from an incomplete merge — those are harmless
unreachable garbage. The checkpoint (persisted index) is the source
of truth for what's valid.

## File Layout

```
data/
├── state.dat         — value slots, append + overwrite during merge
├── code.dat          — contract bytecode, append-only
├── index.dat         — compact_art snapshot (atomic write)
├── code_index.dat    — code entry table (atomic write)
└── checkpoint.meta   — block number, entry counts, CRC32
```

## Normal Operation

```
Block N..N+K execute:
  writes → buffer (memory only, never touches disk)

Merge (every K blocks):
  1. pwrite values to state.dat (new/updated slots)
  2. Update compact_art in memory
  3. fdatasync(state.dat)

Checkpoint (immediately after merge):
  4. Write compact_art snapshot to temp file
  5. fdatasync(temp file)
  6. rename(temp, "index.dat")       ← atomic on POSIX
  7. Write checkpoint.meta to temp
  8. fdatasync + rename checkpoint.meta
```

Steps 6 and 8 are the commit points. Before rename, the old checkpoint
is still valid. After rename, the new one is valid. No window where
neither is valid.

## Crash Scenarios

### Crash during block execution (before merge)

- Buffer is lost (volatile)
- state.dat and index.dat are from the last checkpoint — consistent
- Recovery: load index.dat → rebuild compact_art, re-execute K blocks

### Crash during merge (steps 1-3)

- state.dat has some new/updated slots that aren't checkpointed
- compact_art is lost (was in memory)
- index.dat is still the old checkpoint — consistent
- Orphaned slots in state.dat are unreachable garbage
- Recovery: load old index.dat, re-execute K blocks
- Next merge overwrites or reclaims orphans

### Crash during checkpoint (steps 4-5)

- Temp file is partially written, but index.dat is untouched
- rename never happened → old checkpoint is valid
- Recovery: same as above, temp file gets cleaned up on startup

### Crash after checkpoint rename (step 6+)

- New checkpoint is fully committed
- Recovery: load new index.dat, re-execute blocks since this checkpoint

**Every scenario recovers correctly.** The atomic rename is the
single commit point.

## Index File Format

```
Header (4096 bytes, page-aligned):
  magic:           "ARTIDX01" (8 bytes)
  version:         uint32
  block_number:    uint64         ← block at which this checkpoint was taken
  num_entries:     uint64         ← number of key-ref pairs
  key_size:        uint32 (32)
  value_size:      uint32 (4)
  next_slot:       uint32         ← state.dat slot allocator position
  free_count:      uint32         ← number of freed slots
  checksum:        uint32         ← CRC32 of entries + free list
  padding to 4096

Entries (sorted by key, starting at offset 4096):
  [32B key][4B ref] × num_entries
  Fixed 36-byte stride. No per-entry framing.

Free list (after entries):
  [4B slot_index] × free_count
```

File size at scale:
- 100M entries: 4KB header + 3.6 GB entries = ~3.6 GB
- 500M entries: 4KB header + 18 GB entries = ~18 GB

## Checkpoint Procedure

```c
checkpoint(data_layer_t *dl, uint64_t block_number):
  // 1. Open temp file
  int fd = open("index.dat.tmp", O_WRONLY | O_CREAT | O_TRUNC, 0644);

  // 2. Write header (placeholder, fill checksum later)
  index_header_t hdr = {
    .magic = "ARTIDX01",
    .version = 1,
    .block_number = block_number,
    .num_entries = compact_art_size(&dl->index),
    .key_size = 32,
    .value_size = 4,
    .next_slot = dl->store.next_slot,
    .free_count = dl->store.free_count,
  };
  pwrite(fd, &hdr, 4096, 0);

  // 3. Walk compact_art iterator, write sorted (key, ref) pairs
  uint32_t crc = 0;
  off_t offset = 4096;
  compact_art_iterator_t *iter = compact_art_iterator_create(&dl->index);
  while (compact_art_iterator_next(iter)) {
    const uint8_t *key = compact_art_iterator_key(iter);
    const void *ref = compact_art_iterator_value(iter);
    uint8_t entry[36];
    memcpy(entry, key, 32);
    memcpy(entry + 32, ref, 4);
    pwrite(fd, entry, 36, offset);
    crc = crc32_update(crc, entry, 36);
    offset += 36;
  }
  compact_art_iterator_destroy(iter);

  // 4. Write free list
  for (uint32_t i = 0; i < dl->store.free_count; i++) {
    pwrite(fd, &dl->store.free_list[i], 4, offset);
    crc = crc32_update(crc, &dl->store.free_list[i], 4);
    offset += 4;
  }

  // 5. Update header with checksum
  hdr.checksum = crc;
  pwrite(fd, &hdr, 4096, 0);

  // 6. Sync and atomic rename
  fdatasync(fd);
  close(fd);
  rename("index.dat.tmp", "index.dat");

  // 7. Write checkpoint.meta (same atomic pattern)
  write_checkpoint_meta(block_number, hdr.num_entries, crc);
```

### Checkpoint Cost

Sequential write to disk. NVMe handles ~2 GB/s sequential:

| Keys | Index file | Write time |
|------|-----------|------------|
| 10M | 360 MB | ~0.2s |
| 100M | 3.6 GB | ~1.8s |
| 500M | 18 GB | ~9s |

Acceptable for something that happens every K=100 blocks (~20 minutes
of chain time). The merge itself takes <1 second, so checkpoint is
the dominant cost.

### Optimization: Buffered Sequential Write

Instead of pwrite per entry, buffer 64 KB chunks and write
sequentially. This saturates NVMe bandwidth:

```c
uint8_t buf[65536];
size_t buf_pos = 0;

while (compact_art_iterator_next(iter)) {
    memcpy(buf + buf_pos, key, 32);
    memcpy(buf + buf_pos + 32, ref, 4);
    buf_pos += 36;
    if (buf_pos + 36 > sizeof(buf)) {
        write(fd, buf, buf_pos);
        buf_pos = 0;
    }
}
if (buf_pos > 0) write(fd, buf, buf_pos);
```

## Recovery Procedure

```c
recover(data_layer_t *dl):
  // 1. Open existing data files
  state_store_open(&dl->store, "state.dat");
  code_store_open(&dl->code, "code.dat");

  // 2. Load index.dat
  int fd = open("index.dat", O_RDONLY);
  index_header_t hdr;
  pread(fd, &hdr, 4096, 0);

  // Verify magic and checksum
  assert(memcmp(hdr.magic, "ARTIDX01", 8) == 0);

  // 3. Rebuild compact_art from sorted entries
  compact_art_init(&dl->index, hdr.key_size, hdr.value_size);
  off_t offset = 4096;
  uint8_t entry[36];
  for (uint64_t i = 0; i < hdr.num_entries; i++) {
      pread(fd, entry, 36, offset);
      compact_art_insert(&dl->index, entry, entry + 32);
      offset += 36;
  }

  // 4. Restore free list
  dl->store.next_slot = hdr.next_slot;
  dl->store.free_count = hdr.free_count;
  for (uint32_t i = 0; i < hdr.free_count; i++) {
      pread(fd, &dl->store.free_list[i], 4, offset);
      offset += 4;
  }
  close(fd);

  // 5. Read checkpoint.meta → last_block_number
  uint64_t last_block = read_checkpoint_meta();

  // 6. Re-execute blocks since checkpoint
  //    (caller provides blocks from consensus layer)
  return last_block;
```

### Recovery Time

| Keys | Index load | Block re-exec | Total |
|------|-----------|--------------|-------|
| 10M | ~0.5s | ~1s | ~1.5s |
| 100M | ~5s | ~2s | ~7s |
| 500M | ~25s | ~5s | ~30s |

Index load is dominated by compact_art_insert in a loop. Sequential
read at 2 GB/s + insert at ~3-4M keys/s. Block re-execution is fast
because state is already in page cache.

## Compaction

Over time, state.dat accumulates dead slots from deletes and updates.
The free list tracks freed slots for reuse, but if churn is high,
the file grows with garbage.

### When to Compact

Compact when dead space exceeds a threshold:

```
dead_ratio = free_count / next_slot
if dead_ratio > 0.10:   // >10% wasted
    compact()
```

### Compaction Procedure

```
compact():
  1. Create state_new.dat
  2. Walk compact_art in sorted order (iterator)
  3. For each live key:
     a. pread value from old state.dat (via compact_art ref)
     b. pwrite to next sequential slot in state_new.dat
     c. Update compact_art ref to new slot index
  4. fdatasync(state_new.dat)
  5. Checkpoint (write new index.dat with updated refs)
  6. rename(state_new.dat, state.dat)
  7. Delete old state.dat
  8. Clear free list, reset next_slot = num_live_entries
```

Compaction is a full rewrite of state.dat. Cost:

| Keys | state.dat | Rewrite time |
|------|----------|-------------|
| 100M | 6.4 GB | ~3s |
| 500M | 32 GB | ~16s |

Sequential I/O, so NVMe handles it well. Run infrequently — weekly
or when dead ratio exceeds threshold.

### Alternative: Incremental Reuse

Instead of periodic compaction, aggressively reuse freed slots during
merge. The free list acts as a pool:

```
merge():
  for each insert:
    if free_list not empty:
      slot = free_list.pop()    // reuse dead slot
    else:
      slot = next_slot++        // extend file
```

This is already implemented in the PoC. If reuse keeps pace with
deletes, state.dat doesn't grow with garbage and compaction is
rarely needed.

## Checkpoint.meta Format

Small metadata file for fast startup validation:

```
checkpoint.meta (512 bytes):
  magic:           "ARTCKPT1" (8 bytes)
  block_number:    uint64
  index_entries:   uint64
  index_checksum:  uint32
  state_slots:     uint32
  code_entries:    uint32
  timestamp:       uint64
  padding to 512
```

Written atomically (write to temp + fdatasync + rename). Read first
on startup to determine recovery point before loading the full
index file.

## Summary

```
                     Merge                    Checkpoint
                       │                          │
  buffer ─────→ state.dat (pwrite)         index.dat (atomic)
                compact_art (memory)       checkpoint.meta
                fdatasync                  fdatasync + rename
                       │                          │
                       ▼                          ▼
                 data on disk              recovery point
                 (may be ahead             (always consistent)
                  of checkpoint)

  Crash at any point → load last checkpoint → re-execute K blocks → done
```

No WAL. No journal. No two-phase commit. Just atomic rename on a
single file. The blockchain provides the replay log for everything
between checkpoints.
