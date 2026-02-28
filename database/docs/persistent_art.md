# Persistent ART

COW-based memory-mapped ART index. Replaces compact_art + checkpoint system.

## File Layout

Single file: `index.art`

```
[Page 0: Metadata]     64 bytes (root page ID, page count, free list head)
[Page 1..N: Tree]      Node and leaf pages
```

Page size: 4KB (OS page aligned, mmap friendly).

## Node Packing

Small nodes share pages via slab allocator to avoid waste:

| Type    | Size   | Per page |
|---------|--------|----------|
| Node4   | 64B    | 63       |
| Node16  | 256B   | 15       |
| Leaf    | 64B    | 63       |
| Node48  | 768B   | 5        |
| Node256 | 2KB    | 2        |

Page header (8B): page type + slab bitmap/metadata.

Refs are 4-byte page-local addresses: `[page_id:20 | slot_index:12]`.

## COW Writes

Mutation path (insert/delete):

1. Copy leaf page (or slab slot) to new location, apply change
2. Copy parent, update child ref
3. Walk up to root, copying each level
4. Write new root page ID to metadata page
5. `fdatasync` the file
6. Old pages added to free list

Shared upper paths are COW'd once per merge batch, not per key.

## Merge Integration

```
dl_merge():
  for each key in write_buffer:
    pwrite value to state.dat
    persistent_art_insert(key, slot_ref)   // COW pages
  fdatasync(state.dat)
  persistent_art_commit()                  // root swap + fsync
  clear write_buffer
```

## Recovery

Read metadata page 0 → get root page ID → done.
No replay, no rebuild. Tree is always consistent on disk.

## API (replaces compact_art + checkpoint)

```c
bool     part_open(persistent_art_t *tree, const char *path, uint32_t key_size);
void     part_close(persistent_art_t *tree);
bool     part_insert(persistent_art_t *tree, const uint8_t *key, const void *value);
bool     part_delete(persistent_art_t *tree, const uint8_t *key);
bool     part_get(const persistent_art_t *tree, const uint8_t *key, void *value);
bool     part_commit(persistent_art_t *tree);  // atomic root swap + fsync

// Iterator (sorted order)
part_iterator_t *part_iterator_create(const persistent_art_t *tree);
bool     part_iterator_next(part_iterator_t *iter);
// ... key/value/done/destroy same pattern as compact_art
```

## Estimated Costs (1B keys, 32B keys, 4B values)

| Metric | Value |
|--------|-------|
| File size | ~80-120 GB (with slab packing) |
| Lookup (hot) | ~200ns |
| Lookup (cold) | ~10-50us (page fault) |
| Merge 200 keys | ~200-500us |
| Commit | ~microseconds + fsync |
| Recovery | instant |
| Write amp / block | ~1.5-3 MB |

## What It Replaces

- `compact_art` (in-memory sorted index)
- `checkpoint.c` (index.dat serialization + CRC + recovery)
- `checkpoint.h`

## What Stays

- `state.dat` / `state_store.c` (flat value slots)
- `code.dat` / `code_store.c` (bytecode storage)
- `mem_art` (per-block write buffer)
- `state_db` / `evm_state` (unchanged API)
