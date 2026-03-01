# TODO Checklist

## Future Improvements (Non-Critical)

- [ ] Range scan optimization (4.3x slower than LMDB: 43M vs 189M ops/s)
  - **Bottleneck**: each `next()` re-loads every node from mmap via `data_art_load_node` (~32 calls per leaf for 32-byte keys). LMDB just advances a pointer within contiguous sorted pages.
  - **Fix 1**: Store raw `const void *` pointers in `iter_stack_frame_t` instead of just `node_ref_t` — avoid re-loading parent nodes on backtrack.
  - **Fix 2**: `__builtin_prefetch` the next child's mmap page before descending.
  - **Fix 3**: Inline a fast-path `load_node` that skips checks when `scanning=true` (locks already held).
  - **File**: `database/src/data_art_iterator.c`

- [ ] nt_root_hash: value resolver callback for data_layer integration
  - **Problem**: data_layer's nibble_trie stores 4-byte slot refs, not actual values. `nt_root_hash` reads inline values via `leaf_value_ptr()` — produces wrong root when values are slot refs.
  - **Fix**: Add `nt_root_hash_ex(t, resolver, ctx)` with a `nt_value_resolver_t` callback. When hashing a leaf, if resolver is set, call it to fetch actual value bytes from state_store instead of using inline ref.
  - **Files**: `database/include/nt_hash.h`, `database/src/nt_hash.c`

- [ ] nt_root_hash: COW snapshot for non-blocking hash computation
  - **Problem**: hash computation blocks the pipeline (~150ms+ per block). Can't mutate the trie while nt_root_hash walks it.
  - **Fix**: Take a COW snapshot of the nibble_trie before starting next block's mutations. Hash the snapshot in a background thread while mutations proceed on the live trie. nibble_trie is already arena-allocated — COW ensures new mutations allocate fresh nodes without touching the snapshot.
  - **Depends on**: value resolver (above)
  - **Files**: `database/include/nibble_trie.h`, `database/src/nibble_trie.c`, `database/src/nt_hash.c`

- [ ] Drop intermediate_hashes in favor of nibble_trie hash caching
  - **Problem**: intermediate_hashes maintains a separate compact_art (33B keys / 51B values) just for branch hash metadata — redundant parallel trie. ih_update cost grows with trie size (~392ms at 900K keys).
  - **Fix**: With value resolver + COW in place, nt_root_hash replaces intermediate_hashes entirely. One trie for both index and MPT hash. Per-block cost proportional to dirty nodes, not total trie size.
  - **Depends on**: value resolver + COW snapshot (above)
