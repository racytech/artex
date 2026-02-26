# TODO Checklist

## Future Improvements (Non-Critical)

- [ ] Range scan optimization (4.3x slower than LMDB: 43M vs 189M ops/s)
  - **Bottleneck**: each `next()` re-loads every node from mmap via `data_art_load_node` (~32 calls per leaf for 32-byte keys). LMDB just advances a pointer within contiguous sorted pages.
  - **Fix 1**: Store raw `const void *` pointers in `iter_stack_frame_t` instead of just `node_ref_t` — avoid re-loading parent nodes on backtrack.
  - **Fix 2**: `__builtin_prefetch` the next child's mmap page before descending.
  - **Fix 3**: Inline a fast-path `load_node` that skips checks when `scanning=true` (locks already held).
  - **File**: `database/src/data_art_iterator.c`
