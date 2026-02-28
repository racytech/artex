# Intermediate Hash Table — MPT Commitment via Flat Hash Storage

## Problem

Ethereum requires a state root (Merkle Patricia Trie hash) in every block header.
A naive in-memory trie duplicates all keys and values alongside compact_art and
costs ~100 GB at mainnet scale (500M keys). We need a structure that stores only
the hashes, not the data.

## Core Idea

Instead of building a trie in memory, store a flat table of intermediate hashes:

```
key:   trie nibble prefix (variable length, 0–64 nibbles)
value: 32-byte keccak256 hash
```

Every internal trie node (branch, extension) becomes one row. Leaf hashes are
computed on the fly from values in state.dat — never stored.

```
Full trie:                    Flat table:

     root                     ""    → 0xabcd...
    /    \                    "3"   → 0x1234...
  [3]    [7]                  "7"   → 0x5678...
  / \      \                  "3a"  → 0x9999...
[a] [f]    [2]                "3f"  → 0xaaaa...
 |    |     |                 "72"  → 0xbbbb...
v0   v1    v2                 (leaves not stored)
```

Root = `table[""]`.

## Why This Works

A block changes ~2K–50K keys out of 500M. Only the paths from dirty leaves to
the root need recomputation. Everything else is unchanged — its cached hash in
the table is still valid.

```
Block N: 50K dirty keys
  → sort by key
  → shared prefixes cluster together
  → recompute ~50K × 10 affected branches ≈ 500K keccak256 calls
  → at 200ns/hash ≈ 100ms total
```

## Data Structure

### Hash table entry

```c
// One intermediate hash entry
typedef struct {
    uint8_t prefix[32];    // nibble prefix, packed (2 nibbles/byte)
    uint8_t prefix_len;    // number of nibbles (0–64)
    uint8_t hash[32];      // keccak256 of RLP-encoded node
    uint8_t flags;         // node metadata (see below)
} ih_entry_t;
```

Flags byte:

```
bit 0-3:  unused (reserved)
bit 4-7:  unused (reserved)
```

We may later use flags to track which children exist in a branch node (a 16-bit
bitmap compressed into the entry) to avoid scanning for children.

### Storage backend: compact_art

Use a dedicated `compact_art` tree:

```c
compact_art_init(&ih_tree, 33, 32);
//                         ^key  ^value
// key:   1 byte length + 32 bytes packed prefix (padded with zeros)
// value: 32 bytes hash
```

Key encoding:

```
byte 0:      nibble count (0–64)
bytes 1–32:  nibbles packed 2-per-byte, zero-padded
```

This gives us:
- O(key_length) lookup by exact prefix
- Ordered iteration (for scanning children of a branch)
- Insert/update for modified hashes

### Memory budget

```
500M account keys → ~40M intermediate hash entries
compact_art overhead: ~50 bytes/key × 40M = 2.0 GB (index nodes)
Entry data:           65 bytes/key × 40M  = 2.6 GB (leaves pool)
Total:                                    ≈ 4.6 GB

With mem_art (variable keys): ~120 bytes/key × 40M ≈ 4.8 GB
```

Alternative: flat sorted file on disk (~2.6 GB), loaded incrementally.
For initial implementation, in-memory compact_art is simpler.

## Update Algorithm

### Overview

Per-block root computation in three phases:

```
Phase 1: Collect dirty keys from write buffer (already sorted in mem_art)
Phase 2: Stream sorted keys, compute affected subtree hashes bottom-up
Phase 3: Return root = hash at prefix ""
```

### Phase 2: Streaming hash recomputation

The algorithm processes sorted dirty keys left-to-right, maintaining a stack of
open branch nodes. When the key path diverges from the current stack, branches
are closed (hashed) and popped.

```
Stack state while processing sorted keys:

  key 0x3a01... → stack: [root, "3", "3a", "3a0", "3a01"]
  key 0x3a02... → same prefix "3a0" → close "3a01", open "3a02"
  key 0x7f00... → diverges at root → close entire "3a" subtree, open "7"
```

#### Stack frame

```c
typedef struct {
    uint8_t prefix[32];     // packed nibble prefix for this branch
    uint8_t prefix_len;     // nibble count
    uint8_t child_hashes[16][32];  // collected children hashes
    uint16_t child_bitmap;         // which children are present
    uint8_t *branch_value;         // value at this branch (if any)
    uint16_t branch_value_len;
    bool dirty;                    // any child changed?
} ih_stack_frame_t;
```

Maximum stack depth: 64 (one per nibble). Each frame is ~560 bytes.
Total stack: 64 × 560 ≈ 36 KB. Trivial.

#### Algorithm pseudocode

```
update(sorted_dirty_keys[], values[], count):
    stack = []

    for i = 0..count-1:
        key_nibbles = key_to_nibbles(sorted_dirty_keys[i])

        // Close branches that diverge from current key
        common = common_prefix(stack.top().prefix, key_nibbles)
        while stack.depth > common:
            frame = stack.pop()
            hash = hash_branch_node(frame)
            if stack.not_empty():
                nibble = frame.prefix[frame.prefix_len - 1]
                stack.top().child_hashes[nibble] = hash
                stack.top().dirty = true
            // Update intermediate hash table
            ih_tree.put(frame.prefix, hash)

        // Open branches down to leaf depth
        for d = stack.depth..key_nibbles.len-1:
            frame = new_stack_frame(key_nibbles[0..d])
            // Load existing children from ih_tree
            load_existing_children(frame, &ih_tree)
            stack.push(frame)

        // Compute leaf hash and place in parent
        leaf_hash = hash_leaf(key_nibbles, values[i])
        nibble = key_nibbles[key_nibbles.len - 1]
        stack.top().child_hashes[nibble] = leaf_hash
        stack.top().dirty = true

    // Close remaining stack frames
    while stack.not_empty():
        frame = stack.pop()
        hash = hash_branch_node(frame)
        if stack.not_empty():
            nibble = frame.prefix[frame.prefix_len - 1]
            stack.top().child_hashes[nibble] = hash
            stack.top().dirty = true
        ih_tree.put(frame.prefix, hash)

    root = ih_tree.get("")
```

#### Loading existing children

When opening a branch frame at prefix P, we need the hashes of all 16 possible
children. For non-dirty children, these come from the intermediate hash table:

```
load_existing_children(frame, ih_tree):
    for nibble = 0..15:
        child_prefix = frame.prefix + nibble
        hash = ih_tree.get(child_prefix)
        if hash != NULL:
            frame.child_hashes[nibble] = hash
            frame.child_bitmap |= (1 << nibble)
```

With compact_art's ordered iteration, we can scan once for all 16 children
(they're consecutive in sort order) instead of 16 separate lookups.

### Handling extensions

Extensions (shared path nodes) are implicit in the prefix structure. When a
branch at prefix "3a" has only one child at nibble "f", and that child has only
one child at nibble "0", the trie would have:

```
extension(path=[f,0]) → branch(...)
```

We detect this during hash computation: if a branch has exactly one child and no
value, it collapses into an extension. The hash computation must account for this:

```
hash_branch_node(frame):
    children_count = popcount(frame.child_bitmap)

    if children_count == 1 && !frame.branch_value:
        // This is actually an extension node, not a branch
        child_nibble = find_single_child(frame)
        child_hash = frame.child_hashes[child_nibble]
        return hash_extension([child_nibble], child_hash)

    if children_count == 0 && !frame.branch_value:
        return NULL  // empty node, remove from table

    // Standard branch: RLP([child_0, ..., child_15, value])
    return hash_branch(frame.child_hashes, frame.child_bitmap,
                       frame.branch_value, frame.branch_value_len)
```

**Note**: Extension detection must also handle multi-nibble extensions. If branch
at "3" has one child "a" which has one child "f", we need extension path [a,f].
This requires looking ahead or compressing after the fact. The stack-based approach
handles this naturally: when closing a branch that collapses to an extension, we
merge it with the parent's extension prefix if the parent also collapses.

### Handling deletes

A NULL value in the dirty keys list means delete. The leaf hash becomes empty
(0x80 in RLP). If this causes a branch to have fewer children, the structural
normalization (extension collapsing) handles it during hash computation.

When all children of a branch are removed, the branch's hash entry is deleted
from the intermediate hash table.

## Node Hashing Rules (Ethereum Yellow Paper)

### Hex-prefix encoding

Encode nibble path + leaf/extension flag:

```
odd length:   byte 0 = (flag|1) << 4 | first_nibble, then pairs
even length:  byte 0 = flag << 4, then pairs
flag:         0 = extension, 2 = leaf
```

### RLP encoding per node type

```
leaf:       RLP([hex_prefix(path, leaf=true), value])
extension:  RLP([hex_prefix(path, leaf=false), child_ref])
branch:     RLP([ref_0, ref_1, ..., ref_15, value])
```

### Embedding rule

If `len(RLP(node)) < 32 bytes`, the parent embeds the raw RLP inline instead
of the keccak256 hash. Root is always hashed regardless of size.

This means the intermediate hash table must store either:
- A 32-byte hash (for nodes with RLP ≥ 32 bytes), or
- Raw RLP bytes (for nodes with RLP < 32 bytes)

In practice, only leaf and extension nodes with very short paths/values are
small enough to be inline. Branch nodes are always ≥ 32 bytes (17 elements).

To handle this, the value in the hash table is:

```c
// Value stored in ih_tree:
// byte 0:      flags (bit 0 = is_inline)
// bytes 1-32:  hash (if not inline) or inline RLP (if inline, len in flags)
//
// Actually simpler: just store 32 bytes always.
// If inline, store the RLP padded to 32 bytes + real length in flags.
// Since inline means < 32 bytes, it always fits.
```

## Integration with Data Layer

### Where it fits

```
Block execution:
  EVM runs → dl_put/dl_delete accumulate in write buffer (mem_art)

After execution:
  1. Extract sorted dirty keys from write buffer
  2. For each key, read current value (dl_get) for leaf hash
  3. Run streaming hash update → new state root
  4. dl_merge() flushes buffer to persistent index
  5. Checkpoint if needed (includes ih_tree serialization)
```

### API

```c
typedef struct ih_state ih_state_t;

// Lifecycle
ih_state_t *ih_create(void);
void ih_destroy(ih_state_t *ih);

// Per-block update
//   keys:       sorted 32-byte account keys
//   values:     RLP-encoded account state (NULL = delete)
//   value_lens: length of each value (0 = delete)
//   count:      number of dirty keys
//   root_out:   receives the 32-byte state root
void ih_update(ih_state_t *ih,
               const uint8_t *const *keys,
               const uint8_t *const *values,
               const uint16_t *value_lens,
               size_t count,
               uint8_t root_out[32]);

// Get current root (no recomputation)
void ih_root(const ih_state_t *ih, uint8_t out[32]);

// Stats
size_t ih_entry_count(const ih_state_t *ih);

// Persistence (checkpoint integration)
bool ih_save(const ih_state_t *ih, int fd);
bool ih_load(ih_state_t *ih, int fd);
```

### Commitment interface wrapper

```c
// commitment.h vtable adapter
commitment_t *ih_as_commitment(ih_state_t *ih);
```

## Persistence and Recovery

### Checkpoint path

At checkpoint time, serialize the ih_tree alongside index.dat:

```
hashes.dat:
  [4 bytes: entry count]
  [entries sorted by prefix, each 33+32 = 65 bytes]
  [4 bytes: CRC32]
```

Loading: bulk-insert into compact_art (or direct mmap for flat array approach).

### Recovery without checkpoint

The intermediate hash table is fully deterministic — it can be rebuilt from
compact_art + state.dat by walking all keys and computing hashes from scratch.
Cost: 500M keys × 200ns/hash × 10 hashes/key ≈ 1000 seconds (~17 minutes).

With checkpointing every N blocks, recovery replays at most N blocks.

## Performance

```
Per-block (50K dirty keys):
  Sort:              50K keys × O(k log k)  ≈ 1ms
  Leaf hashing:      50K × 200ns            ≈ 10ms
  Branch hashing:    500K × 200ns           ≈ 100ms
  IH table lookups:  500K × O(1)            ≈ 5ms
  IH table updates:  50K × O(1)             ≈ 1ms
  Total:                                    ≈ 117ms

Memory:
  Stack:             64 × 560 bytes         ≈ 36 KB
  IH table:          40M entries            ≈ 4.6 GB (compact_art)
                     or                     ≈ 2.6 GB (flat file)
```

## Open Questions

1. **Storage tries**: Each contract has a storage trie. Use one global tree with
   composite keys `keccak256(addr) || keccak256(slot)` (simpler) or per-contract
   trees (protocol-accurate)? PROOF_GENERATION.md recommends the global approach.

2. **Inline node handling**: Store inline RLP alongside hashes, or recompute
   inline nodes each time? Inline nodes are rare enough that recomputation may
   be simpler.

3. **Flat file vs compact_art**: Flat sorted file uses less memory (~2.6 GB)
   but requires binary search. compact_art uses more memory (~4.6 GB) but gives
   O(1) lookups. Decision depends on whether 2 GB matters at mainnet scale.

4. **Extension path compression**: Can we avoid storing extension nodes entirely?
   Since extensions are deterministic (single-child branch → extension), we could
   detect them during hash computation and skip the table entry.

## Files

```
database/include/intermediate_hashes.h    — public API
database/src/intermediate_hashes.c        — implementation
database/tests/test_intermediate_hashes.c — tests
```
