# Hart — Hashed Adaptive Radix Trie

## Overview

Hart is a purpose-built data structure for Ethereum state management. It combines
an Adaptive Radix Trie (ART) for O(1) key-value lookups with an embedded MPT
(Merkle Patricia Trie) hash cache for incremental state root computation.

Two hart instances exist per node:
- **Account index** (`acct_index`): maps `keccak256(address)` → account vector index (4 bytes)
- **Per-account storage** (`resource.storage`): maps `keccak256(slot)` → 32-byte value

Both use fixed 32-byte keys (keccak hashes) and fixed-size values.

## Memory Layout

All nodes and leaves live in a single **bump arena** (`hart_t.arena`).
Arena grows via `realloc` (doubling), never shrinks except during compaction.

```
hart_t
├── root:       hart_ref_t (offset into arena)
├── size:       number of key-value pairs
├── arena:      uint8_t* (heap-allocated, realloc on growth)
├── arena_used: current watermark
├── arena_cap:  allocated capacity
└── value_size: 4 (acct_index) or 32 (storage)
```

### References (hart_ref_t)

A `hart_ref_t` is a 32-bit value encoding an arena offset:

```
Bit 31:     0 = inner node, 1 = leaf
Bits 30-0:  arena_offset >> 4 (16-byte aligned)
```

Max addressable arena: `0x7FFFFFFF * 16 = ~32 GB`.
`HART_REF_NULL = 0` (offset 0 is reserved, arena_used starts at 16).

### Pointer resolution

```c
void *ref_ptr(hart_t *t, hart_ref_t ref) {
    return t->arena + ((ref & 0x7FFFFFFF) << 4);
}
```

**Important**: pointers are invalidated by any operation that triggers arena
`realloc` (insert, alloc_leaf, alloc_node). Always re-resolve after allocation.

## Node Types

All nodes share a 4-byte header: `type(1) num_children(1) flags(1) _pad(1)`.
Each node embeds a 32-byte `hash[]` field for the MPT hash cache.

### Node4 (56 bytes)

```
┌──────────────────────────────────────────────┐
│ type=0 │ num(1-4) │ flags │ pad              │  4 bytes
├──────────────────────────────────────────────┤
│ keys[4]          (sorted byte values)        │  4 bytes
├──────────────────────────────────────────────┤
│ children[4]      (hart_ref_t, 4 each)        │ 16 bytes
├──────────────────────────────────────────────┤
│ hash[32]         (cached MPT hash)           │ 32 bytes
└──────────────────────────────────────────────┘
```

Linear scan for lookup. Grows to Node16 at 5 children.

### Node16 (104 bytes)

```
┌──────────────────────────────────────────────┐
│ type=1 │ num(5-16) │ flags │ pad             │  4 bytes
├──────────────────────────────────────────────┤
│ keys[16]         (sorted byte values)        │ 16 bytes
├──────────────────────────────────────────────┤
│ children[16]     (hart_ref_t, 4 each)        │ 64 bytes
├──────────────────────────────────────────────┤
│ hash[32]         (cached MPT hash)           │ 32 bytes
└──────────────────────────────────────────────┘

Lookup: SSE _mm_cmpeq_epi8 + _mm_movemask_epi8 (single instruction match).
```

Grows to Node48 at 17 children.

### Node48 (484 bytes)

```
┌──────────────────────────────────────────────┐
│ type=2 │ num(17-48) │ flags │ pad            │   4 bytes
├──────────────────────────────────────────────┤
│ index[256]       (byte → slot, 0xFF=empty)   │ 256 bytes
├──────────────────────────────────────────────┤
│ children[48]     (hart_ref_t, 4 each)        │ 192 bytes
├──────────────────────────────────────────────┤
│ hash[32]         (cached MPT hash)           │  32 bytes
└──────────────────────────────────────────────┘
```

Lookup: `index[byte]` gives slot in O(1). Grows to Node256 at 49 children.

### Node256 (1060 bytes)

```
┌──────────────────────────────────────────────┐
│ type=3 │ num(49-256) │ flags │ pad           │    4 bytes
├──────────────────────────────────────────────┤
│ children[256]    (hart_ref_t, 4 each)        │ 1024 bytes
├──────────────────────────────────────────────┤
│ hash[32]         (cached MPT hash)           │   32 bytes
└──────────────────────────────────────────────┘
```

Direct indexed: `children[byte]`. No key array needed.

### Leaf (32 + value_size bytes)

```
┌──────────────────────────────────────────────┐
│ key[32]          (full 32-byte keccak hash)  │ 32 bytes
├──────────────────────────────────────────────┤
│ value[value_size] (4 for index, 32 for stor) │  4 or 32 bytes
└──────────────────────────────────────────────┘
```

No header, no hash cache. Distinguished from nodes by the high bit of `hart_ref_t`.

## Tree Structure

Hart is a **pure 256-way radix trie** with no path compression.
Each inner node level dispatches on one byte of the 32-byte key:

```
depth 0:  key[0]   → root node
depth 1:  key[1]   → child of root
...
depth 31: key[31]  → child at max depth
leaf:     full key → stored value
```

Because keys are keccak hashes (uniformly distributed), the tree is well-balanced.
No degenerate chains occur. Typical tree with 30M entries has ~8-10 active levels
before reaching leaves (upper levels are dense Node256, lower levels thin out).

### No path compression

Unlike traditional ART, hart does NOT compress single-child chains.
With keccak-hashed keys, path compression provides no benefit — every byte
value is equally likely, so single-child chains are rare in practice.

This simplifies the implementation and eliminates the need for partial key
reconstruction during hash computation.

## Insert

```
insert_recursive(ref, key, value, depth):
  if ref is NULL:
    return alloc_leaf(key, value)

  if ref is leaf:
    if keys match: update value, return ref
    else: split — find first differing byte, create Node4 chain

  // inner node
  mark_dirty(ref)
  child = find_child(ref, key[depth])
  if child exists:
    new_child = insert_recursive(child, key, value, depth+1)
    update child pointer (re-resolve after possible realloc)
  else:
    new_leaf = alloc_leaf(key, value)
    ref = add_child(ref, key[depth], new_leaf)  // may grow node type
  return ref
```

Node growth: Node4 → Node16 → Node48 → Node256 as children are added.
All pointers re-resolved after any allocation (arena realloc invalidation).

## Delete

```
delete_recursive(ref, key, depth):
  if leaf and matches: return NULL (deleted)
  if inner: mark_dirty, recurse, then:
    - remove_child (may shrink: Node256→48→16→4)
    - collapse: if Node4 has 0 children → NULL
    - collapse: if Node4 has 1 child that is a leaf → promote leaf
```

Node shrinking: Node256 → Node48 → Node16 → Node4 as children are removed.

## Dirty Flags

Each inner node has a 1-bit `NODE_DIRTY` flag in `flags` byte.
- Set on: `insert` (marks path from root to insertion point)
- Set on: `delete` (marks path from root to deletion point)
- Set on: `hart_mark_path_dirty` (external API for state layer)
- Cleared on: `hart_root_hash` after computing and caching the node's hash
- New nodes born dirty

Dirty flag purpose: during `hart_root_hash`, clean subtrees return their
cached 32-byte hash in O(1). Only dirty paths are re-walked and re-hashed.

## MPT Root Hash Computation

Hart computes Ethereum's MPT (Merkle Patricia Trie) root hash directly from
the ART structure. The key insight: each ART node (one byte of the key) maps
to two MPT trie levels (high nibble + low nibble).

### ART byte → MPT nibble mapping

```
ART node at depth d dispatches on key[d] = 0xAB

MPT sees this as two levels:
  High nibble: 0xA  (key[d] >> 4)
  Low nibble:  0xB  (key[d] & 0xF)
```

The `hash_ref` function groups an ART node's children by high nibble,
then within each high-nibble group by low nibble:

```
ART Node (e.g., Node16 with children keyed on bytes):
  children: [0x1A, 0x1B, 0x2C, 0x3D, 0x3E]

Grouped by high nibble:
  hi=1: lo=[A, B]  → MPT branch with 2 children
  hi=2: lo=[C]     → single child (MPT extension)
  hi=3: lo=[D, E]  → MPT branch with 2 children

If multiple hi groups exist → outer MPT branch on hi nibbles
If single hi group → MPT extension for the hi nibble
If single child total → MPT extension for both nibbles
```

### Optimization: single-child collapsing

When an ART node has only one child, both nibbles are accumulated as a
prefix extension and passed down recursively:

```
Node4(key=0xAB) → child → Node4(key=0xCD) → child → leaf

Instead of: branch[A] → branch[B] → branch[C] → branch[D] → leaf
Produces:   extension[A,B,C,D] → leaf
```

This matches the MPT's path compression, producing compact extension nodes
for the common case of sparse subtrees.

### Hash caching

After computing a node's MPT hash, the 32-byte result is stored in the
node's embedded `hash[32]` field and the dirty flag is cleared.

On the next `hart_root_hash` call, if a node is clean:
```c
if (!is_node_dirty(ref)) {
    cached = node_hash_ptr(ref);
    return cached;  // O(1), no recursion
}
```

This makes incremental updates efficient: modifying 1000 accounts in a
tree of 30M only rehashes ~1000 paths (each ~8-10 levels deep).

## Usage in Ethereum State

### Account Index (acct_index)

```
key:   keccak256(address)       [32 bytes]
value: index into accounts[]    [4 bytes]

Operations per block:
  - insert: new account created
  - get: every account access (balance, nonce, storage, code)
  - delete: EIP-161 empty account pruning, self-destruct
  - mark_path_dirty: when account data changes
  - root_hash: compute account trie MPT root
```

The encode callback (`acct_trie_encode`) builds account RLP:
`[nonce, balance, storage_root, code_hash]`

### Per-account Storage

```
key:   keccak256(slot_number)   [32 bytes]
value: slot value               [32 bytes]

One hart per account that has storage. Created lazily.
Initial arena capacity: 1024 bytes.
```

The encode callback (`stor_value_encode`) RLP-encodes the 32-byte value
with leading zero stripping.

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| get       | O(key_len) = O(32) | ~8-10 levels for 30M entries |
| insert    | O(key_len) | May trigger arena realloc |
| delete    | O(key_len) | May shrink nodes |
| root_hash (full) | O(n) | All nodes dirty, visits every leaf |
| root_hash (incremental) | O(k * depth) | k = dirty paths |

### Memory per entry (approximate, account index with 30M entries)

Upper levels: Node256 (1060 bytes) — dense, few nodes
Mid levels: Node48/Node16 — transition zone
Lower levels: Node4 (56 bytes) — sparse, many nodes
Leaves: 36 bytes (32-byte key + 4-byte value)

Empirically: ~130 bytes per entry at 30M scale (4GB arena for 30M entries).

### Node16 SSE lookup

```c
__m128i kv = _mm_set1_epi8(byte);
__m128i cmp = _mm_cmpeq_epi8(kv, _mm_loadu_si128(keys));
int mask = _mm_movemask_epi8(cmp) & ((1 << num_children) - 1);
if (mask) return &children[__builtin_ctz(mask)];
```

Single-cycle comparison of all 16 keys simultaneously.
