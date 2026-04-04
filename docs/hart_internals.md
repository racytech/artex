# Hart — Hashed Adaptive Radix Trie

## What is Hart?

Hart is an in-memory tree that serves two purposes simultaneously:

1. **Key-value index** — fast O(1) lookups by 32-byte keccak hash keys
2. **MPT hash engine** — computes Ethereum's Merkle Patricia Trie root hash
   incrementally, using cached hashes embedded in each node

It replaces the previous two-component design (mem_art for indexing + art_mpt
for hashing) with a single unified structure.

---

## Part 1: The Tree Structure

### How keys are stored

Every key in hart is a 32-byte keccak hash (e.g., `keccak256(address)` or
`keccak256(slot)`). The tree dispatches on one byte at a time:

```
Key: a7 3f 9c 42 ... (32 bytes)
      |   |   |   |
      v   v   v   v
    depth depth depth depth
      0    1    2    3  ... up to 31
```

At each depth, an inner node holds up to 256 children (one per possible
byte value). The node TYPE determines how those children are stored.

### A small tree (4 keys)

```
Keys:
  K1 = a7 3f ...    K2 = a7 81 ...
  K3 = b2 10 ...    K4 = b2 10 ...  (differ at depth 2+)

Tree:

  Root (Node4, 2 children)
  ├── [0xa7] → Node4 (2 children)
  │            ├── [0x3f] → Leaf(K1)
  │            └── [0x81] → Leaf(K2)
  │
  └── [0xb2] → Node4 (1 child)
               └── [0x10] → Node4 (2 children)
                            ├── [...] → Leaf(K3)
                            └── [...] → Leaf(K4)
```

Each inner node dispatches on the byte at its depth. Leaves store the full
32-byte key + the value.

### Node types (adaptive sizing)

The "adaptive" in ART means nodes grow/shrink based on how many children
they have. This saves memory — a node with 3 children uses 56 bytes (Node4),
not 1060 bytes (Node256).

```
Children    Node Type    Size       Lookup Method
─────────────────────────────────────────────────────
  1-4       Node4         56 B     linear scan (4 keys)
  5-16      Node16       104 B     SSE parallel compare
 17-48      Node48       484 B     index[256] → slot
 49-256     Node256     1060 B     direct: children[byte]
```

Growth: when a Node4 gets a 5th child, it becomes a Node16.
Shrink: when a Node16 drops to 4 children, it becomes a Node4.

### Node layouts in memory

All nodes live in a contiguous arena. Each has a 4-byte header:

```
┌─────────┬──────────────┬───────┬─────┐
│ type(1) │ num_children │ flags │ pad │   4 bytes (header)
└─────────┴──────────────┴───────┴─────┘
```

**Node4** — linear array of up to 4 key-child pairs:

```
 56 bytes total
┌────────────────────┐
│ header (4B)        │
│ keys[4]   (4B)     │  ← sorted byte values
│ children[4] (16B)  │  ← hart_ref_t per child
│ hash[32]  (32B)    │  ← MPT hash cache
└────────────────────┘
```

**Node16** — SSE-searchable array of up to 16:

```
 104 bytes total
┌────────────────────┐
│ header (4B)        │
│ keys[16]  (16B)    │  ← 16 bytes = 1 SSE register
│ children[16] (64B) │
│ hash[32]  (32B)    │
└────────────────────┘

Lookup: load all 16 keys into SSE register, compare in 1 cycle:
  __m128i cmp = _mm_cmpeq_epi8(search_byte, keys_register);
  int match = _mm_movemask_epi8(cmp);
```

**Node48** — 256-byte index mapping byte → slot:

```
 484 bytes total
┌────────────────────┐
│ header (4B)        │
│ index[256] (256B)  │  ← byte value → child slot (0xFF = empty)
│ children[48] (192B)│  ← up to 48 children
│ hash[32]  (32B)    │
└────────────────────┘

Lookup: slot = index[byte]; if (slot != 0xFF) return children[slot];
```

**Node256** — direct indexed, no key array needed:

```
 1060 bytes total
┌─────────────────────┐
│ header (4B)         │
│ children[256] (1KB) │  ← children[byte], NULL = no child
│ hash[32]  (32B)     │
└─────────────────────┘

Lookup: return children[byte];  // single array access
```

**Leaf** — raw key + value, no header:

```
 36 or 64 bytes (depending on value_size)
┌────────────────────┐
│ key[32]   (32B)    │  ← full 32-byte keccak hash
│ value[N]  (4/32B)  │  ← account index (4B) or storage value (32B)
└────────────────────┘
```

### What the tree looks like at scale (30M keys)

With 30 million keccak-hashed keys (uniformly distributed), the tree is
**top-heavy** — dense at the root, sparse at the leaves:

```
Depth 0:  Root = Node256
          All 256 slots occupied → ~117K keys per subtree
          ┌───────────────────────────────────────────┐
          │ [00] [01] [02] ... [a7] ... [fe] [ff]     │
          │  ↓    ↓    ↓        ↓        ↓    ↓      │
          │ 117K 117K 117K   117K     117K 117K       │
          └───────────────────────────────────────────┘

Depth 1:  Each child = Node256 (most fully populated)
          256 × 256 = 65,536 subtrees → ~458 keys each
          ┌───────────────────────────────────────────┐
          │ [00] [01] ... [ff]                         │
          │  ↓    ↓        ↓                           │
          │ 458  458      458  keys each               │
          └───────────────────────────────────────────┘

Depth 2:  Node256 or Node48 (still fairly dense)
          ~16M subtrees → ~2 keys each
          ┌───────────────────────────────────────────┐
          │ Many children, but most have 1-3 entries   │
          │ Nodes start thinning: Node48 → Node16      │
          └───────────────────────────────────────────┘

Depth 3+: Node4 (1-2 children) → leads quickly to leaves
          ┌──────────┐
          │ Node4    │
          │  └─ Leaf │   Most lookups terminate here
          └──────────┘

Typical lookup depth: 4-5 levels (NOT 32)
```

The reason lookups are so shallow: 256^3 = 16.7M, so 3 bytes of a keccak
hash are enough to distinguish most of 30M keys. By depth 4-5, nearly
every subtree contains a single leaf.

### Arena allocation

All nodes and leaves live in a single contiguous byte array:

```
arena (heap-allocated, grows via realloc)
┌──────────┬────────┬────────┬────────┬──────────┬─────────────┐
│ reserved │ Node256│ Node256│ Leaf   │ Node4    │ ... free    │
│ (16B)    │ (root) │        │        │          │             │
└──────────┴────────┴────────┴────────┴──────────┴─────────────┘
 offset 0    16       1076     2136     2172       arena_used
                                                   ↑ watermark
```

References (`hart_ref_t`) encode the byte offset divided by 16:

```
hart_ref_t = (offset >> 4) | leaf_bit

Bit 31: 1 = leaf, 0 = inner node
Bits 0-30: offset / 16

Example: offset 1076 → ref = 1076/16 = 67 → hart_ref_t = 0x00000043
         leaf at 2136 → ref = 2136/16 = 133 → hart_ref_t = 0x80000085
```

To get back to a pointer: `arena + (ref & 0x7FFFFFFF) << 4`

Deleted nodes leave holes in the arena (never reclaimed until compaction).
New allocations always come from the watermark — pure bump allocation.

---

## Part 2: Embedded MPT Hash Cache

### Why each node has a hash[32]

Ethereum requires computing a Merkle Patricia Trie (MPT) root hash over
all accounts (or all storage slots). This is a cryptographic commitment
to the entire state — any change produces a completely different root.

Without caching, computing the root requires visiting every leaf and
hashing up the entire tree — O(n). With 30M accounts, that's ~15 seconds.

Hart embeds a 32-byte hash cache in each inner node. After computing a
subtree's hash, the result is stored in the node:

```
Node256 (depth 0)
┌─────────────────────┐
│ children[256]       │  ← pointers to depth-1 nodes
│ hash[32] = 0xab..   │  ← cached MPT hash of this entire subtree
│ flags: CLEAN        │  ← hash is valid
└─────────────────────┘
```

When the tree is modified, only the path from root to the modified leaf
is marked dirty. Everything else keeps its cached hash:

```
Insert key starting with 0xa7...

Root (DIRTY)              ← path to modified leaf
├── [0x00] (clean) ✓      ← cached hash still valid
├── [0x01] (clean) ✓
│   ...
├── [0xa7] (DIRTY)        ← this subtree changed
│   ├── [0x3f] (DIRTY)    ← path continues
│   │   └── Leaf (new)
│   └── [0x81] (clean) ✓  ← sibling unchanged
│   ...
├── [0xfe] (clean) ✓
└── [0xff] (clean) ✓

Only 3-4 nodes need rehashing. The other ~250 subtrees
return their cached hash in O(1).
```

### Dirty flag

Each node's `flags` byte contains a `NODE_DIRTY` bit:

```
Set when:
  - Node is newly allocated (born dirty)
  - insert() marks the path from root to leaf
  - delete() marks the path from root to deleted leaf
  - hart_mark_path_dirty() called externally

Cleared when:
  - hart_root_hash() computes and caches this node's hash

Clean node = cached hash is valid, skip this subtree
Dirty node = must recompute hash by visiting children
```

### Incremental cost

```
Tree: 30M entries, ~5 levels deep
Block: 500 accounts modified

Per-block root hash:
  500 dirty paths × 5 levels = ~2,500 nodes to rehash
  29,999,500 clean subtrees = return cached hash (O(1) each)

  Cost: ~10ms (vs ~15 seconds for full recomputation)
  Speedup: ~1500x
```

---

## Part 3: ART → MPT Mapping

### The problem

Hart stores keys as bytes (8-bit, 256 branches per level).
Ethereum's MPT uses nibbles (4-bit, 16 branches per level).

One hart level = one byte = TWO MPT levels:

```
Hart depth d: key[d] = 0xAB
              ↓
MPT sees two levels:
  Level 1: high nibble = 0xA (key[d] >> 4)
  Level 2: low nibble  = 0xB (key[d] & 0xF)
```

### How hash_ref converts ART → MPT

When `hart_root_hash` visits an inner node, it:

1. Gets all children (byte keys)
2. Groups them by high nibble, then low nibble
3. Builds MPT nodes (branch/extension) from the groups

```
Example: Node16 at depth d, children keyed on:
  0x1A, 0x1B, 0x2C, 0x3D, 0x3E

Step 1: Group by high nibble
  hi=0x1: children with lo=[0xA, 0xB]
  hi=0x2: children with lo=[0xC]
  hi=0x3: children with lo=[0xD, 0xE]

Step 2: Build MPT structure

  ┌─ MPT Branch (hi nibbles) ─────────────────────────┐
  │                                                     │
  │ [0] [1]        [2]        [3]        [4]...[F]     │
  │      │          │          │          (empty)       │
  │      ▼          ▼          ▼                        │
  │  lo-branch   extension   lo-branch                  │
  │  ┌──┬──┐    (single C)   ┌──┬──┐                   │
  │  A   B                   D   E                      │
  │  ↓   ↓        ↓          ↓   ↓                     │
  │  child child  child      child child                │
  └─────────────────────────────────────────────────────┘
```

### Single-child optimization

When a hart node has only 1 child, both nibbles become an extension
prefix that accumulates as we descend:

```
Hart:                          MPT equivalent:
  Node4[0xAB] (1 child)
    └─ Node4[0xCD] (1 child)    extension[A,B,C,D]
        └─ Node4[0xEF] (2 ch)     └─ branch
            ├─ [0x12] Leaf              ├─ ... 
            └─ [0x34] Leaf              └─ ...

Without optimization:
  branch[A] → branch[B] → branch[C] → branch[D] → branch[E] → ...
  (6 MPT levels, 6 hash computations)

With optimization:
  extension[A,B,C,D] → branch[E] → ...
  (2 MPT levels, 2 hash computations)
```

### Cases handled by hash_ref

```
Case 1: Single child total (nchildren == 1)
  → Accumulate both nibbles as prefix, recurse deeper
  → No MPT node emitted at this level

Case 2: All children share same high nibble (hi_occupied == 1)
  → Accumulate high nibble as prefix
  → If single lo child: accumulate lo nibble too, recurse
  → If multiple lo children: emit lo-level MPT branch

Case 3: Multiple high nibbles (hi_occupied > 1)
  → Emit hi-level MPT branch (up to 16 slots)
  → Each hi slot: if 1 lo child → extension + recurse
                   if N lo children → lo-level branch
  → Cache result hash in node, clear dirty flag
```

### MPT node types produced

```
Leaf node:        [hex_prefix(remaining_path, leaf=true), encoded_value]
Extension node:   [hex_prefix(shared_path, leaf=false), child_hash]
Branch node:      [child_0, child_1, ..., child_15, value]

Where child_i is:
  - empty (0x80) if no child at nibble i
  - inline RLP if child encoding < 32 bytes
  - keccak256(child_encoding) if >= 32 bytes (hash reference)
```

### Full example: 3 keys

```
Keys (keccak hashes, showing first 2 bytes):
  K1 = 0xa73f...
  K2 = 0xa781...
  K3 = 0xb210...

Hart tree:
  Root (Node4, keys=[0xa7, 0xb2])
  ├── [0xa7] → Node4 (keys=[0x3f, 0x81])
  │            ├── [0x3f] → Leaf(K1, val1)
  │            └── [0x81] → Leaf(K2, val2)
  └── [0xb2] → Node4 (keys=[0x10])
               └── [0x10] → Leaf(K3, val3)

MPT hash computation (hash_ref walk):

  Root: 2 children → hi nibbles [0xa, 0xb]
  ┌─────────────────────────────────────────────────────────┐
  │  MPT Branch (hi=a, hi=b)                                │
  │                                                         │
  │  [a] → extension[7] → (hash child at depth 1)          │
  │        child 0xa7 has 2 children:                       │
  │          hi=3: lo=f → Leaf(K1) with path suffix         │
  │          hi=8: lo=1 → Leaf(K2) with path suffix         │
  │        → MPT branch[3,8]                                │
  │          [3] → extension[f] → leaf(K1)                  │
  │          [8] → extension[1] → leaf(K2)                  │
  │                                                         │
  │  [b] → extension[2,1,0] → leaf(K3) with path suffix    │
  │        (single child chain: 0xb2→0x10→leaf collapsed)   │
  └─────────────────────────────────────────────────────────┘

  Final root = keccak256(RLP(branch_node)) or inline if < 32 bytes
```

---

## Part 4: Usage in Ethereum State

### Two hart instances

```
┌─────────────────────────────────────────────────────────────┐
│  state_t                                                     │
│                                                              │
│  Account Index Hart (acct_index)                             │
│  ┌─────────────────────────────────────────────┐            │
│  │ key:   keccak256(address)     [32 bytes]     │            │
│  │ value: index into accounts[]  [4 bytes]      │            │
│  │                                              │            │
│  │ One hart for ALL accounts (~30M entries)      │            │
│  │ Arena: ~4GB at scale                          │            │
│  └─────────────────────────────────────────────┘            │
│                                                              │
│  Per-Account Storage Harts                                   │
│  ┌──────────────────────────────────────────┐               │
│  │ key:   keccak256(slot_number) [32 bytes]  │  × N accounts │
│  │ value: slot_value             [32 bytes]  │  (with storage)│
│  │                                           │               │
│  │ One hart PER account with storage         │               │
│  │ Arena: 1KB initial, grows on demand       │               │
│  └──────────────────────────────────────────┘               │
│                                                              │
│  accounts[] vector        resources[] vector                 │
│  ┌──────────────────┐   ┌───────────────────────┐           │
│  │ addr, nonce, bal  │   │ code_hash, stor_root  │           │
│  │ flags, res_idx ───┼──→│ code*, storage_hart*  │           │
│  └──────────────────┘   └───────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### Root hash computation flow

```
state_compute_root(prune_empty):

  Step 1: For each dirty account
  ┌──────────────────────────────────────────────────────┐
  │  if ACCT_STORAGE_DIRTY:                               │
  │    hart_root_hash(storage_hart) → storage_root       │
  │    (computes storage trie MPT hash)                   │
  └──────────────────────────────────────────────────────┘

  Step 2: Prune dead/empty accounts from acct_index hart
  ┌──────────────────────────────────────────────────────┐
  │  for each dirty account:                              │
  │    if not existed or (empty && prune):                 │
  │      hart_delete(acct_index, keccak(addr))            │
  └──────────────────────────────────────────────────────┘

  Step 3: Compute account trie root
  ┌──────────────────────────────────────────────────────┐
  │  hart_root_hash(acct_index, acct_trie_encode)        │
  │                                                       │
  │  For each dirty leaf, acct_trie_encode builds:        │
  │    RLP([nonce, balance, storage_root, code_hash])     │
  │                                                       │
  │  Returns: 32-byte state root                          │
  └──────────────────────────────────────────────────────┘

  Step 4: Clear dirty flags + blk_dirty list
```

### Incremental update (one block)

```
Block execution modifies accounts A, B, C:

  Before:                          After:
  acct_index hart                  acct_index hart
  ┌───────────────┐               ┌───────────────┐
  │ Root (clean)  │               │ Root (DIRTY)  │
  │  ├─ ... clean │               │  ├─ ... clean │
  │  ├─ path to A │               │  ├─ path to A │ DIRTY
  │  │   (clean)  │               │  │   (dirty)  │
  │  ├─ path to B │               │  ├─ path to B │ DIRTY
  │  │   (clean)  │               │  │   (dirty)  │
  │  ├─ path to C │               │  ├─ path to C │ DIRTY
  │  │   (clean)  │               │  │   (dirty)  │
  │  └─ ... clean │               │  └─ ... clean │
  └───────────────┘               └───────────────┘

  Root hash:
    - Visit root (dirty) → must recompute
    - Visit A's path (dirty) → recompute + re-encode leaf
    - Visit B's path (dirty) → recompute + re-encode leaf
    - Visit C's path (dirty) → recompute + re-encode leaf
    - All other paths (clean) → return cached hash[32]
    - Total: ~15 nodes rehashed out of ~millions
```

---

## Part 5: Memory Profile

### Per-entry cost at different scales

```
Entries     Arena Size    Bytes/Entry    Dominant Node Type
──────────────────────────────────────────────────────────
    1,000      128 KB        131        Node4 (sparse)
   10,000        1 MB        105        Node4/Node16 mix
  100,000       16 MB        168        Node16/Node48
1,000,000      128 MB        134        Node48/Node256
5,000,000      512 MB        107        Node256 (dense top)
30,000,000       4 GB        143        Node256 (very dense top)
```

### Arena waste

Deleted nodes leave holes. Insert a key, delete it — the leaf and any
created nodes remain in the arena as dead space. Only compaction reclaims:

```
Before compaction:
┌──────┬──────┬──dead──┬──────┬──dead──┬──────┬─── free ──┐
│ Node │ Leaf │ (hole) │ Node │ (hole) │ Leaf │            │
└──────┴──────┴────────┴──────┴────────┴──────┴────────────┘
                  ↑                ↑
               deleted          deleted      arena_used ──→

After compaction (rebuild from scratch):
┌──────┬──────┬──────┬──────┬─────────── free ─────────────┐
│ Node │ Leaf │ Node │ Leaf │                               │
└──────┴──────┴──────┴──────┴───────────────────────────────┘
                              arena_used ──→  (much smaller)
```
