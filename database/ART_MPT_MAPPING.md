# ART-to-MPT Mapping — Why It's Not a Simple Embedding

## The Appealing Idea

Both ART and MPT are radix trees over the same keys. The tempting design: attach
a 32-byte hash to each compact_art inner node, mark dirty on writes, recompute
bottom-up. No separate hash table. Minimal memory overhead.

This document explains why that doesn't quite work and what the real options are.

## The Two Trees

### ART (compact_art)

```
Branching:     byte-level (256-way)
Path compress: partial key bytes (up to COMPACT_MAX_PREFIX=8 stored inline)
Key depth:     32 bytes → max 32 levels (but ~4-5 with path compression)
Node types:    Node4, Node16, Node32, Node48, Node256 (by child count)
Leaves:        store full 32-byte key + 4-byte value ref
```

### MPT (Ethereum Merkle Patricia Trie)

```
Branching:     nibble-level (16-way)
Path compress: extension nodes (shared nibble prefix)
Key depth:     64 nibbles → max 64 levels
Node types:    Branch (16 children + value), Extension (path + child), Leaf (path + value)
Hash:          keccak256(RLP(node))
```

## The Fundamental Mismatch

One ART byte = two MPT nibbles.

```
ART byte 0x3a  →  MPT high nibble 3, low nibble a
                  (two levels of 16-way branching)
```

An ART node with children at bytes [0x3a, 0x3b, 0x7f] maps to:

```
ART level:                    MPT equivalent:

Node with 3 children          Branch (high nibble level)
  [0x3a] → subtree_a            [3] → Branch (low nibble level)
  [0x3b] → subtree_b                    [a] → subtree_a
  [0x7f] → subtree_c                    [b] → subtree_b
                                 [7] → ext([f]) → subtree_c
```

Children 0x3a and 0x3b share high nibble 3, so they become siblings in an inner
branch at the low nibble level. Child 0x7f is alone in high nibble 7, so the
low nibble level collapses to an extension.

**One ART branching level always produces two MPT levels.**

## Byte-to-Nibble Decomposition

Given an ART node's children (byte-indexed), the MPT structure is:

```
1. Group children by high nibble (byte >> 4)
2. For each high nibble group:
   - Multiple low nibbles → MPT branch node (16-way)
   - Single low nibble → collapses to extension [low_nibble]
3. High nibble level:
   - Multiple groups → MPT branch node (16-way)
   - Single group → collapses to extension [high_nibble]
```

### Worked example

ART Node256 at byte depth 0:

```
children: [0x11, 0x12, 0x13, 0x3a, 0x3f, 0x70]
```

High nibble groups:

```
group 1: low nibbles [1, 2, 3]  → MPT branch(children at 1,2,3)
group 3: low nibbles [a, f]     → MPT branch(children at a,f)
group 7: low nibbles [0]        → collapses to extension [0]
```

High nibble level: 3 groups → MPT branch(children at 1,3,7)

Full MPT:

```
Branch [1, 3, 7]
  [1] → Branch [1, 2, 3]
          [1] → subtree_0x11
          [2] → subtree_0x12
          [3] → subtree_0x13
  [3] → Branch [a, f]
          [a] → subtree_0x3a
          [f] → subtree_0x3f
  [7] → ext([0]) → subtree_0x70
```

## The Extension Merging Problem

Here's where embedding hashes in ART nodes breaks down.

### Setup

```
ART:
  Root (partial=[]) → Node256
    child[0x3a] → Node4 (partial=[0x01, 0x22])
      child[0x55] → Leaf(v1)
      child[0x66] → Leaf(v2)
```

### What happens if we cache one hash per ART node

1. **Hash Node4 subtree:**
   - Partial key [0x01, 0x22] → nibbles [0, 1, 2, 2] → extension
   - Children {0x55, 0x66}:
     - Group 5: low [5] → ext([5]), Group 6: low [6] → ext([6])
     - High level: 2 groups → branch [5, 6]
   - Result: `ext([0,1,2,2]) → branch[5,6] → ...`
   - Cache this as `node4_hash`

2. **Hash Root subtree:**
   - Child at byte 0x3a: high=3, low=a
   - 0x3a is the only child, so:
     - Low level: single child → ext([a])
     - High level: single group → ext([3])
     - Combined: ext([3, a])
   - Now compose: `ext([3, a]) → node4_hash`
   - But `node4_hash` = hash of `ext([0,1,2,2]) → branch → ...`
   - So the full MPT is: `ext([3, a]) → ext([0,1,2,2]) → branch → ...`

### The problem

**Two adjacent extensions are invalid in canonical MPT.** They must merge:

```
WRONG:   ext([3, a]) → ext([0, 1, 2, 2]) → branch → ...
CORRECT: ext([3, a, 0, 1, 2, 2]) → branch → ...
```

These produce **different hashes** because the RLP encoding differs.

To merge correctly, the parent can't just use the child's cached hash. It needs
to know that the child starts with an extension, extract the prefix, merge it
with its own prefix, and re-hash. This defeats the purpose of caching.

### When does this happen?

Extension merging occurs when a path has **no branching** — a single child at
each level. This happens:

- **Always at sparse depths**: At byte depth 4+ for 500M keys, subtrees have
  0-1 keys, so ART nodes are Node4 with 1 child and long partial keys.
- **Rarely at dense depths**: At byte depth 0-2, virtually all 256 byte values
  are populated. Both nibble levels have multiple children, no collapsing occurs.

### Why this is fatal for caching

The sparse depths are exactly where most ART nodes live (millions of Node4s
near the leaves). These are the nodes where per-node hash caching would save
the most work, but they're also the ones where extension merging makes it
incorrect.

## What Does Work

### Option A: Separate nibble-level hash table

Store intermediate hashes keyed by nibble prefix, completely independent of
ART structure. The ART provides:
- Key-value storage (read values for leaf hashing)
- Ordered iteration (for streaming hash computation)

The hash table provides:
- Cached MPT branch hashes at every nibble depth
- No extension merging issue (hashes are stored at the correct granularity)

See `INTERMEDIATE_HASHES.md` for the full design.

```
ART:            account key → state.dat ref  (data store)
Hash table:     nibble prefix → 32-byte hash (commitment layer)
```

### Option B: Two hashes per ART node (inner + prefix length)

Store in each ART node:
- `inner_hash[32]`: hash of the MPT subtree EXCLUDING the partial key extension
- `prefix_nibbles_len`: how many nibbles the partial key contributes

The parent then:
1. Gets child's `inner_hash` and `prefix_nibbles_len`
2. Knows the byte that led to this child → 2 nibbles
3. Detects single-child paths and merges nibble prefixes
4. Builds the extension with the merged prefix → child's `inner_hash`

This works but has complexity:
- Every hash computation must handle extension construction and merging
- The parent still needs to decompose bytes to nibbles and detect collapsing
- When multiple ART levels collapse (rare but possible), merging chains

### Option C: Hybrid — ART hashes for fast dirty tracking, nibble hashes for correctness

Use ART's tree structure for dirty propagation only:
- When a leaf changes during merge, walk up ART ancestors, set dirty bit
- When computing root, only visit dirty ART subtrees
- At each dirty ART subtree, recompute the nibble-level MPT hash from scratch

This gives O(dirty) performance without the extension merging problem, because
we recompute the full nibble structure for dirty subtrees rather than composing
cached hashes.

Cost per dirty ART subtree: ~16 children × 2 nibble levels × keccak256 ≈ 32
hash operations ≈ 6.4μs. For 50K dirty keys touching ~5K distinct ART inner
nodes: 5K × 6.4μs ≈ 32ms. Acceptable.

## Recommendation

**Option A (separate hash table) for correctness and simplicity.** The ART byte
structure and MPT nibble structure are fundamentally different granularities.
Trying to shoehorn one into the other creates edge cases around extension
merging that are hard to get right and hard to test.

The streaming algorithm from `INTERMEDIATE_HASHES.md` processes sorted dirty
keys with a 36 KB stack and directly produces correct MPT hashes. The separate
hash table costs ~2.6–4.6 GB depending on implementation, but this is a one-time
cost that doesn't grow with the number of blocks.

**Option C is worth revisiting** if we find the separate hash table's memory
overhead is too high. It uses ART's existing dirty tracking infrastructure
(adding only a dirty bit per node, ~0 extra memory) and recomputes nibble-level
hashes locally for dirty subtrees.

## Appendix: ART Shape for 32-byte Keccak256 Keys

For 500M uniformly distributed 32-byte keys:

```
Byte depth 0:   1 root node (Node256, all 256 children)
Byte depth 1:   256 nodes (Node256, ~2M keys each)
Byte depth 2:   65K nodes (Node256 or Node48, ~7600 keys each)
Byte depth 3:   16M nodes (Node16 or Node4, ~30 keys each)
Byte depth 4:   ~500M leaves (most subtrees have 0-1 keys)
                + ~400M Node4 with partial keys covering bytes 4-31
```

Total inner nodes: ~16.5M
Total leaves: 500M
Average path: root → 3-4 inner nodes → leaf

At depths 0-2 (dense): all 256 bytes populated, no nibble-level collapsing.
Byte-to-nibble decomposition produces real 16-way branches at both levels.

At depth 3+ (sparse): mostly 1-4 children, frequent collapsing. Extension
merging is the norm, not the exception. This is where the embedding approach
fails.

## Appendix: MPT Hash Rules Reference

```
Leaf hash:      keccak256(RLP([hex_prefix(path, leaf=true), value]))
Extension hash: keccak256(RLP([hex_prefix(path, leaf=false), child_ref]))
Branch hash:    keccak256(RLP([ref_0, ..., ref_15, value]))
Empty trie:     keccak256(0x80) = 0x56e81f171b...

Embedding:      if len(RLP(node)) < 32, parent embeds raw RLP instead of hash
Root:           always hashed regardless of RLP size

Hex-prefix:     odd length  → byte 0 = (flag|1)<<4 | first_nibble, then pairs
                even length → byte 0 = flag<<4 | 0, then pairs
                flag: 0=extension, 2=leaf
```
