# Verkle Tree Structure

## Overview

A verkle tree is a width-256 trie where every node commits to its children
using Pedersen commitments over the Banderwagon curve. The "verkle" name
comes from "vector commitment" + "merkle".

```
                    Root (internal)
                   /    |    \
              [0] /  [1]|  [2]\ ...  [255]
                /      |      \
           Internal  Internal  Leaf
           /  |  \
         ...........
```

Each internal node has up to 256 children (indexed 0-255).
Leaf nodes hold up to 256 values.

## Key Structure

Every key in the verkle tree is 32 bytes. It splits into:

```
|<---------- 31 bytes (stem) ---------->|<- 1 byte (suffix) ->|

 byte 0    byte 1    ...    byte 30         byte 31
[  s_0  ] [  s_1  ] ... [  s_30  ]       [  suffix  ]
```

- **Stem** (bytes 0-30): determines which LEAF NODE you land on.
  The stem is the "address" — all 256 values sharing a stem live
  in the same leaf.

- **Suffix** (byte 31): determines which SLOT (0-255) within that leaf.

### Example: Ethereum account storage

For an account at address `0xABCD...`, all its state lives under stems
that share a common prefix. The suffix selects which piece of state:

```
stem = hash(address, tree_index)     suffix = sub_index

suffix 0 = version
suffix 1 = balance
suffix 2 = nonce
suffix 3 = code_hash
suffix 4 = code_size
suffix 64-127 = first 64 code chunks
suffix 128-255 = storage slots
```

The key insight: ONE leaf node holds an entire account's basic state
(version, balance, nonce, code_hash, code_size) plus some code and
storage. This is much more efficient than MPT where each field is a
separate trie path.

## Tree Traversal

To look up a key, we consume the stem one byte at a time:

```
Key: [0x03][0xA1][0x7F]...[0x42]  (31 stem bytes + 1 suffix)
      |     |     |          |
      |     |     |          +-- suffix: slot 0x42 in the leaf
      |     |     +-- byte 2 of stem: child index at depth 2
      |     +-- byte 1 of stem: child index at depth 1
      +-- byte 0 of stem: child index at depth 0 (root level)

Root
  |
  +--[0x03]--> Internal (depth 1)
                  |
                  +--[0xA1]--> Internal (depth 2)
                                  |
                                  +--[0x7F]--> ... --> Leaf
                                                        |
                                                        +--[0x42]--> value
```

Maximum depth = 31 (one level per stem byte), but in practice the tree
is much shallower because stems share prefixes and are compressed.

## Node Types

### Internal Node

An internal node has 256 child slots. Each slot either:
- Points to another internal node
- Points to a leaf node
- Is empty (commitment contribution = 0)

```
Internal Node
+-----+-----+-----+-----+     +-------+
|  0  |  1  |  2  |  3  | ... |  255  |
+-----+-----+-----+-----+     +-------+
  |           |                    |
  v           v                    v
 Leaf      Internal              empty
```

**Commitment:**
```
C_internal = sum( map_to_field(child_commitment[i]) * G_i )  for i=0..255
```

Where `map_to_field()` converts a Banderwagon point to a scalar (X/Y mod r),
and `G_i` are the 256 CRS points.

Empty children contribute 0 (zero scalar * G_i = identity, adds nothing).

### Leaf Node

A leaf node stores up to 256 values (32 bytes each), addressed by suffix.
It also stores its stem (31 bytes).

```
Leaf Node
+-----------+-----------------------------------------------+
| stem (31B)| values[0] values[1] ... values[127] values[128] ... values[255] |
+-----------+-----------------------------------------------+
              \_____________ _______________/ \_____________ _______________/
                            v                               v
                     C1 (lower half)                  C2 (upper half)
```

The values are committed in two halves because each 32-byte value must
fit in the 253-bit scalar field. Each value is split:

```
value[i] = 32 bytes
         = [ upper 16 bytes ] [ lower 16 bytes ]

In the commitment vector:
  slot 2*i     = lower 16 bytes (as 128-bit LE integer)
  slot 2*i + 1 = upper 16 bytes (as 128-bit LE integer)
```

16 bytes = 128 bits, which fits comfortably in the 253-bit scalar field.

**C1** commits values[0..127] (256 scalar slots from 128 values):
```
C1 = sum( split_value(values[i])[j] * G_{2*i+j} )  for i=0..127, j=0..1
```

**C2** commits values[128..255] (same structure):
```
C2 = sum( split_value(values[i])[j] * G_{2*(i-128)+j} )  for i=128..255, j=0..1
```

**Leaf commitment** combines stem, C1, C2 with a marker:
```
C_leaf = 1 * G_0              (marker: "this is a leaf")
       + stem_as_scalar * G_1 (the 31-byte stem, LE, as a scalar)
       + map_to_field(C1) * G_2
       + map_to_field(C2) * G_3
```

Only 4 terms! The rest of the 256 CRS slots are unused (zero) in the
leaf commitment formula.

## Commitment Propagation

When a value changes, commitments update bottom-up:

```
1. Value changes in leaf at suffix s
   |
   v
2. Recompute C1 or C2 (depending on s < 128 or s >= 128)
   - Only the changed slot's contribution changes
   - C1_new = C1_old + delta_value * G_{slot}    <-- O(1) update!
   |
   v
3. Recompute leaf commitment
   - C_leaf_new = C_leaf_old
                + (map(C1_new) - map(C1_old)) * G_2    <-- O(1) update!
   |
   v
4. Propagate to parent internal node
   - C_parent_new = C_parent_old
                   + (map(C_leaf_new) - map(C_leaf_old)) * G_{child_index}
   |
   v
5. Continue up to root
```

At every level, the update is O(1): one scalar multiplication + one point
addition. No need to read siblings. This is the key advantage over MPT.

## Concrete Example

Insert key `[03 A1 7F ... 42]` with value `0x00...01` (the number 1):

```
Step 1: Split key
  stem   = [03 A1 7F ...]  (31 bytes)
  suffix = 0x42 = 66

Step 2: Navigate tree
  Root --[03]--> node1 --[A1]--> node2 --[7F]--> ... --> leaf

Step 3: If leaf doesn't exist, create it with this stem
  leaf.stem = [03 A1 7F ...]
  leaf.values[66] = 0x00...01

Step 4: Compute leaf commitment
  value = 0x00...01
  lower_16 = 0x00...01 (as 128-bit LE)
  upper_16 = 0x00...00

  suffix 66 is in C1 (since 66 < 128)
  C1 contribution at slot 2*66=132: lower_16 * G_132
  C1 contribution at slot 2*66+1=133: upper_16 * G_133 (= 0, identity)

  C1 = 1 * G_132  (only non-zero contribution)

  C_leaf = 1*G_0 + stem_scalar*G_1 + map(C1)*G_2 + map(C2)*G_3
         = 1*G_0 + stem_scalar*G_1 + map(C1)*G_2 + 0*G_3

Step 5: Update parent
  C_parent += map(C_leaf) * G_{child_index}
  Continue up to root.
```

## Update Example

Change value at suffix 66 from 1 to 5:

```
old_value = 0x00...01,  new_value = 0x00...05
old_lower = 1,          new_lower = 5
delta_lower = 5 - 1 = 4

C1_new = C1_old + 4 * G_132        (one scalar mul + one add)

old_C1_scalar = map_to_field(C1_old)
new_C1_scalar = map_to_field(C1_new)
delta_C1 = new_C1_scalar - old_C1_scalar

C_leaf_new = C_leaf_old + delta_C1 * G_2   (one scalar mul + one add)

old_leaf_scalar = map_to_field(C_leaf_old)
new_leaf_scalar = map_to_field(C_leaf_new)
delta_leaf = new_leaf_scalar - old_leaf_scalar

C_parent_new = C_parent_old + delta_leaf * G_{child_index}

... continue to root ...
```

Total work per value change: O(depth) scalar multiplications.
No sibling reads. No rehashing. Just point lookups for the
old commitments at each level.

## Why This Solves Our Cursor Problem

In MPT (intermediate_hashes), computing the state root requires:
- Ordered iteration over all keys (cursor scan)
- Reading sibling hashes to compute parent hashes
- hash_store only supports point lookups, not cursors

In verkle:
- Each update is O(1) at each tree level
- Only need the OLD commitment at each node (point lookup)
- hash_store can store commitments keyed by stem/path
- No ordered iteration needed

This is exactly why we moved from MPT to verkle.

## Tree vs Flat Storage

We don't necessarily need a full tree in memory. Since updates are
additive, we can store just:

```
hash_store: stem -> (C_leaf, C1, C2)           leaf commitments
hash_store: path_prefix -> C_internal           internal commitments
```

For each block's state changes:
1. Group changes by stem
2. For each changed stem: look up old C1/C2, compute deltas, update
3. For each changed internal node: look up old commitment, apply delta
4. Propagate to root

This is the "flat commitment updater" approach — no tree traversal,
just sorted key processing + point lookups into hash_store.
