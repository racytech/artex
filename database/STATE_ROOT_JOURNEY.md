# How We Compute State Root: The Full Journey

## Chapter 1: What Is a State Root?

Ethereum has millions of accounts. Each has a balance, nonce, code, storage.
After every block, Ethereum asks one question:

> "Give me ONE 32-byte hash that represents the ENTIRE state of ALL accounts."

That hash is the **state root**. If even one bit changes in one account,
the state root changes completely. Every block header contains it.

---

## Chapter 2: The Naive Way (Full MPT Trie)

The Ethereum Yellow Paper says: use a **Merkle Patricia Trie (MPT)**.

Think of it like a tree where:
- **Leaves** hold actual data (account balance, storage value)
- **Branches** split into up to 16 children (one per hex nibble: 0-9, a-f)
- **Extensions** are shortcuts when many keys share the same prefix

```
         [Root Hash]
              |
         [Branch: splits by first nibble]
        /    |    \
     0/     3|     a\
      |      |       |
   [Ext]  [Ext]   [Leaf]
     |      |     key=0xa8f3...
  [Branch] [Leaf]  val=42 ETH
   / \     key=0x3c12...
  |   |    val=100 ETH
[Leaf][Leaf]
```

Every node gets hashed (keccak256). Parent hashes include children's hashes.
Change a leaf → its hash changes → parent hash changes → all the way to root.

**How we compute the root:**
1. Build the whole tree in memory (or on disk)
2. Hash every leaf: `keccak256(RLP([key_suffix, value]))`
3. Hash every branch: `keccak256(RLP([child0, child1, ..., child15, value]))`
4. Hash extensions: `keccak256(RLP([prefix, child_hash]))`
5. The root node's hash = state root

**This is what our `mpt.c` does.** It's a full persistent MPT trie backed
by mmap'd files. Two pools: one for internal nodes, one for leaves.

### The Problem

At mainnet scale (~200M accounts, ~1B storage slots):
- The full trie takes **many gigabytes** of RAM/disk
- Each leaf stores the full value → **duplicates** the data layer
- `mpt_commit()` does `fdatasync()` → **4 extra fsyncs per block**
- The trie grows without bound (deleted nodes waste space until recycled)

We already store account/storage data in `hash_store` (our data layer).
The MPT trie stores the **same data again**, just arranged as a tree.

---

## Chapter 3: Erigon's Insight — Intermediate Hashes

Erigon (an Ethereum client) asked: **do we really need the full trie?**

The key insight: to compute the state root, you DON'T need the full tree.
You only need the **hashes at branch points**. Leaf data can be read
from the data store on demand.

Think of it this way. Imagine a library with 1000 books:

```
Full trie approach:
  - Make a COPY of every book
  - Arrange copies in a special tree
  - Hash the tree to get the root

Intermediate hashes approach:
  - DON'T copy the books
  - Just remember the HASHES at each shelf/section boundary
  - When computing root, read book data from the original shelf
```

### What We Store

Instead of a full trie, we store a **flat table** of intermediate hashes:

```
Key: nibble prefix (e.g., [3, a, 7] = "branch at position 3a7")
Value: [bitmap, branch_hash, child_depths]
  - bitmap:      which of the 16 children exist (2 bytes)
  - branch_hash: the keccak256 of this branch node (33 bytes)
  - child_depths: how deep each child's branch is (16 bytes)
```

That's 51 bytes per branch node. At mainnet scale:
- ~20M branch entries × 51 bytes ≈ **1 GB** of hashes
- vs multi-GB for the full trie with duplicated data

### How `ih_build` Works (Initial Sync)

Given ALL keys sorted, build the hash table from scratch:

```
Sorted keys:   [0x1a3f..., 0x1a72..., 0x3c01..., 0x3c8e..., 0xa811...]
                 ↓          ↓          ↓          ↓          ↓

Step 1: Keys starting with 0x1a share prefix [1,a] → branch at depth 2
        Hash children (leaves 0x1a3f... and 0x1a72...)
        Store: prefix=[1,a] → {bitmap, hash, child_depths}

Step 2: Keys starting with 0x3c share prefix [3,c] → branch at depth 2
        Hash children (leaves 0x3c01... and 0x3c8e...)
        Store: prefix=[3,c] → {bitmap, hash, child_depths}

Step 3: Root level: nibbles 1, 3, a have children → branch at depth 0
        Children: [1]→extension+branch, [3]→extension+branch, [a]→leaf
        Store: prefix=[] → {bitmap, hash, child_depths}

Root hash = hash of the root branch node
```

This is what `ih_build()` / `ihs_build()` does. Recursive, bottom-up.

### How `ih_update` Works (Per Block)

Each block changes ~2000 keys. We don't rebuild everything.
Only recompute the branches that contain dirty keys:

```
Block changes keys: 0x3c01... (update), 0xa811... (delete), 0xf2... (insert)

                    [Root Branch]  ← DIRTY (children changed)
                   /      |      \
                [1,a]   [3,c]   [a,8]
                clean   DIRTY   DIRTY
                        /   \      \
                    [3c01] [3c8e] [a811]
                    DIRTY  clean  DELETED

Step 1: Recompute branch [3,c]:
        - child [3c01]: DIRTY → read new value from data, hash leaf
        - child [3c8e]: CLEAN → read from cursor*, hash leaf
        - Hash branch [3,c] with both children
        - Store updated entry

Step 2: Recompute branch [a,8]:
        - child [a811]: DELETED → gone
        - Only 0 children left → branch collapses
        - Need to REBUILD this subtree from cursor*

Step 3: Recompute root branch:
        - child [1]: CLEAN → load cached hash from IH table ✓
        - child [3]: was recomputed in step 1 ✓
        - child [a]: was rebuilt in step 2 ✓
        - Hash root branch

New state root = hash of updated root branch
```

**The asterisk (*) on "cursor" is the key issue. Read on.**

---

## Chapter 4: The Cursor Problem

Look at step 1 above. Child `[3c8e]` is CLEAN — it wasn't modified.
But we still need its hash to recompute the parent branch.

For a **leaf** child, the hash depends on the key AND value:
```
leaf_hash = keccak256(RLP([hex_prefix(remaining_nibbles), value]))
```

So we need to READ the value from somewhere. The `ih_update` algorithm
uses a **cursor** — an iterator that can:
- `seek(prefix)` → jump to the first key matching a nibble prefix
- `next()` → move to the next key in sorted order
- `key()` / `value()` → read current entry

### Where Does the Cursor Read From?

The cursor needs the **full post-merge state** — all keys in sorted order,
including both clean keys and dirty keys after they've been applied.

### What Our Data Layer Provides

Our data is in `hash_store` — a zero-RAM mmap'd hash table.
It supports:
- `hash_store_get(key)` → O(1) point lookup ✓
- `hash_store_put(key, val)` → O(1) write ✓
- **NO sorted iteration** ✗
- **NO seek-by-prefix** ✗

Keys are distributed by hash fingerprint, not sorted.
You can't ask "give me all keys starting with 0x3c".

### What We Had Before (Compact ART)

The original `intermediate_hashes.c` uses `compact_art` — an in-memory
sorted tree. It has a full iterator with seek. But it costs ~4.6 GB RAM
at mainnet scale.

### What We Built (ih_hash_store)

`ih_hash_store.c` stores the IH entries in hash_store (zero RAM, persistent).
But it STILL needs a cursor over the DATA to compute leaf hashes for
clean children.

---

## Chapter 5: Where We Are Now

```
WHAT WE HAVE:

  Data Layer (hash_store)          IH Store (ih_hash_store)
  ┌─────────────────────┐         ┌──────────────────────┐
  │ key → value          │         │ nibble_prefix → hash │
  │ (all accounts/slots) │         │ (branch hashes only) │
  │                      │         │                      │
  │ ✓ Point lookup O(1)  │         │ ✓ Point lookup O(1)  │
  │ ✗ No sorted iterate  │         │ ✗ No sorted iterate  │
  │ ✗ No prefix seek     │         │                      │
  └─────────────────────┘         └──────────────────────┘

  EVM Execution (mem_art)
  ┌──────────────────────┐
  │ Dirty keys this block │
  │                       │
  │ ✓ Sorted iteration    │
  │ ✓ Has key + value     │
  │ ✗ Only THIS block     │
  │   (not full database) │
  └───────────────────────┘
```

To call `ihs_update()`, we need:
1. **Sorted dirty keys** → ✅ `mem_art` gives these for free
2. **Cursor over full state** → ❌ `hash_store` can't provide this

The cursor is used for exactly two things:
- **Clean leaf hash**: find a key by prefix → read its value → hash it
- **Subtree rebuild**: collect ALL keys under a prefix (rare, structural changes)

---

## Chapter 6: The Solutions

### Solution A: Store Leaf Hashes in IH

What if we also cached the hash of every LEAF in the IH store?

```
Currently stored:           What we'd add:
  Branch at [3,c]:            Leaf at [3,c,0,1,...]:
    bitmap = 0x0003             leaf_hash = keccak256(...)
    branch_hash = 0xab12...
    child_depths = [0,0,...]  Leaf at [3,c,8,e,...]:
                                leaf_hash = keccak256(...)
```

Then `get_clean_child_ref` for a leaf just returns the stored hash.
No need to read the value. No cursor needed.

**Trade-off**: IH entry count goes from ~20M (branches only) to ~220M
(branches + leaves). At 96 bytes each = ~21 GB on disk. Zero RAM though.

### Solution B: Child Key Store

Store a mapping: nibble_prefix → first_data_key

```
  Child Key Store (another hash_store)
  ┌──────────────────────────────────────┐
  │ nibble_prefix [3,c,0] → 0x3c01...   │
  │ nibble_prefix [3,c,8] → 0x3c8e...   │
  │ nibble_prefix [a,8,1] → 0xa811...   │
  └──────────────────────────────────────┘
```

When we need a clean child's value:
1. Look up child_key_store: prefix → full 32-byte key
2. Look up hash_store: full key → value
3. Compute leaf hash

Two O(1) lookups instead of a cursor seek. No sorted iteration needed.

### Solution C: Sorted Key File

Keep a simple sorted file of all 32-byte keys on disk (mmap'd).
Binary search for seeks. ~6.4 GB at 200M keys, zero RAM.

Provides the cursor directly but needs maintenance (insert/delete on each block).

---

## Chapter 7: Visual Summary

```
EVOLUTION:

Stage 1: Full MPT Trie (current mpt.c)
┌─────────────────────────────────────────────────┐
│  Full trie in memory/disk                       │
│  Every leaf stores key+value (DUPLICATES data)  │
│  4 extra fdatasync per block                    │
│  Multi-GB RAM + disk                            │
│  ✓ Simple: put/delete/root                      │
└─────────────────────────────────────────────────┘
                    ↓ eliminate duplication

Stage 2: Intermediate Hashes + compact_art (ih_build/ih_update)
┌─────────────────────────────────────────────────┐
│  Only branch hashes stored (~1 GB)              │
│  Leaf data read from data layer on demand       │
│  compact_art provides sorted cursor ✓           │
│  BUT: compact_art costs ~4.6 GB RAM ✗           │
└─────────────────────────────────────────────────┘
                    ↓ eliminate RAM

Stage 3: IH + hash_store backend (ihs_build/ihs_update)
┌─────────────────────────────────────────────────┐
│  Branch hashes in hash_store (zero RAM) ✓       │
│  Persistent across restarts ✓                   │
│  BUT: still needs sorted cursor for leaves ✗    │
│  Currently provided by test infrastructure      │
│  Need production cursor implementation          │
└─────────────────────────────────────────────────┘
                    ↓ eliminate cursor need

Stage 4: Cursor-free IH (next step)
┌─────────────────────────────────────────────────┐
│  Branch hashes in hash_store (zero RAM) ✓       │
│  Leaf hashes cached OR child keys indexed ✓     │
│  Only needs: sorted dirty keys (mem_art) ✓      │
│              + point lookups (hash_store) ✓     │
│  No sorted iteration needed ✓                   │
│  Unified checkpoint with data layer ✓           │
└─────────────────────────────────────────────────┘
```
