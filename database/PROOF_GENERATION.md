# Proof Generation: State Roots and Merkle Proofs

## Problem

compact_art + state.dat is a flat key-value store optimized for speed.
It has no cryptographic structure. But Ethereum requires:

1. **State root** — 32-byte MPT hash in every block header
2. **Storage roots** — per-contract MPT hash in each account object
3. **Merkle proofs** — for `eth_getProof`, light clients, bridges

ART is not a Merkle tree. We need a separate mechanism to produce
these commitments.

## Architecture: Separate Hash Computation

Keep compact_art for fast reads/writes. Compute MPT hashes as a
separate structure:

```
compact_art + state.dat          MPT Hash Tree
(data — fast reads/writes)       (hashes only — for proofs)
┌────────────────────┐           ┌────────────────────┐
│ key[32] → value    │           │ prefix → hash[32]  │
│ 60 B/key           │           │ incremental update │
│ flat, fast         │           │ per-block           │
└────────────────────┘           └────────────────────┘
         │                                │
         └──── both needed for ───────────┘
              state_root + eth_getProof
```

This is the approach used by Erigon (the most storage-efficient
Ethereum execution client). The data store and the commitment
scheme are fully decoupled.

## MPT Hash Tree

### What It Stores

Only intermediate 32-byte hashes at each branch point of the Modified
Merkle Patricia Trie. No key-value data — that lives in compact_art
and state.dat.

Each entry is a trie node hash keyed by its path prefix:

```
Entry: path_prefix (variable, 0-64 nibbles) → hash[32]
```

MPT has three node types, each producing a 32-byte hash:
- **Branch node**: 16 children + optional value → keccak256(RLP(...))
- **Extension node**: shared nibble path + child hash → keccak256(RLP(...))
- **Leaf node**: remaining nibble path + value → keccak256(RLP(...))

### Memory Budget

At 500M keys, the MPT has roughly:
- 500M leaf nodes (one per key)
- ~30M branch nodes (256-ary branching thins out fast)
- ~10M extension nodes (path compression)

Each intermediate node needs: prefix (avg ~4 bytes) + hash (32 bytes)
≈ 36-40 bytes. Total: ~40M intermediate nodes × 40 bytes ≈ 1.6 GB.

Leaf hashes can be computed on-the-fly from state.dat values (no need
to store them). So the hash tree overhead is ~1.6 GB for 500M keys,
much less than the compact_art index itself (~30 GB).

At 1B keys: ~3-4 GB for the hash tree. Manageable.

## Per-Block Hash Update

After each block executes, the write buffer contains ~5K-50K dirty keys.
Only the MPT paths from those keys to the root need recomputation:

```
Block N executes:
  dirty_keys = {key1, key2, ..., keyM}   (M ≈ 5K-50K)

For each dirty_key:
  1. Find the MPT leaf for this key
  2. Recompute leaf hash from new value
  3. Walk up to root, recomputing branch/extension hashes

state_root = root hash after all dirty paths updated
```

### Cost Analysis

Each dirty key touches O(log₁₆(N)) = O(depth) MPT nodes on the path
to the root. For 500M keys, MPT depth ≈ 8-10 levels (16-ary branching).

Per-block cost:
- 50K dirty keys × 10 hash computations × ~200ns per keccak256
- = 50K × 10 × 200ns = 100ms

This is fast enough to compute per-block without buffering. The hash
update runs after block execution, before the state root is needed
for the block header.

### Ordering Optimization

Sort dirty keys before updating the hash tree. Adjacent keys share
upper MPT nodes, so sorting reduces redundant hash recomputations:

```
Without sorting: each dirty key recomputes root → O(M × depth) hashes
With sorting:    shared prefixes computed once → O(M × branch_factor) hashes

For 50K sorted keys with depth 10:
  ~50K unique leaf hashes + ~5K branch recomputations ≈ 55K hashes
  vs. 500K hashes without sorting (10x reduction)
```

## Proof Generation (eth_getProof)

To generate a Merkle proof for a given key:

```
1. Walk MPT hash tree from root, collecting sibling hashes at each level
2. At the leaf: fetch value from compact_art → state.dat
3. Assemble proof: [branch_node_0, ..., branch_node_N, leaf_node]
4. Return: { proof: [...], value: ..., storageRoot: ... }
```

Each proof is a list of RLP-encoded MPT nodes. Typical size:
~8-10 nodes × ~532 bytes (branch node) ≈ 3-5 KB per proof.

The hash tree provides all intermediate hashes. The leaf value comes
from the data layer (compact_art + state.dat). No redundant data
stored.

## Storage Tries

Each contract has its own storage trie mapping `keccak256(slot)` →
`RLP(value)`. The storage root goes into the account object.

Two approaches:

### A. One Global MPT Hash Tree (simpler)

Flatten all storage into the global keyspace with a prefix:

```
State trie key:   keccak256(address)
Storage trie key: keccak256(address) || keccak256(slot)
```

The MPT hash tree handles both. Storage roots are intermediate hashes
at the address prefix boundary. One tree to maintain, one update path.

### B. Per-Contract Hash Trees (protocol-accurate)

Maintain separate MPT hash trees per contract. More closely matches
the Ethereum spec (each account has its own storageRoot). But requires
managing potentially millions of small trees.

**Recommendation**: Start with approach A. The global keyspace is simpler
and compact_art already handles 64-byte composite keys efficiently
(32B address + 32B slot). The MPT hash tree can derive storage roots
from the structure.

## Crash Recovery for Hash Tree

The hash tree is deterministic — it can always be recomputed from the
flat state. So crash recovery does not require persisting the hash tree
separately:

```
On crash:
  1. Recover compact_art + state.dat from last checkpoint
  2. Re-execute K blocks (rebuilds write buffer)
  3. Rebuild hash tree from compact_art (full walk, one-time cost)
  4. Resume normal operation
```

Rebuilding the hash tree from 500M keys takes ~30-60 seconds (sequential
iteration + hashing). This is acceptable for a cold start event.

For faster restarts, optionally persist the hash tree to a file at
checkpoint time. On recovery, load it instead of recomputing.

## Verkle Trees (Future)

Ethereum is moving to Verkle trees (EIP-6800). Verkle replaces Merkle
hashing with polynomial commitments (IPA or KZG):

- 256-ary branching (vs MPT's 16-ary)
- Proofs: ~150 bytes (vs MPT's ~3-5 KB)
- Incremental updates are cheaper

ART's structure maps naturally to Verkle because both use byte-level
(256-ary) branching:

```
ART Node256              Verkle Internal Node
┌────────────────┐       ┌────────────────┐
│ children[256]  │  ←→   │ commitments[256]│
│ byte-indexed   │       │ byte-indexed    │
└────────────────┘       └────────────────┘
```

A Verkle commitment layer could sit on top of compact_art the same way
the MPT hash tree does — as a separate structure updated incrementally
after each block. The data layer (compact_art + state.dat) stays the
same. Only the commitment module changes.

### Design for Swappability

Abstract the commitment interface:

```c
typedef struct {
    // Update commitments for a set of changed keys
    void (*update)(commitment_t *c, const uint8_t **keys,
                   const void **values, size_t count);

    // Get the root commitment (state root)
    void (*root)(commitment_t *c, uint8_t out[32]);

    // Generate proof for a key
    void (*prove)(commitment_t *c, const uint8_t *key,
                  uint8_t **proof, size_t *proof_len);

    // Verify a proof
    bool (*verify)(const uint8_t root[32], const uint8_t *key,
                   const void *value, const uint8_t *proof, size_t proof_len);
} commitment_ops_t;
```

Implement MPT first (required for current Ethereum). Swap to Verkle
when the protocol transitions. The data layer is unaffected.

## Implementation Priority

```
Priority   Component              Effort    Notes
────────────────────────────────────────────────────────────
1          MPT hash tree struct   Medium    In-memory, incremental
2          Per-block update       Medium    Sorted dirty keys → hash walk
3          State root compute     Low       Root of hash tree
4          eth_getProof           Medium    Walk hash tree + fetch leaf
5          Persistence (optional) Low       Dump/load at checkpoint
6          Verkle commitment      High      Future, same interface
```

## Files

| File | Type | Description |
|------|------|-------------|
| `mpt_hash.h` | Header | MPT hash tree API (TODO) |
| `mpt_hash.c` | Source | Incremental hash computation (TODO) |
| `commitment.h` | Header | Abstract commitment interface (TODO) |
