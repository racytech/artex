# Verkle State — TODO

## Done

- [x] **Banderwagon curve** — twisted Edwards arithmetic over blst_fr
  - `banderwagon.h/c` — add, double, neg, scalar_mul, MSM, serialize/deserialize, map_to_field
  - Pippenger bucket MSM (~5-10x over naive for 256 points)

- [x] **Pedersen commitment** — CRS generation + vector commitment
  - `pedersen.h/c` — 256-point CRS, commit (MSM), update (O(1) delta), scalar_diff

- [x] **Verkle tree** — in-memory width-256 trie with Pedersen commitments
  - `verkle.h/c` — insert, get, leaf/internal commitment computation
  - Incremental commitment updates: O(1) per level via pedersen_update

- [x] **Key derivation** — pedersen hash for address/storage → tree key mapping
  - `verkle_key.h/c` — stem = map_to_field(commit([domain, addr, tree_idx_lo, tree_idx_hi]))[0:31]
  - Account header (tree_index=0): version/balance/nonce/code_hash/code_size at suffixes 0-4
  - Storage: tree_index = (slot >> 8) + 1, sub_index = slot & 0xFF

- [x] **Verkle state** — typed account/storage interface over verkle tree
  - `verkle_state.h/c` — get/set version, nonce, balance, code_hash, code_size, storage

- [x] **Commitment store** — hash_store-backed persistence for commitment points
  - `verkle_commit_store.h/c` — persist/load leaf (C1,C2,commit) and internal commitments
  - Self-contained hash_store copy in state/ (no database/ dependency)

- [x] **Code chunking** — store contract bytecode in the verkle tree
  - 32-byte chunks with domain-separated keys (domain 3, no EIP-4762 push_offset — new blockchain)
  - `verkle_code_chunk_key()` in `verkle_key.h/c` — tree_index = chunk_id/256, sub_index = chunk_id%256
  - `verkle_state_set_code()` / `verkle_state_get_code()` in `verkle_state.h/c`
  - Code hash is caller's responsibility (no keccak dependency)

- [ ] **IPA proofs** — inner product argument over banderwagon
  - Generate multipoint opening proofs against the tree
  - Verify proofs (stateless client path)
  - Required primitives: transcript (Fiat-Shamir), IPA prover/verifier, multiproof aggregation

- [x] **Tree snapshot** — binary serialization/deserialization of entire verkle tree
  - `verkle_snapshot.h/c` — save/load full tree (structure + values + commitments) to binary file
  - Sparse encoding: bitmaps for set values/children, only non-NULL entries written
  - Commitments preserved on load — no recomputation needed
  - Format: 16B header ("VRKLSNAP" + version) + recursive node encoding

- [ ] **Flat commitment updater** — process sorted changes without full tree in memory
  - Group block's changes by stem, look up old commits from store, compute deltas
  - No tree traversal needed: sorted key processing + point lookups into hash_store
  - See VERKLE_TREE.md "Tree vs Flat Storage" section

- [x] **State journal** — block revert + crash recovery + background checkpoint
  - `verkle_journal.h/c` — in-memory undo journal (records old values, replays in reverse)
  - `verkle_unset()` in `verkle.h/c` — clears key, removes empty leaves, collapses single-child internals
  - Forward journal file: appends new values to disk (no fsync during execution)
  - Background checkpoint: `fork()` for consistent CoW snapshot, parent continues executing
  - Recovery: load last snapshot + replay forward journal

- [x] **Benchmarking** — verkle state scale test (block execution simulation)
  - `bench_verkle_state.c` — parameterized: accounts_K, blocks, checkpoint_interval
  - Realistic op mix per block (~500 ops): balance 40%, nonce 20%, storage 25%, new account 10%, code deploy 5%
  - Full stack: verkle_state → verkle_journal → verkle_snapshot (checkpoint + recovery)
  - Metrics: per-block exec/commit latency, RSS, snapshot/journal sizes, latency percentiles (p50/p95/p99)
  - Recovery verification: snapshot load + journal replay + root hash comparison
