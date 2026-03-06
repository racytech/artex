# Verkle State — TODO

## Priority — Ethereum Replay (MPT → Verkle)

- [ ] **P0: Full Ethereum state replay** — replay all blocks, build verkle state
  - In-memory tree path: verkle_state → verkle_journal → verkle_snapshot
  - Feed EVM execution results (state changes) into verkle_state per block
  - Checkpoint periodically via verkle_journal (snapshot + forward journal)
  - Goal: compute verkle state root for every Ethereum block

- [ ] **P1: Tree → flat migration** — switch to flat backend when tree outgrows RAM
  - Walk the in-memory tree, export all values + commitments to flat stores
  - Continue replay from that point using flat backend (O(block) RAM)
  - Trigger: when RSS exceeds threshold (e.g. tree at ~2.2 TB for full state)

- [ ] **P2: Leaf splitting in flat updater** — handle stem collisions at same internal depth
  - When a new stem maps to a slot already occupied by a different leaf, must create
    intermediate internal nodes to separate them (same logic as verkle.c tree insertion)
  - Required before flat backend can process arbitrary blocks
  - Affects: `verkle_flat.c` → `process_stem()` / `find_attach_depth()`

## Later — Post-Replay

- [ ] **Unified block execution pipeline** — single entry point for both backends
  - Currently caller must know which path to use and call the right APIs
  - Consider: block executor abstraction that wraps both

- [ ] **Hash store compaction / GC** — reclaim space from deleted entries
  - hash_store grows forever (tombstones on delete, never reclaimed)
  - Matters for long-running nodes after replay is complete

- [ ] **IPA proofs** — inner product argument over banderwagon
  - Generate multipoint opening proofs against the tree
  - Verify proofs (stateless client path)
  - Required primitives: transcript (Fiat-Shamir), IPA prover/verifier, multiproof aggregation

- [ ] **Thread safety** — concurrent read access during block execution

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

- [x] **Verkle state** — backend-agnostic typed account/storage interface
  - `verkle_state.h/c` — tagged union (tree/flat), get/set version, nonce, balance, code_hash, code_size, storage
  - Constructors: `create` (tree), `create_flat` / `open_flat` (disk-backed)
  - Block ops: `begin_block`, `commit_block`, `revert_block`, `sync`
  - Backend accessors: `get_tree`, `get_flat`

- [x] **Commitment store** — hash_store-backed persistence for commitment points
  - `verkle_commit_store.h/c` — persist/load leaf (C1,C2,commit) and internal commitments
  - Self-contained hash_store copy in state/ (no database/ dependency)

- [x] **Code chunking** — store contract bytecode in the verkle tree
  - 32-byte chunks with domain-separated keys (domain 3, no EIP-4762 push_offset — new blockchain)
  - `verkle_code_chunk_key()` in `verkle_key.h/c` — tree_index = chunk_id/256, sub_index = chunk_id%256
  - `verkle_state_set_code()` / `verkle_state_get_code()` in `verkle_state.h/c`
  - Code hash is caller's responsibility (no keccak dependency)

- [x] **Tree snapshot** — binary serialization/deserialization of entire verkle tree
  - `verkle_snapshot.h/c` — save/load full tree (structure + values + commitments) to binary file
  - Sparse encoding: bitmaps for set values/children, only non-NULL entries written
  - Commitments preserved on load — no recomputation needed
  - Format: 16B header ("VRKLSNAP" + version) + recursive node encoding

- [x] **Flat commitment updater** — disk-backed commitment updates, O(block) RAM
  - `verkle_flat.h/c` — replaces in-memory tree for block execution (~2.2 TB → O(block_changes))
  - Value store (hash_store, key=32B, value=32B) + existing commitment store
  - Per-block: group by stem, incremental leaf update via pedersen_update, bottom-up internal propagation
  - New leaves: full MSM from scratch; existing leaves: O(1) incremental per suffix
  - Undo log for both values and commitments — block revert restores full state
  - Cross-validated against in-memory tree (9 test phases, 44 assertions)

- [x] **State journal** — block revert + crash recovery + background checkpoint
  - `verkle_journal.h/c` — in-memory undo journal (records old values, replays in reverse)
  - `verkle_unset()` in `verkle.h/c` — clears key, removes empty leaves, collapses single-child internals
  - Forward journal file: appends new values to disk (no fsync during execution)
  - Background checkpoint: `fork()` for consistent CoW snapshot, parent continues executing
  - Recovery: load last snapshot + replay forward journal

- [x] **Benchmarking** — verkle state scale tests (tree + flat)
  - `bench_verkle_state.c` — in-memory tree path: verkle_state → verkle_journal → verkle_snapshot
  - `bench_verkle_flat.c` — disk-backed flat path: verkle_state → verkle_flat → hash_stores
  - Realistic op mix per block (~500 ops): balance 40%, nonce 20%, storage 25%, new account 10%, code deploy 5%
  - Metrics: exec/commit latency percentiles, peak RSS, disk usage, persistence round-trip
