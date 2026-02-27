# Database Development Plan

## Current State

| Component | Status |
|-----------|--------|
| compact_art (index) | Done — 50 B/key at 300M, 4-byte refs, stress-tested |
| mem_art (write buffer backend) | Done |
| Data layer PoC | Inline in test file, proves architecture works |
| Design docs | DATA_LAYER, CHECKPOINT, PROOF_GENERATION — all written |

## Stage 1 — Data Layer Module

Extract PoC into proper library.

| File | Description |
|------|-------------|
| `state_store.h/.c` | 64B slot allocator, pread/pwrite, free list |
| `write_buffer.h/.c` | mem_art wrapper with tombstone flags |
| `data_layer.h/.c` | Merge logic, read path (buffer → index → disk), public API |

Tests: port PoC test to use the module API.

## Stage 2 — Code Store

| File | Description |
|------|-------------|
| `code_store.h/.c` | Append-only code.dat, content-addressed by keccak256(bytecode) |

Dedup via compact_art lookup before append. Bit 31 of ref selects
state_store vs code_store.

## Stage 3 — Checkpoint & Recovery

| File | Description |
|------|-------------|
| `checkpoint.h/.c` | Serialize compact_art to index.dat (sorted key+ref pairs), atomic rename |

checkpoint.meta for fast startup validation. Recovery: load index.dat →
rebuild compact_art → re-execute K blocks. Compaction: rewrite state.dat
when dead ratio > 10%.

## Stage 4 — MPT Hash Tree

| File | Description |
|------|-------------|
| `commitment.h` | Abstract interface: update, root, prove, verify |
| `mpt_hash.h/.c` | Incremental MPT hash tree over compact_art |

Per-block update: sort dirty keys, walk shared prefixes, recompute hashes.
Provides `state_root()` and `eth_getProof`.

## Stage 5 — Integration API

| File | Description |
|------|-------------|
| `db.h/.c` | Top-level database handle tying everything together |

Block execution interface: `db_begin_block`, `db_put`, `db_get`,
`db_delete`, `db_commit_block`. Automatic merge every K blocks,
checkpoint after merge.

## Stage 6 (future) — Verkle Commitment

Swap MPT for Verkle behind the same `commitment.h` interface.
ART's 256-ary branching maps directly to Verkle internal nodes.
