# Artex Network Architecture

## Two Node Types

artex can be built and run in two modes:

```
Archive Node                          Execution Node
┌──────────────────────┐              ┌──────────────────────┐
│ Full chain replay     │              │ Download snapshot     │
│ State history (diffs) │──snapshots──→│ Rebuild indices       │
│ MPT + Verkle state    │              │ Engine API (CL)       │
│ Historical queries    │              │ Forward execution     │
│ Few instances         │              │ Many instances        │
└──────────────────────┘              └──────────────────────┘
```

### Archive Node

Executes every block from genesis. Produces:

- **State history** — per-block diffs (append-only, ~40GB at 6M blocks)
- **MPT state** — account + storage trie (.dat files)
- **Verkle state** — built from same diffs via `verkle_reconstruct`
- **Code store** — all deployed contract bytecode
- **State snapshots** — .dat files at checkpoint boundaries for distribution

Expensive to run (months for full mainnet replay). Few instances needed.
Serves historical queries: balance at block N, storage at block N.

### Execution Node

Downloads a state snapshot, bootstraps, executes forward:

```
1. Download .dat + .meta + .free files from archive node
2. rebuild_idx to create local .idx indices
3. chain_replay --resume to continue from snapshot block
4. Connect to CL via Engine API for new blocks
```

Cheap to bootstrap (hours, not months). No historical state.
This is what most users run.

## Data Distribution

The distribution format is the `.dat` files:

- **Compact** — only raw trie node data, no hash table overhead
- **Portable** — `.idx` files rebuilt locally (format-specific to machine)
- **Verifiable** — state root in `.meta` validates against era1 headers

Distribution methods: HTTP, rsync, torrent, or any file transfer.
No custom protocol needed.

| File | Size (6M blocks) | Distributed? |
|------|-------------------|--------------|
| `chain_replay_mpt.dat` | ~16 GB | Yes |
| `chain_replay_mpt_storage.dat` | ~16 GB | Yes |
| `chain_replay_mpt.meta` | 56 bytes | Yes |
| `chain_replay_mpt.free` | ~4 MB | Yes |
| `chain_replay_mpt_storage.free` | ~700 KB | Yes |
| `chain_replay_code.dat` | ~512 MB | Yes |
| `chain_replay_mpt.idx` | ~32 GB | No (rebuilt) |
| `chain_replay_mpt_storage.idx` | ~130 GB | No (rebuilt) |
| `chain_replay_code.idx` | ~17 MB | No (rebuilt) |

Total download: ~33 GB. Total after rebuild: ~195 GB.

## Build Modes

```cmake
# Archive node (full history + verkle building)
set(ENABLE_MPT ON)
set(ENABLE_HISTORY ON)
set(ENABLE_VERKLE_BUILD ON)

# Execution node (forward execution only)
set(ENABLE_MPT ON)
set(ENABLE_HISTORY OFF)
set(ENABLE_VERKLE_BUILD OFF)
```
