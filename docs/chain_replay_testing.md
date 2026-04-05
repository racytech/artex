# Chain Replay Testing & Debug Workflow

## Full Run (from genesis)

```bash
./chain_replay --data-dir <dir> <era1_dir> <genesis.json> 0 <end_block>
```

State files created in `<dir>/`:
- `chain_replay_mpt.dat`, `.idx` — account trie
- `chain_replay_mpt_storage.dat`, `.idx` — storage tries
- `chain_replay_code.dat`, `.idx` — contract bytecode
- `chain_replay_history/` — per-block state diffs
- `chain_replay.ckpt` — checkpoint file
- `chain_replay_mpt.meta` — written on mismatch (safe checkpoint)

Validates gas per-block, state root every 256 blocks.

## Debug Workflow (mismatch → fix → resume)

### 1. Mismatch detected at block N

chain_replay stops and writes:
- `.meta` with `last_block = ((N-1)/256)*256` (last passing checkpoint C)
- `known_issues/block_<N>/alloc.json` (prestate dump)

### 2. Debug with history diffs

```bash
# Compare our diffs against reference client for blocks C+1..N
./history_dump <dir>/chain_replay_history <block>
```

History diffs for C+1..N are from our (buggy) execution — useful for comparison,
but not ground truth. Diffs 1..C are proven correct (checkpoint passed).

### 3. Fix the bug, rebuild

### 4. Reconstruct state to checkpoint C

```bash
./state_reconstruct --resume --data-dir <dir> <target_block_C>
```

state_reconstruct reads history diffs 1..C (proven correct) and rebuilds the MPT.
It writes to `chain_replay_mpt` (same files chain_replay uses).
Code store (`chain_replay_code`) is read-only — already populated from step 1.

### 5. Resume chain_replay from C+1

```bash
./chain_replay --resume --data-dir <dir> <era1_dir> <genesis.json> <C+1> <end>
```

Resume flow:
1. Read `.meta` → `last_block = C`
2. **Truncate history to C** — removes wrong diffs C+1..N
3. Open existing MPT/code stores (no genesis load)
4. Read 256 block hashes from era1 for BLOCKHASH ring
5. Call `sync_resume(C, block_hashes, count)`
6. Execute from C+1 — new (correct) diffs appended to history

## .meta format (56 bytes)

```
magic:      u32 "RMPT"
version:    u32
last_block: u64
state_root: [32]byte
prune_empty: u8
reserved:   [7]byte
```

## History

Per-block state diffs in `<dir>/chain_replay_history/`:
- `state_history.dat` — variable-length diff records
- `state_history.idx` — 16-byte header + 16-byte entries (block→offset)

Query: `./history_dump <history_dir> <block_number>`

Truncation: `state_history_truncate(sh, last_block)` — already implemented.

## Flags

| Flag | Description |
|------|-------------|
| `--clean` | Delete state files, start fresh |
| `--data-dir DIR` | State file directory (default: `data/`) |
| `--resume` | Resume from `.meta` (truncates history to safe block) |
| `--no-history` | Disable state diff history |
| `--follow` | Wait for new era1 files |
| `--trace-block N` | EIP-3155 trace for block N |
| `--dump-prestate N [P]` | Dump pre-state for block N |

## state_reconstruct

Rebuilds MPT from history diffs (no EVM re-execution).

```bash
# Fresh build from genesis
./state_reconstruct --data-dir <dir>/chain_replay_history <target_block>

# Resume from existing snapshot
./state_reconstruct --resume --data-dir <dir>/chain_replay_history <target_block>
```

Flags: `--no-validate`, `--validate-interval N`, `--evict-interval N`

Writes to `chain_replay_mpt` (same path chain_replay uses).
Reads code store from `chain_replay_code` (read-only).
