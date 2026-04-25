# Persistence and replay

> Assumes you've already been through the
> [replay walkthrough](replay_walkthrough.md) — snapshot loaded,
> era files in place, `execute` runs. This doc adds the second piece:
> persisting per-block diffs so a crash doesn't cost you a full
> re-execution on the next start.

## How history complements snapshots

A snapshot freezes every account at a single height; a history log
records the per-block changes that happened *after* that height.
With both, recovery is "load the snapshot, then fast-forward by
applying recorded diffs" — no EVM re-execution needed.

When the engine is started with history enabled, every committed
block's diff (account fields, storage slot writes, destructed-account
markers) is appended to a disk-backed log as `execute` runs. The
write is async and off the critical path. On restart, `replay-history`
reads those diffs back and applies them to the loaded snapshot —
about **half the wall time** of re-executing the same window through
the EVM, because the gas/opcode/state-access work is skipped.

The intended pattern is infrequent snapshots (once per fork, once
per long-run session, …) combined with continuous history.

## Generating history diffs

> *Published snapshots ship with a matching diff log, so you normally
> won't need to record one yourself — `load-state` followed by
> `replay-history` works out of the box. This section covers the
> case where you do want to capture your own log (running an
> unattended node, recording a custom window, exporting diffs for
> downstream tools, …).*

### Enabling history

Build with `ENABLE_HISTORY=ON` (off by default):

```bash
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_HISTORY=ON ..
cmake --build . --target artex_shared -j
```

The `artex_cli` REPL takes `--history <dir>` to tell the engine where
to write the log:

```bash
python3 examples/python/artex_cli.py \
    --data-dir ~/.artex \
    --history ~/.artex/history
```

Two files get created or reopened in `<dir>`:

- `state_history.dat` — the diff records (append-only)
- `state_history.idx` — the per-block offset index

If you start without `--history`, execute runs exactly as in the
basic walkthrough but without persisting diffs.

### Basic flow

Three phases in one session: execute with history on, crash, recover.

#### 1. Start execute with history

```
artex> load-state ~/.artex/state_24864361.bin
  ...
  loaded state at block 24864361 (762.9s)

artex> root
  0xc6022854cc3540f3b036a96b19bed69b0547e1a84e71d21d1dc8d9b62e0c4dca (0.0s)
```

Note: `load-state` restores the cached internal-node hashes from
the snapshot, so this first `root` returns in seconds. Subsequent
calls are incremental and only re-hash dirty subtrees touched
since the last compute.

```
artex> execute data/era --to 24913378 --adaptive
  replaying from 24864362 → 24913378 (adaptive validation)
  block 24865420  |   27.3 blk/s  |   365 tps  |   830 Mgas/s  |  1823 txs  | ...
INFO  [history] history: blk 24865617 (+256)  144865 accts  196687 slots  12627 created  20058.2 KB
  ...
```

`INFO [history]` lines appear every 256 blocks summarizing what was
appended. The log file grows in the background; execute never blocks
on I/O.

#### 2. Simulate a crash

Kill the process hard (the point is to not give it a clean shutdown):

```bash
kill -9 $(pgrep -f artex_cli | head -1)
```

On disk: whatever diffs had been flushed by the consumer thread are
intact. The most recent one or two may be a partial record — that's
fine, see "Crash safety" below.

#### 3. Recover

Restart the REPL with the same `--history` arg, then:

```
artex> load-state ~/.artex/state_24864361.bin
  ...
  loaded state at block 24864361 (762.9s)

artex> history_range
  first : 24864362
  last  : 24876800
  count : 12439 blocks

artex> replay_history 24876800
  replaying 12439 diffs from 24864362 to 24876800 ...
INFO  [history] root at block 24864425 = 0xafc3...aa01
INFO  [history] root at block 24864489 = 0xb6e1...3c9d
  ...
INFO  [history] root at block 24876800 = 0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
INFO  [history] history: replay complete — 12439 blocks applied (24864362..24876800)
  done: now at block 24876800 in 550.4s (22.6 blk/s)

artex> seed_hashes data/era
  # refreshes the 256-entry block-hash ring to the replay endpoint
  # so BLOCKHASH resolves correctly for subsequent execute.

artex> root
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455 (0.0s)
```

The ordering matters:

1. `replay_history` brings the state to the target block. The
   `root at block N` lines fire every 64 applied blocks (configurable
   via `Config(replay_root_interval=N)`), each one a real
   `compute_root` against the in-memory trie. The final line is the
   state root at the replay endpoint — compare against the canonical
   state root from any trusted source (block explorer, another full
   node) for that block; a match proves the state reconstructed
   exactly.
2. `seed_hashes` reads the last 256 header hashes from era files and
   writes them into the engine's block-hash ring — mandatory before
   any `execute` that follows, since the ring after `load_state`
   still holds snapshot-era hashes and `BLOCKHASH` in the next block
   would otherwise return stale values.
3. A standalone `root` returns instantly because the periodic compute
   already produced the cached root for the endpoint block. Use
   `root_full` only if you want a from-scratch sanity check that
   ignores all caches.

Once those have run, state is fully reconstructed and `execute` can
continue from the next block; new diffs append to the same log.

## Useful commands

All are REPL commands; the library functions they call are in
`include/artex.h` under the `rx_engine_history_*` and
`rx_compute_state_root_full` prefixes.

### `history_range`

Prints the first and last block stored in the log:

```
artex> history_range
  first : 24864362
  last  : 24883273
  count : 18912 blocks
```

Use before `replay_history` to pick a valid target.

### `replay_history <target>`

Forward-applies all diffs from `current_block + 1` through `<target>`.
Runs fast — bypasses the EVM, journal, warm-address tracking, and the
storage-read-before-write that live execute needs for gas accounting.

Computes a state root every 64 applied blocks (override via
`Config(replay_root_interval=N)`; `0xFFFFFFFF` disables) so the
endpoint already has a verified, cached root by the time the call
returns. Each interval logs `root at block N = 0x...`, useful for
spot-checking against canonical headers mid-replay.

Target must be in `[first..last]` as reported by `history_range`.

### `root_full`

Computes the full MPT state root at the current block. Invalidates
all cached node hashes first, then walks the entire account +
storage tries and re-hashes every node from scratch.

```
artex> root_full
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
```

After `replay_history` you usually don't need this — the periodic
compute already produced a verified root, and `root` returns it
instantly. Reach for `root_full` only as a paranoia check that
ignores every cache, e.g. when chasing a suspected hash-cache
corruption bug.

**Expensive.** ~30–35 min on the mainnet ~24.8 M snapshot,
CPU-bound on keccak.

### `seed_hashes <era-dir>`

Reads the last 256 block-header hashes from era files and writes them
into the engine's block-hash ring. Mandatory before `execute` that
follows a `replay_history`: `load_state` populates the ring from the
snapshot's `.hashes` sidecar, `replay_history` advances state past
that anchor without touching the ring, and the first executed block
would otherwise see stale `BLOCKHASH` values for any block in the
last-256 window. `seed_hashes` closes that gap.

```
artex> seed_hashes data/era
  seeded 256 block hashes ending at block 24876800
```

### `truncate_history <last-block>`

Discards every entry past `<last-block>` from the log. Two uses:

```
artex> history_range
  first : 24864362
  last  : 24883273
  count : 18912 blocks

artex> truncate_history 24880000
  truncating: keep 15639 block(s) [24864362..24880000], discarding 3273 block(s) [24880001..24883273]
  done: history now spans [24864362..24880000] (15639 blocks)
```

- **Roll back before a reorg.** Future execute from here overwrites
  the discarded range.
- **Rewrite a known-bad range.** If a diff needs to be regenerated,
  truncate back to the last good block and re-`execute` to re-record.

Does not modify in-memory state — you're responsible for bringing
state and history into agreement (usually `load_state` + fresh
`execute`).

## Crash safety

What survives a hard kill in each layer:

- **Snapshots.** `load_state` validates the header + full account
  count before the engine trusts the file. A truncated snapshot
  (disk full, killed mid-save, …) is rejected at load time.
- **History diff log.** Each record carries a CRC32C. On reopen, a
  tail-scan walks backwards from the last indexed entry, truncating
  any record with a bad CRC or mismatched block number. You lose at
  most the single in-flight record that was being written when the
  process died; every fully-written prior record survives.
- **Contiguity guard.** The consumer thread refuses any diff whose
  block number isn't exactly `tail + 1` and logs a WARN. Prevents
  out-of-order writes from silently corrupting the positional `.idx`
  layout — useful defense if e.g. you forgot to `replay_history` up
  to the tail before starting a fresh `execute`.

## Full recovery example

End-to-end commands used during this project's own stress test:

```
# (1) Prepare: build with history on, arrange files.
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_HISTORY=ON ..
cmake --build . -j

# (2) First run: execute, appending to history, hard-kill partway.
python3 examples/python/artex_cli.py \
    --data-dir ~/snapshot \
    --history  ~/snapshot/history

artex> load-state ~/snapshot/state_24864361.bin
  loaded state at block 24864361 (2100s)

artex> root
  0xc602...4dca
  # one-time cost to materialize cached hashes after load

artex> execute data/era --to 24913378 --adaptive
  block 24876800 | 19.8 blk/s | ...
  ...
  # user issues kill -9 externally
  (process dies mid-block)

# (3) Restart: check the log, replay it, verify root.
python3 examples/python/artex_cli.py \
    --data-dir ~/snapshot \
    --history  ~/snapshot/history

artex> load-state ~/snapshot/state_24864361.bin
  loaded state at block 24864361

artex> history_range
  first : 24864362
  last  : 24876800            # last durable block before kill
  count : 12439 blocks

artex> replay_history 24876800
  replaying 12439 diffs from 24864362 to 24876800 ...
INFO  [history] root at block 24876800 = 0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
  done: now at block 24876800 in 550.4s (22.6 blk/s)

artex> seed_hashes data/era
  # refresh block-hash ring for the new anchor block

artex> root
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455 (0.0s)
  # matches canonical at block 24876800 — state is correct

artex> execute data/era --to 24913378 --adaptive
  replaying from 24876801 → 24913378 (adaptive validation)
  # execute continues, appending to the same history
```

Total recovery cost for a 49 k-block window: **~13 min snapshot
load + ~36 min diff replay.** A standalone `root_full` for
paranoia adds ~30-35 min on top.
