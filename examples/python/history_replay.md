# Persistence and replay

> Assumes you've already been through the
> [replay walkthrough](replay_walkthrough.md) — snapshot loaded,
> era files in place, `execute` runs. This doc adds the second piece:
> persisting per-block diffs so a crash doesn't cost you a full
> re-execution on the next start.

## What history gives you

When the engine is built and started with history enabled, every
committed block's state diff (account fields, storage slot writes,
destructed-account markers) is appended to a disk-backed log as
execute runs. After an unclean shutdown you don't need to re-run
EVM bytecode for the blocks you already processed — replay skips
the EVM entirely and just applies the recorded state updates, so
diffs reconstruct state at roughly **140 blocks per second** on the
reference hardware (49 k-block window in ~5 min).

The diff log is separate from, and complementary to, full-state
snapshots:

| | Snapshot | History log |
|---|---|---|
| Size (mainnet ~24.8 M) | ~140 GB at block 24,864,361 | ~87 KB/block (measured over 49,017 blocks: 4.05 GB on disk) |
| Write cost | Manual, ~35 min | Per-block, async, negligible overhead |
| Read cost | ~35 min for a cold load | ~10 ms/block replay |
| Covers | All accounts at one height | All changes across a range of heights |
| Recovery use | Starting point | Forward catch-up from the starting point |

The intended pattern is infrequent snapshots (once per fork, once per
long-run session, …) combined with continuous history.

## Enabling history

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

## Basic flow

Three phases in one session: execute with history on, crash, recover.

### 1. Start execute with history

```
artex> load-state ~/.artex/state_24864361.bin
  ...
  loaded state at block 24864361 (2100.0s)

artex> root
  0xc6022854cc3540f3b036a96b19bed69b0547e1a84e71d21d1dc8d9b62e0c4dca
```

Note: the first `root` after `load-state` is **expensive** — every
trie node is marked dirty by the loader, so this first call walks
the entire account + storage tries and re-hashes everything from
scratch (~30–35 min on the mainnet ~24.8 M snapshot, CPU-bound on
keccak). Subsequent calls are incremental — they only re-hash dirty
subtrees — and finish in seconds between checkpoints. This up-front
cost also verifies the snapshot's integrity against the header root.

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

### 2. Simulate a crash

Kill the process hard (the point is to not give it a clean shutdown):

```bash
kill -9 $(pgrep -f artex_cli | head -1)
```

On disk: whatever diffs had been flushed by the consumer thread are
intact. The most recent one or two may be a partial record — that's
fine, see "Crash safety" below.

### 3. Recover

Restart the REPL with the same `--history` arg, then:

```
artex> load-state ~/.artex/state_24864361.bin
  ...
  loaded state at block 24864361

artex> history_range
  first : 24864362
  last  : 24876800
  count : 12439 blocks

artex> replay_history 24876800
  replaying 12439 diffs from 24864362 to 24876800 ...
INFO  [history] apply_diff #1 block=24864362 groups=771  [vm=239.8G rss=84.1G]
INFO  [history]   after_apply: vm=239.77G (+0.00G) rss=84.08G (+0.00G) glibc.arena=0.00G glibc.hblkhd=0.02G glibc.uordblks=0.00G
INFO  [history] apply_diff #500 block=24864861 groups=1118  ...
  ...
INFO  [history] history: replay complete — 12439 blocks applied (24864362..24876800)
  done: now at block 24876800 in 146.4s (84.9 blk/s)

artex> seed_hashes data/era
  # refreshes the 256-entry block-hash ring to the replay endpoint
  # so BLOCKHASH resolves correctly for subsequent execute.

artex> root_full
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
```

The ordering matters:

1. `replay_history` brings the state to the target block.
2. `seed_hashes` reads the last 256 header hashes from era files and
   writes them into the engine's block-hash ring — mandatory before
   any `execute` that follows, since the ring after `load_state`
   still holds snapshot-era hashes and `BLOCKHASH` in the next block
   would otherwise return stale values.
3. `root_full` does a from-scratch recomputation of the MPT and
   returns the state root at the replay endpoint. Compare against
   the canonical state root from any trusted source (block explorer,
   another full node) for that block; a match proves the state
   reconstructed exactly. `root_full` (not `root`)
   is used here because the fast replay path marks dirty paths but
   skips hash invalidation of its own, so the full recomputation is
   the clean confirmation.

Once those three have run, state is fully reconstructed and `execute`
can continue from the next block; new diffs append to the same log.

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

Target must be in `[first..last]` as reported by `history_range`.

### `root_full`

Computes the full MPT state root at the current block. Invalidates
all cached node hashes first, then walks the entire account +
storage tries and re-hashes every node. Useful right after
`replay_history` to confirm replay matched the canonical chain:

```
artex> root_full
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
```

Compare to the `stateRoot` reported by a trusted source (block
explorer or another full node) for the same block.

**Expensive.** Because it ignores any cached hashes, `root_full`
costs roughly the same as the first `root` call after `load-state`
— ~30–35 min on the mainnet ~24.8 M snapshot, CPU-bound on keccak.
Use it sparingly: once to confirm replay, not for every checkpoint.
For regular checkpoint-style verification during `execute`, prefer
the incremental `root` command, which only re-hashes subtrees that
changed since the last call.

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
  done: now at block 24876800 in 146.4s (84.9 blk/s)

artex> seed_hashes data/era
  # refresh block-hash ring for the new anchor block

artex> root_full
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
  # matches canonical at block 24876800 — state is correct

artex> execute data/era --to 24913378 --adaptive
  replaying from 24876801 → 24913378 (adaptive validation)
  # execute continues, appending to the same history
```

Total recovery cost for a 49 k-block window: **~30 min snapshot
load + ~5 min replay + ~35 min full root re-verification.**
