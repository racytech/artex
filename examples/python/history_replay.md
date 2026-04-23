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
EVM bytecode for the blocks you already processed — a fast forward
apply of the diffs reconstructs state at roughly **100 blocks per
second** on the reference hardware.

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

`load-state` no longer computes the state root automatically — it
just streams the snapshot into memory. Call `root` explicitly once
afterwards to materialize the cached MPT hashes and sanity-check
that the snapshot decoded cleanly. Compare against the canonical
state root for the snapshot's block on etherscan. From this point
forward all cached hashes are warm, so subsequent incremental
`root` calls after execute are cheap.

```
artex> execute data/era --to 24913378 --adaptive
  replaying from 24864362 → 24913378 (adaptive validation)
  block 24865420  |   27.3 blk/s  |   365 tps  |   830 Mgas/s  |  1823 txs  | ...
INFO  [history] history: blk 24865617 (+256)  144865 accts  196687 slots  12627 created  20058.2 KB
  ...
```

Execute runs the same as before. The new `INFO [history]` lines
appear every 256 blocks summarizing what was appended. The log file
grows in the background; execute never blocks on I/O.

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

artex> root
  0xc6022854cc3540f3b036a96b19bed69b0547e1a84e71d21d1dc8d9b62e0c4dca
  # Verifies the snapshot was loaded cleanly. Compare to etherscan
  # for the snapshot's block.

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

artex> root_full
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
```

Use `root_full` (not `root`) after `replay_history`: the fast replay
path marks dirty paths but doesn't attempt any hash invalidation of
its own, so a full recomputation from scratch is the safe confirmation
that replay matched the canonical chain. The root above is
bit-identical to the canonical state root at block 24876800 —
verifiable on etherscan. State is fully reconstructed and you can
`execute` forward from here; new diffs append to the same log.

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

Computes the full MPT state root at the current block over every
account. Useful right after `replay_history` to confirm replay
matched the canonical chain:

```
artex> root_full
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
```

Compare to etherscan's `stateRoot` for the same block.

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
- **Rewrite a known-bad range.** If a diff for some block was produced
  by a buggy engine version, truncate back and re-`execute` to
  re-record.

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

## Replay vs execute — why it's faster

For a given block, applying a pre-computed diff is cheaper than
running EVM:

| Per storage slot write | execute (EVM opcode) | replay (apply_diff) |
|---|---|---|
| Pre-op gas calc lookup | 1 ART read (current + original) | — |
| Journal entry with old value | 1 ART read | — |
| Storage write | 1 ART write | 1 ART write |
| Warm-address / originals tracking | 3 mem_art inserts | — |

Roughly **5–10× fewer state ops per net change**. That's why a
12,000-block window replays in ~3 minutes where the same window
executes in ~15 minutes. The tradeoff: replay needs a pre-recorded
diff log. It's the right tool for catching up after a crash, not
for first-time sync.

## Full recovery example

End-to-end commands used during this project's own stress test, with
real observed timings:

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

artex> root
  0xc602...4dca
  # verify snapshot decoded cleanly — matches canonical at 24864361

artex> history_range
  first : 24864362
  last  : 24876800            # last durable block before kill
  count : 12439 blocks

artex> replay_history 24876800
  replaying 12439 diffs from 24864362 to 24876800 ...
  done: now at block 24876800 in 146.4s (84.9 blk/s)

artex> root_full
  0x925b43e100fcd487df408cdd9800b81d57e041ffc6c6267e441474f228b31455
  # matches etherscan canonical at block 24876800 — state is correct

artex> execute data/era --to 24913378 --adaptive
  replaying from 24876801 → 24913378 (adaptive validation)
  # execute continues, appending to the same history
```

Total recovery cost: **~35 min snapshot load + ~3 min replay** to get
state to the tail of a 12 k-block execute run. Vs re-executing those
12 k blocks from the snapshot: ~15+ min on top of the snapshot load.
Replay savings scale with window size — the longer the execute run
you lost, the bigger the win from replay-based recovery.
