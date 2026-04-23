# Reproducing the mainnet replay

Step-by-step to run the forward replay over the same 49,017-block
window as the artex [Performance](../../README.md#performance)
section: per-block gas validation against each header, plus
state-root validation on a schedule of your choice (`--adaptive`
below; `--every N` as an alternative). Expect ~60–70 min of wall
time for the replay itself, on top of the ~35 min snapshot load.

## Prerequisites

Before starting, have these in place:

1. **`libartex.so` built.** Follow the Installation subsection of
   the top-level [README](../../README.md#installation).
2. **Snapshot package in `~/.artex/`.** Follow
   [Data files → Option A](../../README.md#option-a--download-the-hosted-snapshot)
   to fetch, verify, and extract the published snapshot. The four
   files (`state_24864361.bin`, `.hashes`, `chain_replay_code.dat`,
   `chain_replay_code.idx`) all live in the same directory.
3. **Era files covering block 24,864,362 → 24,913,378 in `data/era/`.**
   For the exact 49,017-block window the Performance section
   measured, you need these **six** era files (~4.5 GiB total):

   | file | blocks | size |
   |---|---|---|
   | `mainnet-01722-94a4c88c.era` | 24,864,362 – 24,872,529 | 825 MiB |
   | `mainnet-01723-443913b8.era` | 24,872,530 – 24,880,702 | 795 MiB |
   | `mainnet-01724-53dee8bd.era` | 24,880,703 – 24,888,868 | 794 MiB |
   | `mainnet-01725-d1ead1a2.era` | 24,888,869 – 24,897,037 | 716 MiB |
   | `mainnet-01726-adcba5d7.era` | 24,897,038 – 24,905,209 | 764 MiB |
   | `mainnet-01727-faa7d011.era` | 24,905,210 – 24,913,378 | 710 MiB |

   Fetch them from <https://mainnet.era.nimbus.team/>:

   ```bash
   mkdir -p data/era && cd data/era
   for f in \
       mainnet-01722-94a4c88c.era \
       mainnet-01723-443913b8.era \
       mainnet-01724-53dee8bd.era \
       mainnet-01725-d1ead1a2.era \
       mainnet-01726-adcba5d7.era \
       mainnet-01727-faa7d011.era; do
       curl -LO "https://mainnet.era.nimbus.team/$f"
   done
   cd -
   ```

   The hash suffix (e.g. `94a4c88c`) is part of the canonical
   filename on nimbus.team — the names above are stable. If any era
   number hosts a newer/different hash by the time you fetch, the
   directory listing at the URL above lists what's currently
   published.
4. **Python venv activated** with the examples' requirements:

   ```bash
   source venv/bin/activate
   ```

## Run it

```bash
python3 examples/python/artex_cli.py
```

You land in the REPL with no state loaded. Two commands drive the
whole replay:

```
libartex version: 0.1.0

  artex interactive CLI — type 'help' or '?' for commands, 'quit' to exit.
  no state is loaded on startup — run 'load-state <path>' or 'load-genesis <path>' first.

artex> load-state ~/.artex/state_24864361.bin
  data dir set to: /home/<you>/.artex
  loading ~/.artex/state_24864361.bin ...
  loaded state at block 24864361 (2100.0s)

artex> root
  0xc6022854cc3540f3b036a96b19bed69b0547e1a84e71d21d1dc8d9b62e0c4dca

artex> execute data/era --to 24913378 --adaptive
  replaying from 24864362 → 24913378 (adaptive validation)
  block 24865420  |   27.3 blk/s  |   365 tps  |   830 Mgas/s  |  1823 txs  |  interval=1024  |  validated=0
  block 24866442  |   28.0 blk/s  |   372 tps  |   840 Mgas/s  |  1850 txs  |  interval=1024  |  validated=1
  ✓ root @ 24866432: 0x7a8f…
  ...
  block 24912378  |   12.4 blk/s  |   167 tps  |   391 Mgas/s  |   836 txs  |  interval=64   |  validated=45
  ✓ root @ 24912384: 0x3b2c…
  ...
  block 24913378  |    2.1 blk/s  |    28 tps  |    66 Mgas/s  |   140 txs  |  interval=1    |  validated=107
  ✓ root @ 24913378: 0x9d4e…

  blocks    : 49017
  txs       : 15,641,066
  total gas : 1,487,263,056,619
  time      : 4019.3s
  avg       : 12.2 blk/s, 3891 tps, 370 Mgas/s
  validated : 109
```

`execute` prints a window line every ~5 seconds and a
`✓ root @ <block>: 0x…` line after every successful state-root
validation. With `--adaptive`, the `interval` column tightens as
you approach `--to`: 1024 far from target, down to 1 for the final
15 blocks. If the final committed block didn't fall on a validation
interval, a post-loop validation runs automatically so you always
end with a verified root. Ctrl-C stops replay mid-run; the engine
state reflects the last committed block.

## Expected timing

- **`load-state` on cold OS page cache:** ~35 min for the 140 GiB
  `state_24864361.bin`. The breakdown is roughly 10 min to read and
  reconstruct the ART from disk, then ~25 min to re-verify the MPT
  root (full walk + keccak per node). Second load in the same
  session (file still in page cache, ART destroyed between loads)
  skips most of the read cost but the root re-verification is still
  CPU-bound — expect ~15–17 min, not seconds. The verification step
  can't be skipped; it's what proves the snapshot's integrity.
- **`execute`:** 20–25 blk/s / 600–700 Mgas/s between validation
  points on this window, averaging ~12–13 blk/s / ~370–400 Mgas/s
  once root checks are amortized in. `--adaptive` does ~43% fewer
  validations than `--every 256` (109 vs 191), but each adaptive
  validation accumulates more dirty storage tries and is therefore
  heavier — so total wall time ends up roughly equal between the
  two. Pick `--adaptive` for tighter diagnostics near `--to` (every
  block validates in the last 15), pick `--every N` for a flat,
  predictable cadence.
- **Total for the 49,017-block window:** ~70 min from start of
  `execute` to the block you stop at. Add the 35 min load to get
  the ~105 min cold-start figure in the README.

See [Total cold-start sync time](../../README.md#total-cold-start-sync-time)
for the scaling formula if you want to close a different gap.

## How `--adaptive` chooses intervals

The schedule is keyed on distance from the current block to `--to`:

| distance to `--to` | validation interval |
|---|---|
| > 10,000 | every 1024 blocks |
| > 1,000 | every 256 |
| > 100 | every 64 |
| > 15 | every 16 |
| ≤ 15 | every 1 |

Coarse during bulk replay, tight in the final stretch. The per-1
cadence only kicks in for the last 15 blocks — worth the handful of
extra root checks so that if something diverges near the end, you
know exactly which block caused it.

`--adaptive` requires `--to` to be meaningful; without it the whole
run sits in the "distance > 10,000" bucket and the schedule
collapses to "every 1024" flat. Always pair the two.

## Useful variations

### Fixed validation interval

If you prefer a constant cadence regardless of progress:

```
artex> execute data/era --to 24913378 --every 256
```

`--every N` validates every Nth block. The Performance section's
measured run used `--every 256` — ~191 validations in 3,684.8 s
(13.3 blk/s, 404 Mgas/s). `--adaptive` hits ~109 validations in
4,019.3 s (12.2 blk/s, 370 Mgas/s) — fewer validations but each
one is heavier because more dirty storage tries accumulate between
checks. Wall time comes out roughly the same; pick based on what
you want out of the diagnostic side:

- `--every N` for a flat, predictable cadence.
- `--adaptive` for pinpoint validation (every block) in the last
  15 blocks before `--to`, so any divergence near the target is
  attributed to a specific block instead of a 256-block window.

### Replay without a stop

```
artex> execute data/era
```

Omitting `--to` runs until the era files are exhausted. Defaults to
`--every 256`. Good for "whatever blocks I have, go through them
all."

### Save a new snapshot when you're done

```
artex> save-state /tmp/state_after.bin
```

Writes the state and its `.hashes` sidecar. Next session you can
`load-state /tmp/state_after.bin` instead of re-replaying from
24,864,361.

### Interrupt and inspect

Ctrl-C stops replay mid-run. The engine state reflects the last
committed block. Use `block`, `root`, `balance <addr>`, `nonce
<addr>`, `storage <addr> <slot>` to inspect before quitting.

## Troubleshooting

**`no mainnet-*.era files in data/era`**
You either don't have era files yet (run `./data/download_era.sh`)
or they're in a different directory (pass the correct path as the
first positional arg to `execute`).

**Gas or root mismatch at block N**
The replay halts and reverts the offending block. Double-check:

- The libartex version matches what the snapshot was created with
  (forks may have shifted).
- No other process is touching `~/.artex/` — a half-written
  snapshot would manifest as a root mismatch on first validation.

**Swap thrashing on load**
The `state_24864361.bin` is 140 GiB; most machines pull it through
swap. Run `vmstat 1` in another terminal — sustained `si` rates of
50–100k pages/s during load are normal. After load, steady-state
`si` / `so` drop to the hundreds. The README's [Major-fault
profile](../../README.md#major-fault-profile) has expected ranges.
