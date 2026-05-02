# artex - Ethereum Execution Engine Library

A single-threaded, in-memory Ethereum execution engine written in C,
shipped as a shared library (`libartex.so`) with a stable C API. Use
it from Python via ctypes, Go via cgo, C/C++ by direct linking, or
any other language with a C FFI, to execute blocks, query state,
build new blocks, and compute MPT roots.

> The name comes from **ART + ex**(ecution): the engine's state layer
> is an Adaptive Radix Trie, and everything else in the library sits
> on top of it.

## What makes it different

The full world state — the account index and every account's storage
— lives in one kind of structure: an **Adaptive Radix Trie (ART)**
with keccak-256-hashed keys. One ART holds the account index (keyed
by address-hash); every contract has its own ART holding its storage
(keyed by slot-hash). That one structure serves two purposes at
once:

1. **State indexing.** Any read or write is an O(k) walk from the
   trie root down to the leaf — 32 bytes of hashed key, one byte per
   step, one branch per step.
2. **MPT root source.** Each ART inner node carries a 32-byte Merkle
   hash and a dirty bit directly in its body. When a block header
   needs a state root, we walk the ART over only the dirty subtrees,
   rehashing where the dirty bit is set and reusing the cached hash
   everywhere else.

So the ART *is* the state, and the MPT root is its cached Merkle
digest. No separate flat store, no separate MPT, no double writes —
one walk covers lookup, and one walk (over just the changed parts)
covers root computation.

### What ART gives us

- **Adaptive node layouts (4 / 16 / 48 / 256 children).** The trie
  switches between compact and dense node types as fan-out grows, so
  sparse branches don't waste memory and full branches don't cost
  lookups. Most storage tries in real contracts are sparse at most
  levels and dense at a few — ART fits that perfectly.
- **O(k) on the key, independent of N.** A lookup is bounded by the
  key length (32 bytes for hashes) regardless of how many accounts
  or slots exist. No rebalancing, no tree height growth with scale.
- **Cache-friendly node bodies.** Each node type is sized to fit in
  a handful of cache lines, and nodes are allocated out of the same
  pool, so walking the trie has better locality than node-per-malloc
  trees.
- **No path compression.** Textbook ART stores shared-prefix bytes
  inside internal nodes to collapse single-child chains. Hashed keys
  essentially never share prefixes past the first byte or two, so
  we skip the bookkeeping — nodes carry no `prefix[]` field, and
  lookup is one byte → one branch → one step.
- **No compaction, no rebalancing, no write amplification.** Inserts
  and deletes mutate one or two nodes and are done. Nothing like an
  LSM-tree merge pass or B-tree split cascade.

## Performance

These are measurements from an actual mainnet replay on one
developer workstation. They're not marketing numbers — different
hardware and fork timing will move them around. Treat them as a
sanity baseline, not a spec.

**At a glance** (one run, one machine, 49,017 blocks; details below):

- **13.3 blk/s / 404 Mgas/s** average, bursting to ~30 blk/s /
  ~880 Mgas/s between root-validation points.
- **~83 min cold-start sync** for this 49,017-block window:
  ~13 min to load a ~150 GB snapshot, ~70 min to replay. Roughly
  halves to **~49 min** when the catch-up uses recorded per-block
  diffs (`replay-history`, ~36 min for the same window) instead of
  re-executing — see [`history_replay.md`](examples/python/history_replay.md).
- **~232 GB memory footprint** during replay (90 GB RSS + 142 GB
  swap) from a single engine instance.

### System under test

| | |
|---|---|
| **CPU** | AMD Ryzen 9 9950X |
| **RAM** | 92 GB |
| **Swap** | 254 GB total (30 GB NVMe partition + 7 × 32 GB swap files on the same NVMe) |
| **Disk** | WD_BLACK SN850X 2 TB (NVMe, PCIe Gen4) |
| **OS** | Ubuntu 24.04 LTS (Linux 6.17), zswap disabled during measurement |

### Minimum requirements to replay mainnet at ~24.8 M blocks

| Resource | Minimum | Why |
|---|---|---|
| **RAM + swap (combined)** | 92 GB RAM + 254 GB swap | Load the ~150 GB snapshot and have room to grow. Measured during replay: **RSS 90 GB + Swap 142 GB ≈ 232 GB total**, `VmData` ~234 GB. |
| **Swap media** | NVMe | Cold-account page faults dominate steady state; SATA SSD is a noticeable bottleneck, spinning disk is orders of magnitude worse. |
| **Free disk** | ~300 GB | ~150 GB snapshot + 16 GB code store + a few recent era files + headroom to save a new snapshot. |
| **OS** | Linux | The pool uses `MAP_ANONYMOUS \| MAP_PRIVATE` + `mremap`. No macOS/Windows port. |

### Replay throughput

Forward replay **from block 24,864,361 to 24,913,378** (49,017
blocks) starting from a warm state snapshot, with **per-block gas
validation** against the canonical header and **state-root
validation every 256 blocks**. Root hashing is parallelized — up to
8 threads on dirty storage tries, 4 threads on the account trie.

- **49,017 blocks in ~4,200 s** (~70 minutes of wall time)
- **1.49 trillion gas consumed**
- **Average: 13.3 blk/s, 404 Mgas/s**
- **Between root-validation points: 25–30 blk/s, 770–880 Mgas/s**;
  the slower numbers fall on blocks where the state-root check lands

The between-validation bursts are what a re-execution workload (test
harness, fork simulation, analytics replay) will actually see once
the hot working set is in RAM.

### Total cold-start sync time

Treating the measured run as a cold-start sync (load a snapshot,
re-verify it, then replay forward to catch up), the end-to-end
breakdown for this 49,017-block window:

| Phase | Time | Notes |
|---|---|---|
| `state_load` | **~13 min** | Cold page cache. Reads ~150 GB from disk and reconstructs the in-memory ART. Cached internal-node hashes are restored from the snapshot's persisted DFS stream, so the first `root` returns in seconds — no full re-verification walk. |
| Forward replay (per-block gas, per-256 root) | **~70 min** | 49,017 blocks. |
| **Total** | **~83 min** | From engine construction to a state caught up to block 24,913,378. |

The `state_load` cost is paid once per process. Keep the engine
alive across your workload and subsequent block executions run at
steady-state throughput without paying it again.

Step-by-step to reproduce the run on your own machine — prereq
checklist, exact commands, expected output, and useful variations —
is in [`examples/python/replay_walkthrough.md`](examples/python/replay_walkthrough.md).

### Swap behavior

On a machine where state doesn't fit entirely in RAM, the first few
minutes of replay produce heavy page-fault activity as the hot
working set gets pulled into memory. Rates settle down once the
working set is resident. More RAM and faster NVMe both shorten this
warm-up; tmpfs-backed runs skip it entirely.

### Durability

State lives in memory for speed, but the engine has a full story for
surviving restarts and crashes without re-executing every block after
the snapshot. Persistence is split across three independent layers —
full-state snapshots, an append-only per-block diff log, and a
disk-backed code store — and recovery is just "load the nearest
snapshot, then replay forward." More on this in
[examples/python/history_replay.md](examples/python/history_replay.md).

### Library footprint

`libartex.so` is ~4.7 MB with debug info retained, or ~2.1 MB after
`strip`. Runtime dynamic deps are just `libcjson`, `libssl`, and
`libc` — all system packages on Linux. GMP, libsecp256k1, and blst
are compiled in as static archives; nothing from `third_party/`
travels alongside.

Note: the calling thread must have at least **~32 MB of stack** for
worst-case EVM depth (1024 nested CALLs). `rx_engine_create` fails
if the current `RLIMIT_STACK` is smaller — run `ulimit -s 32768`
(or `resource.setrlimit` in Python) first.

### Scope and limitations

- **Platforms.** Linux x86_64 only today. The pool relies on
  `MAP_ANONYMOUS | MAP_PRIVATE` + `mremap` for in-place growth and on
  a POSIX threading model. No macOS, Windows, or ARM64 builds.
- **Forks.** Supports Frontier through **Osaka** (the current
  Ethereum mainnet fork). Amsterdam and later are not yet
  implemented; EELS fixtures that target those forks are marked
  *skipped* by the test runner rather than attempted. Amsterdam
  work (including EIP-7928 Block Access Lists) tracks on its own
  branch.

## Quick start

Three things you need: `libartex.so` (see [Installation](#installation)
below), a snapshot package (see [Data files](#data-files) — hosted
download, genesis boot, or self-build), and your language of choice.
Below: how to build the library, then the same engine driven from
Python and C.

### Installation

Ubuntu / Debian:

```bash
sudo apt install build-essential cmake pkg-config perl \
    libcjson-dev libssl-dev libsnappy-dev
```

First-time setup — fetches the two submodules (`blst`, `secp256k1`)
and builds their static archives:

```bash
./setup.sh
```

Idempotent: safe to rerun after a `git pull`; skips anything that's
already built. `gmp` is vendored directly in the repo so it doesn't
need a build step. Pass `--force` to rebuild the submodules from
scratch if you ever need to.

Then build `libartex.so` itself:

```bash
cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build build --target artex_shared -j
```

Produces `build/libartex.so` in a few seconds once the build cache is
warm. For a full build (tests, benchmarks, `chain_replay`, and other
tools) drop the `--target artex_shared` flag.

For the Python examples, also set up a venv:

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r examples/python/requirements.txt
```

### Python — query a loaded state

```python
from artex import Engine, Config

with Engine(Config(data_dir="/home/me/.artex")) as engine:
    engine.load_state("/home/me/.artex/state_24864361.bin")
    print("at block:", engine.block_number())
    print("state root:", engine.state_root().hex())

    usdt = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
    print("USDT slot 0:", hex(engine.storage(usdt, 0)))
    print("USDT balance:", engine.balance(usdt))
```

### Python — replay blocks from era files

```python
from artex import Engine, Config
from era import iter_era_blocks, ep_to_header_body

with Engine(Config(data_dir="/home/me/.artex")) as engine:
    engine.load_state("/home/me/.artex/state_24864361.bin")

    start = engine.block_number() + 1
    for b in iter_era_blocks("data/era", start_block=start):
        header, body, block_hash = ep_to_header_body(
            b.ep, parent_beacon_root=b.parent_beacon_root)
        engine.set_block_hash(b.number, block_hash)

        result = engine.execute_block(header, body, block_hash,
                                      compute_root=(b.number % 256 == 0))
        assert result.gas_used == header.gas_used
        engine.commit_block()

        if b.number >= start + 1000:
            break
```

### C — direct link against `libartex.so`

```c
#include <artex.h>
#include <stdio.h>

int main(void) {
    rx_config_t cfg = { .chain_id = 1, .data_dir = "/home/me/.artex" };
    rx_engine_t *eng = rx_engine_create(&cfg);
    if (!eng) return 1;

    if (!rx_engine_load_state(eng, "/home/me/.artex/state_24864361.bin")) {
        fprintf(stderr, "load failed: %s\n", rx_engine_last_error_msg(eng));
        rx_engine_destroy(eng);
        return 1;
    }

    printf("at block %lu\n", rx_get_block_number(eng));

    rx_address_t usdt = {{0xdA,0xC1,0x7F,0x95,0x8D,0x2e,0xe5,0x23,
                         0xa2,0x20,0x62,0x06,0x99,0x45,0x97,0xC1,
                         0x3D,0x83,0x1e,0xc7}};
    rx_uint256_t bal = rx_get_balance(rx_engine_get_state(eng), &usdt);
    printf("USDT balance (big-endian): ");
    for (int i = 0; i < 32; i++) printf("%02x", bal.bytes[i]);
    printf("\n");

    rx_engine_destroy(eng);
    return 0;
}
```

Build: `gcc main.c -I./include -L./build -lartex -Wl,-rpath,./build -o demo`

From Go, use cgo the same way you'd bind any other C library (the full
header is `include/artex.h`). A Go example is not yet shipped.

## Data files

The snippets above load a **snapshot package** — four files plus a
history directory that travel together in one directory:

```
state_<block>.bin          # ART state (accounts + storage + MPT roots)
state_<block>.bin.hashes   # last 256 block hashes — required by load_state
chain_replay_code.dat      # contract bytecode blobs (content-addressed)
chain_replay_code.idx      # code_store index
history/                   # per-block diff log — feeds replay-history (see history_replay.md)
```

`load_state` refuses to open a snapshot whose `.hashes` sidecar is
missing — the `BLOCKHASH` opcode would otherwise silently return zero
and any contract depending on that opcode would diverge from mainnet.

Three ways to get a package:

### Option A — download the hosted snapshot

Published block: **24,864,361**. Archive: **~73 GiB compressed
(tar + zstd -9 --long=27) → ~168 GiB extracted.** Hosted on
Cloudflare R2 (zero-egress-fee public bucket):

```
https://pub-c7b4d8e7ff8d40fda1b752becf26fba9.r2.dev/state_24864361.tar.zst
https://pub-c7b4d8e7ff8d40fda1b752becf26fba9.r2.dev/state_24864361.tar.zst.sha256
https://pub-c7b4d8e7ff8d40fda1b752becf26fba9.r2.dev/state_24864361.manifest.txt
```

```bash
BASE="https://pub-c7b4d8e7ff8d40fda1b752becf26fba9.r2.dev"

mkdir -p ~/snapshot-download && cd ~/snapshot-download

# 1. Fetch the two small sidecar files:
curl -fL -o state_24864361.tar.zst.sha256 "$BASE/state_24864361.tar.zst.sha256"
curl -fL -o state_24864361.manifest.txt   "$BASE/state_24864361.manifest.txt"

# 2. Fetch the 73 GiB archive — resumable on drop. If your connection
#    dies, re-run this exact command and curl will pick up from the
#    last byte (R2 supports HTTP range requests).
curl -fL -C - --retry 10 --retry-delay 15 \
    -o state_24864361.tar.zst \
    "$BASE/state_24864361.tar.zst"

# 3. Verify the archive's integrity (fails fast if the download corrupted):
sha256sum -c state_24864361.tar.zst.sha256

# 4. Decompress into the directory you'll pass as data_dir:
mkdir -p ~/.artex
zstd -d --long=27 -c state_24864361.tar.zst | tar -xf - -C ~/.artex

# 5. Verify each extracted file against the per-file manifest:
( cd ~/.artex && sha256sum -c ~/snapshot-download/state_24864361.manifest.txt )

# 6. (optional) Remove the compressed archive once extraction is verified:
rm state_24864361.tar.zst
```

Flags on step 2: `-f` fails fast on HTTP errors (so curl doesn't write
a 403 HTML body into your archive file), `-L` follows redirects, `-C -`
resumes from wherever the local file left off, and the retries cover
transient network blips without restarting the whole download.

Then point `load_state` at `~/.artex/state_24864361.bin` — the
sidecars and `history/` directory are picked up from the same
directory automatically.

Disk budget during setup: **~245 GiB free** (73 GiB archive +
168 GiB extracted; you can reclaim the 73 GiB after step 4).

### Option B — boot from genesis (no download)

Quickest way to try the library if you don't need recent state. Load
the shipped genesis and execute era blocks forward from block 0:

```python
with Engine(Config(data_dir="./state")) as engine:
    engine.load_genesis("data/mainnet_genesis.json")
    # then iter_era_blocks(...) + execute_block(...) as in the replay snippet
```

Good for API experiments, correctness work, and small replays.
Replaying mainnet from block 0 to the current tip is many days of
compute, so this is **not** a fast path to a recent block.

### Option C — build your own snapshot via `chain_replay`

If you need a different target block than the one we publish, or
want to reproduce the snapshot build from scratch:

```bash
# 1. Fetch era files covering the range you want:
./data/download_era1.sh            # pre-merge (~500 GB full; fetch what you need)
./data/download_era.sh             # post-merge

# 2. Build the replay tool:
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target chain_replay -j

# 3. Replay forward from genesis, saving a snapshot at block N:
./build/chain_replay --data-dir ~/.artex --save-state 24000000

# 4. Generate the mandatory .hashes sidecar:
python3 tools/make_hashes.py \
    --state ~/.artex/state_24000000.bin --era-dir data/era
```

Wall time: hours to days depending on target block. Option A is
much faster if the published block height works for your use case.

## Running EELS tests

`test_runner_batch` drives the library against the Ethereum Execution
Layer Specification (EELS) test fixtures.

Build it:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target test_runner_batch -j
```

Fetch the latest EELS fixture pack from
<https://github.com/ethereum/execution-spec-tests/releases> (the
`fixtures_stable.tar.gz` asset is the usual target) and extract into
`integration_tests/fixtures/`. The expected layout is:

```
integration_tests/fixtures/
├── state_tests/
├── blockchain_tests/
└── blockchain_tests_engine/
```

Run a suite by pointing the binary at any directory of fixtures:

```bash
./build/test_runner_batch integration_tests/fixtures/state_tests/
./build/test_runner_batch integration_tests/fixtures/blockchain_tests/
./build/test_runner_batch integration_tests/fixtures/blockchain_tests_engine/
```

Useful flags:

| flag | effect |
|---|---|
| `-v` | print per-test output, not just the roll-up |
| `-s` | stop on first failure |
| `-f <fork>` | restrict to one fork (e.g. `-f Cancun`); can be repeated |
| `-t <ms>` | per-test timeout (default 30 s) |

Each run prints a summary: `Passed: N  Failed: M  Errors: K  Skipped: L`.

## License

This project is licensed under the **[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html)** (LGPLv3). See the [COPYING](COPYING) file for details.
