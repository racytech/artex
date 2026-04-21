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

### Why hashed keys

Feeding keys through keccak-256 before insert turns every key into a
uniformly-distributed 32-byte string. Two consequences:

- ART's adaptive node layouts always land in their sweet spot — no
  pathological long chains from sequential key patterns (nonce-like
  slot indices, sequential contract addresses, and similar).
- We drop the **path compression** (prefix storage inside internal
  nodes) that textbook ART uses to collapse single-child chains:
  hashed keys essentially never share a prefix past the first byte
  or two, so the bookkeeping would cost more than it saves. Our
  nodes carry no `prefix[]` field; lookup is one byte → one branch
  → one step.

### What else ART buys

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
- **No compaction, no rebalancing, no write amplification.** Inserts
  and deletes mutate one or two nodes and are done. Nothing like an
  LSM-tree merge pass or B-tree split cascade.

### Implementation choices on top

- **Slab-chained pool allocation.** All ART nodes live in a single
  anonymous mmap pool, sliced into per-account slab chains that
  never get moved on grow. No memcpy storms when a contract's
  storage expands, no reallocation tails, bounded fragmentation.
- **Fully in-memory on the hot path.** No explicit buffer pool, no
  I/O in the read/write path. The shared pool is one `MAP_PRIVATE`
  mmap; Linux pages cold accounts out transparently via swap.

### Tradeoffs

- **No on-disk durability story.** After a crash, recovery follows
  the same path as a cold start — load the most recent snapshot,
  then replay forward from there.

## Performance

These are measurements from an actual mainnet replay on one
developer workstation. They're not marketing numbers — different
hardware and fork timing will move them around. Treat them as a
sanity baseline, not a spec.

**At a glance** (one run, one machine, 49,017 blocks; details below):

- **13.3 blk/s / 404 Mgas/s** average, bursting to ~30 blk/s /
  ~880 Mgas/s between root-validation points.
- **~105 min cold-start sync** for this 49,017-block window:
  ~35 min to load + verify a 140 GB snapshot, ~70 min to replay.
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
| **RAM + swap (combined)** | 92 GB RAM + 254 GB swap | Load the 140 GB snapshot and have room to grow. Measured during replay: **RSS 90 GB + Swap 142 GB ≈ 232 GB total**, `VmData` ~234 GB. |
| **Swap media** | NVMe | Cold-account page faults dominate steady state; SATA SSD is a noticeable bottleneck, spinning disk is orders of magnitude worse. |
| **Free disk** | ~300 GB | 140 GB snapshot + 16 GB code store + a few recent era files + headroom to save a new snapshot. |
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
| `state_load` + full root re-verification | **~35 min** | Cold page cache. Reads 140 GB from disk, then walks the entire account + storage tries and rehashes every node from scratch. `load_state` deliberately marks all nodes dirty so the first root computation traverses everything — that's how it verifies the snapshot's integrity against the header. Subsequent root computations are incremental (only dirty subtrees); see [What makes it different](#what-makes-it-different). |
| Forward replay (per-block gas, per-256 root) | **~70 min** | 49,017 blocks. |
| **Total** | **~105 min** | From engine construction to a state caught up to block 24,913,378. |

Scaled linearly, closing a larger gap is roughly `35 min +
N_blocks / 13.3 s`. The load cost is paid once per process; keeping
the engine alive and executing blocks in a loop avoids it on every
subsequent block.

Step-by-step to reproduce the run on your own machine — prereq
checklist, exact commands, expected output, and useful variations —
is in [`examples/python/replay_walkthrough.md`](examples/python/replay_walkthrough.md).

### Major-fault profile

Ballpark fault rates during a bulk replay (measured on an earlier
pool design; the current `hart_pool` / `storage_hart2` layout may
shift the steady-state numbers but the shape is the same):

- First ~30 seconds: 20k–34k major faults/s as the hot working set
  gets pulled back into RAM from swap
- Settling to ~1,500/s after ~5 minutes
- Steady state (warm hot set): ~700/s average, with bursts to
  ~3,500/s when a block touches a dormant contract

These rates depend heavily on the swap device. With tmpfs or enough
RAM to hold the whole state, you'd see close to zero major faults
and the throughput numbers above would move up.

### Scope and limitations

- **Platforms.** Linux x86_64 only today. The pool relies on
  `MAP_ANONYMOUS | MAP_PRIVATE` + `mremap` for in-place growth and on
  a POSIX threading model. No macOS, Windows, or ARM64 builds.

## Quick start

Three things you need: `libartex.so` (see [Installation](#installation)
below), a snapshot package (see [Data files](#data-files) — hosted
download, genesis boot, or self-build), and your language of choice.
Below: how to build the library, then the same engine driven from
Python and C.

### Installation

Ubuntu / Debian:

```bash
sudo apt install build-essential cmake pkg-config \
    libcjson-dev libssl-dev libsnappy-dev
```

Build the shared library from the repo root:

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

Fedora / RHEL package names, test-fixture setup, and troubleshooting
live in [`examples/python/README.md`](examples/python/README.md).

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

The snippets above load a **snapshot package** — four files that
travel together in one directory:

```
state_<block>.bin          # ART state (accounts + storage + MPT roots)
state_<block>.bin.hashes   # last 256 block hashes — required by load_state
chain_replay_code.dat      # contract bytecode blobs (content-addressed)
chain_replay_code.idx      # code_store index
```

`load_state` refuses to open a snapshot whose `.hashes` sidecar is
missing — the `BLOCKHASH` opcode would otherwise silently return zero
and any contract depending on that opcode would diverge from mainnet.

Three ways to get a package:

### Option A — download the hosted snapshot

Published block: **24,864,361**. Archive: **~63 GiB compressed
(tar + zstd -9 --long=27) → ~156 GiB extracted.** Hosted on
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

# 2. Fetch the 63 GiB archive — resumable on drop. If your connection
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

Then point `load_state` at `~/.artex/state_24864361.bin` — the three
sidecars are picked up from the same directory automatically.

Disk budget during setup: **~220 GiB free** (63 GiB archive +
156 GiB extracted; you can reclaim the 63 GiB after step 4).

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

## License

TBD
