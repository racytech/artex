# artex Python examples

Interactive and scriptable Python examples for the `libartex` execution
engine. Use them to load a mainnet state snapshot, query accounts,
execute blocks from era files, and compute state roots.

| file | what it is |
|---|---|
| `artex_cli.py` | Interactive REPL. The main example. |
| `example_query.py` | One-shot snapshot query (scriptable). |
| `artex.py` | High-level `Engine` wrapper (imported by the examples). |
| `era.py` | Post-merge era file reader. |

## Prerequisites

### 1. System packages

`libartex` links a few system libraries. On Debian/Ubuntu:

```bash
sudo apt install build-essential cmake pkg-config \
    libcjson-dev libssl-dev libsnappy-dev
```

On Fedora/RHEL:

```bash
sudo dnf install gcc cmake pkgconf-pkg-config \
    cjson-devel openssl-devel snappy-devel
```

On macOS (Homebrew):

```bash
brew install cmake cjson openssl snappy
```

The rest of the native deps are bundled as pre-built static libraries in
`third_party/` (no action needed):
- **`gmp`** — arbitrary-precision arithmetic (used by EVM for u256 ops)
- **`secp256k1`** — ECDSA signature verification (Bitcoin's, since it's
  faster than OpenSSL's for this curve)
- **`blst`** — BLS12-381 signatures (for KZG blob verification, EIP-4844)

### 2. Build `libartex` (the shared library only)

From the repo root:

```bash
cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build build --target artex_shared -j
```

This builds only `libartex.so` and its direct dependencies — not the
tests, benchmarks, or other tools in the repo. Much faster than a full
build (~3–10 s depending on cache state).

The examples find `build/libartex.so` automatically. Override with the
`ARTEX_LIB` environment variable if it lives elsewhere.

### 3. Install Python deps

```bash
cd examples/python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Snapshot files

The examples need **four files** that travel together:

```
state_NNNNNNNN.bin          # the state (accounts, storage, MPT roots)
state_NNNNNNNN.bin.hashes   # last 256 block hashes (REQUIRED; 8192 B sidecar)
chain_replay_code.dat       # contract bytecode blobs (content-addressed)
chain_replay_code.idx       # code_store index
```

All four live in the **same directory**. Unpack the snapshot archive
anywhere you like.

The `.hashes` sidecar is required because `rx_engine_load_state`
needs to populate the BLOCKHASH ring (last 256 block hashes) — without
it, the `BLOCKHASH` opcode would silently return zero and any contract
depending on it would diverge. If your snapshot doesn't ship with
`.hashes`, generate it from era files with `tools/make_hashes.py`:

```bash
python3 tools/make_hashes.py \
    --state /path/to/state_NNNNNNNN.bin \
    --era-dir /path/to/era
```

Examples accept `--state <path>` and infer the data directory from
it (the directory containing the state file is used for the two code
files). Override with `--data-dir` if they live elsewhere.

Download URL: **TODO** (publish pending)

### 5. Era files (only if you use `execute`)

Post-merge era files contain SSZ-encoded beacon blocks. Download from:

- **`https://mainnet.era.nimbus.team/`** (post-merge `.era`, Paris+)

Or use the helper script:

```bash
./data/download_era.sh <target_slot>
```

## Running the interactive CLI

The CLI requires a state snapshot path on startup (`--state`). The
directory containing the snapshot must also hold the three sidecar
files (`.hashes`, `chain_replay_code.dat`, `chain_replay_code.idx`).
The CLI validates them upfront and refuses to launch with a clear
error if anything is missing.

```bash
python3 artex_cli.py --state /path/to/state_24864361.bin
```

Session (state is loaded automatically at startup):

```
libartex version: 0.1.0
data dir: /path/to
  loading /path/to/state_24864361.bin ...
recomputing state root on state load (block 24864361)...
  loaded state at block 24864361 (95.3s)

artex> version
libartex 0.1.0

artex> block
24864361

artex> root
0x7a8f0d1c...

artex> balance 0xdAC17F958D2ee523a2206206994597C13D831ec7
0 ETH

artex> nonce 0xdAC17F958D2ee523a2206206994597C13D831ec7
1

artex> storage 0xdAC17F958D2ee523a2206206994597C13D831ec7 0
  0x000000000000000000000000c6cde7c39eb2f0f0095f41570af89efc2c1ea828

artex> execute data/era --to 24865000 --every 256
  replaying from 24864362 → 24865000 (every 256 validation)
  ...

artex> save-state /tmp/snap.bin
artex> quit
```

### Available commands

| command | description |
|---|---|
| `version` | libartex version |
| `block` | current block number |
| `root` | compute current state root |
| `load-state <path>` | load a state snapshot |
| `load-genesis <path>` | load genesis JSON |
| `save-state [name]` | write state snapshot to the load-time data dir (default name: `state_<block>.bin`). `.hashes` sidecar is written automatically. |
| `balance <addr>` | ETH balance |
| `nonce <addr>` | transaction count |
| `code-size <addr>` | bytecode size in bytes |
| `code-hash <addr>` | keccak256(code) |
| `exists <addr>` | account existence |
| `storage <addr> <slot>` | read storage slot |
| `execute <era-dir> [--to N] [--every N \| --adaptive]` | replay blocks |
| `quit` | exit |

### `execute` validation modes

- `--every N` — compute + verify state root every N blocks (fixed)
- `--adaptive` — pick the interval from distance to `--to`:

  | distance to `--to` | interval |
  |---|---|
  | > 100,000 | every 1024 blocks |
  | > 10,000  | every 256 |
  | > 1,000   | every 64 |
  | > 100     | every 16 |
  | ≤ 100     | every 1 |

## Running the scriptable query

```bash
python3 example_query.py \
    --state /path/to/state_24864361.bin \
    --slot 0 \
    0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
```

## Troubleshooting

**`libartex.so not found`**
Build the project or set `ARTEX_LIB=/path/to/libartex.so`.

**`no mainnet-*.era files in ...`**
Pass the correct directory to `execute` as the first positional argument.

**State loads but queries return empty / block execution fails with
unknown code**
The code_store files (`chain_replay_code.dat`, `.idx`) are missing from
the snapshot directory. Put all four files (`state_*.bin`,
`.bin.hashes`, `.dat`, `.idx`) in the same folder.

**`load-state` fails: `missing <path>.hashes`**
The snapshot's sidecar is not present. Generate it with
`tools/make_hashes.py --state <snapshot> --era-dir <era>` (requires
era files for the last 256 blocks before the snapshot). If you don't
have era files, obtain a snapshot package that includes the sidecar.
