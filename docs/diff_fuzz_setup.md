# Differential Fuzzing Setup with GoEVMLab

Cross-client differential fuzzing using GoEVMLab's `generic-fuzzer` to compare
art's EVM against geth and evmone. Finds consensus bugs by generating random
state tests and comparing EIP-3155 traces + state roots across all three clients.

## Prerequisites

- Go 1.22+ (for building goevmlab and geth)
- GCC/Clang with C11 support (for building art and evmone)
- CMake 3.16+
- Git

## 1. Clone repositories

```bash
mkdir -p ~/workspace && cd ~/workspace

git clone git@github.com:racytech/art.git
git clone git@github.com:ethereum/go-ethereum.git
git clone --recursive git@github.com:ethereum/evmone.git
git clone git@github.com:holiman/goevmlab.git
```

## 2. Build art (evm_statetest)

```bash
cd ~/workspace/art
git checkout evm-diff-fuzz

mkdir -p build && cd build
cmake .. -DENABLE_EVM_TRACE=ON
make -j$(nproc) evm_statetest
```

Binary: `~/workspace/art/build/evm_statetest`

## 3. Build geth (evm)

```bash
cd ~/workspace/go-ethereum
make all
```

Binary: `~/workspace/go-ethereum/build/bin/evm`

## 4. Build evmone

```bash
cd ~/workspace/evmone
mkdir -p build && cd build
cmake .. -DEVMONE_TESTING=ON
make -j$(nproc) evmone-statetest
```

Binary: `~/workspace/evmone/build/bin/evmone-statetest`

## 5. Patch and build GoEVMLab

GoEVMLab needs the art adapter registered. The changes are in two files:

### evms/art.go (new file)

Copy from `~/workspace/art` repo or create manually — implements the `Evm`
interface for `evm_statetest`. The file lives at `~/workspace/goevmlab/evms/art.go`.

### common/utils.go (add art flag + registration)

Add `ArtFlag` alongside the other VM flags:

```go
ArtFlag = &cli.StringSliceFlag{
    Name:  "art",
    Usage: "Location of art 'evm_statetest' binary",
}
```

Add `ArtFlag` to the `VMFlags` slice, and add this line in `InitVMs()`:

```go
addVM(ArtFlag.Name, evms.NewArtVM)
```

### Build goevmlab binaries

```bash
cd ~/workspace/goevmlab
go build -o ./bin/generic-fuzzer ./cmd/generic-fuzzer/
go build -o ./bin/generic-generator ./cmd/generic-generator/
go build -o ./bin/runtest ./cmd/runtest/
```

## 6. Run the fuzzer

### Quick smoke test (single file)

```bash
cd ~/workspace/goevmlab

./bin/runtest \
  --art ~/workspace/art/build/evm_statetest \
  --geth ~/workspace/go-ethereum/build/bin/evm \
  --evmone ~/workspace/evmone/build/bin/evmone-statetest \
  --verbosity -4 \
  ~/workspace/goevmlab/evms/testdata/cases/statetest_filled.json
```

All three VMs should execute with no "Consensus error" in the output.

### Generate test cases only

```bash
mkdir -p /tmp/tests

./bin/generic-generator \
  --outdir /tmp/tests \
  --count 100 \
  --engine naive \
  --fork Cancun
```

### Continuous fuzzing (generate + compare in a loop)

```bash
mkdir -p /tmp/fuzz_out

./bin/generic-fuzzer \
  --art ~/workspace/art/build/evm_statetest \
  --geth ~/workspace/go-ethereum/build/bin/evm \
  --evmone ~/workspace/evmone/build/bin/evmone-statetest \
  --engine naive \
  --fork Cancun \
  --outdir /tmp/fuzz_out \
  --verbosity 0
```

Stops on first consensus flaw. The failing test case is saved in `--outdir`
along with per-VM trace output files.

### Run on pre-generated tests

```bash
./bin/runtest \
  --art ~/workspace/art/build/evm_statetest \
  --geth ~/workspace/go-ethereum/build/bin/evm \
  --evmone ~/workspace/evmone/build/bin/evmone-statetest \
  --verbosity -4 \
  "/tmp/tests/*.json"
```

## 7. Available engines

The `--engine` flag selects what kind of bytecode the fuzzer generates:

| Engine        | Description                          |
|---------------|--------------------------------------|
| `naive`       | Random opcodes with precompile calls |
| `simpleops`   | Basic arithmetic/logic operations    |
| `memops`      | Memory-heavy operations              |
| `sstore_sload`| Storage read/write patterns          |
| `ecrecover`   | ECRECOVER precompile calls           |
| `bn254`       | BN254 precompile calls               |
| `modexp`      | MODEXP precompile calls              |
| `blake`       | BLAKE2F precompile calls             |
| `precompiles` | Mixed precompile calls               |
| `tstore_tload`| Transient storage (EIP-1153)         |

Omit `--engine` to use all engines (round-robin).

## 8. Available forks

`Berlin`, `London`, `Shanghai`, `Cancun`, `Prague`

Use `--fork` to select. Default is `Prague`.

## 9. Interpreting results

On consensus flaw, the fuzzer prints:

```
Consensus error
Testcase: /tmp/fuzz_out/00004042-mixed-8.json
- geth-0: /tmp/fuzz_out/geth-0-output.jsonl
- evmone-0: /tmp/fuzz_out/evmone-0-output.jsonl
- art-0: /tmp/fuzz_out/art-0-output.jsonl
```

The diff shows the first diverging trace line. To investigate:

```bash
# View the trace diff
cat /tmp/fuzz_out/art-0-output.jsonl | head -20
cat /tmp/fuzz_out/geth-0-output.jsonl | head -20

# Re-run art with full trace to inspect
~/workspace/art/build/evm_statetest --trace /tmp/fuzz_out/00004042-mixed-8.json

# Compare stateRoot only (faster)
~/workspace/art/build/evm_statetest --trace-summary /tmp/fuzz_out/00004042-mixed-8.json
```

## 10. Useful flags

| Flag              | Description                                    |
|-------------------|------------------------------------------------|
| `--parallel N`    | Number of parallel test executions (default: nproc) |
| `--skiptrace`     | Compare only stateRoot, skip trace comparison (faster but less precise) |
| `--verbosity -4`  | DEBUG level (show per-test execution times)    |
| `--cleanupFiles`  | Remove test files after successful execution   |
