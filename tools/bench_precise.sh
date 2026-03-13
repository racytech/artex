#!/bin/bash
# Precise benchmark: measures only the heavy fixtures with many iterations
# and subtracts baseline process startup cost.

set -euo pipefail

ART_BIN="${ART_BIN:-/home/racytech/workspace/art/build/evm_statetest}"
EVMONE_BIN="${EVMONE_BIN:-/home/racytech/workspace/evmone/build/bin/evmone-statetest}"
BENCH_DIR="${BENCH_DIR:-/home/racytech/workspace/evmone/test/evm-benchmarks/benchmarks}"
ITERATIONS="${ITERATIONS:-50}"
WARMUP="${WARMUP:-5}"

# Fixtures worth benchmarking (heavy enough that process startup is negligible)
FIXTURES=(
    "main/snailtracer.json"
    "main/blake2b_shifts.json"
    "main/sha1_divs.json"
    "main/sha1_shifts.json"
    "main/blake2b_huff.json"
    "main/weierstrudel.json"
    "micro/loop_with_many_jumpdests.json"
)

bench_one() {
    local bin="$1"
    local fixture="$2"
    local iters="$3"
    local total=0
    local min=999999
    local max=0

    for ((i = 0; i < iters; i++)); do
        t0=$(date +%s%N)
        "$bin" "$fixture" >/dev/null 2>/dev/null || true
        t1=$(date +%s%N)
        elapsed_us=$(( (t1 - t0) / 1000 ))
        total=$((total + elapsed_us))
        if [ "$elapsed_us" -lt "$min" ]; then min=$elapsed_us; fi
        if [ "$elapsed_us" -gt "$max" ]; then max=$elapsed_us; fi
    done

    avg=$((total / iters))
    echo "$avg $min $max"
}

printf "%-30s %14s %14s %14s %14s %8s\n" \
    "Benchmark" "art avg(us)" "art min(us)" "evmone avg(us)" "evmone min(us)" "ratio"
printf "%s\n" "$(printf '=%.0s' {1..100})"

for rel in "${FIXTURES[@]}"; do
    fixture="$BENCH_DIR/$rel"
    if [ ! -f "$fixture" ]; then
        echo "SKIP: $rel (not found)"
        continue
    fi

    name="${rel%.json}"

    # Warmup both
    for ((i = 0; i < WARMUP; i++)); do
        "$ART_BIN" "$fixture" >/dev/null 2>/dev/null || true
        "$EVMONE_BIN" "$fixture" >/dev/null 2>/dev/null || true
    done

    read art_avg art_min art_max <<< $(bench_one "$ART_BIN" "$fixture" "$ITERATIONS")
    read evm_avg evm_min evm_max <<< $(bench_one "$EVMONE_BIN" "$fixture" "$ITERATIONS")

    # Use min times (least noisy) for ratio
    if [ "$evm_min" -gt 0 ]; then
        ratio=$(echo "scale=2; $art_min / $evm_min" | bc)
    else
        ratio="N/A"
    fi

    printf "%-30s %12d %12d %12d %12d %8sx\n" \
        "$name" "$art_avg" "$art_min" "$evm_avg" "$evm_min" "$ratio"
done
