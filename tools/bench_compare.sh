#!/bin/bash
# Benchmark: art evm_statetest vs evmone-statetest
# Runs each fixture N times (no tracing) and compares wall-clock execution time.

set -euo pipefail

ART_BIN="${ART_BIN:-/home/racytech/workspace/art/build/evm_statetest}"
EVMONE_BIN="${EVMONE_BIN:-/home/racytech/workspace/evmone/build/bin/evmone-statetest}"
BENCH_DIR="${BENCH_DIR:-/home/racytech/workspace/evmone/test/evm-benchmarks/benchmarks}"
ITERATIONS="${ITERATIONS:-20}"
WARMUP="${WARMUP:-3}"

if [ ! -x "$ART_BIN" ]; then echo "art binary not found: $ART_BIN"; exit 1; fi
if [ ! -x "$EVMONE_BIN" ]; then echo "evmone binary not found: $EVMONE_BIN"; exit 1; fi

printf "%-28s %12s %12s %10s\n" "Benchmark" "art (ms)" "evmone (ms)" "ratio"
printf "%-28s %12s %12s %10s\n" "---" "---" "---" "---"

for category in main micro; do
    if [ ! -d "$BENCH_DIR/$category" ]; then continue; fi
    for fixture in "$BENCH_DIR/$category"/*.json; do
        name="$category/$(basename "$fixture" .json)"

        # Warmup
        for ((i = 0; i < WARMUP; i++)); do
            "$ART_BIN" "$fixture" >/dev/null 2>/dev/null || true
            "$EVMONE_BIN" "$fixture" >/dev/null 2>/dev/null || true
        done

        # Benchmark art
        art_total=0
        for ((i = 0; i < ITERATIONS; i++)); do
            t0=$(date +%s%N)
            "$ART_BIN" "$fixture" >/dev/null 2>/dev/null || true
            t1=$(date +%s%N)
            elapsed=$(( (t1 - t0) / 1000000 ))
            art_total=$((art_total + elapsed))
        done
        art_avg=$((art_total / ITERATIONS))

        # Benchmark evmone
        evmone_total=0
        for ((i = 0; i < ITERATIONS; i++)); do
            t0=$(date +%s%N)
            "$EVMONE_BIN" "$fixture" >/dev/null 2>/dev/null || true
            t1=$(date +%s%N)
            elapsed=$(( (t1 - t0) / 1000000 ))
            evmone_total=$((evmone_total + elapsed))
        done
        evmone_avg=$((evmone_total / ITERATIONS))

        # Compute ratio
        if [ "$evmone_avg" -gt 0 ]; then
            # ratio = art/evmone, <1.0 means art is faster
            ratio=$(echo "scale=2; $art_avg / $evmone_avg" | bc)
        else
            ratio="N/A"
        fi

        printf "%-28s %10d ms %10d ms %10sx\n" "$name" "$art_avg" "$evmone_avg" "$ratio"
    done
done
