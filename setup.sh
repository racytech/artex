#!/usr/bin/env bash
#
# setup.sh — prepare third-party dependencies for a fresh clone.
#
# What libartex links against and where each piece comes from:
#
#   gmp         -- vendored directly in the repo (third_party/gmp/)
#   blst        -- git submodule, built via its own build.sh
#   secp256k1   -- git submodule, built via cmake (static, PIC)
#
# This script handles the two submodule builds and the initial
# submodule fetch. Run it once after cloning, then the normal
# `cmake --build build --target artex_shared` works.
#
# Idempotent: safe to rerun after a `git pull`. Skips anything
# that's already built unless --force is passed.
#
# Usage:
#   ./setup.sh                Build what's missing
#   ./setup.sh --force        Rebuild blst + secp256k1 from scratch
#   ./setup.sh -h | --help    Show this help

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

FORCE=0
for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        -h|--help)
            sed -n '2,22p' "$0" | sed 's/^#//; s/^ //'
            exit 0 ;;
        *)
            echo "ERROR: unknown flag '$arg' (try --help)" >&2
            exit 2 ;;
    esac
done

# ---------------------------------------------------------------
# Tool check
# ---------------------------------------------------------------
missing=()
for tool in git cmake make cc perl; do
    command -v "$tool" >/dev/null || missing+=("$tool")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: missing tools on PATH: ${missing[*]}" >&2
    echo "       On Debian/Ubuntu: sudo apt install git cmake build-essential perl" >&2
    exit 1
fi

# ---------------------------------------------------------------
# [1/3] Submodule sources
# ---------------------------------------------------------------
echo "[1/3] fetching submodule sources (blst, secp256k1) ..."
git submodule update --init --recursive
echo "      done."
echo

# ---------------------------------------------------------------
# [2/3] blst — static library with PIC (its build.sh defaults to -fPIC)
# ---------------------------------------------------------------
BLST_A="$ROOT/third_party/blst/libblst.a"
echo "[2/3] building blst (static, PIC) ..."
if [[ $FORCE -eq 0 && -f "$BLST_A" ]]; then
    echo "      libblst.a already present — skipping (use --force to rebuild)"
else
    ( cd third_party/blst && ./build.sh )
    if [[ ! -f "$BLST_A" ]]; then
        echo "ERROR: blst build did not produce $BLST_A" >&2
        exit 1
    fi
fi
echo

# ---------------------------------------------------------------
# [3/3] secp256k1 — static library, PIC, with recovery + ecdh modules
# ---------------------------------------------------------------
SECP_A="$ROOT/third_party/secp256k1/build/lib/libsecp256k1.a"
echo "[3/3] building secp256k1 (static, PIC, +recovery +ecdh +extrakeys +schnorrsig) ..."
if [[ $FORCE -eq 0 && -f "$SECP_A" ]]; then
    echo "      libsecp256k1.a already present — skipping (use --force to rebuild)"
else
    cd third_party/secp256k1
    cmake -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DSECP256K1_BUILD_BENCHMARK=OFF \
        -DSECP256K1_BUILD_TESTS=OFF \
        -DSECP256K1_BUILD_EXHAUSTIVE_TESTS=OFF \
        -DSECP256K1_BUILD_EXAMPLES=OFF \
        -DSECP256K1_ENABLE_MODULE_RECOVERY=ON \
        -DSECP256K1_ENABLE_MODULE_ECDH=ON \
        -DSECP256K1_ENABLE_MODULE_EXTRAKEYS=ON \
        -DSECP256K1_ENABLE_MODULE_SCHNORRSIG=ON
    cmake --build build -j
    cd "$ROOT"
    if [[ ! -f "$SECP_A" ]]; then
        echo "ERROR: secp256k1 build did not produce $SECP_A" >&2
        exit 1
    fi
fi
echo

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo "=== setup complete ==="
ls -lh \
    "$ROOT/third_party/gmp/lib/libgmp.a" \
    "$ROOT/third_party/blst/libblst.a" \
    "$ROOT/third_party/secp256k1/build/lib/libsecp256k1.a"
echo
echo "Next: build libartex:"
echo "  cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON"
echo "  cmake --build build --target artex_shared -j"
