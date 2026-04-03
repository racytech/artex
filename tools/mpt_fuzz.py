#!/usr/bin/env python3
"""
Differential fuzz test: hart vs Python HexaryTrie.

Infinite loop that inserts N random key-value pairs into both tries,
computes roots, and asserts they match. Catches any divergence immediately.

Usage:
    python3 tools/mpt_fuzz.py [--n 100] [--lib build/libmpt_fuzz.so]
"""

import argparse
import ctypes
import os
import random
import sys
import time

from trie import HexaryTrie

# ============================================================================
# Load shared library
# ============================================================================

def load_lib(path):
    lib = ctypes.CDLL(path)

    # --- hashed_art (hart) ---
    lib.hart_ctx_create.restype  = ctypes.c_void_p
    lib.hart_ctx_create.argtypes = []

    lib.hart_ctx_destroy.restype  = None
    lib.hart_ctx_destroy.argtypes = [ctypes.c_void_p]

    lib.hart_ctx_insert.restype  = None
    lib.hart_ctx_insert.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    ]

    lib.hart_ctx_delete.restype  = None
    lib.hart_ctx_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.hart_ctx_root.restype  = ctypes.c_bool
    lib.hart_ctx_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    return lib


# ============================================================================
# Helpers
# ============================================================================

def random_key():
    """Random 32-byte key (pre-hashed, as Ethereum MPT expects)."""
    return os.urandom(32)

def random_value():
    """Random 32-byte value (fixed size, matching hart storage slots)."""
    return os.urandom(32)

# ============================================================================
# One round
# ============================================================================

def run_round(lib, rnd, n, state, hart_ctx):
    """
    Insert/update N random keys across both tries.
    On odd rounds, also delete ~10% of existing keys.
    """
    # Generate operations
    ops = []  # list of (key, value_or_None)

    # Inserts / updates
    for _ in range(n):
        key = random_key()
        val = random_value()
        ops.append((key, val))

    # Deletions on odd rounds
    if rnd % 2 == 1 and len(state) > 0:
        n_del = max(1, len(state) // 10)
        del_keys = random.sample(list(state.keys()), min(n_del, len(state)))
        for k in del_keys:
            ops.append((k, None))

    # --- Python HexaryTrie ---
    py_trie = HexaryTrie({})
    for k, v in state.items():
        py_trie[k] = v
    for k, v in ops:
        if v is None:
            if k in state:
                del py_trie[k]
        else:
            py_trie[k] = v
    py_root = py_trie.root_hash

    # --- Update state dict ---
    for k, v in ops:
        if v is None:
            state.pop(k, None)
        else:
            state[k] = v

    # --- hashed_art (hart, incremental) ---
    for k, v in ops:
        if v is None:
            lib.hart_ctx_delete(hart_ctx, k)
        else:
            lib.hart_ctx_insert(hart_ctx, k, v, len(v))
    hart_root = ctypes.create_string_buffer(32)
    lib.hart_ctx_root(hart_ctx, hart_root)
    hart_root = hart_root.raw

    # --- Compare ---
    match = (py_root == hart_root)
    return state, py_root, hart_root, match


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="MPT differential fuzz test")
    parser.add_argument("--n", type=int, default=100,
                        help="keys per round (default: 100)")
    parser.add_argument("--lib", type=str,
                        default="build/libmpt_fuzz.so",
                        help="path to libmpt_fuzz.so")
    parser.add_argument("--seed", type=int, default=None,
                        help="random seed (default: random)")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        seed = args.seed
    else:
        seed = random.randint(0, 2**32 - 1)
        random.seed(seed)

    lib = load_lib(args.lib)

    print(f"seed={seed}  n={args.n}")
    print(f"backends: python(HexaryTrie) vs hart")
    print(f"{'round':>6}  {'keys':>6}  {'state':>7}  {'time':>8}  status")
    print("-" * 55)

    hart_ctx = lib.hart_ctx_create()
    assert hart_ctx, "hart_ctx_create failed"

    state = {}
    rnd = 0

    try:
        while True:
            t0 = time.monotonic()
            state, py, hart, match = run_round(
                lib, rnd, args.n, state, hart_ctx)
            dt = time.monotonic() - t0

            status = "OK" if match else "MISMATCH"
            print(f"{rnd:6d}  {args.n:6d}  {len(state):7d}  {dt:7.3f}s  {status}")

            if not match:
                print(f"\n  MISMATCH at round {rnd}!")
                print(f"  python: {py.hex()}")
                print(f"  hart:   {hart.hex()}")
                print(f"  seed={seed}")
                sys.exit(1)

            rnd += 1

    except KeyboardInterrupt:
        print(f"\nStopped after {rnd} rounds. All matched.")
    finally:
        lib.hart_ctx_destroy(hart_ctx)


if __name__ == "__main__":
    main()
