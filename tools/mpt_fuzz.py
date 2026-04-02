#!/usr/bin/env python3
"""
Differential fuzz test: mem_art_mpt vs Python HexaryTrie.

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

    # --- mem_mpt batch wrapper ---
    # lib.batch_create.restype  = ctypes.c_void_p
    # lib.batch_create.argtypes = []
    # lib.batch_add.restype  = None
    # lib.batch_add.argtypes = [
    #     ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    # ]
    # lib.batch_root.restype  = ctypes.c_bool
    # lib.batch_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    # lib.batch_reset.restype  = None
    # lib.batch_reset.argtypes = [ctypes.c_void_p]
    # lib.batch_destroy.restype  = None
    # lib.batch_destroy.argtypes = [ctypes.c_void_p]

    # --- art_mpt (compact_art backend) ---
    # lib.art_mpt_ctx_create.restype  = ctypes.c_void_p
    # lib.art_mpt_ctx_create.argtypes = []
    # lib.art_mpt_ctx_destroy.restype  = None
    # lib.art_mpt_ctx_destroy.argtypes = [ctypes.c_void_p]
    # lib.art_mpt_ctx_insert.restype  = None
    # lib.art_mpt_ctx_insert.argtypes = [
    #     ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    # ]
    # lib.art_mpt_ctx_delete.restype  = None
    # lib.art_mpt_ctx_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    # lib.art_mpt_ctx_root.restype  = ctypes.c_bool
    # lib.art_mpt_ctx_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    # --- mem_art_mpt (mem_art + art_iface_mem backend) ---
    lib.mem_art_mpt_ctx_create.restype  = ctypes.c_void_p
    lib.mem_art_mpt_ctx_create.argtypes = []

    lib.mem_art_mpt_ctx_destroy.restype  = None
    lib.mem_art_mpt_ctx_destroy.argtypes = [ctypes.c_void_p]

    lib.mem_art_mpt_ctx_insert.restype  = None
    lib.mem_art_mpt_ctx_insert.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    ]

    lib.mem_art_mpt_ctx_delete.restype  = None
    lib.mem_art_mpt_ctx_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.mem_art_mpt_ctx_root.restype  = ctypes.c_bool
    lib.mem_art_mpt_ctx_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    return lib


# ============================================================================
# Helpers
# ============================================================================

def random_key():
    """Random 32-byte key (pre-hashed, as Ethereum MPT expects)."""
    return os.urandom(32)

def random_value():
    """Random value, 1-128 bytes."""
    n = random.randint(1, 128)
    return os.urandom(n)

# ============================================================================
# One round
# ============================================================================

def run_round(lib, rnd, n, state, mart_ctx):
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

    # --- mem_art_mpt (mem_art + art_iface_mem, incremental) ---
    for k, v in ops:
        if v is None:
            lib.mem_art_mpt_ctx_delete(mart_ctx, k)
        else:
            lib.mem_art_mpt_ctx_insert(mart_ctx, k, v, len(v))
    mart_root = ctypes.create_string_buffer(32)
    lib.mem_art_mpt_ctx_root(mart_ctx, mart_root)
    mart_root = mart_root.raw

    # --- Compare ---
    match = (py_root == mart_root)
    return state, py_root, mart_root, match


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
    print(f"backends: python(HexaryTrie) vs mem_art_mpt(mem_art)")
    print(f"{'round':>6}  {'keys':>6}  {'state':>7}  {'time':>8}  status")
    print("-" * 55)

    mart_ctx = lib.mem_art_mpt_ctx_create()
    assert mart_ctx, "mem_art_mpt_ctx_create failed"

    state = {}
    rnd = 0

    try:
        while True:
            t0 = time.monotonic()
            state, py, mart, match = run_round(
                lib, rnd, args.n, state, mart_ctx)
            dt = time.monotonic() - t0

            status = "OK" if match else "MISMATCH"
            print(f"{rnd:6d}  {args.n:6d}  {len(state):7d}  {dt:7.3f}s  {status}")

            if not match:
                print(f"\n  MISMATCH at round {rnd}!")
                print(f"  python:      {py.hex()}")
                print(f"  mem_art_mpt: {mart.hex()}")
                print(f"  seed={seed}")
                sys.exit(1)

            rnd += 1

    except KeyboardInterrupt:
        print(f"\nStopped after {rnd} rounds. All matched.")
    finally:
        lib.mem_art_mpt_ctx_destroy(mart_ctx)


if __name__ == "__main__":
    main()
