#!/usr/bin/env python3
"""
Differential fuzz test: mpt_store vs mem_mpt vs Python HexaryTrie.

Infinite loop that inserts N random key-value pairs into all three tries,
computes roots, and asserts they match. Catches any divergence immediately.

Usage:
    python3 tools/mpt_fuzz.py [--n 100] [--lib build/libmpt_fuzz.so]
"""

import argparse
import ctypes
import hashlib
import os
import random
import shutil
import struct
import sys
import tempfile
import time

from trie import HexaryTrie

# ============================================================================
# Load shared library
# ============================================================================

def load_lib(path):
    lib = ctypes.CDLL(path)

    # --- mpt_store ---
    lib.mpt_store_create.restype  = ctypes.c_void_p
    lib.mpt_store_create.argtypes = [ctypes.c_char_p, ctypes.c_uint64]

    lib.mpt_store_destroy.restype  = None
    lib.mpt_store_destroy.argtypes = [ctypes.c_void_p]

    lib.mpt_store_set_cache.restype  = None
    lib.mpt_store_set_cache.argtypes = [ctypes.c_void_p, ctypes.c_uint64]

    lib.mpt_store_begin_batch.restype  = ctypes.c_bool
    lib.mpt_store_begin_batch.argtypes = [ctypes.c_void_p]

    lib.mpt_store_update.restype  = ctypes.c_bool
    lib.mpt_store_update.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    ]

    lib.mpt_store_delete.restype  = ctypes.c_bool
    lib.mpt_store_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.mpt_store_commit_batch.restype  = ctypes.c_bool
    lib.mpt_store_commit_batch.argtypes = [ctypes.c_void_p]

    lib.mpt_store_root.restype  = None
    lib.mpt_store_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    # --- mem_mpt batch wrapper ---
    lib.batch_create.restype  = ctypes.c_void_p
    lib.batch_create.argtypes = []

    lib.batch_add.restype  = None
    lib.batch_add.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    ]

    lib.batch_root.restype  = ctypes.c_bool
    lib.batch_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.batch_reset.restype  = None
    lib.batch_reset.argtypes = [ctypes.c_void_p]

    lib.batch_destroy.restype  = None
    lib.batch_destroy.argtypes = [ctypes.c_void_p]

    # --- mpt_arena ---
    lib.mpt_arena_create.restype  = ctypes.c_void_p
    lib.mpt_arena_create.argtypes = []

    lib.mpt_arena_destroy.restype  = None
    lib.mpt_arena_destroy.argtypes = [ctypes.c_void_p]

    lib.mpt_arena_begin_batch.restype  = ctypes.c_bool
    lib.mpt_arena_begin_batch.argtypes = [ctypes.c_void_p]

    lib.mpt_arena_update.restype  = ctypes.c_bool
    lib.mpt_arena_update.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    ]

    lib.mpt_arena_delete.restype  = ctypes.c_bool
    lib.mpt_arena_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.mpt_arena_commit_batch.restype  = ctypes.c_bool
    lib.mpt_arena_commit_batch.argtypes = [ctypes.c_void_p]

    lib.mpt_arena_root.restype  = None
    lib.mpt_arena_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    # --- art_mpt (compact_art → MPT hash) ---
    lib.art_mpt_ctx_create.restype  = ctypes.c_void_p
    lib.art_mpt_ctx_create.argtypes = []

    lib.art_mpt_ctx_destroy.restype  = None
    lib.art_mpt_ctx_destroy.argtypes = [ctypes.c_void_p]

    lib.art_mpt_ctx_insert.restype  = None
    lib.art_mpt_ctx_insert.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
    ]

    lib.art_mpt_ctx_delete.restype  = None
    lib.art_mpt_ctx_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.art_mpt_ctx_root.restype  = ctypes.c_bool
    lib.art_mpt_ctx_root.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    return lib


# ============================================================================
# Helpers
# ============================================================================

def random_key():
    """Random 32-byte key (pre-hashed, as Ethereum MPT expects)."""
    return os.urandom(32)

def random_value():
    """Random value, 1–128 bytes."""
    n = random.randint(1, 128)
    return os.urandom(n)

def keccak(data):
    import hashlib
    return hashlib.sha3_256(data).digest()  # keccak256 in Python 3.6+

# ============================================================================
# One round
# ============================================================================

def run_round(lib, rnd, n, state, ms, ma, art_ctx, tmpdir):
    """
    Insert/update N random keys across all three tries.
    On odd rounds, also delete ~10% of existing keys.
    Returns updated state dict.
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
    # Apply existing state
    for k, v in state.items():
        py_trie[k] = v
    # Apply new ops
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

    # --- mem_mpt (batch) ---
    batch = lib.batch_create()
    for k, v in state.items():
        lib.batch_add(batch, k, v, len(v))
    mem_root = ctypes.create_string_buffer(32)
    ok = lib.batch_root(batch, mem_root)
    assert ok, "batch_root failed"
    lib.batch_destroy(batch)
    mem_root = mem_root.raw

    # --- mpt_store (incremental) ---
    ok = lib.mpt_store_begin_batch(ms)
    assert ok, "mpt_store_begin_batch failed"
    for k, v in ops:
        if v is None:
            ok = lib.mpt_store_delete(ms, k)
        else:
            ok = lib.mpt_store_update(ms, k, v, len(v))
        assert ok, "mpt_store_update/delete failed"
    ok = lib.mpt_store_commit_batch(ms)
    assert ok, "mpt_store_commit_batch failed"
    store_root = ctypes.create_string_buffer(32)
    lib.mpt_store_root(ms, store_root)
    store_root = store_root.raw

    # --- mpt_arena (incremental, in-memory) ---
    ok = lib.mpt_arena_begin_batch(ma)
    assert ok, "mpt_arena_begin_batch failed"
    for k, v in ops:
        if v is None:
            ok = lib.mpt_arena_delete(ma, k)
        else:
            ok = lib.mpt_arena_update(ma, k, v, len(v))
        assert ok, "mpt_arena_update/delete failed"
    ok = lib.mpt_arena_commit_batch(ma)
    assert ok, "mpt_arena_commit_batch failed"
    arena_root = ctypes.create_string_buffer(32)
    lib.mpt_arena_root(ma, arena_root)
    arena_root = arena_root.raw

    # --- art_mpt (compact_art → MPT hash) ---
    for k, v in ops:
        if v is None:
            lib.art_mpt_ctx_delete(art_ctx, k)
        else:
            lib.art_mpt_ctx_insert(art_ctx, k, v, len(v))
    art_root = ctypes.create_string_buffer(32)
    lib.art_mpt_ctx_root(art_ctx, art_root)
    art_root = art_root.raw

    # --- Compare ---
    match = (py_root == mem_root == store_root == arena_root == art_root)
    return state, py_root, mem_root, store_root, arena_root, art_root, match


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
    tmpdir = tempfile.mkdtemp(prefix="mpt_fuzz_")

    print(f"seed={seed}  n={args.n}  tmpdir={tmpdir}")
    print(f"{'round':>6}  {'keys':>6}  {'state':>7}  {'time':>8}  status")
    print("-" * 55)

    ms_path = os.path.join(tmpdir, "mpt").encode()
    ms = lib.mpt_store_create(ms_path, 1000000)
    assert ms, "mpt_store_create failed"
    lib.mpt_store_set_cache(ms, 64 * 1024 * 1024)

    ma = lib.mpt_arena_create()
    assert ma, "mpt_arena_create failed"

    art_ctx = lib.art_mpt_ctx_create()
    assert art_ctx, "art_mpt_ctx_create failed"

    state = {}
    rnd = 0

    try:
        while True:
            t0 = time.monotonic()
            state, py, mem, store, arena, art, match = run_round(
                lib, rnd, args.n, state, ms, ma, art_ctx, tmpdir)
            dt = time.monotonic() - t0

            status = "OK" if match else "MISMATCH"
            print(f"{rnd:6d}  {args.n:6d}  {len(state):7d}  {dt:7.3f}s  {status}")

            if not match:
                print(f"\n  MISMATCH at round {rnd}!")
                print(f"  python:    {py.hex()}")
                print(f"  mem_mpt:   {mem.hex()}")
                print(f"  mpt_store: {store.hex()}")
                print(f"  mpt_arena: {arena.hex()}")
                print(f"  art_mpt:   {art.hex()}")
                print(f"  seed={seed}")
                sys.exit(1)

            rnd += 1

    except KeyboardInterrupt:
        print(f"\nStopped after {rnd} rounds. All matched.")
    finally:
        lib.mpt_store_destroy(ms)
        lib.mpt_arena_destroy(ma)
        lib.art_mpt_ctx_destroy(art_ctx)
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
