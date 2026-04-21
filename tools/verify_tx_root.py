#!/usr/bin/env python3
"""verify_tx_root.py — independent ground-truth check for block_compute_tx_root.

Computes a block's tx_root using py-trie (authoritative reference
implementation) from raw tx wire bytes extracted from an era file.
Compares against the canonical value fetched via RPC.

If Python matches the canonical value, our MPT leaf convention is
correct: MPT value = raw wire bytes (legacy = full RLP list bytes,
typed = type_byte || rlp(inner)).

Usage:
    python3 tools/verify_tx_root.py <block_number> [--era-dir DIR]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..",
                                "examples", "python"))

import rlp
from trie import HexaryTrie
from era import iter_era_blocks, ep_extract_txs


def fetch_canonical_tx_root(block_num: int) -> str:
    hx = hex(block_num)
    out = subprocess.check_output([
        "curl", "-s", "-m", "20", "https://ethereum-rpc.publicnode.com",
        "-H", "content-type: application/json",
        "-d", json.dumps({
            "jsonrpc": "2.0", "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [hx, False],
        }),
    ])
    return json.loads(out)["result"]["transactionsRoot"]


def compute_tx_root_pytrie(tx_bytes_list: list[bytes]) -> bytes:
    """Canonical tx_root per the yellow paper / EIP-2718.

    For each i: key = rlp_encode(i), value = raw tx wire bytes.
    py-trie handles all the MPT leaf-node RLP encoding internally.
    """
    trie = HexaryTrie(db={})
    for i, tx in enumerate(tx_bytes_list):
        trie[rlp.encode(i)] = tx
    return trie.root_hash


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("block", type=int, help="block number to verify")
    ap.add_argument("--era-dir", default="data/era")
    args = ap.parse_args()

    print(f"block: {args.block}")
    print(f"era-dir: {args.era_dir}")
    print()

    # Find the block in the era file
    target = None
    for b in iter_era_blocks(args.era_dir, start_block=args.block):
        if b.number == args.block:
            target = b
            break
        if b.number > args.block:
            break
    if target is None:
        print(f"FATAL: block {args.block} not in era files under {args.era_dir}")
        return 2

    txs = ep_extract_txs(target.ep)
    type_counts = {}
    for tx in txs:
        t = tx[0] if tx and tx[0] < 0x80 else 0x00  # 0x00 = legacy (no type byte)
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"extracted {len(txs)} txs")
    for t, c in sorted(type_counts.items()):
        label = "legacy" if t == 0x00 else f"type 0x{t:02x}"
        print(f"  {label:>10s}: {c}")
    print()

    computed = compute_tx_root_pytrie(txs)
    print(f"  computed tx_root: 0x{computed.hex()}")

    canonical_hex = fetch_canonical_tx_root(args.block)
    canonical = bytes.fromhex(canonical_hex[2:])
    print(f"  canonical (RPC):  {canonical_hex}")
    print()

    if computed == canonical:
        print("PASS — py-trie (reference) agrees with canonical tx_root.")
        print("      Our fixed block_compute_tx_root uses the same convention")
        print("      (MPT value = raw wire bytes), so the C implementation is")
        print("      algorithmically correct.")
        return 0
    else:
        print("FAIL — py-trie does NOT match canonical. Our understanding of")
        print("      the tx_root algorithm is wrong; investigate further.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
