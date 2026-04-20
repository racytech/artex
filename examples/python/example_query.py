#!/usr/bin/env python3
"""
example_query.py — load a state snapshot and query accounts.

No RPC, no block execution. Just opens a state snapshot file and reads
account balances, nonces, storage slots via the artex library.

Example:

    python3 example_query.py --state /path/to/state_24864361.bin \\
        0xdAC17F958D2ee523a2206206994597C13D831ec7

Use any address you care about. Known mainnet contracts:
    USDT: 0xdAC17F958D2ee523a2206206994597C13D831ec7
    USDC: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    WETH: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
"""

from __future__ import annotations

import argparse
import os
import sys

from artex import Engine, Config, version


USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"


def fmt_wei(wei: int) -> str:
    """Pretty-print a wei value as ETH."""
    if wei == 0:
        return "0 ETH"
    eth = wei / 1e18
    if eth >= 0.001:
        return f"{eth:.6f} ETH"
    return f"{wei:,} wei"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--state", required=True, help="Path to state snapshot .bin")
    ap.add_argument(
        "--data-dir",
        help="directory holding chain_replay_code.{dat,idx}. "
             "Defaults to the --state file's directory.",
    )
    ap.add_argument(
        "addresses", nargs="*", default=[USDT],
        help="Addresses to query (default: USDT contract)",
    )
    ap.add_argument(
        "--slot", action="append", default=[],
        help="Also read this storage slot (hex integer). Repeatable. "
             "Slot is read for every address argument.",
    )
    args = ap.parse_args()

    print(f"libartex version: {version()}")

    data_dir = args.data_dir or os.path.dirname(os.path.abspath(args.state))

    with Engine(Config(data_dir=data_dir)) as engine:
        print(f"loading state from {args.state} ...")
        engine.load_state(args.state)

        print(f"block: {engine.block_number()}")
        print(f"root:  0x{engine.state_root().hex()}")
        print()

        slots = [int(s, 16) if s.startswith(("0x", "0X")) else int(s) for s in args.slot]

        for addr in args.addresses:
            exists = engine.exists(addr)
            print(f"[{addr}]  exists={exists}")
            if not exists:
                print()
                continue

            print(f"  nonce      : {engine.nonce(addr)}")
            print(f"  balance    : {fmt_wei(engine.balance(addr))}")
            print(f"  code size  : {engine.code_size(addr)} bytes")
            code_hash = engine.code_hash(addr)
            print(f"  code hash  : 0x{code_hash.hex()}")

            for slot in slots:
                val = engine.storage(addr, slot)
                print(f"  storage[{slot:#066x}]")
                print(f"    = 0x{val:064x}  ({val})")
            print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
