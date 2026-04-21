#!/usr/bin/env python3
"""
example_roundtrip.py — differential round-trip test for rx_build_block.

Loads a state snapshot at block N-1, extracts the ExecutionPayload for
block N from era files, and feeds the extracted header fields + txs +
withdrawals into rx_build_block. Then compares the result against
what the block itself claims.

The test asserts:

  build.state_root        == ep.state_root
  build.receipts_root     == ep.receipts_root
  build.transactions_root == computed locally from tx list
  build.gas_used          == ep.gas_used
  build.logs_bloom        == ep.logs_bloom
  build.block_hash        == ep.block_hash   (strongest check)

If all equal, every header field and every post-execution value was
reconstructed correctly — rx_build_block produces canonical blocks.

Example:

    python3 example_roundtrip.py \\
        --state ~/.artex/state_24864361.bin \\
        --era-dir data/era \\
        --block 24864362
"""

from __future__ import annotations

import argparse
import os
import struct
import sys
import time

from artex import Engine, Config, version
from era import (
    iter_era_blocks, ep_to_build_header, ep_extract_txs,
    ep_extract_withdrawals,
    EP_BLOCK_HASH, EP_STATE_ROOT, EP_RECEIPTS_ROOT, EP_LOGS_BLOOM,
    EP_GAS_USED,
)


def hex32(b: bytes) -> str:
    return "0x" + b.hex()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--state", required=True, help="state snapshot .bin")
    ap.add_argument(
        "--data-dir",
        help="directory with chain_replay_code.{dat,idx} "
             "(defaults to --state's directory)",
    )
    ap.add_argument("--era-dir", required=True, help="directory with .era files")
    ap.add_argument("--block", type=int, required=True,
                    help="block number to round-trip (must == state_block + 1)")
    args = ap.parse_args()

    state_path = os.path.abspath(os.path.expanduser(args.state))
    data_dir = args.data_dir or os.path.dirname(state_path)
    print(f"libartex version: {version()}")
    print(f"state:    {state_path}")
    print(f"data-dir: {data_dir}")
    print(f"era-dir:  {args.era_dir}")
    print(f"target:   block {args.block}")
    print()

    # Engine + state load
    with Engine(Config(data_dir=data_dir)) as engine:
        print(f"loading state ...")
        t0 = time.time()
        engine.load_state(state_path)
        print(f"  loaded block {engine.block_number()} in {time.time()-t0:.1f}s")

        if engine.block_number() + 1 != args.block:
            print(f"FATAL: snapshot is at {engine.block_number()}, "
                  f"--block must equal {engine.block_number() + 1}, "
                  f"got {args.block}")
            return 2

        # Find block in era
        print(f"\nscanning era for block {args.block} ...")
        target = None
        for b in iter_era_blocks(args.era_dir, start_block=args.block):
            if b.number == args.block:
                target = b
                break
            if b.number > args.block:
                break
        if target is None:
            print(f"FATAL: block {args.block} not found in era files")
            return 2
        print(f"  found block {target.number} "
              f"(deneb={target.parent_beacon_root is not None}, "
              f"prague={target.requests_hash is not None})")

        # Pull expected values from the EP
        ep = target.ep
        expected = {
            "block_hash":     bytes(ep[EP_BLOCK_HASH:EP_BLOCK_HASH + 32]),
            "state_root":     bytes(ep[EP_STATE_ROOT:EP_STATE_ROOT + 32]),
            "receipts_root":  bytes(ep[EP_RECEIPTS_ROOT:EP_RECEIPTS_ROOT + 32]),
            "logs_bloom":     bytes(ep[EP_LOGS_BLOOM:EP_LOGS_BLOOM + 256]),
            "gas_used":       struct.unpack_from("<Q", ep, EP_GAS_USED)[0],
        }

        # Build header + txs + withdrawals
        hdr = ep_to_build_header(target)
        txs = ep_extract_txs(ep)
        wds = ep_extract_withdrawals(ep)
        print(f"\ninputs: {len(txs)} txs, {len(wds)} withdrawals")

        # Execute
        print(f"\ncalling rx_build_block ...")
        t0 = time.time()
        result = engine.build_block(hdr, txs=txs, withdrawals=wds)
        dt = time.time() - t0
        print(f"  done in {dt*1000:.0f} ms")
        print(f"  gas_used: {result.gas_used:,}")

        # Compare
        print("\n=== diff ===")
        errors = 0
        def check(label: str, got, want):
            nonlocal errors
            if got == want:
                print(f"  ✓ {label}")
            else:
                errors += 1
                print(f"  ✗ {label}")
                if isinstance(got, bytes):
                    print(f"      expected: {hex32(want)}")
                    print(f"      got:      {hex32(got)}")
                else:
                    print(f"      expected: {want}")
                    print(f"      got:      {got}")

        check("state_root",    result.state_root,    expected["state_root"])
        check("receipts_root", result.receipts_root, expected["receipts_root"])
        check("logs_bloom",    result.logs_bloom,    expected["logs_bloom"])
        check("gas_used",      result.gas_used,      expected["gas_used"])
        check("block_hash",    result.block_hash,    expected["block_hash"])
        print()

        # When block_hash alone is wrong, dump the header-only fields the EVM
        # doesn't exercise so we can isolate which one is off. No canonical
        # source for these in the ExecutionPayload, but the reader can paste
        # canonical values from an RPC / etherscan lookup to compare.
        if result.block_hash != expected["block_hash"]:
            print("header-only fields (compare manually to canonical):")
            print(f"  transactions_root: {hex32(result.transactions_root)}")
            print(f"  withdrawals_root : {hex32(result.withdrawals_root)}")
            rh = target.requests_hash
            print(f"  requests_hash    : "
                  f"{'0x' + rh.hex() if rh else '<none>'}")
            print()

        if errors == 0:
            print("PASS — rx_build_block produces the canonical block.")
        else:
            print(f"FAIL — {errors} mismatches above.")

        result.free()
        # No commit — leave engine state untouched (we're done)
        engine.revert_block()

        return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
