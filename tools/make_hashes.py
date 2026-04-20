#!/usr/bin/env python3
"""
make_hashes.py — build a <state>.hashes sidecar file.

The libartex API reads <snapshot>.hashes alongside the state snapshot
on rx_engine_load_state; it contains 256 block hashes (32 B each),
indexed by `block_number % 256`, used to answer the BLOCKHASH opcode
for the last 256 blocks.

When a snapshot is produced by chain_replay.c via the sync engine's
state_save path, the .hashes file is NOT written (chain_replay tracks
the ring internally but doesn't serialize it).  This script walks post-
merge era files and reconstructs the .hashes sidecar from scratch.

Usage:
    python3 tools/make_hashes.py \
        --state /path/to/state_24864361.bin \
        --era-dir data/era
"""

from __future__ import annotations

import argparse
import os
import struct
import sys

# Reuse the era reader from examples/python/
_EXAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "examples", "python")
sys.path.insert(0, _EXAMPLES)
from era import iter_era_blocks, EP_BLOCK_HASH  # noqa: E402


WINDOW = 256  # matches BLOCK_HASH_WINDOW in lib/artex.c and sync/src/sync.c


def read_state_block_number(path: str) -> int:
    """State file header: 4-byte magic "ART1" + 8-byte little-endian block number + 32-byte root + ..."""
    with open(path, "rb") as f:
        magic = f.read(4)
        if magic != b"ART1":
            raise ValueError(f"not a state snapshot (magic={magic!r}): {path}")
        bn = struct.unpack("<Q", f.read(8))[0]
    return bn


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--state", required=True,
                    help="path to state snapshot .bin")
    ap.add_argument("--era-dir", required=True,
                    help="directory with mainnet-*.era files")
    ap.add_argument("--out", default=None,
                    help="output path (default: <state>.hashes)")
    args = ap.parse_args()

    block_num = read_state_block_number(args.state)
    start = max(0, block_num - WINDOW + 1)
    expected_count = block_num - start + 1  # = WINDOW when block_num >= 255
    print(f"state is at block {block_num}")
    print(f"extracting block hashes for [{start}..{block_num}] "
          f"({expected_count} blocks)")

    # 256 zero-initialized slots — block_number % 256 is the index
    hashes = [b"\x00" * 32 for _ in range(WINDOW)]
    filled_slots = set()  # remember which slots we wrote, for verification

    for b in iter_era_blocks(args.era_dir, start_block=start):
        if b.number > block_num:
            break
        if b.number < start:
            continue
        slot = b.number % WINDOW
        hashes[slot] = bytes(b.ep[EP_BLOCK_HASH:EP_BLOCK_HASH + 32])
        filled_slots.add(slot)

    got = len(filled_slots)
    if got == 0:
        print("ERROR: no matching blocks found in era files — is --era-dir correct?")
        return 1
    if got < expected_count:
        missing = expected_count - got
        print(f"ERROR: {missing} blocks missing from era files — "
              f"got {got}/{expected_count} for range [{start}..{block_num}]")
        return 1

    # Sanity: every slot must have a real 32-byte hash, not left zero.
    for i, h in enumerate(hashes):
        if h == b"\x00" * 32:
            # Only acceptable if the block that SHOULD live in this slot
            # is before our start (true only when block_num < WINDOW-1).
            expected_bn = None
            # Find a block number B in [start..block_num] with B % 256 == i
            for candidate in range(start, block_num + 1):
                if candidate % WINDOW == i:
                    expected_bn = candidate
                    break
            if expected_bn is not None:
                print(f"ERROR: slot {i} is zero but should hold block "
                      f"{expected_bn} (bug in extraction)")
                return 1

    out_path = args.out or (args.state + ".hashes")
    with open(out_path, "wb") as f:
        for h in hashes:
            f.write(h)

    # Verify the file we just wrote matches our in-memory buffer.
    with open(out_path, "rb") as f:
        written = f.read()
    if len(written) != WINDOW * 32:
        print(f"ERROR: wrote {len(written)} bytes, expected {WINDOW * 32}")
        return 1
    for i in range(WINDOW):
        if written[i * 32:(i + 1) * 32] != hashes[i]:
            print(f"ERROR: slot {i} mismatch on readback")
            return 1

    print(f"wrote {out_path} ({WINDOW * 32} bytes, {got}/{WINDOW} slots filled)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
