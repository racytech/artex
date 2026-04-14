#!/usr/bin/env python3
"""Test execution payload parsing without loading state."""

import ctypes
import sys
sys.path.insert(0, '.')
from tools.py_chain_replay import *


def test_ep_parsing():
    """Verify we extract the same data from SSZ as the C era reader."""
    print("=== Test EP Parsing ===")

    for bn, ep in iter_era_blocks('data/era', 17000193):
        if bn != 17000193:
            continue

        print(f"Block {bn}, EP size: {len(ep)} bytes")

        # Raw field reads
        extra_data_off = read_le32(ep, EP_EXTRA_DATA_OFF)
        tx_off = read_le32(ep, 504)
        print(f"  extra_data_off: {extra_data_off}")
        print(f"  tx_off: {tx_off}")
        print(f"  gas_limit: {read_le64(ep, EP_GAS_LIMIT)}")
        print(f"  gas_used: {read_le64(ep, EP_GAS_USED)}")
        print(f"  timestamp: {read_le64(ep, EP_TIMESTAMP)}")
        print(f"  base_fee (LE256): {read_le256(ep, EP_BASE_FEE)}")
        print(f"  block_number: {read_le64(ep, EP_BLOCK_NUMBER)}")

        # Fork detection
        is_deneb = (extra_data_off >= 528)
        is_capella = (not is_deneb and extra_data_off >= 512)
        print(f"  is_capella: {is_capella}, is_deneb: {is_deneb}")

        # Parse transactions raw
        tx_end = len(ep)
        if (is_capella or is_deneb):
            wd_off = read_le32(ep, 508)
            if wd_off > tx_off and wd_off < len(ep):
                tx_end = wd_off
        tx_region = tx_end - tx_off

        first_off = read_le32(ep, tx_off)
        tx_count = first_off // 4
        print(f"  tx_count: {tx_count}")
        print(f"  tx_region: {tx_region} bytes")

        # Show first 3 txs
        for t in range(min(3, tx_count)):
            t_start = read_le32(ep, tx_off + t * 4)
            t_end_off = read_le32(ep, tx_off + (t+1) * 4) if t + 1 < tx_count else tx_region
            tx_raw = ep[tx_off + t_start : tx_off + t_end_off]
            tx_type = "legacy" if tx_raw[0] >= 0xc0 else f"type-{tx_raw[0]}"
            print(f"  tx[{t}]: {len(tx_raw)} bytes, {tx_type}, first=0x{tx_raw[:4].hex()}")

        break
    else:
        print("Block 17000193 not found!")
        return False

    return True


def test_header_conversion():
    """Test rx_block_header_t construction from EP."""
    print("\n=== Test Header Conversion ===")

    for bn, ep in iter_era_blocks('data/era', 17000193):
        if bn != 17000193:
            continue

        hdr, body, block_hash = ep_to_header_body(ep)

        print(f"  number: {hdr.number}")
        print(f"  gas_limit: {hdr.gas_limit}")
        print(f"  gas_used: {hdr.gas_used}")
        print(f"  timestamp: {hdr.timestamp}")
        print(f"  has_base_fee: {hdr.has_base_fee}")
        print(f"  base_fee bytes: {bytes(hdr.base_fee.bytes).hex()}")
        print(f"  has_blob_gas: {hdr.has_blob_gas}")
        print(f"  has_parent_beacon_root: {hdr.has_parent_beacon_root}")
        print(f"  has_withdrawals_root: {hdr.has_withdrawals_root}")
        print(f"  coinbase: {bytes(hdr.coinbase.bytes).hex()}")
        print(f"  mix_hash: {bytes(hdr.mix_hash.bytes)[:8].hex()}...")
        print(f"  extra_data_len: {hdr.extra_data_len}")
        print(f"  block_hash: {bytes(block_hash.bytes).hex()[:16]}...")
        print(f"  parent_hash: {bytes(hdr.parent_hash.bytes).hex()[:16]}...")
        print(f"  difficulty: {bytes(hdr.difficulty.bytes).hex()}")

        print(f"\n  body.tx_count: {body.tx_count}")
        print(f"  body.withdrawal_count: {body.withdrawal_count}")

        # Verify tx bytes are accessible
        for t in range(min(3, body.tx_count)):
            tx_ptr = body.transactions[t]
            tx_len = body.tx_lengths[t]
            tx_bytes = bytes(tx_ptr[:tx_len])
            tx_type = "legacy" if tx_bytes[0] >= 0xc0 else f"type-{tx_bytes[0]}"
            print(f"  body.tx[{t}]: {tx_len} bytes, {tx_type}, first=0x{tx_bytes[:4].hex()}")

        break

    return True


def test_convert_body_c():
    """Test that convert_body in C produces correct tx count."""
    print("\n=== Test C convert_body ===")

    lib = load_libartex('./build/libartex.so')
    print(f"  libartex version: {lib.rx_version().decode()}")

    # Create engine (no state load needed - just testing body conversion)
    config = RxConfig()
    config.chain_id = 1
    engine = lib.rx_engine_create(ctypes.byref(config))
    if not engine:
        print("  FAIL: can't create engine")
        return False

    # We can't directly test convert_body, but we can check if
    # rx_execute_block at least parses the body correctly by checking
    # the result.tx_count (it should match even if execution fails
    # due to no state)

    for bn, ep in iter_era_blocks('data/era', 17000193):
        if bn != 17000193:
            continue

        hdr, body, block_hash = ep_to_header_body(ep)

        # Load a trivial genesis so engine is initialized
        lib.rx_engine_load_genesis_alloc(engine, None, 0, None)

        result = RxBlockResult()
        ok = lib.rx_execute_block(engine, ctypes.byref(hdr), ctypes.byref(body),
                                   ctypes.byref(block_hash), ctypes.byref(result))

        print(f"  ok: {ok}")
        print(f"  result.tx_count: {result.tx_count}")
        print(f"  result.gas_used: {result.gas_used}")
        print(f"  expected gas: {hdr.gas_used}")

        # With empty state, all txs should fail but tx_count should be correct
        if result.tx_count != body.tx_count:
            print(f"  FAIL: tx_count mismatch: body={body.tx_count} result={result.tx_count}")
        else:
            print(f"  OK: tx_count matches ({result.tx_count})")

        lib.rx_block_result_free(ctypes.byref(result))
        break

    lib.rx_engine_destroy(engine)
    return True


def test_rlp_path():
    """Test rx_execute_block_rlp with a block built from EP data."""
    print("\n=== Test RLP Path ===")

    lib = load_libartex('./build/libartex.so')
    config = RxConfig()
    config.chain_id = 1
    engine = lib.rx_engine_create(ctypes.byref(config))
    lib.rx_engine_load_genesis_alloc(engine, None, 0, None)

    # We need to build RLP header + body from the EP.
    # This is hard to do from Python. Instead, let's compare
    # rx_execute_block gas output against known value.

    # Actually, let's just verify the body tx bytes match between
    # what Python sends and what C receives.

    for bn, ep in iter_era_blocks('data/era', 17000193):
        if bn != 17000193: continue

        hdr, body, block_hash = ep_to_header_body(ep)

        # Verify tx bytes haven't been corrupted
        for t in range(min(5, body.tx_count)):
            tx_ptr = body.transactions[t]
            tx_len = body.tx_lengths[t]
            py_bytes = bytes(tx_ptr[:tx_len])

            # Compare against raw EP extraction
            tx_off = read_le32(ep, 504)
            tx_region_end = len(ep)
            t_start = read_le32(ep, tx_off + t * 4)
            t_end = read_le32(ep, tx_off + (t+1) * 4) if t + 1 < body.tx_count else (tx_region_end - tx_off)
            ep_bytes = bytes(ep[tx_off + t_start : tx_off + t_end])

            match = py_bytes == ep_bytes
            print(f"  tx[{t}]: py_len={len(py_bytes)} ep_len={len(ep_bytes)} match={match}")
            if not match:
                print(f"    py: {py_bytes[:20].hex()}")
                print(f"    ep: {ep_bytes[:20].hex()}")
        break

    lib.rx_engine_destroy(engine)


if __name__ == "__main__":
    test_ep_parsing()
    test_header_conversion()
    test_convert_body_c()
    test_rlp_path()
