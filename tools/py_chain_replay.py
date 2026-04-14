#!/usr/bin/env python3
"""
Chain replay using only the public libartex API (rx_* functions).

Validates that the API is complete and usable from Python via ctypes.
Reads post-merge era files (SSZ beacon blocks) and feeds decoded
block data through rx_execute_block.

Usage:
    python3 tools/py_chain_replay.py --state ~/.artex/state_17000000.bin --era-dir data/era1
    python3 tools/py_chain_replay.py --genesis data/mainnet_genesis.json --era-dir data/era1
"""

import argparse
import ctypes
import glob
import os
import struct
import sys
import time

import snappy

# =============================================================================
# ctypes bindings for libartex
# =============================================================================

class RxHash(ctypes.Structure):
    _fields_ = [("bytes", ctypes.c_uint8 * 32)]

class RxAddress(ctypes.Structure):
    _fields_ = [("bytes", ctypes.c_uint8 * 20)]

class RxUint256(ctypes.Structure):
    _fields_ = [("bytes", ctypes.c_uint8 * 32)]

class RxConfig(ctypes.Structure):
    _fields_ = [
        ("chain_id", ctypes.c_int),
        ("data_dir", ctypes.c_char_p),
    ]

class RxReceipt(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_uint8),
        ("tx_type", ctypes.c_uint8),
        ("gas_used", ctypes.c_uint64),
        ("cumulative_gas", ctypes.c_uint64),
        ("logs_bloom", ctypes.c_uint8 * 256),
        ("logs", ctypes.c_void_p),
        ("log_count", ctypes.c_size_t),
        ("contract_created", ctypes.c_bool),
        ("contract_addr", RxAddress),
    ]

class RxBlockResult(ctypes.Structure):
    _fields_ = [
        ("ok", ctypes.c_bool),
        ("gas_used", ctypes.c_uint64),
        ("tx_count", ctypes.c_size_t),
        ("state_root", RxHash),
        ("receipt_root", RxHash),
        ("logs_bloom", ctypes.c_uint8 * 256),
        ("receipts", ctypes.POINTER(RxReceipt)),
    ]

class RxBlockHeader(ctypes.Structure):
    _fields_ = [
        ("parent_hash", RxHash),
        ("uncle_hash", RxHash),
        ("coinbase", RxAddress),
        ("state_root", RxHash),
        ("tx_root", RxHash),
        ("receipt_root", RxHash),
        ("logs_bloom", ctypes.c_uint8 * 256),
        ("difficulty", RxUint256),
        ("number", ctypes.c_uint64),
        ("gas_limit", ctypes.c_uint64),
        ("gas_used", ctypes.c_uint64),
        ("timestamp", ctypes.c_uint64),
        ("extra_data", ctypes.c_uint8 * 32),
        ("extra_data_len", ctypes.c_size_t),
        ("mix_hash", RxHash),
        ("nonce", ctypes.c_uint64),
        ("has_base_fee", ctypes.c_bool),
        ("base_fee", RxUint256),
        ("has_withdrawals_root", ctypes.c_bool),
        ("withdrawals_root", RxHash),
        ("has_blob_gas", ctypes.c_bool),
        ("blob_gas_used", ctypes.c_uint64),
        ("excess_blob_gas", ctypes.c_uint64),
        ("has_parent_beacon_root", ctypes.c_bool),
        ("parent_beacon_root", RxHash),
        ("has_requests_hash", ctypes.c_bool),
        ("requests_hash", RxHash),
    ]

class RxWithdrawal(ctypes.Structure):
    _fields_ = [
        ("index", ctypes.c_uint64),
        ("validator_index", ctypes.c_uint64),
        ("address", RxAddress),
        ("amount_gwei", ctypes.c_uint64),
    ]

class RxBlockBody(ctypes.Structure):
    _fields_ = [
        ("transactions", ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8))),
        ("tx_lengths", ctypes.POINTER(ctypes.c_size_t)),
        ("tx_count", ctypes.c_size_t),
        ("withdrawals", ctypes.POINTER(RxWithdrawal)),
        ("withdrawal_count", ctypes.c_size_t),
    ]


def load_libartex(path):
    lib = ctypes.CDLL(path)

    lib.rx_version.restype = ctypes.c_char_p
    lib.rx_version.argtypes = []

    lib.rx_error_string.restype = ctypes.c_char_p
    lib.rx_error_string.argtypes = [ctypes.c_int]

    lib.rx_engine_create.restype = ctypes.c_void_p
    lib.rx_engine_create.argtypes = [ctypes.POINTER(RxConfig)]

    lib.rx_engine_destroy.restype = None
    lib.rx_engine_destroy.argtypes = [ctypes.c_void_p]

    lib.rx_engine_last_error.restype = ctypes.c_int
    lib.rx_engine_last_error.argtypes = [ctypes.c_void_p]

    lib.rx_engine_last_error_msg.restype = ctypes.c_char_p
    lib.rx_engine_last_error_msg.argtypes = [ctypes.c_void_p]

    lib.rx_engine_load_genesis.restype = ctypes.c_bool
    lib.rx_engine_load_genesis.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(RxHash)]

    lib.rx_engine_load_state.restype = ctypes.c_bool
    lib.rx_engine_load_state.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.rx_set_block_hash.restype = None
    lib.rx_set_block_hash.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(RxHash)]

    lib.rx_engine_save_state.restype = ctypes.c_bool
    lib.rx_engine_save_state.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.rx_execute_block.restype = ctypes.c_bool
    lib.rx_execute_block.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(RxBlockHeader),
        ctypes.POINTER(RxBlockBody),
        ctypes.POINTER(RxHash),
        ctypes.POINTER(RxBlockResult),
    ]

    lib.rx_commit_block.restype = ctypes.c_bool
    lib.rx_commit_block.argtypes = [ctypes.c_void_p]

    lib.rx_compute_state_root.restype = RxHash
    lib.rx_compute_state_root.argtypes = [ctypes.c_void_p]

    lib.rx_get_block_number.restype = ctypes.c_uint64
    lib.rx_get_block_number.argtypes = [ctypes.c_void_p]

    lib.rx_block_result_free.restype = None
    lib.rx_block_result_free.argtypes = [ctypes.POINTER(RxBlockResult)]

    return lib


# =============================================================================
# Era file reading (SSZ beacon blocks → execution payloads)
# =============================================================================

ENTRY_HEADER_SIZE = 8
TYPE_VERSION = 0x3265
TYPE_COMPRESSED_BLOCK = 0x0001
TYPE_SLOT_INDEX = 0x3269

EP_PARENT_HASH    = 0
EP_FEE_RECIPIENT  = 32
EP_STATE_ROOT     = 52
EP_RECEIPTS_ROOT  = 84
EP_LOGS_BLOOM     = 116
EP_PREV_RANDAO    = 372
EP_BLOCK_NUMBER   = 404
EP_GAS_LIMIT      = 412
EP_GAS_USED       = 420
EP_TIMESTAMP      = 428
EP_EXTRA_DATA_OFF = 436
EP_BASE_FEE       = 440
EP_BLOCK_HASH     = 472


def read_le16(data, off): return struct.unpack_from('<H', data, off)[0]
def read_le32(data, off): return struct.unpack_from('<I', data, off)[0]
def read_le64(data, off): return struct.unpack_from('<Q', data, off)[0]
def read_le64_signed(data, off): return struct.unpack_from('<q', data, off)[0]
def read_le256(data, off):
    return int.from_bytes(data[off:off+32], 'little')


def snappy_frame_decode(data):
    if len(data) < 10 or data[0] != 0xFF or data[4:10] != b'sNaPpY':
        raise ValueError("invalid snappy framing header")
    pos = 10
    output = bytearray()
    while pos + 4 <= len(data):
        chunk_type = data[pos]
        chunk_len = data[pos+1] | (data[pos+2] << 8) | (data[pos+3] << 16)
        pos += 4
        if pos + chunk_len > len(data): break
        if chunk_type == 0x00:
            output.extend(snappy.decompress(data[pos+4:pos+chunk_len]))
        elif chunk_type == 0x01:
            output.extend(data[pos+4:pos+chunk_len])
        pos += chunk_len
    return bytes(output)


def find_execution_payload(ssz):
    if len(ssz) < 104: return None
    msg_off = read_le32(ssz, 0)
    if msg_off + 84 > len(ssz): return None
    bb = ssz[msg_off:]
    bb_len = len(ssz) - msg_off
    if bb_len < 84: return None
    body_off = read_le32(bb, 80)
    if body_off > bb_len: return None
    body = bb[body_off:]
    body_len = bb_len - body_off
    if body_len < 384: return None
    ep_off = read_le32(body, 380)
    if ep_off >= body_len: return None
    ep_end = body_len
    body_fixed = read_le32(body, 200)
    if body_fixed > 384 and body_len >= body_fixed:
        next_off = read_le32(body, 384)
        if next_off > ep_off and next_off <= body_len:
            ep_end = next_off
    return body[ep_off:ep_end]


def _probe_first_block(data, file_size):
    """Read the first block number from an era file without full scan."""
    if file_size < 16:
        return None
    pos = 0
    if file_size >= ENTRY_HEADER_SIZE:
        typ = read_le16(data, 0)
        elen = read_le32(data, 2)
        if typ == TYPE_VERSION:
            pos = ENTRY_HEADER_SIZE + elen

    # Find first compressed block
    while pos + ENTRY_HEADER_SIZE <= file_size:
        typ = read_le16(data, pos)
        elen = read_le32(data, pos + 2)
        value_start = pos + ENTRY_HEADER_SIZE
        if typ == TYPE_COMPRESSED_BLOCK and elen > 0:
            try:
                ssz = snappy_frame_decode(data[value_start:value_start + elen])
                ep = find_execution_payload(ssz)
                if ep and len(ep) >= 508:
                    bn = read_le64(ep, EP_BLOCK_NUMBER)
                    if bn > 0:
                        return bn
            except Exception:
                pass
        pos = value_start + elen
    return None


def iter_era_blocks(era_dir, start_block=0):
    """Iterate era files, yielding (block_number, ep_bytes) in order."""
    pattern = os.path.join(era_dir, "mainnet-*.era")
    files = sorted(glob.glob(pattern))

    # Skip files whose blocks are entirely before start_block
    # Probe one block per file to find the right starting file
    start_idx = 0
    if start_block > 0:
        for i, path in enumerate(files):
            try:
                with open(path, 'rb') as f:
                    data = f.read()
                bn = _probe_first_block(data, len(data))
                if bn is not None and bn > start_block and i > 0:
                    start_idx = i - 1
                    break
            except IOError:
                continue
        else:
            start_idx = max(0, len(files) - 1)
        files = files[start_idx:]

    for path in files:
        try:
            with open(path, 'rb') as f:
                data = f.read()
        except IOError:
            continue

        file_size = len(data)
        if file_size < 16:
            continue

        try:
            count = read_le64(data, file_size - 8)
            if count == 0 or count > 100000:
                continue
        except struct.error:
            continue

        # Iterate entries
        pos = 0
        if file_size >= ENTRY_HEADER_SIZE:
            typ = read_le16(data, 0)
            elen = read_le32(data, 2)
            if typ == TYPE_VERSION:
                pos = ENTRY_HEADER_SIZE + elen

        index_size = 8 + count * 8 + 8
        index_start = file_size - ENTRY_HEADER_SIZE - index_size

        while pos + ENTRY_HEADER_SIZE <= index_start:
            typ = read_le16(data, pos)
            elen = read_le32(data, pos + 2)
            value_start = pos + ENTRY_HEADER_SIZE

            if typ == TYPE_COMPRESSED_BLOCK and elen > 0:
                try:
                    compressed = data[value_start:value_start + elen]
                    ssz = snappy_frame_decode(compressed)
                    ep = find_execution_payload(ssz)
                    if ep and len(ep) >= 508:
                        bn = read_le64(ep, EP_BLOCK_NUMBER)
                        if bn >= start_block:
                            yield bn, ep
                except Exception:
                    pass

            pos = value_start + elen


def ep_to_header_body(ep):
    """Convert execution payload bytes → (RxBlockHeader, RxBlockBody, block_hash)."""
    hdr = RxBlockHeader()

    # Fixed fields
    ctypes.memmove(hdr.parent_hash.bytes, ep[EP_PARENT_HASH:EP_PARENT_HASH+32], 32)
    ctypes.memmove(hdr.coinbase.bytes, ep[EP_FEE_RECIPIENT:EP_FEE_RECIPIENT+20], 20)
    ctypes.memmove(hdr.state_root.bytes, ep[EP_STATE_ROOT:EP_STATE_ROOT+32], 32)
    ctypes.memmove(hdr.receipt_root.bytes, ep[EP_RECEIPTS_ROOT:EP_RECEIPTS_ROOT+32], 32)
    ctypes.memmove(hdr.logs_bloom, ep[EP_LOGS_BLOOM:EP_LOGS_BLOOM+256], 256)
    ctypes.memmove(hdr.mix_hash.bytes, ep[EP_PREV_RANDAO:EP_PREV_RANDAO+32], 32)

    hdr.number = read_le64(ep, EP_BLOCK_NUMBER)
    hdr.gas_limit = read_le64(ep, EP_GAS_LIMIT)
    hdr.gas_used = read_le64(ep, EP_GAS_USED)
    hdr.timestamp = read_le64(ep, EP_TIMESTAMP)
    hdr.nonce = 0

    # Base fee (LE 256-bit → BE 32 bytes)
    base_fee = read_le256(ep, EP_BASE_FEE)
    hdr.has_base_fee = True
    hdr.base_fee.bytes[:] = base_fee.to_bytes(32, 'big')

    # Extra data
    extra_data_off = read_le32(ep, EP_EXTRA_DATA_OFF)
    tx_off = read_le32(ep, 504)
    ed_end = tx_off if tx_off > extra_data_off else len(ep)
    ed_len = min(ed_end - extra_data_off, 32)
    if ed_len > 0 and extra_data_off + ed_len <= len(ep):
        ctypes.memmove(hdr.extra_data, ep[extra_data_off:extra_data_off+ed_len], ed_len)
        hdr.extra_data_len = ed_len

    # Detect fork from extra_data_off position
    is_deneb = (extra_data_off >= 528)
    is_capella = (not is_deneb and extra_data_off >= 512)

    # Blob gas (Deneb+)
    if is_deneb:
        hdr.has_blob_gas = True
        hdr.blob_gas_used = read_le64(ep, 512)
        hdr.excess_blob_gas = read_le64(ep, 520)

    # Block hash
    block_hash = RxHash()
    ctypes.memmove(block_hash.bytes, ep[EP_BLOCK_HASH:EP_BLOCK_HASH+32], 32)

    # Parse transactions
    wd_off = 0
    if is_capella or is_deneb:
        wd_off = read_le32(ep, 508)

    txs_raw = []
    if tx_off > 0 and tx_off < len(ep):
        tx_end = wd_off if wd_off > tx_off else len(ep)
        tx_region = tx_end - tx_off
        if tx_region >= 4:
            first_off = read_le32(ep, tx_off)
            tx_count = first_off // 4
            if 0 < tx_count < 100000 and first_off <= tx_region:
                for t in range(tx_count):
                    t_start = read_le32(ep, tx_off + t * 4)
                    t_end = read_le32(ep, tx_off + (t+1) * 4) if t + 1 < tx_count else tx_region
                    if t_start < tx_region and t_end <= tx_region and t_end > t_start:
                        txs_raw.append(bytes(ep[tx_off + t_start : tx_off + t_end]))

    # Build body
    body = RxBlockBody()
    body.tx_count = len(txs_raw)

    # Allocate tx arrays
    TxPtrArray = (ctypes.POINTER(ctypes.c_uint8) * len(txs_raw))
    LenArray = (ctypes.c_size_t * len(txs_raw))
    tx_ptrs = TxPtrArray()
    tx_lens = LenArray()
    tx_bufs = []  # prevent GC
    for i, raw in enumerate(txs_raw):
        buf = (ctypes.c_uint8 * len(raw))(*raw)
        tx_bufs.append(buf)
        tx_ptrs[i] = ctypes.cast(buf, ctypes.POINTER(ctypes.c_uint8))
        tx_lens[i] = len(raw)
    body.transactions = ctypes.cast(tx_ptrs, ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8)))
    body.tx_lengths = ctypes.cast(tx_lens, ctypes.POINTER(ctypes.c_size_t))

    # Parse withdrawals
    withdrawals = []
    if (is_capella or is_deneb) and wd_off > 0 and wd_off < len(ep):
        wd_region = len(ep) - wd_off
        wd_count = wd_region // 44
        for w in range(wd_count):
            wb = ep[wd_off + w * 44:]
            wd = RxWithdrawal()
            wd.index = read_le64(wb, 0)
            wd.validator_index = read_le64(wb, 8)
            ctypes.memmove(wd.address.bytes, wb[16:36], 20)
            wd.amount_gwei = read_le64(wb, 36)
            withdrawals.append(wd)

    if withdrawals:
        WdArray = (RxWithdrawal * len(withdrawals))
        wd_arr = WdArray(*withdrawals)
        body.withdrawals = ctypes.cast(wd_arr, ctypes.POINTER(RxWithdrawal))
        body.withdrawal_count = len(withdrawals)
        body._wd_arr = wd_arr  # prevent GC

    # Keep references alive
    body._tx_bufs = tx_bufs
    body._tx_ptrs = tx_ptrs
    body._tx_lens = tx_lens

    return hdr, body, block_hash


# =============================================================================
# Main
# =============================================================================

def hash_hex(h):
    return bytes(h.bytes).hex()


def main():
    parser = argparse.ArgumentParser(description="Chain replay via libartex Python API")
    parser.add_argument("--lib", default="./build/libartex.so", help="Path to libartex.so")
    parser.add_argument("--state", help="State snapshot to resume from")
    parser.add_argument("--genesis", help="Genesis JSON file")
    parser.add_argument("--era-dir", required=True, help="Directory with era files")
    parser.add_argument("--data-dir", help="Data dir for code_store persistence")
    parser.add_argument("--limit", type=int, default=0, help="Max blocks to execute (0=unlimited)")
    parser.add_argument("--checkpoint", type=int, default=256, help="Validate root every N blocks")
    parser.add_argument("--save-every", type=int, default=0, help="Save snapshot every N blocks")
    parser.add_argument("--save-path", default="py_state", help="Snapshot save path prefix")
    parser.add_argument("--debug", action="store_true", help="Print every block number")
    args = parser.parse_args()

    lib = load_libartex(args.lib)
    print(f"libartex version: {lib.rx_version().decode()}")

    # Create engine
    config = RxConfig()
    config.chain_id = 1  # mainnet
    if args.data_dir:
        config.data_dir = args.data_dir.encode()

    engine = lib.rx_engine_create(ctypes.byref(config))
    if not engine:
        print("FATAL: rx_engine_create failed")
        return 1

    # Load state
    if args.state:
        if not lib.rx_engine_load_state(engine, args.state.encode()):
            msg = lib.rx_engine_last_error_msg(engine).decode()
            print(f"FATAL: rx_engine_load_state failed: {msg}")
            return 1
        block_num = lib.rx_get_block_number(engine)
        print(f"Loaded state at block {block_num}")
    elif args.genesis:
        if not lib.rx_engine_load_genesis(engine, args.genesis.encode(), None):
            msg = lib.rx_engine_last_error_msg(engine).decode()
            print(f"FATAL: rx_engine_load_genesis failed: {msg}")
            return 1
        print("Loaded genesis")
    else:
        print("FATAL: specify --state or --genesis")
        return 1

    start_block = lib.rx_get_block_number(engine) + 1

    # Populate block hash ring (last 256 block hashes for BLOCKHASH opcode)
    if args.state and start_block > 1:
        hash_start = start_block - 1 - 255 if start_block > 256 else 0
        hash_count = 0
        print(f"Populating block hash ring ({hash_start}..{start_block-1})...")
        for bn, ep in iter_era_blocks(args.era_dir, hash_start):
            if bn >= start_block:
                break
            bh = RxHash()
            ctypes.memmove(bh.bytes, ep[EP_BLOCK_HASH:EP_BLOCK_HASH+32], 32)
            lib.rx_set_block_hash(engine, bn, ctypes.byref(bh))
            hash_count += 1
        print(f"  loaded {hash_count} block hashes")

    print(f"Starting from block {start_block}")
    print(f"Era directory: {args.era_dir}")
    print()

    # Replay blocks
    blocks_ok = 0
    blocks_fail = 0
    total_gas = 0
    t_start = time.time()
    t_window = t_start
    window_gas = 0
    window_blocks = 0

    last_block_num = start_block - 1

    try:
        for block_num, ep in iter_era_blocks(args.era_dir, start_block):
            if block_num < start_block:
                continue

            if args.limit and (blocks_ok + blocks_fail) >= args.limit:
                break

            # Continuity check
            if block_num != last_block_num + 1:
                print(f"WARNING: block gap {last_block_num} -> {block_num}")
            last_block_num = block_num

            # Print first 5 blocks for verification (or all with --debug)
            if blocks_ok < 5 or args.debug:
                tx_count_ep = 0
                tx_off = read_le32(ep, 504)
                if tx_off > 0 and tx_off < len(ep):
                    region = len(ep) - tx_off
                    if region >= 4:
                        first = read_le32(ep, tx_off)
                        if first > 0:
                            tx_count_ep = first // 4
                gas = read_le64(ep, EP_GAS_USED)
                print(f"  block {block_num}: {tx_count_ep} txs, "
                      f"gas={gas}, hash={ep[EP_BLOCK_HASH:EP_BLOCK_HASH+32].hex()[:16]}...")

            hdr, body, block_hash = ep_to_header_body(ep)

            result = RxBlockResult()
            ok = lib.rx_execute_block(engine, ctypes.byref(hdr), ctypes.byref(body),
                                       ctypes.byref(block_hash), ctypes.byref(result))

            if not ok:
                msg = lib.rx_engine_last_error_msg(engine).decode()
                print(f"FATAL: block {block_num} execution error: {msg}")
                blocks_fail += 1
                break

            # Validate gas
            expected_gas = hdr.gas_used
            if result.gas_used != expected_gas:
                print(f"FAIL: block {block_num} gas mismatch: "
                      f"expected={expected_gas} actual={result.gas_used}")
                blocks_fail += 1
                lib.rx_block_result_free(ctypes.byref(result))
                lib.rx_commit_block(engine)
                continue

            # Validate state root from block execution result
            if args.checkpoint and block_num % args.checkpoint == 0:
                expected = bytes(hdr.state_root.bytes)
                actual = bytes(result.state_root.bytes)
                if expected != actual:
                    print(f"ROOT MISMATCH at block {block_num}:")
                    print(f"  expected: {expected.hex()}")
                    print(f"  actual:   {actual.hex()}")
                    blocks_fail += 1
                else:
                    print(f"  checkpoint {block_num}: root OK")

            lib.rx_block_result_free(ctypes.byref(result))
            lib.rx_commit_block(engine)

            blocks_ok += 1
            total_gas += expected_gas
            window_gas += expected_gas
            window_blocks += 1

            # Progress
            if window_blocks >= 100:
                t_now = time.time()
                elapsed = t_now - t_window
                if elapsed > 0:
                    bps = window_blocks / elapsed
                    mgps = window_gas / elapsed / 1e6
                    total_elapsed = t_now - t_start
                    print(f"Block {block_num:>10} | "
                          f"{bps:6.1f} blk/s | "
                          f"{mgps:6.1f} Mgas/s | "
                          f"ok={blocks_ok} fail={blocks_fail} | "
                          f"total {total_elapsed:.0f}s")
                t_window = t_now
                window_gas = 0
                window_blocks = 0

            # Save snapshot
            if args.save_every and block_num % args.save_every == 0:
                save_path = f"{args.save_path}_{block_num}.bin"
                if lib.rx_engine_save_state(engine, save_path.encode()):
                    print(f"  Saved snapshot: {save_path}")
                else:
                    print(f"  WARNING: failed to save snapshot")

    except KeyboardInterrupt:
        print(f"\nInterrupted at block {block_num}")

    # Summary
    t_total = time.time() - t_start
    print()
    print(f"{'='*60}")
    print(f"Blocks executed: {blocks_ok}")
    print(f"Blocks failed:   {blocks_fail}")
    print(f"Total gas:       {total_gas:,}")
    print(f"Total time:      {t_total:.1f}s")
    if t_total > 0:
        print(f"Average:         {blocks_ok/t_total:.1f} blk/s, "
              f"{total_gas/t_total/1e6:.1f} Mgas/s")
    print(f"{'='*60}")

    lib.rx_engine_destroy(engine)
    return 1 if blocks_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
