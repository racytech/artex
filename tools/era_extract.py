#!/usr/bin/env python3
"""Extract block data from post-merge era files (beacon chain SSZ format).

Usage:
    python3 tools/era_extract.py --era-dir data/era --block 15540500
    python3 tools/era_extract.py --era-dir data/era --block 15540500 --json
    python3 tools/era_extract.py --era-dir data/era --block 15540500 --t8n-dir known_issues/block_15540500_tx_150
"""

import argparse
import glob
import json
import os
import struct
import sys

import rlp
import snappy


def snappy_frame_decode(data):
    """Decode snappy framing format (used by e2store)."""
    if len(data) < 10 or data[0] != 0xFF or data[4:10] != b'sNaPpY':
        raise ValueError("invalid snappy framing header")

    pos = 10
    output = bytearray()

    while pos + 4 <= len(data):
        chunk_type = data[pos]
        chunk_len = data[pos+1] | (data[pos+2] << 8) | (data[pos+3] << 16)
        pos += 4

        if pos + chunk_len > len(data):
            raise ValueError(f"truncated chunk at {pos}")

        if chunk_type == 0x00:
            compressed = data[pos+4 : pos+chunk_len]
            output.extend(snappy.decompress(compressed))
        elif chunk_type == 0x01:
            output.extend(data[pos+4 : pos+chunk_len])

        pos += chunk_len

    return bytes(output)


# E2Store entry types
TYPE_VERSION           = 0x3265
TYPE_COMPRESSED_BLOCK  = 0x0001  # CompressedSignedBeaconBlock
TYPE_COMPRESSED_STATE  = 0x0002  # CompressedBeaconState
TYPE_SLOT_INDEX        = 0x3269  # SlotIndex

ENTRY_HEADER_SIZE = 8


def read_le16(data, off):
    return struct.unpack_from('<H', data, off)[0]

def read_le32(data, off):
    return struct.unpack_from('<I', data, off)[0]

def read_le64(data, off):
    return struct.unpack_from('<Q', data, off)[0]

def read_le64_signed(data, off):
    return struct.unpack_from('<q', data, off)[0]

def read_le256(data, off):
    """Read 256-bit little-endian integer."""
    b = data[off:off+32]
    return int.from_bytes(b, 'little')


def to_hex(val):
    """int -> '0x...' hex string"""
    if val == 0:
        return "0x0"
    return hex(val)

def to_hex_raw(b):
    """bytes -> '0x...' preserving all bytes"""
    return "0x" + b.hex()


class EraFile:
    def __init__(self, path):
        with open(path, 'rb') as f:
            self.data = f.read()
        self.file_size = len(self.data)
        self.path = path
        self._parse_index()

    def _parse_index(self):
        """Parse SlotIndex from the end of the file."""
        # SlotIndex is at the end: entry_header(8) + starting_slot(8) + offsets(count*8) + count(8)
        # Read count from last 8 bytes before the entry header
        # Actually, the index structure:
        #   SlotIndex entry: type(2) + reserved(2) + length(4) + [starting_slot(8) + offsets(N*8) + count(8)]
        # count is at the very end of the file
        self.count = read_le64(self.data, self.file_size - 8)
        if self.count == 0 or self.count > 100000:
            raise ValueError(f"invalid slot count {self.count}")

        index_value_size = 8 + self.count * 8 + 8  # starting_slot + offsets + count
        self.index_entry_start = self.file_size - ENTRY_HEADER_SIZE - index_value_size

        idx_type = read_le16(self.data, self.index_entry_start)
        idx_len = read_le32(self.data, self.index_entry_start + 2)
        if idx_type != TYPE_SLOT_INDEX:
            raise ValueError(f"expected SlotIndex (0x{TYPE_SLOT_INDEX:04x}), got 0x{idx_type:04x}")

        self.index_start = self.index_entry_start + ENTRY_HEADER_SIZE
        self.start_slot = read_le64(self.data, self.index_start)

    def _slot_offset(self, slot_idx):
        offsets_base = self.index_start + 8  # skip starting_slot
        rel = read_le64_signed(self.data, offsets_base + slot_idx * 8)
        if rel == 0:
            return None  # empty slot
        return self.index_entry_start + rel

    def iter_blocks(self):
        """Iterate over all beacon blocks in the file.
        Yields (slot, ssz_data) for each non-empty slot."""
        pos = 0
        # Skip version entry
        if self.file_size >= ENTRY_HEADER_SIZE:
            typ = read_le16(self.data, 0)
            elen = read_le32(self.data, 2)
            if typ == TYPE_VERSION:
                pos = ENTRY_HEADER_SIZE + elen

        while pos + ENTRY_HEADER_SIZE <= self.index_entry_start:
            typ = read_le16(self.data, pos)
            elen = read_le32(self.data, pos + 2)
            value_start = pos + ENTRY_HEADER_SIZE

            if typ == TYPE_COMPRESSED_BLOCK and elen > 0:
                compressed = self.data[value_start:value_start + elen]
                ssz = snappy_frame_decode(compressed)
                # Extract slot from BeaconBlock
                if len(ssz) >= 108:
                    msg_off = read_le32(ssz, 0)
                    slot = read_le64(ssz, msg_off)
                    yield slot, ssz

            pos = value_start + elen


def find_execution_payload(ssz):
    """Navigate SSZ to find ExecutionPayload. Returns (ep_bytes, ep_len) or None."""
    if len(ssz) < 104:
        return None

    msg_off = read_le32(ssz, 0)
    if msg_off + 84 > len(ssz):
        return None

    bb = ssz[msg_off:]
    bb_len = len(ssz) - msg_off

    if bb_len < 84:
        return None

    body_off = read_le32(bb, 80)
    if body_off > bb_len:
        return None

    body = bb[body_off:]
    body_len = bb_len - body_off

    if body_len < 384:
        return None

    ep_off = read_le32(body, 380)
    if ep_off >= body_len:
        return None

    ep_end = body_len
    # Only read bounding offset at byte 384 for Capella+ (body fixed > 384).
    # For Bellatrix (body fixed = 384), EP is the last variable field.
    body_fixed = read_le32(body, 200)  # proposer_slashings offset = body fixed size
    if body_fixed > 384 and body_len >= body_fixed:
        next_off = read_le32(body, 384)
        if next_off > ep_off and next_off <= body_len:
            ep_end = next_off

    return body[ep_off:ep_end]


# ExecutionPayload field offsets
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


def parse_execution_payload(ep):
    """Parse ExecutionPayload SSZ into header dict and raw transaction list."""
    if len(ep) < 508:
        return None, None

    header = {
        'parentHash':   to_hex_raw(ep[EP_PARENT_HASH:EP_PARENT_HASH+32]),
        'coinbase':     to_hex_raw(ep[EP_FEE_RECIPIENT:EP_FEE_RECIPIENT+20]),
        'stateRoot':    to_hex_raw(ep[EP_STATE_ROOT:EP_STATE_ROOT+32]),
        'receiptRoot':  to_hex_raw(ep[EP_RECEIPTS_ROOT:EP_RECEIPTS_ROOT+32]),
        'logsBloom':    to_hex_raw(ep[EP_LOGS_BLOOM:EP_LOGS_BLOOM+256]),
        'mixHash':      to_hex_raw(ep[EP_PREV_RANDAO:EP_PREV_RANDAO+32]),
        'number':       to_hex(read_le64(ep, EP_BLOCK_NUMBER)),
        'gasLimit':     to_hex(read_le64(ep, EP_GAS_LIMIT)),
        'gasUsed':      to_hex(read_le64(ep, EP_GAS_USED)),
        'timestamp':    to_hex(read_le64(ep, EP_TIMESTAMP)),
        'difficulty':   '0x0',
        'blockHash':    to_hex_raw(ep[EP_BLOCK_HASH:EP_BLOCK_HASH+32]),
    }

    base_fee = read_le256(ep, EP_BASE_FEE)
    header['baseFee'] = to_hex(base_fee)

    extra_data_off = read_le32(ep, EP_EXTRA_DATA_OFF)

    # Detect fork
    is_deneb   = (extra_data_off >= 528)
    is_capella = (not is_deneb and extra_data_off >= 512)

    # Transaction offset at byte 504
    tx_off = read_le32(ep, 504)
    wd_off = 0

    if is_capella or is_deneb:
        wd_off = read_le32(ep, 508)

    if is_deneb:
        header['blobGasUsed']   = to_hex(read_le64(ep, 512))
        header['excessBlobGas'] = to_hex(read_le64(ep, 520))

    # Extra data
    ed_end = tx_off if tx_off > extra_data_off else len(ep)
    ed_len = ed_end - extra_data_off
    if ed_len > 0 and extra_data_off + ed_len <= len(ep):
        header['extraData'] = to_hex_raw(ep[extra_data_off:extra_data_off + ed_len])

    # Parse transactions
    txs_raw = []
    if tx_off > 0 and tx_off < len(ep):
        tx_data = ep[tx_off:]
        tx_end = len(ep)
        if wd_off > tx_off:
            tx_end = wd_off
        tx_region = tx_end - tx_off

        if tx_region >= 4:
            first_off = read_le32(ep, tx_off)
            tx_count = first_off // 4

            if tx_count > 0 and tx_count < 100000 and first_off <= tx_region:
                for t in range(tx_count):
                    t_start = read_le32(ep, tx_off + t * 4)
                    t_end_off = read_le32(ep, tx_off + (t+1) * 4) if t + 1 < tx_count else tx_region
                    if t_start < tx_region and t_end_off <= tx_region and t_end_off > t_start:
                        txs_raw.append(ep[tx_off + t_start : tx_off + t_end_off])

    # Parse withdrawals
    withdrawals = []
    if (is_capella or is_deneb) and wd_off > 0 and wd_off < len(ep):
        wd_region = len(ep) - wd_off
        if wd_region >= 44:
            wd_count = wd_region // 44
            for w in range(wd_count):
                wb = ep[wd_off + w * 44:]
                withdrawals.append({
                    'index':          to_hex(read_le64(wb, 0)),
                    'validatorIndex': to_hex(read_le64(wb, 8)),
                    'address':        to_hex_raw(wb[16:36]),
                    'amount':         to_hex(read_le64(wb, 36)),
                })

    header['withdrawals'] = withdrawals
    return header, txs_raw


def decode_typed_tx(raw_bytes):
    """Decode a typed (EIP-2718) transaction."""
    tx_type = raw_bytes[0]
    payload = raw_bytes[1:]
    fields = rlp.decode(payload)

    if tx_type == 1:  # EIP-2930
        tx = {
            'type': '0x1',
            'chainId': to_hex(int.from_bytes(fields[0], 'big') if fields[0] else 0),
            'nonce': to_hex(int.from_bytes(fields[1], 'big') if fields[1] else 0),
            'gasPrice': to_hex(int.from_bytes(fields[2], 'big') if fields[2] else 0),
            'gas': to_hex(int.from_bytes(fields[3], 'big') if fields[3] else 0),
            'to': to_hex_raw(fields[4]) if fields[4] else None,
            'value': to_hex(int.from_bytes(fields[5], 'big') if fields[5] else 0),
            'input': to_hex_raw(fields[6]),
            'v': to_hex(int.from_bytes(fields[8], 'big') if fields[8] else 0),
            'r': to_hex(int.from_bytes(fields[9], 'big') if fields[9] else 0),
            's': to_hex(int.from_bytes(fields[10], 'big') if fields[10] else 0),
        }
    elif tx_type == 2:  # EIP-1559
        tx = {
            'type': '0x2',
            'chainId': to_hex(int.from_bytes(fields[0], 'big') if fields[0] else 0),
            'nonce': to_hex(int.from_bytes(fields[1], 'big') if fields[1] else 0),
            'maxPriorityFeePerGas': to_hex(int.from_bytes(fields[2], 'big') if fields[2] else 0),
            'maxFeePerGas': to_hex(int.from_bytes(fields[3], 'big') if fields[3] else 0),
            'gas': to_hex(int.from_bytes(fields[4], 'big') if fields[4] else 0),
            'to': to_hex_raw(fields[5]) if fields[5] else None,
            'value': to_hex(int.from_bytes(fields[6], 'big') if fields[6] else 0),
            'input': to_hex_raw(fields[7]),
            'v': to_hex(int.from_bytes(fields[9], 'big') if fields[9] else 0),
            'r': to_hex(int.from_bytes(fields[10], 'big') if fields[10] else 0),
            's': to_hex(int.from_bytes(fields[11], 'big') if fields[11] else 0),
        }
    elif tx_type == 3:  # EIP-4844
        tx = {
            'type': '0x3',
            'chainId': to_hex(int.from_bytes(fields[0], 'big') if fields[0] else 0),
            'nonce': to_hex(int.from_bytes(fields[1], 'big') if fields[1] else 0),
            'maxPriorityFeePerGas': to_hex(int.from_bytes(fields[2], 'big') if fields[2] else 0),
            'maxFeePerGas': to_hex(int.from_bytes(fields[3], 'big') if fields[3] else 0),
            'gas': to_hex(int.from_bytes(fields[4], 'big') if fields[4] else 0),
            'to': to_hex_raw(fields[5]) if fields[5] else None,
            'value': to_hex(int.from_bytes(fields[6], 'big') if fields[6] else 0),
            'input': to_hex_raw(fields[7]),
            'maxFeePerBlobGas': to_hex(int.from_bytes(fields[9], 'big') if fields[9] else 0),
            'blobVersionedHashes': [to_hex_raw(h) for h in fields[10]],
            'v': to_hex(int.from_bytes(fields[11], 'big') if fields[11] else 0),
            'r': to_hex(int.from_bytes(fields[12], 'big') if fields[12] else 0),
            's': to_hex(int.from_bytes(fields[13], 'big') if fields[13] else 0),
        }
    else:
        tx = {'type': hex(tx_type), 'raw': raw_bytes.hex()}
    return tx


def decode_legacy_tx(raw_bytes):
    """Decode legacy transaction from RLP bytes."""
    fields = rlp.decode(raw_bytes)
    return {
        'type': '0x0',
        'nonce': to_hex(int.from_bytes(fields[0], 'big') if fields[0] else 0),
        'gasPrice': to_hex(int.from_bytes(fields[1], 'big') if fields[1] else 0),
        'gas': to_hex(int.from_bytes(fields[2], 'big') if fields[2] else 0),
        'to': to_hex_raw(fields[3]) if fields[3] else None,
        'value': to_hex(int.from_bytes(fields[4], 'big') if fields[4] else 0),
        'input': to_hex_raw(fields[5]),
        'v': to_hex(int.from_bytes(fields[6], 'big') if fields[6] else 0),
        'r': to_hex(int.from_bytes(fields[7], 'big') if fields[7] else 0),
        's': to_hex(int.from_bytes(fields[8], 'big') if fields[8] else 0),
    }


def decode_tx(raw_bytes):
    """Decode a transaction from raw bytes (legacy or typed)."""
    if len(raw_bytes) > 0 and raw_bytes[0] >= 0xc0:
        return decode_legacy_tx(raw_bytes)
    elif len(raw_bytes) > 0 and raw_bytes[0] < 0x80:
        return decode_typed_tx(raw_bytes)
    else:
        return {'raw': raw_bytes.hex()}


def find_era_file_for_block(era_dir, block_number):
    """Find the era file containing a given block number by scanning files."""
    pattern = os.path.join(era_dir, "mainnet-*.era")
    files = sorted(glob.glob(pattern))

    for path in files:
        try:
            era = EraFile(path)
        except (ValueError, struct.error):
            continue

        for slot, ssz in era.iter_blocks():
            ep = find_execution_payload(ssz)
            if ep and len(ep) >= 508:
                bn = read_le64(ep, EP_BLOCK_NUMBER)
                if bn == block_number:
                    return path, slot, ssz
                if bn > block_number:
                    return None, None, None  # past it
    return None, None, None


def main():
    parser = argparse.ArgumentParser(description='Extract block data from post-merge era files')
    parser.add_argument('--era-dir', required=True, help='Directory containing .era files')
    parser.add_argument('--block', type=int, required=True, help='Block number to extract')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--t8n-dir', help='Write t8n-compatible files (env.json, txs.json as raw hex)')
    args = parser.parse_args()

    print(f"Searching for block {args.block} in {args.era_dir}...", file=sys.stderr)

    path, slot, ssz = find_era_file_for_block(args.era_dir, args.block)
    if path is None:
        print(f"Error: block {args.block} not found in era files", file=sys.stderr)
        sys.exit(1)

    print(f"Found in {path} at slot {slot}", file=sys.stderr)

    ep = find_execution_payload(ssz)
    header, txs_raw = parse_execution_payload(ep)

    txs = [decode_tx(raw) for raw in txs_raw]

    if args.json:
        out = {
            'header': header,
            'transactions': txs,
            'txCount': len(txs),
        }
        print(json.dumps(out, indent=2))
    else:
        bn = int(header['number'], 16)
        print(f"Block #{bn} (slot {slot})")
        print(f"  Coinbase:    {header['coinbase']}")
        print(f"  GasLimit:    {int(header['gasLimit'], 16)}")
        print(f"  GasUsed:     {int(header['gasUsed'], 16)}")
        print(f"  Timestamp:   {int(header['timestamp'], 16)}")
        print(f"  BaseFee:     {header['baseFee']}")
        print(f"  PrevRandao:  {header['mixHash']}")
        print(f"  StateRoot:   {header['stateRoot']}")
        print(f"  BlockHash:   {header['blockHash']}")
        print(f"  TxCount:     {len(txs)}")
        if header.get('withdrawals'):
            print(f"  Withdrawals: {len(header['withdrawals'])}")
        print()

        for i, tx in enumerate(txs):
            print(f"  TX[{i}] type={tx.get('type', '?')}")
            print(f"    Nonce: {tx.get('nonce', '?')}  Gas: {tx.get('gas', '?')}")
            print(f"    To:    {tx.get('to', 'CREATE')}")
            val = tx.get('value', '0x0')
            print(f"    Value: {val}")
            data = tx.get('input', '0x')
            if len(data) > 74:
                print(f"    Input: {data[:74]}... ({(len(data)-2)//2} bytes)")
            else:
                print(f"    Input: {data}")
            print()

    # Write t8n-compatible files
    if args.t8n_dir:
        os.makedirs(args.t8n_dir, exist_ok=True)

        # env.json
        env = {
            'currentCoinbase': header['coinbase'],
            'currentDifficulty': '0x0',
            'currentGasLimit': header['gasLimit'],
            'currentNumber': header['number'],
            'currentTimestamp': header['timestamp'],
            'currentBaseFee': header['baseFee'],
            'currentRandom': header['mixHash'],
            'parentHash': header['parentHash'],
        }
        if 'blobGasUsed' in header:
            env['currentBlobGasUsed'] = header['blobGasUsed']
            env['currentExcessBlobGas'] = header['excessBlobGas']

        with open(os.path.join(args.t8n_dir, 'env.json'), 'w') as f:
            json.dump(env, f, indent=2)
        print(f"Wrote env.json", file=sys.stderr)

        # txs.json — raw hex strings for geth t8n
        txs_hex = []
        for raw in txs_raw:
            txs_hex.append("0x" + raw.hex())

        with open(os.path.join(args.t8n_dir, 'txs.json'), 'w') as f:
            json.dump(txs_hex, f, indent=2)
        print(f"Wrote txs.json ({len(txs_hex)} txs)", file=sys.stderr)

        # expected.json
        expected = {
            'gasUsed': header['gasUsed'],
            'stateRoot': header['stateRoot'],
            'blockHash': header['blockHash'],
        }
        with open(os.path.join(args.t8n_dir, 'expected.json'), 'w') as f:
            json.dump(expected, f, indent=2)
        print(f"Wrote expected.json", file=sys.stderr)

        print(f"\nNOTE: alloc.json not written — use --dump-tx in chain_replay", file=sys.stderr)


if __name__ == '__main__':
    main()
