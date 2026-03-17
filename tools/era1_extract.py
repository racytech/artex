#!/usr/bin/env python3
"""Extract block data (header, transactions, receipts) from era1 files.

Usage:
    python3 tools/era1_extract.py --era1-dir data/era1 --block 549413
    python3 tools/era1_extract.py --era1-dir data/era1 --block 549413 --json
    python3 tools/era1_extract.py --era1-dir data/era1 --block 549413 --t8n-dir /tmp/t8n_549413
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
    """Decode snappy framing format (used by era1/e2store).

    Format:
      StreamHeader: 0xFF + 3-byte len + "sNaPpY"
      Chunks: type(1) + length(3 LE) + data(length)
        type 0x00 = compressed (4-byte CRC + snappy block)
        type 0x01 = uncompressed (4-byte CRC + raw data)
        type 0xFE-0xFF = padding/stream header (skip)
    """
    if len(data) < 10 or data[0] != 0xFF or data[4:10] != b'sNaPpY':
        raise ValueError("invalid snappy framing header")

    pos = 10  # skip stream header
    output = bytearray()

    while pos + 4 <= len(data):
        chunk_type = data[pos]
        chunk_len = data[pos+1] | (data[pos+2] << 8) | (data[pos+3] << 16)
        pos += 4

        if pos + chunk_len > len(data):
            raise ValueError(f"truncated chunk at {pos}")

        if chunk_type == 0x00:
            # Compressed: skip 4-byte CRC, decompress rest
            compressed = data[pos+4 : pos+chunk_len]
            output.extend(snappy.decompress(compressed))
        elif chunk_type == 0x01:
            # Uncompressed: skip 4-byte CRC, copy rest
            output.extend(data[pos+4 : pos+chunk_len])
        # else: padding/reserved, skip

        pos += chunk_len

    return bytes(output)

# E2Store entry types
TYPE_VERSION           = 0x3265
TYPE_COMPRESSED_HEADER = 0x0003
TYPE_COMPRESSED_BODY   = 0x0004
TYPE_COMPRESSED_RCPTS  = 0x0005
TYPE_TOTAL_DIFFICULTY  = 0x0006
TYPE_ACCUMULATOR       = 0x0007
TYPE_BLOCK_INDEX       = 0x3266

ENTRY_HEADER_SIZE = 8
BLOCKS_PER_FILE = 8192


def read_le16(data, off):
    return struct.unpack_from('<H', data, off)[0]

def read_le32(data, off):
    return struct.unpack_from('<I', data, off)[0]

def read_le64(data, off):
    return struct.unpack_from('<Q', data, off)[0]

def read_le64_signed(data, off):
    return struct.unpack_from('<q', data, off)[0]


class Era1File:
    def __init__(self, path):
        with open(path, 'rb') as f:
            self.data = f.read()
        self.file_size = len(self.data)

        # Parse BlockIndex from end of file
        self.count = read_le64(self.data, self.file_size - 8)
        assert 0 < self.count <= 8192, f"invalid count {self.count}"

        index_value_size = 8 + self.count * 8 + 8
        self.index_entry_start = self.file_size - ENTRY_HEADER_SIZE - index_value_size

        # Verify BlockIndex header
        idx_type = read_le16(self.data, self.index_entry_start)
        idx_len = read_le32(self.data, self.index_entry_start + 2)
        assert idx_type == TYPE_BLOCK_INDEX, f"expected BlockIndex, got 0x{idx_type:04x}"
        assert idx_len == index_value_size, f"index len {idx_len} != {index_value_size}"

        self.index_start = self.index_entry_start + ENTRY_HEADER_SIZE
        self.start_block = read_le64(self.data, self.index_start)

    def contains(self, block_number):
        return self.start_block <= block_number < self.start_block + self.count

    def _block_offset(self, index):
        offsets_base = self.index_start + 8  # skip starting_block
        rel = read_le64_signed(self.data, offsets_base + index * 8)
        return self.index_entry_start + rel

    def _read_entry(self, offset, expected_type):
        etype = read_le16(self.data, offset)
        elen = read_le32(self.data, offset + 2)
        assert etype == expected_type, f"expected type 0x{expected_type:04x}, got 0x{etype:04x}"
        value = self.data[offset + ENTRY_HEADER_SIZE : offset + ENTRY_HEADER_SIZE + elen]
        return value, offset + ENTRY_HEADER_SIZE + elen

    def read_block(self, block_number):
        """Returns (header_rlp, body_rlp, receipts_rlp) as bytes."""
        assert self.contains(block_number)
        index = block_number - self.start_block
        offset = self._block_offset(index)

        # CompressedHeader
        compressed, offset = self._read_entry(offset, TYPE_COMPRESSED_HEADER)
        header_rlp = snappy_frame_decode(compressed)

        # CompressedBody
        compressed, offset = self._read_entry(offset, TYPE_COMPRESSED_BODY)
        body_rlp = snappy_frame_decode(compressed)

        # CompressedReceipts
        compressed, offset = self._read_entry(offset, TYPE_COMPRESSED_RCPTS)
        receipts_rlp = snappy_frame_decode(compressed)

        return header_rlp, body_rlp, receipts_rlp


def decode_rlp_list(data):
    """Decode RLP bytes into nested list of bytes objects."""
    return rlp.decode(data)


def to_hex(b):
    """bytes -> '0x...' hex string"""
    if not b:
        return "0x0"
    h = b.hex()
    # Strip leading zeros for integers
    h = h.lstrip('0') or '0'
    return "0x" + h

def to_hex_raw(b):
    """bytes -> '0x...' preserving all bytes (for hashes, addresses, data)"""
    return "0x" + b.hex()

def to_int(b):
    """RLP bytes -> int"""
    if not b:
        return 0
    return int.from_bytes(b, 'big')


def decode_header(rlp_bytes):
    """Decode block header RLP into dict."""
    fields = decode_rlp_list(rlp_bytes)
    names = [
        'parentHash', 'uncleHash', 'coinbase', 'stateRoot', 'txRoot',
        'receiptRoot', 'logsBloom', 'difficulty', 'number', 'gasLimit',
        'gasUsed', 'timestamp', 'extraData', 'mixHash', 'nonce'
    ]
    h = {}
    for i, name in enumerate(names):
        if i >= len(fields):
            break
        val = fields[i]
        if name in ('parentHash', 'uncleHash', 'stateRoot', 'txRoot',
                     'receiptRoot', 'mixHash'):
            h[name] = to_hex_raw(val)
        elif name == 'coinbase':
            h[name] = to_hex_raw(val)
        elif name == 'logsBloom':
            h[name] = to_hex_raw(val)
        elif name == 'extraData':
            h[name] = to_hex_raw(val)
        elif name == 'nonce':
            h[name] = to_hex_raw(val)
        else:
            h[name] = to_hex(val)
    # Extra fields for post-London headers
    if len(fields) > 15:
        h['baseFee'] = to_hex(fields[15])
    if len(fields) > 16:
        h['withdrawalsRoot'] = to_hex_raw(fields[16])
    if len(fields) > 17:
        h['blobGasUsed'] = to_hex(fields[17])
    if len(fields) > 18:
        h['excessBlobGas'] = to_hex(fields[18])
    if len(fields) > 19:
        h['parentBeaconBlockRoot'] = to_hex_raw(fields[19])
    return h


def decode_legacy_tx(fields):
    """Decode a legacy (type 0) transaction from RLP list."""
    tx = {
        'type': '0x0',
        'nonce': to_hex(fields[0]),
        'gasPrice': to_hex(fields[1]),
        'gas': to_hex(fields[2]),
        'to': to_hex_raw(fields[3]) if fields[3] else None,
        'value': to_hex(fields[4]),
        'input': to_hex_raw(fields[5]),
        'v': to_hex(fields[6]),
        'r': to_hex(fields[7]),
        's': to_hex(fields[8]),
    }
    return tx


def decode_typed_tx(raw_bytes):
    """Decode a typed (EIP-2718) transaction."""
    tx_type = raw_bytes[0]
    payload = raw_bytes[1:]
    fields = decode_rlp_list(payload)

    if tx_type == 1:  # EIP-2930
        tx = {
            'type': '0x1',
            'chainId': to_hex(fields[0]),
            'nonce': to_hex(fields[1]),
            'gasPrice': to_hex(fields[2]),
            'gas': to_hex(fields[3]),
            'to': to_hex_raw(fields[4]) if fields[4] else None,
            'value': to_hex(fields[5]),
            'input': to_hex_raw(fields[6]),
            'accessList': fields[7],  # raw for now
            'v': to_hex(fields[8]),
            'r': to_hex(fields[9]),
            's': to_hex(fields[10]),
        }
    elif tx_type == 2:  # EIP-1559
        tx = {
            'type': '0x2',
            'chainId': to_hex(fields[0]),
            'nonce': to_hex(fields[1]),
            'maxPriorityFeePerGas': to_hex(fields[2]),
            'maxFeePerGas': to_hex(fields[3]),
            'gas': to_hex(fields[4]),
            'to': to_hex_raw(fields[5]) if fields[5] else None,
            'value': to_hex(fields[6]),
            'input': to_hex_raw(fields[7]),
            'accessList': fields[8],
            'v': to_hex(fields[9]),
            'r': to_hex(fields[10]),
            's': to_hex(fields[11]),
        }
    elif tx_type == 3:  # EIP-4844
        tx = {
            'type': '0x3',
            'chainId': to_hex(fields[0]),
            'nonce': to_hex(fields[1]),
            'maxPriorityFeePerGas': to_hex(fields[2]),
            'maxFeePerGas': to_hex(fields[3]),
            'gas': to_hex(fields[4]),
            'to': to_hex_raw(fields[5]) if fields[5] else None,
            'value': to_hex(fields[6]),
            'input': to_hex_raw(fields[7]),
            'accessList': fields[8],
            'maxFeePerBlobGas': to_hex(fields[9]),
            'blobVersionedHashes': [to_hex_raw(h) for h in fields[10]],
            'v': to_hex(fields[11]),
            'r': to_hex(fields[12]),
            's': to_hex(fields[13]),
        }
    else:
        tx = {'type': hex(tx_type), 'raw': raw_bytes.hex()}
    return tx


def recover_sender(tx):
    """Recover sender address from v, r, s signature.
    Uses eth_keys if available, otherwise returns None."""
    try:
        from eth_keys import KeyAPI
        from eth_hash.auto import keccak

        v_int = int(tx['v'], 16)
        r_int = int(tx['r'], 16)
        s_int = int(tx['s'], 16)

        tx_type = int(tx.get('type', '0x0'), 16)

        # Build signing hash based on tx type
        if tx_type == 0:
            # Legacy: determine chain_id from v
            if v_int >= 35:
                chain_id = (v_int - 35) // 2
                recovery_bit = v_int - 35 - 2 * chain_id
                # EIP-155 signing: RLP([nonce, gasprice, gas, to, value, data, chain_id, 0, 0])
                items = [
                    int(tx['nonce'], 16).to_bytes(max(1, (int(tx['nonce'], 16).bit_length() + 7) // 8), 'big') if int(tx['nonce'], 16) > 0 else b'',
                    int(tx['gasPrice'], 16).to_bytes(max(1, (int(tx['gasPrice'], 16).bit_length() + 7) // 8), 'big') if int(tx['gasPrice'], 16) > 0 else b'',
                    int(tx['gas'], 16).to_bytes(max(1, (int(tx['gas'], 16).bit_length() + 7) // 8), 'big') if int(tx['gas'], 16) > 0 else b'',
                    bytes.fromhex(tx['to'][2:]) if tx.get('to') else b'',
                    int(tx['value'], 16).to_bytes(max(1, (int(tx['value'], 16).bit_length() + 7) // 8), 'big') if int(tx['value'], 16) > 0 else b'',
                    bytes.fromhex(tx['input'][2:]) if tx.get('input') and tx['input'] != '0x' else b'',
                    chain_id.to_bytes(max(1, (chain_id.bit_length() + 7) // 8), 'big') if chain_id > 0 else b'',
                    b'',
                    b'',
                ]
                signing_data = rlp.encode(items)
            else:
                # Pre-EIP-155
                recovery_bit = v_int - 27
                items = [
                    int(tx['nonce'], 16).to_bytes(max(1, (int(tx['nonce'], 16).bit_length() + 7) // 8), 'big') if int(tx['nonce'], 16) > 0 else b'',
                    int(tx['gasPrice'], 16).to_bytes(max(1, (int(tx['gasPrice'], 16).bit_length() + 7) // 8), 'big') if int(tx['gasPrice'], 16) > 0 else b'',
                    int(tx['gas'], 16).to_bytes(max(1, (int(tx['gas'], 16).bit_length() + 7) // 8), 'big') if int(tx['gas'], 16) > 0 else b'',
                    bytes.fromhex(tx['to'][2:]) if tx.get('to') else b'',
                    int(tx['value'], 16).to_bytes(max(1, (int(tx['value'], 16).bit_length() + 7) // 8), 'big') if int(tx['value'], 16) > 0 else b'',
                    bytes.fromhex(tx['input'][2:]) if tx.get('input') and tx['input'] != '0x' else b'',
                ]
                signing_data = rlp.encode(items)

            msg_hash = keccak(signing_data)
            sig = KeyAPI.Signature(vrs=(recovery_bit, r_int, s_int))
            pubkey = sig.recover_public_key_from_msg_hash(msg_hash)
            return '0x' + keccak(pubkey.to_bytes())[12:].hex()
        else:
            return None  # typed tx sender recovery not implemented here
    except ImportError:
        return None


def decode_body(rlp_bytes):
    """Decode block body RLP into (transactions, uncles)."""
    body = decode_rlp_list(rlp_bytes)
    tx_list_raw = body[0]  # list of encoded txs
    uncle_list = body[1] if len(body) > 1 else []

    txs = []
    for raw_tx in tx_list_raw:
        if isinstance(raw_tx, list):
            # Legacy transaction (already decoded as RLP list)
            tx = decode_legacy_tx(raw_tx)
        elif isinstance(raw_tx, bytes) and len(raw_tx) > 0 and raw_tx[0] < 0x7f:
            # Typed transaction (starts with type byte < 0x7f)
            tx = decode_typed_tx(raw_tx)
        else:
            tx = {'raw': raw_tx.hex() if isinstance(raw_tx, bytes) else str(raw_tx)}
        txs.append(tx)

    return txs, uncle_list


def decode_receipts(rlp_bytes):
    """Decode receipts RLP."""
    receipt_list = decode_rlp_list(rlp_bytes)
    receipts = []
    for r in receipt_list:
        if isinstance(r, bytes) and len(r) > 0 and r[0] < 0x7f:
            # Typed receipt
            rtype = r[0]
            fields = decode_rlp_list(r[1:])
        elif isinstance(r, list):
            rtype = 0
            fields = r
        else:
            receipts.append({'raw': r.hex() if isinstance(r, bytes) else str(r)})
            continue

        # Pre-Byzantium: [postStateOrStatus, cumulativeGasUsed, logsBloom, logs]
        # Post-Byzantium: [status, cumulativeGasUsed, logsBloom, logs]
        receipt = {
            'type': hex(rtype),
            'postStateOrStatus': to_hex_raw(fields[0]) if len(fields[0]) > 1 else to_hex(fields[0]),
            'cumulativeGasUsed': to_hex(fields[1]),
            'logsBloom': to_hex_raw(fields[2]),
            'logCount': len(fields[3]) if len(fields) > 3 else 0,
        }
        if len(fields) > 3 and fields[3]:
            logs = []
            for log in fields[3]:
                logs.append({
                    'address': to_hex_raw(log[0]),
                    'topics': [to_hex_raw(t) for t in log[1]],
                    'data': to_hex_raw(log[2]),
                })
            receipt['logs'] = logs
        receipts.append(receipt)
    return receipts


def find_era1_file(era1_dir, block_number):
    """Find the era1 file containing the given block number."""
    file_index = block_number // BLOCKS_PER_FILE
    pattern = os.path.join(era1_dir, f"mainnet-{file_index:05d}-*.era1")
    matches = glob.glob(pattern)
    if not matches:
        print(f"Error: no era1 file found for block {block_number} (file index {file_index})")
        print(f"Pattern: {pattern}")
        sys.exit(1)
    return matches[0]


def print_block_info(header, txs, receipts, uncles):
    """Print human-readable block info."""
    print(f"Block #{int(header['number'], 16)}")
    print(f"  Coinbase:    {header['coinbase']}")
    print(f"  Difficulty:  {header['difficulty']}")
    print(f"  GasLimit:    {int(header['gasLimit'], 16)}")
    print(f"  GasUsed:     {int(header['gasUsed'], 16)}")
    print(f"  Timestamp:   {int(header['timestamp'], 16)}")
    print(f"  StateRoot:   {header['stateRoot']}")
    print(f"  TxCount:     {len(txs)}")
    print(f"  UncleCount:  {len(uncles)}")
    if 'baseFee' in header:
        print(f"  BaseFee:     {header['baseFee']}")
    print()

    cumulative_prev = 0
    for i, (tx, rcpt) in enumerate(zip(txs, receipts)):
        cum_gas = int(rcpt['cumulativeGasUsed'], 16)
        tx_gas = cum_gas - cumulative_prev
        cumulative_prev = cum_gas

        sender = recover_sender(tx)
        sender_str = f" from={sender}" if sender else ""

        print(f"  TX[{i}]:{sender_str}")
        print(f"    Nonce:    {tx.get('nonce', '?')}")
        print(f"    To:       {tx.get('to', 'CREATE')}")
        print(f"    Value:    {tx.get('value', '?')}")
        print(f"    Gas:      {int(tx.get('gas', '0x0'), 16)}")
        print(f"    GasPrice: {tx.get('gasPrice', tx.get('maxFeePerGas', '?'))}")
        data = tx.get('input', '0x')
        if len(data) > 74:
            print(f"    Input:    {data[:74]}... ({(len(data)-2)//2} bytes)")
        else:
            print(f"    Input:    {data}")
        print(f"    GasUsed:  {tx_gas} (cumulative: {cum_gas})")
        status = rcpt.get('postStateOrStatus', '?')
        print(f"    Status:   {status}")
        if rcpt.get('logs'):
            print(f"    Logs:     {len(rcpt['logs'])}")
        print()


def main():
    parser = argparse.ArgumentParser(description='Extract block data from era1 files')
    parser.add_argument('--era1-dir', required=True, help='Directory containing .era1 files')
    parser.add_argument('--block', type=int, required=True, help='Block number to extract')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--t8n-dir', help='Write t8n-compatible env.json and txs.json (no alloc — need pre-state)')
    args = parser.parse_args()

    path = find_era1_file(args.era1_dir, args.block)
    print(f"Reading from: {path}", file=sys.stderr)

    era = Era1File(path)
    header_rlp, body_rlp, receipts_rlp = era.read_block(args.block)

    header = decode_header(header_rlp)
    txs, uncles = decode_body(body_rlp)
    receipts = decode_receipts(receipts_rlp)

    if args.json:
        out = {
            'header': header,
            'transactions': txs,
            'receipts': receipts,
            'uncleCount': len(uncles),
        }
        print(json.dumps(out, indent=2))
    else:
        print_block_info(header, txs, receipts, uncles)

    # Write t8n-compatible files
    if args.t8n_dir:
        os.makedirs(args.t8n_dir, exist_ok=True)

        # env.json
        env = {
            'currentCoinbase': header['coinbase'],
            'currentDifficulty': header['difficulty'],
            'currentGasLimit': header['gasLimit'],
            'currentNumber': header['number'],
            'currentTimestamp': header['timestamp'],
            'parentHash': header['parentHash'],
        }
        if 'baseFee' in header:
            env['currentBaseFee'] = header['baseFee']
        if 'currentRandom' in header:
            env['currentRandom'] = header['mixHash']

        with open(os.path.join(args.t8n_dir, 'header_env.json'), 'w') as f:
            json.dump(env, f, indent=2)

        # txs.json — note: these have v/r/s but NOT secretKey
        # For t8n we'd need to provide sender addresses
        # We add a 'sender' field if we can recover it
        t8n_txs = []
        for tx in txs:
            t = dict(tx)
            sender = recover_sender(tx)
            if sender:
                t['sender'] = sender
            t8n_txs.append(t)

        with open(os.path.join(args.t8n_dir, 'txs.json'), 'w') as f:
            json.dump(t8n_txs, f, indent=2)

        # Write expected results for comparison
        expected = {
            'stateRoot': header['stateRoot'],
            'gasUsed': header['gasUsed'],
            'receipts': receipts,
        }
        with open(os.path.join(args.t8n_dir, 'expected.json'), 'w') as f:
            json.dump(expected, f, indent=2)

        print(f"\nWrote t8n files to {args.t8n_dir}/", file=sys.stderr)
        print(f"  NOTE: alloc.json not written — need pre-state from chain_replay checkpoint", file=sys.stderr)


if __name__ == '__main__':
    main()
