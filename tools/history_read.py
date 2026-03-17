#!/usr/bin/env python3
"""
Read state history files (v3 format).

Usage:
  ./history_read.py <history_dir>                     # summary + stats
  ./history_read.py <history_dir> <block>             # dump single block
  ./history_read.py <history_dir> <start> <end>       # dump block range
  ./history_read.py <history_dir> --addr <0xaddr>     # find all changes for an address
  ./history_read.py <history_dir> --slot <0xaddr> <0xslot>  # track a storage slot
"""

import struct
import sys
import os
import mmap

# Format constants (must match state_history.c)
HIST_MAGIC   = 0x54534948  # "HIST" LE
HIST_VERSION = 3
IDX_HEADER_SIZE = 16
IDX_ENTRY_SIZE  = 16
DAT_RECORD_HEADER = 16
GROUP_HEADER_SIZE = 24

FIELD_NONCE     = 1 << 0
FIELD_BALANCE   = 1 << 1
FIELD_CODE_HASH = 1 << 2

ACCT_CREATED    = 1 << 0
ACCT_DESTRUCTED = 1 << 1


def read_u16(buf, off): return struct.unpack_from('<H', buf, off)[0]
def read_u32(buf, off): return struct.unpack_from('<I', buf, off)[0]
def read_u64(buf, off): return struct.unpack_from('<Q', buf, off)[0]


def hex_addr(b):
    return '0x' + b.hex()

def hex_u256(b):
    v = int.from_bytes(b, 'big')
    return hex(v)


def parse_idx(path):
    """Parse index file, return (first_block, [(block_number, dat_offset)])."""
    with open(path, 'rb') as f:
        hdr = f.read(IDX_HEADER_SIZE)
        if len(hdr) < IDX_HEADER_SIZE:
            raise ValueError("Index file too small")

        magic = read_u32(hdr, 0)
        version = read_u32(hdr, 4)
        first_block = read_u64(hdr, 8)

        if magic != HIST_MAGIC:
            raise ValueError(f"Bad magic: 0x{magic:08x} (expected 0x{HIST_MAGIC:08x})")
        if version != HIST_VERSION:
            raise ValueError(f"Version {version} (expected {HIST_VERSION})")

        entries = []
        while True:
            entry = f.read(IDX_ENTRY_SIZE)
            if len(entry) < IDX_ENTRY_SIZE:
                break
            bn = read_u64(entry, 0)
            offset = read_u64(entry, 8)
            entries.append((bn, offset))

        return first_block, entries


def parse_record(buf):
    """Parse a single data record, return dict."""
    if len(buf) < DAT_RECORD_HEADER:
        return None

    block_number = read_u64(buf, 0)
    record_len = read_u32(buf, 8)
    group_count = read_u16(buf, 12)

    groups = []
    off = DAT_RECORD_HEADER

    for _ in range(group_count):
        if off + GROUP_HEADER_SIZE > len(buf):
            break

        addr = buf[off:off+20]
        flags = buf[off+20]
        field_mask = buf[off+21]
        slot_count = read_u16(buf, off+22)
        off += GROUP_HEADER_SIZE

        group = {
            'addr': addr,
            'flags': flags,
            'field_mask': field_mask,
            'slots': [],
        }

        if field_mask & FIELD_NONCE:
            group['nonce'] = read_u64(buf, off)
            off += 8
        if field_mask & FIELD_BALANCE:
            group['balance'] = buf[off:off+32]
            off += 32
        if field_mask & FIELD_CODE_HASH:
            group['code_hash'] = buf[off:off+32]
            off += 32

        for _ in range(slot_count):
            slot = buf[off:off+32]
            value = buf[off+32:off+64]
            group['slots'].append((slot, value))
            off += 64

        groups.append(group)

    return {
        'block_number': block_number,
        'groups': groups,
    }


def print_record(rec):
    total_slots = sum(len(g['slots']) for g in rec['groups'])
    print(f"Block {rec['block_number']}: {len(rec['groups'])} addresses, {total_slots} storage slots")

    for g in rec['groups']:
        flags_str = ''
        if g['flags'] & ACCT_CREATED:    flags_str += ' [CREATED]'
        if g['flags'] & ACCT_DESTRUCTED: flags_str += ' [DESTRUCTED]'
        print(f"  {hex_addr(g['addr'])}{flags_str}")

        if 'nonce' in g:
            print(f"    nonce:   {g['nonce']}")
        if 'balance' in g:
            print(f"    balance: {hex_u256(g['balance'])}")
        if 'code_hash' in g:
            print(f"    code:    0x{g['code_hash'].hex()}")

        for slot, value in g['slots']:
            print(f"    slot {hex_u256(slot)} = {hex_u256(value)}")


class HistoryReader:
    def __init__(self, dir_path):
        self.idx_path = os.path.join(dir_path, 'state_history.idx')
        self.dat_path = os.path.join(dir_path, 'state_history.dat')

        if not os.path.exists(self.idx_path) or not os.path.exists(self.dat_path):
            raise FileNotFoundError(f"History files not found in {dir_path}")

        self.first_block, self.entries = parse_idx(self.idx_path)
        self.dat_size = os.path.getsize(self.dat_path)

        # Build block_number -> dat_offset lookup
        self.block_to_offset = {}
        for bn, off in self.entries:
            self.block_to_offset[bn] = off

    @property
    def block_count(self):
        return len(self.entries)

    @property
    def last_block(self):
        return self.entries[-1][0] if self.entries else 0

    def get_diff(self, block_number):
        off = self.block_to_offset.get(block_number)
        if off is None:
            return None

        with open(self.dat_path, 'rb') as f:
            f.seek(off)
            # Read header to get record_len
            hdr = f.read(DAT_RECORD_HEADER)
            if len(hdr) < DAT_RECORD_HEADER:
                return None
            record_len = read_u32(hdr, 8)
            # Read full record (header + payload + CRC)
            f.seek(off)
            total = DAT_RECORD_HEADER + record_len + 4
            buf = f.read(total)
            if len(buf) < total:
                return None

        return parse_record(buf)

    def summary(self):
        print(f"History: blocks {self.first_block} to {self.last_block} ({self.block_count} blocks)")
        print(f"  .dat size: {self.dat_size / (1024*1024*1024):.2f} GB")
        print(f"  .idx size: {os.path.getsize(self.idx_path) / (1024*1024):.1f} MB")
        print(f"  avg record: {self.dat_size / max(self.block_count, 1):.0f} bytes")

    def stats(self, sample=10000):
        """Compute stats over a sample of blocks."""
        import random
        total = self.block_count
        if total == 0:
            print("No blocks.")
            return

        # Sample evenly
        step = max(1, total // sample)
        sampled = self.entries[::step]

        total_groups = 0
        total_slots = 0
        max_groups = 0
        max_groups_bn = 0
        max_slots = 0
        max_slots_bn = 0
        count = 0

        with open(self.dat_path, 'rb') as f:
            for bn, off in sampled:
                f.seek(off)
                hdr = f.read(DAT_RECORD_HEADER)
                if len(hdr) < DAT_RECORD_HEADER:
                    continue
                record_len = read_u32(hdr, 8)
                f.seek(off)
                buf = f.read(DAT_RECORD_HEADER + record_len + 4)
                rec = parse_record(buf)
                if not rec:
                    continue

                ng = len(rec['groups'])
                ns = sum(len(g['slots']) for g in rec['groups'])
                total_groups += ng
                total_slots += ns
                if ng > max_groups:
                    max_groups = ng
                    max_groups_bn = bn
                if ns > max_slots:
                    max_slots = ns
                    max_slots_bn = bn
                count += 1

        print(f"\nStats (sampled {count} of {total} blocks):")
        print(f"  Avg addresses/block: {total_groups / max(count, 1):.1f}")
        print(f"  Avg slots/block:     {total_slots / max(count, 1):.1f}")
        print(f"  Max addresses: {max_groups} (block {max_groups_bn})")
        print(f"  Max slots:     {max_slots} (block {max_slots_bn})")


def find_addr_changes(reader, addr_hex):
    """Find all blocks where an address changed."""
    addr_bytes = bytes.fromhex(addr_hex.replace('0x', ''))
    if len(addr_bytes) != 20:
        print("Invalid address length")
        return

    print(f"Searching for changes to {addr_hex}...")
    found = 0

    with open(reader.dat_path, 'rb') as f:
        for bn, off in reader.entries:
            f.seek(off)
            hdr = f.read(DAT_RECORD_HEADER)
            if len(hdr) < DAT_RECORD_HEADER:
                continue
            record_len = read_u32(hdr, 8)
            f.seek(off)
            buf = f.read(DAT_RECORD_HEADER + record_len + 4)
            rec = parse_record(buf)
            if not rec:
                continue

            for g in rec['groups']:
                if g['addr'] == addr_bytes:
                    found += 1
                    flags_str = ''
                    if g['flags'] & ACCT_CREATED:    flags_str += ' [CREATED]'
                    if g['flags'] & ACCT_DESTRUCTED: flags_str += ' [DESTRUCTED]'
                    parts = [f"Block {bn}:{flags_str}"]
                    if 'nonce' in g: parts.append(f"nonce={g['nonce']}")
                    if 'balance' in g: parts.append(f"bal={hex_u256(g['balance'])}")
                    if g['slots']: parts.append(f"{len(g['slots'])} slots")
                    print(f"  {'  '.join(parts)}")

    print(f"\nFound {found} blocks with changes to this address.")


def track_slot(reader, addr_hex, slot_hex):
    """Track a specific storage slot across all blocks."""
    addr_bytes = bytes.fromhex(addr_hex.replace('0x', ''))
    slot_bytes = int(slot_hex, 16).to_bytes(32, 'big')

    print(f"Tracking slot {slot_hex} at {addr_hex}...")
    found = 0

    with open(reader.dat_path, 'rb') as f:
        for bn, off in reader.entries:
            f.seek(off)
            hdr = f.read(DAT_RECORD_HEADER)
            if len(hdr) < DAT_RECORD_HEADER:
                continue
            record_len = read_u32(hdr, 8)
            f.seek(off)
            buf = f.read(DAT_RECORD_HEADER + record_len + 4)
            rec = parse_record(buf)
            if not rec:
                continue

            for g in rec['groups']:
                if g['addr'] != addr_bytes:
                    continue
                for s, v in g['slots']:
                    if s == slot_bytes:
                        found += 1
                        print(f"  Block {bn}: {hex_u256(slot_bytes)} = {hex_u256(v)}")

    print(f"\nSlot changed in {found} blocks.")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1

    dir_path = sys.argv[1]
    reader = HistoryReader(dir_path)

    if len(sys.argv) == 2:
        reader.summary()
        reader.stats()
        return 0

    if sys.argv[2] == '--addr':
        if len(sys.argv) < 4:
            print("Usage: --addr <0xaddress>")
            return 1
        find_addr_changes(reader, sys.argv[3])
        return 0

    if sys.argv[2] == '--slot':
        if len(sys.argv) < 5:
            print("Usage: --slot <0xaddress> <0xslot>")
            return 1
        track_slot(reader, sys.argv[3], sys.argv[4])
        return 0

    # Block number mode
    start = int(sys.argv[2])
    end = int(sys.argv[3]) if len(sys.argv) > 3 else start

    for bn in range(start, end + 1):
        rec = reader.get_diff(bn)
        if rec is None:
            print(f"Block {bn}: not found")
        else:
            print_record(rec)
        if bn < end:
            print()

    return 0


if __name__ == '__main__':
    sys.exit(main() or 0)
