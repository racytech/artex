#!/usr/bin/env python3
"""Verify MPT state root from a dump file using Python HexaryTrie reference."""
import sys
from eth_hash.auto import keccak
from trie import HexaryTrie

def rlp_encode_int(n):
    """RLP-encode a non-negative integer."""
    if n == 0:
        return b'\x80'
    data = n.to_bytes((n.bit_length() + 7) // 8, 'big')
    if len(data) == 1 and data[0] < 0x80:
        return data
    return bytes([0x80 + len(data)]) + data

def rlp_encode_bytes(data):
    """RLP-encode a byte string."""
    if len(data) == 1 and data[0] < 0x80:
        return data
    if len(data) <= 55:
        return bytes([0x80 + len(data)]) + data
    len_bytes = len(data).to_bytes((len(data).bit_length() + 7) // 8, 'big')
    return bytes([0xb7 + len(len_bytes)]) + len_bytes + data

def rlp_encode_list(items):
    """RLP-encode a list of already-encoded items."""
    payload = b''.join(items)
    if len(payload) <= 55:
        return bytes([0xc0 + len(payload)]) + payload
    len_bytes = len(payload).to_bytes((len(payload).bit_length() + 7) // 8, 'big')
    return bytes([0xf7 + len(len_bytes)]) + len_bytes + payload

def main():
    dump_file = sys.argv[1] if len(sys.argv) > 1 else "/tmp/mpt_dump_62102.txt"

    accounts = {}  # addr_bytes -> (nonce, balance_int, has_code, code_hash_bytes)
    storage = {}   # addr_bytes -> {slot_bytes -> value_bytes}

    with open(dump_file) as f:
        for line in f:
            parts = line.strip().split()
            if parts[0] == 'ACCT':
                addr = bytes.fromhex(parts[1])
                nonce = int(parts[2])
                balance = int(parts[3], 16)
                has_code = int(parts[4])
                code_hash = bytes.fromhex(parts[5])
                accounts[addr] = (nonce, balance, has_code, code_hash)
            elif parts[0] == 'SLOT':
                addr = bytes.fromhex(parts[1])
                slot = bytes.fromhex(parts[2])  # 32 bytes big-endian
                value = bytes.fromhex(parts[3])  # 32 bytes big-endian
                if addr not in storage:
                    storage[addr] = {}
                storage[addr][slot] = value

    print(f"Loaded {len(accounts)} accounts, {sum(len(s) for s in storage.values())} storage entries")

    EMPTY_STORAGE_ROOT = keccak(b'\x80')
    EMPTY_CODE_HASH = keccak(b'')

    # Compute per-account storage roots
    storage_roots = {}
    for addr, slots in storage.items():
        trie = HexaryTrie(db={})
        for slot_be, value_be in slots.items():
            key = keccak(slot_be)
            # RLP-encode the trimmed big-endian value
            value_trimmed = value_be.lstrip(b'\x00')
            if len(value_trimmed) == 0:
                continue  # skip zero values
            value_rlp = rlp_encode_bytes(value_trimmed)
            trie[key] = value_rlp
        storage_roots[addr] = trie.root_hash

    # Build state trie
    state_trie = HexaryTrie(db={})
    prune_empty = False  # Frontier

    for addr, (nonce, balance, has_code, code_hash) in accounts.items():
        sr = storage_roots.get(addr, EMPTY_STORAGE_ROOT)

        # Check if empty
        is_empty = (nonce == 0 and balance == 0 and
                    not has_code and sr == EMPTY_STORAGE_ROOT)
        if is_empty and prune_empty:
            continue

        # RLP account: [nonce, balance, storage_root, code_hash]
        account_rlp = rlp_encode_list([
            rlp_encode_int(nonce),
            rlp_encode_int(balance),
            rlp_encode_bytes(sr),
            rlp_encode_bytes(code_hash),
        ])

        key = keccak(addr)
        state_trie[key] = account_rlp

    root = state_trie.root_hash
    print(f"Python MPT root: 0x{root.hex()}")
    print(f"Expected:        0x60110f62f5e5fc861d4e4c81747a85e0856e22e068039f9863dea045a33148dc")
    print(f"C got:           0x3940771ecd8234f92a99e4b0d375d430b21edcd1b76aa4074b2806c98e740cbc")

    if root.hex() == "60110f62f5e5fc861d4e4c81747a85e0856e22e068039f9863dea045a33148dc":
        print("MATCH with expected! Issue is in C MPT computation.")
    elif root.hex() == "3940771ecd8234f92a99e4b0d375d430b21edcd1b76aa4074b2806c98e740cbc":
        print("MATCH with C output! Issue is in state data.")
    else:
        print("MATCHES NEITHER - check dump format.")

if __name__ == '__main__':
    main()
