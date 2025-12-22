#!/usr/bin/env python3
"""
Manually compute MPT root from post-state JSON to debug state root mismatch.
This helps identify if the issue is in our MPT implementation or account data.

Uses HexaryTrie library for MPT computation.
Install: pip install trie
"""

import json
import sys
from Crypto.Hash import keccak
from trie import HexaryTrie

def keccak256(data: bytes) -> bytes:
    """Compute Keccak-256 hash."""
    k = keccak.new(digest_bits=256)
    k.update(data)
    return k.digest()

def rlp_encode_length(length: int, offset: int) -> bytes:
    """RLP encode a length value."""
    if length < 56:
        return bytes([length + offset])
    elif length < 256**8:
        bl = length.to_bytes((length.bit_length() + 7) // 8, 'big')
        return bytes([len(bl) + offset + 55]) + bl
    else:
        raise ValueError("Length too large for RLP encoding")

def rlp_encode_bytes(data: bytes) -> bytes:
    """RLP encode a byte string."""
    if len(data) == 1 and data[0] < 128:
        return data
    else:
        return rlp_encode_length(len(data), 128) + data

def rlp_encode_list(items: list) -> bytes:
    """RLP encode a list of items."""
    output = b''.join(items)
    return rlp_encode_length(len(output), 192) + output

def hex_to_bytes(hex_str: str) -> bytes:
    """Convert hex string to bytes, handling '0x' prefix."""
    if hex_str.startswith('0x') or hex_str.startswith('0X'):
        hex_str = hex_str[2:]
    if not hex_str:
        return b''
    # Pad to even length
    if len(hex_str) % 2:
        hex_str = '0' + hex_str
    return bytes.fromhex(hex_str)

def int_to_bytes_minimal(value: int) -> bytes:
    """Convert integer to minimal big-endian bytes (no leading zeros)."""
    if value == 0:
        return b''
    return value.to_bytes((value.bit_length() + 7) // 8, 'big')

def encode_account(nonce: int, balance: int, storage_root: bytes, code_hash: bytes) -> bytes:
    """
    Encode an Ethereum account as RLP: [nonce, balance, storage_root, code_hash]
    """
    nonce_bytes = int_to_bytes_minimal(nonce)
    balance_bytes = int_to_bytes_minimal(balance)
    
    items = [
        rlp_encode_bytes(nonce_bytes),
        rlp_encode_bytes(balance_bytes),
        rlp_encode_bytes(storage_root),
        rlp_encode_bytes(code_hash)
    ]
    
    return rlp_encode_list(items)

def main():
    if len(sys.argv) < 2:
        print("Usage: compute_state_root.py <post_state.json>")
        sys.exit(1)
    
    post_state_file = sys.argv[1]
    
    with open(post_state_file, 'r') as f:
        post_state = json.load(f)
    
    print("=" * 80)
    print("MPT State Root Computation")
    print("=" * 80)
    print(f"Post-state file: {post_state_file}")
    print(f"Total accounts: {len(post_state)}")
    print()
    
    # Create a new trie
    trie = HexaryTrie(db={})
    
    # For each account, compute address hash and RLP encoding
    for addr_str, account in sorted(post_state.items()):
        addr_bytes = hex_to_bytes(addr_str)
        nonce = int(account.get('nonce', '0x0'), 16)
        balance = int(account.get('balance', '0x0'), 16)
        
        # Get code hash
        code = account.get('code', '0x')
        if code and code != '0x':
            code_bytes = hex_to_bytes(code)
            code_hash = keccak256(code_bytes)
        else:
            # Empty code hash
            code_hash = keccak256(b'')
        
        # Get storage root - for now assume empty
        # TODO: Compute storage root from storage dict
        storage = account.get('storage', {})
        if storage:
            print(f"WARNING: Account {addr_str} has storage, but we're using empty storage root")
            storage_root = keccak256(b'')  # Placeholder
        else:
            storage_root = bytes.fromhex('56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421')
        
        # Encode account
        account_rlp = encode_account(nonce, balance, storage_root, code_hash)
        
        # Hash address for secure trie
        addr_hash = keccak256(addr_bytes)
        
        print(f"Account: {addr_str}")
        print(f"  Nonce: {nonce}")
        print(f"  Balance: {balance} ({hex(balance)})")
        print(f"  Code hash: 0x{code_hash.hex()}")
        print(f"  Storage root: 0x{storage_root.hex()}")
        print(f"  Address hash: 0x{addr_hash.hex()}")
        print(f"  RLP encoded: 0x{account_rlp.hex()} ({len(account_rlp)} bytes)")
        print()
        
        # Insert into trie using hashed address as key
        trie[addr_hash] = account_rlp
    
    # Get the root hash
    root_hash = trie.root_hash
    
    print("=" * 80)
    print(f"Computed MPT Root: 0x{root_hash.hex()}")
    print("=" * 80)
    
    return root_hash

if __name__ == '__main__':
    main()
