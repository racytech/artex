#!/usr/bin/env python3
"""
Test state root calculation for the test_dup test case.
"""

from eth_hash.auto import keccak
from rlp import encode as rlp_encode, Serializable
from rlp.sedes import big_endian_int, Binary
from trie import HexaryTrie

# RLP structure for Ethereum account
class Account(Serializable):
    fields = (
        ('nonce', big_endian_int),
        ('balance', big_endian_int),
        ('storage_root', Binary(32, 32)),
        ('code_hash', Binary(32, 32)),
    )

EMPTY_TRIE_ROOT = keccak(b'\x80')
EMPTY_CODE_HASH = keccak(b'')

# Test case data from test_dup.json
sender_addr = bytes.fromhex('10322eceef6f565eb7b831a90e05711ed89d4e15')
sender_nonce = 1
sender_balance = int('0x3635c9adc5de6373ce', 16)

coinbase_addr = bytes.fromhex('2adc25665018aa1fe0e6bc666dac8fc2697ff9ba')
coinbase_nonce = 0
coinbase_balance = int('0x3c8c32', 16)

contract_addr = bytes.fromhex('7761f573c4b44c0244612dfdf2570a17c7ebfc7b')
contract_nonce = 1
contract_balance = 0
contract_code = bytes.fromhex('6000600160026003600460056006600760086009600a600b600c600d600e600f601089600055600155600255600355600455600555600655600755600855600955600a55600b55600c55600d55600e55600f55601055')
contract_code_hash = keccak(contract_code)

# Storage for contract
storage = {
    0: 0x07,
    1: 0x10,
    2: 0x0f,
    3: 0x0e,
    4: 0x0d,
    5: 0x0c,
    6: 0x0b,
    7: 0x0a,
    8: 0x09,
    9: 0x08,
    10: 0x07,
    11: 0x06,
    12: 0x05,
    13: 0x04,
    14: 0x03,
    15: 0x02,
    16: 0x01,
}

print("Computing state root for test_dup test case...")
print()

# Compute storage root
storage_trie = HexaryTrie(db={})
for key, value in storage.items():
    if value == 0:
        continue
    # Convert key to 32-byte format
    key_bytes = key.to_bytes(32, 'big')
    # RLP encode value
    value_rlp = rlp_encode(big_endian_int.serialize(value))
    
    # Try both raw and hashed keys
    storage_trie.set(key_bytes, value_rlp)

storage_root = storage_trie.root_hash
print(f"Storage root (raw keys): {storage_root.hex()}")

# Try with hashed storage keys
storage_trie_hashed = HexaryTrie(db={})
for key, value in storage.items():
    if value == 0:
        continue
    key_bytes = key.to_bytes(32, 'big')
    key_hash = keccak(key_bytes)
    value_rlp = rlp_encode(big_endian_int.serialize(value))
    storage_trie_hashed.set(key_hash, value_rlp)

storage_root_hashed = storage_trie_hashed.root_hash
print(f"Storage root (hashed keys): {storage_root_hashed.hex()}")
print()

# Test both approaches for state trie
for use_hashed_keys in [False, True]:
    state_trie = HexaryTrie(db={})
    
    # Add sender account
    sender_account = Account(
        nonce=sender_nonce,
        balance=sender_balance,
        storage_root=EMPTY_TRIE_ROOT,
        code_hash=EMPTY_CODE_HASH
    )
    sender_rlp = rlp_encode(sender_account)
    sender_key = keccak(sender_addr) if use_hashed_keys else sender_addr
    state_trie.set(sender_key, sender_rlp)
    
    # Add coinbase account
    coinbase_account = Account(
        nonce=coinbase_nonce,
        balance=coinbase_balance,
        storage_root=EMPTY_TRIE_ROOT,
        code_hash=EMPTY_CODE_HASH
    )
    coinbase_rlp = rlp_encode(coinbase_account)
    coinbase_key = keccak(coinbase_addr) if use_hashed_keys else coinbase_addr
    state_trie.set(coinbase_key, coinbase_rlp)
    
    # Add contract account (with both raw and hashed storage roots)
    for use_hashed_storage in [False, True]:
        temp_trie = HexaryTrie(db={})
        contract_account = Account(
            nonce=contract_nonce,
            balance=contract_balance,
            storage_root=storage_root_hashed if use_hashed_storage else storage_root,
            code_hash=contract_code_hash
        )
        contract_rlp = rlp_encode(contract_account)
        contract_key = keccak(contract_addr) if use_hashed_keys else contract_addr
        
        # Clone the state trie
        for addr_key, account_key in [(sender_key, 'sender'), (coinbase_key, 'coinbase')]:
            if account_key == 'sender':
                temp_trie.set(addr_key, sender_rlp)
            else:
                temp_trie.set(addr_key, coinbase_rlp)
        
        temp_trie.set(contract_key, contract_rlp)
        
        state_root = temp_trie.root_hash
        
        key_type = "hashed" if use_hashed_keys else "raw"
        storage_type = "hashed" if use_hashed_storage else "raw"
        print(f"State root ({key_type} address keys, {storage_type} storage keys): {state_root.hex()}")

print()
print(f"Expected from test: 84a4698112964e48d1c11972b1d71dde847b5bfac0959040a6c18d6d9179d8ee")
