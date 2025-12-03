#!/usr/bin/env python3
"""
Generate valid Ethereum state test vectors for MPT and StateDB verification.

This script creates random but valid state scenarios and calculates expected
state roots using reference Ethereum libraries. The C implementation can then
verify it produces the same roots.
"""

import json
import os
import random
import secrets
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict

try:
    from eth_hash.auto import keccak
    from rlp import encode as rlp_encode, Serializable
    from rlp.sedes import big_endian_int, Binary
    from trie import HexaryTrie
    from eth_utils import decode_hex
except ImportError:
    print("Missing dependencies. Install with:")
    print("  pip install eth-hash rlp trie")
    exit(1)


# RLP structure for Ethereum account
class Account(Serializable):
    fields = (
        ('nonce', big_endian_int),
        ('balance', big_endian_int),
        ('storage_root', Binary(32, 32)),
        ('code_hash', Binary(32, 32)),
    )


# Empty trie root (keccak256 of RLP empty string 0x80)
EMPTY_TRIE_ROOT = keccak(b'\x80')

# Empty code hash (keccak256 of empty string)
EMPTY_CODE_HASH = keccak(b'')


def random_address() -> bytes:
    """Generate random 20-byte Ethereum address."""
    return secrets.token_bytes(20)


def random_hash() -> bytes:
    """Generate random 32-byte hash."""
    return secrets.token_bytes(32)


def random_balance() -> int:
    """Generate random balance (0 to 10000 ETH in wei)."""
    max_eth = 10000
    return random.randint(0, max_eth * 10**18)


def random_nonce() -> int:
    """Generate random nonce (0 to 1000)."""
    return random.randint(0, 1000)


@dataclass
class AccountData:
    """Account state data."""
    address: str  # hex string
    nonce: int
    balance: int
    storage_root: str  # hex string
    code_hash: str  # hex string
    storage: Dict[str, str]  # key -> value (hex strings)
    code: str  # hex string


@dataclass
class StateTestVector:
    """Complete state test vector."""
    name: str
    accounts: List[AccountData]
    expected_state_root: str  # hex string


def calculate_storage_root(storage: Dict[str, str]) -> bytes:
    """Calculate storage root from key-value pairs using HexaryTrie."""
    if not storage:
        return EMPTY_TRIE_ROOT
    
    # Create trie
    trie = HexaryTrie(db={})
    
    # Insert storage entries
    for key_hex, value_hex in storage.items():
        key_bytes = bytes.fromhex(key_hex.replace('0x', ''))
        value_int = int(value_hex, 16)
        
        # Storage values are RLP encoded
        if value_int == 0:
            continue  # Don't store zero values
        
        value_rlp = rlp_encode(big_endian_int.serialize(value_int))
        trie.set(key_bytes, value_rlp)
    
    return trie.root_hash


def calculate_state_root(accounts: List[AccountData]) -> bytes:
    """Calculate state root from accounts using HexaryTrie."""
    if not accounts:
        return EMPTY_TRIE_ROOT
    
    # Create trie
    trie = HexaryTrie(db={})
    
    for acc_data in accounts:
        # Address as key
        address = bytes.fromhex(acc_data.address.replace('0x', ''))
        
        # Calculate storage root
        storage_root = calculate_storage_root(acc_data.storage)
        
        # Code hash
        if acc_data.code:
            code_bytes = bytes.fromhex(acc_data.code.replace('0x', ''))
            code_hash = keccak(code_bytes)
        else:
            code_hash = EMPTY_CODE_HASH
        
        # Create account RLP
        account = Account(
            nonce=acc_data.nonce,
            balance=acc_data.balance,
            storage_root=storage_root,
            code_hash=code_hash
        )
        account_rlp = rlp_encode(account)
        
        # Insert into state trie
        trie.set(address, account_rlp)
    
    return trie.root_hash


def generate_empty_state_test() -> StateTestVector:
    """Generate test with no accounts (empty state)."""
    return StateTestVector(
        name="empty_state",
        accounts=[],
        expected_state_root=EMPTY_TRIE_ROOT.hex()
    )


def generate_single_account_test() -> StateTestVector:
    """Generate test with single account, no storage."""
    addr = random_address()
    nonce = random_nonce()
    balance = random_balance()
    
    account = AccountData(
        address='0x' + addr.hex(),
        nonce=nonce,
        balance=balance,
        storage_root=EMPTY_TRIE_ROOT.hex(),
        code_hash=EMPTY_CODE_HASH.hex(),
        storage={},
        code=''
    )
    
    state_root = calculate_state_root([account])
    
    return StateTestVector(
        name="single_account_no_storage",
        accounts=[account],
        expected_state_root=state_root.hex()
    )


def generate_account_with_storage_test() -> StateTestVector:
    """Generate test with single account with storage."""
    addr = random_address()
    nonce = random_nonce()
    balance = random_balance()
    
    # Generate random storage slots (1-5 slots)
    num_slots = random.randint(1, 5)
    storage = {}
    for _ in range(num_slots):
        slot = '0x' + secrets.token_bytes(32).hex()
        value = '0x' + random.randint(1, 2**256 - 1).to_bytes(32, 'big').hex()
        storage[slot] = value
    
    account = AccountData(
        address='0x' + addr.hex(),
        nonce=nonce,
        balance=balance,
        storage_root='',  # Will be calculated
        code_hash=EMPTY_CODE_HASH.hex(),
        storage=storage,
        code=''
    )
    
    state_root = calculate_state_root([account])
    storage_root = calculate_storage_root(storage)
    account.storage_root = storage_root.hex()
    
    return StateTestVector(
        name="account_with_storage",
        accounts=[account],
        expected_state_root=state_root.hex()
    )


def generate_multiple_accounts_test() -> StateTestVector:
    """Generate test with multiple accounts."""
    num_accounts = random.randint(2, 5)
    accounts = []
    
    for i in range(num_accounts):
        addr = random_address()
        nonce = random_nonce()
        balance = random_balance()
        
        # Some accounts have storage, some don't
        storage = {}
        if random.random() > 0.5:
            num_slots = random.randint(1, 3)
            for _ in range(num_slots):
                slot = '0x' + secrets.token_bytes(32).hex()
                value = '0x' + random.randint(1, 2**256 - 1).to_bytes(32, 'big').hex()
                storage[slot] = value
        
        account = AccountData(
            address='0x' + addr.hex(),
            nonce=nonce,
            balance=balance,
            storage_root='',  # Will be calculated
            code_hash=EMPTY_CODE_HASH.hex(),
            storage=storage,
            code=''
        )
        
        if storage:
            storage_root = calculate_storage_root(storage)
            account.storage_root = storage_root.hex()
        else:
            account.storage_root = EMPTY_TRIE_ROOT.hex()
        
        accounts.append(account)
    
    state_root = calculate_state_root(accounts)
    
    return StateTestVector(
        name=f"multiple_accounts_{num_accounts}",
        accounts=accounts,
        expected_state_root=state_root.hex()
    )


def generate_account_with_code_test() -> StateTestVector:
    """Generate test with account that has code."""
    addr = random_address()
    nonce = random_nonce()
    balance = random_balance()
    
    # Generate simple bytecode (PUSH1 0x42 PUSH1 0x00 MSTORE STOP)
    code_bytes = bytes([0x60, 0x42, 0x60, 0x00, 0x52, 0x00])
    code_hash = keccak(code_bytes)
    
    account = AccountData(
        address='0x' + addr.hex(),
        nonce=nonce,
        balance=balance,
        storage_root=EMPTY_TRIE_ROOT.hex(),
        code_hash=code_hash.hex(),
        storage={},
        code=code_bytes.hex()
    )
    
    state_root = calculate_state_root([account])
    
    return StateTestVector(
        name="account_with_code",
        accounts=[account],
        expected_state_root=state_root.hex()
    )


def main():
    """Generate test vectors and save to JSON."""
    random.seed(42)  # Deterministic for reproducibility
    
    test_vectors = []
    
    # Generate different test scenarios
    test_vectors.append(generate_empty_state_test())
    
    # Generate multiple single account tests
    for _ in range(3):
        test_vectors.append(generate_single_account_test())
    
    # Generate accounts with storage
    for _ in range(3):
        test_vectors.append(generate_account_with_storage_test())
    
    # Generate multiple accounts tests
    for _ in range(3):
        test_vectors.append(generate_multiple_accounts_test())
    
    # Generate account with code
    test_vectors.append(generate_account_with_code_test())
    
    # Convert to JSON-serializable format
    output = {
        "version": "1.0",
        "description": "State test vectors for MPT and StateDB verification",
        "tests": []
    }
    
    for i, test in enumerate(test_vectors):
        test_dict = asdict(test)
        test_dict["name"] = f"{i:02d}_{test.name}"
        
        # Convert large balances to strings to preserve precision
        for account in test_dict["accounts"]:
            account["balance"] = str(account["balance"])
        
        output["tests"].append(test_dict)
    
    # Save to file (use absolute path relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, "mpt_test_vectors.json")
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"Generated {len(test_vectors)} test vectors")
    print(f"Saved to {output_file}")
    
    # Print summary
    print("\nTest Summary:")
    for test in output["tests"]:
        num_accounts = len(test["accounts"])
        total_storage = sum(len(acc["storage"]) for acc in test["accounts"])
        print(f"  {test['name']}: {num_accounts} accounts, {total_storage} storage slots")


if __name__ == "__main__":
    main()
