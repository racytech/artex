#!/usr/bin/env python3
"""Debug MPT genesis root computation - compare C output vs Python reference."""
import json, hashlib, struct, sys
from trie import HexaryTrie
import rlp

def keccak256(data):
    from Crypto.Hash import keccak
    return keccak.new(digest_bits=256, data=data).digest()

# Empty code hash and empty storage root
EMPTY_CODE_HASH = keccak256(b'')
EMPTY_STORAGE_ROOT = keccak256(bytes([0x80]))

def rlp_encode_account(nonce, balance, storage_root, code_hash):
    """RLP encode account: [nonce, balance, storage_root, code_hash]"""
    return rlp.encode([nonce, balance, storage_root, code_hash])

def main():
    genesis_path = sys.argv[1] if len(sys.argv) > 1 else "../data/mainnet_genesis.json"

    with open(genesis_path) as f:
        genesis = json.load(f)

    # Our genesis format is flat: {addr: {balance, code, ...}, ...}
    # (no "alloc" wrapper)
    alloc = genesis.get("alloc", genesis)
    print(f"Genesis accounts: {len(alloc)}")

    t = HexaryTrie({})

    count = 0
    for addr_hex, acct in sorted(alloc.items()):
        addr_hex = addr_hex.lower()
        if addr_hex.startswith("0x"):
            addr_hex = addr_hex[2:]
        addr_bytes = bytes.fromhex(addr_hex)

        nonce = int(acct.get("nonce", "0x0"), 16) if isinstance(acct.get("nonce"), str) else acct.get("nonce", 0)
        balance_str = acct.get("balance", "0")
        if isinstance(balance_str, str):
            if balance_str.startswith("0x"):
                balance = int(balance_str, 16)
            else:
                balance = int(balance_str)
        else:
            balance = balance_str

        code = bytes.fromhex(acct["code"][2:]) if acct.get("code") else b''
        code_hash = keccak256(code) if code else EMPTY_CODE_HASH

        # TODO: handle storage if present
        storage_root = EMPTY_STORAGE_ROOT

        # RLP encode account
        account_rlp = rlp_encode_account(nonce, balance, storage_root, code_hash)

        # Key = keccak256(address)
        key = keccak256(addr_bytes)

        t[key] = account_rlp
        count += 1

        if count <= 3:
            print(f"  Account {addr_hex}: nonce={nonce} balance={balance}")
            print(f"    key={key.hex()}")
            print(f"    rlp={account_rlp.hex()} (len={len(account_rlp)})")

    print(f"\nGenesis root: 0x{t.root_hash.hex()}")
    print(f"Expected:     0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544")
    print(f"Match: {t.root_hash.hex() == 'd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544'}")

if __name__ == '__main__':
    main()
