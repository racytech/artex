#!/usr/bin/env python3
"""
Generate MPT test vectors using Python's HexaryTrie (reference implementation).
Outputs a binary file consumed by test_mpt_cross_validation.c.

Format:
  [4 bytes: num_scenarios (little-endian)]
  For each scenario:
    [1 byte: scenario_type]  0=build, 1=update
    [4 bytes: num_keys]
    For each key:
      [32 bytes: key]
      [2 bytes: value_len (LE)]
      [value_len bytes: value]
    [32 bytes: expected_root]
    If scenario_type == 1 (update):
      [4 bytes: num_dirty]
      For each dirty key:
        [32 bytes: key]
        [2 bytes: value_len (LE)]  (0 = delete)
        [value_len bytes: value]
      [32 bytes: expected_root_after_update]
"""

import os
import struct
import hashlib
import random
from trie import HexaryTrie


def write_key_values(f, keys_values):
    """Write key-value pairs to file."""
    f.write(struct.pack('<I', len(keys_values)))
    for key, value in keys_values:
        assert len(key) == 32
        f.write(key)
        f.write(struct.pack('<H', len(value)))
        f.write(value)


def scenario_build(keys_values):
    """Build trie from scratch, return root hash."""
    t = HexaryTrie({})
    for k, v in keys_values:
        t[k] = v
    return t.root_hash


def gen_build_scenario(f, keys_values, label=""):
    """Write a build scenario."""
    root = scenario_build(keys_values)
    f.write(struct.pack('B', 0))  # type = build
    write_key_values(f, keys_values)
    f.write(root)
    if label:
        print(f"  {label}: {len(keys_values)} keys, root={root.hex()}")
    return root


def gen_update_scenario(f, initial_kvs, dirty_kvs, label=""):
    """Write an update scenario: build initial, then apply dirty updates."""
    # Initial build
    initial_root = scenario_build(initial_kvs)

    # Apply updates to get new state
    merged = dict(initial_kvs)
    for k, v in dirty_kvs:
        if len(v) == 0:
            merged.pop(k, None)  # delete
        else:
            merged[k] = v

    # Build new trie with merged state
    new_root = scenario_build(list(merged.items()))

    # Write
    f.write(struct.pack('B', 1))  # type = update
    write_key_values(f, initial_kvs)
    f.write(initial_root)
    # Dirty keys
    f.write(struct.pack('<I', len(dirty_kvs)))
    for k, v in dirty_kvs:
        assert len(k) == 32
        f.write(k)
        f.write(struct.pack('<H', len(v)))
        f.write(v)
    f.write(new_root)

    if label:
        print(f"  {label}: {len(initial_kvs)} initial, "
              f"{len(dirty_kvs)} dirty, root={new_root.hex()}")
    return new_root


def make_key(i):
    """Deterministic 32-byte key from integer."""
    return hashlib.sha256(struct.pack('<Q', i)).digest()


def make_value(i, prefix=b"v"):
    """Deterministic value from integer."""
    return prefix + struct.pack('<I', i)


def main():
    random.seed(42)
    outpath = os.path.join(os.path.dirname(__file__), "mpt_vectors.bin")
    scenarios = []

    print("Generating MPT test vectors...")

    # -- Build scenarios --

    # 1. Empty trie
    scenarios.append(("build", [], "empty trie"))

    # 2. Single key
    k0 = make_key(0)
    scenarios.append(("build", [(k0, b"hello")], "single key"))

    # 3. Two keys, different first nibble
    k1 = bytes([0x11]) + b'\x00' * 31
    k2 = bytes([0x22]) + b'\x00' * 31
    scenarios.append(("build", [(k1, b"aa"), (k2, b"bb")], "two keys diff nibble"))

    # 4. Two keys, shared first nibble (extension)
    k3 = bytes([0x3A]) + b'\x00' * 31
    k4 = bytes([0x3B]) + b'\x00' * 31
    scenarios.append(("build", [(k3, b"aa"), (k4, b"bb")], "shared prefix ext"))

    # 5. Five keys spread across nibbles
    keys5 = []
    for i, b in enumerate([0x11, 0x22, 0x33, 0x44, 0x55]):
        k = bytes([b]) + b'\x00' * 31
        keys5.append((k, make_value(i)))
    scenarios.append(("build", keys5, "5 keys spread"))

    # 6. Ten keys
    keys10 = [(make_key(i), make_value(i)) for i in range(10)]
    keys10.sort()
    scenarios.append(("build", keys10, "10 random keys"))

    # 7. 100 keys
    keys100 = [(make_key(i), make_value(i)) for i in range(100)]
    keys100.sort()
    scenarios.append(("build", keys100, "100 random keys"))

    # 8. 1000 keys
    keys1000 = [(make_key(i), make_value(i)) for i in range(1000)]
    keys1000.sort()
    scenarios.append(("build", keys1000, "1000 random keys"))

    # 9. Keys with shared deep prefix
    deep_keys = []
    base = bytes([0xAA, 0xBB, 0xCC]) + b'\x00' * 29
    for i in range(5):
        k = bytearray(base)
        k[3] = i * 0x20
        deep_keys.append((bytes(k), make_value(i, b"deep")))
    deep_keys.sort()
    scenarios.append(("build", deep_keys, "deep shared prefix"))

    # -- Update scenarios --

    # 10. Single value update
    scenarios.append(("update", keys5, [(keys5[2][0], b"UPDATED")],
                      "single value update"))

    # 11. Multiple value updates
    dirty3 = [(keys10[1][0], b"NEW_1"), (keys10[4][0], b"NEW_4"),
              (keys10[7][0], b"NEW_7")]
    scenarios.append(("update", keys10, dirty3, "3 value updates"))

    # 12. Insert new key
    new_key = bytes([0x17]) + b'\x00' * 31
    scenarios.append(("update", keys5, [(new_key, b"inserted")],
                      "insert new key"))

    # 13. Insert into shared prefix
    k_new = bytes([0x3C]) + b'\x00' * 31
    scenarios.append(("update", [(k3, b"aa"), (k4, b"bb")],
                      [(k_new, b"cc")], "insert into shared prefix"))

    # 14. Extension split
    k_split = bytes([0x40]) + b'\x00' * 31
    scenarios.append(("update", [(k3, b"aa"), (k4, b"bb")],
                      [(k_split, b"split")], "extension split"))

    # 15. All keys dirty
    all_dirty = [(k, b"allnew" + bytes([i])) for i, (k, _) in enumerate(keys5)]
    scenarios.append(("update", keys5, all_dirty, "all keys dirty"))

    # 16. Insert many into existing
    new_keys = [(make_key(1000 + i), make_value(1000 + i))
                for i in range(20)]
    scenarios.append(("update", keys100, new_keys, "insert 20 into 100"))

    # 17. Update in deep prefix
    scenarios.append(("update", deep_keys,
                      [(deep_keys[2][0], b"deep_updated")],
                      "update in deep prefix"))

    # 18. Mixed: updates + inserts
    mixed_dirty = [
        (keys10[0][0], b"upd0"),  # update
        (keys10[5][0], b"upd5"),  # update
        (make_key(9999), b"brand_new"),  # insert
    ]
    scenarios.append(("update", keys10, mixed_dirty, "mixed update+insert"))

    # Write binary file
    with open(outpath, 'wb') as f:
        # Count scenarios
        num = len(scenarios)
        f.write(struct.pack('<I', num))

        for s in scenarios:
            if s[0] == "build":
                _, kvs, label = s
                gen_build_scenario(f, kvs, label)
            else:
                _, initial, dirty, label = s
                gen_update_scenario(f, initial, dirty, label)

    print(f"\nWrote {num} scenarios to {outpath}")
    print(f"File size: {os.path.getsize(outpath)} bytes")


if __name__ == '__main__':
    main()
