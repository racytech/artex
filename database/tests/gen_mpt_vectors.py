#!/usr/bin/env python3
"""
Generate MPT test vectors using Python's HexaryTrie (reference implementation).
Outputs a binary file consumed by test_mpt_store_vectors.c.

Format:
  [4 bytes: num_scenarios (little-endian)]
  For each scenario:
    [1 byte: scenario_type]  0=build, 1=update, 2=multi-round
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
    If scenario_type == 2 (multi-round):
      [1 byte: num_rounds]
      For each round:
        [4 bytes: num_dirty]
        For each dirty key:
          [32 bytes: key]
          [2 bytes: value_len (LE)]  (0 = delete)
          [value_len bytes: value]
        [32 bytes: expected_root_after_round]
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


def write_dirty_entries(f, dirty_kvs):
    """Write dirty key-value pairs (value_len=0 means delete)."""
    f.write(struct.pack('<I', len(dirty_kvs)))
    for k, v in dirty_kvs:
        assert len(k) == 32
        f.write(k)
        f.write(struct.pack('<H', len(v)))
        f.write(v)


def scenario_build(keys_values):
    """Build trie from scratch, return root hash."""
    t = HexaryTrie({})
    for k, v in keys_values:
        t[k] = v
    return t.root_hash


def apply_dirty(state, dirty_kvs):
    """Apply dirty updates to a state dict, return new state."""
    state = dict(state)
    for k, v in dirty_kvs:
        if len(v) == 0:
            state.pop(k, None)
        else:
            state[k] = v
    return state


def gen_build_scenario(f, keys_values, label=""):
    """Write a build scenario."""
    root = scenario_build(keys_values)
    f.write(struct.pack('B', 0))  # type = build
    write_key_values(f, keys_values)
    f.write(root)
    if label:
        print(f"  {label}: {len(keys_values)} keys, root={root.hex()[:16]}...")
    return root


def gen_update_scenario(f, initial_kvs, dirty_kvs, label=""):
    """Write an update scenario: build initial, then apply dirty updates."""
    initial_root = scenario_build(initial_kvs)
    merged = apply_dirty(dict(initial_kvs), dirty_kvs)
    new_root = scenario_build(list(merged.items()))

    f.write(struct.pack('B', 1))  # type = update
    write_key_values(f, initial_kvs)
    f.write(initial_root)
    write_dirty_entries(f, dirty_kvs)
    f.write(new_root)

    if label:
        print(f"  {label}: {len(initial_kvs)} initial, "
              f"{len(dirty_kvs)} dirty, root={new_root.hex()[:16]}...")
    return new_root


def gen_multi_round_scenario(f, initial_kvs, rounds, label=""):
    """Write a multi-round scenario: build initial, then apply N rounds."""
    initial_root = scenario_build(initial_kvs)

    f.write(struct.pack('B', 2))  # type = multi-round
    write_key_values(f, initial_kvs)
    f.write(initial_root)
    f.write(struct.pack('B', len(rounds)))

    state = dict(initial_kvs)
    for rnd_idx, dirty_kvs in enumerate(rounds):
        state = apply_dirty(state, dirty_kvs)
        rnd_root = scenario_build(list(state.items()))
        write_dirty_entries(f, dirty_kvs)
        f.write(rnd_root)

    if label:
        final_root = scenario_build(list(state.items()))
        print(f"  {label}: {len(initial_kvs)} initial, "
              f"{len(rounds)} rounds, root={final_root.hex()[:16]}...")
    return state


def make_key(i):
    """Deterministic 32-byte key from integer."""
    return hashlib.sha256(struct.pack('<Q', i)).digest()


def make_value(i, prefix=b"v"):
    """Deterministic value from integer."""
    return prefix + struct.pack('<I', i)


def make_account_rlp(nonce, balance, storage_root=None, code_hash=None):
    """Make a realistic account RLP value (simplified)."""
    import rlp
    if storage_root is None:
        storage_root = bytes.fromhex(
            "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
    if code_hash is None:
        code_hash = bytes.fromhex(
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
    return rlp.encode([nonce, balance, storage_root, code_hash])


def main():
    random.seed(42)
    outpath = os.path.join(os.path.dirname(__file__), "mpt_vectors.bin")

    print("Generating MPT test vectors...")

    # Pre-generate reusable key sets
    k0 = make_key(0)

    k1 = bytes([0x11]) + b'\x00' * 31
    k2 = bytes([0x22]) + b'\x00' * 31

    k3 = bytes([0x3A]) + b'\x00' * 31
    k4 = bytes([0x3B]) + b'\x00' * 31

    keys5 = []
    for i, b in enumerate([0x11, 0x22, 0x33, 0x44, 0x55]):
        k = bytes([b]) + b'\x00' * 31
        keys5.append((k, make_value(i)))

    keys10 = [(make_key(i), make_value(i)) for i in range(10)]
    keys10.sort()

    keys100 = [(make_key(i), make_value(i)) for i in range(100)]
    keys100.sort()

    keys1000 = [(make_key(i), make_value(i)) for i in range(1000)]
    keys1000.sort()

    deep_keys = []
    base = bytes([0xAA, 0xBB, 0xCC]) + b'\x00' * 29
    for i in range(5):
        k = bytearray(base)
        k[3] = i * 0x20
        deep_keys.append((bytes(k), make_value(i, b"deep")))
    deep_keys.sort()

    with open(outpath, 'wb') as f:
        # Placeholder for scenario count
        f.write(struct.pack('<I', 0))
        count = 0

        # =====================================================================
        # BUILD SCENARIOS
        # =====================================================================

        # 1. Empty trie
        gen_build_scenario(f, [], "empty trie"); count += 1

        # 2. Single key
        gen_build_scenario(f, [(k0, b"hello")], "single key"); count += 1

        # 3. Two keys, different first nibble
        gen_build_scenario(f, [(k1, b"aa"), (k2, b"bb")],
                           "two keys diff nibble"); count += 1

        # 4. Two keys, shared first nibble (extension)
        gen_build_scenario(f, [(k3, b"aa"), (k4, b"bb")],
                           "shared prefix ext"); count += 1

        # 5. Five keys spread across nibbles
        gen_build_scenario(f, keys5, "5 keys spread"); count += 1

        # 6. Ten keys
        gen_build_scenario(f, keys10, "10 random keys"); count += 1

        # 7. 100 keys
        gen_build_scenario(f, keys100, "100 random keys"); count += 1

        # 8. 1000 keys
        gen_build_scenario(f, keys1000, "1000 random keys"); count += 1

        # 9. Keys with shared deep prefix
        gen_build_scenario(f, deep_keys, "deep shared prefix"); count += 1

        # 10. Large account-like RLP values
        try:
            acct_keys = [(make_key(5000 + i),
                          make_account_rlp(i, i * 10**18))
                         for i in range(50)]
            acct_keys.sort()
            gen_build_scenario(f, acct_keys,
                               "50 account-like RLP values"); count += 1
        except ImportError:
            # rlp not available, use large synthetic values
            acct_keys = [(make_key(5000 + i),
                          b'\x01' * 80 + struct.pack('<Q', i))
                         for i in range(50)]
            acct_keys.sort()
            gen_build_scenario(f, acct_keys,
                               "50 large values (88 bytes)"); count += 1

        # 11. 5000 keys
        keys5000 = [(make_key(i), make_value(i)) for i in range(5000)]
        keys5000.sort()
        gen_build_scenario(f, keys5000, "5000 random keys"); count += 1

        # 12. All 16 nibbles populated at root
        root16 = []
        for nibble in range(16):
            k = bytes([nibble * 0x10]) + b'\x00' * 31
            root16.append((k, make_value(nibble, b"n")))
        root16.sort()
        gen_build_scenario(f, root16,
                           "16 keys all root nibbles"); count += 1

        # 13. Keys sharing 62 nibbles (differ only in last nibble)
        prefix62 = bytes([0xDE, 0xAD, 0xBE, 0xEF] + [0x11] * 27 + [0x00])
        near_keys = []
        for i in range(4):
            k = bytearray(prefix62)
            k[31] = i * 0x11  # differ in last byte = last 2 nibbles
            near_keys.append((bytes(k), make_value(i, b"near")))
        near_keys.sort()
        gen_build_scenario(f, near_keys,
                           "4 keys differ in last byte"); count += 1

        # 14. Single-byte values (tests inline nodes)
        tiny_keys = [(make_key(8000 + i), bytes([i + 1]))
                     for i in range(20)]
        tiny_keys.sort()
        gen_build_scenario(f, tiny_keys,
                           "20 keys tiny 1-byte values"); count += 1

        # 15. Large values (200+ bytes)
        big_keys = [(make_key(9000 + i), os.urandom(200))
                    for i in range(10)]
        # Use deterministic "random" for reproducibility
        rng = random.Random(123)
        big_keys = [(make_key(9000 + i),
                     bytes(rng.getrandbits(8) for _ in range(200)))
                    for i in range(10)]
        big_keys.sort()
        gen_build_scenario(f, big_keys,
                           "10 keys large 200-byte values"); count += 1

        # =====================================================================
        # UPDATE SCENARIOS (type=1)
        # =====================================================================

        # 16. Single value update
        gen_update_scenario(f, keys5, [(keys5[2][0], b"UPDATED")],
                            "single value update"); count += 1

        # 17. Multiple value updates
        dirty3 = [(keys10[1][0], b"NEW_1"), (keys10[4][0], b"NEW_4"),
                  (keys10[7][0], b"NEW_7")]
        gen_update_scenario(f, keys10, dirty3,
                            "3 value updates"); count += 1

        # 18. Insert new key
        new_key = bytes([0x17]) + b'\x00' * 31
        gen_update_scenario(f, keys5, [(new_key, b"inserted")],
                            "insert new key"); count += 1

        # 19. Insert into shared prefix
        k_new = bytes([0x3C]) + b'\x00' * 31
        gen_update_scenario(f, [(k3, b"aa"), (k4, b"bb")],
                            [(k_new, b"cc")],
                            "insert into shared prefix"); count += 1

        # 20. Extension split
        k_split = bytes([0x40]) + b'\x00' * 31
        gen_update_scenario(f, [(k3, b"aa"), (k4, b"bb")],
                            [(k_split, b"split")],
                            "extension split"); count += 1

        # 21. All keys dirty
        all_dirty = [(k, b"allnew" + bytes([i]))
                     for i, (k, _) in enumerate(keys5)]
        gen_update_scenario(f, keys5, all_dirty,
                            "all keys dirty"); count += 1

        # 22. Insert many into existing
        new_keys20 = [(make_key(1000 + i), make_value(1000 + i))
                      for i in range(20)]
        gen_update_scenario(f, keys100, new_keys20,
                            "insert 20 into 100"); count += 1

        # 23. Update in deep prefix
        gen_update_scenario(f, deep_keys,
                            [(deep_keys[2][0], b"deep_updated")],
                            "update in deep prefix"); count += 1

        # 24. Mixed: updates + inserts
        mixed_dirty = [
            (keys10[0][0], b"upd0"),
            (keys10[5][0], b"upd5"),
            (make_key(9999), b"brand_new"),
        ]
        gen_update_scenario(f, keys10, mixed_dirty,
                            "mixed update+insert"); count += 1

        # =====================================================================
        # DELETE SCENARIOS (type=1, dirty with value_len=0)
        # =====================================================================

        # 25. Delete single key from 2-key trie
        gen_update_scenario(f, [(k1, b"aa"), (k2, b"bb")],
                            [(k1, b"")],
                            "delete 1 of 2 keys"); count += 1

        # 26. Delete single key from 5-key trie
        gen_update_scenario(f, keys5, [(keys5[0][0], b"")],
                            "delete 1 of 5 keys"); count += 1

        # 27. Delete all keys (→ empty trie)
        del_all5 = [(k, b"") for k, _ in keys5]
        gen_update_scenario(f, keys5, del_all5,
                            "delete all 5 keys"); count += 1

        # 28. Delete from extension pair (should collapse)
        gen_update_scenario(f, [(k3, b"aa"), (k4, b"bb")],
                            [(k3, b"")],
                            "delete from ext pair (collapse)"); count += 1

        # 29. Delete 1 of 16 root-level keys (branch with 15 children)
        gen_update_scenario(f, root16, [(root16[7][0], b"")],
                            "delete 1 of 16 root keys"); count += 1

        # 30. Delete 15 of 16 keys (single leaf remains)
        del_15 = [(k, b"") for k, _ in root16[:15]]
        gen_update_scenario(f, root16, del_15,
                            "delete 15 of 16 root keys"); count += 1

        # 31. Delete from deep prefix
        gen_update_scenario(f, deep_keys, [(deep_keys[0][0], b"")],
                            "delete from deep prefix"); count += 1

        # 32. Delete non-existent key (should be a no-op)
        phantom_key = bytes([0xFF]) + b'\x00' * 31
        gen_update_scenario(f, keys5, [(phantom_key, b"")],
                            "delete non-existent key (noop)"); count += 1

        # 33. Delete + insert in same batch
        gen_update_scenario(
            f, keys10,
            [(keys10[0][0], b""),           # delete
             (keys10[3][0], b""),           # delete
             (make_key(7777), b"new_ins"),  # insert
             (keys10[8][0], b"updated8")],  # update
            "mixed delete+insert+update"); count += 1

        # 34. Delete causing branch→extension collapse
        # Build 3 keys sharing first nibble, delete 1 → should collapse
        # branch to extension
        ka = bytes([0x5A]) + bytes([0x11]) + b'\x00' * 30
        kb = bytes([0x5A]) + bytes([0x22]) + b'\x00' * 30
        kc = bytes([0x5B]) + b'\x00' * 31
        gen_update_scenario(
            f, [(ka, b"A"), (kb, b"B"), (kc, b"C")],
            [(kc, b"")],
            "delete causes branch→ext collapse"); count += 1

        # 35. Delete then re-insert same key with different value
        gen_update_scenario(
            f, keys5,
            [(keys5[1][0], b""),                # delete
             (keys5[1][0], b"resurrected!")],    # re-insert
            "delete then re-insert same key"); count += 1

        # 36. Delete all from 100-key trie
        del_all100 = [(k, b"") for k, _ in keys100]
        gen_update_scenario(f, keys100, del_all100,
                            "delete all 100 keys"); count += 1

        # 37. Delete half the keys from 100
        del_half = [(keys100[i][0], b"") for i in range(0, 100, 2)]
        gen_update_scenario(f, keys100, del_half,
                            "delete 50 of 100 keys"); count += 1

        # 38. Delete from 1000-key trie (100 deletes)
        del_100_of_1000 = [(keys1000[i][0], b"") for i in range(0, 1000, 10)]
        gen_update_scenario(f, keys1000, del_100_of_1000,
                            "delete 100 of 1000 keys"); count += 1

        # 39. Insert into empty trie (initial = empty, dirty = inserts)
        gen_update_scenario(
            f, [],
            [(make_key(i + 20000), make_value(i)) for i in range(10)],
            "insert 10 into empty trie"); count += 1

        # =====================================================================
        # MULTI-ROUND SCENARIOS (type=2)
        # =====================================================================

        # 40. 5 rounds of inserts (10 keys each)
        initial_mr = [(make_key(10000 + i), make_value(i))
                      for i in range(10)]
        initial_mr.sort()
        rounds_inserts = []
        for rnd in range(5):
            dirty = [(make_key(10010 + rnd * 10 + i),
                      make_value(10 + rnd * 10 + i))
                     for i in range(10)]
            rounds_inserts.append(dirty)
        gen_multi_round_scenario(f, initial_mr, rounds_inserts,
                                 "5 rounds of 10 inserts"); count += 1

        # 41. Insert then delete (2 rounds)
        mr_initial2 = [(make_key(11000 + i), make_value(i))
                       for i in range(20)]
        mr_initial2.sort()
        round1 = [(make_key(11020 + i), make_value(100 + i))
                  for i in range(10)]  # insert 10
        round2 = [(mr_initial2[i][0], b"")
                  for i in range(0, 20, 2)]  # delete 10 originals
        gen_multi_round_scenario(f, mr_initial2, [round1, round2],
                                 "insert then delete (2 rounds)"); count += 1

        # 42. Grow and shrink: insert 50, then delete 40
        mr_base = [(make_key(12000 + i), make_value(i)) for i in range(10)]
        mr_base.sort()
        grow = [(make_key(12010 + i), make_value(100 + i))
                for i in range(50)]
        shrink = [(make_key(12010 + i), b"") for i in range(40)]
        gen_multi_round_scenario(f, mr_base, [grow, shrink],
                                 "grow 50 then shrink 40"); count += 1

        # 43. 10 rounds of mixed ops (insert, update, delete)
        mr_mixed_init = [(make_key(13000 + i), make_value(i))
                         for i in range(50)]
        mr_mixed_init.sort()
        mr_rng = random.Random(999)
        all_keys_pool = [make_key(13000 + i) for i in range(200)]
        current_state = dict(mr_mixed_init)
        mr_rounds = []
        for rnd in range(10):
            dirty = []
            # Insert 3 new keys
            for j in range(3):
                k = all_keys_pool[50 + rnd * 5 + j]
                dirty.append((k, make_value(1000 + rnd * 10 + j)))
            # Update 2 existing keys
            existing = [k for k in current_state.keys()]
            if len(existing) >= 2:
                for k in mr_rng.sample(existing, min(2, len(existing))):
                    dirty.append((k, make_value(2000 + rnd * 10)))
            # Delete 1 existing key
            if len(existing) >= 1:
                dk = mr_rng.choice(existing)
                # Don't delete a key we just updated in this round
                dirty_keys_set = set(k for k, _ in dirty)
                if dk not in dirty_keys_set:
                    dirty.append((dk, b""))

            mr_rounds.append(dirty)
            current_state = apply_dirty(current_state, dirty)

        gen_multi_round_scenario(f, mr_mixed_init, mr_rounds,
                                 "10 rounds mixed ops"); count += 1

        # 44. Delete all then rebuild from scratch
        mr_rebuild_init = [(make_key(14000 + i), make_value(i))
                           for i in range(20)]
        mr_rebuild_init.sort()
        del_all = [(k, b"") for k, _ in mr_rebuild_init]
        rebuild = [(make_key(14100 + i), make_value(500 + i))
                   for i in range(15)]
        gen_multi_round_scenario(f, mr_rebuild_init, [del_all, rebuild],
                                 "delete all then rebuild"); count += 1

        # 45. Single key lifecycle: insert → update → delete → re-insert
        single_k = make_key(15000)
        gen_multi_round_scenario(
            f,
            [(single_k, b"v1")],
            [
                [(single_k, b"v2")],               # update
                [(single_k, b"")],                  # delete
                [(single_k, b"v3_resurrected")],    # re-insert
            ],
            "single key lifecycle"); count += 1

        # 46. Large scale multi-round: 1000 initial, 5 rounds of 100 ops
        mr_large = [(make_key(16000 + i), make_value(i))
                    for i in range(1000)]
        mr_large.sort()
        large_rng = random.Random(456)
        large_rounds = []
        large_state = dict(mr_large)
        for rnd in range(5):
            dirty = []
            # 30 inserts
            for j in range(30):
                k = make_key(17000 + rnd * 100 + j)
                dirty.append((k, make_value(5000 + rnd * 100 + j)))
            # 40 updates
            existing = list(large_state.keys())
            for k in large_rng.sample(existing, min(40, len(existing))):
                dirty.append((k, make_value(6000 + rnd * 100)))
            # 30 deletes
            remaining = [k for k in existing
                         if k not in set(dk for dk, _ in dirty)]
            for k in large_rng.sample(remaining, min(30, len(remaining))):
                dirty.append((k, b""))
            large_rounds.append(dirty)
            large_state = apply_dirty(large_state, dirty)

        gen_multi_round_scenario(f, mr_large, large_rounds,
                                 "1000 init + 5 rounds of 100 ops"); count += 1

        # Rewrite scenario count
        f.seek(0)
        f.write(struct.pack('<I', count))

    print(f"\nWrote {count} scenarios to {outpath}")
    print(f"File size: {os.path.getsize(outpath)} bytes")


if __name__ == '__main__':
    main()
