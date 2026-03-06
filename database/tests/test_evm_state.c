/*
 * EVM State Test Suite
 *
 * 6 phases:
 *   1. Account operations (nonce, balance)
 *   2. Storage operations
 *   3. Snapshot & revert
 *   4. Code operations
 *   5. Self-destruct
 *   6. Access lists (EIP-2929)
 */

#include "../include/evm_state.h"
#include "../include/account.h"
#include "../../common/include/uint256.h"
#include "../../common/include/address.h"
#include "../../common/include/hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <unistd.h>

// ============================================================================
// Test helpers
// ============================================================================

static int tests_passed = 0;
static int tests_failed = 0;

#define ASSERT(cond, fmt, ...) do { \
    if (!(cond)) { \
        printf("  FAIL [%s:%d]: " fmt "\n", __func__, __LINE__, ##__VA_ARGS__); \
        tests_failed++; \
        return; \
    } else { \
        tests_passed++; \
    } \
} while(0)

#define TEST_DIR "/tmp/test_evm_state"

static void cleanup_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    (void)system(cmd);
}

static address_t make_addr(uint8_t seed) {
    address_t addr;
    memset(addr.bytes, seed, ADDRESS_SIZE);
    addr.bytes[0] = seed;
    addr.bytes[19] = seed;
    return addr;
}

static uint256_t make_u256(uint64_t val) {
    return uint256_from_uint64(val);
}

// ============================================================================
// Phase 1: Account Operations
// ============================================================================

static void test_account_ops(void) {
    printf("\n--- Phase 1: Account Operations ---\n");

    cleanup_dir(TEST_DIR);
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL, "sdb_create failed");

    evm_state_t *es = evm_state_create(sdb);
    ASSERT(es != NULL, "evm_state_create failed");

    // Set nonce and balance on 10 accounts
    for (uint8_t i = 0; i < 10; i++) {
        address_t addr = make_addr(i);
        evm_state_set_nonce(es, &addr, (uint64_t)(i + 1));
        uint256_t bal = make_u256((uint64_t)(i + 1) * 1000);
        evm_state_set_balance(es, &addr, &bal);
    }

    // Verify reads
    for (uint8_t i = 0; i < 10; i++) {
        address_t addr = make_addr(i);
        uint64_t nonce = evm_state_get_nonce(es, &addr);
        ASSERT(nonce == (uint64_t)(i + 1), "nonce[%u] = %" PRIu64, i, nonce);

        uint256_t bal = evm_state_get_balance(es, &addr);
        uint256_t expected = make_u256((uint64_t)(i + 1) * 1000);
        ASSERT(uint256_is_equal(&bal, &expected), "balance[%u] mismatch", i);
    }
    printf("  10 accounts set/get:       OK\n");

    // Increment nonce
    {
        address_t addr = make_addr(0);
        evm_state_increment_nonce(es, &addr);
        uint64_t nonce = evm_state_get_nonce(es, &addr);
        ASSERT(nonce == 2, "increment_nonce: expected 2, got %" PRIu64, nonce);
    }
    printf("  increment nonce:           OK\n");

    // Add balance
    {
        address_t addr = make_addr(0);
        uint256_t amt = make_u256(500);
        evm_state_add_balance(es, &addr, &amt);
        uint256_t bal = evm_state_get_balance(es, &addr);
        uint256_t expected = make_u256(1500);
        ASSERT(uint256_is_equal(&bal, &expected), "add_balance: expected 1500");
    }
    printf("  add balance:               OK\n");

    // Sub balance (success)
    {
        address_t addr = make_addr(0);
        uint256_t amt = make_u256(200);
        bool ok = evm_state_sub_balance(es, &addr, &amt);
        ASSERT(ok, "sub_balance should succeed");
        uint256_t bal = evm_state_get_balance(es, &addr);
        uint256_t expected = make_u256(1300);
        ASSERT(uint256_is_equal(&bal, &expected), "sub_balance: expected 1300");
    }
    printf("  sub balance (success):     OK\n");

    // Sub balance (insufficient)
    {
        address_t addr = make_addr(0);
        uint256_t amt = make_u256(999999);
        bool ok = evm_state_sub_balance(es, &addr, &amt);
        ASSERT(!ok, "sub_balance should fail");
        // Balance should be unchanged
        uint256_t bal = evm_state_get_balance(es, &addr);
        uint256_t expected = make_u256(1300);
        ASSERT(uint256_is_equal(&bal, &expected), "balance should be unchanged");
    }
    printf("  sub balance (insufficient): OK\n");

    // Is empty
    {
        address_t empty_addr = make_addr(99);
        ASSERT(evm_state_is_empty(es, &empty_addr), "unset account should be empty");

        address_t addr = make_addr(0);
        ASSERT(!evm_state_is_empty(es, &addr), "account with balance not empty");
    }
    printf("  is_empty:                  OK\n");

    // Finalize and verify persistence
    ASSERT(evm_state_finalize(es), "finalize failed");
    sdb_merge(sdb);

    // Verify account 0 persisted
    {
        address_t addr = make_addr(0);
        uint8_t addr_hash[32];
        hash_t h = hash_keccak256(addr.bytes, ADDRESS_SIZE);
        memcpy(addr_hash, h.bytes, 32);

        uint8_t buf[ACCOUNT_MAX_ENCODED];
        uint16_t buf_len = 0;
        ASSERT(sdb_get(sdb, addr_hash, buf, &buf_len), "sdb_get failed");

        account_t acct;
        ASSERT(account_decode(buf, buf_len, &acct), "decode failed");
        ASSERT(acct.nonce == 2, "persisted nonce = %" PRIu64, acct.nonce);
        uint256_t expected = make_u256(1300);
        ASSERT(uint256_is_equal(&acct.balance, &expected), "persisted balance mismatch");
    }
    printf("  finalize + persistence:    OK\n");

    evm_state_destroy(es);
    sdb_destroy(sdb);
}

// ============================================================================
// Phase 2: Storage Operations
// ============================================================================

static void test_storage_ops(void) {
    printf("\n--- Phase 2: Storage Operations ---\n");

    cleanup_dir(TEST_DIR);
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL, "sdb_create failed");

    evm_state_t *es = evm_state_create(sdb);
    ASSERT(es != NULL, "evm_state_create failed");

    // Set 100 storage slots across 5 accounts (20 each)
    for (uint8_t a = 0; a < 5; a++) {
        address_t addr = make_addr(a);
        // Need to create account so it exists
        evm_state_set_nonce(es, &addr, 1);

        for (uint8_t s = 0; s < 20; s++) {
            uint256_t key = make_u256(s);
            uint256_t val = make_u256((uint64_t)(a + 1) * 100 + s);
            evm_state_set_storage(es, &addr, &key, &val);
        }
    }

    // Read back and verify
    for (uint8_t a = 0; a < 5; a++) {
        address_t addr = make_addr(a);
        for (uint8_t s = 0; s < 20; s++) {
            uint256_t key = make_u256(s);
            uint256_t val = evm_state_get_storage(es, &addr, &key);
            uint256_t expected = make_u256((uint64_t)(a + 1) * 100 + s);
            ASSERT(uint256_is_equal(&val, &expected),
                   "storage[%u][%u] mismatch", a, s);
        }
    }
    printf("  100 storage slots set/get: OK\n");

    // Overwrite some slots
    {
        address_t addr = make_addr(0);
        uint256_t key = make_u256(5);
        uint256_t new_val = make_u256(99999);
        evm_state_set_storage(es, &addr, &key, &new_val);

        uint256_t got = evm_state_get_storage(es, &addr, &key);
        ASSERT(uint256_is_equal(&got, &new_val), "overwrite mismatch");
    }
    printf("  overwrite slot:            OK\n");

    // Unset slot returns zero
    {
        address_t addr = make_addr(0);
        uint256_t key = make_u256(999);
        uint256_t got = evm_state_get_storage(es, &addr, &key);
        ASSERT(uint256_is_zero(&got), "unset slot should be zero");
    }
    printf("  unset slot = zero:         OK\n");

    // Finalize and verify persistence
    ASSERT(evm_state_finalize(es), "finalize failed");
    sdb_merge(sdb);

    // Verify a slot persisted in state_db
    {
        address_t addr = make_addr(1);
        uint8_t addr_hash[32], slot_hash[32];
        hash_t ah = hash_keccak256(addr.bytes, ADDRESS_SIZE);
        memcpy(addr_hash, ah.bytes, 32);

        uint256_t slot = make_u256(10);
        uint8_t slot_bytes[32];
        uint256_to_bytes(&slot, slot_bytes);
        hash_t sh = hash_keccak256(slot_bytes, 32);
        memcpy(slot_hash, sh.bytes, 32);

        uint8_t val_buf[32];
        uint16_t val_len = 0;
        ASSERT(sdb_get_storage(sdb, addr_hash, slot_hash, val_buf, &val_len),
               "sdb_get_storage failed");

        // Reconstruct value — stored as trimmed big-endian
        uint256_t got = uint256_from_bytes(val_buf, val_len);
        uint256_t expected = make_u256(210);  // account 1: (1+1)*100+10 = 210
        ASSERT(uint256_is_equal(&got, &expected), "persisted storage mismatch");
    }
    printf("  finalize + persistence:    OK\n");

    evm_state_destroy(es);
    sdb_destroy(sdb);
}

// ============================================================================
// Phase 3: Snapshot & Revert
// ============================================================================

static void test_snapshot_revert(void) {
    printf("\n--- Phase 3: Snapshot & Revert ---\n");

    cleanup_dir(TEST_DIR);
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL, "sdb_create failed");

    evm_state_t *es = evm_state_create(sdb);
    ASSERT(es != NULL, "evm_state_create failed");

    address_t addr_a = make_addr(0xAA);

    // Set initial state
    evm_state_set_nonce(es, &addr_a, 1);
    uint256_t bal100 = make_u256(100);
    evm_state_set_balance(es, &addr_a, &bal100);

    // Take snapshot
    uint32_t snap1 = evm_state_snapshot(es);

    // Modify state
    evm_state_set_nonce(es, &addr_a, 5);
    uint256_t bal999 = make_u256(999);
    evm_state_set_balance(es, &addr_a, &bal999);
    uint256_t slot_key = make_u256(42);
    uint256_t slot_val = make_u256(12345);
    evm_state_set_storage(es, &addr_a, &slot_key, &slot_val);

    // Verify modified state
    ASSERT(evm_state_get_nonce(es, &addr_a) == 5, "nonce should be 5");
    {
        uint256_t bal = evm_state_get_balance(es, &addr_a);
        ASSERT(uint256_is_equal(&bal, &bal999), "balance should be 999");
    }
    {
        uint256_t sv = evm_state_get_storage(es, &addr_a, &slot_key);
        ASSERT(uint256_is_equal(&sv, &slot_val), "storage should be 12345");
    }
    printf("  modified state verified:   OK\n");

    // Revert to snapshot
    evm_state_revert(es, snap1);

    // Verify reverted state
    ASSERT(evm_state_get_nonce(es, &addr_a) == 1, "nonce should revert to 1");
    {
        uint256_t bal = evm_state_get_balance(es, &addr_a);
        ASSERT(uint256_is_equal(&bal, &bal100), "balance should revert to 100");
    }
    {
        uint256_t sv = evm_state_get_storage(es, &addr_a, &slot_key);
        ASSERT(uint256_is_zero(&sv), "storage should revert to 0");
    }
    printf("  revert to snap1:           OK\n");

    // Nested snapshots
    evm_state_set_nonce(es, &addr_a, 10);
    uint32_t snap2 = evm_state_snapshot(es);

    evm_state_set_nonce(es, &addr_a, 20);
    uint32_t snap3 = evm_state_snapshot(es);

    evm_state_set_nonce(es, &addr_a, 30);

    ASSERT(evm_state_get_nonce(es, &addr_a) == 30, "nonce should be 30");

    // Revert to snap3
    evm_state_revert(es, snap3);
    ASSERT(evm_state_get_nonce(es, &addr_a) == 20, "nonce should revert to 20");

    // Revert to snap2
    evm_state_revert(es, snap2);
    ASSERT(evm_state_get_nonce(es, &addr_a) == 10, "nonce should revert to 10");

    printf("  nested snapshots:          OK\n");

    evm_state_destroy(es);
    sdb_destroy(sdb);
}

// ============================================================================
// Phase 4: Code Operations
// ============================================================================

static void test_code_ops(void) {
    printf("\n--- Phase 4: Code Operations ---\n");

    cleanup_dir(TEST_DIR);
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL, "sdb_create failed");

    evm_state_t *es = evm_state_create(sdb);
    ASSERT(es != NULL, "evm_state_create failed");

    address_t addr = make_addr(0xCC);

    // Initially no code
    hash_t ch = evm_state_get_code_hash(es, &addr);
    ASSERT(hash_equal(&ch, &HASH_EMPTY_CODE), "empty account should have empty code hash");
    ASSERT(evm_state_get_code_size(es, &addr) == 0, "empty account should have 0 code size");
    printf("  empty account code:        OK\n");

    // Set code
    uint8_t bytecode[] = { 0x60, 0x80, 0x60, 0x40, 0x52, 0x34, 0x80, 0x15 };
    evm_state_set_code(es, &addr, bytecode, sizeof(bytecode));

    // Verify code hash
    hash_t expected_hash = hash_keccak256(bytecode, sizeof(bytecode));
    ch = evm_state_get_code_hash(es, &addr);
    ASSERT(hash_equal(&ch, &expected_hash), "code hash mismatch");
    printf("  set code + hash:           OK\n");

    // Verify code size
    uint32_t code_size = evm_state_get_code_size(es, &addr);
    ASSERT(code_size == sizeof(bytecode), "code size = %u, expected %zu",
           code_size, sizeof(bytecode));
    printf("  code size:                 OK\n");

    // Verify code content
    uint8_t code_out[64];
    uint32_t code_len = sizeof(code_out);
    ASSERT(evm_state_get_code(es, &addr, code_out, &code_len), "get_code failed");
    ASSERT(code_len == sizeof(bytecode), "get_code len mismatch");
    ASSERT(memcmp(code_out, bytecode, sizeof(bytecode)) == 0, "code content mismatch");
    printf("  get code content:          OK\n");

    // Snapshot, change code, revert
    uint32_t snap = evm_state_snapshot(es);

    uint8_t new_code[] = { 0xFF, 0xFE, 0xFD };
    evm_state_set_code(es, &addr, new_code, sizeof(new_code));

    hash_t new_hash = evm_state_get_code_hash(es, &addr);
    hash_t expected_new = hash_keccak256(new_code, sizeof(new_code));
    ASSERT(hash_equal(&new_hash, &expected_new), "new code hash mismatch");

    evm_state_revert(es, snap);

    // After revert, code hash should be restored
    ch = evm_state_get_code_hash(es, &addr);
    ASSERT(hash_equal(&ch, &expected_hash), "code hash should revert");
    printf("  code snapshot/revert:      OK\n");

    // Finalize and verify persistence
    evm_state_set_nonce(es, &addr, 1);  // ensure account is non-empty
    ASSERT(evm_state_finalize(es), "finalize failed");
    sdb_merge(sdb);

    // Verify code persisted via state_db
    {
        uint8_t addr_hash[32];
        hash_t ah = hash_keccak256(addr.bytes, ADDRESS_SIZE);
        memcpy(addr_hash, ah.bytes, 32);

        uint8_t persisted_code[64];
        uint32_t persisted_len = 0;
        ASSERT(sdb_get_code(sdb, addr_hash, persisted_code, &persisted_len),
               "sdb_get_code failed");
        ASSERT(persisted_len == sizeof(bytecode), "persisted code len mismatch");
        ASSERT(memcmp(persisted_code, bytecode, sizeof(bytecode)) == 0,
               "persisted code content mismatch");
    }
    printf("  finalize + code persist:   OK\n");

    evm_state_destroy(es);
    sdb_destroy(sdb);
}

// ============================================================================
// Phase 5: Self-Destruct
// ============================================================================

static void test_self_destruct(void) {
    printf("\n--- Phase 5: Self-Destruct ---\n");

    cleanup_dir(TEST_DIR);
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL, "sdb_create failed");

    evm_state_t *es = evm_state_create(sdb);
    ASSERT(es != NULL, "evm_state_create failed");

    address_t addr = make_addr(0xDD);

    // Create account with balance
    evm_state_create_account(es, &addr);
    evm_state_set_nonce(es, &addr, 1);
    uint256_t bal = make_u256(5000);
    evm_state_set_balance(es, &addr, &bal);

    // Set some storage
    uint256_t key = make_u256(1);
    uint256_t val = make_u256(42);
    evm_state_set_storage(es, &addr, &key, &val);

    ASSERT(evm_state_exists(es, &addr), "account should exist");
    ASSERT(!evm_state_is_self_destructed(es, &addr), "should not be destructed");
    printf("  created account:           OK\n");

    // Self-destruct (account still exists until finalization)
    evm_state_self_destruct(es, &addr);
    ASSERT(evm_state_is_self_destructed(es, &addr), "should be destructed");
    ASSERT(evm_state_exists(es, &addr), "destructed account still exists until finalize");
    printf("  self-destruct:             OK\n");

    // Snapshot, self-destruct another account, revert
    address_t addr2 = make_addr(0xEE);
    evm_state_set_nonce(es, &addr2, 1);

    uint32_t snap = evm_state_snapshot(es);
    evm_state_self_destruct(es, &addr2);
    ASSERT(evm_state_is_self_destructed(es, &addr2), "should be destructed");

    evm_state_revert(es, snap);
    ASSERT(!evm_state_is_self_destructed(es, &addr2), "should revert destruct");
    printf("  destruct snapshot/revert:  OK\n");

    // Finalize — self-destructed account should be deleted from state_db
    ASSERT(evm_state_finalize(es), "finalize failed");
    sdb_merge(sdb);

    {
        uint8_t addr_hash[32];
        hash_t ah = hash_keccak256(addr.bytes, ADDRESS_SIZE);
        memcpy(addr_hash, ah.bytes, 32);

        uint8_t buf[ACCOUNT_MAX_ENCODED];
        uint16_t buf_len = 0;
        bool found = sdb_get(sdb, addr_hash, buf, &buf_len);
        ASSERT(!found, "destructed account should not be in state_db");
    }
    printf("  finalize deletes account:  OK\n");

    evm_state_destroy(es);
    sdb_destroy(sdb);
}

// ============================================================================
// Phase 6: Access Lists (EIP-2929)
// ============================================================================

static void test_access_lists(void) {
    printf("\n--- Phase 6: Access Lists (EIP-2929) ---\n");

    cleanup_dir(TEST_DIR);
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL, "sdb_create failed");

    evm_state_t *es = evm_state_create(sdb);
    ASSERT(es != NULL, "evm_state_create failed");

    address_t addr = make_addr(0xAA);

    // Initially cold
    ASSERT(!evm_state_is_address_warm(es, &addr), "should be cold");

    // Warm address — returns false (was cold)
    bool was_warm = evm_state_warm_address(es, &addr);
    ASSERT(!was_warm, "warm_address should return false (was cold)");

    // Now warm
    ASSERT(evm_state_is_address_warm(es, &addr), "should be warm");

    // Second warm — returns true (already warm)
    was_warm = evm_state_warm_address(es, &addr);
    ASSERT(was_warm, "warm_address should return true (already warm)");
    printf("  warm address:              OK\n");

    // Warm slot
    uint256_t slot = make_u256(42);
    ASSERT(!evm_state_is_slot_warm(es, &addr, &slot), "slot should be cold");

    bool slot_was_warm = evm_state_warm_slot(es, &addr, &slot);
    ASSERT(!slot_was_warm, "warm_slot should return false (was cold)");
    ASSERT(evm_state_is_slot_warm(es, &addr, &slot), "slot should be warm");

    slot_was_warm = evm_state_warm_slot(es, &addr, &slot);
    ASSERT(slot_was_warm, "warm_slot should return true (already warm)");
    printf("  warm slot:                 OK\n");

    // Snapshot, warm new address, revert
    uint32_t snap = evm_state_snapshot(es);

    address_t addr2 = make_addr(0xBB);
    evm_state_warm_address(es, &addr2);
    ASSERT(evm_state_is_address_warm(es, &addr2), "addr2 should be warm");

    uint256_t slot2 = make_u256(99);
    evm_state_warm_slot(es, &addr2, &slot2);
    ASSERT(evm_state_is_slot_warm(es, &addr2, &slot2), "slot2 should be warm");

    // Revert
    evm_state_revert(es, snap);

    ASSERT(!evm_state_is_address_warm(es, &addr2), "addr2 should revert to cold");
    ASSERT(!evm_state_is_slot_warm(es, &addr2, &slot2), "slot2 should revert to cold");

    // Original warmth preserved
    ASSERT(evm_state_is_address_warm(es, &addr), "addr should still be warm");
    ASSERT(evm_state_is_slot_warm(es, &addr, &slot), "slot should still be warm");
    printf("  access list snapshot/revert: OK\n");

    evm_state_destroy(es);
    sdb_destroy(sdb);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("============================================\n");
    printf("EVM State Test Suite\n");
    printf("============================================\n");

    test_account_ops();
    test_storage_ops();
    test_snapshot_revert();
    test_code_ops();
    test_self_destruct();
    test_access_lists();

    cleanup_dir(TEST_DIR);

    printf("\n============================================\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);
    printf("============================================\n");

    return tests_failed > 0 ? 1 : 0;
}
