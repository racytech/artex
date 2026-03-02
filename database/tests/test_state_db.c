#include "state_db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>

// ============================================================================
// Test infrastructure
// ============================================================================

static int assertions = 0;
#define ASSERT(cond) do { \
    assertions++; \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        abort(); \
    } \
} while(0)

#define TEST_DIR "/tmp/test_state_db"

static void cleanup(void) {
    system("rm -rf " TEST_DIR);
}

// ============================================================================
// Key/value helpers
// ============================================================================

static void make_addr(uint8_t out[32], uint32_t id) {
    memset(out, 0, 32);
    out[0] = (uint8_t)(id >> 24);
    out[1] = (uint8_t)(id >> 16);
    out[2] = (uint8_t)(id >> 8);
    out[3] = (uint8_t)(id);
}

static void make_slot(uint8_t out[32], uint32_t id) {
    memset(out, 0, 32);
    out[28] = (uint8_t)(id >> 24);
    out[29] = (uint8_t)(id >> 16);
    out[30] = (uint8_t)(id >> 8);
    out[31] = (uint8_t)(id);
}

static void make_account_key(uint8_t out[32], uint32_t id) {
    memset(out, 0, 32);
    out[0] = 0xAC;  // "account" prefix byte for test keys
    out[1] = (uint8_t)(id >> 24);
    out[2] = (uint8_t)(id >> 16);
    out[3] = (uint8_t)(id >> 8);
    out[4] = (uint8_t)(id);
}

static void make_code_key(uint8_t out[32], uint32_t id) {
    memset(out, 0, 32);
    out[0] = 0xCC;  // "code" prefix byte for test keys
    out[1] = (uint8_t)(id >> 24);
    out[2] = (uint8_t)(id >> 16);
    out[3] = (uint8_t)(id >> 8);
    out[4] = (uint8_t)(id);
}

// Value = 4-byte big-endian encoding of id
static void make_value(uint8_t out[4], uint32_t id) {
    out[0] = (uint8_t)(id >> 24);
    out[1] = (uint8_t)(id >> 16);
    out[2] = (uint8_t)(id >> 8);
    out[3] = (uint8_t)(id);
}

static uint32_t read_value(const uint8_t buf[4]) {
    return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] << 8)  | (uint32_t)buf[3];
}

// Storage value = 32-byte uint256 with id in last 4 bytes
static void make_storage_value(uint8_t out[32], uint32_t id) {
    memset(out, 0, 32);
    out[28] = (uint8_t)(id >> 24);
    out[29] = (uint8_t)(id >> 16);
    out[30] = (uint8_t)(id >> 8);
    out[31] = (uint8_t)(id);
}

// ============================================================================
// Phase 1: Basic CRUD
// ============================================================================

static void test_basic_crud(void) {
    int start = assertions;
    printf("Phase 1: Basic account + storage CRUD\n");

    cleanup();
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL);

    // Insert 100 accounts
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t key[32], val[4];
        make_account_key(key, i);
        make_value(val, i);
        ASSERT(sdb_put(sdb, key, val, 4));
    }

    // Insert 100 storage slots for account 0
    uint8_t addr0[32];
    make_addr(addr0, 0);
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t slot[32], val[32];
        make_slot(slot, i);
        make_storage_value(val, i + 1000);
        ASSERT(sdb_put_storage(sdb, addr0, slot, val, 32));
    }

    // Merge (no-op — writes go directly to hash_store)
    sdb_merge(sdb);

    // Verify all accounts
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t key[32], buf[4];
        uint16_t len = 0;
        make_account_key(key, i);
        ASSERT(sdb_get(sdb, key, buf, &len));
        ASSERT(len == 4);
        ASSERT(read_value(buf) == i);
    }

    // Verify all storage
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t slot[32], buf[32];
        uint16_t len = 0;
        make_slot(slot, i);
        ASSERT(sdb_get_storage(sdb, addr0, slot, buf, &len));
        ASSERT(len == 32);

        uint8_t expected[32];
        make_storage_value(expected, i + 1000);
        ASSERT(memcmp(buf, expected, 32) == 0);
    }

    // Delete 10 storage slots
    for (uint32_t i = 0; i < 10; i++) {
        uint8_t slot[32];
        make_slot(slot, i);
        ASSERT(sdb_delete_storage(sdb, addr0, slot));
    }
    sdb_merge(sdb);

    // Deleted slots should be gone
    for (uint32_t i = 0; i < 10; i++) {
        uint8_t slot[32], buf[32];
        uint16_t len = 0;
        make_slot(slot, i);
        ASSERT(!sdb_get_storage(sdb, addr0, slot, buf, &len));
    }

    // Remaining 90 still readable
    for (uint32_t i = 10; i < 100; i++) {
        uint8_t slot[32], buf[32];
        uint16_t len = 0;
        make_slot(slot, i);
        ASSERT(sdb_get_storage(sdb, addr0, slot, buf, &len));
        ASSERT(len == 32);
    }

    // Check stats
    sdb_stats_t stats = sdb_stats(sdb);
    ASSERT(stats.account_keys == 100);
    ASSERT(stats.storage_keys == 90);
    ASSERT(stats.account_buffer == 0);
    ASSERT(stats.storage_buffer == 0);

    sdb_destroy(sdb);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 2: Multi-Account Storage
// ============================================================================

static void test_multi_account_storage(void) {
    int start = assertions;
    printf("Phase 2: Multi-account storage (10 x 100 slots)\n");

    cleanup();
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL);

    // Insert 10 accounts
    for (uint32_t a = 0; a < 10; a++) {
        uint8_t key[32], val[4];
        make_account_key(key, a);
        make_value(val, a);
        ASSERT(sdb_put(sdb, key, val, 4));
    }

    // Insert 100 storage slots per account
    for (uint32_t a = 0; a < 10; a++) {
        uint8_t addr[32];
        make_addr(addr, a);
        for (uint32_t s = 0; s < 100; s++) {
            uint8_t slot[32], val[32];
            make_slot(slot, s);
            make_storage_value(val, a * 1000 + s);
            ASSERT(sdb_put_storage(sdb, addr, slot, val, 32));
        }
    }

    sdb_merge(sdb);

    // Verify all 1000 storage entries
    for (uint32_t a = 0; a < 10; a++) {
        uint8_t addr[32];
        make_addr(addr, a);
        for (uint32_t s = 0; s < 100; s++) {
            uint8_t slot[32], buf[32];
            uint16_t len = 0;
            make_slot(slot, s);
            ASSERT(sdb_get_storage(sdb, addr, slot, buf, &len));
            ASSERT(len == 32);

            uint8_t expected[32];
            make_storage_value(expected, a * 1000 + s);
            ASSERT(memcmp(buf, expected, 32) == 0);
        }
    }

    // Cross-account isolation: slot 0 of addr 0 should not appear under addr 1
    {
        uint8_t addr1[32], slot0[32], buf[32];
        uint16_t len = 0;
        make_addr(addr1, 1);
        // Use addr_0's slot encoding pattern but look under addr_1
        // Slot 0 exists for addr_1 with value 1*1000+0=1000
        // Let's verify a non-existent slot instead
        uint8_t fake_slot[32];
        make_slot(fake_slot, 999);  // slot 999 doesn't exist
        ASSERT(!sdb_get_storage(sdb, addr1, fake_slot, buf, &len));

        // Also verify slots are account-specific: addr_0 slot_50 vs addr_1 slot_50
        uint8_t addr0[32];
        make_addr(addr0, 0);
        make_slot(slot0, 50);

        ASSERT(sdb_get_storage(sdb, addr0, slot0, buf, &len));
        uint32_t val0 = (uint32_t)buf[28] << 24 | (uint32_t)buf[29] << 16 |
                         (uint32_t)buf[30] << 8 | buf[31];
        ASSERT(val0 == 0 * 1000 + 50);  // addr 0, slot 50

        ASSERT(sdb_get_storage(sdb, addr1, slot0, buf, &len));
        uint32_t val1 = (uint32_t)buf[28] << 24 | (uint32_t)buf[29] << 16 |
                         (uint32_t)buf[30] << 8 | buf[31];
        ASSERT(val1 == 1 * 1000 + 50);  // addr 1, slot 50

        ASSERT(val0 != val1);
    }

    sdb_stats_t stats = sdb_stats(sdb);
    ASSERT(stats.account_keys == 10);
    ASSERT(stats.storage_keys == 1000);

    sdb_destroy(sdb);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 3: Checkpoint & Recovery
// ============================================================================

static void test_checkpoint_recovery(void) {
    int start = assertions;
    printf("Phase 3: Checkpoint + crash recovery\n");

    cleanup();
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL);

    // Insert 1000 accounts
    for (uint32_t i = 0; i < 1000; i++) {
        uint8_t key[32], val[4];
        make_account_key(key, i);
        make_value(val, i);
        ASSERT(sdb_put(sdb, key, val, 4));
    }

    // Insert 5 storage slots per account (first 1000 accounts)
    for (uint32_t a = 0; a < 1000; a++) {
        uint8_t addr[32];
        make_addr(addr, a);
        for (uint32_t s = 0; s < 5; s++) {
            uint8_t slot[32], val[32];
            make_slot(slot, s);
            make_storage_value(val, a * 100 + s);
            ASSERT(sdb_put_storage(sdb, addr, slot, val, 32));
        }
    }

    // Deploy 10 code entries
    for (uint32_t i = 0; i < 10; i++) {
        uint8_t key[32];
        make_code_key(key, i);
        uint8_t code[64];
        memset(code, (uint8_t)i, 64);
        ASSERT(sdb_put_code(sdb, key, code, 64));
    }

    sdb_merge(sdb);

    // Checkpoint at block 42
    ASSERT(sdb_checkpoint(sdb));

    sdb_stats_t pre_stats = sdb_stats(sdb);
    ASSERT(pre_stats.account_keys == 1010);  // 1000 accounts + 10 code
    ASSERT(pre_stats.storage_keys == 5000);
    ASSERT(pre_stats.code_count == 10);

    // Destroy (simulate crash)
    sdb_destroy(sdb);

    // Reopen
    sdb = sdb_open(TEST_DIR);
    ASSERT(sdb != NULL);

    // Verify stats match
    sdb_stats_t post_stats = sdb_stats(sdb);
    ASSERT(post_stats.account_keys == 1010);  // 1000 accounts + 10 code
    ASSERT(post_stats.storage_keys == 5000);
    ASSERT(post_stats.code_count == 10);

    // Verify sample accounts
    for (uint32_t i = 0; i < 1000; i += 100) {
        uint8_t key[32], buf[4];
        uint16_t len = 0;
        make_account_key(key, i);
        ASSERT(sdb_get(sdb, key, buf, &len));
        ASSERT(len == 4);
        ASSERT(read_value(buf) == i);
    }

    // Verify sample storage
    for (uint32_t a = 0; a < 1000; a += 100) {
        uint8_t addr[32];
        make_addr(addr, a);
        for (uint32_t s = 0; s < 5; s++) {
            uint8_t slot[32], buf[32];
            uint16_t len = 0;
            make_slot(slot, s);
            ASSERT(sdb_get_storage(sdb, addr, slot, buf, &len));
            ASSERT(len == 32);

            uint8_t expected[32];
            make_storage_value(expected, a * 100 + s);
            ASSERT(memcmp(buf, expected, 32) == 0);
        }
    }

    // Verify code
    for (uint32_t i = 0; i < 10; i++) {
        uint8_t key[32];
        make_code_key(key, i);
        uint8_t code[64];
        uint32_t code_len = 0;
        ASSERT(sdb_get_code(sdb, key, code, &code_len));
        ASSERT(code_len == 64);
        uint8_t expected_byte = (uint8_t)i;
        ASSERT(code[0] == expected_byte);
        ASSERT(code[63] == expected_byte);
    }

    // Insert more post-recovery
    for (uint32_t i = 1000; i < 1100; i++) {
        uint8_t key[32], val[4];
        make_account_key(key, i);
        make_value(val, i);
        ASSERT(sdb_put(sdb, key, val, 4));
    }
    for (uint32_t s = 5; s < 10; s++) {
        uint8_t addr[32], slot[32], val[32];
        make_addr(addr, 0);
        make_slot(slot, s);
        make_storage_value(val, s + 9000);
        ASSERT(sdb_put_storage(sdb, addr, slot, val, 32));
    }
    sdb_merge(sdb);

    // Verify new entries alongside old
    {
        uint8_t key[32], buf[4];
        uint16_t len = 0;
        make_account_key(key, 1050);
        ASSERT(sdb_get(sdb, key, buf, &len));
        ASSERT(read_value(buf) == 1050);
    }
    {
        uint8_t addr[32], slot[32], buf[32];
        uint16_t len = 0;
        make_addr(addr, 0);
        make_slot(slot, 7);
        ASSERT(sdb_get_storage(sdb, addr, slot, buf, &len));
        ASSERT(len == 32);
    }

    sdb_stats_t final_stats = sdb_stats(sdb);
    ASSERT(final_stats.account_keys == 1110);  // 1100 accounts + 10 code
    ASSERT(final_stats.storage_keys == 5005);

    sdb_destroy(sdb);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 4: Mixed Block Operations
// ============================================================================

static void test_mixed_blocks(void) {
    int start = assertions;
    printf("Phase 4: Mixed block operations (10 blocks)\n");

    cleanup();
    state_db_t *sdb = sdb_create(TEST_DIR);
    ASSERT(sdb != NULL);

    uint32_t total_accounts = 0;
    uint32_t total_storage = 0;
    uint32_t total_code = 0;

    for (uint32_t block = 0; block < 10; block++) {
        // 50 accounts per block
        for (uint32_t i = 0; i < 50; i++) {
            uint32_t id = block * 50 + i;
            uint8_t key[32], val[4];
            make_account_key(key, id);
            make_value(val, id);
            ASSERT(sdb_put(sdb, key, val, 4));
        }
        total_accounts += 50;

        // 200 storage slots per block (spread across 10 accounts)
        for (uint32_t i = 0; i < 200; i++) {
            uint32_t acct_id = block * 50 + (i % 10);
            uint32_t slot_id = block * 200 + i;
            uint8_t addr[32], slot[32], val[32];
            make_addr(addr, acct_id);
            make_slot(slot, slot_id);
            make_storage_value(val, slot_id);
            ASSERT(sdb_put_storage(sdb, addr, slot, val, 32));
        }
        total_storage += 200;

        // 2 code deploys per block
        for (uint32_t i = 0; i < 2; i++) {
            uint32_t id = block * 2 + i;
            uint8_t key[32];
            make_code_key(key, id);
            uint8_t code[128];
            memset(code, (uint8_t)(id & 0xFF), 128);
            ASSERT(sdb_put_code(sdb, key, code, 128));
        }
        total_code += 2;

        sdb_merge(sdb);

        // Checkpoint every 5 blocks
        if ((block + 1) % 5 == 0) {
            ASSERT(sdb_checkpoint(sdb));
        }
    }

    // Verify stats
    sdb_stats_t stats = sdb_stats(sdb);
    ASSERT(stats.account_keys == total_accounts + total_code);  // code shares index
    ASSERT(stats.storage_keys == total_storage);
    ASSERT(stats.code_count == total_code);
    printf("  accounts: %u, storage: %u, code: %u (index: %" PRIu64 ")\n",
           total_accounts, total_storage, total_code, stats.account_keys);

    // Verify sample accounts
    for (uint32_t i = 0; i < total_accounts; i += 50) {
        uint8_t key[32], buf[4];
        uint16_t len = 0;
        make_account_key(key, i);
        ASSERT(sdb_get(sdb, key, buf, &len));
        ASSERT(read_value(buf) == i);
    }

    // Verify sample storage
    for (uint32_t s = 0; s < total_storage; s += 100) {
        // Find which account owns this slot
        uint32_t block = s / 200;
        uint32_t i_in_block = s % 200;
        uint32_t acct_id = block * 50 + (i_in_block % 10);

        uint8_t addr[32], slot[32], buf[32];
        uint16_t len = 0;
        make_addr(addr, acct_id);
        make_slot(slot, s);
        ASSERT(sdb_get_storage(sdb, addr, slot, buf, &len));
        ASSERT(len == 32);
    }

    // Verify all code
    for (uint32_t i = 0; i < total_code; i++) {
        uint8_t key[32], code[128];
        uint32_t code_len = 0;
        make_code_key(key, i);
        ASSERT(sdb_get_code(sdb, key, code, &code_len));
        ASSERT(code_len == 128);
    }

    // Destroy, reopen from last checkpoint
    sdb_destroy(sdb);

    sdb = sdb_open(TEST_DIR);
    ASSERT(sdb != NULL);

    // Verify after recovery
    stats = sdb_stats(sdb);
    ASSERT(stats.account_keys == total_accounts + total_code);
    ASSERT(stats.storage_keys == total_storage);
    ASSERT(stats.code_count == total_code);

    sdb_destroy(sdb);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== State DB Tests ===\n\n");

    test_basic_crud();
    test_multi_account_storage();
    test_checkpoint_recovery();
    test_mixed_blocks();

    cleanup();

    printf("=== ALL PHASES PASSED (%d total assertions) ===\n", assertions);
    return 0;
}
