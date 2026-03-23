/**
 * Test Verkle Reconstruct — exercises the diff-to-verkle pipeline with
 * manually constructed diffs, including SELFDESTRUCT clearing.
 *
 * Tests:
 *   1. Create account via diff (nonce, balance, storage)
 *   2. Update account via diff (balance change, new storage slot)
 *   3. SELFDESTRUCT clears all state
 *   4. Re-create after destruction
 *   5. Root hash determinism (same diffs → same root)
 */

#include "verkle_state.h"
#include "verkle_key.h"
#include "mem_art.h"
#include "state_history.h"
#include "uint256.h"
#include "address.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* =========================================================================
 * Slot Tracker (copied from verkle_reconstruct.c — self-contained test)
 * ========================================================================= */

typedef struct {
    uint8_t (*keys)[32];
    uint32_t count;
    uint32_t cap;
} slot_set_t;

typedef struct {
    mem_art_t tree;
} slot_tracker_t;

static bool slot_tracker_init(slot_tracker_t *st) {
    return mem_art_init(&st->tree);
}

static bool slot_tracker_free_cb(const uint8_t *key, size_t key_len,
                                  const void *value, size_t value_len,
                                  void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    const slot_set_t *ss = (const slot_set_t *)value;
    free(ss->keys);
    return true;
}

static void slot_tracker_destroy(slot_tracker_t *st) {
    mem_art_foreach(&st->tree, slot_tracker_free_cb, NULL);
    mem_art_destroy(&st->tree);
}

static void slot_tracker_add(slot_tracker_t *st,
                              const uint8_t addr[20],
                              const uint8_t slot[32]) {
    size_t val_len = 0;
    slot_set_t *ss = (slot_set_t *)mem_art_get_mut(
        &st->tree, addr, 20, &val_len);

    if (!ss) {
        slot_set_t new_ss = {0};
        new_ss.cap = 8;
        new_ss.keys = malloc(new_ss.cap * 32);
        if (!new_ss.keys) return;
        memcpy(new_ss.keys[0], slot, 32);
        new_ss.count = 1;
        mem_art_insert(&st->tree, addr, 20, &new_ss, sizeof(slot_set_t));
        return;
    }

    for (uint32_t i = 0; i < ss->count; i++) {
        if (memcmp(ss->keys[i], slot, 32) == 0)
            return;
    }

    if (ss->count >= ss->cap) {
        uint32_t new_cap = ss->cap * 2;
        uint8_t (*new_keys)[32] = realloc(ss->keys, new_cap * 32);
        if (!new_keys) return;
        ss->keys = new_keys;
        ss->cap = new_cap;
    }

    memcpy(ss->keys[ss->count], slot, 32);
    ss->count++;
}

static const uint8_t *slot_tracker_get(const slot_tracker_t *st,
                                        const uint8_t addr[20],
                                        uint32_t *out_count) {
    size_t val_len = 0;
    const slot_set_t *ss = (const slot_set_t *)mem_art_get(
        &st->tree, addr, 20, &val_len);
    if (!ss || ss->count == 0) {
        *out_count = 0;
        return NULL;
    }
    *out_count = ss->count;
    return (const uint8_t *)ss->keys;
}

static void slot_tracker_clear(slot_tracker_t *st,
                                const uint8_t addr[20]) {
    size_t val_len = 0;
    slot_set_t *ss = (slot_set_t *)mem_art_get_mut(
        &st->tree, addr, 20, &val_len);
    if (ss) {
        free(ss->keys);
        ss->keys = NULL;
        ss->count = 0;
        ss->cap = 0;
    }
}

/* =========================================================================
 * Apply diff (same logic as verkle_reconstruct.c, without code_store)
 * ========================================================================= */

static bool apply_diff(verkle_state_t *vs, slot_tracker_t *tracker,
                        const block_diff_t *diff) {
    for (uint16_t g = 0; g < diff->group_count; g++) {
        const addr_diff_t *grp = &diff->groups[g];
        const uint8_t *addr = grp->addr.bytes;

        if (grp->flags & ACCT_DIFF_DESTRUCTED) {
            uint32_t slot_count = 0;
            const uint8_t *slots = slot_tracker_get(tracker, addr,
                                                     &slot_count);
            verkle_state_clear_account(vs, addr, slots, slot_count);
            slot_tracker_clear(tracker, addr);
        }

        if (grp->flags & ACCT_DIFF_CREATED) {
            verkle_state_set_version(vs, addr, 0);
        }

        if (grp->field_mask & FIELD_NONCE) {
            verkle_state_set_nonce(vs, addr, grp->nonce);
        }

        if (grp->field_mask & FIELD_BALANCE) {
            uint8_t bal_bytes[32];
            uint256_to_bytes_le(&grp->balance, bal_bytes);
            verkle_state_set_balance(vs, addr, bal_bytes);
        }

        for (uint16_t s = 0; s < grp->slot_count; s++) {
            const slot_diff_t *sd = &grp->slots[s];
            uint8_t slot_bytes[32], val_bytes[32];
            uint256_to_bytes_le(&sd->slot, slot_bytes);
            uint256_to_bytes_le(&sd->value, val_bytes);
            verkle_state_set_storage(vs, addr, slot_bytes, val_bytes);
            slot_tracker_add(tracker, addr, slot_bytes);
        }
    }
    return true;
}

/* =========================================================================
 * Test Helpers
 * ========================================================================= */

#define VAL_DIR  "/tmp/test_vreconstruct_val"
#define COMM_DIR "/tmp/test_vreconstruct_comm"

static int tests_run = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do {                                    \
    tests_run++;                                                 \
    if (!(cond)) {                                               \
        fprintf(stderr, "  FAIL: %s (line %d)\n", msg, __LINE__); \
    } else {                                                     \
        tests_passed++;                                          \
    }                                                            \
} while(0)

static void cleanup(void) {
    system("rm -rf " VAL_DIR " " COMM_DIR);
}

static void make_addr(uint8_t addr[20], uint8_t seed) {
    memset(addr, seed, 20);
}

static uint256_t u256_from_u64(uint64_t v) {
    uint256_t r = UINT256_ZERO_INIT;
    r.low = (uint128_t)v;
    return r;
}

static void print_hash(const uint8_t *h) {
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
}

/* =========================================================================
 * Tests
 * ========================================================================= */

static void test_create_and_read(void) {
    printf("--- test_create_and_read ---\n");
    cleanup();
    mkdir(VAL_DIR, 0755);
    mkdir(COMM_DIR, 0755);

    verkle_state_t *vs = verkle_state_create_flat(VAL_DIR, COMM_DIR);
    CHECK(vs != NULL, "create verkle state");

    slot_tracker_t tracker;
    slot_tracker_init(&tracker);

    /* Block 1: create account 0x01..01 with nonce=5, balance=1000,
     * and two storage slots */
    uint8_t addr[20];
    make_addr(addr, 0x01);

    slot_diff_t slots[2];
    slots[0].slot = u256_from_u64(7);
    slots[0].value = u256_from_u64(0xDEAD);
    slots[1].slot = u256_from_u64(42);
    slots[1].value = u256_from_u64(0xBEEF);

    addr_diff_t grp = {0};
    memcpy(grp.addr.bytes, addr, 20);
    grp.flags = ACCT_DIFF_CREATED;
    grp.field_mask = FIELD_NONCE | FIELD_BALANCE;
    grp.nonce = 5;
    grp.balance = u256_from_u64(1000);
    grp.slots = slots;
    grp.slot_count = 2;

    block_diff_t diff = {0};
    diff.block_number = 1;
    diff.groups = &grp;
    diff.group_count = 1;

    verkle_state_begin_block(vs, 1);
    apply_diff(vs, &tracker, &diff);
    verkle_state_commit_block(vs);

    /* Verify state */
    CHECK(verkle_state_get_nonce(vs, addr) == 5, "nonce == 5");

    uint8_t bal[32];
    verkle_state_get_balance(vs, addr, bal);
    uint64_t bal_lo = 0;
    memcpy(&bal_lo, bal, sizeof(bal_lo));
    CHECK(bal_lo == 1000, "balance == 1000");

    uint8_t slot_key[32], slot_val[32];
    uint256_t s7 = u256_from_u64(7);
    uint256_to_bytes_le(&s7, slot_key);
    verkle_state_get_storage(vs, addr, slot_key, slot_val);
    uint64_t sv = 0;
    memcpy(&sv, slot_val, sizeof(sv));
    CHECK(sv == 0xDEAD, "slot 7 == 0xDEAD");

    uint256_t s42 = u256_from_u64(42);
    uint256_to_bytes_le(&s42, slot_key);
    verkle_state_get_storage(vs, addr, slot_key, slot_val);
    sv = 0;
    memcpy(&sv, slot_val, sizeof(sv));
    CHECK(sv == 0xBEEF, "slot 42 == 0xBEEF");

    CHECK(verkle_state_exists(vs, addr), "account exists");

    slot_tracker_destroy(&tracker);
    verkle_state_destroy(vs);
    cleanup();
}

static void test_update(void) {
    printf("--- test_update ---\n");
    cleanup();
    mkdir(VAL_DIR, 0755);
    mkdir(COMM_DIR, 0755);

    verkle_state_t *vs = verkle_state_create_flat(VAL_DIR, COMM_DIR);
    slot_tracker_t tracker;
    slot_tracker_init(&tracker);

    uint8_t addr[20];
    make_addr(addr, 0x02);

    /* Block 1: create with nonce=1, balance=500, slot 10=0xAA */
    slot_diff_t slot1 = { .slot = u256_from_u64(10), .value = u256_from_u64(0xAA) };
    addr_diff_t grp1 = {0};
    memcpy(grp1.addr.bytes, addr, 20);
    grp1.flags = ACCT_DIFF_CREATED;
    grp1.field_mask = FIELD_NONCE | FIELD_BALANCE;
    grp1.nonce = 1;
    grp1.balance = u256_from_u64(500);
    grp1.slots = &slot1;
    grp1.slot_count = 1;

    block_diff_t diff1 = { .block_number = 1, .groups = &grp1, .group_count = 1 };
    verkle_state_begin_block(vs, 1);
    apply_diff(vs, &tracker, &diff1);
    verkle_state_commit_block(vs);

    /* Block 2: update balance to 999, add slot 20=0xBB */
    slot_diff_t slot2 = { .slot = u256_from_u64(20), .value = u256_from_u64(0xBB) };
    addr_diff_t grp2 = {0};
    memcpy(grp2.addr.bytes, addr, 20);
    grp2.flags = 0;
    grp2.field_mask = FIELD_BALANCE;
    grp2.balance = u256_from_u64(999);
    grp2.slots = &slot2;
    grp2.slot_count = 1;

    block_diff_t diff2 = { .block_number = 2, .groups = &grp2, .group_count = 1 };
    verkle_state_begin_block(vs, 2);
    apply_diff(vs, &tracker, &diff2);
    verkle_state_commit_block(vs);

    /* Verify: nonce unchanged, balance updated, both slots exist */
    CHECK(verkle_state_get_nonce(vs, addr) == 1, "nonce still 1");

    uint8_t bal[32];
    verkle_state_get_balance(vs, addr, bal);
    uint64_t bal_lo = 0;
    memcpy(&bal_lo, bal, sizeof(bal_lo));
    CHECK(bal_lo == 999, "balance updated to 999");

    uint8_t sk[32], sv_buf[32];
    uint256_t s10 = u256_from_u64(10);
    uint256_to_bytes_le(&s10, sk);
    verkle_state_get_storage(vs, addr, sk, sv_buf);
    uint64_t sv = 0;
    memcpy(&sv, sv_buf, sizeof(sv));
    CHECK(sv == 0xAA, "slot 10 still 0xAA");

    uint256_t s20 = u256_from_u64(20);
    uint256_to_bytes_le(&s20, sk);
    verkle_state_get_storage(vs, addr, sk, sv_buf);
    sv = 0;
    memcpy(&sv, sv_buf, sizeof(sv));
    CHECK(sv == 0xBB, "slot 20 == 0xBB");

    slot_tracker_destroy(&tracker);
    verkle_state_destroy(vs);
    cleanup();
}

static void test_selfdestruct(void) {
    printf("--- test_selfdestruct ---\n");
    cleanup();
    mkdir(VAL_DIR, 0755);
    mkdir(COMM_DIR, 0755);

    verkle_state_t *vs = verkle_state_create_flat(VAL_DIR, COMM_DIR);
    slot_tracker_t tracker;
    slot_tracker_init(&tracker);

    uint8_t addr[20];
    make_addr(addr, 0x03);

    /* Block 1: create with nonce=10, balance=5000, slots 1,2,3 */
    slot_diff_t slots[3];
    slots[0].slot = u256_from_u64(1);
    slots[0].value = u256_from_u64(0x111);
    slots[1].slot = u256_from_u64(2);
    slots[1].value = u256_from_u64(0x222);
    slots[2].slot = u256_from_u64(3);
    slots[2].value = u256_from_u64(0x333);

    addr_diff_t grp1 = {0};
    memcpy(grp1.addr.bytes, addr, 20);
    grp1.flags = ACCT_DIFF_CREATED;
    grp1.field_mask = FIELD_NONCE | FIELD_BALANCE;
    grp1.nonce = 10;
    grp1.balance = u256_from_u64(5000);
    grp1.slots = slots;
    grp1.slot_count = 3;

    block_diff_t diff1 = { .block_number = 1, .groups = &grp1, .group_count = 1 };
    verkle_state_begin_block(vs, 1);
    apply_diff(vs, &tracker, &diff1);
    verkle_state_commit_block(vs);

    /* Verify account exists with all state */
    CHECK(verkle_state_exists(vs, addr), "account exists before destruct");
    CHECK(verkle_state_get_nonce(vs, addr) == 10, "nonce == 10 before destruct");

    /* Block 2: SELFDESTRUCT */
    addr_diff_t grp2 = {0};
    memcpy(grp2.addr.bytes, addr, 20);
    grp2.flags = ACCT_DIFF_DESTRUCTED;
    grp2.field_mask = 0;
    grp2.slots = NULL;
    grp2.slot_count = 0;

    block_diff_t diff2 = { .block_number = 2, .groups = &grp2, .group_count = 1 };
    verkle_state_begin_block(vs, 2);
    apply_diff(vs, &tracker, &diff2);
    verkle_state_commit_block(vs);

    /* Verify everything is zeroed */
    CHECK(verkle_state_get_nonce(vs, addr) == 0, "nonce == 0 after destruct");

    uint8_t bal[32];
    verkle_state_get_balance(vs, addr, bal);
    uint64_t bal_lo = 0;
    memcpy(&bal_lo, bal, sizeof(bal_lo));
    CHECK(bal_lo == 0, "balance == 0 after destruct");

    /* All 3 storage slots should be zeroed */
    for (uint64_t i = 1; i <= 3; i++) {
        uint8_t sk[32], sv_buf[32];
        uint256_t si = u256_from_u64(i);
        uint256_to_bytes_le(&si, sk);
        verkle_state_get_storage(vs, addr, sk, sv_buf);
        uint64_t sv = 0;
        memcpy(&sv, sv_buf, sizeof(sv));
        char msg[64];
        snprintf(msg, sizeof(msg), "slot %lu == 0 after destruct", i);
        CHECK(sv == 0, msg);
    }

    slot_tracker_destroy(&tracker);
    verkle_state_destroy(vs);
    cleanup();
}

static void test_recreate_after_destruct(void) {
    printf("--- test_recreate_after_destruct ---\n");
    cleanup();
    mkdir(VAL_DIR, 0755);
    mkdir(COMM_DIR, 0755);

    verkle_state_t *vs = verkle_state_create_flat(VAL_DIR, COMM_DIR);
    slot_tracker_t tracker;
    slot_tracker_init(&tracker);

    uint8_t addr[20];
    make_addr(addr, 0x04);

    /* Block 1: create */
    slot_diff_t s1 = { .slot = u256_from_u64(99), .value = u256_from_u64(0xFFF) };
    addr_diff_t grp1 = {0};
    memcpy(grp1.addr.bytes, addr, 20);
    grp1.flags = ACCT_DIFF_CREATED;
    grp1.field_mask = FIELD_NONCE | FIELD_BALANCE;
    grp1.nonce = 1;
    grp1.balance = u256_from_u64(100);
    grp1.slots = &s1;
    grp1.slot_count = 1;

    block_diff_t diff1 = { .block_number = 1, .groups = &grp1, .group_count = 1 };
    verkle_state_begin_block(vs, 1);
    apply_diff(vs, &tracker, &diff1);
    verkle_state_commit_block(vs);

    /* Block 2: destruct */
    addr_diff_t grp2 = {0};
    memcpy(grp2.addr.bytes, addr, 20);
    grp2.flags = ACCT_DIFF_DESTRUCTED;
    block_diff_t diff2 = { .block_number = 2, .groups = &grp2, .group_count = 1 };
    verkle_state_begin_block(vs, 2);
    apply_diff(vs, &tracker, &diff2);
    verkle_state_commit_block(vs);

    CHECK(verkle_state_get_nonce(vs, addr) == 0, "nonce 0 after destruct");

    /* Block 3: re-create with different state */
    slot_diff_t s3 = { .slot = u256_from_u64(50), .value = u256_from_u64(0xCAFE) };
    addr_diff_t grp3 = {0};
    memcpy(grp3.addr.bytes, addr, 20);
    grp3.flags = ACCT_DIFF_CREATED;
    grp3.field_mask = FIELD_NONCE | FIELD_BALANCE;
    grp3.nonce = 1;
    grp3.balance = u256_from_u64(777);
    grp3.slots = &s3;
    grp3.slot_count = 1;

    block_diff_t diff3 = { .block_number = 3, .groups = &grp3, .group_count = 1 };
    verkle_state_begin_block(vs, 3);
    apply_diff(vs, &tracker, &diff3);
    verkle_state_commit_block(vs);

    /* Verify new state */
    CHECK(verkle_state_get_nonce(vs, addr) == 1, "nonce 1 after re-create");

    uint8_t bal[32];
    verkle_state_get_balance(vs, addr, bal);
    uint64_t bal_lo = 0;
    memcpy(&bal_lo, bal, sizeof(bal_lo));
    CHECK(bal_lo == 777, "balance 777 after re-create");

    /* Old slot 99 should still be zero */
    uint8_t sk[32], sv_buf[32];
    uint256_t s99 = u256_from_u64(99);
    uint256_to_bytes_le(&s99, sk);
    verkle_state_get_storage(vs, addr, sk, sv_buf);
    uint64_t sv = 0;
    memcpy(&sv, sv_buf, sizeof(sv));
    CHECK(sv == 0, "old slot 99 still 0");

    /* New slot 50 should have value */
    uint256_t s50 = u256_from_u64(50);
    uint256_to_bytes_le(&s50, sk);
    verkle_state_get_storage(vs, addr, sk, sv_buf);
    sv = 0;
    memcpy(&sv, sv_buf, sizeof(sv));
    CHECK(sv == 0xCAFE, "new slot 50 == 0xCAFE");

    slot_tracker_destroy(&tracker);
    verkle_state_destroy(vs);
    cleanup();
}

static void test_root_determinism(void) {
    printf("--- test_root_determinism ---\n");

    uint8_t root1[32], root2[32];

    /* Run the same sequence twice, verify same root */
    for (int run = 0; run < 2; run++) {
        cleanup();
        mkdir(VAL_DIR, 0755);
        mkdir(COMM_DIR, 0755);

        verkle_state_t *vs = verkle_state_create_flat(VAL_DIR, COMM_DIR);
        slot_tracker_t tracker;
        slot_tracker_init(&tracker);

        uint8_t addr_a[20], addr_b[20];
        make_addr(addr_a, 0xAA);
        make_addr(addr_b, 0xBB);

        /* Block 1: create two accounts */
        slot_diff_t sa = { .slot = u256_from_u64(1), .value = u256_from_u64(100) };
        addr_diff_t grps[2] = {0};
        memcpy(grps[0].addr.bytes, addr_a, 20);
        grps[0].flags = ACCT_DIFF_CREATED;
        grps[0].field_mask = FIELD_NONCE | FIELD_BALANCE;
        grps[0].nonce = 1;
        grps[0].balance = u256_from_u64(1000);
        grps[0].slots = &sa;
        grps[0].slot_count = 1;

        memcpy(grps[1].addr.bytes, addr_b, 20);
        grps[1].flags = ACCT_DIFF_CREATED;
        grps[1].field_mask = FIELD_NONCE;
        grps[1].nonce = 42;
        grps[1].slots = NULL;
        grps[1].slot_count = 0;

        block_diff_t diff = { .block_number = 1, .groups = grps, .group_count = 2 };
        verkle_state_begin_block(vs, 1);
        apply_diff(vs, &tracker, &diff);
        verkle_state_commit_block(vs);

        uint8_t *root = (run == 0) ? root1 : root2;
        verkle_state_root_hash(vs, root);

        slot_tracker_destroy(&tracker);
        verkle_state_destroy(vs);
    }

    printf("  root1: 0x"); print_hash(root1); printf("\n");
    printf("  root2: 0x"); print_hash(root2); printf("\n");
    CHECK(memcmp(root1, root2, 32) == 0, "roots match across runs");

    /* Verify root is not all zeros (sanity) */
    uint8_t zero[32] = {0};
    CHECK(memcmp(root1, zero, 32) != 0, "root is not zero");

    cleanup();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Verkle Reconstruct Tests ===\n\n");

    test_create_and_read();
    test_update();
    test_selfdestruct();
    test_recreate_after_destruct();
    test_root_determinism();

    printf("\n========================================\n");
    printf("  %d / %d tests passed\n", tests_passed, tests_run);
    printf("========================================\n");

    return (tests_passed == tests_run) ? 0 : 1;
}
