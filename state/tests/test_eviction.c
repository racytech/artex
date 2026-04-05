/**
 * Test: cold storage hart eviction and reload.
 *
 * Verifies:
 *   1. Evicted accounts produce same storage root after reload
 *   2. Storage reads return correct values after reload
 *   3. Storage writes work after reload (evict → reload → modify)
 *   4. state_save roundtrip works with evicted accounts
 *   5. has_storage returns true for evicted accounts
 */

#include "evm_state.h"
#include "state.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

static int errors = 0;

static void check(int cond, const char *msg) {
    if (!cond) {
        printf("  FAIL: %s\n", msg);
        errors++;
    } else {
        printf("  OK: %s\n", msg);
    }
}

static void print_hash(const char *label, const uint8_t h[32]) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

static address_t make_addr(uint8_t seed) {
    address_t a = {0};
    for (int i = 0; i < 20; i++) a.bytes[i] = seed + (uint8_t)i;
    return a;
}

static uint256_t make_slot(uint64_t v) {
    uint256_t s = {0};
    s.low = v;
    return s;
}

static uint256_t make_val(uint64_t v) {
    uint256_t s = {0};
    s.low = v;
    return s;
}

int main(void) {
    printf("=== test_eviction ===\n\n");

    const char *evict_dir = "/tmp/test_eviction";
    char evict_file[256];
    snprintf(evict_file, sizeof(evict_file), "%s/storage_evict.dat", evict_dir);

    /* Ensure directory exists */
    mkdir(evict_dir, 0755);
    unlink(evict_file);

    /* Create state */
    evm_state_t *es = evm_state_create(NULL);
    state_t *st = evm_state_get_state(es);

    /* Enable eviction */
    state_set_evict_path(st, evict_dir);
    state_set_evict_threshold(st, 10); /* evict after 10 blocks of inactivity */

    /* ====== Setup: create accounts with storage ====== */
    printf("--- Setup: create 3 accounts with storage ---\n");

    address_t a1 = make_addr(0x10);
    address_t a2 = make_addr(0x20);
    address_t a3 = make_addr(0x30);

    /* Block 1: populate all accounts */
    evm_state_begin_block(es, 1);

    /* Account 1: 5 storage slots */
    evm_state_set_nonce(es, &a1, 1);
    for (uint64_t i = 1; i <= 5; i++) {
        uint256_t slot = make_slot(i);
        uint256_t val = make_val(100 + i);
        evm_state_set_storage(es, &a1, &slot, &val);
    }
    evm_state_commit_tx(es);

    /* Account 2: 3 storage slots */
    evm_state_set_nonce(es, &a2, 1);
    for (uint64_t i = 1; i <= 3; i++) {
        uint256_t slot = make_slot(i);
        uint256_t val = make_val(200 + i);
        evm_state_set_storage(es, &a2, &slot, &val);
    }
    evm_state_commit_tx(es);

    /* Account 3: 2 storage slots */
    evm_state_set_nonce(es, &a3, 1);
    for (uint64_t i = 1; i <= 2; i++) {
        uint256_t slot = make_slot(i);
        uint256_t val = make_val(300 + i);
        evm_state_set_storage(es, &a3, &slot, &val);
    }
    evm_state_commit_tx(es);

    /* Compute initial root to populate storage_roots */
    evm_state_compute_state_root_ex(es, false);

    uint256_t slot1 = make_slot(3);

    /* Record pre-eviction values */
    uint256_t pre_val_a1 = evm_state_get_storage(es, &a1, &slot1);
    uint256_t pre_val_a2 = evm_state_get_storage(es, &a2, &slot1);
    printf("  a1 slot 3 = %lu\n", pre_val_a1.low);
    printf("  a2 slot 3 = %lu\n", pre_val_a2.low);

    /* ====== Test 1: Evict cold accounts ====== */
    printf("\n--- Test 1: Evict cold accounts ---\n");

    /* Advance blocks to make accounts cold (>10 blocks since last access) */
    for (uint64_t b = 2; b <= 20; b++) {
        evm_state_begin_block(es, b);
        /* Touch a3 to keep it hot (must write — reads don't update last_access_block) */
        if (b == 15) {
            uint256_t touch_val = make_val(301);
            evm_state_set_storage(es, &a3, &slot1, &touch_val);
            evm_state_commit_tx(es);
        }
        evm_state_compute_state_root_ex2(es, false, false);
    }

    /* Compute reference root AFTER all modifications (including a3 at block 15) */
    evm_state_invalidate_all(es);
    hash_t root_before = evm_state_compute_state_root_ex(es, false);
    print_hash("root before eviction", root_before.bytes);

    /* Now: a1 last accessed at block 1, a2 at block 1, a3 at block 15 */
    /* At block 20 with threshold=10: a1 and a2 are cold, a3 is hot */

    uint32_t n_evicted = state_evict_cold_storage(st);
    printf("  evicted: %u accounts\n", n_evicted);
    check(n_evicted == 2, "evicted 2 cold accounts (a1, a2)");

    /* ====== Test 2: has_storage still returns true ====== */
    printf("\n--- Test 2: has_storage for evicted accounts ---\n");
    check(evm_state_has_storage(es, &a1), "a1 has_storage after eviction");
    check(evm_state_has_storage(es, &a2), "a2 has_storage after eviction");
    check(evm_state_has_storage(es, &a3), "a3 has_storage (not evicted)");

    /* ====== Test 3: Read triggers reload, values correct ====== */
    printf("\n--- Test 3: Read after eviction (triggers reload) ---\n");
    uint256_t post_val_a1 = evm_state_get_storage(es, &a1, &slot1);
    check(post_val_a1.low == pre_val_a1.low, "a1 slot 3 value matches after reload");

    uint256_t post_val_a2 = evm_state_get_storage(es, &a2, &slot1);
    check(post_val_a2.low == pre_val_a2.low, "a2 slot 3 value matches after reload");

    /* Check all slots for a1 */
    for (uint64_t i = 1; i <= 5; i++) {
        uint256_t slot = make_slot(i);
        uint256_t val = evm_state_get_storage(es, &a1, &slot);
        check(val.low == 100 + i, "a1 slot value round-trip");
    }

    /* ====== Test 4: Root matches after reload ====== */
    printf("\n--- Test 4: Root after evict+reload ---\n");
    /* Need to invalidate and recompute to get a clean root */
    evm_state_invalidate_all(es);
    hash_t root_after = evm_state_compute_state_root_ex(es, false);
    print_hash("root after reload", root_after.bytes);
    check(memcmp(root_before.bytes, root_after.bytes, 32) == 0,
          "root matches after evict+reload");

    /* ====== Test 5: Write after eviction ====== */
    printf("\n--- Test 5: Evict again, then write ---\n");

    /* Make a1 cold again */
    for (uint64_t b = 21; b <= 40; b++) {
        evm_state_begin_block(es, b);
        evm_state_compute_state_root_ex2(es, false, false);
    }
    n_evicted = state_evict_cold_storage(st);
    printf("  evicted: %u accounts\n", n_evicted);

    /* Write to evicted a1 — should trigger reload first */
    evm_state_begin_block(es, 41);
    uint256_t new_val = make_val(999);
    evm_state_set_storage(es, &a1, &slot1, &new_val);
    evm_state_commit_tx(es);

    uint256_t read_back = evm_state_get_storage(es, &a1, &slot1);
    check(read_back.low == 999, "write after eviction works");

    /* Other slots still intact */
    uint256_t slot2 = make_slot(2);
    uint256_t read_other = evm_state_get_storage(es, &a1, &slot2);
    check(read_other.low == 102, "other slots preserved after evict+write");

    /* ====== Test 6: state_save with evicted accounts ====== */
    printf("\n--- Test 6: state_save/load with evicted accounts ---\n");

    /* Make a2, a3 cold and evict */
    for (uint64_t b = 42; b <= 60; b++) {
        evm_state_begin_block(es, b);
        evm_state_compute_state_root_ex2(es, false, false);
    }
    state_evict_cold_storage(st);

    /* Compute root before save */
    evm_state_invalidate_all(es);
    hash_t root_save = evm_state_compute_state_root_ex(es, false);
    print_hash("root before save", root_save.bytes);

    /* Save */
    const char *save_path = "/tmp/test_eviction_state.bin";
    check(state_save(st, save_path, &root_save), "state_save succeeds");

    /* Load into fresh state WITH eviction path — storage streams to evict file */
    const char *evict_dir2 = "/tmp/test_eviction2";
    char evict_file2[256];
    snprintf(evict_file2, sizeof(evict_file2), "%s/storage_evict.dat", evict_dir2);
    mkdir(evict_dir2, 0755);
    unlink(evict_file2);

    evm_state_t *es2 = evm_state_create(NULL);
    state_t *st2 = evm_state_get_state(es2);
    state_set_evict_path(st2, evict_dir2);

    hash_t loaded_root;
    check(state_load(st2, save_path, &loaded_root), "state_load succeeds");

    /* Verify storage is on disk, not in RAM (evict_count > 0, storage == NULL) */
    /* Read a value — should trigger lazy reload from evict file */
    uint256_t lazy_val = evm_state_get_storage(es2, &a1, &slot1);
    check(lazy_val.low == 999, "lazy reload after state_load returns correct value");

    /* Compute root on loaded state */
    evm_state_invalidate_all(es2);
    hash_t root_loaded = evm_state_compute_state_root_ex(es2, false);
    print_hash("root after load", root_loaded.bytes);
    check(memcmp(root_save.bytes, root_loaded.bytes, 32) == 0,
          "save/load root matches with evicted accounts");

    /* Cleanup */
    evm_state_destroy(es2);
    evm_state_destroy(es);
    unlink(save_path);
    unlink(evict_file);
    unlink(evict_file2);
    rmdir(evict_dir);
    rmdir(evict_dir2);

    printf("\n=== %s (%d errors) ===\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
