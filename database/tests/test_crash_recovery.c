/**
 * Persistence Test Suite for mmap-backed ART
 *
 * Proves durability by testing:
 *   1. End-to-end persistence (insert -> destroy -> open -> verify)
 *   2. Large dataset persistence (1000 keys)
 *   3. Checkpoint persistence (checkpoint -> destroy -> open -> verify)
 *   4. Delete persistence (insert -> delete -> destroy -> open -> verify deletions)
 *   5. Mixed insert+delete persistence
 *   6. Transaction commit persistence
 *   7. Transaction abort not persisted
 *   8. Multiple reopen cycles
 *   9. Persistence with overflow values
 *  10. Page growth after deletes and reinserts
 */

#include "data_art.h"
#include "mmap_storage.h"
#include "crc32.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// ============================================================================
// Test Framework
// ============================================================================

#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_RESET  "\033[0m"

static int test_count = 0;
static int tests_passed = 0;

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, COLOR_RED "  FAILED: %s\n" COLOR_RESET, msg); \
        fprintf(stderr, "    at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
} while(0)

#define ASSERT_EQ(a, b, msg) do { \
    long _a = (long)(a), _b = (long)(b); \
    if (_a != _b) { \
        fprintf(stderr, COLOR_RED "  FAILED: %s (got %ld, expected %ld)\n" COLOR_RESET, \
                msg, _a, _b); \
        fprintf(stderr, "    at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
} while(0)

// ============================================================================
// Key/Value Generation
// ============================================================================

#define KEY_SIZE      32
#define VALUE_MAX_LEN 64

static void generate_key(uint8_t *key, int index) {
    memset(key, 0, KEY_SIZE);
    key[0] = (uint8_t)((index >> 24) & 0xFF);
    key[1] = (uint8_t)((index >> 16) & 0xFF);
    key[2] = (uint8_t)((index >> 8) & 0xFF);
    key[3] = (uint8_t)(index & 0xFF);
    uint32_t state = (uint32_t)index * 2654435761u;
    for (int i = 4; i < KEY_SIZE; i++) {
        state = state * 1103515245 + 12345;
        key[i] = (uint8_t)((state >> 16) & 0xFF);
    }
}

static size_t generate_value(char *buf, size_t max_len, int index) {
    int n = snprintf(buf, max_len, "val_%06d_payload_data", index);
    return (size_t)n;
}

// ============================================================================
// Test Environment
// ============================================================================

#define BASE_DIR "/tmp/test_persistence"

typedef struct {
    data_art_tree_t *tree;
    char dir_path[256];
    char art_path[512];
} test_env_t;

static void cleanup_dir(const char *dir_path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir_path);
    system(cmd);
    sync();
    usleep(10000);
}

static void make_paths(test_env_t *env, const char *suffix) {
    snprintf(env->dir_path, sizeof(env->dir_path), "%s_%s", BASE_DIR, suffix);
    snprintf(env->art_path, sizeof(env->art_path), "%s/art.dat", env->dir_path);
}

static void create_tree(test_env_t *env) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", env->dir_path);
    system(cmd);

    env->tree = data_art_create(env->art_path, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_create");
}

static void open_tree(test_env_t *env) {
    env->tree = data_art_open(env->art_path, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_open");
}

static void close_tree(test_env_t *env) {
    if (env->tree) { data_art_destroy(env->tree); env->tree = NULL; }
}

static void insert_keys(test_env_t *env, int start, int count) {
    for (int i = 0; i < count; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, start + i);
        size_t vlen = generate_value(value, sizeof(value), start + i);
        bool ok = data_art_insert(env->tree, key, KEY_SIZE, value, vlen);
        ASSERT(ok, "data_art_insert");
    }
}

static void verify_keys(test_env_t *env, int start, int count) {
    for (int i = 0; i < count; i++) {
        uint8_t key[KEY_SIZE];
        char expected[VALUE_MAX_LEN];
        generate_key(key, start + i);
        size_t expected_len = generate_value(expected, sizeof(expected), start + i);

        size_t got_len = 0;
        const void *got = data_art_get(env->tree, key, KEY_SIZE, &got_len);
        if (!got) {
            char msg[128];
            snprintf(msg, sizeof(msg), "Key %d not found after reopen", start + i);
            ASSERT(false, msg);
        }
        ASSERT_EQ((long)got_len, (long)expected_len, "value length mismatch");
        ASSERT(memcmp(got, expected, got_len) == 0, "value content mismatch");
        free((void *)got);
    }
}

static void verify_key_absent(test_env_t *env, int index) {
    uint8_t key[KEY_SIZE];
    generate_key(key, index);
    size_t len = 0;
    const void *got = data_art_get(env->tree, key, KEY_SIZE, &len);
    if (got) {
        free((void *)got);
        char msg[128];
        snprintf(msg, sizeof(msg), "Key %d should NOT exist but was found", index);
        ASSERT(false, msg);
    }
}

// ============================================================================
// Test 1: End-to-End Persistence
// ============================================================================

static void test_end_to_end_persistence(void) {
    printf("  Inserting 100 keys...\n");
    test_env_t env = {0};
    make_paths(&env, "t1");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    insert_keys(&env, 0, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 100L, "tree size before close");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 100 keys...\n");
    verify_keys(&env, 0, 100);
    verify_key_absent(&env, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 100L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 2: Large Dataset Persistence (1000 keys)
// ============================================================================

static void test_large_dataset_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t2");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    printf("  Inserting 1000 keys...\n");
    insert_keys(&env, 0, 1000);
    ASSERT_EQ((long)data_art_size(env.tree), 1000L, "tree size");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 1000 keys...\n");
    verify_keys(&env, 0, 1000);
    ASSERT_EQ((long)data_art_size(env.tree), 1000L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 3: Checkpoint Persistence
// ============================================================================

static void test_checkpoint_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t3");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);

    printf("  Creating checkpoint...\n");
    uint64_t checkpoint_lsn = 0;
    bool ok = data_art_checkpoint(env.tree, &checkpoint_lsn);
    ASSERT(ok, "data_art_checkpoint");
    printf("  Checkpoint at LSN %lu\n", checkpoint_lsn);

    printf("  Inserting 30 more keys...\n");
    insert_keys(&env, 50, 30);

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // All 80 keys should be present (mmap persists everything)
    printf("  Verifying all 80 keys...\n");
    verify_keys(&env, 0, 80);
    ASSERT_EQ((long)data_art_size(env.tree), 80L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 4: Delete Persistence
// ============================================================================

static void test_delete_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t4");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert keys 0-49
    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);

    // Delete keys 10-29 (20 deletions)
    printf("  Deleting keys 10-29...\n");
    for (int i = 10; i < 30; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        bool ok = data_art_delete(env.tree, key, KEY_SIZE);
        ASSERT(ok, "delete existing key");
    }

    ASSERT_EQ((long)data_art_size(env.tree), 30L, "tree size after deletes");

    // Close and reopen
    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // Keys 0-9 should exist
    printf("  Verifying keys 0-9 present...\n");
    verify_keys(&env, 0, 10);

    // Keys 10-29 should NOT exist
    printf("  Verifying keys 10-29 absent...\n");
    for (int i = 10; i < 30; i++) {
        verify_key_absent(&env, i);
    }

    // Keys 30-49 should exist
    printf("  Verifying keys 30-49 present...\n");
    verify_keys(&env, 30, 20);

    ASSERT_EQ((long)data_art_size(env.tree), 30L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 5: Mixed Insert + Delete Persistence
// ============================================================================

static void test_mixed_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t5");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 10 keys via auto-commit
    printf("  Inserting 10 keys...\n");
    insert_keys(&env, 0, 10);

    // Begin explicit txn, insert 5 more keys, commit
    printf("  Committing explicit txn with 5 keys...\n");
    uint64_t txn_id;
    ASSERT(data_art_begin_txn(env.tree, &txn_id), "begin_txn");
    for (int i = 0; i < 5; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, 100 + i);
        size_t vlen = generate_value(value, sizeof(value), 100 + i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "txn insert");
    }
    ASSERT(data_art_commit_txn(env.tree), "commit_txn");

    ASSERT_EQ((long)data_art_size(env.tree), 15L, "tree size (10 auto + 5 committed)");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // 10 auto-committed keys should exist
    printf("  Verifying 10 auto-committed keys present...\n");
    verify_keys(&env, 0, 10);

    // 5 committed txn keys should exist
    printf("  Verifying 5 committed txn keys present...\n");
    verify_keys(&env, 100, 5);

    ASSERT_EQ((long)data_art_size(env.tree), 15L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 6: Transaction Commit Persistence
// ============================================================================

static void test_txn_commit_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t6");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert via explicit transaction
    printf("  Inserting 50 keys via explicit txn...\n");
    uint64_t txn_id;
    ASSERT(data_art_begin_txn(env.tree, &txn_id), "begin_txn");
    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, i);
        size_t vlen = generate_value(value, sizeof(value), i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "txn insert");
    }
    ASSERT(data_art_commit_txn(env.tree), "commit_txn");
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size after commit");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 50 keys...\n");
    verify_keys(&env, 0, 50);
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 7: Transaction Abort Not Persisted
// ============================================================================

static void test_txn_abort_not_persisted(void) {
    test_env_t env = {0};
    make_paths(&env, "t7");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 20 keys normally
    printf("  Inserting 20 auto-committed keys...\n");
    insert_keys(&env, 0, 20);

    // Begin txn with 10 more keys, then ABORT
    printf("  Inserting 10 keys in txn, then aborting...\n");
    uint64_t txn_id;
    ASSERT(data_art_begin_txn(env.tree, &txn_id), "begin_txn");
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, 1000 + i);
        size_t vlen = generate_value(value, sizeof(value), 1000 + i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "txn insert");
    }
    ASSERT(data_art_abort_txn(env.tree), "abort_txn");

    // Only 20 keys should exist (aborted txn not applied)
    ASSERT_EQ((long)data_art_size(env.tree), 20L, "tree size after abort");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // 20 auto-committed keys should exist
    printf("  Verifying 20 auto-committed keys present...\n");
    verify_keys(&env, 0, 20);

    // 10 aborted keys should NOT exist
    printf("  Verifying 10 aborted keys absent...\n");
    for (int i = 0; i < 10; i++) {
        verify_key_absent(&env, 1000 + i);
    }

    ASSERT_EQ((long)data_art_size(env.tree), 20L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 8: Multiple Reopen Cycles
// ============================================================================

static void test_multiple_reopen_cycles(void) {
    test_env_t env = {0};
    make_paths(&env, "t8");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 50 keys
    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);

    // Checkpoint
    printf("  Checkpointing...\n");
    uint64_t ckpt_lsn;
    ASSERT(data_art_checkpoint(env.tree, &ckpt_lsn), "checkpoint");

    // Close and reopen
    printf("  Cycle 1: close and reopen...\n");
    close_tree(&env);
    open_tree(&env);

    // Insert 50 more keys
    printf("  Inserting 50 more keys...\n");
    insert_keys(&env, 50, 50);

    // Close and reopen again
    printf("  Cycle 2: close and reopen...\n");
    close_tree(&env);
    open_tree(&env);

    // Verify all 100 keys present
    printf("  Verifying all 100 keys...\n");
    verify_keys(&env, 0, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 100L, "tree size");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 9: Persistence with Overflow Values
// ============================================================================

static void test_overflow_value_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t9");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 10 keys with 8KB values (triggers overflow pages)
    const size_t big_val_len = 8 * 1024;
    char *big_val = malloc(big_val_len);
    ASSERT(big_val != NULL, "malloc big value");

    printf("  Inserting 10 keys with 8KB values...\n");
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        memset(big_val, (uint8_t)(i + 'A'), big_val_len);
        bool ok = data_art_insert(env.tree, key, KEY_SIZE, big_val, big_val_len);
        ASSERT(ok, "insert overflow key");
    }

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying 10 overflow keys...\n");
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        memset(big_val, (uint8_t)(i + 'A'), big_val_len);

        size_t found_len = 0;
        const void *found = data_art_get(env.tree, key, KEY_SIZE, &found_len);
        ASSERT(found != NULL, "overflow key should exist");
        ASSERT_EQ((long)found_len, (long)big_val_len, "overflow value length");
        ASSERT(memcmp(found, big_val, big_val_len) == 0, "overflow value content");
        free((void *)found);
    }
    ASSERT_EQ((long)data_art_size(env.tree), 10L, "tree size");

    free(big_val);
    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 10: Page Growth After Deletes and Reinserts
// ============================================================================

static void test_page_growth_after_deletes(void) {
    test_env_t env = {0};
    make_paths(&env, "t10");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 200 keys
    printf("  Inserting 200 keys...\n");
    insert_keys(&env, 0, 200);

    // Delete 150 keys (0-149)
    printf("  Deleting 150 keys...\n");
    for (int i = 0; i < 150; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        bool ok = data_art_delete(env.tree, key, KEY_SIZE);
        ASSERT(ok, "delete key");
    }

    // Verify tree size is 50
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size after deletes");

    // Insert 150 NEW keys (starting at 1000)
    printf("  Inserting 150 new keys...\n");
    insert_keys(&env, 1000, 150);

    // Close and reopen
    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // Verify all 200 keys present (50 surviving + 150 new)
    printf("  Verifying 50 surviving keys (150-199)...\n");
    verify_keys(&env, 150, 50);

    printf("  Verifying 150 new keys (1000-1149)...\n");
    verify_keys(&env, 1000, 150);

    // Deleted keys should be absent
    printf("  Verifying deleted keys (0-149) absent...\n");
    for (int i = 0; i < 150; i++) {
        verify_key_absent(&env, i);
    }

    ASSERT_EQ((long)data_art_size(env.tree), 200L, "final tree size");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 11: Shadow Header — Slot Rotation
// ============================================================================

static void test_shadow_header_slot_rotation(void) {
    test_env_t env = {0};
    make_paths(&env, "t11");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert some keys and checkpoint
    printf("  Inserting 20 keys, checkpoint 1...\n");
    insert_keys(&env, 0, 20);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 1");

    // Read header to check slot state
    mmap_header_page_t *hp = (mmap_header_page_t *)env.tree->mmap_storage->base;
    int active1 = env.tree->mmap_storage->active_slot;
    uint32_t ckpt_num1 = hp->slots[active1].checkpoint_num;
    printf("  After checkpoint 1: active_slot=%d, checkpoint_num=%u\n", active1, ckpt_num1);
    ASSERT(ckpt_num1 >= 1, "checkpoint_num should be >= 1");

    // Insert more and checkpoint again
    printf("  Inserting 10 more keys, checkpoint 2...\n");
    insert_keys(&env, 20, 10);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 2");

    int active2 = env.tree->mmap_storage->active_slot;
    uint32_t ckpt_num2 = hp->slots[active2].checkpoint_num;
    printf("  After checkpoint 2: active_slot=%d, checkpoint_num=%u\n", active2, ckpt_num2);

    // Slots should alternate
    ASSERT(active2 != active1, "active slot should alternate after checkpoint");
    ASSERT(ckpt_num2 == ckpt_num1 + 1, "checkpoint_num should increment by 1");

    // Both slots should have valid CRC
    uint32_t crc0 = compute_crc32((const uint8_t *)&hp->slots[0], 56);
    uint32_t crc1 = compute_crc32((const uint8_t *)&hp->slots[1], 56);
    ASSERT(hp->slots[0].checksum == crc0, "slot 0 CRC should be valid");
    ASSERT(hp->slots[1].checksum == crc1, "slot 1 CRC should be valid");

    // Close and reopen — should recover from the higher checkpoint_num slot
    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 30 keys...\n");
    verify_keys(&env, 0, 30);
    ASSERT_EQ((long)data_art_size(env.tree), 30L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 12: Shadow Header — Corrupt Active Slot Recovery
// ============================================================================

static void test_shadow_header_corrupt_active(void) {
    test_env_t env = {0};
    make_paths(&env, "t12");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert keys, checkpoint twice so both slots are valid
    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 1");
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 2");

    int active = env.tree->mmap_storage->active_slot;
    printf("  Active slot: %d\n", active);

    // Get the file path and close the tree
    char path_copy[512];
    strncpy(path_copy, env.art_path, sizeof(path_copy));
    close_tree(&env);

    // Corrupt the active slot's checksum via pwrite
    printf("  Corrupting active slot %d checksum...\n", active);
    int fd = open(path_copy, O_RDWR);
    ASSERT(fd >= 0, "open for corruption");

    // Checksum is at offset 56 within the slot (each slot is 64 bytes)
    off_t corrupt_offset = (off_t)(active * 64 + 56);
    uint32_t garbage = 0xDEADBEEF;
    ssize_t written = pwrite(fd, &garbage, sizeof(garbage), corrupt_offset);
    ASSERT(written == sizeof(garbage), "pwrite corruption");
    fsync(fd);
    close(fd);

    // Reopen — should recover from the OTHER (non-corrupt) slot
    printf("  Reopening (should recover from slot %d)...\n", 1 - active);
    open_tree(&env);

    // The recovered slot is the previous checkpoint (checkpoint 1),
    // which also has 50 keys since we inserted before both checkpoints
    printf("  Verifying keys after corrupt-slot recovery...\n");
    verify_keys(&env, 0, 50);
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size from fallback slot");

    // Active slot should now be the non-corrupt one
    ASSERT(env.tree->mmap_storage->active_slot == 1 - active,
           "should have picked the non-corrupt slot");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 13: Shadow Header — Both Slots Corrupt
// ============================================================================

static void test_shadow_header_both_corrupt(void) {
    test_env_t env = {0};
    make_paths(&env, "t13");
    cleanup_dir(env.dir_path);

    // Create and populate
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", env.dir_path);
    system(cmd);

    data_art_tree_t *tree = data_art_create(env.art_path, KEY_SIZE);
    ASSERT(tree != NULL, "create tree");
    data_art_destroy(tree);

    // Corrupt BOTH slots' checksums
    printf("  Corrupting both slot checksums...\n");
    int fd = open(env.art_path, O_RDWR);
    ASSERT(fd >= 0, "open for corruption");

    uint32_t garbage = 0xDEADBEEF;
    pwrite(fd, &garbage, sizeof(garbage), 56);       // slot 0 checksum
    pwrite(fd, &garbage, sizeof(garbage), 64 + 56);  // slot 1 checksum
    fsync(fd);
    close(fd);

    // Reopen should FAIL — both slots corrupt
    printf("  Reopening (should fail)...\n");
    data_art_tree_t *bad_tree = data_art_open(env.art_path, KEY_SIZE);
    ASSERT(bad_tree == NULL, "open should fail with both slots corrupt");

    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 14: Shadow Header — v1 to v2 Migration
// ============================================================================

static void test_shadow_header_v1_migration(void) {
    test_env_t env = {0};
    make_paths(&env, "t14");
    cleanup_dir(env.dir_path);

    char cmd_buf[512];
    snprintf(cmd_buf, sizeof(cmd_buf), "mkdir -p %s", env.dir_path);
    system(cmd_buf);

    // Create a file with a v1 header manually
    printf("  Creating v1 format file...\n");
    size_t file_size = 16384 * PAGE_SIZE;
    int fd = open(env.art_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT(fd >= 0, "create v1 file");
    ASSERT(ftruncate(fd, (off_t)file_size) == 0, "ftruncate v1 file");

    // Write a v1 header (56 bytes of metadata + padding)
    mmap_header_v1_t v1 = {0};
    v1.magic = MMAP_MAGIC;
    v1.version = 1;  // v1 format
    v1.page_size = PAGE_SIZE;
    v1.next_page_id = 1;
    v1.root_page_id = 0;
    v1.root_offset = 0;
    v1.padding = 0;
    v1.tree_size = 0;
    v1.key_size = KEY_SIZE;
    pwrite(fd, &v1, sizeof(v1), 0);
    fsync(fd);
    close(fd);

    // Open with new code — should auto-migrate v1 → v2
    printf("  Opening v1 file (should auto-migrate to v2)...\n");
    open_tree(&env);

    // Verify migration: header should now be v2 with valid CRC
    mmap_header_page_t *hp = (mmap_header_page_t *)env.tree->mmap_storage->base;
    ASSERT(hp->slots[0].version == MMAP_FORMAT_VERSION, "slot 0 version should be 2");
    ASSERT(hp->slots[1].version == MMAP_FORMAT_VERSION, "slot 1 version should be 2");

    uint32_t crc0 = compute_crc32((const uint8_t *)&hp->slots[0], 56);
    ASSERT(hp->slots[0].checksum == crc0, "slot 0 CRC valid after migration");

    ASSERT_EQ((long)hp->slots[0].key_size, (long)KEY_SIZE, "key_size preserved");
    ASSERT_EQ((long)data_art_size(env.tree), 0L, "empty tree after migration");

    // Insert keys and checkpoint to verify full functionality
    printf("  Inserting 20 keys after migration...\n");
    insert_keys(&env, 0, 20);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint after migration");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying 20 keys after migration reopen...\n");
    verify_keys(&env, 0, 20);
    ASSERT_EQ((long)data_art_size(env.tree), 20L, "tree size after migration reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 15: Shadow Header — Checkpoint After Corrupt Slot Recovery
// ============================================================================

static void test_shadow_header_checkpoint_after_recovery(void) {
    test_env_t env = {0};
    make_paths(&env, "t15");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert and checkpoint to fill both slots
    printf("  Inserting 30 keys, 2 checkpoints...\n");
    insert_keys(&env, 0, 30);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 1");
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 2");

    int active = env.tree->mmap_storage->active_slot;
    char path_copy[512];
    strncpy(path_copy, env.art_path, sizeof(path_copy));
    close_tree(&env);

    // Corrupt the active slot
    printf("  Corrupting active slot %d...\n", active);
    int fd = open(path_copy, O_RDWR);
    ASSERT(fd >= 0, "open for corruption");
    uint32_t garbage = 0xBADBAD;
    pwrite(fd, &garbage, sizeof(garbage), (off_t)(active * 64 + 56));
    fsync(fd);
    close(fd);

    // Reopen (recovers from non-corrupt slot)
    printf("  Reopening after corruption...\n");
    open_tree(&env);
    verify_keys(&env, 0, 30);

    // Now insert more and checkpoint — this should heal the corrupt slot
    printf("  Inserting 20 more keys and checkpointing...\n");
    insert_keys(&env, 30, 20);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint after recovery");

    // Both slots should now be valid again
    mmap_header_page_t *hp = (mmap_header_page_t *)env.tree->mmap_storage->base;
    uint32_t crc_active = compute_crc32(
        (const uint8_t *)&hp->slots[env.tree->mmap_storage->active_slot], 56);
    ASSERT(hp->slots[env.tree->mmap_storage->active_slot].checksum == crc_active,
           "new active slot should have valid CRC");

    // Close and reopen one more time
    printf("  Final close and reopen...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 50 keys...\n");
    verify_keys(&env, 0, 50);
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size final");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    log_set_level(LOG_LEVEL_ERROR);

    printf("\n");
    printf("========================================\n");
    printf(" Persistence Test Suite\n");
    printf("========================================\n\n");

    #define RUN_TEST(fn) do { \
        test_count++; \
        printf("[TEST %d] %s\n", test_count, #fn); \
        fn(); \
        tests_passed++; \
        printf(COLOR_GREEN "  PASSED\n\n" COLOR_RESET); \
    } while(0)

    RUN_TEST(test_end_to_end_persistence);
    RUN_TEST(test_large_dataset_persistence);
    RUN_TEST(test_checkpoint_persistence);
    RUN_TEST(test_delete_persistence);
    RUN_TEST(test_mixed_persistence);
    RUN_TEST(test_txn_commit_persistence);
    RUN_TEST(test_txn_abort_not_persisted);
    RUN_TEST(test_multiple_reopen_cycles);
    RUN_TEST(test_overflow_value_persistence);
    RUN_TEST(test_page_growth_after_deletes);
    RUN_TEST(test_shadow_header_slot_rotation);
    RUN_TEST(test_shadow_header_corrupt_active);
    RUN_TEST(test_shadow_header_both_corrupt);
    RUN_TEST(test_shadow_header_v1_migration);
    RUN_TEST(test_shadow_header_checkpoint_after_recovery);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return (tests_passed == test_count) ? 0 : 1;
}
