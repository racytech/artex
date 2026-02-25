#include "../include/data_art.h"
#include "../include/logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

// Helper to create tree with mmap-only API
static data_art_tree_t *create_test_tree(const char *dir_path) {
    // Clean up and create directory
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir_path, dir_path);
    system(cmd);

    char art_path[512];
    snprintf(art_path, sizeof(art_path), "%s/art.dat", dir_path);

    data_art_tree_t *tree = data_art_create(art_path, 20);  // Use valid key_size for Ethereum
    assert(tree != NULL);
    assert(tree->mvcc_manager != NULL);

    return tree;
}

// Helper to destroy tree and clean up
static void destroy_test_tree(data_art_tree_t *tree, const char *dir_path) {
    data_art_destroy(tree);

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir_path);
    system(cmd);
}

// Test that snapshots provide isolation from concurrent changes
void test_snapshot_isolation_basic(void) {
    printf("\n=== Test: Basic Snapshot Isolation ===\n");

    const char *dir = "/tmp/test_snapshot";
    data_art_tree_t *tree = create_test_tree(dir);

    // Insert initial data
    uint64_t txn1;
    assert(data_art_begin_txn(tree, &txn1));

    // Use 20-byte keys (Ethereum address size)
    uint8_t key1[20] = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
    uint8_t value1[] = "value1";
    assert(data_art_insert(tree, key1, sizeof(key1), value1, sizeof(value1)));

    uint8_t key2[20] = {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02};
    uint8_t value2[] = "value2";
    assert(data_art_insert(tree, key2, sizeof(key2), value2, sizeof(value2)));

    assert(data_art_commit_txn(tree));
    printf("Committed initial data (txn %lu)\n", txn1);

    // Begin a snapshot
    data_art_snapshot_t *snapshot1 = data_art_begin_snapshot(tree);
    assert(snapshot1 != NULL);
    printf("Started snapshot\n");

    // Snapshot should see the committed data
    size_t read_len;
    const void *read_val = data_art_get_snapshot(tree, key1, sizeof(key1), &read_len, snapshot1);
    assert(read_val != NULL);
    assert(read_len == sizeof(value1));
    assert(memcmp(read_val, value1, read_len) == 0);
    printf("Snapshot sees key1 = %s\n", (char*)read_val);

    // Now insert more data in a new transaction
    uint64_t txn2;
    assert(data_art_begin_txn(tree, &txn2));

    uint8_t key3[20] = {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03};
    uint8_t value3[] = "value3_new";
    assert(data_art_insert(tree, key3, sizeof(key3), value3, sizeof(value3)));

    assert(data_art_commit_txn(tree));
    printf("Committed changes (txn %lu): added key3\n", txn2);

    // The snapshot should still see key1 and key2
    printf("DEBUG: About to read key1 from snapshot (after txn2 commit)\n");
    read_val = data_art_get_snapshot(tree, key1, sizeof(key1), &read_len, snapshot1);
    if (!read_val) {
        printf("ERROR: Snapshot cannot see key1 after txn2 commit (read_val=NULL)\n");
    }
    assert(read_val != NULL);
    assert(read_len == sizeof(value1));
    assert(memcmp(read_val, value1, read_len) == 0);
    printf("Snapshot still sees key1 = %s\n", (char*)read_val);

    // Snapshot should NOT see key3 (inserted after snapshot)
    const void *result = data_art_get_snapshot(tree, key3, sizeof(key3), &read_len, snapshot1);
    if (result) {
        printf("ERROR: Snapshot should NOT see key3 (inserted after snapshot)\n");
        assert(false);
    }
    printf("Snapshot does NOT see key3 (correct isolation)\n");

    // End snapshot
    data_art_end_snapshot(tree, snapshot1);
    printf("Ended snapshot\n");

    //Now without snapshot, we should see new data (key1, key2, key3)
    read_val = data_art_get(tree, key1, sizeof(key1), &read_len);
    assert(read_val != NULL);
    printf("After snapshot: key1 still visible\n");

    read_val = data_art_get(tree, key3, sizeof(key3), &read_len);
    assert(read_val != NULL);
    assert(read_len == sizeof(value3));
    assert(memcmp(read_val, value3, read_len) == 0);
    printf("After snapshot: key3 = %s (visible)\n", (char*)read_val);

    destroy_test_tree(tree, dir);

    printf("Test passed: Snapshot isolation working\n");
}

// Test snapshot isolation with deletes
void test_snapshot_isolation_deletes(void) {
    printf("\n=== Test: Snapshot Isolation with Deletes ===\n");

    const char *dir = "/tmp/test_snapshot2";
    data_art_tree_t *tree = create_test_tree(dir);

    // Insert data
    uint64_t txn1;
    assert(data_art_begin_txn(tree, &txn1));
    uint8_t key1[20] = {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF};
    uint8_t value1[] = "original";
    assert(data_art_insert(tree, key1, sizeof(key1), value1, sizeof(value1)));
    assert(data_art_commit_txn(tree));
    printf("Inserted key\n");

    // Begin snapshot
    data_art_snapshot_t *snapshot1 = data_art_begin_snapshot(tree);
    assert(snapshot1 != NULL);
    printf("Started snapshot\n");

    // Snapshot should see the key
    size_t read_len;
    const void *read_val = data_art_get_snapshot(tree, key1, sizeof(key1), &read_len, snapshot1);
    assert(read_val != NULL);
    printf("Snapshot sees key = %s\n", (char*)read_val);

    // Delete the key in a new transaction
    uint64_t txn2;
    assert(data_art_begin_txn(tree, &txn2));
    assert(data_art_delete(tree, key1, sizeof(key1)));
    assert(data_art_commit_txn(tree));
    printf("Deleted key in new transaction\n");

    // Snapshot should STILL see the key (snapshot isolation)
    read_val = data_art_get_snapshot(tree, key1, sizeof(key1), &read_len, snapshot1);
    assert(read_val != NULL);
    assert(memcmp(read_val, value1, read_len) == 0);
    printf("Snapshot still sees key = %s (delete not visible)\n", (char*)read_val);

    // End snapshot
    data_art_end_snapshot(tree, snapshot1);
    printf("Ended snapshot\n");

    // Now the delete should be visible
    read_val = data_art_get(tree, key1, sizeof(key1), &read_len);
    if (read_val) {
        printf("ERROR: Key should be deleted\n");
        assert(false);
    }
    printf("After snapshot: key deleted (visible)\n");

    destroy_test_tree(tree, dir);

    printf("Test passed: Snapshot isolation with deletes\n");
}

// Test multiple concurrent snapshots
void test_multiple_snapshots(void) {
    printf("\n=== Test: Multiple Concurrent Snapshots ===\n");

    const char *dir = "/tmp/test_snapshot3";
    data_art_tree_t *tree = create_test_tree(dir);

    // Insert v1
    uint64_t txn1;
    assert(data_art_begin_txn(tree, &txn1));
    uint8_t key[20] = {0xAA, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xAA};
    uint8_t v1[] = "version1";
    assert(data_art_insert(tree, key, sizeof(key), v1, sizeof(v1)));
    assert(data_art_commit_txn(tree));
    printf("Committed v1\n");

    // Take snapshot1
    data_art_snapshot_t *snapshot1 = data_art_begin_snapshot(tree);
    assert(snapshot1 != NULL);
    printf("Created snapshot1\n");

    // Update to v2
    uint64_t txn2;
    assert(data_art_begin_txn(tree, &txn2));
    assert(data_art_delete(tree, key, sizeof(key)));
    uint8_t v2[] = "version2";
    assert(data_art_insert(tree, key, sizeof(key), v2, sizeof(v2)));
    assert(data_art_commit_txn(tree));
    printf("Committed v2\n");

    // Verify snapshot1 still sees v1
    size_t read_len;
    const void *read_val = data_art_get_snapshot(tree, key, sizeof(key), &read_len, snapshot1);
    assert(read_val != NULL);
    assert(memcmp(read_val, v1, read_len) == 0);
    printf("Snapshot1 sees: %s\n", (char*)read_val);

    // End snapshot1 and start snapshot2
    data_art_end_snapshot(tree, snapshot1);
    data_art_snapshot_t *snapshot2 = data_art_begin_snapshot(tree);
    assert(snapshot2 != NULL);
    printf("Created snapshot2\n");

    // Snapshot2 should see v2
    read_val = data_art_get_snapshot(tree, key, sizeof(key), &read_len, snapshot2);
    assert(read_val != NULL);
    assert(memcmp(read_val, v2, read_len) == 0);
    printf("Snapshot2 sees: %s\n", (char*)read_val);

    // Update to v3
    uint64_t txn3;
    assert(data_art_begin_txn(tree, &txn3));
    assert(data_art_delete(tree, key, sizeof(key)));
    uint8_t v3[] = "version3";
    assert(data_art_insert(tree, key, sizeof(key), v3, sizeof(v3)));
    assert(data_art_commit_txn(tree));
    printf("Committed v3\n");

    // Snapshot2 should still see v2
    read_val = data_art_get_snapshot(tree, key, sizeof(key), &read_len, snapshot2);
    assert(read_val != NULL);
    assert(memcmp(read_val, v2, read_len) == 0);
    printf("Snapshot2 still sees: %s (isolated from v3)\n", (char*)read_val);

    data_art_end_snapshot(tree, snapshot2);

    // Without snapshot, should see v3
    read_val = data_art_get(tree, key, sizeof(key), &read_len);
    assert(read_val != NULL);
    assert(memcmp(read_val, v3, read_len) == 0);
    printf("Current view sees: %s\n", (char*)read_val);

    destroy_test_tree(tree, dir);

    printf("Test passed: Multiple snapshots maintain correct isolation\n");
}

int main(void) {
    log_set_level(LOG_LEVEL_INFO);

    printf("\n");
    printf("======================================================\n");
    printf("      MVCC Snapshot Isolation Integration Tests        \n");
    printf("======================================================\n");

    test_snapshot_isolation_basic();
    test_snapshot_isolation_deletes();
    test_multiple_snapshots();

    printf("\n");
    printf("======================================================\n");
    printf("              ALL SNAPSHOT TESTS PASSED                \n");
    printf("======================================================\n");
    printf("\n");

    return 0;
}
