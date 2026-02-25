/**
 * Test: data_art_node_ops - Node Growth and Child Management
 *
 * Tests all node operations and transitions:
 * - Adding children to NODE_4, NODE_16, NODE_48, NODE_256
 * - Node growth: 4->16, 16->48, 48->256
 * - Proper child retrieval after operations
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Test counter
static int tests_run = 0;
static int tests_passed = 0;

// External functions we're testing (from data_art_node_ops.c)
extern node_ref_t data_art_add_child(data_art_tree_t *tree, node_ref_t node_ref,
                                      uint8_t byte, node_ref_t child_ref);

// Helper functions from data_art_core.c
extern node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref,
                                  const void *node, size_t size);
extern void data_art_reset_arena(void);

// Test result macros
#define TEST(name) \
    do { \
        printf("TEST: %s ... ", name); \
        fflush(stdout); \
        tests_run++; \
    } while (0)

#define PASS() \
    do { \
        printf("✓ PASS\n"); \
        tests_passed++; \
    } while (0)

#define FAIL(msg) \
    do { \
        printf("✗ FAIL: %s\n", msg); \
        return false; \
    } while (0)

/**
 * Create a dummy leaf node for testing
 */
static node_ref_t create_dummy_leaf(data_art_tree_t *tree, uint8_t id) {
    size_t leaf_size = sizeof(data_art_leaf_t) + 10;
    node_ref_t ref = data_art_alloc_node(tree, leaf_size);
    if (node_ref_is_null(ref)) {
        return NULL_NODE_REF;
    }

    // Allocate full leaf_size on the stack (struct + flexible array data)
    char buf[sizeof(data_art_leaf_t) + 10];
    memset(buf, 0, sizeof(buf));
    data_art_leaf_t *leaf = (data_art_leaf_t *)buf;
    leaf->type = DATA_NODE_LEAF;
    leaf->key_len = 1;
    leaf->value_len = 1;
    leaf->inline_data_len = 2;

    // Write unique identifier
    leaf->data[0] = id;  // key
    leaf->data[1] = id;  // value

    if (!data_art_write_node(tree, ref, leaf, leaf_size)) {
        return NULL_NODE_REF;
    }

    return ref;
}

/**
 * Verify a child exists at a specific byte in a node
 */
static bool verify_child_exists(data_art_tree_t *tree, node_ref_t node_ref,
                                 uint8_t byte, uint8_t expected_leaf_id) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) return false;

    uint8_t type = *(const uint8_t *)node;
    node_ref_t child_ref = NULL_NODE_REF;

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    child_ref.page_id = n->child_page_ids[i];
                    child_ref.offset = n->child_offsets[i];
                    break;
                }
            }
            break;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    child_ref.page_id = n->child_page_ids[i];
                    child_ref.offset = n->child_offsets[i];
                    break;
                }
            }
            break;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            uint8_t idx = n->keys[byte];
            if (idx != 255) {
                child_ref.page_id = n->child_page_ids[idx];
                child_ref.offset = n->child_offsets[idx];
            }
            break;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            if (n->child_page_ids[byte] != 0) {
                child_ref.page_id = n->child_page_ids[byte];
                child_ref.offset = n->child_offsets[byte];
            }
            break;
        }
        default:
            return false;
    }

    if (node_ref_is_null(child_ref)) return false;

    // Verify the leaf has the expected ID
    const data_art_leaf_t *leaf = (const data_art_leaf_t *)data_art_load_node(tree, child_ref);
    if (!leaf || leaf->type != DATA_NODE_LEAF) return false;

    return leaf->data[0] == expected_leaf_id;
}

/**
 * Test: Add children to NODE_4 (without growth)
 */
static bool test_node4_add_children(void) {
    TEST("NODE_4: add 4 children");

    system("rm -rf /tmp/test_nodeops_n4add && mkdir -p /tmp/test_nodeops_n4add");

    data_art_tree_t *tree = data_art_create("/tmp/test_nodeops_n4add/art.dat", 32);
    assert(tree != NULL);

    // Create NODE_4
    node_ref_t n4_ref = data_art_alloc_node(tree, sizeof(data_art_node4_t));
    if (node_ref_is_null(n4_ref)) {
        data_art_destroy(tree);
        FAIL("failed to allocate NODE_4");
    }

    data_art_node4_t n4;
    memset(&n4, 0, sizeof(n4));
    n4.type = DATA_NODE_4;
    n4.num_children = 0;

    if (!data_art_write_node(tree, n4_ref, &n4, sizeof(n4))) {
        data_art_destroy(tree);
        FAIL("failed to write NODE_4");
    }

    // Add 4 children
    uint8_t keys[] = {10, 20, 30, 40};
    for (int i = 0; i < 4; i++) {
        node_ref_t leaf = create_dummy_leaf(tree, keys[i]);
        if (node_ref_is_null(leaf)) {
            data_art_destroy(tree);
            FAIL("failed to create leaf");
        }

        n4_ref = data_art_add_child(tree, n4_ref, keys[i], leaf);
        if (node_ref_is_null(n4_ref)) {
            data_art_destroy(tree);
            FAIL("failed to add child");
        }
    }

    // Verify all children exist
    for (int i = 0; i < 4; i++) {
        if (!verify_child_exists(tree, n4_ref, keys[i], keys[i])) {
            data_art_destroy(tree);
            FAIL("child verification failed");
        }
    }

    // Verify it's still NODE_4
    const data_art_node4_t *final = (const data_art_node4_t *)data_art_load_node(tree, n4_ref);
    if (!final || final->type != DATA_NODE_4 || final->num_children != 4) {
        data_art_destroy(tree);
        FAIL("node type or count incorrect");
    }

    data_art_destroy(tree);

    PASS();
    return true;
}

/**
 * Test: NODE_4 grows to NODE_16
 */
static bool test_node4_grows_to_node16(void) {
    TEST("NODE_4 → NODE_16: growth on 5th child");

    system("rm -rf /tmp/test_nodeops_4to16 && mkdir -p /tmp/test_nodeops_4to16");

    data_art_tree_t *tree = data_art_create("/tmp/test_nodeops_4to16/art.dat", 32);
    assert(tree != NULL);

    // Create NODE_4 with 4 children
    node_ref_t node_ref = data_art_alloc_node(tree, sizeof(data_art_node4_t));
    if (node_ref_is_null(node_ref)) {
        data_art_destroy(tree);
        FAIL("failed to allocate NODE_4");
    }

    data_art_node4_t n4;
    memset(&n4, 0, sizeof(n4));
    n4.type = DATA_NODE_4;
    n4.num_children = 0;

    if (!data_art_write_node(tree, node_ref, &n4, sizeof(n4))) {
        data_art_destroy(tree);
        FAIL("failed to write NODE_4");
    }

    // Add 5 children (should trigger growth on the 5th)
    uint8_t keys[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        node_ref_t leaf = create_dummy_leaf(tree, keys[i]);
        if (node_ref_is_null(leaf)) {
            data_art_destroy(tree);
            FAIL("failed to create leaf");
        }

        node_ref = data_art_add_child(tree, node_ref, keys[i], leaf);
        if (node_ref_is_null(node_ref)) {
            data_art_destroy(tree);
            FAIL("failed to add child");
        }
    }

    // Verify it grew to NODE_16
    const void *node = data_art_load_node(tree, node_ref);
    if (!node || *(const uint8_t *)node != DATA_NODE_16) {
        data_art_destroy(tree);
        FAIL("did not grow to NODE_16");
    }

    const data_art_node16_t *n16 = (const data_art_node16_t *)node;
    if (n16->num_children != 5) {
        data_art_destroy(tree);
        FAIL("incorrect child count");
    }

    // Verify all children are accessible
    for (int i = 0; i < 5; i++) {
        if (!verify_child_exists(tree, node_ref, keys[i], keys[i])) {
            data_art_destroy(tree);
            FAIL("child verification failed after growth");
        }
    }

    data_art_destroy(tree);

    PASS();
    return true;
}

/**
 * Test: Add children to NODE_16 (without growth)
 */
static bool test_node16_add_children(void) {
    TEST("NODE_16: add 16 children");

    system("rm -rf /tmp/test_nodeops_n16add && mkdir -p /tmp/test_nodeops_n16add");

    data_art_tree_t *tree = data_art_create("/tmp/test_nodeops_n16add/art.dat", 32);
    assert(tree != NULL);

    // Create NODE_16
    node_ref_t n16_ref = data_art_alloc_node(tree, sizeof(data_art_node16_t));
    if (node_ref_is_null(n16_ref)) {
        data_art_destroy(tree);
        FAIL("failed to allocate NODE_16");
    }

    data_art_node16_t n16;
    memset(&n16, 0, sizeof(n16));
    n16.type = DATA_NODE_16;
    n16.num_children = 0;

    if (!data_art_write_node(tree, n16_ref, &n16, sizeof(n16))) {
        data_art_destroy(tree);
        FAIL("failed to write NODE_16");
    }

    // Add 16 children
    for (int i = 0; i < 16; i++) {
        uint8_t key = 10 + i * 5;
        node_ref_t leaf = create_dummy_leaf(tree, key);
        if (node_ref_is_null(leaf)) {
            data_art_destroy(tree);
            FAIL("failed to create leaf");
        }

        n16_ref = data_art_add_child(tree, n16_ref, key, leaf);
        if (node_ref_is_null(n16_ref)) {
            data_art_destroy(tree);
            FAIL("failed to add child");
        }
    }

    // Verify all children exist
    for (int i = 0; i < 16; i++) {
        uint8_t key = 10 + i * 5;
        if (!verify_child_exists(tree, n16_ref, key, key)) {
            data_art_destroy(tree);
            FAIL("child verification failed");
        }
    }

    // Verify it's still NODE_16
    const data_art_node16_t *final = (const data_art_node16_t *)data_art_load_node(tree, n16_ref);
    if (!final || final->type != DATA_NODE_16 || final->num_children != 16) {
        data_art_destroy(tree);
        FAIL("node type or count incorrect");
    }

    data_art_destroy(tree);

    PASS();
    return true;
}

/**
 * Test: NODE_16 grows to NODE_48
 */
static bool test_node16_grows_to_node48(void) {
    TEST("NODE_16 → NODE_48: growth on 17th child");

    system("rm -rf /tmp/test_nodeops_16to48 && mkdir -p /tmp/test_nodeops_16to48");

    data_art_tree_t *tree = data_art_create("/tmp/test_nodeops_16to48/art.dat", 32);
    assert(tree != NULL);

    // Create NODE_16
    node_ref_t node_ref = data_art_alloc_node(tree, sizeof(data_art_node16_t));
    if (node_ref_is_null(node_ref)) {
        data_art_destroy(tree);
        FAIL("failed to allocate NODE_16");
    }

    data_art_node16_t n16;
    memset(&n16, 0, sizeof(n16));
    n16.type = DATA_NODE_16;
    n16.num_children = 0;

    if (!data_art_write_node(tree, node_ref, &n16, sizeof(n16))) {
        data_art_destroy(tree);
        FAIL("failed to write NODE_16");
    }

    // Add 17 children (should trigger growth on the 17th)
    uint8_t keys[17];
    for (int i = 0; i < 17; i++) {
        keys[i] = 10 + i * 5;
        node_ref_t leaf = create_dummy_leaf(tree, keys[i]);
        if (node_ref_is_null(leaf)) {
            data_art_destroy(tree);
            FAIL("failed to create leaf");
        }

        node_ref = data_art_add_child(tree, node_ref, keys[i], leaf);
        if (node_ref_is_null(node_ref)) {
            data_art_destroy(tree);
            FAIL("failed to add child");
        }
    }

    // Verify it grew to NODE_48
    const void *node = data_art_load_node(tree, node_ref);
    if (!node || *(const uint8_t *)node != DATA_NODE_48) {
        data_art_destroy(tree);
        FAIL("did not grow to NODE_48");
    }

    const data_art_node48_t *n48 = (const data_art_node48_t *)node;
    if (n48->num_children != 17) {
        data_art_destroy(tree);
        FAIL("incorrect child count");
    }

    // Verify all children are accessible
    for (int i = 0; i < 17; i++) {
        if (!verify_child_exists(tree, node_ref, keys[i], keys[i])) {
            data_art_destroy(tree);
            FAIL("child verification failed after growth");
        }
    }

    data_art_destroy(tree);

    PASS();
    return true;
}

/**
 * Test: Add children to NODE_48 (without growth)
 */
static bool test_node48_add_children(void) {
    TEST("NODE_48: add 48 children");

    system("rm -rf /tmp/test_nodeops_n48add && mkdir -p /tmp/test_nodeops_n48add");

    data_art_tree_t *tree = data_art_create("/tmp/test_nodeops_n48add/art.dat", 32);
    assert(tree != NULL);

    // Create NODE_48
    node_ref_t n48_ref = data_art_alloc_node(tree, sizeof(data_art_node48_t));
    if (node_ref_is_null(n48_ref)) {
        data_art_destroy(tree);
        FAIL("failed to allocate NODE_48");
    }

    data_art_node48_t n48;
    memset(&n48, 0, sizeof(n48));
    n48.type = DATA_NODE_48;
    n48.num_children = 0;
    memset(n48.keys, 255, 256);  // Initialize index array

    if (!data_art_write_node(tree, n48_ref, &n48, sizeof(n48))) {
        data_art_destroy(tree);
        FAIL("failed to write NODE_48");
    }

    // Add 48 children
    uint8_t keys[48];
    for (int i = 0; i < 48; i++) {
        keys[i] = i * 5;
        data_art_reset_arena();
        node_ref_t leaf = create_dummy_leaf(tree, keys[i]);
        if (node_ref_is_null(leaf)) {
            data_art_destroy(tree);
            FAIL("failed to create leaf");
        }

        n48_ref = data_art_add_child(tree, n48_ref, keys[i], leaf);
        if (node_ref_is_null(n48_ref)) {
            data_art_destroy(tree);
            FAIL("failed to add child");
        }
    }

    // Verify all children exist
    for (int i = 0; i < 48; i++) {
        data_art_reset_arena();
        if (!verify_child_exists(tree, n48_ref, keys[i], keys[i])) {
            data_art_destroy(tree);
            FAIL("child verification failed");
        }
    }

    // Verify it's still NODE_48
    const data_art_node48_t *final = (const data_art_node48_t *)data_art_load_node(tree, n48_ref);
    if (!final || final->type != DATA_NODE_48 || final->num_children != 48) {
        data_art_destroy(tree);
        FAIL("node type or count incorrect");
    }

    data_art_destroy(tree);

    PASS();
    return true;
}

/**
 * Test: NODE_48 grows to NODE_256
 */
static bool test_node48_grows_to_node256(void) {
    TEST("NODE_48 → NODE_256: growth on 49th child");

    system("rm -rf /tmp/test_nodeops_48to256 && mkdir -p /tmp/test_nodeops_48to256");

    data_art_tree_t *tree = data_art_create("/tmp/test_nodeops_48to256/art.dat", 32);
    assert(tree != NULL);

    // Create NODE_48
    node_ref_t node_ref = data_art_alloc_node(tree, sizeof(data_art_node48_t));
    if (node_ref_is_null(node_ref)) {
        data_art_destroy(tree);
        FAIL("failed to allocate NODE_48");
    }

    data_art_node48_t n48;
    memset(&n48, 0, sizeof(n48));
    n48.type = DATA_NODE_48;
    n48.num_children = 0;
    memset(n48.keys, 255, 256);

    if (!data_art_write_node(tree, node_ref, &n48, sizeof(n48))) {
        data_art_destroy(tree);
        FAIL("failed to write NODE_48");
    }

    // Add 49 children (should trigger growth on the 49th)
    uint8_t keys[49];
    for (int i = 0; i < 49; i++) {
        keys[i] = i * 5;
        data_art_reset_arena();
        node_ref_t leaf = create_dummy_leaf(tree, keys[i]);
        if (node_ref_is_null(leaf)) {
            data_art_destroy(tree);
            FAIL("failed to create leaf");
        }

        node_ref = data_art_add_child(tree, node_ref, keys[i], leaf);
        if (node_ref_is_null(node_ref)) {
            data_art_destroy(tree);
            FAIL("failed to add child");
        }
    }

    // Reset arena before verification (add_child calls accumulated allocations)
    data_art_reset_arena();

    // Verify it grew to NODE_256
    const void *node = data_art_load_node(tree, node_ref);
    if (!node || *(const uint8_t *)node != DATA_NODE_256) {
        data_art_destroy(tree);
        FAIL("did not grow to NODE_256");
    }

    const data_art_node256_t *n256 = (const data_art_node256_t *)node;
    if (n256->num_children != 49) {
        data_art_destroy(tree);
        FAIL("incorrect child count");
    }

    // Verify all children are accessible
    for (int i = 0; i < 49; i++) {
        data_art_reset_arena();
        if (!verify_child_exists(tree, node_ref, keys[i], keys[i])) {
            data_art_destroy(tree);
            FAIL("child verification failed after growth");
        }
    }

    data_art_destroy(tree);

    PASS();
    return true;
}

/**
 * Test: Add children to NODE_256
 */
static bool test_node256_add_children(void) {
    TEST("NODE_256: add 100 children");

    system("rm -rf /tmp/test_nodeops_n256add && mkdir -p /tmp/test_nodeops_n256add");

    data_art_tree_t *tree = data_art_create("/tmp/test_nodeops_n256add/art.dat", 32);
    assert(tree != NULL);

    // Create NODE_256
    node_ref_t n256_ref = data_art_alloc_node(tree, sizeof(data_art_node256_t));
    if (node_ref_is_null(n256_ref)) {
        data_art_destroy(tree);
        FAIL("failed to allocate NODE_256");
    }

    data_art_node256_t n256;
    memset(&n256, 0, sizeof(n256));
    n256.type = DATA_NODE_256;
    n256.num_children = 0;

    if (!data_art_write_node(tree, n256_ref, &n256, sizeof(n256))) {
        data_art_destroy(tree);
        FAIL("failed to write NODE_256");
    }

    // Add 100 children at various positions
    uint8_t keys[100];
    for (int i = 0; i < 100; i++) {
        keys[i] = i;
        data_art_reset_arena();
        node_ref_t leaf = create_dummy_leaf(tree, keys[i]);
        if (node_ref_is_null(leaf)) {
            data_art_destroy(tree);
            FAIL("failed to create leaf");
        }

        n256_ref = data_art_add_child(tree, n256_ref, keys[i], leaf);
        if (node_ref_is_null(n256_ref)) {
            data_art_destroy(tree);
            FAIL("failed to add child");
        }
    }

    // Verify all children exist
    for (int i = 0; i < 100; i++) {
        data_art_reset_arena();
        if (!verify_child_exists(tree, n256_ref, keys[i], keys[i])) {
            data_art_destroy(tree);
            FAIL("child verification failed");
        }
    }

    // Verify it's still NODE_256
    data_art_reset_arena();
    const data_art_node256_t *final = (const data_art_node256_t *)data_art_load_node(tree, n256_ref);
    if (!final || final->type != DATA_NODE_256 || final->num_children != 100) {
        data_art_destroy(tree);
        FAIL("node type or count incorrect");
    }

    data_art_destroy(tree);

    PASS();
    return true;
}

int main(void) {
    printf("\n");
    printf("========================================\n");
    printf("Node Operations Tests (data_art_node_ops.c)\n");
    printf("========================================\n");
    printf("\n");

    // Run tests
    test_node4_add_children();
    test_node4_grows_to_node16();
    test_node16_add_children();
    test_node16_grows_to_node48();
    test_node48_add_children();
    test_node48_grows_to_node256();
    test_node256_add_children();

    // Summary
    printf("\n");
    printf("========================================\n");
    printf("Test Results: %d/%d passed\n", tests_passed, tests_run);
    printf("========================================\n");

    return (tests_passed == tests_run) ? 0 : 1;
}
