/**
 * Determinism Test: Verify ART structure is independent of insertion order.
 *
 * Inserts the same set of keys into two separate trees in different orders,
 * then iterates both and verifies they produce identical key sequences.
 */

#include "data_art.h"
#include "page_manager.h"
#include "buffer_pool.h"
#include "wal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>

// ============================================================================
// Test Framework
// ============================================================================

#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_RESET  "\033[0m"

static int tests_passed = 0;

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, COLOR_RED "  FAILED: %s\n" COLOR_RESET, msg); \
        fprintf(stderr, "    at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
} while(0)

// ============================================================================
// Test Environment
// ============================================================================

#define KEY_SIZE 32

typedef struct {
    page_manager_t  *pm;
    buffer_pool_t   *bp;
    wal_t           *wal;
    data_art_tree_t *tree;
    char db_path[256];
    char wal_path[256];
} test_env_t;

static void cleanup_paths(const char *db_path, const char *wal_path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s %s", db_path, wal_path);
    system(cmd);
    sync();
    usleep(10000);
}

static void open_env(test_env_t *env) {
    env->pm = page_manager_create(env->db_path, false);
    ASSERT(env->pm != NULL, "page_manager_create");

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 1024;
    env->bp = buffer_pool_create(&bp_config, env->pm);
    ASSERT(env->bp != NULL, "buffer_pool_create");

    wal_config_t wal_config = wal_default_config();
    wal_config.segment_size = 8 * 1024 * 1024;
    env->wal = wal_open(env->wal_path, &wal_config);
    ASSERT(env->wal != NULL, "wal_open");

    env->tree = data_art_create(env->pm, env->bp, env->wal, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_create");
}

static void close_env(test_env_t *env) {
    if (env->tree) { data_art_destroy(env->tree); env->tree = NULL; }
    if (env->wal)  { wal_close(env->wal);         env->wal = NULL; }
    if (env->bp)   { buffer_pool_destroy(env->bp); env->bp = NULL; }
    if (env->pm)   { page_manager_destroy(env->pm); env->pm = NULL; }
}

// Generate a deterministic key from index (same as iterator test)
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

// Fisher-Yates shuffle
static void shuffle(int *arr, int n, uint32_t seed) {
    for (int i = n - 1; i > 0; i--) {
        seed = seed * 1103515245 + 12345;
        int j = (int)((seed >> 16) % (uint32_t)(i + 1));
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}

// ============================================================================
// Test: Same keys, different insertion orders → identical iteration
// ============================================================================

static void test_determinism(void) {
    printf("\n[Test] Determinism: same keys in different order → same tree\n");

    const int N = 500;
    const char *value = "determinism_test_value!!";
    size_t val_len = strlen(value);

    // Build insertion orders
    int *order_a = malloc(N * sizeof(int));
    int *order_b = malloc(N * sizeof(int));
    int *order_c = malloc(N * sizeof(int));
    ASSERT(order_a && order_b && order_c, "malloc orders");

    for (int i = 0; i < N; i++) {
        order_a[i] = i;  // sequential
        order_b[i] = i;
        order_c[i] = i;
    }
    shuffle(order_b, N, 0xDEADBEEF);  // random shuffle #1
    shuffle(order_c, N, 0xCAFEBABE);  // random shuffle #2

    int *orders[] = { order_a, order_b, order_c };
    const char *labels[] = { "sequential", "shuffle_1", "shuffle_2" };

    // Collect iterated keys from each tree
    uint8_t **collected_keys[3] = {0};
    int collected_counts[3] = {0};

    for (int t = 0; t < 3; t++) {
        test_env_t env = {0};
        char suffix[64];
        snprintf(suffix, sizeof(suffix), "det_%s", labels[t]);
        snprintf(env.db_path, sizeof(env.db_path), "/tmp/test_det_db_%s", labels[t]);
        snprintf(env.wal_path, sizeof(env.wal_path), "/tmp/test_det_wal_%s", labels[t]);
        cleanup_paths(env.db_path, env.wal_path);
        open_env(&env);

        // Insert in this order
        for (int i = 0; i < N; i++) {
            uint8_t key[KEY_SIZE];
            generate_key(key, orders[t][i]);
            bool ok = data_art_insert(env.tree, key, KEY_SIZE, value, val_len);
            ASSERT(ok, "insert failed");
        }

        // Iterate and collect
        collected_keys[t] = malloc(N * sizeof(uint8_t *));
        ASSERT(collected_keys[t], "malloc collected");

        data_art_iterator_t *iter = data_art_iterator_create(env.tree);
        ASSERT(iter != NULL, "iterator_create");

        int count = 0;
        while (data_art_iterator_next(iter)) {
            size_t klen;
            const uint8_t *k = data_art_iterator_key(iter, &klen);
            ASSERT(k != NULL, "iterator_key null");
            ASSERT(klen == KEY_SIZE, "key length mismatch");

            collected_keys[t][count] = malloc(KEY_SIZE);
            memcpy(collected_keys[t][count], k, KEY_SIZE);
            count++;
        }
        data_art_iterator_destroy(iter);

        collected_counts[t] = count;
        printf("  Tree %d (%s): iterated %d keys\n", t, labels[t], count);

        close_env(&env);
        cleanup_paths(env.db_path, env.wal_path);
    }

    // Verify all three trees produced identical iteration
    ASSERT(collected_counts[0] == N, "tree 0 count mismatch");
    ASSERT(collected_counts[1] == N, "tree 1 count mismatch");
    ASSERT(collected_counts[2] == N, "tree 2 count mismatch");

    for (int i = 0; i < N; i++) {
        if (memcmp(collected_keys[0][i], collected_keys[1][i], KEY_SIZE) != 0) {
            fprintf(stderr, COLOR_RED "  Key %d differs between sequential and shuffle_1\n" COLOR_RESET, i);
            exit(1);
        }
        if (memcmp(collected_keys[0][i], collected_keys[2][i], KEY_SIZE) != 0) {
            fprintf(stderr, COLOR_RED "  Key %d differs between sequential and shuffle_2\n" COLOR_RESET, i);
            exit(1);
        }
    }

    // Verify sorted order
    for (int i = 1; i < N; i++) {
        int cmp = memcmp(collected_keys[0][i - 1], collected_keys[0][i], KEY_SIZE);
        ASSERT(cmp < 0, "keys not in sorted order");
    }

    // Cleanup
    for (int t = 0; t < 3; t++) {
        for (int i = 0; i < N; i++) free(collected_keys[t][i]);
        free(collected_keys[t]);
    }
    free(order_a);
    free(order_b);
    free(order_c);

    printf(COLOR_GREEN "  PASSED" COLOR_RESET "\n");
    tests_passed++;
}

// ============================================================================
// Test: Determinism with deletes — insert same keys, delete same subset,
//       different order → identical remaining keys
// ============================================================================

static void test_determinism_with_deletes(void) {
    printf("\n[Test] Determinism with deletes: same ops, different order → same tree\n");

    const int N = 300;
    const int DELETE_COUNT = 100;  // delete keys 0..99
    const char *value = "det_del_value";
    size_t val_len = strlen(value);

    // Two insertion orders
    int *order_a = malloc(N * sizeof(int));
    int *order_b = malloc(N * sizeof(int));
    ASSERT(order_a && order_b, "malloc orders");

    for (int i = 0; i < N; i++) {
        order_a[i] = i;
        order_b[i] = i;
    }
    shuffle(order_b, N, 0x12345678);

    // Two deletion orders
    int *del_order_a = malloc(DELETE_COUNT * sizeof(int));
    int *del_order_b = malloc(DELETE_COUNT * sizeof(int));
    ASSERT(del_order_a && del_order_b, "malloc del orders");

    for (int i = 0; i < DELETE_COUNT; i++) {
        del_order_a[i] = i;
        del_order_b[i] = i;
    }
    shuffle(del_order_b, DELETE_COUNT, 0xABCDEF01);

    int *ins_orders[] = { order_a, order_b };
    int *del_orders[] = { del_order_a, del_order_b };
    const char *labels[] = { "seq_ins_seq_del", "shuf_ins_shuf_del" };

    uint8_t **collected_keys[2] = {0};
    int collected_counts[2] = {0};

    for (int t = 0; t < 2; t++) {
        test_env_t env = {0};
        snprintf(env.db_path, sizeof(env.db_path), "/tmp/test_det_del_db_%s", labels[t]);
        snprintf(env.wal_path, sizeof(env.wal_path), "/tmp/test_det_del_wal_%s", labels[t]);
        cleanup_paths(env.db_path, env.wal_path);
        open_env(&env);

        // Insert all N keys
        for (int i = 0; i < N; i++) {
            uint8_t key[KEY_SIZE];
            generate_key(key, ins_orders[t][i]);
            bool ok = data_art_insert(env.tree, key, KEY_SIZE, value, val_len);
            ASSERT(ok, "insert failed");
        }

        // Delete first DELETE_COUNT keys (by index 0..99)
        for (int i = 0; i < DELETE_COUNT; i++) {
            uint8_t key[KEY_SIZE];
            generate_key(key, del_orders[t][i]);
            bool ok = data_art_delete(env.tree, key, KEY_SIZE);
            ASSERT(ok, "delete failed");
        }

        // Iterate remaining
        int expect = N - DELETE_COUNT;
        collected_keys[t] = malloc(expect * sizeof(uint8_t *));
        ASSERT(collected_keys[t], "malloc collected");

        data_art_iterator_t *iter = data_art_iterator_create(env.tree);
        ASSERT(iter != NULL, "iterator_create");

        int count = 0;
        while (data_art_iterator_next(iter)) {
            size_t klen;
            const uint8_t *k = data_art_iterator_key(iter, &klen);
            ASSERT(k != NULL, "key null");
            ASSERT(klen == KEY_SIZE, "key length");
            ASSERT(count < expect, "too many keys iterated");

            collected_keys[t][count] = malloc(KEY_SIZE);
            memcpy(collected_keys[t][count], k, KEY_SIZE);
            count++;
        }
        data_art_iterator_destroy(iter);

        collected_counts[t] = count;
        printf("  Tree %d (%s): iterated %d keys (expected %d)\n",
               t, labels[t], count, expect);

        close_env(&env);
        cleanup_paths(env.db_path, env.wal_path);
    }

    // Verify identical
    int expect = N - DELETE_COUNT;
    ASSERT(collected_counts[0] == expect, "tree 0 count");
    ASSERT(collected_counts[1] == expect, "tree 1 count");

    for (int i = 0; i < expect; i++) {
        if (memcmp(collected_keys[0][i], collected_keys[1][i], KEY_SIZE) != 0) {
            fprintf(stderr, COLOR_RED "  Key %d differs between trees after deletes\n" COLOR_RESET, i);
            exit(1);
        }
    }

    // Cleanup
    for (int t = 0; t < 2; t++) {
        for (int i = 0; i < expect; i++) free(collected_keys[t][i]);
        free(collected_keys[t]);
    }
    free(order_a);
    free(order_b);
    free(del_order_a);
    free(del_order_b);

    printf(COLOR_GREEN "  PASSED" COLOR_RESET "\n");
    tests_passed++;
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== ART Determinism Tests ===\n");

    test_determinism();
    test_determinism_with_deletes();

    printf("\n=== Results: %d/2 passed ===\n", tests_passed);
    return (tests_passed == 2) ? 0 : 1;
}
