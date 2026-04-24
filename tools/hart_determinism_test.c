/**
 * hart_determinism_test — validate that hart / storage_hart produce the
 * same DFS hash stream given the same key set, regardless of insertion
 * order.
 *
 * The snapshot-hash-persistence work rests on: save_state emits hashes in
 * DFS order, load_state reconstructs the trie and walks DFS in the same
 * order. That's safe if (and only if) the hart's topology and cached
 * hashes at each DFS position depend solely on the key set, not on the
 * specific sequence of inserts. This test hammers that property with
 * several distributions, key sizes, and permutations.
 *
 * Usage:   ./hart_determinism_test [seed]
 * Exit 0 on all-pass, non-zero on any failure.
 */

#include "hashed_art.h"
#include "storage_hart2.h"
#include "hart_pool.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * xoshiro256** PRNG — reproducible with a fixed seed
 * ========================================================================= */

static uint64_t g_rng[4];

static inline uint64_t rotl64(uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

static uint64_t rnd64(void) {
    const uint64_t r = rotl64(g_rng[1] * 5, 7) * 9;
    const uint64_t t = g_rng[1] << 17;
    g_rng[2] ^= g_rng[0];
    g_rng[3] ^= g_rng[1];
    g_rng[1] ^= g_rng[2];
    g_rng[0] ^= g_rng[3];
    g_rng[2] ^= t;
    g_rng[3] = rotl64(g_rng[3], 45);
    return r;
}

static void seed_rng(uint64_t seed) {
    g_rng[0] = seed ^ 0x9E3779B97F4A7C15ull;
    g_rng[1] = seed ^ 0xDEADBEEFCAFEBABEull;
    g_rng[2] = seed + 0x1000000;
    g_rng[3] = seed + 0x2000000;
    for (int i = 0; i < 16; i++) (void)rnd64();
}

static void fill_random_key(uint8_t key[32]) {
    for (int i = 0; i < 4; i++) {
        uint64_t r = rnd64();
        memcpy(&key[i * 8], &r, 8);
    }
}

/* =========================================================================
 * Encode callbacks — deterministic byte emission; doesn't have to be real
 * RLP since we're only testing the hart's internal determinism
 * ========================================================================= */

static uint32_t acct_encode(const uint8_t key[32], const void *val,
                             uint8_t *out, void *ctx) {
    (void)ctx;
    memcpy(out, key, 32);
    memcpy(out + 32, val, 4);
    return 36;
}

static uint32_t stor_encode(const uint8_t key[32], const void *val,
                             uint8_t *out, void *ctx) {
    (void)ctx;
    memcpy(out, key, 32);
    memcpy(out + 32, val, 32);
    return 64;
}

/* =========================================================================
 * Visit log — collects (depth, is_leaf, hash) tuples during DFS walk
 * ========================================================================= */

typedef struct {
    int      depth;
    bool     is_leaf;
    uint8_t  hash[32];   /* zeroed for leaves */
} visit_t;

typedef struct {
    visit_t *arr;
    size_t   count;
    size_t   cap;
} visit_log_t;

static void visit_log_init(visit_log_t *v) {
    v->arr = NULL; v->count = 0; v->cap = 0;
}

static void visit_log_destroy(visit_log_t *v) {
    free(v->arr); v->arr = NULL; v->count = v->cap = 0;
}

static void visit_log_push(visit_log_t *v, int depth, bool is_leaf,
                            const uint8_t *h) {
    if (v->count == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 64;
        v->arr = realloc(v->arr, v->cap * sizeof(visit_t));
    }
    v->arr[v->count].depth = depth;
    v->arr[v->count].is_leaf = is_leaf;
    if (h) memcpy(v->arr[v->count].hash, h, 32);
    else memset(v->arr[v->count].hash, 0, 32);
    v->count++;
}

static void hart_cb(hart_ref_t ref, int depth, const uint8_t *hash,
                     bool is_leaf, void *user) {
    (void)ref;
    visit_log_push((visit_log_t *)user, depth, is_leaf, hash);
}

static void sh_cb(uint64_t ref, int depth, const uint8_t *hash,
                   bool is_leaf, void *user) {
    (void)ref;
    visit_log_push((visit_log_t *)user, depth, is_leaf, hash);
}

static bool visit_log_eq(const visit_log_t *a, const visit_log_t *b,
                          const char *name) {
    if (a->count != b->count) {
        fprintf(stderr, "  [%s] visit-count mismatch: %zu vs %zu\n",
                name, a->count, b->count);
        return false;
    }
    for (size_t i = 0; i < a->count; i++) {
        if (a->arr[i].depth != b->arr[i].depth
         || a->arr[i].is_leaf != b->arr[i].is_leaf
         || memcmp(a->arr[i].hash, b->arr[i].hash, 32) != 0) {
            fprintf(stderr, "  [%s] mismatch at visit %zu: "
                    "depth %d/%d is_leaf %d/%d\n", name, i,
                    a->arr[i].depth, b->arr[i].depth,
                    (int)a->arr[i].is_leaf, (int)b->arr[i].is_leaf);
            return false;
        }
    }
    return true;
}

/* =========================================================================
 * Key generators
 * ========================================================================= */

typedef struct {
    uint8_t  key[32];
    uint32_t value;
} kv_t;

static void gen_random(kv_t *arr, size_t n) {
    for (size_t i = 0; i < n; i++) {
        fill_random_key(arr[i].key);
        arr[i].value = (uint32_t)i;
    }
}

static void gen_shared_prefix(kv_t *arr, size_t n) {
    /* All keys share a 16-byte prefix — stresses path compression. */
    for (size_t i = 0; i < n; i++) {
        memset(arr[i].key, 0xAA, 16);
        for (int b = 16; b < 32; b++)
            arr[i].key[b] = (uint8_t)(rnd64() & 0xFF);
        arr[i].value = (uint32_t)i;
    }
}

static void gen_wide_branch(kv_t *arr, size_t n) {
    /* All 256 byte values at position 16 — forces the node at that depth
     * through NODE_4 → NODE_16 → NODE_48 → NODE_256 transitions. */
    for (size_t i = 0; i < n; i++) {
        memset(arr[i].key, 0x11, 16);
        arr[i].key[16] = (uint8_t)(i & 0xFF);
        uint64_t r = rnd64();
        memcpy(&arr[i].key[17], &r, 8);
        r = rnd64();
        memcpy(&arr[i].key[25], &r, 7);
        arr[i].value = (uint32_t)i;
    }
}

static void shuffle_kv(kv_t *arr, size_t n) {
    for (size_t i = n; i-- > 1;) {
        size_t j = rnd64() % (i + 1);
        kv_t tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp;
    }
}

/* =========================================================================
 * Test — same insertion order, two separate harts, expect identical
 * ========================================================================= */

static bool test_same_order(const char *name, size_t n,
                             void (*gen)(kv_t *, size_t)) {
    kv_t *keys = malloc(n * sizeof(kv_t));
    gen(keys, n);

    hart_t a, b;
    hart_init(&a, 4);
    hart_init(&b, 4);
    for (size_t i = 0; i < n; i++) {
        hart_insert(&a, keys[i].key, &keys[i].value);
        hart_insert(&b, keys[i].key, &keys[i].value);
    }

    uint8_t ra[32], rb[32];
    hart_root_hash(&a, acct_encode, NULL, ra);
    hart_root_hash(&b, acct_encode, NULL, rb);

    visit_log_t va, vb;
    visit_log_init(&va); visit_log_init(&vb);
    hart_walk_dfs(&a, hart_cb, &va);
    hart_walk_dfs(&b, hart_cb, &vb);

    bool ok = true;
    if (memcmp(ra, rb, 32) != 0) {
        fprintf(stderr, "  [%s] root hash mismatch\n", name);
        ok = false;
    }
    if (!visit_log_eq(&va, &vb, name)) ok = false;

    if (ok)
        printf("  %-32s PASS  n=%-7zu  visits=%zu\n", name, n, va.count);
    else
        fprintf(stderr, "  %-32s FAIL\n", name);

    hart_destroy(&a); hart_destroy(&b);
    visit_log_destroy(&va); visit_log_destroy(&vb);
    free(keys);
    return ok;
}

/* =========================================================================
 * Test — same key set inserted in DIFFERENT orders; expect identical
 *         (the strongest claim: tree shape depends only on key set)
 * ========================================================================= */

static bool test_permuted(const char *name, size_t n, int perms) {
    kv_t *keys = malloc(n * sizeof(kv_t));
    gen_random(keys, n);

    /* Baseline: original order */
    hart_t baseline; hart_init(&baseline, 4);
    for (size_t i = 0; i < n; i++)
        hart_insert(&baseline, keys[i].key, &keys[i].value);
    uint8_t root0[32];
    hart_root_hash(&baseline, acct_encode, NULL, root0);
    visit_log_t vbase;
    visit_log_init(&vbase);
    hart_walk_dfs(&baseline, hart_cb, &vbase);

    bool ok = true;
    for (int p = 0; p < perms; p++) {
        shuffle_kv(keys, n);

        hart_t h; hart_init(&h, 4);
        for (size_t i = 0; i < n; i++)
            hart_insert(&h, keys[i].key, &keys[i].value);

        uint8_t r[32];
        hart_root_hash(&h, acct_encode, NULL, r);

        visit_log_t v;
        visit_log_init(&v);
        hart_walk_dfs(&h, hart_cb, &v);

        char tag[80]; snprintf(tag, sizeof(tag), "%s perm %d", name, p);
        if (memcmp(root0, r, 32) != 0) {
            fprintf(stderr, "  [%s] root mismatch\n", tag);
            ok = false;
        }
        if (!visit_log_eq(&vbase, &v, tag)) ok = false;

        hart_destroy(&h);
        visit_log_destroy(&v);
    }

    if (ok)
        printf("  %-32s PASS  n=%-7zu  perms=%d  visits=%zu\n",
               name, n, perms, vbase.count);
    else
        fprintf(stderr, "  %-32s FAIL\n", name);

    hart_destroy(&baseline);
    visit_log_destroy(&vbase);
    free(keys);
    return ok;
}

/* =========================================================================
 * Test — insert/delete/reinsert converges to same structure as the
 *         plain "insert all" case
 * ========================================================================= */

static bool test_churn(size_t n) {
    kv_t *keys = malloc(n * sizeof(kv_t));
    gen_random(keys, n);

    hart_t base; hart_init(&base, 4);
    for (size_t i = 0; i < n; i++)
        hart_insert(&base, keys[i].key, &keys[i].value);
    uint8_t rbase[32];
    hart_root_hash(&base, acct_encode, NULL, rbase);
    visit_log_t vbase;
    visit_log_init(&vbase);
    hart_walk_dfs(&base, hart_cb, &vbase);

    /* Churn run: insert all, delete 25 %, reinsert the deleted */
    hart_t churn; hart_init(&churn, 4);
    for (size_t i = 0; i < n; i++)
        hart_insert(&churn, keys[i].key, &keys[i].value);
    size_t q = n / 4;
    for (size_t i = 0; i < q; i++) hart_delete(&churn, keys[i].key);
    for (size_t i = 0; i < q; i++)
        hart_insert(&churn, keys[i].key, &keys[i].value);

    uint8_t rchurn[32];
    hart_root_hash(&churn, acct_encode, NULL, rchurn);
    visit_log_t vchurn;
    visit_log_init(&vchurn);
    hart_walk_dfs(&churn, hart_cb, &vchurn);

    bool ok = true;
    if (memcmp(rbase, rchurn, 32) != 0) {
        fprintf(stderr, "  [churn] root mismatch\n");
        ok = false;
    }
    if (!visit_log_eq(&vbase, &vchurn, "churn")) ok = false;

    if (ok)
        printf("  %-32s PASS  n=%-7zu  churned=%zu  visits=%zu\n",
               "churn (delete+reinsert)", n, q, vbase.count);
    else
        fprintf(stderr, "  %-32s FAIL\n", "churn (delete+reinsert)");

    hart_destroy(&base); hart_destroy(&churn);
    visit_log_destroy(&vbase); visit_log_destroy(&vchurn);
    free(keys);
    return ok;
}

/* =========================================================================
 * Test — empty hart
 * ========================================================================= */

static bool test_empty(void) {
    hart_t a; hart_init(&a, 4);
    uint8_t r[32];
    hart_root_hash(&a, acct_encode, NULL, r);
    visit_log_t v;
    visit_log_init(&v);
    hart_walk_dfs(&a, hart_cb, &v);

    bool ok = (v.count == 0);
    if (ok)
        printf("  %-32s PASS  (0 visits)\n", "empty hart");
    else
        fprintf(stderr, "  %-32s FAIL — got %zu visits\n",
                "empty hart", v.count);

    visit_log_destroy(&v);
    hart_destroy(&a);
    return ok;
}

/* =========================================================================
 * Storage-hart tests — same pattern against the pool-backed variant
 * ========================================================================= */

typedef struct {
    uint8_t key[32];
    uint8_t val[32];
} skv_t;

static void gen_skv(skv_t *arr, size_t n) {
    for (size_t i = 0; i < n; i++) {
        fill_random_key(arr[i].key);
        fill_random_key(arr[i].val);
    }
}

static void shuffle_skv(skv_t *arr, size_t n) {
    for (size_t i = n; i-- > 1;) {
        size_t j = rnd64() % (i + 1);
        skv_t tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp;
    }
}

static bool test_storage_permuted(const char *name, size_t n, int perms) {
    skv_t *keys = malloc(n * sizeof(skv_t));
    gen_skv(keys, n);

    hart_pool_t *pool = hart_pool_create();
    if (!pool) { fprintf(stderr, "  [%s] pool create failed\n", name); free(keys); return false; }

    storage_hart_t base = STORAGE_HART_INIT;
    for (size_t i = 0; i < n; i++)
        storage_hart_put(pool, &base, keys[i].key, keys[i].val);

    uint8_t root0[32];
    storage_hart_root_hash(pool, &base, stor_encode, NULL, root0);
    visit_log_t vbase;
    visit_log_init(&vbase);
    storage_hart_walk_dfs(pool, &base, sh_cb, &vbase);

    bool ok = true;
    for (int p = 0; p < perms; p++) {
        shuffle_skv(keys, n);

        storage_hart_t h = STORAGE_HART_INIT;
        for (size_t i = 0; i < n; i++)
            storage_hart_put(pool, &h, keys[i].key, keys[i].val);

        uint8_t r[32];
        storage_hart_root_hash(pool, &h, stor_encode, NULL, r);

        visit_log_t v;
        visit_log_init(&v);
        storage_hart_walk_dfs(pool, &h, sh_cb, &v);

        char tag[80]; snprintf(tag, sizeof(tag), "%s perm %d", name, p);
        if (memcmp(root0, r, 32) != 0) {
            fprintf(stderr, "  [%s] root mismatch\n", tag);
            ok = false;
        }
        if (!visit_log_eq(&vbase, &v, tag)) ok = false;

        storage_hart_clear(pool, &h);
        visit_log_destroy(&v);
    }

    if (ok)
        printf("  %-32s PASS  n=%-7zu  perms=%d  visits=%zu\n",
               name, n, perms, vbase.count);
    else
        fprintf(stderr, "  %-32s FAIL\n", name);

    storage_hart_clear(pool, &base);
    visit_log_destroy(&vbase);
    hart_pool_destroy(pool);
    free(keys);
    return ok;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    uint64_t seed = 0xBEEF;
    if (argc > 1) seed = (uint64_t)strtoull(argv[1], NULL, 0);
    printf("hart_determinism_test  seed=0x%" PRIx64 "\n", seed);
    seed_rng(seed);

    bool all = true;

    printf("\n--- same-order determinism ---\n");
    all &= test_empty();
    all &= test_same_order("single key",            1,       gen_random);
    all &= test_same_order("small random",          100,     gen_random);
    all &= test_same_order("medium random",         10000,   gen_random);
    all &= test_same_order("large random",          100000,  gen_random);
    all &= test_same_order("shared 16B prefix",     10000,   gen_shared_prefix);
    all &= test_same_order("wide branch (4→16→48→256)", 1000, gen_wide_branch);

    printf("\n--- key-set determinism (permuted inserts) ---\n");
    all &= test_permuted("small permuted",          100,    5);
    all &= test_permuted("medium permuted",         10000,  3);
    all &= test_permuted("large permuted",          100000, 2);

    printf("\n--- delete/reinsert convergence ---\n");
    all &= test_churn(5000);
    all &= test_churn(50000);

    printf("\n--- storage_hart (pool-backed) ---\n");
    all &= test_storage_permuted("storage small permuted",  100,    5);
    all &= test_storage_permuted("storage medium permuted", 10000,  3);
    all &= test_storage_permuted("storage large permuted",  100000, 2);

    printf("\n%s\n", all ? "ALL TESTS PASSED" : "*** FAILURES ***");
    return all ? 0 : 1;
}
