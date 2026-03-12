/*
 * MPT Store Refcount Test — validates shared-mode reference counting.
 *
 * Exercises:
 *   1. Two tries sharing a subtree → refcount=2 for shared nodes
 *   2. Delete one reference → shared nodes survive (refcount=1)
 *   3. Delete second reference → shared nodes freed (refcount=0)
 *   4. Verify node_count and free_bytes track correctly
 *   5. Verify data integrity (get returns correct values throughout)
 */

#include "mpt_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define STORE_PATH "/tmp/test_mpt_refcount"

static void cleanup(void) {
    unlink(STORE_PATH ".idx");
    unlink(STORE_PATH ".dat");
    unlink(STORE_PATH ".free");
}

static void make_key(uint8_t out[32], uint8_t prefix) {
    memset(out, 0, 32);
    out[0] = prefix;
}

static void make_value(uint8_t *out, size_t *len, uint8_t id) {
    /* Simple RLP: short string 0x82 + 2 bytes */
    out[0] = 0x82;
    out[1] = id;
    out[2] = id;
    *len = 3;
}

static int failures = 0;

#define CHECK(cond, msg, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: " msg "\n", ##__VA_ARGS__); \
        failures++; \
    } \
} while (0)

/*
 * Test 1: Two tries that share a common key.
 *
 * Trie A: keys {0x01, 0x02, 0x03}
 * Trie B: keys {0x01, 0x04, 0x05}  (0x01 is shared)
 *
 * After building both tries, the node for key 0x01's leaf and any
 * shared internal nodes should have refcount > 1.
 *
 * Then: update trie A to remove key 0x01. The shared nodes should
 * survive (trie B still references them). Trie B should still
 * return the correct value for key 0x01.
 */
static void test_shared_subtree(void) {
    printf("Test: shared subtree refcount...\n");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 1024);
    CHECK(ms != NULL, "create store");
    if (!ms) return;

    mpt_store_set_shared(ms, true);
    mpt_store_set_cache(ms, 0); /* disable cache to force disk I/O */

    uint8_t key[32], val[8];
    size_t vlen;
    uint8_t root_a[32], root_b[32];

    /* Build trie A: keys 0x01, 0x02, 0x03 */
    mpt_store_begin_batch(ms);
    make_key(key, 0x01); make_value(val, &vlen, 0xAA);
    mpt_store_update(ms, key, val, vlen);
    make_key(key, 0x02); make_value(val, &vlen, 0xBB);
    mpt_store_update(ms, key, val, vlen);
    make_key(key, 0x03); make_value(val, &vlen, 0xCC);
    mpt_store_update(ms, key, val, vlen);
    mpt_store_commit_batch(ms);
    mpt_store_root(ms, root_a);
    mpt_store_flush(ms);

    mpt_store_stats_t stats_after_a = mpt_store_stats(ms);
    printf("  After trie A: %llu nodes, %llu free bytes\n",
           (unsigned long long)stats_after_a.node_count,
           (unsigned long long)stats_after_a.free_bytes);

    /* Build trie B: keys 0x01, 0x04, 0x05 (reuses 0x01 leaf from trie A) */
    uint8_t empty_root[32];
    memset(empty_root, 0, 32);
    /* Start from empty root for trie B */
    uint8_t empty_trie[32] = {
        0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
        0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
        0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
        0x01, 0x62, 0x02, 0xd5, 0x46, 0x3c, 0x60, 0x9d
    };
    mpt_store_set_root(ms, empty_trie);
    mpt_store_begin_batch(ms);
    make_key(key, 0x01); make_value(val, &vlen, 0xAA); /* same key+value as trie A */
    mpt_store_update(ms, key, val, vlen);
    make_key(key, 0x04); make_value(val, &vlen, 0xDD);
    mpt_store_update(ms, key, val, vlen);
    make_key(key, 0x05); make_value(val, &vlen, 0xEE);
    mpt_store_update(ms, key, val, vlen);
    mpt_store_commit_batch(ms);
    mpt_store_root(ms, root_b);
    mpt_store_flush(ms);

    mpt_store_stats_t stats_after_b = mpt_store_stats(ms);
    printf("  After trie B: %llu nodes, %llu free bytes\n",
           (unsigned long long)stats_after_b.node_count,
           (unsigned long long)stats_after_b.free_bytes);

    /* Trie B should have fewer new nodes than trie A (shared nodes reused) */
    /* Both tries should be readable */
    uint8_t got[64];
    uint32_t got_len;

    /* Verify trie A */
    mpt_store_set_root(ms, root_a);
    make_key(key, 0x01);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie A key 0x01 exists (got %u)", got_len);
    CHECK(got_len >= 2 && got[1] == 0xAA, "trie A key 0x01 value correct");

    make_key(key, 0x02);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie A key 0x02 exists");

    /* Verify trie B */
    mpt_store_set_root(ms, root_b);
    make_key(key, 0x01);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie B key 0x01 exists (got %u)", got_len);
    CHECK(got_len >= 2 && got[1] == 0xAA, "trie B key 0x01 value correct");

    make_key(key, 0x04);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie B key 0x04 exists");

    /* Now delete key 0x01 from trie A */
    mpt_store_set_root(ms, root_a);
    mpt_store_begin_batch(ms);
    make_key(key, 0x01);
    mpt_store_delete(ms, key);
    mpt_store_commit_batch(ms);
    mpt_store_root(ms, root_a); /* updated root */
    mpt_store_flush(ms);

    mpt_store_stats_t stats_after_del = mpt_store_stats(ms);
    printf("  After delete from A: %llu nodes, %llu free bytes\n",
           (unsigned long long)stats_after_del.node_count,
           (unsigned long long)stats_after_del.free_bytes);

    /* Trie A: key 0x01 should be gone */
    make_key(key, 0x01);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 0, "trie A key 0x01 deleted (got %u)", got_len);

    /* Trie A: key 0x02 should still exist */
    make_key(key, 0x02);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie A key 0x02 still exists");

    /* Trie B: key 0x01 should STILL exist (shared node survived) */
    mpt_store_set_root(ms, root_b);
    make_key(key, 0x01);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie B key 0x01 survives after A delete (got %u)", got_len);
    if (got_len >= 2)
        CHECK(got[1] == 0xAA, "trie B key 0x01 value still correct (got 0x%02x)", got[1]);

    make_key(key, 0x04);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie B key 0x04 still exists");

    make_key(key, 0x05);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie B key 0x05 still exists");

    /* Now delete key 0x01 from trie B too */
    mpt_store_begin_batch(ms);
    make_key(key, 0x01);
    mpt_store_delete(ms, key);
    mpt_store_commit_batch(ms);
    mpt_store_root(ms, root_b);
    mpt_store_flush(ms);

    mpt_store_stats_t stats_final = mpt_store_stats(ms);
    printf("  After delete from B: %llu nodes, %llu free bytes\n",
           (unsigned long long)stats_final.node_count,
           (unsigned long long)stats_final.free_bytes);

    /* Shared nodes should now be freed — free_bytes should increase */
    CHECK(stats_final.free_bytes > stats_after_del.free_bytes,
          "free bytes increased after last reference removed (%llu > %llu)",
          (unsigned long long)stats_final.free_bytes,
          (unsigned long long)stats_after_del.free_bytes);

    /* Trie B: key 0x01 should now be gone */
    make_key(key, 0x01);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 0, "trie B key 0x01 now deleted (got %u)", got_len);

    /* Trie B: other keys still exist */
    make_key(key, 0x04);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "trie B key 0x04 still exists after 0x01 delete");

    mpt_store_destroy(ms);
    cleanup();
    printf("  Done.\n");
}

/*
 * Test 2: Repeated updates to same key across multiple tries.
 *
 * Simulates what happens in real chain execution: the same storage
 * slot is updated across blocks, each producing a different trie root.
 * Old roots still reference old leaf nodes.
 */
static void test_repeated_updates(void) {
    printf("Test: repeated updates with root tracking...\n");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 1024);
    CHECK(ms != NULL, "create store");
    if (!ms) return;

    mpt_store_set_shared(ms, true);
    mpt_store_set_cache(ms, 0);

    uint8_t key[32], val[8];
    size_t vlen;
    uint8_t roots[5][32];

    /* Build 5 independent tries, each with key 0x01 (different values)
     * and a unique key per version. Each starts from empty root. */
    uint8_t empty_trie[32] = {
        0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
        0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
        0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
        0x01, 0x62, 0x02, 0xd5, 0x46, 0x3c, 0x60, 0x9d
    };

    for (int i = 0; i < 5; i++) {
        mpt_store_set_root(ms, empty_trie);
        mpt_store_begin_batch(ms);
        make_key(key, 0x01);
        make_value(val, &vlen, (uint8_t)(0x10 + i));
        mpt_store_update(ms, key, val, vlen);
        /* Also add a unique key per version to keep tries distinct */
        make_key(key, (uint8_t)(0x10 + i));
        make_value(val, &vlen, (uint8_t)(0xF0 + i));
        mpt_store_update(ms, key, val, vlen);
        mpt_store_commit_batch(ms);
        mpt_store_root(ms, roots[i]);
        mpt_store_flush(ms);
    }

    mpt_store_stats_t stats_all = mpt_store_stats(ms);
    printf("  After 5 versions: %llu nodes\n",
           (unsigned long long)stats_all.node_count);

    /* Verify all 5 roots return correct values */
    uint8_t got[64];
    uint32_t got_len;
    for (int i = 0; i < 5; i++) {
        mpt_store_set_root(ms, roots[i]);
        make_key(key, 0x01);
        got_len = mpt_store_get(ms, key, got, sizeof(got));
        CHECK(got_len == 3 && got[1] == (uint8_t)(0x10 + i),
              "version %d key 0x01 value=0x%02x (expected 0x%02x)",
              i, got_len >= 2 ? got[1] : 0, 0x10 + i);
    }

    /* Now "forget" old roots by overwriting them — simulate keeping only
     * the latest root and deleting old versions' unique keys */
    for (int i = 0; i < 4; i++) {
        mpt_store_set_root(ms, roots[i]);
        mpt_store_begin_batch(ms);
        /* Delete unique key for this version */
        make_key(key, (uint8_t)(0x10 + i));
        mpt_store_delete(ms, key);
        /* Delete key 0x01 from old version */
        make_key(key, 0x01);
        mpt_store_delete(ms, key);
        mpt_store_commit_batch(ms);
        mpt_store_flush(ms);
    }

    mpt_store_stats_t stats_pruned = mpt_store_stats(ms);
    printf("  After pruning 4 old roots: %llu nodes, %llu free bytes\n",
           (unsigned long long)stats_pruned.node_count,
           (unsigned long long)stats_pruned.free_bytes);

    /* Latest root should still work perfectly */
    mpt_store_set_root(ms, roots[4]);
    make_key(key, 0x01);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3 && got[1] == 0x14,
          "latest version key 0x01 survives pruning (got 0x%02x)",
          got_len >= 2 ? got[1] : 0);

    make_key(key, 0x14);
    got_len = mpt_store_get(ms, key, got, sizeof(got));
    CHECK(got_len == 3, "latest version unique key survives");

    /* Some nodes should have been freed */
    CHECK(stats_pruned.free_bytes > 0,
          "free bytes > 0 after pruning (%llu)",
          (unsigned long long)stats_pruned.free_bytes);

    mpt_store_destroy(ms);
    cleanup();
    printf("  Done.\n");
}

/*
 * Test 3: Persistence — close and reopen, verify refcount survives.
 */
static void test_persistence(void) {
    printf("Test: refcount persistence across close/reopen...\n");
    cleanup();

    uint8_t key[32], val[8];
    size_t vlen;
    uint8_t root_a[32], root_b[32];

    uint8_t empty_trie[32] = {
        0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
        0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
        0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
        0x01, 0x62, 0x02, 0xd5, 0x46, 0x3c, 0x60, 0x9d
    };

    /* Phase 1: Build two tries sharing key 0x01 */
    {
        mpt_store_t *ms = mpt_store_create(STORE_PATH, 1024);
        CHECK(ms != NULL, "create store");
        if (!ms) return;
        mpt_store_set_shared(ms, true);
        mpt_store_set_cache(ms, 0);

        /* Trie A */
        mpt_store_begin_batch(ms);
        make_key(key, 0x01); make_value(val, &vlen, 0xAA);
        mpt_store_update(ms, key, val, vlen);
        make_key(key, 0x02); make_value(val, &vlen, 0xBB);
        mpt_store_update(ms, key, val, vlen);
        mpt_store_commit_batch(ms);
        mpt_store_root(ms, root_a);
        mpt_store_flush(ms);

        /* Trie B */
        mpt_store_set_root(ms, empty_trie);
        mpt_store_begin_batch(ms);
        make_key(key, 0x01); make_value(val, &vlen, 0xAA); /* shared */
        mpt_store_update(ms, key, val, vlen);
        make_key(key, 0x03); make_value(val, &vlen, 0xCC);
        mpt_store_update(ms, key, val, vlen);
        mpt_store_commit_batch(ms);
        mpt_store_root(ms, root_b);
        mpt_store_flush(ms);

        mpt_store_destroy(ms);
    }

    /* Phase 2: Reopen, delete from trie A, verify trie B intact */
    {
        mpt_store_t *ms = mpt_store_open(STORE_PATH);
        CHECK(ms != NULL, "reopen store");
        if (!ms) return;
        mpt_store_set_shared(ms, true);
        mpt_store_set_cache(ms, 0);

        /* Delete key 0x01 from trie A */
        mpt_store_set_root(ms, root_a);
        mpt_store_begin_batch(ms);
        make_key(key, 0x01);
        mpt_store_delete(ms, key);
        mpt_store_commit_batch(ms);
        mpt_store_flush(ms);

        /* Trie B key 0x01 should still exist (refcount persisted) */
        mpt_store_set_root(ms, root_b);
        make_key(key, 0x01);
        uint8_t got[64];
        uint32_t got_len = mpt_store_get(ms, key, got, sizeof(got));
        CHECK(got_len == 3, "key 0x01 survives reopen + delete from A (got %u)", got_len);
        if (got_len >= 2)
            CHECK(got[1] == 0xAA, "value correct after reopen (got 0x%02x)", got[1]);

        mpt_store_destroy(ms);
    }

    cleanup();
    printf("  Done.\n");
}

int main(void) {
    printf("=== MPT Store Refcount Tests ===\n\n");

    test_shared_subtree();
    test_repeated_updates();
    test_persistence();

    printf("\n=== Results: %d failure(s) ===\n", failures);
    return failures > 0 ? 1 : 0;
}
