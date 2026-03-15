/*
 * MPT Store Stress Test — validates data integrity under LRU cache pressure.
 *
 * Exercises:
 *   1. Tiny cache (64 entries) forcing constant eviction + disk reload
 *   2. Many batches with mixed inserts, updates, deletes
 *   3. After each batch: verify ALL live keys return correct values
 *   4. After each batch: verify deleted keys return 0
 *   5. Close + reopen from disk, verify all data survives
 *   6. Shared mode: multiple tries with cache pressure
 *   7. Discard batch: verify no corruption
 *   8. Flush between batches: verify deferred → disk → cache round-trip
 *
 * Designed to catch:
 *   - Lost nodes after LRU eviction
 *   - Corrupt data after evict → reload from deferred/disk
 *   - Stale cache entries after updates
 *   - Missing nodes after flush
 */

#include "mpt_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define STORE_PATH "/tmp/test_mpt_stress"

static void cleanup(void) {
    unlink(STORE_PATH ".idx");
    unlink(STORE_PATH ".dat");
    unlink(STORE_PATH ".free");
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static int failures = 0;

#define CHECK(cond, msg, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL: " msg "\n", ##__VA_ARGS__); \
        failures++; \
    } \
} while (0)

/* Deterministic key from index */
static void make_key(uint8_t out[32], uint32_t idx) {
    memset(out, 0, 32);
    /* Spread across key space for realistic trie structure */
    out[0] = (uint8_t)(idx >> 24);
    out[1] = (uint8_t)(idx >> 16);
    out[2] = (uint8_t)(idx >> 8);
    out[3] = (uint8_t)(idx);
    /* Add some entropy in middle bytes */
    out[16] = (uint8_t)(idx * 7 + 13);
    out[17] = (uint8_t)(idx * 31 + 97);
}

/* Deterministic value from index + version */
static size_t make_value(uint8_t *out, uint32_t idx, uint32_t version) {
    /* Variable-length values to exercise different slot sizes */
    size_t len = 10 + (idx % 200);  /* 10–209 bytes */
    for (size_t i = 0; i < len; i++)
        out[i] = (uint8_t)(idx ^ version ^ i);
    return len;
}

/* Verify a single key returns expected value */
static bool verify_key(const mpt_store_t *ms, uint32_t idx, uint32_t version) {
    uint8_t key[32];
    make_key(key, idx);

    uint8_t expected[256], actual[256];
    size_t exp_len = make_value(expected, idx, version);

    uint32_t got = mpt_store_get(ms, key, actual, sizeof(actual));
    if (got != exp_len) return false;
    return memcmp(expected, actual, exp_len) == 0;
}

/* Verify a key is absent */
static bool verify_absent(const mpt_store_t *ms, uint32_t idx) {
    uint8_t key[32];
    make_key(key, idx);
    return mpt_store_get(ms, key, NULL, 0) == 0;
}

/* =========================================================================
 * Test 1: Insert + evict + read-back
 *
 * Insert 500 keys with a 64-entry cache. Most nodes will be evicted.
 * Then read back every key — forces reload from deferred/disk.
 * ========================================================================= */
static void test_insert_evict_readback(void) {
    printf("Test 1: insert + evict + read-back ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
    CHECK(ms, "create failed");
    if (!ms) { printf("SKIP\n"); return; }

    /* Tiny cache — forces heavy eviction */
    mpt_store_set_cache(ms, 64);

    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 500; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 0);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);

    /* Verify every key is readable */
    int bad = 0;
    for (uint32_t i = 0; i < 500; i++) {
        if (!verify_key(ms, i, 0)) bad++;
    }
    CHECK(bad == 0, "insert readback: %d/500 keys corrupted", bad);

    mpt_store_stats_t st = mpt_store_stats(ms);
    CHECK(st.cache_misses > 0, "expected cache misses with 64-entry cache");

    mpt_store_destroy(ms);
    if (bad == 0) printf("OK\n");
}

/* =========================================================================
 * Test 2: Multi-batch updates under pressure
 *
 * 20 batches of 50 keys each. Each batch updates existing keys and adds
 * new ones. After each batch, verify ALL live keys.
 * ========================================================================= */
static void test_multi_batch_updates(void) {
    printf("Test 2: multi-batch updates under cache pressure ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
    mpt_store_set_cache(ms, 64);

    /* Track expected version per key */
    #define MAX_KEYS 1000
    uint32_t versions[MAX_KEYS];
    memset(versions, 0, sizeof(versions));

    int bad = 0;
    uint32_t total_keys = 0;

    for (uint32_t batch = 0; batch < 20; batch++) {
        mpt_store_begin_batch(ms);

        /* Update some existing keys with new version */
        for (uint32_t i = 0; i < total_keys && i < 25; i++) {
            uint8_t key[32], val[256];
            make_key(key, i);
            size_t vlen = make_value(val, i, batch + 1);
            mpt_store_update(ms, key, val, vlen);
            versions[i] = batch + 1;
        }

        /* Add new keys */
        uint32_t new_start = total_keys;
        uint32_t new_count = 50;
        for (uint32_t i = 0; i < new_count; i++) {
            uint32_t idx = new_start + i;
            uint8_t key[32], val[256];
            make_key(key, idx);
            size_t vlen = make_value(val, idx, batch + 1);
            mpt_store_update(ms, key, val, vlen);
            versions[idx] = batch + 1;
        }

        mpt_store_commit_batch(ms);
        total_keys += new_count;

        /* Verify ALL keys after this batch */
        for (uint32_t i = 0; i < total_keys; i++) {
            if (!verify_key(ms, i, versions[i])) bad++;
        }
    }

    CHECK(bad == 0, "multi-batch: %d key verifications failed (total_keys=%u)",
          bad, total_keys);

    mpt_store_destroy(ms);
    if (bad == 0) printf("OK (total_keys=%u)\n", total_keys);
    #undef MAX_KEYS
}

/* =========================================================================
 * Test 3: Insert + delete + verify absent
 *
 * Insert 200 keys, delete odd-indexed ones, verify even survive and
 * odd are gone. Then re-insert deleted keys with new values.
 * ========================================================================= */
static void test_delete_and_reinsert(void) {
    printf("Test 3: delete + verify absent + reinsert ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
    mpt_store_set_cache(ms, 64);

    /* Insert 200 keys */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 200; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 0);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);

    /* Save root after full insert */
    uint8_t root_full[32];
    mpt_store_root(ms, root_full);

    /* Delete odd keys */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 1; i < 200; i += 2) {
        uint8_t key[32];
        make_key(key, i);
        mpt_store_delete(ms, key);
    }
    mpt_store_commit_batch(ms);

    /* Verify even keys survive, odd keys gone */
    int bad = 0;
    for (uint32_t i = 0; i < 200; i++) {
        if (i % 2 == 0) {
            if (!verify_key(ms, i, 0)) bad++;
        } else {
            if (!verify_absent(ms, i)) bad++;
        }
    }
    CHECK(bad == 0, "delete phase: %d failures", bad);

    /* Root should have changed */
    uint8_t root_after_delete[32];
    mpt_store_root(ms, root_after_delete);
    CHECK(memcmp(root_full, root_after_delete, 32) != 0,
          "root unchanged after deleting 100 keys");

    /* Reinsert deleted keys with new values */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 1; i < 200; i += 2) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 99);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);

    /* Verify all 200 keys present with correct versions */
    bad = 0;
    for (uint32_t i = 0; i < 200; i++) {
        uint32_t ver = (i % 2 == 0) ? 0 : 99;
        if (!verify_key(ms, i, ver)) bad++;
    }
    CHECK(bad == 0, "reinsert phase: %d failures", bad);

    mpt_store_destroy(ms);
    if (bad == 0) printf("OK\n");
}

/* =========================================================================
 * Test 4: Flush + close + reopen — disk persistence
 *
 * Insert keys, flush to disk, destroy, reopen, verify all data.
 * ========================================================================= */
static void test_persistence(void) {
    printf("Test 4: flush + close + reopen persistence ... ");
    cleanup();

    uint8_t saved_root[32];

    /* Phase 1: create and populate */
    {
        mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
        mpt_store_set_cache(ms, 64);

        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < 300; i++) {
            uint8_t key[32], val[256];
            make_key(key, i);
            size_t vlen = make_value(val, i, 0);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);
        mpt_store_flush(ms);
        mpt_store_root(ms, saved_root);
        mpt_store_destroy(ms);
    }

    /* Phase 2: reopen and verify */
    {
        mpt_store_t *ms = mpt_store_open(STORE_PATH);
        CHECK(ms, "reopen failed");
        if (!ms) { printf("SKIP\n"); return; }

        mpt_store_set_cache(ms, 64);

        uint8_t reopened_root[32];
        mpt_store_root(ms, reopened_root);
        CHECK(memcmp(saved_root, reopened_root, 32) == 0,
              "root mismatch after reopen");

        int bad = 0;
        for (uint32_t i = 0; i < 300; i++) {
            if (!verify_key(ms, i, 0)) bad++;
        }
        CHECK(bad == 0, "persistence: %d/300 keys lost after reopen", bad);

        mpt_store_destroy(ms);
        if (bad == 0) printf("OK\n");
    }
}

/* =========================================================================
 * Test 5: Flush between every batch — deferred → disk round-trip
 *
 * Forces nodes through: write → deferred → flush → disk → evict → reload
 * ========================================================================= */
static void test_flush_every_batch(void) {
    printf("Test 5: flush between batches (deferred → disk cycle) ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
    mpt_store_set_cache(ms, 32);  /* Even smaller cache */

    int bad = 0;
    for (uint32_t batch = 0; batch < 30; batch++) {
        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < 20; i++) {
            uint32_t idx = batch * 20 + i;
            uint8_t key[32], val[256];
            make_key(key, idx);
            size_t vlen = make_value(val, idx, 0);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);
        mpt_store_flush(ms);  /* Force deferred → disk */

        /* Verify ALL keys inserted so far */
        uint32_t total = (batch + 1) * 20;
        for (uint32_t i = 0; i < total; i++) {
            if (!verify_key(ms, i, 0)) bad++;
        }
    }

    CHECK(bad == 0, "flush-every-batch: %d verifications failed", bad);

    mpt_store_stats_t st = mpt_store_stats(ms);
    CHECK(st.cache_misses > 100,
          "expected many cache misses, got %lu", st.cache_misses);

    mpt_store_destroy(ms);
    if (bad == 0) printf("OK (600 keys, cache=32)\n");
}

/* =========================================================================
 * Test 6: Discard batch — no corruption
 *
 * Insert keys, begin new batch with updates, discard, verify original
 * values unchanged.
 * ========================================================================= */
static void test_discard_batch(void) {
    printf("Test 6: discard batch — no side effects ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
    mpt_store_set_cache(ms, 64);

    /* Insert 100 keys */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 0);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);

    uint8_t root_before[32];
    mpt_store_root(ms, root_before);

    /* Begin batch with updates + new keys, then DISCARD */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 50; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 999);  /* new version */
        mpt_store_update(ms, key, val, vlen);
    }
    for (uint32_t i = 100; i < 150; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 999);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_discard_batch(ms);

    /* Root should be unchanged */
    uint8_t root_after[32];
    mpt_store_root(ms, root_after);
    CHECK(memcmp(root_before, root_after, 32) == 0,
          "root changed after discard");

    /* Original values should be intact */
    int bad = 0;
    for (uint32_t i = 0; i < 100; i++) {
        if (!verify_key(ms, i, 0)) bad++;
    }
    CHECK(bad == 0, "discard: %d/100 original values corrupted", bad);

    /* New keys should NOT exist */
    for (uint32_t i = 100; i < 150; i++) {
        if (!verify_absent(ms, i)) bad++;
    }
    CHECK(bad == 0, "discard: phantom keys found after discard");

    mpt_store_destroy(ms);
    if (bad == 0) printf("OK\n");
}

/* =========================================================================
 * Test 7: Shared mode — multiple tries, cache pressure
 *
 * Two tries sharing a store with tiny cache. Verify switching between
 * roots doesn't lose data.
 * ========================================================================= */
static void test_shared_mode_pressure(void) {
    printf("Test 7: shared mode + cache pressure ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
    mpt_store_set_cache(ms, 64);
    mpt_store_set_shared(ms, true);

    /* Build trie A: keys 0–99 */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 0);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);
    uint8_t root_a[32];
    mpt_store_root(ms, root_a);

    /* Build trie B: keys 50–149 (overlaps 50–99 with A) */
    uint8_t empty_root[32];
    mpt_store_root(ms, empty_root);  /* save current root */

    /* Reset to empty for trie B */
    static const uint8_t EMPTY_ROOT[32] = {
        0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
        0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
        0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
        0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21
    };
    mpt_store_set_root(ms, EMPTY_ROOT);

    mpt_store_begin_batch(ms);
    for (uint32_t i = 50; i < 150; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 1);  /* different version */
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);
    uint8_t root_b[32];
    mpt_store_root(ms, root_b);

    /* Flush everything to disk */
    mpt_store_flush(ms);

    /* Now switch back and forth, verifying data under cache pressure */
    int bad = 0;

    /* Check trie A */
    mpt_store_set_root(ms, root_a);
    for (uint32_t i = 0; i < 100; i++) {
        if (!verify_key(ms, i, 0)) bad++;
    }
    CHECK(bad == 0, "shared trie A: %d/100 corrupted", bad);

    /* Check trie B */
    mpt_store_set_root(ms, root_b);
    for (uint32_t i = 50; i < 150; i++) {
        if (!verify_key(ms, i, 1)) bad++;
    }
    CHECK(bad == 0, "shared trie B: %d/100 corrupted", bad);

    /* Switch back to A again (cache has been polluted by B reads) */
    mpt_store_set_root(ms, root_a);
    int bad2 = 0;
    for (uint32_t i = 0; i < 100; i++) {
        if (!verify_key(ms, i, 0)) bad2++;
    }
    CHECK(bad2 == 0, "shared trie A (re-read): %d/100 corrupted", bad2);

    mpt_store_destroy(ms);
    if (bad == 0 && bad2 == 0) printf("OK\n");
}

/* =========================================================================
 * Test 8: Rapid update same keys — stale cache detection
 *
 * Update the same 50 keys 50 times. Each time, verify the latest value.
 * Catches stale cache entries that weren't invalidated on update.
 * ========================================================================= */
static void test_rapid_updates_same_keys(void) {
    printf("Test 8: rapid updates of same keys (stale cache test) ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 4096);
    mpt_store_set_cache(ms, 64);

    /* Initial insert */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 50; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 0);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);

    int bad = 0;
    for (uint32_t round = 1; round <= 50; round++) {
        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < 50; i++) {
            uint8_t key[32], val[256];
            make_key(key, i);
            size_t vlen = make_value(val, i, round);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);

        /* Flush every 10 rounds to cycle deferred → disk */
        if (round % 10 == 0) mpt_store_flush(ms);

        /* Verify latest values */
        for (uint32_t i = 0; i < 50; i++) {
            if (!verify_key(ms, i, round)) bad++;
        }
    }

    CHECK(bad == 0, "rapid updates: %d verifications failed over 50 rounds", bad);

    mpt_store_destroy(ms);
    if (bad == 0) printf("OK\n");
}

/* =========================================================================
 * Test 9: Large batch stress — 2000 keys, cache=32
 *
 * Extreme pressure: 2000 keys with 32-entry cache. Nearly every read
 * is a cache miss.
 * ========================================================================= */
static void test_large_batch_tiny_cache(void) {
    printf("Test 9: 2000 keys with 32-entry cache ... ");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, 8192);
    mpt_store_set_cache(ms, 32);

    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 2000; i++) {
        uint8_t key[32], val[256];
        make_key(key, i);
        size_t vlen = make_value(val, i, 0);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);
    mpt_store_flush(ms);

    /* Verify all 2000 keys — almost all will miss cache */
    int bad = 0;
    for (uint32_t i = 0; i < 2000; i++) {
        if (!verify_key(ms, i, 0)) bad++;
    }
    CHECK(bad == 0, "large batch: %d/2000 keys corrupted", bad);

    mpt_store_stats_t st = mpt_store_stats(ms);
    printf("(hits=%lu misses=%lu evict_skip=%lu) ",
           st.cache_hits, st.cache_misses, st.cache_evict_skipped);

    /* Delete half, verify other half survives */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < 2000; i += 2) {
        uint8_t key[32];
        make_key(key, i);
        mpt_store_delete(ms, key);
    }
    mpt_store_commit_batch(ms);
    mpt_store_flush(ms);

    int bad2 = 0;
    for (uint32_t i = 0; i < 2000; i++) {
        if (i % 2 == 0) {
            if (!verify_absent(ms, i)) bad2++;
        } else {
            if (!verify_key(ms, i, 0)) bad2++;
        }
    }
    CHECK(bad2 == 0, "large batch delete: %d failures", bad2);

    mpt_store_destroy(ms);
    if (bad == 0 && bad2 == 0) printf("OK\n");
}

/* =========================================================================
 * Test 10: Persistence across multiple open/close cycles
 *
 * Simulate what chain_replay does: batch of updates → flush → close → reopen
 * Repeat 10 times, verify cumulative state each time.
 * ========================================================================= */
static void test_multi_session_persistence(void) {
    printf("Test 10: multi-session persistence (10 cycles) ... ");
    cleanup();

    for (uint32_t session = 0; session < 10; session++) {
        mpt_store_t *ms;
        if (session == 0) {
            ms = mpt_store_create(STORE_PATH, 4096);
        } else {
            ms = mpt_store_open(STORE_PATH);
        }
        CHECK(ms, "session %u: open failed", session);
        if (!ms) { printf("SKIP\n"); return; }

        mpt_store_set_cache(ms, 48);

        /* Verify all previously inserted keys */
        int bad = 0;
        for (uint32_t i = 0; i < session * 30; i++) {
            if (!verify_key(ms, i, 0)) bad++;
        }
        CHECK(bad == 0, "session %u: %d prior keys lost", session, bad);

        /* Add 30 new keys */
        mpt_store_begin_batch(ms);
        for (uint32_t i = session * 30; i < (session + 1) * 30; i++) {
            uint8_t key[32], val[256];
            make_key(key, i);
            size_t vlen = make_value(val, i, 0);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);
        mpt_store_flush(ms);

        mpt_store_destroy(ms);
    }

    /* Final verification: reopen and check all 300 keys */
    mpt_store_t *ms = mpt_store_open(STORE_PATH);
    CHECK(ms, "final reopen failed");
    if (!ms) { printf("SKIP\n"); return; }

    mpt_store_set_cache(ms, 48);
    int bad = 0;
    for (uint32_t i = 0; i < 300; i++) {
        if (!verify_key(ms, i, 0)) bad++;
    }
    CHECK(bad == 0, "final: %d/300 keys lost across 10 sessions", bad);

    mpt_store_destroy(ms);
    if (bad == 0) printf("OK\n");
}

/* ========================================================================= */

int main(void) {
    printf("=== MPT Store Stress Test ===\n\n");

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    test_insert_evict_readback();
    test_multi_batch_updates();
    test_delete_and_reinsert();
    test_persistence();
    test_flush_every_batch();
    test_discard_batch();
    test_shared_mode_pressure();
    test_rapid_updates_same_keys();
    test_large_batch_tiny_cache();
    test_multi_session_persistence();

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("\n=== %s (%d failures, %.2fs) ===\n",
           failures == 0 ? "ALL PASSED" : "FAILURES", failures, elapsed);

    cleanup();
    return failures == 0 ? 0 : 1;
}
