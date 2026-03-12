/*
 * MPT Store Stress Test — Checkpoint Flow Simulator
 *
 * Mimics the exact chain_replay checkpoint cycle:
 *   1. commit_batch  (update_subtrie: write new nodes, delete old nodes)
 *   2. flush_wait    (wait for PREVIOUS bg flush)
 *   3. flush_bg      (move deferred → snapshot, spawn bg thread)
 *   4. [no explicit flush_wait — bg thread runs concurrently]
 *   5. verify ALL entries via mpt_store_get  (read-through from snapshot/disk)
 *   6. repeat for many rounds
 *
 * Also tests:
 *   - Mixed inserts + deletes + updates in same batch (triggers collapse_branch)
 *   - Entries surviving across multiple checkpoint cycles
 *   - Close/reopen after bg_flush (simulates process restart)
 *   - Cache destruction between batches (simulates evict_cache)
 *   - High churn: rapid insert/delete cycles on overlapping key sets
 *
 * Fail-fast: aborts on first mismatch with full diagnostic output.
 */

#include "mpt_store.h"
#include "keccak256.h"

bool g_trace_calls = false;

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* Convenience wrapper — keccak is static in mpt_store.c */
static void keccak_hash(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update_long(&ctx, data, len);
    keccak_final(&ctx, out);
}

/* =========================================================================
 * Config
 * ========================================================================= */

#define STORE_PATH      "/tmp/test_mpt_stress"
#define CAPACITY_HINT   100000

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void cleanup(void) {
    unlink(STORE_PATH ".idx");
    unlink(STORE_PATH ".dat");
}

static void print_hash(const uint8_t h[32]) {
    for (int i = 0; i < 4; i++) fprintf(stderr, "%02x", h[i]);
    fprintf(stderr, "..");
    for (int i = 30; i < 32; i++) fprintf(stderr, "%02x", h[i]);
}

/* Deterministic 32-byte key from index (keccak for good trie distribution) */
static void make_key(uint32_t idx, uint8_t key[32]) {
    uint8_t buf[4];
    buf[0] = (uint8_t)(idx >> 24);
    buf[1] = (uint8_t)(idx >> 16);
    buf[2] = (uint8_t)(idx >> 8);
    buf[3] = (uint8_t)(idx);
    keccak_hash(buf, 4, key);
}

/* Deterministic value from index + generation (allows detecting stale values) */
static void make_value(uint32_t idx, uint32_t gen, uint8_t *val, size_t *len) {
    /* Variable length 8..67 bytes, content depends on both idx and gen */
    size_t n = 8 + ((idx ^ gen) % 60);
    *len = n;
    for (size_t i = 0; i < n; i++)
        val[i] = (uint8_t)(idx * 13 + gen * 7 + i * 3);
}

#define VAL_BUF 128

/* Tracking structure: what keys exist and at what generation */
typedef struct {
    uint32_t *gens;     /* generation per key index, 0 = deleted */
    uint32_t count;     /* number of key indices tracked */
} key_tracker_t;

static void tracker_init(key_tracker_t *t, uint32_t max_keys) {
    t->gens = calloc(max_keys, sizeof(uint32_t));
    t->count = max_keys;
}

static void tracker_free(key_tracker_t *t) {
    free(t->gens);
    t->gens = NULL;
    t->count = 0;
}

/* =========================================================================
 * Verification: walk every tracked key, check mpt_store_get matches
 * ========================================================================= */

static int verify_all(const mpt_store_t *ms, const key_tracker_t *t,
                       const char *phase, uint32_t round) {
    int errors = 0;
    int live = 0;

    for (uint32_t i = 0; i < t->count; i++) {
        uint8_t key[32], got[VAL_BUF];
        make_key(i, key);
        uint32_t got_len = mpt_store_get(ms, key, got, VAL_BUF);

        if (t->gens[i] == 0) {
            /* Key should NOT exist */
            if (got_len != 0) {
                if (errors < 5) {
                    fprintf(stderr, "  FAIL [round %u %s] key %u: expected DELETED, got len=%u\n",
                            round, phase, i, got_len);
                }
                errors++;
            }
        } else {
            /* Key should exist with specific value */
            live++;
            uint8_t expected[VAL_BUF];
            size_t expected_len;
            make_value(i, t->gens[i], expected, &expected_len);

            if (got_len != (uint32_t)expected_len) {
                if (errors < 5) {
                    fprintf(stderr, "  FAIL [round %u %s] key %u: len mismatch got=%u expected=%zu gen=%u\n",
                            round, phase, i, got_len, expected_len, t->gens[i]);
                    fprintf(stderr, "    key_hash=");
                    print_hash(key);
                    fprintf(stderr, "\n");
                }
                errors++;
            } else if (memcmp(got, expected, expected_len) != 0) {
                if (errors < 5) {
                    fprintf(stderr, "  FAIL [round %u %s] key %u: value mismatch gen=%u\n",
                            round, phase, i, t->gens[i]);
                }
                errors++;
            }
        }
    }

    if (errors > 0) {
        fprintf(stderr, "  TOTAL: %d errors out of %u keys (%d live)\n",
                errors, t->count, live);
    }
    return errors;
}

/* =========================================================================
 * Test 1: Checkpoint cycle stress test
 *
 * Mimics chain_replay: N rounds of commit + bg_flush + verify.
 * Each round: insert some keys, update some, delete some.
 * After bg_flush (no flush_wait!), destroy cache, then verify all keys.
 * ========================================================================= */

#define T1_MAX_KEYS   5000
#define T1_ROUNDS     20
#define T1_INSERT     200   /* new keys per round */
#define T1_UPDATE     100   /* existing keys updated per round */
#define T1_DELETE      50   /* existing keys deleted per round */

static int test_checkpoint_cycle(void) {
    printf("Test 1: checkpoint cycle (%d rounds, %d insert/round)...\n",
           T1_ROUNDS, T1_INSERT);
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T1_MAX_KEYS);

    uint32_t next_key = 0;  /* next unused key index */
    uint32_t rng = 42;      /* simple LCG for deterministic "randomness" */

    for (uint32_t round = 0; round < T1_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        /* 1. Insert new keys */
        uint32_t insert_count = T1_INSERT;
        if (next_key + insert_count > T1_MAX_KEYS)
            insert_count = T1_MAX_KEYS - next_key;

        for (uint32_t i = 0; i < insert_count; i++) {
            uint32_t idx = next_key++;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            tracker.gens[idx] = round + 1;  /* generation 1-based */
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        /* 2. Update some existing keys */
        for (uint32_t i = 0; i < T1_UPDATE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;  /* already deleted */

            tracker.gens[idx] = round + 1;  /* new generation */
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        /* 3. Delete some existing keys */
        for (uint32_t i = 0; i < T1_DELETE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;  /* already deleted */

            tracker.gens[idx] = 0;  /* mark deleted */
            uint8_t key[32];
            make_key(idx, key);
            mpt_store_delete(ms, key);
        }

        mpt_store_commit_batch(ms);

        /* === Checkpoint flow (matches sync_checkpoint) === */

        /* flush_wait(prev) — wait for previous bg flush */
        mpt_store_flush_wait(ms);

        /* flush_bg — rotate deferred → snapshot, spawn bg thread */
        mpt_store_flush_bg(ms);

        /* Destroy cache (simulates evm_state_evict_cache) */
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);  /* re-enable empty cache */

        /* === Verify ALL keys via read-through === */
        int errors = verify_all(ms, &tracker, "post-checkpoint", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: round %u failed with %d errors\n", round, errors);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }
    }

    /* Final: flush_wait + destroy + reopen + verify */
    uint8_t root_before[32];
    mpt_store_root(ms, root_before);
    mpt_store_destroy(ms);  /* calls flush_wait internally */

    ms = mpt_store_open(STORE_PATH);
    if (!ms) { printf("  FAIL: cannot reopen\n"); tracker_free(&tracker); return 1; }

    uint8_t root_after[32];
    mpt_store_root(ms, root_after);
    if (memcmp(root_before, root_after, 32) != 0) {
        printf("  FAIL: root mismatch after reopen\n");
        mpt_store_destroy(ms);
        tracker_free(&tracker);
        return 1;
    }

    int errors = verify_all(ms, &tracker, "after-reopen", T1_ROUNDS);
    mpt_store_destroy(ms);
    tracker_free(&tracker);

    if (errors > 0) return 1;
    printf("  PASS (%d rounds, root + values verified)\n", T1_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 2: High churn — heavy deletes causing branch collapse
 *
 * Insert a large batch, then delete most of it across multiple rounds.
 * This triggers collapse_branch (branch → extension/leaf merging) which
 * is suspected of causing the delete+re-create same-hash bug.
 * ========================================================================= */

#define T2_INITIAL   3000
#define T2_ROUNDS    10
#define T2_DEL_PER   200   /* deletes per round */
#define T2_ADD_PER    30   /* new inserts per round */

static int test_high_churn(void) {
    printf("Test 2: high churn (delete-heavy, collapse_branch exercise)...\n");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T2_INITIAL + T2_ROUNDS * T2_ADD_PER);

    /* Initial bulk insert */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < T2_INITIAL; i++) {
        uint8_t key[32], val[VAL_BUF];
        size_t vlen;
        tracker.gens[i] = 1;
        make_key(i, key);
        make_value(i, 1, val, &vlen);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);
    mpt_store_flush(ms);

    uint32_t next_key = T2_INITIAL;
    uint32_t rng = 1337;

    for (uint32_t round = 0; round < T2_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        /* Delete many existing keys (triggers collapse_branch) */
        uint32_t deleted = 0;
        for (uint32_t attempt = 0; attempt < T2_DEL_PER * 3 && deleted < T2_DEL_PER; attempt++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;

            tracker.gens[idx] = 0;
            uint8_t key[32];
            make_key(idx, key);
            mpt_store_delete(ms, key);
            deleted++;
        }

        /* Add a few new keys */
        for (uint32_t i = 0; i < T2_ADD_PER; i++) {
            uint32_t idx = next_key++;
            if (idx >= tracker.count) break;
            tracker.gens[idx] = round + 2;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        mpt_store_commit_batch(ms);

        /* Checkpoint cycle: flush_wait(prev) + flush_bg + evict cache */
        mpt_store_flush_wait(ms);
        mpt_store_flush_bg(ms);
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        int errors = verify_all(ms, &tracker, "post-churn", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: churn round %u failed\n", round);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }
    }

    /* Reopen and verify */
    mpt_store_destroy(ms);
    ms = mpt_store_open(STORE_PATH);
    if (!ms) { printf("  FAIL: cannot reopen\n"); tracker_free(&tracker); return 1; }

    int errors = verify_all(ms, &tracker, "after-reopen", T2_ROUNDS);
    mpt_store_destroy(ms);
    tracker_free(&tracker);
    if (errors > 0) return 1;

    printf("  PASS (delete-heavy churn survived %d rounds)\n", T2_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 3: bg_flush NOT waited — read-through from snapshot
 *
 * The critical scenario: flush_bg starts the bg thread, but we
 * immediately read via mpt_store_get WITHOUT calling flush_wait.
 * The read-through chain must find nodes in the snapshot.
 * ========================================================================= */

#define T3_KEYS     2000
#define T3_ROUNDS   10

static int test_bg_no_wait_readthrough(void) {
    printf("Test 3: bg_flush read-through (no flush_wait before read)...\n");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T3_KEYS * T3_ROUNDS);
    uint32_t next_key = 0;

    for (uint32_t round = 0; round < T3_ROUNDS; round++) {
        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < T3_KEYS; i++) {
            uint32_t idx = next_key++;
            if (idx >= tracker.count) break;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);

        /* flush_wait for PREVIOUS round's bg flush */
        mpt_store_flush_wait(ms);
        /* Start bg flush for THIS round */
        mpt_store_flush_bg(ms);

        /* Kill cache — force read-through */
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        /* Read ALL keys immediately (bg thread may still be running!) */
        int errors = verify_all(ms, &tracker, "bg-inflight", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: bg read-through failed at round %u\n", round);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }
    }

    mpt_store_destroy(ms);
    tracker_free(&tracker);
    printf("  PASS (%d rounds, read-through during bg flush OK)\n", T3_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 4: Reopen without flush_wait (simulates crash/exit during bg flush)
 *
 * After bg_flush, call mpt_store_destroy (which does flush_wait internally),
 * then reopen and verify. This is what happens when chain_replay exits
 * after a failed block.
 * ========================================================================= */

#define T4_KEYS     1500
#define T4_ROUNDS   8

static int test_reopen_after_bg(void) {
    printf("Test 4: reopen after bg_flush + destroy (%d rounds)...\n", T4_ROUNDS);
    cleanup();

    key_tracker_t tracker;
    tracker_init(&tracker, T4_KEYS * T4_ROUNDS);
    uint32_t next_key = 0;

    for (uint32_t round = 0; round < T4_ROUNDS; round++) {
        mpt_store_t *ms;
        if (round == 0) {
            ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
        } else {
            ms = mpt_store_open(STORE_PATH);
        }
        if (!ms) { printf("  FAIL: cannot open/create at round %u\n", round); tracker_free(&tracker); return 1; }

        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < T4_KEYS; i++) {
            uint32_t idx = next_key++;
            if (idx >= tracker.count) break;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);

        /* bg_flush WITHOUT explicit flush_wait */
        mpt_store_flush_bg(ms);

        /* Destroy (which calls flush_wait internally) */
        mpt_store_destroy(ms);

        /* Reopen and verify ALL keys from all rounds so far */
        ms = mpt_store_open(STORE_PATH);
        if (!ms) { printf("  FAIL: cannot reopen at round %u\n", round); tracker_free(&tracker); return 1; }

        int errors = verify_all(ms, &tracker, "reopen", round);
        mpt_store_destroy(ms);
        if (errors > 0) {
            fprintf(stderr, "ABORT: reopen test failed at round %u\n", round);
            tracker_free(&tracker);
            return 1;
        }
    }

    tracker_free(&tracker);
    printf("  PASS (%d close/reopen cycles)\n", T4_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 5: Mixed insert/delete/update with bg_flush + reopen per round
 *
 * This is the most realistic test: each round does inserts, updates,
 * and deletes, then bg_flush + destroy + reopen + verify.
 * ========================================================================= */

#define T5_MAX_KEYS  6000
#define T5_ROUNDS    15
#define T5_INSERT    300
#define T5_UPDATE    150
#define T5_DELETE    100

static int test_full_lifecycle(void) {
    printf("Test 5: full lifecycle (insert+update+delete, bg_flush, reopen)...\n");
    cleanup();

    key_tracker_t tracker;
    tracker_init(&tracker, T5_MAX_KEYS);
    uint32_t next_key = 0;
    uint32_t rng = 9999;

    for (uint32_t round = 0; round < T5_ROUNDS; round++) {
        mpt_store_t *ms;
        if (round == 0) {
            ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
        } else {
            ms = mpt_store_open(STORE_PATH);
        }
        if (!ms) { printf("  FAIL: open at round %u\n", round); tracker_free(&tracker); return 1; }

        mpt_store_begin_batch(ms);

        /* Inserts */
        uint32_t ins = T5_INSERT;
        if (next_key + ins > T5_MAX_KEYS) ins = T5_MAX_KEYS - next_key;
        for (uint32_t i = 0; i < ins; i++) {
            uint32_t idx = next_key++;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        /* Updates */
        for (uint32_t i = 0; i < T5_UPDATE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        /* Deletes */
        for (uint32_t i = 0; i < T5_DELETE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = 0;
            uint8_t key[32];
            make_key(idx, key);
            mpt_store_delete(ms, key);
        }

        mpt_store_commit_batch(ms);

        /* Checkpoint flow: flush_wait(prev) is a no-op on fresh-opened store */
        mpt_store_flush_bg(ms);

        /* Kill cache and verify with bg thread potentially running */
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        int errors = verify_all(ms, &tracker, "pre-reopen", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: pre-reopen verify failed at round %u\n", round);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }

        /* Destroy + reopen (simulates process restart) */
        mpt_store_destroy(ms);

        ms = mpt_store_open(STORE_PATH);
        if (!ms) { printf("  FAIL: reopen at round %u\n", round); tracker_free(&tracker); return 1; }

        errors = verify_all(ms, &tracker, "post-reopen", round);
        mpt_store_destroy(ms);
        if (errors > 0) {
            fprintf(stderr, "ABORT: post-reopen verify failed at round %u\n", round);
            tracker_free(&tracker);
            return 1;
        }
    }

    tracker_free(&tracker);
    printf("  PASS (%d lifecycle rounds)\n", T5_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 6: Sustained multi-batch with continuous bg_flush (no reopen)
 *
 * This matches the in-process chain_replay flow: the store stays open
 * across many checkpoint cycles. Each cycle: commit + flush_wait(prev) +
 * flush_bg + evict cache + verify.
 * ========================================================================= */

#define T6_MAX_KEYS  10000
#define T6_ROUNDS    40
#define T6_INSERT    200
#define T6_UPDATE    100
#define T6_DELETE     60

static int test_sustained_checkpoint(void) {
    printf("Test 6: sustained checkpoint (%d rounds, no reopen)...\n", T6_ROUNDS);
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T6_MAX_KEYS);
    uint32_t next_key = 0;
    uint32_t rng = 31415;

    for (uint32_t round = 0; round < T6_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        uint32_t ins = T6_INSERT;
        if (next_key + ins > T6_MAX_KEYS) ins = T6_MAX_KEYS - next_key;
        for (uint32_t i = 0; i < ins; i++) {
            uint32_t idx = next_key++;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        for (uint32_t i = 0; i < T6_UPDATE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        for (uint32_t i = 0; i < T6_DELETE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = 0;
            uint8_t key[32];
            make_key(idx, key);
            mpt_store_delete(ms, key);
        }

        mpt_store_commit_batch(ms);

        /* Exact checkpoint flow */
        mpt_store_flush_wait(ms);   /* wait for PREVIOUS bg flush */
        mpt_store_flush_bg(ms);     /* start bg flush for THIS batch */

        /* Evict cache */
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        /* Verify read-through (bg thread may still be running) */
        int errors = verify_all(ms, &tracker, "sustained", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: sustained round %u failed\n", round);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }
    }

    /* Final destroy + reopen + verify */
    uint8_t root_before[32];
    mpt_store_root(ms, root_before);
    mpt_store_destroy(ms);

    ms = mpt_store_open(STORE_PATH);
    if (!ms) { printf("  FAIL: cannot reopen\n"); tracker_free(&tracker); return 1; }

    uint8_t root_after[32];
    mpt_store_root(ms, root_after);
    if (memcmp(root_before, root_after, 32) != 0) {
        printf("  FAIL: root mismatch after final reopen\n");
        mpt_store_destroy(ms);
        tracker_free(&tracker);
        return 1;
    }

    int errors = verify_all(ms, &tracker, "final-reopen", T6_ROUNDS);
    mpt_store_destroy(ms);
    tracker_free(&tracker);
    if (errors > 0) return 1;

    printf("  PASS (%d sustained rounds + final reopen verified)\n", T6_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 7: Sustained checkpoint + simulated failed block + reopen
 *
 * This matches the exact chain_replay bug scenario:
 *   1. Multiple checkpoint rounds (commit + flush_wait + flush_bg + evict)
 *   2. Then do MORE work (dirty accounts) WITHOUT committing
 *   3. Destroy WITHOUT flushing (discard_on_destroy path)
 *   4. Reopen and verify ALL data from the last checkpoint
 *
 * The critical question: are nodes from the last checkpoint's bg_flush
 * properly on disk after destroy?
 * ========================================================================= */

#define T7_MAX_KEYS  8000
#define T7_ROUNDS    20
#define T7_INSERT    300
#define T7_UPDATE    100
#define T7_DELETE     80
#define T7_EXTRA_DIRTY 200  /* extra dirty entries after last checkpoint */

static int test_failed_block_reopen(void) {
    printf("Test 7: failed block + reopen (discard_on_destroy path)...\n");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T7_MAX_KEYS);
    uint32_t next_key = 0;
    uint32_t rng = 271828;

    /* Phase 1: multiple checkpoint rounds (build up trie state) */
    for (uint32_t round = 0; round < T7_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        uint32_t ins = T7_INSERT;
        if (next_key + ins > T7_MAX_KEYS) ins = T7_MAX_KEYS - next_key;
        for (uint32_t i = 0; i < ins; i++) {
            uint32_t idx = next_key++;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        for (uint32_t i = 0; i < T7_UPDATE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        for (uint32_t i = 0; i < T7_DELETE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = 0;
            uint8_t key[32];
            make_key(idx, key);
            mpt_store_delete(ms, key);
        }

        mpt_store_commit_batch(ms);

        /* Checkpoint flow */
        mpt_store_flush_wait(ms);
        mpt_store_flush_bg(ms);
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);
    }

    /* Save the tracker state at this checkpoint
     * (this is what the on-disk state should match after reopen) */
    uint32_t *saved_gens = malloc(tracker.count * sizeof(uint32_t));
    memcpy(saved_gens, tracker.gens, tracker.count * sizeof(uint32_t));

    /* Phase 2: simulate post-checkpoint work (blocks after checkpoint)
     * These changes are NOT committed via commit_batch */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < T7_EXTRA_DIRTY && next_key > 0; i++) {
        rng = rng * 1103515245 + 12345;
        uint32_t idx = rng % next_key;
        /* Do random updates (these should be discarded) */
        uint8_t key[32], val[VAL_BUF];
        size_t vlen;
        make_key(idx, key);
        make_value(idx, 999, val, &vlen);
        mpt_store_update(ms, key, val, vlen);
    }
    /* DON'T commit — simulate block execution in progress */
    mpt_store_discard_batch(ms);

    /* Phase 3: destroy WITHOUT explicit flush (discard_on_destroy path)
     * mpt_store_destroy calls flush_wait internally, which waits for
     * the bg thread from the last checkpoint */
    mpt_store_destroy(ms);

    /* Phase 4: reopen and verify state matches the last checkpoint */
    ms = mpt_store_open(STORE_PATH);
    if (!ms) { printf("  FAIL: cannot reopen\n"); free(saved_gens); tracker_free(&tracker); return 1; }

    /* Restore tracker to checkpoint state for verification */
    memcpy(tracker.gens, saved_gens, tracker.count * sizeof(uint32_t));
    free(saved_gens);

    int errors = verify_all(ms, &tracker, "post-failed-block", T7_ROUNDS);
    mpt_store_destroy(ms);
    tracker_free(&tracker);

    if (errors > 0) {
        fprintf(stderr, "ABORT: failed block reopen test failed with %d errors\n", errors);
        return 1;
    }

    printf("  PASS (%d rounds + failed block + reopen verified)\n", T7_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 8: Multi-reopen sustained (chain_replay resume scenario)
 *
 * Close and reopen the store every N rounds. This is the exact scenario
 * where the bug manifests: the store is opened from disk, bg_flush is nil,
 * and all data must be read from disk.
 * ========================================================================= */

#define T8_MAX_KEYS   10000
#define T8_TOTAL_ROUNDS 30
#define T8_REOPEN_EVERY  5
#define T8_INSERT    250
#define T8_UPDATE    120
#define T8_DELETE     70

static int test_multi_reopen_sustained(void) {
    printf("Test 8: multi-reopen sustained (%d rounds, reopen every %d)...\n",
           T8_TOTAL_ROUNDS, T8_REOPEN_EVERY);
    cleanup();

    key_tracker_t tracker;
    tracker_init(&tracker, T8_MAX_KEYS);
    uint32_t next_key = 0;
    uint32_t rng = 161803;

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); tracker_free(&tracker); return 1; }

    for (uint32_t round = 0; round < T8_TOTAL_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        uint32_t ins = T8_INSERT;
        if (next_key + ins > T8_MAX_KEYS) ins = T8_MAX_KEYS - next_key;
        for (uint32_t i = 0; i < ins; i++) {
            uint32_t idx = next_key++;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        for (uint32_t i = 0; i < T8_UPDATE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        for (uint32_t i = 0; i < T8_DELETE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = 0;
            uint8_t key[32];
            make_key(idx, key);
            mpt_store_delete(ms, key);
        }

        mpt_store_commit_batch(ms);

        /* Checkpoint flow */
        mpt_store_flush_wait(ms);
        mpt_store_flush_bg(ms);
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        /* Verify after each checkpoint */
        int errors = verify_all(ms, &tracker, "checkpoint", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: checkpoint verify failed at round %u\n", round);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }

        /* Reopen every N rounds (simulates process restart) */
        if ((round + 1) % T8_REOPEN_EVERY == 0) {
            mpt_store_destroy(ms);
            ms = mpt_store_open(STORE_PATH);
            if (!ms) {
                printf("  FAIL: reopen at round %u\n", round);
                tracker_free(&tracker);
                return 1;
            }

            errors = verify_all(ms, &tracker, "after-reopen", round);
            if (errors > 0) {
                fprintf(stderr, "ABORT: post-reopen verify failed at round %u\n", round);
                mpt_store_destroy(ms);
                tracker_free(&tracker);
                return 1;
            }
        }
    }

    mpt_store_destroy(ms);
    tracker_free(&tracker);
    printf("  PASS (%d rounds with %d reopens)\n",
           T8_TOTAL_ROUNDS, T8_TOTAL_ROUNDS / T8_REOPEN_EVERY);
    return 0;
}

/* =========================================================================
 * Test 9: Untouched entries survive sibling modifications
 *
 * Insert a set of "sentinel" keys early. Then across many rounds, only
 * modify OTHER keys in the same trie. Sentinels share branch nodes with
 * the modified keys at various depths. After each checkpoint, verify
 * sentinels are still readable. This catches the case where modifying
 * one branch child accidentally deletes a sibling's subtree.
 * ========================================================================= */

#define T9_SENTINELS   500   /* keys that are NEVER modified after insert */
#define T9_ACTIVE     2000   /* keys that get modified/deleted every round */
#define T9_ROUNDS      25

static int test_sentinel_survival(void) {
    printf("Test 9: sentinel survival (untouched keys across %d rounds)...\n", T9_ROUNDS);
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    /* Phase 1: insert sentinels */
    mpt_store_begin_batch(ms);
    uint8_t sentinel_keys[T9_SENTINELS][32];
    uint8_t sentinel_vals[T9_SENTINELS][VAL_BUF];
    size_t sentinel_lens[T9_SENTINELS];

    for (uint32_t i = 0; i < T9_SENTINELS; i++) {
        make_key(i, sentinel_keys[i]);
        make_value(i, 1, sentinel_vals[i], &sentinel_lens[i]);
        mpt_store_update(ms, sentinel_keys[i], sentinel_vals[i], sentinel_lens[i]);
    }
    mpt_store_commit_batch(ms);
    mpt_store_flush(ms);

    /* Phase 2: multiple rounds modifying only active keys */
    uint32_t rng = 54321;
    key_tracker_t active;
    tracker_init(&active, T9_ACTIVE);
    uint32_t next_active = 0;

    for (uint32_t round = 0; round < T9_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        /* Insert new active keys (offset by T9_SENTINELS to avoid overlap) */
        uint32_t ins = T9_ACTIVE / T9_ROUNDS;
        for (uint32_t i = 0; i < ins && next_active < T9_ACTIVE; i++) {
            uint32_t idx = next_active++;
            active.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(T9_SENTINELS + idx, key);  /* offset keys */
            make_value(T9_SENTINELS + idx, active.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        /* Update some active keys */
        for (uint32_t i = 0; i < 50 && next_active > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_active;
            if (active.gens[idx] == 0) continue;
            active.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(T9_SENTINELS + idx, key);
            make_value(T9_SENTINELS + idx, active.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        /* Delete some active keys */
        for (uint32_t i = 0; i < 30 && next_active > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_active;
            if (active.gens[idx] == 0) continue;
            active.gens[idx] = 0;
            uint8_t key[32];
            make_key(T9_SENTINELS + idx, key);
            mpt_store_delete(ms, key);
        }

        mpt_store_commit_batch(ms);

        /* Checkpoint flow */
        mpt_store_flush_wait(ms);
        mpt_store_flush_bg(ms);
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        /* Verify ALL sentinels are still intact */
        int errors = 0;
        for (uint32_t i = 0; i < T9_SENTINELS; i++) {
            uint8_t got[VAL_BUF];
            uint32_t got_len = mpt_store_get(ms, sentinel_keys[i], got, VAL_BUF);
            if (got_len != (uint32_t)sentinel_lens[i] ||
                memcmp(got, sentinel_vals[i], sentinel_lens[i]) != 0) {
                if (errors < 5) {
                    fprintf(stderr, "  FAIL [round %u] sentinel %u: got_len=%u expected=%zu\n",
                            round, i, got_len, sentinel_lens[i]);
                    fprintf(stderr, "    key_hash=");
                    print_hash(sentinel_keys[i]);
                    fprintf(stderr, "\n");
                }
                errors++;
            }
        }
        if (errors > 0) {
            fprintf(stderr, "ABORT: %d sentinels lost at round %u\n", errors, round);
            mpt_store_destroy(ms);
            tracker_free(&active);
            return 1;
        }
    }

    /* Final: reopen and verify sentinels */
    mpt_store_destroy(ms);
    ms = mpt_store_open(STORE_PATH);
    if (!ms) { printf("  FAIL: reopen\n"); tracker_free(&active); return 1; }

    int errors = 0;
    for (uint32_t i = 0; i < T9_SENTINELS; i++) {
        uint8_t got[VAL_BUF];
        uint32_t got_len = mpt_store_get(ms, sentinel_keys[i], got, VAL_BUF);
        if (got_len != (uint32_t)sentinel_lens[i] ||
            memcmp(got, sentinel_vals[i], sentinel_lens[i]) != 0) {
            if (errors < 5) {
                fprintf(stderr, "  FAIL [reopen] sentinel %u: got_len=%u expected=%zu\n",
                        i, got_len, sentinel_lens[i]);
            }
            errors++;
        }
    }
    mpt_store_destroy(ms);
    tracker_free(&active);

    if (errors > 0) {
        fprintf(stderr, "ABORT: %d sentinels lost after reopen\n", errors);
        return 1;
    }

    printf("  PASS (%d sentinels survived %d rounds + reopen)\n", T9_SENTINELS, T9_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 10: Heavy collapse_branch stress
 *
 * Create a dense trie, then systematically delete keys to force
 * collapse_branch at many depths. The collapse_branch path deletes a
 * child node and re-creates it with make_branch — if the same hash
 * ends up in both def_entries and def_deletes, data is lost.
 *
 * After each round of deletes, verify ALL remaining keys.
 * ========================================================================= */

#define T10_INITIAL   5000
#define T10_ROUNDS    30
#define T10_DELETE_PER 150

static int test_collapse_stress(void) {
    printf("Test 10: collapse_branch stress (%d initial, delete %d/round)...\n",
           T10_INITIAL, T10_DELETE_PER);
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T10_INITIAL);

    /* Bulk insert */
    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < T10_INITIAL; i++) {
        tracker.gens[i] = 1;
        uint8_t key[32], val[VAL_BUF];
        size_t vlen;
        make_key(i, key);
        make_value(i, 1, val, &vlen);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);
    mpt_store_flush(ms);

    /* Verify initial state */
    int errors = verify_all(ms, &tracker, "initial", 0);
    if (errors > 0) {
        mpt_store_destroy(ms);
        tracker_free(&tracker);
        return 1;
    }

    /* Delete keys in order (sequential deletes hit the same branch paths
     * repeatedly, maximizing collapse_branch triggers) */
    uint32_t del_cursor = 0;

    for (uint32_t round = 0; round < T10_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        for (uint32_t i = 0; i < T10_DELETE_PER && del_cursor < T10_INITIAL; i++) {
            if (tracker.gens[del_cursor] == 0) { del_cursor++; i--; continue; }
            tracker.gens[del_cursor] = 0;
            uint8_t key[32];
            make_key(del_cursor, key);
            mpt_store_delete(ms, key);
            del_cursor++;
        }

        if (del_cursor >= T10_INITIAL) {
            mpt_store_discard_batch(ms);
            break;
        }

        mpt_store_commit_batch(ms);

        /* Checkpoint flow */
        mpt_store_flush_wait(ms);
        mpt_store_flush_bg(ms);
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        errors = verify_all(ms, &tracker, "collapse", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: collapse round %u failed\n", round);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }
    }

    /* Reopen and verify */
    mpt_store_destroy(ms);
    ms = mpt_store_open(STORE_PATH);
    if (!ms) { printf("  FAIL: reopen\n"); tracker_free(&tracker); return 1; }

    errors = verify_all(ms, &tracker, "collapse-reopen", T10_ROUNDS);
    mpt_store_destroy(ms);
    tracker_free(&tracker);
    if (errors > 0) return 1;

    printf("  PASS (survived %u sequential deletes + reopen)\n", del_cursor);
    return 0;
}

/* =========================================================================
 * Test 11: commit_batch during active bg_flush (concurrent snapshot read)
 *
 * This tests the most dangerous race: commit_batch's update_subtrie needs
 * to load nodes that are in the bg_flush snapshot (previous batch wrote
 * them, bg thread is still writing to disk). The read-through must find
 * them in the snapshot.
 *
 * Procedure:
 *   1. Insert keys, commit, flush_bg (no flush_wait)
 *   2. Immediately insert MORE keys, commit again
 *   3. flush_wait + flush_bg
 *   4. Evict cache, verify ALL keys
 * ========================================================================= */

#define T11_KEYS_PER  1000
#define T11_ROUNDS    15

static int test_commit_during_bg(void) {
    printf("Test 11: commit_batch during active bg_flush (%d rounds)...\n", T11_ROUNDS);
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T11_KEYS_PER * T11_ROUNDS * 2);
    uint32_t next_key = 0;

    for (uint32_t round = 0; round < T11_ROUNDS; round++) {
        /* Batch A: insert keys */
        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < T11_KEYS_PER; i++) {
            uint32_t idx = next_key++;
            if (idx >= tracker.count) break;
            tracker.gens[idx] = round * 2 + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);

        /* Start bg flush for batch A (do NOT wait) */
        mpt_store_flush_wait(ms);  /* wait for previous round's bg flush */
        mpt_store_flush_bg(ms);

        /* Batch B: insert MORE keys while bg thread from A is running */
        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < T11_KEYS_PER; i++) {
            uint32_t idx = next_key++;
            if (idx >= tracker.count) break;
            tracker.gens[idx] = round * 2 + 2;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);
        /* Batch B's commit_batch may need to load nodes from A's snapshot */

        /* Now flush B */
        mpt_store_flush_wait(ms);  /* wait for A's bg thread */
        mpt_store_flush_bg(ms);    /* start bg flush for B */

        /* Evict cache and verify everything */
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);

        int errors = verify_all(ms, &tracker, "concurrent-bg", round);
        if (errors > 0) {
            fprintf(stderr, "ABORT: concurrent bg round %u failed\n", round);
            mpt_store_destroy(ms);
            tracker_free(&tracker);
            return 1;
        }
    }

    /* Final reopen verify */
    mpt_store_destroy(ms);
    ms = mpt_store_open(STORE_PATH);
    if (!ms) { printf("  FAIL: reopen\n"); tracker_free(&tracker); return 1; }

    int errors = verify_all(ms, &tracker, "concurrent-reopen", T11_ROUNDS);
    mpt_store_destroy(ms);
    tracker_free(&tracker);
    if (errors > 0) return 1;

    printf("  PASS (%d rounds of concurrent commit+bg_flush)\n", T11_ROUNDS);
    return 0;
}

/* =========================================================================
 * Test 12: Full trie integrity walk after sustained operations
 *
 * After many rounds of operations, walk every reachable node in the trie
 * to verify nothing is missing. Uses mpt_store_walk_leaves to touch
 * every node from root to every leaf.
 * ========================================================================= */

#define T12_MAX_KEYS  5000
#define T12_ROUNDS    20
#define T12_INSERT    200
#define T12_DELETE     80

typedef struct {
    uint32_t count;
} leaf_counter_t;

static bool count_leaf_cb(const uint8_t *value, size_t value_len, void *ud) {
    (void)value; (void)value_len;
    leaf_counter_t *lc = (leaf_counter_t *)ud;
    lc->count++;
    return true;
}

static int test_integrity_walk(void) {
    printf("Test 12: full trie integrity walk after sustained ops...\n");
    cleanup();

    mpt_store_t *ms = mpt_store_create(STORE_PATH, CAPACITY_HINT);
    if (!ms) { printf("  FAIL: cannot create\n"); return 1; }

    key_tracker_t tracker;
    tracker_init(&tracker, T12_MAX_KEYS);
    uint32_t next_key = 0;
    uint32_t rng = 77777;
    uint32_t expected_live = 0;

    for (uint32_t round = 0; round < T12_ROUNDS; round++) {
        mpt_store_begin_batch(ms);

        uint32_t ins = T12_INSERT;
        if (next_key + ins > T12_MAX_KEYS) ins = T12_MAX_KEYS - next_key;
        for (uint32_t i = 0; i < ins; i++) {
            uint32_t idx = next_key++;
            tracker.gens[idx] = round + 1;
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(idx, key);
            make_value(idx, tracker.gens[idx], val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }

        for (uint32_t i = 0; i < T12_DELETE && next_key > 0; i++) {
            rng = rng * 1103515245 + 12345;
            uint32_t idx = rng % next_key;
            if (tracker.gens[idx] == 0) continue;
            tracker.gens[idx] = 0;
            uint8_t key[32];
            make_key(idx, key);
            mpt_store_delete(ms, key);
        }

        mpt_store_commit_batch(ms);

        /* Checkpoint flow */
        mpt_store_flush_wait(ms);
        mpt_store_flush_bg(ms);
        mpt_store_set_cache(ms, 0);
        mpt_store_set_cache_mb(ms, 64);
    }

    /* Count expected live entries */
    expected_live = 0;
    for (uint32_t i = 0; i < tracker.count; i++)
        if (tracker.gens[i] > 0) expected_live++;

    /* Full integrity walk — this loads EVERY node in the trie */
    mpt_store_flush_wait(ms);  /* ensure everything on disk */

    leaf_counter_t lc = { .count = 0 };
    bool walk_ok = mpt_store_walk_leaves(ms, count_leaf_cb, &lc);
    if (!walk_ok) {
        fprintf(stderr, "  FAIL: walk_leaves returned false (missing node!)\n");
        mpt_store_destroy(ms);
        tracker_free(&tracker);
        return 1;
    }

    if (lc.count != expected_live) {
        fprintf(stderr, "  FAIL: walk found %u leaves, expected %u\n",
                lc.count, expected_live);
        mpt_store_destroy(ms);
        tracker_free(&tracker);
        return 1;
    }

    /* Also verify every key individually */
    int errors = verify_all(ms, &tracker, "integrity", T12_ROUNDS);
    mpt_store_destroy(ms);
    tracker_free(&tracker);
    if (errors > 0) return 1;

    printf("  PASS (walked %u leaves, all nodes intact)\n", expected_live);
    return 0;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    printf("MPT Store Stress Test — Checkpoint Flow\n");
    printf("========================================\n");

    int failures = 0;
    failures += test_checkpoint_cycle();
    failures += test_high_churn();
    failures += test_bg_no_wait_readthrough();
    failures += test_reopen_after_bg();
    failures += test_full_lifecycle();
    failures += test_sustained_checkpoint();
    failures += test_failed_block_reopen();
    failures += test_multi_reopen_sustained();
    failures += test_sentinel_survival();
    failures += test_collapse_stress();
    failures += test_commit_during_bg();
    failures += test_integrity_walk();

    cleanup();

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("\n%s (%d test%s failed, %.1fs elapsed)\n",
           failures == 0 ? "ALL PASSED" : "FAILED",
           failures, failures == 1 ? "" : "s", elapsed);
    return failures > 0 ? 1 : 0;
}
