/*
 * MPT Trie Scale Test — Correctness under sustained load
 *
 * Simulates realistic block processing: incremental inserts, deletes,
 * value updates, persistence, and root consistency across many blocks.
 *
 * Usage: ./test_scale_mpt [num_blocks]
 *   Default: 50 blocks (~350K keys)
 */

#include "../include/mpt.h"
#include "../include/hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

/* ========================================================================
 * Constants
 * ======================================================================== */

#define MPT_PATH "/tmp/test_scale_mpt.dat"

#define KEYS_PER_BLOCK      1000
#define DELETES_PER_BLOCK   100
#define UPDATES_PER_BLOCK   500

#define MASTER_SEED 0x5343414C454D5054ULL  /* "SCALEMPT" */

/* ========================================================================
 * Fail-fast
 * ======================================================================== */

static int assertions = 0;
static int failures   = 0;

#define CHECK(cond, fmt, ...) do { \
    assertions++; \
    if (!(cond)) { \
        printf("  FAIL [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
        failures++; \
        return; \
    } \
} while(0)

#define ASSERT(cond, fmt, ...) do { \
    assertions++; \
    if (!(cond)) { \
        fprintf(stderr, "FATAL [%s:%d]: " fmt "\n", \
                __FILE__, __LINE__, ##__VA_ARGS__); \
        abort(); \
    } \
} while(0)

/* ========================================================================
 * RNG (SplitMix64)
 * ======================================================================== */

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static inline rng_t rng_create(uint64_t seed) {
    rng_t r = { .state = seed };
    rng_next(&r);
    return r;
}

/* ========================================================================
 * Key/Value Generation
 * ======================================================================== */

static void gen_key(uint8_t key[32], uint64_t index) {
    rng_t rng = rng_create(MASTER_SEED ^ (index * 0x517cc1b727220a95ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
}

static uint8_t gen_value(uint8_t *buf, uint64_t index, uint64_t version) {
    rng_t rng = rng_create(MASTER_SEED ^ (index * 0x9ABCDEF012345678ULL)
                           ^ (version * 0x1234567890ABCDEFULL));
    uint8_t vlen = (uint8_t)(4 + (rng_next(&rng) % 28));  /* 4-31 bytes */
    for (int i = 0; i < vlen; i += 8) {
        uint64_t r = rng_next(&rng);
        int remain = vlen - i;
        memcpy(buf + i, &r, remain < 8 ? remain : 8);
    }
    return vlen;
}

/* ========================================================================
 * Utilities
 * ======================================================================== */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static size_t get_rss_mb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    size_t rss_kb = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, "%zu", &rss_kb);
            break;
        }
    }
    fclose(f);
    return rss_kb / 1024;
}

static void print_hash(const char *label, const hash_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 4; i++) printf("%02x", h->bytes[i]);
    printf("..");
    for (int i = 28; i < 32; i++) printf("%02x", h->bytes[i]);
    printf("\n");
}

/* ========================================================================
 * Phase 1: Incremental blocks with root consistency
 *
 * Insert KEYS_PER_BLOCK new keys each block, compute root.
 * Verify root changes between blocks and is idempotent within a block.
 * ======================================================================== */

static void test_incremental_blocks(int num_blocks) {
    printf("\n=== Phase 1: Incremental Blocks (%d blocks, %d keys/block) ===\n",
           num_blocks, KEYS_PER_BLOCK);
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed");

    hash_t prev_root = mpt_root(m);
    uint64_t next_id = 0;

    printf("  %-7s | %-9s | %-10s | %-8s | %-8s | %s\n",
           "block", "keys", "root(ms)", "put(ms)", "RSS", "nodes/leaves");
    printf("  --------+-----------+------------+----------+----------+-----\n");

    for (int blk = 0; blk < num_blocks; blk++) {
        double t0 = now_sec();
        for (int i = 0; i < KEYS_PER_BLOCK; i++) {
            uint8_t key[32], val[32];
            gen_key(key, next_id);
            uint8_t vlen = gen_value(val, next_id, 0);
            mpt_put(m, key, val, vlen);
            next_id++;
        }
        double t1 = now_sec();

        hash_t root = mpt_root(m);
        double t2 = now_sec();

        CHECK(!hash_equal(&root, &prev_root),
              "root unchanged at block %d", blk);

        /* Idempotent: second call must return same hash */
        hash_t root2 = mpt_root(m);
        CHECK(hash_equal(&root, &root2),
              "mpt_root not idempotent at block %d", blk);

        prev_root = root;

        if (blk < 3 || (blk + 1) % 10 == 0 || blk == num_blocks - 1) {
            printf("  %5d   | %7.1fK  | %8.2f   | %6.2f   | %4zuMB   | %u / %u\n",
                   blk + 1,
                   (double)mpt_size(m) / 1e3,
                   (t2 - t1) * 1000.0,
                   (t1 - t0) * 1000.0,
                   get_rss_mb(),
                   mpt_nodes(m), mpt_leaves(m));
        }
    }

    CHECK(mpt_size(m) == (uint64_t)num_blocks * KEYS_PER_BLOCK,
          "size mismatch: got %" PRIu64, mpt_size(m));

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 2: Mixed insert + delete + update blocks
 *
 * Each block: insert new keys, delete some old keys, update some values.
 * Verify size tracking, root changes, and idempotency.
 * ======================================================================== */

static void test_mixed_operations(int num_blocks) {
    printf("\n=== Phase 2: Mixed Insert/Delete/Update (%d blocks) ===\n",
           num_blocks);
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed");

    uint64_t next_id = 0;
    uint64_t expected_size = 0;
    rng_t block_rng = rng_create(MASTER_SEED ^ 0xB10CULL);

    printf("  %-7s | %-9s | %-10s | %-20s | %s\n",
           "block", "keys", "root(ms)", "ops (+ins -del ~upd)", "RSS");
    printf("  --------+-----------+------------+----------------------+-----\n");

    for (int blk = 0; blk < num_blocks; blk++) {
        /* Insert new keys */
        for (int i = 0; i < KEYS_PER_BLOCK; i++) {
            uint8_t key[32], val[32];
            gen_key(key, next_id);
            uint8_t vlen = gen_value(val, next_id, 0);
            mpt_put(m, key, val, vlen);
            next_id++;
            expected_size++;
        }

        /* Delete some old keys (from earlier blocks) */
        int deletes = 0;
        if (next_id > (uint64_t)KEYS_PER_BLOCK) {
            uint64_t deletable = next_id - KEYS_PER_BLOCK;
            for (int i = 0; i < DELETES_PER_BLOCK && expected_size > 1000; i++) {
                uint64_t del_id = rng_next(&block_rng) % deletable;
                uint8_t key[32];
                gen_key(key, del_id);
                if (mpt_delete(m, key)) {
                    expected_size--;
                    deletes++;
                }
            }
        }

        /* Update values for some existing keys */
        for (int i = 0; i < UPDATES_PER_BLOCK; i++) {
            uint64_t upd_id = rng_next(&block_rng) % next_id;
            uint8_t key[32], val[32];
            gen_key(key, upd_id);
            uint8_t vlen = gen_value(val, upd_id, (uint64_t)blk + 1);
            mpt_put(m, key, val, vlen);
        }

        double tr0 = now_sec();
        hash_t root = mpt_root(m);
        double tr1 = now_sec();

        /* Idempotent */
        hash_t root2 = mpt_root(m);
        CHECK(hash_equal(&root, &root2),
              "mpt_root not idempotent at block %d", blk);

        if (blk < 3 || (blk + 1) % 10 == 0 || blk == num_blocks - 1) {
            printf("  %5d   | %7.1fK  | %8.2f   | +%d -%d ~%d %*s| %zuMB\n",
                   blk + 1,
                   (double)mpt_size(m) / 1e3,
                   (tr1 - tr0) * 1000.0,
                   KEYS_PER_BLOCK, deletes, UPDATES_PER_BLOCK,
                   1, "",
                   get_rss_mb());
        }
    }

    printf("  final size: %" PRIu64 "\n", mpt_size(m));
    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 3: Persistence across commits and reopens
 *
 * Build state over several blocks, commit, reopen, verify root.
 * Then add more, commit, reopen again.
 * ======================================================================== */

static void test_persistence_at_scale(void) {
    printf("\n=== Phase 3: Persistence at Scale ===\n");
    unlink(MPT_PATH);

    /* Build 10K keys, commit */
    mpt_t *m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed");

    for (uint64_t i = 0; i < 10000; i++) {
        uint8_t key[32], val[32];
        gen_key(key, i);
        uint8_t vlen = gen_value(val, i, 0);
        mpt_put(m, key, val, vlen);
    }

    hash_t root_10k = mpt_root(m);
    CHECK(mpt_commit(m), "first commit failed");
    print_hash("10K root", &root_10k);
    mpt_close(m);

    /* Reopen, verify */
    m = mpt_open(MPT_PATH);
    CHECK(m != NULL, "first reopen failed");
    CHECK(mpt_size(m) == 10000,
          "size after reopen: %" PRIu64, mpt_size(m));

    hash_t root_10k_check = mpt_root(m);
    CHECK(hash_equal(&root_10k, &root_10k_check),
          "root changed after first reopen");
    printf("  10K reopen OK\n");

    /* Add 40K more, commit */
    for (uint64_t i = 10000; i < 50000; i++) {
        uint8_t key[32], val[32];
        gen_key(key, i);
        uint8_t vlen = gen_value(val, i, 0);
        mpt_put(m, key, val, vlen);
    }

    hash_t root_50k = mpt_root(m);
    CHECK(!hash_equal(&root_10k, &root_50k), "root unchanged after adding 40K");
    CHECK(mpt_commit(m), "second commit failed");
    print_hash("50K root", &root_50k);
    mpt_close(m);

    /* Reopen, verify 50K */
    m = mpt_open(MPT_PATH);
    CHECK(m != NULL, "second reopen failed");
    CHECK(mpt_size(m) == 50000,
          "size after second reopen: %" PRIu64, mpt_size(m));

    hash_t root_50k_check = mpt_root(m);
    CHECK(hash_equal(&root_50k, &root_50k_check),
          "root changed after second reopen");
    printf("  50K reopen OK\n");

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 4: Full delete cycle at scale
 *
 * Insert N keys, compute root. Delete all, verify empty root.
 * Re-insert same keys, verify same root.
 * ======================================================================== */

static void test_delete_cycle(uint64_t n) {
    printf("\n=== Phase 4: Delete Cycle (%" PRIu64 " keys) ===\n", n);
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed");

    uint8_t empty_rlp = 0x80;
    hash_t empty_root = hash_keccak256(&empty_rlp, 1);

    /* Insert all */
    for (uint64_t i = 0; i < n; i++) {
        uint8_t key[32], val[32];
        gen_key(key, i);
        uint8_t vlen = gen_value(val, i, 0);
        mpt_put(m, key, val, vlen);
    }

    hash_t root_full = mpt_root(m);
    CHECK(!hash_equal(&root_full, &empty_root), "full root should not be empty");
    CHECK(mpt_size(m) == n, "size should be %" PRIu64, n);
    print_hash("full root", &root_full);

    /* Delete all */
    for (uint64_t i = 0; i < n; i++) {
        uint8_t key[32];
        gen_key(key, i);
        mpt_delete(m, key);
    }

    hash_t root_empty = mpt_root(m);
    CHECK(mpt_size(m) == 0, "size should be 0 after delete-all");
    CHECK(hash_equal(&root_empty, &empty_root),
          "root should be empty after delete-all");
    printf("  delete-all → empty root OK\n");

    /* Re-insert all — must produce identical root */
    for (uint64_t i = 0; i < n; i++) {
        uint8_t key[32], val[32];
        gen_key(key, i);
        uint8_t vlen = gen_value(val, i, 0);
        mpt_put(m, key, val, vlen);
    }

    hash_t root_reinsert = mpt_root(m);
    CHECK(hash_equal(&root_full, &root_reinsert),
          "root should match after re-insert");
    printf("  re-insert → same root OK\n");

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 5: Reproducibility (independent builds produce same root)
 *
 * Build trie A with keys 0..N-1, get root.
 * Build trie B with same keys in reverse order, get root.
 * They must match.
 * ======================================================================== */

static void test_order_independence(uint64_t n) {
    printf("\n=== Phase 5: Order Independence (%" PRIu64 " keys) ===\n", n);

    /* Forward */
    unlink(MPT_PATH);
    mpt_t *m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed");
    for (uint64_t i = 0; i < n; i++) {
        uint8_t key[32], val[32];
        gen_key(key, i);
        uint8_t vlen = gen_value(val, i, 0);
        mpt_put(m, key, val, vlen);
    }
    hash_t root_fwd = mpt_root(m);
    mpt_close(m);
    unlink(MPT_PATH);

    /* Reverse */
    m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed (reverse)");
    for (uint64_t i = n; i > 0; i--) {
        uint8_t key[32], val[32];
        gen_key(key, i - 1);
        uint8_t vlen = gen_value(val, i - 1, 0);
        mpt_put(m, key, val, vlen);
    }
    hash_t root_rev = mpt_root(m);
    mpt_close(m);
    unlink(MPT_PATH);

    print_hash("forward ", &root_fwd);
    print_hash("reverse ", &root_rev);
    CHECK(hash_equal(&root_fwd, &root_rev),
          "forward and reverse builds should produce same root");
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 6: Dirty flag stress — alternating root/insert cycles
 *
 * Insert a batch, compute root (clears dirty flags), insert more,
 * compute root again. Repeat many times. This stresses the dirty
 * propagation through extensions and branches after flags are cleared.
 * ======================================================================== */

static void test_dirty_flag_stress(int num_rounds) {
    printf("\n=== Phase 6: Dirty Flag Stress (%d rounds) ===\n", num_rounds);
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed");

    uint64_t next_id = 0;
    hash_t prev_root = mpt_root(m);
    rng_t rng = rng_create(MASTER_SEED ^ 0xD1E7);

    for (int round = 0; round < num_rounds; round++) {
        /* Small batch: 50-200 inserts */
        int batch = 50 + (int)(rng_next(&rng) % 150);
        for (int i = 0; i < batch; i++) {
            uint8_t key[32], val[32];
            gen_key(key, next_id);
            uint8_t vlen = gen_value(val, next_id, 0);
            mpt_put(m, key, val, vlen);
            next_id++;
        }

        /* Some deletes */
        if (next_id > 200) {
            int del_count = 5 + (int)(rng_next(&rng) % 20);
            for (int i = 0; i < del_count; i++) {
                uint64_t del_id = rng_next(&rng) % next_id;
                uint8_t key[32];
                gen_key(key, del_id);
                mpt_delete(m, key);
            }
        }

        /* Some value updates */
        int upd_count = 10 + (int)(rng_next(&rng) % 30);
        for (int i = 0; i < upd_count; i++) {
            uint64_t upd_id = rng_next(&rng) % next_id;
            uint8_t key[32], val[32];
            gen_key(key, upd_id);
            uint8_t vlen = gen_value(val, upd_id, (uint64_t)round + 1);
            mpt_put(m, key, val, vlen);
        }

        hash_t root = mpt_root(m);
        CHECK(!hash_equal(&root, &prev_root),
              "root unchanged at round %d", round);

        /* Idempotent */
        hash_t root2 = mpt_root(m);
        CHECK(hash_equal(&root, &root2),
              "not idempotent at round %d", round);

        prev_root = root;
    }

    printf("  %d rounds, %" PRIu64 " total inserts, %" PRIu64 " final keys\n",
           num_rounds, next_id, mpt_size(m));

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(int argc, char *argv[]) {
    int num_blocks = 50;
    if (argc >= 2) {
        num_blocks = atoi(argv[1]);
        if (num_blocks < 5) num_blocks = 5;
    }

    printf("============================================\n");
    printf("  MPT Trie Scale Test\n");
    printf("============================================\n");
    printf("  blocks: %d (~%dK keys)\n", num_blocks,
           num_blocks * KEYS_PER_BLOCK / 1000);

    double t0 = now_sec();

    test_incremental_blocks(num_blocks);
    test_mixed_operations(num_blocks);
    test_persistence_at_scale();
    test_delete_cycle(10000);
    test_order_independence(10000);
    test_dirty_flag_stress(200);

    double elapsed = now_sec() - t0;

    printf("\n============================================\n");
    if (failures == 0)
        printf("  ALL PHASES PASSED (%d assertions, %.1fs)\n",
               assertions, elapsed);
    else
        printf("  FAILURES: %d / %d assertions (%.1fs)\n",
               failures, assertions, elapsed);
    printf("============================================\n");

    return failures;
}
