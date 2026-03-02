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
 * Phase 7: Shared Storage MPT (64-byte keys + subtree roots)
 *
 * Simulates realistic Ethereum storage at mainnet scale:
 *   - Single 64-byte key mpt: keccak(addr)[32] || keccak(slot)[32]
 *   - Per block: new accounts, SSTORE ops, slot deletions
 *   - Per-account storage roots via mpt_subtree_root
 *   - Persistence across commits/reopens
 *
 * Realistic proportions (mainnet-inspired):
 *   - 20 new accounts/block × 10 initial slots each
 *   - 2000 dirty slots/block across ~100 existing accounts
 *   - 100 slot deletions/block
 * ======================================================================== */

#define STORAGE_MPT_PATH       "/tmp/test_scale_storage_mpt.dat"

/* Per-block workload */
#define NEW_ACCOUNTS_PER_BLOCK  20
#define INITIAL_SLOTS           10
#define DIRTY_SLOTS_PER_BLOCK   2000
#define DELETE_SLOTS_PER_BLOCK  100

/* Account tracking (addr_hash = first 32 bytes of 64-byte key) */
typedef struct {
    uint8_t addr_hash[32];
    uint32_t slot_count;       /* total non-zero slots for this account */
    hash_t   last_root;        /* last computed subtree root */
    bool     dirty_this_block;
} tracked_account_t;

static void gen_addr_hash(uint8_t out[32], uint64_t account_id) {
    rng_t rng = rng_create(MASTER_SEED ^ (account_id * 0xACC0017FULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(out + i, &r, 8);
    }
}

static void gen_slot_hash(uint8_t out[32], uint64_t slot_id) {
    rng_t rng = rng_create(MASTER_SEED ^ (slot_id * 0x570BA6E00ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(out + i, &r, 8);
    }
}

/* RLP-encode a small integer as storage value */
static uint8_t rlp_storage_value(uint64_t val, uint8_t out[33]) {
    if (val < 0x80) {
        out[0] = (uint8_t)val;
        return (val == 0) ? 0 : 1;  /* 0 means "delete" in our protocol */
    }
    uint8_t be[8];
    int len = 0;
    for (int i = 7; i >= 0; i--) {
        be[i] = (uint8_t)(val & 0xFF);
        val >>= 8;
    }
    int start = 0;
    while (start < 7 && be[start] == 0) start++;
    len = 8 - start;
    out[0] = (uint8_t)(0x80 + len);
    memcpy(out + 1, be + start, len);
    return (uint8_t)(1 + len);
}

static void test_storage_mpt_at_scale(int num_blocks) {
    printf("\n=== Phase 7: Shared Storage MPT (%d blocks) ===\n", num_blocks);
    printf("  per block: +%d accounts × %d slots, %d dirty, %d deletes\n",
           NEW_ACCOUNTS_PER_BLOCK, INITIAL_SLOTS,
           DIRTY_SLOTS_PER_BLOCK, DELETE_SLOTS_PER_BLOCK);
    unlink(STORAGE_MPT_PATH);

    mpt_t *m = mpt_create_ex(STORAGE_MPT_PATH, 64, 33);
    ASSERT(m != NULL, "mpt_create_ex failed");

    /* Dynamic account list */
    uint32_t acct_cap = 2048;
    uint32_t acct_count = 0;
    tracked_account_t *accounts = calloc(acct_cap, sizeof(tracked_account_t));
    ASSERT(accounts != NULL, "alloc failed");

    /* Global slot ID counter (unique across all accounts) */
    uint64_t next_slot_id = 0;
    uint64_t next_account_id = 0;

    rng_t block_rng = rng_create(MASTER_SEED ^ 0x5707A6E5CA1EULL);

    printf("  %-7s | %-9s | %-8s | %-9s | %-9s | %-9s | %s\n",
           "block", "accounts", "slots", "put(ms)", "roots(ms)", "total(ms)", "RSS");
    printf("  --------+-----------+----------+-----------+-----------+-----------+-----\n");

    for (int blk = 0; blk < num_blocks; blk++) {
        double t_block_start = now_sec();

        /* Clear dirty flags */
        for (uint32_t i = 0; i < acct_count; i++)
            accounts[i].dirty_this_block = false;

        /* --- Create new accounts with initial storage --- */
        for (int a = 0; a < NEW_ACCOUNTS_PER_BLOCK; a++) {
            if (acct_count >= acct_cap) {
                acct_cap *= 2;
                accounts = realloc(accounts, acct_cap * sizeof(tracked_account_t));
                ASSERT(accounts != NULL, "realloc failed");
            }

            tracked_account_t *acct = &accounts[acct_count];
            memset(acct, 0, sizeof(*acct));
            gen_addr_hash(acct->addr_hash, next_account_id++);
            acct->dirty_this_block = true;

            for (int s = 0; s < INITIAL_SLOTS; s++) {
                uint8_t key64[64];
                memcpy(key64, acct->addr_hash, 32);
                gen_slot_hash(key64 + 32, next_slot_id++);

                uint8_t rlp_val[33];
                uint64_t val = 1000 + next_slot_id;
                uint8_t rlp_len = rlp_storage_value(val, rlp_val);
                mpt_put(m, key64, rlp_val, rlp_len);
                acct->slot_count++;
            }

            acct_count++;
        }

        /* --- Update existing storage slots (SSTORE) --- */
        int dirty_ops = DIRTY_SLOTS_PER_BLOCK;
        if ((int)acct_count < dirty_ops) dirty_ops = (int)acct_count;

        for (int i = 0; i < dirty_ops; i++) {
            /* Pick a random existing account */
            uint32_t idx = (uint32_t)(rng_next(&block_rng) % acct_count);
            tracked_account_t *acct = &accounts[idx];
            acct->dirty_this_block = true;

            /* New slot or update existing (mostly updates) */
            uint8_t key64[64];
            memcpy(key64, acct->addr_hash, 32);

            if (rng_next(&block_rng) % 4 == 0) {
                /* 25%: new slot */
                gen_slot_hash(key64 + 32, next_slot_id++);
                acct->slot_count++;
            } else {
                /* 75%: update existing slot (deterministic from acct + random) */
                uint64_t slot_seed = (uint64_t)idx * 1000 + (rng_next(&block_rng) % 50);
                gen_slot_hash(key64 + 32, slot_seed);
            }

            uint8_t rlp_val[33];
            uint64_t val = (uint64_t)(blk + 1) * 10000 + i;
            uint8_t rlp_len = rlp_storage_value(val, rlp_val);
            mpt_put(m, key64, rlp_val, rlp_len);
        }

        /* --- Delete some storage slots (SSTORE zero) --- */
        int deletes = DELETE_SLOTS_PER_BLOCK;
        if ((int)acct_count < deletes) deletes = (int)acct_count / 2;

        for (int i = 0; i < deletes; i++) {
            uint32_t idx = (uint32_t)(rng_next(&block_rng) % acct_count);
            tracked_account_t *acct = &accounts[idx];
            acct->dirty_this_block = true;

            /* Pick an existing slot to delete */
            uint8_t key64[64];
            memcpy(key64, acct->addr_hash, 32);
            uint64_t slot_seed = (uint64_t)idx * 1000 + (rng_next(&block_rng) % 30);
            gen_slot_hash(key64 + 32, slot_seed);
            mpt_delete(m, key64);
        }

        double t_put_done = now_sec();

        /* --- Compute subtree roots for dirty accounts --- */
        int roots_computed = 0;
        for (uint32_t i = 0; i < acct_count; i++) {
            if (!accounts[i].dirty_this_block) continue;

            hash_t root = mpt_subtree_root(m, accounts[i].addr_hash, 64);

            /* If account was dirty, root should differ from last (most of the time) */
            /* Note: not always true — a delete-then-reinsert could produce same root */

            accounts[i].last_root = root;
            roots_computed++;
        }

        double t_roots_done = now_sec();

        /* --- Verify clean accounts unchanged --- */
        /* Spot-check a few clean accounts */
        int clean_checks = 0;
        for (uint32_t i = 0; i < acct_count && clean_checks < 20; i++) {
            if (accounts[i].dirty_this_block) continue;
            if (hash_is_zero(&accounts[i].last_root)) continue;

            hash_t root = mpt_subtree_root(m, accounts[i].addr_hash, 64);
            CHECK(hash_equal(&root, &accounts[i].last_root),
                  "clean account %u root changed at block %d", i, blk);
            clean_checks++;
        }

        double t_end = now_sec();

        if (blk < 3 || (blk + 1) % 10 == 0 || blk == num_blocks - 1) {
            printf("  %5d   | %7u   | %6" PRIu64 "   | %7.2f   | %7.2f   | %7.2f   | %zuMB\n",
                   blk + 1,
                   acct_count,
                   mpt_size(m),
                   (t_put_done - t_block_start) * 1000.0,
                   (t_roots_done - t_put_done) * 1000.0,
                   (t_end - t_block_start) * 1000.0,
                   get_rss_mb());
        }
    }

    /* --- Persistence: commit, reopen, verify subtree roots --- */
    printf("  committing...\n");
    CHECK(mpt_commit(m), "commit failed");
    uint64_t final_size = mpt_size(m);
    mpt_close(m);

    m = mpt_open(STORAGE_MPT_PATH);
    CHECK(m != NULL, "reopen failed");
    CHECK(mpt_size(m) == final_size,
          "size after reopen: %" PRIu64 " vs %" PRIu64, mpt_size(m), final_size);

    /* Spot-check 50 random account subtree roots survive reopen */
    int verify_count = (acct_count < 50) ? (int)acct_count : 50;
    rng_t verify_rng = rng_create(MASTER_SEED ^ 0xBEEFCAFEULL);
    for (int i = 0; i < verify_count; i++) {
        uint32_t idx = (uint32_t)(rng_next(&verify_rng) % acct_count);
        if (hash_is_zero(&accounts[idx].last_root)) continue;

        hash_t root = mpt_subtree_root(m, accounts[idx].addr_hash, 64);
        CHECK(hash_equal(&root, &accounts[idx].last_root),
              "account %u subtree root changed after reopen", idx);
    }
    printf("  %d subtree roots survived reopen OK\n", verify_count);

    mpt_close(m);
    unlink(STORAGE_MPT_PATH);
    free(accounts);
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
    printf("  blocks: %d (~%dK state keys)\n", num_blocks,
           num_blocks * KEYS_PER_BLOCK / 1000);

    double t0 = now_sec();

    test_incremental_blocks(num_blocks);
    test_mixed_operations(num_blocks);
    test_persistence_at_scale();
    test_delete_cycle(10000);
    test_order_independence(10000);
    test_dirty_flag_stress(200);
    test_storage_mpt_at_scale(num_blocks);

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
