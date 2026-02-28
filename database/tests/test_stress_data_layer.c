/*
 * Data Layer Integration Stress Test
 *
 * Exercises the full pipeline: write buffer + compact_art index + state.dat +
 * code.dat + checkpoint + crash recovery. Simulates realistic Ethereum block
 * processing with hot key patterns and mixed state/code operations.
 *
 * Shadow state (uthash) tracks expected values for fail-fast verification.
 * Fixed seed ensures full reproducibility across runs.
 *
 * Five phases:
 *   1. Genesis: bulk state insert + code deploys + merge + verify
 *   2. Block processing: 200 blocks, mixed ops, periodic checkpoints
 *   3. Crash recovery: destroy + reopen from checkpoint + verify consistency
 *   4. Post-recovery: 50 more blocks, prove system functional
 *   5. Final verification: full scan + determinism hash
 *
 * Usage: ./test_stress_data_layer [scale_factor]
 *   Default: scale=1 (50K genesis accounts, 5K ops/block, 200+50 blocks)
 */

#include "../include/data_layer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

#include "uthash.h"

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE            32
#define STATE_VALUE_LEN     32
#define CODE_MIN_LEN        64
#define CODE_MAX_LEN        2048

#define DEFAULT_GENESIS_ACCOUNTS    50000
#define DEFAULT_GENESIS_CONTRACTS   2000
#define DEFAULT_NUM_BLOCKS_P2       200
#define DEFAULT_NUM_BLOCKS_P4       50
#define DEFAULT_OPS_PER_BLOCK       5000
#define DEFAULT_CHECKPOINT_INTERVAL 50

#define HOT_KEY_POOL_SIZE       500
#define HOT_KEY_PROBABILITY     20      // percent

#define STATE_PATH  "/tmp/stress_dl_state.dat"
#define CODE_PATH   "/tmp/stress_dl_code.dat"
#define INDEX_PATH  "/tmp/stress_dl_index.dat"

#define MASTER_SEED 0x5354524553534454ULL

// ============================================================================
// Fail-fast
// ============================================================================

#define ASSERT_MSG(cond, fmt, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
                __FILE__, __LINE__, ##__VA_ARGS__); \
        abort(); \
    } \
} while(0)

// ============================================================================
// RNG (SplitMix64)
// ============================================================================

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

static void generate_key(uint8_t key[KEY_SIZE], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng), r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng), r3 = rng_next(&rng);
    memcpy(key,      &r0, 8); memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8); memcpy(key + 24, &r3, 8);
}

static void generate_value(uint8_t *buf, size_t len, uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x9c2f0b3a71d8e6f5ULL));
    for (size_t i = 0; i < len; i += 8) {
        uint64_t r = rng_next(&rng);
        size_t remain = len - i;
        memcpy(buf + i, &r, remain < 8 ? remain : 8);
    }
}

static uint32_t code_length_for(uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0xC0DE1E4F00000001ULL));
    return CODE_MIN_LEN + (uint32_t)(rng_next(&rng) % (CODE_MAX_LEN - CODE_MIN_LEN));
}

// ============================================================================
// Utilities
// ============================================================================

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

// ============================================================================
// FNV-1a (determinism hash)
// ============================================================================

#define FNV_OFFSET_BASIS 0xcbf29ce484222325ULL
#define FNV_PRIME        0x100000001b3ULL

static inline uint64_t fnv1a_update(uint64_t hash, const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= FNV_PRIME;
    }
    return hash;
}

// ============================================================================
// Shadow State (ground truth via uthash)
// ============================================================================

typedef struct {
    uint8_t key[KEY_SIZE];
    uint8_t *value;
    uint32_t value_len;
    bool is_code;
    bool deleted;
    UT_hash_handle hh;
} shadow_entry_t;

static shadow_entry_t *g_shadow = NULL;

static void shadow_put(const uint8_t *key, const void *value, uint32_t len,
                       bool is_code) {
    shadow_entry_t *se = NULL;
    HASH_FIND(hh, g_shadow, key, KEY_SIZE, se);
    if (se) {
        free(se->value);
        se->value = malloc(len);
        memcpy(se->value, value, len);
        se->value_len = len;
        se->is_code = is_code;
        se->deleted = false;
    } else {
        se = calloc(1, sizeof(*se));
        memcpy(se->key, key, KEY_SIZE);
        se->value = malloc(len);
        memcpy(se->value, value, len);
        se->value_len = len;
        se->is_code = is_code;
        se->deleted = false;
        HASH_ADD(hh, g_shadow, key, KEY_SIZE, se);
    }
}

static void shadow_delete(const uint8_t *key) {
    shadow_entry_t *se = NULL;
    HASH_FIND(hh, g_shadow, key, KEY_SIZE, se);
    if (se) se->deleted = true;
}

static shadow_entry_t *shadow_get(const uint8_t *key) {
    shadow_entry_t *se = NULL;
    HASH_FIND(hh, g_shadow, key, KEY_SIZE, se);
    if (se && !se->deleted) return se;
    return NULL;
}

static uint64_t shadow_live_count(void) {
    uint64_t count = 0;
    shadow_entry_t *se, *tmp;
    HASH_ITER(hh, g_shadow, se, tmp) {
        if (!se->deleted) count++;
    }
    return count;
}

static void shadow_destroy_map(shadow_entry_t **map) {
    shadow_entry_t *se, *tmp;
    HASH_ITER(hh, *map, se, tmp) {
        HASH_DEL(*map, se);
        free(se->value);
        free(se);
    }
    *map = NULL;
}

// Deep copy for checkpoint snapshot
static shadow_entry_t *shadow_snapshot(void) {
    shadow_entry_t *snap = NULL;
    shadow_entry_t *se, *tmp;
    HASH_ITER(hh, g_shadow, se, tmp) {
        shadow_entry_t *copy = calloc(1, sizeof(*copy));
        memcpy(copy->key, se->key, KEY_SIZE);
        copy->value = malloc(se->value_len);
        memcpy(copy->value, se->value, se->value_len);
        copy->value_len = se->value_len;
        copy->is_code = se->is_code;
        copy->deleted = se->deleted;
        HASH_ADD(hh, snap, key, KEY_SIZE, copy);
    }
    return snap;
}

// Replace global shadow with a snapshot
static void shadow_restore_from(shadow_entry_t *snap) {
    shadow_destroy_map(&g_shadow);
    g_shadow = snap;
}

// ============================================================================
// Verification
// ============================================================================

static void verify_key(data_layer_t *dl, const uint8_t *key, const char *ctx) {
    shadow_entry_t *se = NULL;
    HASH_FIND(hh, g_shadow, key, KEY_SIZE, se);

    if (!se || se->deleted) {
        // Should NOT be found
        uint8_t got[64];
        uint16_t got_len = 0;
        bool found = dl_get(dl, key, got, &got_len);
        ASSERT_MSG(!found, "[%s] dl_get returned true for deleted/absent key", ctx);
        return;
    }

    if (se->is_code) {
        uint8_t *got = malloc(se->value_len + 1);
        uint32_t got_len = 0;
        bool found = dl_get_code(dl, key, got, &got_len);
        ASSERT_MSG(found, "[%s] dl_get_code returned false for expected code key", ctx);
        ASSERT_MSG(got_len == se->value_len,
                   "[%s] code len mismatch: got %u expected %u",
                   ctx, got_len, se->value_len);
        ASSERT_MSG(memcmp(got, se->value, se->value_len) == 0,
                   "[%s] code data mismatch", ctx);
        free(got);
    } else {
        uint8_t got[64];
        uint16_t got_len = 0;
        bool found = dl_get(dl, key, got, &got_len);
        ASSERT_MSG(found, "[%s] dl_get returned false for expected state key", ctx);
        ASSERT_MSG(got_len == (uint16_t)se->value_len,
                   "[%s] state len mismatch: got %u expected %u",
                   ctx, got_len, se->value_len);
        ASSERT_MSG(memcmp(got, se->value, se->value_len) == 0,
                   "[%s] state data mismatch", ctx);
    }
}

// ============================================================================
// Live State Key Tracking (for random update/delete selection)
// ============================================================================

static uint64_t *g_live_indices = NULL;
static uint64_t g_live_count = 0;
static uint64_t g_live_cap = 0;

static void live_add(uint64_t idx) {
    if (g_live_count >= g_live_cap) {
        g_live_cap = g_live_cap ? g_live_cap * 2 : 8192;
        g_live_indices = realloc(g_live_indices, g_live_cap * sizeof(uint64_t));
    }
    g_live_indices[g_live_count++] = idx;
}

static uint64_t live_remove_random(rng_t *rng) {
    if (g_live_count == 0) return UINT64_MAX;
    uint64_t pos = rng_next(rng) % g_live_count;
    uint64_t idx = g_live_indices[pos];
    g_live_indices[pos] = g_live_indices[--g_live_count];
    return idx;
}

// ============================================================================
// Hot Key Pool
// ============================================================================

static uint64_t g_hot_keys[HOT_KEY_POOL_SIZE];

static void init_hot_keys(rng_t *rng, uint64_t total_keys) {
    for (int i = 0; i < HOT_KEY_POOL_SIZE; i++) {
        g_hot_keys[i] = rng_next(rng) % total_keys;
    }
}

static uint64_t pick_key_index(rng_t *rng) {
    if (g_live_count == 0) return UINT64_MAX;
    if ((rng_next(rng) % 100) < HOT_KEY_PROBABILITY) {
        return g_hot_keys[rng_next(rng) % HOT_KEY_POOL_SIZE];
    }
    return g_live_indices[rng_next(rng) % g_live_count];
}

// ============================================================================
// Key generation seeds (separate namespaces for state vs code)
// ============================================================================

#define STATE_KEY_SEED (MASTER_SEED ^ 0x5354415445000000ULL)
#define CODE_KEY_SEED  (MASTER_SEED ^ 0xC0DE000000000000ULL)

// Value seed incorporates a "version" so updates produce different values
static uint64_t value_seed(uint64_t version) {
    return MASTER_SEED ^ (version * 0x56414C5545000000ULL);
}

// ============================================================================
// Phase 1: Genesis Block
// ============================================================================

static uint64_t g_next_state_idx = 0;
static uint64_t g_next_code_idx = 0;

static void phase1_genesis(data_layer_t *dl, uint64_t n_accounts,
                           uint64_t n_contracts) {
    printf("\n========================================\n");
    printf("Phase 1: Genesis Block\n");
    printf("========================================\n");
    printf("  accounts:  %" PRIu64 "\n", n_accounts);
    printf("  contracts: %" PRIu64 "\n\n", n_contracts);

    uint8_t key[KEY_SIZE];
    uint8_t state_val[STATE_VALUE_LEN];
    double t0 = now_sec();

    // Insert state entries
    for (uint64_t i = 0; i < n_accounts; i++) {
        generate_key(key, STATE_KEY_SEED, i);
        generate_value(state_val, STATE_VALUE_LEN, value_seed(0), i);
        bool ok = dl_put(dl, key, state_val, STATE_VALUE_LEN);
        ASSERT_MSG(ok, "dl_put failed at genesis state %" PRIu64, i);
        shadow_put(key, state_val, STATE_VALUE_LEN, false);
        live_add(i);
    }
    g_next_state_idx = n_accounts;

    double t1 = now_sec();
    printf("  state writes: %.3fs (%.1f Kk/s)\n",
           t1 - t0, n_accounts / (t1 - t0) / 1000.0);

    // Deploy contracts (bypasses buffer)
    uint8_t *code_buf = malloc(CODE_MAX_LEN);
    for (uint64_t i = 0; i < n_contracts; i++) {
        generate_key(key, CODE_KEY_SEED, i);
        uint32_t code_len = code_length_for(CODE_KEY_SEED, i);
        generate_value(code_buf, code_len, CODE_KEY_SEED, i);
        bool ok = dl_put_code(dl, key, code_buf, code_len);
        ASSERT_MSG(ok, "dl_put_code failed at genesis contract %" PRIu64, i);
        shadow_put(key, code_buf, code_len, true);
    }
    g_next_code_idx = n_contracts;
    free(code_buf);

    double t2 = now_sec();
    printf("  code deploys: %.3fs\n", t2 - t1);

    // Merge
    uint64_t merged = dl_merge(dl);
    double t3 = now_sec();
    printf("  merge: %" PRIu64 " entries in %.3fs\n", merged, t3 - t2);

    dl_stats_t st = dl_stats(dl);
    ASSERT_MSG(st.buffer_entries == 0, "buffer not empty after merge");
    ASSERT_MSG(st.index_keys == n_accounts + n_contracts,
               "index_keys mismatch: got %" PRIu64 " expected %" PRIu64,
               st.index_keys, n_accounts + n_contracts);

    // Verify samples
    uint64_t verify_count = n_accounts < 5000 ? n_accounts : 5000;
    rng_t vrng = rng_create(MASTER_SEED ^ 0x5645524946590000ULL);
    for (uint64_t i = 0; i < verify_count; i++) {
        uint64_t idx = rng_next(&vrng) % n_accounts;
        generate_key(key, STATE_KEY_SEED, idx);
        verify_key(dl, key, "genesis-state");
    }
    // Verify all code
    for (uint64_t i = 0; i < n_contracts; i++) {
        generate_key(key, CODE_KEY_SEED, i);
        verify_key(dl, key, "genesis-code");
    }

    // Init hot keys
    rng_t hrng = rng_create(MASTER_SEED ^ 0x484F544B45595300ULL);
    init_hot_keys(&hrng, n_accounts);

    printf("  verified: %" PRIu64 " state + %" PRIu64 " code OK\n",
           verify_count, n_contracts);
    printf("  RSS: %zu MB\n", get_rss_mb());
    printf("  Phase 1: PASS\n");
}

// ============================================================================
// Phase 2: Block Processing
// ============================================================================

static uint64_t g_checkpoint_block = 0;
static shadow_entry_t *g_checkpoint_snap = NULL;

static void phase2_blocks(data_layer_t *dl, uint64_t num_blocks,
                          uint64_t ops_per_block, uint64_t ckpt_interval) {
    printf("\n========================================\n");
    printf("Phase 2: Block Processing (%" PRIu64 " blocks)\n", num_blocks);
    printf("========================================\n");
    printf("  ops/block: %" PRIu64 "\n", ops_per_block);
    printf("  checkpoint interval: %" PRIu64 "\n\n", ckpt_interval);

    uint8_t key[KEY_SIZE];
    uint8_t state_val[STATE_VALUE_LEN];
    uint8_t *code_buf = malloc(CODE_MAX_LEN);
    double phase_start = now_sec();

    for (uint64_t b = 0; b < num_blocks; b++) {
        uint64_t block_num = b + 1;  // block 0 was genesis
        rng_t brng = rng_create(MASTER_SEED ^ (block_num * 0x426C6F636B000000ULL));

        uint64_t puts = 0, deletes = 0, codes = 0, reads = 0;
        uint64_t buf_ops = 0;  // ops that go through buffer (puts + deletes)

        for (uint64_t op = 0; op < ops_per_block; op++) {
            uint64_t roll = rng_next(&brng) % 100;

            if (roll < 50) {
                // STATE PUT (50%): update existing or insert new
                bool is_new = (g_live_count == 0) || (rng_next(&brng) % 3 == 0);
                if (is_new) {
                    uint64_t idx = g_next_state_idx++;
                    generate_key(key, STATE_KEY_SEED, idx);
                    generate_value(state_val, STATE_VALUE_LEN,
                                   value_seed(block_num), idx);
                    bool ok = dl_put(dl, key, state_val, STATE_VALUE_LEN);
                    ASSERT_MSG(ok, "dl_put failed block %" PRIu64 " op %" PRIu64,
                               block_num, op);
                    shadow_put(key, state_val, STATE_VALUE_LEN, false);
                    live_add(idx);
                } else {
                    uint64_t idx = pick_key_index(&brng);
                    if (idx != UINT64_MAX) {
                        generate_key(key, STATE_KEY_SEED, idx);
                        generate_value(state_val, STATE_VALUE_LEN,
                                       value_seed(block_num), idx);
                        bool ok = dl_put(dl, key, state_val, STATE_VALUE_LEN);
                        ASSERT_MSG(ok, "dl_put(update) failed block %" PRIu64, block_num);
                        shadow_put(key, state_val, STATE_VALUE_LEN, false);
                    }
                }
                puts++;
                buf_ops++;

            } else if (roll < 65) {
                // DELETE (15%)
                uint64_t idx = live_remove_random(&brng);
                if (idx != UINT64_MAX) {
                    generate_key(key, STATE_KEY_SEED, idx);
                    bool ok = dl_delete(dl, key);
                    ASSERT_MSG(ok, "dl_delete failed block %" PRIu64, block_num);
                    shadow_delete(key);
                    deletes++;
                    buf_ops++;
                }

            } else if (roll < 75) {
                // CODE DEPLOY (10%)
                uint64_t idx = g_next_code_idx++;
                generate_key(key, CODE_KEY_SEED, idx);
                uint32_t code_len = code_length_for(CODE_KEY_SEED, idx);
                generate_value(code_buf, code_len, CODE_KEY_SEED, idx);
                bool ok = dl_put_code(dl, key, code_buf, code_len);
                ASSERT_MSG(ok, "dl_put_code failed block %" PRIu64, block_num);
                shadow_put(key, code_buf, code_len, true);
                codes++;

            } else {
                // READ/VERIFY (25%)
                if (g_live_count > 0) {
                    uint64_t idx = pick_key_index(&brng);
                    if (idx != UINT64_MAX) {
                        generate_key(key, STATE_KEY_SEED, idx);
                        verify_key(dl, key, "block-read");
                    }
                }
                reads++;
            }
        }

        // Merge
        double t0 = now_sec();
        uint64_t merged = dl_merge(dl);
        double t1 = now_sec();
        (void)merged;

        dl_stats_t st = dl_stats(dl);
        ASSERT_MSG(st.buffer_entries == 0,
                   "buffer not empty after merge at block %" PRIu64, block_num);

        // Spot-check 100 keys
        rng_t vrng = rng_create(MASTER_SEED ^ (block_num * 0x5650000000000000ULL));
        for (int i = 0; i < 100 && g_live_count > 0; i++) {
            uint64_t idx = g_live_indices[rng_next(&vrng) % g_live_count];
            generate_key(key, STATE_KEY_SEED, idx);
            verify_key(dl, key, "block-spot");
        }

        // Progress every 10 blocks
        if (block_num % 10 == 0 || block_num == num_blocks) {
            printf("  block %3" PRIu64 " | %4" PRIu64 "p %3" PRIu64 "d %3" PRIu64
                   "c %4" PRIu64 "r | merge %.3fs | index %" PRIu64
                   "K | free %u | RSS %zu MB\n",
                   block_num, puts, deletes, codes, reads, t1 - t0,
                   st.index_keys / 1000, st.free_slots, get_rss_mb());
        }

        // Checkpoint
        if (block_num % ckpt_interval == 0) {
            double ct0 = now_sec();
            bool ok = dl_checkpoint(dl, INDEX_PATH, block_num);
            double ct1 = now_sec();
            ASSERT_MSG(ok, "dl_checkpoint failed at block %" PRIu64, block_num);

            // Snapshot shadow
            if (g_checkpoint_snap) shadow_destroy_map(&g_checkpoint_snap);
            g_checkpoint_snap = shadow_snapshot();
            g_checkpoint_block = block_num;

            printf("  ** CHECKPOINT at block %" PRIu64 " | %.3fs | index %"
                   PRIu64 "K | code %u\n",
                   block_num, ct1 - ct0, st.index_keys / 1000, st.code_count);
        }
    }

    free(code_buf);
    double elapsed = now_sec() - phase_start;
    printf("\n  total: %.1fs (%.0f blocks/s)\n", elapsed, num_blocks / elapsed);
    printf("  state keys generated: %" PRIu64 "\n", g_next_state_idx);
    printf("  code keys generated:  %" PRIu64 "\n", g_next_code_idx);
    printf("  Phase 2: PASS\n");
}

// ============================================================================
// Phase 3: Crash Recovery
// ============================================================================

static void phase3_recovery(data_layer_t **dl_ptr) {
    printf("\n========================================\n");
    printf("Phase 3: Crash Recovery\n");
    printf("========================================\n");

    data_layer_t *dl = *dl_ptr;
    uint8_t key[KEY_SIZE];
    uint8_t state_val[STATE_VALUE_LEN];
    uint8_t *code_buf = malloc(CODE_MAX_LEN);

    // Process 10 extra blocks without checkpoint (creates diverged state)
    printf("  processing 10 post-checkpoint blocks...\n");

    // Track keys added after checkpoint for later verification
    uint64_t post_ckpt_state_start = g_next_state_idx;
    uint64_t post_ckpt_code_start = g_next_code_idx;

    for (uint64_t b = 0; b < 10; b++) {
        uint64_t block_num = g_checkpoint_block + 1 + b;
        rng_t brng = rng_create(MASTER_SEED ^ (block_num * 0x426C6F636B000000ULL));

        for (uint64_t op = 0; op < 1000; op++) {
            uint64_t roll = rng_next(&brng) % 100;
            if (roll < 60) {
                uint64_t idx = g_next_state_idx++;
                generate_key(key, STATE_KEY_SEED, idx);
                generate_value(state_val, STATE_VALUE_LEN,
                               value_seed(block_num), idx);
                dl_put(dl, key, state_val, STATE_VALUE_LEN);
                shadow_put(key, state_val, STATE_VALUE_LEN, false);
                live_add(idx);
            } else if (roll < 80) {
                uint64_t idx = g_next_code_idx++;
                generate_key(key, CODE_KEY_SEED, idx);
                uint32_t code_len = code_length_for(CODE_KEY_SEED, idx);
                generate_value(code_buf, code_len, CODE_KEY_SEED, idx);
                dl_put_code(dl, key, code_buf, code_len);
                shadow_put(key, code_buf, code_len, true);
            }
        }
        dl_merge(dl);
    }

    uint64_t post_ckpt_state_end = g_next_state_idx;
    uint64_t post_ckpt_code_end = g_next_code_idx;

    printf("  post-checkpoint: %" PRIu64 " new state, %" PRIu64 " new code\n",
           post_ckpt_state_end - post_ckpt_state_start,
           post_ckpt_code_end - post_ckpt_code_start);

    // Destroy (simulates crash)
    printf("  destroying data layer (simulating crash)...\n");
    dl_destroy(dl);
    *dl_ptr = NULL;

    // Restore shadow from checkpoint snapshot
    ASSERT_MSG(g_checkpoint_snap != NULL, "no checkpoint snapshot");
    shadow_restore_from(g_checkpoint_snap);
    g_checkpoint_snap = NULL;

    // Reset live indices to match checkpoint state
    // Rebuild from shadow (only non-deleted state entries)
    g_live_count = 0;
    g_next_state_idx = post_ckpt_state_start;  // roll back
    g_next_code_idx = post_ckpt_code_start;

    // Rebuild live_indices: scan all state key indices that are alive
    for (uint64_t i = 0; i < g_next_state_idx; i++) {
        generate_key(key, STATE_KEY_SEED, i);
        shadow_entry_t *se = shadow_get(key);
        if (se && !se->is_code) {
            live_add(i);
        }
    }

    // Recover
    double t0 = now_sec();
    uint64_t recovered_block = 0;
    *dl_ptr = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                       KEY_SIZE, 4, &recovered_block);
    double t1 = now_sec();
    dl = *dl_ptr;
    ASSERT_MSG(dl != NULL, "dl_open failed");
    ASSERT_MSG(recovered_block == g_checkpoint_block,
               "recovered block %" PRIu64 " != checkpoint %" PRIu64,
               recovered_block, g_checkpoint_block);

    printf("  dl_open: %.3fs\n", t1 - t0);
    printf("  recovered block: %" PRIu64 "\n", recovered_block);

    dl_stats_t st = dl_stats(dl);
    uint64_t expected = shadow_live_count();
    printf("  index_keys: %" PRIu64 " (expected %" PRIu64 ")\n",
           st.index_keys, expected);
    ASSERT_MSG(st.index_keys == expected,
               "index_keys mismatch after recovery");

    // Verify checkpoint-era keys
    rng_t vrng = rng_create(MASTER_SEED ^ 0x5245434F56455230ULL);
    uint64_t verified = 0;
    for (uint64_t i = 0; i < 5000 && g_live_count > 0; i++) {
        uint64_t idx = g_live_indices[rng_next(&vrng) % g_live_count];
        generate_key(key, STATE_KEY_SEED, idx);
        verify_key(dl, key, "recovery-state");
        verified++;
    }

    // Verify post-checkpoint state keys are absent
    uint64_t absent_verified = 0;
    for (uint64_t i = post_ckpt_state_start; i < post_ckpt_state_end; i++) {
        generate_key(key, STATE_KEY_SEED, i);
        uint8_t tmp[64];
        uint16_t tmp_len = 0;
        bool found = dl_get(dl, key, tmp, &tmp_len);
        ASSERT_MSG(!found,
                   "post-checkpoint state key %" PRIu64 " found after recovery", i);
        absent_verified++;
    }

    free(code_buf);
    printf("  verified: %" PRIu64 " checkpoint keys OK\n", verified);
    printf("  verified: %" PRIu64 " post-checkpoint keys absent\n", absent_verified);
    printf("  Phase 3: PASS\n");
}

// ============================================================================
// Phase 4: Post-Recovery Blocks
// ============================================================================

static void phase4_post_recovery(data_layer_t *dl, uint64_t num_blocks,
                                 uint64_t ops_per_block) {
    printf("\n========================================\n");
    printf("Phase 4: Post-Recovery Blocks (%" PRIu64 " blocks)\n", num_blocks);
    printf("========================================\n");

    uint8_t key[KEY_SIZE];
    uint8_t state_val[STATE_VALUE_LEN];
    uint8_t *code_buf = malloc(CODE_MAX_LEN);
    double phase_start = now_sec();

    for (uint64_t b = 0; b < num_blocks; b++) {
        uint64_t block_num = g_checkpoint_block + 1 + b;
        rng_t brng = rng_create(MASTER_SEED ^
                                ((block_num + 10000) * 0x426C6F636B000000ULL));

        uint64_t puts = 0, deletes = 0, codes = 0;

        for (uint64_t op = 0; op < ops_per_block; op++) {
            uint64_t roll = rng_next(&brng) % 100;

            if (roll < 50) {
                bool is_new = (g_live_count == 0) || (rng_next(&brng) % 3 == 0);
                if (is_new) {
                    uint64_t idx = g_next_state_idx++;
                    generate_key(key, STATE_KEY_SEED, idx);
                    generate_value(state_val, STATE_VALUE_LEN,
                                   value_seed(block_num), idx);
                    bool ok = dl_put(dl, key, state_val, STATE_VALUE_LEN);
                    ASSERT_MSG(ok, "dl_put failed post-recovery block %" PRIu64,
                               block_num);
                    shadow_put(key, state_val, STATE_VALUE_LEN, false);
                    live_add(idx);
                } else {
                    uint64_t idx = pick_key_index(&brng);
                    if (idx != UINT64_MAX) {
                        generate_key(key, STATE_KEY_SEED, idx);
                        generate_value(state_val, STATE_VALUE_LEN,
                                       value_seed(block_num), idx);
                        bool ok = dl_put(dl, key, state_val, STATE_VALUE_LEN);
                        ASSERT_MSG(ok, "dl_put(update) failed post-recovery");
                        shadow_put(key, state_val, STATE_VALUE_LEN, false);
                    }
                }
                puts++;
            } else if (roll < 65) {
                uint64_t idx = live_remove_random(&brng);
                if (idx != UINT64_MAX) {
                    generate_key(key, STATE_KEY_SEED, idx);
                    dl_delete(dl, key);
                    shadow_delete(key);
                    deletes++;
                }
            } else if (roll < 75) {
                uint64_t idx = g_next_code_idx++;
                generate_key(key, CODE_KEY_SEED, idx);
                uint32_t code_len = code_length_for(CODE_KEY_SEED, idx);
                generate_value(code_buf, code_len, CODE_KEY_SEED, idx);
                dl_put_code(dl, key, code_buf, code_len);
                shadow_put(key, code_buf, code_len, true);
                codes++;
            }
        }

        dl_merge(dl);

        // Spot check
        rng_t vrng = rng_create(MASTER_SEED ^
                                ((block_num + 10000) * 0x5650000000000000ULL));
        for (int i = 0; i < 100 && g_live_count > 0; i++) {
            uint64_t idx = g_live_indices[rng_next(&vrng) % g_live_count];
            generate_key(key, STATE_KEY_SEED, idx);
            verify_key(dl, key, "post-recovery-spot");
        }

        if (block_num % 10 == 0 || b == num_blocks - 1) {
            dl_stats_t st = dl_stats(dl);
            printf("  block %3" PRIu64 " | %4" PRIu64 "p %3" PRIu64 "d %3" PRIu64
                   "c | index %" PRIu64 "K | RSS %zu MB\n",
                   block_num, puts, deletes, codes,
                   st.index_keys / 1000, get_rss_mb());
        }
    }

    // Final checkpoint
    uint64_t final_block = g_checkpoint_block + num_blocks;
    bool ok = dl_checkpoint(dl, INDEX_PATH, final_block);
    ASSERT_MSG(ok, "final checkpoint failed");

    free(code_buf);
    double elapsed = now_sec() - phase_start;
    printf("\n  total: %.1fs (%.0f blocks/s)\n", elapsed, num_blocks / elapsed);
    printf("  checkpoint at block %" PRIu64 "\n", final_block);
    printf("  Phase 4: PASS\n");
}

// ============================================================================
// Phase 5: Final Verification
// ============================================================================

static int key_compare(const void *a, const void *b) {
    return memcmp(a, b, KEY_SIZE);
}

static void phase5_final(data_layer_t *dl) {
    printf("\n========================================\n");
    printf("Phase 5: Final Verification\n");
    printf("========================================\n");

    double t0 = now_sec();

    // Count live entries
    uint64_t num_live = 0;
    shadow_entry_t *se, *tmp;
    HASH_ITER(hh, g_shadow, se, tmp) {
        if (!se->deleted) num_live++;
    }

    dl_stats_t st = dl_stats(dl);
    printf("  live entries (shadow): %" PRIu64 "\n", num_live);
    printf("  index_keys (dl_stats): %" PRIu64 "\n", st.index_keys);
    ASSERT_MSG(st.index_keys == num_live,
               "count mismatch: index %" PRIu64 " vs shadow %" PRIu64,
               st.index_keys, num_live);

    // Collect live keys, sort for deterministic hash
    uint8_t (*sorted_keys)[KEY_SIZE] = malloc(num_live * KEY_SIZE);
    ASSERT_MSG(sorted_keys != NULL, "malloc failed for sorted keys");

    uint64_t idx = 0;
    HASH_ITER(hh, g_shadow, se, tmp) {
        if (!se->deleted) {
            memcpy(sorted_keys[idx], se->key, KEY_SIZE);
            idx++;
        }
    }
    qsort(sorted_keys, num_live, KEY_SIZE, key_compare);

    // Verify every live entry + accumulate FNV-1a hash
    uint64_t hash = FNV_OFFSET_BASIS;
    uint64_t verified = 0;
    uint64_t code_count = 0;

    for (uint64_t i = 0; i < num_live; i++) {
        verify_key(dl, sorted_keys[i], "final-scan");

        shadow_entry_t *entry = shadow_get(sorted_keys[i]);
        ASSERT_MSG(entry != NULL, "shadow_get failed during final scan");

        hash = fnv1a_update(hash, entry->key, KEY_SIZE);
        hash = fnv1a_update(hash, entry->value, entry->value_len);
        if (entry->is_code) code_count++;
        verified++;

        if (verified % 50000 == 0) {
            printf("  ... verified %" PRIu64 "/%" PRIu64 "\n", verified, num_live);
        }
    }

    free(sorted_keys);
    double t1 = now_sec();

    printf("  verified: %" PRIu64 " keys in %.1fs\n", verified, t1 - t0);
    printf("  state entries: %" PRIu64 "\n", verified - code_count);
    printf("  code entries:  %" PRIu64 "\n", code_count);
    printf("  free slots:    %u\n", st.free_slots);
    printf("  total merged:  %" PRIu64 "\n", st.total_merged);
    printf("  RSS: %zu MB\n", get_rss_mb());
    printf("  determinism hash: 0x%016" PRIx64 "\n", hash);
    printf("  Phase 5: PASS\n");
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t scale = 1;
    if (argc >= 2) scale = (uint64_t)atoll(argv[1]);
    if (scale == 0) scale = 1;

    uint64_t genesis_accounts  = DEFAULT_GENESIS_ACCOUNTS * scale;
    uint64_t genesis_contracts = DEFAULT_GENESIS_CONTRACTS * scale;
    uint64_t num_blocks_p2     = DEFAULT_NUM_BLOCKS_P2;
    uint64_t num_blocks_p4     = DEFAULT_NUM_BLOCKS_P4;
    uint64_t ops_per_block     = DEFAULT_OPS_PER_BLOCK * scale;
    uint64_t ckpt_interval     = DEFAULT_CHECKPOINT_INTERVAL;

    printf("============================================\n");
    printf("  Data Layer Integration Stress Test\n");
    printf("============================================\n");
    printf("  scale factor:     %" PRIu64 "\n", scale);
    printf("  genesis accounts: %" PRIu64 "\n", genesis_accounts);
    printf("  genesis contracts:%" PRIu64 "\n", genesis_contracts);
    printf("  blocks (P2+P4):   %" PRIu64 "+%" PRIu64 "\n",
           num_blocks_p2, num_blocks_p4);
    printf("  ops/block:        %" PRIu64 "\n", ops_per_block);
    printf("  checkpoint every: %" PRIu64 " blocks\n", ckpt_interval);
    printf("  master seed:      0x%016" PRIx64 "\n", MASTER_SEED);
    printf("============================================\n");

    double total_start = now_sec();

    // Clean up from prior runs
    unlink(STATE_PATH);
    unlink(CODE_PATH);
    unlink(INDEX_PATH);

    // Create data layer
    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, 4);
    ASSERT_MSG(dl != NULL, "dl_create failed");

    // Phase 1: Genesis
    phase1_genesis(dl, genesis_accounts, genesis_contracts);

    // Phase 2: Block processing
    phase2_blocks(dl, num_blocks_p2, ops_per_block, ckpt_interval);

    // Phase 3: Crash recovery
    phase3_recovery(&dl);

    // Phase 4: Post-recovery blocks
    phase4_post_recovery(dl, num_blocks_p4, ops_per_block);

    // Phase 5: Final verification
    phase5_final(dl);

    // Cleanup
    dl_destroy(dl);
    shadow_destroy_map(&g_shadow);
    free(g_live_indices);
    unlink(STATE_PATH);
    unlink(CODE_PATH);
    unlink(INDEX_PATH);

    double total = now_sec() - total_start;
    printf("\n============================================\n");
    printf("  ALL PHASES PASSED\n");
    printf("  total time: %.1fs\n", total);
    printf("============================================\n");

    return 0;
}
