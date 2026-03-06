/*
 * Dual compact_art RSS Benchmark — Accounts (32B) + Storage (64B)
 *
 * Measures combined memory usage of two compact_art trees running
 * simultaneously, mirroring the state_db architecture:
 *   - accounts: key_size=32, value_size=4 (keccak256(addr) → slot ref)
 *   - storage:  key_size=64, value_size=4 (addr_hash||slot_hash → slot ref)
 *
 * Realistic distribution: configurable storage-to-account ratio (default 5:1).
 * Block simulation: 5K-50K ops/block, 70/20/10 ins/upd/del.
 *
 * Usage: ./bench_dual_index [total_millions] [storage_ratio]
 *   Default: 10M total keys, 5:1 storage:account ratio
 */

#include "../include/compact_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>

// ============================================================================
// Constants
// ============================================================================

#define ACCT_KEY_SIZE    32
#define STOR_KEY_SIZE    64
#define VALUE_SIZE       4
#define OPS_MIN          5000
#define OPS_MAX          50000
#define STATS_INTERVAL   10

#define MASTER_SEED      0x4455414C49445800ULL  // "DUALIDX\0"
#define ACCT_SEED        (MASTER_SEED ^ 0x4143435400000000ULL)
#define STOR_SEED        (MASTER_SEED ^ 0x53544F5200000000ULL)

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

// ============================================================================
// Key Generation
// ============================================================================

static void generate_acct_key(uint8_t key[ACCT_KEY_SIZE], uint64_t seed,
                               uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng), r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng), r3 = rng_next(&rng);
    memcpy(key,      &r0, 8); memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8); memcpy(key + 24, &r3, 8);
}

// Storage key = addr_hash[32] || slot_hash[32]
// addr_id selects which account, slot_id is the slot within that account
static void generate_stor_key(uint8_t key[STOR_KEY_SIZE], uint64_t addr_id,
                               uint64_t slot_id) {
    // First 32 bytes: deterministic from addr_id (same as account key)
    generate_acct_key(key, ACCT_SEED, addr_id);
    // Last 32 bytes: deterministic from slot_id
    rng_t rng = rng_create(STOR_SEED ^ (slot_id * 0x9c2f0b3a71d8e6f5ULL));
    uint64_t r0 = rng_next(&rng), r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng), r3 = rng_next(&rng);
    memcpy(key + 32, &r0, 8); memcpy(key + 40, &r1, 8);
    memcpy(key + 48, &r2, 8); memcpy(key + 56, &r3, 8);
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

static size_t pool_used_mb(const compact_art_t *tree) {
    return (tree->nodes.used + tree->leaves.used) / (1024 * 1024);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t total_millions = 10;
    uint32_t storage_ratio = 5;

    if (argc >= 2) {
        total_millions = (uint64_t)atoll(argv[1]);
        if (total_millions == 0) total_millions = 10;
    }
    if (argc >= 3) {
        storage_ratio = (uint32_t)atoi(argv[2]);
        if (storage_ratio == 0) storage_ratio = 5;
    }

    uint64_t total_keys = total_millions * 1000000ULL;
    uint64_t target_accts = total_keys / (1 + storage_ratio);
    uint64_t target_slots = total_keys - target_accts;

    printf("============================================\n");
    printf("  Dual Index RSS Benchmark\n");
    printf("============================================\n");
    printf("  total:        %" PRIu64 "M keys\n", total_millions);
    printf("  ratio:        1:%u (acct:storage)\n", storage_ratio);
    printf("  accounts:     %" PRIu64 "K (32B keys)\n", target_accts / 1000);
    printf("  storage:      %" PRIu64 "K (64B keys)\n", target_slots / 1000);
    printf("  ops/block:    %d-%d (70/20/10 ins/upd/del)\n", OPS_MIN, OPS_MAX);
    printf("  seed:         0x%016" PRIx64 "\n", (uint64_t)MASTER_SEED);
    printf("============================================\n\n");

    // Create both trees
    compact_art_t accts, storage;
    ASSERT_MSG(compact_art_init(&accts, ACCT_KEY_SIZE, VALUE_SIZE),
               "acct tree init failed");
    ASSERT_MSG(compact_art_init(&storage, STOR_KEY_SIZE, VALUE_SIZE),
               "storage tree init failed");

    // Simulation state
    uint64_t next_acct_id = 0;
    uint64_t next_slot_id = 0;
    uint64_t total_blocks = 0;
    double t_start = now_sec();

    printf("--- Block Simulation ---\n\n");

    while (next_acct_id < target_accts || next_slot_id < target_slots) {
        rng_t brng = rng_create(MASTER_SEED ^
                                (total_blocks * 0x426C6F636B000000ULL));
        uint32_t ops_count = OPS_MIN + (uint32_t)(rng_next(&brng) %
                             (OPS_MAX - OPS_MIN + 1));

        // Split ops by ratio
        uint32_t acct_ops = ops_count / (1 + storage_ratio);
        uint32_t stor_ops = ops_count - acct_ops;

        uint32_t val = 0;

        // --- Account operations ---
        {
            uint8_t key[ACCT_KEY_SIZE];

            // 70% inserts
            uint32_t ins = (uint32_t)(acct_ops * 0.7);
            for (uint32_t i = 0; i < ins && next_acct_id < target_accts; i++) {
                val = (uint32_t)next_acct_id;
                generate_acct_key(key, ACCT_SEED, next_acct_id++);
                compact_art_insert(&accts, key, &val);
            }

            // 20% updates
            uint32_t upd = (uint32_t)(acct_ops * 0.2);
            for (uint32_t i = 0; i < upd && next_acct_id > 0; i++) {
                uint64_t kid = rng_next(&brng) % next_acct_id;
                val = (uint32_t)(kid ^ total_blocks);
                generate_acct_key(key, ACCT_SEED, kid);
                compact_art_insert(&accts, key, &val);
            }

            // 10% deletes
            uint32_t del = (uint32_t)(acct_ops * 0.1);
            for (uint32_t i = 0; i < del && next_acct_id > 0; i++) {
                uint64_t kid = rng_next(&brng) % next_acct_id;
                generate_acct_key(key, ACCT_SEED, kid);
                compact_art_delete(&accts, key);
            }
        }

        // --- Storage operations ---
        {
            uint8_t key[STOR_KEY_SIZE];

            // 70% inserts
            uint32_t ins = (uint32_t)(stor_ops * 0.7);
            for (uint32_t i = 0; i < ins && next_slot_id < target_slots; i++) {
                // Assign slot to a random existing account
                uint64_t addr_id = (next_acct_id > 0)
                    ? rng_next(&brng) % next_acct_id : 0;
                val = (uint32_t)next_slot_id;
                generate_stor_key(key, addr_id, next_slot_id++);
                compact_art_insert(&storage, key, &val);
            }

            // 20% updates
            uint32_t upd = (uint32_t)(stor_ops * 0.2);
            for (uint32_t i = 0; i < upd && next_slot_id > 0; i++) {
                uint64_t sid = rng_next(&brng) % next_slot_id;
                uint64_t addr_id = (next_acct_id > 0)
                    ? rng_next(&brng) % next_acct_id : 0;
                val = (uint32_t)(sid ^ total_blocks);
                generate_stor_key(key, addr_id, sid);
                compact_art_insert(&storage, key, &val);
            }

            // 10% deletes
            uint32_t del = (uint32_t)(stor_ops * 0.1);
            for (uint32_t i = 0; i < del && next_slot_id > 0; i++) {
                uint64_t sid = rng_next(&brng) % next_slot_id;
                uint64_t addr_id = (next_acct_id > 0)
                    ? rng_next(&brng) % next_acct_id : 0;
                generate_stor_key(key, addr_id, sid);
                compact_art_delete(&storage, key);
            }
        }

        total_blocks++;

        // --- Stats ---
        uint64_t total_inserted = next_acct_id + next_slot_id;
        bool is_last = (next_acct_id >= target_accts &&
                        next_slot_id >= target_slots);
        if (total_blocks % STATS_INTERVAL == 0 || is_last) {
            double elapsed = now_sec() - t_start;
            double kkeys_s = total_inserted / elapsed / 1000.0;

            size_t acct_pool = pool_used_mb(&accts);
            size_t stor_pool = pool_used_mb(&storage);

            printf("block %5" PRIu64 " | accts %7zu | slots %8zu | "
                   "acct %4zuMB | stor %4zuMB | RSS %5zuMB | "
                   "%6.0fKk/s\n",
                   total_blocks,
                   compact_art_size(&accts),
                   compact_art_size(&storage),
                   acct_pool,
                   stor_pool,
                   get_rss_mb(),
                   kkeys_s);
            fflush(stdout);
        }
    }

    double elapsed = now_sec() - t_start;

    // --- Summary ---
    size_t acct_count = compact_art_size(&accts);
    size_t stor_count = compact_art_size(&storage);
    size_t acct_bytes = accts.nodes.used + accts.leaves.used;
    size_t stor_bytes = storage.nodes.used + storage.leaves.used;

    printf("\n============================================\n");
    printf("  Summary\n");
    printf("============================================\n");
    printf("  blocks:         %" PRIu64 "\n", total_blocks);
    printf("  time:           %.1fs\n", elapsed);
    printf("  throughput:     %.0fK keys/s\n",
           (next_acct_id + next_slot_id) / elapsed / 1000.0);
    printf("\n");
    printf("  accounts:       %zu keys\n", acct_count);
    printf("  acct pools:     %zu MB (nodes %zu + leaves %zu)\n",
           acct_bytes / (1024*1024),
           accts.nodes.used / (1024*1024),
           accts.leaves.used / (1024*1024));
    if (acct_count > 0)
        printf("  acct bytes/key: %.1f\n", (double)acct_bytes / acct_count);
    printf("\n");
    printf("  storage:        %zu keys\n", stor_count);
    printf("  stor pools:     %zu MB (nodes %zu + leaves %zu)\n",
           stor_bytes / (1024*1024),
           storage.nodes.used / (1024*1024),
           storage.leaves.used / (1024*1024));
    if (stor_count > 0)
        printf("  stor bytes/key: %.1f\n", (double)stor_bytes / stor_count);
    printf("\n");
    printf("  combined pools: %zu MB\n",
           (acct_bytes + stor_bytes) / (1024*1024));
    printf("  RSS:            %zu MB\n", get_rss_mb());
    printf("============================================\n");

    compact_art_destroy(&accts);
    compact_art_destroy(&storage);

    return 0;
}
