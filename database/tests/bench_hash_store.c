/*
 * Hash Store Benchmark — sharded hash_store vs compact_art for 64B keys
 *
 * Compares the zero-RAM sharded file-backed hash table against compact_art
 * for Ethereum storage keys (addr_hash[32] || slot_hash[32]).
 *
 * Block simulation: 5K-50K ops/block, 70/20/10 ins/upd/del.
 * Reports: RSS, bytes/key, throughput, shard count, disk usage.
 *
 * Usage: ./bench_hash_store [total_millions]
 *   Default: 10M keys
 */

#include "../include/hash_store.h"
#include "../include/compact_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE         64
#define VALUE_SIZE       4       // compact_art ref size
#define OPS_MIN          5000
#define OPS_MAX          50000
#define STATS_INTERVAL   10

#define MASTER_SEED      0x48534254455354ULL  // "HSBTEST"
#define HASH_STORE_DIR   "/tmp/bench_hash_store"

// Shard capacity: 16M slots = 1GB per shard
#define SHARD_CAP        (1ULL << 24)

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

static void generate_key(uint8_t key[KEY_SIZE], uint64_t index) {
    rng_t rng = rng_create(MASTER_SEED ^ (index * 0x517cc1b727220a95ULL));
    for (int i = 0; i < KEY_SIZE; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
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

static size_t dir_disk_mb(const char *dir) {
    DIR *d = opendir(dir);
    if (!d) return 0;
    size_t total_blocks = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
        struct stat st;
        if (stat(path, &st) == 0)
            total_blocks += (size_t)st.st_blocks;
    }
    closedir(d);
    return (total_blocks * 512) / (1024 * 1024);
}

static void cleanup_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    int ret = system(cmd);
    (void)ret;
}

// ============================================================================
// Benchmark: hash_store (sharded)
// ============================================================================

static void bench_hash_store(uint64_t target_keys) {
    printf("\n============================================\n");
    printf("  hash_store (sharded) — %"PRIu64"M keys (64B)\n",
           target_keys / 1000000);
    printf("============================================\n\n");

    cleanup_dir(HASH_STORE_DIR);
    double t0 = now_sec();

    hash_store_t *hs = hash_store_create(HASH_STORE_DIR, SHARD_CAP);
    ASSERT_MSG(hs, "hash_store_create failed");

    double t_create = now_sec() - t0;
    printf("  create: %.2fs (shard_cap=%"PRIu64", shards=%u, depth=%u)\n\n",
           t_create, (uint64_t)SHARD_CAP,
           hash_store_num_shards(hs),
           hash_store_global_depth(hs));

    size_t rss_before = get_rss_mb();
    uint64_t next_id = 0;
    uint64_t total_blocks = 0;
    double t_start = now_sec();

    while (next_id < target_keys) {
        rng_t brng = rng_create(MASTER_SEED ^
                                (total_blocks * 0x426C6F636B000000ULL));
        uint32_t ops_count = OPS_MIN + (uint32_t)(rng_next(&brng) %
                             (OPS_MAX - OPS_MIN + 1));

        uint8_t key[KEY_SIZE];

        // 70% inserts
        uint32_t ins = (uint32_t)(ops_count * 0.7);
        for (uint32_t i = 0; i < ins && next_id < target_keys; i++) {
            uint32_t val = (uint32_t)next_id;
            generate_key(key, next_id++);
            ASSERT_MSG(hash_store_put(hs, key, &val, sizeof(val)),
                       "put failed at %"PRIu64, next_id - 1);
        }

        // 20% updates
        uint32_t upd = (uint32_t)(ops_count * 0.2);
        for (uint32_t i = 0; i < upd && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            uint32_t val = (uint32_t)(kid ^ total_blocks);
            generate_key(key, kid);
            hash_store_put(hs, key, &val, sizeof(val));
        }

        // 10% deletes
        uint32_t del = (uint32_t)(ops_count * 0.1);
        for (uint32_t i = 0; i < del && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, kid);
            hash_store_delete(hs, key);
        }

        total_blocks++;

        bool is_last = (next_id >= target_keys);
        if (total_blocks % STATS_INTERVAL == 0 || is_last) {
            double elapsed = now_sec() - t_start;
            double kkeys_s = next_id / elapsed / 1000.0;
            size_t rss = get_rss_mb();
            size_t disk = dir_disk_mb(HASH_STORE_DIR);

            printf("block %5"PRIu64" | keys %8"PRIu64" | "
                   "count %8"PRIu64" | tomb %6"PRIu64" | "
                   "shards %3u | disk %5zuMB | RSS %5zuMB | %6.0fKk/s\n",
                   total_blocks, next_id,
                   hash_store_count(hs),
                   hash_store_tombstones(hs),
                   hash_store_num_shards(hs),
                   disk, rss, kkeys_s);
            fflush(stdout);
        }
    }

    double elapsed = now_sec() - t_start;
    size_t rss_after = get_rss_mb();
    size_t disk_actual = dir_disk_mb(HASH_STORE_DIR);
    uint64_t count = hash_store_count(hs);

    // Verification: spot-check some keys
    uint32_t verify_ok = 0, verify_total = 1000;
    rng_t vrng = rng_create(0xDEADBEEF);
    for (uint32_t i = 0; i < verify_total; i++) {
        uint64_t kid = rng_next(&vrng) % next_id;
        uint8_t key[KEY_SIZE];
        generate_key(key, kid);
        if (hash_store_contains(hs, key)) verify_ok++;
    }

    printf("\n--- hash_store results ---\n");
    printf("  keys inserted:  %"PRIu64"\n", next_id);
    printf("  live count:     %"PRIu64"\n", count);
    printf("  tombstones:     %"PRIu64"\n", hash_store_tombstones(hs));
    printf("  capacity:       %"PRIu64"\n", hash_store_capacity(hs));
    printf("  shards:         %u\n", hash_store_num_shards(hs));
    printf("  global depth:   %u\n", hash_store_global_depth(hs));
    printf("  shard capacity: %"PRIu64"\n", (uint64_t)SHARD_CAP);
    printf("  time:           %.1fs\n", elapsed);
    printf("  throughput:     %.0fK keys/s\n", next_id / elapsed / 1000.0);
    printf("  disk (actual):  %zu MB\n", disk_actual);
    if (count > 0)
        printf("  disk bytes/key: %.1f\n",
               (double)(disk_actual * 1024 * 1024) / count);
    printf("  RSS before:     %zu MB\n", rss_before);
    printf("  RSS after:      %zu MB\n", rss_after);
    printf("  RSS delta:      %zu MB\n",
           rss_after > rss_before ? rss_after - rss_before : 0);
    printf("  verify:         %u/%u found\n", verify_ok, verify_total);
    printf("  blocks:         %"PRIu64"\n", total_blocks);

    hash_store_sync(hs);
    hash_store_destroy(hs);
}

// ============================================================================
// Benchmark: compact_art (for comparison)
// ============================================================================

static void bench_compact_art(uint64_t target_keys) {
    printf("\n============================================\n");
    printf("  compact_art — %"PRIu64"M keys (64B)\n", target_keys / 1000000);
    printf("============================================\n\n");

    compact_art_t tree;
    ASSERT_MSG(compact_art_init(&tree, KEY_SIZE, VALUE_SIZE),
               "compact_art_init failed");

    size_t rss_before = get_rss_mb();
    uint64_t next_id = 0;
    uint64_t total_blocks = 0;
    double t_start = now_sec();

    while (next_id < target_keys) {
        rng_t brng = rng_create(MASTER_SEED ^
                                (total_blocks * 0x426C6F636B000000ULL));
        uint32_t ops_count = OPS_MIN + (uint32_t)(rng_next(&brng) %
                             (OPS_MAX - OPS_MIN + 1));

        uint8_t key[KEY_SIZE];

        // 70% inserts
        uint32_t ins = (uint32_t)(ops_count * 0.7);
        for (uint32_t i = 0; i < ins && next_id < target_keys; i++) {
            uint32_t val = (uint32_t)next_id;
            generate_key(key, next_id++);
            compact_art_insert(&tree, key, &val);
        }

        // 20% updates
        uint32_t upd = (uint32_t)(ops_count * 0.2);
        for (uint32_t i = 0; i < upd && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            uint32_t val = (uint32_t)(kid ^ total_blocks);
            generate_key(key, kid);
            compact_art_insert(&tree, key, &val);
        }

        // 10% deletes
        uint32_t del = (uint32_t)(ops_count * 0.1);
        for (uint32_t i = 0; i < del && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, kid);
            compact_art_delete(&tree, key);
        }

        total_blocks++;

        bool is_last = (next_id >= target_keys);
        if (total_blocks % STATS_INTERVAL == 0 || is_last) {
            double elapsed = now_sec() - t_start;
            double kkeys_s = next_id / elapsed / 1000.0;
            size_t pool = (tree.nodes.used + tree.leaves.used) / (1024 * 1024);

            printf("block %5"PRIu64" | keys %8"PRIu64" | "
                   "count %8zu | pool %5zuMB | RSS %5zuMB | %6.0fKk/s\n",
                   total_blocks, next_id,
                   compact_art_size(&tree),
                   pool, get_rss_mb(), kkeys_s);
            fflush(stdout);
        }
    }

    double elapsed = now_sec() - t_start;
    size_t rss_after = get_rss_mb();
    size_t count = compact_art_size(&tree);
    size_t pool_bytes = tree.nodes.used + tree.leaves.used;

    printf("\n--- compact_art results ---\n");
    printf("  keys inserted:  %"PRIu64"\n", next_id);
    printf("  live count:     %zu\n", count);
    printf("  time:           %.1fs\n", elapsed);
    printf("  throughput:     %.0fK keys/s\n", next_id / elapsed / 1000.0);
    printf("  pool:           %zu MB (nodes %zu + leaves %zu)\n",
           pool_bytes / (1024*1024),
           tree.nodes.used / (1024*1024),
           tree.leaves.used / (1024*1024));
    if (count > 0)
        printf("  bytes/key:      %.1f\n", (double)pool_bytes / count);
    printf("  RSS before:     %zu MB\n", rss_before);
    printf("  RSS after:      %zu MB\n", rss_after);
    printf("  RSS delta:      %zu MB\n",
           rss_after > rss_before ? rss_after - rss_before : 0);
    printf("  blocks:         %"PRIu64"\n", total_blocks);

    compact_art_destroy(&tree);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t total_millions = 10;
    if (argc >= 2) {
        total_millions = (uint64_t)atoll(argv[1]);
        if (total_millions == 0) total_millions = 10;
    }

    uint64_t target_keys = total_millions * 1000000ULL;

    printf("============================================\n");
    printf("  Hash Store vs Compact ART Benchmark\n");
    printf("============================================\n");
    printf("  target:     %"PRIu64"M keys\n", total_millions);
    printf("  key size:   %d bytes\n", KEY_SIZE);
    printf("  value size: %d bytes\n", VALUE_SIZE);
    printf("  shard cap:  %"PRIu64" slots (%"PRIu64" MB/shard)\n",
           (uint64_t)SHARD_CAP, (uint64_t)SHARD_CAP * 64 / (1024*1024));
    printf("  ops/block:  %d-%d (70/20/10 ins/upd/del)\n", OPS_MIN, OPS_MAX);
    printf("  seed:       0x%016"PRIx64"\n", (uint64_t)MASTER_SEED);
    printf("============================================\n");

    // --- Phase 1: hash_store ---
    bench_hash_store(target_keys);

    // Clean up hash store files before compact_art to avoid RSS influence
    cleanup_dir(HASH_STORE_DIR);

    // --- Phase 2: compact_art ---
    bench_compact_art(target_keys);

    printf("\n============================================\n");
    printf("  Done.\n");
    printf("============================================\n");

    return 0;
}
