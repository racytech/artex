/*
 * compact_art Stress Test
 *
 * Validates correctness of all operations at scale:
 *   - Phase 1: Bulk insert + random sample verify (get, contains) + iterator scan
 *   - Phase 2: Mixed insert/delete (20% churn) + iterator scan
 *   - Phase 3: Delete all keys, verify empty tree + empty iterator
 *   - Determinism: repeat with same seed, verify identical FNV-1a hash
 *
 * Fail-fast: aborts immediately on first incorrect result.
 *
 * Usage: ./test_stress_compact_art [target_millions] [0xseed]
 *   Default: 1M keys, random seed
 */

#include "../include/compact_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE        32
#define VALUE_SIZE      4
#define PROGRESS_EVERY  100000
#define SAMPLE_VERIFY   1000

// ============================================================================
// splitmix64 PRNG
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
// Deterministic key/value generation
// ============================================================================

static void generate_key(uint8_t key[KEY_SIZE], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng);
    uint64_t r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng);
    uint64_t r3 = rng_next(&rng);
    memcpy(key,      &r0, 8);
    memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8);
    memcpy(key + 24, &r3, 8);
}

static uint32_t generate_value(uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0xA2F5C3D7E1B94068ULL));
    return (uint32_t)rng_next(&rng);
}

// ============================================================================
// FNV-1a hash for determinism verification
// ============================================================================

#define FNV_OFFSET_BASIS 0xcbf29ce484222325ULL

static inline uint64_t fnv1a_update(uint64_t hash, const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

// ============================================================================
// Time helpers
// ============================================================================

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void format_elapsed(double secs, char *buf, size_t bufsz) {
    int m = (int)(secs / 60);
    int s = (int)(secs - m * 60);
    if (m > 0)
        snprintf(buf, bufsz, "%dm%02ds", m, s);
    else
        snprintf(buf, bufsz, "%.1fs", secs);
}

// ============================================================================
// Fail macro
// ============================================================================

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL: " fmt "\n", ##__VA_ARGS__); \
    return false; \
} while (0)

// ============================================================================
// Full iterator scan: count + sorted order + FNV-1a hash
// ============================================================================

static bool full_scan(compact_art_t *tree, uint64_t expected_count,
                      uint64_t *out_hash, const char *label) {
    double t0 = now_sec();

    compact_art_iterator_t *iter = compact_art_iterator_create(tree);
    if (!iter)
        FAIL("[%s] iterator create failed", label);

    uint64_t count = 0;
    uint64_t hash = FNV_OFFSET_BASIS;
    uint8_t prev_key[KEY_SIZE];
    bool have_prev = false;

    while (compact_art_iterator_next(iter)) {
        const uint8_t *k = compact_art_iterator_key(iter);
        const void *v = compact_art_iterator_value(iter);

        if (!k || !v) {
            compact_art_iterator_destroy(iter);
            FAIL("[%s] NULL key/value at count=%" PRIu64, label, count);
        }

        // Sorted order check
        if (have_prev && memcmp(prev_key, k, KEY_SIZE) >= 0) {
            compact_art_iterator_destroy(iter);
            FAIL("[%s] sort order violation at count=%" PRIu64, label, count);
        }
        memcpy(prev_key, k, KEY_SIZE);
        have_prev = true;

        hash = fnv1a_update(hash, k, KEY_SIZE);
        hash = fnv1a_update(hash, (const uint8_t *)v, VALUE_SIZE);
        count++;
    }

    compact_art_iterator_destroy(iter);

    if (count != expected_count)
        FAIL("[%s] expected %" PRIu64 " keys, iterator yielded %" PRIu64,
             label, expected_count, count);

    double elapsed = now_sec() - t0;
    printf("  [%s] scan OK: %" PRIu64 " keys, hash=0x%016" PRIx64 " (%.2fs)\n",
           label, count, hash, elapsed);

    if (out_hash) *out_hash = hash;
    return true;
}

// ============================================================================
// Phase 1: Bulk insert + sample verification (get + contains)
// ============================================================================

static bool phase1_bulk_insert(compact_art_t *tree, uint64_t target,
                               uint64_t seed, uint64_t *out_hash) {
    printf("\n--- Phase 1: Bulk insert %" PRIu64 " keys ---\n", target);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE];
    rng_t verify_rng = rng_create(seed ^ 0x1234567890ABCDEFULL);

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);
        uint32_t val = generate_value(seed, i);

        if (!compact_art_insert(tree, key, &val))
            FAIL("insert failed at index %" PRIu64, i);

        if ((i + 1) % PROGRESS_EVERY == 0) {
            // Verify random sample
            for (int s = 0; s < SAMPLE_VERIFY; s++) {
                uint64_t idx = rng_next(&verify_rng) % (i + 1);
                uint8_t exp_key[KEY_SIZE];
                uint32_t exp_val = generate_value(seed, idx);
                generate_key(exp_key, seed, idx);

                // get
                const void *got = compact_art_get(tree, exp_key);
                if (!got)
                    FAIL("key index %" PRIu64 " not found (inserted %" PRIu64 ")", idx, i + 1);
                uint32_t got_val;
                memcpy(&got_val, got, sizeof(got_val));
                if (got_val != exp_val)
                    FAIL("value mismatch at index %" PRIu64 ": got 0x%08x, expected 0x%08x",
                         idx, got_val, exp_val);

                // contains
                if (!compact_art_contains(tree, exp_key))
                    FAIL("contains returned false for existing key index %" PRIu64, idx);
            }

            double elapsed = now_sec() - t0;
            printf("  %7.1fM / %.1fM | %d samples OK | %.0f Kk/s\n",
                   (double)(i + 1) / 1e6, (double)target / 1e6,
                   SAMPLE_VERIFY, (double)(i + 1) / elapsed / 1000.0);
        }
    }

    if (compact_art_size(tree) != (size_t)target)
        FAIL("expected size %" PRIu64 ", got %zu", target, compact_art_size(tree));

    double elapsed = now_sec() - t0;
    printf("  Phase 1 done: %" PRIu64 " keys in %.2fs (%.0f Kk/s)\n",
           target, elapsed, (double)target / elapsed / 1000.0);

    return full_scan(tree, target, out_hash, "phase1");
}

// ============================================================================
// Phase 2: Mixed insert/delete (20% churn)
// ============================================================================

static bool phase2_mixed(compact_art_t *tree, uint64_t target,
                         uint64_t seed, uint64_t *next_index) {
    uint64_t churn = target / 5;
    printf("\n--- Phase 2: Mixed insert/delete (%" PRIu64 " ops) ---\n", churn);
    double t0 = now_sec();

    rng_t del_rng = rng_create(seed ^ 0xDEADBEEFCAFEBABEULL);
    uint8_t key[KEY_SIZE];

    // Bitmap: track which indices are alive
    size_t bitmap_bytes = (target + churn + 7) / 8;
    uint8_t *alive = calloc(1, bitmap_bytes);
    if (!alive) FAIL("bitmap alloc failed");

    for (uint64_t i = 0; i < target; i++)
        alive[i / 8] |= (uint8_t)(1 << (i % 8));

    uint64_t deleted = 0, inserted = 0;
    uint64_t new_index = target;

    for (uint64_t i = 0; i < churn; i++) {
        // Delete a random existing key
        uint64_t del_idx;
        int attempts = 0;
        do {
            del_idx = rng_next(&del_rng) % (target + inserted);
            attempts++;
        } while (!(alive[del_idx / 8] & (1 << (del_idx % 8))) && attempts < 100);

        if (attempts < 100) {
            generate_key(key, seed, del_idx);

            // Verify it exists before deleting
            if (!compact_art_contains(tree, key)) {
                free(alive);
                FAIL("key %" PRIu64 " marked alive but contains() is false", del_idx);
            }

            if (compact_art_delete(tree, key)) {
                alive[del_idx / 8] &= ~(uint8_t)(1 << (del_idx % 8));
                deleted++;

                // Verify it's gone
                if (compact_art_get(tree, key) != NULL) {
                    free(alive);
                    FAIL("key %" PRIu64 " still found after delete", del_idx);
                }
                if (compact_art_contains(tree, key)) {
                    free(alive);
                    FAIL("contains() true after delete for key %" PRIu64, del_idx);
                }
            }
        }

        // Insert a new key
        generate_key(key, seed, new_index);
        uint32_t val = generate_value(seed, new_index);
        if (!compact_art_insert(tree, key, &val)) {
            free(alive);
            FAIL("insert failed at new_index %" PRIu64, new_index);
        }
        alive[new_index / 8] |= (uint8_t)(1 << (new_index % 8));
        inserted++;
        new_index++;

        if ((i + 1) % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  %7.1fM / %.1fM churn | del %" PRIu64 " ins %" PRIu64
                   " | size %zu | %.1fs\n",
                   (double)(i + 1) / 1e6, (double)churn / 1e6,
                   deleted, inserted, compact_art_size(tree), elapsed);
        }
    }

    free(alive);

    uint64_t expected_size = target - deleted + inserted;
    if (compact_art_size(tree) != (size_t)expected_size)
        FAIL("expected size %" PRIu64 " (%" PRIu64 " - %" PRIu64
             " + %" PRIu64 "), got %zu",
             expected_size, target, deleted, inserted, compact_art_size(tree));

    double elapsed = now_sec() - t0;
    printf("  Phase 2 done: del %" PRIu64 ", ins %" PRIu64
           ", size %zu (%.2fs)\n", deleted, inserted,
           compact_art_size(tree), elapsed);

    *next_index = new_index;
    return full_scan(tree, expected_size, NULL, "phase2");
}

// ============================================================================
// Phase 3: Delete all keys, verify empty
// ============================================================================

static bool phase3_delete_all(compact_art_t *tree, uint64_t seed,
                              uint64_t total_indices) {
    size_t before = compact_art_size(tree);
    printf("\n--- Phase 3: Delete all %zu keys ---\n", before);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE];
    uint64_t deleted = 0;

    for (uint64_t i = 0; i < total_indices; i++) {
        generate_key(key, seed, i);
        if (compact_art_delete(tree, key)) {
            deleted++;

            // Spot-check: key must be gone
            if (compact_art_get(tree, key) != NULL)
                FAIL("key %" PRIu64 " still found after delete", i);
        }

        if (deleted > 0 && deleted % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  deleted %" PRIu64 " / %zu | %.1fs\n",
                   deleted, before, elapsed);
        }
    }

    if (compact_art_size(tree) != 0)
        FAIL("tree not empty after deleting all (size=%zu)", compact_art_size(tree));

    if (!compact_art_is_empty(tree))
        FAIL("compact_art_is_empty returned false");

    // Iterator should yield nothing
    compact_art_iterator_t *iter = compact_art_iterator_create(tree);
    if (iter) {
        if (compact_art_iterator_next(iter)) {
            compact_art_iterator_destroy(iter);
            FAIL("iterator returned entry on empty tree");
        }
        compact_art_iterator_destroy(iter);
    }

    double elapsed = now_sec() - t0;
    printf("  Phase 3 done: deleted %" PRIu64 " keys, tree empty (%.2fs)\n",
           deleted, elapsed);

    return true;
}

// ============================================================================
// Run all phases
// ============================================================================

static bool run_all_phases(uint64_t target, uint64_t seed,
                           uint64_t *out_phase1_hash, int run_num) {
    printf("\n========== Run %d (seed=0x%016" PRIx64 ") ==========\n",
           run_num, seed);

    compact_art_t tree;
    if (!compact_art_init(&tree, KEY_SIZE, VALUE_SIZE)) {
        fprintf(stderr, "FAIL: compact_art_init failed\n");
        return false;
    }

    uint64_t phase1_hash = 0;
    if (!phase1_bulk_insert(&tree, target, seed, &phase1_hash)) {
        compact_art_destroy(&tree);
        return false;
    }
    if (out_phase1_hash) *out_phase1_hash = phase1_hash;

    uint64_t next_index = 0;
    if (!phase2_mixed(&tree, target, seed, &next_index)) {
        compact_art_destroy(&tree);
        return false;
    }

    if (!phase3_delete_all(&tree, seed, next_index)) {
        compact_art_destroy(&tree);
        return false;
    }

    compact_art_destroy(&tree);
    return true;
}

// ============================================================================
// main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t target_millions = 1;
    uint64_t seed = 0;

    if (argc > 1) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) target_millions = 1;
    }
    if (argc > 2) {
        seed = strtoull(argv[2], NULL, 16);
    }
    if (seed == 0) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        seed = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
        seed ^= (uint64_t)getpid() << 32;
    }

    uint64_t target = target_millions * 1000000ULL;

    printf("\n=== compact_art Stress Test ===\n");
    printf("  target:     %" PRIu64 "M keys (%" PRIu64 ")\n",
           target_millions, target);
    printf("  key_size:   %d bytes\n", KEY_SIZE);
    printf("  value_size: %d bytes\n", VALUE_SIZE);
    printf("  seed:       0x%016" PRIx64 "\n", seed);

    double t_start = now_sec();

    // Run 1
    uint64_t hash1 = 0;
    if (!run_all_phases(target, seed, &hash1, 1)) {
        fprintf(stderr, "\nFAILED run 1\n");
        fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
                argv[0], target_millions, seed);
        return 1;
    }

    // Run 2 (determinism check)
    uint64_t hash2 = 0;
    if (!run_all_phases(target, seed, &hash2, 2)) {
        fprintf(stderr, "\nFAILED run 2\n");
        fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
                argv[0], target_millions, seed);
        return 1;
    }

    if (hash1 != hash2) {
        fprintf(stderr, "\nFAILED: determinism check — run1 hash=0x%016" PRIx64
                " != run2 hash=0x%016" PRIx64 "\n", hash1, hash2);
        fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
                argv[0], target_millions, seed);
        return 1;
    }

    double wall = now_sec() - t_start;
    char elapsed_str[32];
    format_elapsed(wall, elapsed_str, sizeof(elapsed_str));

    printf("\n============================================\n");
    printf("ALL CHECKS PASSED\n");
    printf("============================================\n");
    printf("  keys:          %" PRIu64 " (%.0fM)\n", target, target / 1e6);
    printf("  phase1 hash:   0x%016" PRIx64 " (deterministic)\n", hash1);
    printf("  wall time:     %s\n", elapsed_str);
    printf("  seed:          0x%016" PRIx64 "\n", seed);
    printf("============================================\n\n");

    return 0;
}
