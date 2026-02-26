/*
 * mem_art Stress Test
 *
 * Validates in-memory ART correctness at scale:
 *   - Phase 1: Bulk insert + random sample verification + full iterator scan
 *   - Phase 2: Mixed insert/delete (20% churn) + iterator scan
 *   - Phase 3: Delete all keys, verify empty
 *   - Determinism: repeat with same seed, verify identical FNV-1a hash
 *
 * Single-threaded (mem_art has no concurrency).
 *
 * Usage: ./test_stress_mem_art [target_millions] [0xseed]
 *   Default: 1M keys, random seed
 */

#include "../include/mem_art.h"

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
#define VALUE_SIZE      32
#define DEFAULT_TARGET  1000000   // 1M keys
#define PROGRESS_EVERY  100000    // 100K
#define SAMPLE_VERIFY   1000

// ============================================================================
// splitmix64 PRNG (same as other stress tests)
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
    rng_next(&r);  // warm up
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

static void generate_value(uint8_t value[VALUE_SIZE], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0xA2F5C3D7E1B94068ULL));
    uint64_t r0 = rng_next(&rng);
    uint64_t r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng);
    uint64_t r3 = rng_next(&rng);
    memcpy(value,      &r0, 8);
    memcpy(value + 8,  &r1, 8);
    memcpy(value + 16, &r2, 8);
    memcpy(value + 24, &r3, 8);
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
// Full iterator scan: count, sorted order, FNV-1a hash
// ============================================================================

static bool full_scan(art_tree_t *tree, uint64_t expected_count,
                      uint64_t *out_hash, const char *label) {
    double t0 = now_sec();

    art_iterator_t *iter = art_iterator_create(tree);
    if (!iter) {
        fprintf(stderr, "FAIL [%s]: iterator create failed\n", label);
        return false;
    }

    uint64_t count = 0;
    uint64_t hash = FNV_OFFSET_BASIS;
    uint8_t prev_key[KEY_SIZE];
    bool have_prev = false;

    while (art_iterator_next(iter)) {
        size_t klen, vlen;
        const uint8_t *k = art_iterator_key(iter, &klen);
        const void *v = art_iterator_value(iter, &vlen);

        if (!k || !v || klen != KEY_SIZE || vlen != VALUE_SIZE) {
            fprintf(stderr, "FAIL [%s]: bad iterator entry at count=%" PRIu64 "\n",
                    label, count);
            art_iterator_destroy(iter);
            return false;
        }

        // Sorted order check
        if (have_prev && memcmp(prev_key, k, KEY_SIZE) >= 0) {
            fprintf(stderr, "FAIL [%s]: sort order violation at count=%" PRIu64 "\n",
                    label, count);
            art_iterator_destroy(iter);
            return false;
        }
        memcpy(prev_key, k, KEY_SIZE);
        have_prev = true;

        hash = fnv1a_update(hash, k, klen);
        hash = fnv1a_update(hash, (const uint8_t *)v, vlen);
        count++;
    }

    art_iterator_destroy(iter);

    if (count != expected_count) {
        fprintf(stderr, "FAIL [%s]: expected %" PRIu64 " keys, got %" PRIu64 "\n",
                label, expected_count, count);
        return false;
    }

    double elapsed = now_sec() - t0;
    printf("  [%s] scan OK: %" PRIu64 " keys, hash=0x%016" PRIx64 " (%.2fs)\n",
           label, count, hash, elapsed);

    if (out_hash) *out_hash = hash;
    return true;
}

// ============================================================================
// Phase 1: Bulk insert + sample verification
// ============================================================================

static bool phase1_bulk_insert(art_tree_t *tree, uint64_t target,
                               uint64_t seed, uint64_t *out_hash) {
    printf("\n--- Phase 1: Bulk insert %" PRIu64 " keys ---\n", target);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE], value[VALUE_SIZE];
    rng_t verify_rng = rng_create(seed ^ 0x1234567890ABCDEFULL);

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);
        generate_value(value, seed, i);

        if (!art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE)) {
            fprintf(stderr, "FAIL: insert failed at index %" PRIu64 "\n", i);
            return false;
        }

        // Progress + sample verification
        if ((i + 1) % PROGRESS_EVERY == 0) {
            // Verify random sample of existing keys
            uint64_t verified = 0;
            for (int s = 0; s < SAMPLE_VERIFY; s++) {
                uint64_t idx = rng_next(&verify_rng) % (i + 1);
                uint8_t exp_key[KEY_SIZE], exp_val[VALUE_SIZE];
                generate_key(exp_key, seed, idx);
                generate_value(exp_val, seed, idx);

                size_t vlen;
                const void *got = art_get(tree, exp_key, KEY_SIZE, &vlen);
                if (!got) {
                    fprintf(stderr, "FAIL: key index %" PRIu64 " not found "
                            "(inserted %" PRIu64 " so far)\n", idx, i + 1);
                    return false;
                }
                if (vlen != VALUE_SIZE || memcmp(got, exp_val, VALUE_SIZE) != 0) {
                    fprintf(stderr, "FAIL: value mismatch at index %" PRIu64 "\n", idx);
                    return false;
                }
                verified++;
            }

            double elapsed = now_sec() - t0;
            double kps = (double)(i + 1) / elapsed / 1000.0;
            printf("  %7.1fM / %.1fM | %d samples OK | %.0f Kkeys/s\n",
                   (double)(i + 1) / 1e6, (double)target / 1e6,
                   SAMPLE_VERIFY, kps);
        }
    }

    if (art_size(tree) != (size_t)target) {
        fprintf(stderr, "FAIL: expected size %" PRIu64 ", got %zu\n",
                target, art_size(tree));
        return false;
    }

    double elapsed = now_sec() - t0;
    printf("  Phase 1 insert done: %" PRIu64 " keys in %.2fs (%.0f Kkeys/s)\n",
           target, elapsed, (double)target / elapsed / 1000.0);

    // Full iterator scan
    return full_scan(tree, target, out_hash, "phase1");
}

// ============================================================================
// Phase 2: Mixed insert/delete (20% churn)
// ============================================================================

static bool phase2_mixed(art_tree_t *tree, uint64_t target,
                         uint64_t seed, uint64_t *next_index) {
    uint64_t churn = target / 5;  // 20%
    printf("\n--- Phase 2: Mixed insert/delete (%" PRIu64 " ops) ---\n", churn);
    double t0 = now_sec();

    rng_t del_rng = rng_create(seed ^ 0xDEADBEEFCAFEBABEULL);
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    // Track which indices are alive. We use a simple bitmap.
    // target keys [0..target-1] were inserted in phase 1.
    // We'll delete random ones and insert new ones at [target..target+churn-1].
    size_t bitmap_bytes = (target + churn + 7) / 8;
    uint8_t *alive = calloc(1, bitmap_bytes);
    if (!alive) {
        fprintf(stderr, "FAIL: bitmap alloc failed\n");
        return false;
    }

    // Mark all phase-1 keys as alive
    for (uint64_t i = 0; i < target; i++) {
        alive[i / 8] |= (uint8_t)(1 << (i % 8));
    }

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
            if (art_delete(tree, key, KEY_SIZE)) {
                alive[del_idx / 8] &= ~(uint8_t)(1 << (del_idx % 8));
                deleted++;
            }
        }

        // Insert a new key
        generate_key(key, seed, new_index);
        generate_value(value, seed, new_index);
        if (!art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE)) {
            fprintf(stderr, "FAIL: insert failed at new_index %" PRIu64 "\n", new_index);
            free(alive);
            return false;
        }
        alive[new_index / 8] |= (uint8_t)(1 << (new_index % 8));
        inserted++;
        new_index++;

        if ((i + 1) % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  %7.1fM / %.1fM churn | del %" PRIu64 " ins %" PRIu64
                   " | size %zu | %.1fs\n",
                   (double)(i + 1) / 1e6, (double)churn / 1e6,
                   deleted, inserted, art_size(tree), elapsed);
        }
    }

    free(alive);

    uint64_t expected_size = target - deleted + inserted;
    if (art_size(tree) != (size_t)expected_size) {
        fprintf(stderr, "FAIL: expected size %" PRIu64 " (%" PRIu64 " - %" PRIu64
                " + %" PRIu64 "), got %zu\n",
                expected_size, target, deleted, inserted, art_size(tree));
        return false;
    }

    double elapsed = now_sec() - t0;
    printf("  Phase 2 done: del %" PRIu64 ", ins %" PRIu64
           ", size %zu (%.2fs)\n",
           deleted, inserted, art_size(tree), elapsed);

    *next_index = new_index;

    // Full iterator scan
    return full_scan(tree, expected_size, NULL, "phase2");
}

// ============================================================================
// Phase 3: Delete all keys
// ============================================================================

static bool phase3_delete_all(art_tree_t *tree, uint64_t seed,
                              uint64_t total_indices) {
    size_t before = art_size(tree);
    printf("\n--- Phase 3: Delete all %zu keys ---\n", before);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE];
    uint64_t deleted = 0;

    for (uint64_t i = 0; i < total_indices; i++) {
        generate_key(key, seed, i);
        if (art_delete(tree, key, KEY_SIZE)) {
            deleted++;
        }

        if (deleted > 0 && deleted % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  deleted %" PRIu64 " / %zu | %.1fs\n",
                   deleted, before, elapsed);
        }
    }

    if (art_size(tree) != 0) {
        fprintf(stderr, "FAIL: tree not empty after deleting all (size=%zu)\n",
                art_size(tree));
        return false;
    }

    if (!art_is_empty(tree)) {
        fprintf(stderr, "FAIL: art_is_empty returned false\n");
        return false;
    }

    // Iterator should yield nothing
    art_iterator_t *iter = art_iterator_create(tree);
    if (iter) {
        if (art_iterator_next(iter)) {
            fprintf(stderr, "FAIL: iterator returned entry on empty tree\n");
            art_iterator_destroy(iter);
            return false;
        }
        art_iterator_destroy(iter);
    }

    double elapsed = now_sec() - t0;
    printf("  Phase 3 done: deleted %" PRIu64 " keys, tree empty (%.2fs)\n",
           deleted, elapsed);

    return true;
}

// ============================================================================
// Run all phases (returns phase1 hash for determinism check)
// ============================================================================

static bool run_all_phases(uint64_t target, uint64_t seed,
                           uint64_t *out_phase1_hash, int run_num) {
    printf("\n========== Run %d (seed=0x%016" PRIx64 ") ==========\n",
           run_num, seed);

    art_tree_t tree;
    if (!art_tree_init(&tree)) {
        fprintf(stderr, "FAIL: art_tree_init failed\n");
        return false;
    }

    uint64_t phase1_hash = 0;
    if (!phase1_bulk_insert(&tree, target, seed, &phase1_hash)) {
        art_tree_destroy(&tree);
        return false;
    }
    if (out_phase1_hash) *out_phase1_hash = phase1_hash;

    uint64_t next_index = 0;
    if (!phase2_mixed(&tree, target, seed, &next_index)) {
        art_tree_destroy(&tree);
        return false;
    }

    if (!phase3_delete_all(&tree, seed, next_index)) {
        art_tree_destroy(&tree);
        return false;
    }

    art_tree_destroy(&tree);
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

    printf("\n=== mem_art Stress Test ===\n");
    printf("  target:    %" PRIu64 "M keys (%" PRIu64 ")\n",
           target_millions, target);
    printf("  key_size:  %d bytes\n", KEY_SIZE);
    printf("  val_size:  %d bytes\n", VALUE_SIZE);
    printf("  seed:      0x%016" PRIx64 "\n", seed);

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
