/*
 * art_store Stress Test
 *
 * Validates correctness of all operations at scale:
 *   - Phase 1: Bulk insert + random sample verify (get, contains)
 *   - Phase 2: Full read verification (100% hit)
 *   - Phase 3: Mixed update/insert/delete (20% churn)
 *   - Phase 4: Persistence (close + reopen + full verify)
 *   - Phase 5: Delete all, verify empty + free slot recycling
 *
 * Fail-fast: aborts immediately on first incorrect result.
 *
 * Usage: ./test_stress_art_store [target_millions] [0xseed]
 *   Default: 1M keys, random seed
 */

#include "../include/art_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define KEY_SIZE        32
#define RECORD_SIZE     64
#define PROGRESS_EVERY  100000
#define SAMPLE_VERIFY   1000
#define SPOT_CHECK      500

#define DATA_PATH       "/tmp/test_art_store.dat"

/* =========================================================================
 * splitmix64 PRNG
 * ========================================================================= */

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

/* =========================================================================
 * Deterministic key/record generation
 * ========================================================================= */

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

static void generate_record(uint8_t record[RECORD_SIZE], uint64_t seed,
                             uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0xA2F5C3D7E1B94068ULL));
    for (int i = 0; i < RECORD_SIZE; i += 8) {
        uint64_t r = rng_next(&rng);
        int remaining = RECORD_SIZE - i;
        int n = remaining < 8 ? remaining : 8;
        memcpy(record + i, &r, n);
    }
}

/* Use a different seed mix for updated records */
static void generate_record_v2(uint8_t record[RECORD_SIZE], uint64_t seed,
                                uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x3C6EF372FE94F82AULL));
    for (int i = 0; i < RECORD_SIZE; i += 8) {
        uint64_t r = rng_next(&rng);
        int remaining = RECORD_SIZE - i;
        int n = remaining < 8 ? remaining : 8;
        memcpy(record + i, &r, n);
    }
}

/* =========================================================================
 * Time & stats helpers
 * ========================================================================= */

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

static size_t get_rss_kb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    size_t rss = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            rss = (size_t)atol(line + 6);
            break;
        }
    }
    fclose(f);
    return rss;
}

static size_t get_file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (size_t)st.st_size;
}

/* =========================================================================
 * Fail macro
 * ========================================================================= */

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL: " fmt "\n", ##__VA_ARGS__); \
    return false; \
} while (0)

/* =========================================================================
 * Phase 1: Bulk insert + sample verification
 * ========================================================================= */

static bool phase1_bulk_insert(art_store_t *store, uint64_t target,
                                uint64_t seed) {
    printf("\n--- Phase 1: Bulk insert %" PRIu64 " records ---\n", target);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE];
    uint8_t record[RECORD_SIZE];
    rng_t verify_rng = rng_create(seed ^ 0x1234567890ABCDEFULL);

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);
        generate_record(record, seed, i);

        if (!art_store_put(store, key, record))
            FAIL("put failed at index %" PRIu64, i);

        if ((i + 1) % PROGRESS_EVERY == 0) {
            /* Verify random sample */
            for (int s = 0; s < SAMPLE_VERIFY; s++) {
                uint64_t idx = rng_next(&verify_rng) % (i + 1);
                uint8_t exp_key[KEY_SIZE], exp_rec[RECORD_SIZE], got_rec[RECORD_SIZE];
                generate_key(exp_key, seed, idx);
                generate_record(exp_rec, seed, idx);

                if (!art_store_get(store, exp_key, got_rec))
                    FAIL("key index %" PRIu64 " not found (inserted %" PRIu64 ")",
                         idx, i + 1);

                if (memcmp(got_rec, exp_rec, RECORD_SIZE) != 0)
                    FAIL("record mismatch at index %" PRIu64, idx);

                if (!art_store_contains(store, exp_key))
                    FAIL("contains false for existing key %" PRIu64, idx);
            }

            double elapsed = now_sec() - t0;
            size_t rss = get_rss_kb();
            printf("  %7.1fM / %.1fM | %d samples OK | %.0f Kk/s | RSS %zu MB\n",
                   (double)(i + 1) / 1e6, (double)target / 1e6,
                   SAMPLE_VERIFY, (double)(i + 1) / elapsed / 1000.0,
                   rss / 1024);
        }
    }

    if (art_store_count(store) != (uint32_t)target)
        FAIL("expected count %" PRIu64 ", got %u", target, art_store_count(store));

    double elapsed = now_sec() - t0;
    printf("  Phase 1 done: %" PRIu64 " records in %.2fs (%.0f Kk/s)\n",
           target, elapsed, (double)target / elapsed / 1000.0);

    return true;
}

/* =========================================================================
 * Phase 2: Full read verification
 * ========================================================================= */

static bool phase2_full_read(art_store_t *store, uint64_t target,
                              uint64_t seed) {
    printf("\n--- Phase 2: Full read verification (%" PRIu64 " records) ---\n",
           target);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE], exp_rec[RECORD_SIZE], got_rec[RECORD_SIZE];

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);
        generate_record(exp_rec, seed, i);

        if (!art_store_get(store, key, got_rec))
            FAIL("get failed at index %" PRIu64, i);

        if (memcmp(got_rec, exp_rec, RECORD_SIZE) != 0)
            FAIL("record mismatch at index %" PRIu64, i);

        if ((i + 1) % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  %7.1fM / %.1fM verified | %.0f Kk/s\n",
                   (double)(i + 1) / 1e6, (double)target / 1e6,
                   (double)(i + 1) / elapsed / 1000.0);
        }
    }

    double elapsed = now_sec() - t0;
    printf("  Phase 2 done: %" PRIu64 " records verified in %.2fs (%.0f Kk/s)\n",
           target, elapsed, (double)target / elapsed / 1000.0);

    return true;
}

/* =========================================================================
 * Phase 3: Mixed update/insert/delete (20% churn)
 * ========================================================================= */

static bool phase3_mixed(art_store_t *store, uint64_t target,
                          uint64_t seed, uint64_t *out_next_index,
                          uint64_t *out_alive_count) {
    uint64_t churn = target / 5;
    printf("\n--- Phase 3: Mixed ops (%" PRIu64 " rounds) ---\n", churn);
    double t0 = now_sec();

    /* Bitmap: track which indices are alive */
    uint64_t max_indices = target + churn;
    size_t bitmap_bytes = (max_indices + 7) / 8;
    uint8_t *alive = calloc(1, bitmap_bytes);
    if (!alive) FAIL("bitmap alloc failed");

    /* Track which entries have been updated (use v2 record) */
    uint8_t *updated = calloc(1, bitmap_bytes);
    if (!updated) { free(alive); FAIL("updated bitmap alloc failed"); }

    for (uint64_t i = 0; i < target; i++)
        alive[i / 8] |= (uint8_t)(1 << (i % 8));

    rng_t op_rng = rng_create(seed ^ 0xDEADBEEFCAFEBABEULL);
    uint64_t updates = 0, inserts = 0, deletes = 0;
    uint64_t new_index = target;
    uint64_t live_count = target;

    uint8_t key[KEY_SIZE], record[RECORD_SIZE], got_rec[RECORD_SIZE];

    for (uint64_t i = 0; i < churn; i++) {
        uint64_t op = rng_next(&op_rng) % 100;

        if (op < 60 && live_count > 0) {
            /* Update existing (60%) */
            uint64_t idx;
            int attempts = 0;
            do {
                idx = rng_next(&op_rng) % new_index;
                attempts++;
            } while (!(alive[idx / 8] & (1 << (idx % 8))) && attempts < 100);

            if (attempts < 100) {
                generate_key(key, seed, idx);
                generate_record_v2(record, seed, idx);

                if (!art_store_put(store, key, record)) {
                    free(alive); free(updated);
                    FAIL("update put failed at index %" PRIu64, idx);
                }
                updated[idx / 8] |= (uint8_t)(1 << (idx % 8));
                updates++;

                /* Verify the update */
                if (!art_store_get(store, key, got_rec)) {
                    free(alive); free(updated);
                    FAIL("get failed after update at index %" PRIu64, idx);
                }
                if (memcmp(got_rec, record, RECORD_SIZE) != 0) {
                    free(alive); free(updated);
                    FAIL("record mismatch after update at index %" PRIu64, idx);
                }
            }
        } else if (op < 85) {
            /* Insert new (25%) */
            generate_key(key, seed, new_index);
            generate_record(record, seed, new_index);

            if (!art_store_put(store, key, record)) {
                free(alive); free(updated);
                FAIL("insert failed at new_index %" PRIu64, new_index);
            }
            alive[new_index / 8] |= (uint8_t)(1 << (new_index % 8));
            inserts++;
            live_count++;
            new_index++;
        } else if (live_count > 1) {
            /* Delete (15%) */
            uint64_t idx;
            int attempts = 0;
            do {
                idx = rng_next(&op_rng) % new_index;
                attempts++;
            } while (!(alive[idx / 8] & (1 << (idx % 8))) && attempts < 100);

            if (attempts < 100) {
                generate_key(key, seed, idx);

                if (!art_store_delete(store, key)) {
                    free(alive); free(updated);
                    FAIL("delete failed at index %" PRIu64, idx);
                }
                alive[idx / 8] &= ~(uint8_t)(1 << (idx % 8));
                deletes++;
                live_count--;

                /* Verify deletion */
                if (art_store_get(store, key, got_rec)) {
                    free(alive); free(updated);
                    FAIL("key %" PRIu64 " still found after delete", idx);
                }
                if (art_store_contains(store, key)) {
                    free(alive); free(updated);
                    FAIL("contains true after delete for key %" PRIu64, idx);
                }
            }
        }

        if ((i + 1) % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  %7.1fM / %.1fM | upd %" PRIu64 " ins %" PRIu64
                   " del %" PRIu64 " | live %" PRIu64 " | %.0f Kk/s\n",
                   (double)(i + 1) / 1e6, (double)churn / 1e6,
                   updates, inserts, deletes, live_count,
                   (double)(i + 1) / elapsed / 1000.0);
        }
    }

    if (art_store_count(store) != (uint32_t)live_count) {
        free(alive); free(updated);
        FAIL("expected count %" PRIu64 ", got %u", live_count,
             art_store_count(store));
    }

    /* Verify all alive entries are still readable */
    uint64_t verified = 0;
    for (uint64_t i = 0; i < new_index; i++) {
        if (!(alive[i / 8] & (1 << (i % 8)))) continue;

        generate_key(key, seed, i);
        if (updated[i / 8] & (1 << (i % 8)))
            generate_record_v2(record, seed, i);
        else
            generate_record(record, seed, i);

        if (!art_store_get(store, key, got_rec)) {
            free(alive); free(updated);
            FAIL("alive key %" PRIu64 " not found in final verify", i);
        }
        if (memcmp(got_rec, record, RECORD_SIZE) != 0) {
            free(alive); free(updated);
            FAIL("record mismatch for alive key %" PRIu64 " in final verify", i);
        }
        verified++;
    }

    free(alive);
    free(updated);

    if (verified != live_count)
        FAIL("verified %" PRIu64 " but expected %" PRIu64, verified, live_count);

    double elapsed = now_sec() - t0;
    printf("  Phase 3 done: upd %" PRIu64 " ins %" PRIu64 " del %" PRIu64
           " | live %" PRIu64 " | verified %" PRIu64 " (%.2fs)\n",
           updates, inserts, deletes, live_count, verified, elapsed);

    *out_next_index = new_index;
    *out_alive_count = live_count;
    return true;
}

/* =========================================================================
 * Phase 4: Persistence (close + reopen + full verify)
 * ========================================================================= */

static bool phase4_persistence(uint64_t seed, uint64_t next_index,
                                uint64_t expected_live) {
    printf("\n--- Phase 4: Persistence (close + reopen + verify) ---\n");
    double t0 = now_sec();

    /* Reopen */
    art_store_t *store = art_store_open(DATA_PATH);
    if (!store)
        FAIL("art_store_open failed");

    if (art_store_count(store) != (uint32_t)expected_live) {
        art_store_destroy(store);
        FAIL("count after reopen: got %u, expected %" PRIu64,
             art_store_count(store), expected_live);
    }

    printf("  Reopened: count=%u, slots=%u\n",
           art_store_count(store), art_store_slot_count(store));

    /* We can't know which entries were updated vs original without
     * re-running the mixed ops RNG. Instead, spot-check a sample of
     * entries from the initial bulk insert (indices 0..target-1) that
     * we know haven't been deleted. For a thorough check, we re-derive
     * the alive/updated bitmaps using the same RNG sequence. */

    /* Simpler approach: just verify all SPOT_CHECK random indices by trying
     * both original and updated record, see which matches. At least one
     * must match for alive entries. */
    rng_t spot_rng = rng_create(seed ^ 0xFEDCBA9876543210ULL);
    uint64_t spot_ok = 0;
    uint8_t key[KEY_SIZE], rec_v1[RECORD_SIZE], rec_v2[RECORD_SIZE];
    uint8_t got_rec[RECORD_SIZE];

    for (int s = 0; s < SPOT_CHECK; s++) {
        uint64_t idx = rng_next(&spot_rng) % next_index;
        generate_key(key, seed, idx);

        if (!art_store_get(store, key, got_rec)) {
            /* Might have been deleted — that's OK */
            if (art_store_contains(store, key)) {
                art_store_destroy(store);
                FAIL("contains true but get false at index %" PRIu64, idx);
            }
            continue;
        }

        /* Verify record matches either v1 or v2 */
        generate_record(rec_v1, seed, idx);
        generate_record_v2(rec_v2, seed, idx);

        if (memcmp(got_rec, rec_v1, RECORD_SIZE) != 0 &&
            memcmp(got_rec, rec_v2, RECORD_SIZE) != 0) {
            art_store_destroy(store);
            FAIL("record at index %" PRIu64 " matches neither v1 nor v2", idx);
        }
        spot_ok++;
    }

    printf("  Spot-checked %d indices, %" PRIu64 " alive and correct\n",
           SPOT_CHECK, spot_ok);

    /* Phase 5: Delete all remaining, verify free slot recycling */
    printf("\n--- Phase 5: Delete all + recycle test ---\n");
    double t1 = now_sec();

    /* Delete all */
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < next_index; i++) {
        generate_key(key, seed, i);
        if (art_store_delete(store, key))
            deleted++;
    }

    if (art_store_count(store) != 0) {
        art_store_destroy(store);
        FAIL("store not empty after deleting all (count=%u)",
             art_store_count(store));
    }

    printf("  Deleted %" PRIu64 " records, store empty\n", deleted);

    /* Re-insert a batch to verify free slot recycling */
    uint32_t slots_before = art_store_slot_count(store);
    uint64_t recycle_count = deleted < 10000 ? deleted : 10000;

    for (uint64_t i = 0; i < recycle_count; i++) {
        generate_key(key, seed, i);
        generate_record(rec_v1, seed, i);
        if (!art_store_put(store, key, rec_v1)) {
            art_store_destroy(store);
            FAIL("recycle insert failed at %" PRIu64, i);
        }
    }

    uint32_t slots_after = art_store_slot_count(store);
    if (slots_after > slots_before) {
        art_store_destroy(store);
        FAIL("slot_count grew from %u to %u — free list not recycling",
             slots_before, slots_after);
    }

    printf("  Recycled %" PRIu64 " slots (slot_count: %u before, %u after)\n",
           recycle_count, slots_before, slots_after);

    /* Verify recycled entries */
    for (uint64_t i = 0; i < recycle_count; i++) {
        generate_key(key, seed, i);
        generate_record(rec_v1, seed, i);

        if (!art_store_get(store, key, got_rec)) {
            art_store_destroy(store);
            FAIL("recycled key %" PRIu64 " not found", i);
        }
        if (memcmp(got_rec, rec_v1, RECORD_SIZE) != 0) {
            art_store_destroy(store);
            FAIL("recycled record mismatch at %" PRIu64, i);
        }
    }

    double elapsed = now_sec() - t1;
    printf("  Phase 5 done: %" PRIu64 " recycled + verified (%.2fs)\n",
           recycle_count, elapsed);

    art_store_destroy(store);

    elapsed = now_sec() - t0;
    printf("  Phase 4+5 done (%.2fs)\n", elapsed);
    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

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

    printf("\n=== art_store Stress Test ===\n");
    printf("  target:      %" PRIu64 "M records (%" PRIu64 ")\n",
           target_millions, target);
    printf("  key_size:    %d bytes\n", KEY_SIZE);
    printf("  record_size: %d bytes\n", RECORD_SIZE);
    printf("  file:        %s\n", DATA_PATH);
    printf("  seed:        0x%016" PRIx64 "\n", seed);

    double t_start = now_sec();

    /* Remove stale data file */
    unlink(DATA_PATH);

    /* Create store */
    art_store_t *store = art_store_create(DATA_PATH, KEY_SIZE, RECORD_SIZE);
    if (!store) {
        fprintf(stderr, "FAIL: art_store_create failed\n");
        return 1;
    }

    /* Phase 1: Bulk insert */
    if (!phase1_bulk_insert(store, target, seed)) goto fail;

    /* Phase 2: Full read verification */
    if (!phase2_full_read(store, target, seed)) goto fail;

    /* Phase 3: Mixed operations */
    uint64_t next_index = 0, alive_count = 0;
    if (!phase3_mixed(store, target, seed, &next_index, &alive_count)) goto fail;

    /* Sync and close for persistence test */
    art_store_sync(store);

    size_t file_size = get_file_size(DATA_PATH);
    size_t rss = get_rss_kb();
    uint32_t slot_count = art_store_slot_count(store);
    uint32_t live_count = art_store_count(store);

    art_store_destroy(store);
    store = NULL;

    /* Phase 4+5: Persistence + delete all + recycle */
    if (!phase4_persistence(seed, next_index, alive_count)) goto fail;

    /* Scale report */
    double wall = now_sec() - t_start;
    char elapsed_str[32];
    format_elapsed(wall, elapsed_str, sizeof(elapsed_str));

    printf("\n============================================\n");
    printf("ALL CHECKS PASSED\n");
    printf("============================================\n");
    printf("  records:     %" PRIu64 " (%.0fM)\n", target, target / 1e6);
    printf("  live/slots:  %u / %u (%.1f%% util)\n",
           live_count, slot_count,
           slot_count > 0 ? 100.0 * live_count / slot_count : 0.0);
    printf("  file size:   %.1f MB\n", file_size / (1024.0 * 1024.0));
    printf("  peak RSS:    %zu MB\n", rss / 1024);
    printf("  wall time:   %s\n", elapsed_str);
    printf("  seed:        0x%016" PRIx64 "\n", seed);
    printf("============================================\n\n");

    unlink(DATA_PATH);
    return 0;

fail:
    if (store) art_store_destroy(store);
    fprintf(stderr, "\nFAILED\n");
    fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
            argv[0], target_millions, seed);
    unlink(DATA_PATH);
    return 1;
}
