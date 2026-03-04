/*
 * disk_hash Stress Test
 *
 * Validates correctness of all operations at scale:
 *   - Phase 1: Bulk insert + random sample verify (single ops)
 *   - Phase 2: Full read verification (100% hit)
 *   - Phase 3: Mixed update/insert/delete (20% churn)
 *   - Phase 4: Batch operations (batch_put + batch_get cross-check)
 *   - Phase 5: Persistence (sync → destroy → open → full verify)
 *   - Phase 6: Delete all, verify empty
 *
 * Fail-fast: aborts immediately on first incorrect result.
 *
 * Usage: ./test_stress_disk_hash [target_millions] [0xseed]
 *   Default: 1M keys, random seed
 */

#include "../include/disk_hash.h"

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
#define RECORD_SIZE     32
#define PROGRESS_EVERY  100000
#define SAMPLE_VERIFY   1000
#define BATCH_SIZE      5000

#define DATA_PATH       "/tmp/test_stress_disk_hash.dat"

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
        memcpy(record + i, &r, (size_t)n);
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
        memcpy(record + i, &r, (size_t)n);
    }
}

/* =========================================================================
 * Bitmap helpers
 * ========================================================================= */

static inline bool bitmap_get(const uint8_t *bm, uint64_t i) {
    return (bm[i / 8] >> (i % 8)) & 1;
}

static inline void bitmap_set(uint8_t *bm, uint64_t i) {
    bm[i / 8] |= (uint8_t)(1 << (i % 8));
}

static inline void bitmap_clear(uint8_t *bm, uint64_t i) {
    bm[i / 8] &= ~(uint8_t)(1 << (i % 8));
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

static bool phase1_bulk_insert(disk_hash_t *dh, uint64_t target,
                                uint64_t seed) {
    printf("\n--- Phase 1: Bulk insert %" PRIu64 " records ---\n", target);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE];
    uint8_t record[RECORD_SIZE];
    rng_t verify_rng = rng_create(seed ^ 0x1234567890ABCDEFULL);

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);
        generate_record(record, seed, i);

        if (!disk_hash_put(dh, key, record))
            FAIL("put failed at index %" PRIu64, i);

        if ((i + 1) % PROGRESS_EVERY == 0) {
            /* Verify random sample */
            for (int s = 0; s < SAMPLE_VERIFY; s++) {
                uint64_t idx = rng_next(&verify_rng) % (i + 1);
                uint8_t exp_key[KEY_SIZE], exp_rec[RECORD_SIZE], got_rec[RECORD_SIZE];
                generate_key(exp_key, seed, idx);
                generate_record(exp_rec, seed, idx);

                if (!disk_hash_get(dh, exp_key, got_rec))
                    FAIL("key index %" PRIu64 " not found (inserted %" PRIu64 ")",
                         idx, i + 1);

                if (memcmp(got_rec, exp_rec, RECORD_SIZE) != 0)
                    FAIL("record mismatch at index %" PRIu64, idx);

                if (!disk_hash_contains(dh, exp_key))
                    FAIL("contains false for existing key %" PRIu64, idx);
            }

            double elapsed = now_sec() - t0;
            size_t rss = get_rss_kb();
            printf("  %7.1fM / %.1fM | %d samples OK | %.0f Kk/s | RSS %zu KB\n",
                   (double)(i + 1) / 1e6, (double)target / 1e6,
                   SAMPLE_VERIFY, (double)(i + 1) / elapsed / 1000.0,
                   rss);
        }
    }

    if (disk_hash_count(dh) != target)
        FAIL("expected count %" PRIu64 ", got %" PRIu64, target,
             disk_hash_count(dh));

    double elapsed = now_sec() - t0;
    printf("  Phase 1 done: %" PRIu64 " records in %.2fs (%.0f Kk/s)\n",
           target, elapsed, (double)target / elapsed / 1000.0);

    return true;
}

/* =========================================================================
 * Phase 2: Full read verification
 * ========================================================================= */

static bool phase2_full_read(disk_hash_t *dh, uint64_t target,
                              uint64_t seed) {
    printf("\n--- Phase 2: Full read verification (%" PRIu64 " records) ---\n",
           target);
    double t0 = now_sec();

    uint8_t key[KEY_SIZE], exp_rec[RECORD_SIZE], got_rec[RECORD_SIZE];

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);
        generate_record(exp_rec, seed, i);

        if (!disk_hash_get(dh, key, got_rec))
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

static bool phase3_mixed(disk_hash_t *dh, uint64_t target, uint64_t seed,
                          uint8_t **out_alive, uint8_t **out_updated,
                          uint64_t *out_next_index, uint64_t *out_live_count) {
    uint64_t churn = target / 5;
    printf("\n--- Phase 3: Mixed ops (%" PRIu64 " rounds) ---\n", churn);
    double t0 = now_sec();

    uint64_t max_indices = target + churn;
    size_t bitmap_bytes = (max_indices + 7) / 8;
    uint8_t *alive = calloc(1, bitmap_bytes);
    uint8_t *updated = calloc(1, bitmap_bytes);
    if (!alive || !updated) FAIL("bitmap alloc failed");

    for (uint64_t i = 0; i < target; i++)
        bitmap_set(alive, i);

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
            } while (!bitmap_get(alive, idx) && attempts < 100);

            if (attempts < 100) {
                generate_key(key, seed, idx);
                generate_record_v2(record, seed, idx);

                if (!disk_hash_put(dh, key, record)) {
                    free(alive); free(updated);
                    FAIL("update put failed at index %" PRIu64, idx);
                }
                bitmap_set(updated, idx);
                updates++;

                /* Verify immediately */
                if (!disk_hash_get(dh, key, got_rec)) {
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

            if (!disk_hash_put(dh, key, record)) {
                free(alive); free(updated);
                FAIL("insert failed at new_index %" PRIu64, new_index);
            }
            bitmap_set(alive, new_index);
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
            } while (!bitmap_get(alive, idx) && attempts < 100);

            if (attempts < 100) {
                generate_key(key, seed, idx);

                if (!disk_hash_delete(dh, key)) {
                    free(alive); free(updated);
                    FAIL("delete failed at index %" PRIu64, idx);
                }
                bitmap_clear(alive, idx);
                deletes++;
                live_count--;

                /* Verify immediately */
                if (disk_hash_get(dh, key, got_rec)) {
                    free(alive); free(updated);
                    FAIL("key %" PRIu64 " still found after delete", idx);
                }
                if (disk_hash_contains(dh, key)) {
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

    if (disk_hash_count(dh) != live_count) {
        free(alive); free(updated);
        FAIL("expected count %" PRIu64 ", got %" PRIu64, live_count,
             disk_hash_count(dh));
    }

    /* Full verify all alive entries */
    printf("  Verifying all %" PRIu64 " alive entries...\n", live_count);
    uint64_t verified = 0;
    for (uint64_t i = 0; i < new_index; i++) {
        if (!bitmap_get(alive, i)) continue;

        generate_key(key, seed, i);
        if (bitmap_get(updated, i))
            generate_record_v2(record, seed, i);
        else
            generate_record(record, seed, i);

        if (!disk_hash_get(dh, key, got_rec)) {
            free(alive); free(updated);
            FAIL("alive key %" PRIu64 " not found in final verify", i);
        }
        if (memcmp(got_rec, record, RECORD_SIZE) != 0) {
            free(alive); free(updated);
            FAIL("record mismatch for alive key %" PRIu64 " in final verify", i);
        }
        verified++;
    }

    if (verified != live_count) {
        free(alive); free(updated);
        FAIL("verified %" PRIu64 " but expected %" PRIu64, verified, live_count);
    }

    double elapsed = now_sec() - t0;
    printf("  Phase 3 done: upd %" PRIu64 " ins %" PRIu64 " del %" PRIu64
           " | live %" PRIu64 " | verified %" PRIu64 " (%.2fs)\n",
           updates, inserts, deletes, live_count, verified, elapsed);

    *out_alive      = alive;
    *out_updated    = updated;
    *out_next_index = new_index;
    *out_live_count = live_count;
    return true;
}

/* =========================================================================
 * Phase 4: Batch operations cross-check
 * ========================================================================= */

static bool phase4_batch(disk_hash_t *dh, uint64_t seed,
                          const uint8_t *alive, const uint8_t *updated,
                          uint64_t next_index, uint64_t live_count) {
    printf("\n--- Phase 4: Batch operations cross-check ---\n");
    double t0 = now_sec();

    uint8_t *keys   = malloc((uint64_t)BATCH_SIZE * KEY_SIZE);
    uint8_t *recs   = malloc((uint64_t)BATCH_SIZE * RECORD_SIZE);
    uint8_t *got    = malloc((uint64_t)BATCH_SIZE * RECORD_SIZE);
    bool    *found  = malloc(BATCH_SIZE * sizeof(bool));
    if (!keys || !recs || !got || !found) FAIL("alloc failed");

    /* Test 1: batch_get all alive entries in chunks */
    printf("  batch_get alive entries (chunks of %d)...\n", BATCH_SIZE);
    uint64_t total_found = 0;
    uint32_t batch_n = 0;

    for (uint64_t i = 0; i < next_index; i++) {
        if (!bitmap_get(alive, i)) continue;

        generate_key(keys + (uint64_t)batch_n * KEY_SIZE, seed, i);
        if (bitmap_get(updated, i))
            generate_record(recs + (uint64_t)batch_n * RECORD_SIZE, seed, i);
        else
            generate_record(recs + (uint64_t)batch_n * RECORD_SIZE, seed, i);
        batch_n++;

        if (batch_n == BATCH_SIZE) {
            memset(got, 0, (uint64_t)BATCH_SIZE * RECORD_SIZE);
            uint32_t fc = disk_hash_batch_get(dh, keys, got, found, batch_n);
            if (fc != batch_n) {
                free(keys); free(recs); free(got); free(found);
                FAIL("batch_get found %u / %u (at total %" PRIu64 ")",
                     fc, batch_n, total_found);
            }
            for (uint32_t j = 0; j < batch_n; j++) {
                if (!found[j]) {
                    free(keys); free(recs); free(got); free(found);
                    FAIL("batch_get: key %u not found", j);
                }
            }
            total_found += fc;
            batch_n = 0;
        }
    }
    /* Flush remaining */
    if (batch_n > 0) {
        uint32_t fc = disk_hash_batch_get(dh, keys, got, found, batch_n);
        if (fc != batch_n) {
            free(keys); free(recs); free(got); free(found);
            FAIL("batch_get tail found %u / %u", fc, batch_n);
        }
        total_found += fc;
    }

    if (total_found != live_count) {
        free(keys); free(recs); free(got); free(found);
        FAIL("batch_get total %" PRIu64 " != live %" PRIu64,
             total_found, live_count);
    }
    printf("  batch_get OK: %" PRIu64 " / %" PRIu64 " found\n",
           total_found, live_count);

    /* Test 2: batch_get with dead keys (should not find them) */
    printf("  batch_get dead keys...\n");
    batch_n = 0;
    uint64_t dead_tested = 0;
    for (uint64_t i = 0; i < next_index && dead_tested < 10000; i++) {
        if (bitmap_get(alive, i)) continue;

        generate_key(keys + (uint64_t)batch_n * KEY_SIZE, seed, i);
        batch_n++;
        dead_tested++;

        if (batch_n == BATCH_SIZE) {
            uint32_t fc = disk_hash_batch_get(dh, keys, got, found, batch_n);
            if (fc != 0) {
                free(keys); free(recs); free(got); free(found);
                FAIL("batch_get dead keys found %u (should be 0)", fc);
            }
            batch_n = 0;
        }
    }
    if (batch_n > 0) {
        uint32_t fc = disk_hash_batch_get(dh, keys, got, found, batch_n);
        if (fc != 0) {
            free(keys); free(recs); free(got); free(found);
            FAIL("batch_get dead tail found %u", fc);
        }
    }
    printf("  batch_get dead OK: %" PRIu64 " dead keys correctly not found\n",
           dead_tested);

    /* Test 3: batch_put new keys, then batch_get to verify */
    printf("  batch_put + batch_get new keys...\n");
    uint64_t batch_base = next_index + 1000000;  /* well beyond existing */
    for (uint32_t i = 0; i < BATCH_SIZE; i++) {
        generate_key(keys + (uint64_t)i * KEY_SIZE, seed, batch_base + i);
        generate_record(recs + (uint64_t)i * RECORD_SIZE, seed, batch_base + i);
    }

    if (!disk_hash_batch_put(dh, keys, recs, BATCH_SIZE)) {
        free(keys); free(recs); free(got); free(found);
        FAIL("batch_put new keys failed");
    }

    memset(got, 0, (uint64_t)BATCH_SIZE * RECORD_SIZE);
    uint32_t fc = disk_hash_batch_get(dh, keys, got, found, BATCH_SIZE);
    if (fc != BATCH_SIZE) {
        free(keys); free(recs); free(got); free(found);
        FAIL("batch_get after batch_put: found %u / %u", fc, BATCH_SIZE);
    }
    for (uint32_t i = 0; i < BATCH_SIZE; i++) {
        if (!found[i]) {
            free(keys); free(recs); free(got); free(found);
            FAIL("batch key %u not found after batch_put", i);
        }
        uint8_t expected[RECORD_SIZE];
        generate_record(expected, seed, batch_base + i);
        if (memcmp(got + (uint64_t)i * RECORD_SIZE, expected, RECORD_SIZE) != 0) {
            free(keys); free(recs); free(got); free(found);
            FAIL("batch record %u mismatch after batch_put", i);
        }
    }
    printf("  batch_put + batch_get OK: %u new keys\n", BATCH_SIZE);

    /* Delete the batch keys so they don't interfere with phase 5 count */
    for (uint32_t i = 0; i < BATCH_SIZE; i++) {
        generate_key(keys + (uint64_t)i * KEY_SIZE, seed, batch_base + i);
        uint8_t *k = keys + (uint64_t)i * KEY_SIZE;
        disk_hash_delete(dh, k);
    }

    if (disk_hash_count(dh) != live_count) {
        free(keys); free(recs); free(got); free(found);
        FAIL("count after batch cleanup: %" PRIu64 " != %" PRIu64,
             disk_hash_count(dh), live_count);
    }

    free(keys); free(recs); free(got); free(found);

    double elapsed = now_sec() - t0;
    printf("  Phase 4 done (%.2fs)\n", elapsed);
    return true;
}

/* =========================================================================
 * Phase 5: Persistence (close + reopen + full verify)
 * ========================================================================= */

static bool phase5_persistence(uint64_t seed,
                                const uint8_t *alive, const uint8_t *updated,
                                uint64_t next_index, uint64_t expected_live) {
    printf("\n--- Phase 5: Persistence (close + reopen + verify) ---\n");
    double t0 = now_sec();

    disk_hash_t *dh = disk_hash_open(DATA_PATH);
    if (!dh) FAIL("disk_hash_open failed");

    if (disk_hash_count(dh) != expected_live) {
        uint64_t got = disk_hash_count(dh);
        disk_hash_destroy(dh);
        FAIL("count after reopen: got %" PRIu64 ", expected %" PRIu64,
             got, expected_live);
    }

    printf("  Reopened: count=%" PRIu64 ", capacity=%" PRIu64 "\n",
           disk_hash_count(dh), disk_hash_capacity(dh));

    /* Full verify */
    uint8_t key[KEY_SIZE], record[RECORD_SIZE], got_rec[RECORD_SIZE];
    uint64_t verified = 0;

    for (uint64_t i = 0; i < next_index; i++) {
        if (!bitmap_get(alive, i)) {
            /* Verify dead key is really gone */
            generate_key(key, seed, i);
            if (disk_hash_contains(dh, key)) {
                disk_hash_destroy(dh);
                FAIL("dead key %" PRIu64 " found after reopen", i);
            }
            continue;
        }

        generate_key(key, seed, i);
        if (bitmap_get(updated, i))
            generate_record_v2(record, seed, i);
        else
            generate_record(record, seed, i);

        if (!disk_hash_get(dh, key, got_rec)) {
            disk_hash_destroy(dh);
            FAIL("alive key %" PRIu64 " not found after reopen", i);
        }
        if (memcmp(got_rec, record, RECORD_SIZE) != 0) {
            disk_hash_destroy(dh);
            FAIL("record mismatch for key %" PRIu64 " after reopen", i);
        }
        verified++;

        if ((verified) % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  %7.1fM / %.1fM verified | %.0f Kk/s\n",
                   (double)verified / 1e6, (double)expected_live / 1e6,
                   (double)verified / elapsed / 1000.0);
        }
    }

    if (verified != expected_live) {
        disk_hash_destroy(dh);
        FAIL("verified %" PRIu64 " but expected %" PRIu64, verified,
             expected_live);
    }

    double elapsed = now_sec() - t0;
    printf("  Phase 5 done: %" PRIu64 " verified (%.2fs)\n", verified, elapsed);

    disk_hash_destroy(dh);
    return true;
}

/* =========================================================================
 * Phase 6: Delete all, verify empty
 * ========================================================================= */

static bool phase6_delete_all(uint64_t seed,
                               const uint8_t *alive, uint64_t next_index,
                               uint64_t expected_live) {
    printf("\n--- Phase 6: Delete all + verify empty ---\n");
    double t0 = now_sec();

    disk_hash_t *dh = disk_hash_open(DATA_PATH);
    if (!dh) FAIL("disk_hash_open failed");

    uint8_t key[KEY_SIZE];
    uint64_t deleted = 0;

    for (uint64_t i = 0; i < next_index; i++) {
        if (!bitmap_get(alive, i)) continue;

        generate_key(key, seed, i);
        if (!disk_hash_delete(dh, key)) {
            disk_hash_destroy(dh);
            FAIL("delete failed at index %" PRIu64, i);
        }
        deleted++;

        if (deleted % PROGRESS_EVERY == 0) {
            double elapsed = now_sec() - t0;
            printf("  %7.1fM / %.1fM deleted | %.0f Kk/s\n",
                   (double)deleted / 1e6, (double)expected_live / 1e6,
                   (double)deleted / elapsed / 1000.0);
        }
    }

    if (disk_hash_count(dh) != 0) {
        disk_hash_destroy(dh);
        FAIL("not empty after deleting all (count=%" PRIu64 ")",
             disk_hash_count(dh));
    }

    /* Verify no key is found */
    uint8_t got_rec[RECORD_SIZE];
    for (uint64_t i = 0; i < next_index; i++) {
        generate_key(key, seed, i);
        if (disk_hash_get(dh, key, got_rec)) {
            disk_hash_destroy(dh);
            FAIL("key %" PRIu64 " still found after full delete", i);
        }
    }

    printf("  Deleted %" PRIu64 " records, all verified gone\n", deleted);

    /* Re-insert a sample to verify tombstone reuse works */
    uint64_t reinsert_count = deleted < 10000 ? deleted : 10000;
    uint8_t record[RECORD_SIZE];
    for (uint64_t i = 0; i < reinsert_count; i++) {
        generate_key(key, seed, i);
        generate_record(record, seed, i);
        if (!disk_hash_put(dh, key, record)) {
            disk_hash_destroy(dh);
            FAIL("re-insert failed at %" PRIu64, i);
        }
    }

    /* Verify re-inserted */
    for (uint64_t i = 0; i < reinsert_count; i++) {
        generate_key(key, seed, i);
        generate_record(record, seed, i);
        if (!disk_hash_get(dh, key, got_rec)) {
            disk_hash_destroy(dh);
            FAIL("re-inserted key %" PRIu64 " not found", i);
        }
        if (memcmp(got_rec, record, RECORD_SIZE) != 0) {
            disk_hash_destroy(dh);
            FAIL("re-inserted record %" PRIu64 " mismatch", i);
        }
    }

    if (disk_hash_count(dh) != reinsert_count) {
        disk_hash_destroy(dh);
        FAIL("count after re-insert: %" PRIu64 " != %" PRIu64,
             disk_hash_count(dh), reinsert_count);
    }

    printf("  Re-inserted + verified %" PRIu64 " records (tombstone reuse OK)\n",
           reinsert_count);

    disk_hash_destroy(dh);

    double elapsed = now_sec() - t0;
    printf("  Phase 6 done (%.2fs)\n", elapsed);
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

    printf("\n=== disk_hash Stress Test ===\n");
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
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, RECORD_SIZE, target);
    if (!dh) {
        fprintf(stderr, "FAIL: disk_hash_create failed\n");
        return 1;
    }

    printf("  capacity:    %" PRIu64 " slots (%" PRIu64 " buckets)\n",
           disk_hash_capacity(dh), disk_hash_capacity(dh) /
           ((4096 - 8) / (1 + KEY_SIZE + RECORD_SIZE)));

    /* Phase 1: Bulk insert */
    if (!phase1_bulk_insert(dh, target, seed)) goto fail;

    /* Phase 2: Full read verification */
    if (!phase2_full_read(dh, target, seed)) goto fail;

    /* Phase 3: Mixed operations */
    uint8_t *alive = NULL, *updated = NULL;
    uint64_t next_index = 0, live_count = 0;
    if (!phase3_mixed(dh, target, seed, &alive, &updated,
                       &next_index, &live_count)) goto fail;

    /* Phase 4: Batch operations */
    if (!phase4_batch(dh, seed, alive, updated, next_index, live_count))
        goto fail;

    /* Sync and close for persistence test */
    disk_hash_sync(dh);

    size_t file_size = get_file_size(DATA_PATH);
    size_t rss = get_rss_kb();

    disk_hash_destroy(dh);
    dh = NULL;

    /* Phase 5: Persistence */
    if (!phase5_persistence(seed, alive, updated, next_index, live_count))
        goto fail;

    /* Phase 6: Delete all */
    if (!phase6_delete_all(seed, alive, next_index, live_count))
        goto fail;

    /* Scale report */
    double wall = now_sec() - t_start;
    char elapsed_str[32];
    format_elapsed(wall, elapsed_str, sizeof(elapsed_str));

    printf("\n============================================\n");
    printf("ALL CHECKS PASSED\n");
    printf("============================================\n");
    printf("  records:     %" PRIu64 " (%.0fM)\n", target, target / 1e6);
    printf("  live after churn: %" PRIu64 "\n", live_count);
    printf("  file size:   %.1f MB\n", file_size / (1024.0 * 1024.0));
    printf("  peak RSS:    %zu KB\n", rss);
    printf("  wall time:   %s\n", elapsed_str);
    printf("  seed:        0x%016" PRIx64 "\n", seed);
    printf("============================================\n\n");

    free(alive);
    free(updated);
    unlink(DATA_PATH);
    return 0;

fail:
    if (dh) disk_hash_destroy(dh);
    free(alive);
    free(updated);
    fprintf(stderr, "\nFAILED\n");
    fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
            argv[0], target_millions, seed);
    unlink(DATA_PATH);
    return 1;
}
