/*
 * Index Structure Scale Benchmark
 *
 * Compares two implementations:
 *
 *   nibble_trie:  16-way nibble trie (in-memory, arena-allocated)
 *   compact_art:  in-memory ART (no persistence)
 *
 * Workload: configurable key sizes (32B or 64B) + 32B values
 *
 * Usage: ./bench_nibble_trie [target_millions] [mode]
 *   Modes: nt | nt64 | cart | both | rss
 *   Default: 1M keys, both
 */

#include "../include/nibble_trie.h"
#include "../include/compact_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>

/* ========================================================================
 * SplitMix64 RNG + key generation
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

/* Generate key of arbitrary size */
static void generate_key(uint8_t *key, int key_size, uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    for (int i = 0; i < key_size; i += 8) {
        uint64_t r = rng_next(&rng);
        int copy = (key_size - i) < 8 ? (key_size - i) : 8;
        memcpy(key + i, &r, copy);
    }
}

/* Account value: 32 bytes */
static void generate_val32(uint8_t val[32], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x9ABCDEF012345678ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(val + i, &r, 8);
    }
}

/* ========================================================================
 * Helpers
 * ======================================================================== */

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

static double elapsed_ms(struct timespec *t0, struct timespec *t1) {
    return (t1->tv_sec - t0->tv_sec) * 1e3 +
           (t1->tv_nsec - t0->tv_nsec) / 1e6;
}

#define SEED 0x42424242BEEFCAFEULL
#define VAL_SIZE 32

/* ========================================================================
 * Benchmark: nibble_trie — parameterized by key_size
 * ======================================================================== */

static void bench_nt(uint64_t target, int key_size) {
    struct timespec t0, t1;

    printf("\n=== nibble_trie — %dB key + %dB val ===\n", key_size, VAL_SIZE);
    printf("  target: %" PRIu64 " keys (in-memory, arena-allocated)\n\n", target);

    nibble_trie_t t;
    if (!nt_init(&t, (uint32_t)key_size, VAL_SIZE)) {
        printf("  FAILED to init\n");
        return;
    }

    uint8_t key[64], val[32];
    uint64_t milestone = target < 1000000 ? target / 10 : 1000000;
    if (milestone == 0) milestone = 1;

    printf("  %-12s %8s %8s %8s %8s\n",
           "Phase", "Keys", "ms", "Kk/s", "RSS MB");
    printf("  %-12s %8s %8s %8s %8s\n",
           "-----", "----", "--", "----", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, key_size, SEED, i);
        generate_val32(val, SEED, i);
        nt_insert(&t, key, val);

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double ms = elapsed_ms(&last, &t1);
            double kks = (double)milestone / ms;
            printf("  insert       %8" PRIu64 " %8.0f %8.0f %8zu\n",
                   i + 1, elapsed_ms(&t0, &t1), kks, get_rss_mb());
            last = t1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double insert_ms = elapsed_ms(&t0, &t1);
    printf("  INSERT TOTAL %8" PRIu64 " %8.0f %8.0f %8zu\n\n",
           target, insert_ms, (double)target / insert_ms, get_rss_mb());

    uint64_t lookups = target < 1000000 ? target : 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t found = 0;
    for (uint64_t i = 0; i < lookups; i++) {
        uint64_t idx = (i * 7919) % target;
        generate_key(key, key_size, SEED, idx);
        if (nt_get(&t, key) != NULL) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  LOOKUP       %8" PRIu64 " %8.0f %8.0f %8s (found=%" PRIu64 ")\n",
           lookups, elapsed_ms(&t0, &t1), (double)lookups / elapsed_ms(&t0, &t1),
           "", found);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    nt_iterator_t *it = nt_iterator_create(&t);
    uint64_t iter_count = 0;
    while (nt_iterator_next(it)) iter_count++;
    nt_iterator_destroy(it);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  ITERATE      %8" PRIu64 " %8.0f %8.0f\n",
           iter_count, elapsed_ms(&t0, &t1),
           (double)iter_count / elapsed_ms(&t0, &t1));

    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < target; i += 2) {
        generate_key(key, key_size, SEED, i);
        if (nt_delete(&t, key)) deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  DELETE half  %8" PRIu64 " %8.0f %8.0f\n",
           deleted, elapsed_ms(&t0, &t1),
           (double)deleted / elapsed_ms(&t0, &t1));

    printf("\n  Memory (RSS):  %zu MB\n", get_rss_mb());
    printf("  Tree size:     %" PRIu64 " keys\n", (uint64_t)nt_size(&t));

    nt_destroy(&t);
}

/* ========================================================================
 * RSS-only mode: insert-only, reports RSS at end
 * ======================================================================== */

static void bench_rss(uint64_t target, int key_size) {
    struct timespec t0, t1;

    printf("\n=== RSS Measurement — nibble_trie %dB key + %dB val ===\n",
           key_size, VAL_SIZE);
    printf("  target: %" PRIu64 " keys\n\n", target);

    nibble_trie_t t;
    if (!nt_init(&t, (uint32_t)key_size, VAL_SIZE)) {
        printf("  FAILED to init\n");
        return;
    }

    uint8_t key[64], val[32];
    uint64_t milestone = target / 10;
    if (milestone == 0) milestone = 1;

    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, key_size, SEED, i);
        generate_val32(val, SEED, i);
        nt_insert(&t, key, val);

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            size_t rss = get_rss_mb();
            printf("  %8" PRIu64 " keys | %6.0f ms | RSS %4zu MB | %zu B/key\n",
                   i + 1, elapsed_ms(&t0, &t1), rss,
                   rss * 1024 * 1024 / (i + 1));
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    size_t rss = get_rss_mb();
    printf("\n  FINAL: %" PRIu64 " keys | %.0f ms | RSS %zu MB | %zu B/key\n",
           target, elapsed_ms(&t0, &t1), rss,
           rss * 1024 * 1024 / target);

    nt_destroy(&t);
}

/* ========================================================================
 * Index simulation: 4B values (slot refs), matches real data_layer
 *
 * Runs both nibble_trie and compact_art side-by-side with the actual
 * value size used in production (uint32_t slot references).
 * ======================================================================== */

static void bench_index(uint64_t target_storage) {
    struct timespec t0, t1;
    rng_t rng = rng_create(0x494E44455856414CULL);  /* "INDEXVAL" */

    uint64_t target_accounts = target_storage / 15;
    if (target_accounts < 1000) target_accounts = 1000;

    printf("\n============================================\n");
    printf("  Index Simulation (4B values = slot refs)\n");
    printf("============================================\n");
    printf("  accounts: 32B keys, target ~%" PRIu64 "\n", target_accounts);
    printf("  storage:  64B keys, target ~%" PRIu64 "\n", target_storage);
    printf("  ops/block: ~200 account + ~3000 storage\n");
    printf("  mix:       70%% ins / 20%% upd / 10%% del\n");
    printf("============================================\n");

    /* --- nibble_trie --- */
    {
        rng_t r = rng;  /* copy seed so both runs see same ops */
        nibble_trie_t acct, stor;
        if (!nt_init(&acct, 32, 4)) { printf("  FAIL nt acct\n"); return; }
        if (!nt_init(&stor, 64, 4)) { printf("  FAIL nt stor\n"); return; }

        uint8_t key64[64];
        uint32_t slot_val = 0;
        uint64_t acct_next = 0, stor_next = 0, block = 0;

        uint64_t report = target_storage / 5;
        if (report == 0) report = 1;
        uint64_t next_report = report;

        printf("\n  --- nibble_trie (4B val) ---\n");
        printf("  %10s  %10s  %10s  %8s\n",
               "acct keys", "stor keys", "total", "RSS MB");

        clock_gettime(CLOCK_MONOTONIC, &t0);

        while (nt_size(&stor) < target_storage) {
            block++;
            int acct_ops = 150 + (int)(rng_next(&r) % 100);
            for (int i = 0; i < acct_ops; i++) {
                int op = (int)(rng_next(&r) % 100);
                if (op < 70 || nt_size(&acct) < 100) {
                    generate_key(key64, 32, SEED, acct_next++);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    nt_insert(&acct, key64, &slot_val);
                } else if (op < 90) {
                    uint64_t idx = rng_next(&r) % acct_next;
                    generate_key(key64, 32, SEED, idx);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    nt_insert(&acct, key64, &slot_val);
                } else {
                    uint64_t idx = rng_next(&r) % acct_next;
                    generate_key(key64, 32, SEED, idx);
                    nt_delete(&acct, key64);
                }
            }
            int stor_ops = 2500 + (int)(rng_next(&r) % 1000);
            for (int i = 0; i < stor_ops; i++) {
                int op = (int)(rng_next(&r) % 100);
                if (op < 70 || nt_size(&stor) < 1000) {
                    generate_key(key64, 64, SEED, stor_next++);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    nt_insert(&stor, key64, &slot_val);
                } else if (op < 90) {
                    uint64_t idx = rng_next(&r) % stor_next;
                    generate_key(key64, 64, SEED, idx);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    nt_insert(&stor, key64, &slot_val);
                } else {
                    uint64_t idx = rng_next(&r) % stor_next;
                    generate_key(key64, 64, SEED, idx);
                    nt_delete(&stor, key64);
                }
            }
            uint64_t total = nt_size(&acct) + nt_size(&stor);
            if (total >= next_report || nt_size(&stor) >= target_storage) {
                printf("  %10" PRIu64 "  %10" PRIu64 "  %10" PRIu64 "  %8zu\n",
                       (uint64_t)nt_size(&acct), (uint64_t)nt_size(&stor),
                       total, get_rss_mb());
                next_report += report;
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);
        uint64_t total = nt_size(&acct) + nt_size(&stor);
        size_t rss = get_rss_mb();
        printf("\n  NT FINAL: %" PRIu64 " keys | %.1fs | RSS %zu MB | %zu B/key\n",
               total, elapsed_ms(&t0, &t1) / 1e3, rss,
               rss * 1024 * 1024 / total);

        nt_destroy(&acct);
        nt_destroy(&stor);
    }

    /* --- compact_art --- */
    {
        rng_t r = rng;  /* same seed */
        compact_art_t acct, stor;
        if (!compact_art_init(&acct, 32, 4)) { printf("  FAIL cart acct\n"); return; }
        if (!compact_art_init(&stor, 64, 4)) { printf("  FAIL cart stor\n"); return; }

        uint8_t key64[64];
        uint32_t slot_val = 0;
        uint64_t acct_next = 0, stor_next = 0, block = 0;

        uint64_t report = target_storage / 5;
        if (report == 0) report = 1;
        uint64_t next_report = report;

        printf("\n  --- compact_art (4B val) ---\n");
        printf("  %10s  %10s  %10s  %8s\n",
               "acct keys", "stor keys", "total", "RSS MB");

        clock_gettime(CLOCK_MONOTONIC, &t0);

        while (compact_art_size(&stor) < target_storage) {
            block++;
            int acct_ops = 150 + (int)(rng_next(&r) % 100);
            for (int i = 0; i < acct_ops; i++) {
                int op = (int)(rng_next(&r) % 100);
                if (op < 70 || compact_art_size(&acct) < 100) {
                    generate_key(key64, 32, SEED, acct_next++);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    compact_art_insert(&acct, key64, &slot_val);
                } else if (op < 90) {
                    uint64_t idx = rng_next(&r) % acct_next;
                    generate_key(key64, 32, SEED, idx);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    compact_art_insert(&acct, key64, &slot_val);
                } else {
                    uint64_t idx = rng_next(&r) % acct_next;
                    generate_key(key64, 32, SEED, idx);
                    compact_art_delete(&acct, key64);
                }
            }
            int stor_ops = 2500 + (int)(rng_next(&r) % 1000);
            for (int i = 0; i < stor_ops; i++) {
                int op = (int)(rng_next(&r) % 100);
                if (op < 70 || compact_art_size(&stor) < 1000) {
                    generate_key(key64, 64, SEED, stor_next++);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    compact_art_insert(&stor, key64, &slot_val);
                } else if (op < 90) {
                    uint64_t idx = rng_next(&r) % stor_next;
                    generate_key(key64, 64, SEED, idx);
                    slot_val = (uint32_t)(rng_next(&r) & 0x7FFFFFFF);
                    compact_art_insert(&stor, key64, &slot_val);
                } else {
                    uint64_t idx = rng_next(&r) % stor_next;
                    generate_key(key64, 64, SEED, idx);
                    compact_art_delete(&stor, key64);
                }
            }
            uint64_t total = compact_art_size(&acct) + compact_art_size(&stor);
            if (total >= next_report || compact_art_size(&stor) >= target_storage) {
                printf("  %10" PRIu64 "  %10" PRIu64 "  %10" PRIu64 "  %8zu\n",
                       (uint64_t)compact_art_size(&acct),
                       (uint64_t)compact_art_size(&stor),
                       total, get_rss_mb());
                next_report += report;
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);
        uint64_t total = compact_art_size(&acct) + compact_art_size(&stor);
        size_t rss = get_rss_mb();
        printf("\n  CART FINAL: %" PRIu64 " keys | %.1fs | RSS %zu MB | %zu B/key\n",
               total, elapsed_ms(&t0, &t1) / 1e3, rss,
               rss * 1024 * 1024 / total);

        compact_art_destroy(&acct);
        compact_art_destroy(&stor);
    }
}

/* ========================================================================
 * Realistic mixed workload: compact_art — accounts (32B) + storage (64B)
 * ======================================================================== */

static void bench_real_cart(uint64_t target_storage) {
    struct timespec t0, t1;
    rng_t rng = rng_create(0x5245414C574F524BULL);  /* same seed as bench_real */

    uint64_t target_accounts = target_storage / 15;
    if (target_accounts < 1000) target_accounts = 1000;

    printf("\n=== Realistic Mixed Workload (compact_art) ===\n");
    printf("  account trie:  32B keys, target ~%" PRIu64 " keys\n", target_accounts);
    printf("  storage trie:  64B keys, target ~%" PRIu64 " keys\n", target_storage);
    printf("  ops/block:     ~200 account + ~3000 storage\n");
    printf("  mix:           70%% insert / 20%% update / 10%% delete\n\n");

    compact_art_t acct, stor;
    if (!compact_art_init(&acct, 32, 32)) { printf("  FAILED acct init\n"); return; }
    if (!compact_art_init(&stor, 64, 32)) { printf("  FAILED stor init\n"); return; }

    uint8_t key32[32], key64[64], val[32];
    uint64_t acct_next = 0, stor_next = 0;
    uint64_t block = 0;

    uint64_t report_interval = target_storage / 10;
    if (report_interval == 0) report_interval = 1;
    uint64_t next_report = report_interval;

    printf("  %-8s  %10s  %10s  %10s  %8s  %8s\n",
           "block", "acct keys", "stor keys", "total keys", "ms", "RSS MB");
    printf("  %-8s  %10s  %10s  %10s  %8s  %8s\n",
           "-----", "---------", "---------", "----------", "--", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (compact_art_size(&stor) < target_storage) {
        block++;

        int acct_ops = 150 + (int)(rng_next(&rng) % 100);
        for (int i = 0; i < acct_ops; i++) {
            int r = (int)(rng_next(&rng) % 100);
            if (r < 70 || compact_art_size(&acct) < 100) {
                generate_key(key32, 32, SEED, acct_next++);
                generate_val32(val, SEED, rng_next(&rng));
                compact_art_insert(&acct, key32, val);
            } else if (r < 90) {
                uint64_t idx = rng_next(&rng) % acct_next;
                generate_key(key32, 32, SEED, idx);
                generate_val32(val, SEED, rng_next(&rng));
                compact_art_insert(&acct, key32, val);
            } else {
                uint64_t idx = rng_next(&rng) % acct_next;
                generate_key(key32, 32, SEED, idx);
                compact_art_delete(&acct, key32);
            }
        }

        int stor_ops = 2500 + (int)(rng_next(&rng) % 1000);
        for (int i = 0; i < stor_ops; i++) {
            int r = (int)(rng_next(&rng) % 100);
            if (r < 70 || compact_art_size(&stor) < 1000) {
                generate_key(key64, 64, SEED, stor_next++);
                generate_val32(val, SEED, rng_next(&rng));
                compact_art_insert(&stor, key64, val);
            } else if (r < 90) {
                uint64_t idx = rng_next(&rng) % stor_next;
                generate_key(key64, 64, SEED, idx);
                generate_val32(val, SEED, rng_next(&rng));
                compact_art_insert(&stor, key64, val);
            } else {
                uint64_t idx = rng_next(&rng) % stor_next;
                generate_key(key64, 64, SEED, idx);
                compact_art_delete(&stor, key64);
            }
        }

        uint64_t total = compact_art_size(&acct) + compact_art_size(&stor);
        if (total >= next_report || compact_art_size(&stor) >= target_storage) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            printf("  %8" PRIu64 "  %10" PRIu64 "  %10" PRIu64 "  %10" PRIu64
                   "  %8.0f  %8zu\n",
                   block, (uint64_t)compact_art_size(&acct),
                   (uint64_t)compact_art_size(&stor),
                   total, elapsed_ms(&t0, &t1), get_rss_mb());
            next_report += report_interval;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    uint64_t acct_keys = (uint64_t)compact_art_size(&acct);
    uint64_t stor_keys = (uint64_t)compact_art_size(&stor);
    uint64_t total_keys = acct_keys + stor_keys;
    size_t rss = get_rss_mb();

    printf("\n  --- Final ---\n");
    printf("  blocks:        %" PRIu64 "\n", block);
    printf("  account keys:  %" PRIu64 "\n", acct_keys);
    printf("  storage keys:  %" PRIu64 "\n", stor_keys);
    printf("  total keys:    %" PRIu64 "\n", total_keys);
    printf("  time:          %.1fs\n", elapsed_ms(&t0, &t1) / 1e3);
    printf("  RSS:           %zu MB\n", rss);
    printf("  B/key (total): %zu\n", rss * 1024 * 1024 / total_keys);
    printf("  B/key (stor):  %zu (storage dominates)\n",
           rss * 1024 * 1024 / stor_keys);

    compact_art_destroy(&acct);
    compact_art_destroy(&stor);
}

/* ========================================================================
 * Benchmark: compact_art — accounts (32B keys, 32B values)
 * ======================================================================== */

static void bench_cart(uint64_t target) {
    struct timespec t0, t1;

    printf("\n=== compact_art — Accounts (32B key + 32B val) ===\n");
    printf("  target: %" PRIu64 " keys (in-memory, no persistence)\n\n", target);

    compact_art_t tree;
    if (!compact_art_init(&tree, 32, 32)) {
        printf("  FAILED to init\n");
        return;
    }

    uint8_t key[32], val[32];
    uint64_t milestone = target < 1000000 ? target / 10 : 1000000;
    if (milestone == 0) milestone = 1;

    printf("  %-12s %8s %8s %8s %8s\n",
           "Phase", "Keys", "ms", "Kk/s", "RSS MB");
    printf("  %-12s %8s %8s %8s %8s\n",
           "-----", "----", "--", "----", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, 32, SEED, i);
        generate_val32(val, SEED, i);
        compact_art_insert(&tree, key, val);

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double ms = elapsed_ms(&last, &t1);
            double kks = (double)milestone / ms;
            printf("  insert       %8" PRIu64 " %8.0f %8.0f %8zu\n",
                   i + 1, elapsed_ms(&t0, &t1), kks, get_rss_mb());
            last = t1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double insert_ms = elapsed_ms(&t0, &t1);
    printf("  INSERT TOTAL %8" PRIu64 " %8.0f %8.0f %8zu\n\n",
           target, insert_ms, (double)target / insert_ms, get_rss_mb());

    printf("  COMMIT       %8s %8s %8s %8s (N/A — in-memory)\n",
           "", "", "", "");

    uint64_t lookups = target < 1000000 ? target : 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t found = 0;
    for (uint64_t i = 0; i < lookups; i++) {
        uint64_t idx = (i * 7919) % target;
        generate_key(key, 32, SEED, idx);
        if (compact_art_get(&tree, key) != NULL) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  LOOKUP       %8" PRIu64 " %8.0f %8.0f %8s (found=%" PRIu64 ")\n",
           lookups, elapsed_ms(&t0, &t1), (double)lookups / elapsed_ms(&t0, &t1),
           "", found);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    compact_art_iterator_t *it = compact_art_iterator_create(&tree);
    uint64_t iter_count = 0;
    while (compact_art_iterator_next(it)) iter_count++;
    compact_art_iterator_destroy(it);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  ITERATE      %8" PRIu64 " %8.0f %8.0f\n",
           iter_count, elapsed_ms(&t0, &t1),
           (double)iter_count / elapsed_ms(&t0, &t1));

    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < target; i += 2) {
        generate_key(key, 32, SEED, i);
        if (compact_art_delete(&tree, key)) deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  DELETE half  %8" PRIu64 " %8.0f %8.0f\n",
           deleted, elapsed_ms(&t0, &t1),
           (double)deleted / elapsed_ms(&t0, &t1));

    printf("\n  Memory (RSS):  %zu MB\n", get_rss_mb());
    printf("  Tree size:     %" PRIu64 " keys\n",
           (uint64_t)compact_art_size(&tree));

    compact_art_destroy(&tree);
}

/* ========================================================================
 * Realistic mixed workload: accounts (32B) + storage (64B)
 *
 * Simulates block-by-block execution:
 *   - Each block: ~200 account ops + ~3000 storage ops
 *   - 70% insert, 20% update, 10% delete
 *   - Storage/account ratio ~15:1 (typical mainnet)
 * ======================================================================== */

static void bench_real(uint64_t target_storage) {
    struct timespec t0, t1;
    rng_t rng = rng_create(0x5245414C574F524BULL);  /* "REALWORK" */

    /* Roughly 1 account per 15 storage slots */
    uint64_t target_accounts = target_storage / 15;
    if (target_accounts < 1000) target_accounts = 1000;

    printf("\n=== Realistic Mixed Workload ===\n");
    printf("  account trie:  32B keys, target ~%" PRIu64 " keys\n", target_accounts);
    printf("  storage trie:  64B keys, target ~%" PRIu64 " keys\n", target_storage);
    printf("  ops/block:     ~200 account + ~3000 storage\n");
    printf("  mix:           70%% insert / 20%% update / 10%% delete\n\n");

    nibble_trie_t acct, stor;
    if (!nt_init(&acct, 32, 32)) { printf("  FAILED acct init\n"); return; }
    if (!nt_init(&stor, 64, 32)) { printf("  FAILED stor init\n"); return; }

    uint8_t key32[32], key64[64], val[32];
    uint64_t acct_next = 0, stor_next = 0;  /* next unique ID for inserts */
    uint64_t block = 0;

    uint64_t report_interval = target_storage / 10;
    if (report_interval == 0) report_interval = 1;
    uint64_t next_report = report_interval;

    printf("  %-8s  %10s  %10s  %10s  %8s  %8s\n",
           "block", "acct keys", "stor keys", "total keys", "ms", "RSS MB");
    printf("  %-8s  %10s  %10s  %10s  %8s  %8s\n",
           "-----", "---------", "---------", "----------", "--", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (nt_size(&stor) < target_storage) {
        block++;

        /* ~200 account ops per block */
        int acct_ops = 150 + (int)(rng_next(&rng) % 100);
        for (int i = 0; i < acct_ops; i++) {
            int r = (int)(rng_next(&rng) % 100);
            if (r < 70 || nt_size(&acct) < 100) {
                /* Insert new */
                generate_key(key32, 32, SEED, acct_next++);
                generate_val32(val, SEED, rng_next(&rng));
                nt_insert(&acct, key32, val);
            } else if (r < 90) {
                /* Update existing */
                uint64_t idx = rng_next(&rng) % acct_next;
                generate_key(key32, 32, SEED, idx);
                generate_val32(val, SEED, rng_next(&rng));
                nt_insert(&acct, key32, val);
            } else {
                /* Delete */
                uint64_t idx = rng_next(&rng) % acct_next;
                generate_key(key32, 32, SEED, idx);
                nt_delete(&acct, key32);
            }
        }

        /* ~3000 storage ops per block */
        int stor_ops = 2500 + (int)(rng_next(&rng) % 1000);
        for (int i = 0; i < stor_ops; i++) {
            int r = (int)(rng_next(&rng) % 100);
            if (r < 70 || nt_size(&stor) < 1000) {
                /* Insert new */
                generate_key(key64, 64, SEED, stor_next++);
                generate_val32(val, SEED, rng_next(&rng));
                nt_insert(&stor, key64, val);
            } else if (r < 90) {
                /* Update existing */
                uint64_t idx = rng_next(&rng) % stor_next;
                generate_key(key64, 64, SEED, idx);
                generate_val32(val, SEED, rng_next(&rng));
                nt_insert(&stor, key64, val);
            } else {
                /* Delete */
                uint64_t idx = rng_next(&rng) % stor_next;
                generate_key(key64, 64, SEED, idx);
                nt_delete(&stor, key64);
            }
        }

        uint64_t total = nt_size(&acct) + nt_size(&stor);
        if (total >= next_report || nt_size(&stor) >= target_storage) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            printf("  %8" PRIu64 "  %10" PRIu64 "  %10" PRIu64 "  %10" PRIu64
                   "  %8.0f  %8zu\n",
                   block, (uint64_t)nt_size(&acct), (uint64_t)nt_size(&stor),
                   total, elapsed_ms(&t0, &t1), get_rss_mb());
            next_report += report_interval;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    uint64_t acct_keys = (uint64_t)nt_size(&acct);
    uint64_t stor_keys = (uint64_t)nt_size(&stor);
    uint64_t total_keys = acct_keys + stor_keys;
    size_t rss = get_rss_mb();

    printf("\n  --- Final ---\n");
    printf("  blocks:        %" PRIu64 "\n", block);
    printf("  account keys:  %" PRIu64 "\n", acct_keys);
    printf("  storage keys:  %" PRIu64 "\n", stor_keys);
    printf("  total keys:    %" PRIu64 "\n", total_keys);
    printf("  time:          %.1fs\n", elapsed_ms(&t0, &t1) / 1e3);
    printf("  RSS:           %zu MB\n", rss);
    printf("  B/key (total): %zu\n", rss * 1024 * 1024 / total_keys);
    printf("  B/key (stor):  %zu (storage dominates)\n",
           rss * 1024 * 1024 / stor_keys);

    nt_destroy(&acct);
    nt_destroy(&stor);
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(int argc, char **argv) {
    uint64_t target = 1000000;
    const char *mode = "both";

    if (argc > 1) target = (uint64_t)atol(argv[1]) * 1000000;
    if (argc > 2) mode = argv[2];
    if (target == 0) target = 1000000;

    printf("=== Index Structure Scale Benchmark ===\n");
    printf("  Target: %" PRIu64 " keys (%.1fM)\n", target, (double)target / 1e6);
    printf("  Mode:   %s\n", mode);

    if (strcmp(mode, "nt") == 0 || strcmp(mode, "both") == 0)
        bench_nt(target, 32);
    if (strcmp(mode, "nt64") == 0)
        bench_nt(target, 64);
    if (strcmp(mode, "cart") == 0 || strcmp(mode, "both") == 0)
        bench_cart(target);
    if (strcmp(mode, "rss") == 0) {
        bench_rss(target, 32);
        bench_rss(target, 64);
    }
    if (strcmp(mode, "real") == 0)
        bench_real(target);
    if (strcmp(mode, "real_cart") == 0)
        bench_real_cart(target);
    if (strcmp(mode, "index") == 0)
        bench_index(target);

    printf("\nDone.\n");
    return 0;
}
