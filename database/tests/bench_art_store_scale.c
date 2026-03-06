/*
 * art_store Scale Benchmark
 *
 * Measures throughput (kkeys/s), RSS, disk usage, and bytes/key at scale.
 * Simulates verkle-style key patterns (31-byte stem + 1-byte suffix,
 * ~5 suffixes per stem) to match real Ethereum state distribution.
 *
 * Reports:
 *   - Insertion throughput (kkeys/s, including degradation over time)
 *   - Resident memory (RSS) and per-key RAM cost
 *   - Disk usage
 *   - Lookup throughput after build
 *
 * Usage: ./bench_art_store_scale [target_millions] [record_size]
 *   Default: 10M keys, record_size=32
 *   Examples:
 *     ./bench_art_store_scale 100        # 100M keys, record=32
 *     ./bench_art_store_scale 2000       # 2B keys, record=32 (value_store)
 *     ./bench_art_store_scale 400 96     # 400M keys, record=96 (leaf_store)
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

#define KEY_SIZE         32
#define REPORT_EVERY     1000000   /* every 1M keys */
#define LOOKUP_SAMPLE    1000000   /* 1M random lookups after build */

#define DATA_PATH        "/tmp/bench_art_store_scale.dat"

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
 * Verkle-style key generation
 *
 * Generates keys that mimic Ethereum account patterns:
 *   - stem = first 31 bytes (pseudorandom per account)
 *   - suffix = byte 31 (0-4 for header slots, cycling)
 *   - Average ~5 suffixes per stem
 * ========================================================================= */

static void generate_verkle_key(uint8_t key[KEY_SIZE], uint64_t index) {
    /* Each "account" (stem) gets 5 keys (suffixes 0-4) */
    uint64_t stem_id = index / 5;
    uint8_t suffix = (uint8_t)(index % 5);

    /* Generate stem from stem_id */
    rng_t rng = rng_create(stem_id * 0x517cc1b727220a95ULL);
    uint64_t r0 = rng_next(&rng);
    uint64_t r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng);
    uint64_t r3 = rng_next(&rng);  /* only 7 bytes used */
    memcpy(key,      &r0, 8);
    memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8);
    memcpy(key + 24, &r3, 7);      /* 31-byte stem */
    key[31] = suffix;
}

static void generate_record(uint8_t *record, uint32_t record_size,
                             uint64_t index) {
    rng_t rng = rng_create(index * 0xA2F5C3D7E1B94068ULL);
    for (uint32_t i = 0; i < record_size; i += 8) {
        uint64_t r = rng_next(&rng);
        uint32_t n = record_size - i;
        if (n > 8) n = 8;
        memcpy(record + i, &r, n);
    }
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
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

static void format_size(double bytes, char *buf, size_t bufsz) {
    if (bytes >= 1024.0 * 1024.0 * 1024.0)
        snprintf(buf, bufsz, "%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    else if (bytes >= 1024.0 * 1024.0)
        snprintf(buf, bufsz, "%.1f MB", bytes / (1024.0 * 1024.0));
    else
        snprintf(buf, bufsz, "%.0f KB", bytes / 1024.0);
}

static void format_elapsed(double secs, char *buf, size_t bufsz) {
    int m = (int)(secs / 60);
    int s = (int)(secs - m * 60);
    if (m > 0)
        snprintf(buf, bufsz, "%dm%02ds", m, s);
    else
        snprintf(buf, bufsz, "%.1fs", secs);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char *argv[]) {
    uint64_t target_millions = 10;
    uint32_t record_size = 32;

    if (argc > 1) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) target_millions = 10;
    }
    if (argc > 2) {
        record_size = (uint32_t)atoi(argv[2]);
        if (record_size == 0) record_size = 32;
    }

    uint64_t target = target_millions * 1000000ULL;
    uint32_t slot_size = 1 + KEY_SIZE + record_size;

    printf("\n");
    printf("=== art_store Scale Benchmark ===\n");
    printf("  target:      %" PRIu64 "M keys (%" PRIu64 ")\n",
           target_millions, target);
    printf("  key_size:    %d bytes\n", KEY_SIZE);
    printf("  record_size: %u bytes\n", record_size);
    printf("  slot_size:   %u bytes (on disk per entry)\n", slot_size);
    printf("  key pattern: verkle (31B stem + 1B suffix, 5 per stem)\n");
    printf("  file:        %s\n", DATA_PATH);

    char disk_est[32];
    format_size((double)target * slot_size, disk_est, sizeof(disk_est));
    printf("  est. disk:   %s\n", disk_est);
    printf("\n");

    /* Header */
    printf("  %10s | %10s | %10s | %10s | %8s | %8s\n",
           "keys", "kkeys/s", "RSS", "disk", "B/key", "elapsed");
    printf("  %10s-+-%10s-+-%10s-+-%10s-+-%8s-+-%8s\n",
           "----------", "----------", "----------", "----------",
           "--------", "--------");

    /* Remove stale data file */
    unlink(DATA_PATH);

    /* Baseline RSS before allocation */
    size_t base_rss_kb = get_rss_kb();

    art_store_t *store = art_store_create(DATA_PATH, KEY_SIZE, record_size);
    if (!store) {
        fprintf(stderr, "FAIL: art_store_create failed\n");
        return 1;
    }

    uint8_t key[KEY_SIZE];
    uint8_t *record = malloc(record_size);
    if (!record) {
        fprintf(stderr, "FAIL: record alloc failed\n");
        art_store_destroy(store);
        return 1;
    }

    double t_start = now_sec();
    double t_last = t_start;
    uint64_t last_count = 0;

    for (uint64_t i = 0; i < target; i++) {
        generate_verkle_key(key, i);
        generate_record(record, record_size, i);

        if (!art_store_put(store, key, record)) {
            fprintf(stderr, "\nFAIL: put failed at %" PRIu64 "\n", i);
            free(record);
            art_store_destroy(store);
            unlink(DATA_PATH);
            return 1;
        }

        if ((i + 1) % REPORT_EVERY == 0) {
            double now = now_sec();
            double elapsed = now - t_start;
            double interval = now - t_last;
            uint64_t interval_count = (i + 1) - last_count;

            size_t rss_kb = get_rss_kb();
            size_t disk = get_file_size(DATA_PATH);
            size_t art_rss_kb = rss_kb > base_rss_kb ? rss_kb - base_rss_kb : 0;
            double bytes_per_key = (double)(art_rss_kb * 1024) / (double)(i + 1);

            /* Interval throughput (recent speed, not cumulative average) */
            double interval_kks = (double)interval_count / interval / 1000.0;

            char rss_str[32], disk_str[32], elapsed_str[32];
            format_size((double)rss_kb * 1024, rss_str, sizeof(rss_str));
            format_size((double)disk, disk_str, sizeof(disk_str));
            format_elapsed(elapsed, elapsed_str, sizeof(elapsed_str));

            printf("  %7.0fM    | %10.0f | %10s | %10s | %8.1f | %8s\n",
                   (double)(i + 1) / 1e6,
                   interval_kks,
                   rss_str, disk_str,
                   bytes_per_key,
                   elapsed_str);
            fflush(stdout);

            t_last = now;
            last_count = i + 1;
        }
    }

    /* Final insert stats */
    double t_insert_done = now_sec();
    double insert_elapsed = t_insert_done - t_start;

    size_t final_rss_kb = get_rss_kb();
    size_t final_disk = get_file_size(DATA_PATH);
    size_t final_art_rss_kb = final_rss_kb > base_rss_kb
                                ? final_rss_kb - base_rss_kb : 0;

    printf("\n");
    printf("--- Insert complete ---\n");

    char rss_str[32], disk_str[32], elapsed_str[32];
    format_size((double)final_rss_kb * 1024, rss_str, sizeof(rss_str));
    format_size((double)final_disk, disk_str, sizeof(disk_str));
    format_elapsed(insert_elapsed, elapsed_str, sizeof(elapsed_str));

    printf("  keys:        %" PRIu64 " (%.0fM)\n", target, target / 1e6);
    printf("  throughput:  %.0f kkeys/s (avg)\n",
           (double)target / insert_elapsed / 1000.0);
    printf("  RSS:         %s (%.1f bytes/key)\n",
           rss_str, (double)(final_art_rss_kb * 1024) / (double)target);
    printf("  disk:        %s (%.1f bytes/key)\n",
           disk_str, (double)final_disk / (double)target);
    printf("  time:        %s\n", elapsed_str);

    /* Sync and measure */
    art_store_sync(store);
    size_t synced_disk = get_file_size(DATA_PATH);
    char synced_str[32];
    format_size((double)synced_disk, synced_str, sizeof(synced_str));
    printf("  disk(sync):  %s\n", synced_str);

    /* Lookup benchmark */
    printf("\n--- Lookup benchmark (%" PRIu64 "M random reads) ---\n",
           (uint64_t)LOOKUP_SAMPLE / 1000000);

    rng_t rng = rng_create(0xBEEFCAFE12345678ULL);
    uint8_t got[96];  /* max record size */
    uint64_t found = 0;

    double t_lookup = now_sec();
    for (uint64_t i = 0; i < LOOKUP_SAMPLE; i++) {
        uint64_t idx = rng_next(&rng) % target;
        generate_verkle_key(key, idx);
        if (art_store_get(store, key, got))
            found++;
    }
    double lookup_elapsed = now_sec() - t_lookup;

    printf("  %" PRIu64 "/%" PRIu64 " found | %.0f kkeys/s\n",
           found, (uint64_t)LOOKUP_SAMPLE,
           (double)LOOKUP_SAMPLE / lookup_elapsed / 1000.0);

    /* Mainnet projection */
    printf("\n--- Mainnet projection (2B keys) ---\n");
    double bpk_ram = (double)(final_art_rss_kb * 1024) / (double)target;
    double bpk_disk = (double)final_disk / (double)target;
    double proj_ram = bpk_ram * 2e9;
    double proj_disk = bpk_disk * 2e9;

    char proj_ram_str[32], proj_disk_str[32];
    format_size(proj_ram, proj_ram_str, sizeof(proj_ram_str));
    format_size(proj_disk, proj_disk_str, sizeof(proj_disk_str));

    printf("  value_store (key=32, rec=32):  RAM ~%s  disk ~%s\n",
           proj_ram_str, proj_disk_str);

    /* leaf_store projection: same bytes/key for RAM, different disk */
    double proj_leaf_disk = (1 + 32 + 96) * 400e6;  /* 400M stems */
    char proj_leaf_disk_str[32];
    format_size(proj_leaf_disk, proj_leaf_disk_str, sizeof(proj_leaf_disk_str));
    printf("  leaf_store  (key=32, rec=96):  RAM ~%.1f GB  disk ~%s  (400M stems)\n",
           bpk_ram * 400e6 / (1024.0 * 1024.0 * 1024.0), proj_leaf_disk_str);

    double proj_slot_ram = bpk_ram * 400e6;
    double proj_slot_disk = (1 + 32 + 32) * 400e6;
    char proj_slot_ram_str[32], proj_slot_disk_str[32];
    format_size(proj_slot_ram, proj_slot_ram_str, sizeof(proj_slot_ram_str));
    format_size(proj_slot_disk, proj_slot_disk_str, sizeof(proj_slot_disk_str));
    printf("  slot_store  (key=32, rec=32):  RAM ~%s  disk ~%s  (400M stems)\n",
           proj_slot_ram_str, proj_slot_disk_str);

    printf("  internal    (key=32, rec=32):  RAM ~negligible  disk ~negligible  (~3M nodes)\n");

    double total_ram = bpk_ram * 2e9 + bpk_ram * 400e6 * 2;
    double total_disk = bpk_disk * 2e9 + proj_leaf_disk + proj_slot_disk;
    char total_ram_str[32], total_disk_str[32];
    format_size(total_ram, total_ram_str, sizeof(total_ram_str));
    format_size(total_disk, total_disk_str, sizeof(total_disk_str));
    printf("  ─────────────────────────────────────────────────────\n");
    printf("  TOTAL:                         RAM ~%s  disk ~%s\n",
           total_ram_str, total_disk_str);

    /* Cleanup */
    free(record);
    art_store_destroy(store);
    unlink(DATA_PATH);

    printf("\n");
    return 0;
}
