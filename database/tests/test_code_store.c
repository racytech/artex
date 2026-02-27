/*
 * Code Store Test
 *
 * Tests code_store (append-only code.dat) integration with data_layer.
 *
 * Five phases:
 *   1. Store + read back: varying sizes (32B, 1KB, 24KB)
 *   2. Dedup: same key twice, code_store count stays the same
 *   3. Mixed state + code: dl_get vs dl_get_code route correctly
 *   4. Merge with code keys: state merge doesn't interfere with code
 *   5. Scale: 100K code entries, random sample read-back
 *
 * Usage: ./test_code_store [scale_thousands]
 *   Default: 100 (100K code entries in phase 5)
 */

#include "../include/data_layer.h"
#include "../include/state_store.h"

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

#define KEY_SIZE       32
#define STATE_VAL_LEN  32   // state value size

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
    uint64_t r0 = rng_next(&rng);
    uint64_t r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng);
    uint64_t r3 = rng_next(&rng);
    memcpy(key,      &r0, 8);
    memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8);
    memcpy(key + 24, &r3, 8);
}

// Generate deterministic bytecode of given length
static void generate_bytecode(uint8_t *buf, uint32_t len,
                               uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0xA3F1C2D4E5B6A7F8ULL));
    for (uint32_t i = 0; i < len; i += 8) {
        uint64_t r = rng_next(&rng);
        uint32_t remain = len - i;
        memcpy(buf + i, &r, remain < 8 ? remain : 8);
    }
}

// ============================================================================
// Utilities
// ============================================================================

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

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// ============================================================================
// Phase 1: Store + Read Back (varying sizes)
// ============================================================================

static bool phase1(data_layer_t *dl, uint64_t seed) {
    printf("\n========================================\n");
    printf("Phase 1: Store + Read Back\n");
    printf("========================================\n");

    uint32_t sizes[] = { 32, 256, 1024, 4096, 12000, 24576 };
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);

    for (int i = 0; i < num_sizes; i++) {
        uint32_t len = sizes[i];
        uint8_t key[KEY_SIZE];
        // Use separate key space for code (seed + 0x1000000)
        generate_key(key, seed + 0x1000000ULL, (uint64_t)i);

        uint8_t *bytecode = malloc(len);
        generate_bytecode(bytecode, len, seed, (uint64_t)i);

        // Store
        if (!dl_put_code(dl, key, bytecode, len)) {
            printf("  FAIL: dl_put_code failed for size %u\n", len);
            free(bytecode);
            return false;
        }

        // Verify length
        uint32_t got_len = dl_code_length(dl, key);
        if (got_len != len) {
            printf("  FAIL: dl_code_length = %u, expected %u\n", got_len, len);
            free(bytecode);
            return false;
        }

        // Read back
        uint8_t *got = malloc(len);
        uint32_t out_len = 0;
        if (!dl_get_code(dl, key, got, &out_len)) {
            printf("  FAIL: dl_get_code failed for size %u\n", len);
            free(bytecode);
            free(got);
            return false;
        }
        if (out_len != len || memcmp(got, bytecode, len) != 0) {
            printf("  FAIL: data mismatch for size %u\n", len);
            free(bytecode);
            free(got);
            return false;
        }

        printf("  %5u bytes: store + read OK\n", len);
        free(bytecode);
        free(got);
    }

    dl_stats_t st = dl_stats(dl);
    printf("\n  code entries: %u\n", st.code_count);
    printf("  code.dat:     %" PRIu64 " bytes\n", st.code_file_size);
    printf("  Phase 1: PASS\n");
    return true;
}

// ============================================================================
// Phase 2: Dedup
// ============================================================================

static bool phase2(data_layer_t *dl, uint64_t seed) {
    printf("\n========================================\n");
    printf("Phase 2: Dedup\n");
    printf("========================================\n");

    dl_stats_t before = dl_stats(dl);

    // Re-insert the same keys from phase 1
    uint32_t sizes[] = { 32, 256, 1024, 4096, 12000, 24576 };
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);

    for (int i = 0; i < num_sizes; i++) {
        uint32_t len = sizes[i];
        uint8_t key[KEY_SIZE];
        generate_key(key, seed + 0x1000000ULL, (uint64_t)i);

        uint8_t *bytecode = malloc(len);
        generate_bytecode(bytecode, len, seed, (uint64_t)i);

        // Should be a no-op (dedup)
        if (!dl_put_code(dl, key, bytecode, len)) {
            printf("  FAIL: dedup dl_put_code failed\n");
            free(bytecode);
            return false;
        }
        free(bytecode);
    }

    dl_stats_t after = dl_stats(dl);
    if (after.code_count != before.code_count) {
        printf("  FAIL: code_count changed from %u to %u (should be unchanged)\n",
               before.code_count, after.code_count);
        return false;
    }
    if (after.code_file_size != before.code_file_size) {
        printf("  FAIL: code_file_size changed (should be unchanged)\n");
        return false;
    }

    printf("  dedup: %d re-inserts, code_count unchanged at %u\n",
           num_sizes, after.code_count);
    printf("  Phase 2: PASS\n");
    return true;
}

// ============================================================================
// Phase 3: Mixed State + Code
// ============================================================================

static bool phase3(data_layer_t *dl, uint64_t seed) {
    printf("\n========================================\n");
    printf("Phase 3: Mixed State + Code\n");
    printf("========================================\n");

    uint64_t num_state = 1000;
    uint64_t num_code = 100;
    uint8_t key[KEY_SIZE];

    // Insert state keys
    double t0 = now_sec();
    for (uint64_t i = 0; i < num_state; i++) {
        generate_key(key, seed, i);
        uint8_t value[STATE_VAL_LEN];
        generate_bytecode(value, STATE_VAL_LEN, seed, i);
        dl_put(dl, key, value, STATE_VAL_LEN);
    }

    // Merge state to disk
    dl_merge(dl);
    double t1 = now_sec();
    printf("  inserted + merged %"PRIu64" state keys in %.3fs\n",
           num_state, t1 - t0);

    // Insert code keys (separate key space)
    for (uint64_t i = 0; i < num_code; i++) {
        generate_key(key, seed + 0x2000000ULL, i);
        uint32_t len = 256 + (uint32_t)(i % 1024);  // varying sizes
        uint8_t *bytecode = malloc(len);
        generate_bytecode(bytecode, len, seed + 0x2000000ULL, i);
        dl_put_code(dl, key, bytecode, len);
        free(bytecode);
    }
    printf("  inserted %" PRIu64 " code entries\n", num_code);

    // Verify state keys read via dl_get (not dl_get_code)
    uint64_t state_errors = 0;
    for (uint64_t i = 0; i < num_state; i++) {
        generate_key(key, seed, i);
        uint8_t got[STATE_VAL_LEN];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            state_errors++;
        } else {
            uint8_t expected[STATE_VAL_LEN];
            generate_bytecode(expected, STATE_VAL_LEN, seed, i);
            if (got_len != STATE_VAL_LEN || memcmp(got, expected, STATE_VAL_LEN) != 0)
                state_errors++;
        }
    }
    if (state_errors > 0) {
        printf("  FAIL: %" PRIu64 " state read errors\n", state_errors);
        return false;
    }

    // Verify code keys read via dl_get_code (not dl_get)
    uint64_t code_errors = 0;
    for (uint64_t i = 0; i < num_code; i++) {
        generate_key(key, seed + 0x2000000ULL, i);
        uint32_t len = 256 + (uint32_t)(i % 1024);
        uint8_t *expected = malloc(len);
        generate_bytecode(expected, len, seed + 0x2000000ULL, i);

        uint8_t *got = malloc(len);
        uint32_t got_len = 0;
        if (!dl_get_code(dl, key, got, &got_len)) {
            code_errors++;
        } else if (got_len != len || memcmp(got, expected, len) != 0) {
            code_errors++;
        }
        free(expected);
        free(got);
    }
    if (code_errors > 0) {
        printf("  FAIL: %" PRIu64 " code read errors\n", code_errors);
        return false;
    }

    // Verify dl_get returns false for code keys
    generate_key(key, seed + 0x2000000ULL, 0);
    uint8_t tmp[STATE_VAL_LEN];
    uint16_t tmp_len = 0;
    if (dl_get(dl, key, tmp, &tmp_len)) {
        printf("  FAIL: dl_get should return false for code key\n");
        return false;
    }

    // Verify dl_get_code returns false for state keys
    generate_key(key, seed, 0);
    uint8_t code_tmp[256];
    uint32_t code_tmp_len = 0;
    if (dl_get_code(dl, key, code_tmp, &code_tmp_len)) {
        printf("  FAIL: dl_get_code should return false for state key\n");
        return false;
    }

    dl_stats_t st = dl_stats(dl);
    printf("\n  state: %" PRIu64 " reads OK\n", num_state);
    printf("  code:  %" PRIu64 " reads OK\n", num_code);
    printf("  cross-check: dl_get(code_key)=false, dl_get_code(state_key)=false\n");
    printf("  index keys: %" PRIu64 " (state + code)\n", st.index_keys);
    printf("  Phase 3: PASS\n");
    return true;
}

// ============================================================================
// Phase 4: Merge with Code Keys
// ============================================================================

static bool phase4(data_layer_t *dl, uint64_t seed) {
    printf("\n========================================\n");
    printf("Phase 4: Merge + Code Coexistence\n");
    printf("========================================\n");

    dl_stats_t before = dl_stats(dl);

    // Buffer more state writes + merge
    uint64_t num_state = 5000;
    uint8_t key[KEY_SIZE];

    for (uint64_t i = 0; i < num_state; i++) {
        generate_key(key, seed + 0x3000000ULL, i);
        uint8_t value[STATE_VAL_LEN];
        generate_bytecode(value, STATE_VAL_LEN, seed + 0x3000000ULL, i);
        dl_put(dl, key, value, STATE_VAL_LEN);
    }

    double t0 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t1 = now_sec();

    dl_stats_t after = dl_stats(dl);
    printf("  merged %" PRIu64 " state entries in %.3fs\n", merged, t1 - t0);
    printf("  index keys: %" PRIu64 " (was %" PRIu64 ")\n",
           after.index_keys, before.index_keys);
    printf("  code_count unchanged: %u\n", after.code_count);

    // Verify code entries from phase 3 are still readable
    uint64_t code_errors = 0;
    for (uint64_t i = 0; i < 100; i++) {
        generate_key(key, seed + 0x2000000ULL, i);
        uint32_t len = 256 + (uint32_t)(i % 1024);
        uint8_t *expected = malloc(len);
        generate_bytecode(expected, len, seed + 0x2000000ULL, i);

        uint8_t *got = malloc(len);
        uint32_t got_len = 0;
        if (!dl_get_code(dl, key, got, &got_len) ||
            got_len != len || memcmp(got, expected, len) != 0) {
            code_errors++;
        }
        free(expected);
        free(got);
    }
    if (code_errors > 0) {
        printf("  FAIL: %" PRIu64 " code entries corrupted after merge\n", code_errors);
        return false;
    }

    printf("  code entries intact after state merge\n");
    printf("  Phase 4: PASS\n");
    return true;
}

// ============================================================================
// Phase 5: Scale
// ============================================================================

static bool phase5(data_layer_t *dl, uint64_t seed, uint64_t num_codes) {
    printf("\n========================================\n");
    printf("Phase 5: Scale (%" PRIu64 "K code entries)\n", num_codes / 1000);
    printf("========================================\n");

    double t0 = now_sec();
    uint64_t total_bytes = 0;

    for (uint64_t i = 0; i < num_codes; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, seed + 0x5000000ULL, i);

        // Vary bytecode size: 100 to ~3000 bytes (avg ~1500)
        uint32_t len = 100 + (uint32_t)(rng_next(&(rng_t){ .state = seed ^ i }) % 2900);
        uint8_t *bytecode = malloc(len);
        generate_bytecode(bytecode, len, seed + 0x5000000ULL, i);

        if (!dl_put_code(dl, key, bytecode, len)) {
            printf("  FAIL: dl_put_code at index %" PRIu64 "\n", i);
            free(bytecode);
            return false;
        }
        total_bytes += len;
        free(bytecode);

        if ((i + 1) % (num_codes / 5) == 0) {
            dl_stats_t st = dl_stats(dl);
            printf("  %6" PRIu64 "K entries | code.dat %4" PRIu64 " MB | RSS %zu MB\n",
                   (i + 1) / 1000, st.code_file_size / (1024 * 1024),
                   get_rss_mb());
        }
    }
    double t1 = now_sec();

    // Verify random sample
    printf("\n  verifying...");
    fflush(stdout);
    uint64_t check = num_codes < 10000 ? num_codes : 10000;
    uint64_t step = num_codes / check;
    uint64_t errors = 0;

    for (uint64_t i = 0; i < num_codes; i += step) {
        uint8_t key[KEY_SIZE];
        generate_key(key, seed + 0x5000000ULL, i);

        uint32_t len = 100 + (uint32_t)(rng_next(&(rng_t){ .state = seed ^ i }) % 2900);
        uint8_t *expected = malloc(len);
        generate_bytecode(expected, len, seed + 0x5000000ULL, i);

        uint8_t *got = malloc(len);
        uint32_t got_len = 0;
        if (!dl_get_code(dl, key, got, &got_len) ||
            got_len != len || memcmp(got, expected, len) != 0) {
            errors++;
        }
        free(expected);
        free(got);
    }
    printf(" %" PRIu64 " sampled, %" PRIu64 " errors\n", check, errors);

    if (errors > 0) {
        printf("  FAIL\n");
        return false;
    }

    dl_stats_t st = dl_stats(dl);
    printf("\n  ============================================\n");
    printf("  Phase 5 Summary\n");
    printf("  ============================================\n");
    printf("  code entries:    %u\n", st.code_count);
    printf("  code.dat:        %" PRIu64 " MB\n", st.code_file_size / (1024 * 1024));
    printf("  total bytecode:  %" PRIu64 " MB\n", total_bytes / (1024 * 1024));
    printf("  insert time:     %.2fs (%.1f Kk/s)\n",
           t1 - t0, num_codes / (t1 - t0) / 1000.0);
    printf("  index keys:      %" PRIu64 "\n", st.index_keys);
    printf("  RSS:             %zu MB\n", get_rss_mb());
    printf("  ============================================\n");
    printf("  Phase 5: PASS\n");
    return true;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t scale_thousands = 100;
    if (argc >= 2) {
        scale_thousands = (uint64_t)atoll(argv[1]);
        if (scale_thousands == 0) {
            fprintf(stderr, "Usage: %s [scale_thousands]\n", argv[0]);
            return 1;
        }
    }

    uint64_t seed = 0xC0DE5700EC0DE500ULL;
    const char *state_path = "/tmp/art_cs_test_state.dat";
    const char *code_path = "/tmp/art_cs_test_code.dat";

    printf("============================================\n");
    printf("Code Store Test\n");
    printf("============================================\n");
    printf("scale:  %" PRIu64 "K code entries (phase 5)\n", scale_thousands);
    printf("RSS:    %zu MB\n", get_rss_mb());

    data_layer_t *dl = dl_create(state_path, code_path, KEY_SIZE, 4);
    if (!dl) {
        fprintf(stderr, "FAIL: dl_create\n");
        return 1;
    }

    double t_start = now_sec();
    int result = 0;

    if (!phase1(dl, seed))                                   { result = 1; goto cleanup; }
    if (!phase2(dl, seed))                                   { result = 1; goto cleanup; }
    if (!phase3(dl, seed))                                   { result = 1; goto cleanup; }
    if (!phase4(dl, seed))                                   { result = 1; goto cleanup; }
    if (!phase5(dl, seed, scale_thousands * 1000))           { result = 1; goto cleanup; }

    double t_end = now_sec();
    dl_stats_t st = dl_stats(dl);

    printf("\n============================================\n");
    printf("ALL PHASES PASSED\n");
    printf("============================================\n");
    printf("total time:   %.1fs\n", t_end - t_start);
    printf("index keys:   %" PRIu64 "\n", st.index_keys);
    printf("code entries: %u\n", st.code_count);
    printf("code.dat:     %" PRIu64 " MB\n", st.code_file_size / (1024 * 1024));
    printf("final RSS:    %zu MB\n", get_rss_mb());
    printf("============================================\n");

cleanup:
    dl_destroy(dl);
    unlink(state_path);
    unlink(code_path);
    return result;
}
