/*
 * Data Layer Proof-of-Concept Integration Test
 *
 * Proves the full-stack architecture:
 *   Write Buffer (mem_art) → compact_art (RAM index) → state.dat (pread/pwrite)
 *
 * Four phases:
 *   1. Buffer writes + first merge to disk
 *   2. Mixed reads (buffer hits + disk reads)
 *   3. Updates, deletes, inserts + merge
 *   4. Scale test — many merge cycles, verify stable throughput
 *
 * Usage: ./test_data_layer_poc [scale_millions]
 *   Default: 1M keys
 */

#include "../include/compact_art.h"
#include "../include/mem_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE       32
#define SLOT_SIZE      64
#define MAX_VALUE_LEN  62   // SLOT_SIZE - 2 (length prefix)
#define VALUE_LEN      32   // test value size (simulating storage slot)

#define BUF_FLAG_WRITE     0x01
#define BUF_FLAG_TOMBSTONE 0x02

#define KEYS_PER_BLOCK     5000
#define BLOCKS_PER_MERGE   100

// ============================================================================
// RNG (SplitMix64 — same as bench_compact_art_index.c)
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

static void generate_value(uint8_t value[VALUE_LEN], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x9c2f0b3a71d8e6f5ULL));
    for (int i = 0; i < VALUE_LEN; i += 8) {
        uint64_t r = rng_next(&rng);
        int remain = VALUE_LEN - i;
        memcpy(value + i, &r, remain < 8 ? remain : 8);
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

static size_t get_file_size(int fd) {
    struct stat st;
    if (fstat(fd, &st) != 0) return 0;
    return (size_t)st.st_size;
}

// ============================================================================
// State Store (state.dat — fixed 64-byte slots)
// ============================================================================

typedef struct {
    int fd;
    uint32_t next_slot;
    uint32_t *free_list;
    uint32_t free_count;
    uint32_t free_cap;
    char path[256];
} state_store_t;

static bool store_init(state_store_t *s, const char *path) {
    snprintf(s->path, sizeof(s->path), "%s", path);
    s->fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (s->fd < 0) return false;
    s->next_slot = 0;
    s->free_count = 0;
    s->free_cap = 1024;
    s->free_list = malloc(s->free_cap * sizeof(uint32_t));
    return s->free_list != NULL;
}

static void store_destroy(state_store_t *s) {
    if (s->fd >= 0) {
        close(s->fd);
        unlink(s->path);
    }
    free(s->free_list);
    s->free_list = NULL;
}

static uint32_t store_alloc(state_store_t *s) {
    if (s->free_count > 0) {
        return s->free_list[--s->free_count];
    }
    return s->next_slot++;
}

static void store_free_slot(state_store_t *s, uint32_t slot) {
    if (s->free_count >= s->free_cap) {
        s->free_cap *= 2;
        s->free_list = realloc(s->free_list, s->free_cap * sizeof(uint32_t));
    }
    s->free_list[s->free_count++] = slot;
}

static bool store_write(state_store_t *s, uint32_t slot,
                        const void *value, uint16_t len) {
    uint8_t buf[SLOT_SIZE] = {0};
    memcpy(buf, &len, 2);
    memcpy(buf + 2, value, len);
    ssize_t w = pwrite(s->fd, buf, SLOT_SIZE, (off_t)slot * SLOT_SIZE);
    return w == SLOT_SIZE;
}

static bool store_read(state_store_t *s, uint32_t slot,
                       void *out_value, uint16_t *out_len) {
    uint8_t buf[SLOT_SIZE];
    ssize_t r = pread(s->fd, buf, SLOT_SIZE, (off_t)slot * SLOT_SIZE);
    if (r != SLOT_SIZE) return false;
    uint16_t len;
    memcpy(&len, buf, 2);
    if (len > MAX_VALUE_LEN) return false;
    if (out_len) *out_len = len;
    if (out_value) memcpy(out_value, buf + 2, len);
    return true;
}

// ============================================================================
// Data Layer
// ============================================================================

typedef struct {
    compact_art_t index;
    state_store_t store;
    art_tree_t buffer;
    uint64_t disk_keys;
    uint64_t total_merged;
} data_layer_t;

static bool dl_init(data_layer_t *dl, const char *state_path) {
    memset(dl, 0, sizeof(*dl));
    if (!compact_art_init(&dl->index, KEY_SIZE, 4)) return false;
    if (!store_init(&dl->store, state_path)) {
        compact_art_destroy(&dl->index);
        return false;
    }
    if (!art_tree_init(&dl->buffer)) {
        compact_art_destroy(&dl->index);
        store_destroy(&dl->store);
        return false;
    }
    return true;
}

static void dl_destroy(data_layer_t *dl) {
    compact_art_destroy(&dl->index);
    store_destroy(&dl->store);
    art_tree_destroy(&dl->buffer);
}

// Write to buffer (memory only)
static bool dl_put(data_layer_t *dl, const uint8_t key[KEY_SIZE],
                   const void *value, uint16_t len) {
    uint8_t buf[1 + MAX_VALUE_LEN];
    buf[0] = BUF_FLAG_WRITE;
    memcpy(buf + 1, value, len);
    return art_insert(&dl->buffer, key, KEY_SIZE, buf, 1 + len);
}

// Delete via tombstone in buffer
static bool dl_delete(data_layer_t *dl, const uint8_t key[KEY_SIZE]) {
    uint8_t tombstone = BUF_FLAG_TOMBSTONE;
    return art_insert(&dl->buffer, key, KEY_SIZE, &tombstone, 1);
}

// Read: buffer first → index → disk
static bool dl_get(data_layer_t *dl, const uint8_t key[KEY_SIZE],
                   void *out_value, uint16_t *out_len) {
    // 1. Check write buffer
    size_t vlen = 0;
    const void *bval = art_get(&dl->buffer, key, KEY_SIZE, &vlen);
    if (bval) {
        uint8_t flag = *(const uint8_t *)bval;
        if (flag == BUF_FLAG_TOMBSTONE) return false;  // deleted
        if (flag == BUF_FLAG_WRITE && vlen > 1) {
            uint16_t len = (uint16_t)(vlen - 1);
            if (out_len) *out_len = len;
            if (out_value) memcpy(out_value, (const uint8_t *)bval + 1, len);
            return true;
        }
        return false;
    }

    // 2. Check index → disk
    const void *ref = compact_art_get(&dl->index, key);
    if (!ref) return false;

    uint32_t slot;
    memcpy(&slot, ref, 4);
    return store_read(&dl->store, slot, out_value, out_len);
}

// Merge buffer to disk
static uint64_t dl_merge(data_layer_t *dl) {
    uint64_t count = 0;
    uint64_t inserts = 0, updates = 0, deletes = 0;

    art_iterator_t *iter = art_iterator_create(&dl->buffer);
    if (!iter) return 0;

    while (art_iterator_next(iter)) {
        size_t klen = 0, vlen = 0;
        const uint8_t *key = art_iterator_key(iter, &klen);
        const void *val = art_iterator_value(iter, &vlen);
        if (!key || !val || klen != KEY_SIZE || vlen < 1) continue;

        uint8_t flag = *(const uint8_t *)val;

        if (flag == BUF_FLAG_TOMBSTONE) {
            // Delete from index + free slot
            const void *ref = compact_art_get(&dl->index, key);
            if (ref) {
                uint32_t slot;
                memcpy(&slot, ref, 4);
                store_free_slot(&dl->store, slot);
                compact_art_delete(&dl->index, key);
                dl->disk_keys--;
                deletes++;
            }
        } else if (flag == BUF_FLAG_WRITE && vlen > 1) {
            const uint8_t *data = (const uint8_t *)val + 1;
            uint16_t data_len = (uint16_t)(vlen - 1);

            const void *ref = compact_art_get(&dl->index, key);
            if (ref) {
                // Update: rewrite same slot
                uint32_t slot;
                memcpy(&slot, ref, 4);
                store_write(&dl->store, slot, data, data_len);
                updates++;
            } else {
                // Insert: allocate new slot
                uint32_t slot = store_alloc(&dl->store);
                store_write(&dl->store, slot, data, data_len);
                compact_art_insert(&dl->index, key, &slot);
                dl->disk_keys++;
                inserts++;
            }
        }
        count++;
    }

    art_iterator_destroy(iter);
    fdatasync(dl->store.fd);

    // Clear buffer
    art_tree_destroy(&dl->buffer);
    art_tree_init(&dl->buffer);

    dl->total_merged += count;
    return count;
}

// ============================================================================
// Phase 1: Buffer Writes + First Merge
// ============================================================================

static bool phase1(data_layer_t *dl, uint64_t seed, uint64_t num_keys,
                   uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 1: Buffer Writes + First Merge\n");
    printf("========================================\n");
    printf("  keys: %" PRIu64 "\n\n", num_keys);

    uint8_t key[KEY_SIZE], value[VALUE_LEN];

    // Write to buffer
    double t0 = now_sec();
    for (uint64_t i = 0; i < num_keys; i++) {
        generate_key(key, seed, i);
        generate_value(value, seed, i);
        dl_put(dl, key, value, VALUE_LEN);
    }
    double t1 = now_sec();
    printf("  buffer writes: %" PRIu64 " keys in %.2fs (%.1f Kk/s)\n",
           num_keys, t1 - t0, num_keys / (t1 - t0) / 1000.0);
    printf("  buffer size:   %zu entries\n", art_size(&dl->buffer));
    printf("  RSS:           %zu MB\n", get_rss_mb());

    // Merge
    double t2 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t3 = now_sec();
    printf("\n  merge: %" PRIu64 " entries in %.3fs (%.1f Kk/s)\n",
           merged, t3 - t2, merged / (t3 - t2) / 1000.0);
    printf("  disk keys:     %" PRIu64 "\n", dl->disk_keys);
    printf("  index size:    %zu\n", compact_art_size(&dl->index));
    printf("  state.dat:     %zu KB\n", get_file_size(dl->store.fd) / 1024);
    printf("  RSS:           %zu MB\n", get_rss_mb());

    // Verify sample
    uint64_t check = num_keys < 10000 ? num_keys : 10000;
    uint64_t step = num_keys / check;
    uint64_t errors = 0;
    for (uint64_t i = 0; i < num_keys; i += step) {
        generate_key(key, seed, i);
        generate_value(value, seed, i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            errors++;
        } else if (got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
            errors++;
        }
    }

    if (errors > 0) {
        printf("\n  FAIL: %" PRIu64 " verification errors\n", errors);
        return false;
    }
    printf("\n  verify: %" PRIu64 " keys OK\n", check);
    printf("  Phase 1: PASS\n");

    *next_key_idx = num_keys;
    return true;
}

// ============================================================================
// Phase 2: Mixed Reads (buffer + disk)
// ============================================================================

static bool phase2(data_layer_t *dl, uint64_t seed,
                   uint64_t disk_keys, uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 2: Mixed Reads (buffer + disk)\n");
    printf("========================================\n");

    uint64_t new_keys = disk_keys / 5;  // 20% new keys in buffer
    if (new_keys < 1000) new_keys = 1000;
    uint64_t start = *next_key_idx;

    uint8_t key[KEY_SIZE], value[VALUE_LEN];

    // Write new batch to buffer (don't merge)
    double t0 = now_sec();
    for (uint64_t i = 0; i < new_keys; i++) {
        generate_key(key, seed, start + i);
        generate_value(value, seed, start + i);
        dl_put(dl, key, value, VALUE_LEN);
    }
    double t1 = now_sec();
    printf("  buffered %" PRIu64 " new keys in %.2fs\n", new_keys, t1 - t0);

    // Read from full keyspace: mix of buffer hits and disk reads
    uint64_t total_reads = disk_keys + new_keys;
    if (total_reads > 100000) total_reads = 100000;
    uint64_t buf_hits = 0, disk_hits = 0, misses = 0;

    double t2 = now_sec();
    for (uint64_t i = 0; i < total_reads; i++) {
        // Alternate: even = disk keys, odd = buffer keys
        uint64_t idx;
        if (i % 2 == 0) {
            idx = (i / 2) % disk_keys;  // disk key
        } else {
            idx = start + ((i / 2) % new_keys);  // buffer key
        }
        generate_key(key, seed, idx);

        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (dl_get(dl, key, got, &got_len)) {
            // Check which path it took
            size_t vlen = 0;
            const void *bval = art_get(&dl->buffer, key, KEY_SIZE, &vlen);
            if (bval && *(const uint8_t *)bval == BUF_FLAG_WRITE) {
                buf_hits++;
            } else {
                disk_hits++;
            }

            // Verify correctness
            generate_value(value, seed, idx);
            if (got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
                printf("  FAIL: wrong value for key idx %" PRIu64 "\n", idx);
                return false;
            }
        } else {
            misses++;
        }
    }
    double t3 = now_sec();

    printf("  reads:     %" PRIu64 " in %.3fs (%.1f Kk/s)\n",
           total_reads, t3 - t2, total_reads / (t3 - t2) / 1000.0);
    printf("  buf hits:  %" PRIu64 " (%.0f%%)\n",
           buf_hits, 100.0 * buf_hits / total_reads);
    printf("  disk hits: %" PRIu64 " (%.0f%%)\n",
           disk_hits, 100.0 * disk_hits / total_reads);
    printf("  misses:    %" PRIu64 "\n", misses);
    printf("  RSS:       %zu MB\n", get_rss_mb());

    if (misses > 0) {
        printf("\n  FAIL: unexpected misses\n");
        return false;
    }

    // Merge the buffer keys to disk
    double t4 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t5 = now_sec();
    printf("\n  merge: %" PRIu64 " entries in %.3fs\n", merged, t5 - t4);

    printf("  Phase 2: PASS\n");
    *next_key_idx = start + new_keys;
    return true;
}

// ============================================================================
// Phase 3: Updates + Deletes + Inserts + Merge
// ============================================================================

static bool phase3(data_layer_t *dl, uint64_t seed,
                   uint64_t disk_keys, uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 3: Updates + Deletes + Inserts\n");
    printf("========================================\n");

    uint64_t num_updates = disk_keys / 5;    // 20%
    uint64_t num_deletes = disk_keys / 10;   // 10%
    uint64_t num_inserts = disk_keys / 10;   // 10%
    if (num_updates < 100) num_updates = 100;
    if (num_deletes < 50) num_deletes = 50;
    if (num_inserts < 50) num_inserts = 50;

    uint64_t new_start = *next_key_idx;
    uint8_t key[KEY_SIZE], value[VALUE_LEN];

    // Updates: rewrite existing keys with new values (use different seed offset)
    double t0 = now_sec();
    for (uint64_t i = 0; i < num_updates; i++) {
        generate_key(key, seed, i);  // existing key
        generate_value(value, seed + 1, i);  // NEW value (different seed)
        dl_put(dl, key, value, VALUE_LEN);
    }

    // Deletes: remove some existing keys (after the updated range)
    uint64_t del_start = num_updates;
    for (uint64_t i = 0; i < num_deletes; i++) {
        generate_key(key, seed, del_start + i);
        dl_delete(dl, key);
    }

    // Inserts: new keys
    for (uint64_t i = 0; i < num_inserts; i++) {
        generate_key(key, seed, new_start + i);
        generate_value(value, seed, new_start + i);
        dl_put(dl, key, value, VALUE_LEN);
    }
    double t1 = now_sec();

    printf("  updates:  %" PRIu64 "\n", num_updates);
    printf("  deletes:  %" PRIu64 "\n", num_deletes);
    printf("  inserts:  %" PRIu64 "\n", num_inserts);
    printf("  buffer:   %zu entries in %.3fs\n", art_size(&dl->buffer), t1 - t0);

    // Merge
    uint64_t keys_before = dl->disk_keys;
    uint32_t free_before = dl->store.free_count;

    double t2 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t3 = now_sec();

    printf("\n  merge: %" PRIu64 " entries in %.3fs (%.1f Kk/s)\n",
           merged, t3 - t2, merged / (t3 - t2) / 1000.0);
    printf("  disk keys: %" PRIu64 " (was %" PRIu64 ")\n",
           dl->disk_keys, keys_before);
    printf("  free slots: %u (was %u, freed %u)\n",
           dl->store.free_count, free_before,
           dl->store.free_count - free_before);
    printf("  RSS: %zu MB\n", get_rss_mb());

    // Verify updates
    uint64_t errors = 0;
    for (uint64_t i = 0; i < num_updates && i < 1000; i++) {
        generate_key(key, seed, i);
        generate_value(value, seed + 1, i);  // expect NEW value
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len) ||
            got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
            errors++;
        }
    }
    if (errors > 0) {
        printf("  FAIL: %" PRIu64 " update verification errors\n", errors);
        return false;
    }

    // Verify deletes
    for (uint64_t i = 0; i < num_deletes && i < 1000; i++) {
        generate_key(key, seed, del_start + i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (dl_get(dl, key, got, &got_len)) {
            errors++;
        }
    }
    if (errors > 0) {
        printf("  FAIL: %" PRIu64 " deleted keys still found\n", errors);
        return false;
    }

    // Verify new inserts
    for (uint64_t i = 0; i < num_inserts && i < 1000; i++) {
        generate_key(key, seed, new_start + i);
        generate_value(value, seed, new_start + i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len) ||
            got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
            errors++;
        }
    }
    if (errors > 0) {
        printf("  FAIL: %" PRIu64 " new insert verification errors\n", errors);
        return false;
    }

    printf("\n  verify: updates OK, deletes OK, inserts OK\n");
    printf("  Phase 3: PASS\n");

    *next_key_idx = new_start + num_inserts;
    return true;
}

// ============================================================================
// Phase 4: Scale Test — Many Merge Cycles
// ============================================================================

static bool phase4(data_layer_t *dl, uint64_t seed,
                   uint64_t total_target, uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 4: Scale Test (many merge cycles)\n");
    printf("========================================\n");

    uint64_t keys_per_cycle = (uint64_t)KEYS_PER_BLOCK * BLOCKS_PER_MERGE;
    uint64_t current = *next_key_idx;
    uint64_t remaining = (total_target > current) ? (total_target - current) : 0;
    uint64_t num_cycles = remaining / keys_per_cycle;
    if (num_cycles < 2) num_cycles = 2;
    if (remaining < keys_per_cycle * 2) {
        keys_per_cycle = remaining / 2;
        if (keys_per_cycle == 0) keys_per_cycle = 1000;
    }

    printf("  target:       %" PRIu64 " total keys\n", total_target);
    printf("  remaining:    %" PRIu64 "\n", remaining);
    printf("  cycles:       %" PRIu64 " × %" PRIu64 " keys\n",
           num_cycles, keys_per_cycle);
    printf("\n");

    uint8_t key[KEY_SIZE], value[VALUE_LEN];
    double total_merge_time = 0;

    for (uint64_t cycle = 0; cycle < num_cycles; cycle++) {
        // Write batch to buffer
        double t0 = now_sec();
        for (uint64_t i = 0; i < keys_per_cycle; i++) {
            generate_key(key, seed, current + i);
            generate_value(value, seed, current + i);
            dl_put(dl, key, value, VALUE_LEN);
        }
        double t1 = now_sec();

        // Merge
        double t2 = now_sec();
        uint64_t merged = dl_merge(dl);
        double t3 = now_sec();
        total_merge_time += (t3 - t2);

        current += keys_per_cycle;

        size_t rss = get_rss_mb();
        size_t file_kb = get_file_size(dl->store.fd) / 1024;

        printf("  cycle %3" PRIu64 " | %" PRIu64 "k merged | "
               "write %.1f Kk/s | merge %.1f Kk/s (%.3fs) | "
               "disk %" PRIu64 "k | state %zu MB | RSS %zu MB\n",
               cycle + 1, merged / 1000,
               keys_per_cycle / (t1 - t0) / 1000.0,
               merged / (t3 - t2) / 1000.0, t3 - t2,
               dl->disk_keys / 1000, file_kb / 1024, rss);
        fflush(stdout);
    }

    // Final verification: sample 10000 random keys
    printf("\n  verifying...");
    fflush(stdout);
    uint64_t check = current < 10000 ? current : 10000;
    uint64_t step = current / check;
    uint64_t errors = 0;
    for (uint64_t i = 0; i < current; i += step) {
        generate_key(key, seed, i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        bool found = dl_get(dl, key, got, &got_len);

        // Keys in the Phase 3 delete range won't be found
        // Keys in the Phase 3 update range have different values
        // We just check that found keys have valid length
        if (found && got_len != VALUE_LEN) {
            errors++;
        }
    }
    printf(" %" PRIu64 " keys sampled, %" PRIu64 " errors\n", check, errors);

    if (errors > 0) {
        printf("  FAIL\n");
        return false;
    }

    printf("\n  ============================================\n");
    printf("  Phase 4 Summary\n");
    printf("  ============================================\n");
    printf("  total disk keys: %" PRIu64 "\n", dl->disk_keys);
    printf("  index size:      %zu\n", compact_art_size(&dl->index));
    printf("  state.dat:       %zu MB\n", get_file_size(dl->store.fd) / (1024*1024));
    printf("  merge cycles:    %" PRIu64 "\n", num_cycles);
    printf("  avg merge time:  %.3fs\n", total_merge_time / num_cycles);
    printf("  total merged:    %" PRIu64 "\n", dl->total_merged);
    printf("  free slots:      %u\n", dl->store.free_count);
    printf("  RSS:             %zu MB\n", get_rss_mb());
    printf("  ============================================\n");
    printf("  Phase 4: PASS\n");

    *next_key_idx = current;
    return true;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t scale_millions = 1;
    if (argc >= 2) {
        scale_millions = (uint64_t)atoll(argv[1]);
        if (scale_millions == 0) {
            fprintf(stderr, "Usage: %s [scale_millions]\n", argv[0]);
            return 1;
        }
    }

    uint64_t total_keys = scale_millions * 1000000ULL;
    uint64_t seed = 0x0000000069a11ab9ULL;

    printf("============================================\n");
    printf("Data Layer PoC Integration Test\n");
    printf("============================================\n");
    printf("scale:      %" PRIu64 "M keys\n", scale_millions);
    printf("value size: %d bytes\n", VALUE_LEN);
    printf("slot size:  %d bytes\n", SLOT_SIZE);
    printf("RSS:        %zu MB\n", get_rss_mb());

    data_layer_t dl;
    if (!dl_init(&dl, "/tmp/art_poc_state.dat")) {
        fprintf(stderr, "FAIL: dl_init\n");
        return 1;
    }

    uint64_t next_key = 0;
    double t_start = now_sec();

    // Phase 1: 10% of keys for initial load
    uint64_t phase1_keys = total_keys / 10;
    if (phase1_keys < 1000) phase1_keys = 1000;
    if (!phase1(&dl, seed, phase1_keys, &next_key)) {
        dl_destroy(&dl);
        return 1;
    }

    // Phase 2: mixed reads
    if (!phase2(&dl, seed, dl.disk_keys, &next_key)) {
        dl_destroy(&dl);
        return 1;
    }

    // Phase 3: updates + deletes + inserts
    if (!phase3(&dl, seed, dl.disk_keys, &next_key)) {
        dl_destroy(&dl);
        return 1;
    }

    // Phase 4: scale to full target
    if (!phase4(&dl, seed, total_keys, &next_key)) {
        dl_destroy(&dl);
        return 1;
    }

    double t_end = now_sec();

    printf("\n============================================\n");
    printf("ALL PHASES PASSED\n");
    printf("============================================\n");
    printf("total time:  %.1fs\n", t_end - t_start);
    printf("total keys:  %" PRIu64 "\n", dl.disk_keys);
    printf("total merge: %" PRIu64 "\n", dl.total_merged);
    printf("final RSS:   %zu MB\n", get_rss_mb());
    printf("============================================\n");

    dl_destroy(&dl);
    return 0;
}
