/*
 * convert_idx — Convert .idx from disk_hash format to disk_table format.
 *
 * Reads all entries from an existing disk_hash .idx file and writes them
 * to a new disk_table .idx file. The record format is preserved as-is.
 *
 * Usage:
 *   convert_idx <old.idx> <new.idx> <key_size> <record_size> [--capacity N]
 *
 * Example:
 *   convert_idx code.idx.old code.idx 32 12
 */

#include "disk_hash.h"
#include "disk_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>

typedef struct {
    disk_table_t *dt;
    uint32_t      key_size;
    uint32_t      record_size;
    uint64_t      count;
} convert_ctx_t;

/* Globals for the callback */
static disk_hash_t *g_src = NULL;
static uint8_t     *g_rec_buf = NULL;

static void foreach_cb(const uint8_t *key, void *user_data) {
    convert_ctx_t *ctx = (convert_ctx_t *)user_data;

    if (disk_hash_get(g_src, key, g_rec_buf)) {
        disk_table_put(ctx->dt, key, g_rec_buf);
        ctx->count++;
    }
}

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr,
            "Usage: %s <old.idx> <new.idx> <key_size> <record_size> [--capacity N]\n",
            argv[0]);
        return 1;
    }

    const char *old_path = argv[1];
    const char *new_path = argv[2];
    uint32_t key_size    = (uint32_t)atoi(argv[3]);
    uint32_t record_size = (uint32_t)atoi(argv[4]);
    uint64_t capacity    = 0;

    for (int i = 5; i < argc; i++) {
        if (strcmp(argv[i], "--capacity") == 0 && i + 1 < argc)
            capacity = strtoull(argv[++i], NULL, 10);
    }

    /* Open old disk_hash */
    g_src = disk_hash_open(old_path);
    if (!g_src) {
        fprintf(stderr, "Failed to open old index: %s\n", old_path);
        return 1;
    }

    uint64_t src_count = disk_hash_count(g_src);
    if (capacity == 0) capacity = src_count;

    printf("Source: %s (%" PRIu64 " entries)\n", old_path, src_count);
    printf("Target: %s (capacity %" PRIu64 ")\n", new_path, capacity);

    /* Create new disk_table */
    disk_table_t *dt = disk_table_create(new_path, key_size, record_size, capacity);
    if (!dt) {
        fprintf(stderr, "Failed to create new index: %s\n", new_path);
        disk_hash_destroy(g_src);
        return 1;
    }

    /* Allocate record buffer */
    g_rec_buf = malloc(record_size);
    if (!g_rec_buf) {
        fprintf(stderr, "OOM\n");
        disk_table_destroy(dt);
        disk_hash_destroy(g_src);
        return 1;
    }

    /* Copy all entries */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    convert_ctx_t ctx = { .dt = dt, .key_size = key_size,
                          .record_size = record_size, .count = 0 };
    disk_hash_foreach_key(g_src, foreach_cb, &ctx);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("Converted: %" PRIu64 " entries in %.1fs\n", ctx.count, elapsed);
    printf("disk_table count: %" PRIu64 "\n", disk_table_count(dt));

    disk_table_sync(dt);
    disk_table_destroy(dt);
    free(g_rec_buf);
    disk_hash_destroy(g_src);

    printf("Done.\n");
    return 0;
}
