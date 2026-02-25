/**
 * Benchmark: ART Database vs LMDB
 *
 * Fair comparison — both databases execute identical operations:
 * - Bulk insert: single transaction wrapping all keys
 * - Per-key insert: one transaction per key (autocommit)
 * - Point lookup, range scan, negative lookup
 *
 * Keys: 32 bytes, Values: 32 bytes (Ethereum state dimensions)
 */

#include "data_art.h"
#include "logger.h"

#ifdef HAVE_LMDB
#include <lmdb.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define KEY_SIZE 32
#define VALUE_SIZE 32
#define ART_DIR "/tmp/bench_art_mmap"
#define ART_FILE "/tmp/bench_art_mmap/art.dat"
#define LMDB_PATH "/tmp/bench_lmdb"

// ============================================================================
// Utilities
// ============================================================================

static uint64_t get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

static void make_key(uint32_t index, uint8_t key[KEY_SIZE]) {
    uint32_t h = index * 2654435761u;
    key[0] = (h >> 24) & 0xFF;
    key[1] = (h >> 16) & 0xFF;
    key[2] = (h >> 8) & 0xFF;
    key[3] = h & 0xFF;
    h = (index + 1) * 2246822519u;
    key[4] = (h >> 24) & 0xFF;
    key[5] = (h >> 16) & 0xFF;
    key[6] = (h >> 8) & 0xFF;
    key[7] = h & 0xFF;
    for (int i = 8; i < KEY_SIZE; i++) {
        h = h * 1103515245u + 12345;
        key[i] = (h >> 16) & 0xFF;
    }
}

static void make_sequential_key(uint32_t index, uint8_t key[KEY_SIZE]) {
    memset(key, 0, KEY_SIZE);
    key[0] = (index >> 24) & 0xFF;
    key[1] = (index >> 16) & 0xFF;
    key[2] = (index >> 8) & 0xFF;
    key[3] = index & 0xFF;
}

static void make_value(uint32_t index, uint8_t value[VALUE_SIZE]) {
    for (int i = 0; i < VALUE_SIZE; i++) {
        value[i] = (uint8_t)((index * 7 + i * 13) & 0xFF);
    }
}

static void cleanup_path(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

static void cleanup_all(void) {
    cleanup_path(ART_DIR);
    cleanup_path(LMDB_PATH);
    sync();
    usleep(10000);
}

static uint64_t dir_size(const char *path) {
    uint64_t total = 0;
    DIR *d = opendir(path);
    if (!d) {
        struct stat st;
        if (stat(path, &st) == 0) return st.st_size;
        return 0;
    }
    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        char full[1024];
        snprintf(full, sizeof(full), "%s/%s", path, entry->d_name);
        struct stat st;
        if (stat(full, &st) == 0) {
            if (S_ISDIR(st.st_mode))
                total += dir_size(full);
            else
                total += st.st_size;
        }
    }
    closedir(d);
    return total;
}

static void format_size(uint64_t bytes, char *buf, size_t buf_size) {
    if (bytes >= 1024ULL * 1024 * 1024)
        snprintf(buf, buf_size, "%.1f GB", bytes / (1024.0 * 1024 * 1024));
    else if (bytes >= 1024ULL * 1024)
        snprintf(buf, buf_size, "%.1f MB", bytes / (1024.0 * 1024));
    else if (bytes >= 1024)
        snprintf(buf, buf_size, "%.1f KB", bytes / 1024.0);
    else
        snprintf(buf, buf_size, "%lu B", bytes);
}

// ============================================================================
// Result printing
// ============================================================================

typedef struct {
    uint64_t elapsed_us;
    int num_ops;
} bench_result_t;

static void print_result(const char *label, bench_result_t r) {
    double ops = (r.elapsed_us > 0) ? (double)r.num_ops / (r.elapsed_us / 1e6) : 0;
    double us = (r.num_ops > 0) ? (double)r.elapsed_us / r.num_ops : 0;
    printf("    %-7s %12.0f ops/sec  (%7.2f us/op)\n", label, ops, us);
}

static void print_comparison(const char *name, bench_result_t art,
                             bench_result_t lmdb __attribute__((unused))) {
    printf("  %-28s\n", name);
    print_result("ART:", art);
#ifdef HAVE_LMDB
    print_result("LMDB:", lmdb);
    double art_ops = (art.elapsed_us > 0) ? (double)art.num_ops / (art.elapsed_us / 1e6) : 0;
    double lmdb_ops = (lmdb.elapsed_us > 0) ? (double)lmdb.num_ops / (lmdb.elapsed_us / 1e6) : 0;
    if (art_ops > 0 && lmdb_ops > 0) {
        double ratio = lmdb_ops / art_ops;
        if (ratio > 1.0)
            printf("    Ratio:  ART is %.1fx slower\n", ratio);
        else
            printf("    Ratio:  ART is %.1fx faster\n", 1.0 / ratio);
    }
#endif
    printf("\n");
}

// ============================================================================
// ART helpers
// ============================================================================

static data_art_tree_t *art_open(void) {
    cleanup_path(ART_DIR);
    mkdir(ART_DIR, 0755);

    data_art_tree_t *tree = data_art_create(ART_FILE, KEY_SIZE);
    if (!tree) {
        fprintf(stderr, "ART: data_art_create failed\n");
    }
    return tree;
}

static void art_close(data_art_tree_t *tree) {
    if (tree) data_art_destroy(tree);
}

// ============================================================================
// LMDB helpers
// ============================================================================

#ifdef HAVE_LMDB

typedef struct {
    MDB_env *env;
    MDB_dbi dbi;
} lmdb_ctx_t;

static lmdb_ctx_t lmdb_open(void) {
    lmdb_ctx_t ctx = {0};
    cleanup_path(LMDB_PATH);
    mkdir(LMDB_PATH, 0755);

    int rc = mdb_env_create(&ctx.env);
    if (rc) { fprintf(stderr, "LMDB: env_create: %s\n", mdb_strerror(rc)); return ctx; }

    mdb_env_set_mapsize(ctx.env, 2UL * 1024 * 1024 * 1024);

    // MDB_NOSYNC + MDB_WRITEMAP = no fsync, mmap writes (fastest mode)
    rc = mdb_env_open(ctx.env, LMDB_PATH, MDB_NOSYNC | MDB_WRITEMAP, 0664);
    if (rc) { fprintf(stderr, "LMDB: env_open: %s\n", mdb_strerror(rc)); return ctx; }

    MDB_txn *txn;
    mdb_txn_begin(ctx.env, NULL, 0, &txn);
    mdb_dbi_open(txn, NULL, 0, &ctx.dbi);
    mdb_txn_commit(txn);

    return ctx;
}

static void lmdb_close(lmdb_ctx_t *ctx) {
    if (ctx->env) {
        mdb_dbi_close(ctx->env, ctx->dbi);
        mdb_env_close(ctx->env);
    }
    memset(ctx, 0, sizeof(*ctx));
}

#endif // HAVE_LMDB

// ============================================================================
// Benchmarks
// ============================================================================

/**
 * Bulk insert: all keys in a single transaction.
 * This is the Ethereum block import pattern — batch of state changes committed atomically.
 */
static void bench_bulk_insert_sequential(int num_keys) {
    bench_result_t art_res = {0}, lmdb_res = {0};
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    // --- ART: single txn ---
    {
        data_art_tree_t *tree = art_open();
        if (!tree) return;

        uint64_t txn_id;
        data_art_begin_txn(tree, &txn_id);

        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_sequential_key(i, key);
            make_value(i, value);
            data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
        }
        data_art_commit_txn(tree);
        art_res.elapsed_us = get_time_us() - start;
        art_res.num_ops = num_keys;
        art_close(tree);
    }

    // --- LMDB: single txn ---
#ifdef HAVE_LMDB
    {
        lmdb_ctx_t ctx = lmdb_open();
        if (!ctx.env) return;

        MDB_txn *txn;
        mdb_txn_begin(ctx.env, NULL, 0, &txn);

        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_sequential_key(i, key);
            make_value(i, value);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv = {VALUE_SIZE, value};
            mdb_put(txn, ctx.dbi, &mk, &mv, 0);
        }
        mdb_txn_commit(txn);
        lmdb_res.elapsed_us = get_time_us() - start;
        lmdb_res.num_ops = num_keys;
        lmdb_close(&ctx);
    }
#endif

    print_comparison("Bulk Sequential Insert", art_res, lmdb_res);
}

static void bench_bulk_insert_random(int num_keys) {
    bench_result_t art_res = {0}, lmdb_res = {0};
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    // --- ART: single txn ---
    {
        data_art_tree_t *tree = art_open();
        if (!tree) return;

        uint64_t txn_id;
        data_art_begin_txn(tree, &txn_id);

        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
        }
        data_art_commit_txn(tree);
        art_res.elapsed_us = get_time_us() - start;
        art_res.num_ops = num_keys;
        art_close(tree);
    }

    // --- LMDB: single txn ---
#ifdef HAVE_LMDB
    {
        lmdb_ctx_t ctx = lmdb_open();
        if (!ctx.env) return;

        MDB_txn *txn;
        mdb_txn_begin(ctx.env, NULL, 0, &txn);

        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv = {VALUE_SIZE, value};
            mdb_put(txn, ctx.dbi, &mk, &mv, 0);
        }
        mdb_txn_commit(txn);
        lmdb_res.elapsed_us = get_time_us() - start;
        lmdb_res.num_ops = num_keys;
        lmdb_close(&ctx);
    }
#endif

    print_comparison("Bulk Random Insert", art_res, lmdb_res);
}

/**
 * Per-key insert: one transaction per key (autocommit).
 * Tests per-transaction overhead.
 */
static void bench_perkey_insert(int num_keys) {
    bench_result_t art_res = {0}, lmdb_res = {0};
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    // --- ART: auto-commit per key ---
    {
        data_art_tree_t *tree = art_open();
        if (!tree) return;

        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
        }
        art_res.elapsed_us = get_time_us() - start;
        art_res.num_ops = num_keys;
        art_close(tree);
    }

    // --- LMDB: one txn per key ---
#ifdef HAVE_LMDB
    {
        lmdb_ctx_t ctx = lmdb_open();
        if (!ctx.env) return;

        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);

            MDB_txn *txn;
            mdb_txn_begin(ctx.env, NULL, 0, &txn);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv = {VALUE_SIZE, value};
            mdb_put(txn, ctx.dbi, &mk, &mv, 0);
            mdb_txn_commit(txn);
        }
        lmdb_res.elapsed_us = get_time_us() - start;
        lmdb_res.num_ops = num_keys;
        lmdb_close(&ctx);
    }
#endif

    print_comparison("Per-Key Insert (autocommit)", art_res, lmdb_res);
}

/**
 * Random point lookup (existing keys).
 * Setup: bulk insert all keys first (not timed), then time lookups.
 */
static void bench_random_lookup(int num_keys) {
    bench_result_t art_res = {0}, lmdb_res = {0};
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    // --- ART ---
    {
        data_art_tree_t *tree = art_open();
        if (!tree) return;

        // Setup (not timed): bulk insert
        uint64_t txn_id;
        data_art_begin_txn(tree, &txn_id);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
        }
        data_art_commit_txn(tree);

        // Timed: lookup in reverse order (zero-alloc path)
        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(num_keys - 1 - i, key);
            size_t vlen;
            if (!data_art_get_into(tree, key, KEY_SIZE, value, VALUE_SIZE, &vlen)) {
                fprintf(stderr, "ART: lookup miss at %d\n", num_keys - 1 - i);
                break;
            }
        }
        art_res.elapsed_us = get_time_us() - start;
        art_res.num_ops = num_keys;
        art_close(tree);
    }

    // --- LMDB ---
#ifdef HAVE_LMDB
    {
        lmdb_ctx_t ctx = lmdb_open();
        if (!ctx.env) return;

        // Setup
        MDB_txn *txn;
        mdb_txn_begin(ctx.env, NULL, 0, &txn);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv = {VALUE_SIZE, value};
            mdb_put(txn, ctx.dbi, &mk, &mv, 0);
        }
        mdb_txn_commit(txn);

        // Timed: read-only txn
        mdb_txn_begin(ctx.env, NULL, MDB_RDONLY, &txn);
        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(num_keys - 1 - i, key);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv;
            mdb_get(txn, ctx.dbi, &mk, &mv);
        }
        lmdb_res.elapsed_us = get_time_us() - start;
        lmdb_res.num_ops = num_keys;
        mdb_txn_abort(txn);
        lmdb_close(&ctx);
    }
#endif

    print_comparison("Random Point Lookup", art_res, lmdb_res);
}

/**
 * Range scan: iterate all keys in order.
 */
static void bench_range_scan(int num_keys) {
    bench_result_t art_res = {0}, lmdb_res = {0};
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    // --- ART ---
    {
        data_art_tree_t *tree = art_open();
        if (!tree) return;

        // Setup
        uint64_t txn_id;
        data_art_begin_txn(tree, &txn_id);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
        }
        data_art_commit_txn(tree);

        // Timed: full scan
        uint64_t start = get_time_us();
        data_art_iterator_t *iter = data_art_iterator_create(tree);
        int count = 0;
        while (data_art_iterator_next(iter)) {
            size_t klen, vlen;
            data_art_iterator_key(iter, &klen);
            data_art_iterator_value(iter, &vlen);
            count++;
        }
        data_art_iterator_destroy(iter);
        art_res.elapsed_us = get_time_us() - start;
        art_res.num_ops = count;
        art_close(tree);
    }

    // --- LMDB ---
#ifdef HAVE_LMDB
    {
        lmdb_ctx_t ctx = lmdb_open();
        if (!ctx.env) return;

        // Setup
        MDB_txn *txn;
        mdb_txn_begin(ctx.env, NULL, 0, &txn);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv = {VALUE_SIZE, value};
            mdb_put(txn, ctx.dbi, &mk, &mv, 0);
        }
        mdb_txn_commit(txn);

        // Timed: cursor scan
        mdb_txn_begin(ctx.env, NULL, MDB_RDONLY, &txn);
        MDB_cursor *cursor;
        mdb_cursor_open(txn, ctx.dbi, &cursor);

        uint64_t start = get_time_us();
        MDB_val mk, mv;
        int count = 0;
        int rc = mdb_cursor_get(cursor, &mk, &mv, MDB_FIRST);
        while (rc == 0) {
            count++;
            rc = mdb_cursor_get(cursor, &mk, &mv, MDB_NEXT);
        }
        lmdb_res.elapsed_us = get_time_us() - start;
        lmdb_res.num_ops = count;
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        lmdb_close(&ctx);
    }
#endif

    print_comparison("Range Scan (full)", art_res, lmdb_res);
}

/**
 * Negative lookup: search for keys that don't exist.
 */
static void bench_negative_lookup(int num_keys) {
    bench_result_t art_res = {0}, lmdb_res = {0};
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    // --- ART ---
    {
        data_art_tree_t *tree = art_open();
        if (!tree) return;

        // Setup
        uint64_t txn_id;
        data_art_begin_txn(tree, &txn_id);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
        }
        data_art_commit_txn(tree);

        // Timed: lookup keys that don't exist (zero-alloc path)
        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(num_keys + i, key);
            size_t vlen;
            data_art_get_into(tree, key, KEY_SIZE, value, VALUE_SIZE, &vlen);
        }
        art_res.elapsed_us = get_time_us() - start;
        art_res.num_ops = num_keys;
        art_close(tree);
    }

    // --- LMDB ---
#ifdef HAVE_LMDB
    {
        lmdb_ctx_t ctx = lmdb_open();
        if (!ctx.env) return;

        // Setup
        MDB_txn *txn;
        mdb_txn_begin(ctx.env, NULL, 0, &txn);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv = {VALUE_SIZE, value};
            mdb_put(txn, ctx.dbi, &mk, &mv, 0);
        }
        mdb_txn_commit(txn);

        // Timed
        mdb_txn_begin(ctx.env, NULL, MDB_RDONLY, &txn);
        uint64_t start = get_time_us();
        for (int i = 0; i < num_keys; i++) {
            make_key(num_keys + i, key);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv;
            mdb_get(txn, ctx.dbi, &mk, &mv);
        }
        lmdb_res.elapsed_us = get_time_us() - start;
        lmdb_res.num_ops = num_keys;
        mdb_txn_abort(txn);
        lmdb_close(&ctx);
    }
#endif

    print_comparison("Negative Lookup", art_res, lmdb_res);
}

static const char *slot_class_name(int idx) {
    static const char *names[] = {"Node4", "Node16", "Node48", "Node256", "Leaf"};
    return (idx >= 0 && idx < NUM_SLOT_CLASSES) ? names[idx] : "?";
}

/**
 * Slot utilization stats — shows how well multi-node-per-page packing works.
 */
static void bench_slot_stats(int num_keys) {
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    printf("  %-28s\n", "Slot Allocator Stats");

    data_art_tree_t *tree = art_open();
    if (!tree) { printf("    (failed)\n\n"); return; }

    uint64_t txn_id;
    data_art_begin_txn(tree, &txn_id);
    for (int i = 0; i < num_keys; i++) {
        make_key(i, key);
        make_value(i, value);
        data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
    }
    data_art_commit_txn(tree);

    data_art_stats_t stats;
    data_art_get_stats(tree, &stats);

    uint64_t total_slot_allocs = 0, total_slot_frees = 0, total_slot_pages = 0;
    printf("    %-8s %8s %8s %8s %6s %8s\n",
           "Class", "Pages", "Allocs", "Frees", "Slots", "Packing");
    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        total_slot_allocs += stats.slot_allocs[i];
        total_slot_frees += stats.slot_frees[i];
        total_slot_pages += stats.slot_pages_created[i];
        uint16_t spp = tree->slot_classes[i].slots_per_page;
        double packing = (stats.slot_pages_created[i] > 0 && spp > 0) ?
            (double)(stats.slot_allocs[i] - stats.slot_frees[i]) /
            (stats.slot_pages_created[i] * spp) * 100.0 : 0;
        printf("    %-8s %8lu %8lu %8lu %6u %7.1f%%\n",
               slot_class_name(i),
               stats.slot_pages_created[i],
               stats.slot_allocs[i],
               stats.slot_frees[i],
               spp,
               packing);
    }
    printf("    %-8s %8lu %8lu %8lu\n", "Total",
           total_slot_pages, total_slot_allocs, total_slot_frees);
    printf("    Dedicated pages:  %lu\n", stats.dedicated_pages_created);

    uint64_t total_hints = stats.slot_hint_hits + stats.slot_hint_misses;
    double hint_rate = total_hints > 0 ?
        (double)stats.slot_hint_hits / total_hints * 100.0 : 0;
    printf("    Hint hits/total:  %lu / %lu (%.1f%%)\n",
           stats.slot_hint_hits, total_hints, hint_rate);
    printf("    Pages reused:     %lu\n", stats.pages_reused);

    art_close(tree);
    printf("\n");
}

/**
 * Disk usage comparison.
 */
static void bench_disk_usage(int num_keys) {
    uint8_t key[KEY_SIZE], value[VALUE_SIZE];

    printf("  %-28s\n", "Disk Usage");

    // --- ART ---
    {
        data_art_tree_t *tree = art_open();
        if (!tree) { printf("    ART:   (failed)\n"); return; }

        uint64_t txn_id;
        data_art_begin_txn(tree, &txn_id);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            data_art_insert(tree, key, KEY_SIZE, value, VALUE_SIZE);
        }
        data_art_commit_txn(tree);
        data_art_flush(tree);

        // Get actual used size: next_page_id * PAGE_SIZE
        uint64_t art_size = tree->mmap_storage->next_page_id * PAGE_SIZE;
        char size_str[64];
        format_size(art_size, size_str, sizeof(size_str));
        printf("    ART:    %s  (%lu bytes/key, %lu pages)\n", size_str,
               num_keys > 0 ? art_size / num_keys : 0,
               tree->mmap_storage->next_page_id);

        art_close(tree);
    }

    // --- LMDB ---
#ifdef HAVE_LMDB
    {
        lmdb_ctx_t ctx = lmdb_open();
        if (!ctx.env) { printf("    LMDB:  (failed)\n\n"); return; }

        MDB_txn *txn;
        mdb_txn_begin(ctx.env, NULL, 0, &txn);
        for (int i = 0; i < num_keys; i++) {
            make_key(i, key);
            make_value(i, value);
            MDB_val mk = {KEY_SIZE, key};
            MDB_val mv = {VALUE_SIZE, value};
            mdb_put(txn, ctx.dbi, &mk, &mv, 0);
        }
        mdb_txn_commit(txn);
        mdb_env_sync(ctx.env, 1);

        // Use mdb_env_info to get actual used size, not mmap reservation
        MDB_envinfo info;
        MDB_stat mstat;
        mdb_env_info(ctx.env, &info);
        mdb_env_stat(ctx.env, &mstat);
        uint64_t lmdb_size = (uint64_t)info.me_last_pgno * (uint64_t)mstat.ms_psize;

        lmdb_close(&ctx);

        char size_str[64];
        format_size(lmdb_size, size_str, sizeof(size_str));
        printf("    LMDB:   %s  (%lu bytes/key)\n", size_str,
               num_keys > 0 ? lmdb_size / num_keys : 0);
    }
#else
    printf("    LMDB:   (not available)\n");
#endif

    printf("\n");
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    log_set_level(LOG_LEVEL_ERROR);

    int num_keys = 100000;
    if (argc > 1) {
        num_keys = atoi(argv[1]);
        if (num_keys <= 0) num_keys = 100000;
    }

    printf("\n");
    printf("================================================================\n");
    printf("  ART Database Benchmark (mmap backend)");
#ifdef HAVE_LMDB
    printf(" — vs LMDB");
#endif
    printf("\n");
    printf("  Keys: %d bytes | Values: %d bytes | Count: %d\n",
           KEY_SIZE, VALUE_SIZE, num_keys);
    printf("  Both: no fsync (ART: mmap, LMDB: MDB_NOSYNC)\n");
    printf("================================================================\n\n");

    bench_bulk_insert_sequential(num_keys);
    bench_bulk_insert_random(num_keys);
    bench_perkey_insert(num_keys);
    bench_random_lookup(num_keys);
    bench_range_scan(num_keys);
    bench_negative_lookup(num_keys);
    bench_disk_usage(num_keys);
    bench_slot_stats(num_keys);

    printf("================================================================\n");
    printf("  Benchmark complete\n");
    printf("================================================================\n\n");

    cleanup_all();
    return 0;
}
