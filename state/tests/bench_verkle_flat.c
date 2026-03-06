#include "verkle_state.h"
#include "verkle_key.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static size_t get_rss_mb(void)
{
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    size_t rss_kb = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, " %zu", &rss_kb);
            break;
        }
    }
    fclose(f);
    return rss_kb / 1024;
}

static size_t dir_size_mb(const char *path)
{
    DIR *d = opendir(path);
    if (!d) return 0;
    size_t total = 0;
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        char buf[512];
        snprintf(buf, sizeof(buf), "%s/%s", path, ent->d_name);
        struct stat st;
        if (stat(buf, &st) == 0)
            total += (size_t)st.st_size;
    }
    closedir(d);
    return total / (1024 * 1024);
}

/* =========================================================================
 * RNG — SplitMix64 (deterministic, fast)
 * ========================================================================= */

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng)
{
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void rng_addr(rng_t *rng, uint8_t addr[20])
{
    uint64_t a = rng_next(rng);
    uint64_t b = rng_next(rng);
    uint32_t c = (uint32_t)rng_next(rng);
    memcpy(addr, &a, 8);
    memcpy(addr + 8, &b, 8);
    memcpy(addr + 16, &c, 4);
}

static void rng_bytes(rng_t *rng, uint8_t *buf, size_t len)
{
    while (len >= 8) {
        uint64_t v = rng_next(rng);
        memcpy(buf, &v, 8);
        buf += 8;
        len -= 8;
    }
    if (len > 0) {
        uint64_t v = rng_next(rng);
        memcpy(buf, &v, len);
    }
}

/* =========================================================================
 * Latency Percentiles
 * ========================================================================= */

static int cmp_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

typedef struct {
    double *data;
    uint32_t count;
    uint32_t cap;
} latency_t;

static void lat_init(latency_t *l, uint32_t cap)
{
    l->data = malloc(cap * sizeof(double));
    l->count = 0;
    l->cap = cap;
}

static void lat_push(latency_t *l, double ms)
{
    if (l->count < l->cap)
        l->data[l->count++] = ms;
}

static void lat_print(const char *label, latency_t *l)
{
    if (l->count == 0) return;
    qsort(l->data, l->count, sizeof(double), cmp_double);
    printf("  %-20s min=%.2f  p50=%.2f  p95=%.2f  p99=%.2f  max=%.2f\n",
           label,
           l->data[0],
           l->data[l->count / 2],
           l->data[(uint64_t)(l->count * 0.95)],
           l->data[(uint64_t)(l->count * 0.99)],
           l->data[l->count - 1]);
}

static void lat_free(latency_t *l) { free(l->data); }

/* =========================================================================
 * Paths
 * ========================================================================= */

#define VAL_DIR  "/tmp/bench_vflat_val"
#define COMM_DIR "/tmp/bench_vflat_comm"

static void cleanup(void)
{
    system("rm -rf " VAL_DIR " " COMM_DIR);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv)
{
    uint32_t accounts_k = 100;
    uint32_t num_blocks = 1000;
    uint32_t revert_pct = 5;     /* % of blocks to revert */
    uint64_t shard_cap = 1 << 20; /* 1M entries per shard */

    if (argc > 1) accounts_k = (uint32_t)atoi(argv[1]);
    if (argc > 2) num_blocks = (uint32_t)atoi(argv[2]);
    if (argc > 3) revert_pct = (uint32_t)atoi(argv[3]);

    uint32_t target_accounts = accounts_k * 1000;
    uint32_t ops_per_block = 500;

    printf("=== Verkle Flat Scale Test ===\n");
    printf("Target accounts: %uK (%u)\n", accounts_k, target_accounts);
    printf("Blocks:          %u\n", num_blocks);
    printf("Ops/block:       %u\n", ops_per_block);
    printf("Revert %%:        %u%%\n", revert_pct);
    printf("Shard capacity:  %lu\n", (unsigned long)shard_cap);
    printf("\n");

    cleanup();
    mkdir(VAL_DIR, 0755);
    mkdir(COMM_DIR, 0755);

    rng_t rng = { .state = 0xDEADBEEFCAFEBABEULL };

    /* === Phase 1: Prefill accounts === */
    printf("--- Phase 1: Prefill %u accounts ---\n", target_accounts);
    double t_prefill_start = now_sec();

    (void)shard_cap;  /* no longer needed — art_store auto-sizes */
    verkle_state_t *vs = verkle_state_create_flat(VAL_DIR, COMM_DIR);
    if (!vs) { fprintf(stderr, "Failed to create flat state\n"); return 1; }

    /* Store addresses for later use */
    uint32_t addr_cap = target_accounts + num_blocks * 75;
    uint8_t (*addrs)[20] = malloc(addr_cap * 20);
    if (!addrs) { fprintf(stderr, "OOM\n"); return 1; }
    uint32_t addr_count = 0;

    /* Prefill in batches of 1000 accounts per block */
    uint32_t batch_size = 1000;
    uint32_t prefill_blocks = (target_accounts + batch_size - 1) / batch_size;

    for (uint32_t b = 0; b < prefill_blocks; b++) {
        verkle_state_begin_block(vs, b + 1);

        uint32_t start = b * batch_size;
        uint32_t end = start + batch_size;
        if (end > target_accounts) end = target_accounts;

        for (uint32_t i = start; i < end; i++) {
            uint8_t addr[20];
            rng_addr(&rng, addr);
            memcpy(addrs[addr_count++], addr, 20);

            verkle_state_set_version(vs, addr, 0);
            verkle_state_set_nonce(vs, addr, 1);
            uint8_t bal[32] = {0};
            uint64_t bv = rng_next(&rng) % 1000000;
            memcpy(bal, &bv, sizeof(bv));
            verkle_state_set_balance(vs, addr, bal);
        }

        verkle_state_commit_block(vs);

        if ((b + 1) % 10 == 0 || b + 1 == prefill_blocks) {
            printf("  prefill block %u / %u  (%u accounts, RSS %zu MB)\n",
                   b + 1, prefill_blocks, addr_count, get_rss_mb());
        }
    }

    verkle_state_sync(vs);
    double t_prefill = now_sec() - t_prefill_start;
    printf("  Prefill done: %.1fs (%u accounts, RSS %zu MB)\n",
           t_prefill, addr_count, get_rss_mb());
    printf("  Disk: val=%zu MB  comm=%zu MB\n\n",
           dir_size_mb(VAL_DIR), dir_size_mb(COMM_DIR));

    /* Capture root after prefill */
    uint8_t prefill_root[32];
    verkle_state_root_hash(vs, prefill_root);

    /* === Phase 2: Block execution === */
    printf("--- Phase 2: Execute %u blocks ---\n", num_blocks);

    latency_t lat_exec, lat_commit;
    lat_init(&lat_exec, num_blocks);
    lat_init(&lat_commit, num_blocks);

    uint64_t total_ops = 0;
    uint32_t reverts = 0;
    size_t peak_rss = 0;

    double t_blocks_start = now_sec();

    for (uint32_t blk = 1; blk <= num_blocks; blk++) {
        uint64_t block_num = prefill_blocks + blk;
        verkle_state_begin_block(vs, block_num);

        double t_exec_start = now_sec();

        for (uint32_t op = 0; op < ops_per_block; op++) {
            uint32_t r = (uint32_t)(rng_next(&rng) % 100);

            if (r < 40) {
                /* Balance update (40%) */
                uint32_t idx = (uint32_t)(rng_next(&rng) % addr_count);
                uint8_t bal[32] = {0};
                uint64_t bv = rng_next(&rng) % 1000000;
                memcpy(bal, &bv, sizeof(bv));
                verkle_state_set_balance(vs, addrs[idx], bal);
            } else if (r < 60) {
                /* Nonce increment (20%) */
                uint32_t idx = (uint32_t)(rng_next(&rng) % addr_count);
                uint64_t nonce = verkle_state_get_nonce(vs, addrs[idx]);
                nonce++;
                verkle_state_set_nonce(vs, addrs[idx], nonce);
            } else if (r < 85) {
                /* Storage write (25%) */
                uint32_t idx = (uint32_t)(rng_next(&rng) % addr_count);
                uint8_t slot[32], val[32];
                rng_bytes(&rng, slot, 32);
                rng_bytes(&rng, val, 32);
                verkle_state_set_storage(vs, addrs[idx], slot, val);
            } else if (r < 95) {
                /* New account (10%) */
                if (addr_count < addr_cap) {
                    uint8_t addr[20];
                    rng_addr(&rng, addr);
                    memcpy(addrs[addr_count++], addr, 20);

                    verkle_state_set_version(vs, addr, 0);
                    verkle_state_set_nonce(vs, addr, 1);
                    uint8_t bal[32] = {0};
                    uint64_t bv = rng_next(&rng) % 1000000;
                    memcpy(bal, &bv, sizeof(bv));
                    verkle_state_set_balance(vs, addr, bal);
                }
            } else {
                /* Code deploy (5%) */
                if (addr_count < addr_cap) {
                    uint8_t addr[20];
                    rng_addr(&rng, addr);
                    memcpy(addrs[addr_count++], addr, 20);

                    verkle_state_set_version(vs, addr, 0);
                    verkle_state_set_nonce(vs, addr, 1);

                    uint32_t code_len = 256 + (uint32_t)(rng_next(&rng) % 1793);
                    uint8_t *code = malloc(code_len);
                    rng_bytes(&rng, code, code_len);
                    verkle_state_set_code(vs, addr, code, code_len);
                    free(code);
                }
            }
        }

        double exec_ms = (now_sec() - t_exec_start) * 1000.0;
        lat_push(&lat_exec, exec_ms);

        /* Decide: revert or commit */
        bool do_revert = (rng_next(&rng) % 100) < revert_pct;

        double t_commit_start = now_sec();
        if (do_revert) {
            verkle_state_revert_block(vs);
            reverts++;
        } else {
            verkle_state_commit_block(vs);
        }
        double commit_ms = (now_sec() - t_commit_start) * 1000.0;
        lat_push(&lat_commit, commit_ms);

        total_ops += ops_per_block;

        size_t rss = get_rss_mb();
        if (rss > peak_rss) peak_rss = rss;

        if (blk % 20 == 0 || blk == num_blocks) {
            printf("  block %5u | accts %7u | exec %6.1fms | %s %5.2fms | RSS %zu MB\n",
                   blk, addr_count, exec_ms,
                   do_revert ? "revert" : "commit",
                   commit_ms, rss);
        }
    }

    double t_blocks = now_sec() - t_blocks_start;

    /* === Phase 3: Sync and measure disk === */
    printf("\n--- Phase 3: Sync ---\n");
    double t_sync = now_sec();
    verkle_state_sync(vs);
    double sync_ms = (now_sec() - t_sync) * 1000.0;
    printf("  Sync: %.1fms\n", sync_ms);
    printf("  Disk: val=%zu MB  comm=%zu MB\n",
           dir_size_mb(VAL_DIR), dir_size_mb(COMM_DIR));

    /* === Phase 4: Persistence — close and reopen === */
    printf("\n--- Phase 4: Persistence ---\n");
    uint8_t final_root[32];
    verkle_state_root_hash(vs, final_root);
    verkle_state_destroy(vs);

    double t_reopen = now_sec();
    vs = verkle_state_open_flat(VAL_DIR, COMM_DIR);
    double reopen_ms = (now_sec() - t_reopen) * 1000.0;
    printf("  Reopen: %.1fms\n", reopen_ms);

    uint8_t reopened_root[32];
    verkle_state_root_hash(vs, reopened_root);
    bool root_match = memcmp(final_root, reopened_root, 32) == 0;
    printf("  Root hash match: %s\n", root_match ? "YES" : "NO");

    /* Spot-check a few accounts */
    uint32_t spot_ok = 0;
    uint32_t spot_n = addr_count < 100 ? addr_count : 100;
    for (uint32_t i = 0; i < spot_n; i++) {
        uint32_t idx = (uint32_t)(i * (uint64_t)addr_count / spot_n);
        if (verkle_state_exists(vs, addrs[idx]))
            spot_ok++;
    }
    printf("  Spot check: %u / %u accounts exist\n", spot_ok, spot_n);

    /* === Final Summary === */
    printf("\n");
    printf("========================================\n");
    printf("  Verkle Flat Scale Test Results\n");
    printf("========================================\n");
    printf("Accounts:          %u\n", addr_count);
    printf("Blocks:            %u (+ %u prefill)\n", num_blocks, prefill_blocks);
    printf("Ops/block:         %u\n", ops_per_block);
    printf("Total ops:         %lu\n", (unsigned long)total_ops);
    printf("Reverts:           %u / %u\n", reverts, num_blocks);
    printf("\n");

    printf("Timing:\n");
    printf("  Prefill:         %.1fs\n", t_prefill);
    printf("  Block execution: %.1fs\n", t_blocks);
    double total = t_prefill + t_blocks;
    printf("  Total:           %.1fs\n", total);
    printf("  Blocks/sec:      %.1f\n",
           t_blocks > 0 ? num_blocks / t_blocks : 0);
    printf("  Ops/sec:         %.0f\n",
           t_blocks > 0 ? total_ops / t_blocks : 0);
    printf("\n");

    printf("Latency percentiles:\n");
    lat_print("exec (ms):", &lat_exec);
    lat_print("commit (ms):", &lat_commit);
    printf("\n");

    printf("Memory:\n");
    printf("  Peak RSS:        %zu MB\n", peak_rss);
    printf("\n");

    printf("Disk:\n");
    printf("  Value store:     %zu MB\n", dir_size_mb(VAL_DIR));
    printf("  Commit store:    %zu MB\n", dir_size_mb(COMM_DIR));
    printf("  Total:           %zu MB\n",
           dir_size_mb(VAL_DIR) + dir_size_mb(COMM_DIR));
    printf("\n");

    printf("========================================\n");

    /* Cleanup */
    lat_free(&lat_exec);
    lat_free(&lat_commit);
    free(addrs);
    verkle_state_destroy(vs);
    cleanup();

    return 0;
}
