#include "verkle_journal.h"
#include "verkle_snapshot.h"
#include "verkle_state.h"
#include "verkle_key.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

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

static size_t get_file_size(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (size_t)st.st_size;
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

#define SNAP_PATH "/tmp/bench_verkle_state.snap"
#define FWD_PATH  "/tmp/bench_verkle_state.fwd"

static void cleanup(void)
{
    remove(SNAP_PATH);
    remove(FWD_PATH);
    char tmp[256];
    /* Clean up possible leftover temp files */
    for (int i = 0; i < 100; i++) {
        snprintf(tmp, sizeof(tmp), SNAP_PATH ".tmp.%d", i);
        remove(tmp);
    }
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv)
{
    uint32_t accounts_k = 100;
    uint32_t num_blocks = 1000;
    uint32_t ckpt_interval = 200;

    if (argc > 1) accounts_k = (uint32_t)atoi(argv[1]);
    if (argc > 2) num_blocks = (uint32_t)atoi(argv[2]);
    if (argc > 3) ckpt_interval = (uint32_t)atoi(argv[3]);

    uint32_t target_accounts = accounts_k * 1000;
    uint32_t ops_per_block = 500;

    printf("=== Verkle State Scale Test ===\n");
    printf("Target accounts: %uK (%u)\n", accounts_k, target_accounts);
    printf("Blocks:          %u\n", num_blocks);
    printf("Ops/block:       %u\n", ops_per_block);
    printf("Checkpoint every: %u blocks\n", ckpt_interval);
    printf("\n");

    cleanup();

    rng_t rng = { .state = 0xDEADBEEFCAFEBABEULL };

    /* === Phase 1: Prefill accounts === */
    printf("--- Phase 1: Prefill %u accounts ---\n", target_accounts);
    double t_prefill_start = now_sec();

    verkle_state_t *vs = verkle_state_create();
    if (!vs) { fprintf(stderr, "Failed to create state\n"); return 1; }

    /* Store addresses for later use */
    uint32_t addr_cap = target_accounts + num_blocks * 75; /* extra for new accounts */
    uint8_t (*addrs)[20] = malloc(addr_cap * 20);
    if (!addrs) { fprintf(stderr, "OOM\n"); return 1; }
    uint32_t addr_count = 0;

    for (uint32_t i = 0; i < target_accounts; i++) {
        uint8_t addr[20];
        rng_addr(&rng, addr);
        memcpy(addrs[addr_count++], addr, 20);

        /* Set version + nonce + balance */
        verkle_state_set_version(vs, addr, 0);
        verkle_state_set_nonce(vs, addr, 1);
        uint8_t bal[32] = {0};
        uint64_t b = rng_next(&rng) % 1000000;
        memcpy(bal, &b, sizeof(b));
        verkle_state_set_balance(vs, addr, bal);

        if ((i + 1) % 10000 == 0) {
            printf("  prefill %u / %u  (RSS %zu MB)\n",
                   i + 1, target_accounts, get_rss_mb());
        }
    }

    double t_prefill = now_sec() - t_prefill_start;
    printf("  Prefill done: %.1fs (%u accounts, RSS %zu MB)\n\n",
           t_prefill, target_accounts, get_rss_mb());

    /* === Phase 2: Block execution === */
    printf("--- Phase 2: Execute %u blocks ---\n", num_blocks);

    verkle_journal_t *j = verkle_journal_create(vs->tree);
    if (!j) { fprintf(stderr, "Failed to create journal\n"); return 1; }

    verkle_journal_enable_fwd(j, FWD_PATH, 0);

    latency_t lat_exec, lat_commit, lat_ckpt;
    lat_init(&lat_exec, num_blocks);
    lat_init(&lat_commit, num_blocks);
    lat_init(&lat_ckpt, num_blocks / (ckpt_interval ? ckpt_interval : 1) + 1);

    uint64_t total_ops = 0;
    uint32_t ckpt_count = 0;
    double ckpt_total_time = 0;
    size_t peak_rss = 0;

    double t_blocks_start = now_sec();

    for (uint32_t blk = 1; blk <= num_blocks; blk++) {
        verkle_journal_begin_block(j, blk);

        double t_exec_start = now_sec();

        for (uint32_t op = 0; op < ops_per_block; op++) {
            uint32_t r = (uint32_t)(rng_next(&rng) % 100);

            if (r < 40) {
                /* Balance update (40%) — existing account */
                uint32_t idx = (uint32_t)(rng_next(&rng) % addr_count);
                uint8_t bal[32] = {0};
                uint64_t b = rng_next(&rng) % 1000000;
                memcpy(bal, &b, sizeof(b));
                uint8_t key[32];
                verkle_account_balance_key(key, addrs[idx]);
                verkle_journal_set(j, key, bal);
            } else if (r < 60) {
                /* Nonce increment (20%) — existing account */
                uint32_t idx = (uint32_t)(rng_next(&rng) % addr_count);
                uint64_t nonce = verkle_state_get_nonce(vs, addrs[idx]);
                nonce++;
                uint8_t val[32] = {0};
                memcpy(val, &nonce, sizeof(nonce));
                uint8_t key[32];
                verkle_account_nonce_key(key, addrs[idx]);
                verkle_journal_set(j, key, val);
            } else if (r < 85) {
                /* Storage write (25%) — existing account, random slot */
                uint32_t idx = (uint32_t)(rng_next(&rng) % addr_count);
                uint8_t slot[32], val[32];
                rng_bytes(&rng, slot, 32);
                rng_bytes(&rng, val, 32);
                uint8_t key[32];
                verkle_storage_key(key, addrs[idx], slot);
                verkle_journal_set(j, key, val);
            } else if (r < 95) {
                /* New account (10%) */
                if (addr_count < addr_cap) {
                    uint8_t addr[20];
                    rng_addr(&rng, addr);
                    memcpy(addrs[addr_count++], addr, 20);

                    uint8_t key[32], val[32];
                    /* version */
                    memset(val, 0, 32);
                    verkle_account_version_key(key, addr);
                    verkle_journal_set(j, key, val);
                    /* nonce */
                    val[0] = 1;
                    verkle_account_nonce_key(key, addr);
                    verkle_journal_set(j, key, val);
                    /* balance */
                    memset(val, 0, 32);
                    uint64_t b = rng_next(&rng) % 1000000;
                    memcpy(val, &b, sizeof(b));
                    verkle_account_balance_key(key, addr);
                    verkle_journal_set(j, key, val);
                }
            } else {
                /* Code deploy (5%) */
                if (addr_count < addr_cap) {
                    uint8_t addr[20];
                    rng_addr(&rng, addr);
                    memcpy(addrs[addr_count++], addr, 20);

                    /* Set account header via journal */
                    uint8_t key[32], val[32];
                    memset(val, 0, 32);
                    verkle_account_version_key(key, addr);
                    verkle_journal_set(j, key, val);
                    memset(val, 0, 32); val[0] = 1;
                    verkle_account_nonce_key(key, addr);
                    verkle_journal_set(j, key, val);

                    /* Code: 256-2048 bytes */
                    uint32_t code_len = 256 + (uint32_t)(rng_next(&rng) % 1793);
                    uint8_t *code = malloc(code_len);
                    rng_bytes(&rng, code, code_len);

                    /* Code size */
                    memset(val, 0, 32);
                    memcpy(val, &code_len, sizeof(code_len));
                    verkle_account_code_size_key(key, addr);
                    verkle_journal_set(j, key, val);

                    /* Code chunks via journal */
                    uint32_t num_chunks = (code_len + 31) / 32;
                    for (uint32_t c = 0; c < num_chunks; c++) {
                        memset(val, 0, 32);
                        uint32_t offset = c * 32;
                        uint32_t copy = code_len - offset;
                        if (copy > 32) copy = 32;
                        memcpy(val, code + offset, copy);
                        verkle_code_chunk_key(key, addr, c);
                        verkle_journal_set(j, key, val);
                    }
                    free(code);
                }
            }
        }

        double exec_ms = (now_sec() - t_exec_start) * 1000.0;
        lat_push(&lat_exec, exec_ms);

        double t_commit_start = now_sec();
        verkle_journal_commit_block(j);
        double commit_ms = (now_sec() - t_commit_start) * 1000.0;
        lat_push(&lat_commit, commit_ms);

        total_ops += ops_per_block;

        size_t rss = get_rss_mb();
        if (rss > peak_rss) peak_rss = rss;

        /* Checkpoint */
        bool did_ckpt = false;
        double ckpt_ms = 0;
        if (ckpt_interval > 0 && blk % ckpt_interval == 0) {
            double t_ckpt = now_sec();
            verkle_journal_checkpoint_start(j, SNAP_PATH);
            verkle_journal_checkpoint_wait(j);
            ckpt_ms = (now_sec() - t_ckpt) * 1000.0;
            lat_push(&lat_ckpt, ckpt_ms);
            ckpt_total_time += ckpt_ms;
            ckpt_count++;
            did_ckpt = true;
        }

        /* Progress every 20 blocks */
        if (blk % 20 == 0 || blk == num_blocks) {
            printf("  block %5u | accts %7u | exec %6.1fms | commit %5.2fms | RSS %zu MB",
                   blk, addr_count, exec_ms, commit_ms, rss);
            if (did_ckpt) {
                printf(" | CKPT %.1fs snap=%zuMB jrnl=%zuMB",
                       ckpt_ms / 1000.0,
                       get_file_size(SNAP_PATH) / (1024 * 1024),
                       get_file_size(FWD_PATH) / (1024 * 1024));
            }
            printf("\n");
        }
    }

    double t_blocks = now_sec() - t_blocks_start;

    printf("\n--- Phase 3: Final checkpoint ---\n");
    double t_final_ckpt = now_sec();
    verkle_journal_checkpoint_start(j, SNAP_PATH);
    verkle_journal_checkpoint_wait(j);
    double final_ckpt_ms = (now_sec() - t_final_ckpt) * 1000.0;
    printf("  Final checkpoint: %.1fms (snap=%zu MB)\n",
           final_ckpt_ms, get_file_size(SNAP_PATH) / (1024 * 1024));

    /* Capture root hash before recovery test */
    uint8_t orig_hash[32];
    verkle_state_root_hash(vs, orig_hash);

    printf("\n--- Phase 4: Recovery test ---\n");
    double t_load = now_sec();
    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    double load_ms = (now_sec() - t_load) * 1000.0;
    printf("  Snapshot load: %.1fms\n", load_ms);

    if (loaded) {
        /* Replay forward journal */
        double t_replay = now_sec();
        uint64_t last_block = 0;
        verkle_journal_replay_fwd(FWD_PATH, loaded, &last_block);
        double replay_ms = (now_sec() - t_replay) * 1000.0;
        printf("  Journal replay: %.1fms (last_block=%lu)\n",
               replay_ms, last_block);

        /* Compare root hash */
        uint8_t loaded_hash[32];
        verkle_root_hash(loaded, loaded_hash);
        bool match = memcmp(orig_hash, loaded_hash, 32) == 0;
        printf("  Root hash match: %s\n", match ? "YES" : "NO");

        verkle_destroy(loaded);
    } else {
        printf("  ERROR: snapshot load failed\n");
    }

    /* === Final Summary === */
    printf("\n");
    printf("========================================\n");
    printf("  Verkle State Scale Test Results\n");
    printf("========================================\n");
    printf("Accounts:          %u\n", addr_count);
    printf("Blocks:            %u\n", num_blocks);
    printf("Ops/block:         %u\n", ops_per_block);
    printf("Total ops:         %lu\n", (unsigned long)total_ops);
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
    if (lat_ckpt.count > 0)
        lat_print("checkpoint (ms):", &lat_ckpt);
    printf("\n");

    printf("Memory:\n");
    printf("  Peak RSS:        %zu MB\n", peak_rss);
    printf("\n");

    printf("Disk:\n");
    printf("  Snapshot size:   %.1f MB\n",
           get_file_size(SNAP_PATH) / (1024.0 * 1024.0));
    printf("  Journal size:    %.1f MB\n",
           get_file_size(FWD_PATH) / (1024.0 * 1024.0));
    printf("\n");

    if (ckpt_count > 0) {
        printf("Checkpoints:\n");
        printf("  Count:           %u\n", ckpt_count);
        printf("  Avg time:        %.1fms\n", ckpt_total_time / ckpt_count);
        printf("  Total time:      %.1fs\n", ckpt_total_time / 1000.0);
        printf("\n");
    }

    printf("========================================\n");

    /* Cleanup */
    lat_free(&lat_exec);
    lat_free(&lat_commit);
    lat_free(&lat_ckpt);
    free(addrs);
    verkle_journal_destroy(j);
    verkle_state_destroy(vs);
    cleanup();

    return 0;
}
