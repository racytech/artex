/**
 * Bench Verkle Threads — compare single-threaded vs multi-threaded commit.
 *
 * Creates a verkle state with N accounts, then runs M blocks of mixed
 * operations. Runs twice: once single-threaded, once with T worker threads.
 * Reports timing for both and speedup ratio.
 *
 * Usage:
 *   ./bench_verkle_threads [accounts_k] [blocks] [threads]
 *   Default: 50K accounts, 200 blocks, 4 threads
 */

#include "verkle_state.h"
#include "verkle_flat.h"
#include "verkle_key.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void rng_addr(rng_t *rng, uint8_t addr[20]) {
    uint64_t a = rng_next(rng);
    uint64_t b = rng_next(rng);
    uint32_t c = (uint32_t)rng_next(rng);
    memcpy(addr, &a, 8);
    memcpy(addr + 8, &b, 8);
    memcpy(addr + 16, &c, 4);
}

static void rng_bytes(rng_t *rng, uint8_t *buf, size_t len) {
    while (len >= 8) {
        uint64_t v = rng_next(rng);
        memcpy(buf, &v, 8);
        buf += 8; len -= 8;
    }
    if (len > 0) {
        uint64_t v = rng_next(rng);
        memcpy(buf, &v, len);
    }
}

#define VAL_DIR  "/tmp/bench_vthreads_val"
#define COMM_DIR "/tmp/bench_vthreads_comm"

static void cleanup(void) {
    system("rm -rf " VAL_DIR " " COMM_DIR);
}

/* =========================================================================
 * Run one benchmark pass
 * ========================================================================= */

typedef struct {
    double prefill_sec;
    double exec_sec;
    double total_commit_ms;
    uint32_t blocks_run;
} bench_result_t;

static bench_result_t run_bench(uint32_t target_accounts, uint32_t num_blocks,
                                 uint32_t ops_per_block, int num_threads,
                                 uint64_t seed) {
    bench_result_t result = {0};

    cleanup();
    mkdir(VAL_DIR, 0755);
    mkdir(COMM_DIR, 0755);

    verkle_state_t *vs = verkle_state_create_flat(VAL_DIR, COMM_DIR);
    if (!vs) return result;

    /* Enable thread pool if requested */
    if (num_threads > 0)
        verkle_flat_set_threads(verkle_state_get_flat(vs), num_threads);

    rng_t rng = { .state = seed };

    /* Prefill accounts */
    uint32_t addr_cap = target_accounts + num_blocks * 20;
    uint8_t (*addrs)[20] = malloc(addr_cap * 20);
    if (!addrs) { verkle_state_destroy(vs); return result; }
    uint32_t addr_count = 0;

    double t0 = now_sec();

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
    }

    result.prefill_sec = now_sec() - t0;

    /* Execute blocks */
    double t_exec = now_sec();

    for (uint32_t blk = 1; blk <= num_blocks; blk++) {
        uint64_t block_num = prefill_blocks + blk;
        verkle_state_begin_block(vs, block_num);

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
                verkle_state_set_nonce(vs, addrs[idx], nonce + 1);
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
                    uint32_t code_len = 256 + (uint32_t)(rng_next(&rng) % 1793);
                    uint8_t *code = malloc(code_len);
                    rng_bytes(&rng, code, code_len);
                    verkle_state_set_code(vs, addr, code, code_len);
                    free(code);
                }
            }
        }

        double t_commit = now_sec();
        verkle_state_commit_block(vs);
        result.total_commit_ms += (now_sec() - t_commit) * 1000.0;
        result.blocks_run++;
    }

    result.exec_sec = now_sec() - t_exec;

    free(addrs);
    verkle_state_destroy(vs);
    cleanup();

    return result;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    uint32_t accounts_k = 50;
    uint32_t num_blocks  = 200;
    int      num_threads = 4;

    if (argc > 1) accounts_k = (uint32_t)atoi(argv[1]);
    if (argc > 2) num_blocks  = (uint32_t)atoi(argv[2]);
    if (argc > 3) num_threads = atoi(argv[3]);

    uint32_t target_accounts = accounts_k * 1000;
    uint32_t ops_per_block = 500;
    uint64_t seed = 0xDEADBEEFCAFEBABEULL;

    printf("=== Verkle Thread Benchmark ===\n");
    printf("Accounts:    %uK\n", accounts_k);
    printf("Blocks:      %u\n", num_blocks);
    printf("Ops/block:   %u\n", ops_per_block);
    printf("Threads:     %d\n\n", num_threads);

    /* Run single-threaded */
    printf("--- Single-threaded ---\n");
    bench_result_t st = run_bench(target_accounts, num_blocks, ops_per_block,
                                   0, seed);
    printf("  Prefill:     %.1fs\n", st.prefill_sec);
    printf("  Execution:   %.1fs\n", st.exec_sec);
    printf("  Avg commit:  %.2fms\n", st.total_commit_ms / st.blocks_run);
    printf("  Blocks/sec:  %.1f\n\n",
           st.blocks_run / st.exec_sec);

    /* Run multi-threaded */
    printf("--- %d threads ---\n", num_threads);
    bench_result_t mt = run_bench(target_accounts, num_blocks, ops_per_block,
                                   num_threads, seed);
    printf("  Prefill:     %.1fs\n", mt.prefill_sec);
    printf("  Execution:   %.1fs\n", mt.exec_sec);
    printf("  Avg commit:  %.2fms\n", mt.total_commit_ms / mt.blocks_run);
    printf("  Blocks/sec:  %.1f\n\n",
           mt.blocks_run / mt.exec_sec);

    /* Comparison */
    printf("========================================\n");
    printf("  Speedup: %.2fx\n",
           st.exec_sec / mt.exec_sec);
    printf("  Commit speedup: %.2fx\n",
           (st.total_commit_ms / st.blocks_run) /
           (mt.total_commit_ms / mt.blocks_run));
    printf("========================================\n");

    return 0;
}
