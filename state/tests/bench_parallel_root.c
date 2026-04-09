/*
 * bench_parallel_root — verify parallel storage root computation matches serial,
 * and benchmark 1 vs N threads.
 *
 * Populates a state with random accounts and storage, runs invalidate_all +
 * compute_root with 1 thread, then N threads, compares roots and timing.
 *
 * Usage: bench_parallel_root [num_accounts] [slots_per_account] [threads]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "state.h"
#include "hash.h"

static uint64_t rng_state = 0xdeadbeef42ULL;
static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

static void rng_bytes(uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++)
        out[i] = (uint8_t)(rng_next() & 0xff);
}

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

int main(int argc, char **argv) {
    uint32_t num_accounts = 10000;
    uint32_t slots_per    = 10;
    uint32_t threads      = 4;

    if (argc > 1) num_accounts = (uint32_t)atoi(argv[1]);
    if (argc > 2) slots_per    = (uint32_t)atoi(argv[2]);
    if (argc > 3) threads      = (uint32_t)atoi(argv[3]);

    printf("bench_parallel_root: %u accounts, %u slots each, %u threads\n\n",
           num_accounts, slots_per, threads);

    /* Create state */
    state_t *s = state_create(NULL);
    if (!s) { fprintf(stderr, "state_create failed\n"); return 1; }

    /* Populate accounts with storage */
    printf("Populating state...\n");
    double t0 = now_ms();
    for (uint32_t i = 0; i < num_accounts; i++) {
        address_t addr;
        rng_bytes(addr.bytes, 20);
        state_create_account(s, &addr);
        state_set_nonce(s, &addr, i + 1);

        uint256_t bal = {{ (uint128_t)(i + 1) * 1000000, 0 }};
        state_set_balance(s, &addr, &bal);

        for (uint32_t j = 0; j < slots_per; j++) {
            uint256_t key = {{ (uint128_t)rng_next(), (uint128_t)rng_next() }};
            uint256_t val = {{ (uint128_t)rng_next(), (uint128_t)rng_next() }};
            state_set_storage(s, &addr, &key, &val);
        }

        /* Commit tx state to make storage originals visible */
        state_commit_tx(s);
    }
    double t1 = now_ms();
    printf("  populated in %.0f ms\n\n", t1 - t0);

    /* --- Serial run --- */
    printf("=== Serial (1 thread) ===\n");
    state_set_root_threads(s, 1);
    state_invalidate_all(s);

    double s0 = now_ms();
    hash_t serial_root = state_compute_root(s, true);
    double s1 = now_ms();

    char hex1[65];
    for (int i = 0; i < 32; i++) sprintf(hex1 + i * 2, "%02x", serial_root.bytes[i]);
    printf("  root: %s\n", hex1);
    printf("  time: %.1f ms\n\n", s1 - s0);

    /* --- Parallel run --- */
    printf("=== Parallel (%u threads) ===\n", threads);
    state_set_root_threads(s, threads);
    state_invalidate_all(s);

    double p0 = now_ms();
    hash_t parallel_root = state_compute_root(s, true);
    double p1 = now_ms();

    char hex2[65];
    for (int i = 0; i < 32; i++) sprintf(hex2 + i * 2, "%02x", parallel_root.bytes[i]);
    printf("  root: %s\n", hex2);
    printf("  time: %.1f ms\n\n", p1 - p0);

    /* --- Compare --- */
    bool match = memcmp(serial_root.bytes, parallel_root.bytes, 32) == 0;
    printf("Roots match: %s\n", match ? "YES" : "NO *** MISMATCH ***");
    if (s1 - s0 > 0)
        printf("Speedup: %.2fx\n", (s1 - s0) / (p1 - p0));

    state_destroy(s);
    return match ? 0 : 1;
}
