/**
 * Full-Stack Stress Test — exercises evm_state → flat_state → art_mpt
 * without actual EVM execution.
 *
 * Simulates chain_replay's exact checkpoint loop:
 *   for each block:
 *     commit()
 *     begin_block()
 *     set_nonce/set_balance/set_storage on random accounts
 *   every 256 blocks (checkpoint):
 *     compute_mpt_root()
 *     flush()
 *     evict_cache()
 *
 * Random accounts, random slots, random values. Infinite loop.
 * Fail-fast on LOST NODE (detected via commit stats).
 *
 * Usage:
 *   ./stress_fullstack [state_dir] [num_accounts] [slots_per_account]
 *   Default: /dev/shm/stress_fullstack, 100000 accounts, 50 slots
 */

#include "evm_state.h"
#include "flat_state.h"
#include "keccak256.h"
#include "hash.h"
#include "address.h"
#include "uint256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>

/* =========================================================================
 * RSS helper
 * ========================================================================= */

static size_t get_rss_kb(void) {
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    unsigned long pages = 0, rss = 0;
    fscanf(f, "%lu %lu", &pages, &rss);
    fclose(f);
    return rss * 4; /* pages → KB (4K pages) */
}

/* =========================================================================
 * RNG (xoshiro256**)
 * ========================================================================= */

static uint64_t rng_s[4];

static void rng_seed(uint64_t seed) {
    rng_s[0] = seed;
    rng_s[1] = seed ^ 0x6a09e667f3bcc908ULL;
    rng_s[2] = seed ^ 0xbb67ae8584caa73bULL;
    rng_s[3] = seed ^ 0x3c6ef372fe94f82bULL;
    for (int i = 0; i < 20; i++) {
        uint64_t t = rng_s[1] << 17;
        rng_s[2] ^= rng_s[0]; rng_s[3] ^= rng_s[1];
        rng_s[1] ^= rng_s[2]; rng_s[0] ^= rng_s[3];
        rng_s[2] ^= t;
        rng_s[3] = (rng_s[3] << 45) | (rng_s[3] >> 19);
    }
}

static uint64_t rng64(void) {
    uint64_t result = rng_s[1] * 5;
    result = ((result << 7) | (result >> 57)) * 9;
    uint64_t t = rng_s[1] << 17;
    rng_s[2] ^= rng_s[0]; rng_s[3] ^= rng_s[1];
    rng_s[1] ^= rng_s[2]; rng_s[0] ^= rng_s[3];
    rng_s[2] ^= t;
    rng_s[3] = (rng_s[3] << 45) | (rng_s[3] >> 19);
    return result;
}

static uint32_t rng32(uint32_t n) { return (uint32_t)(rng64() % n); }

/* =========================================================================
 * Generate random Ethereum-like data
 * ========================================================================= */

/* Pre-generate a pool of addresses to draw from */
#define ADDR_POOL_SIZE 200000
static address_t addr_pool[ADDR_POOL_SIZE];

static void init_addr_pool(uint32_t n) {
    if (n > ADDR_POOL_SIZE) n = ADDR_POOL_SIZE;
    for (uint32_t i = 0; i < n; i++) {
        /* Deterministic but realistic-looking addresses */
        SHA3_CTX ctx;
        keccak_init(&ctx);
        keccak_update(&ctx, (const uint8_t *)&i, sizeof(i));
        uint8_t h[32];
        keccak_final(&ctx, h);
        memcpy(addr_pool[i].bytes, h, 20);
    }
}

/* Generate a random uint256 slot index */
static void random_slot(uint256_t *slot) {
    uint8_t buf[32];
    memset(buf, 0, 32);
    /* Mix of small slots (ERC20-style 0-10) and large (mapping hashes) */
    if (rng32(3) == 0) {
        /* Small slot: 0-255 */
        buf[31] = (uint8_t)rng32(256);
    } else {
        /* Large slot: random 32-byte hash (simulates keccak of mapping key) */
        uint64_t r = rng64();
        SHA3_CTX ctx;
        keccak_init(&ctx);
        keccak_update(&ctx, (const uint8_t *)&r, 8);
        keccak_final(&ctx, buf);
    }
    *slot = uint256_from_bytes(buf, 32);
}

/* Generate a random storage value */
static void random_value(uint256_t *val) {
    uint8_t buf[32];
    memset(buf, 0, 32);
    uint32_t kind = rng32(5);
    switch (kind) {
        case 0: /* zero (delete) */
            break;
        case 1: /* small (counter, bool) */
            buf[31] = (uint8_t)(rng32(256) + 1);
            break;
        case 2: /* address-sized */
            for (int i = 12; i < 32; i++)
                buf[i] = (uint8_t)rng32(256);
            break;
        case 3: /* full uint256 */
            for (int i = 0; i < 32; i++)
                buf[i] = (uint8_t)rng32(256);
            break;
        default: /* medium */
            for (int i = 24; i < 32; i++)
                buf[i] = (uint8_t)rng32(256);
            break;
    }
    *val = uint256_from_bytes(buf, 32);
}

/* =========================================================================
 * Signal handling for graceful stop
 * ========================================================================= */

static volatile sig_atomic_t g_stop = 0;

static void sighandler(int sig) {
    (void)sig;
    g_stop = 1;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    const char *state_dir = "/dev/shm/stress_fullstack";
    uint32_t num_accounts = 100000;
    uint32_t max_slots    = 50;
    uint32_t checkpoint_interval = 256;

    if (argc >= 2) state_dir = argv[1];
    if (argc >= 3) num_accounts = (uint32_t)atoi(argv[2]);
    if (argc >= 4) max_slots = (uint32_t)atoi(argv[3]);

    if (num_accounts > ADDR_POOL_SIZE) num_accounts = ADDR_POOL_SIZE;

    signal(SIGINT, sighandler);
    signal(SIGTERM, sighandler);

    rng_seed(42);
    init_addr_pool(num_accounts);

    fprintf(stderr, "=== Full-Stack MPT Stress Test ===\n");
    fprintf(stderr, "  dir=%s  accounts=%u  max_slots=%u  interval=%u\n",
            state_dir, num_accounts, max_slots, checkpoint_interval);

    /* Create directories */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", state_dir, state_dir);
    if (system(cmd) != 0) {
        fprintf(stderr, "FAIL: mkdir %s\n", state_dir);
        return 1;
    }

    /* MPT path */
    char mpt_path[512];
    snprintf(mpt_path, sizeof(mpt_path), "%s/account_mpt", state_dir);

    /* Create evm_state with MPT stores */
    evm_state_t *es = evm_state_create(NULL, mpt_path, NULL);
    if (!es) {
        fprintf(stderr, "FAIL: evm_state_create\n");
        return 1;
    }

    /* Create flat_state — THIS IS CRITICAL for the LOST NODE bug.
     * Without flat_state, evicted accounts restart with EMPTY_ROOT,
     * so old storage roots are never reloaded after eviction.
     * With flat_state, storage_root is persisted across evictions,
     * and the bug manifests when a shared root gets deleted. */
    char flat_path[512];
    snprintf(flat_path, sizeof(flat_path), "%s/flat", state_dir);
    flat_state_t *fs = flat_state_create(flat_path, 500000, 5000000);
    if (!fs) {
        fprintf(stderr, "FAIL: flat_state_create\n");
        evm_state_destroy(es);
        return 1;
    }
    evm_state_set_flat_state(es, fs);

    evm_state_set_batch_mode(es, true);

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint64_t block = 1;
    uint64_t total_updates = 0;
    uint64_t total_lost = 0;
    uint32_t live_accounts = 0;

    fprintf(stderr, "\nRunning... (Ctrl-C to stop)\n\n");

    while (!g_stop) {
        /* --- Per-block: commit + begin_block + apply random diffs --- */
        evm_state_commit(es);
        evm_state_begin_block(es, block);

        /* How many accounts touched this block?
         * Early blocks create accounts; later blocks update existing ones. */
        uint32_t accounts_per_block;
        if (live_accounts < num_accounts) {
            /* Ramp up: create new accounts quickly */
            uint32_t new_accts = 200 + rng32(300);
            if (live_accounts + new_accts > num_accounts)
                new_accts = num_accounts - live_accounts;

            for (uint32_t i = 0; i < new_accts; i++) {
                uint32_t a = live_accounts + i;
                address_t *addr = &addr_pool[a];

                /* Create account with nonce + balance */
                evm_state_set_nonce(es, addr, 1 + rng32(100));
                uint256_t bal;
                random_value(&bal);
                /* Ensure non-zero */
                if (uint256_is_zero(&bal)) bal = UINT256_ONE;
                evm_state_set_balance(es, addr, &bal);

                /* Initialize 1-max_slots storage slots */
                uint32_t n_slots = 1 + rng32(max_slots);
                for (uint32_t s = 0; s < n_slots; s++) {
                    uint256_t slot, val;
                    random_slot(&slot);
                    random_value(&val);
                    if (uint256_is_zero(&val)) val = UINT256_ONE;
                    evm_state_set_storage(es, addr, &slot, &val);
                    total_updates++;
                }
            }
            live_accounts += new_accts;

            /* Also update some existing */
            accounts_per_block = live_accounts / 20;
            if (accounts_per_block < 5) accounts_per_block = 5;
        } else {
            /* Steady state: ~5-10% of accounts touched per block */
            accounts_per_block = num_accounts / 20 + rng32(num_accounts / 20);
            if (accounts_per_block > live_accounts)
                accounts_per_block = live_accounts;
        }

        /* Update random existing accounts.
         * Real Ethereum: ~150-300 accounts/block, 1-3 slots each.
         * Keep it realistic so checkpoints are fast → more iterations. */
        uint32_t touch_count = 150 + rng32(150);
        if (touch_count > live_accounts) touch_count = live_accounts;

        for (uint32_t t = 0; t < touch_count; t++) {
            uint32_t a = rng32(live_accounts);
            address_t *addr = &addr_pool[a];

            /* 30% chance: update nonce (simulates tx sender) */
            if (rng32(10) < 3) {
                uint64_t n = evm_state_get_nonce(es, addr);
                evm_state_set_nonce(es, addr, n + 1);
            }

            /* 20% chance: update balance */
            if (rng32(5) == 0) {
                uint256_t bal;
                random_value(&bal);
                evm_state_set_balance(es, addr, &bal);
            }

            /* 1-3 storage slot updates */
            uint32_t n_slots = 1 + rng32(3);
            for (uint32_t s = 0; s < n_slots; s++) {
                uint256_t slot, val;
                random_slot(&slot);
                random_value(&val); /* can be zero = delete */
                evm_state_set_storage(es, addr, &slot, &val);
                total_updates++;
            }
        }

        /* --- Checkpoint every N blocks --- */
        if (block % checkpoint_interval == 0) {
            bool prune = (block >= 2675000);
            hash_t root = evm_state_compute_mpt_root(es, prune);

            /* No LOST NODE check needed — art_mpt computes from compact_art directly */

            evm_state_flush(es);
            evm_state_evict_cache(es);

            /* Progress */
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            double elapsed = (now.tv_sec - t0.tv_sec) +
                             (now.tv_nsec - t0.tv_nsec) / 1e9;
            double bps = block / elapsed;

            evm_state_stats_t ss = evm_state_get_stats(es);

            fprintf(stderr,
                "  block %lu: live=%u updates=%lu root=0x%02x%02x..%02x%02x "
                "%.0f blk/s (%.1fs)\n",
                (unsigned long)block, live_accounts,
                (unsigned long)total_updates,
                root.bytes[0], root.bytes[1],
                root.bytes[30], root.bytes[31],
                bps, elapsed);
            fprintf(stderr,
                "    flat: %luK accts (%zuMB), %luK slots (%zuMB) | "
                "root: stor=%.1fms acct=%.1fms (%zu dirty) | RSS %zuMB\n",
                ss.flat_acct_count / 1000,
                ss.flat_acct_mem / (1024*1024),
                ss.flat_stor_count / 1000,
                ss.flat_stor_mem / (1024*1024),
                ss.root_stor_ms, ss.root_acct_ms,
                ss.root_dirty_count,
                get_rss_kb() / 1024);
        }

        block++;
    }

    /* Cleanup */
    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);

    struct timespec tend;
    clock_gettime(CLOCK_MONOTONIC, &tend);
    double elapsed = (tend.tv_sec - t0.tv_sec) + (tend.tv_nsec - t0.tv_nsec) / 1e9;

    fprintf(stderr, "\n=== %s — %lu blocks, %lu updates, %lu LOST NODEs (%.1fs) ===\n",
            total_lost > 0 ? "FAIL" : "STOPPED",
            (unsigned long)block - 1,
            (unsigned long)total_updates,
            (unsigned long)total_lost,
            elapsed);

    /* Cleanup files */
    snprintf(cmd, sizeof(cmd), "rm -rf %s", state_dir);
    system(cmd);

    return total_lost > 0 ? 1 : 0;
}
