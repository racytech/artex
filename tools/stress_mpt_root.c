/**
 * stress_mpt_root — Reproduce slow MPT root computation.
 *
 * Simulates the chain_replay checkpoint cycle:
 *   1. Create evm_state with MPT
 *   2. Populate N accounts (simulating accumulated cache)
 *   3. Compute MPT root (stages + commit_batch)
 *   4. Optionally evict cache and repeat
 *
 * Measures time breakdown: staging loop vs commit_batch vs total.
 *
 * Usage:
 *   stress_mpt_root [options]
 *
 * Options:
 *   --accounts <n>    Total unique accounts to create (default: 400000)
 *   --blocks <n>      Blocks per checkpoint interval (default: 256)
 *   --txs-per-block <n>  Txs per block (default: 8)
 *   --no-evict        Skip cache eviction between checkpoints
 *   --checkpoints <n> Number of checkpoints to run (default: 3)
 */

#include "evm_state.h"
#include "uint256.h"
#include "hash.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <getopt.h>

#ifdef ENABLE_DEBUG
bool g_trace_calls = false;
#endif

/* =========================================================================
 * Helpers
 * ========================================================================= */

static double elapsed_ms(struct timespec *t0, struct timespec *t1) {
    return (t1->tv_sec - t0->tv_sec) * 1000.0 +
           (t1->tv_nsec - t0->tv_nsec) / 1e6;
}

static address_t make_addr(uint32_t id) {
    address_t a;
    memset(a.bytes, 0, 20);
    /* Spread across address space for realistic hash distribution */
    a.bytes[0] = (uint8_t)(id >> 24);
    a.bytes[1] = (uint8_t)(id >> 16);
    a.bytes[2] = (uint8_t)(id >> 8);
    a.bytes[3] = (uint8_t)(id);
    /* Add some entropy in the middle */
    a.bytes[10] = (uint8_t)(id * 7);
    a.bytes[11] = (uint8_t)(id * 13);
    return a;
}

static void cleanup_files(const char *base) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -f %s.idx %s.dat %s.free "
             "%s_storage.idx %s_storage.dat %s_storage.free "
             "%s_flat_acct.idx %s_flat_stor.idx 2>/dev/null",
             base, base, base,
             base, base, base,
             base, base);
    (void)system(cmd);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    uint32_t total_accounts = 400000;
    uint32_t blocks_per_ckpt = 256;
    uint32_t txs_per_block = 8;
    bool do_evict = true;
    bool dirty_all = false;
    int num_checkpoints = 3;
    const char *base_path = "/tmp/stress_mpt_root";

    /* Parse args */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--accounts") == 0 && i + 1 < argc)
            total_accounts = (uint32_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--blocks") == 0 && i + 1 < argc)
            blocks_per_ckpt = (uint32_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--txs-per-block") == 0 && i + 1 < argc)
            txs_per_block = (uint32_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--no-evict") == 0)
            do_evict = false;
        else if (strcmp(argv[i], "--dirty-all") == 0)
            dirty_all = true;
        else if (strcmp(argv[i], "--checkpoints") == 0 && i + 1 < argc)
            num_checkpoints = atoi(argv[++i]);
        else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [--accounts N] [--blocks N] [--txs-per-block N] "
                   "[--no-evict] [--dirty-all] [--checkpoints N]\n", argv[0]);
            return 0;
        }
    }

    printf("stress_mpt_root configuration:\n");
    printf("  accounts:       %u\n", total_accounts);
    printf("  blocks/ckpt:    %u\n", blocks_per_ckpt);
    printf("  txs/block:      %u\n", txs_per_block);
    printf("  evict:          %s\n", do_evict ? "yes" : "no");
    printf("  dirty_all:      %s\n", dirty_all ? "yes" : "no");
    printf("  checkpoints:    %d\n", num_checkpoints);
    printf("\n");

#ifndef ENABLE_MPT
    fprintf(stderr, "ERROR: ENABLE_MPT is required for this test\n");
    return 1;
#else
    /* Clean up from previous run */
    cleanup_files(base_path);

    /* Create evm_state with MPT */
    evm_state_t *es = evm_state_create(NULL, base_path, NULL);
    if (!es) {
        fprintf(stderr, "Failed to create evm_state\n");
        return 1;
    }

    /* =====================================================================
     * Phase 1: Populate accounts (simulate history up to block N)
     *
     * Create total_accounts unique accounts across multiple blocks,
     * committing periodically. This builds up the MPT to a realistic
     * size before we start measuring checkpoint performance.
     * ===================================================================== */

    printf("Phase 1: Populating %u accounts...\n", total_accounts);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint32_t accounts_per_block = total_accounts / blocks_per_ckpt;
    if (accounts_per_block == 0) accounts_per_block = 1;
    uint32_t acct_id = 0;
    uint64_t block_num = 1;

    /* Create accounts in blocks, with checkpoint every blocks_per_ckpt */
    while (acct_id < total_accounts) {
        evm_state_begin_block(es, block_num);

        uint32_t this_batch = accounts_per_block;
        if (acct_id + this_batch > total_accounts)
            this_batch = total_accounts - acct_id;

        for (uint32_t i = 0; i < this_batch; i++) {
            address_t addr = make_addr(acct_id);
            uint256_t bal = uint256_from_uint64((uint64_t)(acct_id + 1) * 1000);
            evm_state_set_balance(es, &addr, &bal);
            if (acct_id % 3 == 0)
                evm_state_set_nonce(es, &addr, acct_id + 1);
            acct_id++;
        }

        evm_state_commit_tx(es);
        evm_state_finalize(es);
        evm_state_commit(es);

        /* Checkpoint every blocks_per_ckpt blocks */
        if (block_num % blocks_per_ckpt == 0) {
            hash_t root = evm_state_compute_mpt_root(es, false);
            (void)root;
            if (do_evict)
                evm_state_evict_cache(es);
        }

        block_num++;
    }

    /* Final checkpoint for population phase */
    {
        hash_t root = evm_state_compute_mpt_root(es, false);
        printf("  Population root: 0x");
        for (int i = 0; i < 8; i++) printf("%02x", root.bytes[i]);
        printf("...\n");
        if (do_evict)
            evm_state_evict_cache(es);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  Done in %.1f ms (block %lu)\n\n", elapsed_ms(&t0, &t1), block_num - 1);

    /* =====================================================================
     * Phase 2: Simulate checkpoint cycles
     *
     * Each checkpoint: execute blocks_per_ckpt blocks with txs_per_block
     * transactions each (touching existing accounts), then compute root.
     * This is the hot path we're profiling.
     * ===================================================================== */

    printf("Phase 2: Running %d checkpoint cycles (%u blocks × %u txs each)...\n\n",
           num_checkpoints, blocks_per_ckpt, txs_per_block);

    for (int ckpt = 0; ckpt < num_checkpoints; ckpt++) {
        struct timespec t_exec0, t_exec1, t_root0, t_root1, t_evict0, t_evict1;

        /* Execute blocks */
        clock_gettime(CLOCK_MONOTONIC, &t_exec0);
        for (uint32_t b = 0; b < blocks_per_ckpt; b++) {
            evm_state_begin_block(es, block_num);

            for (uint32_t tx = 0; tx < txs_per_block; tx++) {
                /* Touch existing accounts: sender and recipient */
                uint32_t sender_id = (block_num * txs_per_block + tx) % total_accounts;
                uint32_t recip_id = (sender_id + 1 + tx) % total_accounts;

                address_t sender = make_addr(sender_id);
                address_t recip = make_addr(recip_id);

                /* Debit sender, credit recipient */
                uint256_t debit = uint256_from_uint64(100);
                uint256_t credit = uint256_from_uint64(100);
                uint256_t sender_bal = evm_state_get_balance(es, &sender);
                uint256_t recip_bal = evm_state_get_balance(es, &recip);
                sender_bal = uint256_add(&sender_bal, &debit);
                recip_bal = uint256_add(&recip_bal, &credit);
                evm_state_set_balance(es, &sender, &sender_bal);
                evm_state_set_balance(es, &recip, &recip_bal);

                /* Bump sender nonce */
                uint64_t nonce = evm_state_get_nonce(es, &sender);
                evm_state_set_nonce(es, &sender, nonce + 1);
            }

            evm_state_commit_tx(es);
            evm_state_finalize(es);
            evm_state_commit(es);
            block_num++;
        }
        clock_gettime(CLOCK_MONOTONIC, &t_exec1);

        /* --dirty-all: touch every account to simulate no-eviction scenario */
        if (dirty_all) {
            evm_state_begin_block(es, block_num);
            for (uint32_t i = 0; i < total_accounts; i++) {
                address_t addr = make_addr(i);
                uint64_t nonce = evm_state_get_nonce(es, &addr);
                evm_state_set_nonce(es, &addr, nonce + 1);
            }
            evm_state_commit_tx(es);
            evm_state_finalize(es);
            evm_state_commit(es);
            block_num++;
        }

        /* Compute MPT root (the hot path) */
        clock_gettime(CLOCK_MONOTONIC, &t_root0);
        hash_t root = evm_state_compute_mpt_root(es, false);
        clock_gettime(CLOCK_MONOTONIC, &t_root1);

        /* Get commit stats (filled by compute_mpt_root internally) */
        evm_state_stats_t stats = evm_state_get_stats(es);
        mpt_commit_stats_t ac = stats.acct_commit;

        /* Evict cache */
        clock_gettime(CLOCK_MONOTONIC, &t_evict0);
        if (do_evict)
            evm_state_evict_cache(es);
        clock_gettime(CLOCK_MONOTONIC, &t_evict1);

        /* Report */
        double exec_ms = elapsed_ms(&t_exec0, &t_exec1);
        double root_ms = elapsed_ms(&t_root0, &t_root1);
        double evict_ms = elapsed_ms(&t_evict0, &t_evict1);

        printf("Checkpoint %d (block %lu):\n", ckpt + 1, block_num);
        printf("  exec:   %.1f ms (%u blocks × %u txs)\n",
               exec_ms, blocks_per_ckpt, txs_per_block);
        printf("  root:   %.1f ms\n", root_ms);
        printf("    └ acct commit: keccak=%.1f  load=%.1f  check=%.1f  del=%.1f  "
               "enc=%.1f  sort=%.1f ms\n",
               ac.keccak_ns / 1e6, ac.load_ns / 1e6,
               ac.check_ns / 1e6, ac.delete_ns / 1e6,
               ac.encode_ns / 1e6, ac.sort_ns / 1e6);
        printf("    └ nodes=%u  loaded=%u (cache=%u disk=%u)  chk_hit=%u  del=%u\n",
               ac.nodes_hashed, ac.nodes_loaded,
               ac.load_cache_hits, ac.load_disk_reads,
               ac.check_hits, ac.deletes);
        printf("  evict:  %.1f ms\n", evict_ms);
        printf("  root:   0x");
        for (int i = 0; i < 8; i++) printf("%02x", root.bytes[i]);
        printf("...\n\n");
    }

    /* Cleanup */
    evm_state_destroy(es);
    cleanup_files(base_path);

    return 0;
#endif /* ENABLE_MPT */
}
