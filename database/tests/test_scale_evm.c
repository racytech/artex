/**
 * Full-Stack EVM Scale Test
 *
 * Simulates realistic Ethereum block execution through the entire pipeline:
 *   evm_state ops → compute_state_root → evm_state_finalize → sdb_commit_block
 *
 * Usage: ./test_scale_evm <blocks_K> [commit_interval]
 *   blocks_K:        number of blocks in thousands (e.g. 10 = 10K blocks)
 *   commit_interval: sdb_checkpoint every N committed blocks (default: 1000)
 *
 * Per-block simulation (based on mainnet averages, post-Shanghai):
 *   ~150 transactions per block
 *   Each tx follows the real EVM execution pattern:
 *     1. Nonce validation (get_nonce)
 *     2. EIP-2929 access list warm-up (sender, receiver, coinbase)
 *     3. Fee deduction (get_balance → sub_balance)
 *     4. Nonce increment
 *     5. Value transfer (sub_balance → add_balance + exists check)
 *     6. Contract execution: SLOAD→SSTORE pairs, code reads (CALL/EXTCODEHASH)
 *     7. Gas refund to sender + fee to coinbase
 *     8. Snapshot/revert for ~8% failed txs
 *   ~33% of txs interact with contract storage (5 SLOAD+SSTORE pairs)
 *   ~5% of txs read callee code (get_code for CALL simulation)
 *   ~1% of txs deploy code (256-24576 bytes)
 *   ~0.05% of contract txs self-destruct
 *
 * Read:write ratio ~4:1 (matching real mainnet)
 */

#include "evm_state.h"
#include "state_db.h"
#include "../../common/include/uint256.h"
#include "../../common/include/hash.h"
#include "../../common/include/address.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

// ============================================================================
// Configuration
// ============================================================================

#define TXS_PER_BLOCK       150
#define STORAGE_SLOTS_PER   5     // SLOAD+SSTORE pairs per contract tx
#define CONTRACT_TX_RATE    3     // 1 in N txs touches storage
#define CODE_READ_RATE      20    // 1 in N txs reads callee code (CALL)
#define CODE_DEPLOY_RATE    100   // 1 in N txs deploys code
#define SELFDESTRUCT_RATE   2000  // 1 in N txs self-destructs
#define TX_REVERT_RATE      12    // 1 in N txs reverts (~8% failure rate)

#define CODE_SIZE_MIN       256
#define CODE_SIZE_MAX       24576 // EIP-170 max contract size

#define TEST_DIR            "/tmp/test_scale_evm"
#define STATE_MPT_PATH      "/tmp/test_scale_evm_state.mpt"
#define STORAGE_MPT_PATH    "/tmp/test_scale_evm_storage.mpt"

// ============================================================================
// RNG (SplitMix64 — same as other scale tests)
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

// ============================================================================
// Utilities
// ============================================================================

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

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

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

// ============================================================================
// Address / slot generation
//
// Addresses are generated with a skewed distribution:
//   - 20% of txs hit "hot" addresses (first 100 — exchanges, routers)
//   - 80% hit addresses from an expanding pool (simulates account growth)
// ============================================================================

static void make_address(address_t *out, uint32_t id) {
    memset(out->bytes, 0, ADDRESS_SIZE);
    out->bytes[16] = (uint8_t)(id >> 24);
    out->bytes[17] = (uint8_t)(id >> 16);
    out->bytes[18] = (uint8_t)(id >> 8);
    out->bytes[19] = (uint8_t)(id);
}

static uint32_t pick_address_id(rng_t *rng, uint32_t block) {
    uint32_t pool_size = 1000 + block * 5;  // pool grows ~5 new addrs/block
    if (pool_size > 500000) pool_size = 500000;

    // 20% hot (first 100), 80% from full pool
    if (rng_next(rng) % 5 == 0) {
        return rng_next(rng) % 100;
    }
    return rng_next(rng) % pool_size;
}

// ============================================================================
// Counters
// ============================================================================

typedef struct {
    uint64_t nonce_reads;
    uint64_t balance_reads;
    uint64_t balance_writes;
    uint64_t storage_reads;
    uint64_t storage_writes;
    uint64_t code_reads;
    uint64_t code_deploys;
    uint64_t exist_checks;
    uint64_t warm_ops;
    uint64_t snapshots;
    uint64_t reverts;
    uint64_t selfdestructs;
} block_counters_t;

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <blocks_K> [commit_interval]\n", argv[0]);
        fprintf(stderr, "  blocks_K:        blocks in thousands (e.g. 10 = 10K)\n");
        fprintf(stderr, "  commit_interval: sdb_checkpoint every N blocks (default: 1000)\n");
        return 1;
    }

    uint32_t blocks_k = (uint32_t)atoi(argv[1]);
    if (blocks_k == 0) blocks_k = 1;
    uint32_t total_blocks = blocks_k * 1000;
    uint32_t commit_interval = argc > 2 ? (uint32_t)atoi(argv[2]) : 1000;
    if (commit_interval == 0) commit_interval = 1000;

    rng_t rng = rng_create(0xE0B5CA1E00000000ULL);

    // --- Setup ---
    (void)system("rm -rf " TEST_DIR);
    unlink(STATE_MPT_PATH);
    unlink(STORAGE_MPT_PATH);

    state_db_t *sdb = sdb_create(TEST_DIR);
    if (!sdb) {
        fprintf(stderr, "FAIL: sdb_create\n");
        return 1;
    }

    // Latency arrays
    double *exec_ms   = malloc(total_blocks * sizeof(double));
    double *root_ms   = malloc(total_blocks * sizeof(double));
    double *commit_ms = malloc(total_blocks * sizeof(double));
    if (!exec_ms || !root_ms || !commit_ms) {
        fprintf(stderr, "FAIL: malloc latency arrays\n");
        return 1;
    }

    // Code buffer for deploys (reused across blocks)
    uint8_t *code_buf = malloc(CODE_SIZE_MAX);
    if (!code_buf) {
        fprintf(stderr, "FAIL: malloc code_buf\n");
        return 1;
    }

    // Cumulative counters
    block_counters_t total = {0};

    // Coinbase address (fixed per "epoch", rotates every 32 blocks)
    address_t coinbase;
    make_address(&coinbase, 0xFFFF0000);

    // --- Header ---
    printf("============================================\n");
    printf("  Full-Stack EVM Scale Test\n");
    printf("============================================\n");
    printf("  blocks:           %uK (%u total)\n", blocks_k, total_blocks);
    printf("  commit_interval:  %u blocks\n", commit_interval);
    printf("  txs/block:        %d\n", TXS_PER_BLOCK);
    printf("  storage tx rate:  1/%d (~%d%%)\n", CONTRACT_TX_RATE, 100/CONTRACT_TX_RATE);
    printf("  code read rate:   1/%d (~%d%%)\n", CODE_READ_RATE, 100/CODE_READ_RATE);
    printf("  deploy rate:      1/%d (~%d%%)\n", CODE_DEPLOY_RATE, 100/CODE_DEPLOY_RATE);
    printf("  revert rate:      1/%d (~%d%%)\n", TX_REVERT_RATE, 100/TX_REVERT_RATE);
    printf("============================================\n\n");
    fflush(stdout);

    // Progress interval: print ~20 lines regardless of total blocks
    uint32_t progress_interval = total_blocks / 20;
    if (progress_interval == 0) progress_interval = 1;

    double t_total_start = now_sec();

    // --- Block loop ---
    for (uint32_t block = 0; block < total_blocks; block++) {

        double t_exec_start = now_sec();

        // 1. Begin block (undo log)
        if (!sdb_begin_block(sdb)) {
            fprintf(stderr, "FAIL: sdb_begin_block at block %u\n", block);
            return 1;
        }

        // 2. Create evm_state for this block (persistent MPT for incremental roots)
        evm_state_t *es = evm_state_create(sdb, STATE_MPT_PATH, STORAGE_MPT_PATH);
        if (!es) {
            fprintf(stderr, "FAIL: evm_state_create at block %u\n", block);
            return 1;
        }

        // Rotate coinbase every 32 blocks (simulates different validators)
        if (block % 32 == 0) {
            make_address(&coinbase, 0xFFFF0000 + block / 32);
        }

        block_counters_t bc = {0};

        // 3. Simulate transactions
        for (uint32_t tx = 0; tx < TXS_PER_BLOCK; tx++) {
            uint32_t sender_id   = pick_address_id(&rng, block);
            uint32_t receiver_id = pick_address_id(&rng, block);

            address_t sender, receiver;
            make_address(&sender, sender_id);
            make_address(&receiver, receiver_id);

            // --- Snapshot (every tx is wrapped — reverted if tx fails) ---
            uint32_t snap = evm_state_snapshot(es);
            bc.snapshots++;

            // --- EIP-2929: warm sender, receiver, coinbase ---
            evm_state_warm_address(es, &sender);
            evm_state_warm_address(es, &receiver);
            evm_state_warm_address(es, &coinbase);
            bc.warm_ops += 3;

            // --- Nonce validation: read current nonce ---
            evm_state_get_nonce(es, &sender);
            bc.nonce_reads++;

            // --- Fee deduction: read balance, sub max fee ---
            evm_state_get_balance(es, &sender);
            bc.balance_reads++;

            uint256_t gas_fee = uint256_from_uint64(21000 + (rng_next(&rng) % 500000));
            // Initial fund: ensure sender always has balance (add before sub)
            uint256_t fund = uint256_from_uint64(10000000);
            evm_state_add_balance(es, &sender, &fund);
            evm_state_sub_balance(es, &sender, &gas_fee);
            bc.balance_writes += 2;

            // --- Nonce increment ---
            evm_state_increment_nonce(es, &sender);

            // --- Value transfer ---
            evm_state_exists(es, &receiver);
            bc.exist_checks++;

            uint256_t value = uint256_from_uint64(1000 + (rng_next(&rng) % 1000000));
            evm_state_get_balance(es, &sender);
            bc.balance_reads++;
            evm_state_sub_balance(es, &sender, &value);
            evm_state_add_balance(es, &receiver, &value);
            bc.balance_writes += 2;

            // --- Contract storage interaction (~33% of txs) ---
            // Realistic: SLOAD before SSTORE (read-modify-write)
            if (rng_next(&rng) % CONTRACT_TX_RATE == 0) {
                for (uint32_t s = 0; s < STORAGE_SLOTS_PER; s++) {
                    uint256_t slot = uint256_from_uint64(rng_next(&rng) % 10000);

                    // EIP-2929: check warm, then mark warm
                    evm_state_is_slot_warm(es, &receiver, &slot);
                    evm_state_warm_slot(es, &receiver, &slot);
                    bc.warm_ops += 2;

                    // SLOAD (read current value)
                    evm_state_get_storage(es, &receiver, &slot);
                    bc.storage_reads++;

                    // SSTORE (write new value)
                    uint256_t val = uint256_from_uint64(rng_next(&rng));
                    evm_state_set_storage(es, &receiver, &slot, &val);
                    bc.storage_writes++;
                }
            }

            // --- Code read for CALL simulation (~5% of txs) ---
            if (rng_next(&rng) % CODE_READ_RATE == 0) {
                evm_state_is_address_warm(es, &receiver);
                evm_state_warm_address(es, &receiver);
                bc.warm_ops += 2;

                evm_state_get_code_hash(es, &receiver);
                evm_state_get_code_size(es, &receiver);
                bc.code_reads += 2;

                // ~50% of code reads actually load the bytecode
                if (rng_next(&rng) % 2 == 0) {
                    uint8_t buf[CODE_SIZE_MAX];
                    uint32_t len = 0;
                    evm_state_get_code(es, &receiver, buf, &len);
                    bc.code_reads++;
                }
            }

            // --- Code deploy (~1% of txs) ---
            if (rng_next(&rng) % CODE_DEPLOY_RATE == 0) {
                address_t contract;
                // New contract address derived from sender + nonce
                uint32_t contract_id = sender_id * 1000 + (uint32_t)(rng_next(&rng) % 100000);
                make_address(&contract, contract_id);

                evm_state_exists(es, &contract);
                evm_state_is_empty(es, &contract);
                bc.exist_checks += 2;

                evm_state_create_account(es, &contract);
                evm_state_increment_nonce(es, &contract); // EIP-161: nonce starts at 1

                // Variable code size (256 — 24576 bytes)
                uint32_t code_len = CODE_SIZE_MIN +
                    (uint32_t)(rng_next(&rng) % (CODE_SIZE_MAX - CODE_SIZE_MIN));
                for (uint32_t i = 0; i < code_len; i += 8) {
                    uint64_t r = rng_next(&rng);
                    uint32_t remain = code_len - i;
                    memcpy(code_buf + i, &r, remain < 8 ? remain : 8);
                }
                evm_state_set_code(es, &contract, code_buf, code_len);
                bc.code_deploys++;
            }

            // --- Self-destruct (~0.05% of txs) ---
            if (rng_next(&rng) % SELFDESTRUCT_RATE == 0) {
                address_t beneficiary;
                make_address(&beneficiary, pick_address_id(&rng, block));

                uint256_t dying_bal = evm_state_get_balance(es, &receiver);
                bc.balance_reads++;
                evm_state_add_balance(es, &beneficiary, &dying_bal);
                bc.balance_writes++;

                evm_state_self_destruct(es, &receiver);
                bc.selfdestructs++;
            }

            // --- Gas refund + coinbase fee ---
            uint256_t refund = uint256_from_uint64(rng_next(&rng) % 50000);
            evm_state_add_balance(es, &sender, &refund);
            uint256_t coinbase_fee = uint256_from_uint64(21000 + (rng_next(&rng) % 100000));
            evm_state_add_balance(es, &coinbase, &coinbase_fee);
            bc.balance_writes += 2;

            // --- Revert ~8% of txs (simulates failed transactions) ---
            if (rng_next(&rng) % TX_REVERT_RATE == 0) {
                evm_state_revert(es, snap);
                bc.reverts++;
                // After revert, still increment nonce and charge gas (as real EVM does)
                evm_state_increment_nonce(es, &sender);
                evm_state_sub_balance(es, &sender, &gas_fee);
                bc.balance_writes++;
            }
        }

        double t_exec_end = now_sec();

        // 4. Compute state root (incremental — dirty paths only)
        hash_t root = evm_state_compute_state_root(es);
        (void)root;

        double t_root_end = now_sec();

        // 5. Finalize: flush dirty state to sdb (buffered)
        if (!evm_state_finalize(es)) {
            fprintf(stderr, "FAIL: evm_state_finalize at block %u\n", block);
            return 1;
        }

        // 6. Destroy evm_state
        evm_state_destroy(es);

        // 7. Commit block (undo log → apply → msync → commit)
        if (!sdb_commit_block(sdb)) {
            fprintf(stderr, "FAIL: sdb_commit_block at block %u\n", block);
            return 1;
        }

        double t_commit_end = now_sec();

        exec_ms[block]   = (t_exec_end - t_exec_start) * 1000.0;
        root_ms[block]   = (t_root_end - t_exec_end) * 1000.0;
        commit_ms[block] = (t_commit_end - t_root_end) * 1000.0;

        // Accumulate counters
        total.nonce_reads    += bc.nonce_reads;
        total.balance_reads  += bc.balance_reads;
        total.balance_writes += bc.balance_writes;
        total.storage_reads  += bc.storage_reads;
        total.storage_writes += bc.storage_writes;
        total.code_reads     += bc.code_reads;
        total.code_deploys   += bc.code_deploys;
        total.exist_checks   += bc.exist_checks;
        total.warm_ops       += bc.warm_ops;
        total.snapshots      += bc.snapshots;
        total.reverts        += bc.reverts;
        total.selfdestructs  += bc.selfdestructs;

        // Progress (also print at checkpoint boundaries so they always have context)
        if ((block + 1) % progress_interval == 0 ||
            (block + 1) % commit_interval == 0 ||
            block == total_blocks - 1) {
            sdb_stats_t stats = sdb_stats(sdb);
            printf("block %6u | exec %5.1fms | root %6.1fms | commit %5.1fms | "
                   "accts %7" PRIu64 " | stor %8" PRIu64 " | "
                   "code %4u | RSS %4zu MB\n",
                   block + 1,
                   exec_ms[block], root_ms[block], commit_ms[block],
                   stats.account_keys, stats.storage_keys,
                   stats.code_count, get_rss_mb());
            fflush(stdout);
        }

        // Checkpoint
        if ((block + 1) % commit_interval == 0) {
            double t0 = now_sec();
            sdb_checkpoint(sdb);
            double t1 = now_sec();
            printf("  CHECKPOINT at block %u: %.1fms\n", block + 1, (t1 - t0) * 1000.0);
            fflush(stdout);
        }
    }

    double t_total = now_sec() - t_total_start;

    // --- Percentiles ---
    qsort(exec_ms,   total_blocks, sizeof(double), cmp_double);
    qsort(root_ms,   total_blocks, sizeof(double), cmp_double);
    qsort(commit_ms, total_blocks, sizeof(double), cmp_double);

    sdb_stats_t final_stats = sdb_stats(sdb);

    uint64_t total_reads = total.nonce_reads + total.balance_reads +
                           total.storage_reads + total.code_reads + total.exist_checks;
    uint64_t total_writes = total.balance_writes + total.storage_writes + total.code_deploys;

    printf("\n============================================================\n");
    printf("  Results (%u blocks)\n", total_blocks);
    printf("============================================================\n");
    printf("  total time:       %.1fs\n", t_total);
    printf("  blocks/sec:       %.0f\n", total_blocks / t_total);
    printf("  ------------------------------------------------------------\n");
    printf("  Operation breakdown:\n");
    printf("    nonce reads:    %10" PRIu64 "\n", total.nonce_reads);
    printf("    balance reads:  %10" PRIu64 "\n", total.balance_reads);
    printf("    balance writes: %10" PRIu64 "\n", total.balance_writes);
    printf("    storage reads:  %10" PRIu64 "  (SLOAD)\n", total.storage_reads);
    printf("    storage writes: %10" PRIu64 "  (SSTORE)\n", total.storage_writes);
    printf("    code reads:     %10" PRIu64 "  (EXTCODEHASH/SIZE/COPY)\n", total.code_reads);
    printf("    code deploys:   %10" PRIu64 "\n", total.code_deploys);
    printf("    exist checks:   %10" PRIu64 "\n", total.exist_checks);
    printf("    warm ops:       %10" PRIu64 "  (EIP-2929)\n", total.warm_ops);
    printf("    snapshots:      %10" PRIu64 "\n", total.snapshots);
    printf("    reverts:        %10" PRIu64 "  (failed txs)\n", total.reverts);
    printf("    selfdestructs:  %10" PRIu64 "\n", total.selfdestructs);
    printf("  total reads:      %10" PRIu64 "\n", total_reads);
    printf("  total writes:     %10" PRIu64 "\n", total_writes);
    printf("  read:write ratio: %.1f:1\n",
           total_writes > 0 ? (double)total_reads / total_writes : 0);
    printf("  ------------------------------------------------------------\n");
    printf("  exec latency (ms):   [tx simulation + evm_state ops]\n");
    printf("    min:  %6.2f\n", exec_ms[0]);
    printf("    p50:  %6.2f\n", exec_ms[total_blocks / 2]);
    printf("    p95:  %6.2f\n", exec_ms[(uint64_t)total_blocks * 95 / 100]);
    printf("    p99:  %6.2f\n", exec_ms[(uint64_t)total_blocks * 99 / 100]);
    printf("    max:  %6.2f\n", exec_ms[total_blocks - 1]);
    printf("  root latency (ms):   [compute_state_root (incremental MPT)]\n");
    printf("    min:  %6.2f\n", root_ms[0]);
    printf("    p50:  %6.2f\n", root_ms[total_blocks / 2]);
    printf("    p95:  %6.2f\n", root_ms[(uint64_t)total_blocks * 95 / 100]);
    printf("    p99:  %6.2f\n", root_ms[(uint64_t)total_blocks * 99 / 100]);
    printf("    max:  %6.2f\n", root_ms[total_blocks - 1]);
    printf("  commit latency (ms): [finalize + undo log + hash_store + msync]\n");
    printf("    min:  %6.2f\n", commit_ms[0]);
    printf("    p50:  %6.2f\n", commit_ms[total_blocks / 2]);
    printf("    p95:  %6.2f\n", commit_ms[(uint64_t)total_blocks * 95 / 100]);
    printf("    p99:  %6.2f\n", commit_ms[(uint64_t)total_blocks * 99 / 100]);
    printf("    max:  %6.2f\n", commit_ms[total_blocks - 1]);
    printf("  ------------------------------------------------------------\n");
    printf("  account keys:     %" PRIu64 "\n", final_stats.account_keys);
    printf("  storage keys:     %" PRIu64 "\n", final_stats.storage_keys);
    printf("  code entries:     %u\n", final_stats.code_count);
    printf("  RSS:              %zu MB\n", get_rss_mb());
    printf("============================================================\n");

    free(exec_ms);
    free(root_ms);
    free(commit_ms);
    free(code_buf);
    sdb_destroy(sdb);
    (void)system("rm -rf " TEST_DIR);
    unlink(STATE_MPT_PATH);
    unlink(STORAGE_MPT_PATH);

    return 0;
}
