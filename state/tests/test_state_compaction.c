/**
 * Test state_compact at scale:
 *   1. Create N accounts (some with storage, some phantoms)
 *   2. Self-destruct some, prune others
 *   3. Compute root
 *   4. Compact
 *   5. Verify root matches, memory shrinks
 */
#include "state.h"
#include "evm_state.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

extern state_t *evm_state_get_state(evm_state_t *es);

static void make_addr(uint32_t seed, address_t *out) {
    memset(out->bytes, 0, 20);
    memcpy(out->bytes + 16, &seed, 4);
}

static void print_hash(const char *label, const hash_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 4; i++) printf("%02x", h->bytes[i]);
    printf("...");
    for (int i = 28; i < 32; i++) printf("%02x", h->bytes[i]);
    printf("\n");
}

int main(int argc, char **argv) {
    uint32_t N = 100000;       /* total accounts */
    uint32_t N_storage = 5000; /* accounts with storage */
    uint32_t N_phantom = 50000;/* phantom touches (add_balance(0)) */
    uint32_t N_destruct = 1000;/* self-destructed */

    if (argc > 1) N = (uint32_t)atoi(argv[1]);
    if (argc > 2) N_phantom = (uint32_t)atoi(argv[2]);
    if (argc > 3) N_storage = (uint32_t)atoi(argv[3]);
    if (argc > 4) N_destruct = (uint32_t)atoi(argv[4]);

    printf("Config: N=%u phantoms=%u storage=%u destruct=%u\n",
           N, N_phantom, N_storage, N_destruct);

    evm_state_t *es = evm_state_create(NULL);
    if (!es) { fprintf(stderr, "FAIL: create\n"); return 1; }
    evm_state_set_prune_empty(es, true);

    struct timespec t0, t1;

    /* Phase 1: Create N real accounts */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint32_t i = 0; i < N; i++) {
        address_t addr;
        make_addr(i + 1, &addr);
        uint256_t bal = uint256_from_uint64((uint64_t)(i + 1) * 100);
        evm_state_set_balance(es, &addr, &bal);
        evm_state_set_nonce(es, &addr, 1);
        if (i < N_storage) {
            uint256_t key = uint256_from_uint64(i);
            uint256_t val = uint256_from_uint64(i * 42 + 1);
            evm_state_set_storage(es, &addr, &key, &val);
        }
        if (i < N / 10) {
            uint8_t code[32];
            memset(code, 0x60, sizeof(code));
            evm_state_set_code(es, &addr, code, 32);
        }
        evm_state_mark_existed(es, &addr);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double setup_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    evm_state_commit(es);
    evm_state_compute_mpt_root(es, true);
    evm_state_clear_prestate_dirty(es);

    /* Phase 2: Simulate tx — phantoms + self-destructs */
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Phantoms: touch non-existent addresses */
    uint256_t zero = UINT256_ZERO;
    for (uint32_t i = 0; i < N_phantom; i++) {
        address_t addr;
        make_addr(N + 1000000 + i, &addr);
        evm_state_add_balance(es, &addr, &zero);
    }

    /* Self-destruct some existing accounts */
    for (uint32_t i = 0; i < N_destruct && i < N; i++) {
        address_t addr;
        make_addr(i + 1, &addr);
        evm_state_self_destruct(es, &addr);
    }

    /* Modify some accounts */
    for (uint32_t i = N_destruct; i < N_destruct + 5000 && i < N; i++) {
        address_t addr;
        make_addr(i + 1, &addr);
        uint256_t new_bal = uint256_from_uint64(999999);
        evm_state_set_balance(es, &addr, &new_bal);
    }

    evm_state_commit_tx(es);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double tx_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    /* Phase 3: Compute root */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hash_t root_before = evm_state_compute_mpt_root(es, true);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double root_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    state_t *st = evm_state_get_state(es);
    state_stats_t stats_before = state_get_stats(st);

    printf("\nBefore compaction:\n");
    print_hash("root", &root_before);
    printf("  accounts: %u (in vector)\n", stats_before.account_count);
    printf("  storage:  %u\n", stats_before.storage_account_count);
    printf("  memory:   %.1f MB\n", stats_before.memory_used / 1e6);

    /* Phase 4: Compact */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    state_compact(st);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double compact_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    /* Phase 5: Compute root again */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hash_t root_after = evm_state_compute_mpt_root(es, true);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double root2_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    state_stats_t stats_after = state_get_stats(st);

    printf("\nAfter compaction:\n");
    print_hash("root", &root_after);
    printf("  accounts: %u (in vector)\n", stats_after.account_count);
    printf("  storage:  %u\n", stats_after.storage_account_count);
    printf("  memory:   %.1f MB\n", stats_after.memory_used / 1e6);

    int match = memcmp(root_before.bytes, root_after.bytes, 32) == 0;
    printf("\nRoots match: %s\n", match ? "YES" : "NO");
    printf("Accounts removed: %u → %u (-%u)\n",
           stats_before.account_count, stats_after.account_count,
           stats_before.account_count - stats_after.account_count);

    printf("\nTiming:\n");
    printf("  setup:     %.1f ms\n", setup_ms);
    printf("  tx sim:    %.1f ms\n", tx_ms);
    printf("  root (1):  %.1f ms\n", root_ms);
    printf("  compact:   %.1f ms\n", compact_ms);
    printf("  root (2):  %.1f ms\n", root2_ms);

    /* Verify a few accounts survived */
    address_t check_addr;
    make_addr(N_destruct + 100, &check_addr);
    uint64_t check_nonce = evm_state_get_nonce(es, &check_addr);
    uint256_t check_bal = evm_state_get_balance(es, &check_addr);
    printf("\nSpot check addr[%u]: nonce=%lu bal=%lu\n",
           N_destruct + 100, check_nonce, uint256_to_uint64(&check_bal));

    evm_state_destroy(es);
    return match ? 0 : 1;
}
