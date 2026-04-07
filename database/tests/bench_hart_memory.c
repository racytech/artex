/**
 * Benchmark: hart arena memory usage with overwrites and deletes.
 *
 * Simulates realistic storage workload:
 *   Phase 1: Populate N slots
 *   Phase 2: Overwrite M% of slots K times (simulates SSTORE updates)
 *   Phase 3: Delete D% of slots (simulates SSTORE to zero)
 *
 * Reports: arena_used, arena_cap, live entries, dead space.
 * Run with and without freelist to compare.
 */

#include "hashed_art.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define STOR_VAL_SIZE 32

static void make_key(uint64_t slot, uint8_t key[32]) {
    uint8_t buf[8];
    for (int i = 7; i >= 0; i--) { buf[i] = slot & 0xFF; slot >>= 8; }
    hash_t h = hash_keccak256(buf, 8);
    memcpy(key, h.bytes, 32);
}

static void make_val(uint64_t v, uint8_t val[32]) {
    memset(val, 0, 32);
    for (int i = 31; v > 0 && i >= 0; i--) {
        val[i] = (uint8_t)(v & 0xFF);
        v >>= 8;
    }
}

/* RLP encode callback for root hash */
static uint32_t stor_encode(const uint8_t key[32], const void *leaf_val,
                             uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    const uint8_t *v = (const uint8_t *)leaf_val;
    int skip = 0;
    while (skip < 31 && v[skip] == 0) skip++;
    int len = 32 - skip;
    if (len == 1 && v[skip] <= 0x7f) { rlp_out[0] = v[skip]; return 1; }
    rlp_out[0] = 0x80 + (uint8_t)len;
    memcpy(rlp_out + 1, v + skip, len);
    return 1 + (uint32_t)len;
}

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void bench(uint32_t n_slots, uint32_t overwrite_pct, uint32_t overwrite_rounds,
                  uint32_t delete_pct) {
    hart_t ht;
    hart_init_cap(&ht, STOR_VAL_SIZE, 1024);

    /* Phase 1: Populate */
    double t0 = now_ms();
    for (uint32_t i = 0; i < n_slots; i++) {
        uint8_t key[32], val[32];
        make_key(i, key);
        make_val(i + 1, val);
        hart_insert(&ht, key, val);
    }
    double t1 = now_ms();

    size_t after_populate_used = ht.arena_used;
    size_t after_populate_cap = ht.arena_cap;

    /* Phase 2: Overwrite subset multiple times */
    uint32_t n_overwrite = n_slots * overwrite_pct / 100;
    for (uint32_t round = 0; round < overwrite_rounds; round++) {
        for (uint32_t i = 0; i < n_overwrite; i++) {
            uint8_t key[32], val[32];
            make_key(i, key);
            make_val((round + 1) * 1000000 + i, val);
            hart_insert(&ht, key, val);
        }
    }
    double t2 = now_ms();

    size_t after_overwrite_used = ht.arena_used;
    size_t after_overwrite_cap = ht.arena_cap;

    /* Phase 3: Delete subset */
    uint32_t n_delete = n_slots * delete_pct / 100;
    for (uint32_t i = 0; i < n_delete; i++) {
        uint8_t key[32];
        make_key(n_slots - 1 - i, key);
        hart_delete(&ht, key);
    }
    double t3 = now_ms();

    size_t after_delete_used = ht.arena_used;
    size_t after_delete_cap = ht.arena_cap;
    size_t live = hart_size(&ht);

    /* Compute root to verify correctness */
    uint8_t root[32];
    hart_root_hash(&ht, stor_encode, NULL, root);
    double t4 = now_ms();

    /* Theoretical minimum: live entries × ~96 bytes (leaf + share of internal nodes) */
    size_t theoretical_min = live * 96;

    printf("  slots=%5u overwrite=%u%%×%u delete=%u%% | "
           "populate: used=%zuKB cap=%zuKB | "
           "overwrite: used=%zuKB cap=%zuKB | "
           "delete: used=%zuKB cap=%zuKB | "
           "live=%zu waste=%.0f%% | "
           "root=%.1fms\n",
           n_slots, overwrite_pct, overwrite_rounds, delete_pct,
           after_populate_used / 1024, after_populate_cap / 1024,
           after_overwrite_used / 1024, after_overwrite_cap / 1024,
           after_delete_used / 1024, after_delete_cap / 1024,
           live,
           theoretical_min > 0 ? (1.0 - (double)theoretical_min / after_delete_used) * 100 : 0,
           t4 - t3);

    hart_destroy(&ht);
}

int main(void) {
    printf("=== Hart Memory Benchmark ===\n\n");

    printf("--- Small accounts (typical ERC-20) ---\n");
    bench(50,   50, 10, 10);    /* 50 slots, 50% overwritten 10x, 10% deleted */
    bench(100,  50, 10, 10);
    bench(100,  80, 50, 20);    /* heavy overwrite */

    printf("\n--- Medium accounts (DeFi pools) ---\n");
    bench(500,  30, 20, 5);
    bench(1000, 30, 20, 5);
    bench(1000, 50, 50, 10);    /* heavy overwrite */

    printf("\n--- Large accounts (Uniswap, major contracts) ---\n");
    bench(5000,  20, 10, 2);
    bench(10000, 20, 10, 2);
    bench(10000, 50, 50, 10);   /* extreme overwrite */

    printf("\n--- Stress: many overwrites, few slots ---\n");
    bench(10,   100, 1000, 0);  /* 10 slots, all overwritten 1000x */
    bench(50,   100, 1000, 0);
    bench(100,  100, 1000, 0);

    printf("\n--- Churn: insert + delete cycles (triggers node grow/shrink) ---\n");
    {
        /* Simulate: add 1000 slots, delete 500, add 500 new, repeat */
        uint32_t sizes[] = {100, 500, 1000, 5000};
        for (int si = 0; si < 4; si++) {
            uint32_t n = sizes[si];
            hart_t ht;
            hart_init_cap(&ht, STOR_VAL_SIZE, 1024);

            uint64_t next_key = 0;

            /* Initial populate */
            for (uint32_t i = 0; i < n; i++) {
                uint8_t key[32], val[32];
                make_key(next_key++, key);
                make_val(i + 1, val);
                hart_insert(&ht, key, val);
            }
            size_t base_used = ht.arena_used;

            /* Churn: 10 rounds of delete half + insert new half */
            for (int round = 0; round < 10; round++) {
                /* Delete oldest half */
                uint64_t del_start = next_key - hart_size(&ht);
                uint32_t n_del = (uint32_t)hart_size(&ht) / 2;
                for (uint32_t i = 0; i < n_del; i++) {
                    uint8_t key[32];
                    make_key(del_start + i, key);
                    hart_delete(&ht, key);
                }
                /* Insert new entries */
                for (uint32_t i = 0; i < n_del; i++) {
                    uint8_t key[32], val[32];
                    make_key(next_key++, key);
                    make_val(round * 100000 + i, val);
                    hart_insert(&ht, key, val);
                }
            }

            printf("  slots=%5u churn=10×50%% | base=%zuKB final: used=%zuKB cap=%zuKB live=%zu bloat=%.0f%%\n",
                   n, base_used / 1024,
                   ht.arena_used / 1024, ht.arena_cap / 1024,
                   hart_size(&ht),
                   (double)ht.arena_used / base_used * 100 - 100);
            hart_destroy(&ht);
        }
    }

    printf("\n--- Aggregate: simulate 1000 accounts of mixed sizes ---\n");
    {
        size_t total_used = 0, total_cap = 0, total_live = 0;
        double t0 = now_ms();

        /* 800 small (10-100 slots), 150 medium (100-1000), 50 large (1000-10000) */
        for (int a = 0; a < 1000; a++) {
            uint32_t n;
            if (a < 800) n = 10 + (a % 91);
            else if (a < 950) n = 100 + (a % 901);
            else n = 1000 + (a % 9001);

            hart_t ht;
            hart_init_cap(&ht, STOR_VAL_SIZE, 1024);

            /* Populate */
            for (uint32_t i = 0; i < n; i++) {
                uint8_t key[32], val[32];
                make_key(a * 100000 + i, key);
                make_val(i + 1, val);
                hart_insert(&ht, key, val);
            }

            /* Overwrite 30% × 10 rounds */
            for (int r = 0; r < 10; r++) {
                uint32_t nw = n * 30 / 100;
                for (uint32_t i = 0; i < nw; i++) {
                    uint8_t key[32], val[32];
                    make_key(a * 100000 + i, key);
                    make_val(r * 1000000 + i, val);
                    hart_insert(&ht, key, val);
                }
            }

            /* Delete 5% */
            uint32_t nd = n * 5 / 100;
            for (uint32_t i = 0; i < nd; i++) {
                uint8_t key[32];
                make_key(a * 100000 + n - 1 - i, key);
                hart_delete(&ht, key);
            }

            total_used += ht.arena_used;
            total_cap += ht.arena_cap;
            total_live += hart_size(&ht);
            hart_destroy(&ht);
        }

        double t1 = now_ms();
        size_t theoretical = total_live * 96;
        printf("  1000 accounts: used=%zuMB cap=%zuMB live=%zu theoretical=%zuMB waste=%.0f%% time=%.0fms\n",
               total_used / (1024*1024), total_cap / (1024*1024),
               total_live, theoretical / (1024*1024),
               (1.0 - (double)theoretical / total_used) * 100,
               t1 - t0);
    }

    printf("\n=== done ===\n");
    return 0;
}
