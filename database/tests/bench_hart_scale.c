/**
 * bench_hart_scale — Scale benchmark for hashed_art (account index).
 *
 * Measures arena growth, freelist waste, and memory efficiency at scale.
 * Simulates real chain_replay patterns: insert millions of entries,
 * observe node upgrades (node4→16→48→256), measure wasted space.
 *
 * Usage: bench_hart_scale [count]
 *   count: number of entries to insert (default: 1000000)
 */

#include "hashed_art.h"
#include "keccak256.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>

/* Generate a deterministic 32-byte key from an index */
static void make_key(uint64_t idx, uint8_t key[32]) {
    /* Hash the index to get uniform distribution (like keccak(address)) */
    uint8_t buf[8];
    for (int i = 0; i < 8; i++) buf[i] = (uint8_t)(idx >> (i * 8));
    hash_t h = hash_keccak256(buf, 8);
    memcpy(key, h.bytes, 32);
}

static double elapsed_ms(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1000.0 +
           (end->tv_nsec - start->tv_nsec) / 1e6;
}

/* Count nodes on a freelist */
static uint32_t freelist_count(const hart_t *t, uint32_t head) {
    uint32_t count = 0;
    uint32_t ref = head;
    while (ref != 0) {
        size_t offset = (size_t)ref << 4;
        ref = *(uint32_t *)(t->arena + offset);
        count++;
    }
    return count;
}

static void print_stats(const hart_t *t, const char *label) {
    uint32_t fl_n4   = freelist_count(t, t->free_node4);
    uint32_t fl_n16  = freelist_count(t, t->free_node16);
    uint32_t fl_n48  = freelist_count(t, t->free_node48);
    uint32_t fl_n256 = freelist_count(t, t->free_node256);
    uint32_t fl_leaf = freelist_count(t, t->free_leaf);

    /* Size constants (from hashed_art.c) */
    size_t sz_n4   = 56;   /* node4_t */
    size_t sz_n16  = 116;  /* node16_t */
    size_t sz_n48  = 484;  /* node48_t */
    size_t sz_n256 = 1060; /* node256_t */
    size_t sz_leaf = 32 + t->value_size; /* key + value */

    size_t waste_n4   = fl_n4   * ((sz_n4   + 15) & ~15);
    size_t waste_n16  = fl_n16  * ((sz_n16  + 15) & ~15);
    size_t waste_n48  = fl_n48  * ((sz_n48  + 15) & ~15);
    size_t waste_n256 = fl_n256 * ((sz_n256 + 15) & ~15);
    size_t waste_leaf = fl_leaf * ((sz_leaf + 15) & ~15);
    size_t total_waste = waste_n4 + waste_n16 + waste_n48 + waste_n256 + waste_leaf;

    size_t live_data = t->arena_used - total_waste;

    printf("\n=== %s ===\n", label);
    printf("  entries:     %zu\n", t->size);
    printf("  arena_used:  %.1f MB\n", t->arena_used / (1024.0 * 1024));
    printf("  arena_cap:   %.1f MB\n", t->arena_cap / (1024.0 * 1024));
    printf("  live_data:   %.1f MB (%.1f%%)\n",
           live_data / (1024.0 * 1024),
           100.0 * live_data / (t->arena_used > 0 ? t->arena_used : 1));
    printf("  total_waste: %.1f MB (%.1f%%)\n",
           total_waste / (1024.0 * 1024),
           100.0 * total_waste / (t->arena_used > 0 ? t->arena_used : 1));
    printf("  freelists:\n");
    printf("    node4:   %u entries (%zuKB wasted)\n", fl_n4,   waste_n4 / 1024);
    printf("    node16:  %u entries (%zuKB wasted)\n", fl_n16,  waste_n16 / 1024);
    printf("    node48:  %u entries (%zuKB wasted)\n", fl_n48,  waste_n48 / 1024);
    printf("    node256: %u entries (%zuKB wasted)\n", fl_n256, waste_n256 / 1024);
    printf("    leaf:    %u entries (%zuKB wasted)\n", fl_leaf, waste_leaf / 1024);
    printf("  bytes/entry: %.1f (arena) %.1f (live)\n",
           t->size > 0 ? (double)t->arena_used / t->size : 0,
           t->size > 0 ? (double)live_data / t->size : 0);
}

int main(int argc, char **argv) {
    /* 32MB stack for safety */
    struct rlimit rl;
    if (getrlimit(RLIMIT_STACK, &rl) == 0 && rl.rlim_cur < 32UL * 1024 * 1024) {
        rl.rlim_cur = 32UL * 1024 * 1024;
        setrlimit(RLIMIT_STACK, &rl);
    }

    uint64_t count = argc > 1 ? (uint64_t)atoll(argv[1]) : 1000000;

    printf("Hart Scale Benchmark\n");
    printf("Inserting %lu entries with 4-byte values (account index pattern)\n", count);

    hart_t tree;
    hart_init(&tree, 4);  /* 4-byte value like account index */

    struct timespec t0, t1;

    /* Phase 1: Insert entries, measure at checkpoints */
    uint64_t checkpoints[] = { 1000, 10000, 100000, 500000, 1000000,
                                2000000, 5000000, 10000000, 50000000, 0 };

    clock_gettime(CLOCK_MONOTONIC, &t0);

    int cp_idx = 0;
    for (uint64_t i = 0; i < count; i++) {
        uint8_t key[32];
        make_key(i, key);
        uint32_t val = (uint32_t)i;
        hart_insert(&tree, key, &val);

        if (checkpoints[cp_idx] > 0 && i + 1 == checkpoints[cp_idx]) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            char label[64];
            snprintf(label, sizeof(label), "After %lu inserts (%.0fms)",
                     i + 1, elapsed_ms(&t0, &t1));
            print_stats(&tree, label);
            cp_idx++;
            if (checkpoints[cp_idx] == 0 || checkpoints[cp_idx] > count)
                break;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    char final_label[64];
    snprintf(final_label, sizeof(final_label), "Final (%lu entries, %.0fms)",
             count, elapsed_ms(&t0, &t1));
    print_stats(&tree, final_label);

    /* Phase 2: Delete 10% of entries, measure freelist growth */
    printf("\n--- Deleting 10%% of entries ---\n");
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t del_count = count / 10;
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < del_count; i++) {
        uint8_t key[32];
        make_key(i * 10, key);  /* every 10th entry */
        if (hart_delete(&tree, key))
            deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    char del_label[64];
    snprintf(del_label, sizeof(del_label), "After %lu deletes (%.0fms)",
             deleted, elapsed_ms(&t0, &t1));
    print_stats(&tree, del_label);

    /* Phase 3: Re-insert same count — measure reuse vs new allocation */
    printf("\n--- Re-inserting %lu entries ---\n", deleted);
    size_t arena_before = tree.arena_used;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < del_count; i++) {
        uint8_t key[32];
        make_key(count + i, key);  /* new keys */
        uint32_t val = (uint32_t)(count + i);
        hart_insert(&tree, key, &val);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    size_t arena_after = tree.arena_used;
    char reinsert_label[64];
    snprintf(reinsert_label, sizeof(reinsert_label),
             "After re-insert (%.0fms)", elapsed_ms(&t0, &t1));
    print_stats(&tree, reinsert_label);

    printf("\n  Arena growth during re-insert: %.1f MB (%.1f%% of deleted space reused)\n",
           (arena_after - arena_before) / (1024.0 * 1024),
           arena_before > 0 ? 100.0 * (1.0 - (double)(arena_after - arena_before) /
                               (arena_before > tree.arena_used ? 1 : arena_before * 0.1)) : 0);

    /* Summary */
    printf("\n========================================\n");
    printf("Summary:\n");
    printf("  Total entries:     %zu\n", tree.size);
    printf("  Arena used:        %.1f MB\n", tree.arena_used / (1024.0 * 1024));
    printf("  Arena capacity:    %.1f MB\n", tree.arena_cap / (1024.0 * 1024));
    printf("  Overhead ratio:    %.2fx (arena/ideal)\n",
           tree.size > 0 ? (double)tree.arena_used /
                           (tree.size * ((32 + 4 + 15) & ~15)) : 0);
    printf("========================================\n");

    hart_destroy(&tree);
    return 0;
}
