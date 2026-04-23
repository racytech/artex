/**
 * History Dump — read and print state diffs from history files.
 *
 * Usage: ./history_dump <history_dir> [block_number] [end_block]
 *
 * With no block args: print range summary + stats.
 * With one block: print that block's diff.
 * With two blocks: print diffs for range [start, end].
 */

#include "state_history.h"
#include "uint256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_address(const address_t *addr) {
    for (int i = 0; i < 20; i++)
        printf("%02x", addr->bytes[i]);
}

static void print_uint256(const uint256_t *v) {
    uint8_t buf[32];
    uint256_to_bytes(v, buf);
    int start = 0;
    while (start < 31 && buf[start] == 0) start++;
    printf("0x");
    for (int i = start; i < 32; i++)
        printf("%02x", buf[i]);
}

static void print_hash(const hash_t *h) {
    for (int i = 0; i < 32; i++)
        printf("%02x", h->bytes[i]);
}

static void print_diff(const block_diff_t *diff) {
    /* Count total slots across groups */
    uint32_t total_slots = 0;
    for (uint16_t i = 0; i < diff->group_count; i++)
        total_slots += diff->groups[i].slot_count;

    printf("Block %lu: %u address groups, %u storage slots\n",
           diff->block_number, diff->group_count, total_slots);

    for (uint16_t i = 0; i < diff->group_count; i++) {
        const addr_diff_t *g = &diff->groups[i];
        printf("  ");
        print_address(&g->addr);

        if (g->flags & ACCT_DIFF_CREATED) printf(" [CREATED]");
        if (g->flags & ACCT_DIFF_DESTRUCTED) printf(" [DESTRUCTED]");
        printf("\n");

        if (g->field_mask & FIELD_NONCE)
            printf("    nonce:   %lu\n", g->nonce);

        if (g->field_mask & FIELD_BALANCE) {
            printf("    balance: ");
            print_uint256(&g->balance);
            printf("\n");
        }

        if (g->field_mask & FIELD_CODE_HASH) {
            printf("    code:    ");
            print_hash(&g->code_hash);
            printf("\n");
        }

        for (uint16_t j = 0; j < g->slot_count; j++) {
            printf("    slot ");
            print_uint256(&g->slots[j].slot);
            printf(" = ");
            print_uint256(&g->slots[j].value);
            printf("\n");
        }
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <history_dir> [block_number] [end_block]\n"
                        "       %s --stats <history_dir> <start> <end>\n"
                        "                 one line per block: groups, total slots, "
                        "max slot_count in any one group\n",
                argv[0], argv[0]);
        return 1;
    }

    /* Compact stats mode: --stats <dir> <start> <end> */
    if (argc >= 5 && strcmp(argv[1], "--stats") == 0) {
        const char *dir = argv[2];
        uint64_t start = (uint64_t)atoll(argv[3]);
        uint64_t end   = (uint64_t)atoll(argv[4]);
        state_history_t *sh = state_history_create(dir);
        if (!sh) { fprintf(stderr, "open %s failed\n", dir); return 1; }
        printf("%10s  %8s  %10s  %10s  %s\n",
               "block", "groups", "slots", "max_slots", "top_addr");
        for (uint64_t bn = start; bn <= end; bn++) {
            block_diff_t diff;
            if (!state_history_get_diff(sh, bn, &diff)) { printf("%10lu  missing\n", bn); continue; }
            uint32_t total_slots = 0, max_slots = 0;
            const addr_diff_t *worst = NULL;
            for (uint16_t i = 0; i < diff.group_count; i++) {
                total_slots += diff.groups[i].slot_count;
                if (diff.groups[i].slot_count > max_slots) {
                    max_slots = diff.groups[i].slot_count;
                    worst = &diff.groups[i];
                }
            }
            printf("%10lu  %8u  %10u  %10u  ",
                   bn, diff.group_count, total_slots, max_slots);
            if (worst) print_address(&worst->addr);
            printf("\n");
            block_diff_free(&diff);
        }
        state_history_destroy(sh);
        return 0;
    }

    const char *dir = argv[1];
    state_history_t *sh = state_history_create(dir);
    if (!sh) {
        fprintf(stderr, "Failed to open state history at %s\n", dir);
        return 1;
    }

    uint64_t first, last;
    if (!state_history_range(sh, &first, &last)) {
        printf("No blocks recorded.\n");
        state_history_destroy(sh);
        return 0;
    }

    printf("History range: blocks %lu to %lu (%lu blocks)\n\n", first, last, last - first + 1);

    if (argc == 2) {
        /* Summary mode */
        uint64_t total_groups = 0, total_slots = 0;
        uint64_t max_groups = 0, max_slots = 0;
        uint64_t max_groups_block = 0, max_slots_block = 0;

        for (uint64_t bn = first; bn <= last; bn++) {
            block_diff_t diff;
            if (!state_history_get_diff(sh, bn, &diff)) continue;

            uint32_t block_slots = 0;
            for (uint16_t i = 0; i < diff.group_count; i++)
                block_slots += diff.groups[i].slot_count;

            total_groups += diff.group_count;
            total_slots += block_slots;
            if (diff.group_count > max_groups) {
                max_groups = diff.group_count;
                max_groups_block = bn;
            }
            if (block_slots > max_slots) {
                max_slots = block_slots;
                max_slots_block = bn;
            }
            block_diff_free(&diff);
        }

        printf("Stats:\n");
        printf("  Total address groups: %lu\n", total_groups);
        printf("  Total storage slots:  %lu\n", total_slots);
        printf("  Max groups in one block: %lu (block %lu)\n", max_groups, max_groups_block);
        printf("  Max slots in one block:  %lu (block %lu)\n", max_slots, max_slots_block);
    } else {
        /* Dump specific block(s) */
        uint64_t start = (uint64_t)atoll(argv[2]);
        uint64_t end = (argc > 3) ? (uint64_t)atoll(argv[3]) : start;

        for (uint64_t bn = start; bn <= end; bn++) {
            block_diff_t diff;
            if (!state_history_get_diff(sh, bn, &diff)) {
                printf("Block %lu: not found\n", bn);
                continue;
            }
            print_diff(&diff);
            block_diff_free(&diff);
            if (bn < end) printf("\n");
        }
    }

    state_history_destroy(sh);
    return 0;
}
