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
    /* Skip leading zeros */
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
    printf("Block %lu: %u accounts, %u storage slots\n",
           diff->block_number, diff->account_count, diff->storage_count);

    for (uint32_t i = 0; i < diff->account_count; i++) {
        const account_diff_t *a = &diff->accounts[i];
        printf("  Account ");
        print_address(&a->addr);

        if (a->flags & ACCT_DIFF_CREATED) printf(" [CREATED]");
        if (a->flags & ACCT_DIFF_DESTRUCTED) printf(" [DESTRUCTED]");
        printf("\n");

        if (a->old_nonce != a->new_nonce)
            printf("    nonce:   %lu -> %lu\n", a->old_nonce, a->new_nonce);

        if (!uint256_is_equal(&a->old_balance, &a->new_balance)) {
            printf("    balance: ");
            print_uint256(&a->old_balance);
            printf(" -> ");
            print_uint256(&a->new_balance);
            printf("\n");
        }

        if (memcmp(a->old_code_hash.bytes, a->new_code_hash.bytes, 32) != 0) {
            printf("    code:    ");
            print_hash(&a->old_code_hash);
            printf("\n          -> ");
            print_hash(&a->new_code_hash);
            printf("\n");
        }
    }

    for (uint32_t i = 0; i < diff->storage_count; i++) {
        const storage_diff_t *s = &diff->storage[i];
        printf("  Storage ");
        print_address(&s->addr);
        printf(" slot ");
        print_uint256(&s->slot);
        printf("\n");
        printf("    ");
        print_uint256(&s->old_value);
        printf(" -> ");
        print_uint256(&s->new_value);
        printf("\n");
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <history_dir> [block_number] [end_block]\n", argv[0]);
        return 1;
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
        /* Summary mode: scan all blocks and print stats */
        uint64_t total_accts = 0, total_slots = 0;
        uint64_t blocks_with_accts = 0, blocks_with_slots = 0;
        uint64_t max_accts = 0, max_slots = 0;
        uint64_t max_accts_block = 0, max_slots_block = 0;

        for (uint64_t bn = first; bn <= last; bn++) {
            block_diff_t diff;
            if (!state_history_get_diff(sh, bn, &diff)) continue;

            total_accts += diff.account_count;
            total_slots += diff.storage_count;
            if (diff.account_count > 0) blocks_with_accts++;
            if (diff.storage_count > 0) blocks_with_slots++;
            if (diff.account_count > max_accts) {
                max_accts = diff.account_count;
                max_accts_block = bn;
            }
            if (diff.storage_count > max_slots) {
                max_slots = diff.storage_count;
                max_slots_block = bn;
            }
            block_diff_free(&diff);
        }

        printf("Stats:\n");
        printf("  Total account diffs:  %lu\n", total_accts);
        printf("  Total storage diffs:  %lu\n", total_slots);
        printf("  Blocks with account changes: %lu / %lu\n", blocks_with_accts, last - first + 1);
        printf("  Blocks with storage changes: %lu / %lu\n", blocks_with_slots, last - first + 1);
        printf("  Max accounts in one block: %lu (block %lu)\n", max_accts, max_accts_block);
        printf("  Max storage in one block:  %lu (block %lu)\n", max_slots, max_slots_block);
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
