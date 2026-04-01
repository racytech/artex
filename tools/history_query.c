/**
 * History Query Tool — search state history diffs for specific addresses.
 *
 * Usage:
 *   history_query <history_dir> <address_hex> [start_block] [end_block]
 *
 * Scans backwards from end_block to find which blocks modified the address.
 * Reports field changes and storage slot modifications.
 */

#include "state_history.h"
#include "address.h"
#include "uint256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static bool parse_address(const char *hex, address_t *out) {
    const char *p = hex;
    if (p[0] == '0' && p[1] == 'x') p += 2;
    if (strlen(p) != 40) return false;
    for (int i = 0; i < 20; i++) {
        unsigned int b;
        if (sscanf(p + i*2, "%02x", &b) != 1) return false;
        out->bytes[i] = (uint8_t)b;
    }
    return true;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <history_dir> <address> [start_block] [end_block]\n", argv[0]);
        fprintf(stderr, "  Scans backwards from end_block for blocks that modified <address>.\n");
        fprintf(stderr, "  Default: scans full range.\n");
        return 1;
    }

    const char *history_dir = argv[1];
    address_t target;
    if (!parse_address(argv[2], &target)) {
        fprintf(stderr, "Invalid address: %s\n", argv[2]);
        return 1;
    }

    state_history_t *sh = state_history_create(history_dir);
    if (!sh) {
        fprintf(stderr, "Failed to open history at %s\n", history_dir);
        return 1;
    }

    uint64_t first, last;
    if (!state_history_range(sh, &first, &last)) {
        fprintf(stderr, "No blocks in history\n");
        state_history_destroy(sh);
        return 1;
    }
    fprintf(stderr, "History range: %lu..%lu (%lu blocks)\n", first, last, last - first + 1);

    uint64_t start = argc > 3 ? (uint64_t)atoll(argv[3]) : first;
    uint64_t end   = argc > 4 ? (uint64_t)atoll(argv[4]) : last;
    if (start < first) start = first;
    if (end > last) end = last;

    fprintf(stderr, "Scanning blocks %lu..%lu for 0x", start, end);
    for (int i = 0; i < 20; i++) fprintf(stderr, "%02x", target.bytes[i]);
    fprintf(stderr, " (backwards)\n\n");

    int found = 0;
    for (uint64_t bn = end; bn >= start && bn != UINT64_MAX; bn--) {
        block_diff_t diff;
        if (!state_history_get_diff(sh, bn, &diff)) continue;

        for (uint16_t g = 0; g < diff.group_count; g++) {
            addr_diff_t *ad = &diff.groups[g];
            if (memcmp(ad->addr.bytes, target.bytes, 20) != 0) continue;

            found++;
            printf("Block %lu: 0x", bn);
            for (int i = 0; i < 20; i++) printf("%02x", target.bytes[i]);

            /* Flags */
            if (ad->flags & ACCT_DIFF_CREATED) printf(" CREATED");
            if (ad->flags & ACCT_DIFF_DESTRUCTED) printf(" DESTRUCTED");
            if (ad->flags & ACCT_DIFF_TOUCHED) printf(" TOUCHED");

            /* Field changes */
            if (ad->field_mask & FIELD_NONCE)
                printf(" nonce=%lu", ad->nonce);
            if (ad->field_mask & FIELD_BALANCE) {
                uint8_t bb[32]; uint256_to_bytes(&ad->balance, bb);
                printf(" balance=0x");
                int s = 0; while (s < 31 && bb[s] == 0) s++;
                for (int j = s; j < 32; j++) printf("%02x", bb[j]);
            }
            if (ad->field_mask & FIELD_CODE_HASH) {
                printf(" code_hash=0x");
                for (int j = 0; j < 8; j++) printf("%02x", ad->code_hash.bytes[j]);
                printf("...");
            }

            /* Storage */
            if (ad->slot_count > 0) {
                printf(" storage(%d slots):", ad->slot_count);
                for (uint16_t s = 0; s < ad->slot_count && s < 10; s++) {
                    uint8_t sk[32], sv[32];
                    uint256_to_bytes(&ad->slots[s].slot, sk);
                    uint256_to_bytes(&ad->slots[s].value, sv);
                    printf("\n    slot=0x");
                    for (int j = 0; j < 32; j++) printf("%02x", sk[j]);
                    printf(" val=0x");
                    int z = 0; while (z < 31 && sv[z] == 0) z++;
                    for (int j = z; j < 32; j++) printf("%02x", sv[j]);
                }
                if (ad->slot_count > 10) printf("\n    ... +%d more", ad->slot_count - 10);
            }
            printf("\n");
        }
        block_diff_free(&diff);

        /* Stop after finding 20 matches */
        if (found >= 20) {
            printf("... (stopped after 20 matches)\n");
            break;
        }
    }

    if (found == 0)
        printf("No modifications found for this address in range %lu..%lu\n", start, end);

    state_history_destroy(sh);
    return 0;
}
