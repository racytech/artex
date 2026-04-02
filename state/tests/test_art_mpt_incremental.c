/**
 * Reproduce the exact art_mpt flow from state.c:
 *   acct_index stores uint32_t idx, encode callback looks up account data.
 *   1. Insert 2 accounts → compute root (genesis)
 *   2. mark_path_dirty for modified accounts + insert new → compute root (post-tx)
 *   3. Compare with known-correct root
 */
#include "mem_art.h"
#include "art_iface.h"
#include "art_mpt.h"
#include "mem_mpt.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void hex2bin(const char *hex, uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++) {
        unsigned b; sscanf(hex + i*2, "%02x", &b); out[i] = (uint8_t)b;
    }
}

/* Simulated accounts array — encode callback reads from here */
static uint8_t g_rlp[8][128];
static uint32_t g_rlp_len[8];

static uint32_t encode_by_idx(const uint8_t *key, const void *leaf_val,
                               uint32_t val_size, uint8_t *rlp_out, void *ctx) {
    (void)key; (void)val_size; (void)ctx;
    uint32_t idx;
    memcpy(&idx, leaf_val, sizeof(idx));
    memcpy(rlp_out, g_rlp[idx], g_rlp_len[idx]);
    return g_rlp_len[idx];
}

static void print_hash(const char *label, const uint8_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

int main(void) {
    uint8_t k0[32], k1[32], k2[32];
    hex2bin("47e2672d1bd341fd13f3a9bd6fcb81159e21feb77b1b57022175b30e5793e542", k0, 32);
    hex2bin("f17c57e9781bb80c467568c9c7c12c63aaf9a74cdf7c2d76d5b71b284dcd959a", k1, 32);
    hex2bin("9d860e7bb7e6b09b87ab7406933ef2980c19d7d0192d8939cf6dc6908a03305f", k2, 32);

    /* Genesis RLP */
    g_rlp_len[0] = 70;
    hex2bin("f8440180a04ae3a24841e4183a9b6b03501362e471ce86eb0866055d8ddb2514d0b74818caa0a8b8cdc63cb795487043cc5a7843f1ad3fe16b9d736005b76073ab4b7a3311e1", g_rlp[0], 70);
    g_rlp_len[1] = 79;
    hex2bin("f84d80893635c9adc5dea00000a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470", g_rlp[1], 79);

    /* Post-tx RLP */
    uint8_t v0_post[70], v1_post[79], v2_post[73];
    hex2bin("f8440180a0d827c5bb1889465b200f4562839bd76fc209e3bd8e6106e87edd850e368316b0a0a8b8cdc63cb795487043cc5a7843f1ad3fe16b9d736005b76073ab4b7a3311e1", v0_post, 70);
    hex2bin("f84d01893635c9adc5de9b8c1ea056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470", v1_post, 79);
    hex2bin("f84780830473e2a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470", v2_post, 73);

    uint8_t expected[32];
    hex2bin("69395631019141fe1a0e8c9b7faa1be5cf67884f6acea7e07dd8172a7f5c5ef2", expected, 32);

    /* Batch reference */
    {
        mpt_batch_entry_t entries[3];
        memcpy(entries[0].key, k0, 32); entries[0].value = v0_post; entries[0].value_len = 70;
        memcpy(entries[1].key, k1, 32); entries[1].value = v1_post; entries[1].value_len = 79;
        memcpy(entries[2].key, k2, 32); entries[2].value = v2_post; entries[2].value_len = 73;
        hash_t br;
        mpt_compute_root_batch(entries, 3, &br);
        print_hash("batch ref ", br.bytes);
    }

    /* art_mpt with uint32_t idx values */
    {
        mem_art_t tree;
        mem_art_init(&tree);

        art_iface_mem_ctx_t ctx = { .tree = &tree, .key_size = 32, .value_size = sizeof(uint32_t) };
        art_mpt_t *am = art_mpt_create_iface(art_iface_mem(&ctx), encode_by_idx, NULL);

        /* Genesis */
        uint32_t idx0 = 0, idx1 = 1;
        mem_art_insert(&tree, k0, 32, &idx0, sizeof(idx0));
        mem_art_insert(&tree, k1, 32, &idx1, sizeof(idx1));

        uint8_t genesis[32];
        art_mpt_root_hash(am, genesis);
        print_hash("genesis   ", genesis);

        /* Update account data in global array */
        memcpy(g_rlp[0], v0_post, 70); g_rlp_len[0] = 70;
        memcpy(g_rlp[1], v1_post, 79); g_rlp_len[1] = 79;
        memcpy(g_rlp[2], v2_post, 73); g_rlp_len[2] = 73;

        /* mark_path_dirty for existing accounts */
        mem_art_mark_path_dirty(&tree, k0, 32);
        mem_art_mark_path_dirty(&tree, k1, 32);

        /* insert new account */
        uint32_t idx2 = 2;
        mem_art_insert(&tree, k2, 32, &idx2, sizeof(idx2));

        uint8_t posttx[32];
        art_mpt_root_hash(am, posttx);
        print_hash("incremental", posttx);

        /* Try invalidate_all + recompute */
        art_mpt_invalidate_all(am);
        uint8_t fresh[32];
        art_mpt_root_hash(am, fresh);
        print_hash("invalidated", fresh);

        printf("\n  incr==expected:  %s\n", memcmp(posttx, expected, 32) == 0 ? "YES" : "NO");
        printf("  fresh==expected: %s\n", memcmp(fresh, expected, 32) == 0 ? "YES" : "NO");
        printf("  incr==fresh:     %s\n", memcmp(posttx, fresh, 32) == 0 ? "YES" : "NO");

        art_mpt_destroy(am);
        mem_art_destroy(&tree);
    }

    print_hash("expected  ", expected);
    return 0;
}
