/**
 * Test hashed_art: insert, get, root hash, incremental update.
 * Cross-validate root hash against mem_mpt batch.
 */
#include "hashed_art.h"
#include "mem_mpt.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Simple encode: just copy the stored value as RLP */
static uint32_t encode_raw(const uint8_t key[32], const void *val,
                            uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    /* For 32-byte values, prefix with 0xa0 (RLP bytes header) */
    rlp_out[0] = 0xa0;
    memcpy(rlp_out + 1, val, 32);
    return 33;
}

static void make_key(uint32_t seed, uint8_t out[32]) {
    memset(out, 0, 32);
    memcpy(out, &seed, 4);
    hash_t h = hash_keccak256(out, 32);
    memcpy(out, h.bytes, 32);
}

static void print_hash(const char *label, const uint8_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 4; i++) printf("%02x", h[i]);
    printf("...");
    for (int i = 28; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

int main(void) {
    printf("=== hashed_art basic test ===\n");

    hart_t tree;
    hart_init(&tree, 32);

    /* Insert 100 key-value pairs */
    uint8_t keys[100][32], vals[100][32];
    for (int i = 0; i < 100; i++) {
        make_key((uint32_t)i, keys[i]);
        memset(vals[i], (uint8_t)(i + 1), 32);
        hart_insert(&tree, keys[i], vals[i]);
    }
    printf("  inserted 100 entries, size=%zu\n", hart_size(&tree));

    /* Verify get */
    int found = 0;
    for (int i = 0; i < 100; i++) {
        const void *v = hart_get(&tree, keys[i]);
        if (v && memcmp(v, vals[i], 32) == 0) found++;
    }
    printf("  found %d/100\n", found);

    /* Compute root */
    uint8_t root1[32];
    hart_root_hash(&tree, encode_raw, NULL, root1);
    print_hash("root1", root1);

    /* Cross-validate with mem_mpt batch */
    mpt_batch_entry_t entries[100];
    uint8_t rlp_store[100][33];
    for (int i = 0; i < 100; i++) {
        memcpy(entries[i].key, keys[i], 32);
        rlp_store[i][0] = 0xa0;
        memcpy(rlp_store[i] + 1, vals[i], 32);
        entries[i].value = rlp_store[i];
        entries[i].value_len = 33;
    }
    hash_t batch_root;
    mpt_compute_root_batch(entries, 100, &batch_root);
    print_hash("batch", batch_root.bytes);
    printf("  match: %s\n", memcmp(root1, batch_root.bytes, 32) == 0 ? "YES" : "NO");

    /* Update some values and recompute (incremental) */
    for (int i = 0; i < 10; i++) {
        memset(vals[i], 0xFF, 32);
        hart_insert(&tree, keys[i], vals[i]);
    }
    uint8_t root2[32];
    hart_root_hash(&tree, encode_raw, NULL, root2);
    print_hash("root2 (incr)", root2);

    /* Batch reference for updated state */
    for (int i = 0; i < 10; i++) {
        rlp_store[i][0] = 0xa0;
        memcpy(rlp_store[i] + 1, vals[i], 32);
    }
    mpt_compute_root_batch(entries, 100, &batch_root);
    print_hash("batch2", batch_root.bytes);
    printf("  incr match: %s\n", memcmp(root2, batch_root.bytes, 32) == 0 ? "YES" : "NO");

    /* Different from first root */
    printf("  changed: %s\n", memcmp(root1, root2, 32) != 0 ? "YES" : "NO");

    hart_destroy(&tree);

    printf("\n=== hashed_art scale test (10K entries) ===\n");
    hart_t big;
    hart_init(&big, 32);
    for (int i = 0; i < 10000; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)(i + 1000), k);
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&big, k, v);
    }
    printf("  size=%zu arena=%zu/%zu\n", hart_size(&big), big.arena_used, big.arena_cap);

    uint8_t big_root[32];
    hart_root_hash(&big, encode_raw, NULL, big_root);
    print_hash("10K root", big_root);

    /* Batch reference */
    mpt_batch_entry_t *bent = malloc(10000 * sizeof(mpt_batch_entry_t));
    uint8_t (*brlp)[33] = malloc(10000 * 33);
    for (int i = 0; i < 10000; i++) {
        make_key((uint32_t)(i + 1000), bent[i].key);
        brlp[i][0] = 0xa0;
        memset(brlp[i] + 1, (uint8_t)(i & 0xFF), 32);
        bent[i].value = brlp[i];
        bent[i].value_len = 33;
    }
    mpt_compute_root_batch(bent, 10000, &batch_root);
    print_hash("batch 10K", batch_root.bytes);
    printf("  match: %s\n", memcmp(big_root, batch_root.bytes, 32) == 0 ? "YES" : "NO");

    free(bent);
    free(brlp);
    hart_destroy(&big);

    return 0;
}
