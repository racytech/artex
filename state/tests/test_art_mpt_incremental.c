/**
 * Test art_mpt incremental root computation.
 *
 * Reproduces the exact flow from state.c:
 *   1. Insert 2 accounts → compute root (genesis)
 *   2. Update both + insert 1 more → compute root (post-tx)
 *   3. Compare post-tx root with mem_mpt batch (known correct)
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

/* Value record: length prefix + raw data (like mpt_fuzz) */
typedef struct {
    uint16_t len;
    uint8_t  data[256];
} val_rec_t;

static uint32_t encode_raw(const uint8_t *key, const void *leaf_val,
                            uint32_t val_size, uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx; (void)val_size;
    const val_rec_t *r = leaf_val;
    memcpy(rlp_out, r->data, r->len);
    return r->len;
}

static void insert_val(mem_art_t *tree, const uint8_t key[32],
                       const uint8_t *data, size_t len) {
    val_rec_t rec = {0};
    rec.len = (uint16_t)len;
    memcpy(rec.data, data, len);
    mem_art_insert(tree, key, 32, &rec, sizeof(rec));
}

static void print_hash(const char *label, const uint8_t *h) {
    printf("%s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

/* Compute root from scratch using mem_mpt batch */
static void batch_root(const uint8_t keys[][32], const uint8_t *vals[], const size_t *val_lens,
                       size_t n, uint8_t out[32]) {
    mpt_batch_entry_t *entries = calloc(n, sizeof(mpt_batch_entry_t));
    for (size_t i = 0; i < n; i++) {
        memcpy(entries[i].key, keys[i], 32);
        entries[i].value = vals[i];
        entries[i].value_len = val_lens[i];
    }
    hash_t root;
    mpt_compute_root_batch(entries, n, &root);
    memcpy(out, root.bytes, 32);
    free(entries);
}

int main(void) {
    /* Keys and values from the precompile 5 Berlin test */

    /* Genesis: 2 accounts */
    uint8_t k1[32], k2[32];
    hex2bin("47e2672d1bd341fd13f3a9bd6fcb81159e21feb77b1b57022175b30e5793e542", k1, 32);
    hex2bin("f17c57e9781bb80c467568c9c7c12c63aaf9a74cdf7c2d76d5b71b284dcd959a", k2, 32);

    uint8_t v1_gen[70], v2_gen[79];
    hex2bin("f8440180a04ae3a24841e4183a9b6b03501362e471ce86eb0866055d8ddb2514d0b74818caa0a8b8cdc63cb795487043cc5a7843f1ad3fe16b9d736005b76073ab4b7a3311e1", v1_gen, 70);
    hex2bin("f84d80893635c9adc5dea00000a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470", v2_gen, 79);

    /* Post-tx: updated values + new entry */
    uint8_t v1_post[70], v2_post[79], k3[32], v3[73];
    hex2bin("f8440180a0d827c5bb1889465b200f4562839bd76fc209e3bd8e6106e87edd850e368316b0a0a8b8cdc63cb795487043cc5a7843f1ad3fe16b9d736005b76073ab4b7a3311e1", v1_post, 70);
    hex2bin("f84d01893635c9adc5de9b8c1ea056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470", v2_post, 79);
    hex2bin("9d860e7bb7e6b09b87ab7406933ef2980c19d7d0192d8939cf6dc6908a03305f", k3, 32);
    hex2bin("f84780830473e2a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470", v3, 73);

    /* --- Expected root (from Python HexaryTrie) --- */
    uint8_t expected[32];
    hex2bin("69395631019141fe1a0e8c9b7faa1be5cf67884f6acea7e07dd8172a7f5c5ef2", expected, 32);

    /* --- Batch root (all 3 final values, from scratch) --- */
    {
        const uint8_t *vals[] = {v1_post, v2_post, v3};
        uint8_t keys[][32] = {{0},{0},{0}};
        memcpy(keys[0], k1, 32); memcpy(keys[1], k2, 32); memcpy(keys[2], k3, 32);
        size_t lens[] = {70, 79, 73};
        uint8_t br[32];
        batch_root(keys, vals, lens, 3, br);
        print_hash("batch (3 final)", br);
        printf("  match expected: %s\n", memcmp(br, expected, 32) == 0 ? "YES" : "NO");
    }

    /* --- art_mpt: fresh insert all 3 at once --- */
    {
        mem_art_t tree;
        mem_art_init(&tree);
        insert_val(&tree, k1, v1_post, 70);
        insert_val(&tree, k2, v2_post, 79);
        insert_val(&tree, k3, v3, 73);

        art_iface_mem_ctx_t ctx = { .tree = &tree, .key_size = 32, .value_size = sizeof(val_rec_t) };
        art_mpt_t *am = art_mpt_create_iface(art_iface_mem(&ctx), encode_raw, NULL);
        uint8_t root[32];
        art_mpt_root_hash(am, root);
        print_hash("art_mpt fresh", root);
        printf("  match expected: %s\n", memcmp(root, expected, 32) == 0 ? "YES" : "NO");
        art_mpt_destroy(am);
        mem_art_destroy(&tree);
    }

    /* --- art_mpt: incremental (genesis then post-tx) --- */
    {
        mem_art_t tree;
        mem_art_init(&tree);

        art_iface_mem_ctx_t ctx = { .tree = &tree, .key_size = 32, .value_size = sizeof(val_rec_t) };
        art_mpt_t *am = art_mpt_create_iface(art_iface_mem(&ctx), encode_raw, NULL);

        /* Genesis: insert 2 accounts */
        insert_val(&tree, k1, v1_gen, 70);
        insert_val(&tree, k2, v2_gen, 79);

        uint8_t genesis_root[32];
        art_mpt_root_hash(am, genesis_root);
        print_hash("genesis (2 accts)", genesis_root);

        /* Post-tx: update both + insert new */
        insert_val(&tree, k1, v1_post, 70);  /* update k1 */
        insert_val(&tree, k2, v2_post, 79);  /* update k2 */
        insert_val(&tree, k3, v3, 73);        /* insert k3 */

        uint8_t posttx_root[32];
        art_mpt_root_hash(am, posttx_root);
        print_hash("incremental (3 accts)", posttx_root);
        printf("  match expected: %s\n", memcmp(posttx_root, expected, 32) == 0 ? "YES" : "NO");

        art_mpt_destroy(am);
        mem_art_destroy(&tree);
    }

    print_hash("expected", expected);
    return 0;
}
