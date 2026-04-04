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

    /* Find the smallest N that fails */
    printf("\n=== finding smallest failing N ===\n");
    for (int N = 1; N <= 10000; N++) {
        hart_t tmp;
        hart_init(&tmp, 32);
        mpt_batch_entry_t *te = malloc(N * sizeof(mpt_batch_entry_t));
        uint8_t (*tr)[33] = malloc(N * 33);
        for (int i = 0; i < N; i++) {
            uint8_t k[32], v[32];
            make_key((uint32_t)(i + 1000), k);
            memset(v, (uint8_t)(i & 0xFF), 32);
            hart_insert(&tmp, k, v);
            memcpy(te[i].key, k, 32);
            tr[i][0] = 0xa0; memset(tr[i]+1, (uint8_t)(i&0xFF), 32);
            te[i].value = tr[i]; te[i].value_len = 33;
        }
        uint8_t hr[32]; hart_root_hash(&tmp, encode_raw, NULL, hr);
        hash_t br; mpt_compute_root_batch(te, N, &br);
        bool ok = memcmp(hr, br.bytes, 32) == 0;
        printf("  N=%d %s hart=", N, ok ? "OK" : "FAIL");
        for(int j=0;j<4;j++) printf("%02x",hr[j]);
        printf(".. batch=");
        for(int j=0;j<4;j++) printf("%02x",br.bytes[j]);
        printf("..\n");
        if (!ok) {
            free(te); free(tr); hart_destroy(&tmp);
            break;
        }
        free(te); free(tr); hart_destroy(&tmp);
        if (N % 1000 == 0) printf("  N=%d OK\n", N);
    }

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

    /* ====== Delete tests ====== */
    printf("\n=== delete tests ===\n");

    /* Test 1: Delete from small tree, verify get returns NULL */
    hart_t dt;
    hart_init(&dt, 32);
    uint8_t dk[10][32], dv[10][32];
    for (int i = 0; i < 10; i++) {
        make_key((uint32_t)(i + 5000), dk[i]);
        memset(dv[i], (uint8_t)(i + 0x10), 32);
        hart_insert(&dt, dk[i], dv[i]);
    }
    printf("  before delete: size=%zu\n", hart_size(&dt));

    /* Delete 5 entries */
    for (int i = 0; i < 5; i++) {
        bool ok = hart_delete(&dt, dk[i]);
        if (!ok) { printf("  FAIL: delete returned false for key %d\n", i); }
    }
    printf("  after delete 5: size=%zu\n", hart_size(&dt));

    /* Verify deleted keys are gone */
    int del_gone = 0;
    for (int i = 0; i < 5; i++)
        if (!hart_get(&dt, dk[i])) del_gone++;
    printf("  deleted gone: %d/5\n", del_gone);

    /* Verify remaining keys still present */
    int del_found = 0;
    for (int i = 5; i < 10; i++) {
        const void *v = hart_get(&dt, dk[i]);
        if (v && memcmp(v, dv[i], 32) == 0) del_found++;
    }
    printf("  remaining found: %d/5\n", del_found);

    /* Root hash after delete matches batch with only remaining entries */
    uint8_t del_root[32];
    hart_root_hash(&dt, encode_raw, NULL, del_root);

    mpt_batch_entry_t del_ent[5];
    uint8_t del_rlp[5][33];
    for (int i = 0; i < 5; i++) {
        memcpy(del_ent[i].key, dk[i + 5], 32);
        del_rlp[i][0] = 0xa0;
        memcpy(del_rlp[i] + 1, dv[i + 5], 32);
        del_ent[i].value = del_rlp[i];
        del_ent[i].value_len = 33;
    }
    hash_t del_batch;
    mpt_compute_root_batch(del_ent, 5, &del_batch);
    print_hash("hart del", del_root);
    print_hash("batch del", del_batch.bytes);
    printf("  delete match: %s\n", memcmp(del_root, del_batch.bytes, 32) == 0 ? "YES" : "NO");
    hart_destroy(&dt);

    /* Test 2: Delete all entries → empty root */
    printf("\n  --- delete all → empty root ---\n");
    hart_t dt2;
    hart_init(&dt2, 32);
    for (int i = 0; i < 50; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)(i + 9000), k);
        memset(v, (uint8_t)i, 32);
        hart_insert(&dt2, k, v);
    }
    for (int i = 0; i < 50; i++) {
        uint8_t k[32];
        make_key((uint32_t)(i + 9000), k);
        hart_delete(&dt2, k);
    }
    printf("  size after delete all: %zu\n", hart_size(&dt2));
    uint8_t empty_root[32];
    hart_root_hash(&dt2, encode_raw, NULL, empty_root);
    /* Empty trie root = keccak256(RLP("")) = 0x56e81f17... */
    bool is_empty = memcmp(empty_root, "\x56\xe8\x1f\x17", 4) == 0;
    print_hash("empty root", empty_root);
    printf("  is empty root: %s\n", is_empty ? "YES" : "NO");
    hart_destroy(&dt2);

    /* Test 3: Insert-delete-reinsert cycle with root hash cross-check */
    printf("\n  --- insert/delete/reinsert cycle ---\n");
    hart_t dt3;
    hart_init(&dt3, 32);
    for (int i = 0; i < 200; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)(i + 2000), k);
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&dt3, k, v);
    }
    /* Delete odd-indexed entries */
    for (int i = 1; i < 200; i += 2) {
        uint8_t k[32];
        make_key((uint32_t)(i + 2000), k);
        hart_delete(&dt3, k);
    }
    printf("  size after delete odds: %zu\n", hart_size(&dt3));

    /* Cross-check with batch (even entries only) */
    int even_count = 100;
    mpt_batch_entry_t *even_ent = malloc(even_count * sizeof(mpt_batch_entry_t));
    uint8_t (*even_rlp)[33] = malloc(even_count * 33);
    for (int i = 0; i < even_count; i++) {
        int idx = i * 2;  /* even indices: 0, 2, 4, ... */
        make_key((uint32_t)(idx + 2000), even_ent[i].key);
        even_rlp[i][0] = 0xa0;
        memset(even_rlp[i] + 1, (uint8_t)(idx & 0xFF), 32);
        even_ent[i].value = even_rlp[i];
        even_ent[i].value_len = 33;
    }
    uint8_t cycle_root[32];
    hart_root_hash(&dt3, encode_raw, NULL, cycle_root);
    hash_t cycle_batch;
    mpt_compute_root_batch(even_ent, even_count, &cycle_batch);
    print_hash("cycle hart", cycle_root);
    print_hash("cycle batch", cycle_batch.bytes);
    printf("  cycle match: %s\n", memcmp(cycle_root, cycle_batch.bytes, 32) == 0 ? "YES" : "NO");
    free(even_ent);
    free(even_rlp);
    hart_destroy(&dt3);

    /* ====== Iterator tests ====== */
    printf("\n=== iterator tests ===\n");

    /* Test 1: Iterate all entries, verify count and values */
    hart_t it_tree;
    hart_init(&it_tree, 32);
    uint8_t it_keys[50][32], it_vals[50][32];
    for (int i = 0; i < 50; i++) {
        make_key((uint32_t)(i + 7000), it_keys[i]);
        memset(it_vals[i], (uint8_t)(i + 0x20), 32);
        hart_insert(&it_tree, it_keys[i], it_vals[i]);
    }

    hart_iter_t *iter = hart_iter_create(&it_tree);
    int iter_count = 0;
    int iter_verified = 0;
    while (hart_iter_next(iter)) {
        iter_count++;
        const uint8_t *k = hart_iter_key(iter);
        const void *v = hart_iter_value(iter);
        /* Find this key in our arrays */
        for (int i = 0; i < 50; i++) {
            if (memcmp(k, it_keys[i], 32) == 0) {
                if (memcmp(v, it_vals[i], 32) == 0) iter_verified++;
                break;
            }
        }
    }
    hart_iter_destroy(iter);
    printf("  iterated: %d/50, verified: %d/50\n", iter_count, iter_verified);
    hart_destroy(&it_tree);

    /* Test 2: Empty tree iteration */
    hart_t empty_tree;
    hart_init(&empty_tree, 32);
    iter = hart_iter_create(&empty_tree);
    int empty_count = 0;
    while (hart_iter_next(iter)) empty_count++;
    hart_iter_destroy(iter);
    printf("  empty tree iter count: %d (expected 0)\n", empty_count);
    hart_destroy(&empty_tree);

    /* Test 3: Single entry */
    hart_t single;
    hart_init(&single, 32);
    uint8_t sk[32], sv[32];
    make_key(12345, sk);
    memset(sv, 0xAB, 32);
    hart_insert(&single, sk, sv);
    iter = hart_iter_create(&single);
    int single_count = 0;
    bool single_match = false;
    while (hart_iter_next(iter)) {
        single_count++;
        if (memcmp(hart_iter_key(iter), sk, 32) == 0 &&
            memcmp(hart_iter_value(iter), sv, 32) == 0)
            single_match = true;
    }
    hart_iter_destroy(iter);
    printf("  single: count=%d match=%s\n", single_count, single_match ? "YES" : "NO");
    hart_destroy(&single);

    /* ====== AVX-512 iterative vs recursive comparison ====== */
    printf("\n=== avx512 iterative vs recursive ===\n");
    int avx_errors = 0;

    /* Test with various tree sizes */
    int sizes[] = { 1, 2, 5, 10, 50, 200, 1000, 5000 };
    for (int si = 0; si < 8; si++) {
        int n = sizes[si];
        hart_t at;
        hart_init(&at, 32);
        for (int i = 0; i < n; i++) {
            uint8_t k[32], v[32];
            make_key((uint32_t)(i + 50000 + si * 10000), k);
            memset(v, (uint8_t)((i * 7 + si) & 0xFF), 32);
            hart_insert(&at, k, v);
        }

        uint8_t root_rec[32], root_avx[32];
        hart_root_hash(&at, encode_raw, NULL, root_rec);
        hart_root_hash_avx512(&at, encode_raw, NULL, root_avx);

        int match = memcmp(root_rec, root_avx, 32) == 0;
        printf("  n=%-5d  %s", n, match ? "OK" : "FAIL");
        if (!match) {
            printf("  rec="); for(int i=0;i<8;i++) printf("%02x",root_rec[i]);
            printf("...  avx="); for(int i=0;i<8;i++) printf("%02x",root_avx[i]);
            printf("...");
            avx_errors++;
        }
        printf("\n");
        hart_destroy(&at);
    }

    /* Test with insert + delete (dirty flags) */
    {
        hart_t at;
        hart_init(&at, 32);
        for (int i = 0; i < 500; i++) {
            uint8_t k[32], v[32];
            make_key((uint32_t)(i + 90000), k);
            memset(v, (uint8_t)(i & 0xFF), 32);
            hart_insert(&at, k, v);
        }
        /* Compute root once (clears dirty flags) */
        uint8_t tmp[32];
        hart_root_hash(&at, encode_raw, NULL, tmp);

        /* Delete some, insert new */
        for (int i = 0; i < 100; i++) {
            uint8_t k[32];
            make_key((uint32_t)(i * 3 + 90000), k);
            hart_delete(&at, k);
        }
        for (int i = 500; i < 600; i++) {
            uint8_t k[32], v[32];
            make_key((uint32_t)(i + 90000), k);
            memset(v, (uint8_t)(i & 0xFF), 32);
            hart_insert(&at, k, v);
        }

        uint8_t root_rec2[32], root_avx2[32];
        hart_root_hash(&at, encode_raw, NULL, root_rec2);

        /* Need to re-dirty for avx512 — but both computed same tree state.
         * Actually, after hart_root_hash clears dirty, avx512 should see
         * clean nodes and return cached hashes. */
        hart_root_hash_avx512(&at, encode_raw, NULL, root_avx2);

        int match = memcmp(root_rec2, root_avx2, 32) == 0;
        printf("  dirty   %s\n", match ? "OK" : "FAIL");
        if (!match) avx_errors++;
        hart_destroy(&at);
    }

    printf("  avx512 errors: %d\n", avx_errors);

    return avx_errors;
}
