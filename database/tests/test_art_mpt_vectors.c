/*
 * ART→MPT cross-validation against Python reference vectors.
 *
 * Inserts keys+values into a compact_art, then calls art_mpt_root_hash
 * and compares the root hash against the Python HexaryTrie reference.
 *
 * The value_encode callback returns the raw value bytes as-is (the test
 * vectors store pre-encoded leaf values, not account/storage records).
 */

#include "art_mpt.h"
#include "compact_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_hash(const uint8_t h[32]) {
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
}

static bool read_exact(FILE *f, void *buf, size_t n) {
    return fread(buf, 1, n, f) == n;
}
static uint32_t read_u32(FILE *f) { uint32_t v; read_exact(f, &v, 4); return v; }
static uint16_t read_u16(FILE *f) { uint16_t v; read_exact(f, &v, 2); return v; }

typedef struct {
    uint8_t key[32];
    uint8_t *value;
    uint16_t value_len;
} kv_entry_t;

static kv_entry_t *read_kv_entries(FILE *f, uint32_t *out_count) {
    uint32_t n = read_u32(f);
    *out_count = n;
    if (n == 0) return NULL;
    kv_entry_t *entries = calloc(n, sizeof(*entries));
    for (uint32_t i = 0; i < n; i++) {
        read_exact(f, entries[i].key, 32);
        entries[i].value_len = read_u16(f);
        if (entries[i].value_len > 0) {
            entries[i].value = malloc(entries[i].value_len);
            read_exact(f, entries[i].value, entries[i].value_len);
        }
    }
    return entries;
}

static void free_entries(kv_entry_t *entries, uint32_t count) {
    if (!entries) return;
    for (uint32_t i = 0; i < count; i++) free(entries[i].value);
    free(entries);
}

/* Value encoder: the compact_art stores (value_len:2 + value_data) as leaf value.
 * We return value_data as the RLP (test vectors use raw values). */
typedef struct {
    uint16_t value_len;
    uint8_t  value_data[];
} leaf_record_t;

static uint32_t test_value_encode(const uint8_t *key, const void *leaf_val,
                                   uint32_t val_size, uint8_t *rlp_out,
                                   void *ctx) {
    (void)key; (void)ctx;
    const leaf_record_t *rec = leaf_val;
    if (rec->value_len == 0) return 0;
    memcpy(rlp_out, rec->value_data, rec->value_len);
    return rec->value_len;
}

/* Dummy key_fetch for compact_leaves mode */
static bool dummy_fetch(const void *val, uint8_t *key_out, void *ctx) {
    (void)val; (void)key_out; (void)ctx;
    return false;
}

/* Value size for compact_art: 2-byte length prefix + max value data.
 * We use a fixed max because compact_art needs uniform leaf values. */
#define MAX_VAL 256
#define LEAF_VAL_SIZE (2 + MAX_VAL)

int main(int argc, char **argv) {
    const char *vectors_path = "database/tests/mpt_vectors.bin";
    if (argc > 1) vectors_path = argv[1];

    FILE *f = fopen(vectors_path, "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", vectors_path); return 1; }

    uint32_t num_scenarios = read_u32(f);
    printf("ART→MPT Cross-Validation: %u scenarios\n", num_scenarios);
    printf("==========================================\n");

    int passed = 0, failed = 0, assertions = 0;

    for (uint32_t s = 0; s < num_scenarios; s++) {
        uint8_t scenario_type;
        read_exact(f, &scenario_type, 1);

        uint32_t initial_count;
        kv_entry_t *initial = read_kv_entries(f, &initial_count);
        uint8_t expected_root[32];
        read_exact(f, expected_root, 32);

        /* Build compact_art with initial entries.
         * Use full keys (not compact_leaves) for simplicity. */
        compact_art_t tree;
        compact_art_init(&tree, 32, LEAF_VAL_SIZE, false, dummy_fetch, NULL);

        for (uint32_t i = 0; i < initial_count; i++) {
            leaf_record_t *rec = calloc(1, LEAF_VAL_SIZE);
            rec->value_len = initial[i].value_len;
            if (initial[i].value_len > 0 && initial[i].value_len <= MAX_VAL)
                memcpy(rec->value_data, initial[i].value, initial[i].value_len);
            compact_art_insert(&tree, initial[i].key, rec);
            free(rec);
        }

        uint8_t got_root[32];
        art_mpt_root_hash(&tree, test_value_encode, NULL, got_root);
        assertions++;

        bool build_ok = (memcmp(got_root, expected_root, 32) == 0);

        if (scenario_type == 0) {
            printf("  %2u. build  (%4u keys)       ", s + 1, initial_count);
            if (build_ok) { printf("PASS\n"); passed++; }
            else {
                printf("FAIL\n");
                printf("      got:  "); print_hash(got_root); printf("\n");
                printf("      exp:  "); print_hash(expected_root); printf("\n");
                failed++;
            }

        } else if (scenario_type == 1) {
            uint32_t dirty_count;
            kv_entry_t *dirty = read_kv_entries(f, &dirty_count);
            uint8_t expected_updated[32];
            read_exact(f, expected_updated, 32);

            printf("  %2u. update (%4u + %3u dirty) ", s + 1,
                   initial_count, dirty_count);

            if (!build_ok) {
                printf("FAIL (build)\n");
                printf("      got:  "); print_hash(got_root); printf("\n");
                printf("      exp:  "); print_hash(expected_root); printf("\n");
                failed++;
                free_entries(dirty, dirty_count);
                free_entries(initial, initial_count);
                compact_art_destroy(&tree);
                continue;
            }

            /* Apply dirty batch to compact_art */
            for (uint32_t i = 0; i < dirty_count; i++) {
                if (dirty[i].value_len == 0) {
                    compact_art_delete(&tree, dirty[i].key);
                } else {
                    leaf_record_t *rec = calloc(1, LEAF_VAL_SIZE);
                    rec->value_len = dirty[i].value_len;
                    if (dirty[i].value_len <= MAX_VAL)
                        memcpy(rec->value_data, dirty[i].value, dirty[i].value_len);
                    compact_art_insert(&tree, dirty[i].key, rec);
                    free(rec);
                }
            }

            art_mpt_root_hash(&tree, test_value_encode, NULL, got_root);
            assertions++;

            if (memcmp(got_root, expected_updated, 32) == 0) {
                printf("PASS\n"); passed++;
            } else {
                printf("FAIL (update)\n");
                printf("      got:  "); print_hash(got_root); printf("\n");
                printf("      exp:  "); print_hash(expected_updated); printf("\n");
                failed++;
            }
            free_entries(dirty, dirty_count);

        } else if (scenario_type == 2) {
            uint8_t num_rounds;
            read_exact(f, &num_rounds, 1);
            printf("  %2u. multi  (%4u + %u rounds) ", s + 1,
                   initial_count, num_rounds);

            if (!build_ok) {
                printf("FAIL (build)\n");
                failed++;
                for (uint8_t r = 0; r < num_rounds; r++) {
                    uint32_t dc; kv_entry_t *d = read_kv_entries(f, &dc);
                    free_entries(d, dc); uint8_t skip[32]; read_exact(f, skip, 32);
                }
                free_entries(initial, initial_count);
                compact_art_destroy(&tree);
                continue;
            }

            bool all_ok = true;
            int fail_round = -1;
            uint8_t fail_got[32], fail_exp[32];

            for (uint8_t r = 0; r < num_rounds; r++) {
                uint32_t dc; kv_entry_t *dirty = read_kv_entries(f, &dc);
                uint8_t exp_round[32]; read_exact(f, exp_round, 32);

                if (all_ok) {
                    for (uint32_t i = 0; i < dc; i++) {
                        if (dirty[i].value_len == 0) {
                            compact_art_delete(&tree, dirty[i].key);
                        } else {
                            leaf_record_t *rec = calloc(1, LEAF_VAL_SIZE);
                            rec->value_len = dirty[i].value_len;
                            if (dirty[i].value_len <= MAX_VAL)
                                memcpy(rec->value_data, dirty[i].value, dirty[i].value_len);
                            compact_art_insert(&tree, dirty[i].key, rec);
                            free(rec);
                        }
                    }

                    art_mpt_root_hash(&tree, test_value_encode, NULL, got_root);
                    assertions++;

                    if (memcmp(got_root, exp_round, 32) != 0) {
                        all_ok = false;
                        fail_round = r + 1;
                        memcpy(fail_got, got_root, 32);
                        memcpy(fail_exp, exp_round, 32);
                    }
                }
                free_entries(dirty, dc);
            }

            if (all_ok) { printf("PASS\n"); passed++; }
            else {
                printf("FAIL (round %d)\n", fail_round);
                printf("      got:  "); print_hash(fail_got); printf("\n");
                printf("      exp:  "); print_hash(fail_exp); printf("\n");
                failed++;
            }
        }

        compact_art_destroy(&tree);
        free_entries(initial, initial_count);
    }

    fclose(f);
    printf("\n%d/%d scenarios passed (%d assertions)\n",
           passed, passed + failed, assertions);
    return failed > 0 ? 1 : 0;
}
