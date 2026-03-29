/*
 * MPT Trie (native) cross-validation against Python reference vectors.
 *
 * Uses mpt_trie_insert / mpt_trie_remove / mpt_trie_root_hash directly.
 * No batching — mutations are immediate.
 */

#include "mpt_trie.h"

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

static void apply_batch(mpt_trie_t *t, kv_entry_t *entries, uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        if (entries[i].value_len == 0) {
            mpt_trie_remove(t, entries[i].key);
        } else {
            mpt_trie_insert(t, entries[i].key,
                            entries[i].value, entries[i].value_len);
        }
    }
}

int main(int argc, char **argv) {
    const char *vectors_path = "database/tests/mpt_vectors.bin";
    if (argc > 1) vectors_path = argv[1];

    FILE *f = fopen(vectors_path, "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", vectors_path); return 1; }

    uint32_t num_scenarios = read_u32(f);
    printf("MPT Trie (native) Cross-Validation: %u scenarios\n", num_scenarios);
    printf("==================================================\n");

    int passed = 0, failed = 0, assertions = 0;

    for (uint32_t s = 0; s < num_scenarios; s++) {
        uint8_t scenario_type;
        read_exact(f, &scenario_type, 1);

        uint32_t initial_count;
        kv_entry_t *initial = read_kv_entries(f, &initial_count);
        uint8_t expected_root[32];
        read_exact(f, expected_root, 32);

        mpt_trie_t *t = mpt_trie_create();

        /* Build initial trie */
        apply_batch(t, initial, initial_count);

        uint8_t got_root[32];
        mpt_trie_root_hash(t, got_root);
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
                mpt_trie_destroy(t);
                continue;
            }

            apply_batch(t, dirty, dirty_count);
            mpt_trie_root_hash(t, got_root);
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
                printf("      got:  "); print_hash(got_root); printf("\n");
                printf("      exp:  "); print_hash(expected_root); printf("\n");
                failed++;
                for (uint8_t r = 0; r < num_rounds; r++) {
                    uint32_t dc; kv_entry_t *d = read_kv_entries(f, &dc);
                    free_entries(d, dc); uint8_t skip[32]; read_exact(f, skip, 32);
                }
                free_entries(initial, initial_count);
                mpt_trie_destroy(t);
                continue;
            }

            bool all_ok = true;
            int fail_round = -1;
            uint8_t fail_got[32], fail_exp[32];

            for (uint8_t r = 0; r < num_rounds; r++) {
                uint32_t dc; kv_entry_t *dirty = read_kv_entries(f, &dc);
                uint8_t exp_round[32]; read_exact(f, exp_round, 32);

                if (all_ok) {
                    apply_batch(t, dirty, dc);
                    mpt_trie_root_hash(t, got_root);
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

        mpt_trie_destroy(t);
        free_entries(initial, initial_count);
    }

    fclose(f);
    printf("\n%d/%d scenarios passed (%d assertions)\n",
           passed, passed + failed, assertions);
    return failed > 0 ? 1 : 0;
}
