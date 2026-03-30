/**
 * Verify flat_state root — opens flat_state files, computes account_trie_root.
 * Usage: ./verify_flat_root <flat_base_path>
 */
#include "flat_state.h"
#include "flat_store.h"
#include "account_trie.h"
#include "compact_art.h"
#include <stdio.h>
#include <string.h>

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <flat_base_path>\n", argv[0]);
        return 1;
    }

    const char *path = argv[1];
    fprintf(stderr, "Opening flat_state at %s...\n", path);

    flat_state_t *fs = flat_state_open(path);
    if (!fs) {
        fprintf(stderr, "Failed to open flat_state\n");
        return 1;
    }

    uint64_t acct_count = flat_state_account_count(fs);
    uint64_t stor_count = flat_state_storage_count(fs);
    fprintf(stderr, "Loaded: %lu accounts, %lu storage slots\n",
            (unsigned long)acct_count, (unsigned long)stor_count);

    compact_art_t *a_art = flat_state_account_art(fs);
    flat_store_t *a_store = flat_state_account_store(fs);

    account_trie_t *at = account_trie_create(a_art, a_store);
    if (!at) {
        fprintf(stderr, "Failed to create account_trie\n");
        flat_state_destroy(fs);
        return 1;
    }

    uint8_t root[32];
    account_trie_root(at, root);

    printf("Root: 0x");
    for (int i = 0; i < 32; i++) printf("%02x", root[i]);
    printf("\n");

    account_trie_destroy(at);
    flat_state_destroy(fs);
    return 0;
}
