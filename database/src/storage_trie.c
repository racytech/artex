/*
 * Storage Trie — per-account storage MPT roots from a shared compact_art.
 *
 * The compact_art has 64-byte keys (addr_hash[32] || slot_hash[32]).
 * The art_mpt subtree walk at prefix=addr_hash computes the per-account
 * storage root using slot_hash[32] as the MPT key.
 *
 * Leaf values in the compact_art are flat_store offsets (uint64_t).
 * The encode callback reads the actual compressed storage value from
 * the flat_store's mmap'd data file, then RLP-encodes it.
 */

#include "storage_trie.h"
#include "art_mpt.h"
#include "flat_store.h"

#include <stdlib.h>
#include <string.h>

struct storage_trie {
    art_mpt_t    *mpt;
    flat_store_t *store;   /* non-owning — for reading slot values */
};

/*
 * Encode callback: read compressed storage value from flat_store,
 * then RLP-encode as big-endian integer (strip leading zeros).
 *
 * flat_store stores values with leading zeros stripped (encode_storage).
 * We need to produce the RLP encoding of the original 32-byte BE value.
 * The compressed bytes ARE the significant bytes of the value.
 *
 * RLP rules for a byte string:
 *   len=0          → 0x80 (empty string)
 *   len=1, v<0x80  → v (single byte, self-describing)
 *   len=1..55      → (0x80+len) || bytes
 */
static uint32_t storage_value_encode(const uint8_t *key,
                                      const void *leaf_val,
                                      uint32_t val_size,
                                      uint8_t *rlp_out,
                                      void *user_ctx) {
    (void)key;
    (void)val_size;
    storage_trie_t *st = user_ctx;

    /* Read compressed value from flat_store */
    uint8_t raw[32];
    uint32_t raw_len = flat_store_read_leaf_record(st->store, leaf_val,
                                                    raw, sizeof(raw));

    /* raw contains the significant bytes (leading zeros already stripped).
     * RLP-encode as a byte string. */
    if (raw_len == 0) {
        rlp_out[0] = 0x80;
        return 1;
    }
    if (raw_len == 1 && raw[0] < 0x80) {
        rlp_out[0] = raw[0];
        return 1;
    }
    rlp_out[0] = 0x80 + (uint8_t)raw_len;
    memcpy(rlp_out + 1, raw, raw_len);
    return 1 + raw_len;
}

storage_trie_t *storage_trie_create(compact_art_t *storage_art,
                                      flat_store_t *storage_store) {
    if (!storage_art || !storage_store) return NULL;

    storage_trie_t *st = calloc(1, sizeof(*st));
    if (!st) return NULL;

    st->store = storage_store;
    st->mpt = art_mpt_create(storage_art, storage_value_encode, st);
    if (!st->mpt) {
        free(st);
        return NULL;
    }

    return st;
}

void storage_trie_destroy(storage_trie_t *st) {
    if (!st) return;
    art_mpt_destroy(st->mpt);
    free(st);
}

void storage_trie_root(storage_trie_t *st,
                         const uint8_t addr_hash[32],
                         uint8_t out[32]) {
    if (!st || !addr_hash || !out) return;
    art_mpt_subtree_hash(st->mpt, addr_hash, 32, out);
}

void storage_trie_invalidate_all(storage_trie_t *st) {
    if (!st) return;
    art_mpt_invalidate_all(st->mpt);
}
