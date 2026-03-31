/*
 * Storage Trie — per-account storage MPT roots from a shared compact_art.
 *
 * Dual-path encode callback:
 *   - OVERLAY_BIT set + meta_pool: read current value from slot meta
 *   - Otherwise: read compressed value from flat_store (disk)
 */

#include "storage_trie.h"
#include "art_mpt.h"
#include "flat_store.h"
#include "state_meta.h"

#include <stdlib.h>
#include <string.h>

#define OVERLAY_BIT (1ULL << 63)
#define OVERLAY_IDX(v) ((uint32_t)((v) & 0x7FFFFFFF))

struct storage_trie {
    art_mpt_t        *mpt;
    flat_store_t     *store;
    slot_meta_pool_t *meta_pool;
};

static uint32_t storage_value_encode(const uint8_t *key,
                                      const void *leaf_val,
                                      uint32_t val_size,
                                      uint8_t *rlp_out,
                                      void *user_ctx) {
    (void)key; (void)val_size;
    storage_trie_t *st = user_ctx;

    uint64_t offset;
    memcpy(&offset, leaf_val, sizeof(uint64_t));

    uint8_t be[32];
    const uint8_t *raw;
    uint32_t raw_len;

    if ((offset & OVERLAY_BIT) && st->meta_pool) {
        /* Meta path: read current value directly */
        uint32_t idx = OVERLAY_IDX(offset);
        if (idx < st->meta_pool->capacity) {
            cached_slot_t *cs = &st->meta_pool->entries[idx];
            uint256_to_bytes(&cs->current, be);
            /* Strip leading zeros */
            size_t i = 0;
            while (i < 32 && be[i] == 0) i++;
            raw = be + i;
            raw_len = (uint32_t)(32 - i);
        } else {
            return 0;
        }
    } else {
        /* Disk path: read compressed value from flat_store */
        static __thread uint8_t disk_buf[32];
        raw_len = flat_store_read_leaf_record(st->store, leaf_val,
                                               disk_buf, sizeof(disk_buf));
        raw = disk_buf;
    }

    /* RLP-encode as byte string */
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
                                      flat_store_t *storage_store,
                                      slot_meta_pool_t *meta_pool) {
    if (!storage_art || !storage_store) return NULL;
    storage_trie_t *st = calloc(1, sizeof(*st));
    if (!st) return NULL;
    st->store = storage_store;
    st->meta_pool = meta_pool;
    st->mpt = art_mpt_create(storage_art, storage_value_encode, st);
    if (!st->mpt) { free(st); return NULL; }
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
