/*
 * Flat State — O(1) Account/Storage Lookups via flat_store.
 *
 * Two flat_store instances:
 *   accounts: key=32B (keccak256(addr)) → record=up to 104B (variable via size classes)
 *   storage:  key=64B (addr_hash||slot_hash) → record=32B
 *
 * Account records use size classes to save space:
 *   - Empty EOAs (nonce + small balance): ~12 bytes
 *   - Funded EOAs (nonce + balance + code_hash): ~44 bytes
 *   - Full contracts (nonce + balance + code_hash + storage_root): 104 bytes
 *
 * Storage keys are 64-byte composites (addr_hash || slot_hash).
 * With compact_leaves=true, leaf size is 8 bytes regardless of key size.
 */

#include "flat_state.h"
#include "flat_store.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ACCT_KEY_SIZE      32
#define ACCT_MAX_REC_SIZE  104   /* max compressed account record */
#define STOR_KEY_SIZE      64   /* addr_hash[32] || slot_hash[32] */
#define STOR_MAX_REC_SIZE  32

/* =========================================================================
 * Account Record Compression
 *
 * Encoded format:
 *   [1B flags][variable fields]
 *     bit 0: has_nonce (nonce > 0) → 4B or 8B follows
 *     bit 1: has_balance (balance != 0) → 8B or 32B follows
 *     bit 2: balance_is_full (needs 32B, not 8B)
 *     bit 3: has_code (code_hash != EMPTY_CODE && != zero)  → 32B follows
 *     bit 4: has_storage (storage_root != EMPTY_STORAGE && != zero) → 32B follows
 *     bit 5: nonce_is_big (needs 8B, not 4B)
 *
 * Sizes:
 *   Empty EOA:        1 byte
 *   Funded EOA:       1 + 4 + 8 = 13 bytes
 *   Full contract:    1 + 4 + 8 + 32 + 32 = 77 bytes
 * ========================================================================= */

#define ACCT_FLAG_HAS_NONCE      0x01
#define ACCT_FLAG_HAS_BALANCE    0x02
#define ACCT_FLAG_BALANCE_FULL   0x04
#define ACCT_FLAG_HAS_CODE       0x08
#define ACCT_FLAG_HAS_STORAGE    0x10
#define ACCT_FLAG_NONCE_BIG      0x20

static inline bool is_zero_32(const uint8_t v[32]) {
    const uint64_t *p = (const uint64_t *)v;
    return (p[0] | p[1] | p[2] | p[3]) == 0;
}

/* Returns true if balance fits in 8 bytes (first 24 bytes are zero) */
static inline bool balance_fits_8(const uint8_t bal[32]) {
    const uint64_t *p = (const uint64_t *)bal;
    return (p[0] | p[1] | p[2]) == 0;
}

static uint32_t encode_account(const flat_account_record_t *rec, uint8_t *buf) {
    uint8_t flags = 0;
    uint32_t pos = 1; /* skip flags byte */

    if (rec->nonce > 0) {
        flags |= ACCT_FLAG_HAS_NONCE;
        if (rec->nonce > UINT32_MAX) {
            flags |= ACCT_FLAG_NONCE_BIG;
            uint64_t n = rec->nonce;
            memcpy(buf + pos, &n, 8);
            pos += 8;
        } else {
            uint32_t n = (uint32_t)rec->nonce;
            memcpy(buf + pos, &n, 4);
            pos += 4;
        }
    }

    if (!is_zero_32(rec->balance)) {
        flags |= ACCT_FLAG_HAS_BALANCE;
        if (balance_fits_8(rec->balance)) {
            memcpy(buf + pos, rec->balance + 24, 8);
            pos += 8;
        } else {
            flags |= ACCT_FLAG_BALANCE_FULL;
            memcpy(buf + pos, rec->balance, 32);
            pos += 32;
        }
    }

    if (!is_zero_32(rec->code_hash) &&
        memcmp(rec->code_hash, HASH_EMPTY_CODE.bytes, 32) != 0) {
        flags |= ACCT_FLAG_HAS_CODE;
        memcpy(buf + pos, rec->code_hash, 32);
        pos += 32;
    }

    if (!is_zero_32(rec->storage_root) &&
        memcmp(rec->storage_root, HASH_EMPTY_STORAGE.bytes, 32) != 0) {
        flags |= ACCT_FLAG_HAS_STORAGE;
        memcpy(buf + pos, rec->storage_root, 32);
        pos += 32;
    }


    buf[0] = flags;
    return pos;
}

static void decode_account(const uint8_t *buf, uint32_t len,
                            flat_account_record_t *out) {
    memset(out, 0, sizeof(*out));
    if (len == 0) return;

    uint8_t flags = buf[0];
    uint32_t pos = 1;

    if (flags & ACCT_FLAG_HAS_NONCE) {
        if (flags & ACCT_FLAG_NONCE_BIG) {
            memcpy(&out->nonce, buf + pos, 8);
            pos += 8;
        } else {
            uint32_t n;
            memcpy(&n, buf + pos, 4);
            out->nonce = n;
            pos += 4;
        }
    }

    if (flags & ACCT_FLAG_HAS_BALANCE) {
        if (flags & ACCT_FLAG_BALANCE_FULL) {
            memcpy(out->balance, buf + pos, 32);
            pos += 32;
        } else {
            /* 8 bytes in last position of 32-byte big-endian */
            memcpy(out->balance + 24, buf + pos, 8);
            pos += 8;
        }
    }

    if (flags & ACCT_FLAG_HAS_CODE) {
        memcpy(out->code_hash, buf + pos, 32);
        pos += 32;
    } else {
        memcpy(out->code_hash, HASH_EMPTY_CODE.bytes, 32);
    }

    if (flags & ACCT_FLAG_HAS_STORAGE) {
        memcpy(out->storage_root, buf + pos, 32);
        pos += 32;
    } else {
        memcpy(out->storage_root, HASH_EMPTY_STORAGE.bytes, 32);
    }

    (void)len; /* consumed by pos */
}

/* =========================================================================
 * Storage Value Compression
 *
 * Strip leading zeros from 32-byte big-endian value.
 * Encoded: just the significant bytes (length from slot header).
 * ========================================================================= */

static uint32_t encode_storage(const uint8_t value[32], uint8_t *buf) {
    /* Find first non-zero byte */
    int start = 0;
    while (start < 32 && value[start] == 0) start++;
    uint32_t len = 32 - start;
    if (len > 0) memcpy(buf, value + start, len);
    return len;
}

static void decode_storage(const uint8_t *buf, uint32_t len, uint8_t value[32]) {
    memset(value, 0, 32);
    if (len > 0 && len <= 32)
        memcpy(value + 32 - len, buf, len);
}

struct flat_state {
    flat_store_t *accounts;
    flat_store_t *storage;
    char        *acct_path;
    char        *stor_path;
};

/* =========================================================================
 * Helpers
 * ========================================================================= */

static char *make_path(const char *base, const char *suffix) {
    size_t blen = strlen(base);
    size_t slen = strlen(suffix);
    char *p = malloc(blen + slen + 1);
    if (!p) return NULL;
    memcpy(p, base, blen);
    memcpy(p + blen, suffix, slen + 1);
    return p;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

flat_state_t *flat_state_create(const char *path) {
    if (!path) return NULL;

    char *acct_path = make_path(path, "_acct.art");
    char *stor_path = make_path(path, "_stor.art");
    if (!acct_path || !stor_path) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_store_t *accounts = flat_store_create(acct_path, ACCT_KEY_SIZE, ACCT_MAX_REC_SIZE);
    if (!accounts) {
        fprintf(stderr, "flat_state: failed to create account store at %s\n", acct_path);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_store_t *storage = flat_store_create(stor_path, STOR_KEY_SIZE, STOR_MAX_REC_SIZE);
    if (!storage) {
        fprintf(stderr, "flat_state: failed to create storage store at %s\n", stor_path);
        flat_store_destroy(accounts);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_state_t *fs = calloc(1, sizeof(*fs));
    if (!fs) {
        flat_store_destroy(accounts);
        flat_store_destroy(storage);
        free(acct_path); free(stor_path);
        return NULL;
    }

    fs->accounts  = accounts;
    fs->storage   = storage;
    fs->acct_path = acct_path;
    fs->stor_path = stor_path;
    return fs;
}

flat_state_t *flat_state_open(const char *path) {
    if (!path) return NULL;

    char *acct_path = make_path(path, "_acct.art");
    char *stor_path = make_path(path, "_stor.art");
    if (!acct_path || !stor_path) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_store_t *accounts = flat_store_open(acct_path);
    if (!accounts) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_store_t *storage = flat_store_open(stor_path);
    if (!storage) {
        flat_store_destroy(accounts);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_state_t *fs = calloc(1, sizeof(*fs));
    if (!fs) {
        flat_store_destroy(accounts);
        flat_store_destroy(storage);
        free(acct_path); free(stor_path);
        return NULL;
    }

    fs->accounts  = accounts;
    fs->storage   = storage;
    fs->acct_path = acct_path;
    fs->stor_path = stor_path;
    return fs;
}

void flat_state_destroy(flat_state_t *fs) {
    if (!fs) return;
    flat_store_destroy(fs->accounts);
    flat_store_destroy(fs->storage);
    free(fs->acct_path);
    free(fs->stor_path);
    free(fs);
}

/* =========================================================================
 * Account Operations
 * ========================================================================= */

bool flat_state_get_account(const flat_state_t *fs, const uint8_t addr_hash[32],
                             flat_account_record_t *out) {
    if (!fs || !addr_hash || !out) return false;
    uint8_t buf[ACCT_MAX_REC_SIZE];
    uint32_t out_len = 0;
    if (!flat_store_get(fs->accounts, addr_hash, buf, sizeof(buf), &out_len))
        return false;
    decode_account(buf, out_len, out);
    return true;
}

bool flat_state_put_account(flat_state_t *fs, const uint8_t addr_hash[32],
                             const flat_account_record_t *record) {
    if (!fs || !addr_hash || !record) return false;
    uint8_t buf[ACCT_MAX_REC_SIZE];
    uint32_t len = encode_account(record, buf);
    return flat_store_put(fs->accounts, addr_hash, buf, len);
}

bool flat_state_delete_account(flat_state_t *fs, const uint8_t addr_hash[32]) {
    if (!fs || !addr_hash) return false;
    return flat_store_delete(fs->accounts, addr_hash);
}

/* =========================================================================
 * Storage Operations
 * ========================================================================= */

/* Combine addr_hash and slot_hash into a 32-byte storage key via XOR.
 * Both inputs are keccak256 outputs (independent, uniform). XOR preserves
 * uniformity — collision probability is 1/2^256, same as keccak256. */
static inline void make_stor_key(const uint8_t addr_hash[32],
                                  const uint8_t slot_hash[32],
                                  uint8_t out[64]) {
    memcpy(out, addr_hash, 32);
    memcpy(out + 32, slot_hash, 32);
}

bool flat_state_get_storage(const flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             uint8_t value[32]) {
    if (!fs || !addr_hash || !slot_hash || !value) return false;
    uint8_t key[STOR_KEY_SIZE];
    make_stor_key(addr_hash, slot_hash, key);
    uint8_t buf[32];
    uint32_t out_len = 0;
    if (!flat_store_get(fs->storage, key, buf, sizeof(buf), &out_len))
        return false;
    decode_storage(buf, out_len, value);
    return true;
}

bool flat_state_put_storage(flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             const uint8_t value[32]) {
    if (!fs || !addr_hash || !slot_hash || !value) return false;
    uint8_t key[STOR_KEY_SIZE];
    make_stor_key(addr_hash, slot_hash, key);
    uint8_t buf[32];
    uint32_t len = encode_storage(value, buf);
    return flat_store_put(fs->storage, key, buf, len);
}

bool flat_state_delete_storage(flat_state_t *fs,
                                const uint8_t addr_hash[32],
                                const uint8_t slot_hash[32]) {
    if (!fs || !addr_hash || !slot_hash) return false;
    uint8_t key[STOR_KEY_SIZE];
    make_stor_key(addr_hash, slot_hash, key);
    return flat_store_delete(fs->storage, key);
}

uint64_t flat_state_delete_all_storage(flat_state_t *fs,
                                        const uint8_t addr_hash[32]) {
    if (!fs || !addr_hash) return 0;

    compact_art_t *art = flat_store_get_art(fs->storage);
    if (!art) return 0;

    /* Quick check: does this account have any storage? */
    uint32_t depth_out;
    compact_ref_t subtree = compact_art_find_subtree(art, addr_hash, 32, &depth_out);
    if (subtree == COMPACT_REF_NULL) return 0;

    /* Collect keys into a flat buffer (one alloc, no per-key malloc) */
    size_t cap = 64;
    size_t count = 0;
    uint8_t *keys = malloc(cap * STOR_KEY_SIZE);
    if (!keys) return 0;

    compact_art_iterator_t *it = compact_art_iterator_create(art);
    if (!it) { free(keys); return 0; }

    uint8_t seek_key[STOR_KEY_SIZE];
    memcpy(seek_key, addr_hash, 32);
    memset(seek_key + 32, 0, 32);
    compact_art_iterator_seek(it, seek_key);

    while (compact_art_iterator_next(it)) {
        const void *leaf_val = compact_art_iterator_value(it);
        uint8_t full_key[STOR_KEY_SIZE];
        if (!art->key_fetch(leaf_val, full_key, art->key_fetch_ctx))
            continue;
        if (memcmp(full_key, addr_hash, 32) != 0)
            break;

        if (count >= cap) {
            size_t nc = cap * 2;
            uint8_t *nk = realloc(keys, nc * STOR_KEY_SIZE);
            if (!nk) break;
            keys = nk; cap = nc;
        }
        memcpy(keys + count * STOR_KEY_SIZE, full_key, STOR_KEY_SIZE);
        count++;
    }
    compact_art_iterator_destroy(it);

    for (size_t i = 0; i < count; i++) {
        flat_store_delete(fs->storage, keys + i * STOR_KEY_SIZE);
    }
    free(keys);

    return (uint64_t)count;
}

/* =========================================================================
 * Prefetch
 * ========================================================================= */

void flat_state_prefetch_account(const flat_state_t *fs, const uint8_t addr_hash[32]) {
    /* flat_store uses in-memory index — no prefetch needed for lookups.
     * The data read is a single pread at a known offset. */
    (void)fs; (void)addr_hash;
}

/* =========================================================================
 * Batch Operations
 * ========================================================================= */

bool flat_state_batch_put_accounts(flat_state_t *fs,
                                    const uint8_t *addr_hashes,
                                    const flat_account_record_t *records,
                                    uint32_t count) {
    if (!fs || !addr_hashes || !records || count == 0) return false;
    for (uint32_t i = 0; i < count; i++) {
        uint8_t buf[ACCT_MAX_REC_SIZE];
        uint32_t len = encode_account(&records[i], buf);
        if (!flat_store_put(fs->accounts, addr_hashes + i * 32, buf, len))
            return false;
    }
    return true;
}

bool flat_state_batch_put_storage(flat_state_t *fs,
                                   const uint8_t *keys,
                                   const uint8_t *values,
                                   uint32_t count) {
    if (!fs || !keys || !values || count == 0) return false;
    for (uint32_t i = 0; i < count; i++) {
        uint8_t ckey[64];
        make_stor_key(keys + i * 64, keys + i * 64 + 32, ckey);
        uint8_t buf[32];
        uint32_t len = encode_storage(values + i * 32, buf);
        if (!flat_store_put(fs->storage, ckey, buf, len))
            return false;
    }
    return true;
}

/* =========================================================================
 * Internal Access
 * ========================================================================= */

compact_art_t *flat_state_storage_art(flat_state_t *fs) {
    if (!fs || !fs->storage) return NULL;
    return flat_store_get_art(fs->storage);
}

flat_store_t *flat_state_storage_store(flat_state_t *fs) {
    return fs ? fs->storage : NULL;
}

compact_art_t *flat_state_account_art(flat_state_t *fs) {
    if (!fs || !fs->accounts) return NULL;
    return flat_store_get_art(fs->accounts);
}

flat_store_t *flat_state_account_store(flat_state_t *fs) {
    return fs ? fs->accounts : NULL;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t flat_state_account_count(const flat_state_t *fs) {
    return fs ? flat_store_count(fs->accounts) : 0;
}

uint64_t flat_state_storage_count(const flat_state_t *fs) {
    return fs ? flat_store_count(fs->storage) : 0;
}
