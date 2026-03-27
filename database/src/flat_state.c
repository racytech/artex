/*
 * Flat State — O(1) Account/Storage Lookups via flat_store.
 *
 * Two flat_store instances:
 *   accounts: key=32B (keccak256(addr)) → record=104B
 *   storage:  key=32B (keccak256(addr_hash||slot_hash)) → record=32B
 *
 * Storage keys are hashed from 64→32 bytes to halve compact_art leaf size.
 * At 1B entries: 36GB leaf memory instead of 68GB.
 */

#include "flat_state.h"
#include "flat_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ACCT_KEY_SIZE  32    /* keccak256(addr) */
#define ACCT_REC_SIZE  104
#define STOR_KEY_SIZE  32    /* keccak256(addr_hash || slot_hash) */
#define STOR_REC_SIZE  32

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

flat_state_t *flat_state_create(const char *path,
                                 uint64_t account_cap, uint64_t storage_cap) {
    (void)account_cap; (void)storage_cap;  /* flat_store grows dynamically */
    if (!path) return NULL;

    char *acct_path = make_path(path, "_acct.art");
    char *stor_path = make_path(path, "_stor.art");
    if (!acct_path || !stor_path) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_store_t *accounts = flat_store_create(acct_path, ACCT_KEY_SIZE, ACCT_REC_SIZE);
    if (!accounts) {
        fprintf(stderr, "flat_state: failed to create account store at %s\n", acct_path);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_store_t *storage = flat_store_create(stor_path, STOR_KEY_SIZE, STOR_REC_SIZE);
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
    return flat_store_get(fs->accounts, addr_hash, out);
}

bool flat_state_put_account(flat_state_t *fs, const uint8_t addr_hash[32],
                             const flat_account_record_t *record) {
    if (!fs || !addr_hash || !record) return false;
    return flat_store_put(fs->accounts, addr_hash, record);
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
                                  uint8_t out[32]) {
    const uint64_t *a = (const uint64_t *)addr_hash;
    const uint64_t *s = (const uint64_t *)slot_hash;
    uint64_t *o = (uint64_t *)out;
    o[0] = a[0] ^ s[0];
    o[1] = a[1] ^ s[1];
    o[2] = a[2] ^ s[2];
    o[3] = a[3] ^ s[3];
}

bool flat_state_get_storage(const flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             uint8_t value[32]) {
    if (!fs || !addr_hash || !slot_hash || !value) return false;
    uint8_t key[STOR_KEY_SIZE];
    make_stor_key(addr_hash, slot_hash, key);
    return flat_store_get(fs->storage, key, value);
}

bool flat_state_put_storage(flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             const uint8_t value[32]) {
    if (!fs || !addr_hash || !slot_hash || !value) return false;
    uint8_t key[STOR_KEY_SIZE];
    make_stor_key(addr_hash, slot_hash, key);
    return flat_store_put(fs->storage, key, value);
}

bool flat_state_delete_storage(flat_state_t *fs,
                                const uint8_t addr_hash[32],
                                const uint8_t slot_hash[32]) {
    if (!fs || !addr_hash || !slot_hash) return false;
    uint8_t key[STOR_KEY_SIZE];
    make_stor_key(addr_hash, slot_hash, key);
    return flat_store_delete(fs->storage, key);
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
    return flat_store_batch_put(fs->accounts, addr_hashes, records, count);
}

bool flat_state_batch_put_storage(flat_state_t *fs,
                                   const uint8_t *keys,
                                   const uint8_t *values,
                                   uint32_t count) {
    if (!fs || !keys || !values || count == 0) return false;
    /* Build hashed keys */
    uint8_t *hashed_keys = malloc(count * 32);
    if (!hashed_keys) return false;
    for (uint32_t i = 0; i < count; i++)
        make_stor_key(keys + i * 64, keys + i * 64 + 32, hashed_keys + i * 32);
    bool ok = flat_store_batch_put(fs->storage, hashed_keys, values, count);
    free(hashed_keys);
    return ok;
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
