/*
 * Flat State — O(1) Account/Storage Lookups via mmap'd disk_hash.
 *
 * Thin wrapper over two disk_hash instances:
 *   accounts: key=20B (address) → record=104B (nonce, balance, code_hash, storage_root)
 *   storage:  key=52B (addr+slot) → record=32B (value)
 *
 * All operations delegate directly to disk_hash. The mmap-backed disk_hash
 * provides OS page cache management — no explicit LRU needed.
 *
 * Performance: not suitable as the primary hot-path state cache.
 * Random mmap access + per-op locking + msync(MS_SYNC) in flat_state_sync
 * make it slower than in-memory mem_art. Best used as a cold-path fallback
 * after cache eviction, not a replacement for the in-memory state cache.
 */

#include "flat_state.h"
#include "disk_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ACCT_KEY_SIZE  20
#define ACCT_REC_SIZE  104
#define STOR_KEY_SIZE  52
#define STOR_REC_SIZE  32

struct flat_state {
    disk_hash_t *accounts;
    disk_hash_t *storage;
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
    if (!path) return NULL;

    char *acct_path = make_path(path, "_flat_acct.idx");
    char *stor_path = make_path(path, "_flat_stor.idx");
    if (!acct_path || !stor_path) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_hash_t *accounts = disk_hash_create(acct_path, ACCT_KEY_SIZE,
                                              ACCT_REC_SIZE, account_cap);
    if (!accounts) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_hash_t *storage = disk_hash_create(stor_path, STOR_KEY_SIZE,
                                             STOR_REC_SIZE, storage_cap);
    if (!storage) {
        disk_hash_destroy(accounts);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_state_t *fs = calloc(1, sizeof(*fs));
    if (!fs) {
        disk_hash_destroy(accounts);
        disk_hash_destroy(storage);
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

    char *acct_path = make_path(path, "_flat_acct.idx");
    char *stor_path = make_path(path, "_flat_stor.idx");
    if (!acct_path || !stor_path) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_hash_t *accounts = disk_hash_open(acct_path);
    if (!accounts) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_hash_t *storage = disk_hash_open(stor_path);
    if (!storage) {
        disk_hash_destroy(accounts);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_state_t *fs = calloc(1, sizeof(*fs));
    if (!fs) {
        disk_hash_destroy(accounts);
        disk_hash_destroy(storage);
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
    disk_hash_destroy(fs->accounts);
    disk_hash_destroy(fs->storage);
    free(fs->acct_path);
    free(fs->stor_path);
    free(fs);
}

void flat_state_sync(flat_state_t *fs) {
    if (!fs) return;
    disk_hash_sync(fs->accounts);
    disk_hash_sync(fs->storage);
}

/* =========================================================================
 * Account Operations
 * ========================================================================= */

bool flat_state_get_account(const flat_state_t *fs, const uint8_t addr[20],
                             flat_account_record_t *out) {
    if (!fs || !addr || !out) return false;
    return disk_hash_get(fs->accounts, addr, out);
}

bool flat_state_put_account(flat_state_t *fs, const uint8_t addr[20],
                             const flat_account_record_t *record) {
    if (!fs || !addr || !record) return false;
    return disk_hash_put(fs->accounts, addr, record);
}

bool flat_state_delete_account(flat_state_t *fs, const uint8_t addr[20]) {
    if (!fs || !addr) return false;
    return disk_hash_delete(fs->accounts, addr);
}

/* =========================================================================
 * Storage Operations
 * ========================================================================= */

bool flat_state_get_storage(const flat_state_t *fs, const uint8_t skey[52],
                             uint8_t value[32]) {
    if (!fs || !skey || !value) return false;
    return disk_hash_get(fs->storage, skey, value);
}

bool flat_state_put_storage(flat_state_t *fs, const uint8_t skey[52],
                             const uint8_t value[32]) {
    if (!fs || !skey || !value) return false;
    return disk_hash_put(fs->storage, skey, value);
}

bool flat_state_delete_storage(flat_state_t *fs, const uint8_t skey[52]) {
    if (!fs || !skey) return false;
    return disk_hash_delete(fs->storage, skey);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t flat_state_account_count(const flat_state_t *fs) {
    return fs ? disk_hash_count(fs->accounts) : 0;
}

uint64_t flat_state_storage_count(const flat_state_t *fs) {
    return fs ? disk_hash_count(fs->storage) : 0;
}
