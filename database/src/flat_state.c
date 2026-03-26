/*
 * Flat State — O(1) Account/Storage Lookups via disk_table.
 *
 * Thin wrapper over two disk_table instances:
 *   accounts: key=32B (keccak256(addr)) → record=104B
 *   storage:  key=64B (addr_hash+slot_hash) → record=32B
 *
 * disk_table is mmap'd, lock-free, uses pre-hashed keys directly
 * as bucket indices. No per-op locking, no msync on hot paths.
 */

#include "flat_state.h"
#include "disk_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ACCT_KEY_SIZE  32    /* keccak256(addr) */
#define ACCT_REC_SIZE  104
#define STOR_KEY_SIZE  64    /* addr_hash[32] + slot_hash[32] */
#define STOR_REC_SIZE  32

struct flat_state {
    disk_table_t *accounts;
    disk_table_t *storage;
    char         *acct_path;
    char         *stor_path;
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

    char *acct_path = make_path(path, "_acct.dt");
    char *stor_path = make_path(path, "_stor.dt");
    if (!acct_path || !stor_path) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_table_t *accounts = disk_table_create(acct_path, ACCT_KEY_SIZE,
                                                ACCT_REC_SIZE, account_cap);
    if (!accounts) {
        fprintf(stderr, "flat_state: failed to create account table at %s\n", acct_path);
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_table_t *storage = disk_table_create(stor_path, STOR_KEY_SIZE,
                                               STOR_REC_SIZE, storage_cap);
    if (!storage) {
        fprintf(stderr, "flat_state: failed to create storage table at %s\n", stor_path);
        disk_table_destroy(accounts);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_state_t *fs = calloc(1, sizeof(*fs));
    if (!fs) {
        disk_table_destroy(accounts);
        disk_table_destroy(storage);
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

    char *acct_path = make_path(path, "_acct.dt");
    char *stor_path = make_path(path, "_stor.dt");
    if (!acct_path || !stor_path) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_table_t *accounts = disk_table_open(acct_path);
    if (!accounts) {
        free(acct_path); free(stor_path);
        return NULL;
    }

    disk_table_t *storage = disk_table_open(stor_path);
    if (!storage) {
        disk_table_destroy(accounts);
        free(acct_path); free(stor_path);
        return NULL;
    }

    flat_state_t *fs = calloc(1, sizeof(*fs));
    if (!fs) {
        disk_table_destroy(accounts);
        disk_table_destroy(storage);
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
    disk_table_destroy(fs->accounts);
    disk_table_destroy(fs->storage);
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
    return disk_table_get(fs->accounts, addr_hash, out);
}

bool flat_state_put_account(flat_state_t *fs, const uint8_t addr_hash[32],
                             const flat_account_record_t *record) {
    if (!fs || !addr_hash || !record) return false;
    return disk_table_put(fs->accounts, addr_hash, record);
}

bool flat_state_delete_account(flat_state_t *fs, const uint8_t addr_hash[32]) {
    if (!fs || !addr_hash) return false;
    return disk_table_delete(fs->accounts, addr_hash);
}

/* =========================================================================
 * Storage Operations
 * ========================================================================= */

bool flat_state_get_storage(const flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             uint8_t value[32]) {
    if (!fs || !addr_hash || !slot_hash || !value) return false;
    uint8_t key[STOR_KEY_SIZE];
    memcpy(key, addr_hash, 32);
    memcpy(key + 32, slot_hash, 32);
    return disk_table_get(fs->storage, key, value);
}

bool flat_state_put_storage(flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             const uint8_t value[32]) {
    if (!fs || !addr_hash || !slot_hash || !value) return false;
    uint8_t key[STOR_KEY_SIZE];
    memcpy(key, addr_hash, 32);
    memcpy(key + 32, slot_hash, 32);
    return disk_table_put(fs->storage, key, value);
}

bool flat_state_delete_storage(flat_state_t *fs,
                                const uint8_t addr_hash[32],
                                const uint8_t slot_hash[32]) {
    if (!fs || !addr_hash || !slot_hash) return false;
    uint8_t key[STOR_KEY_SIZE];
    memcpy(key, addr_hash, 32);
    memcpy(key + 32, slot_hash, 32);
    return disk_table_delete(fs->storage, key);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t flat_state_account_count(const flat_state_t *fs) {
    return fs ? disk_table_count(fs->accounts) : 0;
}

uint64_t flat_state_storage_count(const flat_state_t *fs) {
    return fs ? disk_table_count(fs->storage) : 0;
}
