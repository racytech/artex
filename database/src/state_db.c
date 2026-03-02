#include "../include/state_db.h"
#include "../include/data_layer.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

// ============================================================================
// Internal constants
// ============================================================================

#define ACCOUNT_KEY_SIZE   32
#define ACCOUNT_SLOT_SIZE  128   // 128B slot: 10B overhead + 118B value (fits 74B account)
#define STORAGE_KEY_SIZE   64
#define STORAGE_SLOT_SIZE  128   // 128B slot: 10B overhead + 64B key + 54B value
#define SHARD_CAPACITY     (1ULL << 24)  // 16M slots per shard
#define MAX_PATH           512

// ============================================================================
// Opaque struct
// ============================================================================

struct state_db {
    data_layer_t *accounts;   // key=32, with code_store
    data_layer_t *storage;    // key=64, no code_store
    char *dir;
};

// ============================================================================
// Path helpers
// ============================================================================

static bool ensure_dir(const char *path) {
    if (mkdir(path, 0755) == 0) return true;
    return errno == EEXIST;
}

// ============================================================================
// Lifecycle
// ============================================================================

state_db_t *sdb_create(const char *dir) {
    if (!dir) return NULL;

    // Create directories
    if (!ensure_dir(dir)) return NULL;

    char acct_dir[MAX_PATH], stor_dir[MAX_PATH], code_path[MAX_PATH];
    snprintf(acct_dir, MAX_PATH, "%s/accounts", dir);
    snprintf(stor_dir, MAX_PATH, "%s/storage", dir);
    snprintf(code_path, MAX_PATH, "%s/code.dat", dir);

    state_db_t *sdb = malloc(sizeof(state_db_t));
    if (!sdb) return NULL;
    memset(sdb, 0, sizeof(*sdb));

    sdb->dir = strdup(dir);
    if (!sdb->dir) {
        free(sdb);
        return NULL;
    }

    sdb->accounts = dl_create(acct_dir, code_path,
                               ACCOUNT_KEY_SIZE, ACCOUNT_SLOT_SIZE,
                               SHARD_CAPACITY);
    if (!sdb->accounts) {
        free(sdb->dir);
        free(sdb);
        return NULL;
    }

    sdb->storage = dl_create(stor_dir, NULL,
                              STORAGE_KEY_SIZE, STORAGE_SLOT_SIZE,
                              SHARD_CAPACITY);
    if (!sdb->storage) {
        dl_destroy(sdb->accounts);
        free(sdb->dir);
        free(sdb);
        return NULL;
    }

    return sdb;
}

state_db_t *sdb_open(const char *dir) {
    if (!dir) return NULL;

    char acct_dir[MAX_PATH], stor_dir[MAX_PATH], code_path[MAX_PATH];
    snprintf(acct_dir, MAX_PATH, "%s/accounts", dir);
    snprintf(stor_dir, MAX_PATH, "%s/storage", dir);
    snprintf(code_path, MAX_PATH, "%s/code.dat", dir);

    state_db_t *sdb = malloc(sizeof(state_db_t));
    if (!sdb) return NULL;
    memset(sdb, 0, sizeof(*sdb));

    sdb->dir = strdup(dir);
    if (!sdb->dir) {
        free(sdb);
        return NULL;
    }

    sdb->accounts = dl_open(acct_dir, code_path);
    if (!sdb->accounts) {
        free(sdb->dir);
        free(sdb);
        return NULL;
    }

    sdb->storage = dl_open(stor_dir, NULL);
    if (!sdb->storage) {
        dl_destroy(sdb->accounts);
        free(sdb->dir);
        free(sdb);
        return NULL;
    }

    return sdb;
}

void sdb_destroy(state_db_t *sdb) {
    if (!sdb) return;
    dl_destroy(sdb->accounts);
    dl_destroy(sdb->storage);
    free(sdb->dir);
    free(sdb);
}

// ============================================================================
// Account State
// ============================================================================

bool sdb_put(state_db_t *sdb, const uint8_t key[32],
             const void *value, uint16_t len) {
    if (!sdb) return false;
    return dl_put(sdb->accounts, key, value, len);
}

bool sdb_get(state_db_t *sdb, const uint8_t key[32],
             void *out_value, uint16_t *out_len) {
    if (!sdb) return false;
    return dl_get(sdb->accounts, key, out_value, out_len);
}

bool sdb_delete(state_db_t *sdb, const uint8_t key[32]) {
    if (!sdb) return false;
    return dl_delete(sdb->accounts, key);
}

// ============================================================================
// Contract Code
// ============================================================================

bool sdb_put_code(state_db_t *sdb, const uint8_t key[32],
                  const void *bytecode, uint32_t len) {
    if (!sdb) return false;
    return dl_put_code(sdb->accounts, key, bytecode, len);
}

bool sdb_get_code(state_db_t *sdb, const uint8_t key[32],
                  void *out, uint32_t *out_len) {
    if (!sdb) return false;
    return dl_get_code(sdb->accounts, key, out, out_len);
}

uint32_t sdb_code_length(state_db_t *sdb, const uint8_t key[32]) {
    if (!sdb) return 0;
    return dl_code_length(sdb->accounts, key);
}

// ============================================================================
// Storage Slots
// ============================================================================

bool sdb_put_storage(state_db_t *sdb,
                     const uint8_t addr_hash[32],
                     const uint8_t slot_hash[32],
                     const void *value, uint16_t len) {
    if (!sdb) return false;
    uint8_t key[STORAGE_KEY_SIZE];
    memcpy(key, addr_hash, 32);
    memcpy(key + 32, slot_hash, 32);
    return dl_put(sdb->storage, key, value, len);
}

bool sdb_get_storage(state_db_t *sdb,
                     const uint8_t addr_hash[32],
                     const uint8_t slot_hash[32],
                     void *out_value, uint16_t *out_len) {
    if (!sdb) return false;
    uint8_t key[STORAGE_KEY_SIZE];
    memcpy(key, addr_hash, 32);
    memcpy(key + 32, slot_hash, 32);
    return dl_get(sdb->storage, key, out_value, out_len);
}

bool sdb_delete_storage(state_db_t *sdb,
                        const uint8_t addr_hash[32],
                        const uint8_t slot_hash[32]) {
    if (!sdb) return false;
    uint8_t key[STORAGE_KEY_SIZE];
    memcpy(key, addr_hash, 32);
    memcpy(key + 32, slot_hash, 32);
    return dl_delete(sdb->storage, key);
}

// ============================================================================
// Block Lifecycle
// ============================================================================

uint64_t sdb_merge(state_db_t *sdb) {
    if (!sdb) return 0;
    uint64_t a = dl_merge(sdb->accounts);
    uint64_t s = dl_merge(sdb->storage);
    return a + s;
}

bool sdb_checkpoint(state_db_t *sdb) {
    if (!sdb) return false;
    if (!dl_checkpoint(sdb->accounts)) return false;
    if (!dl_checkpoint(sdb->storage)) return false;
    return true;
}

// ============================================================================
// Diagnostics
// ============================================================================

sdb_stats_t sdb_stats(const state_db_t *sdb) {
    sdb_stats_t s = {0};
    if (!sdb) return s;

    dl_stats_t a = dl_stats(sdb->accounts);
    dl_stats_t st = dl_stats(sdb->storage);

    s.account_keys       = a.index_keys;
    s.code_count         = a.code_count;
    s.storage_keys       = st.index_keys;

    return s;
}
