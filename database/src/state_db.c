#include "../include/state_db.h"
#include "../include/data_layer.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

// ============================================================================
// Internal constants
// ============================================================================

#define ACCOUNT_KEY_SIZE   32
#define ACCOUNT_SLOT_SIZE  128   // 128B slot: 10B overhead + 118B value (fits 74B account)
#define STORAGE_KEY_SIZE   64
#define STORAGE_SLOT_SIZE  128   // 128B slot: 10B overhead + 64B key + 54B value
#define SHARD_CAPACITY     (1ULL << 24)  // 16M slots per shard
#define MAX_PATH           512
#define MAX_VALUE_SIZE     86    // max(account=86, storage=54)

// Undo log constants (v2: append-only, multi-block)
#define UNDO_MAGIC         "UNDOLOG2"   // 8 bytes (v2 format)
#define UNDO_HEADER_SIZE   16           // magic(8) + reserved(8)
#define BLOCK_BEGIN_TAG    0xBB         // BLOCK_BEGIN marker
#define BLOCK_COMMIT_TAG   0xBC         // BLOCK_COMMIT marker
#define BLOCK_MARKER_SIZE  5            // tag(1) + block_seq(4)
#define UNDO_FILENAME      "undo.log"
#define PENDING_INIT_CAP   256

// Pending write operation types
#define SDB_OP_ACCT_PUT    0x01
#define SDB_OP_ACCT_DEL    0x02
#define SDB_OP_STOR_PUT    0x03
#define SDB_OP_STOR_DEL    0x04

// ============================================================================
// Internal types
// ============================================================================

typedef struct {
    uint8_t  op;
    uint8_t  key[64];     // 32 for acct, 64 for storage
    uint8_t  value[86];   // max value size across both layers
    uint16_t val_len;
} sdb_pending_write_t;

// ============================================================================
// Opaque struct
// ============================================================================

struct state_db {
    data_layer_t *accounts;   // key=32, with code_store
    data_layer_t *storage;    // key=64, no code_store
    char *dir;
    // Block-level undo
    bool                  block_active;
    sdb_pending_write_t  *pending;
    uint32_t              pending_count;
    uint32_t              pending_cap;
    // Deferred checkpoint: undo log persists across blocks
    int                   undo_fd;          // -1 = not open
    uint32_t              undo_block_count; // sequential counter for markers
};

// ============================================================================
// Path helpers
// ============================================================================

static bool ensure_dir(const char *path) {
    if (mkdir(path, 0755) == 0) return true;
    return errno == EEXIST;
}

static void undo_log_path(const state_db_t *sdb, char *out, size_t out_size) {
    snprintf(out, out_size, "%s/%s", sdb->dir, UNDO_FILENAME);
}

// ============================================================================
// Pending write helpers
// ============================================================================

static uint8_t key_len_for_op(uint8_t op) {
    switch (op) {
        case SDB_OP_ACCT_PUT:
        case SDB_OP_ACCT_DEL:
            return ACCOUNT_KEY_SIZE;
        case SDB_OP_STOR_PUT:
        case SDB_OP_STOR_DEL:
            return STORAGE_KEY_SIZE;
        default:
            return 0;
    }
}

static data_layer_t *layer_for_op(state_db_t *sdb, uint8_t op) {
    switch (op) {
        case SDB_OP_ACCT_PUT:
        case SDB_OP_ACCT_DEL:
            return sdb->accounts;
        case SDB_OP_STOR_PUT:
        case SDB_OP_STOR_DEL:
            return sdb->storage;
        default:
            return NULL;
    }
}

static bool pending_append(state_db_t *sdb, uint8_t op,
                           const uint8_t *key, uint8_t key_len,
                           const void *value, uint16_t val_len) {
    if (sdb->pending_count >= sdb->pending_cap) {
        uint32_t new_cap = sdb->pending_cap * 2;
        sdb_pending_write_t *new_buf = realloc(sdb->pending,
            new_cap * sizeof(sdb_pending_write_t));
        if (!new_buf) return false;
        sdb->pending = new_buf;
        sdb->pending_cap = new_cap;
    }
    sdb_pending_write_t *pw = &sdb->pending[sdb->pending_count++];
    memset(pw, 0, sizeof(*pw));
    pw->op = op;
    memcpy(pw->key, key, key_len);
    if (value && val_len > 0) {
        memcpy(pw->value, value, val_len);
        pw->val_len = val_len;
    }
    return true;
}

// ============================================================================
// Undo log recovery
// ============================================================================

static bool sdb_recover_undo(state_db_t *sdb) {
    char path[MAX_PATH];
    undo_log_path(sdb, path, sizeof(path));

    int fd = open(path, O_RDONLY);
    if (fd < 0) return true;  // No undo log — clean state

    // Read header
    uint8_t hdr[UNDO_HEADER_SIZE];
    if (read(fd, hdr, UNDO_HEADER_SIZE) != UNDO_HEADER_SIZE) {
        close(fd); unlink(path); return true;
    }
    if (memcmp(hdr, UNDO_MAGIC, 8) != 0) {
        close(fd); unlink(path); return true;
    }

    // Read all undo entries across all blocks
    typedef struct {
        uint8_t  op;
        uint8_t  key[64];
        uint8_t  klen;
        bool     present;
        uint8_t  old_len;
        uint8_t  old_val[MAX_VALUE_SIZE];
    } undo_entry_t;

    undo_entry_t *entries = NULL;
    uint32_t entry_count = 0;
    uint32_t entry_cap = 0;

    for (;;) {
        uint8_t tag;
        ssize_t n = read(fd, &tag, 1);
        if (n <= 0) break;  // EOF

        // Block markers: skip the 4-byte block_seq
        if (tag == BLOCK_BEGIN_TAG || tag == BLOCK_COMMIT_TAG) {
            uint32_t seq;
            if (read(fd, &seq, 4) != 4) break;
            continue;
        }

        // Parse undo entry
        uint8_t klen = key_len_for_op(tag);
        if (klen == 0) break;  // Unknown byte — stop parsing

        if (entry_count >= entry_cap) {
            uint32_t new_cap = entry_cap == 0 ? 256 : entry_cap * 2;
            undo_entry_t *new_arr = realloc(entries, new_cap * sizeof(undo_entry_t));
            if (!new_arr) break;
            entries = new_arr;
            entry_cap = new_cap;
        }

        undo_entry_t *e = &entries[entry_count];
        memset(e, 0, sizeof(*e));
        e->op = tag;
        e->klen = klen;

        if (read(fd, e->key, klen) != klen) break;

        uint8_t present;
        if (read(fd, &present, 1) != 1) break;
        e->present = (present == 1);

        if (e->present) {
            if (read(fd, &e->old_len, 1) != 1) break;
            if (e->old_len > 0) {
                if (read(fd, e->old_val, e->old_len) != e->old_len) break;
            }
        }

        entry_count++;
    }

    close(fd);

    // Roll back ALL entries in reverse order.
    // This restores hash_store to the state at the last sdb_checkpoint.
    // Both committed and uncommitted blocks are rolled back because
    // without msync, committed data may not be durable on disk.
    for (uint32_t i = entry_count; i > 0; i--) {
        undo_entry_t *e = &entries[i - 1];
        data_layer_t *dl = (e->op == SDB_OP_ACCT_PUT || e->op == SDB_OP_ACCT_DEL)
                           ? sdb->accounts : sdb->storage;

        if (e->present) {
            dl_put(dl, e->key, e->old_val, e->old_len);
        } else {
            dl_delete(dl, e->key);
        }
    }

    // Sync restored state to disk
    dl_checkpoint(sdb->accounts);
    dl_checkpoint(sdb->storage);

    free(entries);
    unlink(path);
    return true;
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

    sdb->undo_fd = -1;

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

    sdb->undo_fd = -1;
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

    // Recover from crash if undo log exists
    if (!sdb_recover_undo(sdb)) {
        dl_destroy(sdb->accounts);
        dl_destroy(sdb->storage);
        free(sdb->dir);
        free(sdb);
        return NULL;
    }

    return sdb;
}

void sdb_destroy(state_db_t *sdb) {
    if (!sdb) return;
    if (sdb->undo_fd >= 0) close(sdb->undo_fd);
    free(sdb->pending);
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
    if (sdb->block_active)
        return pending_append(sdb, SDB_OP_ACCT_PUT, key, ACCOUNT_KEY_SIZE, value, len);
    return dl_put(sdb->accounts, key, value, len);
}

bool sdb_get(state_db_t *sdb, const uint8_t key[32],
             void *out_value, uint16_t *out_len) {
    if (!sdb) return false;
    return dl_get(sdb->accounts, key, out_value, out_len);
}

bool sdb_delete(state_db_t *sdb, const uint8_t key[32]) {
    if (!sdb) return false;
    if (sdb->block_active)
        return pending_append(sdb, SDB_OP_ACCT_DEL, key, ACCOUNT_KEY_SIZE, NULL, 0);
    return dl_delete(sdb->accounts, key);
}

// ============================================================================
// Contract Code (NOT buffered — content-addressed, idempotent)
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
    if (sdb->block_active)
        return pending_append(sdb, SDB_OP_STOR_PUT, key, STORAGE_KEY_SIZE, value, len);
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
    if (sdb->block_active)
        return pending_append(sdb, SDB_OP_STOR_DEL, key, STORAGE_KEY_SIZE, NULL, 0);
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

// --- Parallel checkpoint helpers ---

typedef struct {
    hash_store_t *store;
} ckpt_hs_arg_t;

typedef struct {
    code_store_t *code;
} ckpt_code_arg_t;

static void *ckpt_hash_store_thread(void *arg) {
    ckpt_hs_arg_t *a = (ckpt_hs_arg_t *)arg;
    if (a->store) hash_store_sync(a->store);
    return NULL;
}

static void *ckpt_code_store_thread(void *arg) {
    ckpt_code_arg_t *a = (ckpt_code_arg_t *)arg;
    if (a->code) code_store_sync(a->code);
    return NULL;
}

bool sdb_checkpoint(state_db_t *sdb) {
    if (!sdb) return false;

    // Parallel flush: accounts hash_store, storage hash_store, code_store
    ckpt_hs_arg_t   acct_arg = { dl_get_hash_store(sdb->accounts) };
    ckpt_hs_arg_t   stor_arg = { dl_get_hash_store(sdb->storage) };
    ckpt_code_arg_t code_arg = { dl_get_code_store(sdb->accounts) };

    pthread_t t_acct, t_stor, t_code;
    pthread_create(&t_acct, NULL, ckpt_hash_store_thread, &acct_arg);
    pthread_create(&t_stor, NULL, ckpt_hash_store_thread, &stor_arg);
    pthread_create(&t_code, NULL, ckpt_code_store_thread, &code_arg);

    pthread_join(t_acct, NULL);
    pthread_join(t_stor, NULL);
    pthread_join(t_code, NULL);

    // Clean undo log — all data is now durable
    if (sdb->undo_fd >= 0) {
        close(sdb->undo_fd);
        sdb->undo_fd = -1;
    }
    char path[MAX_PATH];
    undo_log_path(sdb, path, sizeof(path));
    unlink(path);
    sdb->undo_block_count = 0;

    return true;
}

// ============================================================================
// Block-Level Atomicity (Undo Log)
// ============================================================================

bool sdb_begin_block(state_db_t *sdb) {
    if (!sdb || sdb->block_active) return false;

    if (!sdb->pending) {
        sdb->pending = malloc(PENDING_INIT_CAP * sizeof(sdb_pending_write_t));
        if (!sdb->pending) return false;
        sdb->pending_cap = PENDING_INIT_CAP;
    }
    sdb->pending_count = 0;
    sdb->block_active = true;
    return true;
}

bool sdb_commit_block(state_db_t *sdb) {
    if (!sdb || !sdb->block_active) return false;

    // Fast path: no pending writes
    if (sdb->pending_count == 0) {
        sdb->block_active = false;
        return true;
    }

    // --- Step 0: Deduplicate pending array (keep last write per key) ---
    bool *skip = calloc(sdb->pending_count, sizeof(bool));
    if (!skip) return false;

    for (uint32_t i = sdb->pending_count; i > 0; i--) {
        if (skip[i - 1]) continue;
        uint8_t kl_i = key_len_for_op(sdb->pending[i - 1].op);
        for (uint32_t j = 0; j < i - 1; j++) {
            if (skip[j]) continue;
            uint8_t kl_j = key_len_for_op(sdb->pending[j].op);
            if (kl_i == kl_j &&
                memcmp(sdb->pending[i - 1].key, sdb->pending[j].key, kl_i) == 0) {
                skip[j] = true;
            }
        }
    }

    // --- Step 1: Ensure undo log is open with header ---
    if (sdb->undo_fd < 0) {
        char path[MAX_PATH];
        undo_log_path(sdb, path, sizeof(path));
        sdb->undo_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (sdb->undo_fd < 0) { free(skip); return false; }

        // Write file header
        uint8_t hdr[UNDO_HEADER_SIZE];
        memset(hdr, 0, UNDO_HEADER_SIZE);
        memcpy(hdr, UNDO_MAGIC, 8);
        if (write(sdb->undo_fd, hdr, UNDO_HEADER_SIZE) != UNDO_HEADER_SIZE)
            goto fail;
        sdb->undo_block_count = 0;
    }

    // --- Step 2: Write BLOCK_BEGIN marker ---
    {
        uint8_t marker[BLOCK_MARKER_SIZE];
        marker[0] = BLOCK_BEGIN_TAG;
        uint32_t seq = sdb->undo_block_count;
        memcpy(marker + 1, &seq, 4);
        if (write(sdb->undo_fd, marker, BLOCK_MARKER_SIZE) != BLOCK_MARKER_SIZE)
            goto fail;
    }

    // --- Step 3: Write undo entries (old values from hash_store) ---
    for (uint32_t i = 0; i < sdb->pending_count; i++) {
        if (skip[i]) continue;

        sdb_pending_write_t *pw = &sdb->pending[i];
        uint8_t klen = key_len_for_op(pw->op);
        data_layer_t *dl = layer_for_op(sdb, pw->op);

        if (write(sdb->undo_fd, &pw->op, 1) != 1) goto fail;
        if (write(sdb->undo_fd, pw->key, klen) != klen) goto fail;

        uint8_t old_val[MAX_VALUE_SIZE];
        uint16_t old_len = 0;
        bool present = dl_get(dl, pw->key, old_val, &old_len);

        uint8_t present_byte = present ? 1 : 0;
        if (write(sdb->undo_fd, &present_byte, 1) != 1) goto fail;

        if (present) {
            uint8_t len8 = (uint8_t)old_len;
            if (write(sdb->undo_fd, &len8, 1) != 1) goto fail;
            if (old_len > 0) {
                if (write(sdb->undo_fd, old_val, old_len) != (ssize_t)old_len) goto fail;
            }
        }
    }

    // --- Step 4: fsync undo log — undo is durable before we touch mmap ---
    if (fsync(sdb->undo_fd) != 0) goto fail;

    // --- Step 5: Apply all pending writes to hash_store mmap (NO msync) ---
    for (uint32_t i = 0; i < sdb->pending_count; i++) {
        sdb_pending_write_t *pw = &sdb->pending[i];
        data_layer_t *dl = layer_for_op(sdb, pw->op);

        switch (pw->op) {
        case SDB_OP_ACCT_PUT:
        case SDB_OP_STOR_PUT:
            if (!dl_put(dl, pw->key, pw->value, pw->val_len)) goto fail_applied;
            break;
        case SDB_OP_ACCT_DEL:
        case SDB_OP_STOR_DEL:
            dl_delete(dl, pw->key);
            break;
        }
    }

    // --- Step 6: Write BLOCK_COMMIT marker + fsync ---
    {
        uint8_t marker[BLOCK_MARKER_SIZE];
        marker[0] = BLOCK_COMMIT_TAG;
        uint32_t seq = sdb->undo_block_count;
        memcpy(marker + 1, &seq, 4);
        if (write(sdb->undo_fd, marker, BLOCK_MARKER_SIZE) != BLOCK_MARKER_SIZE)
            goto fail_applied;
        if (fsync(sdb->undo_fd) != 0) goto fail_applied;
    }

    // NO dl_checkpoint, NO close, NO unlink — deferred to sdb_checkpoint
    free(skip);
    sdb->pending_count = 0;
    sdb->undo_block_count++;
    sdb->block_active = false;
    return true;

fail:
    // Nothing applied to hash_store yet — close and remove undo log
    close(sdb->undo_fd);
    sdb->undo_fd = -1;
    {
        char path[MAX_PATH];
        undo_log_path(sdb, path, sizeof(path));
        unlink(path);
    }
    free(skip);
    return false;

fail_applied:
    // Writes partially applied. Undo log is durable.
    // On next sdb_open, recovery will roll back all blocks.
    free(skip);
    sdb->pending_count = 0;
    sdb->block_active = false;
    return false;
}

void sdb_abort_block(state_db_t *sdb) {
    if (!sdb || !sdb->block_active) return;
    sdb->pending_count = 0;
    sdb->block_active = false;
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
