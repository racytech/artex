#ifndef STATE_DB_H
#define STATE_DB_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * State DB — Unified wrapper for account state + contract storage.
 *
 * Architecture:
 *   Two independent data_layer_t instances (hash_store-backed):
 *     - accounts (key=32, slot=128): account state + contract code
 *     - storage  (key=64, slot=64):  contract storage slots (addr_hash || slot_hash)
 *
 * Directory layout:
 *   {dir}/accounts/  — hash_store shard files for account state + code refs
 *   {dir}/code.dat   — contract bytecode (append-only)
 *   {dir}/storage/   — hash_store shard files for storage slots
 *
 * Composite storage key: addr_hash[32] || slot_hash[32] = 64 bytes.
 *
 * Opaque handle — struct defined in state_db.c.
 */

typedef struct state_db state_db_t;

typedef struct {
    uint64_t account_keys;       // committed account entries
    uint32_t code_count;         // code_store entries
    uint64_t storage_keys;       // committed storage entries
} sdb_stats_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new state DB. Creates {dir}/ and {dir}/storage/ directories.
 * Truncates all data files. Returns NULL on failure.
 */
state_db_t *sdb_create(const char *dir);

/**
 * Open from existing hash_store directories (recovery path).
 * Returns NULL on failure.
 */
state_db_t *sdb_open(const char *dir);

/**
 * Destroy the state DB and free all resources.
 */
void sdb_destroy(state_db_t *sdb);

// ============================================================================
// Account State
// ============================================================================

bool sdb_put(state_db_t *sdb, const uint8_t key[32],
             const void *value, uint16_t len);
bool sdb_get(state_db_t *sdb, const uint8_t key[32],
             void *out_value, uint16_t *out_len);
bool sdb_delete(state_db_t *sdb, const uint8_t key[32]);

// ============================================================================
// Contract Code
// ============================================================================

bool sdb_put_code(state_db_t *sdb, const uint8_t key[32],
                  const void *bytecode, uint32_t len);
bool sdb_get_code(state_db_t *sdb, const uint8_t key[32],
                  void *out, uint32_t *out_len);
uint32_t sdb_code_length(state_db_t *sdb, const uint8_t key[32]);

// ============================================================================
// Storage Slots (builds 64-byte composite key internally)
// ============================================================================

bool sdb_put_storage(state_db_t *sdb,
                     const uint8_t addr_hash[32],
                     const uint8_t slot_hash[32],
                     const void *value, uint16_t len);
bool sdb_get_storage(state_db_t *sdb,
                     const uint8_t addr_hash[32],
                     const uint8_t slot_hash[32],
                     void *out_value, uint16_t *out_len);
bool sdb_delete_storage(state_db_t *sdb,
                        const uint8_t addr_hash[32],
                        const uint8_t slot_hash[32]);

// ============================================================================
// Block Lifecycle
// ============================================================================

/** Merge both layers. Returns total entries processed. */
uint64_t sdb_merge(state_db_t *sdb);

/** Sync both hash_stores to disk. */
bool sdb_checkpoint(state_db_t *sdb);

/** Get combined stats. */
sdb_stats_t sdb_stats(const state_db_t *sdb);

#endif // STATE_DB_H
