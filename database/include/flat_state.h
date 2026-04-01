#ifndef FLAT_STATE_H
#define FLAT_STATE_H

#include <stdint.h>
#include <stdbool.h>

/**
 * Flat State — O(1) Account/Storage Lookups via flat_store.
 *
 * Two flat_store instances (compact_art index + mmap'd data file):
 *   accounts: keccak256(addr)[32] → {nonce, balance, code_hash, storage_root}
 *   storage:  addr_hash[32]||slot_hash[32] → value[32]
 *
 * Index lookups are in-memory (compact_art). Data reads are single
 * mmap accesses at known offsets. No hash table bucket scanning.
 */

typedef struct flat_state flat_state_t;

/* Account record: 116 bytes */
typedef struct __attribute__((packed)) {
    uint64_t nonce;              /*   8 bytes */
    uint8_t  balance[32];        /*  32 bytes (big-endian uint256) */
    uint8_t  code_hash[32];      /*  32 bytes */
    uint8_t  storage_root[32];   /*  32 bytes */
    uint64_t stor_file_offset;   /*   8 bytes (packed storage file offset) */
    uint32_t stor_file_count;    /*   4 bytes (number of slots) */
} flat_account_record_t;

_Static_assert(sizeof(flat_account_record_t) == 116,
               "flat_account_record_t must be 116 bytes");

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

flat_state_t *flat_state_create(const char *path);
flat_state_t *flat_state_open(const char *path);
void          flat_state_destroy(flat_state_t *fs);

/* =========================================================================
 * Account Operations (key = keccak256(addr)[32])
 * ========================================================================= */

bool flat_state_get_account(const flat_state_t *fs, const uint8_t addr_hash[32],
                             flat_account_record_t *out);
bool flat_state_put_account(flat_state_t *fs, const uint8_t addr_hash[32],
                             const flat_account_record_t *record);
bool flat_state_delete_account(flat_state_t *fs, const uint8_t addr_hash[32]);

/* =========================================================================
 * Storage Operations (key = addr_hash[32] + slot_hash[32] = 64 bytes)
 * ========================================================================= */

bool flat_state_get_storage(const flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             uint8_t value[32]);
bool flat_state_put_storage(flat_state_t *fs,
                             const uint8_t addr_hash[32],
                             const uint8_t slot_hash[32],
                             const uint8_t value[32]);
bool flat_state_delete_storage(flat_state_t *fs,
                                const uint8_t addr_hash[32],
                                const uint8_t slot_hash[32]);

/**
 * Delete ALL storage slots for an account (self-destruct).
 * Walks the storage compact_art subtree at addr_hash prefix and
 * removes all entries. Returns number of slots deleted.
 */
uint64_t flat_state_delete_all_storage(flat_state_t *fs,
                                        const uint8_t addr_hash[32]);

/* =========================================================================
 * Prefetch (non-blocking, warm page cache for upcoming reads)
 * ========================================================================= */

void flat_state_prefetch_account(const flat_state_t *fs, const uint8_t addr_hash[32]);

/* =========================================================================
 * Batch Operations (sorted by bucket for sequential I/O)
 * ========================================================================= */

bool flat_state_batch_put_accounts(flat_state_t *fs,
                                    const uint8_t *addr_hashes,
                                    const flat_account_record_t *records,
                                    uint32_t count);

bool flat_state_batch_put_storage(flat_state_t *fs,
                                   const uint8_t *keys,
                                   const uint8_t *values,
                                   uint32_t count);

/* =========================================================================
 * Internal Access (for storage_trie / art_mpt integration)
 * ========================================================================= */

struct compact_art;
struct flat_store;

/** Get the compact_art index for storage (non-owning). */
struct compact_art *flat_state_storage_art(flat_state_t *fs);

/** Get the flat_store for storage (non-owning). */
struct flat_store *flat_state_storage_store(flat_state_t *fs);

/** Get the compact_art index for accounts (non-owning). */
struct compact_art *flat_state_account_art(flat_state_t *fs);

/** Get the flat_store for accounts (non-owning). */
struct flat_store *flat_state_account_store(flat_state_t *fs);

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t flat_state_account_count(const flat_state_t *fs);
uint64_t flat_state_storage_count(const flat_state_t *fs);

#endif /* FLAT_STATE_H */
