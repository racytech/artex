#ifndef FLAT_STATE_H
#define FLAT_STATE_H

#include <stdint.h>
#include <stdbool.h>

/**
 * Flat State — O(1) Account/Storage Lookups via flat_store.
 *
 * Two flat_store instances (compact_art index + mmap'd data file):
 *   accounts: keccak256(addr)[32] → {nonce, balance, code_hash, storage_root}
 *   storage:  keccak256(addr_hash||slot_hash)[32] → value[32]
 *
 * Index lookups are in-memory (compact_art). Data reads are single
 * mmap accesses at known offsets. No hash table bucket scanning.
 */

typedef struct flat_state flat_state_t;

/* Account record: 104 bytes */
typedef struct __attribute__((packed)) {
    uint64_t nonce;              /*   8 bytes */
    uint8_t  balance[32];        /*  32 bytes (big-endian uint256) */
    uint8_t  code_hash[32];      /*  32 bytes */
    uint8_t  storage_root[32];   /*  32 bytes */
} flat_account_record_t;

_Static_assert(sizeof(flat_account_record_t) == 104,
               "flat_account_record_t must be 104 bytes");

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

flat_state_t *flat_state_create(const char *path,
                                 uint64_t account_cap, uint64_t storage_cap);
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
 * Stats
 * ========================================================================= */

uint64_t flat_state_account_count(const flat_state_t *fs);
uint64_t flat_state_storage_count(const flat_state_t *fs);

#endif /* FLAT_STATE_H */
