#ifndef FLAT_STATE_H
#define FLAT_STATE_H

#include <stdint.h>
#include <stdbool.h>

/**
 * Flat State — O(1) Account/Storage Lookups via mmap'd disk_hash.
 *
 * Provides a flat key-value store for EVM account and storage data,
 * bypassing expensive MPT trie traversals (5-10 node reads per lookup).
 *
 * Two disk_hash instances:
 *   <path>_flat_acct.idx — address[20] → {nonce, balance, code_hash, storage_root}
 *   <path>_flat_stor.idx — addr[20]+slot[32] → value[32]
 *
 * Populated incrementally during MPT root computation (dirty entries only).
 * Reads are lock-free (mmap). Survives checkpoint evictions — no cold restart.
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
void          flat_state_sync(flat_state_t *fs);

/* =========================================================================
 * Account Operations (key = address[20])
 * ========================================================================= */

bool flat_state_get_account(const flat_state_t *fs, const uint8_t addr[20],
                             flat_account_record_t *out);
bool flat_state_put_account(flat_state_t *fs, const uint8_t addr[20],
                             const flat_account_record_t *record);
bool flat_state_delete_account(flat_state_t *fs, const uint8_t addr[20]);

/* =========================================================================
 * Storage Operations (key = addr[20] + slot_be[32] = 52 bytes)
 * ========================================================================= */

bool flat_state_get_storage(const flat_state_t *fs, const uint8_t skey[52],
                             uint8_t value[32]);
bool flat_state_put_storage(flat_state_t *fs, const uint8_t skey[52],
                             const uint8_t value[32]);
bool flat_state_delete_storage(flat_state_t *fs, const uint8_t skey[52]);

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t flat_state_account_count(const flat_state_t *fs);
uint64_t flat_state_storage_count(const flat_state_t *fs);

#endif /* FLAT_STATE_H */
