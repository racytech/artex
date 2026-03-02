#include "../include/evm_state.h"
#include "../include/account.h"
#include "../include/mem_art.h"
#include "../include/mpt.h"
#include "../../common/include/keccak256.h"
#include "../../common/include/rlp.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

// ============================================================================
// Internal constants
// ============================================================================

#define JOURNAL_INIT_CAP   256
#define SLOT_KEY_SIZE      52   // addr[20] + slot[32]
#define MAX_CODE_SIZE      (24 * 1024 + 1)  // EIP-170: 24576 bytes

// ============================================================================
// Internal types
// ============================================================================

// --- Account cache ---

typedef struct cached_account {
    address_t  addr;            // key (20 bytes) — kept for finalize/revert
    account_t  account;
    uint8_t   *code;            // loaded bytecode (NULL until needed)
    uint32_t   code_size;
    bool dirty;
    bool code_dirty;
    bool created;               // newly created this execution
    bool existed;               // existed in state_db before
    bool self_destructed;
} cached_account_t;

// --- Storage cache ---

typedef struct cached_slot {
    uint8_t   key[SLOT_KEY_SIZE];   // addr[20] || slot_be[32] — kept for finalize
    uint256_t original;             // value when first loaded
    uint256_t current;
    bool dirty;
} cached_slot_t;

// --- Journal ---

typedef enum {
    JOURNAL_NONCE,
    JOURNAL_BALANCE,
    JOURNAL_CODE,
    JOURNAL_STORAGE,
    JOURNAL_ACCOUNT_CREATE,
    JOURNAL_SELF_DESTRUCT,
    JOURNAL_WARM_ADDR,
    JOURNAL_WARM_SLOT,
} journal_type_t;

typedef struct {
    journal_type_t type;
    address_t addr;
    union {
        uint64_t old_nonce;
        uint256_t old_balance;
        struct { hash_t old_hash; bool old_has_code; uint8_t *old_code; uint32_t old_code_size; } code;
        struct { uint256_t slot; uint256_t old_value; } storage;
        uint256_t slot;             // WARM_SLOT
    } data;
} journal_entry_t;

// --- Main struct ---

struct evm_state {
    state_db_t       *sdb;
    mem_art_t         accounts;     // key: addr[20], value: cached_account_t
    mem_art_t         storage;      // key: skey[52], value: cached_slot_t
    journal_entry_t  *journal;
    uint32_t          journal_len;
    uint32_t          journal_cap;
    mem_art_t         warm_addrs;   // key: addr[20], value: (none, 0 bytes)
    mem_art_t         warm_slots;   // key: skey[52], value: (none, 0 bytes)
    mpt_t            *state_mpt;    // persistent state trie (NULL = ephemeral)
    char             *state_mpt_path;
    mpt_t            *storage_mpt;  // persistent shared storage trie, 64-byte keys (NULL = ephemeral)
    char             *storage_mpt_path;
    bool              owns_state_mpt;   // true = opened internally, close on destroy
    bool              owns_storage_mpt; // false = externally owned, caller manages
};

// ============================================================================
// Internal helpers
// ============================================================================

static void hash_address(const address_t *addr, uint8_t out[32]) {
    hash_t h = hash_keccak256(addr->bytes, ADDRESS_SIZE);
    memcpy(out, h.bytes, 32);
}

static void hash_slot(const uint256_t *slot, uint8_t out[32]) {
    uint8_t slot_bytes[32];
    uint256_to_bytes(slot, slot_bytes);
    hash_t h = hash_keccak256(slot_bytes, 32);
    memcpy(out, h.bytes, 32);
}

static void make_slot_key(const address_t *addr, const uint256_t *slot,
                          uint8_t out[SLOT_KEY_SIZE]) {
    memcpy(out, addr->bytes, ADDRESS_SIZE);
    uint256_to_bytes(slot, out + ADDRESS_SIZE);
}

static bool journal_push(evm_state_t *es, const journal_entry_t *entry) {
    if (es->journal_len >= es->journal_cap) {
        uint32_t new_cap = es->journal_cap * 2;
        journal_entry_t *new_j = realloc(es->journal,
                                          new_cap * sizeof(journal_entry_t));
        if (!new_j) return false;
        es->journal = new_j;
        es->journal_cap = new_cap;
    }
    es->journal[es->journal_len++] = *entry;
    return true;
}

// Load account from state_db into cache. Creates empty if not found.
static cached_account_t *ensure_account(evm_state_t *es, const address_t *addr) {
    cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    if (ca) return ca;

    // Build on stack, insert into arena
    cached_account_t ca_local;
    memset(&ca_local, 0, sizeof(ca_local));
    address_copy(&ca_local.addr, addr);
    ca_local.account = account_empty();

    // Try loading from state_db
    uint8_t addr_hash[32];
    hash_address(addr, addr_hash);

    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint16_t buf_len = 0;
    if (sdb_get(es->sdb, addr_hash, buf, &buf_len)) {
        if (account_decode(buf, buf_len, &ca_local.account)) {
            ca_local.existed = true;
        }
    }

    if (!mem_art_insert(&es->accounts, addr->bytes, ADDRESS_SIZE,
                        &ca_local, sizeof(cached_account_t)))
        return NULL;

    return (cached_account_t *)mem_art_get_mut(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
}

// Load storage slot from state_db into cache.
static cached_slot_t *ensure_slot(evm_state_t *es, const address_t *addr,
                                  const uint256_t *slot) {
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, slot, skey);

    cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
        &es->storage, skey, SLOT_KEY_SIZE, NULL);
    if (cs) return cs;

    // Build on stack, insert into arena
    cached_slot_t cs_local;
    memset(&cs_local, 0, sizeof(cs_local));
    memcpy(cs_local.key, skey, SLOT_KEY_SIZE);
    cs_local.original = UINT256_ZERO_INIT;
    cs_local.current = UINT256_ZERO_INIT;

    // Try loading from state_db
    uint8_t addr_hash[32], slot_hash[32];
    hash_address(addr, addr_hash);
    hash_slot(slot, slot_hash);

    uint8_t val_buf[32];
    uint16_t val_len = 0;
    if (sdb_get_storage(es->sdb, addr_hash, slot_hash, val_buf, &val_len)) {
        cs_local.original = uint256_from_bytes(val_buf, val_len);
        cs_local.current = cs_local.original;
    }

    if (!mem_art_insert(&es->storage, skey, SLOT_KEY_SIZE,
                        &cs_local, sizeof(cached_slot_t)))
        return NULL;

    return (cached_slot_t *)mem_art_get_mut(
        &es->storage, skey, SLOT_KEY_SIZE, NULL);
}

// ============================================================================
// Lifecycle
// ============================================================================

evm_state_t *evm_state_create_ex(state_db_t *sdb,
                                  mpt_t *state_mpt,
                                  mpt_t *storage_mpt) {
    if (!sdb) return NULL;

    evm_state_t *es = calloc(1, sizeof(evm_state_t));
    if (!es) return NULL;

    es->sdb = sdb;
    es->journal_cap = JOURNAL_INIT_CAP;
    es->journal = malloc(es->journal_cap * sizeof(journal_entry_t));
    if (!es->journal) {
        free(es);
        return NULL;
    }

    if (!mem_art_init(&es->accounts) ||
        !mem_art_init(&es->storage) ||
        !mem_art_init(&es->warm_addrs) ||
        !mem_art_init(&es->warm_slots)) {
        mem_art_destroy(&es->accounts);
        mem_art_destroy(&es->storage);
        mem_art_destroy(&es->warm_addrs);
        mem_art_destroy(&es->warm_slots);
        free(es->journal);
        free(es);
        return NULL;
    }

    // Externally-owned MPT handles (caller manages commit/close)
    es->state_mpt = state_mpt;
    es->storage_mpt = storage_mpt;
    es->owns_state_mpt = false;
    es->owns_storage_mpt = false;

    return es;
}

evm_state_t *evm_state_create(state_db_t *sdb, const char *state_mpt_path,
                               const char *storage_mpt_path) {
    // Open or create persistent MPTs
    mpt_t *state_mpt = NULL;
    mpt_t *storage_mpt = NULL;

    if (state_mpt_path) {
        state_mpt = mpt_open(state_mpt_path);
        if (!state_mpt) state_mpt = mpt_create(state_mpt_path);
        if (!state_mpt) return NULL;
    }

    if (storage_mpt_path) {
        storage_mpt = mpt_open(storage_mpt_path);
        if (!storage_mpt) storage_mpt = mpt_create_ex(storage_mpt_path, 64, 33);
        if (!storage_mpt) {
            if (state_mpt) mpt_close(state_mpt);
            return NULL;
        }
    }

    evm_state_t *es = evm_state_create_ex(sdb, state_mpt, storage_mpt);
    if (!es) {
        if (state_mpt) mpt_close(state_mpt);
        if (storage_mpt) mpt_close(storage_mpt);
        return NULL;
    }

    // Path-based creation owns the handles
    es->owns_state_mpt = (state_mpt != NULL);
    es->owns_storage_mpt = (storage_mpt != NULL);
    if (state_mpt_path) es->state_mpt_path = strdup(state_mpt_path);
    if (storage_mpt_path) es->storage_mpt_path = strdup(storage_mpt_path);

    return es;
}

// Callback: free heap-allocated code pointers before arena destroy
static bool free_code_cb(const uint8_t *key, size_t key_len,
                          const void *value, size_t value_len,
                          void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_account_t *ca = (cached_account_t *)value;
    free(ca->code);
    return true;
}

void evm_state_destroy(evm_state_t *es) {
    if (!es) return;

    // Free code pointers owned by cached accounts
    mem_art_foreach(&es->accounts, free_code_cb, NULL);

    // Destroy all trees (O(1) — just frees arenas)
    mem_art_destroy(&es->accounts);
    mem_art_destroy(&es->storage);
    mem_art_destroy(&es->warm_addrs);
    mem_art_destroy(&es->warm_slots);

    // Free any code pointers still owned by journal entries (not reverted)
    for (uint32_t i = 0; i < es->journal_len; i++) {
        if (es->journal[i].type == JOURNAL_CODE) {
            free(es->journal[i].data.code.old_code);
        }
    }

    // Close persistent MPTs only if we own them (path-based creation)
    if (es->owns_state_mpt && es->state_mpt) mpt_close(es->state_mpt);
    free(es->state_mpt_path);
    if (es->owns_storage_mpt && es->storage_mpt) mpt_close(es->storage_mpt);
    free(es->storage_mpt_path);

    free(es->journal);
    free(es);
}

// ============================================================================
// Account Existence
// ============================================================================

bool evm_state_exists(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return false;
    if (ca->self_destructed) return false;
    return ca->existed || ca->created;
}

bool evm_state_is_empty(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return true;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return true;
    return ca->account.nonce == 0 &&
           uint256_is_zero(&ca->account.balance) &&
           !ca->account.has_code;
}

// ============================================================================
// Nonce
// ============================================================================

uint64_t evm_state_get_nonce(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return 0;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return 0;
    return ca->account.nonce;
}

void evm_state_set_nonce(evm_state_t *es, const address_t *addr, uint64_t nonce) {
    if (!es || !addr) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_NONCE,
        .addr = *addr,
        .data.old_nonce = ca->account.nonce
    };
    journal_push(es, &je);

    ca->account.nonce = nonce;
    ca->dirty = true;
}

void evm_state_increment_nonce(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return;
    uint64_t n = evm_state_get_nonce(es, addr);
    evm_state_set_nonce(es, addr, n + 1);
}

// ============================================================================
// Balance
// ============================================================================

uint256_t evm_state_get_balance(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return UINT256_ZERO_INIT;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return UINT256_ZERO_INIT;
    return ca->account.balance;
}

void evm_state_set_balance(evm_state_t *es, const address_t *addr,
                           const uint256_t *balance) {
    if (!es || !addr || !balance) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_BALANCE,
        .addr = *addr,
        .data.old_balance = ca->account.balance
    };
    journal_push(es, &je);

    ca->account.balance = *balance;
    ca->dirty = true;
}

void evm_state_add_balance(evm_state_t *es, const address_t *addr,
                           const uint256_t *amount) {
    if (!es || !addr || !amount) return;
    uint256_t bal = evm_state_get_balance(es, addr);
    uint256_t new_bal = uint256_add(&bal, amount);
    evm_state_set_balance(es, addr, &new_bal);
}

bool evm_state_sub_balance(evm_state_t *es, const address_t *addr,
                           const uint256_t *amount) {
    if (!es || !addr || !amount) return false;
    uint256_t bal = evm_state_get_balance(es, addr);
    if (uint256_lt(&bal, amount)) return false;
    uint256_t new_bal = uint256_sub(&bal, amount);
    evm_state_set_balance(es, addr, &new_bal);
    return true;
}

// ============================================================================
// Code
// ============================================================================

hash_t evm_state_get_code_hash(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return hash_zero();
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return hash_zero();
    if (!ca->account.has_code) return HASH_EMPTY_CODE;
    return ca->account.code_hash;
}

uint32_t evm_state_get_code_size(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return 0;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return 0;

    // If code is cached, return cached size
    if (ca->code) return ca->code_size;

    // If no code, return 0
    if (!ca->account.has_code) return 0;

    // Load code size from state_db
    uint8_t addr_hash[32];
    hash_address(addr, addr_hash);
    ca->code_size = sdb_code_length(es->sdb, addr_hash);
    return ca->code_size;
}

bool evm_state_get_code(evm_state_t *es, const address_t *addr,
                        uint8_t *out, uint32_t *out_len) {
    if (!es || !addr) return false;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return false;

    if (!ca->account.has_code) {
        if (out_len) *out_len = 0;
        return true;
    }

    // Load code into cache if not already loaded
    if (!ca->code) {
        uint8_t addr_hash[32];
        hash_address(addr, addr_hash);

        uint32_t len = 0;
        // First get the length
        len = sdb_code_length(es->sdb, addr_hash);
        if (len == 0) return false;

        ca->code = malloc(len);
        if (!ca->code) return false;

        uint32_t got_len = 0;
        if (!sdb_get_code(es->sdb, addr_hash, ca->code, &got_len)) {
            free(ca->code);
            ca->code = NULL;
            return false;
        }
        ca->code_size = got_len;
    }

    if (out && out_len) {
        uint32_t copy_len = ca->code_size;
        if (copy_len > *out_len) copy_len = *out_len;
        memcpy(out, ca->code, copy_len);
        *out_len = ca->code_size;
    } else if (out_len) {
        *out_len = ca->code_size;
    }

    return true;
}

void evm_state_set_code(evm_state_t *es, const address_t *addr,
                        const uint8_t *code, uint32_t len) {
    if (!es || !addr) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    // Journal old state — transfer code ownership to journal
    journal_entry_t je = {
        .type = JOURNAL_CODE,
        .addr = *addr,
        .data.code = {
            .old_hash = ca->account.code_hash,
            .old_has_code = ca->account.has_code,
            .old_code = ca->code,
            .old_code_size = ca->code_size
        }
    };
    journal_push(es, &je);

    // Old code ownership transferred to journal — don't free
    ca->code = NULL;
    ca->code_size = 0;

    if (code && len > 0) {
        ca->code = malloc(len);
        if (ca->code) {
            memcpy(ca->code, code, len);
            ca->code_size = len;
        }
        ca->account.has_code = true;
        ca->account.code_hash = hash_keccak256(code, len);
    } else {
        ca->account.has_code = false;
        ca->account.code_hash = hash_zero();
    }

    ca->dirty = true;
    ca->code_dirty = true;
}

// ============================================================================
// Storage
// ============================================================================

uint256_t evm_state_get_storage(evm_state_t *es, const address_t *addr,
                                const uint256_t *key) {
    if (!es || !addr || !key) return UINT256_ZERO_INIT;
    cached_slot_t *cs = ensure_slot(es, addr, key);
    if (!cs) return UINT256_ZERO_INIT;
    return cs->current;
}

void evm_state_set_storage(evm_state_t *es, const address_t *addr,
                           const uint256_t *key, const uint256_t *value) {
    if (!es || !addr || !key || !value) return;
    cached_slot_t *cs = ensure_slot(es, addr, key);
    if (!cs) return;

    // Journal old value
    journal_entry_t je = {
        .type = JOURNAL_STORAGE,
        .addr = *addr,
        .data.storage = {
            .slot = *key,
            .old_value = cs->current
        }
    };
    journal_push(es, &je);

    cs->current = *value;
    cs->dirty = true;
}

// ============================================================================
// Account Creation
// ============================================================================

void evm_state_create_account(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    // Journal the creation
    journal_entry_t je = {
        .type = JOURNAL_ACCOUNT_CREATE,
        .addr = *addr,
    };
    journal_push(es, &je);

    // Reset account to empty
    ca->account = account_empty();
    free(ca->code);
    ca->code = NULL;
    ca->code_size = 0;
    ca->created = true;
    ca->dirty = true;
    ca->code_dirty = false;
    ca->self_destructed = false;
}

// ============================================================================
// Self-Destruct
// ============================================================================

void evm_state_self_destruct(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_SELF_DESTRUCT,
        .addr = *addr,
    };
    journal_push(es, &je);

    ca->self_destructed = true;
    ca->dirty = true;
}

bool evm_state_is_self_destructed(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;
    const cached_account_t *ca = (const cached_account_t *)mem_art_get(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    if (!ca) return false;
    return ca->self_destructed;
}

// ============================================================================
// Snapshot / Revert
// ============================================================================

uint32_t evm_state_snapshot(evm_state_t *es) {
    if (!es) return 0;
    return es->journal_len;
}

void evm_state_revert(evm_state_t *es, uint32_t snap_id) {
    if (!es) return;
    if (snap_id > es->journal_len) return;

    while (es->journal_len > snap_id) {
        es->journal_len--;
        journal_entry_t *je = &es->journal[es->journal_len];

        switch (je->type) {
        case JOURNAL_NONCE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) ca->account.nonce = je->data.old_nonce;
            break;
        }
        case JOURNAL_BALANCE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) ca->account.balance = je->data.old_balance;
            break;
        }
        case JOURNAL_CODE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->account.code_hash = je->data.code.old_hash;
                ca->account.has_code = je->data.code.old_has_code;
                // Free current code, restore old code from journal
                free(ca->code);
                ca->code = je->data.code.old_code;
                ca->code_size = je->data.code.old_code_size;
                // Don't clear code_dirty — pre-snapshot code may still be dirty
            }
            // Ownership transferred back, clear journal's pointer
            je->data.code.old_code = NULL;
            break;
        }
        case JOURNAL_STORAGE: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.storage.slot, skey);
            cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
                &es->storage, skey, SLOT_KEY_SIZE, NULL);
            if (cs) {
                cs->current = je->data.storage.old_value;
                // If reverted back to original, no longer dirty
                cs->dirty = !uint256_is_equal(&cs->current, &cs->original);
            }
            break;
        }
        case JOURNAL_ACCOUNT_CREATE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->created = false;
                // If it didn't exist before, it's back to not existing
                // The account stays in cache but will be treated as non-existent
            }
            break;
        }
        case JOURNAL_SELF_DESTRUCT: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) ca->self_destructed = false;
            break;
        }
        case JOURNAL_WARM_ADDR: {
            mem_art_delete(&es->warm_addrs, je->addr.bytes, ADDRESS_SIZE);
            break;
        }
        case JOURNAL_WARM_SLOT: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.slot, skey);
            mem_art_delete(&es->warm_slots, skey, SLOT_KEY_SIZE);
            break;
        }
        }
    }
}

// ============================================================================
// Access Lists (EIP-2929)
// ============================================================================

bool evm_state_warm_address(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;

    if (mem_art_contains(&es->warm_addrs, addr->bytes, ADDRESS_SIZE))
        return true;  // already warm

    mem_art_insert(&es->warm_addrs, addr->bytes, ADDRESS_SIZE, NULL, 0);

    // Journal for revert
    journal_entry_t je = {
        .type = JOURNAL_WARM_ADDR,
        .addr = *addr,
    };
    journal_push(es, &je);

    return false;  // was cold
}

bool evm_state_warm_slot(evm_state_t *es, const address_t *addr,
                         const uint256_t *key) {
    if (!es || !addr || !key) return false;

    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);

    if (mem_art_contains(&es->warm_slots, skey, SLOT_KEY_SIZE))
        return true;  // already warm

    mem_art_insert(&es->warm_slots, skey, SLOT_KEY_SIZE, NULL, 0);

    // Journal for revert
    journal_entry_t je = {
        .type = JOURNAL_WARM_SLOT,
        .addr = *addr,
        .data.slot = *key,
    };
    journal_push(es, &je);

    return false;  // was cold
}

bool evm_state_is_address_warm(const evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;
    return mem_art_contains(&es->warm_addrs, addr->bytes, ADDRESS_SIZE);
}

bool evm_state_is_slot_warm(const evm_state_t *es, const address_t *addr,
                            const uint256_t *key) {
    if (!es || !addr || !key) return false;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    return mem_art_contains(&es->warm_slots, skey, SLOT_KEY_SIZE);
}

// ============================================================================
// Finalize
// ============================================================================

typedef struct {
    evm_state_t *es;
    bool ok;
} finalize_ctx_t;

static bool finalize_account_cb(const uint8_t *key, size_t key_len,
                                 const void *value, size_t value_len,
                                 void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    finalize_ctx_t *ctx = (finalize_ctx_t *)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;

    if (ca->self_destructed) {
        uint8_t addr_hash[32];
        hash_address(&ca->addr, addr_hash);
        sdb_delete(ctx->es->sdb, addr_hash);
        return true;
    }

    if (!ca->dirty && !ca->code_dirty) return true;

    uint8_t addr_hash[32];
    hash_address(&ca->addr, addr_hash);

    if (ca->dirty) {
        // Skip writing empty accounts that never existed (inline is_empty check)
        bool is_empty = (ca->account.nonce == 0 &&
                         uint256_is_zero(&ca->account.balance) &&
                         !ca->account.has_code);
        if (!ca->existed && !ca->created && is_empty) return true;

        uint8_t buf[ACCOUNT_MAX_ENCODED];
        uint16_t len = account_encode(&ca->account, buf);
        if (len == 0) { ctx->ok = false; return false; }
        if (!sdb_put(ctx->es->sdb, addr_hash, buf, len)) {
            ctx->ok = false;
            return false;
        }
    }

    if (ca->code_dirty && ca->code && ca->code_size > 0) {
        if (!sdb_put_code(ctx->es->sdb, addr_hash, ca->code, ca->code_size)) {
            ctx->ok = false;
            return false;
        }
    }

    return true;
}

static bool finalize_storage_cb(const uint8_t *key, size_t key_len,
                                 const void *value, size_t value_len,
                                 void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    finalize_ctx_t *ctx = (finalize_ctx_t *)user_data;
    const cached_slot_t *cs = (const cached_slot_t *)value;

    if (!cs->dirty) return true;

    // Extract address and slot from composite key
    address_t addr = address_from_bytes(cs->key);
    uint256_t slot = uint256_from_bytes(cs->key + ADDRESS_SIZE, 32);

    uint8_t addr_hash[32], slot_hash[32];
    hash_address(&addr, addr_hash);
    hash_slot(&slot, slot_hash);

    if (uint256_is_zero(&cs->current)) {
        // Delete zero-valued slots
        sdb_delete_storage(ctx->es->sdb, addr_hash, slot_hash);
    } else {
        uint8_t val_bytes[32];
        uint256_to_bytes(&cs->current, val_bytes);
        // Store only significant bytes
        uint8_t start = 0;
        while (start < 31 && val_bytes[start] == 0) start++;
        uint16_t val_len = 32 - start;
        if (!sdb_put_storage(ctx->es->sdb, addr_hash, slot_hash,
                             val_bytes + start, val_len)) {
            ctx->ok = false;
            return false;
        }
    }

    return true;
}

bool evm_state_finalize(evm_state_t *es) {
    if (!es) return false;

    finalize_ctx_t ctx = { .es = es, .ok = true };

    // Write dirty accounts
    mem_art_foreach(&es->accounts, finalize_account_cb, &ctx);
    if (!ctx.ok) return false;

    // Write dirty storage slots
    mem_art_foreach(&es->storage, finalize_storage_cb, &ctx);
    if (!ctx.ok) return false;

    // Commit persistent MPT meta pages (only if we own them;
    // externally-owned MPTs are committed by the caller at checkpoint)
    if (es->owns_state_mpt && es->state_mpt)
        mpt_commit(es->state_mpt);
    if (es->owns_storage_mpt && es->storage_mpt)
        mpt_commit(es->storage_mpt);

    return true;
}

// ============================================================================
// State Root Computation
// ============================================================================

#define EPHEMERAL_STORAGE_MPT_PATH "/tmp/evm_storage_mpt.dat"
#define EPHEMERAL_STATE_MPT_PATH   "/tmp/evm_state_mpt.dat"

// --- RLP-encode a storage value (trimmed big-endian uint256) ---

static uint8_t rlp_encode_storage_value(const uint256_t *val,
                                         uint8_t out[33]) {
    uint8_t raw[32];
    uint256_to_bytes(val, raw);
    uint8_t start = 0;
    while (start < 31 && raw[start] == 0) start++;
    uint8_t trimmed_len = 32 - start;

    if (trimmed_len == 1 && raw[start] < 0x80) {
        out[0] = raw[start];
        return 1;
    }
    out[0] = 0x80 + trimmed_len;
    memcpy(out + 1, raw + start, trimmed_len);
    return 1 + trimmed_len;
}

// --- Persistent storage mpt: update dirty slots ---

typedef struct {
    mpt_t *storage_mpt;
} persistent_storage_ctx_t;

static bool update_persistent_storage_cb(const uint8_t *key, size_t key_len,
                                          const void *value, size_t value_len,
                                          void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    persistent_storage_ctx_t *ctx = (persistent_storage_ctx_t *)user_data;
    const cached_slot_t *cs = (const cached_slot_t *)value;

    if (!cs->dirty) return true;

    // Build 64-byte key: keccak(addr) || keccak(slot)
    address_t addr = address_from_bytes(cs->key);
    uint8_t key64[64];
    hash_t ah = hash_keccak256(addr.bytes, ADDRESS_SIZE);
    memcpy(key64, ah.bytes, 32);
    hash_t sh = hash_keccak256(cs->key + ADDRESS_SIZE, 32);
    memcpy(key64 + 32, sh.bytes, 32);

    if (uint256_is_zero(&cs->current)) {
        mpt_delete(ctx->storage_mpt, key64);
    } else {
        uint8_t rlp_val[33];
        uint8_t rlp_len = rlp_encode_storage_value(&cs->current, rlp_val);
        mpt_put(ctx->storage_mpt, key64, rlp_val, rlp_len);
    }

    return true;
}

// --- Ephemeral storage root map: addr → hash_t, built in a single pass ---

typedef struct {
    address_t addr;
    hash_t    storage_root;
} storage_root_entry_t;

typedef struct {
    storage_root_entry_t *entries;
    size_t                count;
    size_t                cap;
} storage_root_map_t;

static hash_t *storage_root_map_get(storage_root_map_t *map,
                                     const address_t *addr) {
    for (size_t i = 0; i < map->count; i++) {
        if (memcmp(map->entries[i].addr.bytes, addr->bytes, ADDRESS_SIZE) == 0)
            return &map->entries[i].storage_root;
    }
    return NULL;
}

// Callback context for the single-pass storage scan (ephemeral mode)
typedef struct {
    mpt_t            *mpt;
    storage_root_map_t *map;
    address_t         cur_addr;
    bool              has_cur;
    size_t            cur_count;
} storage_scan_ctx_t;

static void storage_scan_flush(storage_scan_ctx_t *ctx) {
    if (!ctx->has_cur) return;

    hash_t root = (ctx->cur_count > 0) ? mpt_root(ctx->mpt) : HASH_EMPTY_STORAGE;

    storage_root_map_t *map = ctx->map;
    if (map->count >= map->cap) {
        size_t new_cap = map->cap == 0 ? 32 : map->cap * 2;
        map->entries = realloc(map->entries, new_cap * sizeof(storage_root_entry_t));
        map->cap = new_cap;
    }
    map->entries[map->count].addr = ctx->cur_addr;
    map->entries[map->count].storage_root = root;
    map->count++;

    ctx->has_cur = false;
    ctx->cur_count = 0;
}

static void storage_scan_put_slot(storage_scan_ctx_t *ctx,
                                   const cached_slot_t *cs) {
    if (!ctx->has_cur ||
        memcmp(ctx->cur_addr.bytes, cs->key, ADDRESS_SIZE) != 0) {
        storage_scan_flush(ctx);
        mpt_clear(ctx->mpt);
        memcpy(ctx->cur_addr.bytes, cs->key, ADDRESS_SIZE);
        ctx->has_cur = true;
        ctx->cur_count = 0;
    }

    if (uint256_is_zero(&cs->current)) return;

    hash_t hk = hash_keccak256(cs->key + ADDRESS_SIZE, 32);

    uint8_t rlp_val[33];
    uint8_t rlp_len = rlp_encode_storage_value(&cs->current, rlp_val);

    mpt_put(ctx->mpt, hk.bytes, rlp_val, rlp_len);
    ctx->cur_count++;
}

static bool storage_scan_cb(const uint8_t *key, size_t key_len,
                              const void *value, size_t value_len,
                              void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    storage_scan_put_slot((storage_scan_ctx_t *)user_data,
                           (const cached_slot_t *)value);
    return true;
}

static storage_root_map_t build_storage_roots(evm_state_t *es,
                                               mpt_t *storage_mpt) {
    storage_root_map_t map = {0};
    storage_scan_ctx_t ctx = {
        .mpt = storage_mpt,
        .map = &map,
        .has_cur = false,
        .cur_count = 0,
    };

    mem_art_foreach(&es->storage, storage_scan_cb, &ctx);
    storage_scan_flush(&ctx);

    return map;
}

// --- Account trie ---

static size_t uint256_to_trimmed_be(const uint256_t *val, uint8_t out[32]) {
    uint256_to_bytes(val, out);
    size_t start = 0;
    while (start < 32 && out[start] == 0) start++;
    size_t len = 32 - start;
    if (start > 0 && len > 0) memmove(out, out + start, len);
    return len;
}

typedef struct {
    mpt_t              *state_mpt;
    mpt_t              *storage_mpt;       // persistent shared storage trie (NULL = use map)
    storage_root_map_t *storage_roots;     // ephemeral storage root map (NULL if using storage_mpt)
    bool                prune_empty;
    bool                incremental;
} account_root_ctx_t;

static void account_mpt_put(mpt_t *state_mpt,
                              const cached_account_t *ca,
                              const hash_t *storage_root) {
    hash_t code_hash = ca->account.has_code ? ca->account.code_hash
                                            : HASH_EMPTY_CODE;

    rlp_item_t *list = rlp_list_new();
    rlp_list_append(list, rlp_uint64(ca->account.nonce));

    uint8_t bal_be[32];
    size_t bal_len = uint256_to_trimmed_be(&ca->account.balance, bal_be);
    rlp_list_append(list, rlp_string(bal_be, bal_len));
    rlp_list_append(list, rlp_string(storage_root->bytes, 32));
    rlp_list_append(list, rlp_string(code_hash.bytes, 32));

    bytes_t encoded = rlp_encode(list);
    rlp_item_free(list);

    hash_t hk = hash_keccak256(ca->addr.bytes, ADDRESS_SIZE);
    mpt_put(state_mpt, hk.bytes, encoded.data, (uint8_t)encoded.len);
    free(encoded.data);
}

static bool account_root_cb(const uint8_t *key, size_t key_len,
                              const void *value, size_t value_len,
                              void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    account_root_ctx_t *ctx = (account_root_ctx_t *)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;

    // In incremental mode, only process accounts that changed this block
    if (ctx->incremental) {
        if (!ca->dirty && !ca->created && !ca->code_dirty && !ca->self_destructed)
            return true;
    }

    // Self-destructed: delete from persistent mpt
    if (ca->self_destructed) {
        if (ctx->incremental) {
            hash_t hk = hash_keccak256(ca->addr.bytes, ADDRESS_SIZE);
            mpt_delete(ctx->state_mpt, hk.bytes);
        }
        return true;
    }

    // Skip empty accounts based on EIP-161 rules
    bool is_empty = (ca->account.nonce == 0 &&
                     uint256_is_zero(&ca->account.balance) &&
                     !ca->account.has_code);
    if (is_empty) {
        if (ctx->prune_empty) {
            if (ctx->incremental && (ca->existed || ca->created)) {
                hash_t hk = hash_keccak256(ca->addr.bytes, ADDRESS_SIZE);
                mpt_delete(ctx->state_mpt, hk.bytes);
            }
            return true;
        }
        if (!ca->existed && !ca->created && !ca->dirty) return true;
    }

    // Compute storage root
    hash_t storage_root = HASH_EMPTY_STORAGE;
    if (ctx->storage_mpt) {
        // Persistent mode: subtree root from shared storage mpt
        uint8_t addr_hash[32];
        hash_t ah = hash_keccak256(ca->addr.bytes, ADDRESS_SIZE);
        memcpy(addr_hash, ah.bytes, 32);
        storage_root = mpt_subtree_root(ctx->storage_mpt, addr_hash, 64);
    } else if (ctx->storage_roots) {
        hash_t *sr = storage_root_map_get(ctx->storage_roots, &ca->addr);
        if (sr) storage_root = *sr;
    }

    account_mpt_put(ctx->state_mpt, ca, &storage_root);
    return true;
}

hash_t evm_state_compute_state_root(evm_state_t *es) {
    return evm_state_compute_state_root_ex(es, true);
}

hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty) {
    if (!es) return HASH_EMPTY_STORAGE;
    if (mem_art_is_empty(&es->accounts)) return HASH_EMPTY_STORAGE;

    // --- Persistent mode (both state_mpt and storage_mpt present) ---
    if (es->state_mpt && es->storage_mpt) {
        // Phase 1: update persistent storage mpt with dirty slots
        persistent_storage_ctx_t sctx = { .storage_mpt = es->storage_mpt };
        mem_art_foreach(&es->storage, update_persistent_storage_cb, &sctx);

        // Phase 2: update state mpt with dirty accounts (storage roots via subtree)
        account_root_ctx_t ctx = {
            .state_mpt = es->state_mpt,
            .storage_mpt = es->storage_mpt,
            .storage_roots = NULL,
            .prune_empty = prune_empty,
            .incremental = true,
        };
        mem_art_foreach(&es->accounts, account_root_cb, &ctx);

        if (mpt_size(es->state_mpt) == 0)
            return HASH_EMPTY_STORAGE;
        return mpt_root(es->state_mpt);
    }

    // --- Semi-persistent mode (state_mpt only, ephemeral storage) ---
    // Build storage roots ephemerally
    unlink(EPHEMERAL_STORAGE_MPT_PATH);
    mpt_t *storage_mpt = mpt_create(EPHEMERAL_STORAGE_MPT_PATH);
    if (!storage_mpt) return HASH_EMPTY_STORAGE;

    storage_root_map_t sr_map = build_storage_roots(es, storage_mpt);

    mpt_close(storage_mpt);
    unlink(EPHEMERAL_STORAGE_MPT_PATH);

    if (es->state_mpt) {
        account_root_ctx_t ctx = {
            .state_mpt = es->state_mpt,
            .storage_mpt = NULL,
            .storage_roots = &sr_map,
            .prune_empty = prune_empty,
            .incremental = true,
        };
        mem_art_foreach(&es->accounts, account_root_cb, &ctx);
        free(sr_map.entries);

        if (mpt_size(es->state_mpt) == 0)
            return HASH_EMPTY_STORAGE;
        return mpt_root(es->state_mpt);
    }

    // --- Fully ephemeral mode: rebuild from scratch ---
    unlink(EPHEMERAL_STATE_MPT_PATH);
    mpt_t *state_mpt = mpt_create(EPHEMERAL_STATE_MPT_PATH);
    if (!state_mpt) {
        free(sr_map.entries);
        return HASH_EMPTY_STORAGE;
    }

    account_root_ctx_t ctx = {
        .state_mpt = state_mpt,
        .storage_mpt = NULL,
        .storage_roots = &sr_map,
        .prune_empty = prune_empty,
        .incremental = false,
    };
    mem_art_foreach(&es->accounts, account_root_cb, &ctx);

    hash_t root;
    if (mpt_size(state_mpt) == 0)
        root = HASH_EMPTY_STORAGE;
    else
        root = mpt_root(state_mpt);

    mpt_close(state_mpt);
    unlink(EPHEMERAL_STATE_MPT_PATH);
    free(sr_map.entries);

    return root;
}
