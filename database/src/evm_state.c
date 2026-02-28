#include "../include/evm_state.h"
#include "../include/account.h"
#include "../include/mem_art.h"
#include "../include/intermediate_hashes.h"
#include "../../common/include/keccak256.h"
#include "../../common/include/rlp.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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

evm_state_t *evm_state_create(state_db_t *sdb) {
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

const uint8_t *evm_state_get_code_ptr(evm_state_t *es, const address_t *addr,
                                       uint32_t *out_len) {
    if (!es || !addr) {
        if (out_len) *out_len = 0;
        return NULL;
    }
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca || !ca->account.has_code) {
        if (out_len) *out_len = 0;
        return NULL;
    }

    // Load code into cache if not already loaded
    if (!ca->code) {
        uint8_t addr_hash[32];
        hash_address(addr, addr_hash);

        uint32_t len = sdb_code_length(es->sdb, addr_hash);
        if (len == 0) {
            if (out_len) *out_len = 0;
            return NULL;
        }

        ca->code = malloc(len);
        if (!ca->code) {
            if (out_len) *out_len = 0;
            return NULL;
        }

        uint32_t got_len = 0;
        if (!sdb_get_code(es->sdb, addr_hash, ca->code, &got_len)) {
            free(ca->code);
            ca->code = NULL;
            if (out_len) *out_len = 0;
            return NULL;
        }
        ca->code_size = got_len;
    }

    if (out_len) *out_len = ca->code_size;
    return ca->code;
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
    return ctx.ok;
}

// ============================================================================
// State Root Computation
// ============================================================================

// --- Storage root helpers ---

// Entry for sorting hashed storage slots
typedef struct {
    uint8_t hashed_key[32];
    uint8_t value[32];     // trimmed big-endian value
    uint16_t value_len;
} storage_entry_t;

// Collector context for iterating storage slots of one address
typedef struct {
    const uint8_t *target_addr;  // 20-byte address to match
    storage_entry_t *entries;
    size_t count;
    size_t cap;
} storage_collect_ctx_t;

static int compare_32b_keys(const void *a, const void *b) {
    return memcmp(a, b, 32);
}

static bool collect_storage_cb(const uint8_t *key, size_t key_len,
                                const void *value, size_t value_len,
                                void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    storage_collect_ctx_t *ctx = (storage_collect_ctx_t *)user_data;
    const cached_slot_t *cs = (const cached_slot_t *)value;

    // Only collect slots for the target address
    if (memcmp(cs->key, ctx->target_addr, ADDRESS_SIZE) != 0) return true;

    // Skip zero-valued slots
    if (uint256_is_zero(&cs->current)) return true;

    // Grow array if needed
    if (ctx->count >= ctx->cap) {
        size_t new_cap = ctx->cap == 0 ? 16 : ctx->cap * 2;
        storage_entry_t *new_entries = realloc(ctx->entries,
                                                new_cap * sizeof(storage_entry_t));
        if (!new_entries) return false;
        ctx->entries = new_entries;
        ctx->cap = new_cap;
    }

    storage_entry_t *e = &ctx->entries[ctx->count];

    // Hash the slot key: keccak256(slot_be[32])
    hash_t h = hash_keccak256(cs->key + ADDRESS_SIZE, 32);
    memcpy(e->hashed_key, h.bytes, 32);

    // Trimmed big-endian value
    uint256_to_bytes(&cs->current, e->value);
    uint8_t start = 0;
    while (start < 31 && e->value[start] == 0) start++;
    e->value_len = 32 - start;
    if (start > 0) memmove(e->value, e->value + start, e->value_len);

    ctx->count++;
    return true;
}

static hash_t compute_storage_root(evm_state_t *es, const address_t *addr) {
    storage_collect_ctx_t ctx = {
        .target_addr = addr->bytes,
        .entries = NULL,
        .count = 0,
        .cap = 0
    };

    mem_art_foreach(&es->storage, collect_storage_cb, &ctx);

    if (ctx.count == 0) {
        free(ctx.entries);
        return HASH_EMPTY_STORAGE;
    }

    // Sort by hashed key
    qsort(ctx.entries, ctx.count, sizeof(storage_entry_t), compare_32b_keys);

    // Build arrays for ih_build
    const uint8_t **keys = malloc(ctx.count * sizeof(uint8_t *));
    const uint8_t **values = malloc(ctx.count * sizeof(uint8_t *));
    uint16_t *value_lens = malloc(ctx.count * sizeof(uint16_t));

    for (size_t i = 0; i < ctx.count; i++) {
        keys[i] = ctx.entries[i].hashed_key;
        values[i] = ctx.entries[i].value;
        value_lens[i] = ctx.entries[i].value_len;
    }

    ih_state_t *ih = ih_create();
    hash_t root = ih_build(ih, keys, values, value_lens, ctx.count);
    ih_destroy(ih);

    free(keys);
    free(values);
    free(value_lens);
    free(ctx.entries);
    return root;
}

// --- Account trie helpers ---

typedef struct {
    uint8_t hashed_key[32];
    uint8_t *rlp_value;    // heap-allocated RLP([nonce, balance, storageRoot, codeHash])
    uint16_t rlp_len;
} account_entry_t;

typedef struct {
    evm_state_t *es;
    account_entry_t *entries;
    size_t count;
    size_t cap;
} account_collect_ctx_t;

// Convert uint256 to trimmed big-endian bytes, return length (0 for zero)
static size_t uint256_to_trimmed_be(const uint256_t *val, uint8_t out[32]) {
    uint256_to_bytes(val, out);
    size_t start = 0;
    while (start < 32 && out[start] == 0) start++;
    size_t len = 32 - start;
    if (start > 0 && len > 0) memmove(out, out + start, len);
    return len;
}

static bool collect_account_cb(const uint8_t *key, size_t key_len,
                                const void *value, size_t value_len,
                                void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    account_collect_ctx_t *ctx = (account_collect_ctx_t *)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;

    // Skip self-destructed accounts
    if (ca->self_destructed) return true;

    // Skip empty accounts that never existed (same logic as finalize_account_cb)
    bool is_empty = (ca->account.nonce == 0 &&
                     uint256_is_zero(&ca->account.balance) &&
                     !ca->account.has_code);
    if (!ca->existed && !ca->created && is_empty) return true;

    // Grow array if needed
    if (ctx->count >= ctx->cap) {
        size_t new_cap = ctx->cap == 0 ? 16 : ctx->cap * 2;
        account_entry_t *new_entries = realloc(ctx->entries,
                                                new_cap * sizeof(account_entry_t));
        if (!new_entries) return false;
        ctx->entries = new_entries;
        ctx->cap = new_cap;
    }

    account_entry_t *e = &ctx->entries[ctx->count];

    // Hash the address: keccak256(addr[20])
    hash_t h = hash_keccak256(ca->addr.bytes, ADDRESS_SIZE);
    memcpy(e->hashed_key, h.bytes, 32);

    // Compute storage root for this account
    hash_t storage_root = compute_storage_root(ctx->es, &ca->addr);

    // Code hash: HASH_EMPTY_CODE if no code, else the stored hash
    hash_t code_hash = ca->account.has_code ? ca->account.code_hash : HASH_EMPTY_CODE;

    // Build RLP([nonce, balance, storageRoot, codeHash])
    rlp_item_t *list = rlp_list_new();

    // Nonce
    rlp_list_append(list, rlp_uint64(ca->account.nonce));

    // Balance (trimmed big-endian)
    uint8_t bal_be[32];
    size_t bal_len = uint256_to_trimmed_be(&ca->account.balance, bal_be);
    rlp_list_append(list, rlp_string(bal_be, bal_len));

    // Storage root (32 bytes)
    rlp_list_append(list, rlp_string(storage_root.bytes, 32));

    // Code hash (32 bytes)
    rlp_list_append(list, rlp_string(code_hash.bytes, 32));

    bytes_t encoded = rlp_encode(list);
    rlp_item_free(list);

    e->rlp_value = encoded.data;
    e->rlp_len = (uint16_t)encoded.len;
    ctx->count++;
    return true;
}

hash_t evm_state_compute_state_root(evm_state_t *es) {
    if (!es) return HASH_EMPTY_STORAGE;

    if (mem_art_is_empty(&es->accounts)) return HASH_EMPTY_STORAGE;

    // Phase 1: Collect all accounts with their RLP-encoded values
    account_collect_ctx_t ctx = {
        .es = es,
        .entries = NULL,
        .count = 0,
        .cap = 0
    };

    mem_art_foreach(&es->accounts, collect_account_cb, &ctx);

    if (ctx.count == 0) {
        free(ctx.entries);
        return HASH_EMPTY_STORAGE;
    }

    // Phase 2: Sort by hashed address key
    qsort(ctx.entries, ctx.count, sizeof(account_entry_t), compare_32b_keys);

    // Phase 3: Build arrays for ih_build
    const uint8_t **keys = malloc(ctx.count * sizeof(uint8_t *));
    const uint8_t **values = malloc(ctx.count * sizeof(uint8_t *));
    uint16_t *value_lens = malloc(ctx.count * sizeof(uint16_t));

    for (size_t i = 0; i < ctx.count; i++) {
        keys[i] = ctx.entries[i].hashed_key;
        values[i] = ctx.entries[i].rlp_value;
        value_lens[i] = ctx.entries[i].rlp_len;
    }

    ih_state_t *ih = ih_create();
    hash_t root = ih_build(ih, keys, values, value_lens, ctx.count);
    ih_destroy(ih);

    // Cleanup
    free(keys);
    free(values);
    free(value_lens);
    for (size_t i = 0; i < ctx.count; i++) {
        free(ctx.entries[i].rlp_value);
    }
    free(ctx.entries);

    return root;
}
