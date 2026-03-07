#include "evm_state.h"
#include "mem_art.h"
#include "keccak256.h"

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
    uint64_t   nonce;
    uint256_t  balance;
    hash_t     code_hash;
    bool       has_code;
    uint8_t   *code;            // loaded bytecode (NULL until needed)
    uint32_t   code_size;
    bool dirty;
    bool code_dirty;
    bool block_dirty;           // survives commit_tx, cleared at block end
    bool block_code_dirty;      // same for code
    bool created;               // newly created this execution
    bool existed;               // existed in backing store before
    bool self_destructed;
} cached_account_t;

// --- Storage cache ---

typedef struct cached_slot {
    uint8_t   key[SLOT_KEY_SIZE];   // addr[20] || slot_be[32] — kept for finalize
    uint256_t original;             // value when first loaded
    uint256_t current;
    bool dirty;
    bool block_dirty;           // survives commit_tx, cleared at block end
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
    JOURNAL_TRANSIENT_STORAGE,
} journal_type_t;

typedef struct {
    journal_type_t type;
    address_t addr;
    union {
        struct { uint64_t val; bool dirty; } nonce;
        struct { uint256_t val; bool dirty; } balance;
        struct { hash_t old_hash; bool old_has_code; uint8_t *old_code; uint32_t old_code_size; } code;
        struct { uint256_t slot; uint256_t old_value; } storage;
        uint256_t slot;             // WARM_SLOT
        bool old_self_destructed;   // JOURNAL_SELF_DESTRUCT
        struct {                    // JOURNAL_ACCOUNT_CREATE: saved pre-create state
            uint64_t   old_nonce;
            uint256_t  old_balance;
            hash_t     old_code_hash;
            bool       old_has_code;
            uint8_t   *old_code;
            uint32_t   old_code_size;
            bool       old_dirty;
            bool       old_code_dirty;
            bool       old_created;
            bool       old_existed;
            bool       old_self_destructed;
        } create;
    } data;
} journal_entry_t;

// --- Main struct ---

struct evm_state {
    verkle_state_t   *vs;
    mem_art_t         accounts;     // key: addr[20], value: cached_account_t
    mem_art_t         storage;      // key: skey[52], value: cached_slot_t
    journal_entry_t  *journal;
    uint32_t          journal_len;
    uint32_t          journal_cap;
    mem_art_t         warm_addrs;   // key: addr[20], value: (none, 0 bytes)
    mem_art_t         warm_slots;   // key: skey[52], value: (none, 0 bytes)
    mem_art_t         transient;    // key: skey[52], value: uint256_t (EIP-1153)
    witness_gas_t     witness_gas;  // EIP-4762 verkle witness gas tracker
};

// ============================================================================
// Internal helpers
// ============================================================================

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

// Check if a 32-byte buffer is all zeros
static bool is_zero_hash(const uint8_t h[32]) {
    for (int i = 0; i < 32; i++)
        if (h[i] != 0) return false;
    return true;
}

// Load account from verkle_state into cache. Creates empty if not found.
static cached_account_t *ensure_account(evm_state_t *es, const address_t *addr) {
    cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    if (ca) return ca;

    // Build on stack, insert into arena
    cached_account_t ca_local;
    memset(&ca_local, 0, sizeof(ca_local));
    address_copy(&ca_local.addr, addr);

    // Load from verkle_state
    ca_local.nonce = verkle_state_get_nonce(es->vs, addr->bytes);

    uint8_t balance_le[32];
    verkle_state_get_balance(es->vs, addr->bytes, balance_le);
    ca_local.balance = uint256_from_bytes_le(balance_le, 32);

    uint8_t code_hash_bytes[32];
    verkle_state_get_code_hash(es->vs, addr->bytes, code_hash_bytes);
    memcpy(ca_local.code_hash.bytes, code_hash_bytes, 32);

    // Determine if account has code (non-zero, non-empty code hash)
    bool has_code_hash = !is_zero_hash(code_hash_bytes) &&
                         memcmp(code_hash_bytes, HASH_EMPTY_CODE.bytes, 32) != 0;
    ca_local.has_code = has_code_hash;

    // Determine if account existed (any non-default field)
    ca_local.existed = (ca_local.nonce != 0 ||
                        !uint256_is_zero(&ca_local.balance) ||
                        has_code_hash);

    if (!mem_art_insert(&es->accounts, addr->bytes, ADDRESS_SIZE,
                        &ca_local, sizeof(cached_account_t)))
        return NULL;

    return (cached_account_t *)mem_art_get_mut(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
}

// Load storage slot from verkle_state into cache.
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

    // Load from verkle_state
    // Slot key: LE for key derivation arithmetic
    // Value: BE (Ethereum convention — go-ethereum uses common.LeftPadBytes)
    uint8_t slot_le[32], val_be[32];
    uint256_to_bytes_le(slot, slot_le);
    verkle_state_get_storage(es->vs, addr->bytes, slot_le, val_be);
    cs_local.original = uint256_from_bytes(val_be, 32);
    cs_local.current = cs_local.original;

    if (!mem_art_insert(&es->storage, skey, SLOT_KEY_SIZE,
                        &cs_local, sizeof(cached_slot_t)))
        return NULL;

    return (cached_slot_t *)mem_art_get_mut(
        &es->storage, skey, SLOT_KEY_SIZE, NULL);
}

// ============================================================================
// Lifecycle
// ============================================================================

evm_state_t *evm_state_create(verkle_state_t *vs) {
    if (!vs) return NULL;

    evm_state_t *es = calloc(1, sizeof(evm_state_t));
    if (!es) return NULL;

    es->vs = vs;
    es->journal_cap = JOURNAL_INIT_CAP;
    es->journal = malloc(es->journal_cap * sizeof(journal_entry_t));
    if (!es->journal) {
        free(es);
        return NULL;
    }

    if (!mem_art_init(&es->accounts) ||
        !mem_art_init(&es->storage) ||
        !mem_art_init(&es->warm_addrs) ||
        !mem_art_init(&es->warm_slots) ||
        !mem_art_init(&es->transient)) {
        mem_art_destroy(&es->accounts);
        mem_art_destroy(&es->storage);
        mem_art_destroy(&es->warm_addrs);
        mem_art_destroy(&es->warm_slots);
        mem_art_destroy(&es->transient);
        free(es->journal);
        free(es);
        return NULL;
    }

    witness_gas_init(&es->witness_gas);

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

    // Destroy all trees
    mem_art_destroy(&es->accounts);
    mem_art_destroy(&es->storage);
    mem_art_destroy(&es->warm_addrs);
    mem_art_destroy(&es->warm_slots);
    mem_art_destroy(&es->transient);
    witness_gas_destroy(&es->witness_gas);

    // Free any code pointers still owned by journal entries (not reverted)
    for (uint32_t i = 0; i < es->journal_len; i++) {
        if (es->journal[i].type == JOURNAL_CODE) {
            free(es->journal[i].data.code.old_code);
        } else if (es->journal[i].type == JOURNAL_ACCOUNT_CREATE) {
            free(es->journal[i].data.create.old_code);
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
    return ca->existed || ca->created;
}

bool evm_state_is_empty(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return true;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return true;
    return ca->nonce == 0 &&
           uint256_is_zero(&ca->balance) &&
           !ca->has_code;
}

// ============================================================================
// Nonce
// ============================================================================

uint64_t evm_state_get_nonce(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return 0;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return 0;
    return ca->nonce;
}

void evm_state_set_nonce(evm_state_t *es, const address_t *addr, uint64_t nonce) {
    if (!es || !addr) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_NONCE,
        .addr = *addr,
        .data.nonce = { .val = ca->nonce, .dirty = ca->dirty }
    };
    journal_push(es, &je);

    ca->nonce = nonce;
    ca->dirty = true;
    ca->block_dirty = true;
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
    return ca->balance;
}

void evm_state_set_balance(evm_state_t *es, const address_t *addr,
                           const uint256_t *balance) {
    if (!es || !addr || !balance) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_BALANCE,
        .addr = *addr,
        .data.balance = { .val = ca->balance, .dirty = ca->dirty }
    };
    journal_push(es, &je);

    ca->balance = *balance;
    ca->dirty = true;
    ca->block_dirty = true;
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
    if (!ca->has_code) return HASH_EMPTY_CODE;
    return ca->code_hash;
}

uint32_t evm_state_get_code_size(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return 0;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return 0;

    // If code is cached, return cached size
    if (ca->code) return ca->code_size;

    // If no code, return 0
    if (!ca->has_code) return 0;

    // Load code size from verkle_state
    ca->code_size = (uint32_t)verkle_state_get_code_size(es->vs, addr->bytes);
    return ca->code_size;
}

bool evm_state_get_code(evm_state_t *es, const address_t *addr,
                        uint8_t *out, uint32_t *out_len) {
    if (!es || !addr) return false;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return false;

    if (!ca->has_code) {
        if (out_len) *out_len = 0;
        return true;
    }

    // Load code into cache if not already loaded
    if (!ca->code) {
        uint64_t len = verkle_state_get_code_size(es->vs, addr->bytes);
        if (len == 0) {
            if (out_len) *out_len = 0;
            return true;
        }

        ca->code = malloc(len);
        if (!ca->code) return false;

        uint64_t got = verkle_state_get_code(es->vs, addr->bytes, ca->code, len);
        ca->code_size = (uint32_t)got;
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
    if (!ca || !ca->has_code) {
        if (out_len) *out_len = 0;
        return NULL;
    }

    // Load code into cache if not already loaded
    if (!ca->code) {
        uint64_t len = verkle_state_get_code_size(es->vs, addr->bytes);
        if (len == 0) {
            if (out_len) *out_len = 0;
            return NULL;
        }

        ca->code = malloc(len);
        if (!ca->code) {
            if (out_len) *out_len = 0;
            return NULL;
        }

        uint64_t got = verkle_state_get_code(es->vs, addr->bytes, ca->code, len);
        ca->code_size = (uint32_t)got;
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
            .old_hash = ca->code_hash,
            .old_has_code = ca->has_code,
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
        ca->has_code = true;
        ca->code_hash = hash_keccak256(code, len);
    } else {
        ca->has_code = false;
        ca->code_hash = hash_zero();
    }

    ca->dirty = true;
    ca->code_dirty = true;
    ca->block_dirty = true;
    ca->block_code_dirty = true;
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

uint256_t evm_state_get_committed_storage(evm_state_t *es, const address_t *addr,
                                          const uint256_t *key) {
    if (!es || !addr || !key) return UINT256_ZERO_INIT;
    cached_slot_t *cs = ensure_slot(es, addr, key);
    if (!cs) return UINT256_ZERO_INIT;
    return cs->original;
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
    cs->block_dirty = true;
}

bool evm_state_has_storage(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;

    // Scan the storage cache for any entry belonging to this address
    mem_art_iterator_t *iter = mem_art_iterator_create(&es->storage);
    if (!iter) return false;

    bool found = false;
    while (!mem_art_iterator_done(iter)) {
        size_t klen;
        const uint8_t *key = mem_art_iterator_key(iter, &klen);
        if (key && klen == SLOT_KEY_SIZE && memcmp(key, addr->bytes, ADDRESS_SIZE) == 0) {
            size_t vlen;
            const cached_slot_t *cs = (const cached_slot_t *)mem_art_iterator_value(iter, &vlen);
            if (cs && !uint256_is_zero(&cs->current)) {
                found = true;
                break;
            }
        }
        mem_art_iterator_next(iter);
    }
    mem_art_iterator_destroy(iter);
    return found;
}

// ============================================================================
// Transient Storage (EIP-1153)
// ============================================================================

uint256_t evm_state_tload(evm_state_t *es, const address_t *addr,
                           const uint256_t *key) {
    if (!es || !addr || !key) return UINT256_ZERO_INIT;

    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);

    const uint256_t *val = (const uint256_t *)mem_art_get(
        &es->transient, skey, SLOT_KEY_SIZE, NULL);
    if (!val) return UINT256_ZERO_INIT;
    return *val;
}

void evm_state_tstore(evm_state_t *es, const address_t *addr,
                       const uint256_t *key, const uint256_t *value) {
    if (!es || !addr || !key || !value) return;

    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);

    // Get old value for journal
    uint256_t old_value = UINT256_ZERO_INIT;
    const uint256_t *existing = (const uint256_t *)mem_art_get(
        &es->transient, skey, SLOT_KEY_SIZE, NULL);
    if (existing) old_value = *existing;

    // Journal old value
    journal_entry_t je = {
        .type = JOURNAL_TRANSIENT_STORAGE,
        .addr = *addr,
        .data.storage = {
            .slot = *key,
            .old_value = old_value
        }
    };
    journal_push(es, &je);

    // Insert or update
    mem_art_insert(&es->transient, skey, SLOT_KEY_SIZE,
                   value, sizeof(uint256_t));
}

// ============================================================================
// Account Creation
// ============================================================================

void evm_state_create_account(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;

    // Journal the creation — save full pre-create state so revert is exact
    journal_entry_t je = {
        .type = JOURNAL_ACCOUNT_CREATE,
        .addr = *addr,
        .data.create = {
            .old_nonce          = ca->nonce,
            .old_balance        = ca->balance,
            .old_code_hash      = ca->code_hash,
            .old_has_code       = ca->has_code,
            .old_code           = ca->code,       // transfer ownership to journal
            .old_code_size      = ca->code_size,
            .old_dirty          = ca->dirty,
            .old_code_dirty     = ca->code_dirty,
            .old_created        = ca->created,
            .old_existed        = ca->existed,
            .old_self_destructed = ca->self_destructed,
        },
    };
    journal_push(es, &je);

    // Reset account: preserve existing balance per Ethereum spec.
    // CREATE/CREATE2 preserves any pre-existing ether at the target address.
    uint256_t existing_balance = ca->balance;
    ca->nonce = 0;
    ca->balance = existing_balance;
    ca->code_hash = hash_zero();
    ca->has_code = false;
    ca->code = NULL;          // ownership moved to journal entry
    ca->code_size = 0;
    ca->created = true;
    ca->dirty = true;
    ca->block_dirty = true;
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
        .data.old_self_destructed = ca->self_destructed,
    };
    journal_push(es, &je);

    ca->self_destructed = true;
    ca->dirty = true;
    ca->block_dirty = true;
}

bool evm_state_is_self_destructed(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;
    const cached_account_t *ca = (const cached_account_t *)mem_art_get(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    if (!ca) return false;
    return ca->self_destructed;
}

bool evm_state_is_created(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;
    const cached_account_t *ca = (const cached_account_t *)mem_art_get(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    if (!ca) return false;
    return ca->created;
}

// ============================================================================
// Commit (mark current state as "original" for EIP-2200)
// ============================================================================

static bool commit_slot_cb(const uint8_t *key, size_t key_len,
                           const void *value, size_t value_len,
                           void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_slot_t *cs = (cached_slot_t *)(uintptr_t)value;
    cs->original = cs->current;
    cs->dirty = false;
    return true;
}

static bool commit_account_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len,
                               void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    // EIP-161: Only promote to "existed" if the account is non-empty.
    bool is_empty = (ca->nonce == 0 &&
                     uint256_is_zero(&ca->balance) &&
                     !ca->has_code);
    if ((ca->existed || ca->created || ca->dirty || ca->code_dirty) && !is_empty) {
        ca->existed = true;
    }
    ca->created = false;
    ca->dirty = false;
    ca->code_dirty = false;
    ca->self_destructed = false;
    return true;
}

void evm_state_commit(evm_state_t *es) {
    if (!es) return;
    mem_art_foreach(&es->storage, commit_slot_cb, NULL);
    mem_art_foreach(&es->accounts, commit_account_cb, NULL);
    es->journal_len = 0;
}

// ============================================================================
// Per-transaction commit (for block execution inter-tx boundaries)
// ============================================================================

typedef struct {
    address_t addrs[256];
    size_t    count;
} selfdestructed_ctx_t;

static bool commit_tx_account_cb(const uint8_t *key, size_t key_len,
                                  const void *value, size_t value_len,
                                  void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    selfdestructed_ctx_t *ctx = (selfdestructed_ctx_t *)user_data;

    if (ca->self_destructed) {
        if (ctx->count < 256)
            ctx->addrs[ctx->count++] = ca->addr;

        ca->balance = UINT256_ZERO;
        ca->nonce = 0;
        ca->has_code = false;
        memset(ca->code_hash.bytes, 0, 32);
        if (ca->code) { free(ca->code); ca->code = NULL; }
        ca->code_size = 0;
        ca->existed = false;
        ca->created = false;
        ca->dirty = false;
        ca->code_dirty = false;
        ca->block_dirty = true;        // destruction is a block-level change
        ca->block_code_dirty = true;
        ca->self_destructed = false;
        return true;
    }

    // EIP-161: Only promote to "existed" if the account is non-empty.
    // Empty accounts touched by zero-value transfers must NOT be
    // persisted in the state tree (they are pruned).
    bool is_empty = (ca->nonce == 0 &&
                     uint256_is_zero(&ca->balance) &&
                     !ca->has_code);
    if ((ca->existed || ca->created || ca->dirty || ca->code_dirty) && !is_empty) {
        ca->existed = true;
    }
    ca->created = false;
    ca->dirty = false;
    ca->code_dirty = false;
    ca->self_destructed = false;
    return true;
}

static bool commit_tx_slot_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len,
                               void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    cached_slot_t *cs = (cached_slot_t *)(uintptr_t)value;
    selfdestructed_ctx_t *ctx = (selfdestructed_ctx_t *)user_data;

    for (size_t i = 0; i < ctx->count; i++) {
        if (memcmp(cs->key, ctx->addrs[i].bytes, ADDRESS_SIZE) == 0) {
            cs->current = UINT256_ZERO;
            cs->original = UINT256_ZERO;
            cs->dirty = false;
            cs->block_dirty = true;     // destruction is a block-level change
            return true;
        }
    }

    cs->original = cs->current;
    cs->dirty = false;
    return true;
}

void evm_state_commit_tx(evm_state_t *es) {
    if (!es) return;

    selfdestructed_ctx_t ctx = { .count = 0 };

    mem_art_foreach(&es->accounts, commit_tx_account_cb, &ctx);
    mem_art_foreach(&es->storage, commit_tx_slot_cb, &ctx);

    es->journal_len = 0;

    mem_art_destroy(&es->warm_addrs);
    mem_art_init(&es->warm_addrs);
    mem_art_destroy(&es->warm_slots);
    mem_art_init(&es->warm_slots);

    mem_art_destroy(&es->transient);
    mem_art_init(&es->transient);

    witness_gas_reset(&es->witness_gas);
}

void evm_state_begin_block(evm_state_t *es, uint64_t block_number) {
    if (!es) return;
    witness_gas_reset(&es->witness_gas);
    verkle_state_begin_block(es->vs, block_number);
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
            if (ca) {
                ca->nonce = je->data.nonce.val;
                ca->dirty = je->data.nonce.dirty;
            }
            break;
        }
        case JOURNAL_BALANCE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->balance = je->data.balance.val;
                ca->dirty = je->data.balance.dirty;
            }
            break;
        }
        case JOURNAL_CODE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->code_hash = je->data.code.old_hash;
                ca->has_code = je->data.code.old_has_code;
                free(ca->code);
                ca->code = je->data.code.old_code;
                ca->code_size = je->data.code.old_code_size;
            }
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
                cs->dirty = !uint256_is_equal(&cs->current, &cs->original);
            }
            break;
        }
        case JOURNAL_ACCOUNT_CREATE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->nonce           = je->data.create.old_nonce;
                ca->balance         = je->data.create.old_balance;
                ca->code_hash       = je->data.create.old_code_hash;
                ca->has_code        = je->data.create.old_has_code;
                free(ca->code);
                ca->code            = je->data.create.old_code;
                ca->code_size       = je->data.create.old_code_size;
                ca->dirty           = je->data.create.old_dirty;
                ca->code_dirty      = je->data.create.old_code_dirty;
                ca->created         = je->data.create.old_created;
                ca->existed         = je->data.create.old_existed;
                ca->self_destructed = je->data.create.old_self_destructed;
            }
            je->data.create.old_code = NULL;
            break;
        }
        case JOURNAL_SELF_DESTRUCT: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) ca->self_destructed = je->data.old_self_destructed;
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
        case JOURNAL_TRANSIENT_STORAGE: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.storage.slot, skey);
            if (uint256_is_zero(&je->data.storage.old_value)) {
                mem_art_delete(&es->transient, skey, SLOT_KEY_SIZE);
            } else {
                mem_art_insert(&es->transient, skey, SLOT_KEY_SIZE,
                               &je->data.storage.old_value, sizeof(uint256_t));
            }
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
        return true;

    mem_art_insert(&es->warm_addrs, addr->bytes, ADDRESS_SIZE, NULL, 0);

    journal_entry_t je = {
        .type = JOURNAL_WARM_ADDR,
        .addr = *addr,
    };
    journal_push(es, &je);

    return false;
}

bool evm_state_warm_slot(evm_state_t *es, const address_t *addr,
                         const uint256_t *key) {
    if (!es || !addr || !key) return false;

    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);

    if (mem_art_contains(&es->warm_slots, skey, SLOT_KEY_SIZE))
        return true;

    mem_art_insert(&es->warm_slots, skey, SLOT_KEY_SIZE, NULL, 0);

    journal_entry_t je = {
        .type = JOURNAL_WARM_SLOT,
        .addr = *addr,
        .data.slot = *key,
    };
    journal_push(es, &je);

    return false;
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
// Finalize — flush dirty state to verkle_state
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
        // Zero out all account fields in verkle
        verkle_state_set_nonce(ctx->es->vs, ca->addr.bytes, 0);
        uint8_t zero32[32] = {0};
        verkle_state_set_balance(ctx->es->vs, ca->addr.bytes, zero32);
        verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes, zero32);
        verkle_state_set_code_size(ctx->es->vs, ca->addr.bytes, 0);
        return true;
    }

    if (!ca->dirty && !ca->code_dirty) return true;

    // Skip writing empty accounts that never existed
    bool is_empty = (ca->nonce == 0 &&
                     uint256_is_zero(&ca->balance) &&
                     !ca->has_code);
    if (!ca->existed && !ca->created && is_empty) return true;

    if (ca->dirty) {
        verkle_state_set_nonce(ctx->es->vs, ca->addr.bytes, ca->nonce);
        uint8_t balance_le[32];
        uint256_to_bytes_le(&ca->balance, balance_le);
        verkle_state_set_balance(ctx->es->vs, ca->addr.bytes, balance_le);

        // Always write code_hash when account is dirty (e.g. newly created)
        if (!ca->code_dirty) {
            if (ca->has_code) {
                verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes,
                                          ca->code_hash.bytes);
            } else {
                verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes,
                                          HASH_EMPTY_CODE.bytes);
            }
        }
    }

    if (ca->code_dirty) {
        if (ca->code && ca->code_size > 0) {
            verkle_state_set_code(ctx->es->vs, ca->addr.bytes,
                                 ca->code, ca->code_size);
            verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes,
                                      ca->code_hash.bytes);
        } else {
            // Code cleared — write keccak256("") as code hash
            verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes,
                                      HASH_EMPTY_CODE.bytes);
            verkle_state_set_code_size(ctx->es->vs, ca->addr.bytes, 0);
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

    // Extract address from composite key
    const uint8_t *addr = cs->key;

    // Slot key: LE for verkle key derivation arithmetic
    uint256_t slot = uint256_from_bytes(cs->key + ADDRESS_SIZE, 32);
    uint8_t slot_le[32];
    uint256_to_bytes_le(&slot, slot_le);

    // Value: BE (Ethereum convention)
    uint8_t val_be[32];
    uint256_to_bytes(&cs->current, val_be);

    verkle_state_set_storage(ctx->es->vs, addr, slot_le, val_be);

    return true;
}

bool evm_state_finalize(evm_state_t *es) {
    if (!es) return false;

    finalize_ctx_t ctx = { .es = es, .ok = true };

    mem_art_foreach(&es->accounts, finalize_account_cb, &ctx);
    if (!ctx.ok) return false;

    mem_art_foreach(&es->storage, finalize_storage_cb, &ctx);
    return ctx.ok;
}

// ============================================================================
// State Root — delegate to verkle_state
// ============================================================================

// Flush callback: writes block-dirty cached accounts to verkle
static bool flush_all_accounts_cb(const uint8_t *key, size_t key_len,
                                    const void *value, size_t value_len,
                                    void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    finalize_ctx_t *ctx = (finalize_ctx_t *)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;

    // Skip accounts not modified during this block
    if (!ca->block_dirty && !ca->block_code_dirty) return true;

    if (ca->self_destructed) {
        verkle_state_set_nonce(ctx->es->vs, ca->addr.bytes, 0);
        uint8_t zero32[32] = {0};
        verkle_state_set_balance(ctx->es->vs, ca->addr.bytes, zero32);
        verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes, zero32);
        verkle_state_set_code_size(ctx->es->vs, ca->addr.bytes, 0);
        return true;
    }

    // Skip empty accounts that never existed
    bool is_empty = (ca->nonce == 0 &&
                     uint256_is_zero(&ca->balance) &&
                     !ca->has_code);
    if (!ca->existed && !ca->created && is_empty) return true;

    verkle_state_set_nonce(ctx->es->vs, ca->addr.bytes, ca->nonce);
    uint8_t balance_le[32];
    uint256_to_bytes_le(&ca->balance, balance_le);
    verkle_state_set_balance(ctx->es->vs, ca->addr.bytes, balance_le);

    if (ca->code && ca->code_size > 0) {
        verkle_state_set_code(ctx->es->vs, ca->addr.bytes,
                             ca->code, ca->code_size);
        verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes,
                                  ca->code_hash.bytes);
    } else {
        // Account without code: write keccak256("") as code hash
        verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes,
                                  HASH_EMPTY_CODE.bytes);
    }

    return true;
}

static bool flush_all_storage_cb(const uint8_t *key, size_t key_len,
                                   const void *value, size_t value_len,
                                   void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    finalize_ctx_t *ctx = (finalize_ctx_t *)user_data;
    const cached_slot_t *cs = (const cached_slot_t *)value;

    // Skip slots not modified during this block
    if (!cs->block_dirty) return true;

    const uint8_t *addr = cs->key;
    uint256_t slot = uint256_from_bytes(cs->key + ADDRESS_SIZE, 32);
    uint8_t slot_le[32];
    uint256_to_bytes_le(&slot, slot_le);

    // Value: BE (Ethereum convention)
    uint8_t val_be[32];
    uint256_to_bytes(&cs->current, val_be);

    // Skip if value matches what's already in the verkle tree.
    // Prevents creating zero-value leaves for slots written back to their
    // original value (e.g., SSTORE(slot, 0) when slot was already 0).
    uint8_t stored_be[32];
    verkle_state_get_storage(ctx->es->vs, addr, slot_le, stored_be);
    if (memcmp(val_be, stored_be, 32) == 0)
        return true;

    verkle_state_set_storage(ctx->es->vs, addr, slot_le, val_be);
    return true;
}

static bool clear_block_dirty_account_cb(const uint8_t *key, size_t key_len,
                                          const void *value, size_t value_len,
                                          void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    ca->block_dirty = false;
    ca->block_code_dirty = false;
    return true;
}

static bool clear_block_dirty_slot_cb(const uint8_t *key, size_t key_len,
                                       const void *value, size_t value_len,
                                       void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_slot_t *cs = (cached_slot_t *)(uintptr_t)value;
    cs->block_dirty = false;
    return true;
}

hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty) {
    (void)prune_empty;  // Not applicable for verkle
    if (!es) return hash_zero();

    // Flush only block-dirty cached state to verkle
    finalize_ctx_t ctx = { .es = es, .ok = true };
    mem_art_foreach(&es->accounts, flush_all_accounts_cb, &ctx);
    mem_art_foreach(&es->storage, flush_all_storage_cb, &ctx);

    // Commit block in flat backend (processes buffered writes, updates commitments).
    // No-op for in-memory tree backend.
    verkle_state_commit_block(es->vs);

    hash_t root;
    verkle_state_root_hash(es->vs, root.bytes);

    // Clear block_dirty flags — next block starts fresh
    mem_art_foreach(&es->accounts, clear_block_dirty_account_cb, NULL);
    mem_art_foreach(&es->storage, clear_block_dirty_slot_cb, NULL);

    return root;
}

// ============================================================================
// Witness Gas (EIP-4762)
// ============================================================================

uint64_t evm_state_witness_gas_access(evm_state_t *es,
                                       const uint8_t key[32],
                                       bool is_write,
                                       bool value_was_empty)
{
    if (!es) return 0;
    return witness_gas_access_event(&es->witness_gas, key, is_write, value_was_empty);
}
