/**
 * State Overlay — unified in-memory state backed by flat_store.
 *
 * Replaces: mem_art cache + flat_state + all flush/evict logic in evm_state.c
 *
 * Phase 1: Uses mem_art internally (same data structures as evm_state.c).
 *          This allows incremental migration — evm_state.c delegates here.
 * Phase 2: Replace mem_art with flat_store overlay entries directly.
 */

#include "state_overlay.h"
#include "flat_state.h"
#include "flat_store.h"
#include "code_store.h"
#include "compact_art.h"
#include "storage_trie.h"
#include "account_trie.h"
#include "keccak256.h"
#include "mem_art.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define JOURNAL_INIT_CAP  256
#define SLOT_KEY_SIZE     52    /* addr[20] + slot_be[32] */
#define ADDRESS_KEY_SIZE  20
#define MAX_CODE_SIZE     (24 * 1024 + 1)

/* =========================================================================
 * Cached Account
 * ========================================================================= */

typedef struct {
    /* Hot fields */
    uint64_t   nonce;
    uint256_t  balance;
    bool dirty;
    bool block_dirty;
    bool existed;
    bool mpt_dirty;
    bool storage_dirty;
    bool storage_cleared;
    bool has_code;
    bool created;
    bool self_destructed;
    bool code_dirty;
    bool block_code_dirty;
    uint8_t   *code;
    uint32_t   code_size;
    /* Cold fields */
    address_t  addr;
    hash_t     code_hash;
    hash_t     storage_root;
    hash_t     addr_hash;
#ifdef ENABLE_HISTORY
    uint64_t   original_nonce;
    uint256_t  original_balance;
    hash_t     original_code_hash;
    bool       block_self_destructed;
    bool       block_created;
    bool       block_accessed;
#endif
} cached_account_t;

/* =========================================================================
 * Cached Slot
 * ========================================================================= */

typedef struct {
    uint8_t   key[SLOT_KEY_SIZE];
    uint256_t original;
    uint256_t current;
    bool dirty;
    bool block_dirty;
    bool mpt_dirty;
    hash_t slot_hash;
#ifdef ENABLE_HISTORY
    uint256_t block_original;
#endif
} cached_slot_t;

/* =========================================================================
 * Journal
 * ========================================================================= */

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
        struct { uint64_t val; bool dirty; bool block_dirty; } nonce;
        struct { uint256_t val; bool dirty; bool block_dirty; } balance;
        struct { hash_t old_hash; bool old_has_code; uint8_t *old_code;
                 uint32_t old_code_size; } code;
        struct { uint256_t slot; uint256_t old_value; bool old_mpt_dirty; } storage;
        uint256_t slot;
        bool old_self_destructed;
        struct {
            uint64_t old_nonce; uint256_t old_balance;
            hash_t old_code_hash; bool old_has_code;
            uint8_t *old_code; uint32_t old_code_size;
            bool old_dirty; bool old_code_dirty;
            bool old_block_dirty; bool old_block_code_dirty;
            bool old_created; bool old_existed; bool old_self_destructed;
            hash_t old_storage_root;
            bool old_storage_dirty; bool old_storage_cleared;
#ifdef ENABLE_HISTORY
            bool old_block_created;
#endif
        } create;
        uint256_t old_transient_value;
    } data;
} journal_entry_t;

/* =========================================================================
 * Dirty tracking vectors
 * ========================================================================= */

typedef struct { uint8_t *keys; size_t count, cap; } dirty_vec_t;

static inline void dirty_push(dirty_vec_t *v, const uint8_t *key, size_t key_size) {
    if (v->count >= v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 64;
        uint8_t *nk = realloc(v->keys, nc * key_size);
        if (!nk) return;
        v->keys = nk; v->cap = nc;
    }
    memcpy(v->keys + v->count * key_size, key, key_size);
    v->count++;
}

static inline void dirty_clear(dirty_vec_t *v) { v->count = 0; }
static inline void dirty_free(dirty_vec_t *v) {
    free(v->keys); v->keys = NULL; v->count = v->cap = 0;
}

/* =========================================================================
 * Main struct
 * ========================================================================= */

struct state_overlay {
    flat_state_t     *flat_state;
    code_store_t     *code_store;
    account_trie_t   *account_trie;
    storage_trie_t   *storage_trie;

    mem_art_t accounts;    /* addr[20] → cached_account_t */
    mem_art_t storage;     /* skey[52] → cached_slot_t */

    journal_entry_t *journal;
    uint32_t journal_len;
    uint32_t journal_cap;

    mem_art_t warm_addrs;
    mem_art_t warm_slots;
    mem_art_t transient;

    dirty_vec_t tx_dirty_accounts;   /* per-tx, key_size=20 */
    dirty_vec_t tx_dirty_slots;      /* per-tx, key_size=52 */
    dirty_vec_t dirty_accounts;      /* per-checkpoint, key_size=20 */
    dirty_vec_t dirty_slots;         /* per-checkpoint, key_size=52 */

    bool prune_empty;
    bool batch_mode;

    double last_root_stor_ms;
    double last_root_acct_ms;
    size_t last_root_dirty_count;

    uint64_t flat_acct_hit, flat_acct_miss;
    uint64_t flat_stor_hit, flat_stor_miss;
};

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

static void make_slot_key(const address_t *addr, const uint256_t *slot,
                          uint8_t out[SLOT_KEY_SIZE]) {
    memcpy(out, addr->bytes, ADDRESS_SIZE);
    uint256_to_bytes(slot, out + ADDRESS_SIZE);
}

static bool journal_push(state_overlay_t *so, const journal_entry_t *entry) {
    if (so->journal_len >= so->journal_cap) {
        uint32_t nc = so->journal_cap * 2;
        journal_entry_t *nj = realloc(so->journal, nc * sizeof(*nj));
        if (!nj) return false;
        so->journal = nj;
        so->journal_cap = nc;
    }
    so->journal[so->journal_len++] = *entry;
    return true;
}

static inline void mark_account_mpt_dirty(state_overlay_t *so, cached_account_t *ca) {
    if (!ca->mpt_dirty) {
        ca->mpt_dirty = true;
        dirty_push(&so->dirty_accounts, ca->addr.bytes, ADDRESS_KEY_SIZE);
    }
}

static inline void mark_slot_mpt_dirty(state_overlay_t *so, cached_slot_t *cs) {
    if (!cs->mpt_dirty) {
        cs->mpt_dirty = true;
        dirty_push(&so->dirty_slots, cs->key, SLOT_KEY_SIZE);
    }
}

static inline void mark_account_tx_dirty(state_overlay_t *so, cached_account_t *ca) {
    if (!ca->dirty)
        dirty_push(&so->tx_dirty_accounts, ca->addr.bytes, ADDRESS_KEY_SIZE);
}

static inline void mark_slot_tx_dirty(state_overlay_t *so, cached_slot_t *cs) {
    if (!cs->dirty)
        dirty_push(&so->tx_dirty_slots, cs->key, SLOT_KEY_SIZE);
}

/* =========================================================================
 * ensure_account — load or create cached account
 * ========================================================================= */

static cached_account_t *ensure_account(state_overlay_t *so, const address_t *addr) {
    cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
        &so->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    if (ca) return ca;

    cached_account_t local;
    memset(&local, 0, sizeof(local));
    address_copy(&local.addr, addr);
    local.storage_root = HASH_EMPTY_STORAGE;
    local.addr_hash = hash_keccak256(addr->bytes, 20);

    if (so->flat_state) {
        flat_account_record_t frec;
        if (flat_state_get_account(so->flat_state, local.addr_hash.bytes, &frec)) {
            local.nonce = frec.nonce;
            local.balance = uint256_from_bytes(frec.balance, 32);
            memcpy(local.code_hash.bytes, frec.code_hash, 32);
            memcpy(local.storage_root.bytes, frec.storage_root, 32);
            local.has_code = (memcmp(frec.code_hash, ((hash_t){0}).bytes, 32) != 0 &&
                              memcmp(frec.code_hash, HASH_EMPTY_CODE.bytes, 32) != 0);
            local.existed = true;
            so->flat_acct_hit++;
        } else {
            so->flat_acct_miss++;
        }
    }

#ifdef ENABLE_HISTORY
    local.original_nonce = local.nonce;
    local.original_balance = local.balance;
    local.original_code_hash = local.code_hash;
#endif

    return (cached_account_t *)mem_art_upsert(
        &so->accounts, addr->bytes, ADDRESS_SIZE,
        &local, sizeof(local));
}

/* =========================================================================
 * ensure_slot — load or create cached storage slot
 * ========================================================================= */

static cached_slot_t *ensure_slot(state_overlay_t *so, const address_t *addr,
                                  const uint256_t *slot) {
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, slot, skey);

    cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
        &so->storage, skey, SLOT_KEY_SIZE, NULL);
    if (cs) return cs;

    cached_slot_t local;
    memset(&local, 0, sizeof(local));
    memcpy(local.key, skey, SLOT_KEY_SIZE);
    local.slot_hash = hash_keccak256(skey + 20, 32);

    if (so->flat_state) {
        cached_account_t *ca = ensure_account(so, addr);
        if (ca) {
            uint8_t val_be[32];
            if (flat_state_get_storage(so->flat_state, ca->addr_hash.bytes,
                                       local.slot_hash.bytes, val_be)) {
                local.original = uint256_from_bytes(val_be, 32);
                local.current = local.original;
                so->flat_stor_hit++;
            } else {
                so->flat_stor_miss++;
            }
        }
    }

#ifdef ENABLE_HISTORY
    local.block_original = local.original;
#endif

    return (cached_slot_t *)mem_art_upsert(
        &so->storage, skey, SLOT_KEY_SIZE,
        &local, sizeof(local));
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

state_overlay_t *state_overlay_create(flat_state_t *fs, code_store_t *cs) {
    state_overlay_t *so = calloc(1, sizeof(*so));
    if (!so) return NULL;

    so->flat_state = fs;
    so->code_store = cs;

    mem_art_init(&so->accounts);
    mem_art_init(&so->storage);
    mem_art_init(&so->warm_addrs);
    mem_art_init(&so->warm_slots);
    mem_art_init(&so->transient);

    so->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!so->journal) { free(so); return NULL; }
    so->journal_cap = JOURNAL_INIT_CAP;

    if (fs) {
        compact_art_t *s_art = flat_state_storage_art(fs);
        flat_store_t  *s_store = flat_state_storage_store(fs);
        if (s_art && s_store)
            so->storage_trie = storage_trie_create(s_art, s_store);

        compact_art_t *a_art = flat_state_account_art(fs);
        flat_store_t  *a_store = flat_state_account_store(fs);
        if (a_art && a_store)
            so->account_trie = account_trie_create(a_art, a_store);
    }

    return so;
}

static bool free_code_cb(const uint8_t *key, size_t key_len,
                          const void *value, size_t value_len,
                          void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    free(ca->code);
    return true;
}

void state_overlay_destroy(state_overlay_t *so) {
    if (!so) return;

    mem_art_foreach(&so->accounts, free_code_cb, NULL);

    if (so->storage_trie) storage_trie_destroy(so->storage_trie);
    if (so->account_trie) account_trie_destroy(so->account_trie);

    mem_art_destroy(&so->accounts);
    mem_art_destroy(&so->storage);
    mem_art_destroy(&so->warm_addrs);
    mem_art_destroy(&so->warm_slots);
    mem_art_destroy(&so->transient);

    for (uint32_t i = 0; i < so->journal_len; i++) {
        if (so->journal[i].type == JOURNAL_CODE)
            free(so->journal[i].data.code.old_code);
        else if (so->journal[i].type == JOURNAL_ACCOUNT_CREATE)
            free(so->journal[i].data.create.old_code);
    }
    free(so->journal);

    dirty_free(&so->tx_dirty_accounts);
    dirty_free(&so->tx_dirty_slots);
    dirty_free(&so->dirty_accounts);
    dirty_free(&so->dirty_slots);

    free(so);
}

/* =========================================================================
 * Account — read
 * ========================================================================= */

bool state_overlay_exists(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    cached_account_t *ca = ensure_account(so, addr);
    return ca && ca->existed;
}

bool state_overlay_is_empty(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return true;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return true;
    return ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code;
}

uint64_t state_overlay_get_nonce(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return 0;
    cached_account_t *ca = ensure_account(so, addr);
    return ca ? ca->nonce : 0;
}

uint256_t state_overlay_get_balance(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return UINT256_ZERO_INIT;
    cached_account_t *ca = ensure_account(so, addr);
    return ca ? ca->balance : UINT256_ZERO_INIT;
}

hash_t state_overlay_get_code_hash(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return HASH_EMPTY_CODE;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca || !ca->has_code) return HASH_EMPTY_CODE;
    return ca->code_hash;
}

/* =========================================================================
 * Account — write (journaled)
 * ========================================================================= */

void state_overlay_set_nonce(state_overlay_t *so, const address_t *addr, uint64_t nonce) {
    if (!so || !addr) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_NONCE,
        .addr = *addr,
        .data.nonce = { .val = ca->nonce, .dirty = ca->dirty, .block_dirty = ca->block_dirty }
    };
    journal_push(so, &je);

    ca->nonce = nonce;
    mark_account_tx_dirty(so, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#ifdef ENABLE_HISTORY
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(so, ca);
}

void state_overlay_set_balance(state_overlay_t *so, const address_t *addr,
                                const uint256_t *balance) {
    if (!so || !addr || !balance) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_BALANCE,
        .addr = *addr,
        .data.balance = { .val = ca->balance, .dirty = ca->dirty, .block_dirty = ca->block_dirty }
    };
    journal_push(so, &je);

    ca->balance = *balance;
    mark_account_tx_dirty(so, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#ifdef ENABLE_HISTORY
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(so, ca);
}

void state_overlay_add_balance(state_overlay_t *so, const address_t *addr,
                                const uint256_t *amount) {
    if (!so || !addr || !amount) return;
    uint256_t bal = state_overlay_get_balance(so, addr);
    uint256_t new_bal = uint256_add(&bal, amount);
    state_overlay_set_balance(so, addr, &new_bal);
}

bool state_overlay_sub_balance(state_overlay_t *so, const address_t *addr,
                                const uint256_t *amount) {
    if (!so || !addr || !amount) return false;
    uint256_t bal = state_overlay_get_balance(so, addr);
    if (uint256_lt(&bal, amount)) return false;
    uint256_t new_bal = uint256_sub(&bal, amount);
    state_overlay_set_balance(so, addr, &new_bal);
    return true;
}

/* =========================================================================
 * Storage (journaled)
 * ========================================================================= */

uint256_t state_overlay_get_storage(state_overlay_t *so, const address_t *addr,
                                     const uint256_t *key) {
    if (!so || !addr || !key) return UINT256_ZERO_INIT;
    cached_slot_t *cs = ensure_slot(so, addr, key);
    return cs ? cs->current : UINT256_ZERO_INIT;
}

uint256_t state_overlay_get_committed_storage(state_overlay_t *so, const address_t *addr,
                                               const uint256_t *key) {
    if (!so || !addr || !key) return UINT256_ZERO_INIT;
    cached_slot_t *cs = ensure_slot(so, addr, key);
    return cs ? cs->original : UINT256_ZERO_INIT;
}

void state_overlay_set_storage(state_overlay_t *so, const address_t *addr,
                                const uint256_t *key, const uint256_t *value) {
    if (!so || !addr || !key || !value) return;
    cached_slot_t *cs = ensure_slot(so, addr, key);
    if (!cs) return;

    journal_entry_t je = {
        .type = JOURNAL_STORAGE,
        .addr = *addr,
        .data.storage = {
            .slot = *key,
            .old_value = cs->current,
            .old_mpt_dirty = cs->mpt_dirty
        }
    };
    journal_push(so, &je);

    cs->current = *value;
    mark_slot_tx_dirty(so, cs);
    cs->dirty = true;
    cs->block_dirty = true;
    mark_slot_mpt_dirty(so, cs);

    cached_account_t *ca = ensure_account(so, addr);
    if (ca) {
        ca->storage_dirty = true;
        mark_account_mpt_dirty(so, ca);
    }
}

/* =========================================================================
 * Snapshot / Revert
 * ========================================================================= */

uint32_t state_overlay_snapshot(state_overlay_t *so) {
    if (!so) return 0;
    return so->journal_len;
}

void state_overlay_revert(state_overlay_t *so, uint32_t snap_id) {
    if (!so || snap_id > so->journal_len) return;

    while (so->journal_len > snap_id) {
        so->journal_len--;
        journal_entry_t *je = &so->journal[so->journal_len];

        switch (je->type) {
        case JOURNAL_NONCE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &so->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->nonce = je->data.nonce.val;
                ca->dirty = je->data.nonce.dirty;
                ca->block_dirty = je->data.nonce.block_dirty;
            }
            break;
        }
        case JOURNAL_BALANCE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &so->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->balance = je->data.balance.val;
                ca->dirty = je->data.balance.dirty;
                ca->block_dirty = je->data.balance.block_dirty;
            }
            break;
        }
        case JOURNAL_CODE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &so->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                free(ca->code);
                ca->code = je->data.code.old_code;
                ca->code_size = je->data.code.old_code_size;
                ca->code_hash = je->data.code.old_hash;
                ca->has_code = je->data.code.old_has_code;
            }
            je->data.code.old_code = NULL;
            break;
        }
        case JOURNAL_STORAGE: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.storage.slot, skey);
            cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
                &so->storage, skey, SLOT_KEY_SIZE, NULL);
            if (cs) {
                cs->current = je->data.storage.old_value;
                cs->mpt_dirty = je->data.storage.old_mpt_dirty;
            }
            break;
        }
        case JOURNAL_ACCOUNT_CREATE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &so->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
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
                ca->block_dirty     = je->data.create.old_block_dirty;
                ca->block_code_dirty = je->data.create.old_block_code_dirty;
                ca->created         = je->data.create.old_created;
                ca->existed         = je->data.create.old_existed;
                ca->self_destructed = je->data.create.old_self_destructed;
                ca->storage_root    = je->data.create.old_storage_root;
                ca->storage_dirty   = je->data.create.old_storage_dirty;
                ca->storage_cleared = je->data.create.old_storage_cleared;
#ifdef ENABLE_HISTORY
                ca->block_created   = je->data.create.old_block_created;
#endif
            }
            je->data.create.old_code = NULL;
            break;
        }
        case JOURNAL_SELF_DESTRUCT: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &so->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca)
                ca->self_destructed = je->data.old_self_destructed;
            break;
        }
        case JOURNAL_WARM_ADDR:
            mem_art_delete(&so->warm_addrs, je->addr.bytes, ADDRESS_SIZE);
            break;
        case JOURNAL_WARM_SLOT: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.slot, skey);
            mem_art_delete(&so->warm_slots, skey, SLOT_KEY_SIZE);
            break;
        }
        case JOURNAL_TRANSIENT_STORAGE: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.slot, skey);
            if (uint256_is_zero(&je->data.old_transient_value))
                mem_art_delete(&so->transient, skey, SLOT_KEY_SIZE);
            else
                mem_art_upsert(&so->transient, skey, SLOT_KEY_SIZE,
                               &je->data.old_transient_value, sizeof(uint256_t));
            break;
        }
        }
    }
}

/* =========================================================================
 * Code — read (lazy-load from code_store)
 * ========================================================================= */

static void load_code(state_overlay_t *so, cached_account_t *ca) {
    if (ca->code || !ca->has_code || !so->code_store) return;
    uint32_t size = code_store_get_size(so->code_store, ca->code_hash.bytes);
    if (size == 0) return;
    ca->code = malloc(size);
    if (!ca->code) return;
    code_store_get(so->code_store, ca->code_hash.bytes, ca->code, size);
    ca->code_size = size;
}

uint32_t state_overlay_get_code_size(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return 0;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca || !ca->has_code) return 0;
    load_code(so, ca);
    return ca->code_size;
}

bool state_overlay_get_code(state_overlay_t *so, const address_t *addr,
                             uint8_t *out, uint32_t *out_len) {
    if (!so || !addr) return false;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca || !ca->has_code) { if (out_len) *out_len = 0; return true; }
    load_code(so, ca);
    if (!ca->code) { if (out_len) *out_len = 0; return true; }
    if (out && out_len) {
        uint32_t n = ca->code_size < *out_len ? ca->code_size : *out_len;
        memcpy(out, ca->code, n);
        *out_len = ca->code_size;
    } else if (out_len) {
        *out_len = ca->code_size;
    }
    return true;
}

const uint8_t *state_overlay_get_code_ptr(state_overlay_t *so, const address_t *addr,
                                           uint32_t *out_len) {
    if (!so || !addr) { if (out_len) *out_len = 0; return NULL; }
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca || !ca->has_code) { if (out_len) *out_len = 0; return NULL; }
    load_code(so, ca);
    if (!ca->code) { if (out_len) *out_len = 0; return NULL; }
    if (out_len) *out_len = ca->code_size;
    return ca->code;
}

/* =========================================================================
 * Code — write (journaled)
 * ========================================================================= */

void state_overlay_set_code(state_overlay_t *so, const address_t *addr,
                             const uint8_t *code, uint32_t len) {
    if (!so || !addr) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_CODE, .addr = *addr,
        .data.code = { .old_hash = ca->code_hash, .old_has_code = ca->has_code,
                        .old_code = ca->code, .old_code_size = ca->code_size }
    };
    if (!journal_push(so, &je))
        free(ca->code);

    ca->code = NULL;
    ca->code_size = 0;

    if (code && len > 0) {
        ca->code = malloc(len);
        if (ca->code) { memcpy(ca->code, code, len); ca->code_size = len; }
        ca->has_code = true;
        ca->code_hash = hash_keccak256(code, len);
        if (so->code_store)
            code_store_put(so->code_store, ca->code_hash.bytes, code, len);
    } else {
        ca->has_code = false;
        ca->code_hash = hash_zero();
    }

    mark_account_tx_dirty(so, ca);
    ca->dirty = true;
    ca->code_dirty = true;
    ca->block_dirty = true;
    ca->block_code_dirty = true;
#ifdef ENABLE_HISTORY
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(so, ca);
}

void state_overlay_set_code_hash(state_overlay_t *so, const address_t *addr,
                                  const hash_t *code_hash) {
    if (!so || !addr || !code_hash) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return;
    ca->code_hash = *code_hash;
    ca->has_code = (memcmp(code_hash->bytes, HASH_EMPTY_CODE.bytes, 32) != 0 &&
                    memcmp(code_hash->bytes, ((hash_t){0}).bytes, 32) != 0);
    ca->code_dirty = true;
    ca->block_code_dirty = true;
    mark_account_mpt_dirty(so, ca);
}

/* =========================================================================
 * SLOAD / SSTORE combined operations
 * ========================================================================= */

uint256_t state_overlay_sload(state_overlay_t *so, const address_t *addr,
                               const uint256_t *key, bool *was_warm) {
    if (!so || !addr || !key) { if (was_warm) *was_warm = false; return UINT256_ZERO_INIT; }
    cached_slot_t *cs = ensure_slot(so, addr, key);
    if (!cs) { if (was_warm) *was_warm = false; return UINT256_ZERO_INIT; }
    if (was_warm)
        *was_warm = mem_art_contains(&so->warm_slots, cs->key, SLOT_KEY_SIZE);
    return cs->current;
}

void state_overlay_sstore_lookup(state_overlay_t *so, const address_t *addr,
                                  const uint256_t *key,
                                  uint256_t *current, uint256_t *original,
                                  bool *was_warm) {
    if (!so || !addr || !key) {
        if (current) *current = UINT256_ZERO_INIT;
        if (original) *original = UINT256_ZERO_INIT;
        if (was_warm) *was_warm = false;
        return;
    }
    cached_slot_t *cs = ensure_slot(so, addr, key);
    if (!cs) {
        if (current) *current = UINT256_ZERO_INIT;
        if (original) *original = UINT256_ZERO_INIT;
        if (was_warm) *was_warm = false;
        return;
    }
    if (current) *current = cs->current;
    if (original) *original = cs->original;
    if (was_warm)
        *was_warm = mem_art_contains(&so->warm_slots, cs->key, SLOT_KEY_SIZE);
}

/* =========================================================================
 * Account lifecycle
 * ========================================================================= */

void state_overlay_create_account(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_ACCOUNT_CREATE, .addr = *addr,
        .data.create = {
            .old_nonce = ca->nonce, .old_balance = ca->balance,
            .old_code_hash = ca->code_hash, .old_has_code = ca->has_code,
            .old_code = ca->code, .old_code_size = ca->code_size,
            .old_dirty = ca->dirty, .old_code_dirty = ca->code_dirty,
            .old_block_dirty = ca->block_dirty, .old_block_code_dirty = ca->block_code_dirty,
            .old_created = ca->created, .old_existed = ca->existed,
            .old_self_destructed = ca->self_destructed,
            .old_storage_root = ca->storage_root,
            .old_storage_dirty = ca->storage_dirty,
            .old_storage_cleared = ca->storage_cleared,
#ifdef ENABLE_HISTORY
            .old_block_created = ca->block_created,
#endif
        }
    };
    if (!journal_push(so, &je)) free(ca->code);

    uint256_t bal = ca->balance; /* preserve existing balance */
    ca->nonce = 0;
    ca->balance = bal;
    ca->code_hash = hash_zero();
    ca->has_code = false;
    ca->code = NULL;
    ca->code_size = 0;
    ca->created = true;
    mark_account_tx_dirty(so, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#ifdef ENABLE_HISTORY
    ca->block_created = true;
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(so, ca);
    ca->code_dirty = false;
    ca->self_destructed = false;
    ca->storage_root = HASH_EMPTY_STORAGE;
    ca->storage_dirty = true;
    ca->storage_cleared = true;
}

void state_overlay_self_destruct(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return;

    journal_entry_t je = {
        .type = JOURNAL_SELF_DESTRUCT, .addr = *addr,
        .data.old_self_destructed = ca->self_destructed,
    };
    journal_push(so, &je);

    ca->self_destructed = true;
    mark_account_tx_dirty(so, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#ifdef ENABLE_HISTORY
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(so, ca);
}

void state_overlay_mark_existed(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (ca) ca->existed = true;
}

bool state_overlay_is_self_destructed(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    const cached_account_t *ca = (const cached_account_t *)mem_art_get(
        &so->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    return ca ? ca->self_destructed : false;
}

bool state_overlay_is_created(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    const cached_account_t *ca = (const cached_account_t *)mem_art_get(
        &so->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    return ca ? ca->created : false;
}

bool state_overlay_has_storage(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    cached_account_t *ca = ensure_account(so, addr);
    if (ca && memcmp(ca->storage_root.bytes, HASH_EMPTY_STORAGE.bytes, 32) != 0)
        return true;
    /* Scan cached slots */
    mem_art_iterator_t *iter = mem_art_iterator_create(&so->storage);
    if (!iter) return false;
    bool found = false;
    while (!mem_art_iterator_done(iter)) {
        size_t klen;
        const uint8_t *key = mem_art_iterator_key(iter, &klen);
        if (key && klen == SLOT_KEY_SIZE &&
            memcmp(key, addr->bytes, ADDRESS_SIZE) == 0) {
            size_t vlen;
            const cached_slot_t *cs = (const cached_slot_t *)
                mem_art_iterator_value(iter, &vlen);
            if (cs && !uint256_is_zero(&cs->current)) { found = true; break; }
        }
        mem_art_iterator_next(iter);
    }
    mem_art_iterator_destroy(iter);
    return found;
}

/* =========================================================================
 * Access lists (EIP-2929)
 * ========================================================================= */

bool state_overlay_warm_address(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    bool was_new = false;
    mem_art_insert_check(&so->warm_addrs, addr->bytes, ADDRESS_SIZE,
                         NULL, 0, &was_new);
    if (!was_new) return true;
    journal_entry_t je = { .type = JOURNAL_WARM_ADDR, .addr = *addr };
    journal_push(so, &je);
    return false;
}

bool state_overlay_warm_slot(state_overlay_t *so, const address_t *addr,
                              const uint256_t *key) {
    if (!so || !addr || !key) return false;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    bool was_new = false;
    mem_art_insert_check(&so->warm_slots, skey, SLOT_KEY_SIZE,
                         NULL, 0, &was_new);
    if (!was_new) return true;
    journal_entry_t je = { .type = JOURNAL_WARM_SLOT, .addr = *addr, .data.slot = *key };
    journal_push(so, &je);
    return false;
}

bool state_overlay_is_address_warm(const state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    return mem_art_contains(&so->warm_addrs, addr->bytes, ADDRESS_SIZE);
}

bool state_overlay_is_slot_warm(const state_overlay_t *so, const address_t *addr,
                                 const uint256_t *key) {
    if (!so || !addr || !key) return false;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    return mem_art_contains(&so->warm_slots, skey, SLOT_KEY_SIZE);
}

/* =========================================================================
 * Transient storage (EIP-1153)
 * ========================================================================= */

uint256_t state_overlay_tload(state_overlay_t *so, const address_t *addr,
                               const uint256_t *key) {
    if (!so || !addr || !key) return UINT256_ZERO_INIT;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    const uint256_t *val = (const uint256_t *)mem_art_get(
        &so->transient, skey, SLOT_KEY_SIZE, NULL);
    return val ? *val : UINT256_ZERO_INIT;
}

void state_overlay_tstore(state_overlay_t *so, const address_t *addr,
                           const uint256_t *key, const uint256_t *value) {
    if (!so || !addr || !key || !value) return;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);

    uint256_t old = UINT256_ZERO_INIT;
    const uint256_t *existing = (const uint256_t *)mem_art_get(
        &so->transient, skey, SLOT_KEY_SIZE, NULL);
    if (existing) old = *existing;

    journal_entry_t je = {
        .type = JOURNAL_TRANSIENT_STORAGE, .addr = *addr,
        .data = { .old_transient_value = old }
    };
    journal_push(so, &je);
    mem_art_insert(&so->transient, skey, SLOT_KEY_SIZE, value, sizeof(uint256_t));
}

/* =========================================================================
 * EIP-161
 * ========================================================================= */

void state_overlay_set_prune_empty(state_overlay_t *so, bool enabled) {
    if (so) so->prune_empty = enabled;
}

/* =========================================================================
 * Begin block
 * ========================================================================= */

void state_overlay_begin_block(state_overlay_t *so, uint64_t block_number) {
    (void)so; (void)block_number;
}

/* =========================================================================
 * Commit — per-block (EIP-2200 originals)
 * ========================================================================= */

static bool commit_slot_cb(const uint8_t *key, size_t key_len,
                           const void *value, size_t value_len, void *ud) {
    (void)key; (void)key_len; (void)value_len; (void)ud;
    cached_slot_t *cs = (cached_slot_t *)(uintptr_t)value;
    cs->original = cs->current;
    cs->dirty = false;
#ifdef ENABLE_HISTORY
    cs->block_original = cs->current;
#endif
    return true;
}

static bool commit_account_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len, void *ud) {
    (void)key; (void)key_len; (void)value_len; (void)ud;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    bool is_empty = (ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code);
    if ((ca->existed || ca->created || ca->dirty || ca->code_dirty) && !is_empty)
        ca->existed = true;
    ca->created = false;
    ca->dirty = false;
    ca->code_dirty = false;
    ca->self_destructed = false;
#ifdef ENABLE_HISTORY
    ca->original_nonce = ca->nonce;
    ca->original_balance = ca->balance;
    ca->original_code_hash = ca->code_hash;
    ca->block_accessed = false;
    ca->block_created = false;
    ca->block_self_destructed = false;
#endif
    return true;
}

void state_overlay_commit(state_overlay_t *so) {
    if (!so) return;
    mem_art_foreach(&so->storage, commit_slot_cb, NULL);
    mem_art_foreach(&so->accounts, commit_account_cb, NULL);
    so->journal_len = 0;
}

/* =========================================================================
 * Commit TX — per-transaction
 * ========================================================================= */

typedef struct {
    address_t *addrs;
    size_t count, cap;
    state_overlay_t *so;
} sd_ctx_t;

static bool commit_tx_slot_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len, void *ud) {
    (void)key; (void)key_len; (void)value_len;
    cached_slot_t *cs = (cached_slot_t *)(uintptr_t)value;
    sd_ctx_t *ctx = (sd_ctx_t *)ud;
    for (size_t i = 0; i < ctx->count; i++) {
        if (memcmp(cs->key, ctx->addrs[i].bytes, ADDRESS_SIZE) == 0) {
            cs->current = UINT256_ZERO;
            cs->original = UINT256_ZERO;
            cs->dirty = false;
            cs->block_dirty = false;
            if (ctx->so) mark_slot_mpt_dirty(ctx->so, cs);
            return true;
        }
    }
    cs->original = cs->current;
    cs->dirty = false;
    return true;
}

static void sd_ctx_push(sd_ctx_t *ctx, const address_t *addr) {
    if (ctx->count >= ctx->cap) {
        size_t nc = ctx->cap ? ctx->cap * 2 : 16;
        address_t *na = realloc(ctx->addrs, nc * sizeof(*na));
        if (!na) return;
        ctx->addrs = na; ctx->cap = nc;
    }
    ctx->addrs[ctx->count++] = *addr;
}

void state_overlay_commit_tx(state_overlay_t *so) {
    if (!so) return;

    sd_ctx_t ctx = { .addrs = NULL, .count = 0, .cap = 0, .so = so };

    for (size_t i = 0; i < so->tx_dirty_accounts.count; i++) {
        const uint8_t *akey = so->tx_dirty_accounts.keys + i * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &so->accounts, akey, ADDRESS_KEY_SIZE, NULL);
        if (!ca) continue;

        if (ca->self_destructed) {
            sd_ctx_push(&ctx, &ca->addr);
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
#ifdef ENABLE_HISTORY
            ca->block_self_destructed = true;
#endif
            ca->block_dirty = false;
            ca->self_destructed = false;
            ca->block_code_dirty = false;
            ca->storage_root = HASH_EMPTY_STORAGE;
            ca->storage_cleared = true;
            ca->storage_dirty = true;
            continue;
        }

        bool is_empty = (ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code);
        if ((ca->existed || ca->created || ca->dirty || ca->code_dirty) && !is_empty)
            ca->existed = true;

        /* EIP-161: touched empty → prune */
        if (so->prune_empty && is_empty && (ca->dirty || ca->code_dirty)) {
            ca->existed = false;
            mark_account_mpt_dirty(so, ca);
        }

        ca->created = false;
        ca->dirty = false;
        ca->code_dirty = false;
        ca->self_destructed = false;
    }

    /* Process storage */
    if (ctx.count > 0) {
        mem_art_foreach(&so->storage, commit_tx_slot_cb, &ctx);
    } else {
        for (size_t i = 0; i < so->tx_dirty_slots.count; i++) {
            const uint8_t *skey = so->tx_dirty_slots.keys + i * SLOT_KEY_SIZE;
            cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
                &so->storage, skey, SLOT_KEY_SIZE, NULL);
            if (!cs) continue;
            cs->original = cs->current;
            cs->dirty = false;
        }
    }

    free(ctx.addrs);
    dirty_clear(&so->tx_dirty_accounts);
    dirty_clear(&so->tx_dirty_slots);
    so->journal_len = 0;

    mem_art_destroy(&so->warm_addrs);  mem_art_init(&so->warm_addrs);
    mem_art_destroy(&so->warm_slots);  mem_art_init(&so->warm_slots);
    mem_art_destroy(&so->transient);   mem_art_init(&so->transient);
}

/* =========================================================================
 * Clear prestate dirty (for test_runner pre-state setup)
 * ========================================================================= */

static bool clear_prestate_acct_cb(const uint8_t *k, size_t kl,
                                    const void *v, size_t vl, void *u) {
    (void)k; (void)kl; (void)vl; (void)u;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)v;
    ca->block_dirty = false;
    ca->block_code_dirty = false;
    ca->mpt_dirty = false;
#ifdef ENABLE_HISTORY
    ca->block_self_destructed = false;
    ca->block_created = false;
#endif
    return true;
}

static bool clear_prestate_slot_cb(const uint8_t *k, size_t kl,
                                    const void *v, size_t vl, void *u) {
    (void)k; (void)kl; (void)vl; (void)u;
    cached_slot_t *cs = (cached_slot_t *)(uintptr_t)v;
    cs->block_dirty = false;
    cs->mpt_dirty = false;
    return true;
}

/* TODO: expose via header if needed by test_runner */

/* =========================================================================
 * TODO: compute_mpt_root, evict, stats, prefetch, collect_*
 * ========================================================================= */
