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
 * Meta arrays — indexed by flat_store overlay pool index
 * ========================================================================= */

typedef struct {
    cached_account_t *entries;
    uint32_t          capacity;
} account_meta_pool_t;

typedef struct {
    cached_slot_t    *entries;
    uint32_t          capacity;
} slot_meta_pool_t;

static cached_account_t *acct_meta_ensure(account_meta_pool_t *p, uint32_t idx) {
    if (idx >= p->capacity) {
        uint32_t nc = p->capacity ? p->capacity * 2 : 4096;
        while (nc <= idx) nc *= 2;
        cached_account_t *ne = realloc(p->entries, nc * sizeof(*ne));
        if (!ne) return NULL;
        memset(ne + p->capacity, 0, (nc - p->capacity) * sizeof(*ne));
        p->entries = ne;
        p->capacity = nc;
    }
    return &p->entries[idx];
}

static cached_slot_t *slot_meta_ensure(slot_meta_pool_t *p, uint32_t idx) {
    if (idx >= p->capacity) {
        uint32_t nc = p->capacity ? p->capacity * 2 : 4096;
        while (nc <= idx) nc *= 2;
        cached_slot_t *ne = realloc(p->entries, nc * sizeof(*ne));
        if (!ne) return NULL;
        memset(ne + p->capacity, 0, (nc - p->capacity) * sizeof(*ne));
        p->entries = ne;
        p->capacity = nc;
    }
    return &p->entries[idx];
}

/* =========================================================================
 * Main struct
 * ========================================================================= */

struct state_overlay {
    flat_state_t     *flat_state;
    code_store_t     *code_store;
    account_trie_t   *account_trie;
    storage_trie_t   *storage_trie;

    /* Meta arrays — indexed by flat_store overlay pool index.
     * Persistent fields (nonce, balance, etc.) are ALSO in the flat_store
     * overlay entry as compressed records. Meta holds typed access + flags. */
    account_meta_pool_t acct_meta;
    slot_meta_pool_t    slot_meta;

    journal_entry_t *journal;
    uint32_t journal_len;
    uint32_t journal_cap;

    /* Ephemeral structures — not persisted, per-tx/per-block only */
    mem_art_t warm_addrs;   /* EIP-2929 */
    mem_art_t warm_slots;
    mem_art_t transient;    /* EIP-1153 */

    dirty_vec_t tx_dirty_accounts;
    dirty_vec_t tx_dirty_slots;
    dirty_vec_t dirty_accounts;
    dirty_vec_t dirty_slots;

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
 * Re-encode account meta → flat_store overlay compressed record.
 * Called after every write to keep the overlay entry in sync.
 * ========================================================================= */

static void sync_account_to_overlay(state_overlay_t *so, cached_account_t *ca) {
    if (!so->flat_state) return;
    flat_account_record_t frec;
    frec.nonce = ca->nonce;
    uint256_to_bytes(&ca->balance, frec.balance);
    const uint8_t *ch = ca->has_code
        ? ca->code_hash.bytes : HASH_EMPTY_CODE.bytes;
    memcpy(frec.code_hash, ch, 32);
    memcpy(frec.storage_root, ca->storage_root.bytes, 32);
    flat_state_put_account(so->flat_state, ca->addr_hash.bytes, &frec);
}

static void sync_slot_to_overlay(state_overlay_t *so, cached_account_t *ca,
                                  cached_slot_t *cs) {
    if (!so->flat_state) return;
    if (uint256_is_zero(&cs->current)) {
        flat_state_delete_storage(so->flat_state,
                                   ca->addr_hash.bytes, cs->slot_hash.bytes);
    } else {
        uint8_t vbe[32];
        uint256_to_bytes(&cs->current, vbe);
        flat_state_put_storage(so->flat_state,
                                ca->addr_hash.bytes, cs->slot_hash.bytes, vbe);
    }
}

/* =========================================================================
 * ensure_account — load from flat_store overlay or create
 *
 * Uses flat_store_ensure_overlay to get an overlay index, then
 * initializes the meta sidecar at the same index.
 * ========================================================================= */

static cached_account_t *ensure_account(state_overlay_t *so, const address_t *addr) {
    if (!so->flat_state) return NULL; /* flat_state required */

    hash_t addr_hash = hash_keccak256(addr->bytes, 20);
    flat_store_t *acct_store = flat_state_account_store(so->flat_state);

    /* Check if already in overlay */
    uint32_t idx = flat_store_overlay_index(acct_store, addr_hash.bytes);
    if (idx != UINT32_MAX) {
        cached_account_t *ca = acct_meta_ensure(&so->acct_meta, idx);
        return ca;
    }

    /* Load from disk or create new */
    idx = flat_store_ensure_overlay(acct_store, addr_hash.bytes);
    if (idx == UINT32_MAX) return NULL;

    cached_account_t *ca = acct_meta_ensure(&so->acct_meta, idx);
    if (!ca) return NULL;

    /* Initialize meta from the overlay record */
    memset(ca, 0, sizeof(*ca));
    address_copy(&ca->addr, addr);
    ca->addr_hash = addr_hash;
    ca->storage_root = HASH_EMPTY_STORAGE;

    /* Read persistent fields from overlay entry */
    uint32_t rec_len;
    uint8_t *rec = flat_store_overlay_record(acct_store, idx, &rec_len);
    if (rec && rec_len > 0) {
        flat_account_record_t frec;
        if (flat_state_get_account(so->flat_state, addr_hash.bytes, &frec)) {
            ca->nonce = frec.nonce;
            ca->balance = uint256_from_bytes(frec.balance, 32);
            memcpy(ca->code_hash.bytes, frec.code_hash, 32);
            memcpy(ca->storage_root.bytes, frec.storage_root, 32);
            ca->has_code = (memcmp(frec.code_hash, ((hash_t){0}).bytes, 32) != 0 &&
                            memcmp(frec.code_hash, HASH_EMPTY_CODE.bytes, 32) != 0);
            ca->existed = true;
            so->flat_acct_hit++;
        }
    } else {
        so->flat_acct_miss++;
    }

#ifdef ENABLE_HISTORY
    ca->original_nonce = ca->nonce;
    ca->original_balance = ca->balance;
    ca->original_code_hash = ca->code_hash;
#endif

    return ca;
}

/* =========================================================================
 * ensure_slot — load from flat_store overlay or create
 * ========================================================================= */

static cached_slot_t *ensure_slot(state_overlay_t *so, const address_t *addr,
                                  const uint256_t *slot) {
    if (!so->flat_state) return NULL;

    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return NULL;

    hash_t slot_hash = hash_keccak256((const uint8_t *)slot, 32);

    /* Build 64-byte storage key: addr_hash[32] || slot_hash[32] */
    uint8_t stor_key[64];
    memcpy(stor_key, ca->addr_hash.bytes, 32);
    memcpy(stor_key + 32, slot_hash.bytes, 32);

    flat_store_t *stor_store = flat_state_storage_store(so->flat_state);

    /* Check if already in overlay */
    uint32_t idx = flat_store_overlay_index(stor_store, stor_key);
    if (idx != UINT32_MAX) {
        return slot_meta_ensure(&so->slot_meta, idx);
    }

    /* Load from disk or create new */
    idx = flat_store_ensure_overlay(stor_store, stor_key);
    if (idx == UINT32_MAX) return NULL;

    cached_slot_t *cs = slot_meta_ensure(&so->slot_meta, idx);
    if (!cs) return NULL;

    /* Initialize meta */
    memset(cs, 0, sizeof(*cs));
    /* Store addr[20] || slot_be[32] as the SLOT_KEY for dirty tracking */
    memcpy(cs->key, addr->bytes, ADDRESS_SIZE);
    uint256_to_bytes(slot, cs->key + ADDRESS_SIZE);
    cs->slot_hash = slot_hash;

    /* Read value from overlay entry */
    uint32_t rec_len;
    uint8_t *rec = flat_store_overlay_record(stor_store, idx, &rec_len);
    if (rec && rec_len == 32) {
        cs->original = uint256_from_bytes(rec, 32);
        cs->current = cs->original;
        so->flat_stor_hit++;
    } else {
        so->flat_stor_miss++;
    }

#ifdef ENABLE_HISTORY
    cs->block_original = cs->original;
#endif

    return cs;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

static void init_tries(state_overlay_t *so) {
    if (!so->flat_state) return;
    compact_art_t *s_art = flat_state_storage_art(so->flat_state);
    flat_store_t  *s_store = flat_state_storage_store(so->flat_state);
    if (s_art && s_store)
        so->storage_trie = storage_trie_create(s_art, s_store);
    compact_art_t *a_art = flat_state_account_art(so->flat_state);
    flat_store_t  *a_store = flat_state_account_store(so->flat_state);
    if (a_art && a_store)
        so->account_trie = account_trie_create(a_art, a_store);
}

state_overlay_t *state_overlay_create(flat_state_t *fs, code_store_t *cs) {
    state_overlay_t *so = calloc(1, sizeof(*so));
    if (!so) return NULL;

    so->flat_state = fs;
    so->code_store = cs;

    mem_art_init(&so->warm_addrs);
    mem_art_init(&so->warm_slots);
    mem_art_init(&so->transient);

    so->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!so->journal) { free(so); return NULL; }
    so->journal_cap = JOURNAL_INIT_CAP;

    init_tries(so);
    return so;
}

void state_overlay_destroy(state_overlay_t *so) {
    if (!so) return;

    /* Free code pointers in account meta */
    for (uint32_t i = 0; i < so->acct_meta.capacity; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        free(ca->code);
    }
    free(so->acct_meta.entries);
    free(so->slot_meta.entries);

    if (so->storage_trie) storage_trie_destroy(so->storage_trie);
    if (so->account_trie) account_trie_destroy(so->account_trie);

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

void state_overlay_set_flat_state(state_overlay_t *so, flat_state_t *fs) {
    if (!so) return;
    so->flat_state = fs;
    if (so->storage_trie) { storage_trie_destroy(so->storage_trie); so->storage_trie = NULL; }
    if (so->account_trie) { account_trie_destroy(so->account_trie); so->account_trie = NULL; }
    init_tries(so);
}

/* =========================================================================
 * Account — read
 * ========================================================================= */

bool state_overlay_exists(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return false;
    /* Account "exists" if it was in the trie (existed), was created this tx
     * (CREATE), was touched this tx (dirty via add_balance(0) from CALL),
     * or was touched in an earlier tx this block (block_dirty). Without
     * dirty/block_dirty, non-existent addresses get charged 25000 new-account
     * gas on every CALL within the same tx. */
    return ca->existed || ca->created || ca->dirty || ca->block_dirty;
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
    sync_account_to_overlay(so, ca);
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
    sync_account_to_overlay(so, ca);
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
        sync_slot_to_overlay(so, ca, cs);
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
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
            if (ca) {
                ca->nonce = je->data.nonce.val;
                ca->dirty = je->data.nonce.dirty;
                ca->block_dirty = je->data.nonce.block_dirty;
            }
            break;
        }
        case JOURNAL_BALANCE: {
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
            if (ca) {
                ca->balance = je->data.balance.val;
                ca->dirty = je->data.balance.dirty;
                ca->block_dirty = je->data.balance.block_dirty;
            }
            break;
        }
        case JOURNAL_CODE: {
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
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
            cached_slot_t *cs = find_slot_meta(so, skey);
            if (cs) {
                cs->current = je->data.storage.old_value;
                cs->mpt_dirty = je->data.storage.old_mpt_dirty;
            }
            break;
        }
        case JOURNAL_ACCOUNT_CREATE: {
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
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
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
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
            make_slot_key(&je->addr, &je->data.storage.slot, skey);
            if (uint256_is_zero(&je->data.storage.old_value))
                mem_art_delete(&so->transient, skey, SLOT_KEY_SIZE);
            else
                mem_art_upsert(&so->transient, skey, SLOT_KEY_SIZE,
                               &je->data.storage.old_value, sizeof(uint256_t));
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
    sync_account_to_overlay(so, ca);
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
    sync_account_to_overlay(so, ca);
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
    sync_account_to_overlay(so, ca);
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
    const cached_account_t *ca = find_account_meta(so, addr->bytes);
    return ca ? ca->self_destructed : false;
}

bool state_overlay_is_created(state_overlay_t *so, const address_t *addr) {
    if (!so || !addr) return false;
    const cached_account_t *ca = find_account_meta(so, addr->bytes);
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
        .data.storage = { .slot = *key, .old_value = old }
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

void state_overlay_commit(state_overlay_t *so) {
    if (!so) return;

    /* Reset slot originals */
    for (uint32_t i = 0; i < so->slot_meta.capacity; i++) {
        cached_slot_t *cs = &so->slot_meta.entries[i];
        if (cs->slot_hash.bytes[0] == 0 && cs->slot_hash.bytes[1] == 0 &&
            !cs->dirty && !cs->block_dirty && !cs->mpt_dirty) continue; /* uninitialized */
        cs->original = cs->current;
        cs->dirty = false;
#ifdef ENABLE_HISTORY
        cs->block_original = cs->current;
#endif
    }

    /* Promote + reset account flags */
    for (uint32_t i = 0; i < so->acct_meta.capacity; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        if (ca->addr.bytes[0] == 0 && ca->addr.bytes[1] == 0 &&
            !ca->dirty && !ca->existed && !ca->created) continue; /* uninitialized */
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
    }

    so->journal_len = 0;
}

/* =========================================================================
 * Commit TX — per-transaction
 * ========================================================================= */

/* Helper: find account meta by address (via flat_store overlay lookup) */
static cached_account_t *find_account_meta(state_overlay_t *so, const uint8_t *addr) {
    hash_t ah = hash_keccak256(addr, 20);
    flat_store_t *store = flat_state_account_store(so->flat_state);
    uint32_t idx = flat_store_overlay_index(store, ah.bytes);
    if (idx == UINT32_MAX || idx >= so->acct_meta.capacity) return NULL;
    return &so->acct_meta.entries[idx];
}

/* Helper: find slot meta by skey (addr[20]||slot[32]) */
static cached_slot_t *find_slot_meta(state_overlay_t *so, const uint8_t *skey) {
    /* Need addr_hash and slot_hash to build 64-byte storage key */
    hash_t ah = hash_keccak256(skey, 20);
    hash_t sh = hash_keccak256(skey + 20, 32);
    uint8_t stor_key[64];
    memcpy(stor_key, ah.bytes, 32);
    memcpy(stor_key + 32, sh.bytes, 32);
    flat_store_t *store = flat_state_storage_store(so->flat_state);
    uint32_t idx = flat_store_overlay_index(store, stor_key);
    if (idx == UINT32_MAX || idx >= so->slot_meta.capacity) return NULL;
    return &so->slot_meta.entries[idx];
}

void state_overlay_commit_tx(state_overlay_t *so) {
    if (!so) return;

    /* Collect self-destructed addresses */
    address_t *sd_addrs = NULL;
    size_t sd_count = 0, sd_cap = 0;

    for (size_t i = 0; i < so->tx_dirty_accounts.count; i++) {
        const uint8_t *akey = so->tx_dirty_accounts.keys + i * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca) continue;

        if (ca->self_destructed) {
            if (sd_count >= sd_cap) {
                size_t nc = sd_cap ? sd_cap * 2 : 16;
                address_t *na = realloc(sd_addrs, nc * sizeof(*na));
                if (na) { sd_addrs = na; sd_cap = nc; }
            }
            if (sd_count < sd_cap)
                sd_addrs[sd_count++] = ca->addr;

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
            sync_account_to_overlay(so, ca);
            continue;
        }

        bool is_empty = (ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code);
        if ((ca->existed || ca->created || ca->dirty || ca->code_dirty) && !is_empty)
            ca->existed = true;

        if (so->prune_empty && is_empty && (ca->dirty || ca->code_dirty)) {
            ca->existed = false;
            mark_account_mpt_dirty(so, ca);
        }

        ca->created = false;
        ca->dirty = false;
        ca->code_dirty = false;
        ca->self_destructed = false;
    }

    /* Process storage: zero slots for self-destructed accounts */
    if (sd_count > 0) {
        /* Scan ALL cached slots — need to find ones belonging to sd accounts */
        for (uint32_t i = 0; i < so->slot_meta.capacity; i++) {
            cached_slot_t *cs = &so->slot_meta.entries[i];
            if (!cs->dirty && !cs->block_dirty && !cs->mpt_dirty &&
                cs->slot_hash.bytes[0] == 0 && cs->slot_hash.bytes[1] == 0)
                continue; /* uninitialized */

            bool is_sd = false;
            for (size_t j = 0; j < sd_count; j++) {
                if (memcmp(cs->key, sd_addrs[j].bytes, ADDRESS_SIZE) == 0) {
                    is_sd = true;
                    break;
                }
            }
            if (is_sd) {
                cs->current = UINT256_ZERO;
                cs->original = UINT256_ZERO;
                cs->dirty = false;
                cs->block_dirty = false;
                mark_slot_mpt_dirty(so, cs);
            } else {
                cs->original = cs->current;
                cs->dirty = false;
            }
        }
    } else {
        /* Fast path: only process tx-dirty slots */
        for (size_t i = 0; i < so->tx_dirty_slots.count; i++) {
            const uint8_t *skey = so->tx_dirty_slots.keys + i * SLOT_KEY_SIZE;
            cached_slot_t *cs = find_slot_meta(so, skey);
            if (!cs) continue;
            cs->original = cs->current;
            cs->dirty = false;
        }
    }

    free(sd_addrs);
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

void state_overlay_clear_prestate_dirty(state_overlay_t *so) {
    if (!so) return;
    mem_art_foreach(&so->accounts, clear_prestate_acct_cb, NULL);
    mem_art_foreach(&so->storage, clear_prestate_slot_cb, NULL);
    dirty_clear(&so->dirty_accounts);
    dirty_clear(&so->dirty_slots);
}

/* =========================================================================
 * flush callbacks for compute_mpt_root
 * ========================================================================= */

static bool flush_slot_cb(const uint8_t *key, size_t key_len,
                            const void *value, size_t value_len, void *ud) {
    (void)key_len; (void)value_len;
    state_overlay_t *so = ud;
    const cached_slot_t *cs = value;
    const cached_account_t *ca = (const cached_account_t *)mem_art_get(
        &so->accounts, key, ADDRESS_KEY_SIZE, NULL);
    if (!ca || !ca->existed) return true;
    if (ca->storage_cleared && !cs->mpt_dirty) return true;
    if (uint256_is_zero(&cs->current)) {
        flat_state_delete_storage(so->flat_state,
                                   ca->addr_hash.bytes, cs->slot_hash.bytes);
    } else {
        uint8_t vbe[32];
        uint256_to_bytes(&cs->current, vbe);
        flat_state_put_storage(so->flat_state,
                                ca->addr_hash.bytes, cs->slot_hash.bytes, vbe);
    }
    return true;
}

static bool flush_account_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len, void *ud) {
    (void)key; (void)key_len; (void)value_len;
    state_overlay_t *so = ud;
    const cached_account_t *ca = value;
    if (!ca->existed) return true;

    flat_account_record_t frec;
    frec.nonce = ca->nonce;
    uint256_to_bytes(&ca->balance, frec.balance);
    const uint8_t *ch = ca->has_code
        ? ca->code_hash.bytes : HASH_EMPTY_CODE.bytes;
    memcpy(frec.code_hash, ch, 32);
    memcpy(frec.storage_root, ca->storage_root.bytes, 32);
    flat_state_put_account(so->flat_state, ca->addr_hash.bytes, &frec);
    return true;
}

/* =========================================================================
 * compute_mpt_root — 7-step checkpoint flow
 * ========================================================================= */

hash_t state_overlay_compute_mpt_root(state_overlay_t *so, bool prune_empty) {
    hash_t root = hash_zero();
    if (!so) return root;
    if (!so->flat_state || !so->account_trie) {
        fprintf(stderr, "FATAL: compute_mpt_root without flat_state/account_trie\n");
        return root;
    }

    struct timespec t0, t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    size_t dirty_count = so->dirty_accounts.count;

    /* Step 1. Promote existed on dirty accounts */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &so->accounts, akey, ADDRESS_KEY_SIZE, NULL);
        if (!ca || !ca->mpt_dirty) continue;
        if (ca->block_dirty || ca->block_code_dirty) {
            if (ca->self_destructed) {
                ca->existed = false;
            } else {
                bool is_empty = (ca->nonce == 0 &&
                                 uint256_is_zero(&ca->balance) && !ca->has_code);
                if (!(!ca->existed && !ca->created && is_empty && prune_empty))
                    ca->existed = true;
            }
        }
    }

    /* Step 2. Delete orphaned storage for dead/storage-cleared accounts */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &so->accounts, akey, ADDRESS_KEY_SIZE, NULL);
        if (!ca || !ca->mpt_dirty) continue;
        if (!ca->existed || ca->storage_cleared)
            flat_state_delete_all_storage(so->flat_state, ca->addr_hash.bytes);
        if (!ca->existed)
            flat_state_delete_account(so->flat_state, ca->addr_hash.bytes);
    }

    /* Step 3. Flush ALL cached slots to flat_state */
    mem_art_foreach(&so->storage, flush_slot_cb, so);

    /* Step 4. Compute storage roots for storage_dirty accounts */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &so->accounts, akey, ADDRESS_KEY_SIZE, NULL);
        if (!ca || !ca->storage_dirty || !ca->existed) continue;
        storage_trie_root(so->storage_trie, ca->addr_hash.bytes,
                           ca->storage_root.bytes);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Step 5. Flush ALL cached accounts to flat_state */
    mem_art_foreach(&so->accounts, flush_account_cb, so);

    /* Step 6. Compute account trie root */
    account_trie_root(so->account_trie, root.bytes);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    /* Step 7. Clear dirty flags */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &so->accounts, akey, ADDRESS_KEY_SIZE, NULL);
        if (!ca) continue;
        ca->mpt_dirty = false;
        ca->block_dirty = false;
        ca->block_code_dirty = false;
        ca->storage_dirty = false;
        ca->storage_cleared = false;
    }

    so->last_root_stor_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                            (t1.tv_nsec - t0.tv_nsec) / 1e6;
    so->last_root_acct_ms = (t2.tv_sec - t1.tv_sec) * 1000.0 +
                            (t2.tv_nsec - t1.tv_nsec) / 1e6;
    so->last_root_dirty_count = dirty_count;
    return root;
}

/* =========================================================================
 * Evict
 * ========================================================================= */

void state_overlay_evict(state_overlay_t *so) {
    if (!so) return;
    if (so->flat_state) {
        mem_art_foreach(&so->storage, flush_slot_cb, so);
        mem_art_foreach(&so->accounts, flush_account_cb, so);
        flat_store_flush_deferred(flat_state_account_store(so->flat_state));
        flat_store_flush_deferred(flat_state_storage_store(so->flat_state));
    }
    mem_art_foreach(&so->accounts, free_code_cb, NULL);
    dirty_clear(&so->dirty_accounts);
    dirty_clear(&so->dirty_slots);
    mem_art_destroy(&so->accounts);  mem_art_init(&so->accounts);
    mem_art_destroy(&so->storage);   mem_art_init(&so->storage);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

state_overlay_stats_t state_overlay_get_stats(const state_overlay_t *so) {
    state_overlay_stats_t s = {0};
    if (!so) return s;
    s.overlay_accounts = mem_art_size(&so->accounts);
    s.overlay_slots = mem_art_size(&so->storage);
    if (so->flat_state) {
        s.flat_acct_count = flat_state_account_count(so->flat_state);
        s.flat_stor_count = flat_state_storage_count(so->flat_state);
        s.flat_acct_mem = compact_art_memory_usage(flat_state_account_art(so->flat_state));
        s.flat_stor_mem = compact_art_memory_usage(flat_state_storage_art(so->flat_state));
    }
    s.root_stor_ms = so->last_root_stor_ms;
    s.root_acct_ms = so->last_root_acct_ms;
    s.root_dirty_count = so->last_root_dirty_count;
    return s;
}
