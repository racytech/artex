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
#include "state_meta.h"
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
#define SLOT_KEY_SIZE     STATE_META_SLOT_KEY_SIZE
#define ADDRESS_KEY_SIZE  20
#define MAX_CODE_SIZE     (24 * 1024 + 1)

/* EIP-161 RIPEMD special case: address 0x0000...0003.
 * In geth, touching RIPEMD adds an extra dirty counter so that a journal
 * revert does not remove it from the dirty set. This ensures that if
 * RIPEMD is touched by a failing CALL (OOG), it still gets pruned as an
 * empty account under EIP-161.  See go-ethereum journal.go:touchChange. */
static const uint8_t RIPEMD_ADDR[20] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3
};

/* Types from state_meta.h: cached_account_t, cached_slot_t,
 * account_meta_pool_t, slot_meta_pool_t */

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
        struct { uint64_t val; bool dirty; bool block_dirty; bool mpt_dirty; } nonce;
        struct { uint256_t val; bool dirty; bool block_dirty; bool mpt_dirty; } balance;
        struct { hash_t old_hash; bool old_has_code; uint8_t *old_code;
                 uint32_t old_code_size;
                 bool old_dirty; bool old_code_dirty;
                 bool old_block_dirty; bool old_block_code_dirty;
                 bool old_mpt_dirty; } code;
        struct { uint256_t slot; uint256_t old_value;
                 bool old_dirty; bool old_block_dirty; bool old_mpt_dirty;
                 bool old_acct_storage_dirty; bool old_acct_mpt_dirty; } storage;
        uint256_t slot;
        struct { bool old_self_destructed; bool old_dirty; bool old_block_dirty; bool old_mpt_dirty; } sd;
        struct {
            uint64_t old_nonce; uint256_t old_balance;
            hash_t old_code_hash; bool old_has_code;
            uint8_t *old_code; uint32_t old_code_size;
            bool old_dirty; bool old_code_dirty;
            bool old_block_dirty; bool old_block_code_dirty;
            bool old_mpt_dirty;
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

    /* Meta arrays — indexed by sequential meta index (NOT flat_store overlay index).
     * Meta holds typed access + flags. Persistent records in flat_store overlay
     * are synced at checkpoint time. */
    account_meta_pool_t acct_meta;
    slot_meta_pool_t    slot_meta;

    /* Lookup tables: addr/skey → meta index */
    mem_art_t acct_index;   /* addr[20] → uint32_t meta index */
    mem_art_t slot_index;   /* skey[52] → uint32_t meta index */

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

    uint32_t next_acct_idx;
    uint32_t next_slot_idx;

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

/* Lookup helpers: find meta by key via local index tables */
static cached_account_t *find_account_meta(state_overlay_t *so, const uint8_t *addr) {
    const uint32_t *pidx = (const uint32_t *)mem_art_get(
        &so->acct_index, addr, ADDRESS_KEY_SIZE, NULL);
    if (!pidx) return NULL;
    uint32_t idx = *pidx;
    if (idx >= so->acct_meta.capacity) return NULL;
    return &so->acct_meta.entries[idx];
}

static cached_slot_t *find_slot_meta(state_overlay_t *so, const uint8_t *skey) {
    const uint32_t *pidx = (const uint32_t *)mem_art_get(
        &so->slot_index, skey, SLOT_KEY_SIZE, NULL);
    if (!pidx) return NULL;
    uint32_t idx = *pidx;
    if (idx >= so->slot_meta.capacity) return NULL;
    return &so->slot_meta.entries[idx];
}

/* =========================================================================
 * Re-encode account meta → flat_store overlay compressed record.
 * Called after every write to keep the overlay entry in sync.
 * ========================================================================= */

static void sync_account_to_overlay(state_overlay_t *so, cached_account_t *ca) {
    if (!so->flat_state) return;
    /* Only sync to flat_state if account should appear in the trie.
     * Don't write empty accounts that haven't been promoted yet —
     * they'll be promoted by commit_tx/commit, then synced at checkpoint. */
    bool is_empty = (ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code);
    if (!ca->existed && is_empty) return;
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
    /* Check local index first */
    cached_account_t *existing = find_account_meta(so, addr->bytes);
    if (existing) return existing;

    /* Allocate new meta entry (sequential index) */
    uint32_t idx = so->next_acct_idx++;
    hash_t addr_hash = hash_keccak256(addr->bytes, 20);

    cached_account_t *ca = acct_meta_ensure(&so->acct_meta, idx);
    if (!ca) return NULL;

    /* Register in index */
    mem_art_insert(&so->acct_index, addr->bytes, ADDRESS_KEY_SIZE,
                   &idx, sizeof(idx));

    /* Initialize meta */
    memset(ca, 0, sizeof(*ca));
    address_copy(&ca->addr, addr);
    ca->addr_hash = addr_hash;
    ca->storage_root = HASH_EMPTY_STORAGE;

    /* Load from flat_state if available */
    if (so->flat_state) {
        flat_account_record_t frec;
        if (flat_state_get_account(so->flat_state, ca->addr_hash.bytes, &frec)) {
            ca->nonce = frec.nonce;
            ca->balance = uint256_from_bytes(frec.balance, 32);
            memcpy(ca->code_hash.bytes, frec.code_hash, 32);
            memcpy(ca->storage_root.bytes, frec.storage_root, 32);
            ca->has_code = (memcmp(frec.code_hash, ((hash_t){0}).bytes, 32) != 0 &&
                            memcmp(frec.code_hash, HASH_EMPTY_CODE.bytes, 32) != 0);
            ca->existed = true;
            so->flat_acct_hit++;
        } else {
            so->flat_acct_miss++;
        }
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
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return NULL;

    /* Build skey for index lookup */
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, slot, skey);

    /* Check local index */
    cached_slot_t *existing = find_slot_meta(so, skey);
    if (existing) return existing;

    /* Allocate new meta entry (sequential index) */
    uint32_t idx = so->next_slot_idx++;
    hash_t slot_hash = hash_keccak256(skey + ADDRESS_SIZE, 32);

    cached_slot_t *cs = slot_meta_ensure(&so->slot_meta, idx);
    if (!cs) return NULL;

    /* Register in index */
    mem_art_insert(&so->slot_index, skey, SLOT_KEY_SIZE, &idx, sizeof(idx));

    /* Initialize meta */
    memset(cs, 0, sizeof(*cs));
    memcpy(cs->key, skey, SLOT_KEY_SIZE);
    cs->slot_hash = slot_hash;

    /* Load from flat_state */
    if (so->flat_state) {
        uint8_t val_be[32];
        if (flat_state_get_storage(so->flat_state, ca->addr_hash.bytes,
                                   cs->slot_hash.bytes, val_be)) {
            cs->original = uint256_from_bytes(val_be, 32);
            cs->current = cs->original;
            so->flat_stor_hit++;
        } else {
            so->flat_stor_miss++;
        }
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
        so->storage_trie = storage_trie_create(s_art, s_store, NULL);
    compact_art_t *a_art = flat_state_account_art(so->flat_state);
    flat_store_t  *a_store = flat_state_account_store(so->flat_state);
    if (a_art && a_store)
        so->account_trie = account_trie_create(a_art, a_store, NULL);
}

state_overlay_t *state_overlay_create(flat_state_t *fs, code_store_t *cs) {
    state_overlay_t *so = calloc(1, sizeof(*so));
    if (!so) return NULL;

    so->flat_state = fs;
    so->code_store = cs;

    mem_art_init(&so->acct_index);
    mem_art_init(&so->slot_index);
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

    mem_art_destroy(&so->acct_index);
    mem_art_destroy(&so->slot_index);
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
        .data.nonce = { .val = ca->nonce, .dirty = ca->dirty, .block_dirty = ca->block_dirty, .mpt_dirty = ca->mpt_dirty }
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
        .data.balance = { .val = ca->balance, .dirty = ca->dirty, .block_dirty = ca->block_dirty, .mpt_dirty = ca->mpt_dirty }
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

    cached_account_t *ca = ensure_account(so, addr);

    journal_entry_t je = {
        .type = JOURNAL_STORAGE,
        .addr = *addr,
        .data.storage = {
            .slot = *key,
            .old_value = cs->current,
            .old_dirty = cs->dirty,
            .old_block_dirty = cs->block_dirty,
            .old_mpt_dirty = cs->mpt_dirty,
            .old_acct_storage_dirty = ca ? ca->storage_dirty : false,
            .old_acct_mpt_dirty = ca ? ca->mpt_dirty : false,
        }
    };
    journal_push(so, &je);

    cs->current = *value;
    mark_slot_tx_dirty(so, cs);
    cs->dirty = true;
    cs->block_dirty = true;
    mark_slot_mpt_dirty(so, cs);

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
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
            if (ca) {
                ca->nonce = je->data.nonce.val;
                ca->dirty = je->data.nonce.dirty;
                ca->block_dirty = je->data.nonce.block_dirty;
                ca->mpt_dirty = je->data.nonce.mpt_dirty;
            }
            break;
        }
        case JOURNAL_BALANCE: {
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
            if (ca) {
                ca->balance = je->data.balance.val;
                /* RIPEMD special case: keep dirty after revert so EIP-161
                 * can prune it even when the touching CALL fails (OOG).
                 * Only applies post-Spurious Dragon (prune_empty=true). */
                if (so->prune_empty &&
                    memcmp(je->addr.bytes, RIPEMD_ADDR, 20) == 0) {
                    /* Don't restore dirty/block_dirty/mpt_dirty — leave them true */
                } else {
                    ca->dirty = je->data.balance.dirty;
                    ca->block_dirty = je->data.balance.block_dirty;
                    ca->mpt_dirty = je->data.balance.mpt_dirty;
                }
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
                ca->dirty = je->data.code.old_dirty;
                ca->code_dirty = je->data.code.old_code_dirty;
                ca->block_dirty = je->data.code.old_block_dirty;
                ca->block_code_dirty = je->data.code.old_block_code_dirty;
                ca->mpt_dirty = je->data.code.old_mpt_dirty;
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
                cs->dirty = je->data.storage.old_dirty;
                cs->block_dirty = je->data.storage.old_block_dirty;
                cs->mpt_dirty = je->data.storage.old_mpt_dirty;
            }
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
            if (ca) {
                ca->storage_dirty = je->data.storage.old_acct_storage_dirty;
                ca->mpt_dirty = je->data.storage.old_acct_mpt_dirty;
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
                ca->mpt_dirty       = je->data.create.old_mpt_dirty;
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
            if (ca) {
                ca->self_destructed = je->data.sd.old_self_destructed;
                ca->dirty = je->data.sd.old_dirty;
                ca->block_dirty = je->data.sd.old_block_dirty;
                ca->mpt_dirty = je->data.sd.old_mpt_dirty;
            }
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
                        .old_code = ca->code, .old_code_size = ca->code_size,
                        .old_dirty = ca->dirty, .old_code_dirty = ca->code_dirty,
                        .old_block_dirty = ca->block_dirty, .old_block_code_dirty = ca->block_code_dirty,
                        .old_mpt_dirty = ca->mpt_dirty }
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
            .old_mpt_dirty = ca->mpt_dirty,
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
        .data.sd = { .old_self_destructed = ca->self_destructed,
                     .old_dirty = ca->dirty, .old_block_dirty = ca->block_dirty,
                     .old_mpt_dirty = ca->mpt_dirty },
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
    /* Scan slot meta for any non-zero slot belonging to this address */
    for (uint32_t i = 0; i < so->slot_meta.capacity; i++) {
        cached_slot_t *cs = &so->slot_meta.entries[i];
        if (memcmp(cs->key, addr->bytes, ADDRESS_SIZE) == 0 &&
            !uint256_is_zero(&cs->current))
            return true;
    }
    return false;
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

    /* Reset slot originals — iterate only used entries */
    for (uint32_t i = 0; i < so->next_slot_idx; i++) {
        cached_slot_t *cs = &so->slot_meta.entries[i];
        if (cs->slot_hash.bytes[0] == 0 && cs->slot_hash.bytes[1] == 0 &&
            !cs->dirty && !cs->block_dirty && !cs->mpt_dirty) continue;
        cs->original = cs->current;
        cs->dirty = false;
#ifdef ENABLE_HISTORY
        cs->block_original = cs->current;
#endif
    }

    /* Promote + reset account flags — iterate only used entries */
    for (uint32_t i = 0; i < so->next_acct_idx; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        if (ca->addr.bytes[0] == 0 && ca->addr.bytes[1] == 0 &&
            !ca->dirty && !ca->existed && !ca->created) continue;
        bool is_empty = (ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code);
        bool touched = (ca->existed || ca->created || ca->dirty || ca->code_dirty);
        if (touched && (!is_empty || !so->prune_empty))
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
            continue;
        }

        bool is_empty = (ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code);
        bool touched = (ca->existed || ca->created || ca->dirty || ca->code_dirty);
        if (touched && (!is_empty || !so->prune_empty))
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
    for (uint32_t i = 0; i < so->acct_meta.capacity; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        ca->block_dirty = false;
        ca->block_code_dirty = false;
        ca->mpt_dirty = false;
#ifdef ENABLE_HISTORY
        ca->block_self_destructed = false;
        ca->block_created = false;
#endif
    }
    for (uint32_t i = 0; i < so->slot_meta.capacity; i++) {
        cached_slot_t *cs = &so->slot_meta.entries[i];
        cs->block_dirty = false;
        cs->mpt_dirty = false;
    }
    dirty_clear(&so->dirty_accounts);
    dirty_clear(&so->dirty_slots);
}

/* =========================================================================
 * compute_mpt_root — simplified checkpoint flow
 *
 * Since sync_*_to_overlay writes to flat_store on every mutation,
 * the flat_store overlay already has current data. No full flush needed.
 * We only need to:
 *   1. Promote existed on dirty accounts
 *   2. Delete orphaned storage for dead/storage-cleared accounts
 *   3. Compute storage roots for storage_dirty accounts
 *   4. Re-sync accounts with updated storage_root
 *   5. Compute account trie root
 *   6. Clear dirty flags
 * ========================================================================= */

static uint64_t _debug_root_call = 0;
/* Set via env DEBUG_VERIFY_FROM=N to enable full recompute verification
 * starting at the Nth root computation (roughly = block number). */
static uint64_t _debug_verify_from = 0;
static bool _debug_env_checked = false;

hash_t state_overlay_compute_mpt_root(state_overlay_t *so, bool prune_empty) {
    hash_t root = hash_zero();
    if (!so) return root;

    if (!_debug_env_checked) {
        const char *env = getenv("DEBUG_VERIFY_FROM");
        if (env) _debug_verify_from = (uint64_t)atoll(env);
        _debug_env_checked = true;
    }
    _debug_root_call++;
    bool debug_verify = (_debug_verify_from > 0 && _debug_root_call >= _debug_verify_from);
    if (debug_verify && _debug_root_call == _debug_verify_from)
        fprintf(stderr, "DEBUG_VERIFY: activated at call %lu (from=%lu)\n",
                _debug_root_call, _debug_verify_from);
    if (!so->flat_state || !so->account_trie) {
        fprintf(stderr, "FATAL: compute_mpt_root without flat_state/account_trie\n");
        return root;
    }

    struct timespec t0, t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    size_t dirty_count = so->dirty_accounts.count;

    /* Step 1. Promote existed on dirty accounts.
     * commit_tx handles per-block EIP-161 decisions (using the correct
     * per-block prune_empty). Here we promote any remaining non-empty
     * dirty accounts (e.g., block reward coinbase) and handle
     * self-destructed accounts. We do NOT re-evaluate prune_empty here
     * because commit_tx already made the per-block decision. */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca || !ca->mpt_dirty) continue;
        if (ca->block_dirty || ca->block_code_dirty) {
            if (ca->self_destructed) {
                ca->existed = false;
            } else {
                bool is_empty = (ca->nonce == 0 &&
                                 uint256_is_zero(&ca->balance) && !ca->has_code);
                if (!is_empty)
                    ca->existed = true;
                /* Empty accounts: trust commit_tx's decision.
                 * If existed was set by commit_tx (pre-SD), keep it.
                 * If existed was cleared by commit_tx (post-SD prune), keep it. */
            }
        }
    }

    /* Step 2. Delete orphaned storage + dead accounts from flat_state */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca || !ca->mpt_dirty) continue;
        if (!ca->existed || ca->storage_cleared)
            flat_state_delete_all_storage(so->flat_state, ca->addr_hash.bytes);
        if (!ca->existed)
            flat_state_delete_account(so->flat_state, ca->addr_hash.bytes);
    }

    /* Step 3. Sync dirty slots to flat_state */
    struct timespec t3a, t4b, t5a;
    clock_gettime(CLOCK_MONOTONIC, &t3a);
    for (size_t d = 0; d < so->dirty_slots.count; d++) {
        const uint8_t *skey = so->dirty_slots.keys + d * SLOT_KEY_SIZE;
        cached_slot_t *cs = find_slot_meta(so, skey);
        if (!cs || !cs->mpt_dirty) continue;
        cached_account_t *ca = find_account_meta(so, skey); /* first 20 bytes = addr */
        if (!ca || !ca->existed) continue;
        if (ca->storage_cleared && !cs->mpt_dirty) continue;
        sync_slot_to_overlay(so, ca, cs);
        cs->mpt_dirty = false;
    }
    dirty_clear(&so->dirty_slots);

    /* Step 4. Compute storage roots for storage_dirty accounts */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca || !ca->storage_dirty || !ca->existed) continue;

        hash_t old_sr = ca->storage_root;
        storage_trie_root(so->storage_trie, ca->addr_hash.bytes,
                           ca->storage_root.bytes);
        if (debug_verify) {
            uint8_t verify_sr[32];
            storage_trie_invalidate_all(so->storage_trie);
            storage_trie_root(so->storage_trie, ca->addr_hash.bytes, verify_sr);
            if (memcmp(ca->storage_root.bytes, verify_sr, 32) != 0) {
                fprintf(stderr, "STORAGE HASH CACHE BUG (call %lu): addr=0x",
                        _debug_root_call);
                for (int j = 0; j < 20; j++) fprintf(stderr, "%02x", ca->addr.bytes[j]);
                fprintf(stderr, "\n  incremental: ");
                for (int j = 0; j < 32; j++) fprintf(stderr, "%02x", ca->storage_root.bytes[j]);
                fprintf(stderr, "\n  full:        ");
                for (int j = 0; j < 32; j++) fprintf(stderr, "%02x", verify_sr[j]);
                fprintf(stderr, "\n  old_sr:      ");
                for (int j = 0; j < 32; j++) fprintf(stderr, "%02x", old_sr.bytes[j]);
                fprintf(stderr, "\n");
                memcpy(ca->storage_root.bytes, verify_sr, 32);
            }
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t4b);

    /* Step 4b. DEBUG: verify storage_root of ALL dirty accounts (not just
     * storage_dirty). Catches stale storage_root loaded from flat_state. */
    if (debug_verify) {
        storage_trie_invalidate_all(so->storage_trie);
        for (size_t d = 0; d < so->dirty_accounts.count; d++) {
            const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
            cached_account_t *ca = find_account_meta(so, akey);
            if (!ca || !ca->mpt_dirty || !ca->existed) continue;
            uint8_t verify_sr[32];
            storage_trie_root(so->storage_trie, ca->addr_hash.bytes, verify_sr);
            if (memcmp(ca->storage_root.bytes, verify_sr, 32) != 0) {
                fprintf(stderr, "STALE STORAGE ROOT (call %lu): addr=0x",
                        _debug_root_call);
                for (int j = 0; j < 20; j++) fprintf(stderr, "%02x", ca->addr.bytes[j]);
                fprintf(stderr, " storage_dirty=%d\n  cached_sr: ",
                        ca->storage_dirty);
                for (int j = 0; j < 32; j++) fprintf(stderr, "%02x", ca->storage_root.bytes[j]);
                fprintf(stderr, "\n  actual_sr: ");
                for (int j = 0; j < 32; j++) fprintf(stderr, "%02x", verify_sr[j]);
                fprintf(stderr, "\n");
                /* Fix it so the root computation proceeds correctly */
                memcpy(ca->storage_root.bytes, verify_sr, 32);
            }
        }
    }

    /* Step 5. Bulk flush dirty accounts to flat_state (with final storage_root). */
    clock_gettime(CLOCK_MONOTONIC, &t5a);
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca || !ca->mpt_dirty || !ca->existed) continue;
        sync_account_to_overlay(so, ca);

        /* DEBUG: read back from flat_state and verify roundtrip */
        if (debug_verify) {
            flat_account_record_t readback;
            if (flat_state_get_account(so->flat_state, ca->addr_hash.bytes, &readback)) {
                uint8_t meta_bal[32], zero32[32] = {0};
                uint256_to_bytes(&ca->balance, meta_bal);
                const uint8_t *meta_ch = ca->has_code
                    ? ca->code_hash.bytes : (const uint8_t *)"\xc5\xd2\x46\x01\x86\xf7\x23\x3c"
                      "\x92\x7e\x7d\xb2\xdc\xc7\x03\xc0\xe5\x00\xb6\x53\xca\x82\x27\x3b"
                      "\x63\xb6\x8f\xb5\x43\x8d\xc8\x20";
                bool bal_ok = memcmp(readback.balance, meta_bal, 32) == 0;
                bool nonce_ok = readback.nonce == ca->nonce;
                bool ch_ok = memcmp(readback.code_hash, meta_ch, 32) == 0;
                bool sr_ok = memcmp(readback.storage_root, ca->storage_root.bytes, 32) == 0;
                if (!bal_ok || !nonce_ok || !ch_ok || !sr_ok) {
                    fprintf(stderr, "SYNC ROUNDTRIP MISMATCH (call %lu): addr=0x",
                            _debug_root_call);
                    for (int j = 0; j < 20; j++) fprintf(stderr, "%02x", ca->addr.bytes[j]);
                    fprintf(stderr, " bal=%d nonce=%d code_hash=%d storage_root=%d\n",
                            bal_ok, nonce_ok, ch_ok, sr_ok);
                    if (!sr_ok) {
                        fprintf(stderr, "  meta_sr: ");
                        for (int j = 0; j < 32; j++) fprintf(stderr, "%02x", ca->storage_root.bytes[j]);
                        fprintf(stderr, "\n  disk_sr: ");
                        for (int j = 0; j < 32; j++) fprintf(stderr, "%02x", readback.storage_root[j]);
                        fprintf(stderr, "\n");
                    }
                }
            }
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Step 6. Compute account trie root from flat_state */
    account_trie_root(so->account_trie, root.bytes);

    if (debug_verify) {
        hash_t verify;
        account_trie_invalidate_all(so->account_trie);
        account_trie_root(so->account_trie, verify.bytes);
        if (memcmp(root.bytes, verify.bytes, 32) != 0) {
            fprintf(stderr, "ACCT HASH CACHE BUG (call %lu): incremental != full\n",
                    _debug_root_call);
            fprintf(stderr, "  incremental: ");
            for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", root.bytes[i]);
            fprintf(stderr, "\n  full:        ");
            for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", verify.bytes[i]);
            fprintf(stderr, "\n  dirty_accounts=%zu dirty_slots=%zu prune_empty=%d\n",
                    dirty_count, so->dirty_slots.count, prune_empty);
            for (size_t d = 0; d < so->dirty_accounts.count; d++) {
                const uint8_t *ak = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
                cached_account_t *cx = find_account_meta(so, ak);
                if (!cx) continue;
                fprintf(stderr, "  dirty[%zu]: 0x", d);
                for (int j = 0; j < 20; j++) fprintf(stderr, "%02x", cx->addr.bytes[j]);
                fprintf(stderr, " existed=%d mpt_dirty=%d block_dirty=%d "
                        "nonce=%lu empty=%d\n",
                        cx->existed, cx->mpt_dirty, cx->block_dirty,
                        cx->nonce,
                        (cx->nonce == 0 && uint256_is_zero(&cx->balance) && !cx->has_code));
            }
            root = verify;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t2);

    /* Step 7. Clear dirty flags and reset dirty vectors */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca) continue;
        ca->mpt_dirty = false;
        ca->block_dirty = false;
        ca->block_code_dirty = false;
        ca->storage_dirty = false;
        ca->storage_cleared = false;
    }
    dirty_clear(&so->dirty_accounts);

    so->last_root_stor_ms = (t4b.tv_sec - t3a.tv_sec) * 1000.0 +
                            (t4b.tv_nsec - t3a.tv_nsec) / 1e6;
    so->last_root_acct_ms = (t2.tv_sec - t5a.tv_sec) * 1000.0 +
                            (t2.tv_nsec - t5a.tv_nsec) / 1e6;
    so->last_root_dirty_count = dirty_count;
    return root;
}

/* =========================================================================
 * Evict — flush overlay to disk, free meta arrays
 * ========================================================================= */

void state_overlay_evict(state_overlay_t *so) {
    if (!so) return;

    /* DEBUG: save pre-evict root for comparison */
    bool debug_evict = (_debug_verify_from > 0 && _debug_root_call >= _debug_verify_from);
    hash_t pre_evict_root = hash_zero();
    if (debug_evict && so->account_trie) {
        account_trie_invalidate_all(so->account_trie);
        account_trie_root(so->account_trie, pre_evict_root.bytes);
    }

    if (so->flat_state) {
        flat_store_t *astore = flat_state_account_store(so->flat_state);
        flat_store_t *sstore = flat_state_storage_store(so->flat_state);

        /* Pass 1: flush dirty overlay entries to disk */
        flat_store_stale_slot_t *acct_stale = NULL, *stor_stale = NULL;
        size_t acct_sc = 0, stor_sc = 0;
        flat_store_flush_deferred(astore, &acct_stale, &acct_sc);
        flat_store_flush_deferred(sstore, &stor_stale, &stor_sc);

        /* Pass 2: evict clean overlay entries */
        flat_store_evict_clean(astore);
        flat_store_evict_clean(sstore);

        /* Pass 3: free stale disk slots */
        flat_store_free_stale_slots(astore, acct_stale, acct_sc);
        flat_store_free_stale_slots(sstore, stor_stale, stor_sc);
    }

    if (debug_evict && so->account_trie) {
        hash_t post_flush_root;
        account_trie_invalidate_all(so->account_trie);
        account_trie_root(so->account_trie, post_flush_root.bytes);
        if (memcmp(pre_evict_root.bytes, post_flush_root.bytes, 32) != 0) {
            fprintf(stderr, "EVICT BUG (call %lu): root changed after flush+evict!\n",
                    _debug_root_call);
            fprintf(stderr, "  before: ");
            for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", pre_evict_root.bytes[i]);
            fprintf(stderr, "\n  after:  ");
            for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", post_flush_root.bytes[i]);
            fprintf(stderr, "\n");
        }
    }

    /* Free code pointers in used portion of meta */
    for (uint32_t i = 0; i < so->next_acct_idx; i++) {
        if (so->acct_meta.entries[i].code) {
            free(so->acct_meta.entries[i].code);
            so->acct_meta.entries[i].code = NULL;
        }
    }

    /* Reset and shrink meta arrays back to minimum */
    {
        const uint32_t MIN_META_CAP = 4096;
        if (so->acct_meta.capacity > MIN_META_CAP) {
            free(so->acct_meta.entries);
            so->acct_meta.entries = calloc(MIN_META_CAP, sizeof(cached_account_t));
            so->acct_meta.capacity = MIN_META_CAP;
        } else {
            memset(so->acct_meta.entries, 0,
                   so->acct_meta.capacity * sizeof(cached_account_t));
        }
        if (so->slot_meta.capacity > MIN_META_CAP) {
            free(so->slot_meta.entries);
            so->slot_meta.entries = calloc(MIN_META_CAP, sizeof(cached_slot_t));
            so->slot_meta.capacity = MIN_META_CAP;
        } else {
            memset(so->slot_meta.entries, 0,
                   so->slot_meta.capacity * sizeof(cached_slot_t));
        }
    }

    /* Reset sequential counters (used when no flat_state) */
    so->next_acct_idx = 0;
    so->next_slot_idx = 0;

    /* Rebuild index tables — old entries pointed at now-stale meta slots */
    mem_art_destroy(&so->acct_index);
    mem_art_init(&so->acct_index);
    mem_art_destroy(&so->slot_index);
    mem_art_init(&so->slot_index);

    dirty_clear(&so->dirty_accounts);
    dirty_clear(&so->dirty_slots);

    so->journal_len = 0;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

state_overlay_stats_t state_overlay_get_stats(const state_overlay_t *so) {
    state_overlay_stats_t s = {0};
    if (!so) return s;
    s.overlay_accounts = so->acct_meta.capacity; /* approximate */
    s.overlay_slots = so->slot_meta.capacity;
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

/* =========================================================================
 * Collect — enumerate cached accounts/slots for prestate dump
 * ========================================================================= */

size_t state_overlay_collect_addresses(state_overlay_t *so,
                                        address_t *out, size_t max) {
    if (!so || !out || max == 0) return 0;
    size_t n = 0;
    for (uint32_t i = 0; i < so->next_acct_idx && n < max; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        if (ca->existed || ca->dirty || ca->block_dirty || ca->created)
            out[n++] = ca->addr;
    }
    return n;
}

size_t state_overlay_collect_storage_keys(state_overlay_t *so,
                                           const address_t *addr,
                                           uint256_t *out, size_t max) {
    if (!so || !addr || !out || max == 0) return 0;
    size_t n = 0;
    for (uint32_t i = 0; i < so->next_slot_idx && n < max; i++) {
        cached_slot_t *cs = &so->slot_meta.entries[i];
        if (memcmp(cs->key, addr->bytes, ADDRESS_SIZE) != 0) continue;
        if (uint256_is_zero(&cs->current) && uint256_is_zero(&cs->original)) continue;
        out[n++] = uint256_from_bytes(cs->key + ADDRESS_SIZE, 32);
    }
    return n;
}

#ifdef ENABLE_HISTORY
/* =========================================================================
 * Debug dump: write pre_alloc.json and post_alloc.json for all cached accounts.
 * pre_alloc uses original_* fields (start-of-block values).
 * post_alloc uses current values.
 * ========================================================================= */

static void dump_alloc_file(state_overlay_t *so, const char *path, bool use_originals) {
    FILE *f = fopen(path, "w");
    if (!f) return;
    fprintf(f, "{\n");
    bool first = true;
    for (uint32_t i = 0; i < so->next_acct_idx; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];

        uint64_t nonce = use_originals ? ca->original_nonce : ca->nonce;
        uint256_t bal  = use_originals ? ca->original_balance : ca->balance;

        if (!first) fprintf(f, ",\n");
        first = false;
        fprintf(f, "  \"0x");
        for (int j = 0; j < 20; j++) fprintf(f, "%02x", ca->addr.bytes[j]);
        fprintf(f, "\": {\n");

        uint8_t bb[32]; uint256_to_bytes(&bal, bb);
        fprintf(f, "    \"balance\": \"0x");
        int s = 0; while (s < 31 && bb[s] == 0) s++;
        for (int j = s; j < 32; j++) fprintf(f, "%02x", bb[j]);
        fprintf(f, "\",\n");
        fprintf(f, "    \"nonce\": \"0x%lx\"", nonce);

        if (ca->has_code) {
            if (ca->code && ca->code_size > 0) {
                fprintf(f, ",\n    \"code\": \"0x");
                for (uint32_t c = 0; c < ca->code_size; c++) fprintf(f, "%02x", ca->code[c]);
                fprintf(f, "\"");
            } else {
                /* Code not loaded — indicate via code_hash */
                fprintf(f, ",\n    \"_codeHash\": \"0x");
                for (int j = 0; j < 32; j++) fprintf(f, "%02x", ca->code_hash.bytes[j]);
                fprintf(f, "\"");
            }
        }

        /* Storage */
        bool has_slots = false;
        for (uint32_t si = 0; si < so->next_slot_idx; si++) {
            cached_slot_t *cs = &so->slot_meta.entries[si];
            if (memcmp(cs->key, ca->addr.bytes, ADDRESS_SIZE) != 0) continue;
            uint256_t val = use_originals ? cs->block_original : cs->current;
            if (uint256_is_zero(&val)) continue;
            if (!has_slots) { fprintf(f, ",\n    \"storage\": {"); has_slots = true; }
            else fprintf(f, ",");
            uint8_t kb[32], vb[32];
            memcpy(kb, cs->key + ADDRESS_SIZE, 32);
            uint256_to_bytes(&val, vb);
            fprintf(f, "\n      \"0x");
            for (int j = 0; j < 32; j++) fprintf(f, "%02x", kb[j]);
            fprintf(f, "\": \"0x");
            for (int j = 0; j < 32; j++) fprintf(f, "%02x", vb[j]);
            fprintf(f, "\"");
        }
        if (has_slots) fprintf(f, "\n    }");

        /* Debug flags (post_alloc only — pre_alloc must be clean for geth t8n) */
        if (!use_originals) {
            fprintf(f, ",\n    \"_flags\": \"existed=%d dirty=%d block_dirty=%d "
                    "mpt_dirty=%d created=%d sd=%d empty=%d\"",
                    ca->existed, ca->dirty, ca->block_dirty,
                    ca->mpt_dirty, ca->created, ca->self_destructed,
                    (ca->nonce == 0 && uint256_is_zero(&ca->balance) && !ca->has_code));
        }

        fprintf(f, "\n  }");
    }
    fprintf(f, "\n}\n");
    fclose(f);
}

void state_overlay_dump_debug(state_overlay_t *so, const char *dir) {
    if (!so || !dir) return;
    char pre_path[512], post_path[512];
    snprintf(pre_path, sizeof(pre_path), "%s/pre_alloc.json", dir);
    snprintf(post_path, sizeof(post_path), "%s/post_alloc.json", dir);
    dump_alloc_file(so, pre_path, true);
    dump_alloc_file(so, post_path, false);
    fprintf(stderr, "DEBUG: dumped pre/post alloc to %s/\n", dir);
}
#endif /* ENABLE_HISTORY */
