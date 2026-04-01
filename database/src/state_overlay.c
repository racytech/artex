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
/* storage_trie.h no longer needed — per-account art computes storage roots */
#include "account_trie.h"
#include "art_mpt.h"
#include "arena.h"
#include "storage_file.h"
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

/* Per-account storage art pool sizes (virtual only, demand-paged) */
#define ACCT_STOR_NODE_RESERVE  (4ULL * 1024 * 1024)   /* 4 MB */
#define ACCT_STOR_LEAF_RESERVE  (8ULL * 1024 * 1024)   /* 8 MB */
#define ACCT_STOR_KEY_SIZE      32                       /* slot_hash */
#define ACCT_STOR_VAL_SIZE      32                       /* slot value BE */

/* EIP-161 RIPEMD special case: address 0x0000...0003.
 * In geth, touching RIPEMD adds an extra dirty counter so that a journal
 * revert does not remove it from the dirty set. This ensures that if
 * RIPEMD is touched by a failing CALL (OOG), it still gets pruned as an
 * empty account under EIP-161.  See go-ethereum journal.go:touchChange. */
static const uint8_t RIPEMD_ADDR[20] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3
};

/* Storage file index entry — stored in stor_index mem_art */
typedef struct __attribute__((packed)) {
    uint64_t offset;
    uint32_t count;
} stor_index_entry_t;

/* Types from state_meta.h: cached_account_t, account_meta_pool_t */

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
 * Per-account storage art — lifecycle helpers
 * ========================================================================= */

/** Encode callback for per-account storage art_mpt.
 *  Leaf value = raw 32-byte slot value (big-endian).
 *  Returns value bytes as RLP (raw, same as test vectors). */
static uint32_t acct_stor_encode(const uint8_t *key, const void *leaf_val,
                                  uint32_t val_size, uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    const uint8_t *val = (const uint8_t *)leaf_val;
    /* Skip leading zeros */
    int start = 0;
    while (start < 31 && val[start] == 0) start++;
    int len = 32 - start;

    if (len == 1 && val[start] < 0x80) {
        /* Single byte < 0x80: RLP is the byte itself */
        rlp_out[0] = val[start];
        return 1;
    }
    /* Short string: 0x80+len, then bytes */
    rlp_out[0] = (uint8_t)(0x80 + len);
    memcpy(rlp_out + 1, val + start, len);
    return 1 + len;
}

/* acct_stor_create/destroy defined after struct state_overlay */
static void acct_stor_destroy(cached_account_t *ca) {
    if (ca->storage_mpt) { art_mpt_destroy(ca->storage_mpt); ca->storage_mpt = NULL; }
    if (ca->storage_art) {
        compact_art_destroy(ca->storage_art); /* doesn't munmap — arena owned */
        free(ca->storage_art);
        ca->storage_art = NULL;
    }
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

/* slot_meta_pool_t no longer used — per-account storage_art is the single store */

/* =========================================================================
 * Main struct
 * ========================================================================= */

/* Storage arena: 8 GB virtual for all per-account compact_arts.
 * Physical = demand-paged, proportional to actual storage loaded. */
#define STORAGE_ARENA_RESERVE (8ULL * 1024 * 1024 * 1024)

struct state_overlay {
    flat_state_t     *flat_state;
    code_store_t     *code_store;
    account_trie_t   *account_trie;
    arena_t           storage_arena;  /* shared arena for per-account storage arts */
    storage_file_t   *storage_file;  /* packed per-account storage persistence */
    mem_art_t         stor_index;    /* addr_hash[32] → {offset(8), count(4)} */

    /* Meta arrays — indexed by sequential meta index (NOT flat_store overlay index).
     * Meta holds typed access + flags. Persistent records in flat_store overlay
     * are synced at checkpoint time. */
    account_meta_pool_t acct_meta;

    /* Lookup table: addr → meta index */
    mem_art_t acct_index;   /* addr[20] → uint32_t meta index */

    journal_entry_t *journal;
    uint32_t journal_len;
    uint32_t journal_cap;

    /* Ephemeral structures — not persisted, per-tx/per-block only */
    mem_art_t warm_addrs;   /* EIP-2929 */
    mem_art_t warm_slots;
    mem_art_t transient;    /* EIP-1153 */
    mem_art_t originals;    /* EIP-2200: skey[52] → uint256_t (per-tx original values) */

    dirty_vec_t tx_dirty_accounts;
    dirty_vec_t dirty_accounts;

    uint32_t next_acct_idx;

    bool prune_empty;
    bool batch_mode;

    double last_root_stor_ms;
    double last_root_acct_ms;
    size_t last_root_dirty_count;

    uint64_t flat_acct_hit, flat_acct_miss;
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

static inline void mark_account_tx_dirty(state_overlay_t *so, cached_account_t *ca) {
    if (!ca->dirty)
        dirty_push(&so->tx_dirty_accounts, ca->addr.bytes, ADDRESS_KEY_SIZE);
}

/* Per-account storage art creation (needs full struct definition) */
static bool acct_stor_create(state_overlay_t *so, cached_account_t *ca) {
    if (ca->storage_art) return true; /* already exists */
    ca->storage_art = calloc(1, sizeof(compact_art_t));
    if (!ca->storage_art) return false;

    void *nmem = arena_alloc(&so->storage_arena, ACCT_STOR_NODE_RESERVE);
    void *lmem = arena_alloc(&so->storage_arena, ACCT_STOR_LEAF_RESERVE);
    if (!nmem || !lmem) {
        free(ca->storage_art);
        ca->storage_art = NULL;
        return false;
    }
    if (!compact_art_init_arena(ca->storage_art,
                                 ACCT_STOR_KEY_SIZE, ACCT_STOR_VAL_SIZE,
                                 false, NULL, NULL,
                                 nmem, ACCT_STOR_NODE_RESERVE,
                                 lmem, ACCT_STOR_LEAF_RESERVE)) {
        free(ca->storage_art);
        ca->storage_art = NULL;
        return false;
    }
    ca->storage_mpt = art_mpt_create(ca->storage_art, acct_stor_encode, NULL);
    if (!ca->storage_mpt) {
        compact_art_destroy(ca->storage_art);
        free(ca->storage_art);
        ca->storage_art = NULL;
        return false;
    }
    return true;
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

/* find_slot_meta removed — storage accessed via per-account art */

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

/* sync_slot_to_overlay removed — per-account art is the single store */

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
 * Storage read helper — reads slot value from per-account art or flat_state.
 * Creates per-account art and populates from flat_state on first access.
 * ========================================================================= */

/** Bulk-load all storage from packed storage_file into per-account art. */
static void storage_bulk_load(state_overlay_t *so, cached_account_t *ca) {
    if (ca->storage_art) return; /* already loaded */
    if (!so->storage_file) return;

    /* Look up storage_file refs from the separate index */
    const stor_index_entry_t *entry = (const stor_index_entry_t *)
        mem_art_get(&so->stor_index, ca->addr_hash.bytes, 32, NULL);
    if (!entry || entry->count == 0) return;

    uint32_t count = entry->count;
    uint8_t *buf = malloc((size_t)count * STORAGE_SLOT_SIZE);
    if (!buf) return;

    if (!storage_file_read_section(so->storage_file, entry->offset,
                                    count, buf)) {
        free(buf);
        return;
    }

    acct_stor_create(so, ca);
    if (!ca->storage_art) { free(buf); return; }

    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *slot_hash = buf + i * STORAGE_SLOT_SIZE;
        const uint8_t *val_be    = buf + i * STORAGE_SLOT_SIZE + 32;
        compact_art_insert(ca->storage_art, slot_hash, val_be);
    }
    free(buf);
}

static uint256_t storage_read(state_overlay_t *so, cached_account_t *ca,
                               const uint8_t slot_hash[32]) {
    /* Ensure per-account art is populated (bulk-load from storage_file if needed) */
    if (!ca->storage_art)
        storage_bulk_load(so, ca);

    /* Check per-account art */
    if (ca->storage_art) {
        const void *leaf = compact_art_get(ca->storage_art, slot_hash);
        if (leaf) return uint256_from_bytes((const uint8_t *)leaf, 32);
    }

    /* Not in per-account art or storage_file — slot is zero */
    return UINT256_ZERO;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

static void init_tries(state_overlay_t *so) {
    if (!so->flat_state) return;
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
    mem_art_init(&so->stor_index);
    mem_art_init(&so->warm_addrs);
    mem_art_init(&so->warm_slots);
    mem_art_init(&so->transient);
    mem_art_init(&so->originals);

    so->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!so->journal) { free(so); return NULL; }
    so->journal_cap = JOURNAL_INIT_CAP;

    if (!arena_init(&so->storage_arena, STORAGE_ARENA_RESERVE)) {
        free(so->journal); free(so); return NULL;
    }

    init_tries(so);
    return so;
}

void state_overlay_set_storage_path(state_overlay_t *so, const char *path) {
    if (!so || !path) return;
    if (so->storage_file) storage_file_destroy(so->storage_file);
    so->storage_file = storage_file_create(path);
}

void state_overlay_destroy(state_overlay_t *so) {
    if (!so) return;

    /* Free code pointers and per-account storage arts in account meta */
    for (uint32_t i = 0; i < so->acct_meta.capacity; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        free(ca->code);
        acct_stor_destroy(ca);
    }
    free(so->acct_meta.entries);
    arena_destroy(&so->storage_arena);
    if (so->storage_file) storage_file_destroy(so->storage_file);
    if (so->account_trie) account_trie_destroy(so->account_trie);

    mem_art_destroy(&so->acct_index);
    mem_art_destroy(&so->stor_index);
    mem_art_destroy(&so->warm_addrs);
    mem_art_destroy(&so->warm_slots);
    mem_art_destroy(&so->transient);
    mem_art_destroy(&so->originals);

    for (uint32_t i = 0; i < so->journal_len; i++) {
        if (so->journal[i].type == JOURNAL_CODE)
            free(so->journal[i].data.code.old_code);
        else if (so->journal[i].type == JOURNAL_ACCOUNT_CREATE)
            free(so->journal[i].data.create.old_code);
    }
    free(so->journal);

    dirty_free(&so->tx_dirty_accounts);
    dirty_free(&so->dirty_accounts);

    free(so);
}

void state_overlay_set_flat_state(state_overlay_t *so, flat_state_t *fs) {
    if (!so) return;
    so->flat_state = fs;
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
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return UINT256_ZERO_INIT;
    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = hash_keccak256(slot_be, 32);
    return storage_read(so, ca, slot_hash.bytes);
}

uint256_t state_overlay_get_committed_storage(state_overlay_t *so, const address_t *addr,
                                               const uint256_t *key) {
    if (!so || !addr || !key) return UINT256_ZERO_INIT;
    /* Check originals map (has tx-start value if slot was modified this tx) */
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    const uint256_t *orig = (const uint256_t *)mem_art_get(
        &so->originals, skey, SLOT_KEY_SIZE, NULL);
    if (orig) return *orig;
    /* Not modified this tx — read current from art (= original) */
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return UINT256_ZERO_INIT;
    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = hash_keccak256(slot_be, 32);
    return storage_read(so, ca, slot_hash.bytes);
}

void state_overlay_set_storage(state_overlay_t *so, const address_t *addr,
                                const uint256_t *key, const uint256_t *value) {
    if (!so || !addr || !key || !value) return;
    cached_account_t *ca = ensure_account(so, addr);
    if (!ca) return;

    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = hash_keccak256(slot_be, 32);

    /* Read current value for journal + originals */
    uint256_t old_value = storage_read(so, ca, slot_hash.bytes);

    journal_entry_t je = {
        .type = JOURNAL_STORAGE,
        .addr = *addr,
        .data.storage = {
            .slot = *key,
            .old_value = old_value,
            .old_acct_storage_dirty = ca->storage_dirty,
            .old_acct_mpt_dirty = ca->mpt_dirty,
        }
    };
    journal_push(so, &je);

    /* Save original value for EIP-2200 (first write to this slot this tx) */
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    if (!mem_art_contains(&so->originals, skey, SLOT_KEY_SIZE))
        mem_art_insert(&so->originals, skey, SLOT_KEY_SIZE,
                       &old_value, sizeof(uint256_t));

    /* Write to per-account storage art */
    if (!ca->storage_art) acct_stor_create(so, ca);
    if (ca->storage_art) {
        uint8_t val_be[32];
        uint256_to_bytes(value, val_be);
        if (uint256_is_zero(value))
            compact_art_delete(ca->storage_art, slot_hash.bytes);
        else
            compact_art_insert(ca->storage_art, slot_hash.bytes, val_be);
    }

    ca->storage_dirty = true;
    mark_account_mpt_dirty(so, ca);
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
            cached_account_t *ca = find_account_meta(so, je->addr.bytes);
            if (ca && ca->storage_art) {
                uint8_t slot_be[32];
                uint256_to_bytes(&je->data.storage.slot, slot_be);
                hash_t slot_hash = hash_keccak256(slot_be, 32);
                if (uint256_is_zero(&je->data.storage.old_value))
                    compact_art_delete(ca->storage_art, slot_hash.bytes);
                else {
                    uint8_t val_be[32];
                    uint256_to_bytes(&je->data.storage.old_value, val_be);
                    compact_art_insert(ca->storage_art, slot_hash.bytes, val_be);
                }
            }
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
    if (was_warm) {
        uint8_t skey[SLOT_KEY_SIZE];
        make_slot_key(addr, key, skey);
        *was_warm = mem_art_contains(&so->warm_slots, skey, SLOT_KEY_SIZE);
    }
    return state_overlay_get_storage(so, addr, key);
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
    if (current)  *current  = state_overlay_get_storage(so, addr, key);
    if (original) *original = state_overlay_get_committed_storage(so, addr, key);
    if (was_warm) {
        uint8_t skey[SLOT_KEY_SIZE];
        make_slot_key(addr, key, skey);
        *was_warm = mem_art_contains(&so->warm_slots, skey, SLOT_KEY_SIZE);
    }
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
    acct_stor_destroy(ca);  /* destroy old storage, will be recreated if needed */
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
    if (!ca) return false;
    if (memcmp(ca->storage_root.bytes, HASH_EMPTY_STORAGE.bytes, 32) != 0)
        return true;
    if (ca->storage_art && compact_art_size(ca->storage_art) > 0)
        return true;
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

    /* Clear originals — new block starts with current values as committed */
    mem_art_destroy(&so->originals);
    mem_art_init(&so->originals);
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
            acct_stor_destroy(ca);
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

    /* Storage for self-destructed accounts already destroyed by acct_stor_destroy above */
    free(sd_addrs);
    dirty_clear(&so->tx_dirty_accounts);
    so->journal_len = 0;

    mem_art_destroy(&so->warm_addrs);  mem_art_init(&so->warm_addrs);
    mem_art_destroy(&so->warm_slots);  mem_art_init(&so->warm_slots);
    mem_art_destroy(&so->transient);   mem_art_init(&so->transient);
    mem_art_destroy(&so->originals);   mem_art_init(&so->originals);
}

/* =========================================================================
 * Clear prestate dirty (for test_runner pre-state setup)
 * ========================================================================= */

void state_overlay_clear_prestate_dirty(state_overlay_t *so) {
    if (!so) return;
    for (uint32_t i = 0; i < so->acct_meta.capacity; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        ca->block_dirty = false;
        ca->block_code_dirty = false;
        ca->mpt_dirty = false;
        ca->storage_dirty = false;
#ifdef ENABLE_HISTORY
        ca->block_self_destructed = false;
        ca->block_created = false;
#endif
    }
    dirty_clear(&so->dirty_accounts);
    /* Clear originals — pre-state setup set_storage calls populated it
     * with pre-pre-state values (zeros). Must clear so execution sees
     * the actual pre-state values as "committed". */
    mem_art_destroy(&so->originals);
    mem_art_init(&so->originals);
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

    /* Step 2. Delete dead accounts from flat_state (storage handled by per-account art) */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca || !ca->mpt_dirty) continue;
        if (!ca->existed)
            flat_state_delete_account(so->flat_state, ca->addr_hash.bytes);
    }

    /* Step 3. (removed — per-account art is the single store, no flat_store sync) */
    struct timespec t3a, t4b, t5a;
    clock_gettime(CLOCK_MONOTONIC, &t3a);

    /* Step 4. Compute storage roots for storage_dirty accounts.
     * Uses per-account storage art when available (Phase 5),
     * falls back to shared storage trie for accounts without one. */
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca || !ca->storage_dirty || !ca->existed) continue;

        if (ca->storage_mpt) {
            art_mpt_root_hash(ca->storage_mpt, ca->storage_root.bytes);
        }
        /* No fallback needed: set_storage always creates storage_mpt */
    }
    clock_gettime(CLOCK_MONOTONIC, &t4b);

    /* Step 4c. Write dirty accounts' storage to storage_file and update
     * the stor_index (separate from the account record). */
    if (so->storage_file) {
        for (size_t d = 0; d < so->dirty_accounts.count; d++) {
            const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
            cached_account_t *ca = find_account_meta(so, akey);
            if (!ca || !ca->storage_dirty || !ca->storage_art) continue;
            uint32_t count = (uint32_t)compact_art_size(ca->storage_art);
            if (count == 0) {
                mem_art_delete(&so->stor_index, ca->addr_hash.bytes, 32);
                continue;
            }
            uint8_t *buf = malloc((size_t)count * STORAGE_SLOT_SIZE);
            if (!buf) continue;
            uint32_t idx = 0;
            compact_art_iterator_t *it = compact_art_iterator_create(ca->storage_art);
            while (compact_art_iterator_next(it) && idx < count) {
                memcpy(buf + idx * STORAGE_SLOT_SIZE,
                       compact_art_iterator_key(it), 32);
                memcpy(buf + idx * STORAGE_SLOT_SIZE + 32,
                       (const uint8_t *)compact_art_iterator_value(it), 32);
                idx++;
            }
            compact_art_iterator_destroy(it);
            stor_index_entry_t sie = {
                .offset = storage_file_write_section(so->storage_file, buf, idx),
                .count = idx,
            };
            mem_art_upsert(&so->stor_index, ca->addr_hash.bytes, 32,
                           &sie, sizeof(sie));
            free(buf);
        }
    }

    /* Step 5. Bulk flush dirty accounts to flat_state (with final storage_root). */
    clock_gettime(CLOCK_MONOTONIC, &t5a);
    for (size_t d = 0; d < so->dirty_accounts.count; d++) {
        const uint8_t *akey = so->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = find_account_meta(so, akey);
        if (!ca || !ca->mpt_dirty || !ca->existed) continue;
        sync_account_to_overlay(so, ca);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Step 6. Compute account trie root from flat_state */
    account_trie_root(so->account_trie, root.bytes);
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

    if (so->flat_state) {
        flat_store_t *astore = flat_state_account_store(so->flat_state);

        /* Flush account overlay to disk (storage handled by storage_file) */
        flat_store_stale_slot_t *acct_stale = NULL;
        size_t acct_sc = 0;
        flat_store_flush_deferred(astore, &acct_stale, &acct_sc);
        flat_store_evict_clean(astore);
        flat_store_free_stale_slots(astore, acct_stale, acct_sc);
    }

    /* Free code pointers (storage arts kept alive for hash cache) */
    for (uint32_t i = 0; i < so->next_acct_idx; i++) {
        cached_account_t *ca = &so->acct_meta.entries[i];
        if (ca->code) { free(ca->code); ca->code = NULL; }
        /* Don't destroy storage arts — keep hash cache warm */
    }
    /* Don't reset arena — storage arts still point into it */

    /* Don't reset acct_meta or acct_index — accounts persist across windows.
     * Only clear dirty flags and journal. */
    dirty_clear(&so->dirty_accounts);
    so->journal_len = 0;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

state_overlay_stats_t state_overlay_get_stats(const state_overlay_t *so) {
    state_overlay_stats_t s = {0};
    if (!so) return s;
    s.overlay_accounts = so->acct_meta.capacity; /* approximate */
    s.overlay_slots = 0; /* slot_meta removed — per-account art */
    if (so->flat_state) {
        s.flat_acct_count = flat_state_account_count(so->flat_state);
        s.flat_stor_count = 0; /* storage managed by per-account art + storage_file */
        s.flat_acct_mem = compact_art_memory_usage(flat_state_account_art(so->flat_state));
        s.flat_stor_mem = 0;
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
    /* Per-account art stores slot_hash (keccak), not original slot keys.
     * Cannot reverse the hash. Storage key collection not supported
     * with per-account art — debug dump uses originals map instead. */
    (void)so; (void)addr; (void)out; (void)max;
    return 0;
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

        /* Storage: per-account art stores slot_hash, can't dump raw slot keys */
        if (ca->storage_art && compact_art_size(ca->storage_art) > 0 && !use_originals)
            fprintf(f, ",\n    \"_storage_slots\": %zu",
                    compact_art_size(ca->storage_art));

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
