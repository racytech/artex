#include "evm_state.h"
#include "mem_art.h"
#include "keccak256.h"
#ifdef ENABLE_MPT
#include "mpt_arena.h"
#include "code_store.h"
#include "flat_state.h"
#include "storage_trie.h"
#include "flat_store.h"
#endif
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#ifdef ENABLE_DEBUG
extern bool g_trace_calls __attribute__((weak));
#else
static const bool g_trace_calls __attribute__((unused)) = false;
#endif

// ============================================================================
// Internal constants
// ============================================================================

#define JOURNAL_INIT_CAP   256
#define SLOT_KEY_SIZE      52   // addr[20] + slot[32]
#define MAX_CODE_SIZE      (24 * 1024 + 1)  // EIP-170: 24576 bytes

// Forward declarations for RLP helpers (defined later, needed by compaction)
static size_t rlp_byte_hdr(const uint8_t *buf, size_t buf_len, size_t *data_len);
static size_t rlp_list_hdr(const uint8_t *buf, size_t buf_len, size_t *payload_len);

// ============================================================================
// Internal types
// ============================================================================

// --- Account cache ---

typedef struct cached_account {
    /* --- Cache line 1: hot fields (accessed every tx) --- */
    uint64_t   nonce;
    uint256_t  balance;         // 32 bytes
    bool dirty;
    bool block_dirty;           // survives commit_tx, cleared at block end
    bool existed;               // existed in backing store before
    bool mpt_dirty;             // needs update in account_mpt
    bool storage_dirty;         // any storage slot changed (for mpt_store)
    bool has_code;
    bool created;               // newly created this execution
    bool self_destructed;
    bool code_dirty;
    bool block_code_dirty;      // same for code
    uint8_t   *code;            // loaded bytecode (NULL until needed)
    uint32_t   code_size;
    /* --- Cache line 2+: cold fields --- */
    address_t  addr;            // key (20 bytes) — kept for finalize/revert
    hash_t     code_hash;       // 32 bytes
    hash_t     storage_root;    // cached storage root (avoids recomputation for clean accounts)
    hash_t     addr_hash;       // cached keccak256(addr) — computed once on first load
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    /* Pre-block snapshots for state history diffs */
    uint64_t   original_nonce;
    uint256_t  original_balance;
    hash_t     original_code_hash;
    /* Block-level flags that survive commit_tx for diff capture */
    bool       block_self_destructed;
    bool       block_created;
    bool       block_accessed;      // per-block touch flag, cleared by commit()
#endif
} cached_account_t;

// --- Storage cache ---

typedef struct cached_slot {
    uint8_t   key[SLOT_KEY_SIZE];   // addr[20] || slot_be[32] — kept for finalize
    uint256_t original;             // value when first loaded (reset per-tx by commit_tx)
    uint256_t current;
    bool dirty;
    bool block_dirty;           // survives commit_tx, cleared at block end
    bool mpt_dirty;             // needs update in storage mpt_store (cleared after storage root compute)
    hash_t slot_hash;           // cached keccak256(slot_be) — computed once on first load
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    uint256_t block_original;   // value at start of block (for state diff tracking)
#endif
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
        struct { uint64_t val; bool dirty; bool block_dirty; } nonce;
        struct { uint256_t val; bool dirty; bool block_dirty; } balance;
        struct { hash_t old_hash; bool old_has_code; uint8_t *old_code; uint32_t old_code_size; } code;
        struct { uint256_t slot; uint256_t old_value; bool old_mpt_dirty; } storage;
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
            bool       old_block_dirty;
            bool       old_block_code_dirty;
            bool       old_created;
            bool       old_existed;
            bool       old_self_destructed;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
            bool       old_block_created;
#endif
            hash_t     old_storage_root;
            bool       old_storage_dirty;
        } create;
    } data;
} journal_entry_t;

// --- Dirty tracking lists (avoid full cache scan at compute_mpt_root time) ---
// Store keys (not pointers) because mem_art uses a realloc'd arena — pointers
// become dangling when the arena grows.

typedef struct {
    uint8_t  *keys;       // packed addr[20] keys
    size_t    count;
    size_t    cap;
} dirty_account_vec_t;

typedef struct {
    uint8_t  *keys;       // packed skey[52] keys (addr[20] || slot[32])
    size_t    count;
    size_t    cap;
} dirty_slot_vec_t;

#define DIRTY_VEC_INIT_CAP 64
#define ADDRESS_KEY_SIZE 20

static inline void dirty_account_push(dirty_account_vec_t *v, const uint8_t *addr) {
    if (v->count >= v->cap) {
        size_t nc = v->cap ? v->cap * 2 : DIRTY_VEC_INIT_CAP;
        uint8_t *nk = realloc(v->keys, nc * ADDRESS_KEY_SIZE);
        if (!nk) return;
        v->keys = nk; v->cap = nc;
    }
    memcpy(v->keys + v->count * ADDRESS_KEY_SIZE, addr, ADDRESS_KEY_SIZE);
    v->count++;
}

static inline void dirty_slot_push(dirty_slot_vec_t *v, const uint8_t *skey) {
    if (v->count >= v->cap) {
        size_t nc = v->cap ? v->cap * 2 : DIRTY_VEC_INIT_CAP;
        uint8_t *nk = realloc(v->keys, nc * SLOT_KEY_SIZE);
        if (!nk) return;
        v->keys = nk; v->cap = nc;
    }
    memcpy(v->keys + v->count * SLOT_KEY_SIZE, skey, SLOT_KEY_SIZE);
    v->count++;
}

static inline void dirty_account_clear(dirty_account_vec_t *v) {
    v->count = 0;  // keep allocation for reuse
}

static inline void dirty_slot_clear(dirty_slot_vec_t *v) {
    v->count = 0;
}

static inline void dirty_account_free(dirty_account_vec_t *v) {
    free(v->keys);
    v->keys = NULL;
    v->count = v->cap = 0;
}

static inline void dirty_slot_free(dirty_slot_vec_t *v) {
    free(v->keys);
    v->keys = NULL;
    v->count = v->cap = 0;
}

// --- Per-tx dirty tracking (avoids full cache scan in commit_tx) ---

typedef struct {
    uint8_t  *keys;       // packed addr[20] keys
    size_t    count;
    size_t    cap;
} tx_dirty_addr_vec_t;

typedef struct {
    uint8_t  *keys;       // packed skey[52] keys (addr[20] || slot[32])
    size_t    count;
    size_t    cap;
} tx_dirty_slot_vec_t;

static inline void tx_dirty_addr_push(tx_dirty_addr_vec_t *v, const uint8_t *addr) {
    if (v->count >= v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 32;
        uint8_t *nk = realloc(v->keys, nc * ADDRESS_KEY_SIZE);
        if (!nk) return;
        v->keys = nk; v->cap = nc;
    }
    memcpy(v->keys + v->count * ADDRESS_KEY_SIZE, addr, ADDRESS_KEY_SIZE);
    v->count++;
}

static inline void tx_dirty_slot_push(tx_dirty_slot_vec_t *v, const uint8_t *skey) {
    if (v->count >= v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 32;
        uint8_t *nk = realloc(v->keys, nc * SLOT_KEY_SIZE);
        if (!nk) return;
        v->keys = nk; v->cap = nc;
    }
    memcpy(v->keys + v->count * SLOT_KEY_SIZE, skey, SLOT_KEY_SIZE);
    v->count++;
}

static inline void tx_dirty_addr_clear(tx_dirty_addr_vec_t *v) { v->count = 0; }
static inline void tx_dirty_slot_clear(tx_dirty_slot_vec_t *v) { v->count = 0; }

static inline void tx_dirty_addr_free(tx_dirty_addr_vec_t *v) {
    free(v->keys); v->keys = NULL; v->count = v->cap = 0;
}
static inline void tx_dirty_slot_free(tx_dirty_slot_vec_t *v) {
    free(v->keys); v->keys = NULL; v->count = v->cap = 0;
}

// --- Main struct ---

struct evm_state {
#ifdef ENABLE_VERKLE
    verkle_state_t   *vs;
#endif
    mem_art_t         accounts;     // key: addr[20], value: cached_account_t
    mem_art_t         storage;      // key: skey[52], value: cached_slot_t
    journal_entry_t  *journal;
    uint32_t          journal_len;
    uint32_t          journal_cap;
    mem_art_t         warm_addrs;   // key: addr[20], value: (none, 0 bytes)
    mem_art_t         warm_slots;   // key: skey[52], value: (none, 0 bytes)
    mem_art_t         transient;    // key: skey[52], value: uint256_t (EIP-1153)
    tx_dirty_addr_vec_t tx_dirty_accounts;  // accounts dirtied in current tx
    tx_dirty_slot_vec_t tx_dirty_slots;     // slots dirtied in current tx
    bool              batch_mode;   // skip per-block verkle flush, defer to checkpoint
    bool              discard_on_destroy; // skip MPT flush on destroy (set after failed block)
#ifdef ENABLE_VERKLE
    witness_gas_t     witness_gas;  // EIP-4762 verkle witness gas tracker
#endif
#ifdef ENABLE_MPT
    mpt_arena_t      *account_mpt;  // persistent incremental account trie
    storage_trie_t   *storage_trie; // per-account storage roots from flat_state's compact_art
    code_store_t     *code_store;   // content-addressed bytecode store (not owned)
    flat_state_t     *flat_state;   // O(1) disk-backed state (not owned, optional)
    dirty_account_vec_t dirty_accounts; // accounts with mpt_dirty=true
    dirty_slot_vec_t    dirty_slots;    // slots with mpt_dirty=true
    /* Root computation timing (last compute_mpt_root call) */
    double              last_root_stor_ms;
    double              last_root_acct_ms;
    size_t              last_root_dirty_count;
    mpt_arena_commit_stats_t  last_acct_commit;
    /* Flat state lookup counters (reset per checkpoint window) */
    uint64_t flat_acct_hit;
    uint64_t flat_acct_miss;
    uint64_t flat_stor_hit;
    uint64_t flat_stor_miss;
#endif
};

// ============================================================================
// Forward declarations (MPT read-through helpers defined later)
// ============================================================================
#ifdef ENABLE_MPT
static bool mpt_rlp_decode_account(const uint8_t *rlp, size_t len,
                                     cached_account_t *ca);
static uint256_t mpt_rlp_decode_storage_value(const uint8_t *rlp, size_t len);

// Mark account as MPT-dirty and track it for fast iteration.
// Only appends to the dirty list on the first mark (idempotent).
static inline void mark_account_mpt_dirty(evm_state_t *es, cached_account_t *ca) {
    if (!ca->mpt_dirty) {
        ca->mpt_dirty = true;
        dirty_account_push(&es->dirty_accounts, ca->addr.bytes);
    }
}

// Mark slot as MPT-dirty and track it for fast iteration.
static inline void mark_slot_mpt_dirty(evm_state_t *es, cached_slot_t *cs) {
    if (!cs->mpt_dirty) {
        cs->mpt_dirty = true;
        dirty_slot_push(&es->dirty_slots, cs->key);
    }
}
#endif

// Mark account as tx-dirty (for fast commit_tx iteration).
// Only pushes on first dirty transition per tx (idempotent).
static inline void mark_account_tx_dirty(evm_state_t *es, cached_account_t *ca) {
    if (!ca->dirty) {
        tx_dirty_addr_push(&es->tx_dirty_accounts, ca->addr.bytes);
    }
}

// Mark slot as tx-dirty.
static inline void mark_slot_tx_dirty(evm_state_t *es, cached_slot_t *cs) {
    if (!cs->dirty) {
        tx_dirty_slot_push(&es->tx_dirty_slots, cs->key);
    }
}

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

// Load account into cache. From verkle_state if enabled, else empty.
static cached_account_t *ensure_account(evm_state_t *es, const address_t *addr) {
    cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
        &es->accounts, addr->bytes, ADDRESS_SIZE, NULL);
    if (ca) return ca;

    // Build on stack, insert into arena
    cached_account_t ca_local;
    memset(&ca_local, 0, sizeof(ca_local));
    address_copy(&ca_local.addr, addr);

#ifdef ENABLE_VERKLE
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
#endif

    ca_local.storage_root = HASH_EMPTY_STORAGE;
    ca_local.addr_hash = hash_keccak256(addr->bytes, 20);

#ifdef ENABLE_MPT
    // Fast path: O(1) flat state lookup (single disk_table read, mmap'd)
    if (es->flat_state) {
        flat_account_record_t frec;
        if (flat_state_get_account(es->flat_state, ca_local.addr_hash.bytes, &frec)) {
            ca_local.nonce = frec.nonce;
            ca_local.balance = uint256_from_bytes(frec.balance, 32);
            memcpy(ca_local.code_hash.bytes, frec.code_hash, 32);
            memcpy(ca_local.storage_root.bytes, frec.storage_root, 32);
            ca_local.has_code = (memcmp(frec.code_hash, ((hash_t){0}).bytes, 32) != 0 &&
                                 memcmp(frec.code_hash, HASH_EMPTY_CODE.bytes, 32) != 0);
            ca_local.existed = true;
            es->flat_acct_hit++;
        } else {
            es->flat_acct_miss++;
        }
    }
#endif

#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca_local.original_nonce = ca_local.nonce;
    ca_local.original_balance = ca_local.balance;
    ca_local.original_code_hash = ca_local.code_hash;
#endif

    return (cached_account_t *)mem_art_upsert(
        &es->accounts, addr->bytes, ADDRESS_SIZE,
        &ca_local, sizeof(cached_account_t));
}

// Load storage slot into cache. From verkle_state if enabled, else zero.
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

#ifdef ENABLE_VERKLE
    // Load from verkle_state
    // Slot key: LE for key derivation arithmetic
    // Value: BE (Ethereum convention — go-ethereum uses common.LeftPadBytes)
    uint8_t slot_le[32], val_be[32];
    uint256_to_bytes_le(slot, slot_le);
    verkle_state_get_storage(es->vs, addr->bytes, slot_le, val_be);
    cs_local.original = uint256_from_bytes(val_be, 32);
    cs_local.current = cs_local.original;
#endif

#ifdef ENABLE_MPT
    // Compute and cache keccak256(slot_be) — reused at MPT write time
    cs_local.slot_hash = hash_keccak256(skey + 20, 32);

    // Fast path: O(1) flat state lookup
    if (es->flat_state) {
        cached_account_t *ca = ensure_account(es, addr);
        if (ca) {
            uint8_t val_be[32];
            if (flat_state_get_storage(es->flat_state, ca->addr_hash.bytes,
                                        cs_local.slot_hash.bytes, val_be)) {
                cs_local.original = uint256_from_bytes(val_be, 32);
                cs_local.current = cs_local.original;
                es->flat_stor_hit++;
            } else {
                es->flat_stor_miss++;
            }
        }
    }
#endif

#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    cs_local.block_original = cs_local.original;
#endif

    return (cached_slot_t *)mem_art_upsert(
        &es->storage, skey, SLOT_KEY_SIZE,
        &cs_local, sizeof(cached_slot_t));
}

// ============================================================================
// Lifecycle
// ============================================================================

evm_state_t *evm_state_create(verkle_state_t *vs, const char *mpt_path,
                               code_store_t *cs) {
#ifdef ENABLE_VERKLE
    if (!vs) return NULL;
#else
    (void)vs;
#endif
#ifdef ENABLE_MPT
    (void)cs;  /* stored after MPT init */
#else
    (void)mpt_path;
    (void)cs;
#endif

    evm_state_t *es = calloc(1, sizeof(evm_state_t));
    if (!es) return NULL;

#ifdef ENABLE_VERKLE
    es->vs = vs;
#endif
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

#ifdef ENABLE_VERKLE
    witness_gas_init(&es->witness_gas);
#endif

#ifdef ENABLE_MPT
    if (mpt_path) {
        (void)mpt_path; /* path unused — arena is in-memory */
        es->account_mpt = mpt_arena_create();
        if (!es->account_mpt) {
            fprintf(stderr, "FATAL: failed to create account MPT arena\n");
            mem_art_destroy(&es->accounts);
            mem_art_destroy(&es->storage);
            mem_art_destroy(&es->warm_addrs);
            mem_art_destroy(&es->warm_slots);
            mem_art_destroy(&es->transient);
            free(es->journal);
            free(es);
            return NULL;
        }

        /* storage_trie is created lazily when flat_state is set
         * (it needs flat_state's compact_art and flat_store) */
        es->code_store = cs;
    }
#endif

    return es;
}

bool evm_state_init_mpt_stores(evm_state_t *es, const char *path,
                                uint64_t acct_cap, uint64_t stor_cap) {
#ifdef ENABLE_MPT
    (void)path; (void)acct_cap; (void)stor_cap;
    if (!es) return false;
    if (es->account_mpt) return false;

    es->account_mpt = mpt_arena_create();
    if (!es->account_mpt) return false;
    /* storage_trie created lazily when flat_state is set */
    return true;
#else
    (void)es; (void)path; (void)acct_cap; (void)stor_cap;
    return false;
#endif
}

void evm_state_reset_mpt_stores(evm_state_t *es) {
#ifdef ENABLE_MPT
    if (!es) return;
    if (es->account_mpt) mpt_arena_reset(es->account_mpt);
    if (es->storage_trie) storage_trie_invalidate_all(es->storage_trie);
#else
    (void)es;
#endif
}

void evm_state_get_mpt_stores(const evm_state_t *es,
                                void **out_account_mpt,
                                void **out_storage_mpt) {
#ifdef ENABLE_MPT
    if (out_account_mpt) *out_account_mpt = es ? es->account_mpt : NULL;
    if (out_storage_mpt) *out_storage_mpt = NULL; /* storage_trie replaces storage_mpt */
#else
    if (out_account_mpt) *out_account_mpt = NULL;
    if (out_storage_mpt) *out_storage_mpt = NULL;
#endif
}

void evm_state_detach_mpt_stores(evm_state_t *es,
                                   void **out_account_mpt,
                                   void **out_storage_mpt) {
#ifdef ENABLE_MPT
    if (!es) return;
    if (out_account_mpt) *out_account_mpt = es->account_mpt;
    if (out_storage_mpt) *out_storage_mpt = NULL;
    es->account_mpt = NULL;
#else
    (void)es;
    if (out_account_mpt) *out_account_mpt = NULL;
    if (out_storage_mpt) *out_storage_mpt = NULL;
#endif
}

void evm_state_attach_mpt_stores(evm_state_t *es,
                                   void *account_mpt, void *storage_mpt) {
#ifdef ENABLE_MPT
    if (!es) return;
    (void)storage_mpt; /* storage_trie replaces storage_mpt */
    es->account_mpt = (mpt_arena_t *)account_mpt;
    if (es->account_mpt) mpt_arena_reset(es->account_mpt);
#else
    (void)es; (void)account_mpt; (void)storage_mpt;
#endif
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

void evm_state_evict_cache(evm_state_t *es) {
    if (!es) return;

#ifdef ENABLE_MPT
    if (es->flat_state) {
        /* Storage slots already flushed to flat_state in compute_all_storage_roots.
         * Only flush dirty accounts here. */

        /* Flush dirty accounts to flat_state.
         * For shared storage MPT: manage external refcounts so that
         * storage root nodes referenced by flat_state are not freed
         * by other accounts' commits in future checkpoints. */
        for (size_t d = 0; d < es->dirty_accounts.count; d++) {
            const uint8_t *akey = es->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, akey, ADDRESS_KEY_SIZE, NULL);
            if (!ca) continue;

            if (ca->existed) {
                flat_account_record_t frec;
                frec.nonce = ca->nonce;
                uint256_to_bytes(&ca->balance, frec.balance);
                const uint8_t *ch = ca->has_code
                    ? ca->code_hash.bytes : HASH_EMPTY_CODE.bytes;
                memcpy(frec.code_hash, ch, 32);
                memcpy(frec.storage_root, ca->storage_root.bytes, 32);
                flat_state_put_account(es->flat_state, ca->addr_hash.bytes, &frec);
            } else {
                flat_state_delete_account(es->flat_state, ca->addr_hash.bytes);
            }
        }
    }
    /* Free code pointers for all cached accounts before arena destroy */
    mem_art_foreach(&es->accounts, free_code_cb, NULL);
    dirty_account_clear(&es->dirty_accounts);
    dirty_slot_clear(&es->dirty_slots);
#else
    mem_art_foreach(&es->accounts, free_code_cb, NULL);
#endif

    mem_art_destroy(&es->accounts);
    mem_art_destroy(&es->storage);

    /* Fresh current generation */
    mem_art_init(&es->accounts);
    mem_art_init(&es->storage);

#ifdef ENABLE_MPT
    /* Reset flat state counters for next window */
    es->flat_acct_hit = es->flat_acct_miss = 0;
    es->flat_stor_hit = es->flat_stor_miss = 0;
#endif
}

void evm_state_flush(evm_state_t *es) {
    if (!es) return;
#ifdef ENABLE_MPT
    /* mpt_arena: no flush needed — all data is in memory */
#endif
}


void evm_state_set_batch_mode(evm_state_t *es, bool enabled) {
    if (es) es->batch_mode = enabled;
}

#ifdef ENABLE_MPT
void evm_state_set_flat_state(evm_state_t *es, flat_state_t *fs) {
    if (!es) return;
    es->flat_state = fs;

    /* Create or destroy storage_trie based on flat_state availability */
    if (fs) {
        compact_art_t *art = flat_state_storage_art(fs);
        flat_store_t *store = flat_state_storage_store(fs);
        if (art && store && !es->storage_trie)
            es->storage_trie = storage_trie_create(art, store);
    } else {
        if (es->storage_trie) {
            storage_trie_destroy(es->storage_trie);
            es->storage_trie = NULL;
        }
    }
}

flat_state_t *evm_state_get_flat_state(const evm_state_t *es) {
    return es ? es->flat_state : NULL;
}
#endif


void evm_state_flush_verkle(evm_state_t *es) {
#ifdef ENABLE_VERKLE
    if (!es || !es->vs) return;

    // Flush accumulated block-dirty state to verkle backing store
    finalize_ctx_t ctx = { .es = es, .ok = true, .prune_empty = false };
    mem_art_foreach(&es->accounts, flush_all_accounts_cb, &ctx);
    mem_art_foreach(&es->storage, flush_all_storage_cb, &ctx);

    // Single begin/commit cycle for the entire batch
    verkle_state_begin_block(es->vs, 0);
    verkle_state_commit_block(es->vs);

    // Clear block_dirty flags
    mem_art_foreach(&es->accounts, clear_block_dirty_account_cb, NULL);
    mem_art_foreach(&es->storage, clear_block_dirty_slot_cb, NULL);
#else
    (void)es;
#endif
}

void evm_state_destroy(evm_state_t *es) {
    if (!es) return;

#ifdef ENABLE_MPT
    // Flush deferred writes and destroy persistent MPT stores.
    // Skip flush if discard_on_destroy is set (failed block — don't corrupt disk state).
    if (es->account_mpt) {
        mpt_arena_destroy(es->account_mpt);
    }
    if (es->storage_trie) {
        storage_trie_destroy(es->storage_trie);
    }
    dirty_account_free(&es->dirty_accounts);
    dirty_slot_free(&es->dirty_slots);
#endif

    tx_dirty_addr_free(&es->tx_dirty_accounts);
    tx_dirty_slot_free(&es->tx_dirty_slots);

    // Free code pointers owned by cached accounts
    mem_art_foreach(&es->accounts, free_code_cb, NULL);

    // Destroy all trees
    mem_art_destroy(&es->accounts);
    mem_art_destroy(&es->storage);
    mem_art_destroy(&es->warm_addrs);
    mem_art_destroy(&es->warm_slots);
    mem_art_destroy(&es->transient);
#ifdef ENABLE_VERKLE
    witness_gas_destroy(&es->witness_gas);
#endif

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

void evm_state_discard_pending(evm_state_t *es) {
    if (es) es->discard_on_destroy = true;
}

// ============================================================================
// Account Existence
// ============================================================================

void evm_state_prefetch_account(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return;
    mem_art_prefetch(&es->accounts, addr->bytes, ADDRESS_SIZE);
}

bool evm_state_exists(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return false;
    // In geth, Exist() returns true once CreateAccount() is called (e.g. from
    // Call()), even for empty accounts.  Our CALL touch sets dirty via
    // add_balance(addr, 0).  Without checking dirty, the same non-existent
    // address is charged 25000 new-account gas on every CALL within one tx.
    // dirty is properly reverted by the journal on subcall failure.
    // block_dirty survives commit_tx (cleared only at block end), so accounts
    // touched in earlier transactions of the same block are still "existing"
    // — matching geth's Frontier behavior where empty objects persist.
    return ca->existed || ca->created || ca->dirty || ca->block_dirty;
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
        .data.nonce = { .val = ca->nonce, .dirty = ca->dirty, .block_dirty = ca->block_dirty }
    };
    journal_push(es, &je);

    ca->nonce = nonce;
    mark_account_tx_dirty(es, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(es, ca);
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
        .data.balance = { .val = ca->balance, .dirty = ca->dirty, .block_dirty = ca->block_dirty }
    };
    journal_push(es, &je);

    ca->balance = *balance;
    mark_account_tx_dirty(es, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(es, ca);
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

void evm_state_set_code_hash(evm_state_t *es, const address_t *addr,
                              const hash_t *code_hash) {
    if (!es || !addr || !code_hash) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return;
    ca->code_hash = *code_hash;
    ca->has_code = (memcmp(code_hash->bytes, HASH_EMPTY_CODE.bytes, 32) != 0 &&
                    memcmp(code_hash->bytes, ((hash_t){0}).bytes, 32) != 0);
    mark_account_tx_dirty(es, ca);
    ca->dirty = true;
    ca->code_dirty = true;
    ca->block_dirty = true;
    ca->block_code_dirty = true;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(es, ca);
}

uint32_t evm_state_get_code_size(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return 0;
    cached_account_t *ca = ensure_account(es, addr);
    if (!ca) return 0;

    // If code is cached, return cached size
    if (ca->code) return ca->code_size;

    // If no code, return 0
    if (!ca->has_code) return 0;

#ifdef ENABLE_VERKLE
    // Load code size from verkle_state
    ca->code_size = (uint32_t)verkle_state_get_code_size(es->vs, addr->bytes);
#elif defined(ENABLE_MPT)
    // Load code from code_store to get the size (also caches the code)
    if (es->code_store && ca->code_size == 0) {
        uint32_t size = code_store_get_size(es->code_store,
                                             ca->code_hash.bytes);
        if (size > 0) {
            ca->code = malloc(size);
            if (ca->code) {
                code_store_get(es->code_store, ca->code_hash.bytes,
                               ca->code, size);
                ca->code_size = size;
            }
        }
    }
#endif
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
#ifdef ENABLE_VERKLE
        uint64_t len = verkle_state_get_code_size(es->vs, addr->bytes);
        if (len == 0) {
            if (out_len) *out_len = 0;
            return true;
        }

        ca->code = malloc(len);
        if (!ca->code) return false;

        uint64_t got = verkle_state_get_code(es->vs, addr->bytes, ca->code, len);
        ca->code_size = (uint32_t)got;
#elif defined(ENABLE_MPT)
        if (es->code_store) {
            uint32_t size = code_store_get_size(es->code_store,
                                                 ca->code_hash.bytes);
            if (size > 0) {
                ca->code = malloc(size);
                if (ca->code) {
                    code_store_get(es->code_store, ca->code_hash.bytes,
                                   ca->code, size);
                    ca->code_size = size;
                }
            }
        }
        if (!ca->code) {
            if (out_len) *out_len = 0;
            return true;
        }
#else
        if (out_len) *out_len = 0;
        return true;
#endif
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
#ifdef ENABLE_VERKLE
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
#elif defined(ENABLE_MPT)
        if (es->code_store) {
            uint32_t size = code_store_get_size(es->code_store,
                                                 ca->code_hash.bytes);
            if (size > 0) {
                ca->code = malloc(size);
                if (ca->code) {
                    code_store_get(es->code_store, ca->code_hash.bytes,
                                   ca->code, size);
                    ca->code_size = size;
                }
            }
        }
        if (!ca->code) {
            if (out_len) *out_len = 0;
            return NULL;
        }
#else
        if (out_len) *out_len = 0;
        return NULL;
#endif
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
    if (!journal_push(es, &je)) {
        // journal_push failed — ownership was NOT transferred, free old code
        free(ca->code);
        ca->code = NULL;
        ca->code_size = 0;
    } else {
        // Old code ownership transferred to journal — don't free
        ca->code = NULL;
        ca->code_size = 0;
    }

    if (code && len > 0) {
        ca->code = malloc(len);
        if (ca->code) {
            memcpy(ca->code, code, len);
            ca->code_size = len;
        }
        ca->has_code = true;
        ca->code_hash = hash_keccak256(code, len);
#ifdef ENABLE_MPT
        if (es->code_store)
            code_store_put(es->code_store, ca->code_hash.bytes, code, len);
#endif
    } else {
        ca->has_code = false;
        ca->code_hash = hash_zero();
    }

    mark_account_tx_dirty(es, ca);
    ca->dirty = true;
    ca->code_dirty = true;
    ca->block_dirty = true;
    ca->block_code_dirty = true;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(es, ca);
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

uint256_t evm_state_sload(evm_state_t *es, const address_t *addr,
                          const uint256_t *key, bool *was_warm) {
    if (!es || !addr || !key) {
        if (was_warm) *was_warm = false;
        return UINT256_ZERO_INIT;
    }
    // Single make_slot_key + ensure_slot lookup, then check warm_slots
    // with the same skey — avoids double make_slot_key + double ART traversal
    cached_slot_t *cs = ensure_slot(es, addr, key);
    if (!cs) {
        if (was_warm) *was_warm = false;
        return UINT256_ZERO_INIT;
    }
    // Check warm using the skey already stored in cs->key
    if (was_warm)
        *was_warm = mem_art_contains(&es->warm_slots, cs->key, SLOT_KEY_SIZE);
    return cs->current;
}

uint256_t evm_state_get_committed_storage(evm_state_t *es, const address_t *addr,
                                          const uint256_t *key) {
    if (!es || !addr || !key) return UINT256_ZERO_INIT;
    cached_slot_t *cs = ensure_slot(es, addr, key);
    if (!cs) return UINT256_ZERO_INIT;
    return cs->original;
}


void evm_state_sstore_lookup(evm_state_t *es, const address_t *addr,
                              const uint256_t *key,
                              uint256_t *current, uint256_t *original,
                              bool *was_warm) {
    if (!es || !addr || !key) {
        if (current) *current = UINT256_ZERO_INIT;
        if (original) *original = UINT256_ZERO_INIT;
        if (was_warm) *was_warm = false;
        return;
    }
    cached_slot_t *cs = ensure_slot(es, addr, key);
    if (!cs) {
        if (current) *current = UINT256_ZERO_INIT;
        if (original) *original = UINT256_ZERO_INIT;
        if (was_warm) *was_warm = false;
        return;
    }
    if (current) *current = cs->current;
    if (original) *original = cs->original;
    // Check warm using skey already stored in cs->key
    if (was_warm)
        *was_warm = mem_art_contains(&es->warm_slots, cs->key, SLOT_KEY_SIZE);
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
            .old_value = cs->current,
            .old_mpt_dirty = cs->mpt_dirty
        }
    };
    journal_push(es, &je);

    cs->current = *value;
    mark_slot_tx_dirty(es, cs);
    cs->dirty = true;
    cs->block_dirty = true;
    mark_slot_mpt_dirty(es, cs);

    // Mark the owning account as having dirty storage (for mpt_store)
    cached_account_t *ca = ensure_account(es, addr);
    if (ca) {
        ca->storage_dirty = true;
        mark_account_mpt_dirty(es, ca);
    }
}

bool evm_state_has_storage(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return false;

#ifdef ENABLE_MPT
    // Fast path: check cached storage_root from the account.
    // After cache eviction, in-memory slots are gone but the account's
    // storage_root (loaded from the MPT) tells us if storage exists on disk.
    cached_account_t *ca = ensure_account(es, addr);
    if (ca && memcmp(ca->storage_root.bytes, HASH_EMPTY_STORAGE.bytes, 32) != 0)
        return true;
#endif

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
            .old_block_dirty    = ca->block_dirty,
            .old_block_code_dirty = ca->block_code_dirty,
            .old_created        = ca->created,
            .old_existed        = ca->existed,
            .old_self_destructed = ca->self_destructed,
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
            .old_block_created  = ca->block_created,
#endif
            .old_storage_root   = ca->storage_root,
            .old_storage_dirty  = ca->storage_dirty,
        },
    };
    if (!journal_push(es, &je)) {
        // journal_push failed — ownership NOT transferred, free old code
        free(ca->code);
    }

    // Reset account: preserve existing balance per Ethereum spec.
    // CREATE/CREATE2 preserves any pre-existing ether at the target address.
    uint256_t existing_balance = ca->balance;
    ca->nonce = 0;
    ca->balance = existing_balance;
    ca->code_hash = hash_zero();
    ca->has_code = false;
    ca->code = NULL;          // ownership moved to journal entry (or freed above)
    ca->code_size = 0;
    ca->created = true;
    mark_account_tx_dirty(es, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->block_created = true;
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(es, ca);
    ca->code_dirty = false;
    ca->self_destructed = false;
    ca->storage_root = HASH_EMPTY_STORAGE;
    ca->storage_dirty = true;
}

void evm_state_mark_existed(evm_state_t *es, const address_t *addr) {
    if (!es || !addr) return;
    cached_account_t *ca = ensure_account(es, addr);
    if (ca) ca->existed = true;
}

static bool clear_prestate_account_cb(const uint8_t *key, size_t key_len,
                                       const void *value, size_t value_len,
                                       void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    ca->block_dirty = false;
    ca->block_code_dirty = false;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->block_self_destructed = false;
    ca->block_created = false;
#endif
    ca->mpt_dirty = false;
    return true;
}

static bool clear_prestate_slot_cb(const uint8_t *key, size_t key_len,
                                    const void *value, size_t value_len,
                                    void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_slot_t *cs = (cached_slot_t *)(uintptr_t)value;
    cs->block_dirty = false;
    cs->mpt_dirty = false;
    return true;
}

void evm_state_clear_prestate_dirty(evm_state_t *es) {
    if (!es) return;
    mem_art_foreach(&es->accounts, clear_prestate_account_cb, NULL);
    mem_art_foreach(&es->storage, clear_prestate_slot_cb, NULL);
    dirty_account_clear(&es->dirty_accounts);
    dirty_slot_clear(&es->dirty_slots);
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
    mark_account_tx_dirty(es, ca);
    ca->dirty = true;
    ca->block_dirty = true;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->block_accessed = true;
#endif
    mark_account_mpt_dirty(es, ca);
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
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    cs->block_original = cs->current;
#endif
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
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
    ca->original_nonce = ca->nonce;
    ca->original_balance = ca->balance;
    ca->original_code_hash = ca->code_hash;
    ca->block_accessed = false;
    ca->block_created = false;
    ca->block_self_destructed = false;
#endif
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
    address_t   *addrs;
    size_t       count;
    size_t       cap;
    evm_state_t *es;    /* for mark_slot_mpt_dirty on self-destruct */
} selfdestructed_ctx_t;

static bool commit_tx_account_cb(const uint8_t *key, size_t key_len,
                                  const void *value, size_t value_len,
                                  void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    selfdestructed_ctx_t *ctx = (selfdestructed_ctx_t *)user_data;

    if (ca->self_destructed) {
        if (ctx->count >= ctx->cap) {
            size_t nc = ctx->cap ? ctx->cap * 2 : 16;
            address_t *na = realloc(ctx->addrs, nc * sizeof(*na));
            if (na) { ctx->addrs = na; ctx->cap = nc; }
        }
        if (ctx->count < ctx->cap)
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
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
        ca->block_self_destructed = true;
#endif
        ca->block_dirty = false;
        ca->self_destructed = false;
        ca->block_code_dirty = false;
        ca->storage_root = HASH_EMPTY_STORAGE;
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
            cs->block_dirty = false;
#ifdef ENABLE_MPT
            /* Mark as mpt_dirty so it appears in dirty_slots for eviction
             * (ensures flat_state deletes this zeroed slot) */
            if (ctx->es)
                mark_slot_mpt_dirty(ctx->es, cs);
#else
            cs->mpt_dirty = false;
#endif
            return true;
        }
    }

    cs->original = cs->current;
    cs->dirty = false;
    return true;
}

void evm_state_commit_tx(evm_state_t *es) {
    if (!es) return;

    /* --- Process only tx-dirty accounts (fast path) --- */
    selfdestructed_ctx_t ctx = { .addrs = NULL, .count = 0, .cap = 0, .es = es };

    for (size_t i = 0; i < es->tx_dirty_accounts.count; i++) {
        const uint8_t *addr_key = es->tx_dirty_accounts.keys + i * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &es->accounts, addr_key, ADDRESS_KEY_SIZE, NULL);
        if (!ca) continue;

        if (ca->self_destructed) {
            if (ctx.count >= ctx.cap) {
                size_t nc = ctx.cap ? ctx.cap * 2 : 16;
                address_t *na = realloc(ctx.addrs, nc * sizeof(*na));
                if (na) { ctx.addrs = na; ctx.cap = nc; }
            }
            if (ctx.count < ctx.cap)
                ctx.addrs[ctx.count++] = ca->addr;

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
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
            ca->block_self_destructed = true;
#endif
            ca->block_dirty = false;
            ca->self_destructed = false;
            ca->block_code_dirty = false;
            ca->storage_root = HASH_EMPTY_STORAGE;
            continue;
        }

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
    }

    /* --- Process storage --- */
    if (ctx.count > 0) {
        /* Rare path: self-destructs exist — must scan all slots to zero
         * storage belonging to destructed addresses */
        mem_art_foreach(&es->storage, commit_tx_slot_cb, &ctx);
    } else {
        /* Fast path: only iterate tx-dirty slots */
        for (size_t i = 0; i < es->tx_dirty_slots.count; i++) {
            const uint8_t *skey = es->tx_dirty_slots.keys + i * SLOT_KEY_SIZE;
            cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
                &es->storage, skey, SLOT_KEY_SIZE, NULL);
            if (!cs) continue;
            cs->original = cs->current;
            cs->dirty = false;
        }
    }

    free(ctx.addrs);
    tx_dirty_addr_clear(&es->tx_dirty_accounts);
    tx_dirty_slot_clear(&es->tx_dirty_slots);
    es->journal_len = 0;

    mem_art_destroy(&es->warm_addrs);
    mem_art_init(&es->warm_addrs);
    mem_art_destroy(&es->warm_slots);
    mem_art_init(&es->warm_slots);

    mem_art_destroy(&es->transient);
    mem_art_init(&es->transient);

#ifdef ENABLE_VERKLE
    witness_gas_reset(&es->witness_gas);
#endif
}

void evm_state_begin_block(evm_state_t *es, uint64_t block_number) {
    if (!es) return;
#ifdef ENABLE_VERKLE
    witness_gas_reset(&es->witness_gas);
    if (!es->batch_mode)
        verkle_state_begin_block(es->vs, block_number);
#else
    (void)block_number;
#endif
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
                ca->block_dirty = je->data.nonce.block_dirty;
            }
            break;
        }
        case JOURNAL_BALANCE: {
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, je->addr.bytes, ADDRESS_SIZE, NULL);
            if (ca) {
                ca->balance = je->data.balance.val;
                ca->dirty = je->data.balance.dirty;
                ca->block_dirty = je->data.balance.block_dirty;
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
                cs->mpt_dirty = je->data.storage.old_mpt_dirty;
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
                ca->block_dirty     = je->data.create.old_block_dirty;
                ca->block_code_dirty = je->data.create.old_block_code_dirty;
                ca->created         = je->data.create.old_created;
                ca->existed         = je->data.create.old_existed;
                ca->self_destructed = je->data.create.old_self_destructed;
#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
                ca->block_created   = je->data.create.old_block_created;
#endif
                ca->storage_root    = je->data.create.old_storage_root;
                ca->storage_dirty   = je->data.create.old_storage_dirty;
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

    bool was_new = false;
    mem_art_insert_check(&es->warm_addrs, addr->bytes, ADDRESS_SIZE,
                         NULL, 0, &was_new);
    if (!was_new) return true;  // already warm

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

    bool was_new = false;
    mem_art_insert_check(&es->warm_slots, skey, SLOT_KEY_SIZE,
                         NULL, 0, &was_new);
    if (!was_new) return true;  // already warm

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

#ifdef ENABLE_VERKLE
typedef struct {
    evm_state_t *es;
    bool ok;
    bool prune_empty;  // EIP-158: skip writing empty touched accounts
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
#endif /* ENABLE_VERKLE */

bool evm_state_finalize(evm_state_t *es) {
    if (!es) return false;
#ifdef ENABLE_VERKLE
    finalize_ctx_t ctx = { .es = es, .ok = true };

    mem_art_foreach(&es->accounts, finalize_account_cb, &ctx);
    if (!ctx.ok) return false;

    mem_art_foreach(&es->storage, finalize_storage_cb, &ctx);
    return ctx.ok;
#else
    return true;
#endif
}

// ============================================================================
// State Root — delegate to verkle_state
// ============================================================================

#ifdef ENABLE_VERKLE
// Flush callback: writes block-dirty cached accounts to verkle
static bool flush_all_accounts_cb(const uint8_t *key, size_t key_len,
                                    const void *value, size_t value_len,
                                    void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    finalize_ctx_t *ctx = (finalize_ctx_t *)user_data;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;

    // Skip accounts not modified during this block
    if (!ca->block_dirty && !ca->block_code_dirty) return true;

    if (g_trace_calls) {
        fprintf(stderr, "FLUSH_ACCT %02x%02x..%02x%02x nonce=%lu has_code=%d existed=%d created=%d self_destructed=%d bal=",
                key[0], key[1], key[18], key[19],
                ca->nonce, ca->has_code, ca->existed, ca->created, ca->self_destructed);
        uint8_t balbuf[32]; uint256_to_bytes(&ca->balance, balbuf);
        for (int bi = 0; bi < 32; bi++) fprintf(stderr, "%02x", balbuf[bi]);
        fprintf(stderr, "\n");
    }

    if (ca->self_destructed) {
        verkle_state_set_nonce(ctx->es->vs, ca->addr.bytes, 0);
        uint8_t zero32[32] = {0};
        verkle_state_set_balance(ctx->es->vs, ca->addr.bytes, zero32);
        verkle_state_set_code_hash(ctx->es->vs, ca->addr.bytes, zero32);
        verkle_state_set_code_size(ctx->es->vs, ca->addr.bytes, 0);
        return true;
    }

    // Skip empty accounts that never existed.
    // In pre-EIP-158 (prune_empty=false), touched empty accounts must persist
    // — geth keeps empty state objects across blocks in Frontier/Homestead.
    bool is_empty = (ca->nonce == 0 &&
                     uint256_is_zero(&ca->balance) &&
                     !ca->has_code);
    if (!ca->existed && !ca->created && is_empty && ctx->prune_empty) return true;

    // Mark as existing for future blocks (account is now in the store)
    ca->existed = true;

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
#endif /* ENABLE_VERKLE */

#ifdef ENABLE_VERKLE
static bool clear_block_dirty_account_cb(const uint8_t *key, size_t key_len,
                                          const void *value, size_t value_len,
                                          void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    cached_account_t *ca = (cached_account_t *)(uintptr_t)value;
    ca->block_dirty = false;
    ca->block_code_dirty = false;
    // Note: storage_dirty is NOT cleared here — it persists until
    // compute_all_storage_roots() actually computes the cached storage_root.
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
#endif /* ENABLE_VERKLE */


hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty) {
    if (!es) return hash_zero();

#ifdef ENABLE_VERKLE
    // In batch mode, defer flush to checkpoint — block_dirty accumulates
    if (es->batch_mode) {
        (void)prune_empty;
        return hash_zero();
    }

    // Flush only block-dirty cached state to verkle
    finalize_ctx_t ctx = { .es = es, .ok = true, .prune_empty = prune_empty };
    mem_art_foreach(&es->accounts, flush_all_accounts_cb, &ctx);
    mem_art_foreach(&es->storage, flush_all_storage_cb, &ctx);

    // Commit block in flat backend (processes buffered writes, updates commitments).
    verkle_state_commit_block(es->vs);

    hash_t root;
    verkle_state_root_hash(es->vs, root.bytes);

    // Clear block_dirty flags — next block starts fresh
    mem_art_foreach(&es->accounts, clear_block_dirty_account_cb, NULL);
    mem_art_foreach(&es->storage, clear_block_dirty_slot_cb, NULL);

    return root;
#else
    // MPT path: delegate to compute_mpt_root which handles promotion,
    // block_dirty clearing, and EIP-161 pruning.
    // In batch mode, defer to checkpoint — dirty flags must accumulate.
#ifdef ENABLE_MPT
    if (es->account_mpt && !es->batch_mode)
        return evm_state_compute_mpt_root(es, prune_empty);
#endif
    (void)prune_empty;
    return hash_zero();
#endif
}

#ifdef ENABLE_MPT
// ============================================================================
// MPT State Root Computation (pre-Verkle block validation)
// ============================================================================

// RLP-encode big-endian integer: strips leading zeros, encodes as byte string.
// Returns bytes written (max: 1 + be_len). out must be large enough.
static size_t mpt_rlp_be(const uint8_t *be, size_t be_len, uint8_t *out) {
    size_t i = 0;
    while (i < be_len && be[i] == 0) i++;
    size_t len = be_len - i;
    if (len == 0)             { out[0] = 0x80;          return 1; }
    if (len == 1 && be[i] < 0x80) { out[0] = be[i];    return 1; }
    out[0] = 0x80 + (uint8_t)len;
    memcpy(out + 1, be + i, len);
    return 1 + len;
}

// RLP-encode uint64 as big-endian integer.
static size_t mpt_rlp_u64(uint64_t v, uint8_t *out) {
    if (v == 0) { out[0] = 0x80; return 1; }
    uint8_t be[8];
    for (int i = 7; i >= 0; i--) { be[i] = v & 0xFF; v >>= 8; }
    return mpt_rlp_be(be, 8, out);
}

// RLP-encode account [nonce, balance, storage_root, code_hash].
// Returns bytes written. out must be >= 120 bytes.
static size_t mpt_rlp_account(uint64_t nonce, const uint256_t *balance,
                               const uint8_t sr[32], const uint8_t ch[32],
                               uint8_t *out) {
    uint8_t payload[120];
    size_t pos = 0;
    pos += mpt_rlp_u64(nonce, payload + pos);
    uint8_t bal_be[32];
    uint256_to_bytes(balance, bal_be);
    pos += mpt_rlp_be(bal_be, 32, payload + pos);
    payload[pos++] = 0xa0; memcpy(payload + pos, sr, 32); pos += 32;  // storage_root
    payload[pos++] = 0xa0; memcpy(payload + pos, ch, 32); pos += 32;  // code_hash
    // List wrapper — account payload is 66..110 bytes, always > 55
    if (pos <= 55) {
        out[0] = 0xc0 + (uint8_t)pos;
        memcpy(out + 1, payload, pos);
        return 1 + pos;
    } else {
        // Long list: 0xf7 + length_of_length, then length in BE, then payload
        out[0] = 0xf8;  // 0xf7 + 1 (length fits in 1 byte since pos < 256)
        out[1] = (uint8_t)pos;
        memcpy(out + 2, payload, pos);
        return 2 + pos;
    }
}

// Decode RLP byte string header. Returns data offset and sets *data_len.
// For single byte < 0x80, offset is 0 and data_len is 1.
static size_t rlp_byte_hdr(const uint8_t *buf, size_t buf_len,
                             size_t *data_len) {
    if (buf_len == 0) return 0;
    uint8_t b = buf[0];
    if (b < 0x80) { *data_len = 1; return 0; }
    if (b <= 0xb7) { *data_len = b - 0x80; return 1; }
    size_t ll = b - 0xb7;
    if (1 + ll > buf_len) return 0;
    size_t len = 0;
    for (size_t i = 0; i < ll; i++) len = (len << 8) | buf[1 + i];
    *data_len = len;
    return 1 + ll;
}

// Decode RLP list header. Returns data offset (payload start).
static size_t rlp_list_hdr(const uint8_t *buf, size_t buf_len,
                             size_t *payload_len) {
    if (buf_len == 0) return 0;
    uint8_t b = buf[0];
    if (b >= 0xc0 && b <= 0xf7) { *payload_len = b - 0xc0; return 1; }
    if (b >= 0xf8) {
        size_t ll = b - 0xf7;
        if (1 + ll > buf_len) return 0;
        size_t len = 0;
        for (size_t i = 0; i < ll; i++) len = (len << 8) | buf[1 + i];
        *payload_len = len;
        return 1 + ll;
    }
    return 0; /* not a list */
}

// Decode RLP [nonce, balance, storage_root, code_hash] → cached_account_t fields.
// Sets nonce, balance, storage_root, code_hash, has_code, existed.
static bool mpt_rlp_decode_account(const uint8_t *rlp, size_t len,
                                     cached_account_t *ca) {
    size_t payload_len;
    size_t hdr = rlp_list_hdr(rlp, len, &payload_len);
    if (hdr == 0 || hdr + payload_len > len) return false;

    const uint8_t *p = rlp + hdr;
    const uint8_t *end = p + payload_len;

    // 1. nonce (RLP integer)
    size_t item_len;
    size_t off = rlp_byte_hdr(p, (size_t)(end - p), &item_len);
    if (p + off + item_len > end) return false;
    const uint8_t *data = p + off;
    ca->nonce = 0;
    for (size_t i = 0; i < item_len; i++)
        ca->nonce = (ca->nonce << 8) | data[i];
    p += off + item_len;

    // 2. balance (RLP big-endian integer → uint256)
    off = rlp_byte_hdr(p, (size_t)(end - p), &item_len);
    if (p + off + item_len > end) return false;
    data = p + off;
    uint8_t bal_be[32];
    memset(bal_be, 0, 32);
    if (item_len > 0 && item_len <= 32)
        memcpy(bal_be + 32 - item_len, data, item_len);
    ca->balance = uint256_from_bytes(bal_be, 32);
    p += off + item_len;

    // 3. storage_root (32 bytes, encoded as 0xa0 + 32 bytes)
    if (p + 33 > end) return false;
    if (p[0] != 0xa0) return false;
    memcpy(ca->storage_root.bytes, p + 1, 32);
    p += 33;

    // 4. code_hash (32 bytes, encoded as 0xa0 + 32 bytes)
    if (p + 33 > end) return false;
    if (p[0] != 0xa0) return false;
    memcpy(ca->code_hash.bytes, p + 1, 32);
    p += 33;

    ca->has_code = !is_zero_hash(ca->code_hash.bytes) &&
                   memcmp(ca->code_hash.bytes, HASH_EMPTY_CODE.bytes, 32) != 0;
    ca->existed = true;

    return true;
}

// Decode RLP-encoded storage value to uint256.
// Storage values are RLP byte strings containing trimmed big-endian integers.
static uint256_t mpt_rlp_decode_storage_value(const uint8_t *rlp, size_t len) {
    size_t data_len;
    size_t off = rlp_byte_hdr(rlp, len, &data_len);
    const uint8_t *data = rlp + off;
    uint8_t be[32];
    memset(be, 0, 32);
    if (data_len > 0 && data_len <= 32)
        memcpy(be + 32 - data_len, data, data_len);
    return uint256_from_bytes(be, 32);
}

// Storage entry for incremental MPT storage root computation
typedef struct {
    uint8_t  addr[20];
    uint8_t  slot_hash[32];    // keccak256(slot_be)
    uint8_t  value_rlp[33];   // RLP(trimmed big-endian value)
    uint8_t  value_len;        // 0 = delete (slot became zero)
} mpt_storage_entry_t;

typedef struct {
    mpt_storage_entry_t *entries;
    size_t count, cap;
} mpt_storage_vec_t;

static int cmp_storage_by_addr(const void *a, const void *b) {
    return memcmp(((const mpt_storage_entry_t *)a)->addr,
                  ((const mpt_storage_entry_t *)b)->addr, 20);
}

// ============================================================================
// Statistics
// ============================================================================

evm_state_stats_t evm_state_get_stats(const evm_state_t *es) {
    evm_state_stats_t s = {0};
    if (!es) return s;

    s.cache_accounts   = mem_art_size(&es->accounts);
    s.cache_slots      = mem_art_size(&es->storage);
    s.cache_arena_bytes = es->accounts.arena_used + es->storage.arena_used;

#ifdef ENABLE_MPT
    if (es->account_mpt) {
        mpt_arena_stats_t ms = mpt_arena_stats(es->account_mpt);
        s.acct_mpt_nodes       = ms.node_count;
        s.acct_mpt_data_bytes  = ms.arena_bytes;
    }
    /* storage_trie: no mpt_arena stats — uses flat_state's compact_art */
    s.stor_mpt_nodes = 0;
    s.stor_mpt_data_bytes = 0;
    if (es->code_store) {
        code_store_cache_stats_t cs = code_store_cache_stats(es->code_store);
        s.code_count       = code_store_count(es->code_store);
        s.code_cache_hits  = cs.hits;
        s.code_cache_misses = cs.misses;
        s.code_cache_count = cs.count;
        s.code_cache_capacity = cs.capacity;
    }
    s.root_stor_ms     = es->last_root_stor_ms;
    s.root_acct_ms     = es->last_root_acct_ms;
    s.root_dirty_count = es->last_root_dirty_count;
    s.stor_commit      = (mpt_arena_commit_stats_t){0};
    s.acct_commit      = es->last_acct_commit;
    s.flat_acct_hit    = es->flat_acct_hit;
    s.flat_acct_miss   = es->flat_acct_miss;
    s.flat_stor_hit    = es->flat_stor_hit;
    s.flat_stor_miss   = es->flat_stor_miss;
#endif
    return s;
}

#ifdef ENABLE_DEBUG
static bool debug_dump_all_acct_cb(const uint8_t *key, size_t key_len,
                                    const void *value, size_t value_len,
                                    void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;
    if (!ca->existed && !ca->block_dirty && !ca->created && !ca->dirty)
        return true;
    fprintf(stderr, "  ACCT ");
    for (int i = 0; i < 20; i++) fprintf(stderr, "%02x", ca->addr.bytes[i]);
    fprintf(stderr, " nonce=%lu has_code=%d existed=%d created=%d blk_dirty=%d self_dest=%d bal=",
            ca->nonce, ca->has_code, ca->existed, ca->created, ca->block_dirty, ca->self_destructed);
    uint8_t bal[32]; uint256_to_bytes(&ca->balance, bal);
    // trim leading zeros
    int s = 0; while (s < 31 && bal[s] == 0) s++;
    for (int i = s; i < 32; i++) fprintf(stderr, "%02x", bal[i]);
    fprintf(stderr, " sr=");
    for (int i = 0; i < 4; i++) fprintf(stderr, "%02x", ca->storage_root.bytes[i]);
    fprintf(stderr, "...\n");
    return true;
}
static bool debug_dump_all_slot_cb(const uint8_t *key, size_t key_len,
                                    const void *value, size_t value_len,
                                    void *user_data) {
    (void)key_len; (void)value_len; (void)user_data;
    const cached_slot_t *cs = (const cached_slot_t *)value;
    if (uint256_is_zero(&cs->current)) return true;
    fprintf(stderr, "  SLOT addr=");
    for (int i = 0; i < 20; i++) fprintf(stderr, "%02x", key[i]);
    // find first non-zero byte in slot key (key+20..key+51)
    int ss = 20; while (ss < 51 && key[ss] == 0) ss++;
    fprintf(stderr, " slot=");
    for (int i = ss; i < 52; i++) fprintf(stderr, "%02x", key[i]);
    uint8_t vbe[32]; uint256_to_bytes(&cs->current, vbe);
    int vs = 0; while (vs < 31 && vbe[vs] == 0) vs++;
    fprintf(stderr, " val=");
    for (int i = vs; i < 32; i++) fprintf(stderr, "%02x", vbe[i]);
    fprintf(stderr, "\n");
    return true;
}
void evm_state_print_mpt_stats(evm_state_t *es) {
#ifdef ENABLE_MPT
    if (!es) return;
    if (es->account_mpt) {
        mpt_arena_stats_t st = mpt_arena_stats(es->account_mpt);
        fprintf(stderr, "  account MPT: %llu nodes, arena=%llu B\n",
                (unsigned long long)st.node_count,
                (unsigned long long)st.arena_bytes);
    }
    fprintf(stderr, "  storage: via storage_trie (flat_state compact_art)\n");
#else
    (void)es;
#endif
}

void evm_state_debug_dump(evm_state_t *es) {
    fprintf(stderr, "=== EVM STATE DUMP (all non-trivial accounts) ===\n");
    mem_art_foreach(&es->accounts, debug_dump_all_acct_cb, NULL);
    mem_art_foreach(&es->storage, debug_dump_all_slot_cb, NULL);
    fprintf(stderr, "=== END DUMP ===\n");
}

// Debug: dump all state to file for Python cross-validation
typedef struct {
    FILE *f;
    mem_art_t *storage;
} dump_ctx_t;

static bool mpt_dump_acct_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len,
                               void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    dump_ctx_t *ctx = (dump_ctx_t *)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;
    if (!ca->existed) return true;
    // addr nonce balance_hex has_code code_hash_hex
    fprintf(ctx->f, "ACCT ");
    for (int i = 0; i < 20; i++) fprintf(ctx->f, "%02x", ca->addr.bytes[i]);
    fprintf(ctx->f, " %lu ", ca->nonce);
    uint8_t bal[32]; uint256_to_bytes(&ca->balance, bal);
    for (int i = 0; i < 32; i++) fprintf(ctx->f, "%02x", bal[i]);
    fprintf(ctx->f, " %d ", ca->has_code);
    const uint8_t *ch = ca->has_code ? ca->code_hash.bytes : HASH_EMPTY_CODE.bytes;
    for (int i = 0; i < 32; i++) fprintf(ctx->f, "%02x", ch[i]);
    fprintf(ctx->f, "\n");
    return true;
}

static bool mpt_dump_slot_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len,
                               void *user_data) {
    (void)key_len; (void)value_len;
    dump_ctx_t *ctx = (dump_ctx_t *)user_data;
    const cached_slot_t *cs = (const cached_slot_t *)value;
    if (uint256_is_zero(&cs->current)) return true;
    // SLOT addr slot_be value_be
    fprintf(ctx->f, "SLOT ");
    for (int i = 0; i < 20; i++) fprintf(ctx->f, "%02x", key[i]);
    fprintf(ctx->f, " ");
    for (int i = 20; i < 52; i++) fprintf(ctx->f, "%02x", key[i]);
    fprintf(ctx->f, " ");
    uint8_t vbe[32]; uint256_to_bytes(&cs->current, vbe);
    for (int i = 0; i < 32; i++) fprintf(ctx->f, "%02x", vbe[i]);
    fprintf(ctx->f, "\n");
    return true;
}

void evm_state_dump_mpt(evm_state_t *es, const char *path) {
    FILE *f = fopen(path, "w");
    if (!f) return;
    dump_ctx_t ctx = { .f = f, .storage = &es->storage };
    mem_art_foreach(&es->accounts, mpt_dump_acct_cb, &ctx);
    mem_art_foreach(&es->storage, mpt_dump_slot_cb, &ctx);
    fclose(f);
    fprintf(stderr, "MPT state dumped to %s\n", path);
}
#endif /* ENABLE_DEBUG */

// ============================================================================
// JSON alloc dump (geth t8n compatible)
// ============================================================================

typedef struct {
    FILE *f;
    mem_art_t *storage;
    bool first_account;
} alloc_json_ctx_t;

// Collect storage slots for a specific address
typedef struct {
    FILE *f;
    const uint8_t *addr;  // 20-byte address to match
    bool first_slot;
} alloc_storage_ctx_t;

static bool alloc_storage_cb(const uint8_t *key, size_t key_len,
                              const void *value, size_t value_len,
                              void *user_data) {
    (void)key_len; (void)value_len;
    alloc_storage_ctx_t *ctx = (alloc_storage_ctx_t *)user_data;
    const cached_slot_t *cs = (const cached_slot_t *)value;

    // key[0..19] = address, key[20..51] = slot
    if (memcmp(key, ctx->addr, 20) != 0) return true;
    if (uint256_is_zero(&cs->current)) return true;

    if (!ctx->first_slot) fprintf(ctx->f, ",\n");
    ctx->first_slot = false;

    // slot key (big-endian 32 bytes)
    fprintf(ctx->f, "        \"0x");
    for (int i = 20; i < 52; i++) fprintf(ctx->f, "%02x", key[i]);
    fprintf(ctx->f, "\": \"0x");

    uint8_t vbe[32];
    uint256_to_bytes(&cs->current, vbe);
    for (int i = 0; i < 32; i++) fprintf(ctx->f, "%02x", vbe[i]);
    fprintf(ctx->f, "\"");

    return true;
}

static bool alloc_account_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len,
                               void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    alloc_json_ctx_t *ctx = (alloc_json_ctx_t *)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;

    if (!ca->existed) return true;

    if (!ctx->first_account) fprintf(ctx->f, ",\n");
    ctx->first_account = false;

    // Address
    fprintf(ctx->f, "  \"0x");
    for (int i = 0; i < 20; i++) fprintf(ctx->f, "%02x", ca->addr.bytes[i]);
    fprintf(ctx->f, "\": {\n");

    // Balance
    uint8_t bal[32];
    uint256_to_bytes(&ca->balance, bal);
    fprintf(ctx->f, "    \"balance\": \"0x");
    int s = 0; while (s < 31 && bal[s] == 0) s++;
    for (int i = s; i < 32; i++) fprintf(ctx->f, "%02x", bal[i]);
    fprintf(ctx->f, "\",\n");

    // Nonce
    fprintf(ctx->f, "    \"nonce\": \"0x%lx\"", ca->nonce);

    // Code
    if (ca->has_code && ca->code && ca->code_size > 0) {
        fprintf(ctx->f, ",\n    \"code\": \"0x");
        for (uint32_t i = 0; i < ca->code_size; i++)
            fprintf(ctx->f, "%02x", ca->code[i]);
        fprintf(ctx->f, "\"");
    }

    // Storage
    alloc_storage_ctx_t sctx = {
        .f = ctx->f,
        .addr = ca->addr.bytes,
        .first_slot = true,
    };
    // Pre-scan to check if any storage exists for this address
    // (writes slots directly if found)
    fprintf(ctx->f, ",\n    \"storage\": {");
    mem_art_foreach(ctx->storage, alloc_storage_cb, &sctx);
    if (!sctx.first_slot)
        fprintf(ctx->f, "\n    ");
    fprintf(ctx->f, "}\n  }");

    return true;
}

void evm_state_dump_alloc_json(evm_state_t *es, const char *path) {
    if (!es || !path) return;
    FILE *f = fopen(path, "w");
    if (!f) { fprintf(stderr, "Failed to open %s\n", path); return; }
    alloc_json_ctx_t ctx = {
        .f = f,
        .storage = &es->storage,
        .first_account = true,
    };
    fprintf(f, "{\n");
    mem_art_foreach(&es->accounts, alloc_account_cb, &ctx);
    fprintf(f, "\n}\n");
    fclose(f);
}

// ============================================================================
// Address/slot collection for prestate dump
// ============================================================================

typedef struct {
    address_t *out;
    size_t count;
    size_t max;
} collect_addr_ctx_t;

static bool collect_addr_cb(const uint8_t *key, size_t key_len,
                              const void *value, size_t value_len,
                              void *user_data) {
    (void)key; (void)key_len; (void)value_len;
    collect_addr_ctx_t *ctx = (collect_addr_ctx_t *)user_data;
    const cached_account_t *ca = (const cached_account_t *)value;
    /* Only collect accounts that existed or were touched */
    if (!ca->existed && !ca->dirty && !ca->block_dirty && !ca->created)
        return true;
    if (ctx->count >= ctx->max) return false;  /* buffer full */
    memcpy(ctx->out[ctx->count].bytes, ca->addr.bytes, 20);
    ctx->count++;
    return true;
}

size_t evm_state_collect_addresses(evm_state_t *es, address_t *out, size_t max_count) {
    if (!es || !out || max_count == 0) return 0;
    collect_addr_ctx_t ctx = { .out = out, .count = 0, .max = max_count };
    mem_art_foreach(&es->accounts, collect_addr_cb, &ctx);
    return ctx.count;
}

typedef struct {
    const uint8_t *addr;
    uint256_t *out;
    size_t count;
    size_t max;
} collect_slots_ctx_t;

static bool collect_slots_cb(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len,
                               void *user_data) {
    (void)key_len; (void)value_len;
    collect_slots_ctx_t *ctx = (collect_slots_ctx_t *)user_data;
    /* key[0..19] = address, key[20..51] = slot (big-endian) */
    if (memcmp(key, ctx->addr, 20) != 0) return true;
    (void)value;
    /* Include slots that were accessed (even if zero now — they might have been non-zero before) */
    if (ctx->count >= ctx->max) return false;
    ctx->out[ctx->count] = uint256_from_bytes(key + 20, 32);
    ctx->count++;
    return true;
}

size_t evm_state_collect_storage_keys(evm_state_t *es, const address_t *addr,
                                       uint256_t *out, size_t max_count) {
    if (!es || !addr || !out || max_count == 0) return 0;
    collect_slots_ctx_t ctx = {
        .addr = addr->bytes, .out = out, .count = 0, .max = max_count
    };
    mem_art_foreach(&es->storage, collect_slots_cb, &ctx);
    return ctx.count;
}

// ============================================================================
// State history diff collection
// ============================================================================

#if defined(ENABLE_HISTORY) || defined(ENABLE_VERKLE_BUILD)
#include "state_history.h"

void evm_state_collect_block_diff(evm_state_t *es, block_diff_t *out) {
    if (!es || !out) return;

    uint16_t group_cap = 64;
    uint16_t group_count = 0;
    addr_diff_t *groups = calloc(group_cap, sizeof(addr_diff_t));
    if (!groups) return;

    /* --- Phase 1: account diffs → create groups --- */
#ifdef ENABLE_MPT
    for (size_t d = 0; d < es->dirty_accounts.count; d++) {
        const uint8_t *akey = es->dirty_accounts.keys + d * 20;
        const cached_account_t *ca = (const cached_account_t *)mem_art_get(
            &es->accounts, akey, 20, NULL);
        if (!ca) continue;
        if (!ca->block_dirty && !ca->block_code_dirty &&
            !ca->block_self_destructed && !ca->block_created)
            continue;

        bool nonce_changed = (ca->nonce != ca->original_nonce);
        bool balance_changed = !uint256_is_equal(&ca->balance, &ca->original_balance);
        bool code_changed = (memcmp(ca->code_hash.bytes, ca->original_code_hash.bytes, 32) != 0);
        bool is_created = ca->created || ca->block_created;
        bool is_destructed = ca->self_destructed || ca->block_self_destructed;
        /* block_accessed distinguishes "touched THIS block" from "dirty from
         * a previous block within the checkpoint window". Include touched-but-
         * unchanged accounts so reconstruction can set block_dirty for them. */
        bool is_touched = ca->block_accessed && !nonce_changed && !balance_changed &&
                          !code_changed && !is_created && !is_destructed;
        if (!nonce_changed && !balance_changed && !code_changed &&
            !is_created && !is_destructed && !is_touched)
            continue;

        if (group_count >= group_cap) {
            group_cap = group_cap * 2;
            groups = realloc(groups, group_cap * sizeof(addr_diff_t));
        }
        addr_diff_t *g = &groups[group_count++];
        memset(g, 0, sizeof(*g));
        g->addr = ca->addr;
        g->flags = 0;
        if (is_created) g->flags |= ACCT_DIFF_CREATED;
        if (is_destructed) g->flags |= ACCT_DIFF_DESTRUCTED;
        if (is_touched) g->flags |= ACCT_DIFF_TOUCHED;
        if (ca->block_dirty) g->flags |= ACCT_DIFF_FINAL_DIRTY;
        g->field_mask = 0;
        if (nonce_changed) {
            g->field_mask |= FIELD_NONCE;
            g->nonce = ca->nonce;
        }
        if (balance_changed) {
            g->field_mask |= FIELD_BALANCE;
            g->balance = ca->balance;
        }
        if (code_changed) {
            g->field_mask |= FIELD_CODE_HASH;
            g->code_hash = ca->code_hash;
        }
    }
#endif

    /* --- Phase 2: storage diffs → assign to groups --- */
#ifdef ENABLE_MPT
    for (size_t d = 0; d < es->dirty_slots.count; d++) {
        const uint8_t *skey = es->dirty_slots.keys + d * SLOT_KEY_SIZE;
        const cached_slot_t *cs = (const cached_slot_t *)mem_art_get(
            &es->storage, skey, SLOT_KEY_SIZE, NULL);
        if (!cs) continue;
        if (!cs->block_dirty) continue;
        if (uint256_is_equal(&cs->block_original, &cs->current))
            continue;

        /* Find existing group for this address */
        addr_diff_t *g = NULL;
        for (uint16_t i = 0; i < group_count; i++) {
            if (memcmp(groups[i].addr.bytes, skey, 20) == 0) {
                g = &groups[i];
                break;
            }
        }

        /* Create new group if address not found (storage-only change) */
        if (!g) {
            if (group_count >= group_cap) {
                group_cap = group_cap * 2;
                groups = realloc(groups, group_cap * sizeof(addr_diff_t));
            }
            g = &groups[group_count++];
            memset(g, 0, sizeof(*g));
            memcpy(g->addr.bytes, skey, 20);
        }

        /* Append slot to group */
        g->slots = realloc(g->slots, (g->slot_count + 1) * sizeof(slot_diff_t));
        slot_diff_t *s = &g->slots[g->slot_count++];
        s->slot = uint256_from_bytes(skey + 20, 32);
        s->value = cs->current;
    }
#endif

    out->groups = groups;
    out->group_count = group_count;
}
#ifdef ENABLE_HISTORY
void evm_state_apply_diff_bulk(evm_state_t *es, const block_diff_t *diff) {
    if (!es || !diff) return;

    for (uint16_t i = 0; i < diff->group_count; i++) {
        const addr_diff_t *g = &diff->groups[i];

        /* ensure_account creates or loads the cached entry */
        cached_account_t *ca = ensure_account(es, &g->addr);
        if (!ca) continue;

        if (g->flags & ACCT_DIFF_DESTRUCTED) {
            ca->nonce = 0;
            ca->balance = UINT256_ZERO_INIT;
            memset(ca->code_hash.bytes, 0, 32);
            ca->has_code = false;
            ca->storage_root = HASH_EMPTY_STORAGE;

            /* Clear mpt_dirty on all previously-dirty cached slots for this
             * account. In chain_replay, commit_tx_slot_cb zeros self-destructed
             * account slots and clears mpt_dirty. Without this, stale slot
             * values from earlier blocks survive destruction in batch mode and
             * pollute the storage trie at checkpoint time. */
            for (size_t d = 0; d < es->dirty_slots.count; d++) {
                const uint8_t *skey = es->dirty_slots.keys + d * SLOT_KEY_SIZE;
                if (memcmp(skey, g->addr.bytes, ADDRESS_SIZE) == 0) {
                    cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
                        &es->storage, skey, SLOT_KEY_SIZE, NULL);
                    if (cs) {
                        cs->current = UINT256_ZERO;
                        cs->mpt_dirty = false;
                    }
                }
            }
        }

        if (g->field_mask & FIELD_NONCE)
            ca->nonce = g->nonce;

        if (g->field_mask & FIELD_BALANCE)
            ca->balance = g->balance;

        if (g->field_mask & FIELD_CODE_HASH) {
            ca->code_hash = g->code_hash;
            ca->has_code = (memcmp(g->code_hash.bytes, HASH_EMPTY_CODE.bytes, 32) != 0 &&
                            memcmp(g->code_hash.bytes, ((hash_t){0}).bytes, 32) != 0);
            ca->code_dirty = true;
        }

        /* Determine existed/block_dirty based on final account state.
         * For DESTRUCTED accounts: mirror commit_tx behavior — if the final
         * state is empty, set existed=false and skip block_dirty so
         * compute_mpt_root deletes (or skips) instead of writing an empty
         * account to the trie. If destructed but re-created with new values
         * (field_mask non-zero → non-empty), treat as a normal account.
         *
         * FINAL_DIRTY flag: when both CREATED and DESTRUCTED are set, the
         * order matters. If the account was re-created AFTER being destructed
         * (in a later tx within the same block), block_dirty is true in
         * chain_replay at diff collection time. The FINAL_DIRTY flag captures
         * this, so we preserve block_dirty=true and let compute_mpt_root's
         * promotion logic handle it (pre-EIP-161: empty accounts get PUT). */
        bool is_empty = (ca->nonce == 0 &&
                         uint256_is_zero(&ca->balance) &&
                         !ca->has_code);
        if ((g->flags & ACCT_DIFF_DESTRUCTED) && is_empty &&
            !(g->flags & ACCT_DIFF_FINAL_DIRTY)) {
            ca->existed = false;
            ca->block_dirty = false;
            /* Must explicitly clear block_dirty — in batch mode it may
             * carry over from a prior block where this account was modified.
             * If left true, compute_mpt_root's promotion block would
             * re-set existed=true, writing an empty account to the trie. */
        } else {
            ca->existed = true;
            ca->block_dirty = true;
        }
        mark_account_mpt_dirty(es, ca);

        /* Storage slots */
        for (uint16_t j = 0; j < g->slot_count; j++) {
            cached_slot_t *cs = ensure_slot(es, &g->addr, &g->slots[j].slot);
            if (!cs) continue;
            cs->current = g->slots[j].value;
            mark_slot_mpt_dirty(es, cs);
            ca->storage_dirty = true;
        }
    }
}
#endif

#endif /* ENABLE_HISTORY || ENABLE_VERKLE_BUILD */

// Compute storage roots incrementally using shared storage mpt_store.
// Flush dirty storage slots to flat_state (which inserts into the shared
// compact_art), then compute per-account storage roots via storage_trie.
static void compute_all_storage_roots(evm_state_t *es) {
    struct timespec _sr_t0, _sr_t1;
    clock_gettime(CLOCK_MONOTONIC, &_sr_t0);

    if (!es->flat_state || !es->storage_trie) goto clear;

    // 1. Flush dirty slots to flat_state → shared compact_art
    //    (storage_trie reads values from this compact_art)
    for (size_t d = 0; d < es->dirty_slots.count; d++) {
        const uint8_t *skey = es->dirty_slots.keys + d * SLOT_KEY_SIZE;
        cached_slot_t *cs = (cached_slot_t *)mem_art_get_mut(
            &es->storage, skey, SLOT_KEY_SIZE, NULL);
        if (!cs) continue;
        if (!cs->mpt_dirty) continue;

        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &es->accounts, skey, ADDRESS_KEY_SIZE, NULL);
        if (!ca) continue;

        if (uint256_is_zero(&cs->current)) {
            flat_state_delete_storage(es->flat_state,
                                       ca->addr_hash.bytes, cs->slot_hash.bytes);
        } else {
            uint8_t vbe[32];
            uint256_to_bytes(&cs->current, vbe);
            flat_state_put_storage(es->flat_state,
                                    ca->addr_hash.bytes, cs->slot_hash.bytes, vbe);
        }

        cs->mpt_dirty = false;
        cs->block_dirty = false;
    }

    // 2. Compute storage root per dirty account via storage_trie
    for (size_t d = 0; d < es->dirty_accounts.count; d++) {
        const uint8_t *akey = es->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &es->accounts, akey, ADDRESS_KEY_SIZE, NULL);
        if (!ca || !ca->storage_dirty) continue;

        storage_trie_root(es->storage_trie, ca->addr_hash.bytes,
                           ca->storage_root.bytes);
    }

    clock_gettime(CLOCK_MONOTONIC, &_sr_t1);
    double _sr_ms = (_sr_t1.tv_sec - _sr_t0.tv_sec) * 1000.0 +
                    (_sr_t1.tv_nsec - _sr_t0.tv_nsec) / 1e6;
    if (_sr_ms > 100.0)
        fprintf(stderr, "  └ storage_roots: %.1f ms, %zu dirty slots, %zu dirty accts\n",
                _sr_ms, es->dirty_slots.count, es->dirty_accounts.count);

clear:
    // 3. Clear storage_dirty on dirty accounts
    for (size_t d = 0; d < es->dirty_accounts.count; d++) {
        const uint8_t *akey = es->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
        cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
            &es->accounts, akey, ADDRESS_KEY_SIZE, NULL);
        if (ca) ca->storage_dirty = false;
    }
}

hash_t evm_state_compute_mpt_root(evm_state_t *es, bool prune_empty) {
    hash_t root = hash_zero();
    if (!es) return root;

    if (es->account_mpt) {
        /* Reset commit stats for this root computation window */
        mpt_arena_reset_commit_stats(es->account_mpt);

        struct timespec _rt0, _rt1, _rt2;
        clock_gettime(CLOCK_MONOTONIC, &_rt0);
        compute_all_storage_roots(es);
        clock_gettime(CLOCK_MONOTONIC, &_rt1);

        if (!mpt_arena_begin_batch(es->account_mpt)) {
            fprintf(stderr, "FATAL: mpt_arena_begin_batch failed for account trie\n"
                    "  hint: another batch may already be in progress (double begin)\n");
            return root;
        }

        size_t _acct_dirty_count = es->dirty_accounts.count;
        // Iterate dirty account list directly — O(dirty) instead of O(total_cached)
        for (size_t d = 0; d < es->dirty_accounts.count; d++) {
            const uint8_t *akey = es->dirty_accounts.keys + d * ADDRESS_KEY_SIZE;
            cached_account_t *ca = (cached_account_t *)mem_art_get_mut(
                &es->accounts, akey, ADDRESS_KEY_SIZE, NULL);
            if (!ca) continue;
            if (!ca->mpt_dirty) continue;

            // Promote block_dirty accounts to existed:
            // - Self-destructed accounts: set existed = false
            // - Don't promote new empty accounts when pruning (EIP-161)
            if (ca->block_dirty || ca->block_code_dirty) {
                if (ca->self_destructed) {
                    ca->existed = false;
                } else {
                    bool is_empty_check = (ca->nonce == 0 &&
                                           uint256_is_zero(&ca->balance) &&
                                           !ca->has_code);
                    if (!(!ca->existed && !ca->created && is_empty_check && prune_empty))
                        ca->existed = true;
                }
            }

            const uint8_t *sr = ca->storage_root.bytes;
            const uint8_t *code_hash = ca->has_code
                ? ca->code_hash.bytes : HASH_EMPTY_CODE.bytes;

            bool is_empty = (ca->nonce == 0 &&
                             uint256_is_zero(&ca->balance) &&
                             !ca->has_code);

            if (!ca->existed || (is_empty && prune_empty)) {
                mpt_arena_delete(es->account_mpt, ca->addr_hash.bytes);
            } else {
                uint8_t rlp[120];
                size_t rlp_len = mpt_rlp_account(ca->nonce, &ca->balance, sr, code_hash, rlp);
                mpt_arena_update(es->account_mpt, ca->addr_hash.bytes, rlp, rlp_len);
            }

            ca->mpt_dirty = false;
            ca->block_dirty = false;
            ca->block_code_dirty = false;
        }
        /* NOTE: dirty_accounts cleared in evm_state_evict_cache after flat_state flush */

        if (!mpt_arena_commit_batch(es->account_mpt)) {
            fprintf(stderr, "FATAL: mpt_arena_commit_batch failed for account trie\n"
                    "  hint: likely OOM (deferred buffer alloc) or corrupt trie node on disk\n"
                    "  hint: delete account MPT files and replay to rebuild\n");
            return root;
        }
        clock_gettime(CLOCK_MONOTONIC, &_rt2);

        es->last_root_stor_ms = (_rt1.tv_sec - _rt0.tv_sec) * 1000.0 +
                               (_rt1.tv_nsec - _rt0.tv_nsec) / 1e6;
        es->last_root_acct_ms = (_rt2.tv_sec - _rt1.tv_sec) * 1000.0 +
                                (_rt2.tv_nsec - _rt1.tv_nsec) / 1e6;
        es->last_root_dirty_count = _acct_dirty_count;
        es->last_acct_commit = mpt_arena_get_commit_stats(es->account_mpt);

        mpt_arena_root(es->account_mpt, root.bytes);
        return root;
    }

    fprintf(stderr, "FATAL: compute_mpt_root called without mpt_store\n");
    return root;
}
#endif /* ENABLE_MPT */

// ============================================================================
// Witness Gas (EIP-4762)
// ============================================================================

uint64_t evm_state_witness_gas_access(evm_state_t *es,
                                       const uint8_t key[32],
                                       bool is_write,
                                       bool value_was_empty)
{
#ifdef ENABLE_VERKLE
    if (!es) return 0;
    return witness_gas_access_event(&es->witness_gas, key, is_write, value_was_empty);
#else
    (void)es; (void)key; (void)is_write; (void)value_was_empty;
    return 0;
#endif
}
