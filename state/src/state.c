/**
 * State — single in-memory Ethereum world state.
 *
 * Flat vector of accounts indexed by mem_art. No disk on hot path.
 * Journal for snapshot/revert. Per-account mem_art for storage.
 * Account trie (hart) for MPT root computation.
 */

#define _GNU_SOURCE
#include "state.h"
#include "code_store.h"
#include "hashed_art.h"
#include "storage_hart2.h"
#include "keccak256.h"
#include "logger.h"

#include "block_diff.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <xmmintrin.h>
#include <sys/mman.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define ACCT_INIT_CAP       4096
#define JOURNAL_INIT_CAP    256
#define SLOT_KEY_SIZE       52   /* addr[20] + slot_be[32] for originals/warm */

/* EIP-161 RIPEMD special case: address 0x0000...0003.
 * geth keeps RIPEMD dirty after journal revert so that EIP-161 prunes
 * it even when the touching CALL fails (OOG). See geth journal.go:touchChange. */
static const uint8_t RIPEMD_ADDR[20] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3
};

static const hash_t EMPTY_CODE_HASH = {{
    0xc5,0xd2,0x46,0x01,0x86,0xf7,0x23,0x3c,
    0x92,0x7e,0x7d,0xb2,0xdc,0xc7,0x03,0xc0,
    0xe5,0x00,0xb6,0x53,0xca,0x82,0x27,0x3b,
    0x7b,0xfa,0xd8,0x04,0x5d,0x85,0xa4,0x70
}};

static const hash_t EMPTY_STORAGE_ROOT = {{
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
}};

/* =========================================================================
 * Address hash cache — fixed-size direct-mapped, persistent across blocks
 * ========================================================================= */

#define ADDR_HASH_CACHE_BITS  22
#define ADDR_HASH_CACHE_SIZE  (1u << ADDR_HASH_CACHE_BITS)  /* 4M entries, ~208MB */

typedef struct {
    uint8_t addr[20];
    uint8_t hash[32];
} addr_hash_entry_t;

/* Slot hash cache: direct-mapped, indexed by first 2 bytes of slot key.
 * 64K entries × 64 bytes = 4MB — fits in L2 cache.
 * DeFi contracts hit the same ~100 slots repeatedly, so hit rate >95%. */
#define SLOT_HASH_CACHE_BITS  16
#define SLOT_HASH_CACHE_SIZE  (1u << SLOT_HASH_CACHE_BITS)  /* 64K entries, 4MB */

typedef struct {
    uint8_t slot[32];
    uint8_t hash[32];
} slot_hash_entry_t;

/* =========================================================================
 * mmap'd vector helpers — grow via mremap (no copy, no temp spike)
 * ========================================================================= */

static void *vec_alloc(size_t bytes) {
    void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    return (p == MAP_FAILED) ? NULL : p;
}

static void *vec_grow(void *old, size_t old_bytes, size_t new_bytes) {
    void *p = mremap(old, old_bytes, new_bytes, MREMAP_MAYMOVE);
    return (p == MAP_FAILED) ? NULL : p;
}

static void vec_free(void *p, size_t bytes) {
    if (p) munmap(p, bytes);
}

/* Capped-exponential grow delta: use 1.5x while small, cap at +4 GB worth
 * of items once the vector is large. Prevents 10+ GB spikes under memory
 * pressure (the 26→40 GB jump observed in v1 at mainnet scale). */
#define STATE_VEC_LINEAR_STEP (4ULL << 30)  /* 4 GB */

static inline uint32_t vec_grow_delta(uint32_t cur_cap, size_t item_size) {
    uint32_t delta_exp = cur_cap / 2;  /* 1.5x delta */
    uint32_t delta_max = (uint32_t)(STATE_VEC_LINEAR_STEP / item_size);
    return delta_exp < delta_max ? delta_exp : delta_max;
}

/* =========================================================================
 * RLP helpers (for account trie encoding)
 * ========================================================================= */

static size_t rlp_u64(uint64_t v, uint8_t *out) {
    if (v == 0) { out[0] = 0x80; return 1; }
    if (v < 0x80) { out[0] = (uint8_t)v; return 1; }
    uint8_t be[8]; int len = 0; uint64_t tmp = v;
    while (tmp > 0) { be[7 - len++] = (uint8_t)(tmp & 0xFF); tmp >>= 8; }
    out[0] = 0x80 + (uint8_t)len;
    memcpy(out + 1, be + 8 - len, len);
    return 1 + len;
}

static size_t rlp_be(const uint8_t *be, size_t be_len, uint8_t *out) {
    size_t i = 0;
    while (i < be_len && be[i] == 0) i++;
    size_t len = be_len - i;
    if (len == 0)                     { out[0] = 0x80;       return 1; }
    if (len == 1 && be[i] < 0x80)     { out[0] = be[i];      return 1; }
    out[0] = 0x80 + (uint8_t)len;
    memcpy(out + 1, be + i, len);
    return 1 + len;
}

/* Build full account RLP: list(nonce, balance, storage_root, code_hash).
 * art_mpt's encode_leaf wraps the entire output with rbuf_encode_bytes. */
static uint32_t build_account_rlp(uint64_t nonce, const uint8_t balance_be[32],
                                    const uint8_t storage_root[32],
                                    const uint8_t code_hash[32],
                                    uint8_t *rlp_out) {
    uint8_t payload[120]; size_t pos = 0;
    pos += rlp_u64(nonce, payload + pos);
    pos += rlp_be(balance_be, 32, payload + pos);
    payload[pos++] = 0xa0; memcpy(payload + pos, storage_root, 32); pos += 32;
    payload[pos++] = 0xa0; memcpy(payload + pos, code_hash, 32);   pos += 32;
    if (pos <= 55) {
        rlp_out[0] = 0xc0 + (uint8_t)pos;
        memcpy(rlp_out + 1, payload, pos);
        return 1 + (uint32_t)pos;
    }
    rlp_out[0] = 0xf8; rlp_out[1] = (uint8_t)pos;
    memcpy(rlp_out + 2, payload, pos);
    return 2 + (uint32_t)pos;
}

/* Storage value → RLP encode callback for hart_root_hash */
static uint32_t stor_value_encode(const uint8_t key[32], const void *leaf_val,
                                   uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    const uint8_t *v = (const uint8_t *)leaf_val;
    return (uint32_t)rlp_be(v, 32, rlp_out);
}

/* =========================================================================
 * Journal entry
 * ========================================================================= */

typedef enum {
    JE_NONCE,
    JE_BALANCE,
    JE_CODE,
    JE_STORAGE,
    JE_CREATE,
    JE_SELF_DESTRUCT,
    JE_TRANSIENT,
    JE_WARM_ADDR,
    JE_WARM_SLOT,
} journal_type_t;

typedef struct {
    journal_type_t type;
    address_t      addr;
    union {
        struct { uint64_t val; uint16_t flags; } nonce;
        struct { uint256_t val; uint16_t flags; } balance;
        struct { hash_t hash; uint8_t *code; uint32_t size;
                 uint16_t flags; } code;
        struct { uint256_t key; uint256_t val;
                 uint16_t flags; } storage;
        struct { uint64_t nonce; uint256_t balance; hash_t code_hash;
                 hash_t storage_root; uint8_t *code; uint32_t code_size;
                 uint16_t flags; } create;
        struct { uint16_t flags; } sd;
        struct { uint256_t key; uint256_t val; } transient;
        uint256_t warm_slot_key;    /* JE_WARM_SLOT */
    } data;
} journal_entry_t;

/* Snapshot of account fields for block undo log */
typedef struct {
    uint64_t  nonce;
    uint256_t balance;
    hash_t    code_hash;
    bool      existed;
} acct_snapshot_t;

/* =========================================================================
 * Dirty tracking
 * ========================================================================= */

typedef struct {
    uint8_t *keys;
    size_t   count;
    size_t   cap;
} dirty_vec_t;

static void dirty_push(dirty_vec_t *v, const uint8_t *key, size_t key_size) {
    if (v->count >= v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 64;
        uint8_t *nk = realloc(v->keys, nc * key_size);
        if (!nk) return;
        v->keys = nk;
        v->cap = nc;
    }
    memcpy(v->keys + v->count * key_size, key, key_size);
    v->count++;
}

static void dirty_clear(dirty_vec_t *v) { v->count = 0; }
static void dirty_free(dirty_vec_t *v) {
    free(v->keys); v->keys = NULL; v->count = v->cap = 0;
}

static void dead_vec_push(uint32_t **vec, uint32_t *count, uint32_t *cap, uint32_t idx) {
    if (*count >= *cap) {
        uint32_t nc = *cap ? *cap * 2 : 64;
        uint32_t *nv = realloc(*vec, nc * sizeof(uint32_t));
        if (!nv) return;
        *vec = nv; *cap = nc;
    }
    (*vec)[(*count)++] = idx;
}

/* =========================================================================
 * Slot key helpers (addr[20] + slot_be[32] = 52 bytes)
 * ========================================================================= */

static void make_slot_key(const address_t *addr, const uint256_t *slot,
                          uint8_t out[SLOT_KEY_SIZE]) {
    memcpy(out, addr->bytes, 20);
    uint256_to_bytes(slot, out + 20);
}

/* =========================================================================
 * Main struct
 * ========================================================================= */

struct state {
    /* Account vector + index (hart also computes MPT root directly) */
    account_t *accounts;
    uint32_t   count;       /* high-water mark (next append index) */
    uint32_t   capacity;    /* allocated slots */
    hart_t     acct_index;  /* addr_hash[32] → uint32_t idx (lookup + trie) */

    /* Account slot free list — reuse deleted slots before appending */
    uint32_t  *acct_free;
    uint32_t   acct_free_count;
    uint32_t   acct_free_cap;

    /* Resource vector (only accounts with code/storage) */
    resource_t *resources;
    uint32_t    res_count;     /* high-water mark */
    uint32_t    res_capacity;

    /* Resource slot free list */
    uint32_t  *res_free;
    uint32_t   res_free_count;
    uint32_t   res_free_cap;

    /* Code store (not owned) */
    code_store_t *code_store;

    /* Journal */
    journal_entry_t *journal;
    uint32_t journal_len;
    uint32_t journal_cap;

    /* Per-tx ephemeral state */
    mem_art_t warm_addrs;
    mem_art_t warm_slots;
    mem_art_t transient;
    mem_art_t originals;    /* EIP-2200: slot_key[52] → uint256_t */

    /* Dirty tracking */
    dirty_vec_t tx_dirty;
    dirty_vec_t blk_dirty;
    size_t      blk_dirty_cursor;  /* finalize_block processes from here */

    /* Block-level originals for undo log — capture pre-block values on first write */
    mem_art_t blk_orig_acct;  /* addr[20] → acct_snapshot_t (first account state per block) */
    mem_art_t blk_orig_stor;  /* slot_key[52] → uint256_t (first storage value per block) */

    /* Address hash cache: fixed-size direct-mapped, persistent across blocks */
    addr_hash_entry_t *addr_hash_cache;
    slot_hash_entry_t *slot_hash_cache;

    /* Dead account tracking — three categories, kept separate.
     * All checked/cleaned at compute_root and compaction time. */
    uint32_t *phantoms;          /* ensure_account touches that never got EXISTED */
    uint32_t  phantom_count;
    uint32_t  phantom_cap;
    uint32_t *destructed;        /* self-destructed in commit_tx */
    uint32_t  destructed_count;
    uint32_t  destructed_cap;
    uint32_t *pruned;            /* EIP-161 empty accounts pruned in commit_tx */
    uint32_t  pruned_count;
    uint32_t  pruned_cap;

    /* Cumulative dead count — grows across blocks, reset by state_compact */
    uint32_t  dead_total;

    /* Block state */
    uint64_t current_block;
    bool     prune_empty;

    /* Storage root dirty bitmap — 1 bit per resource index.
     * Set by finalize_block when STORAGE_DIRTY, cleared by compute_root. */
    uint8_t *stor_dirty_bits;
    uint32_t stor_dirty_cap;     /* capacity in bits (>= res_capacity) */

    /* Storage pool — hart_pool slab allocator (MAP_ANONYMOUS) */
    hart_pool_t *stor_pool;

    /* dump-prestate support: preserved dirty list + slot key tracking */
    dirty_vec_t last_dirty;       /* blk_dirty from last compute_root (addr[20]) */
    dirty_vec_t accessed_slots;   /* addr[20]+slot_be[32] = 52 bytes each */
    bool        track_accesses;   /* set before target block to enable tracking */
};

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

/* Storage root dirty bitmap helpers */
static inline void stor_dirty_set(state_t *s, uint32_t idx) {
    if (idx < s->stor_dirty_cap)
        s->stor_dirty_bits[idx / 8] |= (1u << (idx % 8));
}
static inline bool stor_dirty_test(const state_t *s, uint32_t idx) {
    return idx < s->stor_dirty_cap &&
           (s->stor_dirty_bits[idx / 8] & (1u << (idx % 8))) != 0;
}
static void stor_dirty_grow(state_t *s, uint32_t min_cap) {
    if (min_cap <= s->stor_dirty_cap) return;
    uint32_t new_cap = s->stor_dirty_cap ? s->stor_dirty_cap * 2 : 1024;
    while (new_cap < min_cap) new_cap *= 2;
    uint32_t old_bytes = (s->stor_dirty_cap + 7) / 8;
    uint32_t new_bytes = (new_cap + 7) / 8;
    uint8_t *nb = realloc(s->stor_dirty_bits, new_bytes);
    if (!nb) return;
    memset(nb + old_bytes, 0, new_bytes - old_bytes);
    s->stor_dirty_bits = nb;
    s->stor_dirty_cap = new_cap;
}

/* Cached addr → keccak256(addr). Computes once per address per block. */
static inline uint32_t addr_cache_idx(const uint8_t addr[20]) {
    return (addr[0] | ((uint32_t)addr[1] << 8) |
            ((uint32_t)addr[2] << 16) | ((uint32_t)addr[3] << 24))
           & (ADDR_HASH_CACHE_SIZE - 1);
}

static hash_t addr_hash_cached(state_t *s, const uint8_t addr[20]) {
    uint32_t idx = addr_cache_idx(addr);
    addr_hash_entry_t *e = &s->addr_hash_cache[idx];
    /* Check hit: addr matches AND hash is non-zero (empty slot has all zeros) */
    if (memcmp(e->addr, addr, 20) == 0 && (e->hash[0] | e->hash[1])) {
        hash_t h;
        memcpy(&h, e->hash, 32);
        return h;
    }
    hash_t h = hash_keccak256(addr, 20);
    memcpy(e->addr, addr, 20);
    memcpy(e->hash, h.bytes, 32);
    return h;
}

/* Cached slot → keccak256(slot). Direct-mapped.
 *
 * Slot keys come in two patterns on Ethereum:
 *   - Small-integer slots for simple state vars (storage[0], storage[N]
 *     with N < 2^16): bytes 0..29 are zero in big-endian representation.
 *   - keccak256-derived slots for mappings / arrays: uniform random bytes.
 *
 * Hashing on the first 2 bytes collides every small-integer slot to idx 0,
 * degenerating the cache to size 1 for the very pattern where it helps
 * most. Mix four uint32 words from the whole key — covers both patterns. */
static inline uint32_t slot_cache_idx(const uint8_t slot[32]) {
    uint32_t a, b, c, d;
    memcpy(&a, slot +  0, 4);
    memcpy(&b, slot +  8, 4);
    memcpy(&c, slot + 16, 4);
    memcpy(&d, slot + 28, 4);
    uint32_t x = a ^ b ^ c ^ d;
    x ^= x >> 16;
    return x & (SLOT_HASH_CACHE_SIZE - 1);
}

static hash_t slot_hash_cached(state_t *s, const uint8_t slot[32]) {
    uint32_t idx = slot_cache_idx(slot);
    slot_hash_entry_t *e = &s->slot_hash_cache[idx];
    if (memcmp(e->slot, slot, 32) == 0 && (e->hash[0] | e->hash[1])) {
        hash_t h;
        memcpy(&h, e->hash, 32);
        return h;
    }
    hash_t h = hash_keccak256(slot, 32);
    memcpy(e->slot, slot, 32);
    memcpy(e->hash, h.bytes, 32);
    return h;
}

/* Core lookup — uses pre-computed addr_hash */
static account_t *find_account_h(const state_t *s, const hash_t *addr_hash) {
    const uint32_t *pidx = (const uint32_t *)
        hart_get(&((state_t *)s)->acct_index, addr_hash->bytes);
    if (!pidx) return NULL;
    uint32_t idx = *pidx;
    if (idx >= s->count) return NULL;
    account_t *a = &((state_t *)s)->accounts[idx];
    /* Prefetch account data + resource (next cache lines to be accessed) */
    _mm_prefetch((const char *)a, _MM_HINT_T0);
    if (a->resource_idx && a->resource_idx < s->res_count)
        _mm_prefetch((const char *)&s->resources[a->resource_idx], _MM_HINT_T0);
    return a;
}

/* Convenience — uses addr_hash_cache */
static account_t *find_account(const state_t *s, const uint8_t addr[20]) {
    hash_t h = addr_hash_cached((state_t *)s, addr);
    return find_account_h(s, &h);
}

static resource_t *get_resource(const state_t *s, const account_t *a) {
    if (!a->resource_idx) return NULL;
    return &((state_t *)s)->resources[a->resource_idx];
}

static resource_t *ensure_resource(state_t *s, account_t *a) {
    if (a->resource_idx) return &s->resources[a->resource_idx];

    /* Reuse freed slot or append */
    uint32_t ridx;
    if (s->res_free_count > 0) {
        ridx = s->res_free[--s->res_free_count];
    } else {
        if (s->res_count >= s->res_capacity) {
            uint32_t nc = s->res_capacity + vec_grow_delta(s->res_capacity, sizeof(resource_t));
            if (nc < 1024) nc = 1024;
            size_t old_bytes = (size_t)s->res_capacity * sizeof(resource_t);
            size_t new_bytes = (size_t)nc * sizeof(resource_t);
            fprintf(stderr, "RES_GROW: %uM -> %uM (%.1fGB -> %.1fGB)\n",
                    s->res_capacity / 1000000, nc / 1000000,
                    (double)old_bytes / (1024*1024*1024),
                    (double)new_bytes / (1024*1024*1024));
            resource_t *nr = vec_grow(s->resources, old_bytes, new_bytes);
            if (!nr) return NULL;
            memset(nr + s->res_capacity, 0, (nc - s->res_capacity) * sizeof(resource_t));
            s->resources = nr;
            s->res_capacity = nc;
            stor_dirty_grow(s, nc);
        }
        ridx = s->res_count++;
    }
    resource_t *r = &s->resources[ridx];
    memset(r, 0, sizeof(*r));
    r->code_hash = EMPTY_CODE_HASH;
    r->storage_root = EMPTY_STORAGE_ROOT;
    a->resource_idx = ridx;
    return r;
}

/* Get or create account — uses pre-computed addr_hash */
static account_t *ensure_account_h(state_t *s, const address_t *addr,
                                    const hash_t *addr_hash) {
    account_t *existing = find_account_h(s, addr_hash);
    if (existing) {
        existing->last_access_block = s->current_block;
        return existing;
    }

    /* Reuse freed slot or append */
    uint32_t idx;
    if (s->acct_free_count > 0) {
        idx = s->acct_free[--s->acct_free_count];
    } else {
        if (s->count >= s->capacity) {
            uint32_t nc = s->capacity + vec_grow_delta(s->capacity, sizeof(account_t));
            if (nc < ACCT_INIT_CAP) nc = ACCT_INIT_CAP;
            size_t old_bytes = (size_t)s->capacity * sizeof(account_t);
            size_t new_bytes = (size_t)nc * sizeof(account_t);
            fprintf(stderr, "ACCT_GROW: %uM -> %uM (%.1fGB -> %.1fGB)\n",
                    s->capacity / 1000000, nc / 1000000,
                    (double)old_bytes / (1024*1024*1024),
                    (double)new_bytes / (1024*1024*1024));
            account_t *na = vec_grow(s->accounts, old_bytes, new_bytes);
            if (!na) return NULL;
            memset(na + s->capacity, 0, (nc - s->capacity) * sizeof(account_t));
            s->accounts = na;
            s->capacity = nc;
        }
        idx = s->count++;
    }
    account_t *a = &s->accounts[idx];
    memset(a, 0, sizeof(*a));

    address_copy(&a->addr, addr);
    a->last_access_block = s->current_block;

    hart_insert(&s->acct_index, addr_hash->bytes, &idx);

    /* Track as potential phantom — will be checked at compute_root */
    dead_vec_push(&s->phantoms, &s->phantom_count, &s->phantom_cap, idx);

    return a;
}

static account_t *ensure_account(state_t *s, const address_t *addr) {
    hash_t h = addr_hash_cached(s, addr->bytes);
    return ensure_account_h(s, addr, &h);
}

static bool journal_push(state_t *s, const journal_entry_t *entry) {
    if (s->journal_len >= s->journal_cap) {
        uint32_t nc = s->journal_cap * 2;
        journal_entry_t *nj = realloc(s->journal, nc * sizeof(*nj));
        if (!nj) return false;
        s->journal = nj;
        s->journal_cap = nc;
    }
    s->journal[s->journal_len++] = *entry;
    return true;
}

static void mark_tx_dirty(state_t *s, const address_t *addr) {
    dirty_push(&s->tx_dirty, addr->bytes, 20);
}

static void mark_blk_dirty_h(state_t *s, account_t *a, const hash_t *addr_hash) {
    if (!acct_has_flag(a, ACCT_IN_BLK_DIRTY)) {
        acct_set_flag(a, ACCT_IN_BLK_DIRTY);
        dirty_push(&s->blk_dirty, a->addr.bytes, 20);
        hart_mark_path_dirty(&s->acct_index, addr_hash->bytes);

        /* Capture pre-block account snapshot for undo log (first touch only) */
        if (!mem_art_contains(&s->blk_orig_acct, a->addr.bytes, 20)) {
            resource_t *r = get_resource(s, a);
            acct_snapshot_t snap = {
                .nonce = a->nonce,
                .balance = a->balance,
                .code_hash = (r && acct_has_flag(a, ACCT_HAS_CODE))
                             ? r->code_hash : EMPTY_CODE_HASH,
                .existed = acct_has_flag(a, ACCT_EXISTED),
            };
            mem_art_insert(&s->blk_orig_acct, a->addr.bytes, 20,
                           &snap, sizeof(snap));
        }
    }
}

static void mark_blk_dirty(state_t *s, account_t *a) {
    hash_t h = addr_hash_cached(s, a->addr.bytes);
    mark_blk_dirty_h(s, a, &h);
}

/* =========================================================================
 * Storage helpers
 * ========================================================================= */

static uint256_t storage_read(const state_t *s, const account_t *a,
                               const uint8_t slot_hash[32]) {
    resource_t *r = get_resource(s, a);
    if (!r || !s->stor_pool) return UINT256_ZERO;
    if (storage_hart_empty(&r->storage)) return UINT256_ZERO;
    uint8_t val[32];
    if (!storage_hart_get(s->stor_pool, &r->storage, slot_hash, val))
        return UINT256_ZERO;
    return uint256_from_bytes(val, 32);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/* Account trie encode callback — reads from accounts[] + resources[] */
static uint32_t acct_trie_encode(const uint8_t key[32], const void *leaf_val,
                                  uint8_t *rlp_out, void *user_ctx) {
    (void)key;
    state_t *s = user_ctx;
    uint32_t idx;
    memcpy(&idx, leaf_val, sizeof(idx));
    if (idx >= s->count) return 0;

    account_t *a = &s->accounts[idx];
    resource_t *r = get_resource(s, a);

    uint8_t bal_be[32];
    uint256_to_bytes(&a->balance, bal_be);

    uint32_t len = build_account_rlp(
        a->nonce, bal_be,
        r ? r->storage_root.bytes : EMPTY_STORAGE_ROOT.bytes,
        r && acct_has_flag(a, ACCT_HAS_CODE) ? r->code_hash.bytes
                                              : EMPTY_CODE_HASH.bytes,
        rlp_out);
    return len;
}

state_t *state_create(code_store_t *cs) {
    state_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;

    s->code_store = cs;
    s->accounts = vec_alloc(ACCT_INIT_CAP * sizeof(account_t));
    if (!s->accounts) { free(s); return NULL; }
    s->capacity = ACCT_INIT_CAP;

    hart_init(&s->acct_index, sizeof(uint32_t));
    s->addr_hash_cache = vec_alloc(ADDR_HASH_CACHE_SIZE * sizeof(addr_hash_entry_t));
    if (!s->addr_hash_cache) { vec_free(s->accounts, ACCT_INIT_CAP * sizeof(account_t)); free(s); return NULL; }
    s->slot_hash_cache = calloc(SLOT_HASH_CACHE_SIZE, sizeof(slot_hash_entry_t));
    if (!s->slot_hash_cache) { vec_free(s->addr_hash_cache, ADDR_HASH_CACHE_SIZE * sizeof(addr_hash_entry_t)); vec_free(s->accounts, ACCT_INIT_CAP * sizeof(account_t)); free(s); return NULL; }
    mem_art_init(&s->warm_addrs);
    mem_art_init(&s->warm_slots);
    mem_art_init(&s->transient);
    mem_art_init(&s->originals);
    mem_art_init(&s->blk_orig_acct);
    mem_art_init(&s->blk_orig_stor);

    s->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!s->journal) { vec_free(s->accounts, ACCT_INIT_CAP * sizeof(account_t)); free(s); return NULL; }
    s->journal_cap = JOURNAL_INIT_CAP;

    /* Reserve resource index 0 as "none" (resource_idx=0 means no resource) */
    s->resources = vec_alloc(1024 * sizeof(resource_t));
    if (!s->resources) { free(s->journal); vec_free(s->accounts, ACCT_INIT_CAP * sizeof(account_t)); free(s); return NULL; }
    s->res_capacity = 1024;
    s->res_count = 1;

    /* Storage root dirty bitmap */
    s->stor_dirty_cap = 1024;
    s->stor_dirty_bits = calloc((1024 + 7) / 8, 1);
    if (!s->stor_dirty_bits) { vec_free(s->resources, 1024 * sizeof(resource_t)); free(s->journal); vec_free(s->accounts, ACCT_INIT_CAP * sizeof(account_t)); free(s); return NULL; }

    /* Storage pool — anonymous mmap, slabs per account. No file. */
    s->stor_pool = hart_pool_create();

    return s;
}

void state_destroy(state_t *s) {
    if (!s) return;

    for (uint32_t i = 0; i < s->res_count; i++) {
        resource_t *r = &s->resources[i];
        free(r->code);
        free(r->jumpdest_bitmap);
        if (s->stor_pool && !storage_hart_empty(&r->storage))
            storage_hart_clear(s->stor_pool, &r->storage);
    }
    vec_free(s->resources, (size_t)s->res_capacity * sizeof(resource_t));
    free(s->stor_dirty_bits);
    vec_free(s->accounts, (size_t)s->capacity * sizeof(account_t));
    free(s->phantoms);
    free(s->destructed);
    free(s->pruned);
    free(s->acct_free);
    free(s->res_free);

    hart_destroy(&s->acct_index);
    vec_free(s->addr_hash_cache, ADDR_HASH_CACHE_SIZE * sizeof(addr_hash_entry_t));
    free(s->slot_hash_cache);
    mem_art_destroy(&s->warm_addrs);
    mem_art_destroy(&s->warm_slots);
    mem_art_destroy(&s->transient);
    mem_art_destroy(&s->originals);
    mem_art_destroy(&s->blk_orig_acct);
    mem_art_destroy(&s->blk_orig_stor);

    for (uint32_t i = 0; i < s->journal_len; i++) {
        if (s->journal[i].type == JE_CODE)
            free(s->journal[i].data.code.code);
        else if (s->journal[i].type == JE_CREATE)
            free(s->journal[i].data.create.code);
    }
    free(s->journal);

    dirty_free(&s->tx_dirty);
    dirty_free(&s->blk_dirty);
    dirty_free(&s->last_dirty);
    dirty_free(&s->accessed_slots);

    if (s->stor_pool) {
        hart_pool_destroy(s->stor_pool);
    }
    free(s);
}

/* =========================================================================
 * Block lifecycle
 * ========================================================================= */

void state_begin_block(state_t *s, uint64_t block_number) {
    if (s) s->current_block = block_number;
}

uint64_t state_get_block(const state_t *s) {
    return s ? s->current_block : 0;
}

void state_set_prune_empty(state_t *s, bool enabled) {
    if (s) s->prune_empty = enabled;
}

bool state_get_prune_empty(const state_t *s) {
    return s ? s->prune_empty : false;
}

/* =========================================================================
 * Account access
 * ========================================================================= */

account_t *state_get_account(state_t *s, const address_t *addr) {
    if (!s || !addr) return NULL;
    return find_account(s, addr->bytes);
}

bool state_exists(state_t *s, const address_t *addr) {
    if (!s || !addr) return false;
    account_t *a = find_account(s, addr->bytes);
    if (!a) return false;
    return acct_has_flag(a, ACCT_EXISTED) ||
           acct_has_flag(a, ACCT_CREATED) ||
           acct_has_flag(a, ACCT_DIRTY);
}

bool state_is_empty(state_t *s, const address_t *addr) {
    if (!s || !addr) return true;
    account_t *a = find_account(s, addr->bytes);
    return !a || acct_is_empty(a);
}

uint64_t state_get_nonce(state_t *s, const address_t *addr) {
    if (!s || !addr) return 0;
    account_t *a = find_account(s, addr->bytes);
    return a ? a->nonce : 0;
}

uint256_t state_get_balance(state_t *s, const address_t *addr) {
    if (!s || !addr) return UINT256_ZERO;
    account_t *a = find_account(s, addr->bytes);
    return a ? a->balance : UINT256_ZERO;
}

void state_set_nonce(state_t *s, const address_t *addr, uint64_t nonce) {
    if (!s || !addr) return;
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = ensure_account_h(s, addr, &ah);
    if (!a) return;

    /* Capture undo snapshot BEFORE modifying the value */
    mark_blk_dirty_h(s, a, &ah);

    journal_entry_t je = { .type = JE_NONCE, .addr = *addr,
        .data.nonce = { .val = a->nonce, .flags = a->flags } };
    journal_push(s, &je);

    a->nonce = nonce;
    acct_set_flag(a, ACCT_DIRTY);
    mark_tx_dirty(s, addr);
}

void state_set_balance(state_t *s, const address_t *addr, const uint256_t *bal) {
    if (!s || !addr || !bal) return;
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = ensure_account_h(s, addr, &ah);
    if (!a) return;

    mark_blk_dirty_h(s, a, &ah);

    journal_entry_t je = { .type = JE_BALANCE, .addr = *addr,
        .data.balance = { .val = a->balance, .flags = a->flags } };
    journal_push(s, &je);

    a->balance = *bal;
    acct_set_flag(a, ACCT_DIRTY);
    mark_tx_dirty(s, addr);
}

/* Raw variants for undo — skip journal, dirty tracking, block originals.
 * Only modify the value and mark acct_index path dirty for root recomputation. */
void state_set_nonce_raw(state_t *s, const address_t *addr, uint64_t nonce) {
    if (!s || !addr) return;
    account_t *a = find_account(s, addr->bytes);
    if (!a) return;
    a->nonce = nonce;
    hash_t ah = hash_keccak256(addr->bytes, 20);
    hart_mark_path_dirty(&s->acct_index, ah.bytes);
}

void state_set_balance_raw(state_t *s, const address_t *addr, const uint256_t *bal) {
    if (!s || !addr || !bal) return;
    account_t *a = find_account(s, addr->bytes);
    if (!a) return;
    a->balance = *bal;
    hash_t ah = hash_keccak256(addr->bytes, 20);
    hart_mark_path_dirty(&s->acct_index, ah.bytes);
}

void state_set_storage_raw(state_t *s, const address_t *addr,
                           const uint256_t *key, const uint256_t *value) {
    if (!s || !addr || !key || !value) return;
    account_t *a = find_account(s, addr->bytes);
    if (!a) return;
    ensure_resource(s, a);
    resource_t *r = get_resource(s, a);
    if (!r || !s->stor_pool) return;

    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = slot_hash_cached(s, slot_be);

    if (uint256_is_zero(value))
        storage_hart_del(s->stor_pool, &r->storage, slot_hash.bytes);
    else {
        uint8_t val_be[32]; uint256_to_bytes(value, val_be);
        storage_hart_put(s->stor_pool, &r->storage, slot_hash.bytes, val_be);
    }
    storage_hart_mark_dirty(s->stor_pool, &r->storage, slot_hash.bytes);

    /* Mark storage dirty for root recomputation */
    hash_t ah = hash_keccak256(addr->bytes, 20);
    hart_mark_path_dirty(&s->acct_index, ah.bytes);
}

void state_add_balance(state_t *s, const address_t *addr, const uint256_t *amount) {
    if (!s || !addr || !amount) return;
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = ensure_account_h(s, addr, &ah);
    if (!a) return;

    mark_blk_dirty_h(s, a, &ah);

    journal_entry_t je = { .type = JE_BALANCE, .addr = *addr,
        .data.balance = { .val = a->balance, .flags = a->flags } };
    journal_push(s, &je);

    a->balance = uint256_add(&a->balance, amount);
    acct_set_flag(a, ACCT_DIRTY);
    mark_tx_dirty(s, addr);
}

bool state_sub_balance(state_t *s, const address_t *addr, const uint256_t *amount) {
    if (!s || !addr || !amount) return false;
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = find_account_h(s, &ah);
    if (!a || uint256_lt(&a->balance, amount)) return false;

    mark_blk_dirty_h(s, a, &ah);

    journal_entry_t je = { .type = JE_BALANCE, .addr = *addr,
        .data.balance = { .val = a->balance, .flags = a->flags } };
    journal_push(s, &je);

    a->balance = uint256_sub(&a->balance, amount);
    acct_set_flag(a, ACCT_DIRTY);
    mark_tx_dirty(s, addr);
    return true;
}

/* =========================================================================
 * Code
 * ========================================================================= */

void state_set_code(state_t *s, const address_t *addr,
                    const uint8_t *code, uint32_t len) {
    if (!s || !addr) return;
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = ensure_account_h(s, addr, &ah);
    if (!a) return;

    mark_blk_dirty_h(s, a, &ah);

    resource_t *r = ensure_resource(s, a);
    if (!r) return;

    journal_entry_t je = { .type = JE_CODE, .addr = *addr,
        .data.code = { .hash = r->code_hash, .code = r->code,
                       .size = r->code_size, .flags = a->flags } };
    if (!journal_push(s, &je))
        free(r->code);

    r->code = NULL;
    r->code_size = 0;
    if (r->jumpdest_bitmap) { free(r->jumpdest_bitmap); r->jumpdest_bitmap = NULL; }

    if (code && len > 0) {
        r->code = malloc(len);
        if (r->code) { memcpy(r->code, code, len); r->code_size = len; }
        acct_set_flag(a, ACCT_HAS_CODE);
        r->code_hash = hash_keccak256(code, len);
        if (s->code_store)
            code_store_put(s->code_store, r->code_hash.bytes, code, len);
    } else {
        acct_clear_flag(a, ACCT_HAS_CODE);
        r->code_hash = EMPTY_CODE_HASH;
    }

    acct_set_flag(a, ACCT_DIRTY);
    mark_tx_dirty(s, addr);
}

void state_set_code_hash(state_t *s, const address_t *addr,
                         const hash_t *code_hash) {
    if (!s || !addr || !code_hash) return;
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = ensure_account_h(s, addr, &ah);
    if (!a) return;

    mark_blk_dirty_h(s, a, &ah);
    resource_t *r = ensure_resource(s, a);
    if (!r) return;

    /* Journal for undo support — preserves bytes if any were attached. */
    journal_entry_t je = { .type = JE_CODE, .addr = *addr,
        .data.code = { .hash = r->code_hash, .code = r->code,
                       .size = r->code_size, .flags = a->flags } };
    if (!journal_push(s, &je))
        free(r->code);

    r->code = NULL;
    r->code_size = 0;
    if (r->jumpdest_bitmap) { free(r->jumpdest_bitmap); r->jumpdest_bitmap = NULL; }
    r->code_hash = *code_hash;

    bool is_empty = memcmp(code_hash->bytes, EMPTY_CODE_HASH.bytes, 32) == 0;
    if (is_empty) acct_clear_flag(a, ACCT_HAS_CODE);
    else          acct_set_flag(a, ACCT_HAS_CODE);

    acct_set_flag(a, ACCT_DIRTY);
    mark_tx_dirty(s, addr);
}

const uint8_t *state_get_code(state_t *s, const address_t *addr, uint32_t *out_len) {
    if (!s || !addr) { if (out_len) *out_len = 0; return NULL; }
    account_t *a = find_account(s, addr->bytes);
    if (!a || !acct_has_flag(a, ACCT_HAS_CODE)) { if (out_len) *out_len = 0; return NULL; }

    resource_t *r = get_resource(s, a);
    if (!r) { if (out_len) *out_len = 0; return NULL; }

    /* Load from code_store if not cached */
    if (!r->code && s->code_store) {
        uint32_t size = code_store_get_size(s->code_store, r->code_hash.bytes);
        if (size > 0) {
            r->code = malloc(size);
            if (r->code) {
                r->code_size = code_store_get(s->code_store, r->code_hash.bytes,
                                               r->code, size);
            }
        }
    }
    if (out_len) *out_len = r->code_size;
    return r->code;
}

const uint8_t *state_get_jumpdest_bitmap(state_t *s, const address_t *addr) {
    if (!s || !addr) return NULL;
    account_t *a = find_account(s, addr->bytes);
    if (!a || !acct_has_flag(a, ACCT_HAS_CODE)) return NULL;
    resource_t *r = get_resource(s, a);
    if (!r || !r->code || r->code_size == 0) return NULL;

    if (!r->jumpdest_bitmap) {
        size_t bitmap_bytes = (r->code_size + 7) / 8;
        r->jumpdest_bitmap = calloc(bitmap_bytes, 1);
        if (!r->jumpdest_bitmap) return NULL;
        uint64_t pc = 0;
        while (pc < r->code_size) {
            uint8_t op = r->code[pc];
            if (op == 0x5b) /* JUMPDEST */
                r->jumpdest_bitmap[pc >> 3] |= (1u << (pc & 7));
            if (op >= 0x60 && op <= 0x7f)
                pc += 1 + (op - 0x60 + 1);
            else
                pc++;
        }
    }
    return r->jumpdest_bitmap;
}

uint32_t state_get_code_size(state_t *s, const address_t *addr) {
    if (!s || !addr) return 0;
    account_t *a = find_account(s, addr->bytes);
    if (!a || !acct_has_flag(a, ACCT_HAS_CODE)) return 0;
    resource_t *r = get_resource(s, a);
    if (!r) return 0;
    if (r->code_size > 0) return r->code_size;
    state_get_code(s, addr, NULL);
    return r ? r->code_size : 0;
}

hash_t state_get_code_hash(state_t *s, const address_t *addr) {
    if (!s || !addr) return EMPTY_CODE_HASH;
    account_t *a = find_account(s, addr->bytes);
    if (!a || !acct_has_flag(a, ACCT_HAS_CODE)) return EMPTY_CODE_HASH;
    resource_t *r = get_resource(s, a);
    return r ? r->code_hash : EMPTY_CODE_HASH;
}

/* =========================================================================
 * Storage
 * ========================================================================= */

uint256_t state_get_storage(state_t *s, const address_t *addr, const uint256_t *key) {
    if (!s || !addr || !key) return UINT256_ZERO;
    if (s->track_accesses) {
        uint8_t sk[SLOT_KEY_SIZE];
        make_slot_key(addr, key, sk);
        dirty_push(&s->accessed_slots, sk, SLOT_KEY_SIZE);
    }
    account_t *a = find_account(s, addr->bytes);
    if (!a) return UINT256_ZERO;
    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = slot_hash_cached(s, slot_be);
    return storage_read(s, a, slot_hash.bytes);
}

void state_set_storage(state_t *s, const address_t *addr,
                       const uint256_t *key, const uint256_t *value) {
    if (!s || !addr || !key || !value) return;
    if (s->track_accesses) {
        uint8_t sk[SLOT_KEY_SIZE];
        make_slot_key(addr, key, sk);
        dirty_push(&s->accessed_slots, sk, SLOT_KEY_SIZE);
    }
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = ensure_account_h(s, addr, &ah);
    if (!a) return;

    /* Capture undo snapshot BEFORE any modifications */
    mark_blk_dirty_h(s, a, &ah);

    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = slot_hash_cached(s, slot_be);

    uint256_t old_value = storage_read(s, a, slot_hash.bytes);

    journal_entry_t je = { .type = JE_STORAGE, .addr = *addr,
        .data.storage = { .key = *key, .val = old_value, .flags = a->flags } };
    journal_push(s, &je);

    /* Save original for EIP-2200 (per-tx) */
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    if (!mem_art_contains(&s->originals, skey, SLOT_KEY_SIZE))
        mem_art_insert(&s->originals, skey, SLOT_KEY_SIZE,
                       &old_value, sizeof(uint256_t));

    /* Save block-level original for undo log (first write per block) */
    if (!mem_art_contains(&s->blk_orig_stor, skey, SLOT_KEY_SIZE))
        mem_art_insert(&s->blk_orig_stor, skey, SLOT_KEY_SIZE,
                       &old_value, sizeof(uint256_t));

    /* Write to storage */
    ensure_resource(s, a);
    resource_t *r = get_resource(s, a);
    if (!r || !s->stor_pool) {
        LOG_ERROR("storage create failed for %02x%02x..%02x%02x",
                  addr->bytes[0], addr->bytes[1], addr->bytes[18], addr->bytes[19]);
        return;
    }

    uint8_t val_be[32];
    uint256_to_bytes(value, val_be);
    if (uint256_is_zero(value))
        storage_hart_del(s->stor_pool, &r->storage, slot_hash.bytes);
    else
        storage_hart_put(s->stor_pool, &r->storage, slot_hash.bytes, val_be);

    acct_set_flag(a, ACCT_STORAGE_DIRTY);
    mark_tx_dirty(s, addr);
}

bool state_has_storage(state_t *s, const address_t *addr) {
    if (!s || !addr) return false;
    account_t *a = find_account(s, addr->bytes);
    if (!a) return false;
    resource_t *r = get_resource(s, a);
    if (r && memcmp(r->storage_root.bytes, EMPTY_STORAGE_ROOT.bytes, 32) != 0)
        return true;
    if (r && !storage_hart_empty(&r->storage))
        return true;
    return false;
}

/* =========================================================================
 * SLOAD / SSTORE combined lookups
 * ========================================================================= */

uint256_t state_sload(state_t *s, const address_t *addr,
                      const uint256_t *key, bool *was_warm) {
    if (!s || !addr || !key) { if (was_warm) *was_warm = false; return UINT256_ZERO; }
    if (was_warm) {
        uint8_t skey[SLOT_KEY_SIZE];
        make_slot_key(addr, key, skey);
        *was_warm = mem_art_contains(&s->warm_slots, skey, SLOT_KEY_SIZE);
    }
    return state_get_storage(s, addr, key);
}

void state_sstore_lookup(state_t *s, const address_t *addr,
                         const uint256_t *key,
                         uint256_t *current, uint256_t *original,
                         bool *was_warm) {
    if (!s || !addr || !key) {
        if (current) *current = UINT256_ZERO;
        if (original) *original = UINT256_ZERO;
        if (was_warm) *was_warm = false;
        return;
    }
    if (current) *current = state_get_storage(s, addr, key);
    if (original) {
        uint8_t skey[SLOT_KEY_SIZE];
        make_slot_key(addr, key, skey);
        const uint256_t *orig = (const uint256_t *)
            mem_art_get(&s->originals, skey, SLOT_KEY_SIZE, NULL);
        *original = orig ? *orig : state_get_storage(s, addr, key);
    }
    if (was_warm) {
        uint8_t skey[SLOT_KEY_SIZE];
        make_slot_key(addr, key, skey);
        *was_warm = mem_art_contains(&s->warm_slots, skey, SLOT_KEY_SIZE);
    }
}

/* =========================================================================
 * EIP-2929 warm/cold
 * ========================================================================= */

void state_mark_addr_warm(state_t *s, const address_t *addr) {
    if (!s || !addr) return;
    if (mem_art_contains(&s->warm_addrs, addr->bytes, 20)) return; /* already warm */
    uint8_t one = 1;
    mem_art_insert(&s->warm_addrs, addr->bytes, 20, &one, 1);
    journal_entry_t je = { .type = JE_WARM_ADDR, .addr = *addr };
    journal_push(s, &je);
}

bool state_is_addr_warm(const state_t *s, const address_t *addr) {
    if (!s || !addr) return false;
    return mem_art_contains(&((state_t *)s)->warm_addrs, addr->bytes, 20);
}

void state_mark_storage_warm(state_t *s, const address_t *addr, const uint256_t *key) {
    if (!s || !addr || !key) return;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    if (mem_art_contains(&s->warm_slots, skey, SLOT_KEY_SIZE)) return; /* already warm */
    uint8_t one = 1;
    mem_art_insert(&s->warm_slots, skey, SLOT_KEY_SIZE, &one, 1);
    journal_entry_t je = { .type = JE_WARM_SLOT, .addr = *addr,
        .data.warm_slot_key = *key };
    journal_push(s, &je);
}

bool state_is_storage_warm(const state_t *s, const address_t *addr, const uint256_t *key) {
    if (!s || !addr || !key) return false;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    return mem_art_contains(&((state_t *)s)->warm_slots, skey, SLOT_KEY_SIZE);
}

/* =========================================================================
 * EIP-1153 transient storage
 * ========================================================================= */

uint256_t state_tload(state_t *s, const address_t *addr, const uint256_t *key) {
    if (!s || !addr || !key) return UINT256_ZERO;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    const uint256_t *val = (const uint256_t *)
        mem_art_get(&s->transient, skey, SLOT_KEY_SIZE, NULL);
    return val ? *val : UINT256_ZERO;
}

void state_tstore(state_t *s, const address_t *addr,
                  const uint256_t *key, const uint256_t *value) {
    if (!s || !addr || !key || !value) return;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);

    /* Journal old value for revert */
    uint256_t old_val = UINT256_ZERO;
    const uint256_t *existing = (const uint256_t *)
        mem_art_get(&s->transient, skey, SLOT_KEY_SIZE, NULL);
    if (existing) old_val = *existing;

    journal_entry_t je = { .type = JE_TRANSIENT, .addr = *addr,
        .data.transient = { .key = *key, .val = old_val } };
    journal_push(s, &je);

    mem_art_insert(&s->transient, skey, SLOT_KEY_SIZE, value, sizeof(uint256_t));
}

/* =========================================================================
 * Snapshot / Revert
 * ========================================================================= */

uint32_t state_snapshot(state_t *s) {
    return s ? s->journal_len : 0;
}

void state_revert(state_t *s, uint32_t snap) {
    if (!s || snap > s->journal_len) return;

    for (uint32_t i = s->journal_len; i > snap; i--) {
        journal_entry_t *je = &s->journal[i - 1];
        account_t *a = find_account(s, je->addr.bytes);

        switch (je->type) {
        case JE_NONCE:
            if (a) {
                a->nonce = je->data.nonce.val;
                /* RIPEMD (0x03): keep dirty flags so EIP-161 can prune after OOG revert.
                 * See geth journal.go:touchChange. Tested to 4M+ blocks (2c08d3a). */
                if (s->prune_empty && memcmp(je->addr.bytes, RIPEMD_ADDR, 20) == 0)
                    a->flags = je->data.nonce.flags | (a->flags & (ACCT_DIRTY | ACCT_IN_BLK_DIRTY));
                else
                    a->flags = je->data.nonce.flags;
            }
            break;
        case JE_BALANCE:
            if (a) {
                a->balance = je->data.balance.val;
                if (s->prune_empty && memcmp(je->addr.bytes, RIPEMD_ADDR, 20) == 0)
                    a->flags = je->data.balance.flags | (a->flags & (ACCT_DIRTY | ACCT_IN_BLK_DIRTY));
                else
                    a->flags = je->data.balance.flags;
            }
            break;
        case JE_CODE:
            if (a) {
                resource_t *r = get_resource(s, a);
                if (r) {
                    free(r->code);
                    r->code = je->data.code.code;
                    r->code_size = je->data.code.size;
                    r->code_hash = je->data.code.hash;
                }
                a->flags = je->data.code.flags;
            }
            je->data.code.code = NULL;
            break;
        case JE_STORAGE: {
            if (a) {
                resource_t *r = get_resource(s, a);
                uint8_t slot_be[32]; uint256_to_bytes(&je->data.storage.key, slot_be);
                hash_t slot_hash = slot_hash_cached(s, slot_be);
                if (r && s->stor_pool) {
                    if (uint256_is_zero(&je->data.storage.val))
                        storage_hart_del(s->stor_pool, &r->storage, slot_hash.bytes);
                    else {
                        uint8_t val_be[32];
                        uint256_to_bytes(&je->data.storage.val, val_be);
                        storage_hart_put(s->stor_pool, &r->storage, slot_hash.bytes, val_be);
                    }
                }
                a->flags = je->data.storage.flags;
            }
            break;
        }
        case JE_CREATE:
            if (a) {
                resource_t *r = get_resource(s, a);
                a->nonce = je->data.create.nonce;
                a->balance = je->data.create.balance;
                a->flags = je->data.create.flags;
                if (r) {
                    r->code_hash = je->data.create.code_hash;
                    r->storage_root = je->data.create.storage_root;
                    free(r->code);
                    r->code = je->data.create.code;
                    r->code_size = je->data.create.code_size;
                }
            }
            je->data.create.code = NULL;
            break;
        case JE_SELF_DESTRUCT:
            if (a) a->flags = je->data.sd.flags;
            break;
        case JE_TRANSIENT: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.transient.key, skey);
            if (uint256_is_zero(&je->data.transient.val))
                mem_art_delete(&s->transient, skey, SLOT_KEY_SIZE);
            else
                mem_art_insert(&s->transient, skey, SLOT_KEY_SIZE,
                               &je->data.transient.val, sizeof(uint256_t));
            break;
        }
        case JE_WARM_ADDR:
            mem_art_delete(&s->warm_addrs, je->addr.bytes, 20);
            break;
        case JE_WARM_SLOT: {
            uint8_t skey[SLOT_KEY_SIZE];
            make_slot_key(&je->addr, &je->data.warm_slot_key, skey);
            mem_art_delete(&s->warm_slots, skey, SLOT_KEY_SIZE);
            break;
        }
        }
    }
    s->journal_len = snap;
}

/* =========================================================================
 * Account lifecycle
 * ========================================================================= */

void state_create_account(state_t *s, const address_t *addr) {
    if (!s || !addr) return;
    hash_t ah = addr_hash_cached(s, addr->bytes);
    account_t *a = ensure_account_h(s, addr, &ah);
    if (!a) return;

    mark_blk_dirty_h(s, a, &ah);

    resource_t *r = get_resource(s, a);
    journal_entry_t je = { .type = JE_CREATE, .addr = *addr,
        .data.create = {
            .nonce = a->nonce, .balance = a->balance,
            .code_hash = r ? r->code_hash : EMPTY_CODE_HASH,
            .storage_root = r ? r->storage_root : EMPTY_STORAGE_ROOT,
            .code = r ? r->code : NULL,
            .code_size = r ? r->code_size : 0,
            .flags = a->flags
        } };
    if (!journal_push(s, &je) && r)
        free(r->code);

    uint256_t bal = a->balance; /* preserve balance */
    a->nonce = 0;
    a->balance = bal;
    a->flags = ACCT_CREATED | ACCT_DIRTY |
               ACCT_STORAGE_DIRTY;

    if (r) {
        r->code = NULL;
        r->code_size = 0;
        r->code_hash = EMPTY_CODE_HASH;
        r->storage_root = EMPTY_STORAGE_ROOT;
        if (s->stor_pool && !storage_hart_empty(&r->storage))
            storage_hart_clear(s->stor_pool, &r->storage);
    }

    mark_tx_dirty(s, addr);
}

void state_self_destruct(state_t *s, const address_t *addr) {
    if (!s || !addr) return;
    account_t *a = ensure_account(s, addr);
    if (!a) return;

    journal_entry_t je = { .type = JE_SELF_DESTRUCT, .addr = *addr,
        .data.sd = { .flags = a->flags } };
    journal_push(s, &je);

    acct_set_flag(a, ACCT_SELF_DESTRUCTED);
}

/* =========================================================================
 * Commit TX
 * ========================================================================= */

void state_commit_tx(state_t *s) {
    if (!s) return;

    for (size_t i = 0; i < s->tx_dirty.count; i++) {
        const uint8_t *akey = s->tx_dirty.keys + i * 20;
        account_t *a = find_account(s, akey);
        if (!a) continue;

        if (acct_has_flag(a, ACCT_SELF_DESTRUCTED)) {
            a->balance = UINT256_ZERO;
            a->nonce = 0;
            acct_clear_flag(a, ACCT_HAS_CODE);
            acct_clear_flag(a, ACCT_EXISTED | ACCT_CREATED | ACCT_DIRTY |
                           ACCT_SELF_DESTRUCTED);
            acct_set_flag(a, ACCT_STORAGE_DIRTY);

            resource_t *r = get_resource(s, a);
            if (r) {
                r->code_hash = EMPTY_CODE_HASH;
                if (r->code) { free(r->code); r->code = NULL; }
                r->code_size = 0;
                r->storage_root = EMPTY_STORAGE_ROOT;
                if (s->stor_pool && !storage_hart_empty(&r->storage))
                    storage_hart_clear(s->stor_pool, &r->storage);
            }
            mark_blk_dirty(s, a);
            dead_vec_push(&s->destructed, &s->destructed_count, &s->destructed_cap,
                         (uint32_t)(a - s->accounts));
            continue;
        }

        bool empty = acct_is_empty(a);
        bool touched = acct_has_flag(a, ACCT_EXISTED | ACCT_CREATED |
                                    ACCT_DIRTY);
        if (touched && (!empty || !s->prune_empty))
            acct_set_flag(a, ACCT_EXISTED);

        if (s->prune_empty && empty &&
            acct_has_flag(a, ACCT_DIRTY)) {
            acct_clear_flag(a, ACCT_EXISTED);
            dead_vec_push(&s->pruned, &s->pruned_count, &s->pruned_cap,
                         (uint32_t)(a - s->accounts));
            mark_blk_dirty(s, a);
        }

        acct_clear_flag(a, ACCT_CREATED | ACCT_DIRTY |
                        ACCT_SELF_DESTRUCTED);
    }

    dirty_clear(&s->tx_dirty);
    s->journal_len = 0;

    mem_art_destroy(&s->warm_addrs);  mem_art_init(&s->warm_addrs);
    mem_art_destroy(&s->warm_slots);  mem_art_init(&s->warm_slots);
    mem_art_destroy(&s->transient);   mem_art_init(&s->transient);
    mem_art_destroy(&s->originals);   mem_art_init(&s->originals);
}

/* =========================================================================
 * Commit Block
 * ========================================================================= */

void state_commit_block(state_t *s) {
    if (!s) return;

    for (size_t d = s->blk_dirty_cursor; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        account_t *a = find_account(s, akey);
        if (!a) continue;

        bool empty = acct_is_empty(a);
        bool touched = acct_has_flag(a, ACCT_EXISTED | ACCT_CREATED |
                                    ACCT_DIRTY);
        if (touched && (!empty || !s->prune_empty))
            acct_set_flag(a, ACCT_EXISTED);

        acct_clear_flag(a, ACCT_CREATED | ACCT_DIRTY |
                        ACCT_SELF_DESTRUCTED);
    }

    s->journal_len = 0;
    mem_art_destroy(&s->originals);
    mem_art_init(&s->originals);
}

/* =========================================================================
 * Clear prestate dirty (for test runner)
 * ========================================================================= */

void state_clear_prestate_dirty(state_t *s) {
    if (!s) return;

    for (uint32_t i = 0; i < s->count; i++) {
        account_t *a = &s->accounts[i];

        /* Compute storage roots for prestate accounts that have storage.
         * Without this, acct_trie_encode would see EMPTY_STORAGE_ROOT
         * for prestate accounts whose storage isn't modified by the tx. */
        resource_t *r = get_resource(s, a);
        if (r && s->stor_pool && !storage_hart_empty(&r->storage)) {
            storage_hart_root_hash(s->stor_pool, &r->storage,
                                   stor_value_encode, NULL, r->storage_root.bytes);
        }

        /* Mark prestate accounts as existing — they're in the state trie.
         * Required for correct EIP-161 pruning decisions in commit_tx. */
        acct_set_flag(a, ACCT_EXISTED);

        acct_clear_flag(a, ACCT_DIRTY | ACCT_IN_BLK_DIRTY | ACCT_STORAGE_DIRTY);
    }

    dirty_clear(&s->blk_dirty);
    s->blk_dirty_cursor = 0;
    dirty_clear(&s->tx_dirty);
    s->journal_len = 0;
    mem_art_destroy(&s->originals);
    mem_art_init(&s->originals);

    /* Clear dead lists accumulated during prestate loading —
     * prestate accounts are NOT phantoms/pruned/destructed. */
    s->phantom_count = 0;
    s->destructed_count = 0;
    s->pruned_count = 0;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

hart_pool_t *state_get_storage_pool(const state_t *s) {
    return s ? s->stor_pool : NULL;
}

state_stats_t state_get_stats(const state_t *s) {
    state_stats_t st = {0};
    if (!s) return st;
    st.account_count = s->count;
    st.account_live = (uint32_t)hart_size(&s->acct_index);
    st.acct_free_count = s->acct_free_count;
    st.storage_account_count = s->res_count;
    st.res_free_count = s->res_free_count;

    st.acct_vec_bytes = (size_t)s->capacity * sizeof(account_t);
    st.res_vec_bytes = (size_t)s->res_capacity * sizeof(resource_t);
    st.acct_arena_bytes = s->acct_index.arena_cap;

    st.total_tracked = st.acct_vec_bytes + st.res_vec_bytes +
                       st.acct_arena_bytes;

    return st;
}

/* =========================================================================
 * compute_root — TODO: implement with account trie
 * save/load — TODO: implement serialization
 * ========================================================================= */

/* Worker: invalidate a contiguous range of storage-hart resources. Each
 * resource's invalidate is independent of every other's, so this shards
 * trivially across threads. Writes to disjoint r->storage entries + the
 * global stor_dirty_bits bitmap (word-level atomic: different threads
 * touch different byte ranges of the bitmap when ranges don't overlap). */
typedef struct {
    state_t  *s;
    uint32_t  start;   /* inclusive */
    uint32_t  end;     /* exclusive */
} stor_invalidate_worker_t;

static void *stor_invalidate_worker_fn(void *arg) {
    stor_invalidate_worker_t *w = (stor_invalidate_worker_t *)arg;
    for (uint32_t i = w->start; i < w->end; i++) {
        resource_t *r = &w->s->resources[i];
        if (!storage_hart_empty(&r->storage))
            storage_hart_invalidate(w->s->stor_pool, &r->storage);
    }
    return NULL;
}

void state_invalidate_all(state_t *s) {
    if (!s) return;

    /* Account trie: parallel walk. */
    hart_invalidate_all_parallel(&s->acct_index);

    /* Storage harts: grow the bitmap, then shard invalidates across threads.
     * stor_dirty_set is done serially on the main thread after the join —
     * sets every bit in the resource range, bypassing the need for workers
     * to contend on the bitmap. */
    stor_dirty_grow(s, s->res_count);

    const uint32_t nres = s->res_count;
    if (nres > 1) {
        /* Parallel above a worthwhile threshold — otherwise serial overhead
         * (pthread_create/join) dominates. 4 threads matches the account-
         * trie parallel structure and mainnet-state cache behavior. */
        enum { NTHR = 4 };
        const uint32_t work = nres - 1;  /* idx 0 is reserved/unused */
        if (work >= NTHR * 8) {
            pthread_t tids[NTHR];
            stor_invalidate_worker_t w[NTHR];
            uint32_t chunk = work / NTHR;
            for (int t = 0; t < NTHR; t++) {
                w[t].s = s;
                w[t].start = 1 + t * chunk;
                w[t].end   = (t == NTHR - 1) ? nres : 1 + (t + 1) * chunk;
                pthread_create(&tids[t], NULL, stor_invalidate_worker_fn, &w[t]);
            }
            for (int t = 0; t < NTHR; t++)
                pthread_join(tids[t], NULL);
        } else {
            for (uint32_t i = 1; i < nres; i++) {
                resource_t *r = &s->resources[i];
                if (!storage_hart_empty(&r->storage))
                    storage_hart_invalidate(s->stor_pool, &r->storage);
            }
        }
    }

    /* Bitmap update is serial: fast (memset of the byte range) and avoids
     * writer-writer races on the shared stor_dirty_bits buffer. */
    for (uint32_t i = 1; i < nres; i++)
        stor_dirty_set(s, i);
}

/* =========================================================================
 * Pre-block value queries (for dump-prestate without temp file)
 *
 * After block execution with keep_undo=true, blk_orig_acct/blk_orig_stor
 * contain pre-block values for modified accounts/slots.
 * For unmodified accounts, the current state IS the pre-block state.
 * ========================================================================= */

bool state_get_preblock_account(const state_t *s, const address_t *addr,
                                uint64_t *nonce, uint256_t *balance) {
    if (!s || !addr) return false;
    const acct_snapshot_t *snap = (const acct_snapshot_t *)
        mem_art_get(&((state_t *)s)->blk_orig_acct, addr->bytes, 20, NULL);
    if (snap) {
        if (nonce) *nonce = snap->nonce;
        if (balance) *balance = snap->balance;
        return true;
    }
    /* Not modified this block — current state is pre-block state */
    const account_t *a = find_account(s, addr->bytes);
    if (!a) { if (nonce) *nonce = 0; if (balance) *balance = UINT256_ZERO; return false; }
    if (nonce) *nonce = a->nonce;
    if (balance) *balance = a->balance;
    return true;
}

uint256_t state_get_preblock_storage(const state_t *s, const address_t *addr,
                                      const uint256_t *key) {
    if (!s || !addr || !key) return UINT256_ZERO;
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    size_t vlen = 0;
    const uint256_t *orig = (const uint256_t *)
        mem_art_get(&((state_t *)s)->blk_orig_stor, skey, SLOT_KEY_SIZE, &vlen);
    if (orig && vlen == sizeof(uint256_t))
        return *orig;
    /* Not modified this block — current state is pre-block state */
    return state_get_storage((state_t *)s, addr, key);
}

/**
 * Compute Merkle root. Requires state_finalize_block() was called for
 * every block since the last root computation.
 *
 *   1. Recompute dirty storage roots (tracked via bitmap)
 *   2. Compute hart_root_hash
 */
/* -------------------------------------------------------------------------
 * Parallel storage root computation
 * ------------------------------------------------------------------------- */

#include <sched.h>

typedef struct {
    state_t            *s;
    uint32_t            start_byte;  /* bitmap byte range [start, end) */
    uint32_t            end_byte;
    uint32_t            computed;    /* count of storage roots computed */
} stor_root_worker_t;

static void *stor_root_worker_fn(void *arg) {
    stor_root_worker_t *w = (stor_root_worker_t *)arg;
    state_t *s = w->s;
    uint32_t computed = 0;

    for (uint32_t b = w->start_byte; b < w->end_byte; b++) {
        if (!s->stor_dirty_bits[b]) continue;
        for (uint32_t bit = 0; bit < 8; bit++) {
            if (!(s->stor_dirty_bits[b] & (1u << bit))) continue;
            uint32_t idx = b * 8 + bit;
            if (idx == 0 || idx >= s->res_count) continue;
            resource_t *r = &s->resources[idx];
            if (!storage_hart_empty(&r->storage))
                storage_hart_root_hash(s->stor_pool, &r->storage,
                                       stor_value_encode, NULL,
                                       r->storage_root.bytes);
            else
                r->storage_root = EMPTY_STORAGE_ROOT;
            computed++;
        }
    }
    w->computed = computed;
    return NULL;
}

/* Number of worker threads for parallel storage root computation.
 * Only used when dirty count is high enough to justify threading. */
#define STOR_ROOT_PAR_THRESH  64   /* min dirty resources to go parallel */

hash_t state_compute_root(state_t *s, bool prune_empty) {
    (void)prune_empty;
    hash_t root = {0};
    if (!s) return root;

    uint32_t bitmap_bytes = (s->stor_dirty_cap + 7) / 8;

    /* Count dirty resources to decide serial vs parallel */
    uint32_t dirty_count = 0;
    for (uint32_t b = 0; b < bitmap_bytes; b++) {
        if (s->stor_dirty_bits[b]) {
            /* popcount of byte */
            uint8_t v = s->stor_dirty_bits[b];
            while (v) { dirty_count++; v &= v - 1; }
        }
    }

    if (dirty_count >= STOR_ROOT_PAR_THRESH) {
        /* Adaptive thread count: 4 threads for 64-255 dirty, 8 for 256+ */
        int nthreads = (dirty_count >= 256) ? 8 : 4;
        if (nthreads > (int)bitmap_bytes) nthreads = (int)bitmap_bytes;

        pthread_t tids[8];
        stor_root_worker_t workers[8];
        uint32_t chunk = bitmap_bytes / nthreads;

        for (int t = 0; t < nthreads; t++) {
            workers[t].s = s;
            workers[t].start_byte = t * chunk;
            workers[t].end_byte = (t == nthreads - 1) ? bitmap_bytes : (t + 1) * chunk;
            workers[t].computed = 0;
            pthread_create(&tids[t], NULL, stor_root_worker_fn, &workers[t]);
        }

        uint32_t total = 0;
        for (int t = 0; t < nthreads; t++) {
            pthread_join(tids[t], NULL);
            total += workers[t].computed;
        }

        if (total > 0)
            fprintf(stderr, "  storage roots: %u computed (%d threads)\n",
                    total, nthreads);
    } else {
        /* Serial: small number of dirty resources */
        for (uint32_t b = 0; b < bitmap_bytes; b++) {
            if (!s->stor_dirty_bits[b]) continue;
            for (uint32_t bit = 0; bit < 8; bit++) {
                if (!(s->stor_dirty_bits[b] & (1u << bit))) continue;
                uint32_t idx = b * 8 + bit;
                if (idx == 0 || idx >= s->res_count) continue;
                resource_t *r = &s->resources[idx];
                if (!storage_hart_empty(&r->storage))
                    storage_hart_root_hash(s->stor_pool, &r->storage,
                                           stor_value_encode, NULL,
                                           r->storage_root.bytes);
                else
                    r->storage_root = EMPTY_STORAGE_ROOT;
            }
        }
    }
    memset(s->stor_dirty_bits, 0, bitmap_bytes);

    /* Compute account trie root — parallel for large trees (4 threads on 16 hi-nibble groups) */
    hart_root_hash_parallel(&s->acct_index, acct_trie_encode, s, root.bytes);

    return root;
}

/* Backward-compat wrapper — compute_hash param ignored. */
hash_t state_compute_root_ex(state_t *s, bool prune_empty, bool _unused) {
    (void)_unused;
    return state_compute_root(s, prune_empty);
}

void state_finalize_block(state_t *s, bool prune_empty) {
    if (!s) return;

    /* Per-block state processing: promote/demote existence, delete dead
     * accounts from acct_index, mark stale storage roots, clear flags. */
    for (size_t d = 0; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        hash_t ah = addr_hash_cached(s, akey);
        account_t *a = find_account_h(s, &ah);
        if (!a || !acct_has_flag(a, ACCT_IN_BLK_DIRTY)) continue;

        /* Promote/demote existence */
        if (acct_has_flag(a, ACCT_SELF_DESTRUCTED))
            acct_clear_flag(a, ACCT_EXISTED);
        else if (!acct_is_empty(a))
            acct_set_flag(a, ACCT_EXISTED);

        /* Dead = not existed.  commit_tx already clears ACCT_EXISTED for
         * EIP-161 pruned accounts, so we only need to check the flag.
         * Do NOT add "is_empty && prune_empty" here — that would kill
         * existed empty accounts whose touch was reverted (OOG). */
        bool dead = !acct_has_flag(a, ACCT_EXISTED);

        /* Mark live accounts for storage root recomputation (skip dead — orphaned) */
        if (acct_has_flag(a, ACCT_STORAGE_DIRTY) && a->resource_idx && !dead)
            stor_dirty_set(s, a->resource_idx);

        if (dead) {
            uint32_t del_idx;
            if (hart_delete_get(&s->acct_index, ah.bytes, &del_idx)) {
                /* Clean up orphaned resources + recycle slots */
                account_t *da = &s->accounts[del_idx];
                resource_t *dr = get_resource(s, da);
                if (dr) {
                    free(dr->code); dr->code = NULL;
                    free(dr->jumpdest_bitmap); dr->jumpdest_bitmap = NULL;
                    dr->code_size = 0;
                    dr->code_hash = EMPTY_CODE_HASH;
                    dr->storage_root = EMPTY_STORAGE_ROOT;
                    if (s->stor_pool && !storage_hart_empty(&dr->storage))
                        storage_hart_clear(s->stor_pool, &dr->storage);
                    dead_vec_push(&s->res_free, &s->res_free_count,
                                  &s->res_free_cap, da->resource_idx);
                    da->resource_idx = 0;
                }
                dead_vec_push(&s->acct_free, &s->acct_free_count,
                              &s->acct_free_cap, del_idx);
            }
        }

        /* Clear flags */
        acct_clear_flag(a, ACCT_IN_BLK_DIRTY | ACCT_STORAGE_DIRTY);
    }

    /* Delete dead accounts from acct_index (phantoms, destructed, pruned).
     * Guard: only delete if account is truly dead — it may have been
     * re-created in a later tx within the same block. */
    #define SAFE_DELETE_IDX(i) do { \
        if ((i) < s->count) { \
            account_t *_a = &s->accounts[(i)]; \
            if (!acct_has_flag(_a, ACCT_EXISTED)) { \
                hash_t _h = addr_hash_cached(s, _a->addr.bytes); \
                const uint32_t *_p = (const uint32_t *) \
                    hart_get(&s->acct_index, _h.bytes); \
                if (_p && *_p == (i)) { \
                    uint32_t _del; \
                    if (hart_delete_get(&s->acct_index, _h.bytes, &_del)) { \
                        account_t *_da = &s->accounts[_del]; \
                        resource_t *_dr = get_resource(s, _da); \
                        if (_dr) { \
                            free(_dr->code); _dr->code = NULL; \
                            free(_dr->jumpdest_bitmap); _dr->jumpdest_bitmap = NULL; \
                            _dr->code_size = 0; \
                            _dr->code_hash = EMPTY_CODE_HASH; \
                            _dr->storage_root = EMPTY_STORAGE_ROOT; \
                            if (s->stor_pool && !storage_hart_empty(&_dr->storage)) \
                                storage_hart_clear(s->stor_pool, &_dr->storage); \
                            dead_vec_push(&s->res_free, &s->res_free_count, \
                                          &s->res_free_cap, _da->resource_idx); \
                            _da->resource_idx = 0; \
                        } \
                        dead_vec_push(&s->acct_free, &s->acct_free_count, \
                                      &s->acct_free_cap, _del); \
                    } \
                } \
            } \
        } \
    } while(0)

    for (uint32_t pi = 0; pi < s->phantom_count; pi++)
        SAFE_DELETE_IDX(s->phantoms[pi]);
    for (uint32_t di = 0; di < s->destructed_count; di++)
        SAFE_DELETE_IDX(s->destructed[di]);
    for (uint32_t ri = 0; ri < s->pruned_count; ri++)
        SAFE_DELETE_IDX(s->pruned[ri]);

    #undef SAFE_DELETE_IDX
    s->dead_total += s->phantom_count + s->destructed_count + s->pruned_count;
    s->phantom_count = 0;
    s->destructed_count = 0;
    s->pruned_count = 0;

    /* Swap blk_dirty → last_dirty (preserves for diff collection) */
    dirty_vec_t tmp = s->last_dirty;
    s->last_dirty = s->blk_dirty;
    s->blk_dirty = tmp;
    dirty_clear(&s->blk_dirty);
    s->blk_dirty_cursor = 0;
}

void state_reset_block(state_t *s) {
    if (!s) return;

    /* Clear per-block originals */
    mem_art_destroy(&s->blk_orig_acct); mem_art_init(&s->blk_orig_acct);
    mem_art_destroy(&s->blk_orig_stor); mem_art_init(&s->blk_orig_stor);

    /* addr_hash_cache is NOT reset — keccak256 is a pure function,
     * cached values never go stale. Persists across blocks. */
}

/* =========================================================================
 * Revert block — restore pre-block state from undo log
 * ========================================================================= */

/* Callback: restore storage slot to its pre-block value */
static bool revert_stor_cb(const uint8_t *key, size_t key_len,
                            const void *value, size_t value_len,
                            void *user_data) {
    (void)key_len; (void)value_len;
    state_t *s = (state_t *)user_data;

    /* key = addr[20] + slot_be[32] */
    const uint8_t *addr_bytes = key;
    const uint8_t *slot_be = key + 20;
    const uint256_t *old_value = (const uint256_t *)value;

    account_t *a = find_account(s, addr_bytes);
    if (!a) return true;

    resource_t *r = get_resource(s, a);
    if (!r || !s->stor_pool) return true;

    /* Recompute slot hash (storage hart key = keccak256(slot_be)) */
    hash_t slot_hash = slot_hash_cached(s, slot_be);

    /* Restore original value */
    if (uint256_is_zero(old_value))
        storage_hart_del(s->stor_pool, &r->storage, slot_hash.bytes);
    else {
        uint8_t val_be[32];
        uint256_to_bytes(old_value, val_be);
        storage_hart_put(s->stor_pool, &r->storage, slot_hash.bytes, val_be);
    }

    /* Mark storage path dirty so next compute_root rehashes */
    storage_hart_mark_dirty(s->stor_pool, &r->storage, slot_hash.bytes);

    return true;
}

/* Callback: restore account nonce/balance/code_hash to pre-block values */
static bool revert_acct_cb(const uint8_t *key, size_t key_len,
                            const void *value, size_t value_len,
                            void *user_data) {
    (void)key_len; (void)value_len;
    state_t *s = (state_t *)user_data;
    const acct_snapshot_t *snap = (const acct_snapshot_t *)value;

    account_t *a = find_account(s, key);
    if (!a) return true;

    /* Restore nonce and balance */
    a->nonce = snap->nonce;
    a->balance = snap->balance;

    /* Restore existence flag */
    if (snap->existed)
        acct_set_flag(a, ACCT_EXISTED);
    else
        acct_clear_flag(a, ACCT_EXISTED);

    /* Clear block-level flags */
    acct_clear_flag(a, ACCT_DIRTY | ACCT_CREATED | ACCT_SELF_DESTRUCTED |
                       ACCT_IN_BLK_DIRTY);

    /* Mark ART path dirty for next root computation */
    hash_t ah = addr_hash_cached(s, key);
    hart_mark_path_dirty(&s->acct_index, ah.bytes);

    return true;
}

bool state_revert_block(state_t *s) {
    if (!s) return false;

    /* Restore storage slots first (accounts reference storage) */
    mem_art_foreach(&s->blk_orig_stor, revert_stor_cb, s);

    /* Restore account fields */
    mem_art_foreach(&s->blk_orig_acct, revert_acct_cb, s);

    /* Clear undo log */
    mem_art_destroy(&s->blk_orig_acct); mem_art_init(&s->blk_orig_acct);
    mem_art_destroy(&s->blk_orig_stor); mem_art_init(&s->blk_orig_stor);

    /* Clear dirty lists — state is back to pre-block */
    dirty_clear(&s->blk_dirty);
    dirty_clear(&s->last_dirty);
    dirty_clear(&s->tx_dirty);
    s->blk_dirty_cursor = 0;

    /* Clear journal */
    s->journal_len = 0;
    mem_art_destroy(&s->originals);
    mem_art_init(&s->originals);

    return true;
}

uint32_t state_dead_count(const state_t *s) {
    if (!s) return 0;
    return s->dead_total + s->phantom_count + s->destructed_count + s->pruned_count;
}

/* =========================================================================
 * dump-prestate: access tracking + collection
 * ========================================================================= */

void state_enable_access_tracking(state_t *s) {
    if (!s) return;
    s->track_accesses = true;
    dirty_clear(&s->accessed_slots);
}

void state_disable_access_tracking(state_t *s) {
    if (!s) return;
    s->track_accesses = false;
    dirty_clear(&s->accessed_slots);
}

size_t state_collect_dirty_addresses(const state_t *s, address_t *out, size_t max) {
    if (!s || !out || max == 0) return 0;
    size_t n = 0;
    for (size_t i = 0; i < s->last_dirty.count && n < max; i++) {
        const uint8_t *akey = s->last_dirty.keys + i * 20;
        /* Deduplicate: skip if already seen */
        bool dup = false;
        for (size_t j = 0; j < n; j++) {
            if (memcmp(out[j].bytes, akey, 20) == 0) { dup = true; break; }
        }
        if (!dup) memcpy(out[n++].bytes, akey, 20);
    }
    return n;
}

size_t state_collect_accessed_storage_keys(const state_t *s,
                                           const address_t *addr,
                                           uint256_t *out, size_t max) {
    if (!s || !addr || !out || max == 0) return 0;
    size_t n = 0;
    for (size_t i = 0; i < s->accessed_slots.count && n < max; i++) {
        const uint8_t *entry = s->accessed_slots.keys + i * SLOT_KEY_SIZE;
        if (memcmp(entry, addr->bytes, 20) != 0) continue;
        /* Deduplicate */
        uint256_t slot = uint256_from_bytes(entry + 20, 32);
        bool dup = false;
        for (size_t j = 0; j < n; j++) {
            if (uint256_eq(&out[j], &slot)) { dup = true; break; }
        }
        if (!dup) out[n++] = slot;
    }
    return n;
}

/* Callback context for storage diff collection */
typedef struct {
    state_t     *s;
    addr_diff_t *groups;
    uint32_t     group_count;
    uint32_t     group_cap;
} stor_cb_ctx_t;

static bool stor_diff_cb(const uint8_t *key, size_t key_len,
                         const void *value, size_t value_len,
                         void *user_data) {
    (void)key_len; (void)value_len;
    stor_cb_ctx_t *ctx = (stor_cb_ctx_t *)user_data;
    const uint8_t *addr_bytes = key;
    const uint256_t *old_val = (const uint256_t *)value;

    address_t addr;
    memcpy(addr.bytes, addr_bytes, 20);
    uint256_t slot = uint256_from_bytes(key + 20, 32);
    uint256_t cur_val = state_get_storage(ctx->s, &addr, &slot);

    if (uint256_is_equal(&cur_val, old_val)) return true;

    /* Find or create group */
    addr_diff_t *g = NULL;
    for (uint32_t i = 0; i < ctx->group_count; i++) {
        if (memcmp(ctx->groups[i].addr.bytes, addr_bytes, 20) == 0) {
            g = &ctx->groups[i]; break;
        }
    }
    if (!g) {
        if (ctx->group_count >= ctx->group_cap) {
            ctx->group_cap *= 2;
            ctx->groups = realloc(ctx->groups,
                                  ctx->group_cap * sizeof(addr_diff_t));
        }
        g = &ctx->groups[ctx->group_count++];
        memset(g, 0, sizeof(*g));
        memcpy(g->addr.bytes, addr_bytes, 20);
    }

    g->slots = realloc(g->slots, (g->slot_count + 1) * sizeof(slot_diff_t));
    slot_diff_t *sd = &g->slots[g->slot_count++];
    sd->slot = slot;
    sd->value = cur_val;
    sd->old_value = *old_val;
    return true;
}

void state_collect_block_diff(state_t *s, block_diff_t *out) {
    if (!s || !out) return;

    uint32_t group_cap = 64;
    uint32_t group_count = 0;
    addr_diff_t *groups = calloc(group_cap, sizeof(addr_diff_t));
    if (!groups) return;

    /* Walk blk_dirty — only NEW entries since last finalize (cursor → count) */
    for (size_t d = s->blk_dirty_cursor; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        account_t *a = find_account(s, akey);
        if (!a) continue;

        /* Get old values from block originals */
        const acct_snapshot_t *snap = (const acct_snapshot_t *)
            mem_art_get(&s->blk_orig_acct, akey, 20, NULL);

        bool nonce_changed = snap && (a->nonce != snap->nonce);
        bool balance_changed = snap && !uint256_is_equal(&a->balance, &snap->balance);
        resource_t *r = get_resource(s, a);
        hash_t cur_code = (r && acct_has_flag(a, ACCT_HAS_CODE))
                          ? r->code_hash : EMPTY_CODE_HASH;
        bool code_changed = snap &&
            memcmp(cur_code.bytes, snap->code_hash.bytes, 32) != 0;
        bool is_created = snap && !snap->existed && acct_has_flag(a, ACCT_EXISTED);
        bool is_destructed = acct_has_flag(a, ACCT_SELF_DESTRUCTED);

        if (!nonce_changed && !balance_changed && !code_changed &&
            !is_created && !is_destructed)
            continue;  /* no account-level change */

        if (group_count >= group_cap) {
            group_cap *= 2;
            groups = realloc(groups, group_cap * sizeof(addr_diff_t));
        }
        addr_diff_t *g = &groups[group_count++];
        memset(g, 0, sizeof(*g));
        memcpy(g->addr.bytes, akey, 20);

        if (is_created) g->flags |= ACCT_DIFF_CREATED;
        if (is_destructed) g->flags |= ACCT_DIFF_DESTRUCTED;

        if (nonce_changed) {
            g->field_mask |= FIELD_NONCE;
            g->nonce = a->nonce;
            g->old_nonce = snap->nonce;
        }
        if (balance_changed) {
            g->field_mask |= FIELD_BALANCE;
            g->balance = a->balance;
            g->old_balance = snap->balance;
        }
        if (code_changed) {
            g->field_mask |= FIELD_CODE_HASH;
            g->code_hash = cur_code;
            g->old_code_hash = snap->code_hash;
        }
    }

    /* Walk blk_orig_stor for storage diffs via mem_art_foreach.
     * Keys are slot_key[52] = addr[20] + slot_be[32].
     * Values are uint256_t old_value. */
    {
        stor_cb_ctx_t ctx = { .s = s, .groups = groups,
                              .group_count = group_count, .group_cap = group_cap };
        mem_art_foreach(&s->blk_orig_stor, stor_diff_cb, &ctx);
        groups = ctx.groups;
        group_count = ctx.group_count;
    }

    out->groups = groups;
    out->group_count = group_count;
}

/* =========================================================================
 * Block diff fast-apply — forward replay that bypasses journal/originals
 * /storage_read-before-write. Used by state_history_apply_diff during
 * catch-up replay from a snapshot. Does not support revert.
 * ========================================================================= */

void state_apply_diff_fast(state_t *s, const block_diff_t *diff) {
    if (!s || !diff) return;

    for (uint32_t i = 0; i < diff->group_count; i++) {
        const addr_diff_t *g = &diff->groups[i];
        hash_t ah = addr_hash_cached(s, g->addr.bytes);

        if (g->flags & ACCT_DIFF_DESTRUCTED) {
            /* Same as the fast path in finalize_block's SAFE_DELETE_IDX:
             * drop the account entirely. If the same diff group ALSO has
             * ACCT_DIFF_CREATED, a re-create will happen below. */
            account_t *a = find_account_h(s, &ah);
            if (a) {
                resource_t *r = get_resource(s, a);
                if (r) {
                    free(r->code); r->code = NULL;
                    free(r->jumpdest_bitmap); r->jumpdest_bitmap = NULL;
                    r->code_size = 0;
                    r->code_hash = EMPTY_CODE_HASH;
                    r->storage_root = EMPTY_STORAGE_ROOT;
                    if (s->stor_pool && !storage_hart_empty(&r->storage))
                        storage_hart_clear(s->stor_pool, &r->storage);
                    dead_vec_push(&s->res_free, &s->res_free_count,
                                  &s->res_free_cap, a->resource_idx);
                    a->resource_idx = 0;
                }
                uint32_t del_idx;
                if (hart_delete_get(&s->acct_index, ah.bytes, &del_idx)) {
                    dead_vec_push(&s->acct_free, &s->acct_free_count,
                                  &s->acct_free_cap, del_idx);
                }
            }
            /* Only skip further processing if the account wasn't re-created
             * in the same block. A re-created account needs its new fields
             * and storage applied via the code below. */
            if (!(g->flags & ACCT_DIFF_CREATED)) continue;
        }

        account_t *a = ensure_account_h(s, &g->addr, &ah);
        if (!a) continue;
        acct_set_flag(a, ACCT_EXISTED);

        if (g->field_mask & FIELD_NONCE)
            a->nonce = g->nonce;

        if (g->field_mask & FIELD_BALANCE)
            a->balance = g->balance;

        if (g->field_mask & FIELD_CODE_HASH) {
            resource_t *r = ensure_resource(s, a);
            if (r) {
                free(r->code); r->code = NULL;
                free(r->jumpdest_bitmap); r->jumpdest_bitmap = NULL;
                r->code_size = 0;
                r->code_hash = g->code_hash;
                bool is_empty = memcmp(g->code_hash.bytes,
                                       EMPTY_CODE_HASH.bytes, 32) == 0;
                if (is_empty) acct_clear_flag(a, ACCT_HAS_CODE);
                else          acct_set_flag(a, ACCT_HAS_CODE);
            }
        }

        if (g->slot_count > 0) {
            resource_t *r = ensure_resource(s, a);
            if (r && s->stor_pool) {
                for (uint16_t j = 0; j < g->slot_count; j++) {
                    uint8_t slot_be[32];
                    uint256_to_bytes(&g->slots[j].slot, slot_be);
                    hash_t slot_hash = slot_hash_cached(s, slot_be);
                    if (uint256_is_zero(&g->slots[j].value))
                        storage_hart_del(s->stor_pool, &r->storage, slot_hash.bytes);
                    else {
                        uint8_t val_be[32];
                        uint256_to_bytes(&g->slots[j].value, val_be);
                        storage_hart_put(s->stor_pool, &r->storage,
                                         slot_hash.bytes, val_be);
                    }
                }
                stor_dirty_set(s, a->resource_idx);
            }
        }

        /* EIP-161: an account drained to fully-empty (nonce=0, balance=0,
         * no code) and with no storage is pruned from the trie when
         * prune_empty is enabled. state_collect_block_diff emits the final
         * zeroed fields without signalling pruning, so we detect it here
         * after applying the diff. */
        if (s->prune_empty && acct_is_empty(a)) {
            resource_t *r = get_resource(s, a);
            bool has_storage = r && s->stor_pool &&
                               !storage_hart_empty(&r->storage);
            if (!has_storage) {
                if (r) {
                    free(r->code); r->code = NULL;
                    free(r->jumpdest_bitmap); r->jumpdest_bitmap = NULL;
                    r->code_size = 0;
                    r->code_hash = EMPTY_CODE_HASH;
                    r->storage_root = EMPTY_STORAGE_ROOT;
                    dead_vec_push(&s->res_free, &s->res_free_count,
                                  &s->res_free_cap, a->resource_idx);
                    a->resource_idx = 0;
                }
                uint32_t del_idx;
                if (hart_delete_get(&s->acct_index, ah.bytes, &del_idx)) {
                    dead_vec_push(&s->acct_free, &s->acct_free_count,
                                  &s->acct_free_cap, del_idx);
                }
                /* acct_index path already marked dirty by the delete path,
                 * so skip the final hart_mark_path_dirty. */
                continue;
            }
        }

        hart_mark_path_dirty(&s->acct_index, ah.bytes);
    }

    /* ensure_account_h pushes new slots to s->phantoms for the normal
     * finalize_block to potentially garbage-collect. The fast path sets
     * ACCT_EXISTED unconditionally for every ensure'd account (or removes
     * the account outright for DESTRUCTED), so nothing in phantoms is
     * actually a phantom — drop the tracking buffer without running the
     * deletion pass. */
    s->phantom_count = 0;
}

/* =========================================================================
 * Block diff revert — undo a block's state changes (always available)
 * ========================================================================= */

void block_diff_revert(struct evm_state *es, const block_diff_t *diff) {
    if (!es || !diff) return;

    /* evm_state_get_state is in evm_state.c — use the extern declaration */
    extern state_t *evm_state_get_state(struct evm_state *);
    state_t *st = evm_state_get_state(es);
    if (!st) return;

    /* Process groups in reverse order using raw writes
     * (skip journal, dirty tracking, block originals) */
    for (int i = (int)diff->group_count - 1; i >= 0; i--) {
        const addr_diff_t *g = &diff->groups[i];

        /* Revert storage slots first (reverse order) */
        for (int j = (int)g->slot_count - 1; j >= 0; j--) {
            state_set_storage_raw(st, &g->addr,
                                  &g->slots[j].slot, &g->slots[j].old_value);
        }

        /* Revert account fields */
        if (g->field_mask & FIELD_BALANCE)
            state_set_balance_raw(st, &g->addr, &g->old_balance);

        if (g->field_mask & FIELD_NONCE)
            state_set_nonce_raw(st, &g->addr, g->old_nonce);

        if (g->flags & ACCT_DIFF_CREATED) {
            state_set_nonce_raw(st, &g->addr, 0);
            uint256_t zero = UINT256_ZERO;
            state_set_balance_raw(st, &g->addr, &zero);
        }

        if (g->flags & ACCT_DIFF_DESTRUCTED) {
            state_set_nonce_raw(st, &g->addr, g->old_nonce);
            state_set_balance_raw(st, &g->addr, &g->old_balance);
        }
    }
}

void state_compact(state_t *s) {
    if (!s) return;

    /* Count live accounts */
    uint32_t live = 0;
    for (uint32_t i = 0; i < s->count; i++) {
        if (acct_has_flag(&s->accounts[i], ACCT_EXISTED))
            live++;
    }

    /* Allocate new vector */
    uint32_t new_cap = live < ACCT_INIT_CAP ? ACCT_INIT_CAP : live;
    account_t *new_accts = vec_alloc((size_t)new_cap * sizeof(account_t));
    if (!new_accts) return;

    /* Rebuild acct_index from scratch */
    hart_destroy(&s->acct_index);
    hart_init(&s->acct_index, sizeof(uint32_t));

    /* Copy live accounts to new vector, insert into fresh acct_index */
    uint32_t new_count = 0;
    for (uint32_t i = 0; i < s->count; i++) {
        account_t *a = &s->accounts[i];
        if (!acct_has_flag(a, ACCT_EXISTED)) continue;

        /* Update resource_idx — resource vector not compacted, indices stay valid */
        new_accts[new_count] = *a;

        hash_t addr_hash = hash_keccak256(a->addr.bytes, 20);
        hart_insert(&s->acct_index, addr_hash.bytes, &new_count);
        new_count++;
    }

    vec_free(s->accounts, (size_t)s->capacity * sizeof(account_t));
    s->accounts = new_accts;
    s->count = new_count;
    s->capacity = new_cap;

    /* Free resources for pruned accounts (no longer in new vector).
     * Build a set of live resource indices from the new vector. */
    {
        bool *res_live = calloc(s->res_capacity, sizeof(bool));
        if (res_live) {
            for (uint32_t i = 0; i < new_count; i++) {
                if (new_accts[i].resource_idx)
                    res_live[new_accts[i].resource_idx] = true;
            }
            for (uint32_t i = 1; i < s->res_count; i++) {
                if (!res_live[i]) {
                    resource_t *r = &s->resources[i];
                    free(r->code);
                    if (s->stor_pool && !storage_hart_empty(&r->storage))
                        storage_hart_clear(s->stor_pool, &r->storage);
                    memset(r, 0, sizeof(*r));
                }
            }
            free(res_live);
        }
    }

    /* With arena freelist, storage harts don't accumulate dead space.
     * No rebuild or trim needed. */

    /* acct_index (hart) was rebuilt — all nodes born dirty, hash will be recomputed */

    /* Clear dead account tracking + free lists (compaction closes all gaps) */
    s->phantom_count = 0;
    s->destructed_count = 0;
    s->pruned_count = 0;
    s->dead_total = 0;
    s->acct_free_count = 0;
    s->res_free_count = 0;

    /* Mark all storage roots dirty — acct_index was rebuilt from scratch */
    for (uint32_t i = 1; i < s->res_count; i++)
        stor_dirty_set(s, i);
}

uint64_t state_count_internal_nodes(const state_t *s,
                                     uint64_t *persistable_out,
                                     uint64_t *clean_out) {
    uint64_t total = 0, persist = 0, clean = 0;
    if (!s) {
        if (persistable_out) *persistable_out = 0;
        if (clean_out) *clean_out = 0;
        return 0;
    }

    uint32_t a_clean = 0;
    total += hart_count_internal_nodes(&s->acct_index, &a_clean);
    clean += a_clean;
    persist += hart_count_persistable_hashes(&s->acct_index);

    if (s->stor_pool) {
        for (uint32_t i = 1; i < s->res_count; i++) {
            const resource_t *r = &s->resources[i];
            if (storage_hart_empty(&r->storage)) continue;
            uint32_t sc = 0;
            total += storage_hart_count_internal_nodes(s->stor_pool,
                                                       &r->storage, &sc);
            clean += sc;
            persist += storage_hart_count_persistable_hashes(s->stor_pool,
                                                             &r->storage);
        }
    }
    if (persistable_out) *persistable_out = persist;
    if (clean_out) *clean_out = clean;
    return total;
}

/* =========================================================================
 * Save / Load — binary snapshot of full state
 *
 * Format (little-endian):
 *   V2: magic("ART2", 4) + block_number(8) + state_root(32) + account_count(4)
 *   V1 (legacy, read-only via state_load_v1): magic("ART1", ...)
 *   Per account:
 *     addr(20) + nonce(8) + balance(32) + code_hash(32) + storage_root(32) +
 *     code_size(4) + storage_count(4)
 *     [storage_count × (key(32) + value(32))]
 *     [if storage_count > 0]: stor_internal_count(4) + [count × hash(32)]
 *   Tail:
 *     acct_internal_count(4) + [acct_internal_count × hash(32)]
 *
 * The internal-node hash streams are emitted in pre-order DFS (key-sorted
 * children). They let state_load skip MPT recomputation: the rebuilt tree
 * has the same DFS shape (proven by hart_determinism_test), so installing
 * the hashes onto each internal node restores the cached state directly.
 *
 * Code bytes are NOT saved — code_store has them on disk.
 * Caller must compute_root before save so the cached hashes are clean.
 * ========================================================================= */

/* =========================================================================
 * Storage pool path
 * ========================================================================= */

void state_set_storage_path(state_t *s, const char *dir) {
    /* hart_pool is MAP_ANONYMOUS — no backing file, no path needed.
     * Kept as a no-op for API compatibility; the `dir` argument is now
     * only informational. */
    (void)s; (void)dir;
}

/* =========================================================================
 * State save/load
 * ========================================================================= */

#define STATE_MAGIC_V1 "ART1"
#define STATE_MAGIC_V2 "ART2"

static bool write_all(FILE *f, const void *buf, size_t n) {
    return fwrite(buf, 1, n, f) == n;
}

/* Callback context for state_save storage iteration */
typedef struct { FILE *f; uint32_t count; bool ok; } save_stor_ctx_t;

static bool save_entry_cb(const uint8_t key[32], const uint8_t val[32], void *ctx) {
    save_stor_ctx_t *sc = ctx;
    if (!write_all(sc->f, key, 32) || !write_all(sc->f, val, 32))
        { sc->ok = false; return false; }
    sc->count++;
    return true;
}

/* Walk callback: write one branch-producing internal hash. */
typedef struct { FILE *f; uint32_t count; bool ok; } save_hash_ctx_t;

static void save_persist_cb(const uint8_t hash32[32], void *user) {
    save_hash_ctx_t *hc = user;
    if (!hc->ok) return;
    if (!write_all(hc->f, hash32, 32)) { hc->ok = false; return; }
    hc->count++;
}

static bool read_all(FILE *f, void *buf, size_t n) {
    return fread(buf, 1, n, f) == n;
}

/* TODO(api): persist the last-256-block-hash ring here (BLOCK_HASH_WINDOW).
 * Currently the ring lives at the engine/sync layer (lib/artex.c,
 * sync/src/sync.c) and is written to a separate <path>.hashes sidecar
 * when rx_engine_save_state is called — but chain_replay's state_save
 * path writes only this file, not the sidecar. As a result, snapshots
 * produced by chain_replay need tools/make_hashes.py to regenerate the
 * .hashes file from era data before rx_engine_load_state can use them
 * correctly (otherwise BLOCKHASH returns zero for the last 256 blocks).
 *
 * Folding the ring into the main snapshot would remove the sidecar
 * entirely: either extend the ART1 header to carry 256 hashes, or append
 * them after the account/storage payload with a trailing magic.
 * Matching change goes in state_load below. */
static bool state_save_impl(const state_t *s, const char *path,
                             const hash_t *state_root,
                             const char *magic, bool with_hashes) {
    if (!s || !path) return false;
    FILE *f = fopen(path, "wb");
    if (!f) return false;

    /* Count live accounts */
    uint32_t live = 0;
    for (uint32_t i = 0; i < s->count; i++) {
        if (acct_has_flag(&s->accounts[i], ACCT_EXISTED)) live++;
    }

    /* Header */
    static const uint8_t zero_root[32] = {0};
    if (!write_all(f, magic, 4)) goto fail;
    if (!write_all(f, &s->current_block, 8)) goto fail;
    if (!write_all(f, state_root ? state_root->bytes : zero_root, 32)) goto fail;
    if (!write_all(f, &live, 4)) goto fail;

    /* Accounts */
    for (uint32_t i = 0; i < s->count; i++) {
        const account_t *a = &s->accounts[i];
        if (!acct_has_flag(a, ACCT_EXISTED)) continue;

        if (!write_all(f, a->addr.bytes, 20)) goto fail;
        if (!write_all(f, &a->nonce, 8)) goto fail;
        if (!write_all(f, &a->balance, 32)) goto fail;

        /* Code hash + storage root (from resource, or defaults) */
        const resource_t *r = (a->resource_idx > 0 && a->resource_idx < s->res_count)
                              ? &s->resources[a->resource_idx] : NULL;

        const uint8_t *ch = (r && acct_has_flag(a, ACCT_HAS_CODE))
                            ? r->code_hash.bytes : EMPTY_CODE_HASH.bytes;
        const uint8_t *sr = r ? r->storage_root.bytes : EMPTY_STORAGE_ROOT.bytes;

        if (!write_all(f, ch, 32)) goto fail;
        if (!write_all(f, sr, 32)) goto fail;

        /* Code size (for reference; code itself is in code_store) */
        uint32_t code_size = (r && acct_has_flag(a, ACCT_HAS_CODE)) ? r->code_size : 0;
        if (!write_all(f, &code_size, 4)) goto fail;

        /* Storage entries — from storage_hart (mmap-backed) */
        uint32_t stor_count = r ? storage_hart_count(&r->storage) : 0;
        if (!write_all(f, &stor_count, 4)) goto fail;
        if (stor_count > 0 && s->stor_pool) {
            save_stor_ctx_t sc = { .f = f, .count = 0, .ok = true };
            storage_hart_foreach(s->stor_pool, &r->storage, save_entry_cb, &sc);
            if (!sc.ok) goto fail;
        }

        /* Storage hash stream — V2 only; skipped on empty storage to keep
         * EOA-heavy snapshots small. One entry per branch-producing
         * internal; passthrough nodes are folded into extensions and
         * never carry a cache. */
        if (with_hashes && stor_count > 0 && r && s->stor_pool) {
            uint32_t sh_total = storage_hart_count_persistable_hashes(
                                    s->stor_pool, &r->storage);
            if (!write_all(f, &sh_total, 4)) goto fail;
            if (sh_total > 0) {
                save_hash_ctx_t hc = { .f = f, .count = 0, .ok = true };
                storage_hart_walk_persistable_hashes(s->stor_pool, &r->storage,
                                                      save_persist_cb, &hc);
                if (!hc.ok || hc.count != sh_total) goto fail;
            }
        }
    }

    /* Account-trie hash stream — V2 only. */
    if (with_hashes) {
        uint32_t ai_total = hart_count_persistable_hashes(&s->acct_index);
        if (!write_all(f, &ai_total, 4)) goto fail;
        if (ai_total > 0) {
            save_hash_ctx_t hc = { .f = f, .count = 0, .ok = true };
            hart_walk_persistable_hashes(&s->acct_index, save_persist_cb, &hc);
            if (!hc.ok || hc.count != ai_total) goto fail;
        }
    }

    fclose(f);
    return true;
fail:
    fclose(f);
    return false;
}

bool state_save(const state_t *s, const char *path, const hash_t *state_root) {
    return state_save_impl(s, path, state_root, STATE_MAGIC_V2, true);
}

bool state_save_v1(const state_t *s, const char *path, const hash_t *state_root) {
    return state_save_impl(s, path, state_root, STATE_MAGIC_V1, false);
}

/* TODO(api): read block-hash ring here once state_save writes it.
 * See the TODO on state_save above. */
static bool state_load_impl(state_t *s, const char *path, hash_t *out_root,
                             const char *expect_magic, bool with_hashes) {
    if (!s || !path) return false;
    FILE *f = fopen(path, "rb");
    if (!f) return false;

    /* Header */
    char magic[4];
    if (!read_all(f, magic, 4)) goto fail;
    if (memcmp(magic, expect_magic, 4) != 0) {
        fprintf(stderr, "state_load: magic mismatch — expected %.4s, got %.4s\n",
                expect_magic, magic);
        if (memcmp(magic, STATE_MAGIC_V1, 4) == 0 && with_hashes)
            fprintf(stderr,
                "  this is a legacy ART1 snapshot — run tools/state_migrate "
                "to upgrade it to ART2\n");
        goto fail;
    }
    uint64_t block_number;
    if (!read_all(f, &block_number, 8)) goto fail;
    hash_t saved_root;
    if (!read_all(f, &saved_root, 32)) goto fail;
    if (out_root) *out_root = saved_root;
    uint32_t acct_count;
    if (!read_all(f, &acct_count, 4)) goto fail;

    /* Reset state */
    hart_destroy(&s->acct_index);
    hart_init(&s->acct_index, sizeof(uint32_t));
    for (uint32_t i = 1; i < s->res_count; i++) {
        resource_t *r = &s->resources[i];
        if (s->stor_pool && !storage_hart_empty(&r->storage))
            storage_hart_clear(s->stor_pool, &r->storage);
        free(r->code); r->code = NULL;
    }
    s->count = 0;
    s->res_count = 1; /* slot 0 reserved */
    s->current_block = block_number;

    /* Load accounts */
    for (uint32_t i = 0; i < acct_count; i++) {
        address_t addr;
        uint64_t nonce;
        uint256_t balance;
        uint8_t code_hash[32], storage_root[32];
        uint32_t code_size, stor_count;

        if (!read_all(f, addr.bytes, 20)) goto fail;
        if (!read_all(f, &nonce, 8)) goto fail;
        if (!read_all(f, &balance, 32)) goto fail;
        if (!read_all(f, code_hash, 32)) goto fail;
        if (!read_all(f, storage_root, 32)) goto fail;
        if (!read_all(f, &code_size, 4)) goto fail;
        if (!read_all(f, &stor_count, 4)) goto fail;

        /* Grow accounts vector if needed */
        if (s->count >= s->capacity) {
            uint32_t nc = s->capacity + vec_grow_delta(s->capacity, sizeof(account_t));
            size_t old_bytes = (size_t)s->capacity * sizeof(account_t);
            size_t new_bytes = (size_t)nc * sizeof(account_t);
            account_t *na = vec_grow(s->accounts, old_bytes, new_bytes);
            if (!na) goto fail;
            memset(na + s->capacity, 0, (nc - s->capacity) * sizeof(account_t));
            s->accounts = na;
            s->capacity = nc;
        }

        uint32_t idx = s->count++;
        account_t *a = &s->accounts[idx];
        memset(a, 0, sizeof(*a));
        a->addr = addr;
        a->nonce = nonce;
        a->balance = balance;
        a->flags = ACCT_EXISTED;

        /* Insert into acct_index */
        hash_t addr_hash = hash_keccak256(addr.bytes, 20);
        hart_insert(&s->acct_index, addr_hash.bytes, &idx);

        bool has_code = memcmp(code_hash, EMPTY_CODE_HASH.bytes, 32) != 0;
        bool has_stor = stor_count > 0 ||
                        memcmp(storage_root, EMPTY_STORAGE_ROOT.bytes, 32) != 0;

        if (has_code || has_stor) {
            /* Allocate resource */
            if (s->res_count >= s->res_capacity) {
                uint32_t nc = s->res_capacity + vec_grow_delta(s->res_capacity, sizeof(resource_t));
                size_t old_bytes = (size_t)s->res_capacity * sizeof(resource_t);
                size_t new_bytes = (size_t)nc * sizeof(resource_t);
                resource_t *nr = vec_grow(s->resources, old_bytes, new_bytes);
                if (!nr) goto fail;
                memset(nr + s->res_capacity, 0, (nc - s->res_capacity) * sizeof(resource_t));
                s->resources = nr;
                s->res_capacity = nc;
            }
            uint32_t ridx = s->res_count++;
            a->resource_idx = ridx;
            resource_t *r = &s->resources[ridx];
            memset(r, 0, sizeof(*r));
            memcpy(r->code_hash.bytes, code_hash, 32);
            memcpy(r->storage_root.bytes, storage_root, 32);
            r->code_size = code_size;

            if (has_code) acct_set_flag(a, ACCT_HAS_CODE);

            /* Load storage into storage_hart */
            if (stor_count > 0 && s->stor_pool) {
                storage_hart_reserve(s->stor_pool, &r->storage, stor_count);
                for (uint32_t j = 0; j < stor_count; j++) {
                    uint8_t skey[32], sval[32];
                    if (!read_all(f, skey, 32) || !read_all(f, sval, 32)) goto fail;
                    storage_hart_put(s->stor_pool, &r->storage, skey, sval);
                }
                if (stor_count > 100000) {
                    fprintf(stderr, "  state_load: acct %u/%u loaded %u storage slots\n",
                            i, acct_count, stor_count);
                }
            } else if (stor_count > 0) {
                /* No pool — skip storage entries */
                fseek(f, (long)stor_count * 64, SEEK_CUR);
            }
        }

        /* Storage hash stream — V2 only, and only present when stor_count
         * > 0 (matches the save side). On install mismatch, the hart
         * stays dirty and compute_root rebuilds it lazily. */
        if (with_hashes && stor_count > 0) {
            uint32_t sh_total;
            if (!read_all(f, &sh_total, 4)) goto fail;
            if (sh_total > 0) {
                uint8_t *buf = malloc((size_t)sh_total * 32);
                if (!buf) goto fail;
                if (!read_all(f, buf, (size_t)sh_total * 32)) { free(buf); goto fail; }
                if (a->resource_idx > 0 && s->stor_pool) {
                    resource_t *rr = &s->resources[a->resource_idx];
                    if (!storage_hart_install_dfs_hashes(s->stor_pool, &rr->storage,
                                                         buf, sh_total)) {
                        fprintf(stderr,
                            "  state_load: storage hash install mismatch for acct %u "
                            "(saved %u internals); will recompute\n", i, sh_total);
                        stor_dirty_grow(s, a->resource_idx + 1);
                        stor_dirty_set(s, a->resource_idx);
                    }
                }
                free(buf);
            }
        }

        if ((i + 1) % 1000000 == 0)
            fprintf(stderr, "  state_load: %u/%u accounts...\n", i + 1, acct_count);
    }

    /* Account-trie internal hash stream — V2 only. On mismatch, leaves the
     * hart dirty so the next compute_root rebuilds it from the (correct)
     * loaded storage_root + code_hash leaf data. */
    if (with_hashes) {
        uint32_t ai_total;
        if (!read_all(f, &ai_total, 4)) goto fail;
        if (ai_total > 0) {
            uint8_t *buf = malloc((size_t)ai_total * 32);
            if (!buf) goto fail;
            if (!read_all(f, buf, (size_t)ai_total * 32)) { free(buf); goto fail; }
            if (!hart_install_dfs_hashes(&s->acct_index, buf, ai_total)) {
                fprintf(stderr,
                    "  state_load: acct hash install mismatch (saved %u internals); "
                    "will recompute\n", ai_total);
            }
            free(buf);
        }
    }

    /* hart_pool is MAP_ANONYMOUS — nothing to sync to disk. */

    /* V2: caches are restored above; storage_root fields are loaded from
     * the file. No bits set — future stor_dirty_set calls (block
     * execution) just need bitmap capacity.
     * V1: no hash streams, so every storage hart is dirty after fresh
     * inserts. Set dirty bits on every loaded resource so the next
     * compute_root rebuilds storage roots and populates caches — the
     * migration tool relies on this to bake correct ART2 hash streams. */
    stor_dirty_grow(s, s->res_count);
    if (!with_hashes) {
        for (uint32_t i = 1; i < s->res_count; i++)
            stor_dirty_set(s, i);
    }

    fclose(f);
    fprintf(stderr, "state_load: %u accounts from %s (block %lu)\n",
            acct_count, path, block_number);
    return true;
fail:
    fclose(f);
    return false;
}

bool state_load(state_t *s, const char *path, hash_t *out_root) {
    return state_load_impl(s, path, out_root, STATE_MAGIC_V2, true);
}

bool state_load_v1(state_t *s, const char *path, hash_t *out_root) {
    return state_load_impl(s, path, out_root, STATE_MAGIC_V1, false);
}
