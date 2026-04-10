/**
 * State — single in-memory Ethereum world state.
 *
 * Flat vector of accounts indexed by mem_art. No disk on hot path.
 * Journal for snapshot/revert. Per-account mem_art for storage.
 * Account trie (hart) for MPT root computation.
 */

#include "state.h"
#include "code_store.h"
#include "hashed_art.h"
#include "storage_hart.h"
#include "keccak256.h"
#include "logger.h"

#include "block_diff.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdatomic.h>
#include <pthread.h>
#include <xmmintrin.h>

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
    uint32_t   count;       /* number of accounts in use */
    uint32_t   capacity;    /* allocated slots */
    hart_t     acct_index;  /* addr_hash[32] → uint32_t idx (lookup + trie) */

    /* Resource vector (only accounts with code/storage) */
    resource_t *resources;
    uint32_t    res_count;
    uint32_t    res_capacity;

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

    /* Address hash cache: addr[20] → keccak256(addr)[32], reset per block */
    mem_art_t addr_hash_cache;

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
    bool     storage_roots_stale;  /* true if any block skipped storage root computation */

    /* Storage pool — mmap-backed per-account ART trie */
    storage_hart_pool_t *stor_pool;
    char                 stor_pool_path[512];

    /* Parallel root computation — persistent thread pool */
    struct root_pool {
        pthread_t      *threads;
        uint32_t        count;         /* number of worker threads */
        pthread_mutex_t mutex;
        pthread_cond_t  work_ready;    /* workers wait on this */
        pthread_cond_t  work_done;     /* main thread waits on this */
        volatile uint32_t active;      /* workers currently running */
        volatile bool   shutdown;

        /* Per-job parameters (set by main thread before signaling) */
        enum { ROOT_JOB_NONE, ROOT_JOB_DIRTY, ROOT_JOB_ALL } job_type;
        /* For DIRTY jobs: */
        const uint8_t       *dirty_keys;
        size_t               dirty_count;
        /* For ALL jobs: */
        resource_t          *resources;
        uint32_t             res_count;
        /* Shared: */
        state_t             *state;
        storage_hart_pool_t *pool;
        _Atomic size_t       next_idx;  /* work-stealing index */
    } root_pool;

    /* dump-prestate support: preserved dirty list + access tracking */
    dirty_vec_t last_dirty;       /* blk_dirty from last compute_root (addr[20]) */
    dirty_vec_t accessed_addrs;   /* addr[20] — all accounts read or written */
    dirty_vec_t accessed_slots;   /* addr[20]+slot_be[32] = 52 bytes each */
    bool        track_accesses;   /* set before target block to enable tracking */
};

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

/* Cached addr → keccak256(addr). Computes once per address per block. */
static hash_t addr_hash_cached(state_t *s, const uint8_t addr[20]) {
    size_t vlen;
    const void *cached = mem_art_get(&s->addr_hash_cache, addr, 20, &vlen);
    if (cached && vlen == 32) {
        hash_t h;
        memcpy(&h, cached, 32);
        return h;
    }
    hash_t h = hash_keccak256(addr, 20);
    mem_art_insert(&s->addr_hash_cache, addr, 20, h.bytes, 32);
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

    /* Grow resource vector if needed */
    if (s->res_count >= s->res_capacity) {
        uint32_t nc = s->res_capacity ? s->res_capacity * 2 : 1024;
        resource_t *nr = realloc(s->resources, nc * sizeof(resource_t));
        if (!nr) return NULL;
        memset(nr + s->res_capacity, 0, (nc - s->res_capacity) * sizeof(resource_t));
        s->resources = nr;
        s->res_capacity = nc;
    }

    uint32_t ridx = s->res_count++;
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

    /* Grow vector if needed */
    if (s->count >= s->capacity) {
        uint32_t nc = s->capacity ? s->capacity * 2 : ACCT_INIT_CAP;
        account_t *na = realloc(s->accounts, nc * sizeof(account_t));
        if (!na) return NULL;
        memset(na + s->capacity, 0, (nc - s->capacity) * sizeof(account_t));
        s->accounts = na;
        s->capacity = nc;
    }

    uint32_t idx = s->count++;
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
    if (!acct_has_flag(a, ACCT_MPT_DIRTY)) {
        acct_set_flag(a, ACCT_MPT_DIRTY);
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
    s->accounts = calloc(ACCT_INIT_CAP, sizeof(account_t));
    if (!s->accounts) { free(s); return NULL; }
    s->capacity = ACCT_INIT_CAP;

    hart_init(&s->acct_index, sizeof(uint32_t));
    mem_art_init(&s->addr_hash_cache);
    mem_art_init(&s->warm_addrs);
    mem_art_init(&s->warm_slots);
    mem_art_init(&s->transient);
    mem_art_init(&s->originals);
    mem_art_init(&s->blk_orig_acct);
    mem_art_init(&s->blk_orig_stor);

    s->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!s->journal) { free(s->accounts); free(s); return NULL; }
    s->journal_cap = JOURNAL_INIT_CAP;

    /* Reserve resource index 0 as "none" (resource_idx=0 means no resource) */
    s->resources = calloc(1024, sizeof(resource_t));
    if (!s->resources) { free(s->journal); free(s->accounts); free(s); return NULL; }
    s->res_capacity = 1024;
    s->res_count = 1;

    /* Default storage pool on tmpfs — replaced by state_set_storage_path if called */
    char tmp[64];
    snprintf(tmp, sizeof(tmp), "/dev/shm/artex_stor_%d.dat", (int)getpid());
    s->stor_pool = storage_hart_pool_create(tmp);
    snprintf(s->stor_pool_path, sizeof(s->stor_pool_path), "%s", tmp);

    return s;
}

/* --- Root pool worker thread --- */
static void *root_pool_worker(void *arg) {
    state_t *s = (state_t *)arg;
    struct root_pool *rp = &s->root_pool;

    for (;;) {
        pthread_mutex_lock(&rp->mutex);
        while (rp->job_type == ROOT_JOB_NONE && !rp->shutdown)
            pthread_cond_wait(&rp->work_ready, &rp->mutex);
        if (rp->shutdown) { pthread_mutex_unlock(&rp->mutex); break; }
        int job = rp->job_type;
        pthread_mutex_unlock(&rp->mutex);

        /* Work-steal loop */
        if (job == ROOT_JOB_DIRTY) {
            for (;;) {
                size_t idx = atomic_fetch_add(&rp->next_idx, 1);
                if (idx >= rp->dirty_count) break;
                const uint8_t *akey = rp->dirty_keys + idx * 20;
                hash_t ah = hash_keccak256(akey, 20);
                account_t *a = find_account_h(rp->state, &ah);
                if (!a || !acct_has_flag(a, ACCT_STORAGE_DIRTY)) continue;
                resource_t *r = get_resource(rp->state, a);
                if (!r) continue;
                if (!storage_hart_empty(&r->storage))
                    storage_hart_root_hash(rp->pool, &r->storage,
                                           stor_value_encode, NULL,
                                           r->storage_root.bytes);
                else
                    r->storage_root = EMPTY_STORAGE_ROOT;
            }
        } else if (job == ROOT_JOB_ALL) {
            for (;;) {
                size_t idx = atomic_fetch_add(&rp->next_idx, 1);
                if (idx >= rp->res_count) break;
                uint32_t ri = (uint32_t)idx + 1;  /* skip slot 0 */
                resource_t *r = &rp->resources[ri];
                if (!storage_hart_empty(&r->storage))
                    storage_hart_root_hash(rp->pool, &r->storage,
                                           stor_value_encode, NULL,
                                           r->storage_root.bytes);
                else
                    r->storage_root = EMPTY_STORAGE_ROOT;
            }
        }

        /* Signal completion */
        uint32_t remaining = __sync_sub_and_fetch(&rp->active, 1);
        if (remaining == 0) {
            pthread_mutex_lock(&rp->mutex);
            rp->job_type = ROOT_JOB_NONE;
            pthread_cond_signal(&rp->work_done);
            pthread_mutex_unlock(&rp->mutex);
        }
    }
    return NULL;
}

void state_set_root_threads(state_t *s, uint32_t n) {
    if (!s) return;

    /* Shut down existing pool */
    if (s->root_pool.threads) {
        pthread_mutex_lock(&s->root_pool.mutex);
        s->root_pool.shutdown = true;
        pthread_cond_broadcast(&s->root_pool.work_ready);
        pthread_mutex_unlock(&s->root_pool.mutex);
        for (uint32_t i = 0; i < s->root_pool.count; i++)
            pthread_join(s->root_pool.threads[i], NULL);
        free(s->root_pool.threads);
        pthread_mutex_destroy(&s->root_pool.mutex);
        pthread_cond_destroy(&s->root_pool.work_ready);
        pthread_cond_destroy(&s->root_pool.work_done);
        memset(&s->root_pool, 0, sizeof(s->root_pool));
    }

    if (n <= 1) return;

    /* Create pool */
    s->root_pool.count = n;
    s->root_pool.shutdown = false;
    s->root_pool.job_type = ROOT_JOB_NONE;
    pthread_mutex_init(&s->root_pool.mutex, NULL);
    pthread_cond_init(&s->root_pool.work_ready, NULL);
    pthread_cond_init(&s->root_pool.work_done, NULL);

    s->root_pool.threads = calloc(n, sizeof(pthread_t));
    for (uint32_t i = 0; i < n; i++)
        pthread_create(&s->root_pool.threads[i], NULL, root_pool_worker, s);
}

/* Dispatch a job to the root pool and wait for completion */
static void root_pool_dispatch(state_t *s, int job_type,
                               const uint8_t *dirty_keys, size_t dirty_count,
                               resource_t *resources, uint32_t res_count) {
    struct root_pool *rp = &s->root_pool;
    pthread_mutex_lock(&rp->mutex);
    rp->job_type = job_type;
    rp->dirty_keys = dirty_keys;
    rp->dirty_count = dirty_count;
    rp->resources = resources;
    rp->res_count = res_count;
    rp->state = s;
    rp->pool = s->stor_pool;
    atomic_store(&rp->next_idx, 0);
    rp->active = rp->count;
    pthread_cond_broadcast(&rp->work_ready);
    /* Wait for all workers to finish */
    while (rp->job_type != ROOT_JOB_NONE)
        pthread_cond_wait(&rp->work_done, &rp->mutex);
    pthread_mutex_unlock(&rp->mutex);
}

void state_destroy(state_t *s) {
    if (!s) return;

    /* Shut down root pool */
    state_set_root_threads(s, 0);

    for (uint32_t i = 0; i < s->res_count; i++) {
        resource_t *r = &s->resources[i];
        free(r->code);
        if (s->stor_pool && !storage_hart_empty(&r->storage))
            storage_hart_clear(s->stor_pool, &r->storage);
    }
    free(s->resources);
    free(s->accounts);
    free(s->phantoms);
    free(s->destructed);
    free(s->pruned);

    hart_destroy(&s->acct_index);
    mem_art_destroy(&s->addr_hash_cache);
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
    dirty_free(&s->accessed_addrs);
    dirty_free(&s->accessed_slots);

    if (s->stor_pool) {
        storage_hart_pool_sync(s->stor_pool);
        storage_hart_pool_destroy(s->stor_pool);
        /* Remove temp pool file if it's on /dev/shm */
        if (strncmp(s->stor_pool_path, "/dev/shm/", 9) == 0)
            unlink(s->stor_pool_path);
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

/* =========================================================================
 * Account access
 * ========================================================================= */

static inline void track_addr_access(state_t *s, const address_t *addr) {
    if (s->track_accesses)
        dirty_push(&s->accessed_addrs, addr->bytes, 20);
}

account_t *state_get_account(state_t *s, const address_t *addr) {
    if (!s || !addr) return NULL;
    track_addr_access(s, addr);
    return find_account(s, addr->bytes);
}

bool state_exists(state_t *s, const address_t *addr) {
    if (!s || !addr) return false;
    track_addr_access(s, addr);
    account_t *a = find_account(s, addr->bytes);
    if (!a) return false;
    return acct_has_flag(a, ACCT_EXISTED) ||
           acct_has_flag(a, ACCT_CREATED) ||
           acct_has_flag(a, ACCT_DIRTY) ||
           acct_has_flag(a, ACCT_BLOCK_DIRTY);
}

bool state_is_empty(state_t *s, const address_t *addr) {
    if (!s || !addr) return true;
    track_addr_access(s, addr);
    account_t *a = find_account(s, addr->bytes);
    return !a || acct_is_empty(a);
}

uint64_t state_get_nonce(state_t *s, const address_t *addr) {
    if (!s || !addr) return 0;
    track_addr_access(s, addr);
    account_t *a = find_account(s, addr->bytes);
    return a ? a->nonce : 0;
}

uint256_t state_get_balance(state_t *s, const address_t *addr) {
    if (!s || !addr) return UINT256_ZERO;
    track_addr_access(s, addr);
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
    acct_set_flag(a, ACCT_DIRTY | ACCT_BLOCK_DIRTY);
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
    acct_set_flag(a, ACCT_DIRTY | ACCT_BLOCK_DIRTY);
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
    hash_t slot_hash = hash_keccak256(slot_be, 32);

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
    acct_set_flag(a, ACCT_DIRTY | ACCT_BLOCK_DIRTY);
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
    acct_set_flag(a, ACCT_DIRTY | ACCT_BLOCK_DIRTY);
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

    acct_set_flag(a, ACCT_CODE_DIRTY | ACCT_BLOCK_DIRTY);
    mark_tx_dirty(s, addr);
}

const uint8_t *state_get_code(state_t *s, const address_t *addr, uint32_t *out_len) {
    if (!s || !addr) { if (out_len) *out_len = 0; return NULL; }
    track_addr_access(s, addr);
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
    track_addr_access(s, addr);
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
        track_addr_access(s, addr);
        uint8_t sk[SLOT_KEY_SIZE];
        make_slot_key(addr, key, sk);
        dirty_push(&s->accessed_slots, sk, SLOT_KEY_SIZE);
    }
    account_t *a = find_account(s, addr->bytes);
    if (!a) return UINT256_ZERO;
    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = hash_keccak256(slot_be, 32);
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
    hash_t slot_hash = hash_keccak256(slot_be, 32);

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
                    a->flags = je->data.nonce.flags | (a->flags & (ACCT_DIRTY | ACCT_BLOCK_DIRTY | ACCT_MPT_DIRTY));
                else
                    a->flags = je->data.nonce.flags;
            }
            break;
        case JE_BALANCE:
            if (a) {
                a->balance = je->data.balance.val;
                if (s->prune_empty && memcmp(je->addr.bytes, RIPEMD_ADDR, 20) == 0)
                    a->flags = je->data.balance.flags | (a->flags & (ACCT_DIRTY | ACCT_BLOCK_DIRTY | ACCT_MPT_DIRTY));
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
                hash_t slot_hash = hash_keccak256(slot_be, 32);
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
    a->flags = ACCT_CREATED | ACCT_DIRTY | ACCT_BLOCK_DIRTY |
               ACCT_STORAGE_DIRTY | ACCT_STORAGE_CLEARED;

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
                           ACCT_CODE_DIRTY | ACCT_BLOCK_DIRTY | ACCT_SELF_DESTRUCTED);
            acct_set_flag(a, ACCT_STORAGE_DIRTY | ACCT_STORAGE_CLEARED);

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
                                    ACCT_DIRTY | ACCT_CODE_DIRTY);
        if (touched && (!empty || !s->prune_empty))
            acct_set_flag(a, ACCT_EXISTED);

        if (s->prune_empty && empty &&
            acct_has_flag(a, ACCT_DIRTY | ACCT_CODE_DIRTY)) {
            acct_clear_flag(a, ACCT_EXISTED);
            dead_vec_push(&s->pruned, &s->pruned_count, &s->pruned_cap,
                         (uint32_t)(a - s->accounts));
            mark_blk_dirty(s, a);
        }

        acct_clear_flag(a, ACCT_CREATED | ACCT_DIRTY |
                        ACCT_CODE_DIRTY | ACCT_SELF_DESTRUCTED);
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

    for (size_t d = 0; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        account_t *a = find_account(s, akey);
        if (!a) continue;

        bool empty = acct_is_empty(a);
        bool touched = acct_has_flag(a, ACCT_EXISTED | ACCT_CREATED |
                                    ACCT_DIRTY | ACCT_CODE_DIRTY);
        if (touched && (!empty || !s->prune_empty))
            acct_set_flag(a, ACCT_EXISTED);

        acct_clear_flag(a, ACCT_CREATED | ACCT_DIRTY |
                        ACCT_CODE_DIRTY | ACCT_SELF_DESTRUCTED);
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

        acct_clear_flag(a, ACCT_DIRTY | ACCT_CODE_DIRTY | ACCT_MPT_DIRTY |
                        ACCT_BLOCK_DIRTY | ACCT_STORAGE_DIRTY | ACCT_STORAGE_CLEARED);
    }

    dirty_clear(&s->blk_dirty);
    s->blk_dirty_cursor = 0;
    dirty_clear(&s->tx_dirty);
    s->journal_len = 0;
    mem_art_destroy(&s->originals);
    mem_art_init(&s->originals);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

state_stats_t state_get_stats(const state_t *s) {
    state_stats_t st = {0};
    if (!s) return st;
    st.account_count = s->count;
    st.storage_account_count = s->res_count;

    st.acct_vec_bytes = (size_t)s->count * sizeof(account_t);
    st.res_vec_bytes = (size_t)s->res_count * sizeof(resource_t);
    st.acct_arena_bytes = s->acct_index.arena_cap;

    st.total_tracked = st.acct_vec_bytes + st.res_vec_bytes +
                       st.acct_arena_bytes;

    return st;
}

/* =========================================================================
 * compute_root — TODO: implement with account trie
 * save/load — TODO: implement serialization
 * ========================================================================= */

hash_t state_compute_root_ex(state_t *s, bool prune_empty, bool compute_hash) {
    hash_t root = {0};
    if (!s) return root;

    bool has_pool = s->root_pool.threads != NULL;

    /* Phase 1: compute dirty storage roots (parallel or serial).
     * Must complete BEFORE the flag-clearing pass below. */
    if (compute_hash && has_pool && s->blk_dirty.count > 0) {
        root_pool_dispatch(s, ROOT_JOB_DIRTY,
                           s->blk_dirty.keys, s->blk_dirty.count,
                           NULL, 0);
    }

    /* Phase 2: single pass — promote existence, compute storage roots (serial only),
     * prune dead accounts, clear flags. */
    for (size_t d = 0; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        hash_t ah = hash_keccak256(akey, 20);
        account_t *a = find_account_h(s, &ah);
        if (!a || !acct_has_flag(a, ACCT_MPT_DIRTY)) continue;

        /* Promote/demote existence */
        if (acct_has_flag(a, ACCT_BLOCK_DIRTY)) {
            if (acct_has_flag(a, ACCT_SELF_DESTRUCTED))
                acct_clear_flag(a, ACCT_EXISTED);
            else if (!acct_is_empty(a))
                acct_set_flag(a, ACCT_EXISTED);
        }

        /* Compute storage root if dirty — only in serial mode (pool done above) */
        if (!has_pool && acct_has_flag(a, ACCT_STORAGE_DIRTY)) {
            if (compute_hash) {
                resource_t *r = get_resource(s, a);
                if (r && !storage_hart_empty(&r->storage))
                    storage_hart_root_hash(s->stor_pool, &r->storage,
                                           stor_value_encode, NULL,
                                           r->storage_root.bytes);
                else if (r)
                    r->storage_root = EMPTY_STORAGE_ROOT;
            } else {
                s->storage_roots_stale = true;
            }
        }

        /* Delete from acct_index if dead/empty — only at checkpoint blocks.
         * At non-checkpoint blocks (compute_hash=false) we must NOT delete
         * because find_account_h relies on acct_index for lookups. Deleting
         * here would cause ensure_account_h to create a duplicate entry,
         * losing the original account's storage/code. */
        if (compute_hash) {
            if (!acct_has_flag(a, ACCT_EXISTED) ||
                (acct_is_empty(a) && prune_empty))
                hart_delete(&s->acct_index, ah.bytes);
        }

        /* Clear flags */
        acct_clear_flag(a, ACCT_MPT_DIRTY | ACCT_BLOCK_DIRTY |
                        ACCT_STORAGE_DIRTY | ACCT_STORAGE_CLEARED);
    }

    /* Remove dead accounts from acct_index (phantoms, destructed, pruned) */
    #define SAFE_DELETE_IDX(i) do { \
        if ((i) < s->count) { \
            account_t *_a = &s->accounts[(i)]; \
            if (!acct_has_flag(_a, ACCT_EXISTED) || \
                (acct_is_empty(_a) && prune_empty)) { \
                hash_t _h = hash_keccak256(_a->addr.bytes, 20); \
                const uint32_t *_p = (const uint32_t *) \
                    hart_get(&s->acct_index, _h.bytes); \
                if (_p && *_p == (i)) \
                    hart_delete(&s->acct_index, _h.bytes); \
            } \
        } \
    } while(0)

    if (compute_hash) {
        for (uint32_t pi = 0; pi < s->phantom_count; pi++)
            SAFE_DELETE_IDX(s->phantoms[pi]);
        for (uint32_t di = 0; di < s->destructed_count; di++)
            SAFE_DELETE_IDX(s->destructed[di]);
        for (uint32_t ri = 0; ri < s->pruned_count; ri++)
            SAFE_DELETE_IDX(s->pruned[ri]);

        s->dead_total += s->phantom_count + s->destructed_count + s->pruned_count;
        s->phantom_count = 0;
        s->destructed_count = 0;
        s->pruned_count = 0;
    }

    #undef SAFE_DELETE_IDX

    /* Compute account trie root */
    if (compute_hash) {
        /* Recompute stale storage roots — only needed if previous blocks
         * skipped storage root computation (no-validate or checkpoint mode) */
        if (s->storage_roots_stale) {
            if (has_pool) {
                root_pool_dispatch(s, ROOT_JOB_ALL, NULL, 0,
                                   s->resources, s->res_count - 1);
            } else {
                for (uint32_t i = 1; i < s->res_count; i++) {
                    resource_t *r = &s->resources[i];
                    if (!storage_hart_empty(&r->storage))
                        storage_hart_root_hash(s->stor_pool, &r->storage,
                                               stor_value_encode, NULL,
                                               r->storage_root.bytes);
                    else
                        r->storage_root = EMPTY_STORAGE_ROOT;
                }
            }
            s->storage_roots_stale = false;
        }
        hart_root_hash(&s->acct_index, acct_trie_encode, s, root.bytes);
    }

    /* Swap blk_dirty → last_dirty (preserves for dump-prestate callers) */
    dirty_vec_t tmp = s->last_dirty;
    s->last_dirty = s->blk_dirty;
    s->blk_dirty = tmp;
    dirty_clear(&s->blk_dirty);
    s->blk_dirty_cursor = 0;

    /* Clear block-level originals (consumed by diff collection before this point) */
    mem_art_destroy(&s->blk_orig_acct); mem_art_init(&s->blk_orig_acct);
    mem_art_destroy(&s->blk_orig_stor); mem_art_init(&s->blk_orig_stor);

    /* Reset addr hash cache — addresses from this block won't repeat next block */
    mem_art_destroy(&s->addr_hash_cache);
    mem_art_init(&s->addr_hash_cache);

    return root;
}

void state_invalidate_all(state_t *s) {
    if (!s) return;
    hart_invalidate_all(&s->acct_index);
    for (uint32_t i = 1; i < s->res_count; i++) {
        resource_t *r = &s->resources[i];
        if (!storage_hart_empty(&r->storage))
            storage_hart_invalidate(s->stor_pool, &r->storage);
    }
}

hash_t state_compute_root(state_t *s, bool prune_empty) {
    return state_compute_root_ex(s, prune_empty, true);
}

void state_finalize_block(state_t *s, bool prune_empty) {
    if (!s) return;

    /* Prune dead/empty accounts from trie — must happen every block.
     * Only process NEW entries since last finalize (cursor → count).
     * Do NOT clear MPT_DIRTY, STORAGE_DIRTY, or blk_dirty — those must
     * accumulate until the checkpoint calls state_compute_root_ex. */
    for (size_t d = s->blk_dirty_cursor; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        account_t *a = find_account(s, akey);
        if (!a || !acct_has_flag(a, ACCT_MPT_DIRTY)) continue;

        /* Promote/demote existence (consumes BLOCK_DIRTY) */
        if (acct_has_flag(a, ACCT_BLOCK_DIRTY)) {
            if (acct_has_flag(a, ACCT_SELF_DESTRUCTED))
                acct_clear_flag(a, ACCT_EXISTED);
            else if (!acct_is_empty(a))
                acct_set_flag(a, ACCT_EXISTED);
            acct_clear_flag(a, ACCT_BLOCK_DIRTY);
        }

        /* Delete from trie if dead */
        if (!acct_has_flag(a, ACCT_EXISTED) ||
            (acct_is_empty(a) && prune_empty)) {
            hash_t addr_hash = hash_keccak256(a->addr.bytes, 20);
            hart_delete(&s->acct_index, addr_hash.bytes);
        }
    }

    /* Clean up dead account lists (consumed by pruning above) */
    #define SAFE_DELETE_IDX(i) do { \
        if ((i) < s->count) { \
            account_t *_a = &s->accounts[(i)]; \
            if (!acct_has_flag(_a, ACCT_EXISTED) || \
                (acct_is_empty(_a) && prune_empty)) { \
                hash_t _h = hash_keccak256(_a->addr.bytes, 20); \
                const uint32_t *_p = (const uint32_t *) \
                    hart_get(&s->acct_index, _h.bytes); \
                if (_p && *_p == (i)) \
                    hart_delete(&s->acct_index, _h.bytes); \
            } \
        } \
    } while(0)

    for (uint32_t pi = 0; pi < s->phantom_count; pi++)
        SAFE_DELETE_IDX(s->phantoms[pi]);
    for (uint32_t di = 0; di < s->destructed_count; di++)
        SAFE_DELETE_IDX(s->destructed[di]);
    for (uint32_t ri = 0; ri < s->pruned_count; ri++)
        SAFE_DELETE_IDX(s->pruned[ri]);

    s->dead_total += s->phantom_count + s->destructed_count + s->pruned_count;
    s->phantom_count = 0;
    s->destructed_count = 0;
    s->pruned_count = 0;

    #undef SAFE_DELETE_IDX

    /* Advance cursor — next finalize starts from here.
     * blk_dirty, MPT_DIRTY, STORAGE_DIRTY are NOT cleared —
     * they accumulate until state_compute_root_ex processes them. */
    s->blk_dirty_cursor = s->blk_dirty.count;
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
    dirty_clear(&s->accessed_addrs);
    dirty_clear(&s->accessed_slots);
}

void state_disable_access_tracking(state_t *s) {
    if (!s) return;
    s->track_accesses = false;
    dirty_clear(&s->accessed_addrs);
    dirty_clear(&s->accessed_slots);
}

size_t state_collect_dirty_addresses(const state_t *s, address_t *out, size_t max) {
    if (!s || !out || max == 0) return 0;
    size_t n = 0;
    for (size_t i = 0; i < s->last_dirty.count && n < max; i++) {
        const uint8_t *akey = s->last_dirty.keys + i * 20;
        bool dup = false;
        for (size_t j = 0; j < n; j++) {
            if (memcmp(out[j].bytes, akey, 20) == 0) { dup = true; break; }
        }
        if (!dup) memcpy(out[n++].bytes, akey, 20);
    }
    return n;
}

size_t state_collect_accessed_addresses(const state_t *s, address_t *out, size_t max) {
    if (!s || !out || max == 0) return 0;
    size_t n = 0;
    for (size_t i = 0; i < s->accessed_addrs.count && n < max; i++) {
        const uint8_t *akey = s->accessed_addrs.keys + i * 20;
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
    uint16_t     group_count;
    uint16_t     group_cap;
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
    for (uint16_t i = 0; i < ctx->group_count; i++) {
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

    uint16_t group_cap = 64;
    uint16_t group_count = 0;
    addr_diff_t *groups = calloc(group_cap, sizeof(addr_diff_t));
    if (!groups) return;

    /* Walk blk_dirty — all accounts modified in this block */
    for (size_t d = 0; d < s->blk_dirty.count; d++) {
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
    account_t *new_accts = calloc(new_cap, sizeof(account_t));
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

    free(s->accounts);
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

    /* Clear dead account tracking */
    s->phantom_count = 0;
    s->destructed_count = 0;
    s->pruned_count = 0;
    s->dead_total = 0;
}

/* =========================================================================
 * Save / Load — binary snapshot of full state
 *
 * Format (little-endian):
 *   Header:  magic("ART1", 4) + block_number(8) + state_root(32) + account_count(4)
 *   Per account:
 *     addr(20) + nonce(8) + balance(32) + code_hash(32) + storage_root(32) +
 *     code_size(4) + storage_count(4)
 *     [storage_count × (key(32) + value(32))]
 *
 * Code bytes are NOT saved — code_store has them on disk.
 * On load, acct_index and storage harts are rebuilt from scratch.
 * ========================================================================= */

/* =========================================================================
 * Storage pool path
 * ========================================================================= */

void state_set_storage_path(state_t *s, const char *dir) {
    if (!s || !dir) return;
    if (s->stor_pool) {
        storage_hart_pool_destroy(s->stor_pool);
        s->stor_pool = NULL;
    }
    snprintf(s->stor_pool_path, sizeof(s->stor_pool_path),
             "%s/storage_pool.dat", dir);
    s->stor_pool = storage_hart_pool_open(s->stor_pool_path);
    if (!s->stor_pool)
        s->stor_pool = storage_hart_pool_create(s->stor_pool_path);
}

/* =========================================================================
 * State save/load
 * ========================================================================= */

#define STATE_MAGIC "ART1"

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

static bool read_all(FILE *f, void *buf, size_t n) {
    return fread(buf, 1, n, f) == n;
}

bool state_save(const state_t *s, const char *path, const hash_t *state_root) {
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
    if (!write_all(f, STATE_MAGIC, 4)) goto fail;
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
    }

    fclose(f);
    return true;
fail:
    fclose(f);
    return false;
}

bool state_load(state_t *s, const char *path, hash_t *out_root) {
    if (!s || !path) return false;
    FILE *f = fopen(path, "rb");
    if (!f) return false;

    /* Header */
    char magic[4];
    if (!read_all(f, magic, 4) || memcmp(magic, STATE_MAGIC, 4) != 0) goto fail;
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
            uint32_t nc = s->capacity * 2;
            account_t *na = realloc(s->accounts, nc * sizeof(account_t));
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
                uint32_t nc = s->res_capacity * 2;
                resource_t *nr = realloc(s->resources, nc * sizeof(resource_t));
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
        if ((i + 1) % 1000000 == 0)
            fprintf(stderr, "  state_load: %u/%u accounts...\n", i + 1, acct_count);
    }

    if (s->stor_pool) storage_hart_pool_sync(s->stor_pool);

    fclose(f);
    fprintf(stderr, "state_load: %u accounts from %s (block %lu)\n",
            acct_count, path, block_number);
    return true;
fail:
    fclose(f);
    return false;
}
