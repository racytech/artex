/**
 * state2.c — Simplified Ethereum world state.
 *
 * Core data structures:
 *   - acct_index (hart): keccak(addr) → uint32_t index into accounts[]
 *   - accounts[]: compact account data (nonce, balance, status, resource_idx)
 *   - resources[]: per-account code + storage (only ~3% of accounts)
 *   - journal: snapshot/revert for EVM execution
 *   - bitmaps: dirty tracking (acct_dirty, stor_dirty)
 *
 * Design principles:
 *   - No per-account flags for dirty tracking — bitmaps on state_t
 *   - Account status is minimal: EXISTED, CREATED, SELF_DESTRUCTED, HAS_CODE
 *   - finalize_block brings state to correct form
 *   - compute_root is pure hash computation
 */

#include "state.h"
#include "hashed_art.h"
#include "mem_art.h"
#include "storage_hart.h"
#include "code_store.h"
#include "keccak256.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

/* =========================================================================
 * Internal types
 * ========================================================================= */

/* Account status bits — minimal set */
#define STATUS_EXISTED         (1 << 0)  /* in the world state trie */
#define STATUS_CREATED         (1 << 1)  /* created this tx (EIP-6780) */
#define STATUS_SELF_DESTRUCTED (1 << 2)  /* self-destructed this tx */
#define STATUS_HAS_CODE        (1 << 3)  /* has deployed code */

typedef struct {
    address_t addr;
    uint64_t  nonce;
    uint256_t balance;
    uint32_t  resource_idx;   /* index into resources[], 0 = none */
    uint8_t   status;
} account_t;

typedef struct {
    hash_t          code_hash;
    hash_t          storage_root;
    uint8_t        *code;
    uint32_t        code_size;
    uint8_t        *jumpdest_bitmap;
    storage_hart_t  storage;
} resource_t;

/* Journal entry types */
typedef enum {
    JE_NONCE,
    JE_BALANCE,
    JE_CODE,
    JE_STORAGE,
    JE_CREATE,
    JE_SELF_DESTRUCT,
    JE_TOUCH,           /* EIP-161: erase_if_empty */
    JE_WARM_ADDR,
    JE_WARM_SLOT,
    JE_TSTORE,
} journal_type_t;

typedef struct {
    journal_type_t type;
    address_t addr;
    union {
        struct { uint64_t val; uint8_t status; }            nonce;
        struct { uint256_t val; uint8_t status; }           balance;
        struct { hash_t code_hash; uint8_t *code;
                 uint32_t code_size; uint8_t status; }      code;
        struct { uint256_t key; uint256_t val; }            storage;
        struct { uint64_t nonce; uint256_t balance;
                 hash_t code_hash; hash_t storage_root;
                 uint8_t *code; uint32_t code_size;
                 uint8_t status; }                          create;
        struct { uint8_t status; }                          selfdestruct;
        struct { uint8_t status; }                          touch;
        struct { uint256_t key; uint256_t val; }            tstore;
        uint256_t warm_slot_key;
    } data;
} journal_entry_t;

/* =========================================================================
 * Bitmap helpers
 * ========================================================================= */

typedef struct {
    uint8_t  *bits;
    uint32_t  cap;      /* capacity in bits */
} bitmap_t;

static inline void bm_set(bitmap_t *bm, uint32_t idx) {
    if (idx < bm->cap)
        bm->bits[idx / 8] |= (1u << (idx % 8));
}

static inline void bm_clear_bit(bitmap_t *bm, uint32_t idx) {
    if (idx < bm->cap)
        bm->bits[idx / 8] &= ~(1u << (idx % 8));
}

static inline bool bm_test(const bitmap_t *bm, uint32_t idx) {
    return idx < bm->cap &&
           (bm->bits[idx / 8] & (1u << (idx % 8))) != 0;
}

static void bm_grow(bitmap_t *bm, uint32_t min_cap) {
    if (min_cap <= bm->cap) return;
    uint32_t new_cap = bm->cap ? bm->cap * 2 : 1024;
    while (new_cap < min_cap) new_cap *= 2;
    uint32_t old_bytes = (bm->cap + 7) / 8;
    uint32_t new_bytes = (new_cap + 7) / 8;
    uint8_t *nb = realloc(bm->bits, new_bytes);
    if (!nb) return;
    memset(nb + old_bytes, 0, new_bytes - old_bytes);
    bm->bits = nb;
    bm->cap = new_cap;
}

static void bm_clear_all(bitmap_t *bm) {
    if (bm->bits)
        memset(bm->bits, 0, (bm->cap + 7) / 8);
}

static void bm_set_all(bitmap_t *bm, uint32_t count) {
    uint32_t full_bytes = count / 8;
    if (full_bytes > 0)
        memset(bm->bits, 0xFF, full_bytes);
    for (uint32_t i = full_bytes * 8; i < count; i++)
        bm_set(bm, i);
}

static void bm_init(bitmap_t *bm, uint32_t cap) {
    bm->cap = cap;
    bm->bits = calloc((cap + 7) / 8, 1);
}

static void bm_free(bitmap_t *bm) {
    free(bm->bits);
    bm->bits = NULL;
    bm->cap = 0;
}

/* =========================================================================
 * state_t
 * ========================================================================= */

#define ACCT_INIT_CAP    4096
#define RES_INIT_CAP     1024
#define JOURNAL_INIT_CAP 4096

struct state {
    /* Account index: keccak(addr) → uint32_t slot in accounts[] */
    hart_t   acct_index;

    /* Account vector */
    account_t *accounts;
    uint32_t   acct_count;
    uint32_t   acct_cap;

    /* Resource vector (slot 0 reserved as "none") */
    resource_t *resources;
    uint32_t    res_count;
    uint32_t    res_cap;

    /* Storage pool */
    storage_hart_pool_t *stor_pool;
    char                 stor_pool_path[512];

    /* Code store (external, not owned) */
    code_store_t *code_store;

    /* Journal for snapshot/revert */
    journal_entry_t *journal;
    uint32_t         journal_len;
    uint32_t         journal_cap;

    /* EIP-2929 warm sets */
    mem_art_t warm_addrs;
    mem_art_t warm_slots;

    /* EIP-1153 transient storage */
    mem_art_t transient;

    /* EIP-2200 committed (original) storage values */
    mem_art_t originals;

    /* Dirty bitmaps — index by account slot */
    bitmap_t acct_dirty;    /* account was modified (needs trie rehash) */
    bitmap_t stor_dirty;    /* storage was modified (needs storage root recompute) */

    /* Per-tx dirty tracking (for commit_tx) */
    uint32_t *tx_dirty;     /* account indices touched this tx */
    uint32_t  tx_dirty_count;
    uint32_t  tx_dirty_cap;

    /* Block state */
    uint64_t current_block;
    bool     prune_empty;

    /* Address hash cache: addr[20] → keccak(addr)[32], reset per block */
    mem_art_t addr_hash_cache;

    /* Block-level originals for undo log */
    mem_art_t blk_orig_acct;
    mem_art_t blk_orig_stor;

    /* Dead account count (for compaction trigger) */
    uint32_t dead_total;

    /* dump-prestate support */
    bool track_accesses;
    mem_art_t accessed_slots;
};

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

static inline bool status_has(const account_t *a, uint8_t f) {
    return (a->status & f) != 0;
}
static inline void status_set(account_t *a, uint8_t f) {
    a->status |= f;
}
static inline void status_clear(account_t *a, uint8_t f) {
    a->status &= ~f;
}

static inline bool acct_is_empty(const account_t *a) {
    return a->nonce == 0 &&
           uint256_is_zero(&a->balance) &&
           !status_has(a, STATUS_HAS_CODE);
}

static const hash_t EMPTY_CODE_HASH = {{
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
    0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
    0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70
}};

static const hash_t EMPTY_STORAGE_ROOT = {{
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21
}};

/* Cached keccak: compute once per address per block */
static hash_t addr_hash(state_t *s, const uint8_t addr[20]) {
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

/* Find account by keccak(addr). Returns NULL if not in acct_index. */
static account_t *find_account_h(const state_t *s, const hash_t *addr_hash) {
    const uint32_t *p = (const uint32_t *)hart_get(&s->acct_index, addr_hash->bytes);
    if (!p) return NULL;
    uint32_t idx = *p;
    return (idx < s->acct_count) ? &s->accounts[idx] : NULL;
}

static account_t *find_account(state_t *s, const address_t *addr) {
    hash_t h = addr_hash(s, addr->bytes);
    return find_account_h(s, &h);
}

/* Get resource for account. Returns NULL if resource_idx == 0. */
static resource_t *get_resource(const state_t *s, const account_t *a) {
    if (!a->resource_idx || a->resource_idx >= s->res_count) return NULL;
    return &((state_t *)s)->resources[a->resource_idx];
}

/* Ensure resource exists for account. Allocates if needed. */
static resource_t *ensure_resource(state_t *s, account_t *a) {
    if (a->resource_idx) return &s->resources[a->resource_idx];

    if (s->res_count >= s->res_cap) {
        uint32_t nc = s->res_cap * 2;
        resource_t *nr = realloc(s->resources, nc * sizeof(resource_t));
        if (!nr) return NULL;
        memset(nr + s->res_cap, 0, (nc - s->res_cap) * sizeof(resource_t));
        s->resources = nr;
        s->res_cap = nc;
        bm_grow(&s->stor_dirty, nc);
    }

    uint32_t ridx = s->res_count++;
    resource_t *r = &s->resources[ridx];
    memset(r, 0, sizeof(*r));
    r->code_hash = EMPTY_CODE_HASH;
    r->storage_root = EMPTY_STORAGE_ROOT;
    a->resource_idx = ridx;
    return r;
}

/* Ensure account exists. Creates new slot if not in acct_index. */
static account_t *ensure_account(state_t *s, const address_t *addr, const hash_t *addr_hash) {
    account_t *existing = find_account_h(s, addr_hash);
    if (existing) return existing;

    if (s->acct_count >= s->acct_cap) {
        uint32_t nc = s->acct_cap * 2;
        account_t *na = realloc(s->accounts, nc * sizeof(account_t));
        if (!na) return NULL;
        memset(na + s->acct_cap, 0, (nc - s->acct_cap) * sizeof(account_t));
        s->accounts = na;
        s->acct_cap = nc;
        bm_grow(&s->acct_dirty, nc);
    }

    uint32_t idx = s->acct_count++;
    account_t *a = &s->accounts[idx];
    memset(a, 0, sizeof(*a));
    address_copy(&a->addr, addr);

    hart_insert(&s->acct_index, addr_hash->bytes, &idx);
    return a;
}

/* Hash a storage slot key: keccak256(uint256_to_big_endian(key)) */
static hash_t slot_hash(const uint256_t *key) {
    uint8_t be[32];
    uint256_to_bytes(key, be);
    return hash_keccak256(be, 32);
}

/* Build a 52-byte slot key: addr[20] + key_be[32] */
static void make_slot_key(const address_t *addr, const uint256_t *key, uint8_t out[52]) {
    memcpy(out, addr->bytes, 20);
    uint256_to_bytes(key, out + 20);
}

/* Read storage value from storage hart */
static uint256_t storage_read(const state_t *s, const account_t *a,
                               const uint8_t sh_key[32]) {
    resource_t *r = get_resource(s, a);
    if (!r || !s->stor_pool) return UINT256_ZERO;
    if (storage_hart_empty(&r->storage)) return UINT256_ZERO;
    uint8_t val[32];
    if (!storage_hart_get(s->stor_pool, &r->storage, sh_key, val))
        return UINT256_ZERO;
    return uint256_from_bytes(val, 32);
}

/* Mark account dirty (needs trie rehash at compute_root time) */
static void mark_dirty(state_t *s, uint32_t acct_idx, const hash_t *addr_hash) {
    if (!bm_test(&s->acct_dirty, acct_idx)) {
        bm_set(&s->acct_dirty, acct_idx);
        hart_mark_path_dirty(&s->acct_index, addr_hash->bytes);
    }
}

/* Track account index for commit_tx processing */
static void mark_tx_dirty(state_t *s, uint32_t acct_idx) {
    if (s->tx_dirty_count >= s->tx_dirty_cap) {
        uint32_t nc = s->tx_dirty_cap * 2;
        uint32_t *nd = realloc(s->tx_dirty, nc * sizeof(uint32_t));
        if (!nd) return;
        s->tx_dirty = nd;
        s->tx_dirty_cap = nc;
    }
    s->tx_dirty[s->tx_dirty_count++] = acct_idx;
}

/* Account index from pointer */
static inline uint32_t acct_idx(const state_t *s, const account_t *a) {
    return (uint32_t)(a - s->accounts);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

state_t *state_create(code_store_t *cs) {
    state_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;

    s->code_store = cs;

    s->accounts = calloc(ACCT_INIT_CAP, sizeof(account_t));
    if (!s->accounts) { free(s); return NULL; }
    s->acct_cap = ACCT_INIT_CAP;

    s->resources = calloc(RES_INIT_CAP, sizeof(resource_t));
    if (!s->resources) { free(s->accounts); free(s); return NULL; }
    s->res_cap = RES_INIT_CAP;
    s->res_count = 1;  /* slot 0 reserved */

    s->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!s->journal) { free(s->resources); free(s->accounts); free(s); return NULL; }
    s->journal_cap = JOURNAL_INIT_CAP;

    s->tx_dirty = malloc(256 * sizeof(uint32_t));
    if (!s->tx_dirty) { free(s->journal); free(s->resources); free(s->accounts); free(s); return NULL; }
    s->tx_dirty_cap = 256;

    hart_init(&s->acct_index, sizeof(uint32_t));
    mem_art_init(&s->addr_hash_cache);
    mem_art_init(&s->warm_addrs);
    mem_art_init(&s->warm_slots);
    mem_art_init(&s->transient);
    mem_art_init(&s->originals);
    mem_art_init(&s->blk_orig_acct);
    mem_art_init(&s->blk_orig_stor);
    mem_art_init(&s->accessed_slots);

    bm_init(&s->acct_dirty, ACCT_INIT_CAP);
    bm_init(&s->stor_dirty, RES_INIT_CAP);

    /* Storage pool on tmpfs */
    char tmp[64];
    snprintf(tmp, sizeof(tmp), "/dev/shm/artex_stor_%d.dat", (int)getpid());
    s->stor_pool = storage_hart_pool_create(tmp);
    snprintf(s->stor_pool_path, sizeof(s->stor_pool_path), "%s", tmp);

    return s;
}

void state_destroy(state_t *s) {
    if (!s) return;

    for (uint32_t i = 1; i < s->res_count; i++) {
        resource_t *r = &s->resources[i];
        free(r->code);
        free(r->jumpdest_bitmap);
        if (s->stor_pool && !storage_hart_empty(&r->storage))
            storage_hart_clear(s->stor_pool, &r->storage);
    }
    free(s->resources);
    free(s->accounts);
    free(s->journal);
    free(s->tx_dirty);

    hart_destroy(&s->acct_index);
    mem_art_destroy(&s->addr_hash_cache);
    mem_art_destroy(&s->warm_addrs);
    mem_art_destroy(&s->warm_slots);
    mem_art_destroy(&s->transient);
    mem_art_destroy(&s->originals);
    mem_art_destroy(&s->blk_orig_acct);
    mem_art_destroy(&s->blk_orig_stor);
    mem_art_destroy(&s->accessed_slots);

    bm_free(&s->acct_dirty);
    bm_free(&s->stor_dirty);

    if (s->stor_pool) {
        storage_hart_pool_destroy(s->stor_pool);
        unlink(s->stor_pool_path);
    }

    free(s);
}

/* =========================================================================
 * Journal helpers
 * ========================================================================= */

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

/* =========================================================================
 * Account access
 * ========================================================================= */

account_t *state_get_account(state_t *s, const address_t *addr) {
    /* NOTE: returns internal account_t which differs from account.h's layout.
     * Callers that depend on account.h fields will need adaptation. */
    if (!s || !addr) return NULL;
    return find_account(s, addr);
}

bool state_exists(state_t *s, const address_t *addr) {
    if (!s || !addr) return false;
    account_t *a = find_account(s, addr);
    return a && (status_has(a, STATUS_EXISTED) ||
                 status_has(a, STATUS_CREATED) ||
                 bm_test(&s->acct_dirty, acct_idx(s, a)));
}

bool state_is_empty(state_t *s, const address_t *addr) {
    if (!s || !addr) return true;
    account_t *a = find_account(s, addr);
    return !a || acct_is_empty(a);
}

uint64_t state_get_nonce(state_t *s, const address_t *addr) {
    if (!s || !addr) return 0;
    account_t *a = find_account(s, addr);
    return a ? a->nonce : 0;
}

uint256_t state_get_balance(state_t *s, const address_t *addr) {
    if (!s || !addr) return UINT256_ZERO;
    account_t *a = find_account(s, addr);
    return a ? a->balance : UINT256_ZERO;
}

void state_set_nonce(state_t *s, const address_t *addr, uint64_t nonce) {
    if (!s || !addr) return;
    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = ensure_account(s, addr, &h);
    if (!a) return;

    uint32_t idx = acct_idx(s, a);
    mark_dirty(s, idx, &h);

    journal_entry_t je = { .type = JE_NONCE, .addr = *addr,
        .data.nonce = { .val = a->nonce, .status = a->status } };
    journal_push(s, &je);

    a->nonce = nonce;
    mark_tx_dirty(s, idx);
}

void state_set_balance(state_t *s, const address_t *addr, const uint256_t *bal) {
    if (!s || !addr || !bal) return;
    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = ensure_account(s, addr, &h);
    if (!a) return;

    uint32_t idx = acct_idx(s, a);
    mark_dirty(s, idx, &h);

    journal_entry_t je = { .type = JE_BALANCE, .addr = *addr,
        .data.balance = { .val = a->balance, .status = a->status } };
    journal_push(s, &je);

    a->balance = *bal;
    mark_tx_dirty(s, idx);
}

void state_add_balance(state_t *s, const address_t *addr, const uint256_t *amount) {
    if (!s || !addr || !amount) return;
    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = ensure_account(s, addr, &h);
    if (!a) return;

    uint32_t idx = acct_idx(s, a);
    mark_dirty(s, idx, &h);

    journal_entry_t je = { .type = JE_BALANCE, .addr = *addr,
        .data.balance = { .val = a->balance, .status = a->status } };
    journal_push(s, &je);

    a->balance = uint256_add(&a->balance, amount);
    mark_tx_dirty(s, idx);
}

bool state_sub_balance(state_t *s, const address_t *addr, const uint256_t *amount) {
    if (!s || !addr || !amount) return false;
    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = find_account_h(s, &h);
    if (!a || uint256_lt(&a->balance, amount)) return false;

    uint32_t idx = acct_idx(s, a);
    mark_dirty(s, idx, &h);

    journal_entry_t je = { .type = JE_BALANCE, .addr = *addr,
        .data.balance = { .val = a->balance, .status = a->status } };
    journal_push(s, &je);

    a->balance = uint256_sub(&a->balance, amount);
    mark_tx_dirty(s, idx);
    return true;
}

/* =========================================================================
 * Code
 * ========================================================================= */

hash_t state_get_code_hash(state_t *s, const address_t *addr) {
    if (!s || !addr) return EMPTY_CODE_HASH;
    account_t *a = find_account(s, addr);
    if (!a || !status_has(a, STATUS_HAS_CODE)) return EMPTY_CODE_HASH;
    resource_t *r = get_resource(s, a);
    return r ? r->code_hash : EMPTY_CODE_HASH;
}

uint32_t state_get_code_size(state_t *s, const address_t *addr) {
    if (!s || !addr) return 0;
    account_t *a = find_account(s, addr);
    if (!a || !status_has(a, STATUS_HAS_CODE)) return 0;
    resource_t *r = get_resource(s, a);
    if (!r) return 0;
    if (r->code_size > 0) return r->code_size;
    /* Trigger code_store load if needed */
    state_get_code(s, addr, NULL);
    return r->code_size;
}

const uint8_t *state_get_code(state_t *s, const address_t *addr, uint32_t *out_len) {
    if (!s || !addr) { if (out_len) *out_len = 0; return NULL; }
    account_t *a = find_account(s, addr);
    if (!a || !status_has(a, STATUS_HAS_CODE)) { if (out_len) *out_len = 0; return NULL; }
    resource_t *r = get_resource(s, a);
    if (!r) { if (out_len) *out_len = 0; return NULL; }

    /* Load from code_store if not cached */
    if (!r->code && s->code_store) {
        uint32_t size = code_store_get_size(s->code_store, r->code_hash.bytes);
        if (size > 0) {
            r->code = malloc(size);
            if (r->code)
                r->code_size = code_store_get(s->code_store, r->code_hash.bytes,
                                               r->code, size);
        }
    }
    if (out_len) *out_len = r->code_size;
    return r->code;
}

const uint8_t *state_get_jumpdest_bitmap(state_t *s, const address_t *addr) {
    if (!s || !addr) return NULL;
    account_t *a = find_account(s, addr);
    if (!a || !status_has(a, STATUS_HAS_CODE)) return NULL;
    resource_t *r = get_resource(s, a);
    if (!r || !r->code || !r->code_size) return NULL;

    if (r->jumpdest_bitmap) return r->jumpdest_bitmap;

    /* Build bitmap: 1 bit per byte, set if valid JUMPDEST */
    uint32_t bm_size = (r->code_size + 7) / 8;
    r->jumpdest_bitmap = calloc(bm_size, 1);
    if (!r->jumpdest_bitmap) return NULL;

    for (uint32_t pc = 0; pc < r->code_size; pc++) {
        uint8_t op = r->code[pc];
        if (op == 0x5B)  /* JUMPDEST */
            r->jumpdest_bitmap[pc / 8] |= (1u << (pc % 8));
        else if (op >= 0x60 && op <= 0x7F)  /* PUSHn */
            pc += (op - 0x5F);
    }
    return r->jumpdest_bitmap;
}

void state_set_code(state_t *s, const address_t *addr,
                    const uint8_t *code, uint32_t len) {
    if (!s || !addr) return;
    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = ensure_account(s, addr, &h);
    if (!a) return;

    resource_t *r = ensure_resource(s, a);
    if (!r) return;

    uint32_t idx = acct_idx(s, a);
    mark_dirty(s, idx, &h);

    /* Journal saves old code pointer — ownership transferred to journal.
     * On revert, journal restores it. On commit, old code is freed. */
    journal_entry_t je = { .type = JE_CODE, .addr = *addr,
        .data.code = { .code_hash = r->code_hash, .code = r->code,
                       .code_size = r->code_size, .status = a->status } };
    if (!journal_push(s, &je))
        free(r->code);  /* journal failed — free old code */

    r->code = NULL;
    r->code_size = 0;
    if (r->jumpdest_bitmap) { free(r->jumpdest_bitmap); r->jumpdest_bitmap = NULL; }

    if (code && len > 0) {
        r->code = malloc(len);
        if (r->code) {
            memcpy(r->code, code, len);
            r->code_size = len;
        }
        r->code_hash = hash_keccak256(code, len);
        status_set(a, STATUS_HAS_CODE);
        if (s->code_store)
            code_store_put(s->code_store, r->code_hash.bytes, code, len);
    } else {
        r->code_hash = EMPTY_CODE_HASH;
        status_clear(a, STATUS_HAS_CODE);
    }

    mark_tx_dirty(s, idx);
}

/* =========================================================================
 * Storage
 * ========================================================================= */

static uint32_t stor_value_encode(const uint8_t key[32], const void *val,
                                   uint8_t *out, void *ctx) {
    (void)key; (void)ctx;
    const uint256_t *v = (const uint256_t *)val;
    /* RLP-encode the value: trim leading zeros */
    uint8_t be[32];
    uint256_to_bytes(v, be);
    int start = 0;
    while (start < 31 && be[start] == 0) start++;
    int vlen = 32 - start;
    if (vlen == 1 && be[start] <= 0x7F) {
        out[0] = be[start];
        return 1;
    }
    out[0] = (uint8_t)(0x80 + vlen);
    memcpy(out + 1, be + start, vlen);
    return 1 + vlen;
}

uint256_t state_get_storage(state_t *s, const address_t *addr, const uint256_t *key) {
    if (!s || !addr || !key) return UINT256_ZERO;
    account_t *a = find_account(s, addr);
    if (!a) return UINT256_ZERO;
    hash_t sh = slot_hash(key);
    return storage_read(s, a, sh.bytes);
}

void state_set_storage(state_t *s, const address_t *addr,
                       const uint256_t *key, const uint256_t *value) {
    if (!s || !addr || !key || !value) return;

    /* Access tracking for dump-prestate */
    if (s->track_accesses) {
        uint8_t sk[52];
        make_slot_key(addr, key, sk);
        if (!mem_art_contains(&s->accessed_slots, sk, 52))
            mem_art_insert(&s->accessed_slots, sk, 52, NULL, 0);
    }

    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = ensure_account(s, addr, &h);
    if (!a) return;

    uint32_t idx = acct_idx(s, a);
    mark_dirty(s, idx, &h);

    hash_t sh = slot_hash(key);
    uint256_t old_value = storage_read(s, a, sh.bytes);

    journal_entry_t je = { .type = JE_STORAGE, .addr = *addr,
        .data.storage = { .key = *key, .val = old_value } };
    journal_push(s, &je);

    /* Save original for EIP-2200 (per-tx, first write only) */
    uint8_t skey[52];
    make_slot_key(addr, key, skey);
    if (!mem_art_contains(&s->originals, skey, 52))
        mem_art_insert(&s->originals, skey, 52, &old_value, sizeof(uint256_t));

    /* Save block-level original for undo log (first write per block) */
    if (!mem_art_contains(&s->blk_orig_stor, skey, 52))
        mem_art_insert(&s->blk_orig_stor, skey, 52, &old_value, sizeof(uint256_t));

    /* Write to storage */
    resource_t *r = ensure_resource(s, a);
    if (!r || !s->stor_pool) return;

    uint8_t val_be[32];
    uint256_to_bytes(value, val_be);
    if (uint256_is_zero(value))
        storage_hart_del(s->stor_pool, &r->storage, sh.bytes);
    else
        storage_hart_put(s->stor_pool, &r->storage, sh.bytes, val_be);

    storage_hart_mark_dirty(s->stor_pool, &r->storage, sh.bytes);
    bm_set(&s->stor_dirty, a->resource_idx);
    mark_tx_dirty(s, idx);
}

bool state_has_storage(state_t *s, const address_t *addr) {
    if (!s || !addr) return false;
    account_t *a = find_account(s, addr);
    if (!a) return false;
    resource_t *r = get_resource(s, a);
    return r && !storage_hart_empty(&r->storage);
}

/* =========================================================================
 * SLOAD / SSTORE combined lookups (EIP-2200)
 * ========================================================================= */

uint256_t state_sload(state_t *s, const address_t *addr,
                      const uint256_t *key, bool *was_warm) {
    if (!s || !addr || !key) {
        if (was_warm) *was_warm = false;
        return UINT256_ZERO;
    }

    /* Warm check — don't auto-insert, EVM layer calls mark_storage_warm */
    uint8_t skey[52];
    make_slot_key(addr, key, skey);
    if (was_warm) *was_warm = mem_art_contains(&s->warm_slots, skey, 52);

    hash_t sh = slot_hash(key);
    account_t *a = find_account(s, addr);
    if (!a) return UINT256_ZERO;
    return storage_read(s, a, sh.bytes);
}

void state_sstore_lookup(state_t *s, const address_t *addr,
                         const uint256_t *key,
                         uint256_t *current, uint256_t *original,
                         bool *was_warm) {
    if (!s || !addr || !key) return;

    /* Warm check — don't auto-insert */
    uint8_t skey[52];
    make_slot_key(addr, key, skey);
    if (was_warm) *was_warm = mem_art_contains(&s->warm_slots, skey, 52);

    /* Current value */
    hash_t sh = slot_hash(key);
    account_t *a = find_account(s, addr);
    uint256_t cur = a ? storage_read(s, a, sh.bytes) : UINT256_ZERO;
    if (current) *current = cur;

    /* Original (committed) value — from originals cache */
    if (original) {
        size_t olen;
        const void *orig = mem_art_get(&s->originals, skey, 52, &olen);
        if (orig && olen == sizeof(uint256_t))
            memcpy(original, orig, sizeof(uint256_t));
        else
            *original = cur;  /* no prior commit → current IS original */
    }
}

/* =========================================================================
 * Account lifecycle
 * ========================================================================= */

void state_create_account(state_t *s, const address_t *addr) {
    if (!s || !addr) return;
    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = ensure_account(s, addr, &h);
    if (!a) return;

    uint32_t idx = acct_idx(s, a);
    mark_dirty(s, idx, &h);

    /* Journal full account state for revert */
    resource_t *r = get_resource(s, a);
    journal_entry_t je = { .type = JE_CREATE, .addr = *addr,
        .data.create = {
            .nonce = a->nonce, .balance = a->balance,
            .code_hash = r ? r->code_hash : EMPTY_CODE_HASH,
            .storage_root = r ? r->storage_root : EMPTY_STORAGE_ROOT,
            .code = r ? r->code : NULL,
            .code_size = r ? r->code_size : 0,
            .status = a->status
        } };
    if (!journal_push(s, &je) && r)
        free(r->code);  /* journal failed — free old code since we'll lose the pointer */

    /* Preserve balance, reset everything else */
    uint256_t bal = a->balance;
    a->nonce = 0;
    a->balance = bal;
    a->status = STATUS_EXISTED | STATUS_CREATED;

    if (r) {
        r->code = NULL;
        r->code_size = 0;
        r->code_hash = EMPTY_CODE_HASH;
        r->storage_root = EMPTY_STORAGE_ROOT;
        free(r->jumpdest_bitmap);
        r->jumpdest_bitmap = NULL;
        if (s->stor_pool && !storage_hart_empty(&r->storage))
            storage_hart_clear(s->stor_pool, &r->storage);
        bm_set(&s->stor_dirty, a->resource_idx);
    }

    mark_tx_dirty(s, idx);
}

void state_self_destruct(state_t *s, const address_t *addr) {
    if (!s || !addr) return;
    hash_t h = addr_hash(s, addr->bytes);
    account_t *a = ensure_account(s, addr, &h);
    if (!a) return;

    journal_entry_t je = { .type = JE_SELF_DESTRUCT, .addr = *addr,
        .data.selfdestruct = { .status = a->status } };
    journal_push(s, &je);

    status_set(a, STATUS_SELF_DESTRUCTED);
}

/* =========================================================================
 * EIP-2929 warm/cold access
 * ========================================================================= */

void state_mark_addr_warm(state_t *s, const address_t *addr) {
    if (!s || !addr) return;
    if (mem_art_contains(&s->warm_addrs, addr->bytes, 20)) return;
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
    uint8_t skey[52];
    make_slot_key(addr, key, skey);
    if (mem_art_contains(&s->warm_slots, skey, 52)) return;
    uint8_t one = 1;
    mem_art_insert(&s->warm_slots, skey, 52, &one, 1);
    journal_entry_t je = { .type = JE_WARM_SLOT, .addr = *addr,
        .data.warm_slot_key = *key };
    journal_push(s, &je);
}

bool state_is_storage_warm(const state_t *s, const address_t *addr, const uint256_t *key) {
    if (!s || !addr || !key) return false;
    uint8_t skey[52];
    make_slot_key(addr, key, skey);
    return mem_art_contains(&((state_t *)s)->warm_slots, skey, 52);
}

/* =========================================================================
 * EIP-1153 transient storage
 * ========================================================================= */

uint256_t state_tload(state_t *s, const address_t *addr, const uint256_t *key) {
    if (!s || !addr || !key) return UINT256_ZERO;
    uint8_t skey[52];
    make_slot_key(addr, key, skey);
    size_t vlen;
    const void *val = mem_art_get(&s->transient, skey, 52, &vlen);
    return (val && vlen == sizeof(uint256_t)) ? *(const uint256_t *)val : UINT256_ZERO;
}

void state_tstore(state_t *s, const address_t *addr,
                  const uint256_t *key, const uint256_t *value) {
    if (!s || !addr || !key || !value) return;
    uint8_t skey[52];
    make_slot_key(addr, key, skey);

    /* Journal old value for revert */
    uint256_t old_val = UINT256_ZERO;
    size_t vlen;
    const void *existing = mem_art_get(&s->transient, skey, 52, &vlen);
    if (existing && vlen == sizeof(uint256_t))
        old_val = *(const uint256_t *)existing;

    journal_entry_t je = { .type = JE_TSTORE, .addr = *addr,
        .data.tstore = { .key = *key, .val = old_val } };
    journal_push(s, &je);

    mem_art_insert(&s->transient, skey, 52, value, sizeof(uint256_t));
}

/* =========================================================================
 * Snapshot / Revert
 * ========================================================================= */

/* RIPEMD-160 precompile address (0x03) — special EIP-161 quirk:
 * on revert, this address stays "touched" so it can be pruned. */
static const uint8_t RIPEMD_ADDR[20] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3
};

uint32_t state_snapshot(state_t *s) {
    return s ? s->journal_len : 0;
}

void state_revert(state_t *s, uint32_t snap) {
    if (!s || snap > s->journal_len) return;

    for (uint32_t i = s->journal_len; i > snap; i--) {
        journal_entry_t *je = &s->journal[i - 1];
        account_t *a = find_account(s, &je->addr);

        switch (je->type) {
        case JE_NONCE:
            if (a) {
                a->nonce = je->data.nonce.val;
                /* RIPEMD quirk: keep account dirty so EIP-161 can prune after OOG */
                if (!(s->prune_empty && memcmp(je->addr.bytes, RIPEMD_ADDR, 20) == 0))
                    a->status = je->data.nonce.status;
            }
            break;
        case JE_BALANCE:
            if (a) {
                a->balance = je->data.balance.val;
                if (!(s->prune_empty && memcmp(je->addr.bytes, RIPEMD_ADDR, 20) == 0))
                    a->status = je->data.balance.status;
            }
            break;
        case JE_CODE:
            if (a) {
                resource_t *r = get_resource(s, a);
                if (r) {
                    free(r->code);
                    r->code = je->data.code.code;
                    r->code_size = je->data.code.code_size;
                    r->code_hash = je->data.code.code_hash;
                }
                a->status = je->data.code.status;
            }
            je->data.code.code = NULL;  /* ownership transferred */
            break;
        case JE_STORAGE: {
            if (a) {
                resource_t *r = get_resource(s, a);
                hash_t sh = slot_hash(&je->data.storage.key);
                if (r && s->stor_pool) {
                    if (uint256_is_zero(&je->data.storage.val))
                        storage_hart_del(s->stor_pool, &r->storage, sh.bytes);
                    else {
                        uint8_t val_be[32];
                        uint256_to_bytes(&je->data.storage.val, val_be);
                        storage_hart_put(s->stor_pool, &r->storage, sh.bytes, val_be);
                    }
                }
            }
            break;
        }
        case JE_CREATE:
            if (a) {
                resource_t *r = get_resource(s, a);
                a->nonce = je->data.create.nonce;
                a->balance = je->data.create.balance;
                a->status = je->data.create.status;
                if (r) {
                    r->code_hash = je->data.create.code_hash;
                    r->storage_root = je->data.create.storage_root;
                    free(r->code);
                    r->code = je->data.create.code;
                    r->code_size = je->data.create.code_size;
                }
            }
            je->data.create.code = NULL;  /* ownership transferred */
            break;
        case JE_SELF_DESTRUCT:
            if (a) a->status = je->data.selfdestruct.status;
            break;
        case JE_TOUCH:
            if (a) a->status = je->data.touch.status;
            break;
        case JE_TSTORE: {
            uint8_t skey[52];
            make_slot_key(&je->addr, &je->data.tstore.key, skey);
            if (uint256_is_zero(&je->data.tstore.val))
                mem_art_delete(&s->transient, skey, 52);
            else
                mem_art_insert(&s->transient, skey, 52,
                               &je->data.tstore.val, sizeof(uint256_t));
            break;
        }
        case JE_WARM_ADDR:
            mem_art_delete(&s->warm_addrs, je->addr.bytes, 20);
            break;
        case JE_WARM_SLOT: {
            uint8_t skey[52];
            make_slot_key(&je->addr, &je->data.warm_slot_key, skey);
            mem_art_delete(&s->warm_slots, skey, 52);
            break;
        }
        }
    }
    s->journal_len = snap;
}

/* =========================================================================
 * Commit TX
 * ========================================================================= */

void state_commit_tx(state_t *s) {
    if (!s) return;

    for (uint32_t i = 0; i < s->tx_dirty_count; i++) {
        uint32_t idx = s->tx_dirty[i];
        if (idx >= s->acct_count) continue;
        account_t *a = &s->accounts[idx];

        /* Self-destructed: zero everything */
        if (status_has(a, STATUS_SELF_DESTRUCTED)) {
            a->balance = UINT256_ZERO;
            a->nonce = 0;
            a->status = 0;  /* clear all: EXISTED, CREATED, HAS_CODE, SELF_DESTRUCTED */

            resource_t *r = get_resource(s, a);
            if (r) {
                r->code_hash = EMPTY_CODE_HASH;
                if (r->code) { free(r->code); r->code = NULL; }
                r->code_size = 0;
                r->storage_root = EMPTY_STORAGE_ROOT;
                if (r->jumpdest_bitmap) { free(r->jumpdest_bitmap); r->jumpdest_bitmap = NULL; }
                if (s->stor_pool && !storage_hart_empty(&r->storage))
                    storage_hart_clear(s->stor_pool, &r->storage);
                bm_set(&s->stor_dirty, a->resource_idx);
            }

            /* Mark dirty so finalize_block sees this account */
            hash_t h = addr_hash(s, a->addr.bytes);
            mark_dirty(s, idx, &h);
            s->dead_total++;
            continue;
        }

        /* Promote/prune existence */
        bool empty = acct_is_empty(a);
        bool touched = status_has(a, STATUS_EXISTED | STATUS_CREATED) ||
                       bm_test(&s->acct_dirty, idx);

        if (touched && (!empty || !s->prune_empty))
            status_set(a, STATUS_EXISTED);

        /* EIP-161: prune touched empty accounts */
        if (s->prune_empty && empty && bm_test(&s->acct_dirty, idx)) {
            status_clear(a, STATUS_EXISTED);
            hash_t h = addr_hash(s, a->addr.bytes);
            mark_dirty(s, idx, &h);
            s->dead_total++;
        }

        /* Clear per-tx status flags */
        status_clear(a, STATUS_CREATED | STATUS_SELF_DESTRUCTED);
    }

    s->tx_dirty_count = 0;
    s->journal_len = 0;

    mem_art_destroy(&s->warm_addrs);  mem_art_init(&s->warm_addrs);
    mem_art_destroy(&s->warm_slots);  mem_art_init(&s->warm_slots);
    mem_art_destroy(&s->transient);   mem_art_init(&s->transient);
    mem_art_destroy(&s->originals);   mem_art_init(&s->originals);
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

void state_commit_block(state_t *s) {
    (void)s; /* dead code — finalize_block handles this */
}

void state_clear_prestate_dirty(state_t *s) {
    if (!s) return;

    for (uint32_t i = 0; i < s->acct_count; i++) {
        account_t *a = &s->accounts[i];

        /* Compute storage roots for prestate accounts that have storage */
        resource_t *r = get_resource(s, a);
        if (r && s->stor_pool && !storage_hart_empty(&r->storage))
            storage_hart_root_hash(s->stor_pool, &r->storage,
                                   stor_value_encode, NULL, r->storage_root.bytes);

        /* Mark prestate accounts as existing */
        status_set(a, STATUS_EXISTED);
    }

    /* Clear dirty bitmaps — prestate is the baseline */
    bm_clear_all(&s->acct_dirty);
    bm_clear_all(&s->stor_dirty);
}

/* =========================================================================
 * Finalize Block / Compute Root / Reset Block
 * ========================================================================= */

/**
 * Per-block state finalization. Called after all txs in a block.
 * Promotes/demotes existence for dirty accounts.
 * Does NOT hart_delete or compute storage roots — deferred to compute_root.
 */
void state_finalize_block(state_t *s, bool prune_empty) {
    if (!s) return;

    /* Iterate dirty accounts via bitmap */
    uint32_t bm_bytes = (s->acct_dirty.cap + 7) / 8;
    for (uint32_t b = 0; b < bm_bytes; b++) {
        if (!s->acct_dirty.bits[b]) continue;
        for (uint32_t bit = 0; bit < 8; bit++) {
            if (!(s->acct_dirty.bits[b] & (1u << bit))) continue;
            uint32_t idx = b * 8 + bit;
            if (idx >= s->acct_count) continue;

            account_t *a = &s->accounts[idx];

            /* Promote/demote existence.
             * Self-destructed accounts were already zeroed by commit_tx
             * (status cleared to 0). Non-empty accounts get EXISTED. */
            if (!status_has(a, STATUS_EXISTED)) {
                if (!acct_is_empty(a))
                    status_set(a, STATUS_EXISTED);
            }

            /* EIP-161: demote empty accounts that still have EXISTED.
             * This catches accounts that were EXISTED, got drained to empty
             * this block, and should be pruned from the trie. */
            if (prune_empty && acct_is_empty(a) && status_has(a, STATUS_EXISTED)) {
                status_clear(a, STATUS_EXISTED);
                s->dead_total++;
            }
        }
    }
}

/**
 * Compute Merkle root. Requires finalize_block was called.
 *
 * 1. Process dirty accounts: compute storage roots, hart_delete dead
 * 2. Compute hart_root_hash
 */
hash_t state_compute_root(state_t *s, bool prune_empty) {
    (void)prune_empty;
    hash_t root = {0};
    if (!s) return root;

    /* Process dirty accounts: storage roots + delete dead from acct_index */
    uint32_t bm_bytes = (s->acct_dirty.cap + 7) / 8;
    for (uint32_t b = 0; b < bm_bytes; b++) {
        if (!s->acct_dirty.bits[b]) continue;
        for (uint32_t bit = 0; bit < 8; bit++) {
            if (!(s->acct_dirty.bits[b] & (1u << bit))) continue;
            uint32_t idx = b * 8 + bit;
            if (idx >= s->acct_count) continue;

            account_t *a = &s->accounts[idx];

            /* Delete dead accounts from acct_index */
            if (!status_has(a, STATUS_EXISTED)) {
                hash_t h = addr_hash(s, a->addr.bytes);
                const uint32_t *p = (const uint32_t *)
                    hart_get(&s->acct_index, h.bytes);
                if (p && *p == idx)
                    hart_delete(&s->acct_index, h.bytes);
                continue;  /* skip storage root for dead accounts */
            }

            /* Compute storage root for live dirty accounts */
            if (bm_test(&s->stor_dirty, a->resource_idx) && a->resource_idx) {
                resource_t *r = &s->resources[a->resource_idx];
                if (!storage_hart_empty(&r->storage))
                    storage_hart_root_hash(s->stor_pool, &r->storage,
                                           stor_value_encode, NULL,
                                           r->storage_root.bytes);
                else
                    r->storage_root = EMPTY_STORAGE_ROOT;
            }
        }
    }

    /* Compute Merkle root */
    hart_root_hash(&s->acct_index, acct_trie_encode, s, root.bytes);

    /* Clear dirty bitmaps */
    bm_clear_all(&s->acct_dirty);
    bm_clear_all(&s->stor_dirty);

    return root;
}

/* Backward-compat wrapper */
hash_t state_compute_root_ex(state_t *s, bool prune_empty, bool compute_hash) {
    (void)compute_hash;
    return state_compute_root(s, prune_empty);
}

/**
 * Reset per-block caches. Called after compute_root (or after finalize_block
 * if no root computation this block).
 */
void state_reset_block(state_t *s) {
    if (!s) return;
    mem_art_destroy(&s->addr_hash_cache); mem_art_init(&s->addr_hash_cache);
    mem_art_destroy(&s->blk_orig_acct);   mem_art_init(&s->blk_orig_acct);
    mem_art_destroy(&s->blk_orig_stor);   mem_art_init(&s->blk_orig_stor);
}

void state_invalidate_all(state_t *s) {
    if (!s) return;
    hart_invalidate_all(&s->acct_index);
    for (uint32_t i = 1; i < s->res_count; i++) {
        resource_t *r = &s->resources[i];
        if (!storage_hart_empty(&r->storage))
            storage_hart_invalidate(s->stor_pool, &r->storage);
        bm_set(&s->stor_dirty, i);
    }
    /* Mark all accounts dirty so compute_root reprocesses everything */
    bm_set_all(&s->acct_dirty, s->acct_count);
}

uint32_t state_dead_count(const state_t *s) {
    return s ? s->dead_total : 0;
}

/* =========================================================================
 * TODO: save/load, compact, stats, collect_diff, access tracking, raw setters
 * ========================================================================= */
