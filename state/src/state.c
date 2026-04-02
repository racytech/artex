/**
 * State — single in-memory Ethereum world state.
 *
 * Flat vector of accounts indexed by mem_art. No disk on hot path.
 * Journal for snapshot/revert. Per-account mem_art for storage.
 * Account trie (compact_art + art_mpt) for MPT root computation.
 */

#include "state.h"
#include "code_store.h"
#include "compact_art.h"
#include "art_mpt.h"
#include "keccak256.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define ACCT_INIT_CAP       4096
#define JOURNAL_INIT_CAP    256
#define STOR_KEY_SIZE       32
#define STOR_VAL_SIZE       32
#define STOR_INIT_CAP       1024
#define SLOT_KEY_SIZE       52   /* addr[20] + slot_be[32] for originals/warm */

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

/* Storage value → RLP encode callback for per-account art_mpt */
static uint32_t stor_value_encode(const uint8_t *key, const void *leaf_val,
                                   uint32_t val_size, uint8_t *rlp_out,
                                   void *ctx) {
    (void)key; (void)ctx;
    /* Storage value: RLP-encode the 32-byte value (strip leading zeros) */
    const uint8_t *v = (const uint8_t *)leaf_val;
    return (uint32_t)rlp_be(v, val_size, rlp_out);
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
    /* Account vector + index (also serves as account trie for MPT) */
    account_t *accounts;
    uint32_t   count;       /* number of accounts in use */
    uint32_t   capacity;    /* allocated slots */
    mem_art_t  acct_index;  /* addr_hash[32] → uint32_t idx (lookup + trie) */

    /* Resource vector (only accounts with code/storage) */
    resource_t *resources;
    uint32_t    res_count;
    uint32_t    res_capacity;

    /* Account trie MPT (wraps acct_index via art_iface_mem) */
    art_iface_mem_ctx_t acct_trie_ctx;
    art_mpt_t          *acct_trie_mpt;

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

    /* Resource tracking for eviction */
    uint32_t *resource_list;
    uint32_t  resource_count;
    uint32_t  resource_cap;

    /* Block state */
    uint64_t current_block;
    bool     prune_empty;
};

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

static account_t *find_account(const state_t *s, const uint8_t addr[20]) {
    hash_t h = hash_keccak256(addr, 20);
    const uint32_t *pidx = (const uint32_t *)
        mem_art_get(&((state_t *)s)->acct_index, h.bytes, 32, NULL);
    if (!pidx) return NULL;
    uint32_t idx = *pidx;
    if (idx >= s->count) return NULL;
    return &((state_t *)s)->accounts[idx];
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

/* Get or create account. Inserts into acct_index (trie) on creation. */
static account_t *ensure_account(state_t *s, const address_t *addr) {
    account_t *existing = find_account(s, addr->bytes);
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

    /* Single index: addr_hash[32] → idx (serves both lookup and trie) */
    hash_t addr_hash = hash_keccak256(addr->bytes, 20);
    mem_art_insert(&s->acct_index, addr_hash.bytes, 32, &idx, sizeof(idx));

    (void)0;

    return a;
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

static void mark_blk_dirty(state_t *s, account_t *a) {
    if (!acct_has_flag(a, ACCT_MPT_DIRTY)) {
        acct_set_flag(a, ACCT_MPT_DIRTY);
        dirty_push(&s->blk_dirty, a->addr.bytes, 20);

        /* Mark acct_index trie path dirty so art_mpt rehashes. */
        hash_t addr_hash = hash_keccak256(a->addr.bytes, 20);
        mem_art_mark_path_dirty(&s->acct_index, addr_hash.bytes, 32);
    }
}

static void resource_list_add(state_t *s, uint32_t idx) {
    if (s->resource_count >= s->resource_cap) {
        uint32_t nc = s->resource_cap ? s->resource_cap * 2 : 1024;
        uint32_t *nl = realloc(s->resource_list, nc * sizeof(uint32_t));
        if (!nl) return;
        s->resource_list = nl;
        s->resource_cap = nc;
    }
    s->resource_list[s->resource_count++] = idx;
}

/* =========================================================================
 * Storage helpers
 * ========================================================================= */

static bool ensure_storage(state_t *s, account_t *a) {
    resource_t *r = ensure_resource(s, a);
    if (!r) return false;
    if (r->storage) return true;

    r->storage = calloc(1, sizeof(mem_art_t));
    if (!r->storage) return false;
    if (!mem_art_init_cap(r->storage, STOR_INIT_CAP)) {
        free(r->storage); r->storage = NULL; return false;
    }

    r->storage_ctx = calloc(1, sizeof(art_iface_mem_ctx_t));
    if (!r->storage_ctx) {
        mem_art_destroy(r->storage); free(r->storage); r->storage = NULL;
        return false;
    }
    r->storage_ctx->tree = r->storage;
    r->storage_ctx->key_size = STOR_KEY_SIZE;
    r->storage_ctx->value_size = STOR_VAL_SIZE;

    r->storage_mpt = art_mpt_create_iface(
        art_iface_mem(r->storage_ctx), stor_value_encode, NULL);
    if (!r->storage_mpt) {
        free(r->storage_ctx); r->storage_ctx = NULL;
        mem_art_destroy(r->storage); free(r->storage); r->storage = NULL;
        return false;
    }

    uint32_t idx = (uint32_t)(a - s->accounts);
    resource_list_add(s, idx);
    return true;
}

static void destroy_resource_storage(resource_t *r) {
    if (!r) return;
    if (r->storage_mpt) { art_mpt_destroy(r->storage_mpt); r->storage_mpt = NULL; }
    if (r->storage_ctx) { free(r->storage_ctx); r->storage_ctx = NULL; }
    if (r->storage) { mem_art_destroy(r->storage); free(r->storage); r->storage = NULL; }
}

static uint256_t storage_read(const state_t *s, const account_t *a,
                               const uint8_t slot_hash[32]) {
    resource_t *r = get_resource(s, a);
    if (!r || !r->storage) return UINT256_ZERO;
    size_t vlen;
    const void *val = mem_art_get(r->storage, slot_hash, STOR_KEY_SIZE, &vlen);
    if (val && vlen == STOR_VAL_SIZE)
        return uint256_from_bytes((const uint8_t *)val, 32);
    return UINT256_ZERO;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/* Account trie encode callback — reads from accounts[] + resources[] */
static uint32_t acct_trie_encode(const uint8_t *key, const void *leaf_val,
                                  uint32_t val_size, uint8_t *rlp_out,
                                  void *user_ctx) {
    (void)key; (void)val_size;
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

    (void)key;

    return len;
}

state_t *state_create(code_store_t *cs) {
    state_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;

    s->code_store = cs;
    s->accounts = calloc(ACCT_INIT_CAP, sizeof(account_t));
    if (!s->accounts) { free(s); return NULL; }
    s->capacity = ACCT_INIT_CAP;

    mem_art_init(&s->acct_index);
    mem_art_init(&s->warm_addrs);
    mem_art_init(&s->warm_slots);
    mem_art_init(&s->transient);
    mem_art_init(&s->originals);

    s->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!s->journal) { free(s->accounts); free(s); return NULL; }
    s->journal_cap = JOURNAL_INIT_CAP;

    /* Reserve resource index 0 as "none" (resource_idx=0 means no resource) */
    s->resources = calloc(1024, sizeof(resource_t));
    if (!s->resources) { free(s->journal); free(s->accounts); free(s); return NULL; }
    s->res_capacity = 1024;
    s->res_count = 1;

    /* Account trie MPT — wraps acct_index mem_art */
    s->acct_trie_ctx.tree = &s->acct_index;
    s->acct_trie_ctx.key_size = 32;   /* addr_hash */
    s->acct_trie_ctx.value_size = sizeof(uint32_t);
    s->acct_trie_mpt = art_mpt_create_iface(
        art_iface_mem(&s->acct_trie_ctx), acct_trie_encode, s);

    return s;
}

void state_destroy(state_t *s) {
    if (!s) return;

    for (uint32_t i = 0; i < s->res_count; i++) {
        resource_t *r = &s->resources[i];
        free(r->code);
        destroy_resource_storage(r);
    }
    free(s->resources);
    free(s->accounts);
    free(s->resource_list);

    if (s->acct_trie_mpt) art_mpt_destroy(s->acct_trie_mpt);
    mem_art_destroy(&s->acct_index);
    mem_art_destroy(&s->warm_addrs);
    mem_art_destroy(&s->warm_slots);
    mem_art_destroy(&s->transient);
    mem_art_destroy(&s->originals);

    for (uint32_t i = 0; i < s->journal_len; i++) {
        if (s->journal[i].type == JE_CODE)
            free(s->journal[i].data.code.code);
        else if (s->journal[i].type == JE_CREATE)
            free(s->journal[i].data.create.code);
    }
    free(s->journal);

    dirty_free(&s->tx_dirty);
    dirty_free(&s->blk_dirty);
    free(s);
}

/* =========================================================================
 * Block lifecycle
 * ========================================================================= */

void state_begin_block(state_t *s, uint64_t block_number) {
    if (s) s->current_block = block_number;
}

void state_set_prune_empty(state_t *s, bool enabled) {
    if (s) s->prune_empty = enabled;
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
           acct_has_flag(a, ACCT_DIRTY) ||
           acct_has_flag(a, ACCT_BLOCK_DIRTY);
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
    account_t *a = ensure_account(s, addr);
    if (!a) return;

    journal_entry_t je = { .type = JE_NONCE, .addr = *addr,
        .data.nonce = { .val = a->nonce, .flags = a->flags } };
    journal_push(s, &je);

    a->nonce = nonce;
    acct_set_flag(a, ACCT_DIRTY | ACCT_BLOCK_DIRTY);
    mark_tx_dirty(s, addr);
    mark_blk_dirty(s, a);
}

void state_set_balance(state_t *s, const address_t *addr, const uint256_t *bal) {
    if (!s || !addr || !bal) return;
    account_t *a = ensure_account(s, addr);
    if (!a) return;

    journal_entry_t je = { .type = JE_BALANCE, .addr = *addr,
        .data.balance = { .val = a->balance, .flags = a->flags } };
    journal_push(s, &je);

    a->balance = *bal;
    acct_set_flag(a, ACCT_DIRTY | ACCT_BLOCK_DIRTY);
    mark_tx_dirty(s, addr);
    mark_blk_dirty(s, a);
}

void state_add_balance(state_t *s, const address_t *addr, const uint256_t *amount) {
    if (!s || !addr || !amount) return;
    uint256_t bal = state_get_balance(s, addr);
    uint256_t new_bal = uint256_add(&bal, amount);
    state_set_balance(s, addr, &new_bal);
}

bool state_sub_balance(state_t *s, const address_t *addr, const uint256_t *amount) {
    if (!s || !addr || !amount) return false;
    uint256_t bal = state_get_balance(s, addr);
    if (uint256_lt(&bal, amount)) return false;
    uint256_t new_bal = uint256_sub(&bal, amount);
    state_set_balance(s, addr, &new_bal);
    return true;
}

/* =========================================================================
 * Code
 * ========================================================================= */

void state_set_code(state_t *s, const address_t *addr,
                    const uint8_t *code, uint32_t len) {
    if (!s || !addr) return;
    account_t *a = ensure_account(s, addr);
    if (!a) return;

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
    mark_blk_dirty(s, a);
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
    account_t *a = find_account(s, addr->bytes);
    if (!a) return UINT256_ZERO;
    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = hash_keccak256(slot_be, 32);
    return storage_read(s, a, slot_hash.bytes);
}

void state_set_storage(state_t *s, const address_t *addr,
                       const uint256_t *key, const uint256_t *value) {
    if (!s || !addr || !key || !value) return;
    account_t *a = ensure_account(s, addr);
    if (!a) return;

    uint8_t slot_be[32]; uint256_to_bytes(key, slot_be);
    hash_t slot_hash = hash_keccak256(slot_be, 32);

    uint256_t old_value = storage_read(s, a, slot_hash.bytes);

    journal_entry_t je = { .type = JE_STORAGE, .addr = *addr,
        .data.storage = { .key = *key, .val = old_value, .flags = a->flags } };
    journal_push(s, &je);

    /* Save original for EIP-2200 */
    uint8_t skey[SLOT_KEY_SIZE];
    make_slot_key(addr, key, skey);
    if (!mem_art_contains(&s->originals, skey, SLOT_KEY_SIZE))
        mem_art_insert(&s->originals, skey, SLOT_KEY_SIZE,
                       &old_value, sizeof(uint256_t));

    /* Write to storage */
    if (!ensure_storage(s, a)) {
        LOG_ERROR("storage create failed for %02x%02x..%02x%02x",
                  addr->bytes[0], addr->bytes[1], addr->bytes[18], addr->bytes[19]);
        return;
    }
    resource_t *r = get_resource(s, a);

    uint8_t val_be[32];
    uint256_to_bytes(value, val_be);
    if (uint256_is_zero(value))
        mem_art_delete(r->storage, slot_hash.bytes, STOR_KEY_SIZE);
    else
        mem_art_insert(r->storage, slot_hash.bytes, STOR_KEY_SIZE,
                       val_be, STOR_VAL_SIZE);

    acct_set_flag(a, ACCT_STORAGE_DIRTY);
    mark_tx_dirty(s, addr);
    mark_blk_dirty(s, a);
}

bool state_has_storage(state_t *s, const address_t *addr) {
    if (!s || !addr) return false;
    account_t *a = find_account(s, addr->bytes);
    if (!a) return false;
    resource_t *r = get_resource(s, a);
    if (r && memcmp(r->storage_root.bytes, EMPTY_STORAGE_ROOT.bytes, 32) != 0)
        return true;
    if (r && r->storage && mem_art_size(r->storage) > 0)
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
            if (a) { a->nonce = je->data.nonce.val; a->flags = je->data.nonce.flags; }
            break;
        case JE_BALANCE:
            if (a) { a->balance = je->data.balance.val; a->flags = je->data.balance.flags; }
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
            resource_t *r = a ? get_resource(s, a) : NULL;
            if (r && r->storage) {
                uint8_t slot_be[32]; uint256_to_bytes(&je->data.storage.key, slot_be);
                hash_t slot_hash = hash_keccak256(slot_be, 32);
                if (uint256_is_zero(&je->data.storage.val))
                    mem_art_delete(r->storage, slot_hash.bytes, STOR_KEY_SIZE);
                else {
                    uint8_t val_be[32];
                    uint256_to_bytes(&je->data.storage.val, val_be);
                    mem_art_insert(r->storage, slot_hash.bytes, STOR_KEY_SIZE,
                                   val_be, STOR_VAL_SIZE);
                }
            }
            if (a) a->flags = je->data.storage.flags;
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
    account_t *a = ensure_account(s, addr);
    if (!a) return;

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
        destroy_resource_storage(r);
    }

    mark_tx_dirty(s, addr);
    mark_blk_dirty(s, a);
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
                destroy_resource_storage(r);
            }
            mark_blk_dirty(s, a);
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
        if (r && r->storage_mpt) {
            art_mpt_root_hash(r->storage_mpt, r->storage_root.bytes);
        }

        /* Mark prestate accounts as existing — they're in the state trie.
         * Required for correct EIP-161 pruning decisions in commit_tx. */
        acct_set_flag(a, ACCT_EXISTED);

        acct_clear_flag(a, ACCT_DIRTY | ACCT_CODE_DIRTY | ACCT_MPT_DIRTY |
                        ACCT_BLOCK_DIRTY | ACCT_STORAGE_DIRTY | ACCT_STORAGE_CLEARED);
    }

    dirty_clear(&s->blk_dirty);
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
    st.storage_account_count = s->resource_count;
    /* Rough memory estimate */
    st.memory_used = (size_t)s->count * sizeof(account_t);
    return st;
}

/* =========================================================================
 * compute_root — TODO: implement with account trie
 * save/load — TODO: implement serialization
 * ========================================================================= */

hash_t state_compute_root(state_t *s, bool prune_empty) {
    hash_t root = {0};
    if (!s || !s->acct_trie_mpt) return root;

    /* Step 1: For each dirty account, decide existence + compute storage root */
    for (size_t d = 0; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        account_t *a = find_account(s, akey);
        if (!a || !acct_has_flag(a, ACCT_MPT_DIRTY)) continue;

        /* Promote non-empty accounts to existed */
        if (acct_has_flag(a, ACCT_BLOCK_DIRTY)) {
            if (acct_has_flag(a, ACCT_SELF_DESTRUCTED)) {
                acct_clear_flag(a, ACCT_EXISTED);
            } else if (!acct_is_empty(a)) {
                acct_set_flag(a, ACCT_EXISTED);
            }
        }

        /* Compute storage root if dirty */
        if (acct_has_flag(a, ACCT_STORAGE_DIRTY)) {
            resource_t *r = get_resource(s, a);
            if (r && r->storage_mpt) {
                art_mpt_root_hash(r->storage_mpt, r->storage_root.bytes);
            }
        }

    }

    /* Step 2: Update account trie — insert/delete for dirty accounts */
    for (size_t d = 0; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        account_t *a = find_account(s, akey);
        if (!a || !acct_has_flag(a, ACCT_MPT_DIRTY)) continue;

        hash_t addr_hash = hash_keccak256(a->addr.bytes, 20);

        if (!acct_has_flag(a, ACCT_EXISTED) ||
            (acct_is_empty(a) && prune_empty)) {
            mem_art_delete(&s->acct_index, addr_hash.bytes, 32);
        }
    }

    /* Step 2b: Remove phantom accounts from acct_index.
     * ensure_account inserts every written-to address into acct_index,
     * including accounts touched by add_balance(0) that never got EXISTED.
     * These must not appear in the trie.
     *
     * IMPORTANT: only delete if this account's index actually matches the
     * acct_index entry. Duplicate addresses (from pruned + re-created
     * accounts) can share the same hash key — don't delete the live entry. */
    for (uint32_t i = 0; i < s->count; i++) {
        account_t *a = &s->accounts[i];
        if (acct_has_flag(a, ACCT_EXISTED) && !(acct_is_empty(a) && prune_empty))
            continue; /* keep */
        hash_t h = hash_keccak256(a->addr.bytes, 20);
        /* Check that acct_index points to THIS entry, not a newer duplicate */
        const uint32_t *pidx = (const uint32_t *)
            mem_art_get(&s->acct_index, h.bytes, 32, NULL);
        if (pidx && *pidx == i)
            mem_art_delete(&s->acct_index, h.bytes, 32);
    }

    /* Step 3: Compute account trie root */
    art_mpt_root_hash(s->acct_trie_mpt, root.bytes);

    /* Step 4: Clear dirty flags */
    for (size_t d = 0; d < s->blk_dirty.count; d++) {
        const uint8_t *akey = s->blk_dirty.keys + d * 20;
        account_t *a = find_account(s, akey);
        if (!a) continue;
        acct_clear_flag(a, ACCT_MPT_DIRTY | ACCT_BLOCK_DIRTY |
                        ACCT_STORAGE_DIRTY | ACCT_STORAGE_CLEARED);
    }
    dirty_clear(&s->blk_dirty);

    return root;
}

bool state_load(state_t *s, const char *path) {
    (void)s; (void)path;
    /* TODO: implement */
    return false;
}

bool state_save(const state_t *s, const char *path) {
    (void)s; (void)path;
    /* TODO: implement */
    return false;
}
