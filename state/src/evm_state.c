/**
 * EVM State adapter — wraps state_t (v2) to implement the evm_state_t API.
 *
 * Drop-in replacement for evm/src/evm_state.c. Same function signatures,
 * backed by state_t instead of state_overlay_t.
 */

#include "evm_state.h"
#include "state.h"
#include "code_store.h"

#include <stdlib.h>
#include <string.h>

struct evm_state {
    state_t      *st;
    code_store_t *cs;     /* not owned */
    bool          batch_mode;
    bool          discard_on_destroy;
};

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

evm_state_t *evm_state_create(code_store_t *cs) {
    evm_state_t *es = calloc(1, sizeof(*es));
    if (!es) return NULL;
    es->cs = cs;
    es->st = state_create(cs);
    if (!es->st) { free(es); return NULL; }
    return es;
}

void evm_state_destroy(evm_state_t *es) {
    if (!es) return;
    state_destroy(es->st);
    free(es);
}

void evm_state_discard_pending(evm_state_t *es) {
    (void)es; /* no-op in v2 — no pending state */
}

bool evm_state_finalize(evm_state_t *es) {
    (void)es;
    return true; /* no-op — compute_root handles everything */
}

void evm_state_flush(evm_state_t *es) {
    (void)es; /* no disk to flush */
}

void evm_state_set_batch_mode(evm_state_t *es, bool enabled) {
    if (es) es->batch_mode = enabled;
}

void evm_state_set_prune_empty(evm_state_t *es, bool enabled) {
    if (es) state_set_prune_empty(es->st, enabled);
}

void evm_state_set_flat_state(evm_state_t *es, flat_state_t *fs) {
    (void)es; (void)fs; /* no flat_state in v2 */
}

flat_state_t *evm_state_get_flat_state(const evm_state_t *es) {
    (void)es;
    return NULL; /* no flat_state in v2 */
}

void evm_state_set_storage_path(evm_state_t *es, const char *path) {
    (void)es; (void)path; /* no storage file in v2 */
}

/* =========================================================================
 * Account access
 * ========================================================================= */

bool evm_state_exists(evm_state_t *es, const address_t *addr) {
    return es ? state_exists(es->st, addr) : false;
}

bool evm_state_is_empty(evm_state_t *es, const address_t *addr) {
    return es ? state_is_empty(es->st, addr) : true;
}

uint64_t evm_state_get_nonce(evm_state_t *es, const address_t *addr) {
    return es ? state_get_nonce(es->st, addr) : 0;
}

void evm_state_set_nonce(evm_state_t *es, const address_t *addr, uint64_t nonce) {
    if (es) state_set_nonce(es->st, addr, nonce);
}

uint256_t evm_state_get_balance(evm_state_t *es, const address_t *addr) {
    return es ? state_get_balance(es->st, addr) : UINT256_ZERO;
}

void evm_state_set_balance(evm_state_t *es, const address_t *addr,
                            const uint256_t *bal) {
    if (es) state_set_balance(es->st, addr, bal);
}

void evm_state_add_balance(evm_state_t *es, const address_t *addr,
                            const uint256_t *amount) {
    if (es) state_add_balance(es->st, addr, amount);
}

bool evm_state_sub_balance(evm_state_t *es, const address_t *addr,
                            const uint256_t *amount) {
    return es ? state_sub_balance(es->st, addr, amount) : false;
}

/* =========================================================================
 * Code
 * ========================================================================= */

void evm_state_set_code(evm_state_t *es, const address_t *addr,
                         const uint8_t *code, uint32_t len) {
    if (es) state_set_code(es->st, addr, code, len);
}

bool evm_state_get_code(evm_state_t *es, const address_t *addr,
                         uint8_t *out, uint32_t *out_len) {
    if (!es) { if (out_len) *out_len = 0; return false; }
    uint32_t size;
    const uint8_t *ptr = state_get_code(es->st, addr, &size);
    if (!ptr || size == 0) { if (out_len) *out_len = 0; return false; }
    if (out && out_len) {
        uint32_t n = size < *out_len ? size : *out_len;
        memcpy(out, ptr, n);
        *out_len = size;
    } else if (out_len) {
        *out_len = size;
    }
    return true;
}

const uint8_t *evm_state_get_code_ptr(evm_state_t *es, const address_t *addr,
                                       uint32_t *out_len) {
    if (!es) { if (out_len) *out_len = 0; return NULL; }
    return state_get_code(es->st, addr, out_len);
}

uint32_t evm_state_get_code_size(evm_state_t *es, const address_t *addr) {
    return es ? state_get_code_size(es->st, addr) : 0;
}

hash_t evm_state_get_code_hash(evm_state_t *es, const address_t *addr) {
    static const hash_t EMPTY = {{
        0xc5,0xd2,0x46,0x01,0x86,0xf7,0x23,0x3c,
        0x92,0x7e,0x7d,0xb2,0xdc,0xc7,0x03,0xc0,
        0xe5,0x00,0xb6,0x53,0xca,0x82,0x27,0x3b,
        0x7b,0xfa,0xd8,0x04,0x5d,0x85,0xa4,0x70
    }};
    return es ? state_get_code_hash(es->st, addr) : EMPTY;
}

/* =========================================================================
 * Storage
 * ========================================================================= */

uint256_t evm_state_get_storage(evm_state_t *es, const address_t *addr,
                                 const uint256_t *key) {
    return es ? state_get_storage(es->st, addr, key) : UINT256_ZERO;
}

void evm_state_set_storage(evm_state_t *es, const address_t *addr,
                            const uint256_t *key, const uint256_t *value) {
    if (es) state_set_storage(es->st, addr, key, value);
}

bool evm_state_has_storage(evm_state_t *es, const address_t *addr) {
    return es ? state_has_storage(es->st, addr) : false;
}

uint256_t evm_state_sload(evm_state_t *es, const address_t *addr,
                           const uint256_t *key, bool *was_warm) {
    return es ? state_sload(es->st, addr, key, was_warm) : UINT256_ZERO;
}

void evm_state_sstore_lookup(evm_state_t *es, const address_t *addr,
                               const uint256_t *key,
                               uint256_t *current, uint256_t *original,
                               bool *was_warm) {
    if (es) state_sstore_lookup(es->st, addr, key, current, original, was_warm);
}

/* =========================================================================
 * Account lifecycle
 * ========================================================================= */

void evm_state_create_account(evm_state_t *es, const address_t *addr) {
    if (es) state_create_account(es->st, addr);
}

void evm_state_self_destruct(evm_state_t *es, const address_t *addr) {
    if (es) state_self_destruct(es->st, addr);
}

/* =========================================================================
 * Snapshot / Revert
 * ========================================================================= */

uint32_t evm_state_snapshot(evm_state_t *es) {
    return es ? state_snapshot(es->st) : 0;
}

void evm_state_revert(evm_state_t *es, uint32_t snap) {
    if (es) state_revert(es->st, snap);
}

/* =========================================================================
 * Transaction / Block commit
 * ========================================================================= */

void evm_state_commit(evm_state_t *es) {
    if (es) state_commit_block(es->st);
}

void evm_state_commit_tx(evm_state_t *es) {
    if (es) state_commit_tx(es->st);
}

void evm_state_begin_block(evm_state_t *es, uint64_t block_number) {
    if (es) state_begin_block(es->st, block_number);
}

/* =========================================================================
 * EIP-2929 warm/cold (evm_state_t level — evm_t level stays in evm.c)
 * ========================================================================= */

bool evm_state_warm_address(evm_state_t *es, const address_t *addr) {
    if (!es) return false;
    bool was = state_is_addr_warm(es->st, addr);
    state_mark_addr_warm(es->st, addr);
    return was;
}

bool evm_state_is_address_warm(const evm_state_t *es, const address_t *addr) {
    return es ? state_is_addr_warm(es->st, addr) : false;
}

bool evm_state_warm_slot(evm_state_t *es, const address_t *addr,
                          const uint256_t *key) {
    if (!es) return false;
    bool was = state_is_storage_warm(es->st, addr, key);
    state_mark_storage_warm(es->st, addr, key);
    return was;
}

/* =========================================================================
 * EIP-1153 transient storage
 * ========================================================================= */

uint256_t evm_state_tload(evm_state_t *es, const address_t *addr,
                           const uint256_t *key) {
    return es ? state_tload(es->st, addr, key) : UINT256_ZERO;
}

void evm_state_tstore(evm_state_t *es, const address_t *addr,
                       const uint256_t *key, const uint256_t *value) {
    if (es) state_tstore(es->st, addr, key, value);
}

/* =========================================================================
 * MPT root
 * ========================================================================= */

hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty) {
    return es ? state_compute_root(es->st, prune_empty) : (hash_t){0};
}

hash_t evm_state_compute_mpt_root(evm_state_t *es, bool prune_empty) {
    return evm_state_compute_state_root_ex(es, prune_empty);
}

hash_t evm_state_compute_state_root_ex2(evm_state_t *es, bool prune_empty, bool compute_hash) {
    return es ? state_compute_root_ex(es->st, prune_empty, compute_hash) : (hash_t){0};
}

void evm_state_prune_empty_accounts(evm_state_t *es) {
    (void)es; /* no mass prune — EIP-161 handled per-tx in commit_tx */
}

void evm_state_invalidate_all(evm_state_t *es) {
    if (es) state_invalidate_all(es->st);
}

void evm_state_finalize_block(evm_state_t *es, bool prune_empty) {
    if (es) state_finalize_block(es->st, prune_empty);
}

/* =========================================================================
 * Eviction
 * ========================================================================= */

void evm_state_evict_cache(evm_state_t *es) {
    (void)es; /* TODO: implement LRU eviction on state_t */
}

/* =========================================================================
 * Stats
 * ========================================================================= */

evm_state_stats_t evm_state_get_stats(const evm_state_t *es) {
    evm_state_stats_t st = {0};
    if (!es) return st;
    state_stats_t ss = state_get_stats(es->st);
    st.cache_accounts = ss.account_count;
    st.cache_slots = ss.storage_account_count;
    st.cache_arena_bytes = ss.total_tracked;
    return st;
}

/* =========================================================================
 * Missing API stubs
 * ========================================================================= */

void evm_state_flush_verkle(evm_state_t *es) { (void)es; }
void evm_state_prefetch_account(evm_state_t *es, const address_t *addr) { (void)es; (void)addr; }

void evm_state_increment_nonce(evm_state_t *es, const address_t *addr) {
    if (!es) return;
    uint64_t n = state_get_nonce(es->st, addr);
    state_set_nonce(es->st, addr, n + 1);
}

void evm_state_set_code_hash(evm_state_t *es, const address_t *addr,
                              const hash_t *code_hash) {
    (void)es; (void)addr; (void)code_hash;
    /* Not needed — code_hash is derived from code in set_code */
}

uint256_t evm_state_get_committed_storage(evm_state_t *es, const address_t *addr,
                                           const uint256_t *key) {
    if (!es) return UINT256_ZERO;
    /* Check originals first, fall back to current (which IS committed
     * if no originals entry exists) */
    uint256_t orig, cur;
    state_sstore_lookup(es->st, addr, key, &cur, &orig, NULL);
    return orig;
}

void evm_state_mark_existed(evm_state_t *es, const address_t *addr) {
    if (!es) return;
    account_t *a = state_get_account(es->st, addr);
    if (a) acct_set_flag(a, ACCT_EXISTED);
}

void evm_state_clear_prestate_dirty(evm_state_t *es) {
    if (es) state_clear_prestate_dirty(es->st);
}

bool evm_state_is_self_destructed(evm_state_t *es, const address_t *addr) {
    if (!es) return false;
    account_t *a = state_get_account(es->st, addr);
    return a ? acct_has_flag(a, ACCT_SELF_DESTRUCTED) : false;
}

bool evm_state_is_created(evm_state_t *es, const address_t *addr) {
    if (!es) return false;
    account_t *a = state_get_account(es->st, addr);
    return a ? acct_has_flag(a, ACCT_CREATED) : false;
}

/* =========================================================================
 * Not applicable in v2
 * ========================================================================= */

void evm_state_collect_block_diff(evm_state_t *es, void *diff) {
    (void)es; (void)diff;
}

#ifdef ENABLE_DEBUG
void evm_state_print_mpt_stats(evm_state_t *es) { (void)es; }
#endif

state_t *evm_state_get_state(evm_state_t *es) {
    return es ? es->st : NULL;
}

size_t evm_state_collect_addresses(evm_state_t *es, address_t *out, size_t max_count) {
    return es ? state_collect_dirty_addresses(es->st, out, max_count) : 0;
}

size_t evm_state_collect_storage_keys(evm_state_t *es, const address_t *addr,
                                       uint256_t *out, size_t max_count) {
    return es ? state_collect_accessed_storage_keys(es->st, addr, out, max_count) : 0;
}

void evm_state_enable_access_tracking(evm_state_t *es) {
    if (es) state_enable_access_tracking(es->st);
}

void evm_state_disable_access_tracking(evm_state_t *es) {
    if (es) state_disable_access_tracking(es->st);
}

void evm_state_dump_debug(evm_state_t *es, const char *dir) {
    (void)es; (void)dir;
    /* TODO: implement for state_v2 */
}
