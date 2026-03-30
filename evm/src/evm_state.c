/**
 * evm_state — thin wrapper delegating to state_overlay.
 *
 * This file replaces the original evm_state.c (~2600 lines).
 * All state management logic lives in state_overlay.c.
 * evm_state.h remains the public API (unchanged).
 */

#include "evm_state.h"
#include "state_overlay.h"
#include "flat_state.h"
#include "flat_store.h"
#include "compact_art.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =========================================================================
 * Internal struct — just holds a state_overlay
 * ========================================================================= */

struct evm_state {
    state_overlay_t *so;
    flat_state_t    *flat_state;   /* not owned — kept for get_flat_state */
    bool             batch_mode;
    bool             discard_on_destroy;
};

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

evm_state_t *evm_state_create(code_store_t *cs) {
    evm_state_t *es = calloc(1, sizeof(*es));
    if (!es) return NULL;

    /* state_overlay created without flat_state initially.
     * flat_state is set later via evm_state_set_flat_state. */
    es->so = state_overlay_create(NULL, cs);
    if (!es->so) { free(es); return NULL; }

    return es;
}

void evm_state_destroy(evm_state_t *es) {
    if (!es) return;
    state_overlay_destroy(es->so);
    free(es);
}

void evm_state_discard_pending(evm_state_t *es) {
    if (es) es->discard_on_destroy = true;
}

void evm_state_flush(evm_state_t *es) {
    (void)es; /* no-op — flat_store handles persistence */
}

void evm_state_set_batch_mode(evm_state_t *es, bool enabled) {
    if (es) es->batch_mode = enabled;
}

void evm_state_set_prune_empty(evm_state_t *es, bool enabled) {
    if (es && es->so) state_overlay_set_prune_empty(es->so, enabled);
}

void evm_state_set_flat_state(evm_state_t *es, flat_state_t *fs) {
    if (!es) return;
    es->flat_state = fs;
    if (es->so) state_overlay_set_flat_state(es->so, fs);
}

flat_state_t *evm_state_get_flat_state(const evm_state_t *es) {
    return es ? es->flat_state : NULL;
}

void evm_state_flush_verkle(evm_state_t *es) { (void)es; }

void evm_state_evict_cache(evm_state_t *es) {
    if (es && es->so) state_overlay_evict(es->so);
}

/* =========================================================================
 * Account existence
 * ========================================================================= */

void evm_state_prefetch_account(evm_state_t *es, const address_t *addr) {
    (void)es; (void)addr; /* no-op hint */
}

bool evm_state_exists(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_exists(es->so, addr) : false;
}

bool evm_state_is_empty(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_is_empty(es->so, addr) : true;
}

/* =========================================================================
 * Nonce
 * ========================================================================= */

uint64_t evm_state_get_nonce(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_get_nonce(es->so, addr) : 0;
}

void evm_state_set_nonce(evm_state_t *es, const address_t *addr, uint64_t nonce) {
    if (es) state_overlay_set_nonce(es->so, addr, nonce);
}

void evm_state_increment_nonce(evm_state_t *es, const address_t *addr) {
    if (!es) return;
    uint64_t n = state_overlay_get_nonce(es->so, addr);
    state_overlay_set_nonce(es->so, addr, n + 1);
}

/* =========================================================================
 * Balance
 * ========================================================================= */

uint256_t evm_state_get_balance(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_get_balance(es->so, addr) : UINT256_ZERO_INIT;
}

void evm_state_set_balance(evm_state_t *es, const address_t *addr,
                            const uint256_t *balance) {
    if (es) state_overlay_set_balance(es->so, addr, balance);
}

void evm_state_add_balance(evm_state_t *es, const address_t *addr,
                            const uint256_t *amount) {
    if (es) state_overlay_add_balance(es->so, addr, amount);
}

bool evm_state_sub_balance(evm_state_t *es, const address_t *addr,
                            const uint256_t *amount) {
    return es ? state_overlay_sub_balance(es->so, addr, amount) : false;
}

/* =========================================================================
 * Code
 * ========================================================================= */

hash_t evm_state_get_code_hash(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_get_code_hash(es->so, addr) : HASH_EMPTY_CODE;
}

void evm_state_set_code_hash(evm_state_t *es, const address_t *addr,
                              const hash_t *code_hash) {
    if (es) state_overlay_set_code_hash(es->so, addr, code_hash);
}

uint32_t evm_state_get_code_size(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_get_code_size(es->so, addr) : 0;
}

bool evm_state_get_code(evm_state_t *es, const address_t *addr,
                         uint8_t *out, uint32_t *out_len) {
    return es ? state_overlay_get_code(es->so, addr, out, out_len) : false;
}

const uint8_t *evm_state_get_code_ptr(evm_state_t *es, const address_t *addr,
                                       uint32_t *out_len) {
    return es ? state_overlay_get_code_ptr(es->so, addr, out_len) : NULL;
}

void evm_state_set_code(evm_state_t *es, const address_t *addr,
                         const uint8_t *code, uint32_t len) {
    if (es) state_overlay_set_code(es->so, addr, code, len);
}

/* =========================================================================
 * Storage
 * ========================================================================= */

uint256_t evm_state_get_storage(evm_state_t *es, const address_t *addr,
                                 const uint256_t *key) {
    return es ? state_overlay_get_storage(es->so, addr, key) : UINT256_ZERO_INIT;
}

uint256_t evm_state_get_committed_storage(evm_state_t *es, const address_t *addr,
                                           const uint256_t *key) {
    return es ? state_overlay_get_committed_storage(es->so, addr, key) : UINT256_ZERO_INIT;
}

uint256_t evm_state_sload(evm_state_t *es, const address_t *addr,
                           const uint256_t *key, bool *was_warm) {
    return es ? state_overlay_sload(es->so, addr, key, was_warm) : UINT256_ZERO_INIT;
}

void evm_state_sstore_lookup(evm_state_t *es, const address_t *addr,
                               const uint256_t *key,
                               uint256_t *current, uint256_t *original,
                               bool *was_warm) {
    if (es) state_overlay_sstore_lookup(es->so, addr, key, current, original, was_warm);
}

void evm_state_set_storage(evm_state_t *es, const address_t *addr,
                            const uint256_t *key, const uint256_t *value) {
    if (es) state_overlay_set_storage(es->so, addr, key, value);
}

bool evm_state_has_storage(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_has_storage(es->so, addr) : false;
}

/* =========================================================================
 * Account lifecycle
 * ========================================================================= */

void evm_state_create_account(evm_state_t *es, const address_t *addr) {
    if (es) state_overlay_create_account(es->so, addr);
}

void evm_state_mark_existed(evm_state_t *es, const address_t *addr) {
    if (es) state_overlay_mark_existed(es->so, addr);
}

void evm_state_clear_prestate_dirty(evm_state_t *es) {
    if (es) state_overlay_clear_prestate_dirty(es->so);
}

void evm_state_self_destruct(evm_state_t *es, const address_t *addr) {
    if (es) state_overlay_self_destruct(es->so, addr);
}

bool evm_state_is_self_destructed(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_is_self_destructed(es->so, addr) : false;
}

bool evm_state_is_created(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_is_created(es->so, addr) : false;
}

/* =========================================================================
 * Commit / Snapshot / Revert
 * ========================================================================= */

void evm_state_commit(evm_state_t *es) {
    if (es) state_overlay_commit(es->so);
}

void evm_state_commit_tx(evm_state_t *es) {
    if (es) state_overlay_commit_tx(es->so);
}

void evm_state_begin_block(evm_state_t *es, uint64_t block_number) {
    if (es) state_overlay_begin_block(es->so, block_number);
}

uint32_t evm_state_snapshot(evm_state_t *es) {
    return es ? state_overlay_snapshot(es->so) : 0;
}

void evm_state_revert(evm_state_t *es, uint32_t snap_id) {
    if (es) state_overlay_revert(es->so, snap_id);
}

/* =========================================================================
 * Access lists (EIP-2929)
 * ========================================================================= */

bool evm_state_warm_address(evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_warm_address(es->so, addr) : false;
}

bool evm_state_warm_slot(evm_state_t *es, const address_t *addr,
                          const uint256_t *key) {
    return es ? state_overlay_warm_slot(es->so, addr, key) : false;
}

bool evm_state_is_address_warm(const evm_state_t *es, const address_t *addr) {
    return es ? state_overlay_is_address_warm(es->so, addr) : false;
}

bool evm_state_is_slot_warm(const evm_state_t *es, const address_t *addr,
                             const uint256_t *key) {
    return es ? state_overlay_is_slot_warm(es->so, addr, key) : false;
}

/* =========================================================================
 * Transient storage (EIP-1153)
 * ========================================================================= */

uint256_t evm_state_tload(evm_state_t *es, const address_t *addr,
                           const uint256_t *key) {
    return es ? state_overlay_tload(es->so, addr, key) : UINT256_ZERO_INIT;
}

void evm_state_tstore(evm_state_t *es, const address_t *addr,
                       const uint256_t *key, const uint256_t *value) {
    if (es) state_overlay_tstore(es->so, addr, key, value);
}

/* =========================================================================
 * Finalize / Root
 * ========================================================================= */

bool evm_state_finalize(evm_state_t *es) {
    (void)es;
    return true; /* no-op — flat_state handles persistence */
}

hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty) {
    return evm_state_compute_mpt_root(es, prune_empty);
}

hash_t evm_state_compute_mpt_root(evm_state_t *es, bool prune_empty) {
    if (!es || !es->so) return hash_zero();
    return state_overlay_compute_mpt_root(es->so, prune_empty);
}

void evm_state_prune_empty_accounts(evm_state_t *es) {
    (void)es; /* no mass prune — EIP-161 handled per-tx */
}

/* =========================================================================
 * Stats
 * ========================================================================= */

evm_state_stats_t evm_state_get_stats(const evm_state_t *es) {
    evm_state_stats_t s = {0};
    if (!es || !es->so) return s;
    state_overlay_stats_t sos = state_overlay_get_stats(es->so);
    s.cache_accounts    = sos.overlay_accounts;
    s.cache_slots       = sos.overlay_slots;
    s.cache_arena_bytes = 0; /* TODO */
    s.flat_acct_count   = sos.flat_acct_count;
    s.flat_stor_count   = sos.flat_stor_count;
    s.flat_acct_mem     = sos.flat_acct_mem;
    s.flat_stor_mem     = sos.flat_stor_mem;
    s.root_stor_ms      = sos.root_stor_ms;
    s.root_acct_ms      = sos.root_acct_ms;
    s.root_dirty_count  = sos.root_dirty_count;
    return s;
}

/* =========================================================================
 * Witness gas (no-op, verkle removed)
 * ========================================================================= */

uint64_t evm_state_witness_gas_access(evm_state_t *es,
                                       const uint8_t key[32],
                                       bool is_write,
                                       bool value_was_empty) {
    (void)es; (void)key; (void)is_write; (void)value_was_empty;
    return 0;
}

/* =========================================================================
 * Debug / Dump (stubs — TODO)
 * ========================================================================= */

#ifdef ENABLE_DEBUG
void evm_state_print_mpt_stats(evm_state_t *es) { (void)es; }
void evm_state_debug_dump(evm_state_t *es) { (void)es; }
void evm_state_dump_mpt(evm_state_t *es, const char *path) { (void)es; (void)path; }
#endif

void evm_state_dump_alloc_json(evm_state_t *es, const char *path) {
    /* TODO: delegate to state_overlay_dump_alloc_json */
    (void)es; (void)path;
}

size_t evm_state_collect_addresses(evm_state_t *es, address_t *out, size_t max) {
    /* TODO */
    (void)es; (void)out; (void)max;
    return 0;
}

size_t evm_state_collect_storage_keys(evm_state_t *es, const address_t *addr,
                                       uint256_t *out, size_t max) {
    /* TODO */
    (void)es; (void)addr; (void)out; (void)max;
    return 0;
}

#ifdef ENABLE_HISTORY
void evm_state_collect_block_diff(evm_state_t *es, struct block_diff_t *out) {
    /* TODO: delegate to state_overlay */
    (void)es; (void)out;
}

void evm_state_apply_diff_bulk(evm_state_t *es, const struct block_diff_t *diff) {
    /* TODO: delegate to state_overlay */
    (void)es; (void)diff;
}
#endif
