/**
 * Sync Engine — block execution + validation.
 *
 * Owns the full state lifecycle (evm_state, evm, verkle_state).
 * Callers provide decoded block headers and bodies from any source.
 *
 * Two-phase sync:
 *   1. Era1 replay (blocks 0 → Paris): batched root validation at interval
 *      boundaries (every N blocks). No per-block response needed.
 *   2. CL client (Paris → head): per-block root validation required
 *      (engine_newPayload needs VALID/INVALID response).
 */

#include "sync.h"
#include "evm.h"
#include "evm_state.h"
#include "block_executor.h"
#ifdef ENABLE_VERKLE
#include "verkle_state.h"
#endif
#ifdef ENABLE_MPT
#include "code_store.h"
#include "flat_state.h"
#endif
#ifdef ENABLE_HISTORY
#include "state_history.h"
#endif
#ifdef ENABLE_VERKLE_BUILD
#include "verkle_builder.h"
#endif
#include "uint256.h"
#include "address.h"
#include "keccak256.h"
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stddef.h>
#include <time.h>
#include <pthread.h>

// ============================================================================
// Constants
// ============================================================================

#define BLOCK_HASH_WINDOW 256

/* Forward declarations */
static void sync_flush_and_evict(sync_t *sync);

// ============================================================================
// Internal struct
// ============================================================================

struct sync {
    sync_config_t config;

#ifdef ENABLE_VERKLE
    verkle_state_t *vs;
#endif
#ifdef ENABLE_MPT
    code_store_t  *cs;
    flat_state_t  *flat_state;
#endif
    evm_state_t *state;
    evm_t       *evm;

    hash_t   block_hashes[BLOCK_HASH_WINDOW];
    uint64_t last_block;
    uint64_t total_gas;
    uint64_t blocks_ok;
    uint64_t blocks_fail;
    bool     genesis_loaded;

    /* Batched MPT root validation: root is computed at interval boundaries,
     * not per-block. We remember the last block's expected root for validation. */
    hash_t   pending_expected_root;
    bool     batch_root_computed;

    /* Stats snapshot taken before cache eviction (so callers see useful values) */
    evm_state_stats_t last_stats;

    /* Checkpoint timing (ms) */
    double last_evict_ms;
    double last_mpt_flush_ms;
    double last_wait_ms;

#ifdef ENABLE_MPT
    /* Background MPT flush thread */
    pthread_t       flush_thread;
    pthread_mutex_t flush_mtx;
    pthread_cond_t  flush_cond;
    bool            flush_pending;  /* work available for flush thread */
    bool            flush_stop;     /* signal thread to exit */
#endif

#ifdef ENABLE_HISTORY
    state_history_t *history;
#endif
#ifdef ENABLE_VERKLE_BUILD
    verkle_builder_t *verkle_builder;
#endif
};

// ============================================================================
// Background MPT flush
// ============================================================================

#ifdef ENABLE_MPT
static void *mpt_flush_thread(void *arg) {
    sync_t *s = (sync_t *)arg;

    pthread_mutex_lock(&s->flush_mtx);
    for (;;) {
        while (!s->flush_pending && !s->flush_stop)
            pthread_cond_wait(&s->flush_cond, &s->flush_mtx);

        if (s->flush_stop) {
            /* Final flush before exit */
            if (s->flush_pending) {
                pthread_mutex_unlock(&s->flush_mtx);
                if (s->cs) code_store_flush(s->cs);
                evm_state_flush(s->state);
                pthread_mutex_lock(&s->flush_mtx);
                s->flush_pending = false;
                pthread_cond_signal(&s->flush_cond);
            }
            break;
        }

        /* Do the flush with mutex unlocked (execution can continue) */
        pthread_mutex_unlock(&s->flush_mtx);

        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        if (s->cs) code_store_flush(s->cs);
        evm_state_flush(s->state);
        clock_gettime(CLOCK_MONOTONIC, &t1);

        pthread_mutex_lock(&s->flush_mtx);
        s->last_mpt_flush_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                                (t1.tv_nsec - t0.tv_nsec) / 1e6;
        s->flush_pending = false;
        pthread_cond_signal(&s->flush_cond);
    }
    pthread_mutex_unlock(&s->flush_mtx);
    return NULL;
}

/* Wait for any in-progress background flush to complete */
static void sync_wait_flush(sync_t *s) {
    pthread_mutex_lock(&s->flush_mtx);
    while (s->flush_pending)
        pthread_cond_wait(&s->flush_cond, &s->flush_mtx);
    pthread_mutex_unlock(&s->flush_mtx);
}

/* Signal background thread to start flushing */
static void sync_start_flush(sync_t *s) {
    pthread_mutex_lock(&s->flush_mtx);
    s->flush_pending = true;
    pthread_cond_signal(&s->flush_cond);
    pthread_mutex_unlock(&s->flush_mtx);
}
#endif

// ============================================================================
// Helpers
// ============================================================================

// ============================================================================
// Genesis loading (internal)
// ============================================================================

static bool load_genesis_json(evm_state_t *state, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        perror("load_genesis: fopen");
        return false;
    }

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);

    char *json_str = malloc(fsize + 1);
    if (!json_str) { fclose(f); return false; }
    size_t nread = fread(json_str, 1, fsize, f);
    (void)nread;
    json_str[fsize] = '\0';
    fclose(f);

    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root) {
        fprintf(stderr, "load_genesis: JSON parse error\n");
        return false;
    }

    size_t count = 0;
    cJSON *entry;
    cJSON_ArrayForEach(entry, root) {
        const char *addr_hex = entry->string;
        if (!addr_hex) continue;

        address_t addr;
        if (!address_from_hex(addr_hex, &addr)) continue;

        cJSON *bal_item = cJSON_GetObjectItem(entry, "balance");
        if (!bal_item || !cJSON_IsString(bal_item)) continue;

        uint256_t balance = uint256_from_hex(bal_item->valuestring);
        if (!uint256_is_zero(&balance)) {
            evm_state_add_balance(state, &addr, &balance);
        } else {
            evm_state_create_account(state, &addr);
        }
        count++;
    }

    cJSON_Delete(root);
    printf("Genesis: loaded %zu accounts\n", count);
    return true;
}

// ============================================================================
// Lifecycle
// ============================================================================

sync_t *sync_create(const sync_config_t *config) {
    if (!config || !config->chain_config) return NULL;

    sync_t *s = calloc(1, sizeof(sync_t));
    if (!s) return NULL;

    s->config = *config;
    /* Deep-copy paths so caller can free originals */
    if (config->verkle_value_dir)
        s->config.verkle_value_dir = strdup(config->verkle_value_dir);
    if (config->verkle_commit_dir)
        s->config.verkle_commit_dir = strdup(config->verkle_commit_dir);
    if (config->mpt_path)
        s->config.mpt_path = strdup(config->mpt_path);
    if (config->code_store_path)
        s->config.code_store_path = strdup(config->code_store_path);
    if (config->history_dir)
        s->config.history_dir = strdup(config->history_dir);
    if (config->verkle_builder_value_dir)
        s->config.verkle_builder_value_dir = strdup(config->verkle_builder_value_dir);
    if (config->verkle_builder_commit_dir)
        s->config.verkle_builder_commit_dir = strdup(config->verkle_builder_commit_dir);

    bool resumed = false;

#ifdef ENABLE_VERKLE
    /* Create fresh verkle state */
    if (s->config.verkle_value_dir && s->config.verkle_commit_dir) {
        s->vs = verkle_state_create_flat(s->config.verkle_value_dir,
                                         s->config.verkle_commit_dir);
        if (!s->vs) {
            fprintf(stderr, "Failed to create verkle state\n");
            goto fail;
        }
    }
#endif

#ifdef ENABLE_MPT
    /* Open or create code store */
    if (s->config.code_store_path) {
        s->cs = code_store_open(s->config.code_store_path);
        if (!s->cs)
            s->cs = code_store_create(s->config.code_store_path, 500000);
        if (!s->cs)
            fprintf(stderr, "WARNING: failed to open/create code store at '%s'\n"
                    "  hint: check disk space and file permissions\n",
                    s->config.code_store_path);
    }

#endif

    /* Create evm_state */
    s->state = evm_state_create(
#ifdef ENABLE_VERKLE
        s->vs,
#else
        NULL,
#endif
#ifdef ENABLE_MPT
        s->config.mpt_path,
        s->cs
#else
        NULL,
        NULL
#endif
    );
    if (!s->state) {
        fprintf(stderr, "FATAL: failed to create EVM state\n"
                "  hint: check stderr above for MPT store errors\n"
                "  hint: common cause: corrupt .idx/.dat files from killed process\n"
                "  hint: delete state files and replay fresh\n");
        goto fail;
    }

    /* Batch mode: defer per-block verkle/MPT flush to checkpoint boundaries */
    evm_state_set_batch_mode(s->state, true);

#ifdef ENABLE_MPT
    /* Flat state: O(1) disk-backed lookups for cache misses */
    if (config->flat_state_path) {
        s->flat_state = flat_state_open(config->flat_state_path);
        if (!s->flat_state) {
            s->flat_state = flat_state_create(config->flat_state_path,
                                               (uint64_t)FLAT_ACCOUNT_CAPACITY,
                                               (uint64_t)FLAT_STORAGE_CAPACITY);
        }
        if (s->flat_state)
            evm_state_set_flat_state(s->state, s->flat_state);
        else
            fprintf(stderr, "warning: failed to open/create flat state at %s\n",
                    config->flat_state_path);
    }
    /* Start background MPT flush thread */
    pthread_mutex_init(&s->flush_mtx, NULL);
    pthread_cond_init(&s->flush_cond, NULL);
    s->flush_pending = false;
    s->flush_stop = false;
    pthread_create(&s->flush_thread, NULL, mpt_flush_thread, s);
#endif

    /* Create EVM */
    s->evm = evm_create(s->state, config->chain_config);
    if (!s->evm) {
        fprintf(stderr, "Failed to create EVM\n");
        goto fail;
    }

#ifdef ENABLE_HISTORY
    if (s->config.history_dir) {
        s->history = state_history_create(s->config.history_dir);
        if (s->history && resumed)
            state_history_truncate(s->history, s->last_block);
    }
#endif

#ifdef ENABLE_VERKLE_BUILD
    if (s->config.verkle_builder_value_dir && s->config.verkle_builder_commit_dir) {
        /* Try open first (resume), fall back to create */
        s->verkle_builder = verkle_builder_open(s->config.verkle_builder_value_dir,
                                                 s->config.verkle_builder_commit_dir,
                                                 s->cs);
        if (!s->verkle_builder)
            s->verkle_builder = verkle_builder_create(s->config.verkle_builder_value_dir,
                                                       s->config.verkle_builder_commit_dir,
                                                       s->cs);
        if (!s->verkle_builder)
            fprintf(stderr, "Warning: failed to create verkle builder\n");
    }
#endif

    (void)resumed;
    return s;

fail:
    if (s->evm) evm_destroy(s->evm);
    if (s->state) evm_state_destroy(s->state);
#ifdef ENABLE_VERKLE
    if (s->vs) verkle_state_destroy(s->vs);
#endif
    free((char *)s->config.verkle_value_dir);
    free((char *)s->config.verkle_commit_dir);
    free((char *)s->config.mpt_path);
    free((char *)s->config.code_store_path);
    free((char *)s->config.history_dir);
    free((char *)s->config.verkle_builder_value_dir);
    free((char *)s->config.verkle_builder_commit_dir);
    free(s);
    return NULL;
}

void sync_destroy(sync_t *sync) {
    if (!sync) return;

    if (sync->evm) evm_destroy(sync->evm);
#ifdef ENABLE_MPT
    /* Stop background flush thread (flush any pending work first) */
    pthread_mutex_lock(&sync->flush_mtx);
    sync->flush_stop = true;
    pthread_cond_signal(&sync->flush_cond);
    pthread_mutex_unlock(&sync->flush_mtx);
    pthread_join(sync->flush_thread, NULL);
    pthread_cond_destroy(&sync->flush_cond);
    pthread_mutex_destroy(&sync->flush_mtx);
#endif
    if (sync->state) evm_state_destroy(sync->state);
#ifdef ENABLE_VERKLE
    if (sync->vs) verkle_state_destroy(sync->vs);
#endif
#ifdef ENABLE_MPT
    if (sync->cs) code_store_destroy(sync->cs);
    if (sync->flat_state) flat_state_destroy(sync->flat_state);
#endif
#ifdef ENABLE_HISTORY
    if (sync->history) state_history_destroy(sync->history);
#endif
#ifdef ENABLE_VERKLE_BUILD
    if (sync->verkle_builder) verkle_builder_destroy(sync->verkle_builder);
#endif

    free((char *)sync->config.verkle_value_dir);
    free((char *)sync->config.verkle_commit_dir);
    free((char *)sync->config.mpt_path);
    free((char *)sync->config.flat_state_path);
    free((char *)sync->config.code_store_path);
    free((char *)sync->config.history_dir);
    free((char *)sync->config.verkle_builder_value_dir);
    free((char *)sync->config.verkle_builder_commit_dir);
    free(sync);
}

// ============================================================================
// Genesis
// ============================================================================

bool sync_load_genesis(sync_t *sync, const char *genesis_json_path,
                       const hash_t *genesis_hash) {
    if (!sync || !genesis_json_path) return false;
    if (sync->genesis_loaded) {
        fprintf(stderr, "sync_load_genesis: genesis already loaded or resumed\n");
        return false;
    }

    /* Open block 0 (required for flat backend tracking) */
    evm_state_begin_block(sync->state, 0);

    if (!load_genesis_json(sync->state, genesis_json_path))
        return false;

    /* Commit genesis as original state (EIP-2200) + flush */
    evm_state_commit(sync->state);
    evm_state_finalize(sync->state);
    evm_state_compute_state_root_ex(sync->state, false);

    /* Store genesis block hash */
    if (genesis_hash)
        sync->block_hashes[0] = *genesis_hash;

#ifdef ENABLE_VERKLE
    if (sync->vs) verkle_state_sync(sync->vs);
#endif

    sync->genesis_loaded = true;
    return true;
}

// ============================================================================
// Resume from existing state
// ============================================================================

bool sync_resume(sync_t *sync, uint64_t last_block,
                 const hash_t *block_hashes, size_t count) {
    if (!sync) return false;
    if (sync->genesis_loaded) {
        fprintf(stderr, "sync_resume: genesis already loaded or resumed\n");
        return false;
    }

    /* Populate block hash ring */
    if (block_hashes && count > 0) {
        for (size_t i = 0; i < count; i++) {
            uint64_t bn = last_block - count + 1 + i;
            sync->block_hashes[bn % BLOCK_HASH_WINDOW] = block_hashes[i];
        }
    }

    sync->last_block = last_block;
    sync->genesis_loaded = true;
    return true;
}

// ============================================================================
// Batch MPT root validation (internal)
// ============================================================================

#ifdef ENABLE_MPT
/**
 * Compute MPT state root and validate against the pending expected root
 * (saved from the last block's header). Called at checkpoint boundaries.
 *
 * Sets sync->batch_root_computed = true on success.
 * Returns true if root matches (or validation is disabled).
 * On mismatch, populates actual/expected in sync for the caller to report.
 */
static bool sync_validate_batch_root(sync_t *sync,
                                     hash_t *actual_out,
                                     hash_t *expected_out) {
#ifdef ENABLE_MPT
    sync_wait_flush(sync);
#endif
    bool prune_empty = (sync->evm->fork >= FORK_SPURIOUS_DRAGON);
    hash_t actual = evm_state_compute_mpt_root(sync->state, prune_empty);
    sync->batch_root_computed = true;

    if (actual_out)   *actual_out   = actual;
    if (expected_out)  *expected_out = sync->pending_expected_root;

    if (!sync->config.validate_state_root)
        return true;

    return memcmp(actual.bytes, sync->pending_expected_root.bytes, 32) == 0;
}
#endif

// ============================================================================
// Block Execution
// ============================================================================

bool sync_execute_block(sync_t *sync,
                        const block_header_t *header,
                        const block_body_t *body,
                        const hash_t *block_hash,
                        sync_block_result_t *result) {
    if (!sync || !header || !body || !block_hash || !result) return false;

    memset(result, 0, sizeof(*result));

    uint64_t bn = header->number;

    /* Execute block (block_hashes contains hashes up to block bn-1) */
    block_result_t br = block_execute(sync->evm, header, body,
                                      sync->block_hashes
#ifdef ENABLE_HISTORY
                                      , sync->history
#endif
#ifdef ENABLE_VERKLE_BUILD
                                      , sync->verkle_builder
#endif
                                      );

    /* Store current block's hash AFTER execution so it doesn't overwrite
     * the hash 256 blocks ago (same ring buffer slot) during execution. */
    sync->block_hashes[bn % BLOCK_HASH_WINDOW] = *block_hash;

    result->gas_used = br.gas_used;
    result->tx_count = br.tx_count;
    result->transfer_count = br.transfer_count;
    result->call_count = br.call_count;

    /* Validate gas */
    bool gas_match = (br.gas_used == header->gas_used);
    result->expected_gas = header->gas_used;
    result->actual_gas   = br.gas_used;

    /* MPT root: deferred to checkpoint boundaries (not per-block).
     * Save the expected root from each block header — at checkpoint time
     * we validate against the last block's root. */
#ifdef ENABLE_MPT
    if (sync->config.validate_state_root) {
        sync->pending_expected_root = header->state_root;
    }
#endif


    /* Determine outcome (gas only — root validated at checkpoint) */
    if (!gas_match) {
        result->ok    = false;
        result->error = SYNC_GAS_MISMATCH;
        /* Transfer receipt ownership to caller for debugging */
        result->receipts = br.receipts;
        result->receipt_count = br.receipt_count;
        br.receipts = NULL;
        br.receipt_count = 0;
        sync->blocks_fail++;
    } else {
        result->ok    = true;
        result->error = SYNC_OK;
        sync->blocks_ok++;
    }

    sync->total_gas += br.gas_used;
    sync->last_block = bn;

    if (!result->ok) {
#ifdef ENABLE_VERKLE
        if (sync->vs) verkle_state_revert_block(sync->vs);
#endif
    }

    /* Periodic root validation + flush + evict */
    if (result->ok &&
        sync->config.checkpoint_interval > 0 &&
        sync->blocks_fail == 0 &&
        bn % sync->config.checkpoint_interval == 0) {

#ifdef ENABLE_MPT
        /* Validate MPT root at interval boundary */
        hash_t actual_root, expected_root;
        if (!sync_validate_batch_root(sync, &actual_root, &expected_root)) {
            result->ok    = false;
            result->error = SYNC_ROOT_MISMATCH;
            result->actual_root   = actual_root;
            result->expected_root = expected_root;
            sync->blocks_ok--;
            sync->blocks_fail++;
            block_result_free(&br);
            return true;
        }
#endif
        sync_flush_and_evict(sync);
    }

    block_result_free(&br);
    return true;
}

// ============================================================================
// Live Mode (per-block validation for CL sync)
// ============================================================================

bool sync_execute_block_live(sync_t *sync,
                              const block_header_t *header,
                              const block_body_t *body,
                              const hash_t *block_hash,
                              sync_block_result_t *result) {
    if (!sync || !header || !body || !block_hash || !result) return false;

    memset(result, 0, sizeof(*result));

    uint64_t bn = header->number;

    /* Execute block (block_hashes contains hashes up to block bn-1) */
    block_result_t br = block_execute(sync->evm, header, body,
                                      sync->block_hashes
#ifdef ENABLE_HISTORY
                                      , sync->history
#endif
#ifdef ENABLE_VERKLE_BUILD
                                      , sync->verkle_builder
#endif
                                      );

    /* Store current block's hash AFTER execution so it doesn't overwrite
     * the hash 256 blocks ago (same ring buffer slot) during execution. */
    sync->block_hashes[bn % BLOCK_HASH_WINDOW] = *block_hash;

    result->gas_used = br.gas_used;
    result->tx_count = br.tx_count;
    result->transfer_count = br.transfer_count;
    result->call_count = br.call_count;
    result->expected_gas = header->gas_used;
    result->actual_gas   = br.gas_used;

    /* Validate gas */
    if (br.gas_used != header->gas_used) {
        result->ok    = false;
        result->error = SYNC_GAS_MISMATCH;
        sync->blocks_fail++;
        sync->total_gas += br.gas_used;
        sync->last_block = bn;
        block_result_free(&br);
        return true;
    }

#ifdef ENABLE_MPT
    /* Immediate state root validation */
    if (sync->config.validate_state_root) {
        sync_wait_flush(sync);
        bool prune_empty = (sync->evm->fork >= FORK_SPURIOUS_DRAGON);
        hash_t actual = evm_state_compute_mpt_root(sync->state, prune_empty);

        if (memcmp(actual.bytes, header->state_root.bytes, 32) != 0) {
            result->ok    = false;
            result->error = SYNC_ROOT_MISMATCH;
            result->actual_root   = actual;
            result->expected_root = header->state_root;
            sync->blocks_fail++;
            sync->total_gas += br.gas_used;
            sync->last_block = bn;
            block_result_free(&br);
            return true;
        }
    }

    /* Synchronous flush — data on disk before returning VALID */
    evm_state_flush(sync->state);
    if (sync->cs) code_store_flush(sync->cs);
#endif

    sync->blocks_ok++;
    sync->total_gas += br.gas_used;
    sync->last_block = bn;

    block_result_free(&br);
    return true;
}

void sync_set_live_mode(sync_t *sync, bool live) {
    if (!sync) return;
    evm_state_set_batch_mode(sync->state, !live);
}


// ============================================================================
// Flush + Evict (periodic, after root validation)
// ============================================================================

static void sync_flush_and_evict(sync_t *sync) {
    if (!sync) return;

#ifdef ENABLE_MPT
    /* Wait for any previous background flush to complete before
     * computing a new root (flush modifies mpt_store state) */
    {
        struct timespec _w0, _w1;
        clock_gettime(CLOCK_MONOTONIC, &_w0);
        sync_wait_flush(sync);
        clock_gettime(CLOCK_MONOTONIC, &_w1);
        sync->last_wait_ms = (_w1.tv_sec - _w0.tv_sec) * 1000.0 +
                              (_w1.tv_nsec - _w0.tv_nsec) / 1e6;
    }

    /* Compute MPT root before eviction if not already done.
     * This ensures dirty data is captured into deferred buffer
     * before cache entries are dropped. */
    if (!sync->batch_root_computed) {
        bool prune_empty = (sync->evm->fork >= FORK_SPURIOUS_DRAGON);
        evm_state_compute_mpt_root(sync->state, prune_empty);
        sync->batch_root_computed = true;
    }
#endif

#ifdef ENABLE_VERKLE
    evm_state_flush_verkle(sync->state);
    if (sync->vs) verkle_state_sync(sync->vs);
#endif

    /* Snapshot stats before eviction clears the cache */
    sync->last_stats = evm_state_get_stats(sync->state);

    /* Evict cache — root computation captured all dirty data into MPT
     * deferred buffer. Safe to drop cached entries now. */
    struct timespec _ev0, _ev1;
    clock_gettime(CLOCK_MONOTONIC, &_ev0);
#ifdef ENABLE_DEBUG
    if (!sync->config.no_evict)
#endif
        evm_state_evict_cache(sync->state);
    clock_gettime(CLOCK_MONOTONIC, &_ev1);

    sync->last_evict_ms = (_ev1.tv_sec - _ev0.tv_sec) * 1000.0 +
                           (_ev1.tv_nsec - _ev0.tv_nsec) / 1e6;

#ifdef ENABLE_MPT
    /* Kick off background flush — execution continues immediately */
    sync_start_flush(sync);
#endif

    sync->batch_root_computed = false;
}

bool sync_get_block_hash(const sync_t *sync, uint64_t block_number, hash_t *out) {
    if (!sync || !out || block_number == 0) return false;
    if (block_number > sync->last_block) return false;
    if (sync->last_block - block_number >= BLOCK_HASH_WINDOW) return false;
    *out = sync->block_hashes[block_number % BLOCK_HASH_WINDOW];
    return true;
}

// ============================================================================
// Status
// ============================================================================

sync_status_t sync_get_status(const sync_t *sync) {
    sync_status_t st = {0};
    if (!sync) return st;
    st.last_block  = sync->last_block;
    st.total_gas   = sync->total_gas;
    st.blocks_ok   = sync->blocks_ok;
    st.blocks_fail = sync->blocks_fail;
    return st;
}

evm_state_stats_t sync_get_state_stats(const sync_t *sync) {
    if (!sync) return (evm_state_stats_t){0};
    evm_state_stats_t st = sync->last_stats;
#ifdef ENABLE_MPT
    st.evict_ms = sync->last_evict_ms;
    st.mpt_flush_ms = sync->last_mpt_flush_ms;
    st.wait_flush_ms = sync->last_wait_ms;
#endif
    return st;
}

sync_history_stats_t sync_get_history_stats(const sync_t *sync) {
    sync_history_stats_t st = {0};
#ifdef ENABLE_HISTORY
    if (sync && sync->history) {
        st.blocks  = state_history_block_count(sync->history);
        st.disk_mb = (double)state_history_disk_bytes(sync->history) / (1024.0 * 1024.0);
    }
#endif
    return st;
}

void sync_truncate_history(sync_t *sync, uint64_t last_block) {
#ifdef ENABLE_HISTORY
    if (sync && sync->history)
        state_history_truncate(sync->history, last_block);
#else
    (void)sync; (void)last_block;
#endif
}

evm_state_t *sync_get_state(const sync_t *sync) {
    return sync ? sync->state : NULL;
}
