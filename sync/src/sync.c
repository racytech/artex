/**
 * Sync Engine — block execution + validation + checkpointing.
 *
 * Owns the full state lifecycle (evm_state, evm, verkle_state).
 * Callers provide decoded block headers and bodies from any source.
 *
 * Two-phase sync:
 *   1. Era1 replay (blocks 0 → Paris): batched root validation at checkpoint
 *      boundaries (every N blocks). No per-block response needed.
 *   2. CL client (Paris → head): per-block root validation required
 *      (engine_newPayload needs VALID/INVALID response).
 *
 * At the era1 → CL transition point, the feeder must call sync_checkpoint()
 * to flush all state before switching to per-block validation mode
 * (checkpoint_interval = 1 or a dedicated per-block validate API).
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
#endif
#ifdef ENABLE_HISTORY
#include "state_history.h"
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

/* Forward declarations */
static void sync_wait_flush(sync_t *sync);

// ============================================================================
// Constants
// ============================================================================

#define BLOCK_HASH_WINDOW 256

// ============================================================================
// Checkpoint format (internal)
// ============================================================================

#define CKPT_MAGIC   0x54504B43  /* "CKPT" little-endian */
#define CKPT_VERSION 1

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t block_number;
    uint64_t total_gas;
    uint64_t blocks_ok;
    uint64_t blocks_fail;
    hash_t   block_hashes[BLOCK_HASH_WINDOW];
    hash_t   checksum;
} checkpoint_t;

// ============================================================================
// Internal struct
// ============================================================================

struct sync {
    sync_config_t config;

#ifdef ENABLE_VERKLE
    verkle_state_t *vs;
#endif
#ifdef ENABLE_MPT
    code_store_t *cs;
#endif
    evm_state_t *state;
    evm_t       *evm;

    hash_t   block_hashes[BLOCK_HASH_WINDOW];
    uint64_t last_block;
    uint64_t total_gas;
    uint64_t blocks_ok;
    uint64_t blocks_fail;
    uint64_t last_checkpoint_block;
    uint64_t resumed_block;   /* block number from checkpoint (0 if fresh) */
    bool     genesis_loaded;

    /* Batched MPT root validation: root is computed at checkpoint boundaries,
     * not per-block. We remember the last block's expected root for validation. */
    hash_t   pending_expected_root;
    bool     batch_root_computed;

    /* Stats snapshot taken before cache eviction (so callers see useful values) */
    evm_state_stats_t last_stats;

#ifdef ENABLE_HISTORY
    state_history_t *history;
#endif

    /* Background flush thread */
    pthread_t       flush_thread;
    bool            flush_thread_active;
};

// ============================================================================
// Helpers
// ============================================================================

#ifdef ENABLE_VERKLE
static bool dir_exists(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}
#endif

static bool file_exists(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

// ============================================================================
// Checkpoint functions
// ============================================================================

static void checkpoint_compute_checksum(checkpoint_t *ckpt) {
    size_t payload_size = offsetof(checkpoint_t, checksum);
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, (const uint8_t *)ckpt, (uint16_t)payload_size);
    keccak_final(&ctx, ckpt->checksum.bytes);
}

static bool checkpoint_verify(const checkpoint_t *ckpt) {
    if (ckpt->magic != CKPT_MAGIC || ckpt->version != CKPT_VERSION)
        return false;

    hash_t expected;
    size_t payload_size = offsetof(checkpoint_t, checksum);
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, (const uint8_t *)ckpt, (uint16_t)payload_size);
    keccak_final(&ctx, expected.bytes);

    return memcmp(expected.bytes, ckpt->checksum.bytes, 32) == 0;
}

static bool checkpoint_save_internal(const char *path,
                                     uint64_t block_number,
                                     const hash_t block_hashes[BLOCK_HASH_WINDOW],
                                     uint64_t total_gas,
                                     uint64_t blocks_ok,
                                     uint64_t blocks_fail) {
    if (!path) return false;

    checkpoint_t ckpt;
    memset(&ckpt, 0, sizeof(ckpt));
    ckpt.magic        = CKPT_MAGIC;
    ckpt.version      = CKPT_VERSION;
    ckpt.block_number = block_number;
    ckpt.total_gas    = total_gas;
    ckpt.blocks_ok    = blocks_ok;
    ckpt.blocks_fail  = blocks_fail;
    memcpy(ckpt.block_hashes, block_hashes, sizeof(ckpt.block_hashes));
    checkpoint_compute_checksum(&ckpt);

    /* Build temp path */
    size_t pathlen = strlen(path);
    char *tmp_path = malloc(pathlen + 5);
    if (!tmp_path) return false;
    snprintf(tmp_path, pathlen + 5, "%s.tmp", path);

    FILE *f = fopen(tmp_path, "wb");
    if (!f) {
        perror("checkpoint_save: fopen tmp");
        free(tmp_path);
        return false;
    }
    if (fwrite(&ckpt, sizeof(ckpt), 1, f) != 1) {
        perror("checkpoint_save: fwrite");
        fclose(f);
        unlink(tmp_path);
        free(tmp_path);
        return false;
    }
    fflush(f);
    fsync(fileno(f));
    fclose(f);

    if (rename(tmp_path, path) != 0) {
        perror("checkpoint_save: rename");
        unlink(tmp_path);
        free(tmp_path);
        return false;
    }

    free(tmp_path);
    return true;
}

static bool checkpoint_load_internal(const char *path, checkpoint_t *out) {
    if (!path) return false;

    FILE *f = fopen(path, "rb");
    if (!f) return false;

    if (fread(out, sizeof(*out), 1, f) != 1) {
        fclose(f);
        return false;
    }
    fclose(f);

    if (!checkpoint_verify(out)) {
        fprintf(stderr, "Checkpoint corrupt — ignoring\n");
        return false;
    }
    return true;
}

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
    if (config->checkpoint_path)
        s->config.checkpoint_path = strdup(config->checkpoint_path);
    if (config->history_dir)
        s->config.history_dir = strdup(config->history_dir);

    /* Try to resume from checkpoint */
    checkpoint_t ckpt;
    bool resumed = false;

    if (s->config.checkpoint_path &&
        file_exists(s->config.checkpoint_path) &&
#ifdef ENABLE_VERKLE
        (!s->config.verkle_value_dir || dir_exists(s->config.verkle_value_dir)) &&
        (!s->config.verkle_commit_dir || dir_exists(s->config.verkle_commit_dir)) &&
#endif
        checkpoint_load_internal(s->config.checkpoint_path, &ckpt))
    {
        printf("Checkpoint found: block %lu (ok=%lu fail=%lu gas=%lu)\n",
               ckpt.block_number, ckpt.blocks_ok, ckpt.blocks_fail, ckpt.total_gas);

#ifdef ENABLE_VERKLE
        if (s->config.verkle_value_dir && s->config.verkle_commit_dir) {
            s->vs = verkle_state_open_flat(s->config.verkle_value_dir,
                                           s->config.verkle_commit_dir);
            if (!s->vs) {
                fprintf(stderr, "Failed to open existing verkle state — starting fresh\n");
            }
        }
        if (s->vs) {
#endif
            memcpy(s->block_hashes, ckpt.block_hashes, sizeof(s->block_hashes));
            s->last_block           = ckpt.block_number;
            s->total_gas            = ckpt.total_gas;
            s->blocks_ok            = ckpt.blocks_ok;
            s->blocks_fail          = ckpt.blocks_fail;
            s->last_checkpoint_block = ckpt.block_number;
            s->resumed_block        = ckpt.block_number;
            s->genesis_loaded       = true;
            resumed = true;
            printf("Resuming from block %lu\n", s->last_block + 1);
#ifdef ENABLE_VERKLE
        }
#endif
    }

#ifdef ENABLE_VERKLE
    /* Fresh verkle state if no checkpoint or resume failed */
    if (!s->vs && s->config.verkle_value_dir && s->config.verkle_commit_dir) {
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
        fprintf(stderr, "Failed to create EVM state\n");
        goto fail;
    }

    /* Batch mode: defer per-block verkle/MPT flush to checkpoint boundaries */
    evm_state_set_batch_mode(s->state, true);

    /* Create EVM */
    s->evm = evm_create(s->state, config->chain_config);
    if (!s->evm) {
        fprintf(stderr, "Failed to create EVM\n");
        goto fail;
    }

#ifdef ENABLE_HISTORY
    if (s->config.history_dir) {
        s->history = state_history_create(s->config.history_dir);
        if (!s->history)
            fprintf(stderr, "Warning: failed to create state history at %s\n",
                    s->config.history_dir);
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
    free((char *)s->config.checkpoint_path);
    free((char *)s->config.history_dir);
    free(s);
    return NULL;
}

void sync_destroy(sync_t *sync) {
    if (!sync) return;

    /* Wait for any in-flight background flush to complete */
    sync_wait_flush(sync);

    /* Do NOT save a final checkpoint for unsaved progress.
     * Only auto-checkpoints (with validated MPT roots) are trustworthy.
     * SIGINT or end-of-run should preserve the last validated checkpoint. */

    /* Discard pending MPT writes to avoid corrupting the on-disk state.
     * The checkpoint file reflects the last validated boundary — any
     * blocks executed past that are unvalidated and must not be flushed. */
    if (sync->state && sync->last_block > sync->last_checkpoint_block)
        evm_state_discard_pending(sync->state);

    if (sync->evm) evm_destroy(sync->evm);
    if (sync->state) evm_state_destroy(sync->state);
#ifdef ENABLE_VERKLE
    if (sync->vs) verkle_state_destroy(sync->vs);
#endif
#ifdef ENABLE_MPT
    if (sync->cs) code_store_destroy(sync->cs);
#endif
#ifdef ENABLE_HISTORY
    if (sync->history) state_history_destroy(sync->history);
#endif

    free((char *)sync->config.verkle_value_dir);
    free((char *)sync->config.verkle_commit_dir);
    free((char *)sync->config.mpt_path);
    free((char *)sync->config.code_store_path);
    free((char *)sync->config.checkpoint_path);
    free((char *)sync->config.history_dir);
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

    /* Save genesis checkpoint */
#ifdef ENABLE_VERKLE
    if (sync->vs) verkle_state_sync(sync->vs);
#endif
    if (sync->config.checkpoint_path) {
        checkpoint_save_internal(sync->config.checkpoint_path,
                                 0, sync->block_hashes, 0, 0, 0);
        printf("Genesis checkpoint saved\n");
    }

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
                                      );

    /* Store current block's hash AFTER execution so it doesn't overwrite
     * the hash 256 blocks ago (same ring buffer slot) during execution. */
    sync->block_hashes[bn % BLOCK_HASH_WINDOW] = *block_hash;

    result->gas_used = br.gas_used;
    result->tx_count = br.tx_count;

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
        sync->blocks_fail++;
    } else {
        result->ok    = true;
        result->error = SYNC_OK;
        sync->blocks_ok++;
    }

    sync->total_gas += br.gas_used;
    sync->last_block = bn;

    /* Handle failure: revert to last checkpoint boundary.
     * The persistent MPT store is only flushed at checkpoint boundaries,
     * so we must resume from there — not from last_good (which may be
     * ahead of what the MPT store has committed). */
    if (!result->ok) {
#ifdef ENABLE_VERKLE
        if (sync->vs) verkle_state_revert_block(sync->vs);
#endif
        /* Keep checkpoint at last_checkpoint_block — don't advance it.
         * On resume, blocks after last_checkpoint_block will be re-executed
         * against the correct MPT state. */
    }

    /* Auto-checkpoint on interval — validate batch root, then save */
    if (result->ok &&
        sync->config.checkpoint_interval > 0 &&
        sync->blocks_fail == 0 &&
        bn % sync->config.checkpoint_interval == 0) {
        struct timespec t_start, t_end, t_root;

        /* Wait for previous background flush to complete before root computation.
         * Normally instant since flush (6.8s) finishes during execution (15.3s). */
        sync_wait_flush(sync);

        clock_gettime(CLOCK_MONOTONIC, &t_start);
#ifdef ENABLE_MPT
        /* Validate MPT root at checkpoint boundary */
        hash_t actual_root, expected_root;
        if (!sync_validate_batch_root(sync, &actual_root, &expected_root)) {
            /* Root mismatch — report failure */
            result->ok    = false;
            result->error = SYNC_ROOT_MISMATCH;
            result->actual_root   = actual_root;
            result->expected_root = expected_root;
            sync->blocks_ok--;      /* undo the ok count from above */
            sync->blocks_fail++;
            block_result_free(&br);
            return true;
        }
#endif
        clock_gettime(CLOCK_MONOTONIC, &t_root);
        sync_checkpoint(sync);
        clock_gettime(CLOCK_MONOTONIC, &t_end);
        double root_ms = (t_root.tv_sec - t_start.tv_sec) * 1000.0 +
                         (t_root.tv_nsec - t_start.tv_nsec) / 1e6;
        double ckpt_ms = (t_end.tv_sec - t_root.tv_sec) * 1000.0 +
                         (t_end.tv_nsec - t_root.tv_nsec) / 1e6;
        double total_ms = root_ms + ckpt_ms;
        fprintf(stderr, "checkpoint block=%lu  root=%.1f ms  spawn=%.1f ms  total=%.1f ms\n",
                bn, root_ms, ckpt_ms, total_ms);
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
                                      );

    /* Store current block's hash AFTER execution so it doesn't overwrite
     * the hash 256 blocks ago (same ring buffer slot) during execution. */
    sync->block_hashes[bn % BLOCK_HASH_WINDOW] = *block_hash;

    result->gas_used = br.gas_used;
    result->tx_count = br.tx_count;
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

    /* Save checkpoint per-block */
    sync->blocks_ok++;
    sync->total_gas += br.gas_used;
    sync->last_block = bn;

    if (sync->config.checkpoint_path) {
        checkpoint_save_internal(sync->config.checkpoint_path,
                                 bn, sync->block_hashes,
                                 sync->total_gas, sync->blocks_ok,
                                 sync->blocks_fail);
        sync->last_checkpoint_block = bn;
    }

    block_result_free(&br);
    return true;
}

void sync_set_live_mode(sync_t *sync, bool live) {
    if (!sync) return;
    evm_state_set_batch_mode(sync->state, !live);
}

struct evm *sync_get_evm(const sync_t *sync) {
    return sync ? sync->evm : NULL;
}

// ============================================================================
// Background Flush
// ============================================================================

typedef struct {
    evm_state_t  *state;
    const char   *checkpoint_path;
    uint64_t      block_number;
    hash_t        block_hashes[BLOCK_HASH_WINDOW];
    uint64_t      total_gas;
    uint64_t      blocks_ok;
    uint64_t      blocks_fail;
} flush_ctx_t;

static void *flush_thread_fn(void *arg) {
    flush_ctx_t *ctx = (flush_ctx_t *)arg;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Flush MPT stores using separate disk_hash instances */
    evm_state_flush_bg(ctx->state);

    /* Save checkpoint marker */
    if (ctx->checkpoint_path) {
        checkpoint_save_internal(ctx->checkpoint_path,
                                 ctx->block_number, ctx->block_hashes,
                                 ctx->total_gas, ctx->blocks_ok,
                                 ctx->blocks_fail);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                (t1.tv_nsec - t0.tv_nsec) / 1e6;
    fprintf(stderr, "  └ bg_flush=%.1f ms\n", ms);

    free(ctx);
    return NULL;
}

static void sync_wait_flush(sync_t *sync) {
    if (!sync->flush_thread_active) return;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    pthread_join(sync->flush_thread, NULL);
    sync->flush_thread_active = false;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double wait_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                     (t1.tv_nsec - t0.tv_nsec) / 1e6;

    /* Finalize: free deferred buffers + refresh disk_hash metadata */
    evm_state_flush_complete(sync->state);

    if (wait_ms > 1.0)
        fprintf(stderr, "  └ flush_join=%.1f ms\n", wait_ms);
}

// ============================================================================
// Checkpoint
// ============================================================================

bool sync_checkpoint(sync_t *sync) {
    if (!sync || !sync->config.checkpoint_path) return false;

#ifdef ENABLE_MPT
    /* Compute MPT root before flush/eviction if not already done.
     * This ensures dirty flags are cleared before eviction (which
     * drops cache entries — dirty data would be lost). */
    if (!sync->batch_root_computed) {
        bool prune_empty = (sync->evm->fork >= FORK_SPURIOUS_DRAGON);
        evm_state_compute_mpt_root(sync->state, prune_empty);
        sync->batch_root_computed = true;
    }
#endif

    /* Flush verkle synchronously (fast, executor needs consistent state) */
#ifdef ENABLE_VERKLE
    evm_state_flush_verkle(sync->state);
    if (sync->vs) verkle_state_sync(sync->vs);
#endif

    /* Snapshot stats before eviction clears the cache */
    sync->last_stats = evm_state_get_stats(sync->state);

    /* Evict cache — root computation captured all dirty data into MPT
     * deferred buffer, verkle flush wrote dirty state to backing store.
     * Safe to drop cached entries now; read-through will reload on demand. */
#ifdef ENABLE_DEBUG
    if (!sync->config.no_evict)
#endif
        evm_state_evict_cache(sync->state);

    /* Advance checkpoint block — root is validated, flush will persist it */
    sync->last_checkpoint_block = sync->last_block;

#ifdef ENABLE_MPT
    /* Spawn background flush thread — MPT flush + code_store + checkpoint save.
     * Executor continues immediately with next batch. */
    flush_ctx_t *ctx = malloc(sizeof(*ctx));
    /* Flush code store synchronously — it shares locks with executor */
    if (sync->cs) code_store_flush(sync->cs);

    if (ctx) {
        ctx->state           = sync->state;
        ctx->checkpoint_path = sync->config.checkpoint_path;
        ctx->block_number    = sync->last_block;
        memcpy(ctx->block_hashes, sync->block_hashes, sizeof(sync->block_hashes));
        ctx->total_gas       = sync->total_gas;
        ctx->blocks_ok       = sync->blocks_ok;
        ctx->blocks_fail     = sync->blocks_fail;

        if (pthread_create(&sync->flush_thread, NULL, flush_thread_fn, ctx) == 0) {
            sync->flush_thread_active = true;
        } else {
            /* Thread creation failed — fall back to synchronous flush */
            fprintf(stderr, "WARNING: bg flush thread creation failed, "
                    "falling back to synchronous\n");
            free(ctx);
            evm_state_flush(sync->state);
            if (sync->cs) code_store_flush(sync->cs);
            checkpoint_save_internal(sync->config.checkpoint_path,
                                     sync->last_block, sync->block_hashes,
                                     sync->total_gas, sync->blocks_ok,
                                     sync->blocks_fail);
            evm_state_evict_cache(sync->state);
        }
    } else {
        /* malloc failed — synchronous fallback */
        evm_state_flush(sync->state);
        if (sync->cs) code_store_flush(sync->cs);
        evm_state_evict_cache(sync->state);
    }
#else
    evm_state_evict_cache(sync->state);
#endif

    /* Reset for next batch */
    sync->batch_root_computed = false;

    return true;
}

uint64_t sync_resumed_from(const sync_t *sync) {
    return sync ? sync->resumed_block : 0;
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
    return sync->last_stats;
}

evm_state_t *sync_get_state(const sync_t *sync) {
    return sync ? sync->state : NULL;
}
