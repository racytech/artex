/**
 * Sync Engine — block execution + validation + checkpointing.
 *
 * Owns the full state lifecycle (evm_state, evm, verkle_state).
 * Callers provide decoded block headers and bodies.
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

    /* Create EVM */
    s->evm = evm_create(s->state, config->chain_config);
    if (!s->evm) {
        fprintf(stderr, "Failed to create EVM\n");
        goto fail;
    }

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
    free(s);
    return NULL;
}

void sync_destroy(sync_t *sync) {
    if (!sync) return;

    /* Final checkpoint if we have unsaved progress */
    if (sync->blocks_fail == 0 &&
        sync->last_block > sync->last_checkpoint_block &&
        sync->config.checkpoint_path) {
#ifdef ENABLE_VERKLE
        if (sync->vs) verkle_state_sync(sync->vs);
#endif
#ifdef ENABLE_MPT
        if (sync->cs) code_store_sync(sync->cs);
#endif
        checkpoint_save_internal(sync->config.checkpoint_path,
                                 sync->last_block, sync->block_hashes,
                                 sync->total_gas, sync->blocks_ok,
                                 sync->blocks_fail);
        printf("Checkpoint saved at block %lu\n", sync->last_block);
    }

    if (sync->evm) evm_destroy(sync->evm);
    if (sync->state) evm_state_destroy(sync->state);
#ifdef ENABLE_VERKLE
    if (sync->vs) verkle_state_destroy(sync->vs);
#endif
#ifdef ENABLE_MPT
    if (sync->cs) code_store_destroy(sync->cs);
#endif

    free((char *)sync->config.verkle_value_dir);
    free((char *)sync->config.verkle_commit_dir);
    free((char *)sync->config.mpt_path);
    free((char *)sync->config.code_store_path);
    free((char *)sync->config.checkpoint_path);
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

    /* Store block hash in ring buffer */
    sync->block_hashes[bn % BLOCK_HASH_WINDOW] = *block_hash;

    /* Execute block */
    block_result_t br = block_execute(sync->evm, header, body,
                                      sync->block_hashes);

    result->gas_used = br.gas_used;
    result->tx_count = br.tx_count;

    /* Validate gas */
    bool gas_match = (br.gas_used == header->gas_used);
    result->expected_gas = header->gas_used;
    result->actual_gas   = br.gas_used;

    /* Validate state root */
    bool root_match = true;
#ifdef ENABLE_MPT
    if (sync->config.validate_state_root) {
        bool prune = (sync->evm->fork >= FORK_SPURIOUS_DRAGON);
        hash_t mpt_root = evm_state_compute_mpt_root(sync->state, prune);
        root_match = (memcmp(mpt_root.bytes, header->state_root.bytes, 32) == 0);
        result->actual_root   = mpt_root;
        result->expected_root = header->state_root;
    }
#endif

    /* Determine outcome */
    if (!gas_match) {
        result->ok    = false;
        result->error = SYNC_GAS_MISMATCH;
        sync->blocks_fail++;
    } else if (!root_match) {
        result->ok    = false;
        result->error = SYNC_ROOT_MISMATCH;
        sync->blocks_fail++;
    } else {
        result->ok    = true;
        result->error = SYNC_OK;
        sync->blocks_ok++;
    }

    sync->total_gas += br.gas_used;
    sync->last_block = bn;

    /* Handle failure: revert verkle state for the bad block */
    if (!result->ok) {
#ifdef ENABLE_VERKLE
        if (sync->vs) verkle_state_revert_block(sync->vs);
#endif
        /* Save checkpoint at the last good block */
        uint64_t last_good = bn - 1;
        if (last_good > sync->last_checkpoint_block &&
            sync->config.checkpoint_path) {
            uint64_t good_gas = sync->total_gas - br.gas_used;
#ifdef ENABLE_VERKLE
            if (sync->vs) verkle_state_sync(sync->vs);
#endif
            checkpoint_save_internal(sync->config.checkpoint_path,
                                     last_good, sync->block_hashes,
                                     good_gas, sync->blocks_ok, 0);
            sync->last_checkpoint_block = last_good;
        }
    }

    /* Auto-checkpoint on interval */
    if (result->ok &&
        sync->config.checkpoint_interval > 0 &&
        sync->blocks_fail == 0 &&
        bn - sync->last_checkpoint_block >= sync->config.checkpoint_interval) {
#ifdef ENABLE_VERKLE
        if (sync->vs) verkle_state_sync(sync->vs);
#endif
        if (sync->config.checkpoint_path) {
            checkpoint_save_internal(sync->config.checkpoint_path,
                                     bn, sync->block_hashes,
                                     sync->total_gas, sync->blocks_ok,
                                     sync->blocks_fail);
            sync->last_checkpoint_block = bn;
        }
    }

    block_result_free(&br);
    return true;
}

// ============================================================================
// Checkpoint
// ============================================================================

bool sync_checkpoint(sync_t *sync) {
    if (!sync || !sync->config.checkpoint_path) return false;

#ifdef ENABLE_VERKLE
    if (sync->vs) verkle_state_sync(sync->vs);
#endif
#ifdef ENABLE_MPT
    if (sync->cs) code_store_sync(sync->cs);
#endif

    bool ok = checkpoint_save_internal(sync->config.checkpoint_path,
                                       sync->last_block, sync->block_hashes,
                                       sync->total_gas, sync->blocks_ok,
                                       sync->blocks_fail);
    if (ok) sync->last_checkpoint_block = sync->last_block;
    return ok;
}

uint64_t sync_resumed_from(const sync_t *sync) {
    return sync ? sync->resumed_block : 0;
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
