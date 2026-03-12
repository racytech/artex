/**
 * Chain Tip — Orchestration between Engine API and Sync Engine.
 *
 * Two modes:
 *   1. Batch (era1 replay): delegates to sync_execute_block(), root validated
 *      at checkpoint boundaries, background MPT flush.
 *   2. Live (CL sync): validates per block via chain_tip_new_payload(),
 *      immediate root computation, synchronous flush.
 *
 * Transition: chain_tip_go_live() after era1 reaches The Merge.
 */

#include "chain_tip.h"
#include "engine_store.h"
#include "engine_types.h"
#include "block.h"
#include "block_executor.h"
#include "evm.h"
#include "evm_state.h"
#include "rlp.h"
#include "hash.h"
#include "uint256.h"
#include "mem_mpt.h"
#include "tx_decoder.h"
#include "keccak256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* =========================================================================
 * Fork choice persistence format
 * ========================================================================= */

#define TIP_MAGIC   0x50495443  /* "CTIP" little-endian */
#define TIP_VERSION 1

typedef struct {
    uint32_t magic;
    uint32_t version;
    hash_t   head_hash;
    hash_t   safe_hash;
    hash_t   finalized_hash;
    uint64_t head_number;
    uint64_t finalized_number;
    hash_t   checksum;
} tip_state_t;

/* =========================================================================
 * Internal struct
 * ========================================================================= */

struct chain_tip {
    sync_t         *sync;       /* owned */
    engine_store_t *store;      /* owned */

    /* Fork choice state */
    hash_t   head_hash;
    hash_t   safe_hash;
    hash_t   finalized_hash;
    uint64_t head_number;
    uint64_t finalized_number;
    bool     has_head;

    /* Mode */
    bool     live;              /* true after go_live() */
    uint64_t merge_block;

    /* Config */
    char    *tip_state_path;    /* owned copy */
    bool     verbose;
};

/* =========================================================================
 * Helpers
 * ========================================================================= */

static bool is_zero_hash(const uint8_t hash[32]) {
    for (int i = 0; i < 32; i++)
        if (hash[i] != 0) return false;
    return true;
}

static void compute_tip_checksum(tip_state_t *ts) {
    size_t payload = offsetof(tip_state_t, checksum);
    hash_t h = hash_keccak256((const uint8_t *)ts, payload);
    memcpy(&ts->checksum, &h, sizeof(hash_t));
}

static bool verify_tip_checksum(const tip_state_t *ts) {
    tip_state_t tmp;
    memcpy(&tmp, ts, sizeof(tmp));
    compute_tip_checksum(&tmp);
    return memcmp(&tmp.checksum, &ts->checksum, sizeof(hash_t)) == 0;
}

static bool tip_state_save(const chain_tip_t *tip) {
    if (!tip->tip_state_path) return true;  /* no persistence = ok */

    tip_state_t ts;
    memset(&ts, 0, sizeof(ts));
    ts.magic   = TIP_MAGIC;
    ts.version = TIP_VERSION;
    memcpy(&ts.head_hash,      &tip->head_hash,      32);
    memcpy(&ts.safe_hash,      &tip->safe_hash,      32);
    memcpy(&ts.finalized_hash, &tip->finalized_hash,  32);
    ts.head_number      = tip->head_number;
    ts.finalized_number = tip->finalized_number;
    compute_tip_checksum(&ts);

    /* Atomic write: tmp + rename */
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", tip->tip_state_path);

    FILE *f = fopen(tmp_path, "wb");
    if (!f) return false;

    bool ok = (fwrite(&ts, sizeof(ts), 1, f) == 1) && (fflush(f) == 0);
    fclose(f);

    if (ok) ok = (rename(tmp_path, tip->tip_state_path) == 0);
    if (!ok) unlink(tmp_path);
    return ok;
}

static bool tip_state_load(chain_tip_t *tip) {
    if (!tip->tip_state_path) return false;

    FILE *f = fopen(tip->tip_state_path, "rb");
    if (!f) return false;

    tip_state_t ts;
    bool ok = (fread(&ts, sizeof(ts), 1, f) == 1);
    fclose(f);

    if (!ok) return false;
    if (ts.magic != TIP_MAGIC || ts.version != TIP_VERSION) return false;
    if (!verify_tip_checksum(&ts)) return false;

    memcpy(&tip->head_hash,      &ts.head_hash,      32);
    memcpy(&tip->safe_hash,      &ts.safe_hash,      32);
    memcpy(&tip->finalized_hash, &ts.finalized_hash,  32);
    tip->head_number      = ts.head_number;
    tip->finalized_number = ts.finalized_number;
    tip->has_head         = !is_zero_hash(tip->head_hash.bytes);

    return true;
}

/* keccak256(RLP([])) — uncle hash for all post-merge blocks */
static const uint8_t EMPTY_UNCLE_HASH[32] = {
    0x1d, 0xcc, 0x4d, 0xe8, 0xde, 0xc7, 0x5d, 0x7a,
    0xab, 0x85, 0xb5, 0x67, 0xb6, 0xcc, 0xd4, 0x1a,
    0xd3, 0x12, 0x45, 0x1b, 0x94, 0x8a, 0x74, 0x13,
    0xf0, 0xa1, 0x42, 0xfd, 0x40, 0xd4, 0x93, 0x47
};

/* =========================================================================
 * Payload → Block conversion helpers
 * (same logic as engine_handlers.c, kept here for chain_tip ownership)
 * ========================================================================= */

static hash_t compute_tx_root_from_raw(uint8_t **transactions,
                                        const size_t *tx_lengths,
                                        size_t tx_count) {
    hash_t root = {0};

    if (tx_count == 0) {
        const uint8_t empty_rlp[] = {0x80};
        return hash_keccak256(empty_rlp, 1);
    }

    mpt_unsecured_entry_t *entries = calloc(tx_count, sizeof(*entries));
    bytes_t *keys = calloc(tx_count, sizeof(bytes_t));
    if (!entries || !keys) goto cleanup;

    for (size_t i = 0; i < tx_count; i++) {
        keys[i] = rlp_encode_uint64_direct(i);
        if (!keys[i].data) goto cleanup;
        entries[i].key       = keys[i].data;
        entries[i].key_len   = keys[i].len;
        entries[i].value     = transactions[i];
        entries[i].value_len = tx_lengths[i];
    }

    mpt_compute_root_unsecured(entries, tx_count, &root);

cleanup:
    if (keys) {
        for (size_t i = 0; i < tx_count; i++) free(keys[i].data);
        free(keys);
    }
    free(entries);
    return root;
}

static void payload_to_header(const execution_payload_t *p,
                               block_header_t *hdr,
                               engine_version_t version,
                               const hash_t *tx_root,
                               const hash_t *withdrawals_root,
                               const uint8_t *parent_beacon_root) {
    memset(hdr, 0, sizeof(*hdr));

    memcpy(hdr->parent_hash.bytes, p->parent_hash, 32);
    memcpy(hdr->uncle_hash.bytes, EMPTY_UNCLE_HASH, 32);
    memcpy(hdr->coinbase.bytes, p->fee_recipient, 20);
    memcpy(hdr->state_root.bytes, p->state_root, 32);
    if (tx_root) hash_copy(&hdr->tx_root, tx_root);
    memcpy(hdr->receipt_root.bytes, p->receipts_root, 32);
    memcpy(hdr->logs_bloom, p->logs_bloom, 256);
    hdr->number    = p->block_number;
    hdr->gas_limit = p->gas_limit;
    hdr->gas_used  = p->gas_used;
    hdr->timestamp = p->timestamp;
    if (p->extra_data_len > 0)
        memcpy(hdr->extra_data, p->extra_data, p->extra_data_len);
    hdr->extra_data_len = p->extra_data_len;
    memcpy(hdr->mix_hash.bytes, p->prev_randao, 32);
    hdr->nonce = 0;

    hdr->has_base_fee = true;
    hdr->base_fee = uint256_from_bytes(p->base_fee_per_gas, 32);

    if (version >= ENGINE_V2 && withdrawals_root) {
        hdr->has_withdrawals_root = true;
        hash_copy(&hdr->withdrawals_root, withdrawals_root);
    }

    if (version >= ENGINE_V3) {
        hdr->has_blob_gas = true;
        hdr->blob_gas_used = p->blob_gas_used;
        hdr->excess_blob_gas = p->excess_blob_gas;
    }

    if (parent_beacon_root) {
        hdr->has_parent_beacon_root = true;
        memcpy(hdr->parent_beacon_root.bytes, parent_beacon_root, 32);
    }
}

static bool payload_build_body(const execution_payload_t *p,
                                block_body_t *body) {
    memset(body, 0, sizeof(*body));

    rlp_item_t *root = rlp_list_new();
    if (!root) return false;

    rlp_item_t *tx_list = rlp_list_new();
    if (!tx_list) { rlp_item_free(root); return false; }

    for (size_t i = 0; i < p->tx_count; i++) {
        const uint8_t *raw = p->transactions[i];
        size_t len = p->tx_lengths[i];
        if (len == 0) { rlp_item_free(tx_list); rlp_item_free(root); return false; }

        if (raw[0] >= 0xc0) {
            rlp_item_t *decoded = rlp_decode(raw, len);
            if (!decoded) { rlp_item_free(tx_list); rlp_item_free(root); return false; }
            rlp_list_append(tx_list, decoded);
        } else {
            rlp_list_append(tx_list, rlp_string(raw, len));
        }
    }

    rlp_list_append(root, tx_list);
    rlp_list_append(root, rlp_list_new());

    body->_rlp = root;
    body->_tx_list_idx = 0;
    body->tx_count = p->tx_count;

    if (p->withdrawal_count > 0) {
        body->withdrawals = calloc(p->withdrawal_count, sizeof(withdrawal_t));
        if (!body->withdrawals) { rlp_item_free(root); body->_rlp = NULL; return false; }
        for (size_t i = 0; i < p->withdrawal_count; i++) {
            body->withdrawals[i].index = p->withdrawals[i].index;
            body->withdrawals[i].validator_index = p->withdrawals[i].validator_index;
            memcpy(body->withdrawals[i].address.bytes, p->withdrawals[i].address, 20);
            body->withdrawals[i].amount_gwei = p->withdrawals[i].amount;
        }
        body->withdrawal_count = p->withdrawal_count;
    }

    return true;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

chain_tip_t *chain_tip_create(const chain_tip_config_t *config) {
    if (!config) return NULL;

    chain_tip_t *tip = calloc(1, sizeof(*tip));
    if (!tip) return NULL;

    /* Create underlying sync engine */
    tip->sync = sync_create(&config->sync_config);
    if (!tip->sync) {
        free(tip);
        return NULL;
    }

    /* Create engine store for live mode block caching */
    tip->store = engine_store_create();
    if (!tip->store) {
        sync_destroy(tip->sync);
        free(tip);
        return NULL;
    }

    tip->merge_block = config->merge_block;
    tip->verbose     = config->verbose;

    if (config->tip_state_path)
        tip->tip_state_path = strdup(config->tip_state_path);

    /* Try to restore fork choice state */
    if (tip_state_load(tip) && tip->verbose)
        printf("Chain tip: restored fork choice (head=%lu)\n", tip->head_number);

    return tip;
}

void chain_tip_destroy(chain_tip_t *tip) {
    if (!tip) return;

    /* Persist fork choice */
    if (tip->has_head)
        tip_state_save(tip);

    /* Sync engine saves final checkpoint in its destroy */
    sync_destroy(tip->sync);
    engine_store_destroy(tip->store);
    free(tip->tip_state_path);
    free(tip);
}

/* =========================================================================
 * Genesis
 * ========================================================================= */

bool chain_tip_load_genesis(chain_tip_t *tip, const char *genesis_json_path,
                             const hash_t *genesis_hash) {
    if (!tip || !tip->sync) return false;
    return sync_load_genesis(tip->sync, genesis_json_path, genesis_hash);
}

/* =========================================================================
 * Batch Mode (era1 replay)
 * ========================================================================= */

bool chain_tip_execute_batch(chain_tip_t *tip,
                              const block_header_t *header,
                              const block_body_t *body,
                              const hash_t *block_hash,
                              sync_block_result_t *result) {
    if (!tip || !tip->sync) return false;
    return sync_execute_block(tip->sync, header, body, block_hash, result);
}

bool chain_tip_go_live(chain_tip_t *tip) {
    if (!tip) return false;
    if (tip->live) return true;  /* already live */

    if (tip->verbose)
        printf("Chain tip: transitioning to live CL sync mode\n");

    /* Force checkpoint to flush all state to disk */
    sync_checkpoint(tip->sync);

    /* Switch sync engine to per-block validation mode */
    sync_set_live_mode(tip->sync, true);

    tip->live = true;

    /* Set head to current sync position */
    sync_status_t st = sync_get_status(tip->sync);
    tip->head_number = st.last_block;

    if (tip->verbose)
        printf("Chain tip: live mode active at block %lu\n", tip->head_number);

    return true;
}

bool chain_tip_is_live(const chain_tip_t *tip) {
    return tip && tip->live;
}

/* =========================================================================
 * Live Mode — newPayload
 * ========================================================================= */

payload_status_t chain_tip_new_payload(chain_tip_t *tip,
                                        const execution_payload_t *payload,
                                        engine_version_t version) {
    payload_status_t status;
    memset(&status, 0, sizeof(status));

    if (!tip || !tip->sync) {
        status.status = PAYLOAD_SYNCING;
        return status;
    }

    /* Check parent is known */
    if (!is_zero_hash(payload->parent_hash) &&
        !engine_store_has(tip->store, payload->parent_hash)) {
        /* Parent unknown — accept payload, return SYNCING */
        status.status = PAYLOAD_SYNCING;
        status.has_latest_valid_hash = false;
        engine_store_put(tip->store, payload, false);
        return status;
    }

    /* Step 1: Compute tx root */
    hash_t tx_root = compute_tx_root_from_raw(
        payload->transactions, payload->tx_lengths, payload->tx_count);

    /* Step 2: Compute withdrawals root (V2+) */
    hash_t wd_root = {0};
    if (version >= ENGINE_V2) {
        withdrawal_t *wds = NULL;
        if (payload->withdrawal_count > 0) {
            wds = calloc(payload->withdrawal_count, sizeof(withdrawal_t));
            for (size_t i = 0; i < payload->withdrawal_count; i++) {
                wds[i].index = payload->withdrawals[i].index;
                wds[i].validator_index = payload->withdrawals[i].validator_index;
                memcpy(wds[i].address.bytes, payload->withdrawals[i].address, 20);
                wds[i].amount_gwei = payload->withdrawals[i].amount;
            }
        }
        wd_root = block_compute_withdrawals_root(wds, payload->withdrawal_count);
        free(wds);
    }

    /* Step 3: Build header for hash verification */
    block_header_t header;
    payload_to_header(payload, &header, version, &tx_root,
                      version >= ENGINE_V2 ? &wd_root : NULL,
                      NULL);  /* TODO: parent_beacon_root from params[2] */

    /* Step 4: Verify block hash */
    hash_t computed_hash = block_header_hash(&header);
    if (memcmp(computed_hash.bytes, payload->block_hash, 32) != 0) {
        status.status = PAYLOAD_INVALID_BLOCK_HASH;
        status.has_latest_valid_hash = false;
        status.validation_error = "blockhash mismatch";
        return status;
    }

    /* Step 5: Build block body */
    block_body_t body;
    if (!payload_build_body(payload, &body)) {
        status.status = PAYLOAD_INVALID;
        status.has_latest_valid_hash = true;
        memcpy(status.latest_valid_hash, payload->parent_hash, 32);
        status.validation_error = "failed to build block body";
        return status;
    }

    /* Step 6: Execute via sync engine (live mode = per-block root validation) */
    sync_block_result_t sync_result;
    hash_t blk_hash;
    memcpy(blk_hash.bytes, payload->block_hash, 32);

    bool executed = tip->live
        ? sync_execute_block_live(tip->sync, &header, &body,
                                   &blk_hash, &sync_result)
        : sync_execute_block(tip->sync, &header, &body,
                              &blk_hash, &sync_result);

    if (!executed || !sync_result.ok) {
        status.status = PAYLOAD_INVALID;
        status.has_latest_valid_hash = true;
        memcpy(status.latest_valid_hash, payload->parent_hash, 32);

        if (!executed)
            status.validation_error = "block execution failed";
        else if (sync_result.error == SYNC_GAS_MISMATCH)
            status.validation_error = "gas mismatch";
        else
            status.validation_error = "state root mismatch";

        block_body_free(&body);
        return status;
    }

    /* Block is valid */
    status.status = PAYLOAD_VALID;
    status.has_latest_valid_hash = true;
    memcpy(status.latest_valid_hash, payload->block_hash, 32);

    /* Store payload and record block hash */
    engine_store_put(tip->store, payload, true);
    engine_store_record_blockhash(tip->store,
                                   payload->block_number,
                                   payload->block_hash);

    /* Update head tracking */
    tip->head_number = payload->block_number;
    memcpy(tip->head_hash.bytes, payload->block_hash, 32);
    tip->has_head = true;

    block_body_free(&body);

    if (tip->verbose)
        printf("Chain tip: VALID block %lu\n", payload->block_number);

    return status;
}

/* =========================================================================
 * Live Mode — forkchoiceUpdated
 * ========================================================================= */

payload_status_t chain_tip_forkchoice_updated(chain_tip_t *tip,
                                               const forkchoice_state_t *fc,
                                               const payload_attributes_t *attrs,
                                               engine_version_t version) {
    (void)attrs;   /* TODO Phase 6: payload building */
    (void)version;

    payload_status_t status;
    memset(&status, 0, sizeof(status));

    if (!tip) {
        status.status = PAYLOAD_SYNCING;
        return status;
    }

    /* Check head is known */
    if (!engine_store_has(tip->store, fc->head_block_hash)) {
        status.status = PAYLOAD_SYNCING;
        status.has_latest_valid_hash = false;
        return status;
    }

    /* Look up the new head block */
    const engine_stored_block_t *new_head =
        engine_store_get(tip->store, fc->head_block_hash);
    if (!new_head) {
        status.status = PAYLOAD_SYNCING;
        status.has_latest_valid_hash = false;
        return status;
    }

    /* Reorg detection: is the new head a continuation of the current head?
     * If new head's parent != current head, a reorg occurred. */
    if (tip->has_head &&
        memcmp(fc->head_block_hash, tip->head_hash.bytes, 32) != 0) {

        bool is_child = (memcmp(new_head->payload.parent_hash,
                                tip->head_hash.bytes, 32) == 0);

        if (!is_child) {
            /* Reorg detected. Find common ancestor depth. */
            const engine_stored_block_t *walk = new_head;
            int depth = 0;
            bool found_ancestor = false;

            /* Walk the new chain backwards looking for the old head's parent
             * or any block on the old chain. Limit walk to engine_store. */
            while (walk && depth < 64) {
                /* Check if this block's parent is on the old chain
                 * (i.e., the old head is a sibling — depth-1 reorg) */
                if (tip->head_number > 0 &&
                    walk->payload.block_number <= tip->head_number) {
                    found_ancestor = true;
                    break;
                }
                /* Walk back via parent */
                const engine_stored_block_t *parent =
                    engine_store_get(tip->store, walk->payload.parent_hash);
                if (!parent) break;
                walk = parent;
                depth++;
            }

            if (!found_ancestor || depth > 1) {
                /* Deep reorg (>1 block) or can't find ancestor.
                 * Return SYNCING — CL will re-send blocks. */
                if (tip->verbose)
                    printf("Chain tip: deep reorg detected (depth>%d), "
                           "returning SYNCING\n", depth);
                status.status = PAYLOAD_SYNCING;
                status.has_latest_valid_hash = false;
                return status;
            }

            if (tip->verbose)
                printf("Chain tip: reorg detected (depth=%d), switching head "
                       "%lu → %lu\n", depth,
                       tip->head_number, new_head->payload.block_number);
        }
    }

    /* Update fork choice state */
    memcpy(tip->head_hash.bytes,      fc->head_block_hash,      32);
    memcpy(tip->safe_hash.bytes,      fc->safe_block_hash,      32);
    memcpy(tip->finalized_hash.bytes, fc->finalized_block_hash, 32);
    tip->has_head    = true;
    tip->head_number = new_head->payload.block_number;

    /* Update engine store fork choice (for pruning/eviction) */
    engine_store_set_forkchoice(tip->store,
                                 fc->head_block_hash,
                                 fc->safe_block_hash,
                                 fc->finalized_block_hash);
    engine_store_prune(tip->store);

    /* Look up finalized block number */
    if (!is_zero_hash(fc->finalized_block_hash)) {
        const engine_stored_block_t *fin_block =
            engine_store_get(tip->store, fc->finalized_block_hash);
        if (fin_block)
            tip->finalized_number = fin_block->payload.block_number;
    }

    /* Persist fork choice */
    tip_state_save(tip);

    status.status = PAYLOAD_VALID;
    status.has_latest_valid_hash = true;
    memcpy(status.latest_valid_hash, fc->head_block_hash, 32);

    if (tip->verbose)
        printf("Chain tip: forkchoice head=%lu finalized=%lu\n",
               tip->head_number, tip->finalized_number);

    return status;
}

/* =========================================================================
 * Accessors
 * ========================================================================= */

sync_t *chain_tip_get_sync(const chain_tip_t *tip) {
    return tip ? tip->sync : NULL;
}

engine_store_t *chain_tip_get_store(const chain_tip_t *tip) {
    return tip ? tip->store : NULL;
}

sync_status_t chain_tip_get_status(const chain_tip_t *tip) {
    if (!tip || !tip->sync) {
        sync_status_t empty = {0};
        return empty;
    }
    return sync_get_status(tip->sync);
}

bool chain_tip_checkpoint(chain_tip_t *tip) {
    if (!tip || !tip->sync) return false;
    tip_state_save(tip);
    return sync_checkpoint(tip->sync);
}

uint64_t chain_tip_head_number(const chain_tip_t *tip) {
    return tip ? tip->head_number : 0;
}

bool chain_tip_head_hash(const chain_tip_t *tip, hash_t *out) {
    if (!tip || !tip->has_head) return false;
    memcpy(out, &tip->head_hash, sizeof(hash_t));
    return true;
}

uint64_t chain_tip_finalized_number(const chain_tip_t *tip) {
    return tip ? tip->finalized_number : 0;
}
