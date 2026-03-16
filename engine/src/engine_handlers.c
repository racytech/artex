/*
 * Engine Handlers — All versions (Paris V1 → Prague V4).
 *
 * Shared core logic with version-specific wrappers for:
 *   newPayload:        V1..V4
 *   forkchoiceUpdated: V1..V3
 *   getPayload:        V1..V4
 *   exchangeCapabilities
 */

#include "engine_handlers.h"
#include "engine_rpc.h"
#include "block.h"
#include "block_executor.h"
#include "evm.h"
#include "evm_state.h"
#include "rlp.h"
#include "hash.h"
#include "uint256.h"
#include "mem_mpt.h"
#include "tx_decoder.h"
#include "sync.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =========================================================================
 * Handler Context
 * ========================================================================= */

engine_handler_ctx_t *engine_handler_ctx_create(engine_store_t *store,
                                                 struct evm *evm,
                                                 struct evm_state *state) {
    engine_handler_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    ctx->store = store;
    ctx->evm = evm;
    ctx->state = state;
    return ctx;
}

void engine_handler_ctx_destroy(engine_handler_ctx_t *ctx) {
    free(ctx);
}

void engine_handler_ctx_set_sync(engine_handler_ctx_t *ctx, struct sync *sync) {
    if (ctx) ctx->sync = sync;
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static bool is_zero_hash(const uint8_t hash[32]) {
    for (int i = 0; i < 32; i++)
        if (hash[i] != 0) return false;
    return true;
}

/* Decode a 0x-prefixed hex string into a 32-byte hash */
static bool parse_hex_hash(const char *hex, uint8_t out[32]) {
    if (!hex) return false;
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) hex += 2;
    if (strlen(hex) != 64) return false;
    for (int i = 0; i < 32; i++) {
        int hi = (hex[i*2] >= 'a')   ? hex[i*2]-'a'+10 :
                 (hex[i*2] >= 'A')   ? hex[i*2]-'A'+10 :
                 hex[i*2]-'0';
        int lo = (hex[i*2+1] >= 'a') ? hex[i*2+1]-'a'+10 :
                 (hex[i*2+1] >= 'A') ? hex[i*2+1]-'A'+10 :
                 hex[i*2+1]-'0';
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return true;
}

/* =========================================================================
 * Block Validation Helpers
 * ========================================================================= */

/* keccak256(RLP([])) — always the uncle hash for post-merge blocks */
static const uint8_t EMPTY_UNCLE_HASH[32] = {
    0x1d, 0xcc, 0x4d, 0xe8, 0xde, 0xc7, 0x5d, 0x7a,
    0xab, 0x85, 0xb5, 0x67, 0xb6, 0xcc, 0xd4, 0x1a,
    0xd3, 0x12, 0x45, 0x1b, 0x94, 0x8a, 0x74, 0x13,
    0xf0, 0xa1, 0x42, 0xfd, 0x40, 0xd4, 0x93, 0x47
};

/**
 * Compute the transaction trie root directly from raw tx bytes.
 * Keys are RLP(index), values are raw tx bytes as-is (correct for both
 * legacy and typed transactions).
 */
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

/**
 * Convert execution_payload_t → block_header_t for hash verification.
 * Fills in computed tx_root and withdrawals_root from the provided hashes.
 */
static void payload_to_header(const execution_payload_t *p,
                               block_header_t *hdr,
                               engine_version_t version,
                               const hash_t *tx_root,
                               const hash_t *withdrawals_root,
                               const uint8_t *parent_beacon_root,
                               const hash_t *requests_hash) {
    memset(hdr, 0, sizeof(*hdr));

    memcpy(hdr->parent_hash.bytes, p->parent_hash, 32);
    memcpy(hdr->uncle_hash.bytes, EMPTY_UNCLE_HASH, 32);
    memcpy(hdr->coinbase.bytes, p->fee_recipient, 20);
    memcpy(hdr->state_root.bytes, p->state_root, 32);
    if (tx_root) hash_copy(&hdr->tx_root, tx_root);
    memcpy(hdr->receipt_root.bytes, p->receipts_root, 32);
    memcpy(hdr->logs_bloom, p->logs_bloom, 256);
    /* difficulty = 0 (post-merge) */
    hdr->number = p->block_number;
    hdr->gas_limit = p->gas_limit;
    hdr->gas_used = p->gas_used;
    hdr->timestamp = p->timestamp;
    if (p->extra_data_len > 0)
        memcpy(hdr->extra_data, p->extra_data, p->extra_data_len);
    hdr->extra_data_len = p->extra_data_len;
    memcpy(hdr->mix_hash.bytes, p->prev_randao, 32);  /* prevRandao post-merge */
    hdr->nonce = 0;  /* zero nonce post-merge */

    /* Base fee (always present post-merge) */
    hdr->has_base_fee = true;
    hdr->base_fee = uint256_from_bytes(p->base_fee_per_gas, 32);

    /* V2+: withdrawals root */
    if (version >= ENGINE_V2 && withdrawals_root) {
        hdr->has_withdrawals_root = true;
        hash_copy(&hdr->withdrawals_root, withdrawals_root);
    }

    /* V3+: blob gas fields */
    if (version >= ENGINE_V3) {
        hdr->has_blob_gas = true;
        hdr->blob_gas_used = p->blob_gas_used;
        hdr->excess_blob_gas = p->excess_blob_gas;
    }

    /* V3+: parent beacon block root */
    if (parent_beacon_root) {
        hdr->has_parent_beacon_root = true;
        memcpy(hdr->parent_beacon_root.bytes, parent_beacon_root, 32);
    }

    /* V4+: requests hash (EIP-7685) */
    if (requests_hash) {
        hdr->has_requests_hash = true;
        hash_copy(&hdr->requests_hash, requests_hash);
    }
}

/**
 * Build a block_body_t from execution_payload_t transactions.
 * Constructs the RLP tree expected by block_body_tx() and block_execute().
 * Caller must call block_body_free() when done.
 */
static bool payload_build_body(const execution_payload_t *p,
                                block_body_t *body) {
    memset(body, 0, sizeof(*body));

    /* Build RLP tree: [tx_list, uncle_list] */
    rlp_item_t *root = rlp_list_new();
    if (!root) return false;

    rlp_item_t *tx_list = rlp_list_new();
    if (!tx_list) { rlp_item_free(root); return false; }

    for (size_t i = 0; i < p->tx_count; i++) {
        const uint8_t *raw = p->transactions[i];
        size_t len = p->tx_lengths[i];

        if (len == 0) {
            rlp_item_free(tx_list);
            rlp_item_free(root);
            return false;
        }

        if (raw[0] >= 0xc0) {
            /* Legacy tx: raw bytes are RLP-encoded list */
            rlp_item_t *decoded = rlp_decode(raw, len);
            if (!decoded) {
                rlp_item_free(tx_list);
                rlp_item_free(root);
                return false;
            }
            rlp_list_append(tx_list, decoded);
        } else {
            /* Typed tx (EIP-2718): raw bytes = type || RLP(payload) */
            rlp_list_append(tx_list, rlp_string(raw, len));
        }
    }

    rlp_list_append(root, tx_list);
    rlp_list_append(root, rlp_list_new()); /* empty uncle list */

    body->_rlp = root;
    body->_tx_list_idx = 0;
    body->tx_count = p->tx_count;

    /* Convert engine_withdrawal_t → withdrawal_t */
    if (p->withdrawal_count > 0) {
        body->withdrawals = calloc(p->withdrawal_count, sizeof(withdrawal_t));
        if (!body->withdrawals) {
            rlp_item_free(root);
            body->_rlp = NULL;
            return false;
        }
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
 * newPayload — Shared Core
 *
 * V1: params[0] = ExecutionPayloadV1
 * V2: params[0] = ExecutionPayloadV2
 * V3: params[0] = ExecutionPayloadV3, params[1] = blobHashes, params[2] = beaconRoot
 * V4: params[0] = ExecutionPayloadV3, params[1] = blobHashes, params[2] = beaconRoot, params[3] = requests
 * ========================================================================= */

static cJSON *new_payload_common(const cJSON *params, void *ctx_ptr,
                                  engine_version_t version,
                                  int *err_code, const char **err_msg) {
    engine_handler_ctx_t *ctx = (engine_handler_ctx_t *)ctx_ptr;
    if (!ctx || !ctx->store) {
        *err_code = -32603;
        *err_msg = "no handler context";
        return NULL;
    }

    if (!params || !cJSON_IsArray(params) || cJSON_GetArraySize(params) < 1) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "expected params array with ExecutionPayload";
        return NULL;
    }

    /* V3+ requires 3 params, V4 requires 4 */
    if (version >= ENGINE_V3 && cJSON_GetArraySize(params) < 3) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "V3/V4 requires expectedBlobVersionedHashes and parentBeaconBlockRoot";
        return NULL;
    }

    /* Parse the execution payload with version-appropriate fields */
    cJSON *payload_json = cJSON_GetArrayItem(params, 0);
    execution_payload_t payload;
    if (!execution_payload_from_json_v(payload_json, &payload, version)) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "invalid execution payload";
        return NULL;
    }

    /* V4: parse execution requests from params[3] */
    if (version >= ENGINE_V4 && cJSON_GetArraySize(params) >= 4) {
        cJSON *reqs_json = cJSON_GetArrayItem(params, 3);
        if (reqs_json && cJSON_IsArray(reqs_json)) {
            payload.request_count = (size_t)cJSON_GetArraySize(reqs_json);
            if (payload.request_count > 0) {
                payload.requests = calloc(payload.request_count, sizeof(uint8_t *));
                payload.request_lengths = calloc(payload.request_count, sizeof(size_t));
                /* Parse each request as hex DATA */
                size_t idx = 0;
                cJSON *req;
                cJSON_ArrayForEach(req, reqs_json) {
                    if (cJSON_IsString(req)) {
                        const char *hex = req->valuestring;
                        if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
                            hex += 2;
                        size_t hex_len = strlen(hex);
                        size_t byte_len = hex_len / 2;
                        payload.requests[idx] = malloc(byte_len > 0 ? byte_len : 1);
                        if (payload.requests[idx] && byte_len > 0) {
                            for (size_t j = 0; j < byte_len; j++) {
                                int hi = (hex[j*2] >= 'a') ? hex[j*2]-'a'+10 :
                                         (hex[j*2] >= 'A') ? hex[j*2]-'A'+10 :
                                         hex[j*2]-'0';
                                int lo = (hex[j*2+1] >= 'a') ? hex[j*2+1]-'a'+10 :
                                         (hex[j*2+1] >= 'A') ? hex[j*2+1]-'A'+10 :
                                         hex[j*2+1]-'0';
                                payload.requests[idx][j] = (uint8_t)((hi << 4) | lo);
                            }
                        }
                        payload.request_lengths[idx] = byte_len;
                    }
                    idx++;
                }
            }
        }
    }

    /* V3+: parse parentBeaconBlockRoot from params[2] */
    uint8_t parent_beacon_root[32];
    bool has_parent_beacon_root = false;
    if (version >= ENGINE_V3) {
        cJSON *beacon_json = cJSON_GetArrayItem(params, 2);
        if (beacon_json && cJSON_IsString(beacon_json))
            has_parent_beacon_root = parse_hex_hash(beacon_json->valuestring,
                                                     parent_beacon_root);
    }

    /* V3+: Validate expectedBlobVersionedHashes from params[1] */
    if (version >= ENGINE_V3) {
        cJSON *blob_json = cJSON_GetArrayItem(params, 1);
        if (!blob_json || !cJSON_IsArray(blob_json)) {
            *err_code = RPC_ERR_INVALID_PARAMS;
            *err_msg = "missing expectedBlobVersionedHashes array";
            execution_payload_free(&payload);
            return NULL;
        }

        /* Parse expected hashes from params[1] */
        size_t expected_count = (size_t)cJSON_GetArraySize(blob_json);
        hash_t *expected_hashes = NULL;
        if (expected_count > 0) {
            expected_hashes = calloc(expected_count, sizeof(hash_t));
            size_t idx = 0;
            cJSON *item;
            cJSON_ArrayForEach(item, blob_json) {
                if (!cJSON_IsString(item) ||
                    !parse_hex_hash(item->valuestring, expected_hashes[idx].bytes)) {
                    free(expected_hashes);
                    *err_code = RPC_ERR_INVALID_PARAMS;
                    *err_msg = "invalid blob versioned hash";
                    execution_payload_free(&payload);
                    return NULL;
                }
                idx++;
            }
        }

        /* Collect actual blob hashes from type-3 transactions */
        size_t actual_count = 0;
        hash_t *actual_hashes = NULL;
        size_t actual_cap = 0;
        uint64_t chain_id = ctx->evm && ((evm_t *)ctx->evm)->chain_config
                          ? ((evm_t *)ctx->evm)->chain_config->chain_id : 1;

        for (size_t i = 0; i < payload.tx_count; i++) {
            /* Quick check: type-3 tx starts with byte 0x03 */
            if (payload.tx_lengths[i] == 0 || payload.transactions[i][0] != 0x03)
                continue;

            transaction_t tx;
            memset(&tx, 0, sizeof(tx));
            if (!tx_decode_raw(&tx, payload.transactions[i],
                               payload.tx_lengths[i], chain_id)) {
                tx_decoded_free(&tx);
                continue;
            }

            for (size_t j = 0; j < tx.blob_versioned_hashes_count; j++) {
                if (actual_count >= actual_cap) {
                    actual_cap = actual_cap ? actual_cap * 2 : 8;
                    actual_hashes = realloc(actual_hashes, actual_cap * sizeof(hash_t));
                }
                memcpy(actual_hashes[actual_count].bytes,
                       tx.blob_versioned_hashes[j].bytes, 32);
                actual_count++;
            }
            tx_decoded_free(&tx);
        }

        /* Compare expected vs actual */
        bool blob_match = (expected_count == actual_count);
        if (blob_match) {
            for (size_t i = 0; i < expected_count; i++) {
                if (memcmp(expected_hashes[i].bytes,
                           actual_hashes[i].bytes, 32) != 0) {
                    blob_match = false;
                    break;
                }
            }
        }
        free(expected_hashes);
        free(actual_hashes);

        if (!blob_match) {
            payload_status_t inv_status;
            memset(&inv_status, 0, sizeof(inv_status));
            inv_status.status = PAYLOAD_INVALID;
            inv_status.has_latest_valid_hash = true;
            memcpy(inv_status.latest_valid_hash, payload.parent_hash, 32);
            inv_status.validation_error = "invalid blob versioned hashes";
            execution_payload_free(&payload);
            return payload_status_to_json(&inv_status);
        }
    }

    payload_status_t status;
    memset(&status, 0, sizeof(status));

    /* Check parent */
    if (!is_zero_hash(payload.parent_hash) &&
        !engine_store_has(ctx->store, payload.parent_hash)) {
        status.status = PAYLOAD_SYNCING;
        status.has_latest_valid_hash = false;
        if (!engine_store_put(ctx->store, &payload, false)) {
            *err_code = -32603;
            *err_msg = "engine store full";
            execution_payload_free(&payload);
            return NULL;
        }
        execution_payload_free(&payload);
    } else if (ctx->sync || ctx->evm) {
        /* ----------------------------------------------------------------
         * Full validation mode — verify hash, execute block, compare state
         * ---------------------------------------------------------------- */

        /* Step 1: Compute tx root from raw transaction bytes */
        hash_t tx_root = compute_tx_root_from_raw(
            payload.transactions, payload.tx_lengths, payload.tx_count);

        /* Step 2: Compute withdrawals root (V2+) */
        hash_t wd_root = {0};
        if (version >= ENGINE_V2) {
            withdrawal_t *wds = NULL;
            if (payload.withdrawal_count > 0) {
                wds = calloc(payload.withdrawal_count, sizeof(withdrawal_t));
                for (size_t i = 0; i < payload.withdrawal_count; i++) {
                    wds[i].index = payload.withdrawals[i].index;
                    wds[i].validator_index = payload.withdrawals[i].validator_index;
                    memcpy(wds[i].address.bytes, payload.withdrawals[i].address, 20);
                    wds[i].amount_gwei = payload.withdrawals[i].amount;
                }
            }
            wd_root = block_compute_withdrawals_root(wds, payload.withdrawal_count);
            free(wds);
        }

        /* Step 2b: Compute requests hash (V4+, EIP-7685) */
        hash_t req_hash = {0};
        bool has_req_hash = false;
        if (version >= ENGINE_V4) {
            req_hash = block_compute_requests_hash(
                (const uint8_t *const *)payload.requests,
                payload.request_lengths,
                payload.request_count);
            has_req_hash = true;
        }

        /* Step 3: Build header for hash verification */
        block_header_t header;
        payload_to_header(&payload, &header, version, &tx_root,
                          version >= ENGINE_V2 ? &wd_root : NULL,
                          has_parent_beacon_root ? parent_beacon_root : NULL,
                          has_req_hash ? &req_hash : NULL);

        /* Step 4: Verify block hash */
        hash_t computed_hash = block_header_hash(&header);
        if (memcmp(computed_hash.bytes, payload.block_hash, 32) != 0) {
            status.status = PAYLOAD_INVALID_BLOCK_HASH;
            status.has_latest_valid_hash = false;
            status.validation_error = "blockhash mismatch";
            execution_payload_free(&payload);
            return payload_status_to_json(&status);
        }

        /* Step 5: Build block body for execution */
        block_body_t body;
        if (!payload_build_body(&payload, &body)) {
            *err_code = -32603;
            *err_msg = "failed to build block body";
            execution_payload_free(&payload);
            return NULL;
        }

        if (ctx->sync) {
            /* ----------------------------------------------------------
             * Sync mode — delegate to sync_execute_block_live()
             * Handles: execution, gas/root validation, checkpointing,
             *          block hash ring buffer, MPT flush
             * ---------------------------------------------------------- */
            hash_t blk_hash;
            memcpy(blk_hash.bytes, payload.block_hash, 32);

            sync_block_result_t sync_result;
            if (!sync_execute_block_live((sync_t *)ctx->sync, &header, &body,
                                         &blk_hash, &sync_result)) {
                status.status = PAYLOAD_INVALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.parent_hash, 32);
                status.validation_error = "block execution failed";
            } else if (!sync_result.ok) {
                status.status = PAYLOAD_INVALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.parent_hash, 32);
                if (sync_result.error == SYNC_GAS_MISMATCH)
                    status.validation_error = "invalid gas used";
                else if (sync_result.error == SYNC_ROOT_MISMATCH)
                    status.validation_error = "invalid state root";
                else
                    status.validation_error = "validation failed";
            } else {
                status.status = PAYLOAD_VALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.block_hash, 32);

                if (!engine_store_put(ctx->store, &payload, true)) {
                    *err_code = -32603;
                    *err_msg = "engine store full";
                    block_body_free(&body);
                    execution_payload_free(&payload);
                    return NULL;
                }
                engine_store_record_blockhash(ctx->store,
                                               payload.block_number,
                                               payload.block_hash);
            }

            block_body_free(&body);

            if (status.status == PAYLOAD_INVALID) {
                if (!engine_store_put(ctx->store, &payload, false))
                    fprintf(stderr, "WARN: failed to store invalid payload (store full)\n");
            }
            execution_payload_free(&payload);
        } else {
            /* ----------------------------------------------------------
             * Standalone mode — direct block_execute() (engine_server)
             * ---------------------------------------------------------- */

            /* Step 6: Populate block hashes from store ring buffer */
            hash_t block_hashes[256];
            memset(block_hashes, 0, sizeof(block_hashes));
            {
                uint64_t lo = payload.block_number > 256
                              ? payload.block_number - 256 : 0;
                for (uint64_t bn = lo; bn < payload.block_number; bn++) {
                    uint8_t h[32];
                    if (engine_store_get_blockhash(ctx->store, bn, h))
                        memcpy(block_hashes[bn % 256].bytes, h, 32);
                }
            }

            /* Step 7: Execute block */
            block_result_t result = block_execute(
                (evm_t *)ctx->evm, &header, &body, block_hashes
#ifdef ENABLE_HISTORY
                , NULL
#endif
#ifdef ENABLE_VERKLE_BUILD
                , NULL
#endif
                );

            /* Step 8: Compute state root (MPT path returns zero from block_execute) */
#ifdef ENABLE_MPT
            if (result.success && ctx->state) {
                evm_t *e = (evm_t *)ctx->evm;
                bool prune = (e->fork >= FORK_SPURIOUS_DRAGON);
                result.state_root = evm_state_compute_mpt_root(
                    (evm_state_t *)ctx->state, prune);
            }
#endif

            /* Step 9: Validate results */
            if (!result.success) {
                status.status = PAYLOAD_INVALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.parent_hash, 32);
                status.validation_error = "block execution failed";
            } else if (memcmp(result.state_root.bytes, payload.state_root, 32) != 0) {
                status.status = PAYLOAD_INVALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.parent_hash, 32);
                status.validation_error = "invalid state root";
            } else if (result.gas_used != payload.gas_used) {
                status.status = PAYLOAD_INVALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.parent_hash, 32);
                status.validation_error = "invalid gas used";
            } else if (memcmp(result.receipt_root.bytes, payload.receipts_root, 32) != 0) {
                status.status = PAYLOAD_INVALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.parent_hash, 32);
                status.validation_error = "invalid receipt root";
            } else if (memcmp(result.logs_bloom, payload.logs_bloom, 256) != 0) {
                status.status = PAYLOAD_INVALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.parent_hash, 32);
                status.validation_error = "invalid logs bloom";
            } else {
                /* Block is valid */
                status.status = PAYLOAD_VALID;
                status.has_latest_valid_hash = true;
                memcpy(status.latest_valid_hash, payload.block_hash, 32);

                if (!engine_store_put(ctx->store, &payload, true)) {
                    *err_code = -32603;
                    *err_msg = "engine store full";
                    block_result_free(&result);
                    block_body_free(&body);
                    execution_payload_free(&payload);
                    return NULL;
                }
                engine_store_record_blockhash(ctx->store,
                                               payload.block_number,
                                               payload.block_hash);
            }

            block_result_free(&result);
            block_body_free(&body);

            /* If INVALID, store but mark as invalid (best-effort cache) */
            if (status.status == PAYLOAD_INVALID) {
                if (!engine_store_put(ctx->store, &payload, false))
                    fprintf(stderr, "WARN: failed to store invalid payload (store full)\n");
            }
            execution_payload_free(&payload);
        }
    } else {
        /* Stub mode (no EVM) — store without execution */
        status.status = PAYLOAD_VALID;
        status.has_latest_valid_hash = true;
        memcpy(status.latest_valid_hash, payload.block_hash, 32);

        if (!engine_store_put(ctx->store, &payload, true)) {
            *err_code = -32603;
            *err_msg = "engine store full";
            execution_payload_free(&payload);
            return NULL;
        }
        engine_store_record_blockhash(ctx->store,
                                       payload.block_number,
                                       payload.block_hash);
        execution_payload_free(&payload);
    }

    return payload_status_to_json(&status);
}

/* newPayload version wrappers */
cJSON *engine_newPayloadV1(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V1, err_code, err_msg);
}

cJSON *engine_newPayloadV2(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V2, err_code, err_msg);
}

cJSON *engine_newPayloadV3(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V3, err_code, err_msg);
}

cJSON *engine_newPayloadV4(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V4, err_code, err_msg);
}

/* =========================================================================
 * forkchoiceUpdated — Shared Core
 *
 * V1: params[0] = ForkchoiceState, params[1] = PayloadAttributesV1 | null
 * V2: params[0] = ForkchoiceState, params[1] = PayloadAttributesV1|V2 | null
 * V3: params[0] = ForkchoiceState, params[1] = PayloadAttributesV3 | null
 * ========================================================================= */

static cJSON *forkchoice_updated_common(const cJSON *params, void *ctx_ptr,
                                         engine_version_t version,
                                         int *err_code, const char **err_msg) {
    engine_handler_ctx_t *ctx = (engine_handler_ctx_t *)ctx_ptr;
    if (!ctx || !ctx->store) {
        *err_code = -32603;
        *err_msg = "no handler context";
        return NULL;
    }

    if (!params || !cJSON_IsArray(params) || cJSON_GetArraySize(params) < 1) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "expected params array with ForkchoiceState";
        return NULL;
    }

    cJSON *fc_json = cJSON_GetArrayItem(params, 0);
    forkchoice_state_t fc;
    if (!forkchoice_state_from_json(fc_json, &fc)) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "invalid forkchoice state";
        return NULL;
    }

    payload_status_t ps;
    memset(&ps, 0, sizeof(ps));

    if (!engine_store_has(ctx->store, fc.head_block_hash)) {
        ps.status = PAYLOAD_SYNCING;
        ps.has_latest_valid_hash = false;
    } else {
        engine_store_set_forkchoice(ctx->store,
                                     fc.head_block_hash,
                                     fc.safe_block_hash,
                                     fc.finalized_block_hash);
        engine_store_prune(ctx->store);

        ps.status = PAYLOAD_VALID;
        ps.has_latest_valid_hash = true;
        memcpy(ps.latest_valid_hash, fc.head_block_hash, 32);
    }

    cJSON *result = cJSON_CreateObject();
    cJSON_AddItemToObject(result, "payloadStatus", payload_status_to_json(&ps));

    /* Handle optional PayloadAttributes */
    cJSON *attr_json = cJSON_GetArrayItem(params, 1);
    if (attr_json && !cJSON_IsNull(attr_json) && ps.status == PAYLOAD_VALID) {
        payload_attributes_t attrs;
        if (payload_attributes_from_json_v(attr_json, &attrs, version)) {
            uint64_t payload_id = attrs.timestamp ^ 0xDEADBEEF;
            char id_hex[19];
            snprintf(id_hex, sizeof(id_hex), "0x%016llx",
                     (unsigned long long)payload_id);
            cJSON_AddStringToObject(result, "payloadId", id_hex);
            payload_attributes_free(&attrs);
        } else {
            cJSON_AddNullToObject(result, "payloadId");
        }
    } else {
        cJSON_AddNullToObject(result, "payloadId");
    }

    return result;
}

/* forkchoiceUpdated version wrappers */
cJSON *engine_forkchoiceUpdatedV1(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return forkchoice_updated_common(params, ctx, ENGINE_V1, err_code, err_msg);
}

cJSON *engine_forkchoiceUpdatedV2(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return forkchoice_updated_common(params, ctx, ENGINE_V2, err_code, err_msg);
}

cJSON *engine_forkchoiceUpdatedV3(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return forkchoice_updated_common(params, ctx, ENGINE_V3, err_code, err_msg);
}

/* =========================================================================
 * getPayload — Shared Core
 *
 * V1: returns ExecutionPayloadV1
 * V2: returns {executionPayload, blockValue}
 * V3: returns {executionPayload, blockValue, blobsBundle, shouldOverrideBuilder}
 * V4: returns V3 + executionRequests
 * ========================================================================= */

static cJSON *get_payload_common(const cJSON *params, void *ctx_ptr,
                                  engine_version_t version,
                                  int *err_code, const char **err_msg) {
    engine_handler_ctx_t *ctx = (engine_handler_ctx_t *)ctx_ptr;
    if (!ctx || !ctx->store) {
        *err_code = -32603;
        *err_msg = "no handler context";
        return NULL;
    }

    if (!params || !cJSON_IsArray(params) || cJSON_GetArraySize(params) < 1) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "expected params array with payloadId";
        return NULL;
    }

    cJSON *pid = cJSON_GetArrayItem(params, 0);
    if (!pid || !cJSON_IsString(pid)) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "payloadId must be a hex string";
        return NULL;
    }

    const char *hex = pid->valuestring;
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
        hex += 2;
    uint64_t payload_id = strtoull(hex, NULL, 16);

    execution_payload_t payload;
    if (!engine_store_take_pending(ctx->store, payload_id, &payload)) {
        *err_code = ENGINE_ERR_UNKNOWN_PAYLOAD;
        *err_msg = "unknown payload";
        return NULL;
    }

    cJSON *result;

    if (version == ENGINE_V1) {
        /* V1: return just the ExecutionPayload */
        result = execution_payload_to_json_v(&payload, ENGINE_V1);
    } else {
        /* V2+: return wrapped object */
        result = cJSON_CreateObject();
        cJSON_AddItemToObject(result, "executionPayload",
                              execution_payload_to_json_v(&payload, version));
        cJSON_AddStringToObject(result, "blockValue", "0x0");

        if (version >= ENGINE_V3) {
            /* Blobs bundle (empty for non-blob blocks) */
            cJSON *blobs = cJSON_CreateObject();
            cJSON_AddItemToObject(blobs, "commitments", cJSON_CreateArray());
            cJSON_AddItemToObject(blobs, "proofs", cJSON_CreateArray());
            cJSON_AddItemToObject(blobs, "blobs", cJSON_CreateArray());
            cJSON_AddItemToObject(result, "blobsBundle", blobs);
            cJSON_AddBoolToObject(result, "shouldOverrideBuilder", false);
        }

        if (version >= ENGINE_V4) {
            /* Execution requests */
            cJSON *reqs = cJSON_AddArrayToObject(result, "executionRequests");
            (void)reqs; /* empty for now */
        }
    }

    execution_payload_free(&payload);
    return result;
}

/* getPayload version wrappers */
cJSON *engine_getPayloadV1(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V1, err_code, err_msg);
}

cJSON *engine_getPayloadV2(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V2, err_code, err_msg);
}

cJSON *engine_getPayloadV3(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V3, err_code, err_msg);
}

cJSON *engine_getPayloadV4(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V4, err_code, err_msg);
}

/* =========================================================================
 * exchangeCapabilities
 * ========================================================================= */

cJSON *engine_exchangeCapabilities(const cJSON *params, const cJSON *id,
                                    void *ctx_ptr, int *err_code,
                                    const char **err_msg) {
    (void)params; (void)id; (void)ctx_ptr;
    (void)err_code; (void)err_msg;

    cJSON *result = cJSON_CreateArray();
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV1"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV2"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV3"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV4"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_forkchoiceUpdatedV1"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_forkchoiceUpdatedV2"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_forkchoiceUpdatedV3"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV1"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV2"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV3"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV4"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_exchangeCapabilities"));
    return result;
}

/* =========================================================================
 * Registration — All Methods
 * ========================================================================= */

void engine_register_handlers(engine_rpc_t *rpc) {
    engine_rpc_register(rpc, "engine_newPayloadV1", engine_newPayloadV1);
    engine_rpc_register(rpc, "engine_newPayloadV2", engine_newPayloadV2);
    engine_rpc_register(rpc, "engine_newPayloadV3", engine_newPayloadV3);
    engine_rpc_register(rpc, "engine_newPayloadV4", engine_newPayloadV4);
    engine_rpc_register(rpc, "engine_forkchoiceUpdatedV1", engine_forkchoiceUpdatedV1);
    engine_rpc_register(rpc, "engine_forkchoiceUpdatedV2", engine_forkchoiceUpdatedV2);
    engine_rpc_register(rpc, "engine_forkchoiceUpdatedV3", engine_forkchoiceUpdatedV3);
    engine_rpc_register(rpc, "engine_getPayloadV1", engine_getPayloadV1);
    engine_rpc_register(rpc, "engine_getPayloadV2", engine_getPayloadV2);
    engine_rpc_register(rpc, "engine_getPayloadV3", engine_getPayloadV3);
    engine_rpc_register(rpc, "engine_getPayloadV4", engine_getPayloadV4);
    engine_rpc_register(rpc, "engine_exchangeCapabilities", engine_exchangeCapabilities);
}
