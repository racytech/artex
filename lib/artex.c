/**
 * artex.c — Implementation of the public rx_ API.
 *
 * Thin wrappers over internal types. rx_engine_t owns:
 *   - evm_state_t (account/storage state)
 *   - evm_t (EVM interpreter)
 *   - block hash ring (last 256 hashes for BLOCKHASH opcode)
 *   - logger callback
 */

#include "artex.h"
#include "evm.h"
#include "evm_state.h"
#include "state.h"
#include "block.h"
#include "block_executor.h"
#include "fork.h"
#include "hash.h"
#include "uint256.h"
#include "address.h"
#include "code_store.h"

#include <cjson/cJSON.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ========================================================================
 * Internal engine struct
 * ======================================================================== */

#define BLOCK_HASH_WINDOW 256

struct rx_engine {
    evm_state_t            *state;
    evm_t                  *evm;
    code_store_t           *cs;           /* contract bytecode store (optional) */
    const chain_config_t   *chain_config;
    hash_t                  block_hashes[BLOCK_HASH_WINDOW];
    uint64_t                last_block;
    bool                    initialized;  /* genesis or snapshot loaded */

    /* Logger */
    rx_log_fn               log_fn;
    void                   *log_userdata;

    /* Last error */
    rx_error_t              last_error;
    char                    last_error_msg[256];
};

/* rx_state_t is just evm_state_t behind an opaque pointer */
struct rx_state {
    evm_state_t *es;
};

/* ========================================================================
 * Internal helpers
 * ======================================================================== */

static void engine_log(rx_engine_t *e, rx_log_level_t level, const char *msg) {
    if (e && e->log_fn)
        e->log_fn(level, msg, e->log_userdata);
}

static void engine_set_error(rx_engine_t *e, rx_error_t err, const char *msg) {
    if (!e) return;
    e->last_error = err;
    if (msg)
        snprintf(e->last_error_msg, sizeof(e->last_error_msg), "%s", msg);
    else
        e->last_error_msg[0] = '\0';
}

static void engine_clear_error(rx_engine_t *e) {
    if (!e) return;
    e->last_error = RX_OK;
    e->last_error_msg[0] = '\0';
}

/* ========================================================================
 * Error reporting
 * ======================================================================== */

const char *rx_error_string(rx_error_t err) {
    switch (err) {
        case RX_OK:               return "no error";
        case RX_ERR_NULL_ARG:     return "required argument was NULL";
        case RX_ERR_INVALID_CONFIG: return "invalid configuration";
        case RX_ERR_OUT_OF_MEMORY:  return "out of memory";
        case RX_ERR_ALREADY_INIT:   return "already initialized";
        case RX_ERR_NOT_INIT:       return "not initialized";
        case RX_ERR_FILE_IO:        return "file I/O error";
        case RX_ERR_PARSE:          return "parse error";
        case RX_ERR_DECODE:         return "decode error";
        case RX_ERR_EXECUTION:      return "execution error";
        case RX_ERR_BLOCK_NOT_FOUND: return "block not found in hash window";
        default:                    return "unknown error";
    }
}

rx_error_t rx_engine_last_error(const rx_engine_t *engine) {
    if (!engine) return RX_ERR_NULL_ARG;
    return engine->last_error;
}

const char *rx_engine_last_error_msg(const rx_engine_t *engine) {
    if (!engine) return "";
    return engine->last_error_msg;
}

/* ========================================================================
 * Engine lifecycle
 * ======================================================================== */

rx_engine_t *rx_engine_create(const rx_config_t *config) {
    if (!config) return NULL;
    if (config->chain_id != RX_CHAIN_MAINNET) return NULL;

    rx_engine_t *e = calloc(1, sizeof(rx_engine_t));
    if (!e) return NULL;

    e->chain_config = chain_config_mainnet();

    /* Open code store if data_dir provided */
    if (config->data_dir) {
        char cs_path[1024];
        snprintf(cs_path, sizeof(cs_path), "%s/code_store", config->data_dir);
        e->cs = code_store_open(cs_path);
        if (!e->cs)
            e->cs = code_store_create(cs_path, 500000);
    }

    e->state = evm_state_create(e->cs);
    if (!e->state) {
        if (e->cs) code_store_destroy(e->cs);
        engine_set_error(e, RX_ERR_OUT_OF_MEMORY, "failed to create evm_state");
        free(e);
        return NULL;
    }

    e->evm = evm_create(e->state, e->chain_config);
    if (!e->evm) {
        engine_set_error(e, RX_ERR_OUT_OF_MEMORY, "failed to create evm");
        evm_state_destroy(e->state);
        free(e);
        return NULL;
    }

    return e;
}

void rx_engine_destroy(rx_engine_t *engine) {
    if (!engine) return;
    if (engine->evm) evm_destroy(engine->evm);
    if (engine->state) evm_state_destroy(engine->state);
    if (engine->cs) code_store_destroy(engine->cs);
    free(engine);
}

/* ========================================================================
 * Genesis
 * ======================================================================== */

bool rx_engine_load_genesis(rx_engine_t *engine, const char *path,
                            const rx_hash_t *genesis_hash) {
    if (!engine || !path) {
        if (engine) engine_set_error(engine, RX_ERR_NULL_ARG, "path is NULL");
        return false;
    }
    if (engine->initialized) {
        engine_set_error(engine, RX_ERR_ALREADY_INIT, "genesis or state already loaded");
        return false;
    }
    engine_clear_error(engine);

    FILE *f = fopen(path, "r");
    if (!f) {
        engine_set_error(engine, RX_ERR_FILE_IO, "failed to open genesis file");
        engine_log(engine, RX_LOG_ERROR, "failed to open genesis file");
        return false;
    }

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);

    char *json_str = malloc(fsize + 1);
    if (!json_str) {
        fclose(f);
        engine_set_error(engine, RX_ERR_OUT_OF_MEMORY, "malloc failed for genesis JSON");
        return false;
    }
    size_t nread = fread(json_str, 1, fsize, f);
    (void)nread;
    json_str[fsize] = '\0';
    fclose(f);

    /* Parse JSON and load accounts */
    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root) {
        engine_set_error(engine, RX_ERR_PARSE, "genesis JSON parse error");
        engine_log(engine, RX_LOG_ERROR, "genesis JSON parse error");
        return false;
    }

    cJSON *entry;
    cJSON_ArrayForEach(entry, root) {
        const char *addr_hex = entry->string;
        if (!addr_hex) continue;

        address_t addr;
        if (!address_from_hex(addr_hex, &addr)) continue;

        cJSON *bal_item = cJSON_GetObjectItem(entry, "balance");
        if (!bal_item || !cJSON_IsString(bal_item)) continue;

        uint256_t balance = uint256_from_hex(bal_item->valuestring);
        if (!uint256_is_zero(&balance))
            evm_state_add_balance(engine->state, &addr, &balance);
        else
            evm_state_create_account(engine->state, &addr);
    }
    cJSON_Delete(root);
    bool ok = true;

    if (!ok) return false;

    /* Commit genesis state */
    evm_state_begin_block(engine->state, 0);
    evm_state_commit(engine->state);
    evm_state_finalize(engine->state);
    evm_state_compute_mpt_root(engine->state, false);

    if (genesis_hash)
        memcpy(engine->block_hashes[0].bytes, genesis_hash->bytes, 32);

    engine->initialized = true;
    return true;
}

/* ========================================================================
 * State save/load
 * ======================================================================== */

bool rx_engine_load_state(rx_engine_t *engine, const char *path) {
    if (!engine || !path) {
        if (engine) engine_set_error(engine, RX_ERR_NULL_ARG, "path is NULL");
        return false;
    }
    engine_clear_error(engine);

    state_t *st = evm_state_get_state(engine->state);
    hash_t loaded_root;
    if (!state_load(st, path, &loaded_root)) {
        engine_set_error(engine, RX_ERR_FILE_IO, "failed to load state snapshot");
        engine_log(engine, RX_LOG_ERROR, "failed to load state snapshot");
        return false;
    }

    engine->last_block = state_get_block(st);

    /* Load block hash ring from .hashes file if present */
    char hashes_path[1024];
    snprintf(hashes_path, sizeof(hashes_path), "%s.hashes", path);
    FILE *hf = fopen(hashes_path, "rb");
    if (hf) {
        size_t nr = fread(engine->block_hashes, 32, BLOCK_HASH_WINDOW, hf);
        (void)nr;
        fclose(hf);
    }

    engine->initialized = true;
    return true;
}

bool rx_engine_save_state(rx_engine_t *engine, const char *path) {
    if (!engine || !path) {
        if (engine) engine_set_error(engine, RX_ERR_NULL_ARG, "path is NULL");
        return false;
    }
    engine_clear_error(engine);

    state_t *st = evm_state_get_state(engine->state);

    /* Compute current root for the snapshot header */
    hash_t root = evm_state_compute_mpt_root(engine->state,
        engine->evm->fork >= FORK_SPURIOUS_DRAGON);

    if (!state_save(st, path, &root)) {
        engine_set_error(engine, RX_ERR_FILE_IO, "failed to save state snapshot");
        engine_log(engine, RX_LOG_ERROR, "failed to save state snapshot");
        return false;
    }

    /* Save block hash ring alongside */
    char hashes_path[1024];
    snprintf(hashes_path, sizeof(hashes_path), "%s.hashes", path);
    FILE *hf = fopen(hashes_path, "wb");
    if (hf) {
        fwrite(engine->block_hashes, 32, BLOCK_HASH_WINDOW, hf);
        fclose(hf);
    }

    return true;
}

/* ========================================================================
 * Block execution — helpers
 * ======================================================================== */

/** Convert internal block_result_t → public rx_block_result_t. */
static void fill_block_result(rx_block_result_t *result, const block_result_t *br) {
    result->ok = br->success;
    result->gas_used = br->gas_used;
    result->tx_count = br->tx_count;
    memcpy(result->state_root.bytes, br->state_root.bytes, 32);
    memcpy(result->receipt_root.bytes, br->receipt_root.bytes, 32);
    memcpy(result->logs_bloom, br->logs_bloom, 256);

    if (br->receipt_count > 0 && br->receipts) {
        result->receipts = calloc(br->receipt_count, sizeof(rx_receipt_t));
        if (result->receipts) {
            for (size_t i = 0; i < br->receipt_count; i++) {
                tx_receipt_t *src = &br->receipts[i];
                rx_receipt_t *dst = &result->receipts[i];

                dst->status = src->status_code;
                dst->tx_type = src->tx_type;
                dst->gas_used = src->gas_used;
                dst->cumulative_gas = src->cumulative_gas;
                memcpy(dst->logs_bloom, src->logs_bloom, 256);
                dst->contract_created = src->contract_created;
                if (src->contract_created)
                    memcpy(dst->contract_addr.bytes, src->contract_addr.bytes, 20);

                if (src->log_count > 0 && src->logs) {
                    dst->logs = calloc(src->log_count, sizeof(rx_log_t));
                    dst->log_count = src->log_count;
                    if (dst->logs) {
                        for (size_t j = 0; j < src->log_count; j++) {
                            evm_log_t *sl = &src->logs[j];
                            rx_log_t *dl = &dst->logs[j];
                            memcpy(dl->address.bytes, sl->address.bytes, 20);
                            dl->topic_count = sl->topic_count;
                            if (sl->topic_count > 0) {
                                dl->topics = calloc(sl->topic_count, sizeof(rx_hash_t));
                                if (dl->topics) {
                                    for (uint8_t t = 0; t < sl->topic_count; t++)
                                        memcpy(dl->topics[t].bytes, sl->topics[t].bytes, 32);
                                }
                            }
                            if (sl->data_len > 0 && sl->data) {
                                dl->data = malloc(sl->data_len);
                                if (dl->data) {
                                    memcpy(dl->data, sl->data, sl->data_len);
                                    dl->data_len = sl->data_len;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/** Convert public rx_block_header_t → internal block_header_t. */
static void convert_header(block_header_t *dst, const rx_block_header_t *src) {
    memcpy(dst->parent_hash.bytes, src->parent_hash.bytes, 32);
    memcpy(dst->uncle_hash.bytes, src->uncle_hash.bytes, 32);
    memcpy(dst->coinbase.bytes, src->coinbase.bytes, 20);
    memcpy(dst->state_root.bytes, src->state_root.bytes, 32);
    memcpy(dst->tx_root.bytes, src->tx_root.bytes, 32);
    memcpy(dst->receipt_root.bytes, src->receipt_root.bytes, 32);
    memcpy(dst->logs_bloom, src->logs_bloom, 256);
    dst->difficulty = uint256_from_bytes(src->difficulty.bytes, 32);
    dst->number = src->number;
    dst->gas_limit = src->gas_limit;
    dst->gas_used = src->gas_used;
    dst->timestamp = src->timestamp;
    memcpy(dst->extra_data, src->extra_data, 32);
    dst->extra_data_len = src->extra_data_len;
    memcpy(dst->mix_hash.bytes, src->mix_hash.bytes, 32);
    dst->nonce = src->nonce;
    dst->has_base_fee = src->has_base_fee;
    if (src->has_base_fee)
        dst->base_fee = uint256_from_bytes(src->base_fee.bytes, 32);
    dst->has_withdrawals_root = src->has_withdrawals_root;
    if (src->has_withdrawals_root)
        memcpy(dst->withdrawals_root.bytes, src->withdrawals_root.bytes, 32);
    dst->has_blob_gas = src->has_blob_gas;
    dst->blob_gas_used = src->blob_gas_used;
    dst->excess_blob_gas = src->excess_blob_gas;
    dst->has_parent_beacon_root = src->has_parent_beacon_root;
    if (src->has_parent_beacon_root)
        memcpy(dst->parent_beacon_root.bytes, src->parent_beacon_root.bytes, 32);
    dst->has_requests_hash = src->has_requests_hash;
    if (src->has_requests_hash)
        memcpy(dst->requests_hash.bytes, src->requests_hash.bytes, 32);
}

/** Build internal block_body_t from public rx_block_body_t. */
static bool convert_body(block_body_t *dst, const rx_block_body_t *src) {
    memset(dst, 0, sizeof(*dst));

    /* Build RLP transaction list from raw tx bytes */
    rlp_item_t *root = rlp_list_new();
    rlp_item_t *tx_list = rlp_list_new();

    for (size_t i = 0; i < src->tx_count; i++) {
        const uint8_t *raw = src->transactions[i];
        size_t len = src->tx_lengths[i];

        if (len > 0 && raw[0] >= 0xc0) {
            /* Legacy: raw bytes are RLP-encoded list */
            rlp_item_t *decoded = rlp_decode(raw, len);
            if (decoded)
                rlp_list_append(tx_list, decoded);
        } else {
            /* Typed tx (EIP-2718): type || RLP_payload */
            rlp_list_append(tx_list, rlp_string(raw, len));
        }
    }

    rlp_list_append(root, tx_list);
    rlp_list_append(root, rlp_list_new()); /* empty uncle list */

    dst->_rlp = root;
    dst->_tx_list_idx = 0;
    dst->tx_count = src->tx_count;

    /* Convert withdrawals */
    if (src->withdrawal_count > 0 && src->withdrawals) {
        dst->withdrawals = calloc(src->withdrawal_count, sizeof(withdrawal_t));
        if (!dst->withdrawals) return false;
        dst->withdrawal_count = src->withdrawal_count;
        for (size_t i = 0; i < src->withdrawal_count; i++) {
            dst->withdrawals[i].index = src->withdrawals[i].index;
            dst->withdrawals[i].validator_index = src->withdrawals[i].validator_index;
            memcpy(dst->withdrawals[i].address.bytes,
                   src->withdrawals[i].address.bytes, 20);
            dst->withdrawals[i].amount_gwei = src->withdrawals[i].amount_gwei;
        }
    }

    return true;
}

/* ========================================================================
 * Block execution
 * ======================================================================== */

/** Shared execution logic: run block_execute, fill result, update ring. */
static bool execute_block_internal(rx_engine_t *engine,
                                   block_header_t *header,
                                   block_body_t *body,
                                   const rx_hash_t *block_hash,
                                   rx_block_result_t *result) {
    engine->evm->keep_undo = true;

    block_result_t br = block_execute(engine->evm, header, body,
                                      engine->block_hashes
#ifdef ENABLE_HISTORY
                                      , NULL
#endif
                                      );

    /* Store block hash in ring */
    if (block_hash) {
        hash_t bh;
        memcpy(bh.bytes, block_hash->bytes, 32);
        engine->block_hashes[header->number % BLOCK_HASH_WINDOW] = bh;
    }
    engine->last_block = header->number;

    fill_block_result(result, &br);

    block_result_free(&br);
    block_body_free(body);

    return true;
}

bool rx_execute_block_rlp(rx_engine_t *engine,
                          const uint8_t *header_rlp, size_t header_len,
                          const uint8_t *body_rlp, size_t body_len,
                          const rx_hash_t *block_hash,
                          rx_block_result_t *result) {
    if (!engine || !header_rlp || !body_rlp || !result) {
        if (engine) engine_set_error(engine, RX_ERR_NULL_ARG, "required argument is NULL");
        return false;
    }
    engine_clear_error(engine);
    memset(result, 0, sizeof(*result));

    block_header_t header;
    if (!block_header_decode_rlp(&header, header_rlp, header_len)) {
        engine_set_error(engine, RX_ERR_DECODE, "failed to decode block header RLP");
        engine_log(engine, RX_LOG_ERROR, "failed to decode block header RLP");
        return false;
    }

    block_body_t body;
    if (!block_body_decode_rlp(&body, body_rlp, body_len)) {
        engine_set_error(engine, RX_ERR_DECODE, "failed to decode block body RLP");
        engine_log(engine, RX_LOG_ERROR, "failed to decode block body RLP");
        return false;
    }

    return execute_block_internal(engine, &header, &body, block_hash, result);
}

bool rx_execute_block(rx_engine_t *engine,
                      const rx_block_header_t *header,
                      const rx_block_body_t *body,
                      const rx_hash_t *block_hash,
                      rx_block_result_t *result) {
    if (!engine || !header || !body || !result) {
        if (engine) engine_set_error(engine, RX_ERR_NULL_ARG, "required argument is NULL");
        return false;
    }
    engine_clear_error(engine);
    memset(result, 0, sizeof(*result));

    block_header_t int_header;
    convert_header(&int_header, header);

    block_body_t int_body;
    if (!convert_body(&int_body, body)) {
        engine_set_error(engine, RX_ERR_OUT_OF_MEMORY, "failed to build block body");
        return false;
    }

    return execute_block_internal(engine, &int_header, &int_body, block_hash, result);
}

rx_hash_t rx_compute_state_root(rx_engine_t *engine) {
    rx_hash_t out = {0};
    if (!engine) return out;

    /* No invalidate_all needed — incremental dirty tracking is correct.
     * block_execute → finalize_block marks dirty paths, compute_root
     * only rehashes those paths. Proven over 17M+ mainnet blocks. */
    bool prune = (engine->evm->fork >= FORK_SPURIOUS_DRAGON);
    hash_t root = evm_state_compute_root_only(engine->state, prune);
    memcpy(out.bytes, root.bytes, 32);
    return out;
}

/* ========================================================================
 * Block commit / revert
 * ======================================================================== */

bool rx_commit_block(rx_engine_t *engine) {
    if (!engine) return false;
    engine_clear_error(engine);
    evm_state_reset_block(engine->state);
    return true;
}

bool rx_revert_block(rx_engine_t *engine) {
    if (!engine) return false;
    engine_clear_error(engine);

    if (!evm_state_revert_block(engine->state)) {
        engine_set_error(engine, RX_ERR_EXECUTION, "block revert failed");
        return false;
    }

    /* Roll back block number and hash ring entry */
    if (engine->last_block > 0) {
        uint64_t idx = engine->last_block % BLOCK_HASH_WINDOW;
        memset(engine->block_hashes[idx].bytes, 0, 32);
        engine->last_block--;
    }

    return true;
}

/* ========================================================================
 * State queries
 * ======================================================================== */

/* Static state wrapper — avoids allocation per query */
static __thread struct rx_state tls_state;

rx_state_t *rx_engine_get_state(rx_engine_t *engine) {
    if (!engine) return NULL;
    tls_state.es = engine->state;
    return &tls_state;
}

bool rx_account_exists(rx_state_t *state, const rx_address_t *addr) {
    if (!state || !addr) return false;
    return evm_state_exists(state->es, (const address_t *)addr);
}

uint64_t rx_get_nonce(rx_state_t *state, const rx_address_t *addr) {
    if (!state || !addr) return 0;
    return evm_state_get_nonce(state->es, (const address_t *)addr);
}

rx_uint256_t rx_get_balance(rx_state_t *state, const rx_address_t *addr) {
    rx_uint256_t out = {0};
    if (!state || !addr) return out;
    uint256_t bal = evm_state_get_balance(state->es, (const address_t *)addr);
    /* Convert uint256_t (low/high uint128) to big-endian bytes */
    uint256_to_bytes(&bal, out.bytes);
    return out;
}

rx_hash_t rx_get_code_hash(rx_state_t *state, const rx_address_t *addr) {
    rx_hash_t out = {0};
    if (!state || !addr) return out;
    hash_t h = evm_state_get_code_hash(state->es, (const address_t *)addr);
    memcpy(out.bytes, h.bytes, 32);
    return out;
}

uint32_t rx_get_code_size(rx_state_t *state, const rx_address_t *addr) {
    if (!state || !addr) return 0;
    return evm_state_get_code_size(state->es, (const address_t *)addr);
}

uint32_t rx_get_code(rx_state_t *state, const rx_address_t *addr,
                     uint8_t *buf, uint32_t buf_len) {
    if (!state || !addr) return 0;
    uint32_t len = 0;
    const uint8_t *code = evm_state_get_code_ptr(state->es,
                                                  (const address_t *)addr, &len);
    if (!code || len == 0) return 0;
    if (buf && buf_len >= len)
        memcpy(buf, code, len);
    return len;
}

rx_uint256_t rx_get_storage(rx_state_t *state, const rx_address_t *addr,
                            const rx_uint256_t *key) {
    rx_uint256_t out = {0};
    if (!state || !addr || !key) return out;
    /* Convert big-endian key bytes to uint256_t */
    uint256_t k = uint256_from_bytes(key->bytes, 32);
    uint256_t val = evm_state_get_storage(state->es, (const address_t *)addr, &k);
    uint256_to_bytes(&val, out.bytes);
    return out;
}

/* ========================================================================
 * Status
 * ======================================================================== */

uint64_t rx_get_block_number(const rx_engine_t *engine) {
    if (!engine) return 0;
    return engine->last_block;
}

/* ========================================================================
 * Logging
 * ======================================================================== */

void rx_set_logger(rx_engine_t *engine, rx_log_fn fn, void *userdata) {
    if (!engine) return;
    engine->log_fn = fn;
    engine->log_userdata = userdata;
}

/* ========================================================================
 * Block hash query
 * ======================================================================== */

bool rx_get_block_hash(const rx_engine_t *engine, uint64_t block_number,
                       rx_hash_t *out) {
    if (!engine || !out) return false;
    if (block_number == 0 || block_number > engine->last_block)
        return false;
    if (engine->last_block - block_number >= BLOCK_HASH_WINDOW)
        return false;

    uint64_t idx = block_number % BLOCK_HASH_WINDOW;
    memcpy(out->bytes, engine->block_hashes[idx].bytes, 32);
    return true;
}

/* ========================================================================
 * Result cleanup
 * ======================================================================== */

void rx_block_result_free(rx_block_result_t *result) {
    if (!result) return;
    if (result->receipts) {
        for (size_t i = 0; i < result->tx_count; i++) {
            rx_receipt_t *r = &result->receipts[i];
            for (size_t j = 0; j < r->log_count; j++) {
                free(r->logs[j].topics);
                free(r->logs[j].data);
            }
            free(r->logs);
        }
        free(result->receipts);
        result->receipts = NULL;
    }
}

/* ========================================================================
 * Version
 * ======================================================================== */

const char *rx_version(void) {
    return "0.1.0";
}
