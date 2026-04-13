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
        free(e);
        return NULL;
    }

    e->evm = evm_create(e->state, e->chain_config);
    if (!e->evm) {
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
    if (!engine || !path || engine->initialized) return false;

    /* Genesis loading requires cJSON — delegate to sync's loader.
     * For now, use the state-level genesis loader directly. */
    FILE *f = fopen(path, "r");
    if (!f) {
        engine_log(engine, RX_LOG_ERROR, "failed to open genesis file");
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

    /* Parse JSON and load accounts */
    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root) {
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
    if (!engine || !path) return false;

    state_t *st = evm_state_get_state(engine->state);
    hash_t loaded_root;
    if (!state_load(st, path, &loaded_root)) {
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
    if (!engine || !path) return false;

    state_t *st = evm_state_get_state(engine->state);

    /* Compute current root for the snapshot header */
    hash_t root = evm_state_compute_mpt_root(engine->state,
        engine->evm->fork >= FORK_SPURIOUS_DRAGON);

    if (!state_save(st, path, &root)) {
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
 * Block execution
 * ======================================================================== */

bool rx_execute_block_rlp(rx_engine_t *engine,
                          const uint8_t *header_rlp, size_t header_len,
                          const uint8_t *body_rlp, size_t body_len,
                          const rx_hash_t *block_hash,
                          rx_block_result_t *result) {
    if (!engine || !header_rlp || !body_rlp || !result)
        return false;

    memset(result, 0, sizeof(*result));

    /* Decode header */
    block_header_t header;
    if (!block_header_decode_rlp(&header, header_rlp, header_len)) {
        engine_log(engine, RX_LOG_ERROR, "failed to decode block header RLP");
        return false;
    }

    /* Decode body */
    block_body_t body;
    if (!block_body_decode_rlp(&body, body_rlp, body_len)) {
        engine_log(engine, RX_LOG_ERROR, "failed to decode block body RLP");
        return false;
    }

    /* Execute */
    block_result_t br = block_execute(engine->evm, &header, &body,
                                      engine->block_hashes
#ifdef ENABLE_HISTORY
                                      , NULL
#endif
                                      );

    /* Store block hash in ring */
    if (block_hash) {
        hash_t bh;
        memcpy(bh.bytes, block_hash->bytes, 32);
        engine->block_hashes[header.number % BLOCK_HASH_WINDOW] = bh;
    }
    engine->last_block = header.number;

    /* Fill public result */
    result->ok = br.success;
    result->gas_used = br.gas_used;
    result->tx_count = br.tx_count;
    memcpy(result->state_root.bytes, br.state_root.bytes, 32);
    memcpy(result->receipt_root.bytes, br.receipt_root.bytes, 32);
    memcpy(result->logs_bloom, br.logs_bloom, 256);

    /* Convert receipts */
    if (br.receipt_count > 0 && br.receipts) {
        result->receipts = calloc(br.receipt_count, sizeof(rx_receipt_t));
        if (result->receipts) {
            for (size_t i = 0; i < br.receipt_count; i++) {
                tx_receipt_t *src = &br.receipts[i];
                rx_receipt_t *dst = &result->receipts[i];

                dst->status = src->status_code;
                dst->tx_type = src->tx_type;
                dst->gas_used = src->gas_used;
                dst->cumulative_gas = src->cumulative_gas;
                memcpy(dst->logs_bloom, src->logs_bloom, 256);
                dst->contract_created = src->contract_created;
                if (src->contract_created)
                    memcpy(dst->contract_addr.bytes, src->contract_addr.bytes, 20);

                /* Move logs (transfer ownership) */
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

    /* Free internal result (logs were copied, not moved) */
    block_result_free(&br);
    block_body_free(&body);

    return true;
}

rx_hash_t rx_compute_state_root(rx_engine_t *engine) {
    rx_hash_t out = {0};
    if (!engine) return out;

    /* No invalidate_all needed — incremental dirty tracking is correct.
     * block_execute → finalize_block marks dirty paths, compute_root
     * only rehashes those paths. Proven over 17M+ mainnet blocks. */
    bool prune = (engine->evm->fork >= FORK_SPURIOUS_DRAGON);
    hash_t root = evm_state_compute_mpt_root(engine->state, prune);
    memcpy(out.bytes, root.bytes, 32);
    return out;
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
