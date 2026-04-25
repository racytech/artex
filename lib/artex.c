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
#include "storage_hart2.h"
#ifdef ENABLE_HISTORY
#include "state_history.h"
#endif

#include <cjson/cJSON.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/resource.h>
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
#ifdef ENABLE_HISTORY
    state_history_t        *history;      /* per-block diff log; NULL if disabled */
    uint32_t                replay_root_interval; /* periodic compute_root cadence
                                                    during rx_engine_replay_history_to;
                                                    UINT32_MAX disables (one walk at end) */
#endif

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

/** Minimum stack size for EVM execution (1024-deep call chains). */
#define RX_MIN_STACK_SIZE (32UL * 1024 * 1024)

rx_engine_t *rx_engine_create(const rx_config_t *config) {
    if (!config) return NULL;
    if (config->chain_id != RX_CHAIN_MAINNET) return NULL;

    /* Fail fast if the calling thread's stack is too small for
     * worst-case EVM depth (1024 nested CALL opcodes ≈ 32 MB).
     * A warning here used to be easy to miss and left users
     * walking toward a stack overflow 1000 blocks in. */
    struct rlimit rl;
    if (getrlimit(RLIMIT_STACK, &rl) == 0 && rl.rlim_cur < RX_MIN_STACK_SIZE) {
        fprintf(stderr,
            "ERROR: thread stack size is %luMB, need >= %luMB\n"
            "  EVM supports 1024-deep call chains which require ~32MB of C stack.\n"
            "  Fix: ulimit -s 32768   (shell)\n"
            "   or: resource.setrlimit(resource.RLIMIT_STACK, (32<<20, hard))   (Python)\n"
            "   or: setrlimit()/pthread_attr_setstacksize()   (C/cgo)\n",
            (unsigned long)(rl.rlim_cur / (1024 * 1024)),
            (unsigned long)(RX_MIN_STACK_SIZE / (1024 * 1024)));
        return NULL;
    }

    rx_engine_t *e = calloc(1, sizeof(rx_engine_t));
    if (!e) return NULL;

    e->chain_config = chain_config_mainnet();

    /* Open code store if data_dir provided */
    if (config->data_dir) {
        char cs_path[1024];
        snprintf(cs_path, sizeof(cs_path), "%s/chain_replay_code", config->data_dir);
        e->cs = code_store_open(cs_path);
        if (!e->cs)
            e->cs = code_store_create(cs_path, 3000000);
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

#ifdef ENABLE_HISTORY
    /* Resolve history_dir: NULL=off, ""=auto (data_dir/history), else explicit.
     * Failure to open the history dir is non-fatal — the engine runs without
     * it and the caller can detect via rx_history_range() returning false. */
    if (config->history_dir) {
        const char *hd = config->history_dir;
        char auto_buf[1024];
        if (hd[0] == '\0') {
            if (!config->data_dir) {
                fprintf(stderr,
                    "WARNING: history_dir=\"\" (auto) requires data_dir to be set; history disabled\n");
                hd = NULL;
            } else {
                snprintf(auto_buf, sizeof(auto_buf), "%s/history", config->data_dir);
                hd = auto_buf;
            }
        }
        if (hd) {
            e->history = state_history_create(hd);
            if (!e->history) {
                fprintf(stderr,
                    "WARNING: failed to open history dir %s; history disabled\n", hd);
            }
        }
    }
    /* 0 = use default (64). Caller passes UINT32_MAX to disable periodic compute. */
    e->replay_root_interval = config->replay_root_interval ?
                              config->replay_root_interval : 64;
#else
    if (config->history_dir && config->history_dir[0] != '\0') {
        fprintf(stderr,
            "WARNING: history_dir set but libartex was built without ENABLE_HISTORY; ignored\n");
    }
#endif

    return e;
}

void rx_engine_destroy(rx_engine_t *engine) {
    if (!engine) return;
#ifdef ENABLE_HISTORY
    /* Flush consumer thread + close files BEFORE destroying evm_state,
     * since the consumer holds refs into the ring's diff slots. */
    if (engine->history) state_history_destroy(engine->history);
#endif
    if (engine->evm) evm_destroy(engine->evm);
    if (engine->state) evm_state_destroy(engine->state);
    if (engine->cs) code_store_destroy(engine->cs);
    free(engine);
}

/* ========================================================================
 * Genesis
 * ======================================================================== */

/** Finalize genesis: commit state, compute root, store genesis hash. */
static void genesis_finalize(rx_engine_t *engine, const rx_hash_t *genesis_hash) {
    evm_state_begin_block(engine->state, 0);
    evm_state_commit(engine->state);
    evm_state_finalize(engine->state);
    evm_state_compute_mpt_root(engine->state, false);

    if (genesis_hash)
        memcpy(engine->block_hashes[0].bytes, genesis_hash->bytes, 32);

    engine->initialized = true;
}

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

    genesis_finalize(engine, genesis_hash);
    return true;
}

bool rx_engine_load_genesis_alloc(rx_engine_t *engine,
                                   const rx_genesis_account_t *accounts,
                                   size_t count,
                                   const rx_hash_t *genesis_hash) {
    if (!engine) return false;
    if (!accounts && count > 0) {
        engine_set_error(engine, RX_ERR_NULL_ARG, "accounts is NULL");
        return false;
    }
    if (engine->initialized) {
        engine_set_error(engine, RX_ERR_ALREADY_INIT, "genesis or state already loaded");
        return false;
    }
    engine_clear_error(engine);

    for (size_t i = 0; i < count; i++) {
        const rx_genesis_account_t *ga = &accounts[i];
        const address_t *addr = (const address_t *)&ga->address;

        /* Balance */
        uint256_t balance = uint256_from_bytes(ga->balance.bytes, 32);
        if (!uint256_is_zero(&balance))
            evm_state_add_balance(engine->state, addr, &balance);
        else
            evm_state_create_account(engine->state, addr);

        /* Nonce */
        if (ga->nonce > 0)
            evm_state_set_nonce(engine->state, addr, ga->nonce);

        /* Code */
        if (ga->code && ga->code_len > 0)
            evm_state_set_code(engine->state, addr, ga->code, ga->code_len);

        /* Storage */
        for (size_t s = 0; s < ga->storage_count; s++) {
            uint256_t key = uint256_from_bytes(ga->storage[s].key.bytes, 32);
            uint256_t val = uint256_from_bytes(ga->storage[s].value.bytes, 32);
            evm_state_set_storage(engine->state, addr, &key, &val);
        }
    }

    genesis_finalize(engine, genesis_hash);
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
    hash_t loaded_root;  /* read from header by state_load; discarded here */
    if (!state_load(st, path, &loaded_root)) {
        engine_set_error(engine, RX_ERR_FILE_IO, "failed to load state snapshot");
        engine_log(engine, RX_LOG_ERROR, "failed to load state snapshot");
        return false;
    }

    engine->last_block = state_get_block(st);

    /* Snapshot integrity verification is intentionally NOT performed here.
     * A full re-hash of the trie is ~35 min on mainnet-scale state and is
     * redundant for the recovery flow (load → replay diffs → verify once).
     * Callers who want trust-but-verify behavior should call
     * rx_compute_state_root() after load and compare against whatever
     * authoritative root they have (canonical header root, snapshot header
     * root if they re-read it, etc.). Every node is born dirty out of
     * state_load so the next compute walks the whole trie automatically. */

    /* Load block hash ring from the mandatory .hashes sidecar.
     * A snapshot without .hashes cannot safely execute new blocks —
     * the BLOCKHASH opcode would return zero for anything in the
     * last-256 window and silently corrupt any contract that depends
     * on it. We refuse to load rather than set a trap. Generate with
     * tools/make_hashes.py if the file is missing. */
    char hashes_path[1024];
    snprintf(hashes_path, sizeof(hashes_path), "%s.hashes", path);
    FILE *hf = fopen(hashes_path, "rb");
    if (!hf) {
        char msg[1280];
        snprintf(msg, sizeof(msg),
                 "missing %s — a .hashes sidecar is required "
                 "(use tools/make_hashes.py to generate one from era files)",
                 hashes_path);
        engine_set_error(engine, RX_ERR_FILE_IO, msg);
        engine_log(engine, RX_LOG_ERROR, msg);
        return false;
    }
    size_t nr = fread(engine->block_hashes, 32, BLOCK_HASH_WINDOW, hf);
    fclose(hf);
    if (nr != BLOCK_HASH_WINDOW) {
        char msg[1280];
        snprintf(msg, sizeof(msg),
                 "short read on %s: got %zu/%u hashes (file corrupted?)",
                 hashes_path, nr, BLOCK_HASH_WINDOW);
        engine_set_error(engine, RX_ERR_FILE_IO, msg);
        engine_log(engine, RX_LOG_ERROR, msg);
        return false;
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

    /* Compute current root for the snapshot header.
     *
     * Uses cached hashes for clean subtrees. Callers who suspect
     * cached-hash drift (e.g. after a long no-validation replay
     * window) should invoke rx_invalidate_state_cache() beforehand
     * to force a full recompute. */
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
        /* Wrap all transactions as RLP strings — tx_decode_rlp handles
         * both legacy (byte[0] >= 0xc0) and typed (byte[0] < 0x80)
         * when presented as RLP_TYPE_STRING. */
        rlp_list_append(tx_list, rlp_string(raw, len));
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
                                   bool compute_root,
                                   rx_block_result_t *result) {
    engine->evm->skip_root_hash = !compute_root;

    block_result_t br = block_execute(engine->evm, header, body,
                                      engine->block_hashes
#ifdef ENABLE_HISTORY
                                      , engine->history
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
                          bool compute_root,
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

    return execute_block_internal(engine, &header, &body, block_hash, compute_root, result);
}

bool rx_execute_block(rx_engine_t *engine,
                      const rx_block_header_t *header,
                      const rx_block_body_t *body,
                      const rx_hash_t *block_hash,
                      bool compute_root,
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

    return execute_block_internal(engine, &int_header, &int_body, block_hash, compute_root, result);
}

rx_hash_t rx_compute_state_root(rx_engine_t *engine) {
    rx_hash_t out = {0};
    if (!engine) return out;

    /* No invalidate_all needed — incremental dirty tracking is correct.
     * block_execute → finalize_block marks dirty paths, compute_root
     * only rehashes those paths. Proven over 17M+ mainnet blocks.
     *
     * Prune policy is derived from last_block rather than evm->fork:
     * fork is only updated by evm_set_block_env (per-block execution),
     * so it's stale/FRONTIER immediately after a bare rx_engine_load_state.
     * 2_675_000 is EIP-161 activation (Spurious Dragon) on mainnet. */
    bool prune = (engine->last_block >= 2675000);
    hash_t root = evm_state_compute_root_only(engine->state, prune);
    memcpy(out.bytes, root.bytes, 32);
    return out;
}

void rx_invalidate_state_cache(rx_engine_t *engine) {
    if (!engine) return;
    evm_state_invalidate_all(engine->state);
}

rx_hash_t rx_compute_state_root_full(rx_engine_t *engine) {
    rx_hash_t out = {0};
    if (!engine) return out;
    /* Two walks today: invalidate every cached hash, then recompute.
     * Per the header TODO this will collapse into a single walk once
     * the MPT helper grows a "force full" gate-bypass mode. */
    evm_state_invalidate_all(engine->state);
    return rx_compute_state_root(engine);
}

bool rx_engine_history_range(const rx_engine_t *engine,
                              uint64_t *first_block,
                              uint64_t *last_block) {
    if (!engine) return false;
#ifdef ENABLE_HISTORY
    if (!engine->history) return false;
    return state_history_range(engine->history, first_block, last_block);
#else
    (void)first_block; (void)last_block;
    return false;
#endif
}

bool rx_engine_replay_history_to(rx_engine_t *engine, uint64_t target_block) {
    if (!engine) return false;
    engine_clear_error(engine);
#ifdef ENABLE_HISTORY
    if (!engine->initialized) {
        engine_set_error(engine, RX_ERR_NULL_ARG,
                         "engine state not loaded; call rx_engine_load_state first");
        return false;
    }
    if (!engine->history) {
        engine_set_error(engine, RX_ERR_NULL_ARG,
                         "history disabled: set rx_config_t.history_dir at create time");
        return false;
    }
    if (target_block <= engine->last_block) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "target_block %lu must be > last_block %lu",
                 target_block, engine->last_block);
        engine_set_error(engine, RX_ERR_NULL_ARG, msg);
        return false;
    }

    /* EIP-161 pruning must be enabled for commit_tx (called inside
     * apply_diff) to prune touched-empty accounts. Normal block execution
     * sets this per-block in block_executor's pre_block_init; replay has
     * no equivalent entry point. Mainnet activated EIP-161 at block
     * 2,675,000 — any replay window past that point wants pruning on. */
    bool prune_empty = engine->last_block >= 2675000;
    evm_state_set_prune_empty(engine->state, prune_empty);

    /* UINT32_MAX → user opted out of periodic compute; pass 0 to the
     * underlying replay so it skips the per-interval compute_root and
     * the trailing flush, matching the historical behaviour. */
    uint32_t interval = engine->replay_root_interval == UINT32_MAX ?
                        0 : engine->replay_root_interval;

    uint64_t first = engine->last_block + 1;
    uint64_t want = target_block - first + 1;
    uint64_t got = state_history_replay_ex(engine->history, engine->state,
                                           first, target_block,
                                           interval, prune_empty);
    if (got != want) {
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "history replay incomplete: applied %lu/%lu blocks (range %lu..%lu)",
                 got, want, first, target_block);
        engine_set_error(engine, RX_ERR_FILE_IO, msg);
        engine_log(engine, RX_LOG_ERROR, msg);
        return false;
    }

    /* Defensive reset of the per-block originals log (blk_orig_acct /
     * blk_orig_stor). The current fast replay path does not populate these,
     * but an earlier code version did — and failed to clear them between
     * blocks. The first execute() call after replay then captured every
     * accumulated touch as a single bogus diff for that block. That
     * malformed history record stalled later replays trying to apply a
     * ~2M-slot synthetic diff. Call state_reset_block unconditionally here
     * so a future regression in any apply path can't repeat that scenario.
     *
     * Why:         historic diff-corruption bug after replay→execute handoff
     * How to apply: any code path that hands state off from replay to
     *               live execute should flush per-block undo state first */
    evm_state_reset_block(engine->state);

    engine->last_block = target_block;
    return true;
#else
    (void)target_block;
    engine_set_error(engine, RX_ERR_NULL_ARG,
                     "libartex built without ENABLE_HISTORY; replay unavailable");
    return false;
#endif
}

bool rx_engine_truncate_history(rx_engine_t *engine, uint64_t last_block) {
    if (!engine) return false;
    engine_clear_error(engine);
#ifdef ENABLE_HISTORY
    if (!engine->history) {
        engine_set_error(engine, RX_ERR_NULL_ARG,
                         "history disabled: set rx_config_t.history_dir at create time");
        return false;
    }
    state_history_truncate(engine->history, last_block);
    return true;
#else
    (void)last_block;
    engine_set_error(engine, RX_ERR_NULL_ARG,
                     "libartex built without ENABLE_HISTORY; truncate unavailable");
    return false;
#endif
}

/* ========================================================================
 * Block production
 * ======================================================================== */

/** Convert public rx_build_header_t → internal block_header_t.
 *  Leaves tx_root / state_root / receipts_root / withdrawals_root /
 *  logs_bloom / gas_used zero — the build pipeline fills them. */
static void fill_header_from_build(block_header_t *dst,
                                   const rx_build_header_t *src) {
    memset(dst, 0, sizeof(*dst));
    memcpy(dst->parent_hash.bytes, src->parent_hash.bytes, 32);
    /* uncle_hash is EMPTY_UNCLE_HASH = keccak256(rlp([])) post-merge.
     * block_header_encode_rlp will still include whatever we set,
     * so use the canonical empty-list hash: */
    const uint8_t EMPTY_UNCLE[32] = {
        0x1d,0xcc,0x4d,0xe8,0xde,0xc7,0x5d,0x7a,0xab,0x85,0xb5,0x67,
        0xb6,0xcc,0xd4,0x1a,0xd3,0x12,0x45,0x1b,0x94,0x8a,0x74,0x13,
        0xf0,0xa1,0x42,0xfd,0x40,0xd4,0x93,0x47
    };
    memcpy(dst->uncle_hash.bytes, EMPTY_UNCLE, 32);
    memcpy(dst->coinbase.bytes, src->coinbase.bytes, 20);
    dst->number = src->number;
    dst->gas_limit = src->gas_limit;
    dst->timestamp = src->timestamp;
    memcpy(dst->extra_data, src->extra_data, 32);
    dst->extra_data_len = src->extra_data_len;
    memcpy(dst->mix_hash.bytes, src->prev_randao.bytes, 32);
    dst->nonce = 0;
    dst->difficulty = UINT256_ZERO;

    dst->has_base_fee = src->has_base_fee;
    if (src->has_base_fee)
        dst->base_fee = uint256_from_bytes(src->base_fee.bytes, 32);

    dst->has_blob_gas = src->has_blob_gas;
    dst->blob_gas_used = src->blob_gas_used;
    dst->excess_blob_gas = src->excess_blob_gas;

    dst->has_parent_beacon_root = src->has_parent_beacon_root;
    if (src->has_parent_beacon_root)
        memcpy(dst->parent_beacon_root.bytes,
               src->parent_beacon_root.bytes, 32);

    dst->has_requests_hash = src->has_requests_hash;
    if (src->has_requests_hash)
        memcpy(dst->requests_hash.bytes, src->requests_hash.bytes, 32);
}

/** Build the RLP encoding of a withdrawal: [index, validator_index, addr, amount]. */
static rlp_item_t *encode_withdrawal_rlp(const rx_withdrawal_t *w) {
    rlp_item_t *item = rlp_list_new();
    if (!item) return NULL;
    rlp_list_append(item, rlp_uint64(w->index));
    rlp_list_append(item, rlp_uint64(w->validator_index));
    rlp_list_append(item, rlp_string(w->address.bytes, 20));
    rlp_list_append(item, rlp_uint64(w->amount_gwei));
    return item;
}

/** Assemble the full block RLP: [header, txs, uncles=[], withdrawals?]. */
static bytes_t assemble_block_rlp(const bytes_t *header_rlp,
                                  const uint8_t *const *txs,
                                  const size_t *tx_lengths, size_t tx_count,
                                  const rx_withdrawal_t *withdrawals,
                                  size_t withdrawal_count,
                                  bool include_withdrawals) {
    bytes_t out = { .data = NULL, .len = 0, .capacity = 0 };

    rlp_item_t *block = rlp_list_new();
    if (!block) return out;

    /* Header — decode pre-encoded header RLP into an item to include.
     * rlp_decode on a header returns an RLP_TYPE_LIST item; we nest it. */
    rlp_item_t *hdr_item = rlp_decode(header_rlp->data, header_rlp->len);
    if (!hdr_item) { rlp_item_free(block); return out; }
    rlp_list_append(block, hdr_item);

    /* Transactions */
    rlp_item_t *tx_list = rlp_list_new();
    for (size_t i = 0; i < tx_count; i++)
        rlp_list_append(tx_list, rlp_string(txs[i], tx_lengths[i]));
    rlp_list_append(block, tx_list);

    /* Uncles — always empty post-merge */
    rlp_list_append(block, rlp_list_new());

    /* Withdrawals (Shanghai+) */
    if (include_withdrawals) {
        rlp_item_t *wd_list = rlp_list_new();
        for (size_t i = 0; i < withdrawal_count; i++) {
            rlp_item_t *w = encode_withdrawal_rlp(&withdrawals[i]);
            if (w) rlp_list_append(wd_list, w);
        }
        rlp_list_append(block, wd_list);
    }

    out = rlp_encode(block);
    rlp_item_free(block);
    return out;
}

bool rx_build_block(rx_engine_t *engine,
                    const rx_build_header_t *header_fields,
                    const uint8_t *const *txs,
                    const size_t *tx_lengths,
                    size_t tx_count,
                    const rx_withdrawal_t *withdrawals,
                    size_t withdrawal_count,
                    rx_build_block_result_t *out) {
    if (!engine || !header_fields || !out ||
        (tx_count > 0 && (!txs || !tx_lengths)) ||
        (withdrawal_count > 0 && !withdrawals)) {
        if (engine) engine_set_error(engine, RX_ERR_NULL_ARG,
                                     "required argument is NULL");
        return false;
    }
    engine_clear_error(engine);
    memset(out, 0, sizeof(*out));

    /* 1. Build internal header from caller fields (roots still zero). */
    block_header_t hdr;
    fill_header_from_build(&hdr, header_fields);

    /* 2. Build internal body from raw tx bytes + withdrawals.
     *    Same pattern as convert_body in rx_execute_block. */
    block_body_t body;
    memset(&body, 0, sizeof(body));
    rlp_item_t *root = rlp_list_new();
    rlp_item_t *tx_list = rlp_list_new();
    for (size_t i = 0; i < tx_count; i++)
        rlp_list_append(tx_list, rlp_string(txs[i], tx_lengths[i]));
    rlp_list_append(root, tx_list);
    rlp_list_append(root, rlp_list_new()); /* empty uncles */
    body._rlp = root;
    body._tx_list_idx = 0;
    body.tx_count = tx_count;

    if (withdrawal_count > 0) {
        body.withdrawals = calloc(withdrawal_count, sizeof(withdrawal_t));
        if (!body.withdrawals) {
            block_body_free(&body);
            engine_set_error(engine, RX_ERR_OUT_OF_MEMORY, "alloc failed");
            return false;
        }
        body.withdrawal_count = withdrawal_count;
        for (size_t i = 0; i < withdrawal_count; i++) {
            body.withdrawals[i].index = withdrawals[i].index;
            body.withdrawals[i].validator_index = withdrawals[i].validator_index;
            memcpy(body.withdrawals[i].address.bytes,
                   withdrawals[i].address.bytes, 20);
            body.withdrawals[i].amount_gwei = withdrawals[i].amount_gwei;
        }
    }

    /* 3. Compute tx_root + withdrawals_root (filled before execute). */
    hdr.tx_root = block_compute_tx_root(&body);
    if (withdrawal_count > 0) {
        hdr.withdrawals_root = block_compute_withdrawals_root(
            body.withdrawals, body.withdrawal_count);
        hdr.has_withdrawals_root = true;
    } else {
        /* Capella+ blocks still carry an empty withdrawals_root (empty trie).
         * Only set has_withdrawals_root if the caller's header fields say so. */
    }

    /* 4. Execute: block_execute fills state_root, receipt_root, logs_bloom,
     *    gas_used in its returned block_result_t. */
    engine->evm->skip_root_hash = false;
    block_result_t br = block_execute(engine->evm, &hdr, &body,
                                      engine->block_hashes
#ifdef ENABLE_HISTORY
                                      , engine->history
#endif
                                      );
    if (!br.success) {
        engine_set_error(engine, RX_ERR_EXECUTION, "block execution failed");
        block_result_free(&br);
        block_body_free(&body);
        return false;
    }

    /* 5. Finalize header with execution results. */
    hdr.state_root = br.state_root;
    hdr.receipt_root = br.receipt_root;
    memcpy(hdr.logs_bloom, br.logs_bloom, 256);
    hdr.gas_used = br.gas_used;

    /* 6. RLP-encode the final header and hash it. */
    bytes_t header_rlp = block_header_encode_rlp(&hdr);
    if (!header_rlp.data) {
        engine_set_error(engine, RX_ERR_EXECUTION, "header RLP encode failed");
        block_result_free(&br);
        block_body_free(&body);
        return false;
    }
    hash_t block_hash = hash_keccak256(header_rlp.data, header_rlp.len);

    /* 7. Register block hash in ring so future BLOCKHASH(N) works. */
    engine->block_hashes[hdr.number % BLOCK_HASH_WINDOW] = block_hash;
    engine->last_block = hdr.number;

    /* 8. Assemble the full block RLP. */
    bytes_t block_rlp = assemble_block_rlp(&header_rlp, txs, tx_lengths,
                                           tx_count, withdrawals,
                                           withdrawal_count,
                                           hdr.has_withdrawals_root);

    /* 9. Fill the caller's result struct. */
    memcpy(out->block_hash.bytes, block_hash.bytes, 32);
    memcpy(out->state_root.bytes, hdr.state_root.bytes, 32);
    memcpy(out->transactions_root.bytes, hdr.tx_root.bytes, 32);
    memcpy(out->receipts_root.bytes, hdr.receipt_root.bytes, 32);
    if (hdr.has_withdrawals_root)
        memcpy(out->withdrawals_root.bytes, hdr.withdrawals_root.bytes, 32);
    memcpy(out->logs_bloom, hdr.logs_bloom, 256);
    out->gas_used = hdr.gas_used;

    out->block_rlp = block_rlp.data;    /* transferred ownership */
    out->block_rlp_len = block_rlp.len;

    /* Copy receipts via the existing helper into a small rx_block_result_t,
     * then move them into the build-block result. */
    rx_block_result_t tmp = {0};
    fill_block_result(&tmp, &br);
    out->receipts = tmp.receipts;
    out->receipt_count = tmp.tx_count;
    /* tmp.receipts is now owned by `out` — clear tmp so it doesn't double-free. */
    tmp.receipts = NULL;

    free(header_rlp.data);
    block_result_free(&br);
    block_body_free(&body);
    return true;
}

void rx_build_block_result_free(rx_build_block_result_t *r) {
    if (!r) return;
    if (r->receipts) {
        /* Each receipt may have a logs array that holds topic arrays and data. */
        for (size_t i = 0; i < r->receipt_count; i++) {
            rx_receipt_t *rc = &r->receipts[i];
            if (rc->logs) {
                for (size_t j = 0; j < rc->log_count; j++) {
                    free(rc->logs[j].topics);
                    free(rc->logs[j].data);
                }
                free(rc->logs);
            }
        }
        free(r->receipts);
    }
    free(r->block_rlp);
    memset(r, 0, sizeof(*r));
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

rx_stats_t rx_get_stats(const rx_engine_t *engine) {
    rx_stats_t st = {0};
    if (!engine || !engine->state) return st;

    /* State stats */
    state_t *s = evm_state_get_state(engine->state);
    state_stats_t ss = state_get_stats(s);
    st.account_count     = ss.account_count;
    st.account_live      = ss.account_live;
    st.resource_count    = ss.storage_account_count;
    st.acct_vec_bytes    = ss.acct_vec_bytes;
    st.res_vec_bytes     = ss.res_vec_bytes;
    st.acct_index_bytes  = ss.acct_arena_bytes;
    st.total_tracked     = ss.total_tracked;

    /* Storage pool stats */
    hart_pool_t *pool = state_get_storage_pool(s);
    if (pool) {
        hart_pool_stats_t ps = hart_pool_stats(pool);
        st.pool_data_size  = ps.used;
        st.pool_free_bytes = ps.free_bytes;
        st.pool_file_size  = ps.mapped;  /* anonymous — mapped virtual bytes */
    }

    /* Code store stats */
    evm_state_stats_t es = evm_state_get_stats(engine->state);
    st.code_count        = es.code_count;
    st.code_cache_hits   = es.code_cache_hits;
    st.code_cache_misses = es.code_cache_misses;
    st.exec_ms           = es.exec_ms;
    st.root_ms           = es.root_stor_ms + es.root_acct_ms;

    return st;
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

void rx_set_block_hash(rx_engine_t *engine, uint64_t block_number,
                       const rx_hash_t *hash) {
    if (!engine || !hash) return;
    uint64_t idx = block_number % BLOCK_HASH_WINDOW;
    memcpy(engine->block_hashes[idx].bytes, hash->bytes, 32);
}

/* ========================================================================
 * Message call (eth_call)
 * ======================================================================== */

void rx_call_result_free(rx_call_result_t *result) {
    if (!result) return;
    free(result->output);
    result->output = NULL;
    result->output_len = 0;
}

bool rx_call(rx_engine_t *engine,
             const rx_call_msg_t *msg,
             rx_call_result_t *result) {
    if (!engine || !msg || !result) {
        if (engine) engine_set_error(engine, RX_ERR_NULL_ARG, "required argument is NULL");
        return false;
    }
    engine_clear_error(engine);
    memset(result, 0, sizeof(*result));

    uint64_t gas = msg->gas;
    if (gas == 0) gas = 30000000; /* default: 30M gas */

    /* Snapshot state — we'll revert after execution */
    uint32_t snap = evm_state_snapshot(engine->state);

    /* Set up block environment from current engine state */
    evm_block_env_t block_env;
    memset(&block_env, 0, sizeof(block_env));
    block_env.number = engine->last_block;
    block_env.gas_limit = gas;
    block_env.chain_id = uint256_from_uint64(1); /* mainnet */
    memcpy(block_env.block_hash, engine->block_hashes, sizeof(block_env.block_hash));
    evm_set_block_env(engine->evm, &block_env);

    /* Set up tx context */
    evm_tx_context_t tx_ctx;
    memset(&tx_ctx, 0, sizeof(tx_ctx));
    memcpy(tx_ctx.origin.bytes, msg->from.bytes, 20);
    evm_set_tx_context(engine->evm, &tx_ctx);

    /* Determine fork for current block */
    engine->evm->fork = fork_get_active(engine->last_block, 0,
                                         engine->chain_config);

    /* Build EVM message */
    evm_message_t evm_msg;
    memset(&evm_msg, 0, sizeof(evm_msg));
    evm_msg.kind = EVM_CALL;
    memcpy(evm_msg.caller.bytes, msg->from.bytes, 20);
    memcpy(evm_msg.recipient.bytes, msg->to.bytes, 20);
    memcpy(evm_msg.code_addr.bytes, msg->to.bytes, 20);
    evm_msg.value = uint256_from_bytes(msg->value.bytes, 32);
    evm_msg.input_data = msg->data;
    evm_msg.input_size = msg->data_len;
    evm_msg.gas = gas;
    evm_msg.depth = 0;
    evm_msg.is_static = false;

    /* Execute */
    evm_result_t evm_result;
    bool ok = evm_execute(engine->evm, &evm_msg, &evm_result);

    if (!ok) {
        evm_state_revert(engine->state, snap);
        engine_set_error(engine, RX_ERR_EXECUTION, "evm_execute internal error");
        return false;
    }

    /* Fill result */
    result->success = (evm_result.status == EVM_SUCCESS);
    result->gas_used = gas - evm_result.gas_left;

    /* Copy output data (take ownership) */
    if (evm_result.output_data && evm_result.output_size > 0) {
        result->output = malloc(evm_result.output_size);
        if (result->output) {
            memcpy(result->output, evm_result.output_data, evm_result.output_size);
            result->output_len = evm_result.output_size;
        }
    }

    evm_result_free(&evm_result);

    /* Revert all state changes */
    evm_state_revert(engine->state, snap);

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
