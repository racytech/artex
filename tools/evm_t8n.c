/**
 * EVM State Transition Tool (t8n)
 *
 * geth-compatible CLI for differential testing.
 * Reads alloc/env/txs JSON, executes transactions, outputs stateRoot.
 *
 * Usage:
 *   ./evm_t8n --input.alloc alloc.json --input.env env.json --input.txs txs.json
 *             --state.fork Cancun --output.result stdout
 */

#include "test_parser.h"
#include "test_runner.h"
#include "transaction.h"
#include "evm.h"
#include "evm_state.h"
#include "flat_state.h"
#include "fork.h"
#include "hash.h"
#include "uint256.h"
#include "secp256k1_wrap.h"
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* From test_runner_core.c */
extern chain_config_t *create_test_chain_config(const char *fork_name);

#ifdef ENABLE_DEBUG
bool g_trace_calls __attribute__((weak)) = false;
#endif

/* =========================================================================
 * CLI argument parsing
 * ========================================================================= */

typedef struct {
    const char *input_alloc;
    const char *input_env;
    const char *input_txs;
    const char *output_result;   /* "stdout" or filepath */
    const char *output_alloc;    /* "stdout" or filepath, NULL to skip */
    const char *state_fork;
    uint64_t    chain_id;
    bool        trace;
    uint256_t   block_reward;
    bool        has_block_reward;
} t8n_args_t;

static void print_usage(void) {
    fprintf(stderr,
        "Usage: evm_t8n [options]\n"
        "  --input.alloc <file>     Pre-state allocation (JSON)\n"
        "  --input.env <file>       Block environment (JSON)\n"
        "  --input.txs <file>       Transaction list (JSON)\n"
        "  --output.result <file>   Execution result (default: stdout)\n"
        "  --output.alloc <file>    Post-state allocation (optional)\n"
        "  --state.fork <name>      Fork name (e.g. Cancun, Shanghai)\n"
        "  --state.chainid <N>      Chain ID (default: 1)\n"
        "  --state.reward <N>       Block reward in wei (decimal, default: none)\n"
        "  --trace                  Enable EIP-3155 tracing to stderr\n"
    );
}

static bool parse_args(int argc, char **argv, t8n_args_t *args) {
    memset(args, 0, sizeof(*args));
    args->output_result = "stdout";
    args->chain_id = 1;
    args->state_fork = "Cancun";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input.alloc") == 0 && i + 1 < argc) {
            args->input_alloc = argv[++i];
        } else if (strcmp(argv[i], "--input.env") == 0 && i + 1 < argc) {
            args->input_env = argv[++i];
        } else if (strcmp(argv[i], "--input.txs") == 0 && i + 1 < argc) {
            args->input_txs = argv[++i];
        } else if (strcmp(argv[i], "--output.result") == 0 && i + 1 < argc) {
            args->output_result = argv[++i];
        } else if (strcmp(argv[i], "--output.alloc") == 0 && i + 1 < argc) {
            args->output_alloc = argv[++i];
        } else if (strcmp(argv[i], "--state.fork") == 0 && i + 1 < argc) {
            args->state_fork = argv[++i];
        } else if (strcmp(argv[i], "--state.chainid") == 0 && i + 1 < argc) {
            args->chain_id = (uint64_t)atoll(argv[++i]);
        } else if (strcmp(argv[i], "--state.reward") == 0 && i + 1 < argc) {
            const char *rw = argv[++i];
            /* Parse decimal or hex reward */
            if (rw[0] == '0' && rw[1] == 'x')
                parse_uint256(rw, &args->block_reward);
            else
                args->block_reward = uint256_from_uint64((uint64_t)strtoull(rw, NULL, 10));
            args->has_block_reward = true;
        } else if (strcmp(argv[i], "--trace") == 0) {
            args->trace = true;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage();
            exit(0);
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            print_usage();
            return false;
        }
    }

    if (!args->input_alloc || !args->input_env || !args->input_txs) {
        fprintf(stderr, "Error: --input.alloc, --input.env, and --input.txs are required\n");
        print_usage();
        return false;
    }
    return true;
}

/* =========================================================================
 * Derive sender address from secret key
 * ========================================================================= */

static bool derive_sender(const uint8_t secret_key[32], address_t *sender) {
    uint8_t pub[64];
    if (!secp256k1_wrap_pubkey_create(pub, secret_key)) {
        return false;
    }
    hash_t h = hash_keccak256(pub, 64);
    memcpy(sender->bytes, h.bytes + 12, 20);
    return true;
}

/* =========================================================================
 * Parse env.json (t8n format)
 * ========================================================================= */

typedef struct {
    address_t coinbase;
    uint64_t  number;
    uint64_t  timestamp;
    uint64_t  gas_limit;
    uint256_t difficulty;
    uint256_t base_fee;
    hash_t    prev_randao;
    uint256_t excess_blob_gas;
    /* blockHashes */
    hash_t    block_hashes[256];
    bool      has_block_hashes;
    /* parentBeaconBlockRoot */
    hash_t    parent_beacon_root;
    bool      has_parent_beacon_root;
    /* withdrawals */
    struct {
        address_t address;
        uint64_t  amount_gwei;
    } *withdrawals;
    size_t    withdrawal_count;
} t8n_env_t;

static bool parse_t8n_env(const cJSON *json, t8n_env_t *env) {
    memset(env, 0, sizeof(*env));

    const char *s;
    if (json_get_string(json, "currentCoinbase", &s))
        parse_address(s, &env->coinbase);

    if (json_get_string(json, "currentNumber", &s))
        parse_uint64(s, &env->number);

    if (json_get_string(json, "currentTimestamp", &s))
        parse_uint64(s, &env->timestamp);

    if (json_get_string(json, "currentGasLimit", &s))
        parse_uint64(s, &env->gas_limit);

    if (json_get_string(json, "currentDifficulty", &s))
        parse_uint256(s, &env->difficulty);

    if (json_get_string(json, "currentBaseFee", &s))
        parse_uint256(s, &env->base_fee);

    if (json_get_string(json, "currentRandom", &s))
        parse_hash(s, &env->prev_randao);

    if (json_get_string(json, "currentExcessBlobGas", &s))
        parse_uint256(s, &env->excess_blob_gas);

    if (json_get_string(json, "parentBeaconBlockRoot", &s)) {
        parse_hash(s, &env->parent_beacon_root);
        env->has_parent_beacon_root = true;
    }

    /* blockHashes: {"0": "0x...", "1": "0x...", ...} */
    const cJSON *bh = cJSON_GetObjectItem(json, "blockHashes");
    if (bh && cJSON_IsObject(bh)) {
        env->has_block_hashes = true;
        const cJSON *item;
        cJSON_ArrayForEach(item, bh) {
            uint64_t num = (uint64_t)atoll(item->string);
            if (cJSON_IsString(item)) {
                parse_hash(item->valuestring, &env->block_hashes[num % 256]);
            }
        }
    }

    /* withdrawals */
    const cJSON *wds = cJSON_GetObjectItem(json, "withdrawals");
    if (wds && cJSON_IsArray(wds)) {
        int wcount = cJSON_GetArraySize(wds);
        if (wcount > 0) {
            env->withdrawals = calloc(wcount, sizeof(env->withdrawals[0]));
            env->withdrawal_count = wcount;
            int wi = 0;
            const cJSON *w;
            cJSON_ArrayForEach(w, wds) {
                if (json_get_string(w, "address", &s))
                    parse_address(s, &env->withdrawals[wi].address);
                if (json_get_string(w, "amount", &s))
                    parse_uint64(s, &env->withdrawals[wi].amount_gwei);
                wi++;
            }
        }
    }

    return true;
}

/* =========================================================================
 * Parse txs.json (t8n format)
 * ========================================================================= */

typedef struct {
    transaction_t tx;
    uint8_t      *data_buf;      /* owned data buffer */
    access_list_entry_t *al_buf; /* owned access list entries */
    uint256_t    *al_keys_buf;   /* owned storage keys (flat) */
    bool          rejected;
    const char   *reject_error;
} t8n_tx_t;

static bool parse_t8n_txs(const cJSON *json, t8n_tx_t **out_txs,
                           size_t *out_count, uint64_t chain_id) {
    (void)chain_id;
    if (!cJSON_IsArray(json)) {
        *out_txs = NULL;
        *out_count = 0;
        return true;
    }

    int count = cJSON_GetArraySize(json);
    t8n_tx_t *txs = calloc(count, sizeof(t8n_tx_t));
    if (!txs) return false;

    int idx = 0;
    const cJSON *item;
    cJSON_ArrayForEach(item, json) {
        t8n_tx_t *t = &txs[idx];
        transaction_t *tx = &t->tx;
        const char *s;

        /* Determine type */
        tx->type = TX_TYPE_LEGACY;
        if (json_get_string(item, "type", &s)) {
            uint64_t ty = 0;
            parse_uint64(s, &ty);
            tx->type = (transaction_type_t)ty;
        } else {
            /* Infer type from fields */
            if (cJSON_GetObjectItem(item, "maxFeePerBlobGas"))
                tx->type = TX_TYPE_EIP4844;
            else if (cJSON_GetObjectItem(item, "maxFeePerGas"))
                tx->type = TX_TYPE_EIP1559;
            else if (cJSON_GetObjectItem(item, "accessList"))
                tx->type = TX_TYPE_EIP2930;
        }

        /* Common fields */
        if (json_get_string(item, "nonce", &s))
            parse_uint64(s, &tx->nonce);

        if (json_get_string(item, "gas", &s))
            parse_uint64(s, &tx->gas_limit);
        else if (json_get_string(item, "gasLimit", &s))
            parse_uint64(s, &tx->gas_limit);

        if (json_get_string(item, "value", &s))
            parse_uint256(s, &tx->value);

        /* to */
        if (json_get_string(item, "to", &s) && strlen(s) >= 2) {
            parse_address(s, &tx->to);
            tx->is_create = false;
        } else {
            memset(&tx->to, 0, sizeof(tx->to));
            tx->is_create = true;
        }

        /* Gas pricing */
        if (json_get_string(item, "gasPrice", &s))
            parse_uint256(s, &tx->gas_price);
        if (json_get_string(item, "maxFeePerGas", &s))
            parse_uint256(s, &tx->max_fee_per_gas);
        if (json_get_string(item, "maxPriorityFeePerGas", &s))
            parse_uint256(s, &tx->max_priority_fee_per_gas);
        if (json_get_string(item, "maxFeePerBlobGas", &s))
            parse_uint256(s, &tx->max_fee_per_blob_gas);

        /* data / input */
        const char *data_hex = NULL;
        if (json_get_string(item, "input", &data_hex) ||
            json_get_string(item, "data", &data_hex)) {
            size_t dlen = 0;
            t->data_buf = parse_hex_alloc(data_hex, &dlen);
            tx->data = t->data_buf;
            tx->data_size = dlen;
        }

        /* secretKey → derive sender */
        if (json_get_string(item, "secretKey", &s)) {
            uint8_t sk[32];
            parse_hex_string(s, sk, 32);
            derive_sender(sk, &tx->sender);
        } else if (json_get_string(item, "sender", &s)) {
            parse_address(s, &tx->sender);
        }

        /* accessList */
        const cJSON *al = cJSON_GetObjectItem(item, "accessList");
        if (al && cJSON_IsArray(al)) {
            int al_count = cJSON_GetArraySize(al);
            if (al_count > 0) {
                t->al_buf = calloc(al_count, sizeof(access_list_entry_t));
                tx->access_list = t->al_buf;
                tx->access_list_count = al_count;

                /* First pass: count total storage keys */
                size_t total_keys = 0;
                const cJSON *entry;
                cJSON_ArrayForEach(entry, al) {
                    const cJSON *keys = cJSON_GetObjectItem(entry, "storageKeys");
                    if (keys && cJSON_IsArray(keys))
                        total_keys += cJSON_GetArraySize(keys);
                }

                /* Allocate flat storage key array */
                t->al_keys_buf = NULL;
                if (total_keys > 0)
                    t->al_keys_buf = calloc(total_keys, sizeof(uint256_t));

                size_t key_offset = 0;
                int ai = 0;
                cJSON_ArrayForEach(entry, al) {
                    if (json_get_string(entry, "address", &s))
                        parse_address(s, &t->al_buf[ai].address);

                    const cJSON *keys = cJSON_GetObjectItem(entry, "storageKeys");
                    if (keys && cJSON_IsArray(keys) && t->al_keys_buf) {
                        int kcount = cJSON_GetArraySize(keys);
                        t->al_buf[ai].storage_keys = &t->al_keys_buf[key_offset];
                        t->al_buf[ai].storage_keys_count = kcount;
                        const cJSON *k;
                        int ki = 0;
                        cJSON_ArrayForEach(k, keys) {
                            if (cJSON_IsString(k))
                                parse_uint256(k->valuestring, &t->al_keys_buf[key_offset + ki]);
                            ki++;
                        }
                        key_offset += kcount;
                    }
                    ai++;
                }
            }
        }

        /* blobVersionedHashes */
        const cJSON *bvh = cJSON_GetObjectItem(item, "blobVersionedHashes");
        if (bvh && cJSON_IsArray(bvh)) {
            int bcount = cJSON_GetArraySize(bvh);
            if (bcount > 0) {
                hash_t *hashes = calloc(bcount, sizeof(hash_t));
                int bi = 0;
                const cJSON *bh;
                cJSON_ArrayForEach(bh, bvh) {
                    if (cJSON_IsString(bh))
                        parse_hash(bh->valuestring, &hashes[bi]);
                    bi++;
                }
                tx->blob_versioned_hashes = hashes;
                tx->blob_versioned_hashes_count = bcount;
            }
        }

        idx++;
    }

    *out_txs = txs;
    *out_count = count;
    return true;
}

static void free_t8n_txs(t8n_tx_t *txs, size_t count) {
    for (size_t i = 0; i < count; i++) {
        free(txs[i].data_buf);
        free(txs[i].al_buf);
        free(txs[i].al_keys_buf);
        free((void *)txs[i].tx.blob_versioned_hashes);
    }
    free(txs);
}

/* =========================================================================
 * System call helper (copied from block_executor.c)
 * ========================================================================= */

static void system_call(evm_t *evm, const uint8_t addr_bytes[20],
                         const uint8_t *calldata, size_t calldata_len) {
    address_t contract_addr, system_addr;
    memcpy(contract_addr.bytes, addr_bytes, 20);
    memset(system_addr.bytes, 0xff, 20);
    system_addr.bytes[19] = 0xfe;  /* SYSTEM_ADDRESS = 0xfff...ffe */

    uint32_t code_len = 0;
    evm_state_get_code_ptr(evm->state, &contract_addr, &code_len);
    if (code_len == 0) return;

    evm_tx_context_t sys_tx;
    memset(&sys_tx, 0, sizeof(sys_tx));
    address_copy(&sys_tx.origin, &system_addr);
    evm_set_tx_context(evm, &sys_tx);

    uint256_t zero = UINT256_ZERO;
    evm_message_t msg = evm_message_call(
        &system_addr, &contract_addr, &zero,
        calldata, calldata_len, 30000000, 0);
    evm_result_t result;
    evm_execute(evm, &msg, &result);
    evm_result_free(&result);
    evm_state_commit_tx(evm->state);
}

/* =========================================================================
 * JSON output helpers
 * ========================================================================= */

static char *hash_to_hex_alloc(const hash_t *h) {
    char *buf = malloc(67);
    buf[0] = '0'; buf[1] = 'x';
    for (int i = 0; i < 32; i++)
        sprintf(buf + 2 + i * 2, "%02x", h->bytes[i]);
    buf[66] = '\0';
    return buf;
}

static char *uint64_to_hex(uint64_t v) {
    char *buf = malloc(19);
    sprintf(buf, "0x%lx", (unsigned long)v);
    return buf;
}

/* =========================================================================
 * Receipt tracking
 * ========================================================================= */

typedef struct {
    bool     success;       /* EVM_SUCCESS or EVM_REVERT with data */
    uint64_t gas_used;
    uint64_t cumulative_gas;
    int      tx_type;       /* for typed receipt encoding */
} t8n_receipt_t;

typedef struct {
    int         index;
    const char *error;
} t8n_rejected_t;

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    t8n_args_t args;
    if (!parse_args(argc, argv, &args))
        return 1;

    /* --- Initialize secp256k1 context (needed for secretKey → sender) --- */
    secp256k1_wrap_init();

    /* --- Load JSON inputs --- */
    cJSON *alloc_json = load_json_file(args.input_alloc);
    cJSON *env_json   = load_json_file(args.input_env);
    cJSON *txs_json   = load_json_file(args.input_txs);

    if (!alloc_json || !env_json || !txs_json) {
        fprintf(stderr, "Error: failed to load input JSON files\n");
        if (alloc_json) cJSON_Delete(alloc_json);
        if (env_json) cJSON_Delete(env_json);
        if (txs_json) cJSON_Delete(txs_json);
        return 1;
    }

    /* --- Parse alloc (pre-state) --- */
    test_account_t *accounts = NULL;
    size_t account_count = 0;
    if (!parse_account_map(alloc_json, &accounts, &account_count)) {
        fprintf(stderr, "Error: failed to parse alloc\n");
        return 1;
    }

    /* --- Parse env --- */
    t8n_env_t env;
    if (!parse_t8n_env(env_json, &env)) {
        fprintf(stderr, "Error: failed to parse env\n");
        return 1;
    }

    /* --- Parse txs --- */
    t8n_tx_t *txs = NULL;
    size_t tx_count = 0;
    if (!parse_t8n_txs(txs_json, &txs, &tx_count, args.chain_id)) {
        fprintf(stderr, "Error: failed to parse txs\n");
        return 1;
    }

    /* --- Setup chain config and EVM --- */
    chain_config_t *chain_config = create_test_chain_config(args.state_fork);
    if (!chain_config) {
        fprintf(stderr, "Error: unknown fork '%s'\n", args.state_fork);
        return 1;
    }
    chain_config->chain_id = args.chain_id;

    evm_state_t *state = evm_state_create(NULL);
    if (!state) {
        fprintf(stderr, "Error: failed to create EVM state\n");
        return 1;
    }
    /* Fresh flat_state in RAM for state root computation */
    {
        flat_state_t *fs = flat_state_create("/dev/shm/evm_t8n_flat");
        if (fs) evm_state_set_flat_state(state, fs);
    }

    evm_t *evm = evm_create(state, chain_config);
    if (!evm) {
        fprintf(stderr, "Error: failed to create EVM\n");
        return 1;
    }

    /* --- Enable tracing --- */
#ifdef ENABLE_EVM_TRACE
    if (args.trace) {
        extern void evm_tracer_init(FILE *);
        evm_tracer_init(stderr);
    }
#endif

    /* --- Populate pre-state --- */
    test_runner_setup_state(state, accounts, account_count);
    evm_state_commit(state);

    /* --- Set block environment (must happen before prune_empty so fork is known) --- */
    evm_block_env_t evm_env;
    memset(&evm_env, 0, sizeof(evm_env));
    evm_env.number    = env.number;
    evm_env.timestamp = env.timestamp;
    evm_env.gas_limit = env.gas_limit;
    evm_env.difficulty = env.difficulty;
    evm_env.coinbase  = env.coinbase;
    evm_env.base_fee  = env.base_fee;
    evm_env.chain_id  = uint256_from_uint64(args.chain_id);
    evm_env.excess_blob_gas = env.excess_blob_gas;
    if (!uint256_is_zero(&evm_env.excess_blob_gas)) {
        evm_env.blob_base_fee = calc_blob_gas_price(&evm_env.excess_blob_gas, evm->fork);
    }
    if (env.has_block_hashes)
        memcpy(evm_env.block_hash, env.block_hashes, sizeof(evm_env.block_hash));

    evm_set_block_env(evm, &evm_env);

    /* Recompute blob base fee after fork is set */
    if (!uint256_is_zero(&evm_env.excess_blob_gas)) {
        evm_env.blob_base_fee = calc_blob_gas_price(&evm_env.excess_blob_gas, evm->fork);
        evm_set_block_env(evm, &evm_env);
    }

    /* EIP-161: enable empty account pruning after fork is determined */
    evm_state_set_prune_empty(state, evm->fork >= FORK_SPURIOUS_DRAGON);
    evm_state_begin_block(state, env.number);

    /* --- System calls --- */
    /* EIP-4788: beacon root storage (Cancun+) */
    static const uint8_t BEACON_ROOT_ADDR[20] = {
        0x00, 0x0F, 0x3d, 0xf6, 0xD7, 0x32, 0x80, 0x7E, 0xf1, 0x31,
        0x9f, 0xB7, 0xB8, 0xbB, 0x85, 0x22, 0xd0, 0xBe, 0xac, 0x02
    };
    if (evm->fork >= FORK_CANCUN && env.has_parent_beacon_root) {
        system_call(evm, BEACON_ROOT_ADDR,
                    env.parent_beacon_root.bytes, 32);
    }

    /* EIP-2935: history storage (Prague+) */
    static const uint8_t HISTORY_ADDR[20] = {
        0x00, 0x00, 0xf9, 0x08, 0x27, 0xf1, 0xc5, 0x3a, 0x10, 0xcb,
        0x7a, 0x02, 0x33, 0x5b, 0x17, 0x53, 0x20, 0x00, 0x29, 0x35
    };
    if (evm->fork >= FORK_PRAGUE && env.has_block_hashes) {
        /* Use block_hashes[(number-1) % 256] as parent hash */
        uint64_t parent_num = (env.number > 0) ? env.number - 1 : 0;
        hash_t parent_hash = env.block_hashes[parent_num % 256];
        system_call(evm, HISTORY_ADDR, parent_hash.bytes, 32);
    }

    /* --- Execute transactions --- */
    t8n_receipt_t *receipts = calloc(tx_count, sizeof(t8n_receipt_t));
    t8n_rejected_t *rejected = calloc(tx_count, sizeof(t8n_rejected_t));
    size_t rejected_count = 0;
    size_t receipt_count = 0;
    uint64_t cumulative_gas = 0;

    block_env_t block_env = {
        .coinbase    = env.coinbase,
        .block_number = env.number,
        .timestamp   = env.timestamp,
        .gas_limit   = env.gas_limit,
        .difficulty  = env.difficulty,
        .base_fee    = env.base_fee,
        .prev_randao = env.prev_randao,
        .excess_blob_gas = env.excess_blob_gas,
        .skip_coinbase_payment = false,
    };

    for (size_t i = 0; i < tx_count; i++) {
        t8n_tx_t *t = &txs[i];
        transaction_t *tx = &t->tx;

        /* Validate nonce before execution (transaction_execute doesn't check) */
        uint64_t expected_nonce = evm_state_get_nonce(state, &tx->sender);
        if (tx->nonce != expected_nonce) {
            rejected[rejected_count].index = (int)i;
            rejected[rejected_count].error = "nonce mismatch";
            rejected_count++;
            continue;
        }

        /* Execute */
        transaction_result_t tx_result;
        bool ok = transaction_execute(evm, tx, &block_env, &tx_result);

        if (!ok) {
            /* Transaction rejected at protocol level */
            fprintf(stderr, "TX %zu REJECTED: sender=0x", i);
            for (int j=0;j<20;j++) fprintf(stderr,"%02x",tx->sender.bytes[j]);
            fprintf(stderr, " nonce=%lu gas=%lu value=", tx->nonce, tx->gas_limit);
            uint8_t vb[32]; uint256_to_bytes(&tx->value, vb);
            for (int j=0;j<4;j++) fprintf(stderr,"%02x",vb[j]);
            fprintf(stderr, "... balance=");
            uint256_t bal = evm_state_get_balance(state, &tx->sender);
            uint8_t bb[32]; uint256_to_bytes(&bal, bb);
            for (int j=0;j<8;j++) fprintf(stderr,"%02x",bb[j]);
            fprintf(stderr, "\n");
            rejected[rejected_count].index = (int)i;
            rejected[rejected_count].error = "transaction rejected";
            rejected_count++;
            continue;
        }

        /* Collect receipt */
        cumulative_gas += tx_result.gas_used;
        receipts[receipt_count].success = (tx_result.status == EVM_SUCCESS);
        receipts[receipt_count].gas_used = tx_result.gas_used;
        receipts[receipt_count].cumulative_gas = cumulative_gas;
        receipts[receipt_count].tx_type = tx->type;
        receipt_count++;

        transaction_result_free(&tx_result);
        evm_state_commit_tx(state);
    }

    /* --- Apply block reward (PoW) --- */
    if (args.has_block_reward && !uint256_is_zero(&args.block_reward)) {
        evm_state_add_balance(state, &env.coinbase, &args.block_reward);
    }

    /* --- Process withdrawals (Shanghai+) --- */
    if (evm->fork >= FORK_SHANGHAI) {
        for (size_t w = 0; w < env.withdrawal_count; w++) {
            uint256_t gwei = uint256_from_uint64(env.withdrawals[w].amount_gwei);
            uint256_t multiplier = uint256_from_uint64(1000000000ULL);
            uint256_t amount_wei = uint256_mul(&gwei, &multiplier);
            evm_state_add_balance(state, &env.withdrawals[w].address, &amount_wei);
        }
    }

    /* --- Compute state root --- */
    bool prune_empty = (evm->fork >= FORK_SPURIOUS_DRAGON);
    hash_t state_root = evm_state_compute_mpt_root(state, prune_empty);

    /* --- Build result JSON --- */
    cJSON *result = cJSON_CreateObject();
    char *root_hex = hash_to_hex_alloc(&state_root);
    cJSON_AddStringToObject(result, "stateRoot", root_hex);
    free(root_hex);

    char *gas_hex = uint64_to_hex(cumulative_gas);
    cJSON_AddStringToObject(result, "gasUsed", gas_hex);
    free(gas_hex);

    /* Receipts */
    cJSON *receipts_arr = cJSON_CreateArray();
    for (size_t i = 0; i < receipt_count; i++) {
        cJSON *r = cJSON_CreateObject();
        cJSON_AddStringToObject(r, "status",
            receipts[i].success ? "0x1" : "0x0");

        char *cgas = uint64_to_hex(receipts[i].cumulative_gas);
        cJSON_AddStringToObject(r, "cumulativeGasUsed", cgas);
        free(cgas);

        /* Empty bloom and logs for now (Phase 3) */
        char bloom_hex[515];
        bloom_hex[0] = '0'; bloom_hex[1] = 'x';
        memset(bloom_hex + 2, '0', 512);
        bloom_hex[514] = '\0';
        cJSON_AddStringToObject(r, "logsBloom", bloom_hex);

        cJSON_AddItemToObject(r, "logs", cJSON_CreateArray());

        cJSON_AddItemToArray(receipts_arr, r);
    }
    cJSON_AddItemToObject(result, "receipts", receipts_arr);

    /* Rejected */
    if (rejected_count > 0) {
        cJSON *rejected_arr = cJSON_CreateArray();
        for (size_t i = 0; i < rejected_count; i++) {
            cJSON *rj = cJSON_CreateObject();
            cJSON_AddNumberToObject(rj, "index", rejected[i].index);
            cJSON_AddStringToObject(rj, "error", rejected[i].error);
            cJSON_AddItemToArray(rejected_arr, rj);
        }
        cJSON_AddItemToObject(result, "rejected", rejected_arr);
    }

    /* --- Output alloc (post-state) --- */
    if (args.output_alloc) {
        address_t addrs[8192];
        size_t n = evm_state_collect_addresses(state, addrs, 8192);

        cJSON *alloc_out = cJSON_CreateObject();
        for (size_t i = 0; i < n; i++) {
            address_t *a = &addrs[i];
            bool exists = evm_state_exists(state, a);
            /* Skip non-existent accounts (pruned empty accounts) */
            uint64_t nonce = evm_state_get_nonce(state, a);
            uint256_t bal = evm_state_get_balance(state, a);
            bool is_empty = (nonce == 0 && uint256_is_zero(&bal) &&
                             evm_state_get_code_size(state, a) == 0);
            if (prune_empty && is_empty && !exists) continue;

            char addr_hex[43];
            addr_hex[0] = '0'; addr_hex[1] = 'x';
            for (int j = 0; j < 20; j++)
                sprintf(addr_hex + 2 + j*2, "%02x", a->bytes[j]);

            cJSON *acct = cJSON_CreateObject();

            /* Balance */
            uint8_t bb[32]; uint256_to_bytes(&bal, bb);
            char bal_hex[67] = "0x";
            int s = 0; while (s < 31 && bb[s] == 0) s++;
            for (int j = s; j < 32; j++)
                sprintf(bal_hex + strlen(bal_hex), "%02x", bb[j]);
            cJSON_AddStringToObject(acct, "balance", bal_hex);

            /* Nonce (only if non-zero) */
            if (nonce > 0) {
                char nonce_hex[20];
                sprintf(nonce_hex, "0x%lx", nonce);
                cJSON_AddStringToObject(acct, "nonce", nonce_hex);
            }

            /* Code */
            uint32_t csz = evm_state_get_code_size(state, a);
            if (csz > 0) {
                const uint8_t *code = evm_state_get_code_ptr(state, a, &csz);
                if (code && csz > 0) {
                    char *code_hex = malloc(csz * 2 + 3);
                    code_hex[0] = '0'; code_hex[1] = 'x';
                    for (uint32_t c = 0; c < csz; c++)
                        sprintf(code_hex + 2 + c*2, "%02x", code[c]);
                    cJSON_AddStringToObject(acct, "code", code_hex);
                    free(code_hex);
                }
            }

            cJSON_AddItemToObject(alloc_out, addr_hex, acct);
        }

        char *alloc_str = cJSON_Print(alloc_out);
        if (strcmp(args.output_alloc, "stdout") == 0) {
            printf("%s\n", alloc_str);
        } else {
            FILE *f = fopen(args.output_alloc, "w");
            if (f) { fprintf(f, "%s\n", alloc_str); fclose(f); }
        }
        free(alloc_str);
        cJSON_Delete(alloc_out);
    }

    /* --- Output result --- */
    char *result_str = cJSON_PrintUnformatted(result);
    if (strcmp(args.output_result, "stdout") == 0) {
        printf("%s\n", result_str);
    } else {
        FILE *f = fopen(args.output_result, "w");
        if (f) {
            fprintf(f, "%s\n", result_str);
            fclose(f);
        }
    }
    free(result_str);
    cJSON_Delete(result);

    /* --- Cleanup --- */
    free(receipts);
    free(rejected);
    free_t8n_txs(txs, tx_count);
    free(env.withdrawals);
    for (size_t i = 0; i < account_count; i++)
        test_account_free(&accounts[i]);
    free(accounts);
    cJSON_Delete(alloc_json);
    cJSON_Delete(env_json);
    cJSON_Delete(txs_json);
    evm_destroy(evm);
    evm_state_destroy(state);

    return 0;
}
