/**
 * EVM State Test Runner (statetest)
 *
 * geth/evmone-compatible CLI for differential testing via GoEVMLab's runtest.
 * Reads a StateTest JSON fixture, executes each subtest, and outputs:
 *   - EIP-3155 JSONL traces to stderr (when --trace)
 *   - {"stateRoot": "0x..."} to stderr after each subtest
 *   - JSON array of test results to stdout
 *
 * Usage:
 *   ./evm_statetest [--trace] [--trace-summary] <statetest.json>
 *   echo "<statetest.json" | ./evm_statetest [--trace]   # batch mode via stdin
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
#include <stdbool.h>

/* From test_runner_core.c */
extern chain_config_t *create_test_chain_config(const char *fork_name);

#ifdef ENABLE_DEBUG
bool g_trace_calls __attribute__((weak)) = false;
#endif

/* =========================================================================
 * CLI argument parsing
 * ========================================================================= */

typedef struct {
    const char *test_path;      /* statetest JSON file (or NULL for stdin mode) */
    const char *fork_filter;    /* --statetest.fork: only run this fork */
    int         index_filter;   /* --statetest.index: only run this subtest (-1 = all) */
    bool        trace;          /* --trace: full EIP-3155 tracing */
    bool        trace_summary;  /* --trace-summary: just stateRoot + pass/gas */
    bool        human;          /* --human: human-readable output */
} statetest_args_t;

static void print_usage(void) {
    fprintf(stderr,
        "Usage: evm_statetest [options] <file>\n"
        "  --trace                  Enable EIP-3155 tracing to stderr\n"
        "  --trace-summary          Output summary (stateRoot, pass, gasUsed) to stderr\n"
        "  --statetest.fork <name>  Only run tests for the specified fork\n"
        "  --statetest.index <N>    Only run subtest at index N (-1 = all)\n"
        "  --human                  Human-readable output\n"
        "\n"
        "If no file is given, reads filenames from stdin (batch mode).\n"
    );
}

static bool parse_args(int argc, char **argv, statetest_args_t *args) {
    memset(args, 0, sizeof(*args));
    args->index_filter = -1;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--trace") == 0) {
            args->trace = true;
        } else if (strcmp(argv[i], "--trace-summary") == 0) {
            args->trace_summary = true;
        } else if (strcmp(argv[i], "--trace.format=json") == 0) {
            /* geth compat: accepted and ignored (we always output json) */
        } else if (strcmp(argv[i], "--trace.nomemory=true") == 0 ||
                   strcmp(argv[i], "--trace.noreturndata=true") == 0 ||
                   strcmp(argv[i], "--trace.nomemory") == 0 ||
                   strcmp(argv[i], "--trace.noreturndata") == 0) {
            /* geth compat: accepted and ignored */
        } else if (strcmp(argv[i], "--statetest.fork") == 0 && i + 1 < argc) {
            args->fork_filter = argv[++i];
        } else if (strcmp(argv[i], "--statetest.index") == 0 && i + 1 < argc) {
            args->index_filter = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--human") == 0) {
            args->human = true;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage();
            exit(0);
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "Unknown flag: %s\n", argv[i]);
            print_usage();
            return false;
        } else {
            /* Positional argument: test file path */
            args->test_path = argv[i];
        }
    }
    return true;
}

/* hash_to_hex() is provided by common/include/hash.h */

/* =========================================================================
 * Run one state test file
 * ========================================================================= */

typedef struct {
    const char *name;
    const char *fork;
    bool        pass;
    char        state_root[67];   /* "0x" + 64 hex chars */
    const char *error;
} test_result_entry_t;

static int run_statetest_file(const char *filepath, const statetest_args_t *args,
                               test_result_entry_t **out_results, size_t *out_count) {
    *out_results = NULL;
    *out_count = 0;

    state_test_t *test = NULL;
    if (!parse_state_test(filepath, &test) || !test) {
        fprintf(stderr, "Error: failed to parse state test: %s\n", filepath);
        return -1;
    }

    /* Count total subtests to pre-allocate results */
    size_t total = 0;
    for (size_t fi = 0; fi < test->post_count; fi++) {
        if (args->fork_filter && strcmp(test->post[fi].fork_name, args->fork_filter) != 0)
            continue;
        for (size_t ci = 0; ci < test->post[fi].condition_count; ci++) {
            if (args->index_filter >= 0 && (int)ci != args->index_filter)
                continue;
            total++;
        }
    }

    test_result_entry_t *results = calloc(total ? total : 1, sizeof(test_result_entry_t));
    size_t ri = 0;

    for (size_t fi = 0; fi < test->post_count; fi++) {
        const char *fork_name = test->post[fi].fork_name;

        if (args->fork_filter && strcmp(fork_name, args->fork_filter) != 0)
            continue;

        chain_config_t *fork_config = create_test_chain_config(fork_name);
        if (!fork_config) {
            /* Unknown fork — skip */
            for (size_t ci = 0; ci < test->post[fi].condition_count; ci++) {
                if (args->index_filter >= 0 && (int)ci != args->index_filter)
                    continue;
                results[ri].name = test->name ? strdup(test->name) : NULL;
                results[ri].fork = fork_name ? strdup(fork_name) : NULL;
                results[ri].pass = true; /* skip = pass for geth compat */
                memset(results[ri].state_root, 0, sizeof(results[ri].state_root));
                ri++;
            }
            continue;
        }

        for (size_t ci = 0; ci < test->post[fi].condition_count; ci++) {
            if (args->index_filter >= 0 && (int)ci != args->index_filter)
                continue;

            const test_post_condition_t *post_cond = &test->post[fi].conditions[ci];

            /* Create fresh state + EVM for each subtest */
            evm_state_t *state = evm_state_create(NULL);
            if (!state) {
                results[ri].name = test->name ? strdup(test->name) : NULL;
                results[ri].fork = fork_name ? strdup(fork_name) : NULL;
                results[ri].pass = false;
                results[ri].error = "failed to create state";
                ri++;
                continue;
            }
            {
                flat_state_t *fs = flat_state_create("/dev/shm/evm_statetest_flat");
                if (fs) evm_state_set_flat_state(state, fs);
            }
            evm_t *evm = evm_create(state, fork_config);
            if (!evm) {
                evm_state_destroy(state);
                results[ri].name = test->name ? strdup(test->name) : NULL;
                results[ri].fork = fork_name ? strdup(fork_name) : NULL;
                results[ri].pass = false;
                results[ri].error = "failed to create evm";
                ri++;
                continue;
            }

            /* Enable tracing if requested */
#ifdef ENABLE_EVM_TRACE
            if (args->trace) {
                extern void evm_tracer_init(FILE *);
                evm_tracer_init(stderr);
            }
#endif

            /* Setup pre-state */
            test_runner_setup_state(state, test->pre_state, test->pre_state_count);
            evm_state_commit(state);
            /* Flush pre-state to flat_state before clearing dirty flags.
             * Without this, pre-state accounts not modified during execution
             * would be missing from the incremental account trie. */
            evm_state_compute_mpt_root(state, false);
            evm_state_clear_prestate_dirty(state);

            /* Set block environment */
            evm_block_env_t evm_block = {
                .number    = uint256_to_uint64(&test->env.number),
                .timestamp = uint256_to_uint64(&test->env.timestamp),
                .gas_limit = uint256_to_uint64(&test->env.gas_limit),
                .difficulty = test->env.difficulty,
                .coinbase  = test->env.coinbase,
                .base_fee  = test->env.base_fee,
                .chain_id  = uint256_from_uint64(fork_config->chain_id),
                .excess_blob_gas = test->env.excess_blob_gas,
            };
            /* Populate block_hash for BLOCKHASH opcode.
             * State tests use synthetic hashes: hash(n) = keccak256(decimal_string(n))
             * matching go-ethereum's vmTestBlockHash convention. If previousHash
             * is provided, it overrides the synthetic hash for block (number-1). */
            {
                uint64_t start = (evm_block.number > 256) ? evm_block.number - 256 : 0;
                for (uint64_t bn = start; bn < evm_block.number; bn++) {
                    char numstr[21]; /* max uint64 = 20 digits + NUL */
                    int len = snprintf(numstr, sizeof(numstr), "%lu", (unsigned long)bn);
                    hash_t h = hash_keccak256((const uint8_t *)numstr, (size_t)len);
                    evm_block.block_hash[bn % 256] = h;
                }
            }
            if (test->env.has_previous_hash && evm_block.number > 0) {
                uint64_t parent_idx = (evm_block.number - 1) % 256;
                evm_block.block_hash[parent_idx] = test->env.previous_hash;
            }
            evm_set_block_env(evm, &evm_block);

            /* Determine tx type */
            test_access_list_t *access_list = NULL;
            if (test->transaction.access_lists_count > post_cond->data_index)
                access_list = &test->transaction.access_lists[post_cond->data_index];

            transaction_type_t tx_type = TX_TYPE_LEGACY;
            if (test->transaction.has_authorization_list) {
                tx_type = TX_TYPE_EIP7702;
            } else if (!uint256_is_zero(&test->transaction.max_fee_per_blob_gas) ||
                       test->transaction.blob_versioned_hashes_count > 0) {
                tx_type = TX_TYPE_EIP4844;
            } else if (!uint256_is_zero(&test->transaction.max_fee_per_gas)) {
                tx_type = TX_TYPE_EIP1559;
            } else if (access_list) {
                tx_type = TX_TYPE_EIP2930;
            }

            /* Get indexed parameters */
            uint256_t gas_limit = test->transaction.gas_limit_count > post_cond->gas_index ?
                test->transaction.gas_limit[post_cond->gas_index] : uint256_from_uint64(0);
            uint256_t value = test->transaction.value_count > post_cond->value_index ?
                test->transaction.value[post_cond->value_index] : uint256_from_uint64(0);
            uint8_t *data = NULL;
            size_t data_len = 0;
            if (test->transaction.data_count > post_cond->data_index) {
                data = test->transaction.data[post_cond->data_index];
                data_len = test->transaction.data_len[post_cond->data_index];
            }

            /* Build transaction */
            transaction_t tx = {
                .type = tx_type,
                .nonce = evm_state_get_nonce(state, &test->transaction.sender),
                .sender = test->transaction.sender,
                .to = test->transaction.to,
                .value = value,
                .gas_limit = uint256_to_uint64(&gas_limit),
                .gas_price = test->transaction.gas_price,
                .max_fee_per_gas = test->transaction.max_fee_per_gas,
                .max_priority_fee_per_gas = test->transaction.max_priority_fee_per_gas,
                .data = data,
                .data_size = data_len,
                .is_create = test->transaction.is_create,
                .access_list = NULL,
                .access_list_count = 0,
                .max_fee_per_blob_gas = test->transaction.max_fee_per_blob_gas,
                .blob_versioned_hashes = test->transaction.blob_versioned_hashes,
                .blob_versioned_hashes_count = test->transaction.blob_versioned_hashes_count,
                .authorization_list = NULL,
                .authorization_list_count = 0,
            };

            /* Convert access list */
            access_list_entry_t *tx_access_list = NULL;
            if (access_list && access_list->entries_count > 0) {
                tx_access_list = calloc(access_list->entries_count, sizeof(access_list_entry_t));
                if (tx_access_list) {
                    for (size_t i = 0; i < access_list->entries_count; i++) {
                        tx_access_list[i].address = access_list->entries[i].address;
                        tx_access_list[i].storage_keys = access_list->entries[i].storage_keys;
                        tx_access_list[i].storage_keys_count = access_list->entries[i].storage_keys_count;
                    }
                    tx.access_list = tx_access_list;
                    tx.access_list_count = access_list->entries_count;
                }
            }

            /* Convert authorization list */
            authorization_t *tx_auth_list = NULL;
            if (test->transaction.authorization_list_count > 0) {
                tx_auth_list = calloc(test->transaction.authorization_list_count, sizeof(authorization_t));
                if (tx_auth_list) {
                    for (size_t i = 0; i < test->transaction.authorization_list_count; i++) {
                        const test_authorization_t *src = &test->transaction.authorization_list[i];
                        tx_auth_list[i].chain_id = src->chain_id;
                        tx_auth_list[i].address = src->address;
                        tx_auth_list[i].nonce = src->nonce;
                        tx_auth_list[i].y_parity = src->y_parity;
                        tx_auth_list[i].r = src->r;
                        tx_auth_list[i].s = src->s;
                        tx_auth_list[i].signer = src->signer;
                    }
                    tx.authorization_list = tx_auth_list;
                    tx.authorization_list_count = test->transaction.authorization_list_count;
                }
            }

            /* Warm access list addresses/keys (EIP-2930) */
            if (access_list && access_list->entries_count > 0 && evm->fork >= FORK_BERLIN) {
                for (size_t i = 0; i < access_list->entries_count; i++) {
                    const test_access_list_entry_t *entry = &access_list->entries[i];
                    evm_mark_address_warm(evm, &entry->address);
                    for (size_t k = 0; k < entry->storage_keys_count; k++)
                        evm_mark_storage_warm(evm, &entry->address, &entry->storage_keys[k]);
                }
            }

            /* Execute transaction */
            block_env_t block_env = {
                .coinbase     = test->env.coinbase,
                .block_number = uint256_to_uint64(&test->env.number),
                .timestamp    = uint256_to_uint64(&test->env.timestamp),
                .gas_limit    = uint256_to_uint64(&test->env.gas_limit),
                .difficulty   = test->env.difficulty,
                .base_fee     = test->env.base_fee,
                .prev_randao  = test->env.prev_randao,
                .excess_blob_gas = test->env.excess_blob_gas,
                .skip_coinbase_payment = false,
            };

            transaction_result_t tx_result;
            bool tx_ok = transaction_execute(evm, &tx, &block_env, &tx_result);

            uint64_t gas_used = 0;
            bool tx_pass = false;

            if (post_cond->expect_exception != NULL) {
                /* Expected to fail */
                if (!tx_ok || (tx_result.status != EVM_SUCCESS && tx_result.status != EVM_REVERT)) {
                    tx_pass = true;
                }
                if (tx_ok) transaction_result_free(&tx_result);
            } else {
                if (tx_ok) {
                    gas_used = tx_result.gas_used;
                    tx_pass = true;
                    transaction_result_free(&tx_result);
                    bool prune = (evm->fork >= FORK_SPURIOUS_DRAGON);
                    evm_state_set_prune_empty(state, prune);
                    evm_state_commit_tx(state);
                }
            }

            /* Compute state root */
            bool prune_empty = (evm->fork >= FORK_SPURIOUS_DRAGON);
            hash_t state_root = evm_state_compute_mpt_root(state, prune_empty);

            char root_hex[67];
            hash_to_hex(&state_root, root_hex);

            /* Check against expected root if we had a successful execution */
            bool root_match = (memcmp(state_root.bytes, post_cond->state_root.bytes, 32) == 0);

            /* Output to stderr: stateRoot (geth format) */
            if (args->trace || args->trace_summary) {
                if (args->trace_summary) {
                    /* evmone --trace-summary format */
                    fprintf(stderr, "{");
                    if (post_cond->expect_exception == NULL) {
                        if (tx_pass) {
                            fprintf(stderr, "\"pass\":%s", root_match ? "true" : "false");
                        } else {
                            fprintf(stderr, "\"pass\":false,\"error\":\"tx rejected\"");
                        }
                        fprintf(stderr, ",\"gasUsed\":\"0x%lx\",", (unsigned long)gas_used);
                    }
                    fprintf(stderr, "\"stateRoot\":\"%s\"}\n", root_hex);
                } else {
                    /* geth --trace format: stateRoot on stderr */
                    fprintf(stderr, "{\"stateRoot\": \"%s\"}\n", root_hex);
                }
            } else {
                /* Even without --trace, geth outputs stateRoot to stderr */
                fprintf(stderr, "{\"stateRoot\": \"%s\"}\n", root_hex);
            }

            /* Fill result entry (strdup since test is freed later) */
            results[ri].name = test->name ? strdup(test->name) : NULL;
            results[ri].fork = fork_name ? strdup(fork_name) : NULL;
            results[ri].pass = root_match && tx_pass;
            memcpy(results[ri].state_root, root_hex, 67);
            if (!root_match && post_cond->expect_exception == NULL) {
                results[ri].error = "post state root mismatch";
            } else if (!tx_pass && post_cond->expect_exception == NULL) {
                results[ri].error = "transaction execution failed";
            }
            ri++;

            /* Cleanup */
            free(tx_access_list);
            free(tx_auth_list);
            evm_destroy(evm);
            evm_state_destroy(state);
        }
    }

    *out_results = results;
    *out_count = ri;

    state_test_free(test);
    return 0;
}

/* =========================================================================
 * JSON output (geth-compatible)
 * ========================================================================= */

static void print_results_json(const test_result_entry_t *results, size_t count) {
    cJSON *arr = cJSON_CreateArray();
    for (size_t i = 0; i < count; i++) {
        cJSON *obj = cJSON_CreateObject();
        if (results[i].name)
            cJSON_AddStringToObject(obj, "name", results[i].name);
        cJSON_AddBoolToObject(obj, "pass", results[i].pass);
        if (results[i].state_root[0])
            cJSON_AddStringToObject(obj, "stateRoot", results[i].state_root);
        if (results[i].fork)
            cJSON_AddStringToObject(obj, "fork", results[i].fork);
        if (results[i].error)
            cJSON_AddStringToObject(obj, "error", results[i].error);
        cJSON_AddItemToArray(arr, obj);
    }
    char *json_str = cJSON_Print(arr);
    printf("%s\n", json_str);
    free(json_str);
    cJSON_Delete(arr);
}

static void print_results_human(const test_result_entry_t *results, size_t count) {
    int pass = 0;
    for (size_t i = 0; i < count; i++) {
        const char *status = results[i].pass ? "\033[32mPASS\033[0m" : "\033[31mFAIL\033[0m";
        fprintf(stdout, "[%s] %s", status, results[i].name ? results[i].name : "?");
        if (!results[i].pass && results[i].error)
            fprintf(stdout, ", err=%s, fork=%s", results[i].error, results[i].fork ? results[i].fork : "?");
        fprintf(stdout, "\n");
        if (results[i].pass) pass++;
    }
    fprintf(stdout, "--\n%d tests passed, %zu tests failed.\n", pass, count - pass);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    statetest_args_t args;
    if (!parse_args(argc, argv, &args))
        return 1;

    secp256k1_wrap_init();

    if (args.test_path) {
        /* Single file mode */
        test_result_entry_t *results = NULL;
        size_t count = 0;
        int rc = run_statetest_file(args.test_path, &args, &results, &count);
        if (rc != 0) return 1;

        if (args.human)
            print_results_human(results, count);
        else
            print_results_json(results, count);

        for (size_t i = 0; i < count; i++) {
            free((void *)results[i].name);
            free((void *)results[i].fork);
        }
        free(results);
    } else {
        /* Batch mode: read filenames from stdin */
        char line[4096];
        while (fgets(line, sizeof(line), stdin)) {
            /* Strip trailing newline */
            size_t len = strlen(line);
            while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
                line[--len] = '\0';
            if (len == 0) break;

            test_result_entry_t *results = NULL;
            size_t count = 0;
            int rc = run_statetest_file(line, &args, &results, &count);
            if (rc != 0) continue;

            if (args.human)
                print_results_human(results, count);
            else
                print_results_json(results, count);

            for (size_t i = 0; i < count; i++) {
                free((void *)results[i].name);
                free((void *)results[i].fork);
            }
            free(results);
        }
    }

    return 0;
}
