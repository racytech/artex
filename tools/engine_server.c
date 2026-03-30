/*
 * Standalone Engine API server with EVM execution.
 *
 * Usage:
 *   ./engine_server --jwt-secret jwt.hex --fixture test.json [--port 8551]
 *
 * Loads pre-state from a blockchain_tests_engine fixture file, initializes
 * the EVM, and runs the Engine API HTTP server with full block execution.
 * The mock CL script (tools/mock_cl.py) can then send newPayload calls
 * that are fully validated (state root, receipt root, bloom, gas).
 *
 * Without --fixture, runs in stub mode (no EVM, returns SYNCING/VALID).
 */

#include "engine.h"
#include "evm.h"
#include "evm_state.h"
#include "fork.h"
#include "test_parser.h"
#include "test_fixtures.h"

#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

static engine_t *g_engine = NULL;

static void sigint_handler(int sig) {
    (void)sig;
    if (g_engine)
        engine_stop(g_engine);
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --jwt-secret <path> [--fixture <test.json>] [--fork <name>]\n"
        "         [--port <port>] [--host <addr>]\n"
        "\n"
        "  --jwt-secret  Path to hex-encoded JWT secret file (required)\n"
        "  --fixture     Path to blockchain_tests_engine JSON fixture\n"
        "  --fork        Fork name override (Paris, Shanghai, Cancun, etc.)\n"
        "  --port        Listen port (default: 8551)\n"
        "  --host        Listen address (default: 127.0.0.1)\n",
        prog);
    exit(1);
}

/* =========================================================================
 * Fork configuration (same as test_runner_core.c)
 * ========================================================================= */

static chain_config_t g_chain_config;

static chain_config_t *setup_fork_config(const char *fork_name) {
    memset(&g_chain_config, 0, sizeof(g_chain_config));
    g_chain_config.chain_id = 1;
    g_chain_config.name = "engine-server";

    /* Start with all forks disabled */
    g_chain_config.fork_blocks.frontier = 0;
    g_chain_config.fork_blocks.homestead = UINT64_MAX;
    g_chain_config.fork_blocks.tangerine_whistle = UINT64_MAX;
    g_chain_config.fork_blocks.spurious_dragon = UINT64_MAX;
    g_chain_config.fork_blocks.byzantium = UINT64_MAX;
    g_chain_config.fork_blocks.constantinople = UINT64_MAX;
    g_chain_config.fork_blocks.petersburg = UINT64_MAX;
    g_chain_config.fork_blocks.istanbul = UINT64_MAX;
    g_chain_config.fork_blocks.muir_glacier = UINT64_MAX;
    g_chain_config.fork_blocks.berlin = UINT64_MAX;
    g_chain_config.fork_blocks.london = UINT64_MAX;
    g_chain_config.fork_blocks.arrow_glacier = UINT64_MAX;
    g_chain_config.fork_blocks.gray_glacier = UINT64_MAX;
    g_chain_config.fork_blocks.paris = UINT64_MAX;
    g_chain_config.fork_blocks.shanghai = UINT64_MAX;
    g_chain_config.fork_blocks.cancun = UINT64_MAX;
    g_chain_config.fork_blocks.prague = UINT64_MAX;
    g_chain_config.fork_blocks.osaka = UINT64_MAX;
    g_chain_config.fork_blocks.verkle = UINT64_MAX;

    /* Enable forks cumulatively */
    if (!fork_name || strcmp(fork_name, "Paris") == 0 ||
        strcmp(fork_name, "Merge") == 0) {
        g_chain_config.fork_blocks.homestead = 0;
        g_chain_config.fork_blocks.tangerine_whistle = 0;
        g_chain_config.fork_blocks.spurious_dragon = 0;
        g_chain_config.fork_blocks.byzantium = 0;
        g_chain_config.fork_blocks.constantinople = 0;
        g_chain_config.fork_blocks.petersburg = 0;
        g_chain_config.fork_blocks.istanbul = 0;
        g_chain_config.fork_blocks.berlin = 0;
        g_chain_config.fork_blocks.london = 0;
        g_chain_config.fork_blocks.paris = 0;
    } else if (strcmp(fork_name, "Shanghai") == 0) {
        g_chain_config.fork_blocks.homestead = 0;
        g_chain_config.fork_blocks.tangerine_whistle = 0;
        g_chain_config.fork_blocks.spurious_dragon = 0;
        g_chain_config.fork_blocks.byzantium = 0;
        g_chain_config.fork_blocks.constantinople = 0;
        g_chain_config.fork_blocks.petersburg = 0;
        g_chain_config.fork_blocks.istanbul = 0;
        g_chain_config.fork_blocks.berlin = 0;
        g_chain_config.fork_blocks.london = 0;
        g_chain_config.fork_blocks.paris = 0;
        g_chain_config.fork_blocks.shanghai = 0;
    } else if (strcmp(fork_name, "Cancun") == 0) {
        g_chain_config.fork_blocks.homestead = 0;
        g_chain_config.fork_blocks.tangerine_whistle = 0;
        g_chain_config.fork_blocks.spurious_dragon = 0;
        g_chain_config.fork_blocks.byzantium = 0;
        g_chain_config.fork_blocks.constantinople = 0;
        g_chain_config.fork_blocks.petersburg = 0;
        g_chain_config.fork_blocks.istanbul = 0;
        g_chain_config.fork_blocks.berlin = 0;
        g_chain_config.fork_blocks.london = 0;
        g_chain_config.fork_blocks.paris = 0;
        g_chain_config.fork_blocks.shanghai = 0;
        g_chain_config.fork_blocks.cancun = 0;
    } else if (strcmp(fork_name, "Prague") == 0) {
        g_chain_config.fork_blocks.homestead = 0;
        g_chain_config.fork_blocks.tangerine_whistle = 0;
        g_chain_config.fork_blocks.spurious_dragon = 0;
        g_chain_config.fork_blocks.byzantium = 0;
        g_chain_config.fork_blocks.constantinople = 0;
        g_chain_config.fork_blocks.petersburg = 0;
        g_chain_config.fork_blocks.istanbul = 0;
        g_chain_config.fork_blocks.berlin = 0;
        g_chain_config.fork_blocks.london = 0;
        g_chain_config.fork_blocks.paris = 0;
        g_chain_config.fork_blocks.shanghai = 0;
        g_chain_config.fork_blocks.cancun = 0;
        g_chain_config.fork_blocks.prague = 0;
    } else {
        fprintf(stderr, "Unknown fork: %s\n", fork_name);
        return NULL;
    }

    return &g_chain_config;
}

/* =========================================================================
 * Load pre-state from first test case in fixture file
 * ========================================================================= */

static bool load_prestate(const char *fixture_path, const char *fork_override,
                          evm_state_t *state, evm_t *evm) {
    cJSON *root = load_json_file(fixture_path);
    if (!root) {
        fprintf(stderr, "Failed to load fixture: %s\n", fixture_path);
        return false;
    }

    /* Find test case matching fork filter */
    cJSON *test_obj = NULL;
    cJSON *item;
    cJSON_ArrayForEach(item, root) {
        if (!cJSON_IsObject(item)) continue;
        if (fork_override) {
            const char *net = NULL;
            if (json_get_string(item, "network", &net) &&
                strcasecmp(net, fork_override) == 0) {
                test_obj = item;
                break;
            }
        } else {
            test_obj = item;  /* Take first if no filter */
            break;
        }
    }
    if (!test_obj) {
        fprintf(stderr, "No test case found matching fork '%s'\n",
                fork_override ? fork_override : "(any)");
        cJSON_Delete(root);
        return false;
    }

    printf("  Test: %s\n", test_obj->string ? test_obj->string : "(unnamed)");

    /* Determine fork */
    const char *network = fork_override;
    if (!network) {
        const char *net_str = NULL;
        if (json_get_string(test_obj, "network", &net_str))
            network = net_str;
    }
    if (!network) network = "Paris";

    printf("  Fork: %s\n", network);

    chain_config_t *fork_config = setup_fork_config(network);
    if (!fork_config) {
        cJSON_Delete(root);
        return false;
    }
    evm->chain_config = fork_config;

    /* Parse pre-state accounts */
    const cJSON *pre_json = json_get_object(test_obj, "pre");
    if (!pre_json) {
        printf("  No pre-state found (stub mode)\n");
        cJSON_Delete(root);
        return true;
    }

    test_account_t *accounts = NULL;
    size_t account_count = 0;
    if (!parse_account_map(pre_json, &accounts, &account_count)) {
        fprintf(stderr, "Failed to parse pre-state accounts\n");
        cJSON_Delete(root);
        return false;
    }

    printf("  Pre-state: %zu accounts\n", account_count);

    /* Load accounts into EVM state */
    evm_state_begin_block(state, 0);

    for (size_t i = 0; i < account_count; i++) {
        const test_account_t *acc = &accounts[i];

        evm_state_create_account(state, &acc->address);

        uint64_t nonce = uint256_to_uint64(&acc->nonce);
        evm_state_set_nonce(state, &acc->address, nonce);
        evm_state_set_balance(state, &acc->address, &acc->balance);

        if (acc->code && acc->code_len > 0)
            evm_state_set_code(state, &acc->address, acc->code,
                               (uint32_t)acc->code_len);

        for (size_t j = 0; j < acc->storage_count; j++) {
            evm_state_set_storage(state, &acc->address,
                                  &acc->storage[j].key,
                                  &acc->storage[j].value);
        }
    }

    /* Commit genesis state */
    evm_state_commit(state);
    evm_state_finalize(state);

    /* Free parsed accounts */
    for (size_t i = 0; i < account_count; i++) {
        free(accounts[i].code);
        free(accounts[i].storage);
    }
    free(accounts);

    /* Store genesis block hash so first payload's parent is found */
    const char *genesis_hash_str = NULL;
    const cJSON *genesis_hdr = json_get_object(test_obj, "genesisBlockHeader");
    if (genesis_hdr && json_get_string(genesis_hdr, "hash", &genesis_hash_str)) {
        hash_t genesis_hash;
        if (parse_hash(genesis_hash_str, &genesis_hash)) {
            /* Seed the engine store with genesis hash */
            engine_store_t *store = engine_get_store(g_engine);
            if (store) {
                engine_store_record_blockhash(store, 0, genesis_hash.bytes);

                /* Create a minimal genesis payload to store */
                execution_payload_t genesis_payload;
                memset(&genesis_payload, 0, sizeof(genesis_payload));
                genesis_payload.block_number = 0;
                memcpy(genesis_payload.block_hash, genesis_hash.bytes, 32);
                engine_store_put(store, &genesis_payload, true);
            }
        }
    }

    cJSON_Delete(root);
    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    engine_config_t config = {0};
    config.host = "127.0.0.1";
    config.port = 8551;

    const char *fixture_path = NULL;
    const char *fork_name = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--jwt-secret") == 0 && i + 1 < argc) {
            config.jwt_secret_path = argv[++i];
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            config.port = (uint16_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            config.host = argv[++i];
        } else if (strcmp(argv[i], "--fixture") == 0 && i + 1 < argc) {
            fixture_path = argv[++i];
        } else if (strcmp(argv[i], "--fork") == 0 && i + 1 < argc) {
            fork_name = argv[++i];
        } else {
            usage(argv[0]);
        }
    }

    if (!config.jwt_secret_path) {
        fprintf(stderr, "Error: --jwt-secret is required\n");
        usage(argv[0]);
    }

    /* Create EVM if fixture provided */
    evm_state_t *state = NULL;
    evm_t *evm = NULL;

    if (fixture_path) {
        state = evm_state_create(NULL);
        if (!state) {
            fprintf(stderr, "Failed to create EVM state\n");
            return 1;
        }

        evm = evm_create(state, NULL);
        if (!evm) {
            fprintf(stderr, "Failed to create EVM\n");
            evm_state_destroy(state);
            return 1;
        }

        config.evm = evm;
        config.evm_state = state;
    }

    /* Create engine */
    g_engine = engine_create(&config);
    if (!g_engine) {
        fprintf(stderr, "Failed to create engine\n");
        if (evm) evm_destroy(evm);
        if (state) evm_state_destroy(state);
        return 1;
    }

    /* Load pre-state after engine is created (need store for genesis hash) */
    if (fixture_path) {
        printf("Loading fixture: %s\n", fixture_path);
        if (!load_prestate(fixture_path, fork_name, state, evm)) {
            fprintf(stderr, "Failed to load pre-state\n");
            engine_destroy(g_engine);
            if (evm) evm_destroy(evm);
            if (state) evm_state_destroy(state);
            return 1;
        }
    } else {
        printf("No fixture — running in stub mode (no EVM execution)\n");
    }

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    printf("Engine API server starting...\n");
    int rc = engine_run(g_engine);

    engine_destroy(g_engine);
    if (evm) evm_destroy(evm);
    if (state) evm_state_destroy(state);
    return rc;
}
