/**
 * Test Runner - Core Implementation
 */

#include "test_runner.h"
#include "fork.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

//==============================================================================
// Test Chain Configuration
//==============================================================================

/**
 * Create a chain config for testing where all forks up to the specified fork
 * are activated at block 0
 */
chain_config_t *create_test_chain_config(const char *fork_name) {
    static chain_config_t test_config = {
        .chain_id = 1,
        .name = "test",
        .fork_blocks = {0}
    };

    // If no fork specified, use sepolia config (all forks at block 0 up to latest)
    if (!fork_name) {
        return (chain_config_t *)chain_config_sepolia();
    }

    // Start with all forks disabled
    test_config.fork_blocks.frontier = 0;  // Always active
    test_config.fork_blocks.homestead = UINT64_MAX;
    test_config.fork_blocks.tangerine_whistle = UINT64_MAX;
    test_config.fork_blocks.spurious_dragon = UINT64_MAX;
    test_config.fork_blocks.byzantium = UINT64_MAX;
    test_config.fork_blocks.constantinople = UINT64_MAX;
    test_config.fork_blocks.petersburg = UINT64_MAX;
    test_config.fork_blocks.istanbul = UINT64_MAX;
    test_config.fork_blocks.muir_glacier = UINT64_MAX;
    test_config.fork_blocks.berlin = UINT64_MAX;
    test_config.fork_blocks.london = UINT64_MAX;
    test_config.fork_blocks.arrow_glacier = UINT64_MAX;
    test_config.fork_blocks.gray_glacier = UINT64_MAX;
    test_config.fork_blocks.paris = UINT64_MAX;
    test_config.fork_blocks.shanghai = UINT64_MAX;
    test_config.fork_blocks.cancun = UINT64_MAX;
    test_config.fork_blocks.prague = UINT64_MAX;
    test_config.fork_blocks.osaka = UINT64_MAX;
    test_config.fork_blocks.verkle = UINT64_MAX;

    // Enable forks cumulatively up to the target fork.
    // Fork chronology: Frontier → Homestead → Tangerine Whistle → Spurious Dragon
    //   → Byzantium → Constantinople/Petersburg → Istanbul → Berlin → London
    //   → Paris → Shanghai → Cancun → Prague
    if (strcmp(fork_name, "Frontier") == 0) {
        // Only Frontier active (already set above)
    } else if (strcmp(fork_name, "Homestead") == 0) {
        test_config.fork_blocks.homestead = 0;
    } else if (strcmp(fork_name, "Tangerine Whistle") == 0 ||
               strcmp(fork_name, "EIP150") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
    } else if (strcmp(fork_name, "Spurious Dragon") == 0 ||
               strcmp(fork_name, "EIP158") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
    } else if (strcmp(fork_name, "Byzantium") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
    } else if (strcmp(fork_name, "Constantinople") == 0) {
        // Constantinople: EIP-1283 active (net gas metering for SSTORE)
        // Do NOT enable Petersburg — it reverts EIP-1283
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
    } else if (strcmp(fork_name, "ConstantinopleFix") == 0 ||
               strcmp(fork_name, "Petersburg") == 0) {
        // Petersburg (ConstantinopleFix): reverts EIP-1283
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
    } else if (strcmp(fork_name, "Istanbul") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
    } else if (strcmp(fork_name, "MuirGlacier") == 0) {
        // Difficulty bomb delay only — no EVM changes vs Istanbul
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
    } else if (strcmp(fork_name, "Berlin") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
    } else if (strcmp(fork_name, "London") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
    } else if (strcmp(fork_name, "ArrowGlacier") == 0) {
        // Difficulty bomb delay only — no EVM changes vs London
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
    } else if (strcmp(fork_name, "GrayGlacier") == 0) {
        // Difficulty bomb delay only — no EVM changes vs London
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
    } else if (strcmp(fork_name, "Paris") == 0 ||
               strcmp(fork_name, "Merge") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
    } else if (strcmp(fork_name, "Shanghai") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 0;
    } else if (strcmp(fork_name, "Cancun") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 0;
        test_config.fork_blocks.cancun = 0;
    } else if (strcmp(fork_name, "Prague") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 0;
        test_config.fork_blocks.cancun = 0;
        test_config.fork_blocks.prague = 0;
    } else if (strcmp(fork_name, "Osaka") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 0;
        test_config.fork_blocks.cancun = 0;
        test_config.fork_blocks.prague = 0;
        test_config.fork_blocks.osaka = 0;
    } else if (strcmp(fork_name, "Verkle") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 0;
        test_config.fork_blocks.cancun = 0;
        test_config.fork_blocks.prague = 0;
        test_config.fork_blocks.osaka = 0;
        test_config.fork_blocks.verkle = 0;
    } else if (strcmp(fork_name, "ParisToShanghaiAtTime15k") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 15000; // timestamp
    } else if (strcmp(fork_name, "ShanghaiToCancunAtTime15k") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 0;
        test_config.fork_blocks.cancun = 15000; // timestamp
    } else if (strcmp(fork_name, "CancunToPragueAtTime15k") == 0) {
        test_config.fork_blocks.homestead = 0;
        test_config.fork_blocks.tangerine_whistle = 0;
        test_config.fork_blocks.spurious_dragon = 0;
        test_config.fork_blocks.byzantium = 0;
        test_config.fork_blocks.constantinople = 0;
        test_config.fork_blocks.petersburg = 0;
        test_config.fork_blocks.istanbul = 0;
        test_config.fork_blocks.muir_glacier = 0;
        test_config.fork_blocks.berlin = 0;
        test_config.fork_blocks.london = 0;
        test_config.fork_blocks.arrow_glacier = 0;
        test_config.fork_blocks.gray_glacier = 0;
        test_config.fork_blocks.paris = 0;
        test_config.fork_blocks.shanghai = 0;
        test_config.fork_blocks.cancun = 0;
        test_config.fork_blocks.prague = 15000; // timestamp
    } else {
        // Unknown fork — return NULL to signal skip
        return NULL;
    }

    return &test_config;
}

//==============================================================================
// Helper Functions
//==============================================================================

bool hash_equals(const hash_t *a, const hash_t *b) {
    if (!a || !b) return false;
    return memcmp(a->bytes, b->bytes, 32) == 0;
}

char *hash_to_hex_string(const hash_t *hash) {
    if (!hash) return NULL;
    
    char *str = malloc(67); // "0x" + 64 chars + null
    if (!str) return NULL;
    
    str[0] = '0';
    str[1] = 'x';
    
    for (int i = 0; i < 32; i++) {
        sprintf(str + 2 + (i * 2), "%02x", hash->bytes[i]);
    }
    
    return str;
}

char *uint256_to_hex_string(const uint256_t *value) {
    if (!value) return NULL;
    
    // Convert uint256 to bytes
    uint8_t bytes[32];
    uint256_to_bytes(value, bytes);
    
    // Find first non-zero byte to avoid leading zeros
    int start = 0;
    while (start < 32 && bytes[start] == 0) {
        start++;
    }
    
    if (start == 32) {
        return strdup("0x0");
    }
    
    int len = (32 - start) * 2;
    char *str = malloc(3 + len); // "0x" + hex + null
    if (!str) return NULL;
    
    str[0] = '0';
    str[1] = 'x';
    
    for (int i = start; i < 32; i++) {
        sprintf(str + 2 + ((i - start) * 2), "%02x", bytes[i]);
    }
    
    return str;
}

uint64_t get_time_microseconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
}

//==============================================================================
// Temp directory helpers (for flat verkle backend)
//==============================================================================

#ifdef ENABLE_VERKLE
static void rm_rf(const char *path) {
    if (!path || !path[0]) return;
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    system(cmd);
}

/* Flat backend temp dirs — stored per runner instance */
static __thread char flat_value_dir[256];
static __thread char flat_commit_dir[256];

static void make_flat_dirs(void) {
    snprintf(flat_value_dir, sizeof(flat_value_dir), "/tmp/vk_val_XXXXXX");
    snprintf(flat_commit_dir, sizeof(flat_commit_dir), "/tmp/vk_com_XXXXXX");
    mkdtemp(flat_value_dir);
    mkdtemp(flat_commit_dir);
}

static void cleanup_flat_dirs(void) {
    rm_rf(flat_value_dir);
    rm_rf(flat_commit_dir);
    flat_value_dir[0] = '\0';
    flat_commit_dir[0] = '\0';
}
#endif /* ENABLE_VERKLE */

//==============================================================================
// Test Runner Lifecycle
//==============================================================================

bool test_runner_init(test_runner_t *runner, const test_runner_config_t *config) {
    if (!runner) return false;

    memset(runner, 0, sizeof(*runner));

    // Set configuration
    if (config) {
        runner->config = *config;
    } else {
        // Default config
        runner->config.verbose = false;
        runner->config.stop_on_fail = false;
        runner->config.fork_filter = NULL;
        runner->config.fork_filter_count = 0;
        runner->config.timeout_ms = 30000; // 30 second default timeout
    }

    // Initialize state backends + EVM State
#ifdef ENABLE_VERKLE
    make_flat_dirs();
    runner->vs = verkle_state_create_flat(flat_value_dir, flat_commit_dir);
    if (!runner->vs) {
        cleanup_flat_dirs();
        return false;
    }
#endif

    runner->state = evm_state_create(
#ifdef ENABLE_VERKLE
        runner->vs,
#else
        NULL,
#endif
        NULL,  /* no mpt_store for tests — use in-memory batch rebuild */
        NULL   /* no code_store for tests */
    );
    if (!runner->state) {
#ifdef ENABLE_VERKLE
        verkle_state_destroy(runner->vs);
        runner->vs = NULL;
        cleanup_flat_dirs();
#endif
        return false;
    }

    // Initialize persistent mpt_store if requested
#ifdef ENABLE_MPT
    if (config && config->mpt_store) {
        if (!evm_state_init_mpt_stores(runner->state, "/dev/shm/test_runner_mpt",
                                        4096, 65536)) {
            fprintf(stderr, "ERROR: Failed to initialize mpt_store\n");
            evm_state_destroy(runner->state);
            runner->state = NULL;
#ifdef ENABLE_VERKLE
            verkle_state_destroy(runner->vs);
            runner->vs = NULL;
            cleanup_flat_dirs();
#endif
            return false;
        }
        // Save mpt_store pointers for reuse across resets (avoids file recreation)
        evm_state_detach_mpt_stores(runner->state,
                                     &runner->account_mpt, &runner->storage_mpt);
        // Re-attach (they're now owned by runner, detach prevents double-free)
        evm_state_attach_mpt_stores(runner->state,
                                     runner->account_mpt, runner->storage_mpt);
    }
#endif

    // Initialize EVM
    runner->evm = evm_create(runner->state, NULL); // NULL = use default mainnet config
    if (!runner->evm) {
        evm_state_destroy(runner->state);
        runner->state = NULL;
#ifdef ENABLE_VERKLE
        verkle_state_destroy(runner->vs);
        runner->vs = NULL;
        cleanup_flat_dirs();
#endif
        return false;
    }

    return true;
}

void test_runner_destroy(test_runner_t *runner) {
    if (!runner) return;

    if (runner->evm) {
        evm_destroy(runner->evm);
    }

    // Detach mpt stores before destroying state (runner owns them)
    if (runner->state) {
        evm_state_detach_mpt_stores(runner->state, NULL, NULL);
        evm_state_destroy(runner->state);
    }

#ifdef ENABLE_MPT
    // Destroy runner-owned mpt stores
    if (runner->account_mpt)
        mpt_store_destroy((mpt_store_t *)runner->account_mpt);
    if (runner->storage_mpt)
        mpt_store_destroy((mpt_store_t *)runner->storage_mpt);
#endif

#ifdef ENABLE_VERKLE
    if (runner->vs) {
        verkle_state_destroy(runner->vs);
    }

    cleanup_flat_dirs();
#endif

    memset(runner, 0, sizeof(*runner));
}

void test_runner_reset(test_runner_t *runner) {
    if (!runner) return;

    // Destroy old state (detach mpt stores first — runner owns them)
    if (runner->evm) {
        evm_destroy(runner->evm);
        runner->evm = NULL;
    }
    if (runner->state) {
        evm_state_detach_mpt_stores(runner->state, NULL, NULL);
        evm_state_destroy(runner->state);
        runner->state = NULL;
    }
#ifdef ENABLE_VERKLE
    if (runner->vs) {
        verkle_state_destroy(runner->vs);
        runner->vs = NULL;
    }
    cleanup_flat_dirs();
#endif

    // Recreate fresh
#ifdef ENABLE_VERKLE
    make_flat_dirs();
    runner->vs = verkle_state_create_flat(flat_value_dir, flat_commit_dir);
    if (runner->vs) {
#endif
        runner->state = evm_state_create(
#ifdef ENABLE_VERKLE
            runner->vs,
#else
            NULL,
#endif
            NULL,  /* no mpt_store — runner owns stores separately */
            NULL   /* no code_store for tests */
        );
        if (runner->state) {
            // Re-attach runner-owned mpt stores (resets them in-place)
            if (runner->account_mpt && runner->storage_mpt) {
                evm_state_attach_mpt_stores(runner->state,
                                             runner->account_mpt,
                                             runner->storage_mpt);
            }
            runner->evm = evm_create(runner->state, NULL);
        }
#ifdef ENABLE_VERKLE
    }
#endif

    runner->total_gas_used = 0;
    runner->total_transactions = 0;
}

//==============================================================================
// Pre-state Setup
//==============================================================================

bool test_runner_setup_state(evm_state_t *state,
                             const test_account_t *accounts,
                             size_t count) {
    if (!state) return false;
    if (count == 0) return true;  /* empty pre-state is valid */
    if (!accounts) return false;

    for (size_t i = 0; i < count; i++) {
        const test_account_t *acc = &accounts[i];

        // Set nonce (convert uint256 to uint64)
        uint64_t nonce = uint256_to_uint64(&acc->nonce);
        evm_state_set_nonce(state, &acc->address, nonce);

        // Set balance
        evm_state_set_balance(state, &acc->address, &acc->balance);

        // Set code if exists
        if (acc->code && acc->code_len > 0) {
            evm_state_set_code(state, &acc->address, acc->code, (uint32_t)acc->code_len);
        }

        // Set storage slots (skip zero-value entries — not part of trie)
        for (size_t j = 0; j < acc->storage_count; j++) {
            const test_storage_entry_t *entry = &acc->storage[j];
            if (uint256_is_zero(&entry->value)) continue;
            evm_state_set_storage(state, &acc->address, &entry->key, &entry->value);
        }

        // Mark as existing in pre-state — even empty accounts must persist
        // in the trie unless touched and pruned by EIP-161 during execution.
        evm_state_mark_existed(state, &acc->address);
    }

    return true;
}

//==============================================================================
// State Root Verification
//==============================================================================

bool test_runner_verify_state_root(evm_state_t *state,
                                   const hash_t *expected_root,
                                   hash_t *actual_root) {
    if (!state || !expected_root) return false;

#ifdef ENABLE_MPT
    hash_t computed_root = evm_state_compute_mpt_root(state, true);
#else
    hash_t computed_root = evm_state_compute_state_root_ex(state, true);
#endif

    if (actual_root) {
        *actual_root = computed_root;
    }

    return hash_equal(&computed_root, expected_root);
}

//==============================================================================
// Result Management
//==============================================================================

void test_result_init(test_result_t *result) {
    if (!result) return;
    memset(result, 0, sizeof(*result));
    result->status = TEST_PASS;
}

void test_result_free(test_result_t *result) {
    if (!result) return;
    
    free(result->test_name);
    free(result->fork);
    free(result->skip_reason);
    
    for (size_t i = 0; i < result->failure_count; i++) {
        free(result->failures[i].field);
        free(result->failures[i].expected);
        free(result->failures[i].actual);
        free(result->failures[i].message);
    }
    free(result->failures);
    
    memset(result, 0, sizeof(*result));
}

void test_result_add_failure(test_result_t *result,
                            const char *field,
                            const char *expected,
                            const char *actual,
                            const char *message) {
    if (!result) return;
    
    result->status = TEST_FAIL;
    
    size_t new_count = result->failure_count + 1;
    test_failure_t *new_failures = realloc(result->failures, 
                                           new_count * sizeof(test_failure_t));
    if (!new_failures) return;
    
    result->failures = new_failures;
    test_failure_t *failure = &result->failures[result->failure_count];
    result->failure_count = new_count;
    
    failure->field = field ? strdup(field) : NULL;
    failure->expected = expected ? strdup(expected) : NULL;
    failure->actual = actual ? strdup(actual) : NULL;
    failure->message = message ? strdup(message) : NULL;
}

void test_results_init(test_results_t *results) {
    if (!results) return;
    memset(results, 0, sizeof(*results));
}

void test_results_free(test_results_t *results) {
    if (!results) return;
    
    for (size_t i = 0; i < results->result_count; i++) {
        test_result_free(&results->results[i]);
    }
    free(results->results);
    
    memset(results, 0, sizeof(*results));
}

void test_results_add(test_results_t *results, const test_result_t *result) {
    if (!results || !result) return;
    
    size_t new_count = results->result_count + 1;
    test_result_t *new_results = realloc(results->results,
                                         new_count * sizeof(test_result_t));
    if (!new_results) return;
    
    results->results = new_results;
    
    // Copy result
    test_result_t *dest = &results->results[results->result_count];
    memcpy(dest, result, sizeof(test_result_t));
    
    // Duplicate strings
    dest->test_name = result->test_name ? strdup(result->test_name) : NULL;
    dest->fork = result->fork ? strdup(result->fork) : NULL;
    dest->skip_reason = result->skip_reason ? strdup(result->skip_reason) : NULL;
    
    // Duplicate failures
    if (result->failure_count > 0) {
        dest->failures = malloc(result->failure_count * sizeof(test_failure_t));
        for (size_t i = 0; i < result->failure_count; i++) {
            dest->failures[i].field = result->failures[i].field ? 
                strdup(result->failures[i].field) : NULL;
            dest->failures[i].expected = result->failures[i].expected ? 
                strdup(result->failures[i].expected) : NULL;
            dest->failures[i].actual = result->failures[i].actual ? 
                strdup(result->failures[i].actual) : NULL;
            dest->failures[i].message = result->failures[i].message ? 
                strdup(result->failures[i].message) : NULL;
        }
    }
    
    results->result_count = new_count;
    results->total++;
    
    // Update counters
    switch (result->status) {
        case TEST_PASS:
            results->passed++;
            break;
        case TEST_FAIL:
            results->failed++;
            break;
        case TEST_ERROR:
            results->errors++;
            break;
        case TEST_SKIP:
            results->skipped++;
            break;
        default:
            break;
    }
}

void test_results_print(const test_results_t *results, bool verbose) {
    if (!results) return;
    
    printf("\n");
    printf("================================================================================\n");
    printf("Test Results Summary\n");
    printf("================================================================================\n");
    printf("Total:   %zu\n", results->total);
    printf("Passed:  %zu (%.1f%%)\n", results->passed, 
           results->total > 0 ? (100.0 * results->passed / results->total) : 0.0);
    printf("Failed:  %zu\n", results->failed);
    printf("Errors:  %zu\n", results->errors);
    printf("Skipped: %zu\n", results->skipped);
    printf("================================================================================\n");
    
    if (verbose && results->result_count > 0) {
        printf("\nDetailed Results:\n");
        printf("--------------------------------------------------------------------------------\n");
        
        for (size_t i = 0; i < results->result_count; i++) {
            const test_result_t *r = &results->results[i];
            
            const char *status_str = "UNKNOWN";
            switch (r->status) {
                case TEST_PASS: status_str = "PASS"; break;
                case TEST_FAIL: status_str = "FAIL"; break;
                case TEST_ERROR: status_str = "ERROR"; break;
                case TEST_SKIP: status_str = "SKIP"; break;
                default: break;
            }
            
            printf("[%s] %s", status_str, r->test_name ? r->test_name : "(unnamed)");
            if (r->fork) {
                printf(" (%s)", r->fork);
            }
            printf(" [%.2f ms]\n", r->duration_us / 1000.0);
            
            if (r->status == TEST_SKIP && r->skip_reason) {
                printf("  Reason: %s\n", r->skip_reason);
            }
            
            if (r->status == TEST_FAIL && r->failure_count > 0) {
                for (size_t j = 0; j < r->failure_count; j++) {
                    const test_failure_t *f = &r->failures[j];
                    printf("  Failure in '%s':\n", f->field ? f->field : "unknown");
                    printf("    Expected: %s\n", f->expected ? f->expected : "(none)");
                    printf("    Actual:   %s\n", f->actual ? f->actual : "(none)");
                    if (f->message) {
                        printf("    Message:  %s\n", f->message);
                    }
                }
            }
        }
    }
    
    printf("\n");
}
