/**
 * Transaction Execution Layer Implementation
 */

#include "transaction.h"
#include "evm.h"
#include "opcodes/create.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef ENABLE_DEBUG
extern bool g_trace_calls __attribute__((weak));
#else
static const bool g_trace_calls = false;
#endif

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Count zero bytes in calldata using 8-byte chunks.
 * Collapses each byte to a 0/1 indicator via bit folding, then sums
 * with the multiply-and-shift trick for SWAR byte reduction.
 */
static inline size_t count_zero_bytes(const uint8_t *data, size_t len) {
    size_t zeros = 0;
    size_t i = 0;

    for (; i + 8 <= len; i += 8) {
        uint64_t w;
        memcpy(&w, data + i, 8);
        if (w == 0) { zeros += 8; continue; }
        /* Fold each byte's bits into its LSB: 1 = nonzero, 0 = zero */
        uint64_t t = w;
        t |= (t >> 4);
        t |= (t >> 2);
        t |= (t >> 1);
        t &= 0x0101010101010101ULL;
        /* Sum 8 indicator bytes via multiply-shift */
        size_t nonzero = (t * 0x0101010101010101ULL) >> 56;
        zeros += 8 - nonzero;
    }

    for (; i < len; i++)
        if (data[i] == 0) zeros++;

    return zeros;
}

/**
 * Calculate intrinsic gas and floor data gas in a single pass over calldata.
 * Counts zero bytes once, derives both values from the count.
 */
static void calculate_gas_pair(const transaction_t *tx, evm_fork_t fork,
                                uint64_t *out_intrinsic, uint64_t *out_floor) {
    const uint64_t G_TRANSACTION = 21000;
    const uint64_t G_TX_DATA_ZERO = 4;
    const uint64_t G_TX_DATA_NONZERO_ISTANBUL = 16;   // EIP-2028 (Istanbul+)
    const uint64_t G_TX_DATA_NONZERO_LEGACY = 68;     // Pre-Istanbul
    const uint64_t G_TX_CREATE = 32000;
    const uint64_t G_ACCESS_LIST_ADDRESS = 2400;  // EIP-2930
    const uint64_t G_ACCESS_LIST_STORAGE_KEY = 1900;  // EIP-2930

    uint64_t g_tx_data_nonzero = (fork >= FORK_ISTANBUL) ?
        G_TX_DATA_NONZERO_ISTANBUL : G_TX_DATA_NONZERO_LEGACY;

    uint64_t gas = G_TRANSACTION;

    // Add cost for contract creation (Homestead+, EIP-2)
    if (tx->is_create && fork >= FORK_HOMESTEAD) {
        gas += G_TX_CREATE;

        // EIP-3860 (Shanghai+): initcode word gas
        if (fork >= FORK_SHANGHAI && tx->data_size > 0) {
            uint64_t words = (tx->data_size + 31) / 32;
            gas += 2 * words;  // INITCODE_WORD_GAS = 2
        }
    }

    // Count zero bytes once, derive both intrinsic gas and floor data gas
    size_t zero_bytes = 0;
    size_t nonzero_bytes = 0;
    if (tx->data && tx->data_size > 0) {
        zero_bytes = count_zero_bytes(tx->data, tx->data_size);
        nonzero_bytes = tx->data_size - zero_bytes;
        gas += zero_bytes * G_TX_DATA_ZERO + nonzero_bytes * g_tx_data_nonzero;
    }

    // Add cost for access list (EIP-2930)
    if (tx->access_list && tx->access_list_count > 0) {
        for (size_t i = 0; i < tx->access_list_count; i++) {
            gas += G_ACCESS_LIST_ADDRESS;
            gas += tx->access_list[i].storage_keys_count * G_ACCESS_LIST_STORAGE_KEY;
        }
    }

    // EIP-7702: authorization list cost (PER_AUTH_BASE_COST per tuple)
    if (tx->authorization_list && tx->authorization_list_count > 0) {
        const uint64_t PER_AUTH_BASE_COST = 25000;
        gas += tx->authorization_list_count * PER_AUTH_BASE_COST;
    }

    *out_intrinsic = gas;

    // EIP-7623 (Prague+): floor data gas
    if (fork >= FORK_PRAGUE && tx->data_size > 0) {
        const uint64_t FLOOR_CALLDATA_COST = 10;
        /* tokens = zero_bytes * 1 + non_zero_bytes * 4 */
        uint64_t tokens = zero_bytes + nonzero_bytes * 4;
        *out_floor = tokens * FLOOR_CALLDATA_COST + G_TRANSACTION;
    } else {
        *out_floor = 0;
    }
}

/**
 * EIP-4844: fake_exponential for blob base fee calculation
 * Approximates factor * e^(numerator/denominator) using integer arithmetic.
 *
 * def fake_exponential(factor, numerator, denominator):
 *     i = 1
 *     output = 0
 *     numerator_accum = factor * denominator
 *     while numerator_accum > 0:
 *         output += numerator_accum
 *         numerator_accum = (numerator_accum * numerator) // (denominator * i)
 *         i += 1
 *     return output // denominator
 */
static uint256_t fake_exponential(const uint256_t *factor, const uint256_t *numerator, const uint256_t *denominator) {
    if (uint256_is_zero(denominator)) return UINT256_ZERO;

    uint256_t i = uint256_from_uint64(1);
    uint256_t output = UINT256_ZERO;
    uint256_t numerator_accum = uint256_mul(factor, denominator);

    /* Cap iterations to prevent runaway on adversarial inputs.
     * For valid excess_blob_gas values, converges in <100 iterations.
     * With 256-bit overflow on huge numerator/denominator ratios,
     * the loop may never terminate without this guard. */
    for (int iter = 0; iter < 4096 && !uint256_is_zero(&numerator_accum); iter++) {
        output = uint256_add(&output, &numerator_accum);
        // numerator_accum = (numerator_accum * numerator) / (denominator * i)
        uint256_t num_product = uint256_mul(&numerator_accum, numerator);
        uint256_t den_product = uint256_mul(denominator, &i);
        if (uint256_is_zero(&den_product)) break;
        numerator_accum = uint256_div(&num_product, &den_product);
        uint256_t one = uint256_from_uint64(1);
        i = uint256_add(&i, &one);
    }

    return uint256_div(&output, denominator);
}

/**
 * EIP-4844: Calculate blob base fee from excess blob gas
 */
uint256_t calc_blob_gas_price(const uint256_t *excess_blob_gas, evm_fork_t fork) {
    /* Legacy path without chain config — uses Prague/Cancun defaults only.
     * Prefer calc_blob_gas_price_ex with chain config for BPO support. */
    uint256_t min_blob_base_fee = uint256_from_uint64(1);
    uint64_t fraction = (fork >= FORK_PRAGUE) ? 5007716 : 3338477;
    uint256_t blob_base_fee_update_fraction = uint256_from_uint64(fraction);
    return fake_exponential(&min_blob_base_fee, excess_blob_gas, &blob_base_fee_update_fraction);
}

uint256_t calc_blob_gas_price_ex(const uint256_t *excess_blob_gas,
                                  uint64_t update_fraction) {
    uint256_t min_blob_base_fee = uint256_from_uint64(1);
    uint256_t frac = uint256_from_uint64(update_fraction);
    return fake_exponential(&min_blob_base_fee, excess_blob_gas, &frac);
}

/**
 * Calculate effective gas price based on transaction type
 */
uint256_t transaction_effective_gas_price(
    const transaction_t *tx,
    const block_env_t *env)
{
    if (!tx || !env)
        return uint256_from_uint64(0);

    switch (tx->type) {
        case TX_TYPE_LEGACY:
        case TX_TYPE_EIP2930:
            return tx->gas_price;

        case TX_TYPE_EIP1559:
        case TX_TYPE_EIP4844:
        case TX_TYPE_EIP7702: {
            // min(max_fee_per_gas, base_fee + max_priority_fee_per_gas)
            uint256_t base_plus_priority = uint256_add(&env->base_fee, &tx->max_priority_fee_per_gas);
            return uint256_lt(&tx->max_fee_per_gas, &base_plus_priority)
                ? tx->max_fee_per_gas
                : base_plus_priority;
        }

        default:
            return tx->gas_price;
    }
}

//==============================================================================
// Transaction Validation
//==============================================================================

bool transaction_validate(
    evm_state_t *state,
    const transaction_t *tx,
    const block_env_t *env)
{
    if (!state || !tx || !env)
        return false;

    // Check nonce
    uint64_t current_nonce = evm_state_get_nonce(state, &tx->sender);
    if (tx->nonce != current_nonce) {
        return false;
    }

    // Check balance for value + max gas cost
    uint256_t sender_balance = evm_state_get_balance(state, &tx->sender);

    uint256_t effective_gas_price = transaction_effective_gas_price(tx, env);
    uint256_t gas_limit_u256 = uint256_from_uint64(tx->gas_limit);
    uint256_t gas_cost = uint256_mul(&effective_gas_price, &gas_limit_u256);

    // Check for multiplication overflow (gasPrice * gasLimit > 2^256)
    if (tx->gas_limit > 0) {
        uint256_t check = uint256_div(&gas_cost, &gas_limit_u256);
        if (!uint256_eq(&check, &effective_gas_price)) {
            return false;
        }
    }

    uint256_t total_cost = uint256_add(&tx->value, &gas_cost);
    // Check for addition overflow (value + gas_cost > 2^256)
    if (uint256_lt(&total_cost, &gas_cost)) {
        return false;
    }

    if (uint256_lt(&sender_balance, &total_cost)) {
        return false;
    }

    // Check gas limit
    if (tx->gas_limit > env->gas_limit) {
        return false;
    }

    return true;
}

//==============================================================================
// Transaction Execution
//==============================================================================

bool transaction_execute(
    evm_t *evm,
    const transaction_t *tx,
    const block_env_t *env,
    transaction_result_t *result)
{
    if (!evm || !tx || !env || !result)
        return false;

    evm_state_t *state = evm->state;

    // Initialize result
    memset(result, 0, sizeof(transaction_result_t));
    result->status = EVM_SUCCESS;

    // Set block environment in EVM (this will determine the fork)
    // Post-merge (Paris+): DIFFICULTY opcode returns PREVRANDAO (EIP-4399)
    // Pre-merge: use the difficulty field from the block header.
    // We must check the fork level, NOT whether mix_hash is non-zero,
    // because pre-merge PoW blocks have non-zero mix_hash (the PoW solution).
    evm_fork_t tx_fork = fork_get_active(env->block_number, env->timestamp, evm->chain_config);

    // EIP-7825 (Osaka+): per-transaction gas limit cap of 2^24.
    // Reject tx before any state changes; state_test runners check
    // the return value, block_executor breaks the block loop
    // separately so an Osaka block containing such a tx is invalid.
    if (tx_fork >= FORK_OSAKA && tx->gas_limit > (1UL << 24)) {
        return false;
    }

    // EIP-7594 (Osaka+, PeerDAS): cap of 6 blobs per transaction.
    // Applies to EIP-4844 blob-carrying transactions only — non-blob
    // txs have blob_versioned_hashes_count == 0 and pass trivially.
    if (tx_fork >= FORK_OSAKA && tx->blob_versioned_hashes_count > 6) {
        return false;
    }
    uint256_t block_difficulty = env->difficulty;
    if (tx_fork >= FORK_PARIS) {
        block_difficulty = uint256_from_bytes(env->prev_randao.bytes, 32);
    }
    evm_block_env_t block_env = {
        .number = env->block_number,
        .timestamp = env->timestamp,
        .gas_limit = env->gas_limit,
        .difficulty = block_difficulty,
        .coinbase = env->coinbase,
        .base_fee = env->base_fee,
        .chain_id = evm->chain_config ? uint256_from_uint64(evm->chain_config->chain_id) : uint256_from_uint64(1),
        .excess_blob_gas = env->excess_blob_gas
    };
    // Preserve block hashes (may have been set by block_execute)
    memcpy(block_env.block_hash, evm->block.block_hash, sizeof(block_env.block_hash));
    evm_set_block_env(evm, &block_env);

    // Reject transaction types not supported by the current fork
    if (tx->type == TX_TYPE_EIP2930 && evm->fork < FORK_BERLIN) {
        return false;
    }
    if (tx->type == TX_TYPE_EIP1559 && evm->fork < FORK_LONDON) {
        return false;
    }
    if (tx->type == TX_TYPE_EIP4844 && evm->fork < FORK_CANCUN) {
        return false;
    }
    if (tx->type == TX_TYPE_EIP7702 && evm->fork < FORK_PRAGUE) {
        return false;
    }

    // EIP-7702 type-4 transaction validations
    if (tx->type == TX_TYPE_EIP7702) {
        // Must have non-empty authorization list
        if (!tx->authorization_list || tx->authorization_list_count == 0) {
            return false;
        }
        // Cannot be contract creation
        if (tx->is_create) {
            return false;
        }
    }

    // EIP-4844 blob transaction validations
    if (tx->type == TX_TYPE_EIP4844) {
        // Blob tx cannot be contract creation
        if (tx->is_create) {
            return false;
        }
        // Must have at least one blob hash
        if (tx->blob_versioned_hashes_count == 0) {
            return false;
        }
        // Max blobs per tx: use blob schedule from chain config
        {
            const blob_config_t *bcfg = blob_config_active(env->timestamp, evm->chain_config);
            uint64_t max_blobs = bcfg ? bcfg->max : ((evm->fork >= FORK_PRAGUE) ? 9 : 6);
            if (tx->blob_versioned_hashes_count > max_blobs) {
                return false;
            }
        }
        // Validate blob hash version: must start with VERSIONED_HASH_VERSION_KZG = 0x01
        for (size_t i = 0; i < tx->blob_versioned_hashes_count; i++) {
            if (tx->blob_versioned_hashes[i].bytes[0] != 0x01) {
                return false;
            }
        }
        // max_fee_per_blob_gas must be >= blob_base_fee (using BPO-aware fraction)
        const blob_config_t *bcfg = blob_config_active(env->timestamp, evm->chain_config);
        uint64_t fraction = bcfg ? bcfg->update_fraction :
                            ((evm->fork >= FORK_PRAGUE) ? 5007716 : 3338477);
        uint256_t blob_base_fee = calc_blob_gas_price_ex(&env->excess_blob_gas, fraction);
        if (uint256_lt(&tx->max_fee_per_blob_gas, &blob_base_fee)) {
            return false;
        }
    }

    // EIP-3860 (Shanghai+): Reject contract creation with initcode > MAX_INITCODE_SIZE
    if (tx->is_create && evm->fork >= FORK_SHANGHAI && tx->data_size > 49152) {
        return false;
    }

    // Transaction gas limit must not exceed block gas limit
    if (tx->gas_limit > env->gas_limit) {
        return false;
    }

    // EIP-1559 validations
    if (tx->type == TX_TYPE_EIP1559 || tx->type == TX_TYPE_EIP4844 || tx->type == TX_TYPE_EIP7702) {
        // max_priority_fee_per_gas must not exceed max_fee_per_gas
        if (uint256_gt(&tx->max_priority_fee_per_gas, &tx->max_fee_per_gas)) {
            return false;
        }
        // max_fee_per_gas must be >= block base_fee
        if (uint256_lt(&tx->max_fee_per_gas, &env->base_fee)) {
            return false;
        }
    }

    // Post-London: legacy/EIP-2930 gas_price must be >= base_fee
    if (evm->fork >= FORK_LONDON &&
        (tx->type == TX_TYPE_LEGACY || tx->type == TX_TYPE_EIP2930)) {
        if (uint256_lt(&tx->gas_price, &env->base_fee)) {
            return false;
        }
    }

    // Balance check: sender must have enough for maximum gas cost + value + blob gas cost
    // EIP-1559: use max_fee_per_gas (not effective_gas_price) for balance check
    {
        uint256_t max_gas_price;
        if (tx->type == TX_TYPE_EIP1559 || tx->type == TX_TYPE_EIP4844 || tx->type == TX_TYPE_EIP7702) {
            max_gas_price = tx->max_fee_per_gas;
        } else {
            max_gas_price = tx->gas_price;
        }
        uint256_t gl = uint256_from_uint64(tx->gas_limit);
        uint256_t max_gas_cost = uint256_mul(&max_gas_price, &gl);
        // Check for multiplication overflow
        if (tx->gas_limit > 0) {
            uint256_t check = uint256_div(&max_gas_cost, &gl);
            if (!uint256_eq(&check, &max_gas_price)) {
                return false;
            }
        }
        uint256_t total_cost = uint256_add(&max_gas_cost, &tx->value);
        if (uint256_lt(&total_cost, &max_gas_cost)) {
            return false;
        }

        // EIP-4844: add blob gas cost (max_fee_per_blob_gas * blob_count * GAS_PER_BLOB)
        if (tx->type == TX_TYPE_EIP4844) {
            uint64_t total_blob_gas = tx->blob_versioned_hashes_count * 131072; // GAS_PER_BLOB = 2^17
            uint256_t blob_gas = uint256_from_uint64(total_blob_gas);
            uint256_t blob_cost = uint256_mul(&tx->max_fee_per_blob_gas, &blob_gas);
            total_cost = uint256_add(&total_cost, &blob_cost);
        }

        uint256_t sender_balance = evm_state_get_balance(state, &tx->sender);
        if (uint256_lt(&sender_balance, &total_cost)) {
            return false;
        }
    }

    // Validate sender nonce
    {
        uint64_t sender_nonce = evm_state_get_nonce(state, &tx->sender);
        if (tx->nonce != sender_nonce) {
            fprintf(stderr, "FATAL: transaction nonce mismatch: expected %lu, got %lu\n  hint: state nonce diverged from expected — likely a state corruption or missed tx\n",
                     sender_nonce, tx->nonce);
            return false;
        }
        if (sender_nonce == UINT64_MAX) {
            return false;
        }
    }

    // EIP-3607: Reject transactions from senders with code
    // EIP-7702: Senders with delegation designator (0xef0100...) are still EOAs
    {
        uint32_t sender_code_len = evm_state_get_code_size(state, &tx->sender);
        if (sender_code_len > 0) {
            // Check if it's a delegation designator (allowed)
            bool is_delegation = false;
            if (sender_code_len == 23) {
                uint32_t cl = 0;
                const uint8_t *sc = evm_state_get_code_ptr(state, &tx->sender, &cl);
                if (sc && cl == 23 && sc[0] == 0xef && sc[1] == 0x01 && sc[2] == 0x00)
                    is_delegation = true;
            }
            if (!is_delegation) {
                return false;
            }
        }
    }

    // Set transaction context on EVM
    evm->tx.origin = tx->sender;
    evm->tx.gas_price = transaction_effective_gas_price(tx, env);
    evm->tx.blob_hashes = tx->blob_versioned_hashes;
    evm->tx.blob_hashes_count = tx->blob_versioned_hashes_count;

    // Calculate effective gas price
    uint256_t effective_gas_price = evm->tx.gas_price;
    uint256_t gas_limit_u256 = uint256_from_uint64(tx->gas_limit);
    uint256_t gas_cost = uint256_mul(&effective_gas_price, &gas_limit_u256);

    // Clear any leftover logs from previous transaction
    evm_logs_clear(evm);

    // Create snapshot for potential rollback
    uint32_t snapshot = evm_state_snapshot(state);

    //==========================================================================
    // Pre-execution: State changes before EVM
    //==========================================================================

    // Get sender nonce
    uint64_t sender_nonce = evm_state_get_nonce(state, &tx->sender);

    // Calculate contract address BEFORE incrementing nonce
    address_t contract_address = {0};
    if (tx->is_create) {
        contract_address = calculate_create_address(&tx->sender, sender_nonce);
    }

    // Increment sender nonce
    evm_state_set_nonce(state, &tx->sender, sender_nonce + 1);

    // Deduct upfront gas cost from sender
    if (!evm_state_sub_balance(state, &tx->sender, &gas_cost)) {
        evm_state_revert(state, snapshot);
        return false;
    }

    // EIP-4844: deduct blob gas cost from sender (blob gas is burned, not paid to coinbase)
    uint256_t blob_gas_cost = UINT256_ZERO;
    if (tx->type == TX_TYPE_EIP4844) {
        const blob_config_t *bcfg2 = blob_config_active(env->timestamp, evm->chain_config);
        uint64_t frac2 = bcfg2 ? bcfg2->update_fraction :
                         ((evm->fork >= FORK_PRAGUE) ? 5007716 : 3338477);
        uint256_t blob_base_fee = calc_blob_gas_price_ex(&env->excess_blob_gas, frac2);
        uint64_t total_blob_gas = tx->blob_versioned_hashes_count * 131072;
        uint256_t blob_gas_u256 = uint256_from_uint64(total_blob_gas);
        blob_gas_cost = uint256_mul(&blob_base_fee, &blob_gas_u256);
        if (!evm_state_sub_balance(state, &tx->sender, &blob_gas_cost)) {
            evm_state_revert(state, snapshot);
            return false;
        }
    }

    // EIP-7702: Process authorization list BEFORE exec_snapshot
    // Authorizations persist even if the transaction reverts.
    uint64_t auth_gas_refund = 0;
    if (tx->type == TX_TYPE_EIP7702 && tx->authorization_list && tx->authorization_list_count > 0) {
        const uint64_t PER_EMPTY_ACCOUNT_COST = 25000;
        const uint64_t PER_AUTH_BASE_COST_REFUND = 12500;
        uint64_t chain_id = evm->chain_config ? evm->chain_config->chain_id : 1;

        for (size_t i = 0; i < tx->authorization_list_count; i++) {
            const authorization_t *auth = &tx->authorization_list[i];

            // 1. Chain ID check: must be 0 (wildcard) or match current chain
            //    Must compare full uint256 — truncating to uint64 would make
            //    large chain IDs with zero lower bits look like wildcards.
            uint256_t expected_chain = uint256_from_uint64(chain_id);
            if (!uint256_is_zero(&auth->chain_id) &&
                !uint256_eq(&auth->chain_id, &expected_chain)) continue;

            // 2. Validate signature r,s ranges per EIP-7702 spec
            //    r must be in (0, SECP256K1N), s must be in (0, SECP256K1N/2]
            {
                static const uint8_t secp256k1n_bytes[32] = {
                    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
                    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFE,
                    0xBA,0xAE,0xDC,0xE6,0xAF,0x48,0xA0,0x3B,
                    0xBF,0xD2,0x5E,0x8C,0xD0,0x36,0x41,0x41
                };
                static const uint8_t secp256k1n_half_bytes[32] = {
                    0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
                    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
                    0x5D,0x57,0x6E,0x73,0x57,0xA4,0x50,0x1D,
                    0xDF,0xE9,0x2F,0x46,0x68,0x1B,0x20,0xA0
                };
                uint256_t secp256k1n = uint256_from_bytes(secp256k1n_bytes, 32);
                uint256_t secp256k1n_half = uint256_from_bytes(secp256k1n_half_bytes, 32);
                if (uint256_is_zero(&auth->r) || !uint256_lt(&auth->r, &secp256k1n))
                    continue;
                if (uint256_is_zero(&auth->s) || uint256_gt(&auth->s, &secp256k1n_half))
                    continue;
            }

            // 3. Use pre-computed signer from test fixtures
            //    Invalid signature → signer is zero address → skip tuple
            const address_t *signer = &auth->signer;
            address_t zero_addr = {0};
            if (memcmp(signer, &zero_addr, sizeof(address_t)) == 0) continue;

            // 4. Nonce overflow check: skip if nonce >= 2^64-1
            //    Per spec: this check comes BEFORE warming the signer
            if (auth->nonce >= UINT64_MAX) continue;

            // 5. Mark signer as warm (EIP-2929)
            //    Per spec: warming happens even if subsequent checks fail
            evm_mark_address_warm(evm, signer);

            // 6. Check signer doesn't already have non-delegated code
            uint32_t signer_code_len = 0;
            const uint8_t *signer_code = evm_state_get_code_ptr(state, signer, &signer_code_len);
            if (signer_code && signer_code_len > 0) {
                // Allow if existing code starts with delegation designator prefix
                if (signer_code_len < 3 ||
                    signer_code[0] != 0xef || signer_code[1] != 0x01 || signer_code[2] != 0x00) {
                    continue;
                }
            }

            // 7. Check nonce match
            uint64_t signer_nonce = evm_state_get_nonce(state, signer);
            if (signer_nonce != auth->nonce) continue;

            // 8. EIP-7702 gas refund: if authority already exists, refund the
            //    "new account creation" overhead (PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST)
            if (!evm_state_is_empty(state, signer)) {
                auth_gas_refund += PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST_REFUND;
            }

            // 9. Increment signer nonce
            evm_state_set_nonce(state, signer, signer_nonce + 1);

            // 10. Set delegation code or clear it
            if (memcmp(&auth->address, &zero_addr, sizeof(address_t)) == 0) {
                // Clear delegation: set empty code
                evm_state_set_code(state, signer, NULL, 0);
            } else {
                // Set delegation designator: 0xef0100 || address (23 bytes)
                uint8_t designator[23];
                designator[0] = 0xef;
                designator[1] = 0x01;
                designator[2] = 0x00;
                memcpy(&designator[3], auth->address.bytes, 20);
                evm_state_set_code(state, signer, designator, 23);
            }
        }
    }

    // EIP-7702: If tx destination has a delegation designator, warm the target.
    // Applies to ALL tx types (not just type 4) — any tx calling a delegated
    // EOA gets the delegation target warmed. Matches geth's convenience warming.
    if (evm->fork >= FORK_PRAGUE && !tx->is_create) {
        address_t delegate_target;
        if (evm_resolve_delegation(state, &tx->to, &delegate_target)) {
            evm_mark_address_warm(evm, &delegate_target);
        }
    }

    // EIP-2930: warm access list addresses and storage keys (Berlin+)
    // Per spec: access list warming happens before execution and persists on revert.
    if (tx->access_list && tx->access_list_count > 0 && evm->fork >= FORK_BERLIN) {
        for (size_t i = 0; i < tx->access_list_count; i++) {
            evm_mark_address_warm(evm, &tx->access_list[i].address);
            for (size_t k = 0; k < tx->access_list[i].storage_keys_count; k++) {
                evm_mark_storage_warm(evm, &tx->access_list[i].address,
                                      &tx->access_list[i].storage_keys[k]);
            }
        }
    }

    // Snapshot AFTER nonce increment and gas deduction, but BEFORE value transfer
    // and contract creation. Per Ethereum spec, on EVM error/revert we revert
    // value transfer and execution state, but keep nonce increment and gas deduction.
    uint32_t exec_snapshot = evm_state_snapshot(state);

    // For contract creation, check for collision and create account
    bool collision = false;
    if (tx->is_create) {
        // EIP-684: collision if account has nonce or code
        // EIP-7610 (retroactive): also collision if account has non-empty storage
        uint64_t existing_nonce = evm_state_get_nonce(state, &contract_address);
        uint32_t existing_code = evm_state_get_code_size(state, &contract_address);
        collision = (existing_nonce > 0 || existing_code > 0);
        if (!collision) {
            collision = evm_state_has_storage(state, &contract_address);
        }
        if (collision) {
            // collision remains true
        } else {
            evm_state_create_account(state, &contract_address);

            // EIP-161 (Spurious Dragon+): new contracts get nonce=1
            if (evm->fork >= FORK_SPURIOUS_DRAGON) {
                evm_state_set_nonce(state, &contract_address, 1);
            }
        }
    }

    //==========================================================================
    // EVM Execution
    //==========================================================================

    // Calculate intrinsic gas and floor data gas in a single calldata pass
    uint64_t intrinsic_gas, floor_data_gas;
    calculate_gas_pair(tx, evm->fork, &intrinsic_gas, &floor_data_gas);

    // Check if transaction has enough gas for intrinsic cost
    // EIP-7623: must also have enough for floor data gas
    uint64_t min_gas_required = intrinsic_gas > floor_data_gas ? intrinsic_gas : floor_data_gas;
    if (tx->gas_limit < min_gas_required) {
        evm_state_revert(state, snapshot);
        return false;
    }

    // Handle collision: revert execution state, consume all gas
    if (collision) {
        evm_state_revert(state, exec_snapshot);
        result->status = EVM_INTERNAL_ERROR;
        result->gas_used = tx->gas_limit;
        result->gas_refund = 0;
        goto post_execution;
    }

    // Transfer value and touch recipient.
    // Always call add_balance (even with 0 value) to touch the recipient in
    // the state trie.  In pre-EIP-161 forks (Frontier/Homestead), touched
    // empty accounts appear in the state root.  go-ethereum always calls
    // Transfer(sender, recipient, value) which internally does AddBalance.
    {
        address_t recipient = tx->is_create ? contract_address : tx->to;
        if (!uint256_is_zero(&tx->value)) {
            if (!evm_state_sub_balance(state, &tx->sender, &tx->value)) {
                evm_state_revert(state, snapshot);
                return false;
            }
        }
        evm_state_add_balance(state, &recipient, &tx->value);
    }

    // Gas available for EVM execution
    uint64_t gas_for_execution = tx->gas_limit - intrinsic_gas;

    // Create EVM message
    address_t recipient = tx->is_create ? contract_address : tx->to;
    address_t code_addr = tx->is_create ? contract_address : tx->to;

    evm_message_t msg = {
        .kind = tx->is_create ? EVM_CREATE : EVM_CALL,
        .caller = tx->sender,
        .recipient = recipient,
        .code_addr = code_addr,
        .value = tx->value,
        .input_data = tx->data,
        .input_size = tx->data_size,
        .gas = gas_for_execution,  // Gas after intrinsic deduction
        .depth = 0,
        .is_static = false
    };

    // Execute EVM
    evm_result_t evm_result;
    if (!evm_execute(evm, &msg, &evm_result)) {
        evm_state_revert(state, snapshot);
        return false;
    }

    // Handle execution result based on status
    result->status = evm_result.status;

    if (evm_result.status != EVM_SUCCESS && evm_result.status != EVM_REVERT) {
        // EVM error (STACK_OVERFLOW, OUT_OF_GAS, INVALID_JUMP, etc.)
        // Per Ethereum spec: revert state changes from execution and consume ALL gas
        evm_state_revert(state, exec_snapshot);
        result->gas_used = tx->gas_limit;
        result->gas_refund = 0;
    } else if (evm_result.status == EVM_REVERT) {
        // REVERT opcode: revert state changes but refund unused gas
        evm_state_revert(state, exec_snapshot);
        result->gas_used = intrinsic_gas + (gas_for_execution - evm_result.gas_left);
        result->gas_refund = 0;
    } else {
        // SUCCESS: keep state changes, normal gas accounting
        result->gas_used = intrinsic_gas + (gas_for_execution - evm_result.gas_left);
        result->gas_refund = evm_result.gas_refund;

        // Handle contract creation on success
        if (tx->is_create) {
            if (evm_result.output_data && evm_result.output_size > 0) {
                // EIP-170 (Spurious Dragon+): max code size check
                if (evm->fork >= FORK_SPURIOUS_DRAGON &&
                    evm_result.output_size > 24576) {
                    evm_state_revert(state, exec_snapshot);
                    result->status = EVM_OUT_OF_GAS;
                    result->gas_used = tx->gas_limit;
                    result->gas_refund = 0;
                    result->contract_created = false;
                }
                // EIP-3541 (London+): reject code starting with 0xEF
                else if (evm->fork >= FORK_LONDON &&
                         evm_result.output_data[0] == 0xEF) {
                    evm_state_revert(state, exec_snapshot);
                    result->status = EVM_OUT_OF_GAS;
                    result->gas_used = tx->gas_limit;
                    result->gas_refund = 0;
                    result->contract_created = false;
                }
                else {
                    // Charge 200 gas per byte
                    const uint64_t G_CODE_DEPOSIT = 200;
                    uint64_t deployment_gas = evm_result.output_size * G_CODE_DEPOSIT;

                    if (evm_result.gas_left < deployment_gas) {
                        if (evm->fork >= FORK_HOMESTEAD) {
                            // Homestead+: OOG, consume all gas, revert
                            evm_state_revert(state, exec_snapshot);
                            result->status = EVM_OUT_OF_GAS;
                            result->gas_used = tx->gas_limit;
                            result->gas_refund = 0;
                            result->contract_created = false;
                        } else {
                            // Frontier: contract created with empty code,
                            // remaining gas refunded (EIP-2 not yet active)
                            result->contract_address = contract_address;
                            result->contract_created = true;
                        }
                    } else {
                        result->gas_used += deployment_gas;
                        evm_state_set_code(state, &contract_address,
                                          evm_result.output_data,
                                          (uint32_t)evm_result.output_size);
                        result->contract_address = contract_address;
                        result->contract_created = true;
                    }
                }
            } else {
                // Empty code - contract created with no code
                result->contract_address = contract_address;
                result->contract_created = true;
            }
        }
    }

    // Transfer or discard logs based on final execution status.
    // On success: move logs to result (zero-copy ownership transfer).
    // On revert/error: logs are discarded per Ethereum spec.
    if (result->status == EVM_SUCCESS) {
        result->logs = evm->logs;
        result->log_count = evm->log_count;
        evm->logs = NULL;
        evm->log_count = 0;
        evm->log_cap = 0;
    } else {
        evm_logs_clear(evm);
        result->logs = NULL;
        result->log_count = 0;
    }

    // Copy output data
    if (evm_result.output_data && evm_result.output_size > 0) {
        result->output_data = malloc(evm_result.output_size);
        if (result->output_data) {
            memcpy(result->output_data, evm_result.output_data, evm_result.output_size);
            result->output_size = evm_result.output_size;
        }
    }

    evm_result_free(&evm_result);

    //==========================================================================
    // Post-execution: Gas refunds and coinbase payment
    //==========================================================================
post_execution:
    // Calculate actual gas cost
    uint64_t gas_used = result->gas_used;
    // Clamp refund to 0 (per-frame refunds can go negative, but net transaction refund >= 0)
    int64_t net_refund = result->gas_refund + (int64_t)auth_gas_refund;
    uint64_t gas_refund = (net_refund > 0) ? (uint64_t)net_refund : 0;

    if (g_trace_calls) {
        fprintf(stderr, "  TX refund: gas_used_before=%lu gas_refund=%lu (raw=%ld) gas_left=%lu\n",
                gas_used, gas_refund, (long)result->gas_refund, tx->gas_limit - gas_used);
    }

    // Apply refund cap: London+ (EIP-3529) = gas_used/5, pre-London = gas_used/2
    uint64_t max_refund = (evm->fork >= FORK_LONDON) ? gas_used / 5 : gas_used / 2;
    if (gas_refund > max_refund) {
        gas_refund = max_refund;
    }

    // EIP-7623 (Prague+): enforce floor data gas after refunds
    // gas_used after refund = gas_used - gas_refund
    // If that's less than floor_data_gas, clamp to floor_data_gas (and reduce refund accordingly)
    if (floor_data_gas > 0) {
        uint64_t gas_used_after_refund = gas_used - gas_refund;
        if (gas_used_after_refund < floor_data_gas) {
            // Clamp: effective gas_used_after_refund = floor_data_gas
            // So gas_refund = gas_used - floor_data_gas (could be 0)
            gas_refund = (gas_used > floor_data_gas) ? gas_used - floor_data_gas : 0;
            gas_used = (gas_used > floor_data_gas) ? gas_used : floor_data_gas;
        }
    }

    // Calculate gas to refund: unused gas + capped refund
    uint64_t gas_to_refund = tx->gas_limit - gas_used + gas_refund;

    uint256_t gas_to_refund_u256 = uint256_from_uint64(gas_to_refund);
    uint256_t refund_amount = uint256_mul(&effective_gas_price, &gas_to_refund_u256);

    // Refund unused gas to sender
    evm_state_add_balance(state, &tx->sender, &refund_amount);

    // Pay coinbase (miner) for gas used
    if (!env->skip_coinbase_payment) {
        uint64_t gas_paid = gas_used - gas_refund;
        uint256_t gas_paid_u256 = uint256_from_uint64(gas_paid);

        // For London+ (EIP-1559): coinbase only gets priority fee (effective_gas_price - base_fee)
        // Pre-London: coinbase gets full effective gas price
        uint256_t coinbase_gas_price;
        if (evm->fork >= FORK_LONDON) {
            coinbase_gas_price = uint256_sub(&effective_gas_price, &env->base_fee);
        } else {
            coinbase_gas_price = effective_gas_price;
        }
        uint256_t coinbase_payment = uint256_mul(&coinbase_gas_price, &gas_paid_u256);

        evm_state_add_balance(state, &env->coinbase, &coinbase_payment);
    }

    // Update gas_used to reflect effective gas after refund (for receipt/block gasUsed)
    result->gas_used = gas_used - gas_refund;

    // NOTE: EIP-161 empty account cleanup and finalize are handled by caller

    return true;
}

//==============================================================================
// Cleanup
//==============================================================================

void transaction_result_free(transaction_result_t *result)
{
    if (!result)
        return;

    if (result->output_data) {
        free(result->output_data);
        result->output_data = NULL;
    }
    result->output_size = 0;

    for (size_t i = 0; i < result->log_count; i++)
        evm_log_free(&result->logs[i]);
    free(result->logs);
    result->logs = NULL;
    result->log_count = 0;
}
