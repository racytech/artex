/**
 * Transaction Execution Layer Implementation
 */

#include "transaction.h"
#include "evm.h"
#include "opcodes/create.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate intrinsic gas for a transaction
 *
 * Intrinsic gas includes:
 * - Base transaction cost (21000 gas)
 * - Cost for calldata (4 gas per zero byte, 16 gas per non-zero byte)
 * - Cost for contract creation (32000 gas if creating contract)
 * - Cost for access list entries (EIP-2930)
 */
static uint64_t calculate_intrinsic_gas(const transaction_t *tx, evm_fork_t fork) {
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

    // Add cost for calldata
    if (tx->data && tx->data_size > 0) {
        for (size_t i = 0; i < tx->data_size; i++) {
            if (tx->data[i] == 0) {
                gas += G_TX_DATA_ZERO;
            } else {
                gas += g_tx_data_nonzero;
            }
        }
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

    return gas;
}

/**
 * EIP-7623 (Prague+): Calculate floor data gas cost
 * floor_gas = tokens_in_calldata * FLOOR_CALLDATA_COST + TX_BASE_COST
 * tokens = zero_bytes * 1 + non_zero_bytes * 4
 */
static uint64_t calculate_floor_data_gas(const transaction_t *tx, evm_fork_t fork) {
    if (fork < FORK_PRAGUE) return 0;

    const uint64_t FLOOR_CALLDATA_COST = 10;
    const uint64_t TX_BASE_COST = 21000;

    uint64_t tokens = 0;
    if (tx->data && tx->data_size > 0) {
        for (size_t i = 0; i < tx->data_size; i++) {
            tokens += (tx->data[i] == 0) ? 1 : 4;
        }
    }

    return tokens * FLOOR_CALLDATA_COST + TX_BASE_COST;
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
    uint256_t i = uint256_from_uint64(1);
    uint256_t output = UINT256_ZERO;
    uint256_t numerator_accum = uint256_mul(factor, denominator);

    while (!uint256_is_zero(&numerator_accum)) {
        output = uint256_add(&output, &numerator_accum);
        // numerator_accum = (numerator_accum * numerator) / (denominator * i)
        uint256_t num_product = uint256_mul(&numerator_accum, numerator);
        uint256_t den_product = uint256_mul(denominator, &i);
        if (uint256_is_zero(&den_product)) break;
        numerator_accum = uint256_div(&num_product, &den_product);
        uint256_t one = uint256_from_uint64(1);
        i = uint256_add(&i, &one);
    }

    if (uint256_is_zero(denominator)) return UINT256_ZERO;
    return uint256_div(&output, denominator);
}

/**
 * EIP-4844: Calculate blob base fee from excess blob gas
 */
uint256_t calc_blob_gas_price(const uint256_t *excess_blob_gas) {
    uint256_t min_blob_base_fee = uint256_from_uint64(1);
    uint256_t blob_base_fee_update_fraction = uint256_from_uint64(3338477);
    return fake_exponential(&min_blob_base_fee, excess_blob_gas, &blob_base_fee_update_fraction);
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
        LOG_EVM_ERROR("Transaction nonce mismatch: expected %lu, got %lu",
                 current_nonce, tx->nonce);
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
            LOG_EVM_ERROR("Gas cost overflow: gasPrice * gasLimit exceeds uint256");
            return false;
        }
    }

    uint256_t total_cost = uint256_add(&tx->value, &gas_cost);
    // Check for addition overflow (value + gas_cost > 2^256)
    if (uint256_lt(&total_cost, &gas_cost)) {
        LOG_EVM_ERROR("Total cost overflow: value + gas_cost exceeds uint256");
        return false;
    }

    if (uint256_lt(&sender_balance, &total_cost)) {
        LOG_EVM_ERROR("Insufficient balance for transaction");
        return false;
    }

    // Check gas limit
    if (tx->gas_limit > env->gas_limit) {
        LOG_EVM_ERROR("Transaction gas limit exceeds block gas limit");
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
    // Post-merge (Paris+): DIFFICULTY opcode returns PREVRANDAO
    uint256_t block_difficulty = env->difficulty;
    hash_t zero_hash;
    memset(&zero_hash, 0, sizeof(zero_hash));
    if (memcmp(&env->prev_randao, &zero_hash, sizeof(hash_t)) != 0) {
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
    evm_set_block_env(evm, &block_env);

    // Reject transaction types not supported by the current fork
    if (tx->type == TX_TYPE_EIP2930 && evm->fork < FORK_BERLIN) {
        LOG_EVM_ERROR("EIP-2930 type-1 tx not valid before Berlin");
        return false;
    }
    if (tx->type == TX_TYPE_EIP1559 && evm->fork < FORK_LONDON) {
        LOG_EVM_ERROR("EIP-1559 type-2 tx not valid before London");
        return false;
    }
    if (tx->type == TX_TYPE_EIP4844 && evm->fork < FORK_CANCUN) {
        LOG_EVM_ERROR("EIP-4844 type-3 tx not valid before Cancun");
        return false;
    }
    if (tx->type == TX_TYPE_EIP7702 && evm->fork < FORK_PRAGUE) {
        LOG_EVM_ERROR("EIP-7702 type-4 tx not valid before Prague");
        return false;
    }

    // EIP-7702 type-4 transaction validations
    if (tx->type == TX_TYPE_EIP7702) {
        // Must have non-empty authorization list
        if (!tx->authorization_list || tx->authorization_list_count == 0) {
            LOG_EVM_ERROR("EIP-7702: empty authorization list");
            return false;
        }
        // Cannot be contract creation
        if (tx->is_create) {
            LOG_EVM_ERROR("EIP-7702: type-4 tx cannot be contract creation");
            return false;
        }
    }

    // EIP-4844 blob transaction validations
    if (tx->type == TX_TYPE_EIP4844) {
        // Blob tx cannot be contract creation
        if (tx->is_create) {
            LOG_EVM_ERROR("EIP-4844: blob tx cannot be contract creation");
            return false;
        }
        // Must have at least one blob hash
        if (tx->blob_versioned_hashes_count == 0) {
            LOG_EVM_ERROR("EIP-4844: blob tx must have at least one blob hash");
            return false;
        }
        // Max blobs per block: 6 in Cancun (max_blob_gas=786432, gas_per_blob=131072)
        if (tx->blob_versioned_hashes_count > 6) {
            LOG_EVM_ERROR("EIP-4844: too many blobs (%zu > 6)", tx->blob_versioned_hashes_count);
            return false;
        }
        // Validate blob hash version: must start with VERSIONED_HASH_VERSION_KZG = 0x01
        for (size_t i = 0; i < tx->blob_versioned_hashes_count; i++) {
            if (tx->blob_versioned_hashes[i].bytes[0] != 0x01) {
                LOG_EVM_ERROR("EIP-4844: invalid blob hash version byte 0x%02x at index %zu",
                              tx->blob_versioned_hashes[i].bytes[0], i);
                return false;
            }
        }
        // max_fee_per_blob_gas must be >= blob_base_fee
        uint256_t blob_base_fee = calc_blob_gas_price(&env->excess_blob_gas);
        if (uint256_lt(&tx->max_fee_per_blob_gas, &blob_base_fee)) {
            LOG_EVM_ERROR("EIP-4844: max_fee_per_blob_gas below blob base fee");
            return false;
        }
    }

    // EIP-3860 (Shanghai+): Reject contract creation with initcode > MAX_INITCODE_SIZE
    if (tx->is_create && evm->fork >= FORK_SHANGHAI && tx->data_size > 49152) {
        LOG_EVM_ERROR("Initcode size %zu exceeds limit 49152", tx->data_size);
        return false;
    }

    // Transaction gas limit must not exceed block gas limit
    if (tx->gas_limit > env->gas_limit) {
        LOG_EVM_ERROR("Transaction gas limit %lu exceeds block gas limit %lu",
                      tx->gas_limit, env->gas_limit);
        return false;
    }

    // EIP-1559 validations
    if (tx->type == TX_TYPE_EIP1559 || tx->type == TX_TYPE_EIP4844 || tx->type == TX_TYPE_EIP7702) {
        // max_priority_fee_per_gas must not exceed max_fee_per_gas
        if (uint256_gt(&tx->max_priority_fee_per_gas, &tx->max_fee_per_gas)) {
            LOG_EVM_ERROR("max_priority_fee_per_gas exceeds max_fee_per_gas");
            return false;
        }
        // max_fee_per_gas must be >= block base_fee
        if (uint256_lt(&tx->max_fee_per_gas, &env->base_fee)) {
            LOG_EVM_ERROR("max_fee_per_gas below block base_fee");
            return false;
        }
    }

    // Post-London: legacy/EIP-2930 gas_price must be >= base_fee
    if (evm->fork >= FORK_LONDON &&
        (tx->type == TX_TYPE_LEGACY || tx->type == TX_TYPE_EIP2930)) {
        if (uint256_lt(&tx->gas_price, &env->base_fee)) {
            LOG_EVM_ERROR("gas_price below block base_fee");
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
                LOG_EVM_ERROR("Gas cost overflow in balance check");
                return false;
            }
        }
        uint256_t total_cost = uint256_add(&max_gas_cost, &tx->value);
        if (uint256_lt(&total_cost, &max_gas_cost)) {
            LOG_EVM_ERROR("Total cost overflow in balance check");
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
            LOG_EVM_ERROR("Insufficient sender balance for max gas cost + value");
            return false;
        }
    }

    // Reject transaction if sender nonce is at max (overflow protection)
    {
        uint64_t sender_nonce = evm_state_get_nonce(state, &tx->sender);
        if (sender_nonce == UINT64_MAX) {
            LOG_EVM_ERROR("Sender nonce at maximum, cannot increment");
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
                LOG_EVM_ERROR("EIP-3607: sender has code (not an EOA)");
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
        LOG_EVM_ERROR("Failed to deduct gas cost from sender");
        evm_state_revert(state, snapshot);
        return false;
    }

    // EIP-4844: deduct blob gas cost from sender (blob gas is burned, not paid to coinbase)
    uint256_t blob_gas_cost = UINT256_ZERO;
    if (tx->type == TX_TYPE_EIP4844) {
        uint256_t blob_base_fee = calc_blob_gas_price(&env->excess_blob_gas);
        uint64_t total_blob_gas = tx->blob_versioned_hashes_count * 131072;
        uint256_t blob_gas_u256 = uint256_from_uint64(total_blob_gas);
        blob_gas_cost = uint256_mul(&blob_base_fee, &blob_gas_u256);
        if (!evm_state_sub_balance(state, &tx->sender, &blob_gas_cost)) {
            LOG_EVM_ERROR("Failed to deduct blob gas cost from sender");
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
            uint64_t auth_chain = uint256_to_uint64(&auth->chain_id);
            if (auth_chain != 0 && auth_chain != chain_id) continue;

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
                // Allow if existing code is a delegation designator
                if (signer_code_len != 23 ||
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

    // EIP-7702: If tx destination has a delegation designator, warm the target
    if (tx->type == TX_TYPE_EIP7702 && !tx->is_create) {
        address_t delegate_target;
        if (evm_resolve_delegation(state, &tx->to, &delegate_target)) {
            evm_mark_address_warm(evm, &delegate_target);
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
            LOG_EVM_DEBUG("Contract address collision: nonce=%lu, code_size=%u",
                         existing_nonce, existing_code);
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

    // Calculate intrinsic gas
    uint64_t intrinsic_gas = calculate_intrinsic_gas(tx, evm->fork);

    // EIP-7623 (Prague+): floor data gas
    uint64_t floor_data_gas = calculate_floor_data_gas(tx, evm->fork);

    // Check if transaction has enough gas for intrinsic cost
    // EIP-7623: must also have enough for floor data gas
    uint64_t min_gas_required = intrinsic_gas > floor_data_gas ? intrinsic_gas : floor_data_gas;
    if (tx->gas_limit < min_gas_required) {
        LOG_EVM_ERROR("Insufficient gas: limit=%lu, intrinsic=%lu, floor=%lu",
                 tx->gas_limit, intrinsic_gas, floor_data_gas);
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

    // Transfer value (if any)
    if (!uint256_is_zero(&tx->value)) {
        if (!evm_state_sub_balance(state, &tx->sender, &tx->value)) {
            LOG_EVM_ERROR("Failed to deduct value from sender");
            evm_state_revert(state, snapshot);
            return false;
        }

        address_t recipient = tx->is_create ? contract_address : tx->to;
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
        LOG_EVM_ERROR("EVM execution failed");
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
                    // Charge deployment gas (200 gas per byte)
                    const uint64_t G_CODE_DEPOSIT = 200;
                    uint64_t deployment_gas = evm_result.output_size * G_CODE_DEPOSIT;

                    // Check if enough gas left for deployment
                    if (evm_result.gas_left < deployment_gas) {
                        LOG_EVM_DEBUG("Insufficient gas for code deployment");
                        evm_state_revert(state, exec_snapshot);
                        result->status = EVM_OUT_OF_GAS;
                        result->gas_used = tx->gas_limit;
                        result->gas_refund = 0;
                        result->contract_created = false;
                    } else {
                        // Deduct deployment gas
                        result->gas_used += deployment_gas;

                        // Store contract code
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
}
