/**
 * Fork-Aware Opcode Example
 * 
 * This demonstrates how to use the `evm->fork` field to implement
 * opcodes that behave differently across Ethereum hard forks.
 */

#include "evm.h"
#include "fork.h"
#include "uint256.h"
#include "logger.h"

/**
 * Example: DIFFICULTY opcode (0x44)
 * 
 * Pre-Paris: Returns block difficulty
 * Paris+:    Returns PREVRANDAO (beacon chain randomness)
 * 
 * This is the canonical example of an opcode changing behavior
 * across forks while keeping the same opcode byte.
 */
evm_status_t op_difficulty_example(evm_t *evm)
{
    if (fork_has_prevrandao(evm->fork))
    {
        // Post-merge: return PREVRANDAO
        LOG_DEBUG("PREVRANDAO opcode (Paris+ fork: %s)", fork_get_name(evm->fork));
        // Push evm->block.difficulty (which actually contains PREVRANDAO)
        // In practice, the semantics changed but the data source remains the same field
    }
    else
    {
        // Pre-merge: return difficulty
        LOG_DEBUG("DIFFICULTY opcode (pre-Paris fork: %s)", fork_get_name(evm->fork));
        // Push evm->block.difficulty (actual PoW difficulty)
    }
    
    return EVM_SUCCESS;
}

/**
 * Example: CHAINID opcode (0x46)
 * 
 * Invalid:   Before Istanbul (causes INVALID_OPCODE)
 * Istanbul+: Returns chain ID
 */
evm_status_t op_chainid_example(evm_t *evm)
{
    if (!fork_has_chainid(evm->fork))
    {
        LOG_ERROR("CHAINID not available in fork %s", fork_get_name(evm->fork));
        return EVM_INVALID_OPCODE;
    }
    
    LOG_DEBUG("CHAINID opcode (available since Istanbul)");
    // Push evm->block.chain_id
    
    return EVM_SUCCESS;
}

/**
 * Example: CREATE2 opcode (0xF5)
 * 
 * Invalid:         Before Constantinople
 * Constantinople+: Deterministic contract creation
 */
evm_status_t op_create2_example(evm_t *evm)
{
    if (!fork_has_create2(evm->fork))
    {
        LOG_ERROR("CREATE2 not available in fork %s", fork_get_name(evm->fork));
        return EVM_INVALID_OPCODE;
    }
    
    LOG_DEBUG("CREATE2 opcode (available since Constantinople)");
    // Perform deterministic contract creation
    
    return EVM_SUCCESS;
}

/**
 * Example: PUSH0 opcode (0x5F)
 * 
 * Invalid:   Before Shanghai
 * Shanghai+: Push 0 onto stack
 * 
 * This is interesting because 0x5F was previously INVALID,
 * so old contracts can't use it, but new contracts save gas.
 */
evm_status_t op_push0_example(evm_t *evm)
{
    if (!fork_has_push0(evm->fork))
    {
        LOG_ERROR("PUSH0 not available in fork %s", fork_get_name(evm->fork));
        return EVM_INVALID_OPCODE;
    }
    
    LOG_DEBUG("PUSH0 opcode (available since Shanghai)");
    // Push 0 onto stack (saves 1 byte compared to PUSH1 0x00)
    
    return EVM_SUCCESS;
}

/**
 * Example: Gas cost computation based on fork
 * 
 * Many opcodes have different gas costs across forks.
 * The most famous example is SLOAD (EIP-2929).
 */
uint64_t get_sload_gas_cost(evm_t *evm, const uint256_t *key)
{
    if (!fork_has_access_lists(evm->fork))
    {
        // Before Berlin (EIP-2929): flat 800 gas
        return 800;
    }
    
    // Berlin+: warm/cold storage access
    // TODO: Check if key is in accessed_storage set
    bool is_warm = false; // Placeholder
    
    if (is_warm)
    {
        return 100;  // Warm access
    }
    else
    {
        return 2100; // Cold access (first time)
    }
}

/**
 * Example: SELFDESTRUCT behavior change
 * 
 * Different gas costs and refund behavior across forks:
 * - Frontier to Tangerine Whistle: 0 gas (refund 24000)
 * - Tangerine Whistle to London: 5000 gas (refund 24000)
 * - London to Cancun: 5000 gas (refund capped at gas_used / 5)
 * - Cancun+ (EIP-6780): Only works in same transaction as creation
 */
uint64_t get_selfdestruct_gas_cost(evm_t *evm, const address_t *beneficiary)
{
    // This is simplified - real implementation considers:
    // - Whether beneficiary exists
    // - Whether value is being transferred
    // - Whether account is being created
    
    if (evm->fork < FORK_TANGERINE_WHISTLE)
    {
        return 0;
    }
    
    // Tangerine Whistle+ (EIP-150)
    return 5000;
}

/**
 * Example: Complete opcode implementation with fork awareness
 */
evm_status_t op_basefee_complete(evm_t *evm)
{
    // Check fork availability
    if (!fork_has_basefee(evm->fork))
    {
        LOG_ERROR("BASEFEE opcode not available before London fork (current: %s)",
                  fork_get_name(evm->fork));
        return EVM_INVALID_OPCODE;
    }
    
    // Use gas (3 gas for BASEFEE in London+)
    if (!evm_use_gas(evm, 3))
    {
        return EVM_OUT_OF_GAS;
    }
    
    // Push base fee onto stack
    // TODO: Implement actual stack push
    // evm_stack_push(evm->stack, &evm->block.base_fee);
    
    LOG_DEBUG("BASEFEE: %s", fork_get_name(evm->fork));
    
    return EVM_SUCCESS;
}

/**
 * Example: Using fork info in EVM initialization
 */
void example_evm_setup(void)
{
    // Create EVM with mainnet configuration
    state_db_t *state = NULL; // Assume we have this
    evm_t *evm = evm_create(state, chain_config_mainnet());
    
    if (!evm)
    {
        return;
    }
    
    // Set up block environment for a specific block
    evm_block_env_t block = {
        .number = 17034870,        // Shanghai activation block
        .timestamp = 1681338455,   // Approximate Shanghai timestamp
        .gas_limit = 30000000,
        .coinbase = {{0}},
        // ... other fields
    };
    
    evm_set_block_env(evm, &block);
    
    // Fork is automatically computed!
    printf("Executing at block %lu\n", block.number);
    printf("Active fork: %s\n", fork_get_name(evm->fork));
    printf("PUSH0 available: %s\n", fork_has_push0(evm->fork) ? "yes" : "no");
    printf("PREVRANDAO: %s\n", fork_has_prevrandao(evm->fork) ? "yes" : "no");
    
    evm_destroy(evm);
}

/**
 * Example: Different chains have different fork schedules
 */
void example_multi_chain(void)
{
    state_db_t *state = NULL;
    
    // Mainnet EVM
    evm_t *mainnet = evm_create(state, chain_config_mainnet());
    evm_block_env_t mainnet_block = { .number = 12000000 };
    evm_set_block_env(mainnet, &mainnet_block);
    printf("Mainnet at block 12000000: %s\n", fork_get_name(mainnet->fork));
    
    // Sepolia EVM (testnet)
    evm_t *sepolia = evm_create(state, chain_config_sepolia());
    evm_block_env_t sepolia_block = { .number = 3000000 };
    evm_set_block_env(sepolia, &sepolia_block);
    printf("Sepolia at block 3000000: %s\n", fork_get_name(sepolia->fork));
    
    // Custom chain
    chain_config_t *custom = chain_config_create(999, "devnet");
    custom->fork_blocks.london = 100;     // London activates at block 100
    custom->fork_blocks.shanghai = 1000;  // Shanghai at block 1000
    
    evm_t *devnet = evm_create(state, custom);
    evm_block_env_t devnet_block = { .number = 500 };
    evm_set_block_env(devnet, &devnet_block);
    printf("Devnet at block 500: %s\n", fork_get_name(devnet->fork));
    
    evm_destroy(mainnet);
    evm_destroy(sepolia);
    evm_destroy(devnet);
    chain_config_free(custom);
}

int main(void)
{
    printf("=== Fork-Aware Opcode Examples ===\n\n");
    
    example_evm_setup();
    printf("\n");
    example_multi_chain();
    
    printf("\n=== Examples Complete ===\n");
    return 0;
}
