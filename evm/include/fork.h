/**
 * Fork Configuration - Ethereum Hard Fork Management
 *
 * This module handles Ethereum hard fork detection and configuration.
 * Different forks introduce new opcodes, change gas costs, and modify
 * behavior of existing opcodes.
 *
 * Fork Timeline:
 *   Frontier (2015) → Homestead → Tangerine Whistle → Spurious Dragon
 *   → Byzantium → Constantinople → Petersburg → Istanbul → Muir Glacier
 *   → Berlin → London → Arrow Glacier → Gray Glacier → Paris (The Merge)
 *   → Shanghai → Cancun
 */

#ifndef ART_EVM_FORK_H
#define ART_EVM_FORK_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif

    //==============================================================================
    // Fork Enumeration
    //==============================================================================

    /**
     * Ethereum hard forks in chronological order
     */
    typedef enum
    {
        FORK_FRONTIER = 0,      // Genesis
        FORK_HOMESTEAD,         // EIP-2, EIP-7, EIP-8
        FORK_TANGERINE_WHISTLE, // EIP-150 (DoS fixes)
        FORK_SPURIOUS_DRAGON,   // EIP-155, EIP-160, EIP-161, EIP-170
        FORK_BYZANTIUM,         // EIP-140, EIP-211, EIP-214, EIP-658
        FORK_CONSTANTINOPLE,    // EIP-145, EIP-1014, EIP-1052, EIP-1283
        FORK_PETERSBURG,        // Reverted EIP-1283
        FORK_ISTANBUL,          // EIP-152, EIP-1108, EIP-1344, EIP-1884, EIP-2028, EIP-2200
        FORK_MUIR_GLACIER,      // Difficulty bomb delay
        FORK_BERLIN,            // EIP-2565, EIP-2718, EIP-2929, EIP-2930
        FORK_LONDON,            // EIP-1559, EIP-3198, EIP-3529, EIP-3541
        FORK_ARROW_GLACIER,     // Difficulty bomb delay
        FORK_GRAY_GLACIER,      // Difficulty bomb delay
        FORK_PARIS,             // The Merge (EIP-3675, EIP-4399)
        FORK_SHANGHAI,          // EIP-3651, EIP-3855, EIP-3860, EIP-4895
        FORK_CANCUN,            // EIP-1153, EIP-4844, EIP-5656, EIP-6780, EIP-7516
        FORK_PRAGUE,            // EIP-7623, EIP-7702, EIP-2935, EIP-7685
        FORK_OSAKA,             // Future
        FORK_VERKLE,            // Verkle tree state (EIP-4762, EIP-6800, EIP-7709)
        FORK_LATEST             // Always points to the latest fork
    } evm_fork_t;

    //==============================================================================
    // Chain Configuration
    //==============================================================================

    /**
     * Fork activation schedule for a specific chain.
     *
     * Pre-merge fields (frontier through paris): block numbers.
     * Post-merge fields (shanghai through osaka): timestamps.
     */
    typedef struct
    {
        /* Pre-merge: activation block numbers */
        uint64_t frontier;
        uint64_t homestead;
        uint64_t tangerine_whistle; // EIP-150
        uint64_t spurious_dragon;   // EIP-155, EIP-160
        uint64_t byzantium;
        uint64_t constantinople;
        uint64_t petersburg;
        uint64_t istanbul;
        uint64_t muir_glacier;
        uint64_t berlin;
        uint64_t london;
        uint64_t arrow_glacier;
        uint64_t gray_glacier;
        uint64_t paris; // The Merge
        /* Post-merge: activation timestamps */
        uint64_t shanghai;
        uint64_t cancun;
        uint64_t prague;
        uint64_t osaka;
        uint64_t verkle;
    } fork_schedule_t;

    /**
     * Chain-specific configuration
     */
    typedef struct
    {
        uint64_t chain_id;           // Chain ID (1 for mainnet)
        fork_schedule_t fork_blocks; // Fork activation blocks
        const char *name;            // Chain name (e.g., "mainnet")
    } chain_config_t;

    //==============================================================================
    // Fork Detection
    //==============================================================================

    /**
     * Determine the active fork for a given block number and timestamp.
     *
     * Pre-merge forks (Frontier-Paris) are checked against block_number.
     * Post-merge forks (Shanghai+) are checked against timestamp.
     *
     * @param block_number Current block number
     * @param timestamp Current block timestamp
     * @param config Chain configuration
     * @return Active fork at this block
     */
    evm_fork_t fork_get_active(uint64_t block_number, uint64_t timestamp, const chain_config_t *config);

    /**
     * Check if a specific fork is active at a block number/timestamp
     *
     * @param block_number Current block number
     * @param timestamp Current block timestamp
     * @param config Chain configuration
     * @param fork Fork to check
     * @return true if fork is active, false otherwise
     */
    bool fork_is_active(uint64_t block_number, uint64_t timestamp, const chain_config_t *config, evm_fork_t fork);

    /**
     * Get the block number when a fork activates
     *
     * @param config Chain configuration
     * @param fork Fork to query
     * @return Block number when fork activates, UINT64_MAX if never
     */
    uint64_t fork_get_activation_block(const chain_config_t *config, evm_fork_t fork);

    //==============================================================================
    // Predefined Chain Configurations
    //==============================================================================

    /**
     * Get Ethereum mainnet configuration
     *
     * @return Mainnet chain configuration
     */
    const chain_config_t *chain_config_mainnet(void);

    /**
     * Get Sepolia testnet configuration
     *
     * @return Sepolia chain configuration
     */
    const chain_config_t *chain_config_sepolia(void);

    /**
     * Get Goerli testnet configuration (deprecated)
     *
     * @return Goerli chain configuration
     */
    const chain_config_t *chain_config_goerli(void);

    /**
     * Get Holesky testnet configuration
     *
     * @return Holesky chain configuration
     */
    const chain_config_t *chain_config_holesky(void);

    /**
     * Create a custom chain configuration
     *
     * @param chain_id Chain ID
     * @param name Chain name
     * @return New chain configuration (caller must free)
     */
    chain_config_t *chain_config_create(uint64_t chain_id, const char *name);

    /**
     * Free a chain configuration
     *
     * @param config Configuration to free
     */
    void chain_config_free(chain_config_t *config);

    //==============================================================================
    // Fork Feature Queries
    //==============================================================================

    /**
     * Check if DELEGATECALL is available (Byzantium+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_delegatecall(evm_fork_t fork);

    /**
     * Check if STATICCALL is available (Byzantium+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_staticcall(evm_fork_t fork);

    /**
     * Check if CREATE2 is available (Constantinople+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_create2(evm_fork_t fork);

    /**
     * Check if EXTCODEHASH is available (Constantinople+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_extcodehash(evm_fork_t fork);

    /**
     * Check if CHAINID is available (Istanbul+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_chainid(evm_fork_t fork);

    /**
     * Check if SELFBALANCE is available (Istanbul+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_selfbalance(evm_fork_t fork);

    /**
     * Check if BASEFEE is available (London+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_basefee(evm_fork_t fork);

    /**
     * Check if PREVRANDAO is available (Paris+, replaces DIFFICULTY)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_prevrandao(evm_fork_t fork);

    /**
     * Check if PUSH0 is available (Shanghai+)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_push0(evm_fork_t fork);

    /**
     * Check if TSTORE/TLOAD are available (Cancun+, EIP-1153)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_transient_storage(evm_fork_t fork);

    /**
     * Check if BLOBHASH/BLOBBASEFEE are available (Cancun+, EIP-4844)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_blob_opcodes(evm_fork_t fork);

    /**
     * Check if MCOPY is available (Cancun+, EIP-5656)
     *
     * @param fork Current fork
     * @return true if available
     */
    bool fork_has_mcopy(evm_fork_t fork);

    /**
     * Check if access lists are tracked (Berlin+, EIP-2929)
     *
     * @param fork Current fork
     * @return true if access lists should be tracked
     */
    bool fork_has_access_lists(evm_fork_t fork);

    //==============================================================================
    // Utilities
    //==============================================================================

    /**
     * Get the name of a fork
     *
     * @param fork Fork to query
     * @return Fork name string
     */
    const char *fork_get_name(evm_fork_t fork);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_FORK_H */
