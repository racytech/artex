/**
 * Gas Costs Implementation
 */

#include "gas.h"
#include "uint256.h"
#include <string.h>

//==============================================================================
// Static Opcode Gas Costs Table
//==============================================================================

/**
 * Base gas costs for all 256 opcodes
 * These are the static costs; dynamic costs are calculated separately
 *
 * Based on Ethereum Yellow Paper and EIPs
 */
static const opcode_gas_info_t OPCODE_GAS_TABLE[256] = {
    // 0x00-0x0F: Stop and Arithmetic
    [0x00] = {0, false}, // STOP
    [0x01] = {3, false}, // ADD
    [0x02] = {5, false}, // MUL
    [0x03] = {3, false}, // SUB
    [0x04] = {5, false}, // DIV
    [0x05] = {5, false}, // SDIV
    [0x06] = {5, false}, // MOD
    [0x07] = {5, false}, // SMOD
    [0x08] = {8, false}, // ADDMOD
    [0x09] = {8, false}, // MULMOD
    [0x0A] = {10, true}, // EXP - dynamic (depends on exponent size)
    [0x0B] = {5, false}, // SIGNEXTEND
    [0x0C] = {0, false}, // Invalid
    [0x0D] = {0, false}, // Invalid
    [0x0E] = {0, false}, // Invalid
    [0x0F] = {0, false}, // Invalid

    // 0x10-0x1F: Comparison & Bitwise Logic
    [0x10] = {3, false}, // LT
    [0x11] = {3, false}, // GT
    [0x12] = {3, false}, // SLT
    [0x13] = {3, false}, // SGT
    [0x14] = {3, false}, // EQ
    [0x15] = {3, false}, // ISZERO
    [0x16] = {3, false}, // AND
    [0x17] = {3, false}, // OR
    [0x18] = {3, false}, // XOR
    [0x19] = {3, false}, // NOT
    [0x1A] = {3, false}, // BYTE
    [0x1B] = {3, false}, // SHL (Constantinople+)
    [0x1C] = {3, false}, // SHR (Constantinople+)
    [0x1D] = {3, false}, // SAR (Constantinople+)
    [0x1E] = {0, false}, // Invalid
    [0x1F] = {0, false}, // Invalid

    // 0x20-0x2F: SHA3
    [0x20] = {30, true}, // SHA3 - dynamic (base + word cost)
    [0x21] = {0, false}, // Invalid
    [0x22] = {0, false}, // Invalid
    [0x23] = {0, false}, // Invalid
    [0x24] = {0, false}, // Invalid
    [0x25] = {0, false}, // Invalid
    [0x26] = {0, false}, // Invalid
    [0x27] = {0, false}, // Invalid
    [0x28] = {0, false}, // Invalid
    [0x29] = {0, false}, // Invalid
    [0x2A] = {0, false}, // Invalid
    [0x2B] = {0, false}, // Invalid
    [0x2C] = {0, false}, // Invalid
    [0x2D] = {0, false}, // Invalid
    [0x2E] = {0, false}, // Invalid
    [0x2F] = {0, false}, // Invalid

    // 0x30-0x3F: Environmental Information
    [0x30] = {2, false},  // ADDRESS
    [0x31] = {100, true}, // BALANCE - cold/warm (Berlin+)
    [0x32] = {2, false},  // ORIGIN
    [0x33] = {2, false},  // CALLER
    [0x34] = {2, false},  // CALLVALUE
    [0x35] = {3, false},  // CALLDATALOAD
    [0x36] = {2, false},  // CALLDATASIZE
    [0x37] = {3, true},   // CALLDATACOPY - dynamic (word cost)
    [0x38] = {2, false},  // CODESIZE
    [0x39] = {3, true},   // CODECOPY - dynamic (word cost)
    [0x3A] = {2, false},  // GASPRICE
    [0x3B] = {100, true}, // EXTCODESIZE - cold/warm (Berlin+)
    [0x3C] = {100, true}, // EXTCODECOPY - cold/warm + word cost
    [0x3D] = {3, false},  // RETURNDATASIZE (Byzantium+)
    [0x3E] = {3, true},   // RETURNDATACOPY - dynamic (word cost)
    [0x3F] = {100, true}, // EXTCODEHASH - cold/warm (Constantinople+, Berlin+)

    // 0x40-0x4F: Block Information
    [0x40] = {20, false}, // BLOCKHASH
    [0x41] = {2, false},  // COINBASE
    [0x42] = {2, false},  // TIMESTAMP
    [0x43] = {2, false},  // NUMBER
    [0x44] = {2, false},  // DIFFICULTY (PREVRANDAO in Paris+)
    [0x45] = {2, false},  // GASLIMIT
    [0x46] = {2, false},  // CHAINID (Istanbul+)
    [0x47] = {5, false},  // SELFBALANCE (Istanbul+)
    [0x48] = {2, false},  // BASEFEE (London+)
    [0x49] = {0, false},  // Invalid
    [0x4A] = {0, false},  // Invalid
    [0x4B] = {0, false},  // Invalid
    [0x4C] = {0, false},  // Invalid
    [0x4D] = {0, false},  // Invalid
    [0x4E] = {0, false},  // Invalid
    [0x4F] = {0, false},  // Invalid

    // 0x50-0x5F: Stack, Memory, Storage, and Flow
    [0x50] = {2, false},  // POP
    [0x51] = {3, true},   // MLOAD - dynamic (memory expansion)
    [0x52] = {3, true},   // MSTORE - dynamic (memory expansion)
    [0x53] = {3, true},   // MSTORE8 - dynamic (memory expansion)
    [0x54] = {100, true}, // SLOAD - cold/warm (Berlin+)
    [0x55] = {100, true}, // SSTORE - complex, fork-dependent
    [0x56] = {8, false},  // JUMP
    [0x57] = {10, false}, // JUMPI
    [0x58] = {2, false},  // PC
    [0x59] = {2, false},  // MSIZE
    [0x5A] = {2, false},  // GAS
    [0x5B] = {1, false},  // JUMPDEST
    [0x5C] = {0, false},  // Invalid (TLOAD in Cancun)
    [0x5D] = {0, false},  // Invalid (TSTORE in Cancun)
    [0x5E] = {0, false},  // Invalid (MCOPY in Cancun)
    [0x5F] = {2, false},  // PUSH0 (Shanghai+)

    // 0x60-0x7F: PUSH1-PUSH32
    [0x60] = {3, false}, // PUSH1
    [0x61] = {3, false}, // PUSH2
    [0x62] = {3, false}, // PUSH3
    [0x63] = {3, false}, // PUSH4
    [0x64] = {3, false}, // PUSH5
    [0x65] = {3, false}, // PUSH6
    [0x66] = {3, false}, // PUSH7
    [0x67] = {3, false}, // PUSH8
    [0x68] = {3, false}, // PUSH9
    [0x69] = {3, false}, // PUSH10
    [0x6A] = {3, false}, // PUSH11
    [0x6B] = {3, false}, // PUSH12
    [0x6C] = {3, false}, // PUSH13
    [0x6D] = {3, false}, // PUSH14
    [0x6E] = {3, false}, // PUSH15
    [0x6F] = {3, false}, // PUSH16
    [0x70] = {3, false}, // PUSH17
    [0x71] = {3, false}, // PUSH18
    [0x72] = {3, false}, // PUSH19
    [0x73] = {3, false}, // PUSH20
    [0x74] = {3, false}, // PUSH21
    [0x75] = {3, false}, // PUSH22
    [0x76] = {3, false}, // PUSH23
    [0x77] = {3, false}, // PUSH24
    [0x78] = {3, false}, // PUSH25
    [0x79] = {3, false}, // PUSH26
    [0x7A] = {3, false}, // PUSH27
    [0x7B] = {3, false}, // PUSH28
    [0x7C] = {3, false}, // PUSH29
    [0x7D] = {3, false}, // PUSH30
    [0x7E] = {3, false}, // PUSH31
    [0x7F] = {3, false}, // PUSH32

    // 0x80-0x8F: DUP1-DUP16
    [0x80] = {3, false}, // DUP1
    [0x81] = {3, false}, // DUP2
    [0x82] = {3, false}, // DUP3
    [0x83] = {3, false}, // DUP4
    [0x84] = {3, false}, // DUP5
    [0x85] = {3, false}, // DUP6
    [0x86] = {3, false}, // DUP7
    [0x87] = {3, false}, // DUP8
    [0x88] = {3, false}, // DUP9
    [0x89] = {3, false}, // DUP10
    [0x8A] = {3, false}, // DUP11
    [0x8B] = {3, false}, // DUP12
    [0x8C] = {3, false}, // DUP13
    [0x8D] = {3, false}, // DUP14
    [0x8E] = {3, false}, // DUP15
    [0x8F] = {3, false}, // DUP16

    // 0x90-0x9F: SWAP1-SWAP16
    [0x90] = {3, false}, // SWAP1
    [0x91] = {3, false}, // SWAP2
    [0x92] = {3, false}, // SWAP3
    [0x93] = {3, false}, // SWAP4
    [0x94] = {3, false}, // SWAP5
    [0x95] = {3, false}, // SWAP6
    [0x96] = {3, false}, // SWAP7
    [0x97] = {3, false}, // SWAP8
    [0x98] = {3, false}, // SWAP9
    [0x99] = {3, false}, // SWAP10
    [0x9A] = {3, false}, // SWAP11
    [0x9B] = {3, false}, // SWAP12
    [0x9C] = {3, false}, // SWAP13
    [0x9D] = {3, false}, // SWAP14
    [0x9E] = {3, false}, // SWAP15
    [0x9F] = {3, false}, // SWAP16

    // 0xA0-0xA4: LOG0-LOG4
    [0xA0] = {375, true}, // LOG0 - dynamic (data cost)
    [0xA1] = {375, true}, // LOG1 - dynamic (topic + data cost)
    [0xA2] = {375, true}, // LOG2 - dynamic (topics + data cost)
    [0xA3] = {375, true}, // LOG3 - dynamic (topics + data cost)
    [0xA4] = {375, true}, // LOG4 - dynamic (topics + data cost)

    // 0xA5-0xEF: Invalid (mostly)
    [0xA5 ... 0xEF] = {0, false}, // Invalid opcodes

    // 0xF0-0xFF: System Operations
    [0xF0] = {32000, true}, // CREATE - dynamic (memory + deployment)
    [0xF1] = {100, true},   // CALL - complex, fork-dependent
    [0xF2] = {100, true},   // CALLCODE - complex (deprecated)
    [0xF3] = {0, true},     // RETURN - dynamic (memory expansion)
    [0xF4] = {100, true},   // DELEGATECALL - complex (Byzantium+)
    [0xF5] = {32000, true}, // CREATE2 - dynamic (Constantinople+)
    [0xF6] = {0, false},    // Invalid
    [0xF7] = {0, false},    // Invalid
    [0xF8] = {0, false},    // Invalid
    [0xF9] = {0, false},    // Invalid
    [0xFA] = {100, true},   // STATICCALL - complex (Byzantium+)
    [0xFB] = {0, false},    // Invalid
    [0xFC] = {0, false},    // Invalid
    [0xFD] = {0, true},     // REVERT - dynamic (memory expansion, Byzantium+)
    [0xFE] = {0, false},    // INVALID - explicitly invalid
    [0xFF] = {5000, true},  // SELFDESTRUCT - dynamic (account creation)
};

//==============================================================================
// Gas Cost Lookup
//==============================================================================

uint64_t gas_get_opcode_cost(uint8_t opcode, evm_fork_t fork)
{
    // Fork-specific adjustments can go here
    // For now, return base cost from table
    (void)fork; // Unused for now, but needed for future fork-specific costs

    return OPCODE_GAS_TABLE[opcode].base_gas;
}

opcode_gas_info_t gas_get_opcode_info(uint8_t opcode, evm_fork_t fork)
{
    (void)fork; // Unused for now
    return OPCODE_GAS_TABLE[opcode];
}

bool gas_has_dynamic_cost(uint8_t opcode)
{
    return OPCODE_GAS_TABLE[opcode].has_dynamic_cost;
}

//==============================================================================
// Memory Gas Calculations
//==============================================================================

uint64_t gas_memory_expansion(uint64_t current_size, uint64_t new_size)
{
    if (new_size <= current_size)
    {
        return 0; // No expansion
    }

    // Convert to words (round up to 32-byte boundary)
    uint64_t current_words = (current_size + 31) / 32;
    uint64_t new_words = (new_size + 31) / 32;

    // Memory cost formula: (words^2 / 512) + (3 * words)
    uint64_t current_cost = (current_words * current_words) / 512 + (3 * current_words);
    uint64_t new_cost = (new_words * new_words) / 512 + (3 * new_words);

    return new_cost - current_cost;
}

uint64_t gas_to_word_size(uint64_t size)
{
    return (size + 31) / 32;
}

//==============================================================================
// Dynamic Gas Cost Calculations
//==============================================================================

uint64_t gas_copy_cost(uint64_t size)
{
    // 3 gas per word (rounded up)
    uint64_t words = gas_to_word_size(size);
    return 3 * words;
}

uint64_t gas_sha3_cost(uint64_t size)
{
    // 30 gas base + 6 gas per word
    uint64_t words = gas_to_word_size(size);
    return 30 + (6 * words);
}

uint64_t gas_log_cost(uint8_t topic_count, uint64_t data_size)
{
    // 375 gas base + 375 per topic + 8 per data byte
    return 375 + (375 * topic_count) + (8 * data_size);
}

uint64_t gas_exp_cost(uint8_t exponent_bytes, evm_fork_t fork)
{
    // Base cost: 10 gas
    // Additional cost: 50 gas per byte (Spurious Dragon+)
    // Before Spurious Dragon: 10 gas per byte

    uint64_t base = 10;
    uint64_t per_byte = (fork >= FORK_SPURIOUS_DRAGON) ? 50 : 10;

    return base + (per_byte * exponent_bytes);
}

//==============================================================================
// SSTORE Gas Costs (Complex)
//==============================================================================

/**
 * Calculate SSTORE gas cost and refund based on EIP-2200 (Istanbul) and EIP-3529 (London)
 * 
 * Implements:
 * - Pre-Istanbul: Simple set (20000) or reset (5000) with 15000 refund for clearing
 * - Istanbul (EIP-2200): Net gas metering with refunds based on original value
 * - Berlin (EIP-2929): Cold/warm storage access costs
 * - London (EIP-3529): Reduced refunds (4800 instead of 15000)
 * 
 * @param fork Current EVM fork
 * @param current_value Current value in storage (before this write)
 * @param original_value Original value at transaction start
 * @param new_value New value being written
 * @param is_cold Whether storage slot is cold (first access in tx)
 * @param gas_refund Output parameter for gas refund amount
 * @return Gas cost for this SSTORE operation
 */
uint64_t gas_sstore_cost(evm_fork_t fork,
                         const uint256_t *current_value,
                         const uint256_t *original_value,
                         const uint256_t *new_value,
                         bool is_cold,
                         int64_t *gas_refund)
{
    // Initialize refund
    if (gas_refund)
    {
        *gas_refund = 0;
    }

    bool current_is_zero = uint256_is_zero(current_value);
    bool new_is_zero = uint256_is_zero(new_value);
    bool original_is_zero = uint256_is_zero(original_value);

    // Pre-Istanbul: Simple model (Frontier/Homestead/Petersburg)
    if (fork < FORK_ISTANBUL)
    {
        if (current_is_zero && !new_is_zero)
        {
            // Setting a zero slot to non-zero
            return GAS_SSTORE_SET;
        }
        else if (!current_is_zero && new_is_zero)
        {
            // Clearing a slot (refund)
            if (gas_refund)
            {
                *gas_refund = GAS_SSTORE_REFUND; // 15000
            }
            return GAS_SSTORE_RESET;
        }
        else
        {
            // Modifying existing slot
            return GAS_SSTORE_RESET;
        }
    }

    // Istanbul+: EIP-2200 (complex, state-dependent)
    // Berlin+: Add cold storage access cost (EIP-2929)
    // London+: Reduce refunds (EIP-3529)

    // Calculate base cost (warm/cold access)
    uint64_t base_cost;
    if (fork >= FORK_BERLIN)
    {
        // Berlin+: Cold/warm access costs
        base_cost = is_cold ? GAS_SLOAD_COLD : GAS_SLOAD_WARM; // 2100 or 100
    }
    else
    {
        // Istanbul to pre-Berlin: Fixed cost
        base_cost = GAS_SLOAD_ISTANBUL; // 800
    }

    // No change - just pay access cost
    if (uint256_eq(current_value, new_value))
    {
        return base_cost;
    }

    // Value is changing - apply EIP-2200 logic
    
    // Check if this is the first write to this slot in the transaction
    bool is_first_write = uint256_eq(original_value, current_value);
    
    if (is_first_write)
    {
        // First write in transaction - charge full cost
        if (original_is_zero && !new_is_zero)
        {
            // Setting from zero to non-zero - ADDED
            return base_cost + GAS_SSTORE_SET; // 100/2100 + 20000
        }
        else if (!original_is_zero && new_is_zero)
        {
            // Clearing storage - DELETED
            // Berlin+: 2900 base, pre-Berlin: 5000
            uint64_t clear_cost = fork >= FORK_BERLIN ? 
                (GAS_SSTORE_RESET - GAS_SLOAD_COLD) : GAS_SSTORE_RESET;
            
            // Grant refund for clearing storage
            if (gas_refund)
            {
                // London (EIP-3529): reduced refund of 4800
                // Pre-London: full refund of 15000
                *gas_refund = (fork >= FORK_LONDON) ? 4800 : GAS_SSTORE_REFUND;
            }
            
            return base_cost + clear_cost; // 100/2100 + 2900/5000
        }
        else
        {
            // Modifying non-zero to different non-zero - MODIFIED
            // Berlin+: 2900, pre-Berlin: 5000
            uint64_t modify_cost = fork >= FORK_BERLIN ? 
                (GAS_SSTORE_RESET - GAS_SLOAD_COLD) : GAS_SSTORE_RESET;
            return base_cost + modify_cost; // 100/2100 + 2900/5000
        }
    }
    else
    {
        // Subsequent write to same slot in transaction - cheap
        // Only pay access cost, but grant refunds if we're resetting
        
        if (gas_refund)
        {
            // Refund logic for subsequent writes
            bool original_equals_new = uint256_eq(original_value, new_value);
            
            if (!original_is_zero && current_is_zero && !new_is_zero)
            {
                // Was cleared earlier, now resetting - remove clear refund
                int64_t clear_refund = (fork >= FORK_LONDON) ? 4800 : GAS_SSTORE_REFUND;
                *gas_refund = -clear_refund;
            }
            else if (!original_is_zero && !current_is_zero && new_is_zero)
            {
                // Clearing now (wasn't cleared before) - grant refund
                *gas_refund = (fork >= FORK_LONDON) ? 4800 : GAS_SSTORE_REFUND;
            }
            else if (original_equals_new)
            {
                // Resetting to original value
                if (original_is_zero)
                {
                    // Original was zero, we set it, now resetting - refund set cost minus base
                    if (fork >= FORK_BERLIN)
                    {
                        *gas_refund = GAS_SSTORE_SET - GAS_SLOAD_COLD + base_cost;
                    }
                    else
                    {
                        // Istanbul: Refund SSTORE_SET_GAS - SLOAD_GAS
                        *gas_refund = GAS_SSTORE_SET - base_cost;
                    }
                }
                else if (current_is_zero)
                {
                    // Was cleared, now resetting to original non-zero
                    int64_t clear_refund = (fork >= FORK_LONDON) ? 4800 : GAS_SSTORE_REFUND;
                    if (fork >= FORK_BERLIN)
                    {
                        *gas_refund = GAS_SSTORE_RESET - GAS_SLOAD_COLD - base_cost - clear_refund;
                    }
                    else
                    {
                        *gas_refund = -clear_refund;
                    }
                }
                else
                {
                    // Was modified, now resetting to original non-zero
                    if (fork >= FORK_BERLIN)
                    {
                        *gas_refund = GAS_SSTORE_RESET - GAS_SLOAD_COLD - base_cost;
                    }
                    else
                    {
                        *gas_refund = GAS_SSTORE_RESET - GAS_SSTORE_RESET;
                    }
                }
            }
        }
        
        return base_cost;
    }
}

//==============================================================================
// CALL Family Gas Costs
//==============================================================================

uint64_t gas_call_cost(evm_fork_t fork,
                       bool is_cold,
                       bool has_value,
                       bool account_exists)
{
    uint64_t cost = 0;

    // Base cost (after Tangerine Whistle)
    if (fork >= FORK_TANGERINE_WHISTLE)
    {
        cost = 700;
    }
    else
    {
        cost = 40; // Pre-EIP-150 cost
    }

    // Berlin+: Cold/warm account access (EIP-2929)
    if (fork >= FORK_BERLIN)
    {
        if (is_cold)
        {
            cost = 2600; // Cold account access
        }
        else
        {
            cost = 100; // Warm account access
        }
    }

    // Value transfer surcharge
    if (has_value)
    {
        cost += 9000;

        // Account creation (if recipient doesn't exist)
        if (!account_exists)
        {
            cost += 25000;
        }
    }

    return cost;
}

uint64_t gas_call_stipend(const uint256_t *value_transferred)
{
    // If value > 0, provide 2300 gas stipend
    if (!uint256_is_zero(value_transferred))
    {
        return 2300;
    }
    return 0;
}

//==============================================================================
// Utility Functions
//==============================================================================

uint64_t gas_max_call_gas(uint64_t gas_left)
{
    // EIP-150: Can forward at most 63/64 of remaining gas
    return gas_left - (gas_left / 64);
}
