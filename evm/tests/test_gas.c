/**
 * Gas Costs Tests
 */

#include "gas.h"
#include "uint256.h"
#include <assert.h>
#include <stdio.h>

void test_basic_opcode_costs(void)
{
    printf("Testing basic opcode gas costs...\n");
    
    // Arithmetic operations
    assert(gas_get_opcode_cost(0x01, FORK_FRONTIER) == 3);  // ADD
    assert(gas_get_opcode_cost(0x02, FORK_FRONTIER) == 5);  // MUL
    assert(gas_get_opcode_cost(0x03, FORK_FRONTIER) == 3);  // SUB
    assert(gas_get_opcode_cost(0x04, FORK_FRONTIER) == 5);  // DIV
    
    // Comparison operations
    assert(gas_get_opcode_cost(0x10, FORK_FRONTIER) == 3);  // LT
    assert(gas_get_opcode_cost(0x14, FORK_FRONTIER) == 3);  // EQ
    assert(gas_get_opcode_cost(0x15, FORK_FRONTIER) == 3);  // ISZERO
    
    // Bitwise operations
    assert(gas_get_opcode_cost(0x16, FORK_FRONTIER) == 3);  // AND
    assert(gas_get_opcode_cost(0x17, FORK_FRONTIER) == 3);  // OR
    assert(gas_get_opcode_cost(0x18, FORK_FRONTIER) == 3);  // XOR
    
    printf("  ✓ Basic opcode costs correct\n");
}

void test_push_dup_swap_costs(void)
{
    printf("Testing PUSH/DUP/SWAP gas costs...\n");
    
    // All PUSH opcodes cost 3 gas
    for (uint8_t i = 0x60; i <= 0x7F; i++)
    {
        assert(gas_get_opcode_cost(i, FORK_FRONTIER) == 3);
    }
    
    // All DUP opcodes cost 3 gas
    for (uint8_t i = 0x80; i <= 0x8F; i++)
    {
        assert(gas_get_opcode_cost(i, FORK_FRONTIER) == 3);
    }
    
    // All SWAP opcodes cost 3 gas
    for (uint8_t i = 0x90; i <= 0x9F; i++)
    {
        assert(gas_get_opcode_cost(i, FORK_FRONTIER) == 3);
    }
    
    printf("  ✓ PUSH/DUP/SWAP costs correct\n");
}

void test_special_opcode_costs(void)
{
    printf("Testing special opcode costs...\n");
    
    assert(gas_get_opcode_cost(0x00, FORK_FRONTIER) == 0);     // STOP
    assert(gas_get_opcode_cost(0x5B, FORK_FRONTIER) == 1);     // JUMPDEST
    assert(gas_get_opcode_cost(0x56, FORK_FRONTIER) == 8);     // JUMP
    assert(gas_get_opcode_cost(0x57, FORK_FRONTIER) == 10);    // JUMPI
    assert(gas_get_opcode_cost(0x20, FORK_FRONTIER) == 30);    // SHA3 (base)
    assert(gas_get_opcode_cost(0x40, FORK_FRONTIER) == 20);    // BLOCKHASH
    
    printf("  ✓ Special opcode costs correct\n");
}

void test_dynamic_cost_flags(void)
{
    printf("Testing dynamic cost flags...\n");
    
    // Opcodes with dynamic costs
    assert(gas_has_dynamic_cost(0x0A) == true);   // EXP
    assert(gas_has_dynamic_cost(0x20) == true);   // SHA3
    assert(gas_has_dynamic_cost(0x37) == true);   // CALLDATACOPY
    assert(gas_has_dynamic_cost(0x51) == true);   // MLOAD
    assert(gas_has_dynamic_cost(0x52) == true);   // MSTORE
    assert(gas_has_dynamic_cost(0x54) == true);   // SLOAD
    assert(gas_has_dynamic_cost(0x55) == true);   // SSTORE
    assert(gas_has_dynamic_cost(0xA0) == true);   // LOG0
    assert(gas_has_dynamic_cost(0xF0) == true);   // CREATE
    assert(gas_has_dynamic_cost(0xF1) == true);   // CALL
    assert(gas_has_dynamic_cost(0xFF) == true);   // SELFDESTRUCT
    
    // Opcodes with static costs
    assert(gas_has_dynamic_cost(0x01) == false);  // ADD
    assert(gas_has_dynamic_cost(0x10) == false);  // LT
    assert(gas_has_dynamic_cost(0x60) == false);  // PUSH1
    assert(gas_has_dynamic_cost(0x80) == false);  // DUP1
    assert(gas_has_dynamic_cost(0x90) == false);  // SWAP1
    
    printf("  ✓ Dynamic cost flags correct\n");
}

void test_memory_expansion_cost(void)
{
    printf("Testing memory expansion costs...\n");
    
    // No expansion
    assert(gas_memory_expansion(0, 0) == 0);
    assert(gas_memory_expansion(32, 32) == 0);
    assert(gas_memory_expansion(64, 32) == 0);  // Shrinking
    
    // Expanding from 0 to 32 bytes (1 word)
    // Cost = (1^2 / 512) + (3 * 1) = 0 + 3 = 3
    assert(gas_memory_expansion(0, 32) == 3);
    
    // Expanding from 0 to 64 bytes (2 words)
    // Cost = (2^2 / 512) + (3 * 2) = 0 + 6 = 6
    assert(gas_memory_expansion(0, 64) == 6);
    
    // Expanding from 32 to 64 bytes
    // New cost = 6, old cost = 3, diff = 3
    assert(gas_memory_expansion(32, 64) == 3);
    
    // Expanding from 0 to 1024 bytes (32 words)
    // Cost = (32^2 / 512) + (3 * 32) = 2 + 96 = 98
    assert(gas_memory_expansion(0, 1024) == 98);
    
    printf("  ✓ Memory expansion costs correct\n");
}

void test_copy_operations(void)
{
    printf("Testing copy operation costs...\n");
    
    // 3 gas per word
    assert(gas_copy_cost(0) == 0);      // 0 bytes = 0 words
    assert(gas_copy_cost(1) == 3);      // 1 byte = 1 word
    assert(gas_copy_cost(32) == 3);     // 32 bytes = 1 word
    assert(gas_copy_cost(33) == 6);     // 33 bytes = 2 words
    assert(gas_copy_cost(64) == 6);     // 64 bytes = 2 words
    assert(gas_copy_cost(65) == 9);     // 65 bytes = 3 words
    assert(gas_copy_cost(100) == 12);   // 100 bytes = 4 words
    
    printf("  ✓ Copy operation costs correct\n");
}

void test_sha3_cost(void)
{
    printf("Testing SHA3 gas costs...\n");
    
    // 30 base + 6 per word
    assert(gas_sha3_cost(0) == 30);      // 30 + 0 = 30
    assert(gas_sha3_cost(1) == 36);      // 30 + 6 = 36
    assert(gas_sha3_cost(32) == 36);     // 30 + 6 = 36
    assert(gas_sha3_cost(33) == 42);     // 30 + 12 = 42
    assert(gas_sha3_cost(64) == 42);     // 30 + 12 = 42
    assert(gas_sha3_cost(100) == 54);    // 30 + 24 = 54 (4 words)
    
    printf("  ✓ SHA3 costs correct\n");
}

void test_log_costs(void)
{
    printf("Testing LOG operation costs...\n");
    
    // 375 base + 375 per topic + 8 per data byte
    assert(gas_log_cost(0, 0) == 375);              // LOG0, no data
    assert(gas_log_cost(0, 10) == 375 + 80);        // LOG0, 10 bytes
    assert(gas_log_cost(1, 0) == 375 + 375);        // LOG1, no data
    assert(gas_log_cost(1, 10) == 375 + 375 + 80);  // LOG1, 10 bytes
    assert(gas_log_cost(2, 10) == 375 + 750 + 80);  // LOG2, 10 bytes
    assert(gas_log_cost(4, 100) == 375 + 1500 + 800); // LOG4, 100 bytes
    
    printf("  ✓ LOG costs correct\n");
}

void test_exp_cost(void)
{
    printf("Testing EXP gas costs...\n");
    
    // Pre-Spurious Dragon: 10 base + 10 per byte
    assert(gas_exp_cost(0, FORK_FRONTIER) == 10);
    assert(gas_exp_cost(1, FORK_FRONTIER) == 20);
    assert(gas_exp_cost(2, FORK_HOMESTEAD) == 30);
    
    // Spurious Dragon+: 10 base + 50 per byte
    assert(gas_exp_cost(0, FORK_SPURIOUS_DRAGON) == 10);
    assert(gas_exp_cost(1, FORK_SPURIOUS_DRAGON) == 60);
    assert(gas_exp_cost(2, FORK_BYZANTIUM) == 110);
    assert(gas_exp_cost(32, FORK_LONDON) == 10 + 1600);
    
    printf("  ✓ EXP costs correct\n");
}

void test_call_costs(void)
{
    printf("Testing CALL operation costs...\n");
    
    // Pre-Tangerine Whistle
    assert(gas_call_cost(FORK_FRONTIER, false, false, true) == 40);
    
    // Tangerine Whistle+: 700 base
    assert(gas_call_cost(FORK_TANGERINE_WHISTLE, false, false, true) == 700);
    
    // With value transfer: +9000
    assert(gas_call_cost(FORK_BYZANTIUM, false, true, true) == 700 + 9000);
    
    // With value + new account: +9000 + 25000
    assert(gas_call_cost(FORK_ISTANBUL, false, true, false) == 700 + 9000 + 25000);
    
    // Berlin+: Cold access
    assert(gas_call_cost(FORK_BERLIN, true, false, true) == 2600);
    
    // Berlin+: Warm access
    assert(gas_call_cost(FORK_BERLIN, false, false, true) == 100);
    
    // Berlin+: Cold + value + new account
    assert(gas_call_cost(FORK_LONDON, true, true, false) == 2600 + 9000 + 25000);
    
    printf("  ✓ CALL costs correct\n");
}

void test_call_stipend(void)
{
    printf("Testing CALL stipend...\n");
    
    uint256_t zero = uint256_from_uint64(0);
    uint256_t value = uint256_from_uint64(1);
    
    assert(gas_call_stipend(&zero) == 0);
    assert(gas_call_stipend(&value) == 2300);
    
    printf("  ✓ CALL stipend correct\n");
}

void test_max_call_gas(void)
{
    printf("Testing 63/64 rule for call gas...\n");
    
    // EIP-150: Can forward at most 63/64 of gas
    assert(gas_max_call_gas(64) == 63);
    assert(gas_max_call_gas(128) == 126);
    assert(gas_max_call_gas(1000) == 1000 - 15);  // 1000 - (1000/64) = 985
    assert(gas_max_call_gas(10000) == 10000 - 156);  // 10000 - 156 = 9844
    
    printf("  ✓ 63/64 rule correct\n");
}

void test_word_size_conversion(void)
{
    printf("Testing word size conversion...\n");
    
    assert(gas_to_word_size(0) == 0);
    assert(gas_to_word_size(1) == 1);
    assert(gas_to_word_size(31) == 1);
    assert(gas_to_word_size(32) == 1);
    assert(gas_to_word_size(33) == 2);
    assert(gas_to_word_size(63) == 2);
    assert(gas_to_word_size(64) == 2);
    assert(gas_to_word_size(65) == 3);
    assert(gas_to_word_size(1024) == 32);
    
    printf("  ✓ Word size conversion correct\n");
}

void test_sstore_basic(void)
{
    printf("Testing basic SSTORE costs...\n");
    
    uint256_t zero = uint256_from_uint64(0);
    uint256_t one = uint256_from_uint64(1);
    uint256_t two = uint256_from_uint64(2);
    int64_t refund = 0;
    
    // Pre-Istanbul: Setting zero to non-zero = 20000
    uint64_t cost = gas_sstore_cost(FORK_BYZANTIUM, &zero, &zero, &one, false, &refund);
    assert(cost == 20000);
    
    // Pre-Istanbul: Modifying non-zero = 5000
    cost = gas_sstore_cost(FORK_BYZANTIUM, &one, &one, &two, false, &refund);
    assert(cost == 5000);
    
    // Pre-Istanbul: Clearing slot = 5000 + refund
    cost = gas_sstore_cost(FORK_BYZANTIUM, &one, &one, &zero, false, &refund);
    assert(cost == 5000);
    assert(refund == 15000);
    
    printf("  ✓ Basic SSTORE costs correct\n");
}

void test_opcode_info(void)
{
    printf("Testing opcode info structure...\n");
    
    opcode_gas_info_t info;
    
    info = gas_get_opcode_info(0x01, FORK_FRONTIER);  // ADD
    assert(info.base_gas == 3);
    assert(info.has_dynamic_cost == false);
    
    info = gas_get_opcode_info(0x20, FORK_FRONTIER);  // SHA3
    assert(info.base_gas == 30);
    assert(info.has_dynamic_cost == true);
    
    info = gas_get_opcode_info(0xF1, FORK_BERLIN);    // CALL
    assert(info.base_gas == 100);
    assert(info.has_dynamic_cost == true);
    
    printf("  ✓ Opcode info correct\n");
}

int main(void)
{
    printf("\n=== Gas Costs Tests ===\n\n");
    
    test_basic_opcode_costs();
    test_push_dup_swap_costs();
    test_special_opcode_costs();
    test_dynamic_cost_flags();
    test_memory_expansion_cost();
    test_copy_operations();
    test_sha3_cost();
    test_log_costs();
    test_exp_cost();
    test_call_costs();
    test_call_stipend();
    test_max_call_gas();
    test_word_size_conversion();
    test_sstore_basic();
    test_opcode_info();
    
    printf("\n=== All Gas Tests Passed ===\n\n");
    return 0;
}
