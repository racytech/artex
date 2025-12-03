/**
 * Test memory gas charging
 */

#include "evm_memory.h"
#include "gas.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>

void test_memory_expansion_cost_basic()
{
    printf("Testing memory expansion cost (basic)...\n");
    
    // No expansion needed
    assert(evm_memory_expansion_cost(0, 0) == 0);
    assert(evm_memory_expansion_cost(32, 32) == 0);
    assert(evm_memory_expansion_cost(64, 64) == 0);
    
    // First word (0 -> 32 bytes)
    // Formula: (words^2 / 512) + (3 * words)
    // 1 word: (1^2 / 512) + (3 * 1) = 0 + 3 = 3
    uint64_t cost = evm_memory_expansion_cost(0, 32);
    assert(cost == 3);
    
    // Second word (32 -> 64 bytes)
    // 2 words: (2^2 / 512) + (3 * 2) = 0 + 6 = 6
    // 1 word: 3
    // Incremental: 6 - 3 = 3
    cost = evm_memory_expansion_cost(32, 64);
    assert(cost == 3);
    
    // 10 words (0 -> 320 bytes)
    // (10^2 / 512) + (3 * 10) = 0 + 30 = 30
    cost = evm_memory_expansion_cost(0, 320);
    assert(cost == 30);
    
    printf("✓ Basic memory expansion costs correct\n");
}

void test_memory_expansion_cost_quadratic()
{
    printf("Testing memory expansion quadratic growth...\n");
    
    // Test that cost grows quadratically
    // 724 words: (724^2 / 512) + (3 * 724) = 1023 + 2172 = 3195
    uint64_t cost_724 = evm_memory_expansion_cost(0, 724 * 32);
    assert(cost_724 == 3195);
    
    // Quadratic term should become significant for large memory
    // 1024 words: (1024^2 / 512) + (3 * 1024) = 2048 + 3072 = 5120
    uint64_t cost_1024 = evm_memory_expansion_cost(0, 1024 * 32);
    assert(cost_1024 == 5120);
    
    printf("✓ Quadratic growth working correctly\n");
}

void test_memory_access_cost()
{
    printf("Testing memory access cost calculation...\n");
    
    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);
    
    // Initial access to offset 0, size 32 (1 word)
    uint64_t cost = evm_memory_access_cost(mem, 0, 32);
    assert(cost == 3); // (1^2 / 512) + (3 * 1) = 3
    
    // Expand memory to 32 bytes
    evm_memory_expand(mem, 0, 32);
    
    // Access same location - no additional cost
    cost = evm_memory_access_cost(mem, 0, 32);
    assert(cost == 0);
    
    // Access second word (offset 32, size 32)
    cost = evm_memory_access_cost(mem, 32, 32);
    assert(cost == 3); // Incremental cost for second word
    
    // Expand to 64 bytes
    evm_memory_expand(mem, 32, 32);
    
    // Access within existing memory - no cost
    cost = evm_memory_access_cost(mem, 16, 32);
    assert(cost == 0);
    
    // Access that requires expansion
    cost = evm_memory_access_cost(mem, 64, 32);
    assert(cost == 3); // Third word
    
    evm_memory_destroy(mem);
    printf("✓ Memory access cost calculation correct\n");
}

void test_memory_rounding()
{
    printf("Testing memory size rounding to word boundaries...\n");
    
    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);
    
    // Access 1 byte should round up to 32 bytes (1 word)
    uint64_t cost = evm_memory_access_cost(mem, 0, 1);
    assert(cost == 3);
    
    // Access 33 bytes should round up to 64 bytes (2 words)
    cost = evm_memory_access_cost(mem, 0, 33);
    assert(cost == 6); // 2 words total
    
    // Access at offset 10, size 25 -> covers bytes 10-34 -> rounds to 64 bytes
    cost = evm_memory_access_cost(mem, 10, 25);
    // Need to round up (10 + 25) = 35 -> 64 bytes = 2 words
    assert(cost == 6);
    
    evm_memory_destroy(mem);
    printf("✓ Memory rounding to word boundaries correct\n");
}

void test_incremental_expansion()
{
    printf("Testing incremental memory expansion costs...\n");
    
    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);
    
    uint64_t total_cost = 0;
    
    // Expand by one word at a time, tracking total cost
    for (int i = 0; i < 10; i++)
    {
        uint64_t cost = evm_memory_access_cost(mem, i * 32, 32);
        total_cost += cost;
        evm_memory_expand(mem, i * 32, 32);
    }
    
    // Calculate expected cost for 10 words directly
    uint64_t expected = evm_memory_expansion_cost(0, 10 * 32);
    
    assert(total_cost == expected);
    printf("✓ Incremental expansion cost matches direct calculation\n");
    
    evm_memory_destroy(mem);
}

int main()
{
    printf("=== Memory Gas Charging Tests ===\n\n");
    
    test_memory_expansion_cost_basic();
    test_memory_expansion_cost_quadratic();
    test_memory_access_cost();
    test_memory_rounding();
    test_incremental_expansion();
    
    printf("\n=== All Memory Gas Tests Passed ===\n");
    return 0;
}
