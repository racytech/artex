/**
 * VM Gas Helpers Implementation
 */

#include "gas.h"

uint64_t vm_gas_to_word_size(uint64_t size)
{
    if (size > UINT64_MAX - 31)
        return UINT64_MAX;
    return (size + 31) / 32;
}

uint64_t vm_gas_memory_expansion(uint64_t current_size, uint64_t new_size)
{
    if (new_size <= current_size)
        return 0;

    uint64_t current_words = vm_gas_to_word_size(current_size);
    uint64_t new_words = vm_gas_to_word_size(new_size);

    if (new_words == UINT64_MAX || new_words > 4294967296ULL)
        return UINT64_MAX;
    if (current_words > UINT64_MAX / 3 || new_words > UINT64_MAX / 3)
        return UINT64_MAX;

    uint64_t current_cost = 3 * current_words + (current_words * current_words) / 512;
    uint64_t new_cost     = 3 * new_words     + (new_words * new_words) / 512;

    return (new_cost > current_cost) ? (new_cost - current_cost) : UINT64_MAX;
}

uint64_t vm_gas_copy_cost(uint64_t size)
{
    uint64_t words = vm_gas_to_word_size(size);
    if (words > UINT64_MAX / 3)
        return UINT64_MAX;
    return 3 * words;
}

uint64_t vm_gas_sha3_cost(uint64_t size)
{
    uint64_t words = vm_gas_to_word_size(size);
    if (words > UINT64_MAX / 6)
        return UINT64_MAX;
    uint64_t word_cost = 6 * words;
    if (word_cost > UINT64_MAX - 30)
        return UINT64_MAX;
    return 30 + word_cost;
}

uint64_t vm_gas_log_cost(uint8_t topic_count, uint64_t data_size)
{
    return 375 + (375 * (uint64_t)topic_count) + (8 * data_size);
}

uint64_t vm_gas_exp_cost(uint8_t exponent_bytes)
{
    return 10 + (50 * (uint64_t)exponent_bytes);
}

uint64_t vm_gas_max_call_gas(uint64_t gas_left)
{
    return gas_left - (gas_left / 64);
}
