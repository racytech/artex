/**
 * EVM Arithmetic Opcodes Implementation
 *
 * Most arithmetic opcodes are inlined directly into interpreter.c dispatch labels.
 * Only op_exp remains here (complex gas calculation).
 */

#include "opcodes/arithmetic.h"
#include "evm_stack.h"
#include "uint256.h"
#include "gas.h"

//==============================================================================
// EXP - Exponentiation
//==============================================================================

evm_status_t op_exp(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, exponent;
    evm_stack_pop(evm->stack, &a);         // First pop: base (top of stack)
    evm_stack_pop(evm->stack, &exponent);  // Second pop: exponent

    // Calculate number of significant bytes in exponent
    uint8_t exponent_bytes = 0;
    for (int i = 31; i >= 0; i--)
    {
        if (((uint8_t*)&exponent)[i] != 0)
        {
            exponent_bytes = i + 1;
            break;
        }
    }

    // Calculate dynamic gas cost for EXP
    uint64_t exp_gas = gas_exp_cost(exponent_bytes, evm->fork);
    if (!evm_use_gas(evm, exp_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Compute a^exponent
    uint256_t result = uint256_exp(&a, &exponent);

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
