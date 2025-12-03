/**
 * EVM Arithmetic Opcodes Implementation
 */

#include "opcodes/arithmetic.h"
#include "evm_stack.h"
#include "uint256.h"
#include "gas.h"
#include "logger.h"

//==============================================================================
// ADD - Addition
//==============================================================================

evm_status_t op_add(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for ADD
    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, b;
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute a + b
    uint256_t result = uint256_add(&a, &b);

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// MUL - Multiplication
//==============================================================================

evm_status_t op_mul(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for MUL
    if (!evm_use_gas(evm, GAS_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, b;
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute a * b
    uint256_t result = uint256_mul(&a, &b);

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// SUB - Subtraction
//==============================================================================

evm_status_t op_sub(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for SUB
    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, b;
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute a - b
    uint256_t result = uint256_sub(&a, &b);

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// DIV - Unsigned Division
//==============================================================================

evm_status_t op_div(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for DIV
    if (!evm_use_gas(evm, GAS_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, b;
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute a / b (or 0 if b == 0)
    uint256_t result;
    if (uint256_is_zero(&b))
    {
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_div(&a, &b);
    }

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// SDIV - Signed Division
//==============================================================================

evm_status_t op_sdiv(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for SDIV
    if (!evm_use_gas(evm, GAS_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, b;
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute a / b (signed, or 0 if b == 0)
    uint256_t result;
    if (uint256_is_zero(&b))
    {
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_sdiv(&a, &b);
    }

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// MOD - Unsigned Modulo
//==============================================================================

evm_status_t op_mod(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for MOD
    if (!evm_use_gas(evm, GAS_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, b;
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute a % b (or 0 if b == 0)
    uint256_t result;
    if (uint256_is_zero(&b))
    {
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_mod(&a, &b);
    }

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// SMOD - Signed Modulo
//==============================================================================

evm_status_t op_smod(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for SMOD
    if (!evm_use_gas(evm, GAS_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t a, b;
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute a % b (signed, or 0 if b == 0)
    uint256_t result;
    if (uint256_is_zero(&b))
    {
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_smod(&a, &b);
    }

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// ADDMOD - Modular Addition
//==============================================================================

evm_status_t op_addmod(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for ADDMOD
    if (!evm_use_gas(evm, GAS_MID))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 3 items
    if (!evm_stack_require(evm->stack, 3))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop three operands
    uint256_t a, b, n;
    evm_stack_pop(evm->stack, &n);
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute (a + b) % N (or 0 if N == 0)
    uint256_t result;
    if (uint256_is_zero(&n))
    {
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_addmod(&a, &b, &n);
    }

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// MULMOD - Modular Multiplication
//==============================================================================

evm_status_t op_mulmod(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for MULMOD
    if (!evm_use_gas(evm, GAS_MID))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 3 items
    if (!evm_stack_require(evm->stack, 3))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop three operands
    uint256_t a, b, n;
    evm_stack_pop(evm->stack, &n);
    evm_stack_pop(evm->stack, &b);
    evm_stack_pop(evm->stack, &a);

    // Compute (a * b) % N (or 0 if N == 0)
    uint256_t result;
    if (uint256_is_zero(&n))
    {
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_mulmod(&a, &b, &n);
    }

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

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
    evm_stack_pop(evm->stack, &exponent);
    evm_stack_pop(evm->stack, &a);

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

//==============================================================================
// SIGNEXTEND - Sign Extension
//==============================================================================

evm_status_t op_signextend(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Use gas for SIGNEXTEND
    if (!evm_use_gas(evm, GAS_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check stack has at least 2 items
    if (!evm_stack_require(evm->stack, 2))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop two operands
    uint256_t b, x;
    evm_stack_pop(evm->stack, &x);
    evm_stack_pop(evm->stack, &b);

    // Sign extend x from (b+1) bytes
    // Convert b to uint64 for byte number
    uint64_t byte_num = uint256_to_uint64(&b);
    uint256_t result = uint256_signextend(&x, byte_num);

    // Push result
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
