/**
 * EVM Comparison and Bitwise Opcodes Implementation
 */

#include "evm.h"
#include "opcodes/comparison.h"
#include "evm_stack.h"
#include "gas.h"
#include "logger.h"

// LT (0x10): Less-than comparison (unsigned)
evm_status_t op_lt(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("LT: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_lt(&a, &b) ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// GT (0x11): Greater-than comparison (unsigned)
evm_status_t op_gt(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("GT: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_gt(&a, &b) ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// SLT (0x12): Signed less-than comparison
evm_status_t op_slt(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("SLT: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_slt(&a, &b) ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// SGT (0x13): Signed greater-than comparison
evm_status_t op_sgt(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("SGT: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_sgt(&a, &b) ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// EQ (0x14): Equality comparison
evm_status_t op_eq(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("EQ: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_eq(&a, &b) ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// ISZERO (0x15): Is-zero check
evm_status_t op_iszero(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 1))
    {
        LOG_EVM_ERROR("ISZERO: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a;
    evm_stack_pop(evm->stack, &a);

    uint256_t result = uint256_is_zero(&a) ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// AND (0x16): Bitwise AND
evm_status_t op_and(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("AND: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_and(&a, &b);
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// OR (0x17): Bitwise OR
evm_status_t op_or(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("OR: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_or(&a, &b);
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// XOR (0x18): Bitwise XOR
evm_status_t op_xor(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("XOR: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a, b;
    evm_stack_pop(evm->stack, &a);
    evm_stack_pop(evm->stack, &b);

    uint256_t result = uint256_xor(&a, &b);
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// NOT (0x19): Bitwise NOT
evm_status_t op_not(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 1))
    {
        LOG_EVM_ERROR("NOT: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t a;
    evm_stack_pop(evm->stack, &a);

    uint256_t result = uint256_not(&a);
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// BYTE (0x1a): Extract byte at position
evm_status_t op_byte(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("BYTE: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t i, x;
    evm_stack_pop(evm->stack, &i);
    evm_stack_pop(evm->stack, &x);

    // Get byte index (0-31, where 0 is most significant byte)
    uint64_t byte_idx = uint256_to_uint64(&i);
    
    uint256_t result;
    if (byte_idx >= 32)
    {
        // Out of range, return 0
        result = UINT256_ZERO;
    }
    else
    {
        uint8_t byte_val = uint256_byte(&x, byte_idx);
        result = uint256_from_uint64(byte_val);
    }
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// SHL (0x1b): Shift left
evm_status_t op_shl(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // EIP-145: SHL introduced in Constantinople
    if (evm->fork < FORK_CONSTANTINOPLE)
        return EVM_INVALID_OPCODE;

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("SHL: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t shift, value;
    evm_stack_pop(evm->stack, &shift);
    evm_stack_pop(evm->stack, &value);

    uint64_t shift_amount = uint256_to_uint64(&shift);
    
    uint256_t result;
    if (shift_amount >= 256)
    {
        // Shift by 256 or more results in 0
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_shl(&value, shift_amount);
    }
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// SHR (0x1c): Logical shift right
evm_status_t op_shr(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (evm->fork < FORK_CONSTANTINOPLE)
        return EVM_INVALID_OPCODE;

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("SHR: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t shift, value;
    evm_stack_pop(evm->stack, &shift);
    evm_stack_pop(evm->stack, &value);

    uint64_t shift_amount = uint256_to_uint64(&shift);
    
    uint256_t result;
    if (shift_amount >= 256)
    {
        // Shift by 256 or more results in 0
        result = UINT256_ZERO;
    }
    else
    {
        result = uint256_shr(&value, shift_amount);
    }
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// SAR (0x1d): Arithmetic shift right (sign-preserving)
evm_status_t op_sar(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (evm->fork < FORK_CONSTANTINOPLE)
        return EVM_INVALID_OPCODE;

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("SAR: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t shift, value;
    evm_stack_pop(evm->stack, &shift);
    evm_stack_pop(evm->stack, &value);

    uint64_t shift_amount = uint256_to_uint64(&shift);
    
    uint256_t result;
    if (shift_amount >= 256)
    {
        // For arithmetic shift, preserve sign
        bool is_negative = (value.high >> 127) & 1;
        if (is_negative)
        {
            result = UINT256_MAX; // All 1s for negative
        }
        else
        {
            result = UINT256_ZERO;
        }
    }
    else
    {
        result = uint256_sar(&value, shift_amount);
    }
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
