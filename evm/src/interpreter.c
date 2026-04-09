/**
 * EVM Interpreter Implementation
 *
 * High-performance bytecode interpreter using computed goto dispatch.
 * This technique provides ~15-30% speedup over traditional switch statements.
 */

#include "interpreter.h"
#include "evm_stack.h"
#include "gas.h"
#include "opcodes/arithmetic.h"
#include "opcodes/comparison.h"
#include "opcodes/stack.h"
#include "opcodes/memory.h"
#include "opcodes/storage.h"
#include "opcodes/control.h"
#include "opcodes/block.h"
#include "opcodes/environmental.h"
#include "opcodes/crypto.h"
#include "opcodes/logging.h"
#include "opcodes/call.h"
#include "opcodes/create.h"
#include "evm_state.h"
#include "evm_tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Debug trace flag — shared across all opcode files (unity build).
#ifdef ENABLE_DEBUG
bool g_trace_calls __attribute__((weak)) = false;
#else
static const bool g_trace_calls = false;
#endif

// Include opcode implementations directly — single translation unit allows
// the compiler to inline all opcode functions into the dispatch loop.
#include "opcodes/arithmetic.c"
#include "opcodes/control.c"
#include "opcodes/environmental.c"
#include "opcodes/block.c"
#include "opcodes/memory.c"
#include "opcodes/storage.c"
#include "opcodes/crypto.c"
#include "opcodes/logging.c"
#include "opcodes/call.c"
#include "opcodes/create.c"

//==============================================================================
// Inline helpers for inlined opcodes
//==============================================================================

// Convert big-endian 32-byte buffer to uint256 using bswap64 (4 swaps vs 32 shifts)
static inline uint256_t bytes32_to_uint256(const uint8_t buf[32]) {
    uint64_t w0, w1, w2, w3;
    memcpy(&w3, buf +  0, 8);
    memcpy(&w2, buf +  8, 8);
    memcpy(&w1, buf + 16, 8);
    memcpy(&w0, buf + 24, 8);
    w0 = __builtin_bswap64(w0);
    w1 = __builtin_bswap64(w1);
    w2 = __builtin_bswap64(w2);
    w3 = __builtin_bswap64(w3);
    return (uint256_t){
        ((__uint128_t)w1 << 64) | w0,
        ((__uint128_t)w3 << 64) | w2
    };
}

// Convert 20-byte address to uint256 directly (no intermediate buffer)
static inline uint256_t addr_to_u256(const address_t *addr) {
    const uint8_t *b = addr->bytes;
    /* Address is 20 bytes big-endian → fits in low 160 bits.
     * high = top 4 bytes of address (bytes 0-3), zero-extended
     * low  = remaining 16 bytes (bytes 4-19) */
    uint64_t w2 = 0;
    /* Only 4 bytes go into w2 (bytes 0-3 of address = bits 128-159) */
    w2 = ((uint64_t)b[0] << 24) | ((uint64_t)b[1] << 16) |
         ((uint64_t)b[2] <<  8) | (uint64_t)b[3];
    uint64_t w1, w0;
    memcpy(&w1, b + 4, 8);
    memcpy(&w0, b + 12, 8);
    w1 = __builtin_bswap64(w1);
    w0 = __builtin_bswap64(w0);
    return (uint256_t){
        ((__uint128_t)w1 << 64) | w0,
        (__uint128_t)w2
    };
}


//==============================================================================
// Dispatch Table - Maps opcodes to label addresses
//==============================================================================

#define DISPATCH()                                                  \
    do                                                              \
    {                                                               \
        if (__builtin_expect(evm->pc >= evm->code_size, 0))          \
        {                                                           \
            EVM_TRACE_IMPLICIT_STOP(evm);                           \
            if (evm->return_data) { free(evm->return_data);         \
                                    evm->return_data = NULL; }      \
            evm->return_data_size = 0;                              \
            goto done;                                              \
        }                                                           \
        EVM_TRACE_DISPATCH(evm);                                    \
        goto *dispatch_table[evm->code[evm->pc]];                  \
    } while (0)

#define VERKLE_PUSH_GAS(N) /* no-op */

#define NEXT()      \
    do              \
    {               \
        evm->pc++;  \
        DISPATCH(); \
    } while (0)

//==============================================================================
// Helper Functions
//==============================================================================

evm_result_t evm_result_create(evm_status_t status,
                               uint64_t gas_left,
                               int64_t gas_refund,
                               uint8_t *output_data,
                               size_t output_size)
{
    evm_result_t result = {0};
    result.status = status;
    result.gas_left = gas_left;
    result.gas_refund = gas_refund;
    result.output_size = output_size;
    /* Take ownership of output_data — caller must not free it */
    result.output_data = (output_data && output_size > 0) ? output_data : NULL;

    return result;
}

//==============================================================================
// Main Interpreter
//==============================================================================

evm_result_t evm_interpret(evm_t *evm)
{
    if (!evm || !evm->code || !evm->stack || !evm->memory)
    {
        fprintf(stderr, "FATAL: evm_interpret called with invalid EVM state (null code/stack/memory)\n");
        return evm_result_create(EVM_INTERNAL_ERROR, 0, 0, NULL, 0);
    }

    if (0) { // Debug output disabled
    printf("DEBUG INTERPRETER: Starting execution\n");
    printf("  Code size: %zu bytes\n", evm->code_size);
    printf("  Gas available: %lu\n", evm->gas_left);
    printf("  PC: %zu\n", evm->pc);
    printf("  First 10 bytes: ");
    for (size_t i = 0; i < 10 && i < evm->code_size; i++) {
        printf("%02x ", evm->code[i]);
    }
    printf("\n");
    } // end debug output


    // Build JUMPDEST bitmap for O(1) jump validation
    uint8_t *jumpdest_bitmap = build_jumpdest_bitmap(evm->code, evm->code_size);
    evm->jumpdest_bitmap = jumpdest_bitmap;

    // Computed goto dispatch table (GCC/Clang extension)
    static const void *dispatch_table[256] = {
        // 0x00-0x0f: Stop and Arithmetic
        &&op_stop,
        &&op_add,
        &&op_mul,
        &&op_sub,
        &&op_div,
        &&op_sdiv,
        &&op_mod,
        &&op_smod,
        &&op_addmod,
        &&op_mulmod,
        &&op_exp,
        &&op_signextend,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,

        // 0x10-0x1f: Comparison & Bitwise Logic
        &&op_lt,
        &&op_gt,
        &&op_slt,
        &&op_sgt,
        &&op_eq,
        &&op_iszero,
        &&op_and,
        &&op_or,
        &&op_xor,
        &&op_not,
        &&op_byte,
        &&op_shl,
        &&op_shr,
        &&op_sar,
        &&op_clz,
        &&op_invalid,

        // 0x20-0x2f: Keccak256
        &&op_keccak256,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,

        // 0x30-0x3f: Environmental Information
        &&op_address,
        &&op_balance,
        &&op_origin,
        &&op_caller,
        &&op_callvalue,
        &&op_calldataload,
        &&op_calldatasize,
        &&op_calldatacopy,
        &&op_codesize,
        &&op_codecopy,
        &&op_gasprice,
        &&op_extcodesize,
        &&op_extcodecopy,
        &&op_returndatasize,
        &&op_returndatacopy,
        &&op_extcodehash,

        // 0x40-0x4f: Block Information
        &&op_blockhash,
        &&op_coinbase,
        &&op_timestamp,
        &&op_number,
        &&op_difficulty,
        &&op_gaslimit,
        &&op_chainid,
        &&op_selfbalance,
        &&op_basefee,
        &&op_blobhash,
        &&op_blobbasefee,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,

        // 0x50-0x5f: Stack, Memory, Storage, and Flow Operations
        &&op_pop,
        &&op_mload,
        &&op_mstore,
        &&op_mstore8,
        &&op_sload,
        &&op_sstore,
        &&op_jump,
        &&op_jumpi,
        &&op_pc,
        &&op_msize,
        &&op_gas,
        &&op_jumpdest,
        &&op_tload,
        &&op_tstore,
        &&op_mcopy,
        &&op_push0,

        // 0x60-0x7f: Push Operations
        &&op_push1,
        &&op_push2,
        &&op_push3,
        &&op_push4,
        &&op_push5,
        &&op_push6,
        &&op_push7,
        &&op_push8,
        &&op_push9,
        &&op_push10,
        &&op_push11,
        &&op_push12,
        &&op_push13,
        &&op_push14,
        &&op_push15,
        &&op_push16,
        &&op_push17,
        &&op_push18,
        &&op_push19,
        &&op_push20,
        &&op_push21,
        &&op_push22,
        &&op_push23,
        &&op_push24,
        &&op_push25,
        &&op_push26,
        &&op_push27,
        &&op_push28,
        &&op_push29,
        &&op_push30,
        &&op_push31,
        &&op_push32,

        // 0x80-0x8f: Dup Operations
        &&op_dup1,
        &&op_dup2,
        &&op_dup3,
        &&op_dup4,
        &&op_dup5,
        &&op_dup6,
        &&op_dup7,
        &&op_dup8,
        &&op_dup9,
        &&op_dup10,
        &&op_dup11,
        &&op_dup12,
        &&op_dup13,
        &&op_dup14,
        &&op_dup15,
        &&op_dup16,

        // 0x90-0x9f: Swap Operations
        &&op_swap1,
        &&op_swap2,
        &&op_swap3,
        &&op_swap4,
        &&op_swap5,
        &&op_swap6,
        &&op_swap7,
        &&op_swap8,
        &&op_swap9,
        &&op_swap10,
        &&op_swap11,
        &&op_swap12,
        &&op_swap13,
        &&op_swap14,
        &&op_swap15,
        &&op_swap16,

        // 0xa0-0xaf: Log Operations
        &&op_log0,
        &&op_log1,
        &&op_log2,
        &&op_log3,
        &&op_log4,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,

        // 0xb0-0xef: Reserved/Invalid
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,

        // 0xf0-0xff: System Operations
        &&op_create,
        &&op_call,
        &&op_callcode,
        &&op_return,
        &&op_delegatecall,
        &&op_create2,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_invalid,
        &&op_staticcall,
        &&op_invalid,
        &&op_invalid,
        &&op_revert,
        &&op_invalid,
        &&op_selfdestruct,
    };

    // Start execution
    evm_status_t status = EVM_SUCCESS;
    DISPATCH();

    //==========================================================================
    // 0x00-0x0f: Stop and Arithmetic Operations
    //==========================================================================

op_stop:
    EVM_TRACE_EXIT(evm, NULL);
    evm->stopped = true;
    if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
    evm->return_data_size = 0;
    goto done;

op_add:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        uint128_t lo = s[t].low + s[t-1].low;
        s[t-1].high = s[t].high + s[t-1].high + (uint128_t)(lo < s[t].low);
        s[t-1].low = lo;
        evm->stack->size = t;
    }
    NEXT();

op_mul:
    if (!evm_use_gas(evm, GAS_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1] = uint256_mul(&s[t], &s[t-1]);
        evm->stack->size = t;
    }
    NEXT();

op_sub:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        uint128_t borrow = (uint128_t)(s[t].low < s[t-1].low);
        s[t-1].low = s[t].low - s[t-1].low;
        s[t-1].high = s[t].high - s[t-1].high - borrow;
        evm->stack->size = t;
    }
    NEXT();

op_div:
    if (!evm_use_gas(evm, GAS_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (uint256_is_zero(&s[t-1]))
            s[t-1] = UINT256_ZERO;
        else
            s[t-1] = uint256_div(&s[t], &s[t-1]);
        evm->stack->size = t;
    }
    NEXT();

op_sdiv:
    if (!evm_use_gas(evm, GAS_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (uint256_is_zero(&s[t-1]))
            s[t-1] = UINT256_ZERO;
        else
            s[t-1] = uint256_sdiv(&s[t], &s[t-1]);
        evm->stack->size = t;
    }
    NEXT();

op_mod:
    if (!evm_use_gas(evm, GAS_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (uint256_is_zero(&s[t-1]))
            s[t-1] = UINT256_ZERO;
        else
            s[t-1] = uint256_mod(&s[t], &s[t-1]);
        evm->stack->size = t;
    }
    NEXT();

op_smod:
    if (!evm_use_gas(evm, GAS_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (uint256_is_zero(&s[t-1]))
            s[t-1] = UINT256_ZERO;
        else
            s[t-1] = uint256_smod(&s[t], &s[t-1]);
        evm->stack->size = t;
    }
    NEXT();

op_addmod:
    if (!evm_use_gas(evm, GAS_MID)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 3, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (uint256_is_zero(&s[t-2]))
            s[t-2] = UINT256_ZERO;
        else
            s[t-2] = uint256_addmod(&s[t], &s[t-1], &s[t-2]);
        evm->stack->size = t - 1;
    }
    NEXT();

op_mulmod:
    if (!evm_use_gas(evm, GAS_MID)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 3, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (uint256_is_zero(&s[t-2]))
            s[t-2] = UINT256_ZERO;
        else
            s[t-2] = uint256_mulmod(&s[t], &s[t-1], &s[t-2]);
        evm->stack->size = t - 1;
    }
    NEXT();

op_exp:
    status = op_exp(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_signextend:
    if (!evm_use_gas(evm, GAS_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (s[t].high == 0 && s[t].low <= 30) {
            unsigned int byte_num = (unsigned int)s[t].low;
            s[t-1] = uint256_signextend(&s[t-1], byte_num);
        }
        // else b >= 31, value unchanged
        evm->stack->size = t;
    }
    NEXT();

    //==========================================================================
    // 0x10-0x1f: Comparison & Bitwise Logic Operations
    //==========================================================================

op_lt:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1] = uint256_lt(&s[t], &s[t-1]) ? UINT256_ONE : UINT256_ZERO;
        evm->stack->size = t;
    }
    NEXT();

op_gt:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1] = uint256_gt(&s[t], &s[t-1]) ? UINT256_ONE : UINT256_ZERO;
        evm->stack->size = t;
    }
    NEXT();

op_slt:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1] = uint256_slt(&s[t], &s[t-1]) ? UINT256_ONE : UINT256_ZERO;
        evm->stack->size = t;
    }
    NEXT();

op_sgt:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1] = uint256_sgt(&s[t], &s[t-1]) ? UINT256_ONE : UINT256_ZERO;
        evm->stack->size = t;
    }
    NEXT();

op_eq:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1] = (s[t].low == s[t-1].low && s[t].high == s[t-1].high) ? UINT256_ONE : UINT256_ZERO;
        evm->stack->size = t;
    }
    NEXT();

op_iszero:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 1, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *top = &evm->stack->items[evm->stack->size - 1];
        *top = ((top->low | top->high) == 0) ? UINT256_ONE : UINT256_ZERO;
    }
    NEXT();

op_and:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1].low = s[t].low & s[t-1].low;
        s[t-1].high = s[t].high & s[t-1].high;
        evm->stack->size = t;
    }
    NEXT();

op_or:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1].low = s[t].low | s[t-1].low;
        s[t-1].high = s[t].high | s[t-1].high;
        evm->stack->size = t;
    }
    NEXT();

op_xor:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        s[t-1].low = s[t].low ^ s[t-1].low;
        s[t-1].high = s[t].high ^ s[t-1].high;
        evm->stack->size = t;
    }
    NEXT();

op_not:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 1, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *top = &evm->stack->items[evm->stack->size - 1];
        top->low = ~top->low;
        top->high = ~top->high;
    }
    NEXT();

op_byte:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        /* If index >= 32 (or has high bits set), result is zero */
        if (s[t].high != 0 || s[t].low >= 32)
            s[t-1] = UINT256_ZERO;
        else {
            /* EVM BYTE: index 0 = MSB, index 31 = LSB.
             * Extract directly from high/low without uint256_to_words. */
            unsigned int idx = (unsigned int)s[t].low;
            unsigned int bit_offset = (31 - idx) * 8;
            uint8_t b;
            if (bit_offset < 128)
                b = (uint8_t)(s[t-1].low >> bit_offset);
            else
                b = (uint8_t)(s[t-1].high >> (bit_offset - 128));
            s[t-1] = (uint256_t){ (uint128_t)b, 0 };
        }
        evm->stack->size = t;
    }
    NEXT();

op_shl:
    if (evm->fork < FORK_CONSTANTINOPLE) { status = EVM_INVALID_OPCODE; goto error; }
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (s[t].high || (uint64_t)(s[t].low >> 64) || uint256_to_uint64(&s[t]) >= 256)
            s[t-1] = UINT256_ZERO;
        else
            s[t-1] = uint256_shl(&s[t-1], (unsigned int)uint256_to_uint64(&s[t]));
        evm->stack->size = t;
    }
    NEXT();

op_shr:
    if (evm->fork < FORK_CONSTANTINOPLE) { status = EVM_INVALID_OPCODE; goto error; }
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (s[t].high || (uint64_t)(s[t].low >> 64) || uint256_to_uint64(&s[t]) >= 256)
            s[t-1] = UINT256_ZERO;
        else
            s[t-1] = uint256_shr(&s[t-1], (unsigned int)uint256_to_uint64(&s[t]));
        evm->stack->size = t;
    }
    NEXT();

op_sar:
    if (evm->fork < FORK_CONSTANTINOPLE) { status = EVM_INVALID_OPCODE; goto error; }
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (s[t].high || (uint64_t)(s[t].low >> 64) || uint256_to_uint64(&s[t]) >= 256) {
            bool is_neg = (s[t-1].high >> 127) & 1;
            s[t-1] = is_neg ? UINT256_MAX : UINT256_ZERO;
        } else
            s[t-1] = uint256_sar(&s[t-1], (unsigned int)uint256_to_uint64(&s[t]));
        evm->stack->size = t;
    }
    NEXT();

    //==========================================================================
    // 0x1e: CLZ (EIP-7939, Osaka+)
    //==========================================================================

op_clz:
    if (evm->fork < FORK_OSAKA) { status = EVM_INVALID_OPCODE; goto error; }
    if (!evm_use_gas(evm, GAS_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 1, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *top = &evm->stack->items[evm->stack->size - 1];
        unsigned int bit_len = (unsigned int)uint256_bit_length(top);
        *top = uint256_from_uint64(256 - bit_len);
    }
    NEXT();

    //==========================================================================
    // 0x20: Keccak256
    //==========================================================================

op_keccak256:
    status = op_keccak256(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

    //==========================================================================
    // 0x30-0x3f: Environmental Information
    //==========================================================================

    // Simple push-one-value pattern: gas + overflow check + push
#define PUSH_VAL(gas_cost, val)                                                \
    if (!evm_use_gas(evm, (gas_cost))) goto done_oog;                          \
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0))          \
        { status = EVM_STACK_OVERFLOW; goto error; }                           \
    evm->stack->items[evm->stack->size++] = (val);                             \
    NEXT();

#define PUSH_ADDR(addr_ptr)                                                    \
    if (!evm_use_gas(evm, GAS_BASE)) goto done_oog;                            \
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0))          \
        { status = EVM_STACK_OVERFLOW; goto error; }                           \
    evm->stack->items[evm->stack->size++] = addr_to_u256(addr_ptr);            \
    NEXT();

op_address:   PUSH_ADDR(&evm->msg.recipient)
op_balance:
    status = op_balance(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();
op_origin:    PUSH_ADDR(&evm->tx.origin)
op_caller:    PUSH_ADDR(&evm->msg.caller)
op_callvalue: PUSH_VAL(GAS_BASE, evm->msg.value)

op_calldataload:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 1, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *top = &evm->stack->items[evm->stack->size - 1];
        uint8_t _cdbuf[32] = {0};
        /* Only read if offset fits in 64 bits and is within calldata */
        if (top->high == 0 && (uint64_t)top->low == top->low) {
            uint64_t offset = uint256_to_uint64(top);
            if (offset < evm->msg.input_size) {
                size_t avail = evm->msg.input_size - offset;
                size_t n = avail < 32 ? avail : 32;
                memcpy(_cdbuf, evm->msg.input_data + offset, n);
            }
        }
        *top = bytes32_to_uint256(_cdbuf);
    }
    NEXT();

op_calldatasize:
    PUSH_VAL(GAS_BASE, ((uint256_t){ (uint128_t)evm->msg.input_size, 0 }))

op_calldatacopy:
    status = op_calldatacopy(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_codesize: PUSH_VAL(GAS_BASE, ((uint256_t){ (uint128_t)evm->code_size, 0 }))

op_codecopy:
    status = op_codecopy(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_gasprice: PUSH_VAL(GAS_BASE, evm->tx.gas_price)

op_extcodesize:
    status = op_extcodesize(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_extcodecopy:
    status = op_extcodecopy(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_returndatasize:
    if (evm->fork < FORK_BYZANTIUM) { status = EVM_INVALID_OPCODE; goto error; }
    PUSH_VAL(GAS_BASE, ((uint256_t){ (uint128_t)evm->return_data_size, 0 }))

op_returndatacopy:
    status = op_returndatacopy(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_extcodehash:
    status = op_extcodehash(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

#undef PUSH_VAL
#undef PUSH_ADDR

    //==========================================================================
    // 0x40-0x4f: Block Information
    //==========================================================================

    // Redefine PUSH_VAL/PUSH_ADDR for block info section
#define PUSH_VAL(gas_cost, val)                                                \
    if (!evm_use_gas(evm, (gas_cost))) goto done_oog;                          \
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0))          \
        { status = EVM_STACK_OVERFLOW; goto error; }                           \
    evm->stack->items[evm->stack->size++] = (val);                             \
    NEXT();

#define PUSH_ADDR(addr_ptr)                                                    \
    if (!evm_use_gas(evm, GAS_BASE)) goto done_oog;                            \
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0))          \
        { status = EVM_STACK_OVERFLOW; goto error; }                           \
    evm->stack->items[evm->stack->size++] = addr_to_u256(addr_ptr);            \
    NEXT();

op_blockhash:
    status = op_blockhash(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_coinbase:  PUSH_ADDR(&evm->block.coinbase)
op_timestamp: PUSH_VAL(GAS_BASE, ((uint256_t){ (uint128_t)evm->block.timestamp, 0 }))
op_number:    PUSH_VAL(GAS_BASE, ((uint256_t){ (uint128_t)evm->block.number, 0 }))
op_difficulty: PUSH_VAL(GAS_BASE, evm->block.difficulty)
op_gaslimit:  PUSH_VAL(GAS_BASE, ((uint256_t){ (uint128_t)evm->block.gas_limit, 0 }))
op_chainid:
    if (evm->fork < FORK_ISTANBUL) { status = EVM_INVALID_OPCODE; goto error; }
    PUSH_VAL(GAS_BASE, evm->block.chain_id)

op_selfbalance:
    status = op_selfbalance(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_basefee:
    if (evm->fork < FORK_LONDON) { status = EVM_INVALID_OPCODE; goto error; }
    PUSH_VAL(GAS_BASE, evm->block.base_fee)

op_blobhash:
    status = op_blobhash(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

op_blobbasefee:
    status = op_blobbasefee(evm);
    if (status != EVM_SUCCESS) goto error;
    NEXT();

#undef PUSH_VAL
#undef PUSH_ADDR

    //==========================================================================
    // 0x50-0x5f: Stack, Memory, Storage, and Flow Operations
    //==========================================================================

op_pop:
    if (!evm_use_gas(evm, GAS_BASE)) goto done_oog;
    if (__builtin_expect(evm->stack->size < 1, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    evm->stack->size--;
    NEXT();

op_mload:
    if (__builtin_expect(evm->stack->size < 1, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *top = &evm->stack->items[evm->stack->size - 1];
        if (top->high != 0 || (uint64_t)(top->low >> 64) != 0)
            { status = EVM_OUT_OF_GAS; goto error; }
        uint64_t off = (uint64_t)top->low;
        uint64_t mem_gas = evm_memory_access_cost(evm->memory, off, 32);
        if (!evm_use_gas(evm, GAS_VERY_LOW + mem_gas)) goto done_oog;
        if (!evm_memory_expand(evm->memory, off, 32))
            { status = EVM_INVALID_MEMORY_ACCESS; goto error; }
        /* Read 32 big-endian bytes directly into stack slot */
        const uint8_t *src = evm->memory->data + off;
        uint64_t w0, w1, w2, w3;
        memcpy(&w3, src +  0, 8); memcpy(&w2, src +  8, 8);
        memcpy(&w1, src + 16, 8); memcpy(&w0, src + 24, 8);
        top->low  = ((__uint128_t)__builtin_bswap64(w1) << 64) | __builtin_bswap64(w0);
        top->high = ((__uint128_t)__builtin_bswap64(w3) << 64) | __builtin_bswap64(w2);
    }
    NEXT();

op_mstore:
    if (__builtin_expect(evm->stack->size < 2, 0)) { status = EVM_STACK_UNDERFLOW; goto error; }
    {
        uint256_t *s = evm->stack->items;
        size_t t = evm->stack->size - 1;
        if (s[t].high != 0 || (uint64_t)(s[t].low >> 64) != 0)
            { status = EVM_OUT_OF_GAS; goto error; }
        uint64_t off = (uint64_t)s[t].low;
        uint64_t mem_gas = evm_memory_access_cost(evm->memory, off, 32);
        if (!evm_use_gas(evm, GAS_VERY_LOW + mem_gas)) goto done_oog;
        if (!evm_memory_expand(evm->memory, off, 32))
            { status = EVM_INVALID_MEMORY_ACCESS; goto error; }
        /* Write 32 big-endian bytes directly from stack value */
        uint8_t *dst = evm->memory->data + off;
        uint64_t w0 = __builtin_bswap64((uint64_t)s[t-1].low);
        uint64_t w1 = __builtin_bswap64((uint64_t)(s[t-1].low >> 64));
        uint64_t w2 = __builtin_bswap64((uint64_t)s[t-1].high);
        uint64_t w3 = __builtin_bswap64((uint64_t)(s[t-1].high >> 64));
        memcpy(dst +  0, &w3, 8); memcpy(dst +  8, &w2, 8);
        memcpy(dst + 16, &w1, 8); memcpy(dst + 24, &w0, 8);
        evm->stack->size = t - 1;
    }
    NEXT();

op_mstore8:
    status = op_mstore8(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_sload:
    status = op_sload(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_sstore:
    status = op_sstore(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_jump:
    status = op_jump(evm);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH(); // PC already updated by jump

op_jumpi:
    status = op_jumpi(evm);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH(); // PC may have been updated by jumpi

op_pc:
    if (!evm_use_gas(evm, GAS_BASE)) goto done_oog;
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0)) { status = EVM_STACK_OVERFLOW; goto error; }
    evm->stack->items[evm->stack->size++] = (uint256_t){ (uint128_t)evm->pc, 0 };
    NEXT();

op_msize:
    status = op_msize(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_gas:
    if (!evm_use_gas(evm, GAS_BASE)) goto done_oog;
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0)) { status = EVM_STACK_OVERFLOW; goto error; }
    evm->stack->items[evm->stack->size++] = (uint256_t){ (uint128_t)evm->gas_left, 0 };
    NEXT();

op_jumpdest:
    if (!evm_use_gas(evm, 1)) goto done_oog;
    NEXT();

op_tload:
    status = op_tload(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_tstore:
    status = op_tstore(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_mcopy:
    status = op_mcopy(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_push0:
    if (evm->fork < FORK_SHANGHAI) { status = EVM_INVALID_OPCODE; goto error; }
    if (!evm_use_gas(evm, GAS_BASE)) goto done_oog;
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0)) { status = EVM_STACK_OVERFLOW; goto error; }
    evm->stack->items[evm->stack->size++] = UINT256_ZERO;
    NEXT();

    //==========================================================================
    // 0x60-0x7f: Push Operations
    //==========================================================================

    // PUSH1: ultra-tight specialization (most common opcode ~15%)
op_push1:
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0))
        { status = EVM_STACK_OVERFLOW; goto error; }
    evm->stack->items[evm->stack->size++] = (uint256_t){
        (evm->pc + 1 < evm->code_size) ? (uint128_t)evm->code[evm->pc + 1] : 0, 0
    };
    evm->pc += 2;
    DISPATCH();

    // PUSH2-32: general inline macro
#define INLINE_PUSH(N)                                                         \
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;                        \
    if (__builtin_expect(evm->stack->size >= EVM_STACK_MAX_DEPTH, 0))          \
        { status = EVM_STACK_OVERFLOW; goto error; }                           \
    {                                                                          \
        uint8_t _buf[32] = {0};                                                \
        uint64_t _avail = evm->code_size - evm->pc - 1;                        \
        uint8_t _n = ((N) <= _avail) ? (N) : (uint8_t)_avail;                  \
        memcpy(_buf + 32 - (N), evm->code + evm->pc + 1, _n);                  \
        evm->stack->items[evm->stack->size++] = bytes32_to_uint256(_buf);      \
        VERKLE_PUSH_GAS(N)                                                     \
        evm->pc += 1 + (N);                                                    \
    }                                                                          \
    DISPATCH();

op_push2:  INLINE_PUSH(2)
op_push3:  INLINE_PUSH(3)
op_push4:  INLINE_PUSH(4)
op_push5:  INLINE_PUSH(5)
op_push6:  INLINE_PUSH(6)
op_push7:  INLINE_PUSH(7)
op_push8:  INLINE_PUSH(8)
op_push9:  INLINE_PUSH(9)
op_push10: INLINE_PUSH(10)
op_push11: INLINE_PUSH(11)
op_push12: INLINE_PUSH(12)
op_push13: INLINE_PUSH(13)
op_push14: INLINE_PUSH(14)
op_push15: INLINE_PUSH(15)
op_push16: INLINE_PUSH(16)
op_push17: INLINE_PUSH(17)
op_push18: INLINE_PUSH(18)
op_push19: INLINE_PUSH(19)
op_push20: INLINE_PUSH(20)
op_push21: INLINE_PUSH(21)
op_push22: INLINE_PUSH(22)
op_push23: INLINE_PUSH(23)
op_push24: INLINE_PUSH(24)
op_push25: INLINE_PUSH(25)
op_push26: INLINE_PUSH(26)
op_push27: INLINE_PUSH(27)
op_push28: INLINE_PUSH(28)
op_push29: INLINE_PUSH(29)
op_push30: INLINE_PUSH(30)
op_push31: INLINE_PUSH(31)
op_push32: INLINE_PUSH(32)

#undef INLINE_PUSH

    //==========================================================================
    // 0x80-0x8f: Dup Operations
    //==========================================================================

#define INLINE_DUP(N)                                                          \
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;                        \
    if (__builtin_expect(evm->stack->size < (N) ||                             \
        evm->stack->size >= EVM_STACK_MAX_DEPTH, 0)) {                         \
        status = (evm->stack->size >= EVM_STACK_MAX_DEPTH) ?                   \
            EVM_STACK_OVERFLOW : EVM_STACK_UNDERFLOW;                          \
        goto error;                                                            \
    }                                                                          \
    evm->stack->items[evm->stack->size] =                                      \
        evm->stack->items[evm->stack->size - (N)];                             \
    evm->stack->size++;                                                        \
    NEXT();

op_dup1:  INLINE_DUP(1)
op_dup2:  INLINE_DUP(2)
op_dup3:  INLINE_DUP(3)
op_dup4:  INLINE_DUP(4)
op_dup5:  INLINE_DUP(5)
op_dup6:  INLINE_DUP(6)
op_dup7:  INLINE_DUP(7)
op_dup8:  INLINE_DUP(8)
op_dup9:  INLINE_DUP(9)
op_dup10: INLINE_DUP(10)
op_dup11: INLINE_DUP(11)
op_dup12: INLINE_DUP(12)
op_dup13: INLINE_DUP(13)
op_dup14: INLINE_DUP(14)
op_dup15: INLINE_DUP(15)
op_dup16: INLINE_DUP(16)

#undef INLINE_DUP

    //==========================================================================
    // 0x90-0x9f: Swap Operations
    //==========================================================================

#define INLINE_SWAP(N)                                                         \
    if (!evm_use_gas(evm, GAS_VERY_LOW)) goto done_oog;                        \
    if (__builtin_expect(evm->stack->size < (size_t)((N) + 1), 0)) {           \
        status = EVM_STACK_UNDERFLOW; goto error;                              \
    }                                                                          \
    {                                                                          \
        size_t top_idx = evm->stack->size - 1;                                 \
        uint256_t tmp = evm->stack->items[top_idx];                            \
        evm->stack->items[top_idx] = evm->stack->items[top_idx - (N)];         \
        evm->stack->items[top_idx - (N)] = tmp;                                \
    }                                                                          \
    NEXT();

op_swap1:  INLINE_SWAP(1)
op_swap2:  INLINE_SWAP(2)
op_swap3:  INLINE_SWAP(3)
op_swap4:  INLINE_SWAP(4)
op_swap5:  INLINE_SWAP(5)
op_swap6:  INLINE_SWAP(6)
op_swap7:  INLINE_SWAP(7)
op_swap8:  INLINE_SWAP(8)
op_swap9:  INLINE_SWAP(9)
op_swap10: INLINE_SWAP(10)
op_swap11: INLINE_SWAP(11)
op_swap12: INLINE_SWAP(12)
op_swap13: INLINE_SWAP(13)
op_swap14: INLINE_SWAP(14)
op_swap15: INLINE_SWAP(15)
op_swap16: INLINE_SWAP(16)

#undef INLINE_SWAP

    //==========================================================================
    // 0xa0-0xaf: Log Operations
    //==========================================================================

op_log0:
    status = op_log0(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_log1:
    status = op_log1(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_log2:
    status = op_log2(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_log3:
    status = op_log3(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_log4:
    status = op_log4(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

    //==========================================================================
    // 0xf0-0xff: System Operations
    //==========================================================================

op_create:
    status = op_create(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_call:
    status = op_call(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_callcode:
    status = op_callcode(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_return:
    status = op_return(evm);
    if (status != EVM_SUCCESS) goto error;
    EVM_TRACE_EXIT(evm, NULL);
    goto done;

op_delegatecall:
    status = op_delegatecall(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_create2:
    status = op_create2(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_staticcall:
    status = op_staticcall(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_revert:
    status = op_revert(evm);
    if (status != EVM_REVERT) goto error;
    EVM_TRACE_EXIT(evm, NULL);
    goto done;

op_selfdestruct:
    status = op_selfdestruct(evm);
    if (status != EVM_SUCCESS) goto error;
    EVM_TRACE_EXIT(evm, NULL);
    // SELFDESTRUCT produces no output — clear stale return data from subcalls
    if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
    evm->return_data_size = 0;
    goto done;

    //==========================================================================
    // Invalid Opcode Handler
    //==========================================================================

op_invalid:
    status = op_invalid(evm);
    goto error;

    //==========================================================================
    // Exit Points
    //==========================================================================

done_oog:
    status = EVM_OUT_OF_GAS;
    EVM_TRACE_EXIT(evm, "out of gas");
    // Fall through to error

error:
    if (status != EVM_OUT_OF_GAS) {
        EVM_TRACE_EXIT(evm, "execution error");
    }
    // All exceptional halts (non-REVERT) consume all remaining gas per EVM spec.
    // Only REVERT preserves remaining gas.
    if (status != EVM_REVERT)
        evm->gas_left = 0;
    // For non-REVERT errors, also clear return data (no output on error).
    if (status != EVM_REVERT)
    {
        if (evm->return_data)
        {
            free(evm->return_data);
            evm->return_data = NULL;
        }
        evm->return_data_size = 0;
    }
    // Fall through to done

done:
    // Emit any remaining pending trace (e.g. when pc >= code_size)
    EVM_TRACE_EXIT(evm, NULL);
    // Free JUMPDEST bitmap
    free(jumpdest_bitmap);
    evm->jumpdest_bitmap = NULL;
    // Create result with output data
    evm_result_t res = evm_result_create(status, evm->gas_left, evm->gas_refund, evm->return_data, evm->return_data_size);
    evm->return_data = NULL;  /* ownership transferred */
    evm->return_data_size = 0;
    return res;
}
