/**
 * VM Interpreter — Computed Goto Dispatch
 *
 * High-performance bytecode interpreter for validated EOF containers.
 * Uses GCC/Clang computed goto (threaded dispatch) for per-opcode
 * branch prediction, giving ~15-30% speedup over switch statements.
 *
 * Because EOF validation guarantees:
 *   - All opcodes are valid
 *   - All jump targets are correct
 *   - No stack underflow/overflow possible
 *   - Every code path terminates
 * The hot path is just: gas check → compute → dispatch.
 * No runtime safety checks needed in the inner loop.
 *
 * Phase 2 implements all "pure" opcodes (arithmetic, comparison, bitwise,
 * stack, control flow, memory, data, crypto). State-dependent opcodes
 * (SLOAD/SSTORE, BALANCE, EXTCALL, etc.) are stubbed for Phase 3/4.
 */

#include "vm.h"
#include "gas.h"
#include "keccak256.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Inline Helpers
//==============================================================================

static inline uint16_t read_u16(const uint8_t *p)
{
    return (uint16_t)((p[0] << 8) | p[1]);
}

static inline int16_t read_i16(const uint8_t *p)
{
    return (int16_t)read_u16(p);
}

/** Return true if val fits in uint64, store in *out. On failure, *out = MAX. */
static inline bool to_u64(const uint256_t *val, uint64_t *out)
{
    if (val->high != 0 || val->low > (uint128_t)UINT64_MAX) {
        *out = UINT64_MAX;
        return false;
    }
    *out = (uint64_t)val->low;
    return true;
}

/** Saturating addition for gas costs. */
static inline uint64_t safe_add_gas(uint64_t a, uint64_t b)
{
    if (a > UINT64_MAX - b) return UINT64_MAX;
    return a + b;
}

//==============================================================================
// Interpreter
//==============================================================================

vm_status_t vm_interpret(vm_t *vm)
{
    // Cache hot state in locals for register allocation
    eof_container_t *c = vm->container;
    eof_func_t      *func = &c->functions[vm->current_func];
    const uint8_t   *code = func->code;
    uint256_t       *stack = vm->stack;
    uint16_t         sp = vm->sp;
    uint32_t         pc = vm->pc;
    uint64_t         gas_left = vm->gas_left;
    vm_memory_t     *memory = vm->memory;

    // Shared variable for PUSH/DUP/SWAP families
    uint32_t n;

    //--------------------------------------------------------------------------
    // Macros
    //--------------------------------------------------------------------------

    // Dispatch: read opcode, advance pc past it, jump to handler.
    // EOF validation guarantees pc stays within code bounds.
    #define DISPATCH() goto *dispatch_table[code[pc++]]

    // Gas check: deduct or jump to out_of_gas.
    #define USE_GAS(amount)                             \
        do {                                            \
            uint64_t _cost = (amount);                  \
            if (__builtin_expect(_cost > gas_left, 0))  \
                goto out_of_gas;                        \
            gas_left -= _cost;                          \
        } while (0)

    //--------------------------------------------------------------------------
    // Dispatch Table (GCC computed goto)
    //--------------------------------------------------------------------------

    static const void *dispatch_table[256] = {
        // Fill all 256 entries with invalid, then override
        [0 ... 255] = &&op_invalid,

        // 0x00: Stop and Arithmetic
        [0x00] = &&op_stop,
        [0x01] = &&op_add,
        [0x02] = &&op_mul,
        [0x03] = &&op_sub,
        [0x04] = &&op_div,
        [0x05] = &&op_sdiv,
        [0x06] = &&op_mod,
        [0x07] = &&op_smod,
        [0x08] = &&op_addmod,
        [0x09] = &&op_mulmod,
        [0x0A] = &&op_exp,
        [0x0B] = &&op_signextend,

        // 0x10-0x1D: Comparison & Bitwise
        [0x10] = &&op_lt,
        [0x11] = &&op_gt,
        [0x12] = &&op_slt,
        [0x13] = &&op_sgt,
        [0x14] = &&op_eq,
        [0x15] = &&op_iszero,
        [0x16] = &&op_and,
        [0x17] = &&op_or,
        [0x18] = &&op_xor,
        [0x19] = &&op_not,
        [0x1A] = &&op_byte,
        [0x1B] = &&op_shl,
        [0x1C] = &&op_shr,
        [0x1D] = &&op_sar,

        // 0x20: Keccak256
        [0x20] = &&op_keccak256,

        // 0x30-0x3F: Environmental (Phase 3/4 — stub)
        [0x30] = &&op_unimplemented,  // ADDRESS
        [0x32] = &&op_unimplemented,  // ORIGIN
        [0x33] = &&op_unimplemented,  // CALLER
        [0x34] = &&op_unimplemented,  // CALLVALUE
        [0x35] = &&op_unimplemented,  // CALLDATALOAD
        [0x36] = &&op_unimplemented,  // CALLDATASIZE
        [0x37] = &&op_unimplemented,  // CALLDATACOPY
        [0x3A] = &&op_unimplemented,  // GASPRICE
        [0x3D] = &&op_unimplemented,  // RETURNDATASIZE
        [0x3E] = &&op_unimplemented,  // RETURNDATACOPY

        // 0x40-0x4A: Block Info (Phase 3/4 — stub)
        [0x40] = &&op_unimplemented,  // BLOCKHASH
        [0x41] = &&op_unimplemented,  // COINBASE
        [0x42] = &&op_unimplemented,  // TIMESTAMP
        [0x43] = &&op_unimplemented,  // NUMBER
        [0x44] = &&op_unimplemented,  // PREVRANDAO
        [0x45] = &&op_unimplemented,  // GASLIMIT
        [0x46] = &&op_unimplemented,  // CHAINID
        [0x47] = &&op_unimplemented,  // SELFBALANCE
        [0x48] = &&op_unimplemented,  // BASEFEE
        [0x49] = &&op_unimplemented,  // BLOBHASH
        [0x4A] = &&op_unimplemented,  // BLOBBASEFEE

        // 0x50-0x5F: Stack, Memory, Flow
        [0x50] = &&op_pop,
        [0x51] = &&op_mload,
        [0x52] = &&op_mstore,
        [0x53] = &&op_mstore8,
        [0x54] = &&op_unimplemented,  // SLOAD
        [0x55] = &&op_unimplemented,  // SSTORE
        [0x59] = &&op_msize,
        [0x5B] = &&op_nop,
        [0x5C] = &&op_unimplemented,  // TLOAD
        [0x5D] = &&op_unimplemented,  // TSTORE
        [0x5E] = &&op_mcopy,
        [0x5F] = &&op_push0,

        // 0x60-0x7F: PUSH1-PUSH32
        [0x60] = &&op_push1,   [0x61] = &&op_push2,
        [0x62] = &&op_push3,   [0x63] = &&op_push4,
        [0x64] = &&op_push5,   [0x65] = &&op_push6,
        [0x66] = &&op_push7,   [0x67] = &&op_push8,
        [0x68] = &&op_push9,   [0x69] = &&op_push10,
        [0x6A] = &&op_push11,  [0x6B] = &&op_push12,
        [0x6C] = &&op_push13,  [0x6D] = &&op_push14,
        [0x6E] = &&op_push15,  [0x6F] = &&op_push16,
        [0x70] = &&op_push17,  [0x71] = &&op_push18,
        [0x72] = &&op_push19,  [0x73] = &&op_push20,
        [0x74] = &&op_push21,  [0x75] = &&op_push22,
        [0x76] = &&op_push23,  [0x77] = &&op_push24,
        [0x78] = &&op_push25,  [0x79] = &&op_push26,
        [0x7A] = &&op_push27,  [0x7B] = &&op_push28,
        [0x7C] = &&op_push29,  [0x7D] = &&op_push30,
        [0x7E] = &&op_push31,  [0x7F] = &&op_push32,

        // 0x80-0x8F: DUP1-DUP16
        [0x80] = &&op_dup1,   [0x81] = &&op_dup2,
        [0x82] = &&op_dup3,   [0x83] = &&op_dup4,
        [0x84] = &&op_dup5,   [0x85] = &&op_dup6,
        [0x86] = &&op_dup7,   [0x87] = &&op_dup8,
        [0x88] = &&op_dup9,   [0x89] = &&op_dup10,
        [0x8A] = &&op_dup11,  [0x8B] = &&op_dup12,
        [0x8C] = &&op_dup13,  [0x8D] = &&op_dup14,
        [0x8E] = &&op_dup15,  [0x8F] = &&op_dup16,

        // 0x90-0x9F: SWAP1-SWAP16
        [0x90] = &&op_swap1,   [0x91] = &&op_swap2,
        [0x92] = &&op_swap3,   [0x93] = &&op_swap4,
        [0x94] = &&op_swap5,   [0x95] = &&op_swap6,
        [0x96] = &&op_swap7,   [0x97] = &&op_swap8,
        [0x98] = &&op_swap9,   [0x99] = &&op_swap10,
        [0x9A] = &&op_swap11,  [0x9B] = &&op_swap12,
        [0x9C] = &&op_swap13,  [0x9D] = &&op_swap14,
        [0x9E] = &&op_swap15,  [0x9F] = &&op_swap16,

        // 0xA0-0xA4: LOG0-LOG4 (Phase 3/4 — stub)
        [0xA0] = &&op_unimplemented,
        [0xA1] = &&op_unimplemented,
        [0xA2] = &&op_unimplemented,
        [0xA3] = &&op_unimplemented,
        [0xA4] = &&op_unimplemented,

        // 0xD0-0xD3: Data Section
        [0xD0] = &&op_dataload,
        [0xD1] = &&op_dataloadn,
        [0xD2] = &&op_datasize,
        [0xD3] = &&op_datacopy,

        // 0xE0-0xE8: EOF Control Flow & Stack
        [0xE0] = &&op_rjump,
        [0xE1] = &&op_rjumpi,
        [0xE2] = &&op_rjumpv,
        [0xE3] = &&op_callf,
        [0xE4] = &&op_retf,
        [0xE5] = &&op_jumpf,
        [0xE6] = &&op_dupn,
        [0xE7] = &&op_swapn,
        [0xE8] = &&op_exchange,

        // 0xEC, 0xEE: Create (Phase 3/4 — stub)
        [0xEC] = &&op_unimplemented,  // EOFCREATE
        [0xEE] = &&op_unimplemented,  // RETURNCONTRACT

        // 0xF3: RETURN
        [0xF3] = &&op_return,

        // 0xF7: RETURNDATALOAD (Phase 3/4 — stub)
        [0xF7] = &&op_unimplemented,

        // 0xF8-0xFB: Calls (Phase 3/4 — stub)
        [0xF8] = &&op_unimplemented,  // EXTCALL
        [0xF9] = &&op_unimplemented,  // EXTDELEGATECALL
        [0xFB] = &&op_unimplemented,  // EXTSTATICCALL

        // 0xFD-0xFE: Revert & Invalid
        [0xFD] = &&op_revert,
        [0xFE] = &&op_invalid_instruction,
    };

    //--------------------------------------------------------------------------
    // Begin Execution
    //--------------------------------------------------------------------------

    DISPATCH();

    //==========================================================================
    // Terminating Opcodes
    //==========================================================================

op_stop:
    // STOP (0x00): success, no return data
    free(vm->return_data);
    vm->return_data = NULL;
    vm->return_data_size = 0;
    vm->status = VM_SUCCESS;
    goto done;

op_return: {
    // RETURN (0xF3): pop offset, size; return memory region
    uint64_t offset, size;
    if (!to_u64(&stack[sp - 1], &offset)) goto out_of_gas;
    if (!to_u64(&stack[sp - 2], &size))   goto out_of_gas;
    sp -= 2;

    if (size > 0) {
        if (offset > UINT64_MAX - size) goto out_of_gas;
        uint64_t exp_cost = vm_memory_access_cost(memory, offset, size);
        USE_GAS(exp_cost);
        vm_memory_expand(memory, offset, size);
    }

    free(vm->return_data);
    vm->return_data = NULL;
    vm->return_data_size = 0;
    if (size > 0) {
        vm->return_data = (uint8_t *)malloc(size);
        if (vm->return_data) {
            memcpy(vm->return_data, &memory->data[offset], size);
            vm->return_data_size = size;
        }
    }
    vm->status = VM_SUCCESS;
    goto done;
}

op_revert: {
    // REVERT (0xFD): same as RETURN but status = REVERT
    uint64_t offset, size;
    if (!to_u64(&stack[sp - 1], &offset)) goto out_of_gas;
    if (!to_u64(&stack[sp - 2], &size))   goto out_of_gas;
    sp -= 2;

    if (size > 0) {
        if (offset > UINT64_MAX - size) goto out_of_gas;
        uint64_t exp_cost = vm_memory_access_cost(memory, offset, size);
        USE_GAS(exp_cost);
        vm_memory_expand(memory, offset, size);
    }

    free(vm->return_data);
    vm->return_data = NULL;
    vm->return_data_size = 0;
    if (size > 0) {
        vm->return_data = (uint8_t *)malloc(size);
        if (vm->return_data) {
            memcpy(vm->return_data, &memory->data[offset], size);
            vm->return_data_size = size;
        }
    }
    vm->status = VM_REVERT;
    goto done;
}

op_invalid_instruction:
    // INVALID (0xFE): consume all gas
    gas_left = 0;
    vm->status = VM_INVALID_OPCODE;
    goto done;

    //==========================================================================
    // Arithmetic (0x01-0x0B)
    //==========================================================================

op_add:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_add(&stack[sp - 1], &stack[sp]);
    DISPATCH();

op_mul:
    USE_GAS(VM_GAS_LOW);
    sp--;
    stack[sp - 1] = uint256_mul(&stack[sp - 1], &stack[sp]);
    DISPATCH();

op_sub:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    // a = stack[sp] (was top), b = stack[sp-1]; result = a - b
    stack[sp - 1] = uint256_sub(&stack[sp], &stack[sp - 1]);
    DISPATCH();

op_div:
    USE_GAS(VM_GAS_LOW);
    sp--;
    // a / b, a was top
    stack[sp - 1] = uint256_div(&stack[sp], &stack[sp - 1]);
    DISPATCH();

op_sdiv:
    USE_GAS(VM_GAS_LOW);
    sp--;
    stack[sp - 1] = uint256_sdiv(&stack[sp], &stack[sp - 1]);
    DISPATCH();

op_mod:
    USE_GAS(VM_GAS_LOW);
    sp--;
    stack[sp - 1] = uint256_mod(&stack[sp], &stack[sp - 1]);
    DISPATCH();

op_smod:
    USE_GAS(VM_GAS_LOW);
    sp--;
    stack[sp - 1] = uint256_smod(&stack[sp], &stack[sp - 1]);
    DISPATCH();

op_addmod:
    // ADDMOD(a, b, N): (a + b) % N; a=top, b=second, N=third
    USE_GAS(VM_GAS_MID);
    sp -= 2;
    // After sp -= 2: stack[sp+1]=a, stack[sp]=b, stack[sp-1]=N
    stack[sp - 1] = uint256_addmod(&stack[sp + 1], &stack[sp], &stack[sp - 1]);
    DISPATCH();

op_mulmod:
    USE_GAS(VM_GAS_MID);
    sp -= 2;
    stack[sp - 1] = uint256_mulmod(&stack[sp + 1], &stack[sp], &stack[sp - 1]);
    DISPATCH();

op_exp: {
    // EXP(base, exponent): base=top, exponent=second
    sp--;
    // base = stack[sp], exponent = stack[sp-1]
    int bits = uint256_bit_length(&stack[sp - 1]);
    uint8_t exp_bytes = (bits == 0) ? 0 : (uint8_t)((bits + 7) / 8);
    USE_GAS(vm_gas_exp_cost(exp_bytes));
    stack[sp - 1] = uint256_exp(&stack[sp], &stack[sp - 1]);
    DISPATCH();
}

op_signextend: {
    // SIGNEXTEND(b, x): b=byte_pos=top, x=value=second
    USE_GAS(VM_GAS_LOW);
    sp--;
    // b = stack[sp], x = stack[sp-1]
    if (stack[sp].high == 0 && stack[sp].low < 31) {
        stack[sp - 1] = uint256_signextend(&stack[sp - 1],
                                            (unsigned int)(uint64_t)stack[sp].low);
    }
    // else: b >= 31, x unchanged
    DISPATCH();
}

    //==========================================================================
    // Comparison (0x10-0x15)
    //==========================================================================

op_lt:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    // a=stack[sp] (top), b=stack[sp-1]; result = a < b
    stack[sp - 1] = uint256_is_less(&stack[sp], &stack[sp - 1])
                      ? UINT256_ONE : UINT256_ZERO;
    DISPATCH();

op_gt:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_is_greater(&stack[sp], &stack[sp - 1])
                      ? UINT256_ONE : UINT256_ZERO;
    DISPATCH();

op_slt:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_slt(&stack[sp], &stack[sp - 1])
                      ? UINT256_ONE : UINT256_ZERO;
    DISPATCH();

op_sgt:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_sgt(&stack[sp], &stack[sp - 1])
                      ? UINT256_ONE : UINT256_ZERO;
    DISPATCH();

op_eq:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_is_equal(&stack[sp], &stack[sp - 1])
                      ? UINT256_ONE : UINT256_ZERO;
    DISPATCH();

op_iszero:
    USE_GAS(VM_GAS_VERY_LOW);
    stack[sp - 1] = uint256_is_zero(&stack[sp - 1])
                      ? UINT256_ONE : UINT256_ZERO;
    DISPATCH();

    //==========================================================================
    // Bitwise (0x16-0x1D)
    //==========================================================================

op_and:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_and(&stack[sp - 1], &stack[sp]);
    DISPATCH();

op_or:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_or(&stack[sp - 1], &stack[sp]);
    DISPATCH();

op_xor:
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    stack[sp - 1] = uint256_xor(&stack[sp - 1], &stack[sp]);
    DISPATCH();

op_not:
    USE_GAS(VM_GAS_VERY_LOW);
    stack[sp - 1] = uint256_not(&stack[sp - 1]);
    DISPATCH();

op_byte: {
    // BYTE(i, x): i=top, x=second; extract byte i (0=MSB)
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    // i = stack[sp], x = stack[sp-1]
    if (stack[sp].high != 0 || stack[sp].low >= 32) {
        stack[sp - 1] = UINT256_ZERO;
    } else {
        uint8_t b = uint256_byte(&stack[sp - 1],
                                  (unsigned int)(uint64_t)stack[sp].low);
        stack[sp - 1] = uint256_from_uint64(b);
    }
    DISPATCH();
}

op_shl: {
    // SHL(shift, value): shift=top, value=second; result = value << shift
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    if (stack[sp].high != 0 || stack[sp].low >= 256) {
        stack[sp - 1] = UINT256_ZERO;
    } else {
        stack[sp - 1] = uint256_shl(&stack[sp - 1],
                                     (unsigned int)(uint64_t)stack[sp].low);
    }
    DISPATCH();
}

op_shr: {
    // SHR(shift, value): shift=top, value=second
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    if (stack[sp].high != 0 || stack[sp].low >= 256) {
        stack[sp - 1] = UINT256_ZERO;
    } else {
        stack[sp - 1] = uint256_shr(&stack[sp - 1],
                                     (unsigned int)(uint64_t)stack[sp].low);
    }
    DISPATCH();
}

op_sar: {
    // SAR(shift, value): arithmetic right shift
    USE_GAS(VM_GAS_VERY_LOW);
    sp--;
    if (stack[sp].high != 0 || stack[sp].low >= 256) {
        // If value is negative (MSB set), result is all 1s; else all 0s
        bool negative = uint256_get_bit(&stack[sp - 1], 255);
        stack[sp - 1] = negative ? UINT256_MAX : UINT256_ZERO;
    } else {
        stack[sp - 1] = uint256_sar(&stack[sp - 1],
                                     (unsigned int)(uint64_t)stack[sp].low);
    }
    DISPATCH();
}

    //==========================================================================
    // Crypto (0x20)
    //==========================================================================

op_keccak256: {
    // TODO: Replace keccak256 with Pedersen hash for Verkle-native VM
    // KECCAK256(offset, size): hash memory region
    uint64_t offset, size;
    if (!to_u64(&stack[sp - 1], &offset)) goto out_of_gas;
    if (!to_u64(&stack[sp - 2], &size))   goto out_of_gas;
    sp -= 2;

    uint64_t sha3_cost = vm_gas_sha3_cost(size);
    uint64_t exp_cost = 0;
    if (size > 0) {
        if (offset > UINT64_MAX - size) goto out_of_gas;
        exp_cost = vm_memory_access_cost(memory, offset, size);
    }
    USE_GAS(safe_add_gas(sha3_cost, exp_cost));

    if (size > 0) {
        vm_memory_expand(memory, offset, size);
    }

    SHA3_CTX ctx;
    keccak_init(&ctx);
    if (size > 0) {
        const uint8_t *ptr = &memory->data[offset];
        uint64_t remaining = size;
        while (remaining > 0) {
            uint16_t chunk = (remaining > 65535) ? 65535 : (uint16_t)remaining;
            keccak_update(&ctx, ptr, chunk);
            ptr += chunk;
            remaining -= chunk;
        }
    }
    uint8_t hash[32];
    keccak_final(&ctx, hash);

    stack[sp] = uint256_from_bytes(hash, 32);
    sp++;
    DISPATCH();
}

    //==========================================================================
    // Stack Operations
    //==========================================================================

op_pop:
    USE_GAS(VM_GAS_BASE);
    sp--;
    DISPATCH();

op_push0:
    USE_GAS(VM_GAS_BASE);
    stack[sp] = UINT256_ZERO;
    sp++;
    DISPATCH();

    // PUSH1-PUSH32: each sets n and jumps to shared handler
op_push1:  n = 1;  goto do_push;
op_push2:  n = 2;  goto do_push;
op_push3:  n = 3;  goto do_push;
op_push4:  n = 4;  goto do_push;
op_push5:  n = 5;  goto do_push;
op_push6:  n = 6;  goto do_push;
op_push7:  n = 7;  goto do_push;
op_push8:  n = 8;  goto do_push;
op_push9:  n = 9;  goto do_push;
op_push10: n = 10; goto do_push;
op_push11: n = 11; goto do_push;
op_push12: n = 12; goto do_push;
op_push13: n = 13; goto do_push;
op_push14: n = 14; goto do_push;
op_push15: n = 15; goto do_push;
op_push16: n = 16; goto do_push;
op_push17: n = 17; goto do_push;
op_push18: n = 18; goto do_push;
op_push19: n = 19; goto do_push;
op_push20: n = 20; goto do_push;
op_push21: n = 21; goto do_push;
op_push22: n = 22; goto do_push;
op_push23: n = 23; goto do_push;
op_push24: n = 24; goto do_push;
op_push25: n = 25; goto do_push;
op_push26: n = 26; goto do_push;
op_push27: n = 27; goto do_push;
op_push28: n = 28; goto do_push;
op_push29: n = 29; goto do_push;
op_push30: n = 30; goto do_push;
op_push31: n = 31; goto do_push;
op_push32: n = 32; goto do_push;

do_push:
    USE_GAS(VM_GAS_VERY_LOW);
    stack[sp] = uint256_from_bytes(&code[pc], n);
    pc += n;
    sp++;
    DISPATCH();

    // DUP1-DUP16: duplicate the Nth stack item from top
op_dup1:  n = 1;  goto do_dup;
op_dup2:  n = 2;  goto do_dup;
op_dup3:  n = 3;  goto do_dup;
op_dup4:  n = 4;  goto do_dup;
op_dup5:  n = 5;  goto do_dup;
op_dup6:  n = 6;  goto do_dup;
op_dup7:  n = 7;  goto do_dup;
op_dup8:  n = 8;  goto do_dup;
op_dup9:  n = 9;  goto do_dup;
op_dup10: n = 10; goto do_dup;
op_dup11: n = 11; goto do_dup;
op_dup12: n = 12; goto do_dup;
op_dup13: n = 13; goto do_dup;
op_dup14: n = 14; goto do_dup;
op_dup15: n = 15; goto do_dup;
op_dup16: n = 16; goto do_dup;

do_dup:
    USE_GAS(VM_GAS_VERY_LOW);
    stack[sp] = stack[sp - n];
    sp++;
    DISPATCH();

    // SWAP1-SWAP16: swap top with Nth item below top
op_swap1:  n = 1;  goto do_swap;
op_swap2:  n = 2;  goto do_swap;
op_swap3:  n = 3;  goto do_swap;
op_swap4:  n = 4;  goto do_swap;
op_swap5:  n = 5;  goto do_swap;
op_swap6:  n = 6;  goto do_swap;
op_swap7:  n = 7;  goto do_swap;
op_swap8:  n = 8;  goto do_swap;
op_swap9:  n = 9;  goto do_swap;
op_swap10: n = 10; goto do_swap;
op_swap11: n = 11; goto do_swap;
op_swap12: n = 12; goto do_swap;
op_swap13: n = 13; goto do_swap;
op_swap14: n = 14; goto do_swap;
op_swap15: n = 15; goto do_swap;
op_swap16: n = 16; goto do_swap;

do_swap: {
    USE_GAS(VM_GAS_VERY_LOW);
    uint256_t tmp = stack[sp - 1];
    stack[sp - 1] = stack[sp - 1 - n];
    stack[sp - 1 - n] = tmp;
    DISPATCH();
}

op_dupn: {
    // DUPN (0xE6): imm8 n; duplicate stack[sp-1-n]
    USE_GAS(VM_GAS_VERY_LOW);
    uint8_t idx = code[pc++];
    stack[sp] = stack[sp - 1 - idx];
    sp++;
    DISPATCH();
}

op_swapn: {
    // SWAPN (0xE7): imm8 n; swap stack[sp-1] with stack[sp-2-n]
    USE_GAS(VM_GAS_VERY_LOW);
    uint8_t idx = code[pc++];
    uint256_t tmp = stack[sp - 1];
    stack[sp - 1] = stack[sp - 2 - idx];
    stack[sp - 2 - idx] = tmp;
    DISPATCH();
}

op_exchange: {
    // EXCHANGE (0xE8): imm8; n=(imm>>4)+1, m=(imm&0xf)+1
    // swap stack[sp-1-n] with stack[sp-1-n-m]
    USE_GAS(VM_GAS_VERY_LOW);
    uint8_t imm = code[pc++];
    uint16_t n_val = (imm >> 4) + 1;
    uint16_t m_val = (imm & 0x0F) + 1;
    uint16_t idx1 = sp - 1 - n_val;
    uint16_t idx2 = sp - 1 - n_val - m_val;
    uint256_t tmp = stack[idx1];
    stack[idx1] = stack[idx2];
    stack[idx2] = tmp;
    DISPATCH();
}

    //==========================================================================
    // Memory (0x51-0x53, 0x59, 0x5E)
    //==========================================================================

op_mload: {
    // MLOAD(offset): load 32 bytes from memory
    uint64_t offset;
    if (!to_u64(&stack[sp - 1], &offset)) goto out_of_gas;
    if (offset > UINT64_MAX - 32) goto out_of_gas;
    uint64_t exp_cost = vm_memory_access_cost(memory, offset, 32);
    USE_GAS(safe_add_gas(VM_GAS_VERY_LOW, exp_cost));
    vm_memory_expand(memory, offset, 32);
    vm_memory_read_word(memory, offset, &stack[sp - 1]);
    DISPATCH();
}

op_mstore: {
    // MSTORE(offset, value): store 32 bytes to memory
    uint64_t offset;
    if (!to_u64(&stack[sp - 1], &offset)) goto out_of_gas;
    if (offset > UINT64_MAX - 32) goto out_of_gas;
    uint64_t exp_cost = vm_memory_access_cost(memory, offset, 32);
    USE_GAS(safe_add_gas(VM_GAS_VERY_LOW, exp_cost));
    vm_memory_expand(memory, offset, 32);
    vm_memory_write_word(memory, offset, &stack[sp - 2]);
    sp -= 2;
    DISPATCH();
}

op_mstore8: {
    // MSTORE8(offset, value): store low byte to memory
    uint64_t offset;
    if (!to_u64(&stack[sp - 1], &offset)) goto out_of_gas;
    uint64_t exp_cost = vm_memory_access_cost(memory, offset, 1);
    USE_GAS(safe_add_gas(VM_GAS_VERY_LOW, exp_cost));
    vm_memory_expand(memory, offset, 1);
    vm_memory_write_byte(memory, offset, (uint8_t)(uint64_t)stack[sp - 2].low);
    sp -= 2;
    DISPATCH();
}

op_msize:
    // MSIZE: push current memory size in bytes
    USE_GAS(VM_GAS_BASE);
    stack[sp] = uint256_from_uint64(vm_memory_size(memory));
    sp++;
    DISPATCH();

op_mcopy: {
    // MCOPY(dest, src, size): copy within memory
    uint64_t dest_off, src_off, size;
    if (!to_u64(&stack[sp - 1], &dest_off)) goto out_of_gas;
    if (!to_u64(&stack[sp - 2], &src_off))  goto out_of_gas;
    if (!to_u64(&stack[sp - 3], &size))     goto out_of_gas;
    sp -= 3;

    uint64_t copy_cost = vm_gas_copy_cost(size);
    uint64_t exp_cost = 0;
    if (size > 0) {
        if (dest_off > UINT64_MAX - size || src_off > UINT64_MAX - size)
            goto out_of_gas;
        uint64_t end1 = dest_off + size;
        uint64_t end2 = src_off + size;
        uint64_t max_end = (end1 > end2) ? end1 : end2;
        exp_cost = vm_memory_access_cost(memory, 0, max_end);
    }
    USE_GAS(safe_add_gas(VM_GAS_VERY_LOW, safe_add_gas(copy_cost, exp_cost)));

    if (size > 0) {
        vm_memory_expand(memory, dest_off, size);
        vm_memory_expand(memory, src_off, size);
        vm_memory_copy(memory, dest_off, src_off, size);
    }
    DISPATCH();
}

    //==========================================================================
    // Control Flow
    //==========================================================================

op_nop:
    // NOP (0x5B, was JUMPDEST): no operation
    USE_GAS(1);
    DISPATCH();

op_rjump: {
    // RJUMP (0xE0): unconditional relative jump
    USE_GAS(VM_GAS_BASE);
    int16_t offset = read_i16(&code[pc]);
    pc += 2;           // past the immediate
    pc += offset;      // relative from after immediate
    DISPATCH();
}

op_rjumpi: {
    // RJUMPI (0xE1): conditional relative jump
    USE_GAS(VM_GAS_BASE);
    int16_t offset = read_i16(&code[pc]);
    pc += 2;
    sp--;
    if (!uint256_is_zero(&stack[sp])) {
        pc += offset;
    }
    DISPATCH();
}

op_rjumpv: {
    // RJUMPV (0xE2): jump table
    USE_GAS(VM_GAS_BASE);
    uint8_t max_index = code[pc++];
    uint16_t count = (uint16_t)max_index + 1;

    sp--;
    uint256_t case_val = stack[sp];
    uint32_t imm_end = pc + count * 2;

    // Check if case value is in range
    if (case_val.high == 0 && case_val.low <= (uint128_t)max_index) {
        uint64_t cv = (uint64_t)case_val.low;
        int16_t offset = read_i16(&code[pc + cv * 2]);
        pc = imm_end + (uint32_t)offset;
    } else {
        pc = imm_end;  // fall through
    }
    DISPATCH();
}

op_callf: {
    // CALLF (0xE3): call function
    USE_GAS(VM_GAS_LOW);
    uint16_t fid = read_u16(&code[pc]);
    pc += 2;

    eof_func_t *target = &c->functions[fid];

    // Push return frame
    vm->return_stack[vm->rsp++] = (vm_return_frame_t){
        .func_id    = vm->current_func,
        .pc         = pc,
        .stack_height = sp - target->inputs,
    };

    // Switch to callee function
    vm->current_func = fid;
    func = target;
    code = func->code;
    pc = 0;
    DISPATCH();
}

op_retf: {
    // RETF (0xE4): return from function
    USE_GAS(VM_GAS_VERY_LOW);

    uint8_t n_outputs = c->functions[vm->current_func].outputs;
    vm_return_frame_t frame = vm->return_stack[--vm->rsp];

    // Set sp: validation guarantees sp == frame.stack_height + n_outputs
    sp = frame.stack_height + n_outputs;

    // Restore caller function
    vm->current_func = frame.func_id;
    func = &c->functions[frame.func_id];
    code = func->code;
    pc = frame.pc;
    DISPATCH();
}

op_jumpf: {
    // JUMPF (0xE5): tail call to function (no return frame)
    USE_GAS(VM_GAS_LOW);
    uint16_t fid = read_u16(&code[pc]);
    pc += 2;

    eof_func_t *target = &c->functions[fid];

    // If target is returning, adjust stack for tail call
    if (target->outputs != EOF_NON_RETURNING && vm->rsp > 0) {
        vm_return_frame_t *frame = &vm->return_stack[vm->rsp - 1];
        uint16_t dst = frame->stack_height;
        uint16_t src = sp - target->inputs;
        if (dst != src && target->inputs > 0) {
            memmove(&stack[dst], &stack[src],
                    target->inputs * sizeof(uint256_t));
        }
        sp = dst + target->inputs;
    }

    // Switch to target function
    vm->current_func = fid;
    func = target;
    code = func->code;
    pc = 0;
    DISPATCH();
}

    //==========================================================================
    // Data Section (0xD0-0xD3)
    //==========================================================================

op_dataload: {
    // DATALOAD(offset): load 32 bytes from data section, zero-pad OOB
    USE_GAS(4);
    uint64_t offset;
    if (!to_u64(&stack[sp - 1], &offset) || offset >= (uint64_t)c->data_size) {
        // Entirely OOB or very large offset
        if (!to_u64(&stack[sp - 1], &offset)) {
            stack[sp - 1] = UINT256_ZERO;
            DISPATCH();
        }
    }

    uint8_t buf[32] = {0};
    uint32_t data_size = c->data_size;
    for (int i = 0; i < 32; i++) {
        uint64_t di = offset + (uint64_t)i;
        if (di < data_size && c->data)
            buf[i] = c->data[di];
    }
    stack[sp - 1] = uint256_from_bytes(buf, 32);
    DISPATCH();
}

op_dataloadn: {
    // DATALOADN(imm16): load 32 bytes at static offset (validated in-bounds)
    USE_GAS(VM_GAS_VERY_LOW);
    uint16_t offset = read_u16(&code[pc]);
    pc += 2;
    stack[sp] = uint256_from_bytes(&c->data[offset], 32);
    sp++;
    DISPATCH();
}

op_datasize:
    // DATASIZE: push data section size
    USE_GAS(VM_GAS_BASE);
    stack[sp] = uint256_from_uint64(c->data_size);
    sp++;
    DISPATCH();

op_datacopy: {
    // DATACOPY(mem_offset, data_offset, size): copy data section to memory
    uint64_t mem_off, data_off, size;
    if (!to_u64(&stack[sp - 1], &mem_off))  goto out_of_gas;
    if (!to_u64(&stack[sp - 2], &data_off)) goto out_of_gas;
    if (!to_u64(&stack[sp - 3], &size))     goto out_of_gas;
    sp -= 3;

    uint64_t copy_cost = vm_gas_copy_cost(size);
    uint64_t exp_cost = 0;
    if (size > 0) {
        if (mem_off > UINT64_MAX - size) goto out_of_gas;
        exp_cost = vm_memory_access_cost(memory, mem_off, size);
    }
    USE_GAS(safe_add_gas(VM_GAS_VERY_LOW, safe_add_gas(copy_cost, exp_cost)));

    if (size > 0) {
        vm_memory_expand(memory, mem_off, size);
        uint8_t *dst = &memory->data[mem_off];
        uint32_t ds = c->data_size;
        for (uint64_t i = 0; i < size; i++) {
            uint64_t di = data_off + i;
            dst[i] = (di < ds && c->data) ? c->data[di] : 0;
        }
    }
    DISPATCH();
}

    //==========================================================================
    // Error / Stub Handlers
    //==========================================================================

op_unimplemented:
    // Phase 3/4 opcode — not yet implemented
    vm->status = VM_INTERNAL_ERROR;
    goto done;

op_invalid:
    // Truly invalid opcode (should never be reached in validated EOF)
    gas_left = 0;
    vm->status = VM_INVALID_OPCODE;
    goto done;

    //==========================================================================
    // Exit Paths
    //==========================================================================

out_of_gas:
    gas_left = 0;
    vm->status = VM_OUT_OF_GAS;
    // fall through to done

done:
    // Sync local state back to VM
    vm->sp = sp;
    vm->pc = pc;
    vm->gas_left = gas_left;
    return vm->status;

    #undef DISPATCH
    #undef USE_GAS
}
