/**
 * EVM Interpreter Implementation
 *
 * High-performance bytecode interpreter using computed goto dispatch.
 * This technique provides ~15-30% speedup over traditional switch statements.
 */

#include "interpreter.h"
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
#include "logger.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Dispatch Table - Maps opcodes to label addresses
//==============================================================================

#define DISPATCH()                                  \
    do                                              \
    {                                               \
        if (evm->pc >= evm->code_size)              \
        {                                           \
            goto done;                              \
        }                                           \
        goto *dispatch_table[evm->code[evm->pc]];  \
    } while (0)

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
    result.output_data = NULL;
    
    // Allocate and copy output data to avoid double-free issues
    if (output_data && output_size > 0)
    {
        result.output_data = malloc(output_size);
        if (result.output_data)
        {
            memcpy(result.output_data, output_data, output_size);
        }
    }
    
    return result;
}

//==============================================================================
// Main Interpreter
//==============================================================================

evm_result_t evm_interpret(evm_t *evm)
{
    if (!evm || !evm->code || !evm->stack || !evm->memory)
    {
        LOG_EVM_ERROR("Invalid EVM state");
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
        &&op_invalid,
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
    status = op_stop(evm);
    // STOP produces no output — clear stale return data from subcalls
    if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
    evm->return_data_size = 0;
    goto done;

op_add:
    status = op_add(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_mul:
    status = op_mul(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_sub:
    status = op_sub(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_div:
    status = op_div(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_sdiv:
    status = op_sdiv(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_mod:
    status = op_mod(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_smod:
    status = op_smod(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_addmod:
    status = op_addmod(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_mulmod:
    status = op_mulmod(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_exp:
    status = op_exp(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_signextend:
    status = op_signextend(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

    //==========================================================================
    // 0x10-0x1f: Comparison & Bitwise Logic Operations
    //==========================================================================

op_lt:
    status = op_lt(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_gt:
    status = op_gt(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_slt:
    status = op_slt(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_sgt:
    status = op_sgt(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_eq:
    status = op_eq(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_iszero:
    status = op_iszero(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_and:
    status = op_and(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_or:
    status = op_or(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_xor:
    status = op_xor(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_not:
    status = op_not(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_byte:
    status = op_byte(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_shl:
    status = op_shl(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_shr:
    status = op_shr(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_sar:
    status = op_sar(evm);
    if (status != EVM_SUCCESS)
        goto error;
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

op_address:
    status = op_address(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_balance:
    status = op_balance(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_origin:
    status = op_origin(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_caller:
    status = op_caller(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_callvalue:
    status = op_callvalue(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_calldataload:
    status = op_calldataload(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_calldatasize:
    status = op_calldatasize(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_calldatacopy:
    status = op_calldatacopy(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_codesize:
    status = op_codesize(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_codecopy:
    status = op_codecopy(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_gasprice:
    status = op_gasprice(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_extcodesize:
    status = op_extcodesize(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_extcodecopy:
    status = op_extcodecopy(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_returndatasize:
    status = op_returndatasize(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_returndatacopy:
    status = op_returndatacopy(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_extcodehash:
    status = op_extcodehash(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

    //==========================================================================
    // 0x40-0x4f: Block Information
    //==========================================================================

op_blockhash:
    status = op_blockhash(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_coinbase:
    status = op_coinbase(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_timestamp:
    status = op_timestamp(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_number:
    status = op_number(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_difficulty:
    status = op_difficulty(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_gaslimit:
    status = op_gaslimit(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_chainid:
    status = op_chainid(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_selfbalance:
    status = op_selfbalance(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_basefee:
    status = op_basefee(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_blobhash:
    status = op_blobhash(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_blobbasefee:
    status = op_blobbasefee(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

    //==========================================================================
    // 0x50-0x5f: Stack, Memory, Storage, and Flow Operations
    //==========================================================================

op_pop:
    status = op_pop(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_mload:
    status = op_mload(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_mstore:
    status = op_mstore(evm);
    if (status != EVM_SUCCESS)
        goto error;
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
    status = op_pc(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_msize:
    status = op_msize(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_gas:
    status = op_gas(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

op_jumpdest:
    status = op_jumpdest(evm);
    if (status != EVM_SUCCESS)
        goto error;
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
    status = op_push0(evm);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

    //==========================================================================
    // 0x60-0x7f: Push Operations
    //==========================================================================

op_push1:
    status = op_push(evm, 1);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push2:
    status = op_push(evm, 2);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push3:
    status = op_push(evm, 3);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push4:
    status = op_push(evm, 4);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push5:
    status = op_push(evm, 5);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push6:
    status = op_push(evm, 6);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push7:
    status = op_push(evm, 7);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push8:
    status = op_push(evm, 8);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push9:
    status = op_push(evm, 9);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push10:
    status = op_push(evm, 10);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push11:
    status = op_push(evm, 11);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push12:
    status = op_push(evm, 12);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push13:
    status = op_push(evm, 13);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push14:
    status = op_push(evm, 14);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push15:
    status = op_push(evm, 15);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push16:
    status = op_push(evm, 16);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push17:
    status = op_push(evm, 17);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push18:
    status = op_push(evm, 18);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push19:
    status = op_push(evm, 19);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push20:
    status = op_push(evm, 20);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push21:
    status = op_push(evm, 21);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push22:
    status = op_push(evm, 22);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push23:
    status = op_push(evm, 23);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push24:
    status = op_push(evm, 24);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push25:
    status = op_push(evm, 25);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push26:
    status = op_push(evm, 26);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push27:
    status = op_push(evm, 27);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push28:
    status = op_push(evm, 28);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push29:
    status = op_push(evm, 29);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push30:
    status = op_push(evm, 30);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push31:
    status = op_push(evm, 31);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();
op_push32:
    status = op_push(evm, 32);
    if (status != EVM_SUCCESS)
        goto error;
    DISPATCH();

    //==========================================================================
    // 0x80-0x8f: Dup Operations
    //==========================================================================

op_dup1:
    status = op_dup(evm, 1);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup2:
    status = op_dup(evm, 2);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup3:
    status = op_dup(evm, 3);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup4:
    status = op_dup(evm, 4);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup5:
    status = op_dup(evm, 5);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup6:
    status = op_dup(evm, 6);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup7:
    status = op_dup(evm, 7);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup8:
    status = op_dup(evm, 8);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup9:
    status = op_dup(evm, 9);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup10:
    status = op_dup(evm, 10);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup11:
    status = op_dup(evm, 11);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup12:
    status = op_dup(evm, 12);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup13:
    status = op_dup(evm, 13);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup14:
    status = op_dup(evm, 14);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup15:
    status = op_dup(evm, 15);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_dup16:
    status = op_dup(evm, 16);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

    //==========================================================================
    // 0x90-0x9f: Swap Operations
    //==========================================================================

op_swap1:
    status = op_swap(evm, 1);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap2:
    status = op_swap(evm, 2);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap3:
    status = op_swap(evm, 3);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap4:
    status = op_swap(evm, 4);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap5:
    status = op_swap(evm, 5);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap6:
    status = op_swap(evm, 6);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap7:
    status = op_swap(evm, 7);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap8:
    status = op_swap(evm, 8);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap9:
    status = op_swap(evm, 9);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap10:
    status = op_swap(evm, 10);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap11:
    status = op_swap(evm, 11);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap12:
    status = op_swap(evm, 12);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap13:
    status = op_swap(evm, 13);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap14:
    status = op_swap(evm, 14);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap15:
    status = op_swap(evm, 15);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();
op_swap16:
    status = op_swap(evm, 16);
    if (status != EVM_SUCCESS)
        goto error;
    NEXT();

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
    goto done;

op_selfdestruct:
    status = op_selfdestruct(evm);
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

error:
    LOG_EVM_ERROR("Execution error at PC=%lu: status=%d", evm->pc, status);
    // Invalid opcode consumes all remaining gas
    if (status == EVM_INVALID_OPCODE)
        evm->gas_left = 0;
    // For non-REVERT errors (OOG, invalid opcode, etc.), clear return data.
    // Only REVERT should propagate output data on error.
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
    // Create result with output data
    return evm_result_create(status, evm->gas_left, evm->gas_refund, evm->return_data, evm->return_data_size);
}
