/**
 * VM — EOF-Native Virtual Machine Core
 *
 * Stack-based VM executing validated EOF containers on Verkle state.
 * Key differences from legacy EVM:
 *   - EOF container with deploy-time validation (no runtime opcode checks)
 *   - Return stack for CALLF/RETF function calls
 *   - Stack sized from EOF max_stack_height (no dynamic overflow checks)
 *   - No JUMP/JUMPI/JUMPDEST — only RJUMP/RJUMPI/RJUMPV
 *   - No fork routing — single instruction set
 *   - Witness-based gas (Verkle) replaces EIP-2929 warm/cold
 *
 * Architecture:
 *   VM Interpreter → Stack/Memory → State Interface → Verkle Tree
 */

#ifndef ART_VM_H
#define ART_VM_H

#include "eof.h"
#include "uint256.h"
#include "hash.h"
#include "address.h"
#include "vm_memory.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//==============================================================================
// Limits
//==============================================================================

#define VM_MAX_STACK_DEPTH   1024
#define VM_MAX_RETURN_DEPTH  1024
#define VM_MAX_CALL_DEPTH    1024

//==============================================================================
// Status Codes
//==============================================================================

typedef enum {
    VM_SUCCESS = 0,
    VM_REVERT,
    VM_OUT_OF_GAS,
    VM_INVALID_OPCODE,
    VM_STACK_OVERFLOW,
    VM_STACK_UNDERFLOW,
    VM_INVALID_MEMORY_ACCESS,
    VM_STATIC_CALL_VIOLATION,
    VM_CALL_DEPTH_EXCEEDED,
    VM_INVALID_EOF,
    VM_INTERNAL_ERROR,
} vm_status_t;

//==============================================================================
// Execution Result
//==============================================================================

typedef struct {
    vm_status_t  status;
    uint64_t     gas_left;
    uint64_t     gas_refund;
    uint8_t     *output_data;   // heap-allocated, caller owns
    size_t       output_size;
    address_t    created_addr;  // set by EOFCREATE
} vm_result_t;

//==============================================================================
// Call Types
//==============================================================================

typedef enum {
    VM_CALL,            // EXTCALL
    VM_DELEGATECALL,    // EXTDELEGATECALL
    VM_STATICCALL,      // EXTSTATICCALL
    VM_EOFCREATE,       // EOFCREATE
} vm_call_type_t;

//==============================================================================
// Message (Call Context)
//==============================================================================

typedef struct {
    vm_call_type_t kind;

    address_t caller;
    address_t recipient;
    address_t code_addr;

    uint256_t value;

    const uint8_t *input_data;
    size_t         input_size;

    uint64_t gas;
    int32_t  depth;
    bool     is_static;
} vm_message_t;

//==============================================================================
// Block Environment
//==============================================================================

typedef struct {
    uint64_t  number;
    uint64_t  timestamp;
    uint64_t  gas_limit;
    uint256_t difficulty;       // PREVRANDAO post-merge
    address_t coinbase;
    uint256_t base_fee;
    uint256_t chain_id;
    hash_t    block_hash[256];  // recent block hashes
} vm_block_env_t;

//==============================================================================
// Transaction Context
//==============================================================================

typedef struct {
    address_t origin;
    uint256_t gas_price;
} vm_tx_context_t;

//==============================================================================
// Return Stack Frame (for CALLF/RETF)
//==============================================================================

typedef struct {
    uint16_t func_id;       // function we'll return to
    uint32_t pc;            // saved program counter
    uint16_t stack_height;  // saved stack pointer
} vm_return_frame_t;

//==============================================================================
// VM Execution Context
//==============================================================================

typedef struct vm {
    // State (opaque — Phase 3 will define the interface)
    void *state;

    // Execution context
    vm_message_t  msg;
    vm_block_env_t block;
    vm_tx_context_t tx;

    // EOF container being executed
    eof_container_t *container;
    uint16_t         current_func;

    // Program counter
    uint32_t pc;

    // Operand stack
    uint256_t *stack;       // heap-allocated, sized from EOF max_stack_height
    uint16_t   sp;          // stack pointer (number of items on stack)

    // Return stack (CALLF/RETF)
    vm_return_frame_t *return_stack;    // heap-allocated, VM_MAX_RETURN_DEPTH
    uint16_t           rsp;            // return stack pointer

    // Memory
    vm_memory_t *memory;

    // Gas
    uint64_t gas_left;
    uint64_t gas_refund;

    // Return data from last external call
    uint8_t *return_data;
    size_t   return_data_size;

    // Execution flags
    bool         stopped;
    vm_status_t  status;
} vm_t;

//==============================================================================
// Lifecycle
//==============================================================================

/** Create a new VM instance. Memory and return stack are allocated. */
vm_t *vm_create(void);

/** Destroy VM instance and all owned resources. */
void vm_destroy(vm_t *vm);

/** Reset runtime state between executions. Frees stack and return data. */
void vm_reset(vm_t *vm);

//==============================================================================
// Context Setup
//==============================================================================

void vm_set_block_env(vm_t *vm, const vm_block_env_t *block);
void vm_set_tx_context(vm_t *vm, const vm_tx_context_t *tx);

//==============================================================================
// Execution
//==============================================================================

/**
 * Execute an EOF container with a given message.
 *
 * Allocates the operand stack from container->functions[0].max_stack_height,
 * sets up the execution context, and runs the interpreter.
 * Phase 1 stub: returns VM_SUCCESS immediately.
 *
 * @param vm        VM instance
 * @param container Validated EOF container (ownership NOT transferred)
 * @param msg       Call message
 * @param result    Output result (caller must call vm_result_free)
 * @return true on successful dispatch, false on internal error
 */
bool vm_execute(vm_t *vm, eof_container_t *container,
                const vm_message_t *msg, vm_result_t *result);

//==============================================================================
// Gas Operations
//==============================================================================

bool     vm_use_gas(vm_t *vm, uint64_t amount);
void     vm_refund_gas(vm_t *vm, uint64_t amount);
uint64_t vm_get_gas_left(const vm_t *vm);

//==============================================================================
// Result Helpers
//==============================================================================

vm_result_t vm_result_success(uint64_t gas_left, const uint8_t *output, size_t size);
vm_result_t vm_result_revert(uint64_t gas_left, const uint8_t *output, size_t size);
vm_result_t vm_result_error(vm_status_t status, uint64_t gas_left);
void        vm_result_free(vm_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* ART_VM_H */
