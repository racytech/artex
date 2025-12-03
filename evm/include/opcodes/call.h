/**
 * EVM Call Opcodes
 *
 * Implements message call and contract creation operations:
 * - CALL, CALLCODE, DELEGATECALL, STATICCALL
 * - CREATE, CREATE2
 */

#ifndef ART_EVM_OPCODES_CALL_H
#define ART_EVM_OPCODES_CALL_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_CREATE 0xf0       // Create contract
#define OP_CALL 0xf1         // Message call
#define OP_CALLCODE 0xf2     // Message call with alternative account's code
#define OP_DELEGATECALL 0xf4 // Message call with caller's context
#define OP_CREATE2 0xf5      // Create contract with deterministic address
#define OP_STATICCALL 0xfa   // Static message call (no state modification)

//==============================================================================
// Call Opcodes
//==============================================================================

/**
 * CALL - Message call into an account
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 * Gas: Complex (base + memory + account access + value transfer + new account)
 */
evm_status_t op_call(evm_t *evm);

/**
 * CALLCODE - Message call with alternative account's code (deprecated)
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 * Gas: Similar to CALL
 */
evm_status_t op_callcode(evm_t *evm);

/**
 * DELEGATECALL - Message call with caller's context
 * Stack: gas addr argsOffset argsSize retOffset retSize => success
 * Gas: Similar to CALL (no value transfer)
 */
evm_status_t op_delegatecall(evm_t *evm);

/**
 * STATICCALL - Static message call (no state modification)
 * Stack: gas addr argsOffset argsSize retOffset retSize => success
 * Gas: Similar to CALL (no value transfer)
 */
evm_status_t op_staticcall(evm_t *evm);

//==============================================================================
// Creation Opcodes
//==============================================================================

/**
 * CREATE - Create new contract
 * Stack: value offset size => address
 * Gas: 32000 + deployment gas + memory_expansion_cost
 */
evm_status_t op_create(evm_t *evm);

/**
 * CREATE2 - Create new contract with deterministic address
 * Stack: value offset size salt => address
 * Gas: 32000 + deployment gas + memory_expansion_cost + keccak256 cost
 */
evm_status_t op_create2(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_CALL_H */
