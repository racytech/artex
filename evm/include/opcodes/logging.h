/**
 * EVM Logging Opcodes
 *
 * Implements event logging operations:
 * - LOG0, LOG1, LOG2, LOG3, LOG4
 */

#ifndef ART_EVM_OPCODES_LOGGING_H
#define ART_EVM_OPCODES_LOGGING_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_LOG0 0xa0 // Log with 0 topics
#define OP_LOG1 0xa1 // Log with 1 topic
#define OP_LOG2 0xa2 // Log with 2 topics
#define OP_LOG3 0xa3 // Log with 3 topics
#define OP_LOG4 0xa4 // Log with 4 topics

//==============================================================================
// Opcode Implementations
//==============================================================================

/**
 * LOG0 - Append log record with no topics
 * Stack: offset size =>
 * Gas: 375 + 8 * size + memory_expansion_cost
 */
evm_status_t op_log0(evm_t *evm);

/**
 * LOG1 - Append log record with 1 topic
 * Stack: offset size topic1 =>
 * Gas: 375 + 375 + 8 * size + memory_expansion_cost
 */
evm_status_t op_log1(evm_t *evm);

/**
 * LOG2 - Append log record with 2 topics
 * Stack: offset size topic1 topic2 =>
 * Gas: 375 + 2*375 + 8 * size + memory_expansion_cost
 */
evm_status_t op_log2(evm_t *evm);

/**
 * LOG3 - Append log record with 3 topics
 * Stack: offset size topic1 topic2 topic3 =>
 * Gas: 375 + 3*375 + 8 * size + memory_expansion_cost
 */
evm_status_t op_log3(evm_t *evm);

/**
 * LOG4 - Append log record with 4 topics
 * Stack: offset size topic1 topic2 topic3 topic4 =>
 * Gas: 375 + 4*375 + 8 * size + memory_expansion_cost
 */
evm_status_t op_log4(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_LOGGING_H */
