/**
 * EVM Interpreter
 *
 * Main execution engine that processes bytecode using computed goto dispatch.
 */

#ifndef ART_EVM_INTERPRETER_H
#define ART_EVM_INTERPRETER_H

#include "evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Run EVM bytecode interpreter
 *
 * @param evm EVM instance with initialized code, stack, memory, etc.
 * @return Execution result with status, gas usage, and output data
 */
evm_result_t evm_interpret(evm_t *evm);

/**
 * Create an EVM result structure
 *
 * @param status Execution status
 * @param gas_left Remaining gas
 * @param gas_refund Gas refund amount
 * @param output_data Output data (can be NULL)
 * @param output_size Size of output data
 * @return Result structure
 */
evm_result_t evm_result_create(evm_status_t status,
                                uint64_t gas_left,
                                int64_t gas_refund,
                                uint8_t *output_data,
                                size_t output_size);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_INTERPRETER_H */
