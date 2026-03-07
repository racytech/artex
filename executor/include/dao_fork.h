#ifndef ART_EXECUTOR_DAO_FORK_H
#define ART_EXECUTOR_DAO_FORK_H

#include "evm_state.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* DAO fork block on mainnet */
#define DAO_FORK_BLOCK 1920000

/**
 * Apply the DAO hard fork irregular state change.
 *
 * Drains the ETH balance from all DAO-related accounts and transfers
 * the total to the refund contract at 0xbf4ed7b27f1d666546e30d74d50d173d20bca754.
 *
 * Must be called at the start of block 1,920,000 (mainnet only),
 * before processing any transactions.
 *
 * @param state  EVM state to mutate
 */
void apply_dao_fork(evm_state_t *state);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_DAO_FORK_H */
