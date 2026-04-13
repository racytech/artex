#ifndef ART_EXECUTOR_TX_ANALYSIS_H
#define ART_EXECUTOR_TX_ANALYSIS_H

#include "transaction.h"
#include "tx_pipeline.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Transaction dependency analysis for parallel execution.
 *
 * After batch decode, analyze transactions to find independent groups
 * that can execute in parallel without conflicts.
 *
 * Conservative rules:
 *   - Simple transfer (no data, to != contract): touches {sender, to}
 *   - Contract call: touches {sender, to, unknown storage}
 *   - Two txs conflict if their touched address sets overlap
 *   - Same sender always conflicts (nonce ordering)
 *   - Same contract target likely conflicts (shared storage)
 *
 * Output: execution groups. Txs within a group are independent.
 * Groups must execute in block order (group 0 before group 1).
 */

#define TX_MAX_GROUPS 16

typedef struct {
    uint16_t *indices;    /* tx indices in this group */
    uint16_t  count;      /* number of txs */
} tx_group_t;

typedef struct {
    tx_group_t groups[TX_MAX_GROUPS];
    int        group_count;
    size_t     parallel_txs;   /* txs that can run in parallel */
    size_t     serial_txs;     /* txs that must run serially */
    size_t     total_txs;
} tx_schedule_t;

/**
 * Analyze decoded transactions and build an execution schedule.
 *
 * @param decoded    Array of decoded transactions (from tx_batch_decode)
 * @param tx_count   Number of transactions
 * @param schedule   Output schedule (caller owns, must call tx_schedule_free)
 */
void tx_analyze(const prepared_tx_t *decoded, size_t tx_count,
                tx_schedule_t *schedule);

/**
 * Free schedule resources.
 */
void tx_schedule_free(tx_schedule_t *schedule);

/**
 * Print schedule summary to stderr (for debugging).
 */
void tx_schedule_print(const tx_schedule_t *schedule);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_TX_ANALYSIS_H */
