/**
 * Transaction dependency analysis.
 *
 * Scans decoded transactions to find groups that can execute in parallel.
 * Uses conservative conflict detection based on touched addresses.
 */

#include "tx_analysis.h"
#include "mem_art.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =========================================================================
 * Build address set for a transaction
 * ========================================================================= */

/** Classify a transaction for parallelism analysis. */
typedef enum {
    TX_CLASS_TRANSFER,  /* simple ETH transfer: no data, not create */
    TX_CLASS_CONTRACT,  /* contract call or create: touches unknown state */
} tx_class_t;

static tx_class_t tx_classify(const transaction_t *tx) {
    if (!tx->is_create && tx->data_size == 0)
        return TX_CLASS_TRANSFER;
    return TX_CLASS_CONTRACT;
}

/* =========================================================================
 * Schedule builder
 * ========================================================================= */

void tx_analyze(const prepared_tx_t *decoded, size_t tx_count,
                tx_schedule_t *schedule) {
    memset(schedule, 0, sizeof(*schedule));
    schedule->total_txs = tx_count;

    if (tx_count == 0) return;

    /* Hash set of addresses claimed by parallel transfers.
     * O(1) lookup per address via mem_art. */
    mem_art_t seen;
    mem_art_init(&seen);
    uint8_t marker = 1;

    /* Parallel group (transfers with no address conflicts) */
    uint16_t *par_indices = calloc(tx_count, sizeof(uint16_t));
    size_t par_count = 0;

    /* Serial group (everything else) */
    uint16_t *ser_indices = calloc(tx_count, sizeof(uint16_t));
    size_t ser_count = 0;

    for (size_t i = 0; i < tx_count; i++) {
        if (!decoded[i].valid) {
            ser_indices[ser_count++] = (uint16_t)i;
            continue;
        }

        const transaction_t *tx = &decoded[i].tx;

        /* Only simple transfers are candidates */
        if (tx_classify(tx) != TX_CLASS_TRANSFER) {
            ser_indices[ser_count++] = (uint16_t)i;
            continue;
        }

        /* Check if sender or to already claimed by another parallel tx */
        bool conflict = mem_art_contains(&seen, tx->sender.bytes, 20) ||
                        mem_art_contains(&seen, tx->to.bytes, 20);

        if (conflict) {
            ser_indices[ser_count++] = (uint16_t)i;
        } else {
            /* Claim both addresses */
            mem_art_insert(&seen, tx->sender.bytes, 20, &marker, 1);
            mem_art_insert(&seen, tx->to.bytes, 20, &marker, 1);
            par_indices[par_count++] = (uint16_t)i;
        }
    }

    mem_art_destroy(&seen);

    /* Build groups */
    if (par_count > 1) {
        schedule->groups[0].indices = par_indices;
        schedule->groups[0].count = (uint16_t)par_count;
        schedule->group_count = 1;
        schedule->parallel_txs = par_count;
    } else {
        /* Not worth parallelizing 0-1 transfers */
        for (size_t i = 0; i < par_count; i++)
            ser_indices[ser_count++] = par_indices[i];
        free(par_indices);
        par_indices = NULL;
    }

    if (ser_count > 0) {
        int g = schedule->group_count++;
        schedule->groups[g].indices = ser_indices;
        schedule->groups[g].count = (uint16_t)ser_count;
        schedule->serial_txs = ser_count;
    } else {
        free(ser_indices);
    }
}

void tx_schedule_free(tx_schedule_t *schedule) {
    if (!schedule) return;
    for (int g = 0; g < schedule->group_count; g++)
        free(schedule->groups[g].indices);
    memset(schedule, 0, sizeof(*schedule));
}

void tx_schedule_print(const tx_schedule_t *schedule) {
    if (!schedule) return;
    fprintf(stderr, "tx_schedule: %zu txs, %d groups (parallel=%zu serial=%zu)\n",
            schedule->total_txs, schedule->group_count,
            schedule->parallel_txs, schedule->serial_txs);
    for (int g = 0; g < schedule->group_count; g++) {
        fprintf(stderr, "  group[%d]: %u txs\n", g, schedule->groups[g].count);
    }
}
