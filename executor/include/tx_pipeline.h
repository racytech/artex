#ifndef ART_EXECUTOR_TX_PIPELINE_H
#define ART_EXECUTOR_TX_PIPELINE_H

#include "transaction.h"
#include "block.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =========================================================================
 * TX Pipeline: SPSC ring buffer + prep thread
 *
 * The prep thread decodes transactions and recovers senders ahead of the
 * exec thread. State-independent work (RLP decode, ecrecover, access list
 * parsing) is moved off the critical execution path.
 *
 * Architecture:
 *   prep thread → [ring buffer] → exec thread (serial EVM execution)
 * ========================================================================= */

#define TX_RING_CAP 64  /* must be power of 2 */

/**
 * A prepared transaction slot in the ring buffer.
 */
typedef struct {
    transaction_t tx;       /* decoded transaction with recovered sender */
    bool          valid;    /* true if decode + ecrecover succeeded */
    bool          done;     /* sentinel: no more transactions in this block */
} prepared_tx_t;

/**
 * SPSC ring buffer with condvar fallback.
 *
 * Producer (prep thread) writes to slots[head % CAP] and advances head.
 * Consumer (exec thread) reads from slots[tail % CAP] and advances tail.
 * Fast path is lock-free; slow path (ring full/empty) blocks on condvar.
 */
typedef struct {
    prepared_tx_t  slots[TX_RING_CAP];
    atomic_size_t  head;    /* written by prep thread only */
    atomic_size_t  tail;    /* written by exec thread only */
    pthread_mutex_t mtx;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;
} tx_ring_t;

/**
 * Context passed to the prep thread.
 */
typedef struct {
    tx_ring_t          *ring;
    const block_body_t *body;
    size_t              tx_count;
    uint64_t            chain_id;
    atomic_bool         cancel;     /* exec thread sets to request early stop */
} tx_prep_ctx_t;

/* ── Ring buffer operations ─────────────────────────────────────────────── */

/** Initialize ring buffer (zero head/tail, init condvars). */
void tx_ring_init(tx_ring_t *ring);

/** Destroy ring buffer (cleanup condvars). */
void tx_ring_destroy(tx_ring_t *ring);

/**
 * Push a prepared tx into the ring. Spins if ring is full.
 * Returns false only if cancelled while spinning.
 */
bool tx_ring_push(tx_ring_t *ring, const prepared_tx_t *ptx,
                  const atomic_bool *cancel);

/**
 * Pop a prepared tx from the ring. Spins if ring is empty.
 * Returns false only if cancelled while spinning.
 */
bool tx_ring_pop(tx_ring_t *ring, prepared_tx_t *out,
                 const atomic_bool *cancel);

/* ── Prep thread ────────────────────────────────────────────────────────── */

/**
 * Prep thread entry point. Decodes all txs in the block body and pushes
 * them into the ring buffer. Pushes a final sentinel (done=true) when
 * finished.
 *
 * Usage: pthread_create(&tid, NULL, tx_prep_thread, &ctx);
 */
void *tx_prep_thread(void *arg);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_TX_PIPELINE_H */
