#include "tx_pipeline.h"
#include "tx_decoder.h"
#include <string.h>
#include <sched.h>

/* =========================================================================
 * Ring buffer operations (SPSC, lock-free)
 * ========================================================================= */

void tx_ring_init(tx_ring_t *ring) {
    memset(ring->slots, 0, sizeof(ring->slots));
    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
}

bool tx_ring_push(tx_ring_t *ring, const prepared_tx_t *ptx,
                  const atomic_bool *cancel) {
    size_t h = atomic_load_explicit(&ring->head, memory_order_relaxed);

    /* Spin while ring is full: head - tail == CAP */
    for (;;) {
        size_t t = atomic_load_explicit(&ring->tail, memory_order_acquire);
        if (h - t < TX_RING_CAP)
            break;
        if (cancel && atomic_load_explicit(cancel, memory_order_relaxed))
            return false;
        sched_yield();
    }

    ring->slots[h & (TX_RING_CAP - 1)] = *ptx;

    /* Release: make the slot contents visible before advancing head */
    atomic_store_explicit(&ring->head, h + 1, memory_order_release);
    return true;
}

bool tx_ring_pop(tx_ring_t *ring, prepared_tx_t *out,
                 const atomic_bool *cancel) {
    size_t t = atomic_load_explicit(&ring->tail, memory_order_relaxed);

    /* Spin while ring is empty: head == tail */
    for (;;) {
        size_t h = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (h > t)
            break;
        if (cancel && atomic_load_explicit(cancel, memory_order_relaxed))
            return false;
        sched_yield();
    }

    *out = ring->slots[t & (TX_RING_CAP - 1)];

    /* Release: mark slot as consumed before advancing tail */
    atomic_store_explicit(&ring->tail, t + 1, memory_order_release);
    return true;
}

/* =========================================================================
 * Prep thread
 * ========================================================================= */

void *tx_prep_thread(void *arg) {
    tx_prep_ctx_t *ctx = (tx_prep_ctx_t *)arg;
    tx_ring_t *ring = ctx->ring;

    for (size_t i = 0; i < ctx->tx_count; i++) {
        if (atomic_load_explicit(&ctx->cancel, memory_order_relaxed))
            break;

        prepared_tx_t ptx;
        memset(&ptx, 0, sizeof(ptx));

        const rlp_item_t *tx_item = block_body_tx(ctx->body, i);
        if (tx_item && tx_decode_rlp(&ptx.tx, tx_item, ctx->chain_id)) {
            ptx.valid = true;
        } else {
            ptx.valid = false;
        }
        ptx.done = false;

        if (!tx_ring_push(ring, &ptx, &ctx->cancel)) {
            /* Cancelled while waiting for ring space — clean up */
            if (ptx.valid) tx_decoded_free(&ptx.tx);
            break;
        }
    }

    /* Push sentinel to signal end of block */
    prepared_tx_t sentinel;
    memset(&sentinel, 0, sizeof(sentinel));
    sentinel.done = true;
    tx_ring_push(ring, &sentinel, NULL);  /* must succeed */

    return NULL;
}
