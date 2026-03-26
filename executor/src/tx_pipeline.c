#include "tx_pipeline.h"
#include "tx_decoder.h"
#include <string.h>

#ifdef __x86_64__
#include <immintrin.h>
#define SPIN_PAUSE() _mm_pause()
#else
#define SPIN_PAUSE() ((void)0)
#endif

#define SPIN_TRIES 64

/* =========================================================================
 * Ring buffer operations (SPSC, lock-free fast path, condvar slow path)
 * ========================================================================= */

void tx_ring_init(tx_ring_t *ring) {
    memset(ring->slots, 0, sizeof(ring->slots));
    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
    pthread_mutex_init(&ring->mtx, NULL);
    pthread_cond_init(&ring->not_full, NULL);
    pthread_cond_init(&ring->not_empty, NULL);
}

void tx_ring_destroy(tx_ring_t *ring) {
    pthread_cond_destroy(&ring->not_empty);
    pthread_cond_destroy(&ring->not_full);
    pthread_mutex_destroy(&ring->mtx);
}

bool tx_ring_push(tx_ring_t *ring, const prepared_tx_t *ptx,
                  const atomic_bool *cancel) {
    size_t h = atomic_load_explicit(&ring->head, memory_order_relaxed);

    /* Fast path: spin briefly */
    for (int i = 0; i < SPIN_TRIES; i++) {
        size_t t = atomic_load_explicit(&ring->tail, memory_order_acquire);
        if (h - t < TX_RING_CAP)
            goto push;
        if (cancel && atomic_load_explicit(cancel, memory_order_relaxed))
            return false;
        SPIN_PAUSE();
    }

    /* Slow path: block on condvar */
    pthread_mutex_lock(&ring->mtx);
    for (;;) {
        size_t t = atomic_load_explicit(&ring->tail, memory_order_acquire);
        if (h - t < TX_RING_CAP) {
            pthread_mutex_unlock(&ring->mtx);
            goto push;
        }
        if (cancel && atomic_load_explicit(cancel, memory_order_relaxed)) {
            pthread_mutex_unlock(&ring->mtx);
            return false;
        }
        pthread_cond_wait(&ring->not_full, &ring->mtx);
    }

push:
    ring->slots[h & (TX_RING_CAP - 1)] = *ptx;
    atomic_store_explicit(&ring->head, h + 1, memory_order_release);

    /* Signal consumer that data is available */
    pthread_mutex_lock(&ring->mtx);
    pthread_cond_signal(&ring->not_empty);
    pthread_mutex_unlock(&ring->mtx);
    return true;
}

bool tx_ring_pop(tx_ring_t *ring, prepared_tx_t *out,
                 const atomic_bool *cancel) {
    size_t t = atomic_load_explicit(&ring->tail, memory_order_relaxed);

    /* Fast path: spin briefly */
    for (int i = 0; i < SPIN_TRIES; i++) {
        size_t h = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (h > t)
            goto pop;
        if (cancel && atomic_load_explicit(cancel, memory_order_relaxed))
            return false;
        SPIN_PAUSE();
    }

    /* Slow path: block on condvar */
    pthread_mutex_lock(&ring->mtx);
    for (;;) {
        size_t h = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (h > t) {
            pthread_mutex_unlock(&ring->mtx);
            goto pop;
        }
        if (cancel && atomic_load_explicit(cancel, memory_order_relaxed)) {
            pthread_mutex_unlock(&ring->mtx);
            return false;
        }
        pthread_cond_wait(&ring->not_empty, &ring->mtx);
    }

pop:
    *out = ring->slots[t & (TX_RING_CAP - 1)];
    atomic_store_explicit(&ring->tail, t + 1, memory_order_release);

    /* Signal producer that space is available */
    pthread_mutex_lock(&ring->mtx);
    pthread_cond_signal(&ring->not_full);
    pthread_mutex_unlock(&ring->mtx);
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
