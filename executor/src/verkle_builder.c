#include "verkle_builder.h"
#include "verkle_flat.h"
#include "verkle_key.h"
#include "pedersen.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>

/* =========================================================================
 * Internal state
 * ========================================================================= */

struct verkle_builder {
    verkle_flat_t  *vf;

    /* SPSC ring (same layout as state_history) */
    diff_ring_t     ring;

    /* Consumer thread */
    pthread_t       thread;
    atomic_bool     stop;

    /* Last committed block */
    atomic_uint_fast64_t last_block;
};

/* =========================================================================
 * SPSC ring helpers (duplicated from state_history — same struct)
 * ========================================================================= */

static void ring_init(diff_ring_t *ring) {
    memset(ring->slots, 0, sizeof(ring->slots));
    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
}

static bool ring_try_push(diff_ring_t *ring, const block_diff_t *diff) {
    size_t h = atomic_load_explicit(&ring->head, memory_order_relaxed);
    size_t t = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (h - t >= DIFF_RING_CAP)
        return false;
    ring->slots[h & (DIFF_RING_CAP - 1)] = *diff;
    atomic_store_explicit(&ring->head, h + 1, memory_order_release);
    return true;
}

static bool ring_pop(diff_ring_t *ring, block_diff_t *out,
                     const atomic_bool *stop) {
    size_t t = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    for (;;) {
        size_t h = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (h > t) break;
        if (stop && atomic_load_explicit(stop, memory_order_relaxed))
            return false;
        sched_yield();
    }
    *out = ring->slots[t & (DIFF_RING_CAP - 1)];
    atomic_store_explicit(&ring->tail, t + 1, memory_order_release);
    return true;
}

/* =========================================================================
 * Diff → Verkle conversion
 *
 * For each address group: update basic_data (nonce + balance), code_hash,
 * and storage slots. Only writes fields that actually changed (field_mask).
 * ========================================================================= */

static void apply_group(verkle_flat_t *vf, const addr_diff_t *g) {
    const uint8_t *addr = g->addr.bytes;

    /* Update basic_data if nonce or balance changed */
    if (g->field_mask & (FIELD_NONCE | FIELD_BALANCE)) {
        uint8_t basic_data[32];
        uint8_t basic_key[32];
        verkle_account_basic_data_key(basic_key, addr);

        if (!verkle_flat_get(vf, basic_key, basic_data))
            memset(basic_data, 0, 32);

        if (g->field_mask & FIELD_NONCE) {
            uint64_t nonce = g->nonce;
            for (int i = 7; i >= 0; i--) {
                basic_data[VERKLE_BASIC_DATA_NONCE_OFFSET + i] = (uint8_t)(nonce & 0xFF);
                nonce >>= 8;
            }
        }

        if (g->field_mask & FIELD_BALANCE) {
            uint8_t bal_be[32];
            uint256_to_bytes(&g->balance, bal_be);
            for (int i = 0; i < VERKLE_BASIC_DATA_BALANCE_SIZE; i++)
                basic_data[31 - i] = bal_be[i];
        }

        verkle_flat_set(vf, basic_key, basic_data);
    }

    /* Code hash — only if changed */
    if (g->field_mask & FIELD_CODE_HASH) {
        uint8_t code_hash_key[32];
        verkle_account_code_hash_key(code_hash_key, addr);
        verkle_flat_set(vf, code_hash_key, g->code_hash.bytes);
    }

    /* Storage slots */
    for (uint16_t j = 0; j < g->slot_count; j++) {
        uint8_t key[32];
        uint8_t slot_le[32], val_le[32];
        uint256_to_bytes(&g->slots[j].slot, slot_le);
        uint256_to_bytes(&g->slots[j].value, val_le);
        verkle_storage_key(key, addr, slot_le);
        verkle_flat_set(vf, key, val_le);
    }
}

static void apply_diff(verkle_flat_t *vf, const block_diff_t *diff) {
    for (uint16_t i = 0; i < diff->group_count; i++)
        apply_group(vf, &diff->groups[i]);
}

/* =========================================================================
 * Consumer thread
 * ========================================================================= */

static void *builder_thread(void *arg) {
    verkle_builder_t *vb = (verkle_builder_t *)arg;
    uint64_t blocks_since_sync = 0;

    while (!atomic_load_explicit(&vb->stop, memory_order_relaxed)) {
        block_diff_t diff;
        if (!ring_pop(&vb->ring, &diff, &vb->stop))
            break;

        /* Apply diff to verkle_flat */
        verkle_flat_begin_block(vb->vf, diff.block_number);
        apply_diff(vb->vf, &diff);
        verkle_flat_commit_block(vb->vf);

        atomic_store_explicit(&vb->last_block, diff.block_number,
                              memory_order_release);

        block_diff_free(&diff);

        blocks_since_sync++;
        if (blocks_since_sync >= 256) {
            verkle_flat_sync(vb->vf);
            blocks_since_sync = 0;
        }
    }

    /* Drain remaining entries */
    for (;;) {
        size_t h = atomic_load_explicit(&vb->ring.head, memory_order_acquire);
        size_t t = atomic_load_explicit(&vb->ring.tail, memory_order_relaxed);
        if (h == t) break;

        block_diff_t diff;
        ring_pop(&vb->ring, &diff, NULL);

        verkle_flat_begin_block(vb->vf, diff.block_number);
        apply_diff(vb->vf, &diff);
        verkle_flat_commit_block(vb->vf);

        atomic_store_explicit(&vb->last_block, diff.block_number,
                              memory_order_release);
        block_diff_free(&diff);
    }

    verkle_flat_sync(vb->vf);
    return NULL;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

static verkle_builder_t *builder_init(verkle_flat_t *vf) {
    if (!vf) return NULL;

    verkle_builder_t *vb = calloc(1, sizeof(verkle_builder_t));
    if (!vb) { verkle_flat_destroy(vf); return NULL; }

    vb->vf = vf;
    ring_init(&vb->ring);
    atomic_store_explicit(&vb->stop, false, memory_order_relaxed);
    atomic_store_explicit(&vb->last_block, 0, memory_order_relaxed);

    if (pthread_create(&vb->thread, NULL, builder_thread, vb) != 0) {
        verkle_flat_destroy(vf);
        free(vb);
        return NULL;
    }

    return vb;
}

verkle_builder_t *verkle_builder_create(const char *value_dir,
                                         const char *commit_dir) {
    pedersen_init();
    verkle_flat_t *vf = verkle_flat_create(value_dir, commit_dir);
    return builder_init(vf);
}

verkle_builder_t *verkle_builder_open(const char *value_dir,
                                       const char *commit_dir) {
    pedersen_init();
    verkle_flat_t *vf = verkle_flat_open(value_dir, commit_dir);
    return builder_init(vf);
}

void verkle_builder_destroy(verkle_builder_t *vb) {
    if (!vb) return;

    atomic_store_explicit(&vb->stop, true, memory_order_release);
    pthread_join(vb->thread, NULL);

    verkle_flat_destroy(vb->vf);
    free(vb);
}

/* =========================================================================
 * Producer API
 * ========================================================================= */

void verkle_builder_push(verkle_builder_t *vb, const block_diff_t *diff) {
    if (!vb || !diff) return;

    if (!ring_try_push(&vb->ring, diff)) {
        fprintf(stderr, "verkle_builder: ring full, dropped block %lu\n",
                diff->block_number);
        /* Caller still owns the diff — don't free here since we're
         * called with a copy from state_history_capture pattern */
    }
}

/* =========================================================================
 * Query API
 * ========================================================================= */

uint64_t verkle_builder_last_block(const verkle_builder_t *vb) {
    if (!vb) return 0;
    return atomic_load_explicit(&vb->last_block, memory_order_acquire);
}
