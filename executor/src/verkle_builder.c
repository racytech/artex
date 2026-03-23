#include "verkle_builder.h"
#include "verkle_state.h"
#include "verkle_key.h"
#include "mem_art.h"
#include "pedersen.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>

/* =========================================================================
 * Slot Tracker (same as verkle_reconstruct — per-account storage index)
 * ========================================================================= */

typedef struct {
    uint8_t (*keys)[32];
    uint32_t count;
    uint32_t cap;
} slot_set_t;

typedef struct {
    mem_art_t tree;
} slot_tracker_t;

static bool slot_tracker_init(slot_tracker_t *st) {
    return mem_art_init(&st->tree);
}

static bool slot_tracker_free_cb(const uint8_t *key, size_t key_len,
                                  const void *value, size_t value_len,
                                  void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    const slot_set_t *ss = (const slot_set_t *)value;
    free(ss->keys);
    return true;
}

static void slot_tracker_destroy(slot_tracker_t *st) {
    mem_art_foreach(&st->tree, slot_tracker_free_cb, NULL);
    mem_art_destroy(&st->tree);
}

static void slot_tracker_add(slot_tracker_t *st,
                              const uint8_t addr[20],
                              const uint8_t slot[32]) {
    size_t val_len = 0;
    slot_set_t *ss = (slot_set_t *)mem_art_get_mut(
        &st->tree, addr, 20, &val_len);

    if (!ss) {
        slot_set_t new_ss = {0};
        new_ss.cap = 8;
        new_ss.keys = malloc(new_ss.cap * 32);
        if (!new_ss.keys) return;
        memcpy(new_ss.keys[0], slot, 32);
        new_ss.count = 1;
        mem_art_insert(&st->tree, addr, 20, &new_ss, sizeof(slot_set_t));
        return;
    }

    for (uint32_t i = 0; i < ss->count; i++) {
        if (memcmp(ss->keys[i], slot, 32) == 0)
            return;
    }

    if (ss->count >= ss->cap) {
        uint32_t new_cap = ss->cap * 2;
        uint8_t (*new_keys)[32] = realloc(ss->keys, new_cap * 32);
        if (!new_keys) return;
        ss->keys = new_keys;
        ss->cap = new_cap;
    }

    memcpy(ss->keys[ss->count], slot, 32);
    ss->count++;
}

static const uint8_t *slot_tracker_get(const slot_tracker_t *st,
                                        const uint8_t addr[20],
                                        uint32_t *out_count) {
    size_t val_len = 0;
    const slot_set_t *ss = (const slot_set_t *)mem_art_get(
        &st->tree, addr, 20, &val_len);
    if (!ss || ss->count == 0) {
        *out_count = 0;
        return NULL;
    }
    *out_count = ss->count;
    return (const uint8_t *)ss->keys;
}

static void slot_tracker_clear(slot_tracker_t *st,
                                const uint8_t addr[20]) {
    size_t val_len = 0;
    slot_set_t *ss = (slot_set_t *)mem_art_get_mut(
        &st->tree, addr, 20, &val_len);
    if (ss) {
        free(ss->keys);
        ss->keys = NULL;
        ss->count = 0;
        ss->cap = 0;
    }
}

/* =========================================================================
 * Internal state
 * ========================================================================= */

struct verkle_builder {
    verkle_state_t *vs;
    code_store_t   *cs;          /* borrowed, not owned */
    slot_tracker_t  tracker;

    /* SPSC ring (same layout as state_history) */
    diff_ring_t     ring;

    /* Consumer thread */
    pthread_t       thread;
    atomic_bool     stop;

    /* Last committed block */
    atomic_uint_fast64_t last_block;
};

/* =========================================================================
 * SPSC ring helpers
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
 * Diff → Verkle conversion (full EIP-6800 compliance)
 * ========================================================================= */

static void apply_group(verkle_state_t *vs, code_store_t *cs,
                         slot_tracker_t *tracker, const addr_diff_t *g) {
    const uint8_t *addr = g->addr.bytes;

    /* SELFDESTRUCT: clear all state for this account */
    if (g->flags & ACCT_DIFF_DESTRUCTED) {
        uint32_t slot_count = 0;
        const uint8_t *slots = slot_tracker_get(tracker, addr, &slot_count);
        verkle_state_clear_account(vs, addr, slots, slot_count);
        slot_tracker_clear(tracker, addr);
    }

    /* Account creation: set version */
    if (g->flags & ACCT_DIFF_CREATED) {
        verkle_state_set_version(vs, addr, 0);
    }

    /* Nonce */
    if (g->field_mask & FIELD_NONCE) {
        verkle_state_set_nonce(vs, addr, g->nonce);
    }

    /* Balance */
    if (g->field_mask & FIELD_BALANCE) {
        uint8_t bal_bytes[32];
        uint256_to_bytes_le(&g->balance, bal_bytes);
        verkle_state_set_balance(vs, addr, bal_bytes);
    }

    /* Code hash + code chunks */
    if (g->field_mask & FIELD_CODE_HASH) {
        verkle_state_set_code_hash(vs, addr, g->code_hash.bytes);

        if (cs) {
            uint32_t code_len = code_store_get_size(cs, g->code_hash.bytes);
            if (code_len > 0) {
                uint8_t *code = malloc(code_len);
                if (code) {
                    uint32_t got = code_store_get(cs, g->code_hash.bytes,
                                                   code, code_len);
                    if (got == code_len)
                        verkle_state_set_code(vs, addr, code, code_len);
                    free(code);
                }
            } else {
                verkle_state_set_code_size(vs, addr, 0);
            }
        }
    }

    /* Storage slots */
    for (uint16_t j = 0; j < g->slot_count; j++) {
        uint8_t slot_bytes[32], val_bytes[32];
        uint256_to_bytes_le(&g->slots[j].slot, slot_bytes);
        uint256_to_bytes_le(&g->slots[j].value, val_bytes);
        verkle_state_set_storage(vs, addr, slot_bytes, val_bytes);
        slot_tracker_add(tracker, addr, slot_bytes);
    }
}

static void apply_diff(verkle_builder_t *vb, const block_diff_t *diff) {
    for (uint16_t i = 0; i < diff->group_count; i++)
        apply_group(vb->vs, vb->cs, &vb->tracker, &diff->groups[i]);
}

/* =========================================================================
 * Consumer thread
 * ========================================================================= */

static void process_diff(verkle_builder_t *vb, block_diff_t *diff) {
    verkle_state_begin_block(vb->vs, diff->block_number);
    apply_diff(vb, diff);
    verkle_state_commit_block(vb->vs);

    atomic_store_explicit(&vb->last_block, diff->block_number,
                          memory_order_release);
    block_diff_free(diff);
}

static void *builder_thread(void *arg) {
    verkle_builder_t *vb = (verkle_builder_t *)arg;
    uint64_t blocks_since_sync = 0;

    while (!atomic_load_explicit(&vb->stop, memory_order_relaxed)) {
        block_diff_t diff;
        if (!ring_pop(&vb->ring, &diff, &vb->stop))
            break;

        process_diff(vb, &diff);

        blocks_since_sync++;
        if (blocks_since_sync >= 256) {
            verkle_state_sync(vb->vs);
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
        process_diff(vb, &diff);
    }

    verkle_state_sync(vb->vs);
    return NULL;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

static verkle_builder_t *builder_init(verkle_state_t *vs, code_store_t *cs) {
    if (!vs) return NULL;

    verkle_builder_t *vb = calloc(1, sizeof(verkle_builder_t));
    if (!vb) { verkle_state_destroy(vs); return NULL; }

    vb->vs = vs;
    vb->cs = cs;  /* borrowed, not owned */
    ring_init(&vb->ring);
    atomic_store_explicit(&vb->stop, false, memory_order_relaxed);
    atomic_store_explicit(&vb->last_block, 0, memory_order_relaxed);

    if (!slot_tracker_init(&vb->tracker)) {
        verkle_state_destroy(vs);
        free(vb);
        return NULL;
    }

    if (pthread_create(&vb->thread, NULL, builder_thread, vb) != 0) {
        slot_tracker_destroy(&vb->tracker);
        verkle_state_destroy(vs);
        free(vb);
        return NULL;
    }

    return vb;
}

verkle_builder_t *verkle_builder_create(const char *value_dir,
                                         const char *commit_dir,
                                         code_store_t *cs) {
    pedersen_init();
    verkle_state_t *vs = verkle_state_create_flat(value_dir, commit_dir);
    return builder_init(vs, cs);
}

verkle_builder_t *verkle_builder_open(const char *value_dir,
                                       const char *commit_dir,
                                       code_store_t *cs) {
    pedersen_init();
    verkle_state_t *vs = verkle_state_open_flat(value_dir, commit_dir);
    return builder_init(vs, cs);
}

void verkle_builder_destroy(verkle_builder_t *vb) {
    if (!vb) return;

    atomic_store_explicit(&vb->stop, true, memory_order_release);
    pthread_join(vb->thread, NULL);

    slot_tracker_destroy(&vb->tracker);
    verkle_state_destroy(vb->vs);
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
    }
}

/* =========================================================================
 * Query API
 * ========================================================================= */

uint64_t verkle_builder_last_block(const verkle_builder_t *vb) {
    if (!vb) return 0;
    return atomic_load_explicit(&vb->last_block, memory_order_acquire);
}
