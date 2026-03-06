/*
 * Portal Overlay Routing Table.
 *
 * 256 k-buckets (k=16), XOR distance, with per-node data_radius
 * for content-aware lookups. Adapted from discv5_table.c.
 */

#include "../include/portal_table.h"
#include <string.h>
#include <stdlib.h>
#include <blst.h>

/* =========================================================================
 * XOR distance helpers
 * ========================================================================= */

static int clz8(uint8_t b) {
    if (b == 0) return 8;
    int n = 0;
    if ((b & 0xF0) == 0) { n += 4; b <<= 4; }
    if ((b & 0xC0) == 0) { n += 2; b <<= 2; }
    if ((b & 0x80) == 0) { n += 1; }
    return n;
}

int portal_table_bucket_index(const uint8_t local_id[32],
                              const uint8_t node_id[32]) {
    for (int i = 0; i < 32; i++) {
        uint8_t x = local_id[i] ^ node_id[i];
        if (x != 0)
            return 255 - (i * 8 + clz8(x));
    }
    return -1;
}

/* Compare XOR distances to a target: returns <0 if a closer, >0 if b closer */
static int distance_cmp(const uint8_t a[32], const uint8_t b[32],
                         const uint8_t target[32]) {
    for (int i = 0; i < 32; i++) {
        uint8_t da = a[i] ^ target[i];
        uint8_t db = b[i] ^ target[i];
        if (da < db) return -1;
        if (da > db) return 1;
    }
    return 0;
}

/* =========================================================================
 * U256 / radius comparison
 *
 * XOR distance is big-endian (byte 0 = MSB).
 * data_radius is little-endian (SSZ, byte 0 = LSB).
 *
 * Returns true if distance(node_id, content_id) <= radius.
 * ========================================================================= */

static bool xor_within_radius(const uint8_t node_id[32],
                               const uint8_t content_id[32],
                               const uint8_t radius_le[32]) {
    /* Compare XOR distance (big-endian) against radius (little-endian).
     * Walk from MSB of distance (byte 0) and MSB of radius (byte 31). */
    for (int i = 0; i < 32; i++) {
        uint8_t dist_byte = node_id[i] ^ content_id[i];  /* big-endian byte i */
        uint8_t rad_byte = radius_le[31 - i];             /* big-endian byte i */
        if (dist_byte < rad_byte) return true;
        if (dist_byte > rad_byte) return false;
    }
    return true; /* equal */
}

/* =========================================================================
 * Table init
 * ========================================================================= */

void portal_table_init(portal_table_t *t,
                       const uint8_t local_id[32],
                       const uint8_t local_radius[32]) {
    memset(t, 0, sizeof(*t));
    memcpy(t->local_id, local_id, 32);
    memcpy(t->local_radius, local_radius, 32);
}

/* =========================================================================
 * Bucket helpers
 * ========================================================================= */

static int bucket_find(const portal_bucket_t *b, const uint8_t node_id[32]) {
    for (int i = 0; i < b->count; i++) {
        if (memcmp(b->entries[i].node_id, node_id, 32) == 0)
            return i;
    }
    return -1;
}

static int repl_find(const portal_bucket_t *b, const uint8_t node_id[32]) {
    for (int i = 0; i < b->repl_count; i++) {
        if (memcmp(b->replacements[i].node_id, node_id, 32) == 0)
            return i;
    }
    return -1;
}

static void bucket_move_to_tail(portal_bucket_t *b, int idx) {
    portal_node_t tmp = b->entries[idx];
    for (int i = idx; i < b->count - 1; i++)
        b->entries[i] = b->entries[i + 1];
    b->entries[b->count - 1] = tmp;
}

/* =========================================================================
 * Insert
 * ========================================================================= */

portal_table_result_t portal_table_insert(portal_table_t *t,
                                          const portal_node_t *node) {
    int idx = portal_table_bucket_index(t->local_id, node->node_id);
    if (idx < 0) return PORTAL_TABLE_SELF;

    portal_bucket_t *bucket = &t->buckets[idx];

    /* Already in bucket? Update and move to tail. */
    int existing = bucket_find(bucket, node->node_id);
    if (existing >= 0) {
        bucket->entries[existing] = *node;
        bucket->entries[existing].last_seen = ++t->clock;
        bucket_move_to_tail(bucket, existing);
        return PORTAL_TABLE_UPDATED;
    }

    /* Bucket has space? */
    if (bucket->count < PORTAL_BUCKET_SIZE) {
        bucket->entries[bucket->count] = *node;
        bucket->entries[bucket->count].last_seen = ++t->clock;
        bucket->count++;
        return PORTAL_TABLE_ADDED;
    }

    /* Bucket full — try replacement cache */
    int ri = repl_find(bucket, node->node_id);
    if (ri >= 0) {
        bucket->replacements[ri] = *node;
        bucket->replacements[ri].last_seen = ++t->clock;
        return PORTAL_TABLE_REPLACEMENT;
    }

    if (bucket->repl_count < PORTAL_BUCKET_SIZE) {
        bucket->replacements[bucket->repl_count] = *node;
        bucket->replacements[bucket->repl_count].last_seen = ++t->clock;
        bucket->repl_count++;
        return PORTAL_TABLE_REPLACEMENT;
    }

    /* Evict LRU replacement */
    int lru = 0;
    for (int i = 1; i < bucket->repl_count; i++) {
        if (bucket->replacements[i].last_seen < bucket->replacements[lru].last_seen)
            lru = i;
    }
    bucket->replacements[lru] = *node;
    bucket->replacements[lru].last_seen = ++t->clock;
    return PORTAL_TABLE_REPLACEMENT;
}

/* =========================================================================
 * Remove
 * ========================================================================= */

bool portal_table_remove(portal_table_t *t, const uint8_t node_id[32]) {
    int idx = portal_table_bucket_index(t->local_id, node_id);
    if (idx < 0) return false;

    portal_bucket_t *bucket = &t->buckets[idx];

    int pos = bucket_find(bucket, node_id);
    if (pos >= 0) {
        for (int i = pos; i < bucket->count - 1; i++)
            bucket->entries[i] = bucket->entries[i + 1];
        bucket->count--;
        return true;
    }

    pos = repl_find(bucket, node_id);
    if (pos >= 0) {
        for (int i = pos; i < bucket->repl_count - 1; i++)
            bucket->replacements[i] = bucket->replacements[i + 1];
        bucket->repl_count--;
        return true;
    }

    return false;
}

/* =========================================================================
 * Find
 * ========================================================================= */

const portal_node_t *portal_table_find(const portal_table_t *t,
                                       const uint8_t node_id[32]) {
    int idx = portal_table_bucket_index(t->local_id, node_id);
    if (idx < 0) return NULL;

    const portal_bucket_t *bucket = &t->buckets[idx];

    int pos = bucket_find(bucket, node_id);
    if (pos >= 0) return &bucket->entries[pos];

    pos = repl_find(bucket, node_id);
    if (pos >= 0) return &bucket->replacements[pos];

    return NULL;
}

/* =========================================================================
 * Mark alive / dead
 * ========================================================================= */

void portal_table_mark_alive(portal_table_t *t, const uint8_t node_id[32]) {
    int idx = portal_table_bucket_index(t->local_id, node_id);
    if (idx < 0) return;

    portal_bucket_t *bucket = &t->buckets[idx];
    int pos = bucket_find(bucket, node_id);
    if (pos >= 0) {
        bucket->entries[pos].last_seen = ++t->clock;
        bucket->entries[pos].checks++;
        bucket_move_to_tail(bucket, pos);
    }
}

bool portal_table_mark_dead(portal_table_t *t, const uint8_t node_id[32]) {
    int idx = portal_table_bucket_index(t->local_id, node_id);
    if (idx < 0) return false;

    portal_bucket_t *bucket = &t->buckets[idx];
    int pos = bucket_find(bucket, node_id);
    if (pos < 0) return false;

    /* Remove dead node */
    for (int i = pos; i < bucket->count - 1; i++)
        bucket->entries[i] = bucket->entries[i + 1];
    bucket->count--;

    /* Promote most recent replacement */
    if (bucket->repl_count > 0) {
        int best = 0;
        for (int i = 1; i < bucket->repl_count; i++) {
            if (bucket->replacements[i].last_seen >
                bucket->replacements[best].last_seen)
                best = i;
        }
        bucket->entries[bucket->count] = bucket->replacements[best];
        bucket->entries[bucket->count].last_seen = ++t->clock;
        bucket->count++;

        for (int i = best; i < bucket->repl_count - 1; i++)
            bucket->replacements[i] = bucket->replacements[i + 1];
        bucket->repl_count--;

        return true;
    }

    return false;
}

/* =========================================================================
 * Nodes at distance
 * ========================================================================= */

size_t portal_table_nodes_at_distance(const portal_table_t *t,
                                      int log_dist,
                                      portal_node_t *out) {
    if (log_dist < 0 || log_dist >= PORTAL_NUM_BUCKETS)
        return 0;

    const portal_bucket_t *bucket = &t->buckets[log_dist];
    for (int i = 0; i < bucket->count; i++)
        out[i] = bucket->entries[i];

    return bucket->count;
}

/* =========================================================================
 * Table size
 * ========================================================================= */

size_t portal_table_size(const portal_table_t *t) {
    size_t total = 0;
    for (int b = 0; b < PORTAL_NUM_BUCKETS; b++)
        total += t->buckets[b].count;
    return total;
}

/* =========================================================================
 * Closest nodes (sorted by XOR distance)
 * ========================================================================= */

static const uint8_t *g_sort_target;

static int closest_cmp(const void *a, const void *b) {
    const portal_node_t *na = (const portal_node_t *)a;
    const portal_node_t *nb = (const portal_node_t *)b;
    return distance_cmp(na->node_id, nb->node_id, g_sort_target);
}

size_t portal_table_closest(const portal_table_t *t,
                            const uint8_t target[32],
                            portal_node_t *out, size_t max_count) {
    size_t total = 0;
    for (int b = 0; b < PORTAL_NUM_BUCKETS; b++) {
        const portal_bucket_t *bucket = &t->buckets[b];
        for (int i = 0; i < bucket->count; i++) {
            out[total++] = bucket->entries[i];
            if (total >= PORTAL_NUM_BUCKETS * PORTAL_BUCKET_SIZE)
                goto sort;
        }
    }

sort:
    g_sort_target = target;
    qsort(out, total, sizeof(portal_node_t), closest_cmp);
    return (total < max_count) ? total : max_count;
}

/* =========================================================================
 * Closest to content (radius-filtered)
 * ========================================================================= */

size_t portal_table_closest_to_content(const portal_table_t *t,
                                       const uint8_t content_id[32],
                                       portal_node_t *out, size_t max_count) {
    /* Collect nodes whose radius covers this content */
    portal_node_t candidates[PORTAL_NUM_BUCKETS * PORTAL_BUCKET_SIZE];
    size_t total = 0;

    for (int b = 0; b < PORTAL_NUM_BUCKETS; b++) {
        const portal_bucket_t *bucket = &t->buckets[b];
        for (int i = 0; i < bucket->count; i++) {
            if (xor_within_radius(bucket->entries[i].node_id,
                                   content_id,
                                   bucket->entries[i].data_radius)) {
                candidates[total++] = bucket->entries[i];
            }
        }
    }

    /* Sort by XOR distance to content_id */
    g_sort_target = content_id;
    qsort(candidates, total, sizeof(portal_node_t), closest_cmp);

    size_t n = (total < max_count) ? total : max_count;
    memcpy(out, candidates, n * sizeof(portal_node_t));
    return n;
}

/* =========================================================================
 * Radius operations
 * ========================================================================= */

bool portal_table_update_radius(portal_table_t *t,
                                const uint8_t node_id[32],
                                const uint8_t radius[32]) {
    int idx = portal_table_bucket_index(t->local_id, node_id);
    if (idx < 0) return false;

    portal_bucket_t *bucket = &t->buckets[idx];

    int pos = bucket_find(bucket, node_id);
    if (pos >= 0) {
        memcpy(bucket->entries[pos].data_radius, radius, 32);
        return true;
    }

    pos = repl_find(bucket, node_id);
    if (pos >= 0) {
        memcpy(bucket->replacements[pos].data_radius, radius, 32);
        return true;
    }

    return false;
}

void portal_table_set_radius(portal_table_t *t, const uint8_t radius[32]) {
    memcpy(t->local_radius, radius, 32);
}

bool portal_table_content_in_radius(const portal_table_t *t,
                                    const uint8_t content_id[32]) {
    return xor_within_radius(t->local_id, content_id, t->local_radius);
}

bool portal_node_content_in_radius(const portal_node_t *node,
                                   const uint8_t content_id[32]) {
    return xor_within_radius(node->node_id, content_id, node->data_radius);
}

/* =========================================================================
 * Content ID derivation
 * ========================================================================= */

void portal_content_id(uint8_t out[32],
                       const uint8_t *content_key, size_t key_len) {
    blst_sha256(out, content_key, key_len);
}
