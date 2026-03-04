/*
 * Discv5 Kademlia Routing Table — 256 k-buckets, XOR distance.
 */

#include "../include/discv5_table.h"
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * XOR distance helpers
 * ========================================================================= */

/* Leading zeros count for a byte */
static int clz8(uint8_t b) {
    if (b == 0) return 8;
    int n = 0;
    if ((b & 0xF0) == 0) { n += 4; b <<= 4; }
    if ((b & 0xC0) == 0) { n += 2; b <<= 2; }
    if ((b & 0x80) == 0) { n += 1; }
    return n;
}

int discv5_table_bucket_index(const uint8_t local_id[32],
                               const uint8_t node_id[32]) {
    /*
     * log-distance = 256 - leading_zeros(XOR(local, node))
     * Bucket index = log-distance - 1  (range 0..255)
     *
     * Returns the bit position of the highest differing bit.
     * If the IDs are identical, returns -1.
     */
    for (int i = 0; i < 32; i++) {
        uint8_t x = local_id[i] ^ node_id[i];
        if (x != 0) {
            int leading = i * 8 + clz8(x);
            return 255 - leading;
        }
    }
    return -1;  /* same ID */
}

/* Compare XOR distances: memcmp on XOR(a,target) vs XOR(b,target) */
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
 * Table init
 * ========================================================================= */

void discv5_table_init(discv5_table_t *table, const uint8_t local_id[32]) {
    memset(table, 0, sizeof(*table));
    memcpy(table->local_id, local_id, 32);
}

/* =========================================================================
 * Insert
 * ========================================================================= */

/* Find node in bucket entries. Returns index or -1. */
static int bucket_find(const discv5_bucket_t *b, const uint8_t node_id[32]) {
    for (int i = 0; i < b->count; i++) {
        if (memcmp(b->entries[i].node_id, node_id, 32) == 0)
            return i;
    }
    return -1;
}

/* Find node in replacement cache. Returns index or -1. */
static int repl_find(const discv5_bucket_t *b, const uint8_t node_id[32]) {
    for (int i = 0; i < b->repl_count; i++) {
        if (memcmp(b->replacements[i].node_id, node_id, 32) == 0)
            return i;
    }
    return -1;
}

/* Move entry at idx to tail (most recently seen). */
static void bucket_move_to_tail(discv5_bucket_t *b, int idx) {
    discv5_node_t tmp = b->entries[idx];
    for (int i = idx; i < b->count - 1; i++)
        b->entries[i] = b->entries[i + 1];
    b->entries[b->count - 1] = tmp;
}

discv5_table_result_t discv5_table_insert(discv5_table_t *table,
                                           const discv5_node_t *node) {
    int idx = discv5_table_bucket_index(table->local_id, node->node_id);
    if (idx < 0)
        return DISCV5_TABLE_SELF;

    discv5_bucket_t *bucket = &table->buckets[idx];

    /* Already in bucket? Move to tail. */
    int existing = bucket_find(bucket, node->node_id);
    if (existing >= 0) {
        bucket->entries[existing] = *node;
        bucket->entries[existing].last_seen = ++table->clock;
        bucket_move_to_tail(bucket, existing);
        return DISCV5_TABLE_UPDATED;
    }

    /* Bucket has space? Add to tail. */
    if (bucket->count < DISCV5_BUCKET_SIZE) {
        bucket->entries[bucket->count] = *node;
        bucket->entries[bucket->count].last_seen = ++table->clock;
        bucket->count++;
        return DISCV5_TABLE_ADDED;
    }

    /* Bucket full — check replacement cache. */
    int repl_idx = repl_find(bucket, node->node_id);
    if (repl_idx >= 0) {
        /* Already in replacement cache, update */
        bucket->replacements[repl_idx] = *node;
        bucket->replacements[repl_idx].last_seen = ++table->clock;
        return DISCV5_TABLE_REPLACEMENT;
    }

    if (bucket->repl_count < DISCV5_BUCKET_SIZE) {
        /* Add to replacement cache */
        bucket->replacements[bucket->repl_count] = *node;
        bucket->replacements[bucket->repl_count].last_seen = ++table->clock;
        bucket->repl_count++;
        return DISCV5_TABLE_REPLACEMENT;
    }

    /* Both bucket and replacement cache full. Evict LRU replacement. */
    int lru = 0;
    for (int i = 1; i < bucket->repl_count; i++) {
        if (bucket->replacements[i].last_seen < bucket->replacements[lru].last_seen)
            lru = i;
    }
    bucket->replacements[lru] = *node;
    bucket->replacements[lru].last_seen = ++table->clock;
    return DISCV5_TABLE_REPLACEMENT;
}

/* =========================================================================
 * Remove
 * ========================================================================= */

bool discv5_table_remove(discv5_table_t *table, const uint8_t node_id[32]) {
    int idx = discv5_table_bucket_index(table->local_id, node_id);
    if (idx < 0) return false;

    discv5_bucket_t *bucket = &table->buckets[idx];

    /* Check bucket entries */
    int pos = bucket_find(bucket, node_id);
    if (pos >= 0) {
        for (int i = pos; i < bucket->count - 1; i++)
            bucket->entries[i] = bucket->entries[i + 1];
        bucket->count--;
        return true;
    }

    /* Check replacement cache */
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

const discv5_node_t *discv5_table_find(const discv5_table_t *table,
                                        const uint8_t node_id[32]) {
    int idx = discv5_table_bucket_index(table->local_id, node_id);
    if (idx < 0) return NULL;

    const discv5_bucket_t *bucket = &table->buckets[idx];

    int pos = bucket_find(bucket, node_id);
    if (pos >= 0)
        return &bucket->entries[pos];

    pos = repl_find(bucket, node_id);
    if (pos >= 0)
        return &bucket->replacements[pos];

    return NULL;
}

/* =========================================================================
 * Mark alive / dead
 * ========================================================================= */

void discv5_table_mark_alive(discv5_table_t *table, const uint8_t node_id[32]) {
    int idx = discv5_table_bucket_index(table->local_id, node_id);
    if (idx < 0) return;

    discv5_bucket_t *bucket = &table->buckets[idx];
    int pos = bucket_find(bucket, node_id);
    if (pos >= 0) {
        bucket->entries[pos].last_seen = ++table->clock;
        bucket->entries[pos].checks++;
        bucket_move_to_tail(bucket, pos);
    }
}

bool discv5_table_mark_dead(discv5_table_t *table, const uint8_t node_id[32]) {
    int idx = discv5_table_bucket_index(table->local_id, node_id);
    if (idx < 0) return false;

    discv5_bucket_t *bucket = &table->buckets[idx];
    int pos = bucket_find(bucket, node_id);
    if (pos < 0) return false;

    /* Remove the dead node */
    for (int i = pos; i < bucket->count - 1; i++)
        bucket->entries[i] = bucket->entries[i + 1];
    bucket->count--;

    /* Promote most recently seen replacement */
    if (bucket->repl_count > 0) {
        int best = 0;
        for (int i = 1; i < bucket->repl_count; i++) {
            if (bucket->replacements[i].last_seen > bucket->replacements[best].last_seen)
                best = i;
        }
        /* Add to tail of bucket */
        bucket->entries[bucket->count] = bucket->replacements[best];
        bucket->entries[bucket->count].last_seen = ++table->clock;
        bucket->count++;

        /* Remove from replacement cache */
        for (int i = best; i < bucket->repl_count - 1; i++)
            bucket->replacements[i] = bucket->replacements[i + 1];
        bucket->repl_count--;

        return true;
    }

    return false;
}

/* =========================================================================
 * Closest nodes (sorted by XOR distance to target)
 * ========================================================================= */

/* qsort comparator context — target id for distance comparison */
static const uint8_t *g_sort_target;

static int closest_cmp(const void *a, const void *b) {
    const discv5_node_t *na = (const discv5_node_t *)a;
    const discv5_node_t *nb = (const discv5_node_t *)b;
    return distance_cmp(na->node_id, nb->node_id, g_sort_target);
}

size_t discv5_table_closest(const discv5_table_t *table,
                             const uint8_t target[32],
                             discv5_node_t *out, size_t max_count) {
    /* Collect all nodes */
    size_t total = 0;
    for (int b = 0; b < DISCV5_NUM_BUCKETS; b++) {
        const discv5_bucket_t *bucket = &table->buckets[b];
        for (int i = 0; i < bucket->count; i++) {
            out[total++] = bucket->entries[i];
            if (total >= DISCV5_NUM_BUCKETS * DISCV5_BUCKET_SIZE)
                goto sort;
        }
    }

sort:
    /* Sort by XOR distance to target */
    g_sort_target = target;
    qsort(out, total, sizeof(discv5_node_t), closest_cmp);

    /* Truncate to max_count */
    return (total < max_count) ? total : max_count;
}

/* =========================================================================
 * Nodes at specific log-distance
 * ========================================================================= */

size_t discv5_table_nodes_at_distance(const discv5_table_t *table,
                                       int log_dist,
                                       discv5_node_t *out) {
    if (log_dist < 0 || log_dist >= DISCV5_NUM_BUCKETS)
        return 0;

    const discv5_bucket_t *bucket = &table->buckets[log_dist];
    for (int i = 0; i < bucket->count; i++)
        out[i] = bucket->entries[i];

    return bucket->count;
}

/* =========================================================================
 * Table size
 * ========================================================================= */

size_t discv5_table_size(const discv5_table_t *table) {
    size_t total = 0;
    for (int b = 0; b < DISCV5_NUM_BUCKETS; b++)
        total += table->buckets[b].count;
    return total;
}
