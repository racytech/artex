/*
 * Node Cache — LRU cache for MPT trie nodes.
 *
 * Hash table with open chaining + doubly-linked LRU list.
 * Entries hold full RLP inline (up to 1024 bytes). Memory-budgeted
 * with configurable limit. Evicts LRU entries when over budget.
 */

#include "node_cache.h"

#include <stdlib.h>
#include <string.h>

/* Per-entry overhead: hash(32) + rlp_len(2) + rlp(1024) + pointers(24) ≈ 1082 bytes.
 * We track used_bytes as entry_count * ENTRY_COST + sum(rlp_len) for simplicity,
 * but since RLP is inline (fixed 1024 buffer), just count entries × sizeof. */
#define ENTRY_SIZE sizeof(nc_entry_t)

/* =========================================================================
 * Hash table helpers
 * ========================================================================= */

static inline uint32_t hash_to_bucket(const uint8_t hash[32], uint32_t mask) {
    uint32_t h;
    memcpy(&h, hash, 4);
    return h & mask;
}

static uint32_t next_pow2_32(uint32_t v) {
    if (v == 0) return 1;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

/* =========================================================================
 * LRU list helpers
 * ========================================================================= */

static void lru_unlink(node_cache_t *nc, nc_entry_t *e) {
    if (e->lru_prev)
        e->lru_prev->lru_next = e->lru_next;
    else
        nc->lru_head = e->lru_next;

    if (e->lru_next)
        e->lru_next->lru_prev = e->lru_prev;
    else
        nc->lru_tail = e->lru_prev;

    e->lru_prev = NULL;
    e->lru_next = NULL;
}

static void lru_push_front(node_cache_t *nc, nc_entry_t *e) {
    e->lru_prev = NULL;
    e->lru_next = nc->lru_head;
    if (nc->lru_head)
        nc->lru_head->lru_prev = e;
    nc->lru_head = e;
    if (!nc->lru_tail)
        nc->lru_tail = e;
}

/* =========================================================================
 * Entry allocation
 * ========================================================================= */

static nc_entry_t *alloc_entry(node_cache_t *nc) {
    if (nc->free_list) {
        nc_entry_t *e = nc->free_list;
        nc->free_list = e->ht_next;
        nc->free_count--;
        memset(e, 0, sizeof(*e));
        return e;
    }
    nc_entry_t *e = calloc(1, sizeof(nc_entry_t));
    return e;
}

static void free_entry(node_cache_t *nc, nc_entry_t *e) {
    /* Keep up to 4096 entries on free list to avoid malloc churn */
    if (nc->free_count < 4096) {
        e->ht_next = nc->free_list;
        nc->free_list = e;
        nc->free_count++;
    } else {
        free(e);
    }
}

/* =========================================================================
 * Hash table operations
 * ========================================================================= */

static void ht_insert(node_cache_t *nc, nc_entry_t *e) {
    uint32_t b = hash_to_bucket(e->hash, nc->bucket_mask);
    e->ht_next = nc->buckets[b];
    nc->buckets[b] = e;
}

static nc_entry_t *ht_find(node_cache_t *nc, const uint8_t hash[32]) {
    uint32_t b = hash_to_bucket(hash, nc->bucket_mask);
    nc_entry_t *e = nc->buckets[b];
    while (e) {
        if (memcmp(e->hash, hash, 32) == 0)
            return e;
        e = e->ht_next;
    }
    return NULL;
}

static void ht_remove(node_cache_t *nc, nc_entry_t *e) {
    uint32_t b = hash_to_bucket(e->hash, nc->bucket_mask);
    nc_entry_t **pp = &nc->buckets[b];
    while (*pp) {
        if (*pp == e) {
            *pp = e->ht_next;
            e->ht_next = NULL;
            return;
        }
        pp = &(*pp)->ht_next;
    }
}

/* =========================================================================
 * Eviction
 * ========================================================================= */

static void evict_one(node_cache_t *nc) {
    /* O(1) eviction via evict_tail pointer — skips pinned entries */
    nc_entry_t *victim = nc->evict_tail;

    if (!victim) {
        /* All entries are pinned — evict true tail as fallback */
        victim = nc->lru_tail;
        if (!victim) return;
    }

    /* Advance evict_tail before unlinking */
    nc->evict_tail = victim->lru_prev;
    while (nc->evict_tail && nc->evict_tail->depth <= NC_PIN_DEPTH)
        nc->evict_tail = nc->evict_tail->lru_prev;

    lru_unlink(nc, victim);
    ht_remove(nc, victim);

    nc->used_bytes -= ENTRY_SIZE;
    nc->entry_count--;
    nc->evictions++;

    free_entry(nc, victim);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

bool node_cache_init(node_cache_t *nc, uint64_t max_bytes) {
    memset(nc, 0, sizeof(*nc));
    nc->max_bytes = max_bytes;

    /* Size hash table to expected entry count.
     * Each entry ≈ 1082 bytes, so max_entries ≈ max_bytes / 1082.
     * Hash table at 2x entries for low load factor. */
    uint64_t expected = max_bytes / ENTRY_SIZE;
    if (expected < 1024) expected = 1024;
    uint32_t buckets = next_pow2_32((uint32_t)(expected * 2));
    if (buckets < 1024) buckets = 1024;

    nc->buckets = calloc(buckets, sizeof(nc_entry_t *));
    if (!nc->buckets) return false;

    nc->bucket_count = buckets;
    nc->bucket_mask = buckets - 1;
    return true;
}

void node_cache_destroy(node_cache_t *nc) {
    if (!nc) return;

    /* Free all entries in the LRU list */
    nc_entry_t *e = nc->lru_head;
    while (e) {
        nc_entry_t *next = e->lru_next;
        free(e);
        e = next;
    }

    /* Free the free list */
    e = nc->free_list;
    while (e) {
        nc_entry_t *next = e->ht_next;
        free(e);
        e = next;
    }

    free(nc->buckets);
    memset(nc, 0, sizeof(*nc));
}

void node_cache_clear(node_cache_t *nc) {
    if (!nc) return;

    /* Move all entries to free list (or free them) */
    nc_entry_t *e = nc->lru_head;
    while (e) {
        nc_entry_t *next = e->lru_next;
        free_entry(nc, e);
        e = next;
    }

    nc->lru_head = NULL;
    nc->lru_tail = NULL;
    nc->evict_tail = NULL;
    nc->used_bytes = 0;
    nc->entry_count = 0;

    /* Clear hash table buckets */
    memset(nc->buckets, 0, nc->bucket_count * sizeof(nc_entry_t *));
}

/* =========================================================================
 * Operations
 * ========================================================================= */

uint32_t node_cache_get(node_cache_t *nc, const uint8_t hash[32],
                         uint8_t *buf, uint32_t buf_len) {
    nc_entry_t *e = ht_find(nc, hash);
    if (!e) {
        nc->misses++;
        return 0;
    }

    nc->hits++;

    /* Move to front (most recently used).
     * If this was the evict_tail, advance it first. */
    if (e == nc->evict_tail) {
        nc->evict_tail = e->lru_prev;
        while (nc->evict_tail && nc->evict_tail->depth <= NC_PIN_DEPTH)
            nc->evict_tail = nc->evict_tail->lru_prev;
    }
    lru_unlink(nc, e);
    lru_push_front(nc, e);

    /* Unpinned entry moved to head — it's now the newest candidate.
     * If evict_tail was NULL, this entry becomes the new evict_tail. */
    if (!nc->evict_tail && e->depth > NC_PIN_DEPTH)
        nc->evict_tail = e;

    if (buf && buf_len >= e->rlp_len)
        memcpy(buf, e->rlp, e->rlp_len);

    return e->rlp_len;
}

void node_cache_put(node_cache_t *nc, const uint8_t hash[32],
                     const uint8_t *rlp, uint16_t rlp_len, uint8_t depth) {
    if (rlp_len > NODE_CACHE_MAX_RLP) return;

    /* Check if already cached — update in place */
    nc_entry_t *e = ht_find(nc, hash);
    if (e) {
        memcpy(e->rlp, rlp, rlp_len);
        e->rlp_len = rlp_len;
        /* Update depth if we now have better info */
        if (depth < e->depth) e->depth = depth;
        lru_unlink(nc, e);
        lru_push_front(nc, e);
        return;
    }

    /* Evict if needed */
    while (nc->used_bytes + ENTRY_SIZE > nc->max_bytes && nc->lru_tail)
        evict_one(nc);

    /* Allocate and insert */
    e = alloc_entry(nc);
    if (!e) return;

    memcpy(e->hash, hash, 32);
    memcpy(e->rlp, rlp, rlp_len);
    e->rlp_len = rlp_len;
    e->depth = depth;

    ht_insert(nc, e);
    lru_push_front(nc, e);

    /* New unpinned entry — if no evict_tail, this is it */
    if (!nc->evict_tail && depth > NC_PIN_DEPTH)
        nc->evict_tail = e;

    nc->used_bytes += ENTRY_SIZE;
    nc->entry_count++;
}

void node_cache_remove(node_cache_t *nc, const uint8_t hash[32]) {
    nc_entry_t *e = ht_find(nc, hash);
    if (!e) return;

    if (e == nc->evict_tail) {
        nc->evict_tail = e->lru_prev;
        while (nc->evict_tail && nc->evict_tail->depth <= NC_PIN_DEPTH)
            nc->evict_tail = nc->evict_tail->lru_prev;
    }

    lru_unlink(nc, e);
    ht_remove(nc, e);

    nc->used_bytes -= ENTRY_SIZE;
    nc->entry_count--;

    free_entry(nc, e);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

node_cache_stats_t node_cache_stats(const node_cache_t *nc) {
    node_cache_stats_t s = {0};
    if (!nc) return s;
    s.entry_count = nc->entry_count;
    s.used_bytes = nc->used_bytes;
    s.max_bytes = nc->max_bytes;
    s.hits = nc->hits;
    s.misses = nc->misses;
    s.evictions = nc->evictions;
    return s;
}
