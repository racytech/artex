/*
 * Node Cache — Two-list LRU cache for MPT trie nodes.
 *
 * Pinned list (depth 0-4): never evicted, O(1) insert/access.
 * Unpinned list (depth 5+): LRU eviction when over budget, O(1) evict.
 * Hash table for O(1) lookup by node hash.
 */

#include "node_cache.h"

#include <stdlib.h>
#include <string.h>

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
    v |= v >> 1; v |= v >> 2; v |= v >> 4;
    v |= v >> 8; v |= v >> 16;
    return v + 1;
}

/* =========================================================================
 * List helpers (generic — works for both pinned and unpinned)
 * ========================================================================= */

static void list_unlink(nc_entry_t **head, nc_entry_t **tail, nc_entry_t *e) {
    if (e->lru_prev)
        e->lru_prev->lru_next = e->lru_next;
    else
        *head = e->lru_next;

    if (e->lru_next)
        e->lru_next->lru_prev = e->lru_prev;
    else
        *tail = e->lru_prev;

    e->lru_prev = NULL;
    e->lru_next = NULL;
}

static void list_push_front(nc_entry_t **head, nc_entry_t **tail, nc_entry_t *e) {
    e->lru_prev = NULL;
    e->lru_next = *head;
    if (*head)
        (*head)->lru_prev = e;
    *head = e;
    if (!*tail)
        *tail = e;
}

/* Unlink from whichever list the entry belongs to */
static void entry_unlink(node_cache_t *nc, nc_entry_t *e) {
    if (e->pinned)
        list_unlink(&nc->pin_head, &nc->pin_tail, e);
    else
        list_unlink(&nc->lru_head, &nc->lru_tail, e);
}

/* Push to front of the appropriate list */
static void entry_push_front(node_cache_t *nc, nc_entry_t *e) {
    if (e->pinned)
        list_push_front(&nc->pin_head, &nc->pin_tail, e);
    else
        list_push_front(&nc->lru_head, &nc->lru_tail, e);
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
    return calloc(1, sizeof(nc_entry_t));
}

static void free_entry(node_cache_t *nc, nc_entry_t *e) {
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
 * Eviction — O(1), always from unpinned tail
 * ========================================================================= */

static void evict_one(node_cache_t *nc) {
    nc_entry_t *victim = nc->lru_tail;
    if (!victim) return;

    list_unlink(&nc->lru_head, &nc->lru_tail, victim);
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

    /* Free pinned list */
    nc_entry_t *e = nc->pin_head;
    while (e) {
        nc_entry_t *next = e->lru_next;
        free(e);
        e = next;
    }

    /* Free unpinned list */
    e = nc->lru_head;
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

    /* Move all pinned entries to free list */
    nc_entry_t *e = nc->pin_head;
    while (e) {
        nc_entry_t *next = e->lru_next;
        free_entry(nc, e);
        e = next;
    }

    /* Move all unpinned entries to free list */
    e = nc->lru_head;
    while (e) {
        nc_entry_t *next = e->lru_next;
        free_entry(nc, e);
        e = next;
    }

    nc->pin_head = NULL;
    nc->pin_tail = NULL;
    nc->pin_count = 0;
    nc->lru_head = NULL;
    nc->lru_tail = NULL;
    nc->used_bytes = 0;
    nc->entry_count = 0;

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

    /* Move to front of its own list */
    entry_unlink(nc, e);
    entry_push_front(nc, e);

    if (buf && buf_len >= e->rlp_len)
        memcpy(buf, e->rlp, e->rlp_len);

    return e->rlp_len;
}

void node_cache_put(node_cache_t *nc, const uint8_t hash[32],
                     const uint8_t *rlp, uint16_t rlp_len, uint8_t depth) {
    if (rlp_len > NODE_CACHE_MAX_RLP) return;

    bool should_pin = (depth <= NC_PIN_DEPTH);

    /* Check if already cached — update in place */
    nc_entry_t *e = ht_find(nc, hash);
    if (e) {
        memcpy(e->rlp, rlp, rlp_len);
        e->rlp_len = rlp_len;

        /* Promote to pinned if we now have better depth info */
        if (should_pin && !e->pinned) {
            entry_unlink(nc, e);
            e->pinned = true;
            e->depth = depth;
            nc->pin_count++;
            entry_push_front(nc, e);
        } else {
            if (depth < e->depth) e->depth = depth;
            entry_unlink(nc, e);
            entry_push_front(nc, e);
        }
        return;
    }

    /* Evict from unpinned list if over budget (pinned entries don't count) */
    if (!should_pin) {
        while (nc->used_bytes + ENTRY_SIZE > nc->max_bytes && nc->lru_tail)
            evict_one(nc);
    }

    /* Allocate and insert */
    e = alloc_entry(nc);
    if (!e) return;

    memcpy(e->hash, hash, 32);
    memcpy(e->rlp, rlp, rlp_len);
    e->rlp_len = rlp_len;
    e->depth = depth;
    e->pinned = should_pin;

    ht_insert(nc, e);
    entry_push_front(nc, e);

    if (should_pin)
        nc->pin_count++;

    nc->used_bytes += ENTRY_SIZE;
    nc->entry_count++;
}

void node_cache_remove(node_cache_t *nc, const uint8_t hash[32]) {
    nc_entry_t *e = ht_find(nc, hash);
    if (!e) return;

    entry_unlink(nc, e);
    ht_remove(nc, e);

    if (e->pinned)
        nc->pin_count--;

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
    s.pin_count = nc->pin_count;
    s.used_bytes = nc->used_bytes;
    s.max_bytes = nc->max_bytes;
    s.hits = nc->hits;
    s.misses = nc->misses;
    s.evictions = nc->evictions;
    return s;
}
