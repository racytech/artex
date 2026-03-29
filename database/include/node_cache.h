#ifndef NODE_CACHE_H
#define NODE_CACHE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Node Cache — Two-list LRU cache for MPT trie nodes.
 *
 * Hash table (keyed by 32-byte node hash) + two separate LRU lists:
 *   - Pinned list: depth 0-4 nodes, never evicted (O(70K) entries, ~70MB)
 *   - Unpinned list: depth 5+ nodes, evicted when over memory budget
 *
 * Eviction is always O(1) — pop from unpinned tail. No scanning.
 * Cache hits move entry to head of its own list.
 *
 * Each entry holds the full RLP bytes inline (up to 1024 bytes).
 * Thread safety: none (single-threaded).
 */

#define NODE_CACHE_MAX_RLP   1024
#define NC_PIN_DEPTH         4       /* nodes at depth 0-4 are pinned */
#define NC_DEPTH_UNKNOWN     0xFF    /* depth not known (e.g., write_node) */

typedef struct nc_entry {
    uint8_t          hash[32];       /* node hash (key) */
    uint16_t         rlp_len;        /* length of RLP data */
    uint8_t          depth;          /* trie depth (for pin policy) */
    bool             pinned;         /* which list this entry belongs to */
    uint8_t          rlp[NODE_CACHE_MAX_RLP]; /* inline RLP */

    /* Hash table chain */
    struct nc_entry *ht_next;

    /* LRU doubly-linked list (pinned or unpinned) */
    struct nc_entry *lru_prev;
    struct nc_entry *lru_next;
} nc_entry_t;

typedef struct {
    /* Hash table: open chaining */
    nc_entry_t **buckets;
    uint32_t     bucket_count;
    uint32_t     bucket_mask;        /* bucket_count - 1 (power of 2) */

    /* Pinned list: depth 0-4, never evicted */
    nc_entry_t  *pin_head;
    nc_entry_t  *pin_tail;
    uint64_t     pin_count;

    /* Unpinned list: depth 5+, evicted LRU */
    nc_entry_t  *lru_head;
    nc_entry_t  *lru_tail;

    /* Memory tracking */
    uint64_t     used_bytes;         /* total bytes (pinned + unpinned) */
    uint64_t     max_bytes;          /* memory budget */
    uint64_t     entry_count;

    /* Free list: reuse evicted entry structs */
    nc_entry_t  *free_list;
    uint64_t     free_count;

    /* Stats */
    uint64_t     hits;
    uint64_t     misses;
    uint64_t     evictions;
} node_cache_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

bool node_cache_init(node_cache_t *nc, uint64_t max_bytes);
void node_cache_destroy(node_cache_t *nc);
void node_cache_clear(node_cache_t *nc);

/* =========================================================================
 * Operations
 * ========================================================================= */

uint32_t node_cache_get(node_cache_t *nc, const uint8_t hash[32],
                         uint8_t *buf, uint32_t buf_len);

void node_cache_put(node_cache_t *nc, const uint8_t hash[32],
                     const uint8_t *rlp, uint16_t rlp_len, uint8_t depth);

void node_cache_remove(node_cache_t *nc, const uint8_t hash[32]);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    uint64_t entry_count;
    uint64_t pin_count;
    uint64_t used_bytes;
    uint64_t max_bytes;
    uint64_t hits;
    uint64_t misses;
    uint64_t evictions;
} node_cache_stats_t;

node_cache_stats_t node_cache_stats(const node_cache_t *nc);

#endif /* NODE_CACHE_H */
