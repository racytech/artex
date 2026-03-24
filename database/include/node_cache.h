#ifndef NODE_CACHE_H
#define NODE_CACHE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Node Cache — LRU cache for MPT trie nodes.
 *
 * Hash table (keyed by 32-byte node hash) + intrusive doubly-linked list
 * for LRU eviction. Each entry holds the full RLP bytes inline (up to
 * MAX_NODE_RLP = 1024 bytes).
 *
 * Memory budget is configurable: when total cached bytes exceeds the
 * limit, the least-recently-used entries are evicted.
 *
 * Depth pinning: nodes at trie depth <= NC_PIN_DEPTH are never evicted.
 * These upper-branch nodes sit on every trie walk path — evicting them
 * always causes a page fault on the next access. ~70K nodes at depth 4,
 * ~70MB — negligible compared to multi-GB budgets.
 *
 * Thread safety: none (single-threaded, same as mpt_store).
 */

#define NODE_CACHE_MAX_RLP   1024
#define NC_PIN_DEPTH         4       /* nodes at depth 0-4 are pinned */
#define NC_DEPTH_UNKNOWN     0xFF    /* depth not known (e.g., write_node) */

typedef struct nc_entry {
    uint8_t          hash[32];       /* node hash (key) */
    uint16_t         rlp_len;        /* length of RLP data */
    uint8_t          depth;          /* trie depth (for pin policy) */
    uint8_t          rlp[NODE_CACHE_MAX_RLP]; /* inline RLP */

    /* Hash table chain */
    struct nc_entry *ht_next;

    /* LRU doubly-linked list */
    struct nc_entry *lru_prev;
    struct nc_entry *lru_next;
} nc_entry_t;

typedef struct {
    /* Hash table: open chaining */
    nc_entry_t **buckets;
    uint32_t     bucket_count;
    uint32_t     bucket_mask;        /* bucket_count - 1 (power of 2) */

    /* LRU list: head = most recent, tail = least recent */
    nc_entry_t  *lru_head;
    nc_entry_t  *lru_tail;

    /* Eviction pointer: last unpinned entry (avoids scanning past pinned tail) */
    nc_entry_t  *evict_tail;

    /* Memory tracking */
    uint64_t     used_bytes;         /* total bytes in cache (entry overhead + rlp) */
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

/** Create a cache with the given memory budget in bytes. */
bool node_cache_init(node_cache_t *nc, uint64_t max_bytes);

/** Destroy cache, free all entries. */
void node_cache_destroy(node_cache_t *nc);

/** Clear all entries, keep allocated buckets. */
void node_cache_clear(node_cache_t *nc);

/* =========================================================================
 * Operations
 * ========================================================================= */

/**
 * Look up a node by hash. On hit, moves entry to LRU head.
 * Returns RLP length on hit (copies to buf), 0 on miss.
 */
uint32_t node_cache_get(node_cache_t *nc, const uint8_t hash[32],
                         uint8_t *buf, uint32_t buf_len);

/**
 * Insert a node into the cache. Evicts LRU entries if over budget,
 * skipping pinned entries (depth <= NC_PIN_DEPTH).
 * If the hash already exists, updates the entry and moves to head.
 * depth: trie depth of this node (use NC_DEPTH_UNKNOWN if not known).
 */
void node_cache_put(node_cache_t *nc, const uint8_t hash[32],
                     const uint8_t *rlp, uint16_t rlp_len, uint8_t depth);

/**
 * Remove a specific entry (e.g., on node deletion from trie).
 */
void node_cache_remove(node_cache_t *nc, const uint8_t hash[32]);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    uint64_t entry_count;
    uint64_t used_bytes;
    uint64_t max_bytes;
    uint64_t hits;
    uint64_t misses;
    uint64_t evictions;
} node_cache_stats_t;

node_cache_stats_t node_cache_stats(const node_cache_t *nc);

#endif /* NODE_CACHE_H */
