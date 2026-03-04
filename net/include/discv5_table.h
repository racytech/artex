#ifndef ART_NET_DISCV5_TABLE_H
#define ART_NET_DISCV5_TABLE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Discv5 Kademlia Routing Table.
 *
 * 256 k-buckets (k=16), XOR distance metric.
 * Bucket index = log2(XOR(local_id, node_id)).
 * Each bucket has a replacement cache (up to k entries).
 *
 * LRU ordering: entries[0] = oldest, entries[count-1] = newest.
 */

#define DISCV5_BUCKET_SIZE  16
#define DISCV5_NUM_BUCKETS  256

/* =========================================================================
 * Node entry
 * ========================================================================= */

typedef struct {
    uint8_t  node_id[32];
    uint8_t  pubkey[33];    /* compressed secp256k1 pubkey */
    uint8_t  ip4[4];
    uint16_t udp_port;
    uint64_t last_seen;     /* monotonic counter */
    uint16_t checks;        /* successful liveness checks */
} discv5_node_t;

/* =========================================================================
 * K-bucket
 * ========================================================================= */

typedef struct {
    discv5_node_t entries[DISCV5_BUCKET_SIZE];
    uint8_t count;
    discv5_node_t replacements[DISCV5_BUCKET_SIZE];
    uint8_t repl_count;
    uint64_t last_refreshed;
} discv5_bucket_t;

/* =========================================================================
 * Routing table
 * ========================================================================= */

typedef struct {
    uint8_t local_id[32];
    discv5_bucket_t buckets[DISCV5_NUM_BUCKETS];
    uint64_t clock;
} discv5_table_t;

/* Insert result codes */
typedef enum {
    DISCV5_TABLE_ADDED = 0,        /* Node added to bucket */
    DISCV5_TABLE_UPDATED,          /* Existing node moved to tail (most recent) */
    DISCV5_TABLE_REPLACEMENT,      /* Bucket full, added to replacement cache */
    DISCV5_TABLE_SELF,             /* Rejected: same as local node */
    DISCV5_TABLE_FULL,             /* Both bucket and replacement cache are full */
} discv5_table_result_t;

/**
 * Initialize routing table with local node ID.
 */
void discv5_table_init(discv5_table_t *table, const uint8_t local_id[32]);

/**
 * Compute the bucket index (log-distance) for a node ID.
 * Returns 0..255, or -1 if node_id == local_id.
 */
int discv5_table_bucket_index(const uint8_t local_id[32],
                               const uint8_t node_id[32]);

/**
 * Insert a node into the routing table.
 * If the node already exists, it is moved to the tail (most recent).
 * If the bucket is full, the node is added to the replacement cache.
 */
discv5_table_result_t discv5_table_insert(discv5_table_t *table,
                                           const discv5_node_t *node);

/**
 * Remove a node from the table (both bucket and replacement cache).
 * Returns true if found and removed.
 */
bool discv5_table_remove(discv5_table_t *table, const uint8_t node_id[32]);

/**
 * Find a node by ID. Returns pointer to entry or NULL.
 * The returned pointer is valid until the next table modification.
 */
const discv5_node_t *discv5_table_find(const discv5_table_t *table,
                                        const uint8_t node_id[32]);

/**
 * Mark a node as alive (successful contact). Updates last_seen and checks.
 */
void discv5_table_mark_alive(discv5_table_t *table, const uint8_t node_id[32]);

/**
 * Mark a node as dead (failed liveness check).
 * Removes it from the bucket and promotes the most recent replacement.
 * Returns true if a replacement was promoted.
 */
bool discv5_table_mark_dead(discv5_table_t *table, const uint8_t node_id[32]);

/**
 * Find the closest nodes to a target ID.
 * Returns up to max_count nodes sorted by XOR distance.
 *
 * @param out          Output array (must hold max_count entries)
 * @param max_count    Maximum number of nodes to return
 * @return             Number of nodes returned
 */
size_t discv5_table_closest(const discv5_table_t *table,
                             const uint8_t target[32],
                             discv5_node_t *out, size_t max_count);

/**
 * Get all nodes at a specific log-distance from the local node.
 * Used for FINDNODE/NODES responses.
 *
 * @param log_dist     Log-distance (0..255)
 * @param out          Output array (must hold DISCV5_BUCKET_SIZE entries)
 * @return             Number of nodes returned
 */
size_t discv5_table_nodes_at_distance(const discv5_table_t *table,
                                       int log_dist,
                                       discv5_node_t *out);

/** Total number of nodes in the table (all buckets). */
size_t discv5_table_size(const discv5_table_t *table);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_DISCV5_TABLE_H */
