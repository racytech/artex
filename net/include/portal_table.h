#ifndef ART_NET_PORTAL_TABLE_H
#define ART_NET_PORTAL_TABLE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Portal Overlay Routing Table.
 *
 * Kademlia table (256 k-buckets, k=16, XOR distance) extended with
 * per-node data_radius for content-aware lookups.
 *
 * Each Portal sub-protocol (history, state, beacon) maintains its own
 * overlay table instance.
 */

#define PORTAL_BUCKET_SIZE  16
#define PORTAL_NUM_BUCKETS  256

/* =========================================================================
 * Node entry
 * ========================================================================= */

typedef struct {
    uint8_t  node_id[32];
    uint8_t  pubkey[33];        /* compressed secp256k1 pubkey */
    uint8_t  ip4[4];
    uint16_t udp_port;
    uint64_t last_seen;         /* monotonic counter */
    uint16_t checks;            /* successful liveness checks */
    uint8_t  data_radius[32];   /* U256 little-endian — storage radius */
} portal_node_t;

/* =========================================================================
 * K-bucket
 * ========================================================================= */

typedef struct {
    portal_node_t entries[PORTAL_BUCKET_SIZE];
    uint8_t count;
    portal_node_t replacements[PORTAL_BUCKET_SIZE];
    uint8_t repl_count;
} portal_bucket_t;

/* =========================================================================
 * Overlay table
 * ========================================================================= */

typedef struct {
    uint8_t local_id[32];
    uint8_t local_radius[32];   /* our announced data_radius (LE) */
    portal_bucket_t buckets[PORTAL_NUM_BUCKETS];
    uint64_t clock;
} portal_table_t;

/* Insert result codes */
typedef enum {
    PORTAL_TABLE_ADDED = 0,
    PORTAL_TABLE_UPDATED,
    PORTAL_TABLE_REPLACEMENT,
    PORTAL_TABLE_SELF,
    PORTAL_TABLE_FULL,
} portal_table_result_t;

/* =========================================================================
 * Core operations
 * ========================================================================= */

/** Initialize with local node ID and data radius. */
void portal_table_init(portal_table_t *t,
                       const uint8_t local_id[32],
                       const uint8_t local_radius[32]);

/** Compute log-distance bucket index. Returns 0..255, or -1 if same ID. */
int portal_table_bucket_index(const uint8_t local_id[32],
                              const uint8_t node_id[32]);

/** Insert or update a node. */
portal_table_result_t portal_table_insert(portal_table_t *t,
                                          const portal_node_t *node);

/** Remove a node. Returns true if found. */
bool portal_table_remove(portal_table_t *t, const uint8_t node_id[32]);

/** Find a node by ID. Returns pointer or NULL. */
const portal_node_t *portal_table_find(const portal_table_t *t,
                                       const uint8_t node_id[32]);

/** Mark node alive (successful contact). */
void portal_table_mark_alive(portal_table_t *t, const uint8_t node_id[32]);

/** Mark node dead. Promotes replacement if available. */
bool portal_table_mark_dead(portal_table_t *t, const uint8_t node_id[32]);

/** Get nodes at a specific log-distance. */
size_t portal_table_nodes_at_distance(const portal_table_t *t,
                                      int log_dist,
                                      portal_node_t *out);

/** Total number of nodes in all buckets. */
size_t portal_table_size(const portal_table_t *t);

/* =========================================================================
 * Portal-specific operations
 * ========================================================================= */

/** Update data_radius for an existing node (from PING/PONG). */
bool portal_table_update_radius(portal_table_t *t,
                                const uint8_t node_id[32],
                                const uint8_t radius[32]);

/** Set our own data_radius. */
void portal_table_set_radius(portal_table_t *t,
                             const uint8_t radius[32]);

/** Find closest nodes to target, sorted by XOR distance. */
size_t portal_table_closest(const portal_table_t *t,
                            const uint8_t target[32],
                            portal_node_t *out, size_t max_count);

/**
 * Find closest nodes to content_id, filtered by radius.
 * Only returns nodes where distance(node_id, content_id) <= node.data_radius.
 */
size_t portal_table_closest_to_content(const portal_table_t *t,
                                       const uint8_t content_id[32],
                                       portal_node_t *out, size_t max_count);

/** Check if content_id is within our local radius. */
bool portal_table_content_in_radius(const portal_table_t *t,
                                    const uint8_t content_id[32]);

/** Check if content_id is within a specific node's radius. */
bool portal_node_content_in_radius(const portal_node_t *node,
                                   const uint8_t content_id[32]);

/** Compute content_id = SHA-256(content_key). */
void portal_content_id(uint8_t out[32],
                       const uint8_t *content_key, size_t key_len);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_PORTAL_TABLE_H */
