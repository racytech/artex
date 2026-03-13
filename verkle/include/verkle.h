#ifndef VERKLE_H
#define VERKLE_H

#include "banderwagon.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Tree — Width-256 Trie with Pedersen Commitments
 *
 * Keys are 32 bytes: [31-byte stem][1-byte suffix].
 * The stem routes through internal nodes (one byte per level).
 * The suffix selects a slot (0-255) within the leaf node.
 *
 * Each node carries a Pedersen commitment over the Banderwagon curve.
 * Updating a value propagates commitment changes O(depth) up to the root.
 */

#define VERKLE_STEM_LEN    31
#define VERKLE_KEY_LEN     32
#define VERKLE_VALUE_LEN   32
#define VERKLE_WIDTH       256

/* =========================================================================
 * Node Types
 * ========================================================================= */

typedef enum {
    VERKLE_INTERNAL,
    VERKLE_LEAF,
} verkle_node_type_t;

typedef struct verkle_node verkle_node_t;

/** Internal node: 256 child pointers + commitment. */
typedef struct {
    verkle_node_t *children[VERKLE_WIDTH];
    banderwagon_point_t commitment;
} verkle_internal_t;

/** Leaf node: stem + 256 value slots + C1/C2 + commitment. */
typedef struct {
    uint8_t stem[VERKLE_STEM_LEN];
    uint8_t values[VERKLE_WIDTH][VERKLE_VALUE_LEN];
    bool    has_value[VERKLE_WIDTH];
    banderwagon_point_t c1;          /* commitment to values[0..127]   */
    banderwagon_point_t c2;          /* commitment to values[128..255] */
    banderwagon_point_t commitment;  /* 1*G0 + stem*G1 + map(C1)*G2 + map(C2)*G3 */
} verkle_leaf_t;

/** Unified node (tagged union). */
struct verkle_node {
    verkle_node_type_t type;
    union {
        verkle_internal_t internal;
        verkle_leaf_t     leaf;
    };
};

/** Tree handle. */
typedef struct {
    verkle_node_t *root;
} verkle_tree_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create an empty verkle tree. */
verkle_tree_t *verkle_create(void);

/** Destroy tree and free all nodes. */
void verkle_destroy(verkle_tree_t *vt);

/* =========================================================================
 * Key-Value Operations
 * ========================================================================= */

/** Insert or update a key-value pair. Returns true on success. */
bool verkle_set(verkle_tree_t *vt,
                const uint8_t key[VERKLE_KEY_LEN],
                const uint8_t value[VERKLE_VALUE_LEN]);

/** Look up a key. Returns true if found, copies value to out. */
bool verkle_get(const verkle_tree_t *vt,
                const uint8_t key[VERKLE_KEY_LEN],
                uint8_t value[VERKLE_VALUE_LEN]);

/** Clear a key's value (set has_value to false).
 *  Returns true if the key existed and was cleared.
 *  Incrementally updates commitments. Does not remove nodes. */
bool verkle_unset(verkle_tree_t *vt,
                  const uint8_t key[VERKLE_KEY_LEN]);

/* =========================================================================
 * Commitment
 * ========================================================================= */

/** Get the root commitment (identity if tree is empty). */
void verkle_root_commitment(const verkle_tree_t *vt,
                            banderwagon_point_t *out);

/** Get the root hash (serialized root commitment, 32 bytes). */
void verkle_root_hash(const verkle_tree_t *vt,
                      uint8_t out[32]);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_H */
