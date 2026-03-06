#ifndef VERKLE_SNAPSHOT_H
#define VERKLE_SNAPSHOT_H

#include "verkle.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Tree Snapshot — Binary Serialization/Deserialization
 *
 * Saves the entire tree (structure + values + commitments) to a binary file.
 * On load, reconstructs the tree with all commitments intact — no recomputation.
 *
 * Binary format:
 *   Header:   [8B "VRKLSNAP"][4B version][4B reserved]
 *   Leaf:     [0x01][31B stem][32B bitmap][N×32B values][32B C1][32B C2][32B commit]
 *   Internal: [0x02][32B child_mask][32B commit][recursive children]
 *
 * Values and children are sparse — only set entries are written.
 */

/** Save entire tree to a binary file. Returns false on I/O error. */
bool verkle_snapshot_save(const verkle_tree_t *vt, const char *filepath);

/** Load tree from a binary file. Returns NULL on error or corrupt data.
 *  The returned tree is fully usable (commitments preserved). */
verkle_tree_t *verkle_snapshot_load(const char *filepath);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_SNAPSHOT_H */
