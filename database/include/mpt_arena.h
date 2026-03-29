#ifndef MPT_ARENA_H
#define MPT_ARENA_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * MPT Arena — In-Memory Merkle Patricia Trie with Incremental Updates.
 *
 * All trie node RLP lives in a memory arena. No disk files, no mmap,
 * no slot headers, no free lists. compact_art index maps node_hash → arena
 * offset+length. Writes go directly to the arena and index — no deferred
 * buffer needed.
 *
 * Persistence is handled externally:
 *   - flat_state stores account/storage data (source of truth)
 *   - On restart, trie is rebuilt from flat_state or loaded from snapshot
 *
 * Shared mode: multiple tries (per-account storage) share one arena.
 * Refcounting tracks how many tries reference each node. Nodes are freed
 * when refcount reaches zero.
 *
 * On each checkpoint:
 *   1. mpt_arena_begin_batch()
 *   2. mpt_arena_update() / mpt_arena_delete() for each dirty key
 *   3. mpt_arena_commit_batch() — walks dirty paths, writes new nodes,
 *      deletes stale nodes, returns new root hash
 *
 * Node reads: compact_art lookup → memcpy from arena. Zero I/O.
 */

typedef struct mpt_arena mpt_arena_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new empty trie.
 * Root starts at EMPTY_ROOT (keccak256(0x80)).
 * Returns NULL on allocation failure.
 */
mpt_arena_t *mpt_arena_create(void);

/**
 * Free all memory. No files to clean up.
 */
void mpt_arena_destroy(mpt_arena_t *ma);

/**
 * Reset to empty state in-place. Clears arena, index, root.
 * Much faster than destroy + create.
 */
void mpt_arena_reset(mpt_arena_t *ma);

/* =========================================================================
 * Trie Root
 * ========================================================================= */

/**
 * Get current root hash.
 */
void mpt_arena_root(const mpt_arena_t *ma, uint8_t out[32]);

/**
 * Set root hash for subsequent batch operations.
 * Use to switch between per-account storage tries sharing one arena.
 */
void mpt_arena_set_root(mpt_arena_t *ma, const uint8_t root[32]);

/**
 * Enable shared (multi-trie) mode.
 * When true, nodes are refcounted. Deletion decrements refcount;
 * node is freed only when refcount reaches zero.
 */
void mpt_arena_set_shared(mpt_arena_t *ma, bool shared);

/* =========================================================================
 * Batch Update Interface
 * ========================================================================= */

/**
 * Begin a batch update. One batch at a time.
 */
bool mpt_arena_begin_batch(mpt_arena_t *ma);

/**
 * Stage an insert or update.
 * key:       32-byte trie key (keccak256 of address or storage slot).
 * value:     RLP-encoded leaf value (copied internally).
 * value_len: length in bytes.
 */
bool mpt_arena_update(mpt_arena_t *ma, const uint8_t key[32],
                       const uint8_t *value, size_t value_len);

/**
 * Stage a deletion.
 */
bool mpt_arena_delete(mpt_arena_t *ma, const uint8_t key[32]);

/**
 * Commit: apply all staged updates/deletes.
 * Walks trie, restructures nodes, recomputes hashes.
 * New nodes written to arena, stale nodes freed (or refcount decremented).
 * Returns true on success.
 */
bool mpt_arena_commit_batch(mpt_arena_t *ma);

/**
 * Discard current batch without applying.
 */
void mpt_arena_discard_batch(mpt_arena_t *ma);

/* =========================================================================
 * Point Lookup
 * ========================================================================= */

/**
 * Retrieve value for a 32-byte key by walking the trie.
 * Returns value length. 0 = not found.
 * If buf_len < value length, returns required size, buf untouched.
 */
uint32_t mpt_arena_get(const mpt_arena_t *ma, const uint8_t key[32],
                        uint8_t *buf, uint32_t buf_len);

/* =========================================================================
 * Snapshot (optional persistence)
 * ========================================================================= */

/**
 * Write arena + index state to a file for fast reload.
 * Format: header + raw arena data. Index is rebuilt from arena on load.
 * Returns true on success.
 */
bool mpt_arena_snapshot_write(const mpt_arena_t *ma, const char *path);

/**
 * Load arena from a snapshot file.
 * Rebuilds compact_art index by scanning arena nodes.
 * Returns NULL on failure.
 */
mpt_arena_t *mpt_arena_snapshot_load(const char *path);

/* =========================================================================
 * Leaf Walking
 * ========================================================================= */

/**
 * Walk all leaves reachable from current root.
 * Calls cb(value, value_len, user_data) for each leaf.
 */
typedef bool (*mpt_arena_leaf_cb_t)(const uint8_t *value, size_t value_len,
                                     void *user_data);
bool mpt_arena_walk_leaves(const mpt_arena_t *ma, mpt_arena_leaf_cb_t cb,
                            void *user_data);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    double   keccak_ns;
    double   load_ns;
    double   encode_ns;
    double   sort_ns;
    uint32_t nodes_hashed;
    uint32_t nodes_loaded;
    uint32_t check_hits;       /** Duplicate nodes found (refcount bump) */
    uint32_t deletes;
    uint32_t commits;
    uint32_t lost_nodes;
} mpt_arena_commit_stats_t;

void mpt_arena_reset_commit_stats(mpt_arena_t *ma);
mpt_arena_commit_stats_t mpt_arena_get_commit_stats(const mpt_arena_t *ma);

typedef struct {
    uint64_t node_count;       /** Live nodes in index */
    uint64_t arena_bytes;      /** Total arena memory used */
    uint64_t arena_capacity;   /** Total arena memory allocated */
} mpt_arena_stats_t;

mpt_arena_stats_t mpt_arena_stats(const mpt_arena_t *ma);

#endif /* MPT_ARENA_H */
