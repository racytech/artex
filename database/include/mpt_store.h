#ifndef MPT_STORE_H
#define MPT_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * MPT Store — Persistent Merkle Patricia Trie with Incremental Updates.
 *
 * Disk-backed Ethereum MPT. Persists trie nodes across blocks so that
 * state root computation is O(dirty * depth) instead of O(total_accounts).
 *
 * Two files:
 *   <path>.idx — disk_hash index: node_hash[32] → {offset[8], length[4]}
 *   <path>.dat — slot-allocated flat file of RLP-encoded trie node data
 *
 * Slot allocation: nodes stored in size-class slots (64–1024 bytes).
 * Deleted slots go onto per-class free lists. New writes reuse free
 * slots before appending. Eliminates garbage accumulation.
 *
 * On each block:
 *   1. mpt_store_begin_batch()
 *   2. mpt_store_update() / mpt_store_delete() for each dirty key
 *   3. mpt_store_commit_batch() — walks dirty paths, writes new nodes,
 *      deletes stale nodes, returns new root hash
 *
 * Node reads: root_hash → disk_hash_get → {offset,len} → pread from .dat
 * Two I/O ops per node, both page-cache friendly.
 *
 * Default: 2 GB in-memory LRU cache for hot trie nodes.
 */

typedef struct mpt_store mpt_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new MPT store, creating/truncating files at <path>.idx/.dat.
 * capacity_hint: expected number of trie nodes (sizes the disk_hash).
 * The trie starts empty (root = keccak256(0x80)).
 * Returns NULL on failure.
 */
mpt_store_t *mpt_store_create(const char *path, uint64_t capacity_hint);

/**
 * Open an existing MPT store from <path>.idx and <path>.dat.
 * Reads the root hash from the .dat header.
 * Returns NULL on failure or corrupt files.
 */
mpt_store_t *mpt_store_open(const char *path);

/**
 * Close files, free all in-memory state. Does NOT remove files.
 */
void mpt_store_destroy(mpt_store_t *ms);

/**
 * Sync both index and data files to disk. Writes root hash to .dat header.
 */
void mpt_store_sync(mpt_store_t *ms);

/**
 * Flush deferred writes to disk. Writes all buffered nodes to .dat,
 * applies pending deletes, syncs both files.
 * Call at checkpoint time for transactional durability.
 */
void mpt_store_flush(mpt_store_t *ms);

/* =========================================================================
 * Trie Root
 * ========================================================================= */

/**
 * Get the current root hash. After commit_batch(), this reflects all
 * updates. After open(), this is the persisted root.
 */
void mpt_store_root(const mpt_store_t *ms, uint8_t out[32]);

/**
 * Set the root hash for subsequent batch operations.
 * Use this to switch between different tries stored in the same mpt_store
 * (e.g., per-account storage tries sharing a single node store).
 */
void mpt_store_set_root(mpt_store_t *ms, const uint8_t root[32]);

/**
 * Enable shared (multi-trie) mode. When true, commit_batch skips deletion
 * of old nodes — they may be referenced by other tries sharing this store.
 * Orphaned nodes are reclaimed by mpt_store_compact().
 */
void mpt_store_set_shared(mpt_store_t *ms, bool shared);

/* =========================================================================
 * Batch Update Interface
 * ========================================================================= */

/**
 * Begin a batch update. Only one batch may be active at a time.
 * Returns true on success.
 */
bool mpt_store_begin_batch(mpt_store_t *ms);

/**
 * Stage an insert or update in the current batch.
 * key:       32-byte trie key (keccak256 of address or storage slot).
 * value:     RLP-encoded leaf value (copied internally).
 * value_len: length of value in bytes.
 *
 * Does NOT modify the trie immediately. Changes are applied on commit.
 */
bool mpt_store_update(mpt_store_t *ms, const uint8_t key[32],
                      const uint8_t *value, size_t value_len);

/**
 * Stage a deletion in the current batch.
 * key: 32-byte trie key. If key does not exist, this is a no-op on commit.
 */
bool mpt_store_delete(mpt_store_t *ms, const uint8_t key[32]);

/**
 * Commit the current batch: apply all staged updates/deletes to the trie.
 *
 * Walks the trie from root, loading nodes from disk as needed.
 * Inserts/deletes leaves, restructures nodes, recomputes hashes bottom-up.
 * Writes new nodes to .dat, updates .idx, deletes stale node hashes.
 *
 * Returns true on success.
 */
bool mpt_store_commit_batch(mpt_store_t *ms);

/**
 * Discard the current batch without applying any changes.
 */
void mpt_store_discard_batch(mpt_store_t *ms);

/* =========================================================================
 * Point Lookup
 * ========================================================================= */

/**
 * Retrieve the value for a 32-byte key by walking the trie from root.
 * Returns actual value length. 0 = key not found or empty trie.
 * If buf_len < value length, returns required size, buf untouched.
 */
uint32_t mpt_store_get(const mpt_store_t *ms, const uint8_t key[32],
                        uint8_t *buf, uint32_t buf_len);

/* =========================================================================
 * Compaction
 * ========================================================================= */

/**
 * Compact the data file by rewriting it with only live nodes.
 * Walks all reachable nodes from root, writes them sequentially to a
 * new .dat file, rebuilds the index, then replaces the old files.
 * Returns true on success.
 */
bool mpt_store_compact(mpt_store_t *ms);

/**
 * Compact a shared-mode store given multiple live trie roots.
 * Walks all nodes reachable from any root, copies to a new store,
 * then swaps files. Reclaims all orphaned nodes from prior updates.
 * @param roots    Array of 32-byte root hashes
 * @param n_roots  Number of roots
 * Returns true on success.
 */
bool mpt_store_compact_roots(mpt_store_t *ms,
                              const uint8_t (*roots)[32], size_t n_roots);

/**
 * Walk all leaf nodes reachable from the current root.
 * Calls cb(value, value_len, user_data) for each leaf.
 * Returns true if the walk completed without error.
 */
typedef bool (*mpt_leaf_cb_t)(const uint8_t *value, size_t value_len,
                               void *user_data);
bool mpt_store_walk_leaves(const mpt_store_t *ms, mpt_leaf_cb_t cb,
                            void *user_data);

/* =========================================================================
 * Node Cache
 * ========================================================================= */

/**
 * Enable an in-memory LRU cache for hot trie nodes.
 * max_entries: number of cache slots (0 = disable cache).
 * Each entry uses ~1070 bytes, so 32K entries ≈ 34 MB.
 * Can be called at any time; replaces any existing cache.
 *
 * Note: create/open auto-enable a 2 GB cache by default.
 * Call with 0 to disable.
 */
void mpt_store_set_cache(mpt_store_t *ms, uint32_t max_entries);

/**
 * Set cache size in megabytes. Convenience wrapper over set_cache().
 * 0 = disable cache.
 */
void mpt_store_set_cache_mb(mpt_store_t *ms, uint32_t megabytes);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    uint64_t node_count;      /** Live nodes in the index */
    uint64_t data_file_size;  /** Total .dat file size in bytes */
    uint64_t live_data_bytes; /** Approximate bytes of live node data */
    uint64_t free_bytes;      /** Bytes on free lists (available for reuse) */
    uint64_t garbage_bytes;   /** Unreclaimable waste (padding in slots) */
    uint64_t cache_hits;      /** Cache hit count (0 if no cache) */
    uint64_t cache_misses;    /** Cache miss count (0 if no cache) */
    uint32_t cache_count;     /** Current entries in cache */
    uint32_t cache_capacity;  /** Max cache entries */
    uint64_t cache_evict_skipped; /** Times eviction skipped a pinned node */
    uint32_t cache_pinned;    /** Pinned entries (depth <= PIN_DEPTH) */
} mpt_store_stats_t;

/**
 * Get store statistics.
 */
mpt_store_stats_t mpt_store_stats(const mpt_store_t *ms);

#endif /* MPT_STORE_H */
