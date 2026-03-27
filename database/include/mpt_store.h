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
 * Single file:
 *   <path>.dat — self-describing slot-allocated file of RLP-encoded trie nodes
 *
 * Each slot has a 4-byte header encoding size class, RLP length, and refcount.
 * Index: in-memory compact_art mapping node_hash(32B) → node_record_t(16B).
 * Rebuilt on open by scanning .dat slot headers + computing keccak256 per node.
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
 * Node reads: compact_art lookup (in-memory) → read RLP from mmap'd .dat.
 * Single I/O op per node, page-cache friendly.
 *
 */

typedef struct mpt_store mpt_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new MPT store, creating/truncating <path>.dat.
 * capacity_hint: unused (compact_art grows dynamically).
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
 * Reset store to empty state in-place without file recreation.
 * Clears index, data, caches, and pending buffers. Root returns to EMPTY_ROOT.
 * Much faster than destroy + create for repeated test use.
 */
void mpt_store_reset(mpt_store_t *ms);

/**
 * Sync both index and data files to disk. Writes root hash to .dat header.
 */
void mpt_store_sync(mpt_store_t *ms);

/**
 * Flush deferred writes to mmap (page cache) + compact_art index, then free
 * deferred buffers. No msync — OS handles writeback asynchronously.
 * Call at checkpoint time before evict_cache.
 */
void mpt_store_flush(mpt_store_t *ms);

/* =========================================================================
 * Prefetch
 * ========================================================================= */

/**
 * Hint the OS to prefetch a trie node's data pages into the page cache.
 * Non-blocking: returns immediately. The node data will be available
 * without a page fault when load_node_rlp reads it later.
 *
 * Use this to overlap I/O with computation — e.g., prefetch the next
 * account's storage root while committing the current account's batch.
 */
void mpt_store_prefetch(const mpt_store_t *ms, const uint8_t hash[32]);

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
 * Node Cache (legacy no-ops, retained for API compatibility)
 * ========================================================================= */

/**
 * Enable the LRU node cache with the given memory budget in bytes.
 * Keeps hot trie node RLP in memory to avoid .dat page faults.
 * Call after create/open, before any batch operations.
 * Typical values: 2GB for accounts, 8GB for storage.
 */
void mpt_store_set_cache(mpt_store_t *ms, uint64_t max_bytes);

/**
 * Verify trie hash integrity by walking all nodes and recomputing hashes.
 * Returns true if the recomputed root matches the stored root.
 * On mismatch, prints diagnostic info to stderr.
 */
bool mpt_store_verify_hashes(const mpt_store_t *ms);

/* =========================================================================
 * Stats
 * ========================================================================= */

/**
 * Commit-batch profiling counters. Accumulate across multiple commit_batch
 * calls (e.g. all storage root commits within one checkpoint).
 * Call mpt_store_reset_commit_stats() before a profiling window,
 * mpt_store_get_commit_stats() after.
 */
typedef struct {
    double   keccak_ns;       /** Time spent in keccak hashing */
    double   load_ns;         /** Time spent loading nodes (cache + disk) */
    double   check_ns;        /** Time spent on existence checks in write_node */
    double   delete_ns;       /** Time spent on delete_node / delete_ref */
    double   encode_ns;       /** Time spent encoding RLP (residual) */
    double   sort_ns;         /** Time spent sorting dirty entries */
    uint32_t nodes_hashed;    /** Nodes that required keccak (>=32 byte RLP) */
    uint32_t nodes_loaded;    /** Nodes loaded from cache or disk */
    uint32_t load_cache_hits; /** Node loads served from cache */
    uint32_t load_disk_reads; /** Node loads that hit disk (pread) */
    uint32_t check_hits;      /** Existence checks that found the node */
    uint32_t deletes;         /** Nodes deleted (delete_ref calls) */
    uint32_t commits;         /** Number of commit_batch calls */
} mpt_commit_stats_t;

void mpt_store_reset_commit_stats(mpt_store_t *ms);
mpt_commit_stats_t mpt_store_get_commit_stats(const mpt_store_t *ms);

typedef struct {
    uint64_t node_count;      /** Live nodes in the index */
    uint64_t data_file_size;  /** Total .dat file size in bytes */
    uint64_t live_data_bytes; /** Approximate bytes of live node data */
    uint64_t free_bytes;      /** Bytes on free lists (available for reuse) */
    uint64_t garbage_bytes;   /** Unreclaimable waste (padding in slots) */
} mpt_store_stats_t;

/**
 * Get store statistics.
 */
mpt_store_stats_t mpt_store_stats(const mpt_store_t *ms);

#endif /* MPT_STORE_H */
