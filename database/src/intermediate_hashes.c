#include "intermediate_hashes.h"
#include "compact_art.h"
#include "hash.h"
#include "rlp.h"
#include "bytes.h"
#include <stdlib.h>
#include <string.h>

/**
 * Intermediate Hash Table — MPT Commitment
 *
 * Stores branch node hashes in a compact_art (key=33, value=51).
 * Computes Ethereum MPT root hashes via recursive build over sorted keys.
 *
 * Node hashing uses manual RLP construction for correct inline embedding.
 * Extension merging handled naturally by find_branch_depth().
 *
 * Two modes:
 *   - ih_build: full trie computation from all sorted keys (initial sync)
 *   - ih_update: incremental per-block update from dirty keys + data cursor
 */

// ============================================================================
// Internal Types
// ============================================================================

// Reference to a child node: either a 32-byte hash or inline RLP (< 32 bytes)
typedef struct {
    uint8_t data[32];   // hash or inline RLP (left-justified)
    uint8_t len;        // 32 = hash, 1-31 = inline RLP, 0 = empty
} node_ref_t;

// Result from build_subtree: the ref + stored branch depth for child_depths tracking
// Uses +1 encoding: 0 = leaf (no branch stored), N = branch at nibble depth N-1
typedef struct {
    node_ref_t ref;
    size_t branch_depth;  // 0 = leaf, >0 = branch at depth (branch_depth - 1)
} subtree_result_t;

struct ih_state {
    compact_art_t tree;     // key=33, value=51
    hash_t root;            // cached root
    bool initialized;       // tree has been init'd
    size_t root_branch_depth; // +1 encoded: 0 = leaf/empty, N = branch at depth N-1
};

// ih_tree value layout:
//   [2 bytes bitmap] [1 byte ref_len] [32 bytes ref_data] [16 bytes child_depths]
// child_depths[i] = depth of child i's branch in ih_tree (0 = leaf/empty)
#define IH_KEY_SIZE   33
#define IH_VALUE_SIZE 51

// ============================================================================
// Nibble Helpers
// ============================================================================

static void key_to_nibbles(const uint8_t *key, size_t key_len,
                           uint8_t *nibbles) {
    for (size_t i = 0; i < key_len; i++) {
        nibbles[i * 2]     = (key[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] = key[i] & 0x0F;
    }
}

// ============================================================================
// Hex-Prefix Encoding (Yellow Paper Appendix C)
// ============================================================================

static bytes_t hex_prefix_encode(const uint8_t *nibbles, size_t count,
                                 bool is_leaf) {
    uint8_t flag = is_leaf ? 2 : 0;
    bool odd = (count % 2) != 0;
    size_t out_len = odd ? (count + 1) / 2 : count / 2 + 1;

    bytes_t result = bytes_with_capacity(out_len);
    bytes_resize(&result, out_len);

    if (odd) {
        result.data[0] = ((flag | 1) << 4) | nibbles[0];
        for (size_t i = 1; i < count; i += 2) {
            result.data[(i + 1) / 2] = (nibbles[i] << 4) | nibbles[i + 1];
        }
    } else {
        result.data[0] = (flag << 4);
        for (size_t i = 0; i < count; i += 2) {
            result.data[i / 2 + 1] = (nibbles[i] << 4) | nibbles[i + 1];
        }
    }

    return result;
}

// ============================================================================
// RLP List Wrapping
// ============================================================================

// Wrap payload bytes in an RLP list header.
static bytes_t rlp_wrap_list(const uint8_t *payload, size_t payload_len) {
    bytes_t result = bytes_new();

    if (payload_len <= 55) {
        bytes_push(&result, (uint8_t)(0xC0 + payload_len));
    } else {
        // Long list: count length bytes
        size_t len_bytes = 0;
        size_t temp = payload_len;
        while (temp > 0) { len_bytes++; temp >>= 8; }
        bytes_push(&result, (uint8_t)(0xF7 + len_bytes));
        for (int i = (int)len_bytes - 1; i >= 0; i--) {
            bytes_push(&result, (uint8_t)((payload_len >> (i * 8)) & 0xFF));
        }
    }

    bytes_append(&result, payload, payload_len);
    return result;
}

// ============================================================================
// Node Reference Construction
// ============================================================================

// Create a node_ref from RLP-encoded bytes.
// If rlp_len >= 32: keccak256 hash. If < 32: inline RLP.
// force_hash: always hash (for root node).
static node_ref_t make_node_ref(const uint8_t *rlp, size_t rlp_len,
                                bool force_hash) {
    node_ref_t ref = {0};

    if (rlp_len == 0) {
        ref.len = 0;
        return ref;
    }

    if (rlp_len >= 32 || force_hash) {
        hash_t h = hash_keccak256(rlp, rlp_len);
        memcpy(ref.data, h.bytes, 32);
        ref.len = 32;
    } else {
        memcpy(ref.data, rlp, rlp_len);
        ref.len = (uint8_t)rlp_len;
    }

    return ref;
}

// Encode a child ref into RLP payload bytes for embedding in a parent list.
static void append_child_ref(bytes_t *payload, const node_ref_t *ref) {
    if (ref->len == 0) {
        // Empty: RLP empty string
        bytes_push(payload, 0x80);
    } else if (ref->len == 32) {
        // Hash: RLP 32-byte string = 0xa0 + 32 bytes
        bytes_push(payload, 0xa0);
        bytes_append(payload, ref->data, 32);
    } else {
        // Inline: raw RLP bytes (already encoded)
        bytes_append(payload, ref->data, ref->len);
    }
}

// ============================================================================
// Node Hashing
// ============================================================================

// Leaf: RLP([hex_prefix(remaining_path, true), value])
static node_ref_t hash_leaf(const uint8_t *nibbles, size_t depth,
                            size_t total_nibbles,
                            const uint8_t *value, size_t value_len) {
    size_t remaining = total_nibbles - depth;
    bytes_t hp = hex_prefix_encode(nibbles + depth, remaining, true);

    // Build list payload manually
    bytes_t payload = bytes_new();

    // Element 1: hex-prefix encoded path
    bytes_t hp_rlp = rlp_encode_bytes(hp.data, hp.len);
    bytes_append(&payload, hp_rlp.data, hp_rlp.len);
    bytes_free(&hp_rlp);
    bytes_free(&hp);

    // Element 2: value
    bytes_t val_rlp = rlp_encode_bytes(value, value_len);
    bytes_append(&payload, val_rlp.data, val_rlp.len);
    bytes_free(&val_rlp);

    // Wrap in list
    bytes_t encoded = rlp_wrap_list(payload.data, payload.len);
    bytes_free(&payload);

    node_ref_t ref = make_node_ref(encoded.data, encoded.len, false);
    bytes_free(&encoded);
    return ref;
}

// Branch: RLP([ref_0, ..., ref_15, value_or_empty])
static node_ref_t hash_branch(const node_ref_t children[16],
                               const uint8_t *value, size_t value_len) {
    bytes_t payload = bytes_with_capacity(600);

    // 16 children
    for (int i = 0; i < 16; i++) {
        append_child_ref(&payload, &children[i]);
    }

    // 17th element: value or empty
    if (value && value_len > 0) {
        bytes_t val_rlp = rlp_encode_bytes(value, value_len);
        bytes_append(&payload, val_rlp.data, val_rlp.len);
        bytes_free(&val_rlp);
    } else {
        bytes_push(&payload, 0x80);
    }

    bytes_t encoded = rlp_wrap_list(payload.data, payload.len);
    bytes_free(&payload);

    node_ref_t ref = make_node_ref(encoded.data, encoded.len, false);
    bytes_free(&encoded);
    return ref;
}

// Extension: RLP([hex_prefix(path, false), child_ref])
static node_ref_t hash_extension(const uint8_t *nibbles, size_t count,
                                  const node_ref_t *child) {
    bytes_t hp = hex_prefix_encode(nibbles, count, false);

    bytes_t payload = bytes_new();

    bytes_t hp_rlp = rlp_encode_bytes(hp.data, hp.len);
    bytes_append(&payload, hp_rlp.data, hp_rlp.len);
    bytes_free(&hp_rlp);
    bytes_free(&hp);

    append_child_ref(&payload, child);

    bytes_t encoded = rlp_wrap_list(payload.data, payload.len);
    bytes_free(&payload);

    node_ref_t ref = make_node_ref(encoded.data, encoded.len, false);
    bytes_free(&encoded);
    return ref;
}

// ============================================================================
// ih_tree Helpers
// ============================================================================

// Build 33-byte key: [packed nibbles, zero-padded] [nibble_count]
// Packed-first layout ensures entries under a common nibble prefix are
// contiguous in sort order, enabling efficient prefix-delete operations.
static void make_ih_key(const uint8_t *nibbles, size_t count,
                        uint8_t out[IH_KEY_SIZE]) {
    memset(out, 0, IH_KEY_SIZE);
    for (size_t i = 0; i < count; i += 2) {
        uint8_t hi = nibbles[i];
        uint8_t lo = (i + 1 < count) ? nibbles[i + 1] : 0;
        out[i / 2] = (hi << 4) | lo;
    }
    out[32] = (uint8_t)count;
}

// Delete all ih_tree entries whose nibble prefix starts with `nibbles[0..count)`.
// With packed-first key layout, these entries are contiguous in sort order.
static void ih_delete_prefix(ih_state_t *ih,
                             const uint8_t *nibbles, size_t count) {
    if (!ih->initialized) return;

    // Build seek key: packed prefix, zero-padded, count=0 (sorts before all entries)
    uint8_t seek[IH_KEY_SIZE];
    memset(seek, 0, IH_KEY_SIZE);
    for (size_t i = 0; i < count; i += 2) {
        uint8_t hi = nibbles[i];
        uint8_t lo = (i + 1 < count) ? nibbles[i + 1] : 0;
        seek[i / 2] = (hi << 4) | lo;
    }
    // seek[32] = 0 → sorts before any real entry with this prefix

    compact_art_iterator_t *iter = compact_art_iterator_create(&ih->tree);
    if (!iter) return;

    if (!compact_art_iterator_seek(iter, seek)) {
        compact_art_iterator_destroy(iter);
        return;
    }

    // Number of full bytes in prefix
    size_t full_bytes = count / 2;
    bool odd = (count % 2) != 0;

    // Collect keys to delete (can't delete during iteration)
    size_t cap = 64, n = 0;
    uint8_t *del_keys = malloc(cap * IH_KEY_SIZE);
    if (!del_keys) {
        compact_art_iterator_destroy(iter);
        return;
    }

    while (!compact_art_iterator_done(iter)) {
        const uint8_t *k = compact_art_iterator_key(iter);
        if (!k) break;

        // Check full bytes match
        bool match = true;
        for (size_t i = 0; i < full_bytes && match; i++) {
            if (k[i] != seek[i]) match = false;
        }
        // Check odd nibble (high nibble of next byte)
        if (match && odd) {
            if ((k[full_bytes] & 0xF0) != (seek[full_bytes] & 0xF0))
                match = false;
        }

        if (!match) break;  // sorted order → no more matches

        // Entry's nibble count must be >= prefix count
        if (k[32] < count) {
            compact_art_iterator_next(iter);
            continue;
        }

        if (n >= cap) {
            cap *= 2;
            uint8_t *tmp = realloc(del_keys, cap * IH_KEY_SIZE);
            if (!tmp) break;
            del_keys = tmp;
        }
        memcpy(del_keys + n * IH_KEY_SIZE, k, IH_KEY_SIZE);
        n++;

        compact_art_iterator_next(iter);
    }

    compact_art_iterator_destroy(iter);

    for (size_t i = 0; i < n; i++) {
        compact_art_delete(&ih->tree, del_keys + i * IH_KEY_SIZE);
    }
    free(del_keys);
}

// Store a branch entry in ih_tree.
static void ih_store(ih_state_t *ih, const uint8_t *nibbles, size_t count,
                     uint16_t bitmap, const node_ref_t *branch_ref,
                     const uint8_t child_depths[16]) {
    uint8_t key[IH_KEY_SIZE];
    make_ih_key(nibbles, count, key);

    uint8_t value[IH_VALUE_SIZE];
    memcpy(value, &bitmap, 2);
    value[2] = branch_ref->len;
    memcpy(value + 3, branch_ref->data, 32);
    memcpy(value + 35, child_depths, 16);

    compact_art_insert(&ih->tree, key, value);
}

// Load a branch entry from ih_tree.
static bool ih_load(const ih_state_t *ih, const uint8_t *nibbles, size_t count,
                    uint16_t *out_bitmap, node_ref_t *out_ref,
                    uint8_t out_child_depths[16]) {
    uint8_t key[IH_KEY_SIZE];
    make_ih_key(nibbles, count, key);

    const void *val = compact_art_get(&ih->tree, key);
    if (!val) return false;

    const uint8_t *v = val;
    memcpy(out_bitmap, v, 2);
    if (out_ref) {
        out_ref->len = v[2];
        memcpy(out_ref->data, v + 3, 32);
    }
    if (out_child_depths) {
        memcpy(out_child_depths, v + 35, 16);
    }
    return true;
}

// ============================================================================
// Core: Recursive Build
// ============================================================================

// Find the depth where keys first diverge (shared prefix length).
// Compares nibbles[from] and nibbles[to-1] since keys are sorted.
static size_t find_branch_depth(const uint8_t *const *nibble_arrays,
                                const size_t *nibble_lens,
                                size_t from, size_t to,
                                size_t start_depth) {
    size_t depth = start_depth;
    // Min nibble length in the range — but since sorted, check first and last
    size_t min_len = nibble_lens[from] < nibble_lens[to - 1]
                     ? nibble_lens[from] : nibble_lens[to - 1];

    while (depth < min_len) {
        if (nibble_arrays[from][depth] != nibble_arrays[to - 1][depth])
            break;
        depth++;
    }
    return depth;
}

// Find the subrange of keys[from..to) where nibble at `depth` == `target`.
// Keys are sorted, so this is a contiguous range.
static void find_nibble_range(const uint8_t *const *nibble_arrays,
                              const size_t *nibble_lens,
                              size_t from, size_t to, size_t depth,
                              uint8_t target,
                              size_t *out_from, size_t *out_to) {
    // Find first key where nibble[depth] == target
    size_t lo = from;
    while (lo < to) {
        if (nibble_lens[lo] > depth && nibble_arrays[lo][depth] == target)
            break;
        // Keys that terminate before depth are sorted before keys with nibble[depth]
        if (nibble_lens[lo] <= depth) { lo++; continue; }
        if (nibble_arrays[lo][depth] < target) { lo++; continue; }
        break; // nibble > target, won't find it
    }

    if (lo >= to || nibble_lens[lo] <= depth || nibble_arrays[lo][depth] != target) {
        *out_from = *out_to = lo;
        return;
    }

    // Find last key where nibble[depth] == target
    size_t hi = lo;
    while (hi < to && nibble_lens[hi] > depth && nibble_arrays[hi][depth] == target)
        hi++;

    *out_from = lo;
    *out_to = hi;
}

static subtree_result_t build_subtree(ih_state_t *ih,
                                      const uint8_t *const *nibble_arrays,
                                      const size_t *nibble_lens,
                                      const uint8_t *const *values,
                                      const size_t *value_lens,
                                      size_t from, size_t to,
                                      size_t depth) {
    subtree_result_t empty = {{0}, 0};

    if (from >= to) return empty;

    // Single key → leaf
    if (to - from == 1) {
        subtree_result_t r = {
            hash_leaf(nibble_arrays[from], depth, nibble_lens[from],
                      values[from], value_lens[from]),
            0  // no branch stored for a single leaf
        };
        return r;
    }

    // Multiple keys: check for terminating key (nibble_len == depth)
    const uint8_t *branch_value = NULL;
    size_t branch_value_len = 0;
    size_t data_from = from;

    // A key that terminates at this depth becomes the branch value.
    // Since keys are sorted, a shorter key comes first.
    if (nibble_lens[from] == depth) {
        branch_value = values[from];
        branch_value_len = value_lens[from];
        data_from = from + 1;
    }

    // If only the terminating key remains, it's a leaf
    if (data_from >= to) {
        subtree_result_t r = {
            hash_leaf(nibble_arrays[from], depth, nibble_lens[from],
                      values[from], value_lens[from]),
            0
        };
        return r;
    }

    // Find where remaining keys diverge.
    size_t branch_depth;
    if (branch_value) {
        branch_depth = depth;
    } else {
        branch_depth = find_branch_depth(nibble_arrays, nibble_lens,
                                         data_from, to, depth);
    }


    // Partition by nibble at branch_depth
    node_ref_t children[16] = {{0}};
    uint16_t bitmap = 0;
    uint8_t child_depths[16] = {0};

    for (uint8_t n = 0; n < 16; n++) {
        size_t sub_from, sub_to;
        find_nibble_range(nibble_arrays, nibble_lens,
                          data_from, to, branch_depth, n,
                          &sub_from, &sub_to);
        if (sub_from < sub_to) {
            subtree_result_t cr = build_subtree(ih, nibble_arrays, nibble_lens,
                                                values, value_lens,
                                                sub_from, sub_to,
                                                branch_depth + 1);
            children[n] = cr.ref;
            child_depths[n] = (uint8_t)cr.branch_depth;
            bitmap |= (1 << n);
        }
    }

    // Compute branch ref
    node_ref_t branch_ref = hash_branch(children, branch_value, branch_value_len);

    // Store branch in ih_tree with child_depths
    ih_store(ih, nibble_arrays[data_from], branch_depth, bitmap,
             &branch_ref, child_depths);

    // Wrap with extension if shared prefix exists
    if (branch_depth > depth) {
        node_ref_t ext_ref = hash_extension(
            nibble_arrays[data_from] + depth, branch_depth - depth,
            &branch_ref);
        subtree_result_t r = {ext_ref, branch_depth + 1};
        return r;
    }

    subtree_result_t r = {branch_ref, branch_depth + 1};
    return r;
}

// ============================================================================
// Public API
// ============================================================================

ih_state_t *ih_create(void) {
    ih_state_t *ih = calloc(1, sizeof(ih_state_t));
    if (!ih) return NULL;
    ih->root = HASH_EMPTY_STORAGE;
    return ih;
}

void ih_destroy(ih_state_t *ih) {
    if (!ih) return;
    if (ih->initialized) {
        compact_art_destroy(&ih->tree);
    }
    free(ih);
}

hash_t ih_build(ih_state_t *ih,
                const uint8_t *const *keys,
                const uint8_t *const *values,
                const uint16_t *value_lens,
                size_t count) {
    if (!ih) return HASH_EMPTY_STORAGE;

    // Re-initialize ih_tree (clear previous state)
    if (ih->initialized) {
        compact_art_destroy(&ih->tree);
    }
    compact_art_init(&ih->tree, IH_KEY_SIZE, IH_VALUE_SIZE);
    ih->initialized = true;

    if (count == 0) {
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    // Convert all keys to nibbles
    uint8_t (*nibble_arrays)[64] = malloc(count * 64);
    size_t *nibble_lens = malloc(count * sizeof(size_t));
    const uint8_t **nibble_ptrs = malloc(count * sizeof(uint8_t *));
    // Cast value_lens from uint16_t to size_t
    size_t *val_lens = malloc(count * sizeof(size_t));

    if (!nibble_arrays || !nibble_lens || !nibble_ptrs || !val_lens) {
        free(nibble_arrays); free(nibble_lens);
        free(nibble_ptrs); free(val_lens);
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    for (size_t i = 0; i < count; i++) {
        key_to_nibbles(keys[i], 32, nibble_arrays[i]);
        nibble_lens[i] = 64;
        nibble_ptrs[i] = nibble_arrays[i];
        val_lens[i] = value_lens[i];
    }

    // Build recursively
    subtree_result_t root_result = build_subtree(ih,
                                        (const uint8_t *const *)nibble_ptrs,
                                        nibble_lens,
                                        values, val_lens,
                                        0, count, 0);
    node_ref_t root_ref = root_result.ref;
    ih->root_branch_depth = root_result.branch_depth;

    // Root is always hashed
    if (root_ref.len > 0 && root_ref.len < 32) {
        hash_t h = hash_keccak256(root_ref.data, root_ref.len);
        memcpy(ih->root.bytes, h.bytes, 32);
    } else if (root_ref.len == 32) {
        memcpy(ih->root.bytes, root_ref.data, 32);
    } else {
        ih->root = HASH_EMPTY_STORAGE;
    }

    free(nibble_arrays);
    free(nibble_lens);
    free(nibble_ptrs);
    free(val_lens);

    return ih->root;
}

hash_t ih_build_varlen(ih_state_t *ih,
                       const uint8_t *const *keys,
                       const size_t *key_lens,
                       const uint8_t *const *values,
                       const size_t *value_lens,
                       size_t count) {
    if (!ih) return HASH_EMPTY_STORAGE;

    // Re-initialize ih_tree
    if (ih->initialized) {
        compact_art_destroy(&ih->tree);
    }
    compact_art_init(&ih->tree, IH_KEY_SIZE, IH_VALUE_SIZE);
    ih->initialized = true;

    if (count == 0) {
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    // Find max nibble length
    size_t max_nibbles = 0;
    for (size_t i = 0; i < count; i++) {
        size_t nlen = key_lens[i] * 2;
        if (nlen > max_nibbles) max_nibbles = nlen;
    }

    // Convert all keys to nibbles
    uint8_t *nibble_storage = calloc(count, max_nibbles);
    size_t *nibble_lens = malloc(count * sizeof(size_t));
    const uint8_t **nibble_ptrs = malloc(count * sizeof(uint8_t *));

    if (!nibble_storage || !nibble_lens || !nibble_ptrs) {
        free(nibble_storage); free(nibble_lens); free(nibble_ptrs);
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    for (size_t i = 0; i < count; i++) {
        nibble_ptrs[i] = nibble_storage + i * max_nibbles;
        key_to_nibbles(keys[i], key_lens[i],
                       (uint8_t *)(nibble_storage + i * max_nibbles));
        nibble_lens[i] = key_lens[i] * 2;
    }

    // Build recursively
    subtree_result_t root_result = build_subtree(ih,
                                        nibble_ptrs, nibble_lens,
                                        values, value_lens,
                                        0, count, 0);
    node_ref_t root_ref = root_result.ref;
    ih->root_branch_depth = root_result.branch_depth;

    // Root is always hashed
    if (root_ref.len > 0 && root_ref.len < 32) {
        hash_t h = hash_keccak256(root_ref.data, root_ref.len);
        memcpy(ih->root.bytes, h.bytes, 32);
    } else if (root_ref.len == 32) {
        memcpy(ih->root.bytes, root_ref.data, 32);
    } else {
        ih->root = HASH_EMPTY_STORAGE;
    }

    free(nibble_storage);
    free(nibble_lens);
    free(nibble_ptrs);

    return ih->root;
}

// ============================================================================
// Incremental Update Helpers
// ============================================================================

// Convert nibble path to 32-byte key for cursor seeking.
// Pads remaining bytes with 0 → smallest key with this nibble prefix.
static void nibbles_to_key(const uint8_t *nibbles, size_t nibble_count,
                           uint8_t out_key[32]) {
    memset(out_key, 0, 32);
    size_t i;
    for (i = 0; i + 1 < nibble_count; i += 2) {
        out_key[i / 2] = (nibbles[i] << 4) | nibbles[i + 1];
    }
    if (nibble_count % 2 != 0) {
        out_key[nibble_count / 2] = nibbles[nibble_count - 1] << 4;
    }
}

// Check if a cursor key's nibbles match a prefix.
static bool nibbles_match_prefix(const uint8_t *key_nibbles, size_t key_nib_len,
                                 const uint8_t *prefix, size_t prefix_len) {
    if (key_nib_len < prefix_len) return false;
    for (size_t i = 0; i < prefix_len; i++) {
        if (key_nibbles[i] != prefix[i]) return false;
    }
    return true;
}

// Get node_ref for a clean (unchanged) child of a dirty branch.
// path[0..branch_depth-1] is the parent branch's nibble path.
// child_nibble is the slot (0-15).
// child_branch_depth: from parent's child_depths (0 = leaf, >0 = branch).
static subtree_result_t get_clean_child_ref(
        ih_state_t *ih,
        uint8_t *path,
        size_t branch_depth,
        uint8_t child_nibble,
        uint8_t child_branch_depth,
        ih_cursor_t *cursor) {
    subtree_result_t empty = {{{0}, 0}, 0};

    // Build child prefix: path[0..branch_depth-1] + child_nibble
    path[branch_depth] = child_nibble;
    size_t child_prefix_len = branch_depth + 1;

    // Seek cursor to first key under this prefix
    uint8_t seek_key[32];
    nibbles_to_key(path, child_prefix_len, seek_key);

    if (!cursor->seek(cursor->ctx, seek_key, 32))
        return empty;
    if (!cursor->valid(cursor->ctx))
        return empty;

    size_t klen, vlen;
    const uint8_t *k = cursor->key(cursor->ctx, &klen);
    const uint8_t *v = cursor->value(cursor->ctx, &vlen);
    if (!k) return empty;

    uint8_t knibbles[64];
    key_to_nibbles(k, klen, knibbles);

    // Verify cursor key matches our prefix
    if (!nibbles_match_prefix(knibbles, klen * 2, path, child_prefix_len))
        return empty;

    if (child_branch_depth == 0) {
        // Leaf child → hash directly from cursor data
        node_ref_t ref = hash_leaf(knibbles, child_prefix_len,
                                   klen * 2, v, vlen);
        subtree_result_t r = {ref, 0};
        return r;
    }

    // Branch child → load from ih_tree, wrap with extension if needed.
    // Decode +1 encoding: actual depth = child_branch_depth - 1
    size_t actual_depth = child_branch_depth - 1;

    // Use cursor key's nibbles to get the extension path.
    memcpy(path + child_prefix_len, knibbles + child_prefix_len,
           actual_depth - child_prefix_len);

    uint16_t bitmap;
    node_ref_t branch_ref;
    if (!ih_load(ih, knibbles, actual_depth, &bitmap, &branch_ref, NULL))
        return empty;

    if (actual_depth > child_prefix_len) {
        node_ref_t ext_ref = hash_extension(
            knibbles + child_prefix_len,
            actual_depth - child_prefix_len,
            &branch_ref);
        subtree_result_t r = {ext_ref, child_branch_depth};
        return r;
    }

    subtree_result_t r = {branch_ref, child_branch_depth};
    return r;
}

// Rebuild a subtree from cursor data (full rebuild of the prefix range).
// Collects ALL keys under path[0..path_len) from cursor, calls build_subtree.
// Used when structural changes occur (extension splits, leaf→branch, etc.).
static subtree_result_t rebuild_from_cursor(
        ih_state_t *ih,
        size_t depth,
        ih_cursor_t *cursor,
        const uint8_t *path,
        size_t path_len) {
    subtree_result_t empty = {{{0}, 0}, 0};

    // Delete all stale ih_tree entries under this prefix before rebuilding.
    // Without this, old branch entries from the previous structure accumulate
    // and corrupt subsequent hash computations.
    if (path_len > 0) {
        ih_delete_prefix(ih, path, path_len);
    } else {
        // Full rebuild from root — clear entire ih_tree
        compact_art_destroy(&ih->tree);
        compact_art_init(&ih->tree, IH_KEY_SIZE, IH_VALUE_SIZE);
    }

    uint8_t seek_key[32];
    nibbles_to_key(path, path_len, seek_key);

    if (!cursor->seek(cursor->ctx, seek_key, 32))
        return empty;

    // Collect all keys matching the prefix
    size_t cap = 64, count = 0;
    uint8_t *nibble_storage = malloc(cap * 64);
    size_t *nibble_lens = malloc(cap * sizeof(size_t));
    uint8_t *value_storage = NULL;
    size_t value_storage_cap = 0, value_storage_used = 0;
    size_t *value_offsets = malloc(cap * sizeof(size_t));
    size_t *value_lens = malloc(cap * sizeof(size_t));

    if (!nibble_storage || !nibble_lens || !value_offsets || !value_lens) {
        free(nibble_storage); free(nibble_lens);
        free(value_offsets); free(value_lens);
        free(value_storage);
        return empty;
    }

    while (cursor->valid(cursor->ctx)) {
        size_t klen, vlen;
        const uint8_t *k = cursor->key(cursor->ctx, &klen);
        const uint8_t *v = cursor->value(cursor->ctx, &vlen);
        if (!k) break;

        uint8_t knibs[64];
        key_to_nibbles(k, klen, knibs);

        if (!nibbles_match_prefix(knibs, klen * 2, path, path_len))
            break;

        // Grow arrays if needed
        if (count >= cap) {
            cap *= 2;
            nibble_storage = realloc(nibble_storage, cap * 64);
            nibble_lens = realloc(nibble_lens, cap * sizeof(size_t));
            value_offsets = realloc(value_offsets, cap * sizeof(size_t));
            value_lens = realloc(value_lens, cap * sizeof(size_t));
        }

        // Copy nibbles
        memcpy(nibble_storage + count * 64, knibs, 64);
        nibble_lens[count] = klen * 2;

        // Copy value into contiguous storage
        if (value_storage_used + vlen > value_storage_cap) {
            value_storage_cap = (value_storage_cap + vlen) * 2;
            value_storage = realloc(value_storage, value_storage_cap);
        }
        value_offsets[count] = value_storage_used;
        memcpy(value_storage + value_storage_used, v, vlen);
        value_storage_used += vlen;
        value_lens[count] = vlen;

        count++;
        cursor->next(cursor->ctx);
    }

    if (count == 0) {
        free(nibble_storage); free(nibble_lens);
        free(value_offsets); free(value_lens);
        free(value_storage);
        return empty;
    }

    // Build pointer arrays for build_subtree
    const uint8_t **nib_ptrs = malloc(count * sizeof(uint8_t *));
    const uint8_t **val_ptrs = malloc(count * sizeof(uint8_t *));
    for (size_t i = 0; i < count; i++) {
        nib_ptrs[i] = nibble_storage + i * 64;
        val_ptrs[i] = value_storage + value_offsets[i];
    }

    subtree_result_t result = build_subtree(ih,
        (const uint8_t *const *)nib_ptrs, nibble_lens,
        (const uint8_t *const *)val_ptrs, value_lens,
        0, count, depth);

    free(nib_ptrs);
    free(val_ptrs);
    free(nibble_storage);
    free(nibble_lens);
    free(value_offsets);
    free(value_lens);
    free(value_storage);

    return result;
}

// ============================================================================
// Core: Incremental Update (recursive)
// ============================================================================

static subtree_result_t update_subtree(
        ih_state_t *ih,
        const uint8_t *const *dirty_nibs,
        const size_t *dirty_nib_lens,
        const uint8_t *const *dirty_vals,
        const size_t *dirty_vlens,
        size_t from, size_t to,
        size_t depth,
        size_t stored_depth,    // +1 encoded: 0 = no branch, >0 = branch at (stored_depth-1)
        ih_cursor_t *cursor,
        uint8_t *path) {
    subtree_result_t empty = {{{0}, 0}, 0};

    if (from >= to) return empty;

    // Case 1: No existing branch — was leaf or empty → rebuild from cursor
    if (stored_depth == 0) {
        return rebuild_from_cursor(ih, depth, cursor, path, depth);
    }

    // Decode +1 encoding to get actual branch depth
    size_t branch_depth = stored_depth - 1;

    // Case 2: Extension check (branch_depth > depth)
    if (branch_depth > depth) {
        // Get extension nibbles from cursor
        uint8_t seek_key[32];
        nibbles_to_key(path, depth, seek_key);

        bool got_ext = false;
        if (cursor->seek(cursor->ctx, seek_key, 32) &&
            cursor->valid(cursor->ctx)) {
            size_t klen;
            const uint8_t *k = cursor->key(cursor->ctx, &klen);
            if (k) {
                uint8_t knibs[64];
                key_to_nibbles(k, klen, knibs);
                if (nibbles_match_prefix(knibs, klen * 2, path, depth)) {
                    memcpy(path + depth, knibs + depth, branch_depth - depth);
                    got_ext = true;
                }
            }
        }

        if (!got_ext) {
            // Can't find extension → rebuild
            return rebuild_from_cursor(ih, depth, cursor, path, depth);
        }

        // Check all dirty keys match the extension
        for (size_t i = from; i < to; i++) {
            for (size_t d = depth; d < branch_depth; d++) {
                if (dirty_nibs[i][d] != path[d]) {
                    // Extension split → rebuild entire subtree
                    return rebuild_from_cursor(ih, depth, cursor, path, depth);
                }
            }
        }
    }

    // Case 3: Load existing branch from ih_tree
    uint16_t old_bitmap = 0;
    node_ref_t old_ref;
    uint8_t old_child_depths[16] = {0};
    memset(&old_ref, 0, sizeof(old_ref));

    if (!ih_load(ih, path, branch_depth, &old_bitmap, &old_ref, old_child_depths)) {
        // Branch entry missing → rebuild from cursor
        return rebuild_from_cursor(ih, depth, cursor, path, depth);
    }

    // Case 4: Partition dirty keys and process each child
    node_ref_t children[16];
    memset(children, 0, sizeof(children));
    uint16_t new_bitmap = 0;
    uint8_t new_child_depths[16] = {0};

    for (uint8_t n = 0; n < 16; n++) {
        size_t sub_from, sub_to;
        find_nibble_range(dirty_nibs, dirty_nib_lens,
                          from, to, branch_depth, n,
                          &sub_from, &sub_to);

        bool is_dirty = (sub_from < sub_to);
        bool existed = (old_bitmap & (1 << n)) != 0;

        if (!is_dirty && !existed) {
            // Empty slot, stays empty
            continue;
        }

        if (!is_dirty && existed) {
            // Clean child → cached ref (child_depths are +1 encoded)
            subtree_result_t cr = get_clean_child_ref(
                ih, path, branch_depth, n,
                old_child_depths[n], cursor);
            children[n] = cr.ref;
            new_child_depths[n] = (uint8_t)cr.branch_depth;
            if (cr.ref.len > 0) new_bitmap |= (1 << n);
            continue;
        }

        // Dirty child
        path[branch_depth] = n;

        if (existed && old_child_depths[n] > 0) {
            // Had a branch → recurse (child_depths are +1 encoded)
            subtree_result_t cr = update_subtree(
                ih, dirty_nibs, dirty_nib_lens,
                dirty_vals, dirty_vlens,
                sub_from, sub_to,
                branch_depth + 1,
                old_child_depths[n],
                cursor, path);
            children[n] = cr.ref;
            new_child_depths[n] = (uint8_t)cr.branch_depth;
        } else {
            // Was leaf or empty → rebuild from cursor
            subtree_result_t cr = rebuild_from_cursor(
                ih, branch_depth + 1, cursor,
                path, branch_depth + 1);
            children[n] = cr.ref;
            new_child_depths[n] = (uint8_t)cr.branch_depth;
        }

        if (children[n].len > 0) {
            new_bitmap |= (1 << n);
        } else {
            new_child_depths[n] = 0;
        }
    }

    // Case 5: Check for structural collapse
    int popcount = 0;
    for (int i = 0; i < 16; i++) {
        if (new_bitmap & (1 << i)) popcount++;
    }

    if (popcount == 0) {
        // All children removed → subtree empty
        return empty;
    }

    if (popcount == 1) {
        // Single child → structural collapse needed
        // Fall back to rebuild from cursor for correct extension merging
        return rebuild_from_cursor(ih, depth, cursor, path, depth);
    }

    // Case 6: Re-hash branch
    node_ref_t branch_ref = hash_branch(children, NULL, 0);

    // Store updated branch in ih_tree (child_depths already +1 encoded)
    ih_store(ih, path, branch_depth, new_bitmap, &branch_ref, new_child_depths);

    // Wrap with extension if needed
    if (branch_depth > depth) {
        node_ref_t ext_ref = hash_extension(
            path + depth, branch_depth - depth, &branch_ref);
        subtree_result_t r = {ext_ref, branch_depth + 1};
        return r;
    }

    subtree_result_t r = {branch_ref, branch_depth + 1};
    return r;
}

// ============================================================================
// Public: Incremental Update
// ============================================================================

hash_t ih_update(ih_state_t *ih,
                 const uint8_t *const *dirty_keys,
                 const uint8_t *const *dirty_vals,
                 const size_t *dirty_vlens,
                 size_t count,
                 ih_cursor_t *cursor) {
    if (!ih || !ih->initialized) return HASH_EMPTY_STORAGE;
    if (count == 0) return ih->root;

    // Convert dirty keys to nibbles
    uint8_t (*nibble_arrays)[64] = malloc(count * 64);
    size_t *nibble_lens = malloc(count * sizeof(size_t));
    const uint8_t **nibble_ptrs = malloc(count * sizeof(uint8_t *));

    if (!nibble_arrays || !nibble_lens || !nibble_ptrs) {
        free(nibble_arrays); free(nibble_lens); free(nibble_ptrs);
        return ih->root;
    }

    for (size_t i = 0; i < count; i++) {
        key_to_nibbles(dirty_keys[i], 32, nibble_arrays[i]);
        nibble_lens[i] = 64;
        nibble_ptrs[i] = nibble_arrays[i];
    }

    // Working path buffer
    uint8_t path[65] = {0};

    // Recursive update from root
    subtree_result_t root_result = update_subtree(
        ih,
        (const uint8_t *const *)nibble_ptrs,
        nibble_lens,
        dirty_vals, dirty_vlens,
        0, count,
        0,
        ih->root_branch_depth,
        cursor, path);

    ih->root_branch_depth = root_result.branch_depth;
    node_ref_t root_ref = root_result.ref;

    // Root is always hashed
    if (root_ref.len > 0 && root_ref.len < 32) {
        hash_t h = hash_keccak256(root_ref.data, root_ref.len);
        memcpy(ih->root.bytes, h.bytes, 32);
    } else if (root_ref.len == 32) {
        memcpy(ih->root.bytes, root_ref.data, 32);
    } else {
        ih->root = HASH_EMPTY_STORAGE;
    }

    free(nibble_arrays);
    free(nibble_lens);
    free(nibble_ptrs);

    return ih->root;
}

hash_t ih_root(const ih_state_t *ih) {
    if (!ih) return HASH_EMPTY_STORAGE;
    return ih->root;
}


size_t ih_entry_count(const ih_state_t *ih) {
    if (!ih || !ih->initialized) return 0;
    return compact_art_size(&ih->tree);
}

