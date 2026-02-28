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
 * Stores branch node hashes in a compact_art (key=33, value=34).
 * Computes Ethereum MPT root hashes via recursive build over sorted keys.
 *
 * Node hashing uses manual RLP construction for correct inline embedding.
 * Extension merging handled naturally by find_branch_depth().
 *
 * TODO: Phase 2 — ih_update() for incremental per-block updates.
 *   Currently ih_build() recomputes the entire trie from all keys (~17 min at
 *   500M keys). For normal block processing we need ih_update() that takes only
 *   dirty keys (~2K-50K per block) and reuses cached branch hashes from ih_tree.
 *   Requirements:
 *     - Prefix scan on compact_art (load existing children for a branch frame)
 *     - Stack-based streaming algorithm (see INTERMEDIATE_HASHES.md pseudocode)
 *     - Delete handling (NULL values remove leaves, may collapse branches)
 *     - Integration with data layer write buffer (sorted dirty keys after EVM)
 *   Expected performance: ~117ms per block vs ~17 min for full rebuild.
 */

// ============================================================================
// Internal Types
// ============================================================================

// Reference to a child node: either a 32-byte hash or inline RLP (< 32 bytes)
typedef struct {
    uint8_t data[32];   // hash or inline RLP (left-justified)
    uint8_t len;        // 32 = hash, 1-31 = inline RLP, 0 = empty
} node_ref_t;

struct ih_state {
    compact_art_t tree;     // key=33, value=34
    hash_t root;            // cached root
    bool initialized;       // tree has been init'd
};

// ih_tree value layout: [2 bytes bitmap] [32 bytes hash]
#define IH_KEY_SIZE   33
#define IH_VALUE_SIZE 34

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

// Build 33-byte key: [nibble_count] [packed nibbles, zero-padded]
static void make_ih_key(const uint8_t *nibbles, size_t count,
                        uint8_t out[IH_KEY_SIZE]) {
    memset(out, 0, IH_KEY_SIZE);
    out[0] = (uint8_t)count;
    for (size_t i = 0; i < count; i += 2) {
        uint8_t hi = nibbles[i];
        uint8_t lo = (i + 1 < count) ? nibbles[i + 1] : 0;
        out[1 + i / 2] = (hi << 4) | lo;
    }
}

// Store a branch entry in ih_tree.
static void ih_store(ih_state_t *ih, const uint8_t *nibbles, size_t count,
                     uint16_t bitmap, const uint8_t hash[32]) {
    uint8_t key[IH_KEY_SIZE];
    make_ih_key(nibbles, count, key);

    uint8_t value[IH_VALUE_SIZE];
    memcpy(value, &bitmap, 2);
    memcpy(value + 2, hash, 32);

    compact_art_insert(&ih->tree, key, value);
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

static node_ref_t build_subtree(ih_state_t *ih,
                                const uint8_t *const *nibble_arrays,
                                const size_t *nibble_lens,
                                const uint8_t *const *values,
                                const size_t *value_lens,
                                size_t from, size_t to,
                                size_t depth) {
    node_ref_t empty = {0};

    if (from >= to) return empty;

    // Single key → leaf
    if (to - from == 1) {
        return hash_leaf(nibble_arrays[from], depth, nibble_lens[from],
                         values[from], value_lens[from]);
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
        return hash_leaf(nibble_arrays[from], depth, nibble_lens[from],
                         values[from], value_lens[from]);
    }

    // Find where remaining keys diverge.
    // If we have a branch_value, the branch MUST be at `depth` because
    // the terminating key's value lives in this branch node's 17th slot.
    // We cannot push it deeper via an extension.
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

    for (uint8_t n = 0; n < 16; n++) {
        size_t sub_from, sub_to;
        find_nibble_range(nibble_arrays, nibble_lens,
                          data_from, to, branch_depth, n,
                          &sub_from, &sub_to);
        if (sub_from < sub_to) {
            children[n] = build_subtree(ih, nibble_arrays, nibble_lens,
                                        values, value_lens,
                                        sub_from, sub_to, branch_depth + 1);
            bitmap |= (1 << n);
        }
    }

    // Compute branch hash
    node_ref_t branch_ref = hash_branch(children, branch_value, branch_value_len);

    // Store branch in ih_tree (always store the keccak hash, even if inline)
    hash_t branch_hash;
    if (branch_ref.len == 32) {
        memcpy(branch_hash.bytes, branch_ref.data, 32);
    } else {
        // Inline branch (rare but possible for very small branches)
        branch_hash = hash_keccak256(branch_ref.data, branch_ref.len);
    }
    ih_store(ih, nibble_arrays[data_from], branch_depth, bitmap, branch_hash.bytes);

    // Wrap with extension if shared prefix exists
    if (branch_depth > depth) {
        node_ref_t ext_ref = hash_extension(
            nibble_arrays[data_from] + depth, branch_depth - depth,
            &branch_ref);
        return ext_ref;
    }

    return branch_ref;
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
    node_ref_t root_ref = build_subtree(ih,
                                        (const uint8_t *const *)nibble_ptrs,
                                        nibble_lens,
                                        values, val_lens,
                                        0, count, 0);

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
    node_ref_t root_ref = build_subtree(ih,
                                        nibble_ptrs, nibble_lens,
                                        values, value_lens,
                                        0, count, 0);

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

hash_t ih_root(const ih_state_t *ih) {
    if (!ih) return HASH_EMPTY_STORAGE;
    return ih->root;
}

size_t ih_entry_count(const ih_state_t *ih) {
    if (!ih || !ih->initialized) return 0;
    return compact_art_size(&ih->tree);
}
