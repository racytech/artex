#include "verkle.h"
#include "pedersen.h"
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Node Allocation
 * ========================================================================= */

static verkle_node_t *new_internal(void) {
    verkle_node_t *n = calloc(1, sizeof(verkle_node_t));
    if (!n) return NULL;
    n->type = VERKLE_INTERNAL;
    /* children[] and commitment are zero-initialized by calloc */
    return n;
}

static verkle_node_t *new_leaf(const uint8_t stem[VERKLE_STEM_LEN]) {
    verkle_node_t *n = calloc(1, sizeof(verkle_node_t));
    if (!n) return NULL;
    n->type = VERKLE_LEAF;
    memcpy(n->leaf.stem, stem, VERKLE_STEM_LEN);
    /* values[], has_value[], c1, c2, commitment zero-initialized */
    return n;
}

static void free_node(verkle_node_t *n) {
    if (!n) return;
    if (n->type == VERKLE_INTERNAL) {
        for (int i = 0; i < VERKLE_WIDTH; i++)
            free_node(n->internal.children[i]);
    }
    free(n);
}

/* =========================================================================
 * Leaf Commitment Computation
 * ========================================================================= */

/**
 * Split 128 values into 256 scalars for Pedersen commitment.
 * Each 32-byte value splits into [lower 16 bytes][upper 16 bytes],
 * each zero-padded to 32-byte scalars.
 *
 * values: array of 128 values (32 bytes each)
 * has:    array of 128 booleans
 * out:    array of 256 scalars (32 bytes each), must be zeroed by caller
 */
static void split_values_to_scalars(uint8_t (*out)[32],
                                    const uint8_t (*values)[32],
                                    const bool *has,
                                    int count)
{
    for (int i = 0; i < count; i++) {
        if (!has[i]) continue;
        /* slot 2*i   = lower 16 bytes (bytes 0..15) as LE scalar */
        memcpy(out[2 * i], values[i], 16);
        /* EIP-6800 leaf marker: add 2^128 to mark value as present */
        out[2 * i][16] = 1;
        /* slot 2*i+1 = upper 16 bytes (bytes 16..31) as LE scalar */
        memcpy(out[2 * i + 1], values[i] + 16, 16);
    }
}

/**
 * Compute C1 commitment (values[0..127]).
 */
static void compute_c1(banderwagon_point_t *c1, const verkle_leaf_t *leaf) {
    uint8_t scalars[256][32];
    memset(scalars, 0, sizeof(scalars));
    split_values_to_scalars(scalars, leaf->values, leaf->has_value, 128);
    pedersen_commit(c1, scalars, 256);
}

/**
 * Compute C2 commitment (values[128..255]).
 */
static void compute_c2(banderwagon_point_t *c2, const verkle_leaf_t *leaf) {
    uint8_t scalars[256][32];
    memset(scalars, 0, sizeof(scalars));
    split_values_to_scalars(scalars,
                            leaf->values + 128,
                            leaf->has_value + 128,
                            128);
    pedersen_commit(c2, scalars, 256);
}

/**
 * Compute leaf commitment:
 *   C = 1*G_0 + stem*G_1 + map(C1)*G_2 + map(C2)*G_3
 */
static void compute_leaf_commitment(verkle_leaf_t *leaf) {
    compute_c1(&leaf->c1, leaf);
    compute_c2(&leaf->c2, leaf);

    uint8_t scalars[4][32];
    memset(scalars, 0, sizeof(scalars));

    /* slot 0: marker = 1 */
    scalars[0][0] = 1;

    /* slot 1: stem as 31-byte LE scalar (fits in 248 bits) */
    memcpy(scalars[1], leaf->stem, VERKLE_STEM_LEN);

    /* slot 2: map_to_field(C1) */
    banderwagon_map_to_field(scalars[2], &leaf->c1);

    /* slot 3: map_to_field(C2) */
    banderwagon_map_to_field(scalars[3], &leaf->c2);

    pedersen_commit(&leaf->commitment, scalars, 4);
}

/* =========================================================================
 * Internal Node Commitment Computation
 * ========================================================================= */

/**
 * Get the commitment pointer for any node type.
 */
static banderwagon_point_t *node_commitment(verkle_node_t *n) {
    if (n->type == VERKLE_INTERNAL)
        return &n->internal.commitment;
    else
        return &n->leaf.commitment;
}

/**
 * Compute internal node commitment:
 *   C = sum(map_to_field(child_commitment[i]) * G_i)
 */
static void compute_internal_commitment(verkle_internal_t *node) {
    uint8_t scalars[256][32];
    memset(scalars, 0, sizeof(scalars));

    for (int i = 0; i < VERKLE_WIDTH; i++) {
        if (node->children[i])
            banderwagon_map_to_field(scalars[i],
                                     node_commitment(node->children[i]));
    }

    pedersen_commit(&node->commitment, scalars, 256);
}

/* =========================================================================
 * Tree Lifecycle
 * ========================================================================= */

verkle_tree_t *verkle_create(void) {
    pedersen_init();
    verkle_tree_t *vt = calloc(1, sizeof(verkle_tree_t));
    return vt;
}

void verkle_destroy(verkle_tree_t *vt) {
    if (!vt) return;
    free_node(vt->root);
    free(vt);
}

/* =========================================================================
 * verkle_get
 * ========================================================================= */

bool verkle_get(const verkle_tree_t *vt,
                const uint8_t key[VERKLE_KEY_LEN],
                uint8_t value[VERKLE_VALUE_LEN])
{
    if (!vt || !vt->root) return false;

    const uint8_t *stem = key;
    uint8_t suffix = key[31];

    verkle_node_t *node = vt->root;
    int depth = 0;

    while (node) {
        if (node->type == VERKLE_LEAF) {
            if (memcmp(node->leaf.stem, stem, VERKLE_STEM_LEN) != 0)
                return false;
            if (!node->leaf.has_value[suffix])
                return false;
            memcpy(value, node->leaf.values[suffix], VERKLE_VALUE_LEN);
            return true;
        }

        /* Internal node: descend */
        node = node->internal.children[stem[depth]];
        depth++;
    }

    return false;
}

/* =========================================================================
 * Incremental Commitment Updates
 * ========================================================================= */

/**
 * Incrementally update a leaf's C1/C2 and leaf commitment after a single
 * value change. O(1) — two pedersen_update calls for C1/C2, one for leaf.
 *
 * Returns the old leaf commitment (needed for propagating to parent).
 */
static banderwagon_point_t incremental_update_leaf(
    verkle_leaf_t *leaf, uint8_t suffix,
    const uint8_t old_value[32], const uint8_t new_value[32],
    bool old_present, bool new_present)
{
    banderwagon_point_t old_leaf_commit = leaf->commitment;

    /* Determine which half: C1 (suffix < 128) or C2 (suffix >= 128) */
    banderwagon_point_t *cx = (suffix < 128) ? &leaf->c1 : &leaf->c2;
    banderwagon_point_t old_cx = *cx;
    int rel = (suffix < 128) ? suffix : suffix - 128;

    /* Compute deltas for lower and upper 16-byte halves */
    uint8_t old_lo[32] = {0}, new_lo[32] = {0};
    uint8_t old_hi[32] = {0}, new_hi[32] = {0};
    memcpy(old_lo, old_value, 16);
    memcpy(new_lo, new_value, 16);
    /* EIP-6800 leaf marker: 2^128 for present values */
    if (new_present) new_lo[16] = 1;
    if (old_present) old_lo[16] = 1;
    memcpy(old_hi, old_value + 16, 16);
    memcpy(new_hi, new_value + 16, 16);

    uint8_t delta_lo[32], delta_hi[32];
    pedersen_scalar_diff(delta_lo, new_lo, old_lo);
    pedersen_scalar_diff(delta_hi, new_hi, old_hi);

    /* Update C1 or C2 */
    pedersen_update(cx, cx, 2 * rel, delta_lo);
    pedersen_update(cx, cx, 2 * rel + 1, delta_hi);

    /* Update leaf commitment: slot 2 for C1, slot 3 for C2 */
    int cx_slot = (suffix < 128) ? 2 : 3;
    uint8_t old_cx_field[32], new_cx_field[32], delta_cx[32];
    banderwagon_map_to_field(old_cx_field, &old_cx);
    banderwagon_map_to_field(new_cx_field, cx);
    pedersen_scalar_diff(delta_cx, new_cx_field, old_cx_field);
    pedersen_update(&leaf->commitment, &leaf->commitment, cx_slot, delta_cx);

    return old_leaf_commit;
}

/**
 * Propagate commitment delta from a changed child up through internal
 * ancestors. O(1) per level — one pedersen_update call per internal node.
 */
static void incremental_propagate_internals(
    verkle_node_t **path, int *path_indices, int path_len,
    const banderwagon_point_t *old_child_commit,
    const banderwagon_point_t *new_child_commit)
{
    banderwagon_point_t old_c = *old_child_commit;
    banderwagon_point_t new_c = *new_child_commit;

    for (int i = path_len - 1; i >= 0; i--) {
        verkle_node_t *node = path[i];
        int child_idx = path_indices[i];

        uint8_t old_field[32], new_field[32], delta[32];
        banderwagon_map_to_field(old_field, &old_c);
        banderwagon_map_to_field(new_field, &new_c);
        pedersen_scalar_diff(delta, new_field, old_field);

        banderwagon_point_t old_node_commit = node->internal.commitment;
        pedersen_update(&node->internal.commitment,
                        &node->internal.commitment,
                        child_idx, delta);

        old_c = old_node_commit;
        new_c = node->internal.commitment;
    }
}

/**
 * Full recompute of commitments from leaf up to root.
 * Used for new nodes (splits) where there's no old commitment to delta from.
 */
static void recompute_commitments(verkle_node_t **path, int path_len) {
    for (int i = path_len - 1; i >= 0; i--) {
        verkle_node_t *n = path[i];
        if (n->type == VERKLE_LEAF) {
            compute_leaf_commitment(&n->leaf);
        } else {
            compute_internal_commitment(&n->internal);
        }
    }
}

bool verkle_set(verkle_tree_t *vt,
                const uint8_t key[VERKLE_KEY_LEN],
                const uint8_t value[VERKLE_VALUE_LEN])
{
    if (!vt) return false;

    const uint8_t *stem = key;
    uint8_t suffix = key[31];

    /* Track path for commitment updates */
    verkle_node_t *path[VERKLE_STEM_LEN + 1];
    int path_indices[VERKLE_STEM_LEN + 1];
    int path_len = 0;

    /* Empty tree: create a leaf as root */
    if (!vt->root) {
        vt->root = new_leaf(stem);
        if (!vt->root) return false;
        memcpy(vt->root->leaf.values[suffix], value, VERKLE_VALUE_LEN);
        vt->root->leaf.has_value[suffix] = true;
        compute_leaf_commitment(&vt->root->leaf);
        return true;
    }

    /* Walk the tree */
    verkle_node_t **slot = &vt->root;
    int depth = 0;

    while (*slot) {
        verkle_node_t *node = *slot;

        if (node->type == VERKLE_LEAF) {
            /* Same stem: update value in this leaf — INCREMENTAL */
            if (memcmp(node->leaf.stem, stem, VERKLE_STEM_LEN) == 0) {
                /* Save old value (zeros if suffix not yet set) */
                uint8_t old_value[32];
                memcpy(old_value, node->leaf.values[suffix], 32);
                bool had_value = node->leaf.has_value[suffix];

                /* Update value */
                memcpy(node->leaf.values[suffix], value, VERKLE_VALUE_LEN);
                node->leaf.has_value[suffix] = true;

                /* Incremental leaf update */
                banderwagon_point_t old_leaf_commit =
                    incremental_update_leaf(&node->leaf, suffix,
                                            old_value, value,
                                            had_value, true);

                /* Propagate up through internal ancestors */
                incremental_propagate_internals(
                    path, path_indices, path_len,
                    &old_leaf_commit, &node->leaf.commitment);
                return true;
            }

            /* Different stem: need to split — FULL RECOMPUTE for new nodes */
            verkle_node_t *existing_leaf = node;
            const uint8_t *existing_stem = existing_leaf->leaf.stem;

            /* Create internal nodes until stems diverge */
            while (depth < VERKLE_STEM_LEN &&
                   existing_stem[depth] == stem[depth]) {
                verkle_node_t *internal = new_internal();
                if (!internal) return false;
                *slot = internal;
                path[path_len] = internal;
                path_indices[path_len] = stem[depth];
                path_len++;
                slot = &internal->internal.children[stem[depth]];
                depth++;
            }

            /* At divergence point: create one more internal node */
            if (depth < VERKLE_STEM_LEN) {
                verkle_node_t *internal = new_internal();
                if (!internal) return false;
                *slot = internal;
                path[path_len] = internal;
                path_indices[path_len] = -1; /* not used for recompute */
                path_len++;

                /* Place existing leaf */
                internal->internal.children[existing_stem[depth]] = existing_leaf;

                /* Create new leaf */
                verkle_node_t *new_lf = new_leaf(stem);
                if (!new_lf) return false;
                memcpy(new_lf->leaf.values[suffix], value, VERKLE_VALUE_LEN);
                new_lf->leaf.has_value[suffix] = true;
                internal->internal.children[stem[depth]] = new_lf;

                /* Full recompute for new leaf and all new internals */
                compute_leaf_commitment(&new_lf->leaf);
                /* existing leaf commitment is already computed */
                recompute_commitments(path, path_len);
                return true;
            }

            /* Stems match fully (shouldn't happen) */
            return false;
        }

        /* Internal node: descend — track child index */
        path[path_len] = node;
        path_indices[path_len] = stem[depth];
        path_len++;
        slot = &node->internal.children[stem[depth]];
        depth++;
    }

    /* Reached a NULL child slot: create new leaf — INCREMENTAL for ancestors */
    verkle_node_t *new_lf = new_leaf(stem);
    if (!new_lf) return false;
    memcpy(new_lf->leaf.values[suffix], value, VERKLE_VALUE_LEN);
    new_lf->leaf.has_value[suffix] = true;
    *slot = new_lf;

    /* Full compute for the new leaf (no old commitment) */
    compute_leaf_commitment(&new_lf->leaf);

    /* Incremental propagation: old child was NULL (identity commitment) */
    banderwagon_point_t old_child = BANDERWAGON_IDENTITY;
    incremental_propagate_internals(
        path, path_indices, path_len,
        &old_child, &new_lf->leaf.commitment);
    return true;
}

/* =========================================================================
 * Unset (clear has_value for a suffix)
 * ========================================================================= */

bool verkle_unset(verkle_tree_t *vt, const uint8_t key[VERKLE_KEY_LEN])
{
    if (!vt || !vt->root) return false;

    const uint8_t *stem = key;
    uint8_t suffix = key[31];

    /* Track path for commitment propagation + structural changes */
    verkle_node_t *path[VERKLE_STEM_LEN + 1];
    int path_indices[VERKLE_STEM_LEN + 1];
    int path_len = 0;

    verkle_node_t **slot = &vt->root;
    int depth = 0;

    while (*slot) {
        verkle_node_t *node = *slot;

        if (node->type == VERKLE_LEAF) {
            if (memcmp(node->leaf.stem, stem, VERKLE_STEM_LEN) != 0)
                return false;
            if (!node->leaf.has_value[suffix])
                return false;

            /* Check if this is the last value in the leaf */
            bool is_last = true;
            for (int i = 0; i < VERKLE_WIDTH; i++) {
                if (i != suffix && node->leaf.has_value[i]) {
                    is_last = false;
                    break;
                }
            }

            if (!is_last) {
                /* Normal unset: clear value, incremental commitment update */
                uint8_t old_value[32];
                memcpy(old_value, node->leaf.values[suffix], 32);
                node->leaf.has_value[suffix] = false;
                memset(node->leaf.values[suffix], 0, 32);

                uint8_t zeros[32] = {0};
                banderwagon_point_t old_leaf_commit =
                    incremental_update_leaf(&node->leaf, suffix,
                                            old_value, zeros,
                                            true, false);
                incremental_propagate_internals(
                    path, path_indices, path_len,
                    &old_leaf_commit, &node->leaf.commitment);
                return true;
            }

            /* Last value: remove leaf and collapse tree structure */
            *slot = NULL;
            free(node);

            /* Walk up, collapsing empty/single-child internals */
            for (int i = path_len - 1; i >= 0; i--) {
                verkle_node_t *parent = path[i];
                int child_count = 0;
                int single_idx = -1;

                for (int c = 0; c < VERKLE_WIDTH; c++) {
                    if (parent->internal.children[c]) {
                        child_count++;
                        single_idx = c;
                    }
                }

                if (child_count == 0) {
                    /* Empty internal: remove from its parent */
                    if (i == 0)
                        vt->root = NULL;
                    else
                        path[i - 1]->internal.children[path_indices[i - 1]] = NULL;
                    free(parent);
                } else if (child_count == 1) {
                    /* Single child: collapse — replace this internal
                     * with the single remaining child */
                    verkle_node_t *child = parent->internal.children[single_idx];
                    if (i == 0)
                        vt->root = child;
                    else
                        path[i - 1]->internal.children[path_indices[i - 1]] = child;
                    parent->internal.children[single_idx] = NULL;
                    free(parent);
                } else {
                    /* 2+ children: recompute this node and all ancestors */
                    compute_internal_commitment(&parent->internal);
                    for (int k = i - 1; k >= 0; k--) {
                        compute_internal_commitment(&path[k]->internal);
                    }
                    return true;
                }
            }
            return true;
        }

        /* Internal node: descend */
        path[path_len] = node;
        path_indices[path_len] = stem[depth];
        path_len++;
        slot = &node->internal.children[stem[depth]];
        depth++;
    }

    return false;
}

/* =========================================================================
 * Commitment Queries
 * ========================================================================= */

void verkle_root_commitment(const verkle_tree_t *vt,
                            banderwagon_point_t *out)
{
    if (!vt || !vt->root) {
        banderwagon_init();
        *out = BANDERWAGON_IDENTITY;
        return;
    }
    *out = *node_commitment(vt->root);
}

void verkle_root_hash(const verkle_tree_t *vt,
                      uint8_t out[32])
{
    banderwagon_point_t root;
    verkle_root_commitment(vt, &root);

    if (banderwagon_is_identity(&root)) {
        memset(out, 0, 32);
    } else {
        banderwagon_serialize(out, &root);
    }
}
