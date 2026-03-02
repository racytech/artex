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
 * verkle_set
 * ========================================================================= */

/**
 * Recompute commitment for a node and all its ancestors.
 * path[] and path_indices[] track the walk from root to leaf.
 */
static void recompute_commitments(verkle_node_t **path, int path_len) {
    /* Recompute from leaf up to root */
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

    /* Track path for commitment recomputation */
    verkle_node_t *path[VERKLE_STEM_LEN + 1];
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
            /* Same stem: update value in this leaf */
            if (memcmp(node->leaf.stem, stem, VERKLE_STEM_LEN) == 0) {
                memcpy(node->leaf.values[suffix], value, VERKLE_VALUE_LEN);
                node->leaf.has_value[suffix] = true;
                path[path_len++] = node;
                recompute_commitments(path, path_len);
                return true;
            }

            /* Different stem: need to split */
            verkle_node_t *existing_leaf = node;
            const uint8_t *existing_stem = existing_leaf->leaf.stem;

            /* Create internal nodes until stems diverge */
            while (depth < VERKLE_STEM_LEN &&
                   existing_stem[depth] == stem[depth]) {
                verkle_node_t *internal = new_internal();
                if (!internal) return false;
                *slot = internal;
                path[path_len++] = internal;
                slot = &internal->internal.children[stem[depth]];
                depth++;
            }

            /* At divergence point: create one more internal node */
            if (depth < VERKLE_STEM_LEN) {
                verkle_node_t *internal = new_internal();
                if (!internal) return false;
                *slot = internal;
                path[path_len++] = internal;

                /* Place existing leaf */
                internal->internal.children[existing_stem[depth]] = existing_leaf;

                /* Create new leaf */
                verkle_node_t *new_lf = new_leaf(stem);
                if (!new_lf) return false;
                memcpy(new_lf->leaf.values[suffix], value, VERKLE_VALUE_LEN);
                new_lf->leaf.has_value[suffix] = true;
                internal->internal.children[stem[depth]] = new_lf;

                /* Recompute: new leaf, then existing leaf (unchanged but
                 * needs commitment if not yet computed), then ancestors */
                compute_leaf_commitment(&new_lf->leaf);
                /* existing leaf commitment is already computed */
                recompute_commitments(path, path_len);
                return true;
            }

            /* Stems match fully (shouldn't happen — same stem case handled above) */
            return false;
        }

        /* Internal node: descend */
        path[path_len++] = node;
        slot = &node->internal.children[stem[depth]];
        depth++;
    }

    /* Reached a NULL child slot: create new leaf */
    verkle_node_t *new_lf = new_leaf(stem);
    if (!new_lf) return false;
    memcpy(new_lf->leaf.values[suffix], value, VERKLE_VALUE_LEN);
    new_lf->leaf.has_value[suffix] = true;
    *slot = new_lf;

    compute_leaf_commitment(&new_lf->leaf);
    recompute_commitments(path, path_len);
    return true;
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
