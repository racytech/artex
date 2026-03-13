#include "verkle_snapshot.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

static const uint8_t SNAPSHOT_MAGIC[8] = {'V','R','K','L','S','N','A','P'};
static const uint32_t SNAPSHOT_VERSION = 1;

#define NODE_TYPE_LEAF     0x01
#define NODE_TYPE_INTERNAL 0x02

/* =========================================================================
 * Bitmap Helpers
 * ========================================================================= */

static void pack_bitmap(uint8_t out[32], const bool *flags, int count) {
    memset(out, 0, 32);
    for (int i = 0; i < count; i++)
        if (flags[i])
            out[i / 8] |= (1u << (i % 8));
}

static void unpack_bitmap(bool *flags, const uint8_t bitmap[32], int count) {
    for (int i = 0; i < count; i++)
        flags[i] = (bitmap[i / 8] >> (i % 8)) & 1;
}

/* Pack non-NULL children[256] into a 32-byte bitmask */
static void pack_child_mask(uint8_t out[32], verkle_node_t *const *children) {
    memset(out, 0, 32);
    for (int i = 0; i < VERKLE_WIDTH; i++)
        if (children[i])
            out[i / 8] |= (1u << (i % 8));
}

/* =========================================================================
 * Write Helpers
 * ========================================================================= */

static bool write_bytes(FILE *f, const void *data, size_t len) {
    return fwrite(data, 1, len, f) == len;
}

static bool write_point(FILE *f, const banderwagon_point_t *p) {
    uint8_t buf[32];
    banderwagon_serialize(buf, p);
    return write_bytes(f, buf, 32);
}

/* =========================================================================
 * Save — Recursive Tree Walk
 * ========================================================================= */

static bool write_node(FILE *f, const verkle_node_t *node) {
    if (node->type == VERKLE_LEAF) {
        uint8_t type = NODE_TYPE_LEAF;
        if (!write_bytes(f, &type, 1)) return false;
        if (!write_bytes(f, node->leaf.stem, VERKLE_STEM_LEN)) return false;

        /* has_value bitmap */
        uint8_t bitmap[32];
        pack_bitmap(bitmap, node->leaf.has_value, VERKLE_WIDTH);
        if (!write_bytes(f, bitmap, 32)) return false;

        /* Only write set values */
        for (int i = 0; i < VERKLE_WIDTH; i++) {
            if (node->leaf.has_value[i]) {
                if (!write_bytes(f, node->leaf.values[i], VERKLE_VALUE_LEN))
                    return false;
            }
        }

        /* Commitments: C1, C2, leaf commitment */
        if (!write_point(f, &node->leaf.c1)) return false;
        if (!write_point(f, &node->leaf.c2)) return false;
        if (!write_point(f, &node->leaf.commitment)) return false;

        return true;
    }

    /* Internal node */
    uint8_t type = NODE_TYPE_INTERNAL;
    if (!write_bytes(f, &type, 1)) return false;

    /* Child mask */
    uint8_t child_mask[32];
    pack_child_mask(child_mask, node->internal.children);
    if (!write_bytes(f, child_mask, 32)) return false;

    /* Commitment */
    if (!write_point(f, &node->internal.commitment)) return false;

    /* Recurse into non-NULL children in order */
    for (int i = 0; i < VERKLE_WIDTH; i++) {
        if (node->internal.children[i]) {
            if (!write_node(f, node->internal.children[i]))
                return false;
        }
    }

    return true;
}

bool verkle_snapshot_save(const verkle_tree_t *vt, const char *filepath) {
    if (!vt || !filepath) return false;

    FILE *f = fopen(filepath, "wb");
    if (!f) return false;

    /* Header */
    uint32_t reserved = 0;
    if (!write_bytes(f, SNAPSHOT_MAGIC, 8)) goto fail;
    if (!write_bytes(f, &SNAPSHOT_VERSION, 4)) goto fail;
    if (!write_bytes(f, &reserved, 4)) goto fail;

    /* Tree data (empty tree = header only) */
    if (vt->root) {
        if (!write_node(f, vt->root)) goto fail;
    }

    fclose(f);
    return true;

fail:
    fclose(f);
    return false;
}

/* =========================================================================
 * Read Helpers
 * ========================================================================= */

static bool read_bytes(FILE *f, void *out, size_t len) {
    return fread(out, 1, len, f) == len;
}

static bool read_point(FILE *f, banderwagon_point_t *p) {
    uint8_t buf[32];
    if (!read_bytes(f, buf, 32)) return false;
    return banderwagon_deserialize(p, buf);
}

/* =========================================================================
 * Load — Recursive Tree Reconstruction
 * ========================================================================= */

static verkle_node_t *read_node(FILE *f) {
    uint8_t type;
    if (!read_bytes(f, &type, 1)) return NULL;

    if (type == NODE_TYPE_LEAF) {
        verkle_node_t *n = calloc(1, sizeof(verkle_node_t));
        if (!n) return NULL;
        n->type = VERKLE_LEAF;

        /* Stem */
        if (!read_bytes(f, n->leaf.stem, VERKLE_STEM_LEN)) goto fail;

        /* has_value bitmap */
        uint8_t bitmap[32];
        if (!read_bytes(f, bitmap, 32)) goto fail;
        unpack_bitmap(n->leaf.has_value, bitmap, VERKLE_WIDTH);

        /* Read only set values */
        for (int i = 0; i < VERKLE_WIDTH; i++) {
            if (n->leaf.has_value[i]) {
                if (!read_bytes(f, n->leaf.values[i], VERKLE_VALUE_LEN))
                    goto fail;
            }
        }

        /* Commitments */
        if (!read_point(f, &n->leaf.c1)) goto fail;
        if (!read_point(f, &n->leaf.c2)) goto fail;
        if (!read_point(f, &n->leaf.commitment)) goto fail;

        return n;

    fail:
        free(n);
        return NULL;
    }

    if (type == NODE_TYPE_INTERNAL) {
        verkle_node_t *n = calloc(1, sizeof(verkle_node_t));
        if (!n) return NULL;
        n->type = VERKLE_INTERNAL;

        /* Child mask */
        uint8_t child_mask[32];
        if (!read_bytes(f, child_mask, 32)) { free(n); return NULL; }

        /* Commitment */
        if (!read_point(f, &n->internal.commitment)) { free(n); return NULL; }

        /* Read children for set bits */
        for (int i = 0; i < VERKLE_WIDTH; i++) {
            if ((child_mask[i / 8] >> (i % 8)) & 1) {
                n->internal.children[i] = read_node(f);
                if (!n->internal.children[i]) {
                    /* Cleanup already-loaded children */
                    for (int j = 0; j < i; j++) {
                        if (n->internal.children[j]) {
                            /* Simple leak for now — verkle_destroy handles deep free */
                        }
                    }
                    free(n);
                    return NULL;
                }
            }
        }

        return n;
    }

    /* Unknown type */
    return NULL;
}

verkle_tree_t *verkle_snapshot_load(const char *filepath) {
    if (!filepath) return NULL;

    FILE *f = fopen(filepath, "rb");
    if (!f) return NULL;

    /* Verify header */
    uint8_t magic[8];
    uint32_t version, reserved;

    if (!read_bytes(f, magic, 8)) { fclose(f); return NULL; }
    if (memcmp(magic, SNAPSHOT_MAGIC, 8) != 0) { fclose(f); return NULL; }

    if (!read_bytes(f, &version, 4)) { fclose(f); return NULL; }
    if (version != SNAPSHOT_VERSION) { fclose(f); return NULL; }

    if (!read_bytes(f, &reserved, 4)) { fclose(f); return NULL; }

    /* Create tree (initializes pedersen/banderwagon) */
    verkle_tree_t *vt = verkle_create();
    if (!vt) { fclose(f); return NULL; }

    /* Check if file has more data (non-empty tree) */
    int c = fgetc(f);
    if (c == EOF) {
        /* Empty tree */
        fclose(f);
        return vt;
    }
    ungetc(c, f);

    /* Read root node */
    vt->root = read_node(f);
    if (!vt->root) {
        /* read_node failed — could be corrupt data */
        fclose(f);
        verkle_destroy(vt);
        return NULL;
    }

    fclose(f);
    return vt;
}
