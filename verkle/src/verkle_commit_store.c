#include "verkle_commit_store.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define VCS_KEY_SIZE            32
#define VCS_LEAF_RECORD_SIZE    96   /* 3 × 32-byte serialized points */
#define VCS_INTERNAL_RECORD_SIZE 32  /* 1 × 32-byte serialized point */
#define VCS_LEAF_CAPACITY       (1 << 20)    /* ~1M initial */
#define VCS_INTERNAL_CAPACITY   (1 << 18)    /* ~256K initial */

/* =========================================================================
 * Key Encoding
 * ========================================================================= */

/**
 * Leaf key: [0x00 || stem[0..30]]
 */
static void make_leaf_key(uint8_t key[32], const uint8_t stem[31]) {
    key[0] = 0x00;
    memcpy(key + 1, stem, 31);
}

/**
 * Internal key: [depth+1 || path[0..depth-1] || zeros]
 * Root node (depth=0): [0x01 || 31 zeros]
 */
static void make_internal_key(uint8_t key[32], int depth,
                               const uint8_t *path_prefix) {
    memset(key, 0, 32);
    key[0] = (uint8_t)(depth + 1);
    if (depth > 0 && path_prefix)
        memcpy(key + 1, path_prefix, depth);
}

/* =========================================================================
 * Path Helpers
 * ========================================================================= */

static void make_leaf_path(char *buf, size_t buf_size, const char *dir) {
    snprintf(buf, buf_size, "%s/leaves.idx", dir);
}

static void make_internal_path(char *buf, size_t buf_size, const char *dir) {
    snprintf(buf, buf_size, "%s/internals.idx", dir);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

verkle_commit_store_t *vcs_create(const char *dir) {
    verkle_commit_store_t *cs = calloc(1, sizeof(verkle_commit_store_t));
    if (!cs) return NULL;

    mkdir(dir, 0755);

    char leaf_path[512], internal_path[512];
    make_leaf_path(leaf_path, sizeof(leaf_path), dir);
    make_internal_path(internal_path, sizeof(internal_path), dir);

    cs->leaf_store = disk_table_create(leaf_path, VCS_KEY_SIZE,
                                       VCS_LEAF_RECORD_SIZE,
                                       VCS_LEAF_CAPACITY);
    if (!cs->leaf_store) {
        free(cs);
        return NULL;
    }

    cs->internal_store = disk_table_create(internal_path, VCS_KEY_SIZE,
                                           VCS_INTERNAL_RECORD_SIZE,
                                           VCS_INTERNAL_CAPACITY);
    if (!cs->internal_store) {
        disk_table_destroy(cs->leaf_store);
        free(cs);
        return NULL;
    }

    return cs;
}

verkle_commit_store_t *vcs_open(const char *dir) {
    verkle_commit_store_t *cs = calloc(1, sizeof(verkle_commit_store_t));
    if (!cs) return NULL;

    char leaf_path[512], internal_path[512];
    make_leaf_path(leaf_path, sizeof(leaf_path), dir);
    make_internal_path(internal_path, sizeof(internal_path), dir);

    cs->leaf_store = disk_table_open(leaf_path);
    if (!cs->leaf_store) {
        free(cs);
        return NULL;
    }

    cs->internal_store = disk_table_open(internal_path);
    if (!cs->internal_store) {
        disk_table_destroy(cs->leaf_store);
        free(cs);
        return NULL;
    }

    return cs;
}

void vcs_destroy(verkle_commit_store_t *cs) {
    if (!cs) return;
    if (cs->leaf_store) disk_table_destroy(cs->leaf_store);
    if (cs->internal_store) disk_table_destroy(cs->internal_store);
    free(cs);
}

/* =========================================================================
 * Leaf Commitments
 * ========================================================================= */

bool vcs_put_leaf(verkle_commit_store_t *cs,
                  const uint8_t stem[31],
                  const banderwagon_point_t *c1,
                  const banderwagon_point_t *c2,
                  const banderwagon_point_t *commitment)
{
    uint8_t key[32];
    make_leaf_key(key, stem);

    uint8_t buf[VCS_LEAF_RECORD_SIZE];
    banderwagon_serialize(buf,      c1);
    banderwagon_serialize(buf + 32, c2);
    banderwagon_serialize(buf + 64, commitment);

    return disk_table_put(cs->leaf_store, key, buf);
}

bool vcs_get_leaf(const verkle_commit_store_t *cs,
                  const uint8_t stem[31],
                  banderwagon_point_t *c1,
                  banderwagon_point_t *c2,
                  banderwagon_point_t *commitment)
{
    uint8_t key[32];
    make_leaf_key(key, stem);

    uint8_t buf[VCS_LEAF_RECORD_SIZE];
    if (!disk_table_get(cs->leaf_store, key, buf))
        return false;

    if (!banderwagon_deserialize(c1, buf))
        return false;
    if (!banderwagon_deserialize(c2, buf + 32))
        return false;
    if (!banderwagon_deserialize(commitment, buf + 64))
        return false;

    return true;
}

/* =========================================================================
 * Internal Node Commitments
 * ========================================================================= */

bool vcs_put_internal(verkle_commit_store_t *cs,
                      int depth,
                      const uint8_t *path_prefix,
                      const banderwagon_point_t *commitment)
{
    uint8_t key[32];
    make_internal_key(key, depth, path_prefix);

    uint8_t buf[VCS_INTERNAL_RECORD_SIZE];
    banderwagon_serialize(buf, commitment);

    return disk_table_put(cs->internal_store, key, buf);
}

bool vcs_get_internal(const verkle_commit_store_t *cs,
                      int depth,
                      const uint8_t *path_prefix,
                      banderwagon_point_t *commitment)
{
    uint8_t key[32];
    make_internal_key(key, depth, path_prefix);

    uint8_t buf[VCS_INTERNAL_RECORD_SIZE];
    if (!disk_table_get(cs->internal_store, key, buf))
        return false;

    return banderwagon_deserialize(commitment, buf);
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void vcs_sync(verkle_commit_store_t *cs) {
    if (!cs) return;
    if (cs->leaf_store) disk_table_sync(cs->leaf_store);
    if (cs->internal_store) disk_table_sync(cs->internal_store);
}
