#include "verkle_flat.h"
#include "pedersen.h"
#include "banderwagon.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define VF_VALUE_KEY_SIZE   32
#define VF_VALUE_RECORD_SIZE 32
#define VF_SLOT_KEY_SIZE    32
#define VF_SLOT_RECORD_SIZE 32
#define VF_INITIAL_CAP      4096
#define VF_STEM_LEN         31
#define VF_KEY_LEN          32
#define VF_VALUE_LEN        32
#define VF_WIDTH            256
#define VF_MAX_DEPTH        31    /* max internal depth (0..30) */

#define VF_STORE_LEAF     0
#define VF_STORE_INTERNAL 1
#define VF_STORE_SLOT     2

/* =========================================================================
 * Dynamic Array Helpers
 * ========================================================================= */

#define ENSURE_CAP(arr, count, cap, type) do {                     \
    if ((count) >= (cap)) {                                        \
        uint32_t _new_cap = (cap) ? (cap) * 2 : VF_INITIAL_CAP;   \
        type *_tmp = realloc((arr), _new_cap * sizeof(type));      \
        if (!_tmp) return false;                                   \
        (arr) = _tmp;                                              \
        (cap) = _new_cap;                                          \
    }                                                              \
} while(0)

/* =========================================================================
 * Key Builders
 * ========================================================================= */

static void make_leaf_cs_key(uint8_t key[32], const uint8_t stem[31]) {
    key[0] = 0x00;
    memcpy(key + 1, stem, 31);
}

static void make_internal_cs_key(uint8_t key[32], int depth,
                                  const uint8_t *path_prefix) {
    memset(key, 0, 32);
    key[0] = (uint8_t)(depth + 1);
    if (depth > 0 && path_prefix)
        memcpy(key + 1, path_prefix, depth);
}

/* Slot store key: [depth+1 || path[0..depth-1] || slot || zeros] */
static void make_slot_key(uint8_t key[32], int depth,
                           const uint8_t *path, uint8_t slot) {
    memset(key, 0, 32);
    key[0] = (uint8_t)(depth + 1);
    if (depth > 0 && path)
        memcpy(key + 1, path, depth);
    key[depth + 1] = slot;
}

/* =========================================================================
 * Undo Helpers
 * ========================================================================= */

static bool record_leaf_undo(verkle_flat_t *vf, const uint8_t stem[31],
                              bool existed,
                              const banderwagon_point_t *c1,
                              const banderwagon_point_t *c2,
                              const banderwagon_point_t *commit)
{
    ENSURE_CAP(vf->commit_undos, vf->cu_count, vf->cu_cap, vf_commit_undo_t);
    vf_commit_undo_t *u = &vf->commit_undos[vf->cu_count++];
    make_leaf_cs_key(u->cs_key, stem);
    u->store_id = VF_STORE_LEAF;
    if (existed) {
        banderwagon_serialize(u->old_data, c1);
        banderwagon_serialize(u->old_data + 32, c2);
        banderwagon_serialize(u->old_data + 64, commit);
        u->data_len = 96;
    } else {
        u->data_len = 0;
    }
    return true;
}

static bool record_internal_undo(verkle_flat_t *vf, int depth,
                                  const uint8_t *path_prefix,
                                  bool existed,
                                  const banderwagon_point_t *commit)
{
    ENSURE_CAP(vf->commit_undos, vf->cu_count, vf->cu_cap, vf_commit_undo_t);
    vf_commit_undo_t *u = &vf->commit_undos[vf->cu_count++];
    make_internal_cs_key(u->cs_key, depth, path_prefix);
    u->store_id = VF_STORE_INTERNAL;
    if (existed) {
        banderwagon_serialize(u->old_data, commit);
        u->data_len = 32;
    } else {
        u->data_len = 0;
    }
    return true;
}

static bool record_slot_undo(verkle_flat_t *vf, int depth,
                              const uint8_t *path, uint8_t slot,
                              bool existed, const uint8_t old_stem[31])
{
    ENSURE_CAP(vf->commit_undos, vf->cu_count, vf->cu_cap, vf_commit_undo_t);
    vf_commit_undo_t *u = &vf->commit_undos[vf->cu_count++];
    make_slot_key(u->cs_key, depth, path, slot);
    u->store_id = VF_STORE_SLOT;
    if (existed) {
        memset(u->old_data, 0, 32);
        memcpy(u->old_data, old_stem, 31);
        u->data_len = 32;
    } else {
        u->data_len = 0;
    }
    return true;
}

/* =========================================================================
 * Slot Store Helpers
 * ========================================================================= */

static bool slot_get(const verkle_flat_t *vf, int depth,
                      const uint8_t *path, uint8_t slot,
                      uint8_t out_stem[31])
{
    if (!vf->slot_store) return false;
    uint8_t key[32], val[32];
    make_slot_key(key, depth, path, slot);
    if (!art_store_get(vf->slot_store, key, val))
        return false;
    memcpy(out_stem, val, 31);
    return true;
}

static void slot_put(verkle_flat_t *vf, int depth,
                      const uint8_t *path, uint8_t slot,
                      const uint8_t stem[31])
{
    if (!vf->slot_store) return;
    uint8_t key[32], val[32];
    make_slot_key(key, depth, path, slot);
    memset(val, 0, 32);
    memcpy(val, stem, 31);
    art_store_put(vf->slot_store, key, val);
}

static void slot_delete(verkle_flat_t *vf, int depth,
                         const uint8_t *path, uint8_t slot)
{
    if (!vf->slot_store) return;
    uint8_t key[32];
    make_slot_key(key, depth, path, slot);
    art_store_delete(vf->slot_store, key);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

static verkle_flat_t *alloc_handle(void) {
    verkle_flat_t *vf = calloc(1, sizeof(verkle_flat_t));
    if (!vf) return NULL;
    pedersen_init();
    return vf;
}

static bool create_slot_store(verkle_flat_t *vf, const char *commit_dir)
{
    char slot_path[512];
    snprintf(slot_path, sizeof(slot_path), "%s/slots.art", commit_dir);
    vf->slot_store = art_store_create(slot_path, VF_SLOT_KEY_SIZE,
                                       VF_SLOT_RECORD_SIZE);
    return vf->slot_store != NULL;
}

static bool open_slot_store(verkle_flat_t *vf, const char *commit_dir)
{
    char slot_path[512];
    snprintf(slot_path, sizeof(slot_path), "%s/slots.art", commit_dir);
    /* If file doesn't exist yet, create it (migration from old format) */
    struct stat st;
    if (stat(slot_path, &st) != 0) {
        vf->slot_store = art_store_create(slot_path, VF_SLOT_KEY_SIZE,
                                           VF_SLOT_RECORD_SIZE);
    } else {
        vf->slot_store = art_store_open(slot_path);
    }
    return vf->slot_store != NULL;
}

verkle_flat_t *verkle_flat_create(const char *value_dir,
                                  const char *commit_dir)
{
    verkle_flat_t *vf = alloc_handle();
    if (!vf) return NULL;

    mkdir(value_dir, 0755);

    char value_path[512];
    snprintf(value_path, sizeof(value_path), "%s/values.art", value_dir);
    vf->value_store = art_store_create(value_path, VF_VALUE_KEY_SIZE,
                                        VF_VALUE_RECORD_SIZE);
    if (!vf->value_store) { free(vf); return NULL; }

    vf->commit_store = vcs_create(commit_dir);
    if (!vf->commit_store) {
        art_store_destroy(vf->value_store);
        free(vf);
        return NULL;
    }

    if (!create_slot_store(vf, commit_dir)) {
        vcs_destroy(vf->commit_store);
        art_store_destroy(vf->value_store);
        free(vf);
        return NULL;
    }

    return vf;
}

verkle_flat_t *verkle_flat_open(const char *value_dir,
                                const char *commit_dir)
{
    verkle_flat_t *vf = alloc_handle();
    if (!vf) return NULL;

    char value_path[512];
    snprintf(value_path, sizeof(value_path), "%s/values.art", value_dir);
    vf->value_store = art_store_open(value_path);
    if (!vf->value_store) { free(vf); return NULL; }

    vf->commit_store = vcs_open(commit_dir);
    if (!vf->commit_store) {
        art_store_destroy(vf->value_store);
        free(vf);
        return NULL;
    }

    if (!open_slot_store(vf, commit_dir)) {
        vcs_destroy(vf->commit_store);
        art_store_destroy(vf->value_store);
        free(vf);
        return NULL;
    }

    return vf;
}

void verkle_flat_destroy(verkle_flat_t *vf) {
    if (!vf) return;
    if (vf->value_store)  art_store_destroy(vf->value_store);
    if (vf->commit_store) vcs_destroy(vf->commit_store);
    if (vf->slot_store)   art_store_destroy(vf->slot_store);
    free(vf->changes);
    free(vf->undos);
    free(vf->commit_undos);
    free(vf->blocks);
    free(vf);
}

/* =========================================================================
 * Block Operations
 * ========================================================================= */

bool verkle_flat_begin_block(verkle_flat_t *vf, uint64_t block_number) {
    if (!vf || vf->block_active) return false;

    ENSURE_CAP(vf->blocks, vf->block_count, vf->block_cap, vf_block_t);
    vf_block_t *b = &vf->blocks[vf->block_count++];
    b->change_start      = vf->change_count;
    b->undo_start        = vf->undo_count;
    b->commit_undo_start = vf->cu_count;
    b->block_number      = block_number;
    vf->block_active     = true;
    return true;
}

bool verkle_flat_set(verkle_flat_t *vf,
                     const uint8_t key[32],
                     const uint8_t value[32])
{
    if (!vf || !vf->block_active) return false;

    /* Record undo: read current value */
    ENSURE_CAP(vf->undos, vf->undo_count, vf->undo_cap, vf_undo_t);
    vf_undo_t *undo = &vf->undos[vf->undo_count];
    memcpy(undo->key, key, 32);

    /* Check in-flight changes first (most recent wins) */
    uint32_t block_start = vf->blocks[vf->block_count - 1].change_start;
    bool found = false;
    for (uint32_t i = vf->change_count; i > block_start; i--) {
        if (memcmp(vf->changes[i - 1].key, key, 32) == 0) {
            memcpy(undo->old_value, vf->changes[i - 1].new_value, 32);
            found = true;
            break;
        }
    }
    if (!found) {
        if (art_store_get(vf->value_store, key, undo->old_value)) {
            found = true;
        }
    }
    undo->had_value = found;
    if (!found) memset(undo->old_value, 0, 32);
    vf->undo_count++;

    /* Buffer change */
    ENSURE_CAP(vf->changes, vf->change_count, vf->change_cap, vf_change_t);
    vf_change_t *ch = &vf->changes[vf->change_count++];
    memcpy(ch->key, key, 32);
    memcpy(ch->new_value, value, 32);
    return true;
}

bool verkle_flat_get(const verkle_flat_t *vf,
                     const uint8_t key[32],
                     uint8_t value[32])
{
    if (!vf) return false;

    /* Check in-flight changes (most recent wins) */
    for (uint32_t i = vf->change_count; i > 0; i--) {
        if (memcmp(vf->changes[i - 1].key, key, 32) == 0) {
            memcpy(value, vf->changes[i - 1].new_value, 32);
            return true;
        }
    }

    /* Fall through to value store */
    return art_store_get(vf->value_store, key, value);
}

/* =========================================================================
 * Commit Block — Core Algorithm
 * ========================================================================= */

/* --- Stem group for sorted changes --- */
typedef struct {
    uint8_t  stem[VF_STEM_LEN];
    uint16_t suffix_count;
    uint16_t suffixes[VF_WIDTH];     /* suffix values */
    uint32_t change_idx[VF_WIDTH];   /* index into changes[] (last write wins) */
} stem_group_t;

/* --- Propagation entry for bottom-up internal updates --- */
typedef struct {
    uint8_t path[VF_STEM_LEN];   /* first `depth` bytes meaningful */
    int     depth;
    uint8_t child_idx;
    banderwagon_point_t old_child;
    banderwagon_point_t new_child;
} prop_entry_t;

/* Compare changes by stem (first 31 bytes), then suffix (byte 31) */
static int cmp_changes(const void *a, const void *b) {
    return memcmp(((const vf_change_t *)a)->key,
                  ((const vf_change_t *)b)->key, 32);
}

/* Find attach depth: deepest internal node along this stem's path.
 * Returns 0 if no internals exist at all (root is depth 0). */
static int find_attach_depth(const verkle_flat_t *vf, const uint8_t stem[31]) {
    banderwagon_point_t dummy;
    int d = 0;
    while (d <= 30) {
        if (!vcs_get_internal(vf->commit_store, d, (d > 0) ? stem : NULL, &dummy))
            break;
        d++;
    }
    return d;
}

/* Find effective attach depth: handles gaps where split_leaf created deeper
 * internals within this block but propagation hasn't created the root yet.
 * Returns the depth of the parent internal for a new leaf at this stem. */
static int find_effective_attach_depth(const verkle_flat_t *vf,
                                        const uint8_t stem[31])
{
    int d = find_attach_depth(vf, stem);
    int attach = (d > 0) ? d - 1 : 0;

    /* Probe deeper: a prior split in this block may have created internals
     * beyond where find_attach_depth stopped (gap at root during block). */
    banderwagon_point_t dummy;
    int probe = attach + 1;
    while (probe <= 30 &&
           vcs_get_internal(vf->commit_store, probe,
                             (probe > 0) ? stem : NULL, &dummy))
        probe++;
    if (probe > attach + 1)
        attach = probe - 1;

    return attach;
}

/* =========================================================================
 * Leaf Splitting — Create intermediate internals for colliding stems
 * ========================================================================= */

/**
 * Split a leaf collision: two different stems share a slot at attach_depth.
 *
 * Creates intermediate internal nodes from attach_depth+1 to diverge_depth,
 * each containing only the existing leaf's commitment chain.
 *
 * Returns:
 *   out_diverge_depth — where the stems first differ
 *   out_chain_top     — commitment of new internal at attach_depth+1
 *   out_existing_commit — the existing leaf's commitment (for prop entry)
 */
static bool split_leaf(verkle_flat_t *vf,
                        const uint8_t existing_stem[31],
                        const uint8_t new_stem[31],
                        int attach_depth,
                        int *out_diverge_depth,
                        banderwagon_point_t *out_chain_top,
                        banderwagon_point_t *out_existing_commit)
{
    /* Find divergence depth */
    int div = attach_depth + 1;
    while (div < VF_STEM_LEN && existing_stem[div] == new_stem[div])
        div++;
    if (div >= VF_STEM_LEN) return false; /* stems fully match — shouldn't happen */

    /* Read existing leaf commitment */
    banderwagon_point_t c1, c2, leaf_commit;
    if (!vcs_get_leaf(vf->commit_store, existing_stem, &c1, &c2, &leaf_commit))
        return false;  /* existing leaf must exist */
    *out_existing_commit = leaf_commit;

    /* Delete old slot entry (existing leaf at attach_depth) */
    uint8_t old_occupant[31];
    bool had_slot = slot_get(vf, attach_depth,
                              (attach_depth > 0) ? new_stem : NULL,
                              new_stem[attach_depth], old_occupant);
    if (!record_slot_undo(vf, attach_depth,
                           (attach_depth > 0) ? new_stem : NULL,
                           new_stem[attach_depth], had_slot, old_occupant))
        return false;
    slot_delete(vf, attach_depth,
                (attach_depth > 0) ? new_stem : NULL,
                new_stem[attach_depth]);

    /* Build chain bottom-up: from div down to attach_depth + 1 */
    banderwagon_point_t child_commit = leaf_commit;

    for (int d = div; d >= attach_depth + 1; d--) {
        uint8_t child_slot = existing_stem[d];

        /* Create internal with single child */
        banderwagon_point_t internal = BANDERWAGON_IDENTITY;
        uint8_t field[32];
        banderwagon_map_to_field(field, &child_commit);
        pedersen_update(&internal, &BANDERWAGON_IDENTITY, child_slot, field);

        /* Record undo (didn't exist before) */
        if (!record_internal_undo(vf, d, (d > 0) ? new_stem : NULL, false, NULL))
            return false;

        /* Write to commit store */
        vcs_put_internal(vf->commit_store, d,
                          (d > 0) ? new_stem : NULL, &internal);

        child_commit = internal;
    }

    *out_chain_top = child_commit;
    *out_diverge_depth = div;

    /* Record slot entry for existing leaf at its new position (diverge_depth) */
    if (!record_slot_undo(vf, div, (div > 0) ? existing_stem : NULL,
                           existing_stem[div], false, NULL))
        return false;
    slot_put(vf, div, (div > 0) ? existing_stem : NULL,
             existing_stem[div], existing_stem);

    return true;
}

/* =========================================================================
 * Process Stem — Update leaf commitments
 * ========================================================================= */

static bool process_stem(verkle_flat_t *vf, const stem_group_t *sg,
                          banderwagon_point_t *out_old_leaf,
                          banderwagon_point_t *out_new_leaf)
{
    banderwagon_point_t c1, c2, leaf_commit;
    bool leaf_exists = vcs_get_leaf(vf->commit_store, sg->stem,
                                     &c1, &c2, &leaf_commit);

    /* Record leaf undo */
    if (!record_leaf_undo(vf, sg->stem, leaf_exists, &c1, &c2, &leaf_commit))
        return false;

    if (leaf_exists) {
        /* --- Case A: Existing leaf — incremental update --- */
        *out_old_leaf = leaf_commit;

        banderwagon_point_t old_c1 = c1, old_c2 = c2;
        bool c1_changed = false, c2_changed = false;

        for (int i = 0; i < sg->suffix_count; i++) {
            uint8_t suffix = (uint8_t)sg->suffixes[i];
            const uint8_t *new_value = vf->changes[sg->change_idx[i]].new_value;

            /* Load old value from value store */
            uint8_t old_value[32] = {0};
            uint8_t full_key[32];
            memcpy(full_key, sg->stem, 31);
            full_key[31] = suffix;
            bool had_old = art_store_get(vf->value_store, full_key, old_value);

            /* Split into lo/hi 16-byte halves with EIP-6800 leaf marker */
            uint8_t old_lo[32] = {0}, new_lo[32] = {0};
            uint8_t old_hi[32] = {0}, new_hi[32] = {0};
            memcpy(old_lo, old_value, 16);
            memcpy(new_lo, new_value, 16);
            new_lo[16] = 1;  /* EIP-6800 leaf marker */
            if (had_old) old_lo[16] = 1;  /* marker was present in old commitment */
            memcpy(old_hi, old_value + 16, 16);
            memcpy(new_hi, new_value + 16, 16);

            uint8_t delta_lo[32], delta_hi[32];
            pedersen_scalar_diff(delta_lo, new_lo, old_lo);
            pedersen_scalar_diff(delta_hi, new_hi, old_hi);

            /* Update C1 or C2 */
            if (suffix < 128) {
                int rel = suffix;
                pedersen_update(&c1, &c1, 2 * rel, delta_lo);
                pedersen_update(&c1, &c1, 2 * rel + 1, delta_hi);
                c1_changed = true;
            } else {
                int rel = suffix - 128;
                pedersen_update(&c2, &c2, 2 * rel, delta_lo);
                pedersen_update(&c2, &c2, 2 * rel + 1, delta_hi);
                c2_changed = true;
            }

            /* Write new value to value store */
            art_store_put(vf->value_store, full_key, new_value);
        }

        /* Update leaf commitment for C1/C2 changes (batch inversions) */
        if (c1_changed && c2_changed) {
            const banderwagon_point_t *pts[4] = {
                &old_c1, &c1, &old_c2, &c2 };
            uint8_t f[4][32];
            banderwagon_batch_map_to_field(f, pts, 4);
            uint8_t delta[32];
            pedersen_scalar_diff(delta, f[1], f[0]);
            pedersen_update(&leaf_commit, &leaf_commit, 2, delta);
            pedersen_scalar_diff(delta, f[3], f[2]);
            pedersen_update(&leaf_commit, &leaf_commit, 3, delta);
        } else if (c1_changed) {
            const banderwagon_point_t *pts[2] = { &old_c1, &c1 };
            uint8_t f[2][32];
            banderwagon_batch_map_to_field(f, pts, 2);
            uint8_t delta[32];
            pedersen_scalar_diff(delta, f[1], f[0]);
            pedersen_update(&leaf_commit, &leaf_commit, 2, delta);
        } else if (c2_changed) {
            const banderwagon_point_t *pts[2] = { &old_c2, &c2 };
            uint8_t f[2][32];
            banderwagon_batch_map_to_field(f, pts, 2);
            uint8_t delta[32];
            pedersen_scalar_diff(delta, f[1], f[0]);
            pedersen_update(&leaf_commit, &leaf_commit, 3, delta);
        }

        vcs_put_leaf(vf->commit_store, sg->stem, &c1, &c2, &leaf_commit);
        *out_new_leaf = leaf_commit;

    } else {
        /* --- Case B: New leaf — full MSM from scratch --- */
        *out_old_leaf = BANDERWAGON_IDENTITY;

        /* Build scalar arrays for C1 (values[0..127]) and C2 (values[128..255]) */
        uint8_t scalars_c1[256][32];
        uint8_t scalars_c2[256][32];
        memset(scalars_c1, 0, sizeof(scalars_c1));
        memset(scalars_c2, 0, sizeof(scalars_c2));

        for (int i = 0; i < sg->suffix_count; i++) {
            uint8_t suffix = (uint8_t)sg->suffixes[i];
            const uint8_t *val = vf->changes[sg->change_idx[i]].new_value;

            if (suffix < 128) {
                int rel = suffix;
                memcpy(scalars_c1[2 * rel], val, 16);
                scalars_c1[2 * rel][16] = 1;  /* EIP-6800 leaf marker */
                memcpy(scalars_c1[2 * rel + 1], val + 16, 16);
            } else {
                int rel = suffix - 128;
                memcpy(scalars_c2[2 * rel], val, 16);
                scalars_c2[2 * rel][16] = 1;  /* EIP-6800 leaf marker */
                memcpy(scalars_c2[2 * rel + 1], val + 16, 16);
            }

            /* Write value to store */
            uint8_t full_key[32];
            memcpy(full_key, sg->stem, 31);
            full_key[31] = suffix;
            art_store_put(vf->value_store, full_key, val);
        }

        /* Compute C1, C2 via full MSM */
        pedersen_commit(&c1, scalars_c1, 256);
        pedersen_commit(&c2, scalars_c2, 256);

        /* Compute leaf commitment: 1*G0 + stem*G1 + map(C1)*G2 + map(C2)*G3 */
        uint8_t leaf_scalars[4][32];
        memset(leaf_scalars, 0, sizeof(leaf_scalars));
        leaf_scalars[0][0] = 1;  /* marker */
        memcpy(leaf_scalars[1], sg->stem, VF_STEM_LEN);
        const banderwagon_point_t *cx_pts[2] = { &c1, &c2 };
        uint8_t cx_f[2][32];
        banderwagon_batch_map_to_field(cx_f, cx_pts, 2);
        memcpy(leaf_scalars[2], cx_f[0], 32);
        memcpy(leaf_scalars[3], cx_f[1], 32);
        pedersen_commit(&leaf_commit, leaf_scalars, 4);

        vcs_put_leaf(vf->commit_store, sg->stem, &c1, &c2, &leaf_commit);
        *out_new_leaf = leaf_commit;
    }

    return true;
}

/* =========================================================================
 * Bottom-Up Propagation
 * ========================================================================= */

static bool propagate_internals(verkle_flat_t *vf,
                                 prop_entry_t *entries, int num_entries)
{
    if (num_entries == 0) return true;

    /* Process from deepest to shallowest */
    while (num_entries > 0) {
        /* Find max depth */
        int max_depth = 0;
        for (int i = 0; i < num_entries; i++) {
            if (entries[i].depth > max_depth)
                max_depth = entries[i].depth;
        }

        /* Collect parent entries for next round */
        int parent_count = 0;
        prop_entry_t *parents = NULL;
        int parents_cap = 0;

        /* Process all entries at max_depth, grouped by path */
        int i = 0;
        while (i < num_entries) {
            if (entries[i].depth != max_depth) { i++; continue; }

            /* Collect all entries at this (depth, path) */
            int group_start = i;

            /* Load or create internal commitment */
            banderwagon_point_t internal_commit;
            bool existed = vcs_get_internal(vf->commit_store, max_depth,
                                             (max_depth > 0) ? entries[i].path : NULL,
                                             &internal_commit);
            if (!existed)
                internal_commit = BANDERWAGON_IDENTITY;

            /* Record undo */
            if (!record_internal_undo(vf, max_depth,
                                       (max_depth > 0) ? entries[i].path : NULL,
                                       existed, &internal_commit))
            {
                free(parents);
                return false;
            }

            banderwagon_point_t old_internal = internal_commit;

            /* Apply all deltas for this group */
            for (int j = i; j < num_entries; j++) {
                if (entries[j].depth != max_depth) continue;
                if (max_depth > 0 &&
                    memcmp(entries[j].path, entries[i].path, max_depth) != 0)
                    continue;

                uint8_t old_f[32], new_f[32], delta[32];
                const banderwagon_point_t *pair[2] = {
                    &entries[j].old_child, &entries[j].new_child };
                uint8_t pf[2][32];
                banderwagon_batch_map_to_field(pf, pair, 2);
                memcpy(old_f, pf[0], 32);
                memcpy(new_f, pf[1], 32);
                pedersen_scalar_diff(delta, new_f, old_f);
                pedersen_update(&internal_commit, &internal_commit,
                                entries[j].child_idx, delta);
            }

            vcs_put_internal(vf->commit_store, max_depth,
                              (max_depth > 0) ? entries[i].path : NULL,
                              &internal_commit);

            /* Add parent propagation entry if not at root */
            if (max_depth > 0) {
                if (parent_count >= parents_cap) {
                    int new_cap = parents_cap ? parents_cap * 2 : 64;
                    prop_entry_t *tmp = realloc(parents, new_cap * sizeof(prop_entry_t));
                    if (!tmp) { free(parents); return false; }
                    parents = tmp;
                    parents_cap = new_cap;
                }
                prop_entry_t *p = &parents[parent_count++];
                p->depth = max_depth - 1;
                if (max_depth > 1)
                    memcpy(p->path, entries[i].path, max_depth - 1);
                p->child_idx = entries[i].path[max_depth - 1];
                p->old_child = old_internal;
                p->new_child = internal_commit;
            }

            /* Mark processed entries (set depth to -1) */
            for (int j = i; j < num_entries; j++) {
                if (entries[j].depth != max_depth) continue;
                if (max_depth > 0 &&
                    memcmp(entries[j].path, entries[group_start].path, max_depth) != 0)
                    continue;
                entries[j].depth = -1;
            }
            i++;
        }

        /* Compact: remove processed entries, add parents */
        int new_count = 0;
        for (int j = 0; j < num_entries; j++) {
            if (entries[j].depth >= 0)
                entries[new_count++] = entries[j];
        }

        /* Merge parents into entries array */
        if (new_count + parent_count > 0) {
            prop_entry_t *merged = realloc(entries,
                (new_count + parent_count + 64) * sizeof(prop_entry_t));
            if (!merged) { free(parents); return false; }
            entries = merged;
            for (int j = 0; j < parent_count; j++)
                entries[new_count + j] = parents[j];
            num_entries = new_count + parent_count;
        } else {
            num_entries = 0;
        }

        free(parents);
    }

    free(entries);
    return true;
}

/* =========================================================================
 * Commit Block
 * ========================================================================= */

bool verkle_flat_commit_block(verkle_flat_t *vf) {
    if (!vf || !vf->block_active) return false;

    vf_block_t *blk = &vf->blocks[vf->block_count - 1];
    uint32_t start = blk->change_start;
    uint32_t count = vf->change_count - start;

    if (count == 0) {
        vf->block_active = false;
        return true;
    }

    /* Step 1: Sort changes by key (stem then suffix) */
    qsort(vf->changes + start, count, sizeof(vf_change_t), cmp_changes);

    /* Step 2: Build stem groups (deduplicate: last write per suffix wins) */
    stem_group_t *groups = NULL;
    int group_count = 0;
    int group_cap = 0;

    for (uint32_t i = start; i < vf->change_count; ) {
        const uint8_t *stem = vf->changes[i].key;

        if (group_count >= group_cap) {
            int new_cap = group_cap ? group_cap * 2 : 256;
            stem_group_t *tmp = realloc(groups, new_cap * sizeof(stem_group_t));
            if (!tmp) { free(groups); return false; }
            groups = tmp;
            group_cap = new_cap;
        }

        stem_group_t *sg = &groups[group_count++];
        memcpy(sg->stem, stem, VF_STEM_LEN);
        sg->suffix_count = 0;

        while (i < vf->change_count &&
               memcmp(vf->changes[i].key, stem, VF_STEM_LEN) == 0)
        {
            uint8_t suffix = vf->changes[i].key[31];

            bool dup = false;
            for (int j = 0; j < sg->suffix_count; j++) {
                if (sg->suffixes[j] == suffix) {
                    sg->change_idx[j] = i;
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                sg->suffixes[sg->suffix_count] = suffix;
                sg->change_idx[sg->suffix_count] = i;
                sg->suffix_count++;
            }
            i++;
        }
    }

    /* Step 3: Process each stem group → leaf updates + collision handling */
    /* Extra space for split propagation entries (2 per split) */
    int prop_cap = group_count * 2 + 64;
    prop_entry_t *prop = malloc(prop_cap * sizeof(prop_entry_t));
    if (!prop) { free(groups); return false; }
    int prop_count = 0;

    for (int g = 0; g < group_count; g++) {
        banderwagon_point_t old_leaf, new_leaf;

        /* Check if leaf already exists (Case A handled inside process_stem) */
        banderwagon_point_t dummy_c1, dummy_c2, dummy_lc;
        bool leaf_exists = vcs_get_leaf(vf->commit_store, groups[g].stem,
                                         &dummy_c1, &dummy_c2, &dummy_lc);

        /* Find effective attach depth (handles in-block split gaps) */
        int attach_depth = find_effective_attach_depth(vf, groups[g].stem);
        bool did_split = false;

        if (!leaf_exists) {
            /* New leaf — check for collision at attach_depth */
            uint8_t occupant_stem[31];
            if (slot_get(vf, attach_depth,
                         (attach_depth > 0) ? groups[g].stem : NULL,
                         groups[g].stem[attach_depth],
                         occupant_stem))
            {
                /* Collision! Different stem occupies this slot */
                int diverge_depth;
                banderwagon_point_t chain_top_commit, existing_leaf_commit;
                if (!split_leaf(vf, occupant_stem, groups[g].stem,
                                attach_depth, &diverge_depth,
                                &chain_top_commit, &existing_leaf_commit))
                {
                    free(groups); free(prop);
                    return false;
                }
                did_split = true;

                /* Grow prop array if needed */
                if (prop_count + 2 >= prop_cap) {
                    prop_cap = prop_cap * 2 + 64;
                    prop_entry_t *tmp = realloc(prop, prop_cap * sizeof(prop_entry_t));
                    if (!tmp) { free(groups); return false; }
                    prop = tmp;
                }

                /* Prop entry 1: at attach_depth, replace leaf with chain top */
                prop_entry_t *pe1 = &prop[prop_count++];
                pe1->depth = attach_depth;
                if (attach_depth > 0)
                    memcpy(pe1->path, groups[g].stem, attach_depth);
                pe1->child_idx = groups[g].stem[attach_depth];
                pe1->old_child = existing_leaf_commit;
                pe1->new_child = chain_top_commit;

                /* Update attach_depth for the new leaf */
                attach_depth = diverge_depth;
            }
        }

        /* Process the stem group (Case A or Case B) */
        if (!process_stem(vf, &groups[g], &old_leaf, &new_leaf)) {
            free(groups); free(prop);
            return false;
        }

        /* Record slot entry for new leaves */
        if (!leaf_exists) {
            uint8_t old_slot_stem[31];
            bool had = slot_get(vf, attach_depth,
                                 (attach_depth > 0) ? groups[g].stem : NULL,
                                 groups[g].stem[attach_depth],
                                 old_slot_stem);
            if (!record_slot_undo(vf, attach_depth,
                                   (attach_depth > 0) ? groups[g].stem : NULL,
                                   groups[g].stem[attach_depth],
                                   had, old_slot_stem))
            {
                free(groups); free(prop);
                return false;
            }
            slot_put(vf, attach_depth,
                     (attach_depth > 0) ? groups[g].stem : NULL,
                     groups[g].stem[attach_depth], groups[g].stem);
        }

        /* Grow prop array if needed */
        if (prop_count + 1 >= prop_cap) {
            prop_cap = prop_cap * 2 + 64;
            prop_entry_t *tmp = realloc(prop, prop_cap * sizeof(prop_entry_t));
            if (!tmp) { free(groups); return false; }
            prop = tmp;
        }

        /* Prop entry for this leaf */
        prop_entry_t *pe = &prop[prop_count++];
        pe->depth = attach_depth;
        if (attach_depth > 0)
            memcpy(pe->path, groups[g].stem, attach_depth);
        pe->child_idx = groups[g].stem[attach_depth];
        pe->old_child = did_split ? BANDERWAGON_IDENTITY : old_leaf;
        pe->new_child = new_leaf;
    }

    free(groups);

    /* Step 4: Bottom-up propagation */
    if (!propagate_internals(vf, prop, prop_count)) {
        return false;
    }

    vf->block_active = false;
    return true;
}

/* =========================================================================
 * Revert Block
 * ========================================================================= */

bool verkle_flat_revert_block(verkle_flat_t *vf) {
    if (!vf || vf->block_count == 0) return false;

    vf_block_t *blk = &vf->blocks[vf->block_count - 1];

    /* Restore values in reverse */
    for (uint32_t i = vf->undo_count; i > blk->undo_start; i--) {
        vf_undo_t *u = &vf->undos[i - 1];
        if (u->had_value) {
            art_store_put(vf->value_store, u->key, u->old_value);
        } else {
            art_store_delete(vf->value_store, u->key);
        }
    }

    /* Restore commitments + slots in reverse */
    for (uint32_t i = vf->cu_count; i > blk->commit_undo_start; i--) {
        vf_commit_undo_t *cu = &vf->commit_undos[i - 1];
        art_store_t *store;
        switch (cu->store_id) {
            case VF_STORE_LEAF:     store = vf->commit_store->leaf_store; break;
            case VF_STORE_INTERNAL: store = vf->commit_store->internal_store; break;
            case VF_STORE_SLOT:     store = vf->slot_store; break;
            default: continue;
        }
        if (cu->data_len == 0) {
            art_store_delete(store, cu->cs_key);
        } else {
            art_store_put(store, cu->cs_key, cu->old_data);
        }
    }

    /* Trim arrays */
    vf->change_count = blk->change_start;
    vf->undo_count   = blk->undo_start;
    vf->cu_count     = blk->commit_undo_start;
    vf->block_count--;
    vf->block_active = false;
    return true;
}

/* =========================================================================
 * Trim
 * ========================================================================= */

void verkle_flat_trim(verkle_flat_t *vf, uint64_t up_to_block) {
    if (!vf) return;

    uint32_t keep = 0;
    for (uint32_t i = 0; i < vf->block_count; i++) {
        if (vf->blocks[i].block_number > up_to_block) {
            keep = i;
            break;
        }
        if (i == vf->block_count - 1) {
            keep = vf->block_count;
        }
    }
    if (keep == 0) return;

    uint32_t ch_start = vf->blocks[keep - 1].change_start +
        (keep < vf->block_count ?
         vf->blocks[keep].change_start - vf->blocks[keep - 1].change_start : 0);
    uint32_t un_start, cu_start;

    if (keep < vf->block_count) {
        ch_start = vf->blocks[keep].change_start;
        un_start = vf->blocks[keep].undo_start;
        cu_start = vf->blocks[keep].commit_undo_start;
    } else {
        ch_start = vf->change_count;
        un_start = vf->undo_count;
        cu_start = vf->cu_count;
    }

    uint32_t ch_remain = vf->change_count - ch_start;
    if (ch_remain > 0) memmove(vf->changes, vf->changes + ch_start, ch_remain * sizeof(vf_change_t));
    vf->change_count = ch_remain;

    uint32_t un_remain = vf->undo_count - un_start;
    if (un_remain > 0) memmove(vf->undos, vf->undos + un_start, un_remain * sizeof(vf_undo_t));
    vf->undo_count = un_remain;

    uint32_t cu_remain = vf->cu_count - cu_start;
    if (cu_remain > 0) memmove(vf->commit_undos, vf->commit_undos + cu_start, cu_remain * sizeof(vf_commit_undo_t));
    vf->cu_count = cu_remain;

    uint32_t blk_remain = vf->block_count - keep;
    if (blk_remain > 0) {
        memmove(vf->blocks, vf->blocks + keep, blk_remain * sizeof(vf_block_t));
        for (uint32_t i = 0; i < blk_remain; i++) {
            vf->blocks[i].change_start      -= ch_start;
            vf->blocks[i].undo_start        -= un_start;
            vf->blocks[i].commit_undo_start -= cu_start;
        }
    }
    vf->block_count = blk_remain;
}

/* =========================================================================
 * Root
 * ========================================================================= */

void verkle_flat_root_hash(const verkle_flat_t *vf, uint8_t out[32]) {
    memset(out, 0, 32);
    if (!vf) return;

    banderwagon_point_t root;
    if (vcs_get_internal(vf->commit_store, 0, NULL, &root)) {
        banderwagon_serialize(out, &root);
    }
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void verkle_flat_sync(verkle_flat_t *vf) {
    if (!vf) return;
    art_store_sync(vf->value_store);
    vcs_sync(vf->commit_store);
    if (vf->slot_store) art_store_sync(vf->slot_store);
}
