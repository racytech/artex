#include "flat_index.h"
#include <stdlib.h>
#include <string.h>

/* Tag sentinels */
#define TAG_EMPTY     0
#define TAG_TOMBSTONE UINT64_MAX

static inline uint64_t make_tag(const uint8_t *key) {
    uint64_t t;
    memcpy(&t, key + 8, 8);
    if (t == TAG_EMPTY) t = 1;
    if (t == TAG_TOMBSTONE) t = TAG_TOMBSTONE - 1;
    return t;
}

static inline uint64_t make_bucket(const uint8_t *key, uint32_t mask) {
    uint64_t h;
    memcpy(&h, key, 8);
    return h & mask;
}

bool flat_index_init(flat_index_t *idx, uint32_t capacity) {
    if (capacity < FLAT_INDEX_INITIAL_CAP) capacity = FLAT_INDEX_INITIAL_CAP;
    /* Round up to power of 2 */
    uint32_t cap = 1;
    while (cap < capacity) cap <<= 1;

    idx->tags = calloc(cap, sizeof(uint64_t));
    idx->offsets = calloc(cap, sizeof(uint32_t));
    if (!idx->tags || !idx->offsets) {
        free(idx->tags); free(idx->offsets);
        idx->tags = NULL; idx->offsets = NULL;
        return false;
    }
    idx->capacity = cap;
    idx->mask = cap - 1;
    idx->count = 0;
    return true;
}

void flat_index_destroy(flat_index_t *idx) {
    if (!idx) return;
    free(idx->tags);
    free(idx->offsets);
    memset(idx, 0, sizeof(*idx));
}

const uint32_t *flat_index_get(const flat_index_t *idx, const uint8_t *key) {
    if (!idx || !key || idx->count == 0) return NULL;

    uint64_t tag = make_tag(key);
    uint32_t pos = (uint32_t)make_bucket(key, idx->mask);

    for (uint32_t i = 0; i < idx->capacity; i++) {
        uint32_t p = (pos + i) & idx->mask;
        uint64_t t = idx->tags[p];

        if (t == TAG_EMPTY) return NULL;          /* empty — not found */
        if (t == tag && idx->offsets[p] != 0)     /* match */
            return &idx->offsets[p];
        /* tombstone or collision — keep probing */
    }
    return NULL;
}

bool flat_index_put(flat_index_t *idx, const uint8_t *key, uint32_t slot_id) {
    if (!idx || !key) return false;

    uint64_t tag = make_tag(key);
    uint32_t pos = (uint32_t)make_bucket(key, idx->mask);
    int32_t first_tombstone = -1;

    for (uint32_t i = 0; i < idx->capacity; i++) {
        uint32_t p = (pos + i) & idx->mask;
        uint64_t t = idx->tags[p];

        if (t == TAG_EMPTY) {
            /* Use tombstone slot if we found one earlier */
            uint32_t insert_pos = (first_tombstone >= 0) ? (uint32_t)first_tombstone : p;
            idx->tags[insert_pos] = tag;
            idx->offsets[insert_pos] = slot_id + 1;
            idx->count++;
            return true;
        }
        if (t == TAG_TOMBSTONE) {
            if (first_tombstone < 0) first_tombstone = (int32_t)p;
            continue;
        }
        if (t == tag && idx->offsets[p] != 0) {
            /* Update existing */
            idx->offsets[p] = slot_id + 1;
            return true;
        }
    }

    /* Table full — shouldn't happen if caller checks needs_grow */
    return false;
}

bool flat_index_delete(flat_index_t *idx, const uint8_t *key) {
    if (!idx || !key || idx->count == 0) return false;

    uint64_t tag = make_tag(key);
    uint32_t pos = (uint32_t)make_bucket(key, idx->mask);

    for (uint32_t i = 0; i < idx->capacity; i++) {
        uint32_t p = (pos + i) & idx->mask;
        uint64_t t = idx->tags[p];

        if (t == TAG_EMPTY) return false;
        if (t == tag && idx->offsets[p] != 0) {
            idx->tags[p] = TAG_TOMBSTONE;
            idx->offsets[p] = 0;
            idx->count--;
            return true;
        }
    }
    return false;
}

bool flat_index_contains(const flat_index_t *idx, const uint8_t *key) {
    return flat_index_get(idx, key) != NULL;
}

bool flat_index_needs_grow(const flat_index_t *idx) {
    if (!idx || idx->capacity == 0) return true;
    return (uint64_t)idx->count * 100 >= (uint64_t)idx->capacity * FLAT_INDEX_LOAD_PCT;
}

bool flat_index_grow(flat_index_t *idx, uint32_t new_capacity) {
    if (!idx) return false;
    /* Round up to power of 2 */
    uint32_t cap = 1;
    while (cap < new_capacity) cap <<= 1;

    free(idx->tags);
    free(idx->offsets);

    idx->tags = calloc(cap, sizeof(uint64_t));
    idx->offsets = calloc(cap, sizeof(uint32_t));
    if (!idx->tags || !idx->offsets) {
        free(idx->tags); free(idx->offsets);
        idx->tags = NULL; idx->offsets = NULL;
        idx->capacity = 0;
        return false;
    }
    idx->capacity = cap;
    idx->mask = cap - 1;
    idx->count = 0;
    return true;
}
