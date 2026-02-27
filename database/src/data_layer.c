#include "../include/data_layer.h"
#include "../include/state_store.h"
#include "../include/code_store.h"
#include "../include/compact_art.h"
#include "../include/mem_art.h"
#include "../include/checkpoint.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Internal constants
// ============================================================================

#define BUF_FLAG_WRITE     0x01
#define BUF_FLAG_TOMBSTONE 0x02
#define CODE_REF_BIT       0x80000000u

// ============================================================================
// Opaque struct
// ============================================================================

struct data_layer {
    compact_art_t index;
    state_store_t *store;
    code_store_t *code;
    art_tree_t buffer;
    uint32_t key_size;
    uint64_t total_merged;
};

// ============================================================================
// Lifecycle
// ============================================================================

data_layer_t *dl_create(const char *state_path, const char *code_path,
                         uint32_t key_size, uint32_t value_size) {
    data_layer_t *dl = malloc(sizeof(data_layer_t));
    if (!dl) return NULL;
    memset(dl, 0, sizeof(*dl));
    dl->key_size = key_size;

    if (!compact_art_init(&dl->index, key_size, value_size)) {
        free(dl);
        return NULL;
    }

    dl->store = state_store_create(state_path);
    if (!dl->store) {
        compact_art_destroy(&dl->index);
        free(dl);
        return NULL;
    }

    if (code_path) {
        dl->code = code_store_create(code_path);
        if (!dl->code) {
            compact_art_destroy(&dl->index);
            state_store_destroy(dl->store);
            free(dl);
            return NULL;
        }
    }

    if (!art_tree_init(&dl->buffer)) {
        compact_art_destroy(&dl->index);
        state_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    return dl;
}

void dl_destroy(data_layer_t *dl) {
    if (!dl) return;
    compact_art_destroy(&dl->index);
    state_store_destroy(dl->store);
    if (dl->code) code_store_destroy(dl->code);
    art_tree_destroy(&dl->buffer);
    free(dl);
}

// ============================================================================
// Per-block Operations
// ============================================================================

bool dl_put(data_layer_t *dl, const uint8_t *key,
            const void *value, uint16_t len) {
    uint8_t buf[1 + STATE_STORE_MAX_VALUE];
    buf[0] = BUF_FLAG_WRITE;
    memcpy(buf + 1, value, len);
    return art_insert(&dl->buffer, key, dl->key_size, buf, 1 + len);
}

bool dl_delete(data_layer_t *dl, const uint8_t *key) {
    uint8_t tombstone = BUF_FLAG_TOMBSTONE;
    return art_insert(&dl->buffer, key, dl->key_size, &tombstone, 1);
}

bool dl_get(data_layer_t *dl, const uint8_t *key,
            void *out_value, uint16_t *out_len) {
    // 1. Check write buffer
    size_t vlen = 0;
    const void *bval = art_get(&dl->buffer, key, dl->key_size, &vlen);
    if (bval) {
        uint8_t flag = *(const uint8_t *)bval;
        if (flag == BUF_FLAG_TOMBSTONE) return false;
        if (flag == BUF_FLAG_WRITE && vlen > 1) {
            uint16_t len = (uint16_t)(vlen - 1);
            if (out_len) *out_len = len;
            if (out_value) memcpy(out_value, (const uint8_t *)bval + 1, len);
            return true;
        }
        return false;
    }

    // 2. Check index → disk
    const void *ref = compact_art_get(&dl->index, key);
    if (!ref) return false;

    uint32_t slot;
    memcpy(&slot, ref, 4);
    if (slot & CODE_REF_BIT) return false;  // code ref, not state
    return state_store_read(dl->store, slot, out_value, out_len);
}

// ============================================================================
// Merge
// ============================================================================

uint64_t dl_merge(data_layer_t *dl) {
    uint64_t count = 0;

    art_iterator_t *iter = art_iterator_create(&dl->buffer);
    if (!iter) return 0;

    while (art_iterator_next(iter)) {
        size_t klen = 0, vlen = 0;
        const uint8_t *key = art_iterator_key(iter, &klen);
        const void *val = art_iterator_value(iter, &vlen);
        if (!key || !val || klen != dl->key_size || vlen < 1) continue;

        uint8_t flag = *(const uint8_t *)val;

        if (flag == BUF_FLAG_TOMBSTONE) {
            const void *ref = compact_art_get(&dl->index, key);
            if (ref) {
                uint32_t slot;
                memcpy(&slot, ref, 4);
                if (!(slot & CODE_REF_BIT)) {
                    // Only free state_store slots, not code entries
                    state_store_free(dl->store, slot);
                }
                compact_art_delete(&dl->index, key);
            }
        } else if (flag == BUF_FLAG_WRITE && vlen > 1) {
            const uint8_t *data = (const uint8_t *)val + 1;
            uint16_t data_len = (uint16_t)(vlen - 1);

            const void *ref = compact_art_get(&dl->index, key);
            if (ref) {
                // Update: rewrite same slot
                uint32_t slot;
                memcpy(&slot, ref, 4);
                state_store_write(dl->store, slot, data, data_len);
            } else {
                // Insert: allocate new slot
                uint32_t slot = state_store_alloc(dl->store);
                state_store_write(dl->store, slot, data, data_len);
                compact_art_insert(&dl->index, key, &slot);
            }
        }
        count++;
    }

    art_iterator_destroy(iter);

    // Clear buffer (no fdatasync — checkpoint concern)
    art_tree_destroy(&dl->buffer);
    art_tree_init(&dl->buffer);

    dl->total_merged += count;
    return count;
}

// ============================================================================
// Code Operations
// ============================================================================

bool dl_put_code(data_layer_t *dl, const uint8_t *key,
                 const void *bytecode, uint32_t len) {
    if (!dl || !dl->code) return false;

    // Dedup: if key already in index, skip
    const void *existing = compact_art_get(&dl->index, key);
    if (existing) return true;

    // Append to code.dat
    uint32_t index = code_store_append(dl->code, bytecode, len);
    if (index == UINT32_MAX) return false;

    // Insert into compact_art with bit 31 set
    uint32_t ref = index | CODE_REF_BIT;
    return compact_art_insert(&dl->index, key, &ref);
}

bool dl_get_code(data_layer_t *dl, const uint8_t *key,
                 void *out, uint32_t *out_len) {
    if (!dl || !dl->code) return false;

    const void *ref_ptr = compact_art_get(&dl->index, key);
    if (!ref_ptr) return false;

    uint32_t ref;
    memcpy(&ref, ref_ptr, 4);
    if (!(ref & CODE_REF_BIT)) return false;  // state ref, not code

    uint32_t index = ref & ~CODE_REF_BIT;
    uint32_t length = code_store_length(dl->code, index);
    if (length == 0) return false;

    if (out_len) *out_len = length;
    if (out) return code_store_read(dl->code, index, out, length);
    return true;
}

uint32_t dl_code_length(data_layer_t *dl, const uint8_t *key) {
    if (!dl || !dl->code) return 0;

    const void *ref_ptr = compact_art_get(&dl->index, key);
    if (!ref_ptr) return 0;

    uint32_t ref;
    memcpy(&ref, ref_ptr, 4);
    if (!(ref & CODE_REF_BIT)) return 0;

    return code_store_length(dl->code, ref & ~CODE_REF_BIT);
}

// ============================================================================
// Checkpoint / Recovery
// ============================================================================

bool dl_checkpoint(data_layer_t *dl, const char *index_path,
                   uint64_t block_number) {
    if (!dl) return false;
    return checkpoint_write(index_path, block_number,
                            &dl->index, dl->store, dl->code);
}

data_layer_t *dl_open(const char *state_path, const char *code_path,
                       const char *index_path,
                       uint32_t key_size, uint32_t value_size,
                       uint64_t *out_block_number) {
    data_layer_t *dl = malloc(sizeof(data_layer_t));
    if (!dl) return NULL;
    memset(dl, 0, sizeof(*dl));
    dl->key_size = key_size;

    if (!compact_art_init(&dl->index, key_size, value_size)) {
        free(dl);
        return NULL;
    }

    dl->store = state_store_open(state_path);
    if (!dl->store) {
        compact_art_destroy(&dl->index);
        free(dl);
        return NULL;
    }

    if (code_path) {
        dl->code = code_store_open(code_path);
        if (!dl->code) {
            compact_art_destroy(&dl->index);
            state_store_destroy(dl->store);
            free(dl);
            return NULL;
        }
    }

    if (!checkpoint_load(index_path, out_block_number,
                         &dl->index, dl->store, dl->code)) {
        compact_art_destroy(&dl->index);
        state_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    if (!art_tree_init(&dl->buffer)) {
        compact_art_destroy(&dl->index);
        state_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    return dl;
}

// ============================================================================
// Diagnostics
// ============================================================================

dl_stats_t dl_stats(const data_layer_t *dl) {
    dl_stats_t s = {0};
    if (!dl) return s;
    s.index_keys = compact_art_size(&dl->index);
    s.buffer_entries = art_size(&dl->buffer);
    s.total_merged = dl->total_merged;
    s.free_slots = state_store_free_count(dl->store);
    s.code_count = dl->code ? code_store_count(dl->code) : 0;
    s.code_file_size = dl->code ? code_store_file_size(dl->code) : 0;
    return s;
}
