#include "../include/data_layer.h"
#include "../include/hash_store.h"
#include "../include/code_store.h"
#include "../include/mem_art.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Internal constants
// ============================================================================

// Buffer flag bytes (in write buffer values)
#define BUF_FLAG_WRITE     0x01
#define BUF_FLAG_TOMBSTONE 0x02

// Code ref size: 4 byte code_store index
#define CODE_REF_SIZE   4

// Max buffer value size (type prefix + actual value)
// Actual limit depends on hash_store max_value at runtime
#define BUF_MAX_VALUE   256

// ============================================================================
// Opaque struct
// ============================================================================

struct data_layer {
    hash_store_t *store;
    code_store_t *code;
    mem_art_t     buffer;
    uint32_t      key_size;
    uint32_t      max_value;  // hash_store max_value (slot_size - 10 - key_size)
    uint64_t      total_merged;
};

// ============================================================================
// Lifecycle
// ============================================================================

data_layer_t *dl_create(const char *dir, const char *code_path,
                         uint32_t key_size, uint32_t slot_size,
                         uint64_t shard_capacity) {
    data_layer_t *dl = malloc(sizeof(data_layer_t));
    if (!dl) return NULL;
    memset(dl, 0, sizeof(*dl));
    dl->key_size = key_size;

    dl->store = hash_store_create(dir, shard_capacity, key_size, slot_size);
    if (!dl->store) {
        free(dl);
        return NULL;
    }
    dl->max_value = hash_store_max_value(dl->store);

    if (code_path) {
        dl->code = code_store_create(code_path);
        if (!dl->code) {
            hash_store_destroy(dl->store);
            free(dl);
            return NULL;
        }
    }

    if (!mem_art_init(&dl->buffer)) {
        hash_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    return dl;
}

data_layer_t *dl_open(const char *dir, const char *code_path) {
    data_layer_t *dl = malloc(sizeof(data_layer_t));
    if (!dl) return NULL;
    memset(dl, 0, sizeof(*dl));

    dl->store = hash_store_open(dir);
    if (!dl->store) {
        free(dl);
        return NULL;
    }
    dl->key_size = hash_store_key_size(dl->store);
    dl->max_value = hash_store_max_value(dl->store);

    if (code_path) {
        dl->code = code_store_open(code_path);
        if (!dl->code) {
            hash_store_destroy(dl->store);
            free(dl);
            return NULL;
        }
    }

    if (!mem_art_init(&dl->buffer)) {
        hash_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    return dl;
}

void dl_destroy(data_layer_t *dl) {
    if (!dl) return;
    hash_store_destroy(dl->store);
    if (dl->code) code_store_destroy(dl->code);
    mem_art_destroy(&dl->buffer);
    free(dl);
}

// ============================================================================
// Per-block Operations
// ============================================================================

bool dl_put(data_layer_t *dl, const uint8_t *key,
            const void *value, uint16_t len) {
    if (!dl || !key || !value) return false;
    // Buffer format: [1B flag][value bytes]
    uint8_t buf[BUF_MAX_VALUE];
    buf[0] = BUF_FLAG_WRITE;
    memcpy(buf + 1, value, len);
    return mem_art_insert(&dl->buffer, key, dl->key_size, buf, 1 + len);
}

bool dl_delete(data_layer_t *dl, const uint8_t *key) {
    if (!dl || !key) return false;
    uint8_t tombstone = BUF_FLAG_TOMBSTONE;
    return mem_art_insert(&dl->buffer, key, dl->key_size, &tombstone, 1);
}

bool dl_get(data_layer_t *dl, const uint8_t *key,
            void *out_value, uint16_t *out_len) {
    if (!dl || !key) return false;

    // 1. Check write buffer
    size_t vlen = 0;
    const void *bval = mem_art_get(&dl->buffer, key, dl->key_size, &vlen);
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

    // 2. Check hash_store directly (raw value, no type prefix)
    uint8_t val_buf[BUF_MAX_VALUE];
    uint8_t val_len = 0;
    if (!hash_store_get(dl->store, key, val_buf, &val_len))
        return false;

    if (out_len) *out_len = val_len;
    if (out_value) memcpy(out_value, val_buf, val_len);
    return true;
}

// ============================================================================
// Merge
// ============================================================================

uint64_t dl_merge(data_layer_t *dl) {
    if (!dl) return 0;
    uint64_t count = 0;

    mem_art_iterator_t *iter = mem_art_iterator_create(&dl->buffer);
    if (!iter) return 0;

    while (mem_art_iterator_next(iter)) {
        size_t klen = 0, vlen = 0;
        const uint8_t *key = mem_art_iterator_key(iter, &klen);
        const void *val = mem_art_iterator_value(iter, &vlen);
        if (!key || !val || klen != dl->key_size || vlen < 1) continue;

        uint8_t flag = *(const uint8_t *)val;

        if (flag == BUF_FLAG_TOMBSTONE) {
            hash_store_delete(dl->store, key);
        } else if (flag == BUF_FLAG_WRITE && vlen > 1) {
            const uint8_t *data = (const uint8_t *)val + 1;
            uint16_t data_len = (uint16_t)(vlen - 1);
            hash_store_put(dl->store, key, data, data_len);
        }
        count++;
    }

    mem_art_iterator_destroy(iter);

    // Clear buffer for next block
    mem_art_destroy(&dl->buffer);
    mem_art_init(&dl->buffer);

    dl->total_merged += count;
    return count;
}

// ============================================================================
// Code Operations
// ============================================================================

// Code entries use a modified key to avoid colliding with state entries:
// XOR last byte with 0xFF. Fingerprint (first 8 bytes) stays the same,
// so the entry routes to the same shard. Full-key comparison in the slot
// ensures no false matches.
static void make_code_key(uint8_t *out, const uint8_t *key, uint32_t key_size) {
    memcpy(out, key, key_size);
    out[key_size - 1] ^= 0xFF;
}

bool dl_put_code(data_layer_t *dl, const uint8_t *key,
                 const void *bytecode, uint32_t len) {
    if (!dl || !dl->code) return false;

    uint8_t ckey[64];  // max key_size
    make_code_key(ckey, key, dl->key_size);

    // Dedup: if code key already in hash_store, skip
    if (hash_store_contains(dl->store, ckey)) return true;

    // Append to code.dat
    uint32_t index = code_store_append(dl->code, bytecode, len);
    if (index == UINT32_MAX) return false;

    // Store code ref in hash_store: [uint32_t index]
    uint8_t ref_buf[4];
    memcpy(ref_buf, &index, 4);
    return hash_store_put(dl->store, ckey, ref_buf, 4);
}

bool dl_get_code(data_layer_t *dl, const uint8_t *key,
                 void *out, uint32_t *out_len) {
    if (!dl || !dl->code) return false;

    uint8_t ckey[64];
    make_code_key(ckey, key, dl->key_size);

    uint8_t val_buf[4];
    uint8_t val_len = 0;
    if (!hash_store_get(dl->store, ckey, val_buf, &val_len))
        return false;

    if (val_len < 4) return false;

    uint32_t index;
    memcpy(&index, val_buf, 4);

    uint32_t length = code_store_length(dl->code, index);
    if (length == 0) return false;

    if (out_len) *out_len = length;
    if (out) return code_store_read(dl->code, index, out, length);
    return true;
}

uint32_t dl_code_length(data_layer_t *dl, const uint8_t *key) {
    if (!dl || !dl->code) return 0;

    uint8_t ckey[64];
    make_code_key(ckey, key, dl->key_size);

    uint8_t val_buf[4];
    uint8_t val_len = 0;
    if (!hash_store_get(dl->store, ckey, val_buf, &val_len))
        return 0;

    if (val_len < 4) return 0;

    uint32_t index;
    memcpy(&index, val_buf, 4);
    return code_store_length(dl->code, index);
}

// ============================================================================
// Checkpoint / Recovery
// ============================================================================

bool dl_checkpoint(data_layer_t *dl) {
    if (!dl) return false;
    hash_store_sync(dl->store);
    if (dl->code) code_store_sync(dl->code);
    return true;
}

// ============================================================================
// Dirty Key Extraction (pre-merge)
// ============================================================================

bool dl_extract_dirty(data_layer_t *dl, dl_dirty_set_t *out) {
    if (!dl || !out) return false;
    memset(out, 0, sizeof(*out));

    size_t buf_size = mem_art_size(&dl->buffer);
    if (buf_size == 0) return false;

    // Allocate arrays
    out->keys = malloc(buf_size * sizeof(uint8_t *));
    out->values = malloc(buf_size * sizeof(uint8_t *));
    out->value_lens = malloc(buf_size * sizeof(size_t));
    if (!out->keys || !out->values || !out->value_lens) {
        free(out->keys); free(out->values); free(out->value_lens);
        memset(out, 0, sizeof(*out));
        return false;
    }

    mem_art_iterator_t *iter = mem_art_iterator_create(&dl->buffer);
    if (!iter) {
        free(out->keys); free(out->values); free(out->value_lens);
        memset(out, 0, sizeof(*out));
        return false;
    }

    size_t count = 0;
    while (mem_art_iterator_next(iter)) {
        size_t klen = 0, vlen = 0;
        const uint8_t *key = mem_art_iterator_key(iter, &klen);
        const void *val = mem_art_iterator_value(iter, &vlen);
        if (!key || !val || klen != dl->key_size || vlen < 1) continue;

        uint8_t flag = *(const uint8_t *)val;

        // Copy key
        uint8_t *kcopy = malloc(dl->key_size);
        if (!kcopy) goto fail;
        memcpy(kcopy, key, dl->key_size);
        out->keys[count] = kcopy;

        if (flag == BUF_FLAG_TOMBSTONE) {
            out->values[count] = NULL;
            out->value_lens[count] = 0;
        } else if (flag == BUF_FLAG_WRITE && vlen > 1) {
            size_t data_len = vlen - 1;
            uint8_t *vcopy = malloc(data_len);
            if (!vcopy) { free(kcopy); goto fail; }
            memcpy(vcopy, (const uint8_t *)val + 1, data_len);
            out->values[count] = vcopy;
            out->value_lens[count] = data_len;
        } else {
            free(kcopy);
            continue;  // skip invalid entries
        }
        count++;
    }

    mem_art_iterator_destroy(iter);
    out->count = count;
    return count > 0;

fail:
    mem_art_iterator_destroy(iter);
    for (size_t i = 0; i < count; i++) {
        free(out->keys[i]);
        free(out->values[i]);
    }
    free(out->keys); free(out->values); free(out->value_lens);
    memset(out, 0, sizeof(*out));
    return false;
}

void dl_dirty_set_free(dl_dirty_set_t *ds) {
    if (!ds) return;
    for (size_t i = 0; i < ds->count; i++) {
        free(ds->keys[i]);
        free(ds->values[i]);
    }
    free(ds->keys);
    free(ds->values);
    free(ds->value_lens);
    memset(ds, 0, sizeof(*ds));
}

// ============================================================================
// Diagnostics
// ============================================================================

dl_stats_t dl_stats(const data_layer_t *dl) {
    dl_stats_t s = {0};
    if (!dl) return s;
    s.index_keys = hash_store_count(dl->store);
    s.buffer_entries = mem_art_size(&dl->buffer);
    s.total_merged = dl->total_merged;
    s.code_count = dl->code ? code_store_count(dl->code) : 0;
    s.code_file_size = dl->code ? code_store_file_size(dl->code) : 0;
    return s;
}
