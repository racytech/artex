#include "../include/data_layer.h"
#include "../include/hash_store.h"
#include "../include/code_store.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Internal constants
// ============================================================================

// Code ref size: 4 byte code_store index
#define CODE_REF_SIZE   4

// ============================================================================
// Opaque struct
//
// No write buffer — dl_put/dl_delete write directly to hash_store.
// The old mem_art buffer was a leftover from the compact_art era.
// hash_store is mmap'd, so puts are O(1) writes to memory-mapped slots.
// ============================================================================

struct data_layer {
    hash_store_t *store;
    code_store_t *code;
    uint32_t      key_size;
    uint32_t      max_value;  // hash_store max_value (slot_size - 10 - key_size)
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

    return dl;
}

void dl_destroy(data_layer_t *dl) {
    if (!dl) return;
    hash_store_destroy(dl->store);
    if (dl->code) code_store_destroy(dl->code);
    free(dl);
}

// ============================================================================
// Per-block Operations (direct to hash_store, no buffer)
// ============================================================================

bool dl_put(data_layer_t *dl, const uint8_t *key,
            const void *value, uint16_t len) {
    if (!dl || !key || !value) return false;
    return hash_store_put(dl->store, key, value, (uint8_t)len);
}

bool dl_delete(data_layer_t *dl, const uint8_t *key) {
    if (!dl || !key) return false;
    return hash_store_delete(dl->store, key);
}

bool dl_get(data_layer_t *dl, const uint8_t *key,
            void *out_value, uint16_t *out_len) {
    if (!dl || !key) return false;

    uint8_t val_buf[256];
    uint8_t val_len = 0;
    if (!hash_store_get(dl->store, key, val_buf, &val_len))
        return false;

    if (out_len) *out_len = val_len;
    if (out_value) memcpy(out_value, val_buf, val_len);
    return true;
}

// ============================================================================
// Merge (no-op — writes go directly to hash_store)
// ============================================================================

uint64_t dl_merge(data_layer_t *dl) {
    (void)dl;
    return 0;
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
// Diagnostics
// ============================================================================

dl_stats_t dl_stats(const data_layer_t *dl) {
    dl_stats_t s = {0};
    if (!dl) return s;
    s.index_keys = hash_store_count(dl->store);
    s.buffer_entries = 0;  // no buffer — writes go directly to hash_store
    s.total_merged = 0;
    s.code_count = dl->code ? code_store_count(dl->code) : 0;
    s.code_file_size = dl->code ? code_store_file_size(dl->code) : 0;
    return s;
}
