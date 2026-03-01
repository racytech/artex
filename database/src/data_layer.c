#include "../include/data_layer.h"
#include "../include/state_store.h"
#include "../include/code_store.h"
#include "../include/nibble_trie.h"
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
    nibble_trie_t index;
    state_store_t *store;
    code_store_t *code;
    mem_art_t buffer;
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

    if (!nt_init(&dl->index, key_size, value_size)) {
        free(dl);
        return NULL;
    }

    dl->store = state_store_create(state_path);
    if (!dl->store) {
        nt_destroy(&dl->index);
        free(dl);
        return NULL;
    }

    if (code_path) {
        dl->code = code_store_create(code_path);
        if (!dl->code) {
            nt_destroy(&dl->index);
            state_store_destroy(dl->store);
            free(dl);
            return NULL;
        }
    }

    if (!mem_art_init(&dl->buffer)) {
        nt_destroy(&dl->index);
        state_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    return dl;
}

void dl_destroy(data_layer_t *dl) {
    if (!dl) return;
    nt_destroy(&dl->index);
    state_store_destroy(dl->store);
    if (dl->code) code_store_destroy(dl->code);
    mem_art_destroy(&dl->buffer);
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
    return mem_art_insert(&dl->buffer, key, dl->key_size, buf, 1 + len);
}

bool dl_delete(data_layer_t *dl, const uint8_t *key) {
    uint8_t tombstone = BUF_FLAG_TOMBSTONE;
    return mem_art_insert(&dl->buffer, key, dl->key_size, &tombstone, 1);
}

bool dl_get(data_layer_t *dl, const uint8_t *key,
            void *out_value, uint16_t *out_len) {
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

    // 2. Check index → disk
    const void *ref = nt_get(&dl->index, key);
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

    mem_art_iterator_t *iter = mem_art_iterator_create(&dl->buffer);
    if (!iter) return 0;

    while (mem_art_iterator_next(iter)) {
        size_t klen = 0, vlen = 0;
        const uint8_t *key = mem_art_iterator_key(iter, &klen);
        const void *val = mem_art_iterator_value(iter, &vlen);
        if (!key || !val || klen != dl->key_size || vlen < 1) continue;

        uint8_t flag = *(const uint8_t *)val;

        if (flag == BUF_FLAG_TOMBSTONE) {
            const void *ref = nt_get(&dl->index, key);
            if (ref) {
                uint32_t slot;
                memcpy(&slot, ref, 4);
                if (!(slot & CODE_REF_BIT)) {
                    // Only free state_store slots, not code entries
                    state_store_free(dl->store, slot);
                }
                nt_delete(&dl->index, key);
            }
        } else if (flag == BUF_FLAG_WRITE && vlen > 1) {
            const uint8_t *data = (const uint8_t *)val + 1;
            uint16_t data_len = (uint16_t)(vlen - 1);

            const void *ref = nt_get(&dl->index, key);
            if (ref) {
                // Update: rewrite same slot
                uint32_t slot;
                memcpy(&slot, ref, 4);
                state_store_write(dl->store, slot, data, data_len);
            } else {
                // Insert: allocate new slot
                uint32_t slot = state_store_alloc(dl->store);
                state_store_write(dl->store, slot, data, data_len);
                nt_insert(&dl->index, key, &slot);
            }
        }
        count++;
    }

    mem_art_iterator_destroy(iter);

    // No explicit sync here — the kernel writeback daemon flushes dirty
    // pages in the background (~5s interval). By checkpoint time (128+
    // blocks later), most pages are already on disk. The blocking
    // fdatasync in dl_checkpoint() handles the rest.

    // Clear buffer
    mem_art_destroy(&dl->buffer);
    mem_art_init(&dl->buffer);

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
    const void *existing = nt_get(&dl->index, key);
    if (existing) return true;

    // Append to code.dat
    uint32_t index = code_store_append(dl->code, bytecode, len);
    if (index == UINT32_MAX) return false;

    // Insert into compact_art with bit 31 set
    uint32_t ref = index | CODE_REF_BIT;
    return nt_insert(&dl->index, key, &ref);
}

bool dl_get_code(data_layer_t *dl, const uint8_t *key,
                 void *out, uint32_t *out_len) {
    if (!dl || !dl->code) return false;

    const void *ref_ptr = nt_get(&dl->index, key);
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

    const void *ref_ptr = nt_get(&dl->index, key);
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
    // Merge kicks off async writeback each block, so most pages are already
    // on disk. This blocking fdatasync ensures everything is durable before
    // the checkpoint commit (rename) makes the new index reachable.
    state_store_sync(dl->store);
    if (dl->code) code_store_sync(dl->code);
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

    if (!nt_init(&dl->index, key_size, value_size)) {
        free(dl);
        return NULL;
    }

    dl->store = state_store_open(state_path);
    if (!dl->store) {
        nt_destroy(&dl->index);
        free(dl);
        return NULL;
    }

    if (code_path) {
        dl->code = code_store_open(code_path);
        if (!dl->code) {
            nt_destroy(&dl->index);
            state_store_destroy(dl->store);
            free(dl);
            return NULL;
        }
    }

    if (!checkpoint_load(index_path, out_block_number,
                         &dl->index, dl->store, dl->code)) {
        nt_destroy(&dl->index);
        state_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    if (!mem_art_init(&dl->buffer)) {
        nt_destroy(&dl->index);
        state_store_destroy(dl->store);
        if (dl->code) code_store_destroy(dl->code);
        free(dl);
        return NULL;
    }

    return dl;
}

// ============================================================================
// Cursor (ih_cursor_t adapter over nibble_trie + state_store)
// ============================================================================

struct dl_cursor {
    nt_iterator_t *iter;
    state_store_t *store;
    uint32_t key_size;
    uint8_t value_buf[STATE_STORE_MAX_VALUE];
    uint16_t value_len;
    bool has_value;
};

// Load value from current iterator position into cursor buffer.
static bool dl_cursor_load_value(dl_cursor_t *cur) {
    cur->has_value = false;
    const void *ref_ptr = nt_iterator_value(cur->iter);
    if (!ref_ptr) return false;

    uint32_t slot;
    memcpy(&slot, ref_ptr, 4);
    if (slot & CODE_REF_BIT) return false;  // skip code refs

    if (!state_store_read(cur->store, slot, cur->value_buf, &cur->value_len))
        return false;

    cur->has_value = true;
    return true;
}

static bool dl_cursor_seek_fn(void *ctx, const uint8_t *key, size_t key_len) {
    dl_cursor_t *cur = ctx;
    (void)key_len;
    if (!nt_iterator_seek(cur->iter, key))
        return false;
    dl_cursor_load_value(cur);
    return true;
}

static bool dl_cursor_next_fn(void *ctx) {
    dl_cursor_t *cur = ctx;
    if (!nt_iterator_next(cur->iter))
        return false;
    dl_cursor_load_value(cur);
    return true;
}

static bool dl_cursor_valid_fn(void *ctx) {
    dl_cursor_t *cur = ctx;
    return !nt_iterator_done(cur->iter);
}

static const uint8_t *dl_cursor_key_fn(void *ctx, size_t *out_len) {
    dl_cursor_t *cur = ctx;
    if (nt_iterator_done(cur->iter)) return NULL;
    *out_len = cur->key_size;
    return nt_iterator_key(cur->iter);
}

static const uint8_t *dl_cursor_value_fn(void *ctx, size_t *out_len) {
    dl_cursor_t *cur = ctx;
    if (!cur->has_value) {
        *out_len = 0;
        return NULL;
    }
    *out_len = cur->value_len;
    return cur->value_buf;
}

dl_cursor_t *dl_cursor_create(data_layer_t *dl) {
    if (!dl) return NULL;
    dl_cursor_t *cur = malloc(sizeof(dl_cursor_t));
    if (!cur) return NULL;
    memset(cur, 0, sizeof(*cur));

    cur->iter = nt_iterator_create(&dl->index);
    if (!cur->iter) {
        free(cur);
        return NULL;
    }
    cur->store = dl->store;
    cur->key_size = dl->key_size;
    return cur;
}

void dl_cursor_destroy(dl_cursor_t *cursor) {
    if (!cursor) return;
    nt_iterator_destroy(cursor->iter);
    free(cursor);
}

ih_cursor_t dl_cursor_as_ih(dl_cursor_t *cursor) {
    ih_cursor_t c;
    c.ctx = cursor;
    c.seek = dl_cursor_seek_fn;
    c.next = dl_cursor_next_fn;
    c.valid = dl_cursor_valid_fn;
    c.key = dl_cursor_key_fn;
    c.value = dl_cursor_value_fn;
    return c;
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
    // Free everything allocated so far
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
    s.index_keys = nt_size(&dl->index);
    s.buffer_entries = mem_art_size(&dl->buffer);
    s.total_merged = dl->total_merged;
    s.free_slots = state_store_free_count(dl->store);
    s.code_count = dl->code ? code_store_count(dl->code) : 0;
    s.code_file_size = dl->code ? code_store_file_size(dl->code) : 0;
    return s;
}
