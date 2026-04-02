/**
 * mpt_fuzz_lib.c — Shared library for Python differential fuzzing.
 *
 * Exposes compact_art→art_mpt, mem_art→art_mpt, mem_mpt (batch),
 * and Python HexaryTrie through a ctypes-friendly C API.
 *
 * Build: added as a SHARED library target in CMakeLists.txt
 */

#include <stdlib.h>
#include <string.h>
#include "art_mpt.h"
#include "art_iface.h"
#include "mem_art.h"
#include "mem_mpt.h"

/* =========================================================================
 * mem_mpt batch wrapper
 *
 * mem_mpt expects an array of mpt_batch_entry_t with embedded pointers.
 * This wrapper accumulates entries, then computes the root in one shot.
 * ========================================================================= */

typedef struct {
    mpt_batch_entry_t *entries;
    uint8_t           *values;      /* concatenated value data */
    size_t             count;
    size_t             cap;
    size_t             val_used;
    size_t             val_cap;
} batch_ctx_t;

batch_ctx_t *batch_create(void) {
    batch_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    ctx->cap     = 256;
    ctx->val_cap = 4096;
    ctx->entries = malloc(ctx->cap * sizeof(mpt_batch_entry_t));
    ctx->values  = malloc(ctx->val_cap);
    if (!ctx->entries || !ctx->values) {
        free(ctx->entries);
        free(ctx->values);
        free(ctx);
        return NULL;
    }
    return ctx;
}

void batch_add(batch_ctx_t *ctx, const uint8_t *key,
               const uint8_t *value, size_t value_len) {
    if (ctx->count >= ctx->cap) {
        ctx->cap *= 2;
        ctx->entries = realloc(ctx->entries, ctx->cap * sizeof(mpt_batch_entry_t));
    }
    while (ctx->val_used + value_len > ctx->val_cap) {
        ctx->val_cap *= 2;
        ctx->values = realloc(ctx->values, ctx->val_cap);
    }

    mpt_batch_entry_t *e = &ctx->entries[ctx->count++];
    memcpy(e->key, key, 32);
    memcpy(ctx->values + ctx->val_used, value, value_len);
    /* Store offset temporarily; fix up pointers before compute */
    e->value     = (const uint8_t *)(uintptr_t)ctx->val_used;
    e->value_len = value_len;
    ctx->val_used += value_len;
}

bool batch_root(batch_ctx_t *ctx, uint8_t *out) {
    /* Fix up value pointers (realloc may have moved the buffer) */
    for (size_t i = 0; i < ctx->count; i++) {
        size_t offset = (size_t)(uintptr_t)ctx->entries[i].value;
        ctx->entries[i].value = ctx->values + offset;
    }
    hash_t root;
    bool ok = mpt_compute_root_batch(ctx->entries, ctx->count, &root);
    if (ok) memcpy(out, root.bytes, 32);
    return ok;
}

void batch_reset(batch_ctx_t *ctx) {
    ctx->count    = 0;
    ctx->val_used = 0;
}

void batch_destroy(batch_ctx_t *ctx) {
    if (!ctx) return;
    free(ctx->entries);
    free(ctx->values);
    free(ctx);
}

/* =========================================================================
 * art_mpt wrapper (compact_art backend)
 *
 * Maintains a compact_art with (key[32] -> value record).
 * The value record stores the raw leaf value bytes for MPT hashing.
 * ========================================================================= */

#define ART_MPT_MAX_VAL 256

typedef struct {
    uint16_t len;
    uint8_t  data[ART_MPT_MAX_VAL];
} art_val_rec_t;

typedef struct {
    compact_art_t tree;
    art_mpt_t    *am;
} art_mpt_ctx_t;

static bool art_mpt_dummy_fetch(const void *v, uint8_t *k, void *c) {
    (void)v; (void)k; (void)c; return false;
}

static uint32_t art_mpt_encode_val(const uint8_t *key, const void *leaf_val,
                                    uint32_t val_size, uint8_t *rlp_out,
                                    void *ctx) {
    (void)key; (void)ctx; (void)val_size;
    const art_val_rec_t *r = leaf_val;
    memcpy(rlp_out, r->data, r->len);
    return r->len;
}

art_mpt_ctx_t *art_mpt_ctx_create(void) {
    art_mpt_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    if (!compact_art_init(&ctx->tree, 32, sizeof(art_val_rec_t),
                          false, art_mpt_dummy_fetch, NULL)) {
        free(ctx);
        return NULL;
    }
    ctx->am = art_mpt_create(&ctx->tree, art_mpt_encode_val, NULL);
    if (!ctx->am) {
        compact_art_destroy(&ctx->tree);
        free(ctx);
        return NULL;
    }
    return ctx;
}

void art_mpt_ctx_destroy(art_mpt_ctx_t *ctx) {
    if (!ctx) return;
    art_mpt_destroy(ctx->am);
    compact_art_destroy(&ctx->tree);
    free(ctx);
}

void art_mpt_ctx_insert(art_mpt_ctx_t *ctx, const uint8_t *key,
                         const uint8_t *value, size_t value_len) {
    art_val_rec_t rec = {0};
    rec.len = (uint16_t)(value_len <= ART_MPT_MAX_VAL ? value_len : ART_MPT_MAX_VAL);
    memcpy(rec.data, value, rec.len);
    art_mpt_insert(ctx->am, key, &rec, sizeof(rec));
}

void art_mpt_ctx_delete(art_mpt_ctx_t *ctx, const uint8_t *key) {
    art_mpt_delete(ctx->am, key);
}

bool art_mpt_ctx_root(art_mpt_ctx_t *ctx, uint8_t *out) {
    art_mpt_root_hash(ctx->am, out);
    return true;
}

/* =========================================================================
 * mem_art_mpt wrapper (mem_art + art_iface_mem backend)
 *
 * Same as art_mpt wrapper but uses mem_art + art_iface_mem.
 * This is the code path used by state_v2.
 * ========================================================================= */

typedef struct {
    mem_art_t             tree;
    art_iface_mem_ctx_t   ictx;
    art_mpt_t            *am;
} mem_art_mpt_ctx_t;

static uint32_t mem_art_mpt_encode_val(const uint8_t *key, const void *leaf_val,
                                        uint32_t val_size, uint8_t *rlp_out,
                                        void *ctx) {
    (void)key; (void)ctx; (void)val_size;
    const art_val_rec_t *r = leaf_val;
    memcpy(rlp_out, r->data, r->len);
    return r->len;
}

mem_art_mpt_ctx_t *mem_art_mpt_ctx_create(void) {
    mem_art_mpt_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;

    mem_art_init(&ctx->tree);

    ctx->ictx.tree       = &ctx->tree;
    ctx->ictx.key_size   = 32;
    ctx->ictx.value_size = sizeof(art_val_rec_t);

    ctx->am = art_mpt_create_iface(
        art_iface_mem(&ctx->ictx), mem_art_mpt_encode_val, NULL);
    if (!ctx->am) {
        mem_art_destroy(&ctx->tree);
        free(ctx);
        return NULL;
    }
    return ctx;
}

void mem_art_mpt_ctx_destroy(mem_art_mpt_ctx_t *ctx) {
    if (!ctx) return;
    art_mpt_destroy(ctx->am);
    mem_art_destroy(&ctx->tree);
    free(ctx);
}

void mem_art_mpt_ctx_insert(mem_art_mpt_ctx_t *ctx, const uint8_t *key,
                             const uint8_t *value, size_t value_len) {
    art_val_rec_t rec = {0};
    rec.len = (uint16_t)(value_len <= ART_MPT_MAX_VAL ? value_len : ART_MPT_MAX_VAL);
    memcpy(rec.data, value, rec.len);
    mem_art_insert(&ctx->tree, key, 32, &rec, sizeof(rec));
}

void mem_art_mpt_ctx_delete(mem_art_mpt_ctx_t *ctx, const uint8_t *key) {
    mem_art_delete(&ctx->tree, key, 32);
}

bool mem_art_mpt_ctx_root(mem_art_mpt_ctx_t *ctx, uint8_t *out) {
    art_mpt_root_hash(ctx->am, out);
    return true;
}
