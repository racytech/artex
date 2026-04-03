/**
 * mpt_fuzz_lib.c — Shared library for Python differential fuzzing.
 *
 * Exposes mem_mpt (batch) and hart through a ctypes-friendly C API.
 *
 * Build: added as a SHARED library target in CMakeLists.txt
 */

#include <stdlib.h>
#include <string.h>
#include "mem_mpt.h"
#include "hashed_art.h"

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
 * hashed_art wrapper (hart — ART with embedded MPT hash cache)
 *
 * Same interface as mem_art_mpt but uses hart_t directly.
 * Values are stored as raw RLP bytes (up to 256 bytes).
 * ========================================================================= */

typedef struct {
    hart_t tree;
} hart_ctx_t;

static uint32_t hart_encode_val(const uint8_t key[32], const void *leaf_val,
                                 uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    memcpy(rlp_out, leaf_val, 32);
    return 32;
}

hart_ctx_t *hart_ctx_create(void) {
    hart_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    if (!hart_init(&ctx->tree, 32)) {
        free(ctx);
        return NULL;
    }
    return ctx;
}

void hart_ctx_destroy(hart_ctx_t *ctx) {
    if (!ctx) return;
    hart_destroy(&ctx->tree);
    free(ctx);
}

void hart_ctx_insert(hart_ctx_t *ctx, const uint8_t *key,
                      const uint8_t *value, size_t value_len) {
    (void)value_len; /* always 32 */
    hart_insert(&ctx->tree, key, value);
}

void hart_ctx_delete(hart_ctx_t *ctx, const uint8_t *key) {
    hart_delete(&ctx->tree, key);
}

bool hart_ctx_root(hart_ctx_t *ctx, uint8_t *out) {
    hart_root_hash(&ctx->tree, hart_encode_val, NULL, out);
    return true;
}
