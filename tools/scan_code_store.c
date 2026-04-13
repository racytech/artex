/**
 * Scan code_store and classify contracts by call opcodes.
 * Usage: ./scan_code_store <code_store_path>
 *
 * Opens code_store, iterates all entries via disk_table,
 * scans each bytecode for CALL/CREATE/DELEGATECALL/STATICCALL.
 */
#include "code_store.h"
#include "disk_table.h"
#include "bytecode_scan.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    code_store_t *cs;
    size_t total;
    size_t has_calls;
    size_t no_calls;
    size_t empty;
    size_t total_bytes_calls;
    size_t total_bytes_no_calls;
} scan_ctx_t;

static void scan_cb(const uint8_t *key, void *user_data) {
    scan_ctx_t *ctx = (scan_ctx_t *)user_data;
    ctx->total++;

    uint32_t len = code_store_get_size(ctx->cs, key);
    if (len == 0) {
        ctx->empty++;
        return;
    }

    uint8_t *buf = malloc(len);
    if (!buf) return;
    uint32_t got = code_store_get(ctx->cs, key, buf, len);
    if (got == 0) { free(buf); return; }

    if (bytecode_has_calls(buf, got)) {
        ctx->has_calls++;
        ctx->total_bytes_calls += got;
    } else {
        ctx->no_calls++;
        ctx->total_bytes_no_calls += got;
    }

    free(buf);

    if (ctx->total % 100000 == 0)
        fprintf(stderr, "  scanned %zu contracts...\n", ctx->total);
}

int main(int argc, char **argv) {
    const char *path = (argc > 1) ? argv[1] : "/home/racytech/.artex/chain_replay_code";

    /* Open code store */
    code_store_t *cs = code_store_open(path);
    if (!cs) {
        fprintf(stderr, "Failed to open code store at %s\n", path);
        return 1;
    }

    uint64_t count = code_store_count(cs);
    printf("Code store: %lu unique contracts\n", count);

    /* Open the disk_table index directly for iteration */
    char idx_path[1024];
    snprintf(idx_path, sizeof(idx_path), "%s.idx", path);
    disk_table_t *dt = disk_table_open(idx_path);
    if (!dt) {
        fprintf(stderr, "Failed to open index at %s\n", idx_path);
        code_store_destroy(cs);
        return 1;
    }

    scan_ctx_t ctx = { .cs = cs };
    printf("Scanning bytecodes...\n");
    disk_table_foreach_key(dt, scan_cb, &ctx);

    printf("\n=== Results ===\n");
    printf("Total contracts:     %zu\n", ctx.total);
    printf("Empty code:          %zu (%.1f%%)\n", ctx.empty,
           ctx.total ? 100.0 * ctx.empty / ctx.total : 0);
    printf("Has CALL opcodes:    %zu (%.1f%%) — avg %.0f bytes\n", ctx.has_calls,
           ctx.total ? 100.0 * ctx.has_calls / ctx.total : 0,
           ctx.has_calls ? (double)ctx.total_bytes_calls / ctx.has_calls : 0);
    printf("No CALL opcodes:     %zu (%.1f%%) — avg %.0f bytes\n", ctx.no_calls,
           ctx.total ? 100.0 * ctx.no_calls / ctx.total : 0,
           ctx.no_calls ? (double)ctx.total_bytes_no_calls / ctx.no_calls : 0);
    printf("\nParallelizable:      %zu (%.1f%%) of non-empty contracts\n",
           ctx.no_calls,
           (ctx.has_calls + ctx.no_calls) > 0
               ? 100.0 * ctx.no_calls / (ctx.has_calls + ctx.no_calls) : 0);

    disk_table_destroy(dt);
    code_store_destroy(cs);
    return 0;
}
