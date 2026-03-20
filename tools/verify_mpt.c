/*
 * verify_mpt — Open an MPT store and verify all node hashes from root.
 *
 * Usage: verify_mpt <base_path>
 *
 * Calls mpt_store_verify_hashes() which walks every reachable node,
 * re-computes keccak256, and checks the recomputed root matches.
 */

#include "mpt_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: verify_mpt <base_path>\n");
        return 1;
    }

    const char *base_path = argv[1];

    printf("Opening %s ...\n", base_path);
    mpt_store_t *ms = mpt_store_open(base_path);
    if (!ms) {
        fprintf(stderr, "ERROR: failed to open MPT store at %s\n", base_path);
        return 1;
    }

    uint8_t root[32];
    mpt_store_root(ms, root);
    printf("Stored root: 0x");
    for (int i = 0; i < 32; i++) printf("%02x", root[i]);
    printf("\n");

    mpt_store_stats_t stats = mpt_store_stats(ms);
    printf("Node count:  %" PRIu64 "\n", stats.node_count);
    printf("Data size:   %" PRIu64 " bytes (%.2f MB)\n",
           stats.data_file_size, stats.data_file_size / (1024.0 * 1024.0));

    printf("\nVerifying hashes (walking all reachable nodes)...\n");

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    bool ok = mpt_store_verify_hashes(ms);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("Verification %s (%.3f s)\n", ok ? "PASSED" : "FAILED", elapsed);

    mpt_store_destroy(ms);
    return ok ? 0 : 1;
}
