/**
 * state_migrate — upgrade an ART1 (legacy) state snapshot to ART2.
 *
 * ART1 (no DFS hash streams) and ART2 (with hash streams) share the same
 * account/storage payload. The upgrade is a one-time recompute: read the
 * V1 file, run a single full state-root computation to populate every
 * cached internal-node hash, then save under the V2 format. Subsequent
 * loads short-circuit the (~30-min at mainnet scale) MPT rehash.
 *
 * Usage:
 *   state_migrate <input.bin>            # writes <input>.bin.art2 alongside
 *   state_migrate <input.bin> <output>   # explicit output path
 */
#include "state.h"
#include "evm_state.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void print_hash(const char *label, const uint8_t h[32]) {
    fprintf(stderr, "  %s: 0x", label);
    for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", h[i]);
    fprintf(stderr, "\n");
}

static double seconds_since(const struct timespec *t0) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - t0->tv_sec) + (now.tv_nsec - t0->tv_nsec) / 1e9;
}

int main(int argc, char **argv) {
    if (argc < 2 || argc > 3) {
        fprintf(stderr,
            "usage: %s <input.bin> [output]\n"
            "  Upgrades a legacy ART1 snapshot to ART2 (with DFS hash streams).\n"
            "  Default output path is <input>.art2.\n", argv[0]);
        return 1;
    }

    const char *in_path = argv[1];
    char auto_out[4096];
    const char *out_path;
    if (argc == 3) {
        out_path = argv[2];
    } else {
        snprintf(auto_out, sizeof(auto_out), "%s.art2", in_path);
        out_path = auto_out;
    }

    fprintf(stderr, "state_migrate: %s -> %s\n", in_path, out_path);

    evm_state_t *es = evm_state_create(NULL);
    if (!es) { fprintf(stderr, "FATAL: evm_state_create failed\n"); return 2; }
    state_t *st = evm_state_get_state(es);

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    hash_t saved_root;
    if (!state_load_v1(st, in_path, &saved_root)) {
        fprintf(stderr, "FATAL: state_load_v1 failed (is the file ART1?)\n");
        evm_state_destroy(es);
        return 3;
    }
    fprintf(stderr, "  load: %.1fs\n", seconds_since(&t0));
    print_hash("saved root", saved_root.bytes);

    /* Recompute root — populates every internal-node hash cache so
     * state_save can emit them. This is the slow step (~30 min at
     * mainnet scale, dominated by storage-root recomputation across
     * dirty resources). */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hash_t computed_root = evm_state_compute_mpt_root(es, false);
    fprintf(stderr, "  recompute: %.1fs\n", seconds_since(&t0));
    print_hash("computed  ", computed_root.bytes);

    if (memcmp(saved_root.bytes, computed_root.bytes, 32) != 0) {
        fprintf(stderr,
            "FATAL: recomputed root does not match the V1 file's stored root.\n"
            "  This usually means the input file is corrupt or was produced by\n"
            "  a different chain config. Refusing to write a misleading V2 file.\n");
        evm_state_destroy(es);
        return 4;
    }

    clock_gettime(CLOCK_MONOTONIC, &t0);
    if (!state_save(st, out_path, &computed_root)) {
        fprintf(stderr, "FATAL: state_save failed\n");
        evm_state_destroy(es);
        return 5;
    }
    fprintf(stderr, "  save: %.1fs\n", seconds_since(&t0));

    evm_state_destroy(es);
    fprintf(stderr, "done.\n");
    return 0;
}
