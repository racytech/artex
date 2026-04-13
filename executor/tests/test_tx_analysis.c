/**
 * Test tx_analysis on real blocks from era files.
 * Reads N blocks, batch-decodes txs, runs dependency analysis,
 * reports parallelism stats.
 */
#include "tx_analysis.h"
#include "tx_pipeline.h"
#include "tx_decoder.h"
#include "era.h"
#include "block.h"
#include "secp256k1_wrap.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

int main(int argc, char **argv) {
    const char *era_dir = (argc > 1) ? argv[1] : "data/era";
    int max_blocks = (argc > 2) ? atoi(argv[2]) : 100;

    secp256k1_wrap_init();

    /* Collect era files */
    DIR *d = opendir(era_dir);
    if (!d) { fprintf(stderr, "can't open %s\n", era_dir); return 1; }

    char **paths = NULL;
    size_t count = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen < 4 || strcmp(ent->d_name + nlen - 4, ".era") != 0) continue;
        if (nlen >= 5 && strcmp(ent->d_name + nlen - 5, ".era1") == 0) continue;
        char **np = realloc(paths, (count + 1) * sizeof(char *));
        if (!np) break;
        paths = np;
        char full[1024];
        snprintf(full, sizeof(full), "%s/%s", era_dir, ent->d_name);
        paths[count++] = strdup(full);
    }
    closedir(d);
    if (count == 0) { fprintf(stderr, "no .era files\n"); return 1; }
    qsort(paths, count, sizeof(char *), cmp_strings);

    /* Stats accumulators */
    size_t total_txs = 0;
    size_t total_parallel = 0;
    size_t total_serial = 0;
    size_t total_groups = 0;
    size_t blocks_read = 0;
    size_t max_group_size = 0;

    /* Iterate blocks */
    for (size_t f = 0; f < count && (int)blocks_read < max_blocks; f++) {
        era_t era;
        if (!era_open(&era, paths[f])) continue;
        era_iter_t it = era_iter(&era);

        block_header_t hdr;
        block_body_t body;
        uint8_t hash[32];
        uint64_t slot;

        while ((int)blocks_read < max_blocks &&
               era_iter_next(&it, &hdr, &body, hash, &slot)) {

            if (body.tx_count == 0) {
                block_body_free(&body);
                blocks_read++;
                continue;
            }

            /* Batch decode */
            prepared_tx_t *decoded = calloc(body.tx_count, sizeof(prepared_tx_t));
            if (!decoded) { block_body_free(&body); break; }
            tx_batch_decode(&body, body.tx_count, 1, decoded, 4);

            /* Analyze */
            tx_schedule_t sched;
            tx_analyze(decoded, body.tx_count, NULL, &sched);

            total_txs += sched.total_txs;
            total_parallel += sched.parallel_txs;
            total_serial += sched.serial_txs;
            total_groups += sched.group_count;

            for (int g = 0; g < sched.group_count; g++) {
                if (sched.groups[g].count > max_group_size)
                    max_group_size = sched.groups[g].count;
            }

            /* Print per-block details for first few */
            if (blocks_read < 10) {
                printf("block %lu: %zu txs, %d groups (par=%zu ser=%zu)\n",
                       hdr.number, body.tx_count, sched.group_count,
                       sched.parallel_txs, sched.serial_txs);
                for (int g = 0; g < sched.group_count && g < 5; g++)
                    printf("  group[%d]: %u txs\n", g, sched.groups[g].count);
                if (sched.group_count > 5)
                    printf("  ... (%d more groups)\n", sched.group_count - 5);
            }

            tx_schedule_free(&sched);
            for (size_t i = 0; i < body.tx_count; i++)
                if (decoded[i].valid) tx_decoded_free(&decoded[i].tx);
            free(decoded);
            block_body_free(&body);
            blocks_read++;
        }
        era_close(&era);
    }

    /* Summary */
    printf("\n=== %zu blocks analyzed ===\n", blocks_read);
    printf("Total txs:    %zu\n", total_txs);
    printf("Parallel:     %zu (%.1f%%)\n", total_parallel,
           total_txs ? 100.0 * total_parallel / total_txs : 0);
    printf("Serial:       %zu (%.1f%%)\n", total_serial,
           total_txs ? 100.0 * total_serial / total_txs : 0);
    printf("Avg groups/block: %.1f\n",
           blocks_read ? (double)total_groups / blocks_read : 0);
    printf("Max group size: %zu\n", max_group_size);

    for (size_t i = 0; i < count; i++) free(paths[i]);
    free(paths);
    return 0;
}
