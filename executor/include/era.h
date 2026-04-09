#ifndef ART_EXECUTOR_ERA_H
#define ART_EXECUTOR_ERA_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "block.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Era file reader — post-merge beacon chain archives.
 *
 * Era files contain beacon blocks in e2store format (snappy-compressed SSZ).
 * Blocks are stored sequentially — no per-slot random access index.
 * Use era_iter_next() to iterate blocks in order.
 *
 * Forks: Bellatrix (Paris), Capella (Shanghai), Deneb (Cancun).
 */

typedef struct {
    uint8_t  *data;         /* mmap'd file contents */
    size_t    file_size;
    uint64_t  start_slot;   /* from SlotIndex at end of file */
} era_t;

/**
 * Sequential iterator over blocks in an era file.
 */
typedef struct {
    const era_t *era;
    size_t       pos;       /* current byte offset in file */
} era_iter_t;

/** Open an era file (memory-mapped). */
bool era_open(era_t *era, const char *path);

/** Close an era file and release resources. */
void era_close(era_t *era);

/** Create an iterator starting at the first block. */
era_iter_t era_iter(const era_t *era);

/**
 * Read the next block from the iterator.
 *
 * Skips empty slots automatically. Returns false when no more blocks.
 * Caller must call block_body_free(body) when done.
 *
 * @param it         Iterator (advanced on success)
 * @param hdr        Output: decoded block header
 * @param body       Output: decoded block body (caller frees)
 * @param block_hash Output: 32-byte block hash from execution payload
 * @param slot_out   Output: beacon slot number (may be NULL)
 * @return true on success, false when done or error
 */
bool era_iter_next(era_iter_t *it,
                   block_header_t *hdr, block_body_t *body,
                   uint8_t block_hash[32], uint64_t *slot_out);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_ERA_H */
