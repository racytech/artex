#ifndef ART_EXECUTOR_ERA1_H
#define ART_EXECUTOR_ERA1_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Era1 file reader.
 *
 * Era1 files contain 8192 blocks of historical Ethereum data in e2store format.
 * Each block is stored as 4 consecutive entries:
 *   CompressedHeader, CompressedBody, CompressedReceipts, TotalDifficulty.
 *
 * A BlockIndex at the end of the file provides random access offsets.
 */
typedef struct {
    uint8_t  *data;         /* mmap'd file contents */
    size_t    file_size;
    uint64_t  start_block;  /* first block number */
    uint64_t  count;        /* number of blocks */
    size_t    index_start;  /* byte offset of BlockIndex entry value */
} era1_t;

/**
 * Open an era1 file (memory-mapped).
 *
 * @param era   Era1 handle to initialize
 * @param path  Path to the .era1 file
 * @return true on success
 */
bool era1_open(era1_t *era, const char *path);

/**
 * Close an era1 file and release resources.
 */
void era1_close(era1_t *era);

/**
 * Read a block's header and body RLP (decompressed).
 *
 * @param era          Open era1 handle
 * @param block_number Absolute block number
 * @param header_rlp   Output: malloc'd header RLP bytes (caller frees)
 * @param header_len   Output: header RLP length
 * @param body_rlp     Output: malloc'd body RLP bytes (caller frees)
 * @param body_len     Output: body RLP length
 * @return true on success
 */
bool era1_read_block(const era1_t *era, uint64_t block_number,
                     uint8_t **header_rlp, size_t *header_len,
                     uint8_t **body_rlp, size_t *body_len);

/**
 * Check if a block number is contained in this era1 file.
 */
static inline bool era1_contains(const era1_t *era, uint64_t block_number) {
    return block_number >= era->start_block &&
           block_number < era->start_block + era->count;
}

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_ERA1_H */
