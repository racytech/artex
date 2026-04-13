#include "era1.h"
#include "snappy_decode.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* E2Store entry types */
#define TYPE_VERSION           0x3265
#define TYPE_COMPRESSED_HEADER 0x0003
#define TYPE_COMPRESSED_BODY   0x0004
#define TYPE_COMPRESSED_RCPTS  0x0005
#define TYPE_TOTAL_DIFFICULTY  0x0006
#define TYPE_ACCUMULATOR       0x0007
#define TYPE_BLOCK_INDEX       0x3266

/* E2Store entry header: type(2) + length(4) + reserved(2) = 8 bytes */
#define ENTRY_HEADER_SIZE 8

/* Max decompressed size for a single entry (50 MB — generous) */
#define MAX_DECOMPRESSED_SIZE (50 * 1024 * 1024)

static inline uint16_t read_le16(const uint8_t *p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static inline uint32_t read_le32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static inline uint64_t read_le64(const uint8_t *p) {
    return (uint64_t)read_le32(p) | ((uint64_t)read_le32(p + 4) << 32);
}

static inline int64_t read_le64_signed(const uint8_t *p) {
    uint64_t v = read_le64(p);
    int64_t result;
    memcpy(&result, &v, sizeof(result));
    return result;
}

/* Parse an e2store entry header at offset. Returns false if truncated. */
static bool read_entry_header(const uint8_t *data, size_t file_size,
                              size_t offset,
                              uint16_t *type, uint32_t *length) {
    if (offset + ENTRY_HEADER_SIZE > file_size)
        return false;
    *type = read_le16(data + offset);
    *length = read_le32(data + offset + 2);
    return true;
}

bool era1_open(era1_t *era, const char *path) {
    if (!era || !path)
        return false;

    memset(era, 0, sizeof(*era));

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        perror("era1_open: open");
        return false;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("era1_open: fstat");
        close(fd);
        return false;
    }

    era->file_size = (size_t)st.st_size;
    if (era->file_size < 24) {  /* minimum: version + BlockIndex */
        close(fd);
        return false;
    }

    era->data = mmap(NULL, era->file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (era->data == MAP_FAILED) {
        era->data = NULL;
        return false;
    }

    /* Parse BlockIndex from end of file.
     * Layout: [starting_block(8)] [offset[0](8)] ... [offset[count-1](8)] [count(8)]
     * The last 8 bytes of the file = count. */
    era->count = read_le64(era->data + era->file_size - 8);

    if (era->count == 0 || era->count > 8192) {
        fprintf(stderr, "era1_open: invalid block count %lu\n", era->count);
        era1_close(era);
        return false;
    }

    /* BlockIndex entry: the value starts after the e2store header.
     * Value size = 8 (start_block) + count*8 (offsets) + 8 (count) */
    size_t index_value_size = 8 + era->count * 8 + 8;
    size_t index_entry_start = era->file_size - ENTRY_HEADER_SIZE - index_value_size;

    /* Verify it's a BlockIndex entry */
    uint16_t idx_type;
    uint32_t idx_length;
    if (!read_entry_header(era->data, era->file_size, index_entry_start,
                           &idx_type, &idx_length)) {
        era1_close(era);
        return false;
    }

    if (idx_type != TYPE_BLOCK_INDEX || idx_length != index_value_size) {
        fprintf(stderr, "era1_open: BlockIndex type=0x%04x len=%u (expected 0x%04x len=%zu)\n",
                idx_type, idx_length, TYPE_BLOCK_INDEX, index_value_size);
        era1_close(era);
        return false;
    }

    era->index_start = index_entry_start + ENTRY_HEADER_SIZE;
    era->start_block = read_le64(era->data + era->index_start);

    return true;
}

void era1_close(era1_t *era) {
    if (era && era->data) {
        munmap(era->data, era->file_size);
        era->data = NULL;
    }
}

/* Get the file offset of block N's first entry (CompressedHeader) */
static size_t block_offset(const era1_t *era, uint64_t index) {
    /* Offsets are relative to the BlockIndex entry start (including header) */
    size_t offsets_base = era->index_start + 8;  /* skip starting_block */
    int64_t rel = read_le64_signed(era->data + offsets_base + index * 8);
    /* The offset is relative to the BlockIndex e2store entry start */
    size_t index_entry_start = era->index_start - ENTRY_HEADER_SIZE;
    return (size_t)((int64_t)index_entry_start + rel);
}

/* Decompress a snappy-framed e2store entry. Returns malloc'd buffer. */
static uint8_t *decompress_entry(const uint8_t *data, size_t file_size,
                                 size_t offset, uint16_t expected_type,
                                 size_t *out_len) {
    uint16_t type;
    uint32_t length;
    if (!read_entry_header(data, file_size, offset, &type, &length))
        return NULL;

    if (type != expected_type)
        return NULL;

    size_t value_start = offset + ENTRY_HEADER_SIZE;
    if (value_start + length > file_size)
        return NULL;

    /* Allocate decompression buffer */
    size_t cap = MAX_DECOMPRESSED_SIZE;
    uint8_t *buf = malloc(cap);
    if (!buf) return NULL;

    size_t decoded = snappy_frame_decode(data + value_start, length, buf, cap);
    if (decoded == 0) {
        free(buf);
        return NULL;
    }

    *out_len = decoded;
    return buf;
}

bool era1_read_block(const era1_t *era, uint64_t block_number,
                     uint8_t **header_rlp, size_t *header_len,
                     uint8_t **body_rlp, size_t *body_len) {
    if (!era || !era->data || !era1_contains(era, block_number))
        return false;

    uint64_t index = block_number - era->start_block;
    size_t offset = block_offset(era, index);

    /* Entry 1: CompressedHeader */
    *header_rlp = decompress_entry(era->data, era->file_size, offset,
                                   TYPE_COMPRESSED_HEADER, header_len);
    if (!*header_rlp)
        return false;

    /* Advance past header entry */
    uint32_t entry_len = 0;
    uint16_t entry_type = 0;
    read_entry_header(era->data, era->file_size, offset, &entry_type, &entry_len);
    offset += ENTRY_HEADER_SIZE + entry_len;

    /* Entry 2: CompressedBody */
    *body_rlp = decompress_entry(era->data, era->file_size, offset,
                                 TYPE_COMPRESSED_BODY, body_len);
    if (!*body_rlp) {
        free(*header_rlp);
        *header_rlp = NULL;
        return false;
    }

    return true;
}
