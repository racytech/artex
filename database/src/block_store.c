/**
 * Block Store — Lightweight append-only block index.
 *
 * File layout:
 *   [0..63]   Header: magic(4) version(4) count(8) lowest_block(8) pad(40)
 *   [64..]    Records: block_number(8) block_hash(32) parent_hash(32) timestamp(8)
 *
 * Reads: mmap the entire file, O(1) lookup by block_number.
 * Writes: pwrite new records, update header, remap if needed.
 */

#include "block_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define BS_MAGIC     0x4B4C4253   /* "SBLK" little-endian */
#define BS_VERSION   1
#define BS_HEADER    64
#define BS_RECSZ     80           /* 8 + 32 + 32 + 8 */

/* =========================================================================
 * Internal struct
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t count;
    uint64_t lowest_block;
    uint8_t  _pad[40];
} bs_header_t;

_Static_assert(sizeof(bs_header_t) == BS_HEADER, "header size mismatch");

typedef struct __attribute__((packed)) {
    uint64_t block_number;
    uint8_t  block_hash[32];
    uint8_t  parent_hash[32];
    uint64_t timestamp;
} bs_record_t;

_Static_assert(sizeof(bs_record_t) == BS_RECSZ, "record size mismatch");

struct block_store {
    int       fd;
    char     *path;
    uint8_t  *map;          /* mmap base */
    size_t    map_len;      /* current mmap length */
    uint64_t  count;        /* number of records */
    uint64_t  lowest_block; /* block number of first record */
};

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

static size_t file_size_for(uint64_t count) {
    return BS_HEADER + count * BS_RECSZ;
}

static bool remap(block_store_t *bs) {
    size_t needed = file_size_for(bs->count);
    if (needed <= bs->map_len && bs->map) return true;

    /* Round up to 4 KB page boundary, grow by 256 records at a time */
    size_t target = file_size_for(bs->count + 256);
    target = (target + 4095) & ~(size_t)4095;

    if (bs->map)
        munmap(bs->map, bs->map_len);

    bs->map = mmap(NULL, target, PROT_READ, MAP_SHARED, bs->fd, 0);
    if (bs->map == MAP_FAILED) {
        bs->map = NULL;
        bs->map_len = 0;
        return false;
    }
    bs->map_len = target;
    return true;
}

static const bs_record_t *get_record(const block_store_t *bs, uint64_t idx) {
    if (!bs->map || idx >= bs->count) return NULL;
    size_t off = BS_HEADER + idx * BS_RECSZ;
    if (off + BS_RECSZ > bs->map_len) return NULL;
    return (const bs_record_t *)(bs->map + off);
}

/* Find the index for a block_number. Returns UINT64_MAX if not found. */
static uint64_t find_index(const block_store_t *bs, uint64_t block_number) {
    if (bs->count == 0) return UINT64_MAX;
    if (block_number < bs->lowest_block) return UINT64_MAX;

    uint64_t idx = block_number - bs->lowest_block;
    if (idx >= bs->count) return UINT64_MAX;

    /* Verify (handles gaps if they ever occur) */
    const bs_record_t *r = get_record(bs, idx);
    if (!r || r->block_number != block_number) return UINT64_MAX;
    return idx;
}

static bool write_header(block_store_t *bs) {
    bs_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic        = BS_MAGIC;
    hdr.version      = BS_VERSION;
    hdr.count        = bs->count;
    hdr.lowest_block = bs->lowest_block;

    return pwrite(bs->fd, &hdr, sizeof(hdr), 0) == sizeof(hdr);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

block_store_t *block_store_create(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return NULL;

    block_store_t *bs = calloc(1, sizeof(*bs));
    if (!bs) { close(fd); return NULL; }

    bs->fd   = fd;
    bs->path = strdup(path);

    /* Write empty header */
    if (ftruncate(fd, BS_HEADER) != 0 || !write_header(bs)) {
        close(fd);
        free(bs->path);
        free(bs);
        return NULL;
    }

    remap(bs);
    return bs;
}

block_store_t *block_store_open(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR);
    if (fd < 0) return NULL;

    /* Read header */
    bs_header_t hdr;
    if (pread(fd, &hdr, sizeof(hdr), 0) != sizeof(hdr)) {
        close(fd);
        return NULL;
    }

    if (hdr.magic != BS_MAGIC || hdr.version != BS_VERSION) {
        close(fd);
        return NULL;
    }

    /* Verify file size */
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return NULL; }

    size_t expected = file_size_for(hdr.count);
    if ((size_t)st.st_size < expected) {
        /* Truncated file — recover by using actual record count */
        uint64_t actual = ((uint64_t)st.st_size - BS_HEADER) / BS_RECSZ;
        hdr.count = actual;
    }

    block_store_t *bs = calloc(1, sizeof(*bs));
    if (!bs) { close(fd); return NULL; }

    bs->fd           = fd;
    bs->path         = strdup(path);
    bs->count        = hdr.count;
    bs->lowest_block = hdr.lowest_block;

    if (!remap(bs)) {
        close(fd);
        free(bs->path);
        free(bs);
        return NULL;
    }

    return bs;
}

void block_store_destroy(block_store_t *bs) {
    if (!bs) return;
    if (bs->map) munmap(bs->map, bs->map_len);
    if (bs->fd >= 0) close(bs->fd);
    free(bs->path);
    free(bs);
}

/* =========================================================================
 * Write
 * ========================================================================= */

bool block_store_put(block_store_t *bs,
                      uint64_t block_number,
                      const uint8_t block_hash[32],
                      const uint8_t parent_hash[32],
                      uint64_t timestamp) {
    if (!bs) return false;

    /* Set lowest_block on first insert */
    if (bs->count == 0) {
        bs->lowest_block = block_number;
    } else {
        /* Must be sequential */
        uint64_t expected = bs->lowest_block + bs->count;
        if (block_number != expected) return false;
    }

    bs_record_t rec;
    rec.block_number = block_number;
    memcpy(rec.block_hash, block_hash, 32);
    memcpy(rec.parent_hash, parent_hash, 32);
    rec.timestamp = timestamp;

    off_t off = (off_t)(BS_HEADER + bs->count * BS_RECSZ);

    /* Extend file if needed */
    off_t needed = off + BS_RECSZ;
    struct stat st;
    if (fstat(bs->fd, &st) == 0 && st.st_size < needed) {
        /* Grow in chunks (256 records) */
        off_t grow = (off_t)file_size_for(bs->count + 256);
        if (ftruncate(bs->fd, grow) != 0) return false;
    }

    ssize_t n = pwrite(bs->fd, &rec, BS_RECSZ, off);
    if (n != BS_RECSZ) return false;

    bs->count++;
    write_header(bs);

    /* Remap if we've exceeded current mapping */
    if ((size_t)needed > bs->map_len)
        remap(bs);

    return true;
}

void block_store_sync(block_store_t *bs) {
    if (bs && bs->fd >= 0)
        fdatasync(bs->fd);
}

/* =========================================================================
 * Read
 * ========================================================================= */

bool block_store_get_hash(const block_store_t *bs,
                           uint64_t block_number,
                           uint8_t out[32]) {
    uint64_t idx = find_index(bs, block_number);
    if (idx == UINT64_MAX) return false;
    const bs_record_t *r = get_record(bs, idx);
    if (!r) return false;
    memcpy(out, r->block_hash, 32);
    return true;
}

bool block_store_get_parent(const block_store_t *bs,
                             uint64_t block_number,
                             uint8_t out[32]) {
    uint64_t idx = find_index(bs, block_number);
    if (idx == UINT64_MAX) return false;
    const bs_record_t *r = get_record(bs, idx);
    if (!r) return false;
    memcpy(out, r->parent_hash, 32);
    return true;
}

uint64_t block_store_get_timestamp(const block_store_t *bs,
                                    uint64_t block_number) {
    uint64_t idx = find_index(bs, block_number);
    if (idx == UINT64_MAX) return 0;
    const bs_record_t *r = get_record(bs, idx);
    return r ? r->timestamp : 0;
}

uint64_t block_store_count(const block_store_t *bs) {
    return bs ? bs->count : 0;
}

uint64_t block_store_highest(const block_store_t *bs) {
    if (!bs || bs->count == 0) return 0;
    return bs->lowest_block + bs->count - 1;
}

uint64_t block_store_lowest(const block_store_t *bs) {
    if (!bs || bs->count == 0) return UINT64_MAX;
    return bs->lowest_block;
}

uint32_t block_store_fill_ring(const block_store_t *bs,
                                uint64_t head_block,
                                uint8_t hashes[256][32]) {
    if (!bs || bs->count == 0 || !hashes) return 0;

    memset(hashes, 0, 256 * 32);

    uint64_t lo = head_block > 255 ? head_block - 255 : 0;
    uint32_t filled = 0;

    for (uint64_t bn = lo; bn <= head_block; bn++) {
        uint64_t idx = find_index(bs, bn);
        if (idx == UINT64_MAX) continue;
        const bs_record_t *r = get_record(bs, idx);
        if (!r) continue;
        memcpy(hashes[bn % 256], r->block_hash, 32);
        filled++;
    }

    return filled;
}

bool block_store_truncate(block_store_t *bs, uint64_t keep_up_to) {
    if (!bs || bs->count == 0) return true;

    if (keep_up_to >= bs->lowest_block + bs->count - 1)
        return true;  /* nothing to truncate */

    if (keep_up_to < bs->lowest_block) {
        /* Truncate everything */
        bs->count = 0;
    } else {
        bs->count = keep_up_to - bs->lowest_block + 1;
    }

    /* Truncate file */
    off_t new_size = (off_t)file_size_for(bs->count);
    if (ftruncate(bs->fd, new_size) != 0) return false;

    write_header(bs);
    remap(bs);
    return true;
}
