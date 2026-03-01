#include "../include/checkpoint.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <nmmintrin.h>

// ============================================================================
// CRC32C — hardware-accelerated via SSE4.2 (Castagnoli polynomial)
// ============================================================================

uint32_t crc32_update(uint32_t crc, const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t c = ~(uint64_t)crc;

    // Process 8 bytes at a time
    while (len >= 8) {
        uint64_t val;
        memcpy(&val, p, 8);
        c = _mm_crc32_u64(c, val);
        p += 8;
        len -= 8;
    }

    // Process remaining bytes
    while (len > 0) {
        c = _mm_crc32_u8((uint32_t)c, *p);
        p++;
        len--;
    }

    return ~(uint32_t)c;
}

// ============================================================================
// Buffered writer
// ============================================================================

#define WRITE_BUF_SIZE (64 * 1024)

typedef struct {
    int fd;
    uint8_t buf[WRITE_BUF_SIZE];
    size_t pos;
    uint32_t crc;
    bool error;
} buffered_writer_t;

static void bw_init(buffered_writer_t *bw, int fd) {
    bw->fd = fd;
    bw->pos = 0;
    bw->crc = 0;
    bw->error = false;
}

static void bw_flush(buffered_writer_t *bw) {
    if (bw->pos > 0 && !bw->error) {
        ssize_t w = write(bw->fd, bw->buf, bw->pos);
        if (w != (ssize_t)bw->pos) bw->error = true;
        bw->pos = 0;
    }
}

static void bw_write(buffered_writer_t *bw, const void *data, size_t len) {
    bw->crc = crc32_update(bw->crc, data, len);
    const uint8_t *p = (const uint8_t *)data;
    while (len > 0) {
        size_t space = WRITE_BUF_SIZE - bw->pos;
        size_t chunk = len < space ? len : space;
        memcpy(bw->buf + bw->pos, p, chunk);
        bw->pos += chunk;
        p += chunk;
        len -= chunk;
        if (bw->pos == WRITE_BUF_SIZE) bw_flush(bw);
    }
}

// ============================================================================
// checkpoint_meta_write (slim sidecar: free list + code entries only)
// ============================================================================

bool checkpoint_meta_write(const char *path, uint64_t block_number,
                           const state_store_t *store,
                           const code_store_t *code) {
    if (!path || !store) return false;

    size_t path_len = strlen(path);
    char *tmp_path = malloc(path_len + 5);
    if (!tmp_path) return false;
    snprintf(tmp_path, path_len + 5, "%s.tmp", path);

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        free(tmp_path);
        return false;
    }

    checkpoint_meta_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, CHECKPOINT_META_MAGIC, 8);
    hdr.version = CHECKPOINT_META_VERSION;
    hdr.block_number = block_number;
    hdr.next_slot = state_store_next_slot(store);
    hdr.free_count = state_store_free_count(store);
    hdr.code_count = code ? code_store_count(code) : 0;
    hdr.code_file_size = code ? code_store_file_size(code) : 0;

    /* Write placeholder header */
    if (write(fd, &hdr, CHECKPOINT_HEADER_SIZE) != CHECKPOINT_HEADER_SIZE) {
        close(fd);
        unlink(tmp_path);
        free(tmp_path);
        return false;
    }

    buffered_writer_t bw;
    bw_init(&bw, fd);

    /* Write free list */
    const uint32_t *free_list = state_store_free_list_ptr(store);
    uint32_t free_count = state_store_free_count(store);
    if (free_count > 0 && free_list) {
        bw_write(&bw, free_list, free_count * sizeof(uint32_t));
    }

    /* Write code entries */
    if (code) {
        uint32_t code_count = code_store_count(code);
        for (uint32_t i = 0; i < code_count; i++) {
            uint64_t offset;
            uint32_t length;
            if (code_store_get_entry(code, i, &offset, &length)) {
                bw_write(&bw, &offset, sizeof(offset));
                bw_write(&bw, &length, sizeof(length));
            }
        }
    }

    bw_flush(&bw);
    if (bw.error) {
        close(fd);
        unlink(tmp_path);
        free(tmp_path);
        return false;
    }

    /* Rewrite header with CRC */
    hdr.checksum = bw.crc;
    if (pwrite(fd, &hdr, CHECKPOINT_HEADER_SIZE, 0) != CHECKPOINT_HEADER_SIZE) {
        close(fd);
        unlink(tmp_path);
        free(tmp_path);
        return false;
    }

    fdatasync(fd);
    close(fd);

    if (rename(tmp_path, path) != 0) {
        unlink(tmp_path);
        free(tmp_path);
        return false;
    }

    free(tmp_path);
    return true;
}

// ============================================================================
// checkpoint_meta_load (slim sidecar: free list + code entries only)
// ============================================================================

bool checkpoint_meta_load(const char *path, uint64_t *out_block_number,
                          state_store_t *store,
                          code_store_t *code) {
    if (!path || !store) return false;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return false;

    uint8_t hdr_buf[CHECKPOINT_HEADER_SIZE];
    if (read(fd, hdr_buf, CHECKPOINT_HEADER_SIZE) != CHECKPOINT_HEADER_SIZE) {
        close(fd);
        return false;
    }

    checkpoint_meta_header_t hdr;
    memcpy(&hdr, hdr_buf, sizeof(hdr));

    if (memcmp(hdr.magic, CHECKPOINT_META_MAGIC, 8) != 0 ||
        hdr.version != CHECKPOINT_META_VERSION) {
        close(fd);
        return false;
    }

    uint32_t crc = 0;

    /* Read free list */
    if (hdr.free_count > 0) {
        size_t fl_bytes = (size_t)hdr.free_count * sizeof(uint32_t);
        uint32_t *fl_buf = malloc(fl_bytes);
        if (!fl_buf) {
            close(fd);
            return false;
        }
        ssize_t r = read(fd, fl_buf, fl_bytes);
        if (r != (ssize_t)fl_bytes) {
            free(fl_buf);
            close(fd);
            return false;
        }
        crc = crc32_update(crc, fl_buf, fl_bytes);
        state_store_restore_free_list(store, fl_buf, hdr.free_count);
        free(fl_buf);
    }

    /* Restore next_slot */
    state_store_set_next_slot(store, hdr.next_slot);

    /* Read code entries */
    if (hdr.code_count > 0 && code) {
        for (uint32_t i = 0; i < hdr.code_count; i++) {
            uint64_t offset;
            uint32_t length;
            ssize_t r = read(fd, &offset, sizeof(offset));
            if (r != sizeof(offset)) {
                close(fd);
                return false;
            }
            crc = crc32_update(crc, &offset, sizeof(offset));

            r = read(fd, &length, sizeof(length));
            if (r != sizeof(length)) {
                close(fd);
                return false;
            }
            crc = crc32_update(crc, &length, sizeof(length));

            code_store_add_entry(code, offset, length);
        }
    }

    close(fd);

    /* Verify CRC */
    if (crc != hdr.checksum) return false;

    if (out_block_number) *out_block_number = hdr.block_number;
    return true;
}
