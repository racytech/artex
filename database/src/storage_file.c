#include "storage_file.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define HEADER_SIZE   16
#define MAGIC         "STOR"
#define VERSION       1
#define INITIAL_CAP   (64ULL * 1024 * 1024)  /* 64 MB initial file */

struct storage_file {
    int       fd;
    uint8_t  *base;       /* mmap'd region */
    uint64_t  file_size;  /* current mmap size */
    uint64_t  used;       /* bytes written (bump pointer) */
};

static bool ensure_capacity(storage_file_t *sf, uint64_t needed) {
    if (sf->used + needed <= sf->file_size) return true;

    uint64_t new_size = sf->file_size;
    while (new_size < sf->used + needed) new_size *= 2;

    if (ftruncate(sf->fd, new_size) < 0) return false;

    if (sf->base) munmap(sf->base, sf->file_size);
    sf->base = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, sf->fd, 0);
    if (sf->base == MAP_FAILED) { sf->base = NULL; return false; }
    sf->file_size = new_size;
    return true;
}

storage_file_t *storage_file_create(const char *path) {
    if (!path) return NULL;

    storage_file_t *sf = calloc(1, sizeof(*sf));
    if (!sf) return NULL;

    sf->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (sf->fd < 0) { free(sf); return NULL; }

    struct stat st;
    fstat(sf->fd, &st);

    if (st.st_size < HEADER_SIZE) {
        /* New file — write header */
        if (ftruncate(sf->fd, INITIAL_CAP) < 0) {
            close(sf->fd); free(sf); return NULL;
        }
        sf->file_size = INITIAL_CAP;
        sf->base = mmap(NULL, sf->file_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, sf->fd, 0);
        if (sf->base == MAP_FAILED) {
            close(sf->fd); free(sf); return NULL;
        }
        memcpy(sf->base, MAGIC, 4);
        uint32_t ver = VERSION;
        memcpy(sf->base + 4, &ver, 4);
        memset(sf->base + 8, 0, 8);
        sf->used = HEADER_SIZE;
    } else {
        /* Existing file — validate header */
        sf->file_size = st.st_size;
        sf->base = mmap(NULL, sf->file_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, sf->fd, 0);
        if (sf->base == MAP_FAILED) {
            close(sf->fd); free(sf); return NULL;
        }
        if (memcmp(sf->base, MAGIC, 4) != 0) {
            fprintf(stderr, "storage_file: bad magic\n");
            munmap(sf->base, sf->file_size);
            close(sf->fd); free(sf); return NULL;
        }
        /* Read used offset from reserved field (bytes 8-15) */
        memcpy(&sf->used, sf->base + 8, 8);
        if (sf->used < HEADER_SIZE || sf->used > sf->file_size)
            sf->used = HEADER_SIZE;
    }

    return sf;
}

void storage_file_destroy(storage_file_t *sf) {
    if (!sf) return;
    /* Persist used offset */
    if (sf->base) {
        memcpy(sf->base + 8, &sf->used, 8);
        msync(sf->base, sf->used, MS_SYNC);
        munmap(sf->base, sf->file_size);
    }
    if (sf->fd >= 0) close(sf->fd);
    free(sf);
}

uint64_t storage_file_write_section(storage_file_t *sf,
                                     const uint8_t *slots,
                                     uint32_t slot_count) {
    if (!sf || !slots || slot_count == 0) return UINT64_MAX;

    uint64_t section_size = (uint64_t)slot_count * STORAGE_SLOT_SIZE;
    if (!ensure_capacity(sf, section_size)) return UINT64_MAX;

    uint64_t offset = sf->used;
    memcpy(sf->base + offset, slots, section_size);
    sf->used += section_size;

    /* Update used in header */
    memcpy(sf->base + 8, &sf->used, 8);

    return offset;
}

bool storage_file_read_section(const storage_file_t *sf,
                                uint64_t offset, uint32_t slot_count,
                                uint8_t *out) {
    if (!sf || !sf->base || !out || slot_count == 0) return false;

    uint64_t section_size = (uint64_t)slot_count * STORAGE_SLOT_SIZE;
    if (offset + section_size > sf->used) return false;

    memcpy(out, sf->base + offset, section_size);
    return true;
}

void storage_file_reset(storage_file_t *sf) {
    if (!sf) return;
    sf->used = HEADER_SIZE;
    memcpy(sf->base + 8, &sf->used, 8);
}

uint64_t storage_file_size(const storage_file_t *sf) {
    return sf ? sf->used : 0;
}
