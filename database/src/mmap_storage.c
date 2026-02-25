/*
 * mmap Storage Backend — Implementation
 *
 * Single memory-mapped file with fixed-size 4KB pages.
 * Pages addressed by page_id * PAGE_SIZE.
 * Page 0 reserved for header (matches NULL_NODE_REF sentinel).
 *
 * Growth: ftruncate(2x) + mremap(MREMAP_MAYMOVE) under write-lock.
 */

#define _GNU_SOURCE  /* mremap */

#include "mmap_storage.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* ========================================================================== */
/* Internal helpers                                                            */
/* ========================================================================== */

/**
 * Grow the mapped region to new_size bytes.
 * Caller must hold resize_lock as write-lock.
 */
static bool mmap_storage_grow(mmap_storage_t *ms, size_t new_size) {
    /* Extend the file */
    if (ftruncate(ms->fd, (off_t)new_size) != 0) {
        LOG_ERROR("mmap_storage_grow: ftruncate to %zu failed: %s",
                  new_size, strerror(errno));
        return false;
    }

    /* Remap — MREMAP_MAYMOVE allows the kernel to relocate the mapping */
    void *new_base = mremap(ms->base, ms->mapped_size, new_size, MREMAP_MAYMOVE);
    if (new_base == MAP_FAILED) {
        LOG_ERROR("mmap_storage_grow: mremap from %zu to %zu failed: %s",
                  ms->mapped_size, new_size, strerror(errno));
        return false;
    }

    ms->base = (uint8_t *)new_base;
    ms->mapped_size = new_size;

    LOG_DEBUG("mmap_storage_grow: resized to %zu bytes (%zu pages)",
              new_size, new_size / PAGE_SIZE);
    return true;
}

/**
 * Write the file header to page 0.
 */
static void write_header(mmap_storage_t *ms,
                          uint64_t root_page_id, uint32_t root_offset,
                          uint64_t tree_size, uint64_t key_size) {
    mmap_header_t *hdr = (mmap_header_t *)ms->base;
    hdr->magic = MMAP_MAGIC;
    hdr->version = MMAP_FORMAT_VERSION;
    hdr->page_size = PAGE_SIZE;
    hdr->next_page_id = ms->next_page_id;
    hdr->root_page_id = root_page_id;
    hdr->root_offset = root_offset;
    hdr->padding = 0;
    hdr->tree_size = tree_size;
    hdr->key_size = key_size;
}

/* ========================================================================== */
/* Lifecycle                                                                   */
/* ========================================================================== */

mmap_storage_t *mmap_storage_create(const char *path, size_t initial_pages) {
    if (!path) {
        LOG_ERROR("mmap_storage_create: path is NULL");
        return NULL;
    }

    if (initial_pages == 0) {
        initial_pages = MMAP_DEFAULT_INITIAL_PAGES;
    }

    /* Minimum: 2 pages (header + at least 1 data page) */
    if (initial_pages < 2) {
        initial_pages = 2;
    }

    size_t file_size = initial_pages * PAGE_SIZE;

    /* Create/truncate file */
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_ERROR("mmap_storage_create: open(%s) failed: %s", path, strerror(errno));
        return NULL;
    }

    /* Set file size */
    if (ftruncate(fd, (off_t)file_size) != 0) {
        LOG_ERROR("mmap_storage_create: ftruncate(%zu) failed: %s",
                  file_size, strerror(errno));
        close(fd);
        return NULL;
    }

    /* Memory-map the file */
    void *base = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        LOG_ERROR("mmap_storage_create: mmap(%zu) failed: %s",
                  file_size, strerror(errno));
        close(fd);
        return NULL;
    }

    /* Allocate storage struct */
    mmap_storage_t *ms = calloc(1, sizeof(mmap_storage_t));
    if (!ms) {
        munmap(base, file_size);
        close(fd);
        return NULL;
    }

    ms->fd = fd;
    ms->base = (uint8_t *)base;
    ms->mapped_size = file_size;
    ms->next_page_id = 1;  /* Page 0 = header */
    ms->path = strdup(path);

    if (pthread_rwlock_init(&ms->resize_lock, NULL) != 0) {
        LOG_ERROR("mmap_storage_create: pthread_rwlock_init failed");
        munmap(base, file_size);
        close(fd);
        free(ms->path);
        free(ms);
        return NULL;
    }

    /* Write initial header (empty tree) */
    write_header(ms, 0, 0, 0, 0);

    LOG_INFO("Created mmap storage: %s (%zu pages, %zu MB)",
             path, initial_pages, file_size / (1024 * 1024));

    return ms;
}

mmap_storage_t *mmap_storage_open(const char *path) {
    if (!path) {
        LOG_ERROR("mmap_storage_open: path is NULL");
        return NULL;
    }

    int fd = open(path, O_RDWR, 0644);
    if (fd < 0) {
        LOG_ERROR("mmap_storage_open: open(%s) failed: %s", path, strerror(errno));
        return NULL;
    }

    /* Get file size */
    struct stat st;
    if (fstat(fd, &st) != 0) {
        LOG_ERROR("mmap_storage_open: fstat failed: %s", strerror(errno));
        close(fd);
        return NULL;
    }

    size_t file_size = (size_t)st.st_size;
    if (file_size < PAGE_SIZE) {
        LOG_ERROR("mmap_storage_open: file too small (%zu bytes)", file_size);
        close(fd);
        return NULL;
    }

    /* Memory-map */
    void *base = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        LOG_ERROR("mmap_storage_open: mmap(%zu) failed: %s",
                  file_size, strerror(errno));
        close(fd);
        return NULL;
    }

    /* Validate header */
    const mmap_header_t *hdr = (const mmap_header_t *)base;
    if (hdr->magic != MMAP_MAGIC) {
        LOG_ERROR("mmap_storage_open: bad magic (got 0x%lx, expected 0x%lx)",
                  hdr->magic, MMAP_MAGIC);
        munmap(base, file_size);
        close(fd);
        return NULL;
    }
    if (hdr->version != MMAP_FORMAT_VERSION) {
        LOG_ERROR("mmap_storage_open: unsupported version %u (expected %u)",
                  hdr->version, MMAP_FORMAT_VERSION);
        munmap(base, file_size);
        close(fd);
        return NULL;
    }
    if (hdr->page_size != PAGE_SIZE) {
        LOG_ERROR("mmap_storage_open: page_size mismatch (%u vs %u)",
                  hdr->page_size, PAGE_SIZE);
        munmap(base, file_size);
        close(fd);
        return NULL;
    }

    /* Allocate struct */
    mmap_storage_t *ms = calloc(1, sizeof(mmap_storage_t));
    if (!ms) {
        munmap(base, file_size);
        close(fd);
        return NULL;
    }

    ms->fd = fd;
    ms->base = (uint8_t *)base;
    ms->mapped_size = file_size;
    ms->next_page_id = hdr->next_page_id;
    ms->path = strdup(path);

    if (pthread_rwlock_init(&ms->resize_lock, NULL) != 0) {
        munmap(base, file_size);
        close(fd);
        free(ms->path);
        free(ms);
        return NULL;
    }

    LOG_INFO("Opened mmap storage: %s (%zu pages, next_page_id=%lu)",
             path, file_size / PAGE_SIZE, ms->next_page_id);

    return ms;
}

void mmap_storage_destroy(mmap_storage_t *ms) {
    if (!ms) return;

    /* Sync to disk */
    if (ms->base && ms->mapped_size > 0) {
        msync(ms->base, ms->mapped_size, MS_SYNC);
        munmap(ms->base, ms->mapped_size);
    }

    if (ms->fd >= 0) {
        close(ms->fd);
    }

    pthread_rwlock_destroy(&ms->resize_lock);
    free(ms->path);
    free(ms);
}

/* ========================================================================== */
/* Page Allocation                                                             */
/* ========================================================================== */

uint64_t mmap_storage_alloc_page(mmap_storage_t *ms) {
    if (!ms) return 0;

    uint64_t page_id = ms->next_page_id;
    size_t required_size = (page_id + 1) * PAGE_SIZE;

    /* Grow if needed */
    if (required_size > ms->mapped_size) {
        /* Double the size (or at least reach required_size) */
        size_t new_size = ms->mapped_size * 2;
        if (new_size < required_size) {
            new_size = required_size;
        }

        /* Take write-lock for resize (blocks all readers briefly) */
        pthread_rwlock_wrlock(&ms->resize_lock);

        /* Double-check after acquiring lock */
        if (required_size > ms->mapped_size) {
            if (!mmap_storage_grow(ms, new_size)) {
                pthread_rwlock_unlock(&ms->resize_lock);
                return 0;
            }
        }

        pthread_rwlock_unlock(&ms->resize_lock);
    }

    ms->next_page_id = page_id + 1;

    /* Zero the new page for clean state */
    memset(ms->base + page_id * PAGE_SIZE, 0, PAGE_SIZE);

    return page_id;
}

/* ========================================================================== */
/* Persistence                                                                 */
/* ========================================================================== */

bool mmap_storage_sync(mmap_storage_t *ms) {
    if (!ms || !ms->base) return false;

    if (msync(ms->base, ms->mapped_size, MS_SYNC) != 0) {
        LOG_ERROR("mmap_storage_sync: msync failed: %s", strerror(errno));
        return false;
    }
    return true;
}

void mmap_storage_save_header(mmap_storage_t *ms,
                              uint64_t root_page_id,
                              uint32_t root_offset,
                              uint64_t tree_size,
                              uint64_t key_size) {
    if (!ms || !ms->base) return;
    write_header(ms, root_page_id, root_offset, tree_size, key_size);
}

bool mmap_storage_load_header(mmap_storage_t *ms,
                              uint64_t *root_page_id,
                              uint32_t *root_offset,
                              uint64_t *tree_size,
                              uint64_t *key_size) {
    if (!ms || !ms->base) return false;

    const mmap_header_t *hdr = (const mmap_header_t *)ms->base;
    if (hdr->magic != MMAP_MAGIC || hdr->version != MMAP_FORMAT_VERSION) {
        return false;
    }

    if (root_page_id) *root_page_id = hdr->root_page_id;
    if (root_offset) *root_offset = hdr->root_offset;
    if (tree_size) *tree_size = hdr->tree_size;
    if (key_size) *key_size = hdr->key_size;

    return true;
}
