/*
 * mmap Storage Backend
 *
 * Memory-mapped file storage for the persistent ART tree.
 * Pages are stored at fixed offsets (page_id * PAGE_SIZE) in a single file.
 * Page 0 is reserved for the file header (matches NULL_NODE_REF sentinel).
 *
 * Advantages over page_manager + buffer_pool:
 * - No user-space cache — kernel VM handles caching
 * - No pin/unpin overhead
 * - No compression overhead (pages stored raw)
 * - Direct pointer arithmetic for page access
 *
 * Growth: file doubles in size via ftruncate + mremap(MREMAP_MAYMOVE).
 * pthread_rwlock_t protects the base pointer during resize.
 */

#ifndef MMAP_STORAGE_H
#define MMAP_STORAGE_H

#include "page_types.h"  /* PAGE_SIZE, page_t */

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>

/* File header magic: "ARTMMAP\0" */
#define MMAP_MAGIC 0x4152544D4D415000ULL
#define MMAP_FORMAT_VERSION 1
#define MMAP_DEFAULT_INITIAL_PAGES 16384  /* 64 MB */

/**
 * File header — occupies page 0 (4096 bytes).
 * Stores allocator state and tree metadata for reopen.
 */
typedef struct {
    uint64_t magic;           /* MMAP_MAGIC */
    uint32_t version;         /* MMAP_FORMAT_VERSION */
    uint32_t page_size;       /* Always PAGE_SIZE (4096) */
    uint64_t next_page_id;    /* Next page to allocate */
    uint64_t root_page_id;    /* Root node page */
    uint32_t root_offset;     /* Root node offset within page */
    uint32_t padding;
    uint64_t tree_size;       /* Number of key-value entries */
    uint64_t key_size;        /* Fixed key size (20 or 32) */
    uint8_t reserved[PAGE_SIZE - 56]; /* Pad to PAGE_SIZE */
} mmap_header_t;

/**
 * mmap storage instance.
 */
typedef struct mmap_storage {
    int fd;                        /* File descriptor */
    uint8_t *base;                 /* mmap base pointer */
    size_t mapped_size;            /* Current mapped region size (bytes) */
    uint64_t next_page_id;         /* Next page to allocate */
    pthread_rwlock_t resize_lock;  /* Protects base pointer during mremap */
    char *path;                    /* File path (for error messages) */
} mmap_storage_t;

/* ========================================================================== */
/* Lifecycle                                                                   */
/* ========================================================================== */

/**
 * Create a new mmap storage file.
 *
 * Creates the file, ftruncates to initial_pages * PAGE_SIZE, mmaps it,
 * and writes the header to page 0. next_page_id starts at 1.
 *
 * @param path       File path (created or truncated)
 * @param initial_pages  Initial file size in pages (0 = use default 16384)
 * @return Storage instance, or NULL on failure
 */
mmap_storage_t *mmap_storage_create(const char *path, size_t initial_pages);

/**
 * Open an existing mmap storage file.
 *
 * Maps the file, validates the header, and restores next_page_id.
 *
 * @param path  File path (must exist)
 * @return Storage instance, or NULL on failure
 */
mmap_storage_t *mmap_storage_open(const char *path);

/**
 * Destroy mmap storage — syncs, saves header, unmaps, closes fd, frees memory.
 */
void mmap_storage_destroy(mmap_storage_t *ms);

/* ========================================================================== */
/* Page Access                                                                 */
/* ========================================================================== */

/**
 * Get a direct pointer to a page in the mapped region.
 *
 * Caller must hold resize_lock (rdlock) to ensure base pointer stability.
 * The returned pointer is valid until the next mremap (resize).
 *
 * @param ms       Storage instance
 * @param page_id  Page ID (must be < next_page_id)
 * @return Pointer to page_t at the given page_id
 */
static inline page_t *mmap_storage_get_page(mmap_storage_t *ms, uint64_t page_id) {
    return (page_t *)(ms->base + page_id * PAGE_SIZE);
}

/**
 * Allocate a new page (bump allocator).
 *
 * Returns next_page_id and increments it. If the file needs to grow,
 * calls ftruncate + mremap under the resize write-lock.
 *
 * @param ms  Storage instance
 * @return Allocated page ID, or 0 on failure
 */
uint64_t mmap_storage_alloc_page(mmap_storage_t *ms);

/* ========================================================================== */
/* Persistence                                                                 */
/* ========================================================================== */

/**
 * Sync all mapped pages to disk (msync MS_SYNC).
 *
 * @return true on success
 */
bool mmap_storage_sync(mmap_storage_t *ms);

/**
 * Save tree metadata to the file header (page 0).
 */
void mmap_storage_save_header(mmap_storage_t *ms,
                              uint64_t root_page_id,
                              uint32_t root_offset,
                              uint64_t tree_size,
                              uint64_t key_size);

/**
 * Load tree metadata from the file header (page 0).
 *
 * @return true if header is valid, false if magic/version mismatch
 */
bool mmap_storage_load_header(mmap_storage_t *ms,
                              uint64_t *root_page_id,
                              uint32_t *root_offset,
                              uint64_t *tree_size,
                              uint64_t *key_size);

#endif /* MMAP_STORAGE_H */
