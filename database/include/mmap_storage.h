/*
 * mmap Storage Backend
 *
 * Memory-mapped file storage for the persistent ART tree.
 * Pages are stored at fixed offsets (page_id * PAGE_SIZE) in a single file.
 * Page 0 is reserved for the file header (matches NULL_NODE_REF sentinel).
 *
 * Crash safety: page 0 contains two 64-byte header slots (shadow paging).
 * Checkpoint writes to the inactive slot, syncs, then flips.  On recovery
 * the slot with the higher valid checkpoint_num is chosen.  If one slot is
 * corrupt (torn write / partial checkpoint), the other is used.
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
#define MMAP_FORMAT_VERSION 2
#define MMAP_DEFAULT_INITIAL_PAGES 16384  /* 64 MB */

/**
 * Header slot — 64 bytes.  Two of these live in page 0.
 *
 * Byte layout (all little-endian on x86):
 *   [0-7]   magic          MMAP_MAGIC
 *   [8-11]  version        MMAP_FORMAT_VERSION (2)
 *   [12-15] page_size      PAGE_SIZE
 *   [16-23] next_page_id   Allocator cursor
 *   [24-31] root_page_id   Root node page
 *   [32-35] root_offset    Root node offset within page
 *   [36-39] checkpoint_num Monotonic counter
 *   [40-47] tree_size      Number of key-value entries
 *   [48-55] key_size       Fixed key size (20 or 32)
 *   [56-59] checksum       CRC32 of bytes 0-55
 *   [60-63] _pad           Reserved (zero)
 */
typedef struct {
    uint64_t magic;           /* MMAP_MAGIC */
    uint32_t version;         /* MMAP_FORMAT_VERSION */
    uint32_t page_size;       /* Always PAGE_SIZE */
    uint64_t next_page_id;    /* Next page to allocate */
    uint64_t root_page_id;    /* Root node page */
    uint32_t root_offset;     /* Root node offset within page */
    uint32_t checkpoint_num;  /* Monotonic checkpoint counter */
    uint64_t tree_size;       /* Number of key-value entries */
    uint64_t key_size;        /* Fixed key size (20 or 32) */
    uint32_t checksum;        /* CRC32 of first 56 bytes */
    uint8_t  _pad[4];         /* Align to 64 bytes */
} mmap_header_slot_t;

/* Page 0 layout — two header slots + reserved space */
typedef struct {
    mmap_header_slot_t slots[2];       /* 128 bytes */
    uint8_t reserved[PAGE_SIZE - 128]; /* 3968 bytes */
} mmap_header_page_t;

/* Legacy v1 header — kept for migration detection only */
typedef struct {
    uint64_t magic;
    uint32_t version;         /* == 1 */
    uint32_t page_size;
    uint64_t next_page_id;
    uint64_t root_page_id;
    uint32_t root_offset;
    uint32_t padding;
    uint64_t tree_size;
    uint64_t key_size;
} mmap_header_v1_t;

/**
 * mmap storage instance.
 */
typedef struct mmap_storage {
    int fd;                        /* File descriptor */
    uint8_t *base;                 /* mmap base pointer */
    size_t mapped_size;            /* Current mapped region size (bytes) */
    uint64_t next_page_id;         /* Next page to allocate */
    int active_slot;               /* 0 or 1 — current valid header slot */
    pthread_rwlock_t resize_lock;  /* Protects base pointer during mremap */
    char *path;                    /* File path (for error messages) */

    /* Dirty page tracking for incremental checkpoint.
     * 1 bit per page — set when a page is written, cleared on checkpoint.
     * Checkpoint only syncs pages with set bits instead of the entire file. */
    uint64_t *dirty_bitmap;        /* Bitmap: 1 bit per page */
    size_t dirty_bitmap_words;     /* Size in uint64_t words */
} mmap_storage_t;

/**
 * Mark a page as dirty (modified since last checkpoint).
 * Must be called whenever page data is written.
 * Page 0 (header) is skipped — it has its own explicit sync.
 */
static inline void mmap_storage_mark_dirty(mmap_storage_t *ms, uint64_t page_id) {
    if (page_id > 0 && ms->dirty_bitmap &&
        page_id / 64 < ms->dirty_bitmap_words) {
        ms->dirty_bitmap[page_id / 64] |= (1ULL << (page_id % 64));
    }
}

/**
 * Count the number of dirty pages (set bits in the bitmap).
 * Used for testing and diagnostics.
 */
static inline size_t mmap_storage_dirty_count(const mmap_storage_t *ms) {
    if (!ms->dirty_bitmap) return 0;
    size_t count = 0;
    for (size_t w = 0; w < ms->dirty_bitmap_words; w++) {
        count += (size_t)__builtin_popcountll(ms->dirty_bitmap[w]);
    }
    return count;
}

/* ========================================================================== */
/* Lifecycle                                                                   */
/* ========================================================================== */

/**
 * Create a new mmap storage file.
 *
 * Creates the file, ftruncates to initial_pages * PAGE_SIZE, mmaps it,
 * and writes both header slots to page 0.  next_page_id starts at 1.
 *
 * @param path       File path (created or truncated)
 * @param initial_pages  Initial file size in pages (0 = use default 16384)
 * @return Storage instance, or NULL on failure
 */
mmap_storage_t *mmap_storage_create(const char *path, size_t initial_pages);

/**
 * Open an existing mmap storage file.
 *
 * Maps the file, validates both header slots (picks highest valid
 * checkpoint_num), and restores next_page_id.  Automatically migrates
 * v1 headers to v2 shadow format.
 *
 * @param path  File path (must exist)
 * @return Storage instance, or NULL on failure
 */
mmap_storage_t *mmap_storage_open(const char *path);

/**
 * Destroy mmap storage — syncs, unmaps, closes fd, frees memory.
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

/**
 * Pre-grow the mapped region to hold at least total_pages pages.
 * Prevents resize_lock wrlock contention during bulk operations.
 */
void mmap_storage_ensure_capacity(mmap_storage_t *ms, uint64_t total_pages);

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
 * Crash-safe checkpoint using shadow header.
 *
 * 1. msync data pages (everything after page 0)
 * 2. Write inactive header slot with new root + incremented checkpoint_num + CRC
 * 3. msync header page (page 0 only)
 * 4. Flip active_slot
 *
 * @return true on success
 */
bool mmap_storage_checkpoint(mmap_storage_t *ms,
                              uint64_t root_page_id,
                              uint32_t root_offset,
                              uint64_t tree_size,
                              uint64_t key_size);

/**
 * Load tree metadata from the active header slot.
 *
 * @return true if header is valid, false if both slots corrupt
 */
bool mmap_storage_load_header(mmap_storage_t *ms,
                              uint64_t *root_page_id,
                              uint32_t *root_offset,
                              uint64_t *tree_size,
                              uint64_t *key_size);

#endif /* MMAP_STORAGE_H */
