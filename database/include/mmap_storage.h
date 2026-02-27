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
 * Growth: a large virtual address range is reserved at init (up to 1 TiB,
 * PROT_NONE). The file is mapped with MAP_FIXED at the start of that range.
 * Growth extends the file and maps the new portion — base never moves.
 * No resize_lock needed; all pointers into the mapping remain stable.
 */

#ifndef MMAP_STORAGE_H
#define MMAP_STORAGE_H

#include "page_types.h"  /* PAGE_SIZE, page_t */

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>  /* still needed for other locks (write_lock, mvcc) */

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
    uint8_t *base;                 /* mmap base pointer (stable — never moves) */
    size_t mapped_size;            /* Current file-backed mapped region (bytes) */
    size_t reserved_size;          /* Total reserved VA space (bytes) */
    uint64_t next_page_id;         /* Next page to allocate */
    int active_slot;               /* 0 or 1 — current valid header slot */
    char *path;                    /* File path (for error messages) */

    /* Dirty page tracking for incremental checkpoint.
     * 1 bit per page — set when a page is written, cleared on checkpoint.
     * Checkpoint only syncs pages with set bits instead of the entire file. */
    uint64_t *dirty_bitmap;        /* Bitmap: 1 bit per page */
    size_t dirty_bitmap_words;     /* Size in uint64_t words */
    size_t flush_cursor;           /* Word index for incremental flush */
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
 * The base pointer is stable (fixed VA reservation), so the returned
 * pointer remains valid for the lifetime of the storage instance.
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
 * extends the file and maps the new portion into the reserved VA space.
 *
 * @param ms  Storage instance
 * @return Allocated page ID, or 0 on failure
 */
uint64_t mmap_storage_alloc_page(mmap_storage_t *ms);

/**
 * Pre-grow the mapped region to hold at least total_pages pages.
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
 * Start non-blocking writeback of dirty pages.
 *
 * Uses sync_file_range(SYNC_FILE_RANGE_WRITE) on Linux to tell the kernel
 * to start flushing dirty pages to disk without waiting.  Call this after
 * each transaction commit so that by checkpoint time, most pages are already
 * on disk and the blocking sync is fast.  No-op on non-Linux platforms.
 */
void mmap_storage_start_writeback(mmap_storage_t *ms);

/**
 * Incrementally flush dirty pages to disk.
 *
 * Scans the dirty bitmap starting from an internal cursor, msyncs up to
 * max_pages worth of dirty pages (coalescing contiguous ranges), clears
 * their dirty bits, and advances the cursor.  Wraps around at the end.
 *
 * Unlike start_writeback (non-blocking hint), this call blocks until the
 * pages are on stable storage.  Call after each transaction commit to
 * spread I/O and keep checkpoint residual small.
 *
 * Does NOT update the header — that is only done in mmap_storage_checkpoint().
 *
 * @return Number of pages actually synced.
 */
size_t mmap_storage_flush_dirty_batch(mmap_storage_t *ms, size_t max_pages);

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

/**
 * Truncate the storage file, releasing tail pages.
 *
 * Remaps the tail region back to PROT_NONE (within the VA reservation)
 * and ftruncates the file.  Caller must ensure no live data exists
 * beyond new_page_count.
 *
 * @param ms             Storage instance
 * @param new_page_count Number of pages to keep (file shrinks to this * PAGE_SIZE)
 * @return true on success
 */
bool mmap_storage_truncate(mmap_storage_t *ms, uint64_t new_page_count);

#endif /* MMAP_STORAGE_H */
