/*
 * mmap Storage Backend — Implementation
 *
 * Single memory-mapped file with fixed-size 4KB pages.
 * Pages addressed by page_id * PAGE_SIZE.
 * Page 0 reserved for header (matches NULL_NODE_REF sentinel).
 *
 * Crash safety: two 64-byte header slots in page 0 (shadow paging).
 * Checkpoint does a two-phase msync:
 *   1. msync data pages  (guarantees CoW tree nodes on disk)
 *   2. write inactive slot + msync header page
 * Recovery picks the slot with higher valid checkpoint_num.
 *
 * Growth: a large virtual address range is reserved upfront (up to 1 TiB).
 * The file is overlaid at the start with MAP_FIXED.  Growth extends the file
 * and maps the new portion — base never moves, no resize_lock needed.
 */

#include "mmap_storage.h"
#include "crc32.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#ifdef __linux__
#include <linux/fs.h>
#ifndef SYNC_FILE_RANGE_WRITE
#define SYNC_FILE_RANGE_WRITE 2
#endif
#endif

/* ========================================================================== */
/* Internal helpers                                                            */
/* ========================================================================== */

/**
 * Allocate (or grow) the dirty page bitmap for the given mapped size.
 */
static bool dirty_bitmap_ensure(mmap_storage_t *ms, size_t mapped_size) {
    size_t total_pages = mapped_size / PAGE_SIZE;
    size_t new_words = (total_pages + 63) / 64;

    if (new_words <= ms->dirty_bitmap_words) return true;

    uint64_t *new_bm = realloc(ms->dirty_bitmap, new_words * sizeof(uint64_t));
    if (!new_bm) {
        LOG_ERROR("dirty_bitmap_ensure: realloc failed");
        return false;
    }
    /* Zero the new portion — new pages start clean */
    memset(new_bm + ms->dirty_bitmap_words, 0,
           (new_words - ms->dirty_bitmap_words) * sizeof(uint64_t));
    ms->dirty_bitmap = new_bm;
    ms->dirty_bitmap_words = new_words;
    return true;
}

/**
 * Reserve a large virtual address range (PROT_NONE, MAP_ANONYMOUS).
 * Tries progressively smaller sizes: 1 TiB → 256 GiB → 64 GiB → 16 GiB.
 * Returns base pointer and sets *out_size, or MAP_FAILED on failure.
 */
static void *reserve_va_space(size_t *out_size) {
    static const size_t candidates[] = {
        (size_t)1ULL << 40,  /* 1 TiB */
        (size_t)256ULL << 30, /* 256 GiB */
        (size_t)64ULL << 30,  /* 64 GiB  */
        (size_t)16ULL << 30,  /* 16 GiB  */
    };

    for (int i = 0; i < 4; i++) {
        void *p = mmap(NULL, candidates[i], PROT_NONE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (p != MAP_FAILED) {
            *out_size = candidates[i];
            LOG_DEBUG("reserve_va_space: reserved %zu bytes (%zu GiB)",
                      candidates[i], candidates[i] >> 30);
            return p;
        }
    }
    LOG_ERROR("reserve_va_space: all attempts failed");
    *out_size = 0;
    return MAP_FAILED;
}

/**
 * Grow the file-backed mapping within the reserved VA space.
 * Base pointer never moves.
 */
static bool mmap_storage_grow(mmap_storage_t *ms, size_t new_size) {
    if (new_size > ms->reserved_size) {
        LOG_ERROR("mmap_storage_grow: new_size %zu exceeds reserved VA space %zu",
                  new_size, ms->reserved_size);
        return false;
    }

    /* Extend the file */
    if (ftruncate(ms->fd, (off_t)new_size) != 0) {
        LOG_ERROR("mmap_storage_grow: ftruncate to %zu failed: %s",
                  new_size, strerror(errno));
        return false;
    }

    /* Map the new portion into the reserved VA space (MAP_FIXED overwrites PROT_NONE) */
    void *p = mmap(ms->base + ms->mapped_size,
                   new_size - ms->mapped_size,
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_FIXED, ms->fd, (off_t)ms->mapped_size);
    if (p == MAP_FAILED) {
        LOG_ERROR("mmap_storage_grow: mmap new region [%zu, %zu) failed: %s",
                  ms->mapped_size, new_size, strerror(errno));
        return false;
    }

    ms->mapped_size = new_size;

    /* Grow the dirty bitmap to cover new pages */
    dirty_bitmap_ensure(ms, new_size);

    LOG_DEBUG("mmap_storage_grow: resized to %zu bytes (%zu pages)",
              new_size, new_size / PAGE_SIZE);
    return true;
}

/* ---- Shadow header helpers ---- */

/* Compute CRC32 over the first 56 bytes of a header slot (everything before checksum). */
static uint32_t slot_crc(const mmap_header_slot_t *slot) {
    return compute_crc32((const uint8_t *)slot, 56);
}

/* Validate a single header slot: magic + version + CRC. */
static bool validate_slot(const mmap_header_slot_t *slot) {
    if (slot->magic != MMAP_MAGIC) return false;
    if (slot->version != MMAP_FORMAT_VERSION) return false;
    return slot->checksum == slot_crc(slot);
}

/* Pick the active slot (highest valid checkpoint_num).  Returns -1 if both invalid. */
static int pick_active_slot(const mmap_header_page_t *hp) {
    bool a_ok = validate_slot(&hp->slots[0]);
    bool b_ok = validate_slot(&hp->slots[1]);
    if (a_ok && b_ok)
        return hp->slots[0].checkpoint_num >= hp->slots[1].checkpoint_num ? 0 : 1;
    if (a_ok) return 0;
    if (b_ok) return 1;
    return -1;
}

/* Write a header slot at the given index with all fields + CRC. */
static void write_slot(mmap_storage_t *ms, int idx,
                       uint64_t root_page_id, uint32_t root_offset,
                       uint64_t tree_size, uint64_t key_size,
                       uint32_t checkpoint_num) {
    mmap_header_page_t *hp = (mmap_header_page_t *)ms->base;
    mmap_header_slot_t *s = &hp->slots[idx];
    s->magic = MMAP_MAGIC;
    s->version = MMAP_FORMAT_VERSION;
    s->page_size = PAGE_SIZE;
    s->next_page_id = ms->next_page_id;
    s->root_page_id = root_page_id;
    s->root_offset = root_offset;
    s->checkpoint_num = checkpoint_num;
    s->tree_size = tree_size;
    s->key_size = key_size;
    memset(s->_pad, 0, sizeof(s->_pad));
    s->checksum = slot_crc(s);
}

/* Migrate a v1 header (single 56-byte header, no CRC) to v2 shadow format. */
static bool migrate_v1_to_v2(mmap_storage_t *ms) {
    const mmap_header_v1_t *v1 = (const mmap_header_v1_t *)ms->base;

    /* Capture v1 values */
    uint64_t next_pid   = v1->next_page_id;
    uint64_t root_pid   = v1->root_page_id;
    uint32_t root_off   = v1->root_offset;
    uint64_t tree_sz    = v1->tree_size;
    uint64_t key_sz     = v1->key_size;

    ms->next_page_id = next_pid;

    /* Write slot B first (inactive) — crash-safe: slot A still has v1 */
    write_slot(ms, 1, root_pid, root_off, tree_sz, key_sz, 1);
    if (msync(ms->base, PAGE_SIZE, MS_SYNC) != 0) {
        LOG_ERROR("migrate_v1_to_v2: msync slot B failed: %s", strerror(errno));
        return false;
    }

    /* Write slot A — now both slots are v2 */
    write_slot(ms, 0, root_pid, root_off, tree_sz, key_sz, 1);
    if (msync(ms->base, PAGE_SIZE, MS_SYNC) != 0) {
        LOG_ERROR("migrate_v1_to_v2: msync slot A failed: %s", strerror(errno));
        return false;
    }

    ms->active_slot = 0;
    LOG_INFO("Migrated header from v1 to v2 shadow format");
    return true;
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

    /* Reserve VA space (up to 1 TiB) — base will never move */
    size_t reserved_size;
    void *base = reserve_va_space(&reserved_size);
    if (base == MAP_FAILED) {
        LOG_ERROR("mmap_storage_create: reserve_va_space failed");
        close(fd);
        return NULL;
    }

    /* Overlay the file at the start of the reserved range */
    void *mapped = mmap(base, file_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_FIXED, fd, 0);
    if (mapped == MAP_FAILED) {
        LOG_ERROR("mmap_storage_create: mmap MAP_FIXED(%zu) failed: %s",
                  file_size, strerror(errno));
        munmap(base, reserved_size);
        close(fd);
        return NULL;
    }

    /* Allocate storage struct */
    mmap_storage_t *ms = calloc(1, sizeof(mmap_storage_t));
    if (!ms) {
        munmap(base, reserved_size);
        close(fd);
        return NULL;
    }

    ms->fd = fd;
    ms->base = (uint8_t *)base;
    ms->mapped_size = file_size;
    ms->reserved_size = reserved_size;
    ms->next_page_id = 1;  /* Page 0 = header */
    ms->active_slot = 0;
    ms->path = strdup(path);

    /* Allocate dirty page bitmap */
    ms->dirty_bitmap = NULL;
    ms->dirty_bitmap_words = 0;
    if (!dirty_bitmap_ensure(ms, file_size)) {
        munmap(base, reserved_size);
        close(fd);
        free(ms->path);
        free(ms);
        return NULL;
    }

    /* Write both header slots (empty tree, checkpoint_num=0) */
    write_slot(ms, 0, 0, 0, 0, 0, 0);
    write_slot(ms, 1, 0, 0, 0, 0, 0);

    LOG_INFO("Created mmap storage: %s (%zu pages, %zu MB, VA reserved %zu GiB)",
             path, initial_pages, file_size / (1024 * 1024), reserved_size >> 30);

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

    /* Reserve VA space */
    size_t reserved_size;
    void *base = reserve_va_space(&reserved_size);
    if (base == MAP_FAILED) {
        LOG_ERROR("mmap_storage_open: reserve_va_space failed");
        close(fd);
        return NULL;
    }

    /* Overlay file at the start of reserved range */
    void *mapped = mmap(base, file_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_FIXED, fd, 0);
    if (mapped == MAP_FAILED) {
        LOG_ERROR("mmap_storage_open: mmap MAP_FIXED(%zu) failed: %s",
                  file_size, strerror(errno));
        munmap(base, reserved_size);
        close(fd);
        return NULL;
    }

    /* Check magic */
    const mmap_header_slot_t *slot0 = (const mmap_header_slot_t *)base;
    if (slot0->magic != MMAP_MAGIC) {
        LOG_ERROR("mmap_storage_open: bad magic (got 0x%lx, expected 0x%lx)",
                  slot0->magic, MMAP_MAGIC);
        munmap(base, reserved_size);
        close(fd);
        return NULL;
    }

    /* Allocate struct (needed before migration) */
    mmap_storage_t *ms = calloc(1, sizeof(mmap_storage_t));
    if (!ms) {
        munmap(base, reserved_size);
        close(fd);
        return NULL;
    }

    ms->fd = fd;
    ms->base = (uint8_t *)base;
    ms->mapped_size = file_size;
    ms->reserved_size = reserved_size;
    ms->path = strdup(path);

    /* Allocate dirty page bitmap */
    ms->dirty_bitmap = NULL;
    ms->dirty_bitmap_words = 0;
    if (!dirty_bitmap_ensure(ms, file_size)) {
        munmap(base, reserved_size);
        close(fd);
        free(ms->path);
        free(ms);
        return NULL;
    }

    /* Detect v1 format and migrate */
    if (slot0->version == 1) {
        /* v1 page_size check */
        const mmap_header_v1_t *v1 = (const mmap_header_v1_t *)base;
        if (v1->page_size != PAGE_SIZE) {
            LOG_ERROR("mmap_storage_open: page_size mismatch (%u vs %u)",
                      v1->page_size, PAGE_SIZE);
            munmap(base, reserved_size);
            close(fd);
            free(ms->dirty_bitmap);
            free(ms->path);
            free(ms);
            return NULL;
        }
        if (!migrate_v1_to_v2(ms)) {
            munmap(base, reserved_size);
            close(fd);
            free(ms->dirty_bitmap);
            free(ms->path);
            free(ms);
            return NULL;
        }
        /* Migration sets active_slot and next_page_id */
    } else {
        /* v2 format — pick best slot */
        const mmap_header_page_t *hp = (const mmap_header_page_t *)base;
        int active = pick_active_slot(hp);
        if (active < 0) {
            LOG_ERROR("mmap_storage_open: both header slots invalid (corrupt file)");
            munmap(base, reserved_size);
            close(fd);
            free(ms->dirty_bitmap);
            free(ms->path);
            free(ms);
            return NULL;
        }

        const mmap_header_slot_t *s = &hp->slots[active];
        if (s->page_size != PAGE_SIZE) {
            LOG_ERROR("mmap_storage_open: page_size mismatch (%u vs %u)",
                      s->page_size, PAGE_SIZE);
            munmap(base, reserved_size);
            close(fd);
            free(ms->dirty_bitmap);
            free(ms->path);
            free(ms);
            return NULL;
        }

        ms->next_page_id = s->next_page_id;
        ms->active_slot = active;
    }

    LOG_INFO("Opened mmap storage: %s (%zu pages, next_page_id=%lu, active_slot=%d)",
             path, file_size / PAGE_SIZE, ms->next_page_id, ms->active_slot);

    return ms;
}

void mmap_storage_destroy(mmap_storage_t *ms) {
    if (!ms) return;

    /* Sync to disk, then unmap the entire reserved VA range */
    if (ms->base && ms->mapped_size > 0) {
        msync(ms->base, ms->mapped_size, MS_SYNC);
    }
    if (ms->base && ms->reserved_size > 0) {
        munmap(ms->base, ms->reserved_size);
    }

    if (ms->fd >= 0) {
        close(ms->fd);
    }

    free(ms->dirty_bitmap);
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

    /* Grow if needed (base never moves — no lock required) */
    if (required_size > ms->mapped_size) {
        size_t new_size = ms->mapped_size * 2;
        if (new_size < required_size) {
            new_size = required_size;
        }

        if (!mmap_storage_grow(ms, new_size)) {
            return 0;
        }
    }

    ms->next_page_id = page_id + 1;

    /* Zero the new page for clean state */
    memset(ms->base + page_id * PAGE_SIZE, 0, PAGE_SIZE);
    mmap_storage_mark_dirty(ms, page_id);

    return page_id;
}

void mmap_storage_ensure_capacity(mmap_storage_t *ms, uint64_t total_pages) {
    if (!ms) return;

    size_t required = total_pages * PAGE_SIZE;
    if (required <= ms->mapped_size) return;

    size_t new_size = ms->mapped_size;
    while (new_size < required) new_size *= 2;

    if (new_size > ms->mapped_size) {
        mmap_storage_grow(ms, new_size);
    }
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

void mmap_storage_start_writeback(mmap_storage_t *ms) {
#ifdef __linux__
    /* Kick off non-blocking writeback for all dirty pages.
     * sync_file_range(SYNC_FILE_RANGE_WRITE) tells the kernel to start
     * flushing dirty pages to disk without waiting for completion.
     * The kernel internally knows which pages are dirty — we don't need
     * to scan our bitmap.  One syscall covers the entire mapped region. */
    if (ms->mapped_size > PAGE_SIZE) {
        sync_file_range(ms->fd, PAGE_SIZE,
                        (off_t)(ms->mapped_size - PAGE_SIZE),
                        SYNC_FILE_RANGE_WRITE);
    }
#else
    (void)ms;
#endif
}

/**
 * Sync dirty data pages to disk (incremental checkpoint).
 *
 * Scans the dirty bitmap at word granularity (64 pages = 256KB per word).
 * Contiguous non-zero words are merged into a single msync call to
 * minimise syscall overhead during bulk writes.  Clean 64-page blocks
 * are skipped entirely.
 *
 * Returns true on success, false if any msync fails.
 */
static bool sync_dirty_pages(mmap_storage_t *ms) {
    size_t total_pages = ms->mapped_size / PAGE_SIZE;
    size_t num_words = ms->dirty_bitmap_words;

    size_t range_start = 0;
    bool in_range = false;

    for (size_t w = 0; w < num_words; w++) {
        if (ms->dirty_bitmap[w] != 0) {
            if (!in_range) {
                range_start = w * 64;
                if (range_start == 0) range_start = 1;  /* skip header page */
                in_range = true;
            }
            /* Extend range through contiguous non-zero words */
        } else {
            if (in_range) {
                size_t range_end = w * 64;
                if (range_end > total_pages) range_end = total_pages;
                if (range_end > range_start) {
                    if (msync(ms->base + range_start * PAGE_SIZE,
                              (range_end - range_start) * PAGE_SIZE, MS_SYNC) != 0) {
                        LOG_ERROR("sync_dirty_pages: msync [%zu, %zu) failed: %s",
                                  range_start, range_end, strerror(errno));
                        return false;
                    }
                }
                in_range = false;
            }
        }
    }

    /* Flush final range */
    if (in_range) {
        size_t range_end = total_pages;
        if (range_end > range_start) {
            if (msync(ms->base + range_start * PAGE_SIZE,
                      (range_end - range_start) * PAGE_SIZE, MS_SYNC) != 0) {
                LOG_ERROR("sync_dirty_pages: msync final [%zu, %zu) failed: %s",
                          range_start, range_end, strerror(errno));
                return false;
            }
        }
    }

    /* Clear bitmap — all pages are now clean */
    memset(ms->dirty_bitmap, 0, num_words * sizeof(uint64_t));
    return true;
}

bool mmap_storage_checkpoint(mmap_storage_t *ms,
                              uint64_t root_page_id,
                              uint32_t root_offset,
                              uint64_t tree_size,
                              uint64_t key_size) {
    if (!ms || !ms->base) return false;

    /* Phase 1: Sync only dirty data pages (skip header page).
     * This guarantees that every CoW tree node reachable from the new root
     * is on stable storage BEFORE the header points to it. */
    if (!sync_dirty_pages(ms)) {
        LOG_ERROR("mmap_storage_checkpoint: sync_dirty_pages failed");
        return false;
    }

    /* Phase 2: Write to the inactive header slot */
    int inactive = 1 - ms->active_slot;
    mmap_header_page_t *hp = (mmap_header_page_t *)ms->base;
    uint32_t prev_num = hp->slots[ms->active_slot].checkpoint_num;
    write_slot(ms, inactive, root_page_id, root_offset,
               tree_size, key_size, prev_num + 1);

    /* Phase 3: Sync header page only (4KB) */
    if (msync(ms->base, PAGE_SIZE, MS_SYNC) != 0) {
        LOG_ERROR("mmap_storage_checkpoint: msync header page failed: %s",
                  strerror(errno));
        return false;
    }

    ms->active_slot = inactive;
    return true;
}

bool mmap_storage_load_header(mmap_storage_t *ms,
                              uint64_t *root_page_id,
                              uint32_t *root_offset,
                              uint64_t *tree_size,
                              uint64_t *key_size) {
    if (!ms || !ms->base) return false;

    const mmap_header_page_t *hp = (const mmap_header_page_t *)ms->base;
    const mmap_header_slot_t *s = &hp->slots[ms->active_slot];

    if (root_page_id) *root_page_id = s->root_page_id;
    if (root_offset) *root_offset = (uint32_t)s->root_offset;
    if (tree_size) *tree_size = s->tree_size;
    if (key_size) *key_size = s->key_size;

    return true;
}

/* ========================================================================== */
/* Truncation (for compaction)                                                 */
/* ========================================================================== */

bool mmap_storage_truncate(mmap_storage_t *ms, uint64_t new_page_count) {
    if (!ms || !ms->base) return false;

    size_t new_size = new_page_count * PAGE_SIZE;
    if (new_size >= ms->mapped_size) return true;  /* nothing to do */

    /* Remap the tail back to PROT_NONE (within our VA reservation) */
    size_t tail_size = ms->mapped_size - new_size;
    void *tail = mmap(ms->base + new_size, tail_size, PROT_NONE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_FIXED,
                      -1, 0);
    if (tail == MAP_FAILED) {
        LOG_ERROR("mmap_storage_truncate: remap tail to PROT_NONE failed: %s",
                  strerror(errno));
        return false;
    }

    /* Truncate the file */
    if (ftruncate(ms->fd, (off_t)new_size) != 0) {
        LOG_ERROR("mmap_storage_truncate: ftruncate to %zu failed: %s",
                  new_size, strerror(errno));
        return false;
    }

    size_t old_pages = ms->mapped_size / PAGE_SIZE;
    ms->mapped_size = new_size;
    ms->next_page_id = new_page_count;

    /* Shrink dirty bitmap */
    size_t new_words = (new_page_count + 63) / 64;
    if (new_words < ms->dirty_bitmap_words) {
        /* Zero any partial word bits beyond new page count */
        if (new_page_count % 64 != 0) {
            uint64_t mask = (1ULL << (new_page_count % 64)) - 1;
            ms->dirty_bitmap[new_words - 1] &= mask;
        }
        ms->dirty_bitmap_words = new_words;
    }

    LOG_INFO("mmap_storage_truncate: %zu → %lu pages (freed %zu pages, %.1f MB)",
             old_pages, new_page_count,
             old_pages - (size_t)new_page_count,
             (double)tail_size / (1024.0 * 1024.0));
    return true;
}
