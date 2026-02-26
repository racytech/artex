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
 * Growth: ftruncate(2x) + mremap(MREMAP_MAYMOVE) under write-lock.
 */

#define _GNU_SOURCE  /* mremap */

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
    ms->active_slot = 0;
    ms->path = strdup(path);

    if (pthread_rwlock_init(&ms->resize_lock, NULL) != 0) {
        LOG_ERROR("mmap_storage_create: pthread_rwlock_init failed");
        munmap(base, file_size);
        close(fd);
        free(ms->path);
        free(ms);
        return NULL;
    }

    /* Write both header slots (empty tree, checkpoint_num=0) */
    write_slot(ms, 0, 0, 0, 0, 0, 0);
    write_slot(ms, 1, 0, 0, 0, 0, 0);

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

    /* Check magic */
    const mmap_header_slot_t *slot0 = (const mmap_header_slot_t *)base;
    if (slot0->magic != MMAP_MAGIC) {
        LOG_ERROR("mmap_storage_open: bad magic (got 0x%lx, expected 0x%lx)",
                  slot0->magic, MMAP_MAGIC);
        munmap(base, file_size);
        close(fd);
        return NULL;
    }

    /* Allocate struct (needed before migration) */
    mmap_storage_t *ms = calloc(1, sizeof(mmap_storage_t));
    if (!ms) {
        munmap(base, file_size);
        close(fd);
        return NULL;
    }

    ms->fd = fd;
    ms->base = (uint8_t *)base;
    ms->mapped_size = file_size;
    ms->path = strdup(path);

    if (pthread_rwlock_init(&ms->resize_lock, NULL) != 0) {
        munmap(base, file_size);
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
            munmap(base, file_size);
            close(fd);
            pthread_rwlock_destroy(&ms->resize_lock);
            free(ms->path);
            free(ms);
            return NULL;
        }
        if (!migrate_v1_to_v2(ms)) {
            munmap(base, file_size);
            close(fd);
            pthread_rwlock_destroy(&ms->resize_lock);
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
            munmap(base, file_size);
            close(fd);
            pthread_rwlock_destroy(&ms->resize_lock);
            free(ms->path);
            free(ms);
            return NULL;
        }

        const mmap_header_slot_t *s = &hp->slots[active];
        if (s->page_size != PAGE_SIZE) {
            LOG_ERROR("mmap_storage_open: page_size mismatch (%u vs %u)",
                      s->page_size, PAGE_SIZE);
            munmap(base, file_size);
            close(fd);
            pthread_rwlock_destroy(&ms->resize_lock);
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

void mmap_storage_ensure_capacity(mmap_storage_t *ms, uint64_t total_pages) {
    if (!ms) return;

    size_t required = total_pages * PAGE_SIZE;
    if (required <= ms->mapped_size) return;

    size_t new_size = ms->mapped_size;
    while (new_size < required) new_size *= 2;

    pthread_rwlock_wrlock(&ms->resize_lock);
    if (new_size > ms->mapped_size) {
        mmap_storage_grow(ms, new_size);
    }
    pthread_rwlock_unlock(&ms->resize_lock);
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

bool mmap_storage_checkpoint(mmap_storage_t *ms,
                              uint64_t root_page_id,
                              uint32_t root_offset,
                              uint64_t tree_size,
                              uint64_t key_size) {
    if (!ms || !ms->base) return false;

    /* Phase 1: Sync all data pages (skip header page).
     * This guarantees that every CoW tree node reachable from the new root
     * is on stable storage BEFORE the header points to it. */
    if (ms->mapped_size > PAGE_SIZE) {
        if (msync(ms->base + PAGE_SIZE, ms->mapped_size - PAGE_SIZE, MS_SYNC) != 0) {
            LOG_ERROR("mmap_storage_checkpoint: msync data pages failed: %s",
                      strerror(errno));
            return false;
        }
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
