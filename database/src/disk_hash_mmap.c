#define _GNU_SOURCE  /* mremap */

/*
 * Disk Hash (mmap) — Memory-mapped disk-backed hash table.
 *
 * Drop-in replacement for disk_hash.c using mmap instead of pread/pwrite.
 * Same file format — binary compatible, same bucket layout.
 *
 * Benefits over pread/pwrite version:
 *   - OS page cache manages all caching (no explicit LRU needed)
 *   - Zero-copy reads: get/contains return pointers into mapped region
 *   - No syscall overhead per bucket access
 *   - Overflow growth via mremap (Linux) — no munmap/mmap cycle
 *
 * Performance notes:
 *   - disk_hash_sync() calls msync(MS_SYNC) on the full mapping, blocking
 *     until all dirty pages are physically written. Avoid on hot paths.
 *   - MAP_SHARED + hash-based bucket lookup = random page access across
 *     a large mapping, causing TLB misses and kernel dirty page writeback
 *     pressure (dirty_ratio throttling).
 *   - Single-threaded design — no locking overhead. If multi-threaded
 *     access is needed in the future, add pthread_rwlock behind a
 *     build-time flag (#ifdef DISK_HASH_THREADSAFE).
 *   - Use mem_art for hot-path state cache; disk_hash is best suited
 *     as a cold-path fallback after cache eviction.
 */

#include "../include/disk_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define DISK_HASH_MAGIC       0x48534844U   /* "DHSH" little-endian */
#define DISK_HASH_VERSION     1
#define PAGE_SIZE             4096
#define BUCKET_HEADER_SIZE    8

#define SLOT_EMPTY            0x00
#define SLOT_OCCUPIED         0x01
#define SLOT_TOMBSTONE        0x02

/* =========================================================================
 * On-disk header (first 64 bytes of the 4096-byte header page)
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t key_size;
    uint32_t record_size;
    uint64_t bucket_count;
    uint64_t entry_count;
    uint64_t overflow_count;
    uint32_t slots_per_bucket;
    uint8_t  dirty;
    uint8_t  reserved[19];
} disk_hash_header_t;

_Static_assert(sizeof(disk_hash_header_t) == 64,
               "disk_hash_header_t must be 64 bytes");

/* =========================================================================
 * Bucket header (first 8 bytes of each 4096-byte bucket page)
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint16_t count;
    uint16_t tombstone_count;
    uint32_t overflow_id;       /* 0 = no overflow */
} bucket_header_t;

_Static_assert(sizeof(bucket_header_t) == BUCKET_HEADER_SIZE,
               "bucket_header_t must be 8 bytes");

/* =========================================================================
 * In-memory structure
 * ========================================================================= */

struct disk_hash {
    int       fd;
    uint8_t  *base;              /* mmap base pointer */
    size_t    mapped_size;       /* current mmap size */
    uint32_t  key_size;
    uint32_t  record_size;
    uint32_t  slot_size;         /* 1 + key_size + record_size */
    uint32_t  slots_per_bucket;
    uint64_t  bucket_count;
    uint64_t  entry_count;
    uint64_t  overflow_count;
    bool      dirty;
};

/* =========================================================================
 * Hash function — MurmurHash3 64-bit finalizer
 * ========================================================================= */

static inline uint64_t hash_key(const uint8_t *key) {
    uint64_t h;
    memcpy(&h, key, 8);
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static inline size_t bucket_offset(uint64_t bucket_id) {
    return PAGE_SIZE + bucket_id * (size_t)PAGE_SIZE;
}

/** Pointer to a bucket page in the mmap region. */
static inline uint8_t *bucket_ptr(disk_hash_t *dh, uint64_t bucket_id) {
    return dh->base + bucket_offset(bucket_id);
}

static inline const uint8_t *bucket_ptr_const(const disk_hash_t *dh,
                                               uint64_t bucket_id) {
    return dh->base + bucket_offset(bucket_id);
}

/** Pointer to slot i within a bucket page. */
static inline uint8_t *slot_ptr(uint8_t *page, uint32_t slot_size,
                                 uint32_t i) {
    return page + BUCKET_HEADER_SIZE + (uint32_t)i * slot_size;
}

static inline const uint8_t *slot_ptr_const(const uint8_t *page,
                                             uint32_t slot_size,
                                             uint32_t i) {
    return page + BUCKET_HEADER_SIZE + (uint32_t)i * slot_size;
}

static void write_header(disk_hash_t *dh) {
    disk_hash_header_t *hdr = (disk_hash_header_t *)dh->base;
    hdr->magic            = DISK_HASH_MAGIC;
    hdr->version          = DISK_HASH_VERSION;
    hdr->key_size         = dh->key_size;
    hdr->record_size      = dh->record_size;
    hdr->bucket_count     = dh->bucket_count;
    hdr->entry_count      = dh->entry_count;
    hdr->overflow_count   = dh->overflow_count;
    hdr->slots_per_bucket = dh->slots_per_bucket;
    hdr->dirty            = dh->dirty ? 1 : 0;
}

static bool read_header(int fd, disk_hash_header_t *hdr) {
    ssize_t n = pread(fd, hdr, sizeof(*hdr), 0);
    return n >= (ssize_t)sizeof(*hdr);
}

/** Grow the mapping to include new overflow buckets. */
static bool remap(disk_hash_t *dh, size_t new_size) {
    if (new_size <= dh->mapped_size) return true;

    /* Extend the file first */
    if (ftruncate(dh->fd, (off_t)new_size) != 0)
        return false;

#ifdef __linux__
    uint8_t *p = mremap(dh->base, dh->mapped_size, new_size, MREMAP_MAYMOVE);
    if (p == MAP_FAILED) return false;
#else
    /* Portable: munmap + mmap */
    munmap(dh->base, dh->mapped_size);
    uint8_t *p = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                       MAP_SHARED, dh->fd, 0);
    if (p == MAP_FAILED) return false;
#endif

    dh->base = p;
    dh->mapped_size = new_size;
    return true;
}

/** Allocate a new overflow bucket ID. Grows mapping if needed. */
static uint64_t alloc_overflow(disk_hash_t *dh) {
    uint64_t new_id = dh->bucket_count + dh->overflow_count;
    dh->overflow_count++;

    size_t needed = bucket_offset(new_id + 1);
    if (needed > dh->mapped_size) {
        /* Grow by 2x or to needed, whichever is larger */
        size_t grow = dh->mapped_size * 2;
        if (grow < needed) grow = needed;
        if (!remap(dh, grow)) {
            dh->overflow_count--;
            return UINT64_MAX;
        }
    }

    /* Zero the new bucket page */
    memset(bucket_ptr(dh, new_id), 0, PAGE_SIZE);
    return new_id;
}

/* =========================================================================
 * Crash safety — dirty flag + recovery
 * ========================================================================= */

static void mark_dirty(disk_hash_t *dh) {
    if (dh->dirty) return;
    dh->dirty = true;
    write_header(dh);
}

static bool recover(disk_hash_t *dh) {
    /* Derive total pages from file size */
    uint64_t total_pages = (dh->mapped_size - PAGE_SIZE) / PAGE_SIZE;
    if (total_pages > dh->bucket_count)
        dh->overflow_count = total_pages - dh->bucket_count;
    else
        dh->overflow_count = 0;

    /* Count occupied slots */
    uint64_t count = 0;
    uint64_t all_pages = dh->bucket_count + dh->overflow_count;

    for (uint64_t bid = 0; bid < all_pages; bid++) {
        const uint8_t *page = bucket_ptr_const(dh, bid);
        for (uint32_t i = 0; i < dh->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dh->slot_size, i);
            if (s[0] == SLOT_OCCUPIED)
                count++;
        }
    }

    dh->entry_count = count;
    dh->dirty = false;
    write_header(dh);
    msync(dh->base, PAGE_SIZE, MS_SYNC);
    return true;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

disk_hash_t *disk_hash_create(const char *path, uint32_t key_size,
                               uint32_t record_size, uint64_t capacity_hint) {
    if (!path || key_size == 0 || key_size < 8)
        return NULL;

    uint32_t slot_size = 1 + key_size + record_size;
    uint32_t slots_per_bucket = (PAGE_SIZE - BUCKET_HEADER_SIZE) / slot_size;
    if (slots_per_bucket == 0) return NULL;

    uint64_t effective = (uint64_t)((double)slots_per_bucket * 0.75);
    if (effective == 0) effective = 1;
    uint64_t bucket_count = (capacity_hint + effective - 1) / effective;
    if (bucket_count == 0) bucket_count = 1;

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return NULL;

    size_t file_size = PAGE_SIZE + bucket_count * (size_t)PAGE_SIZE;
    if (ftruncate(fd, (off_t)file_size) != 0) {
        close(fd);
        return NULL;
    }

    uint8_t *base = mmap(NULL, file_size, PROT_READ | PROT_WRITE,
                          MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    disk_hash_t *dh = calloc(1, sizeof(*dh));
    if (!dh) { munmap(base, file_size); close(fd); return NULL; }

    dh->fd               = fd;
    dh->base             = base;
    dh->mapped_size      = file_size;
    dh->key_size         = key_size;
    dh->record_size      = record_size;
    dh->slot_size        = slot_size;
    dh->slots_per_bucket = slots_per_bucket;
    dh->bucket_count     = bucket_count;
    dh->entry_count      = 0;
    dh->overflow_count   = 0;
    dh->dirty            = false;

    write_header(dh);
    return dh;
}

disk_hash_t *disk_hash_open(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR);
    if (fd < 0) return NULL;

    disk_hash_header_t hdr;
    if (!read_header(fd, &hdr)) { close(fd); return NULL; }

    if (hdr.magic != DISK_HASH_MAGIC || hdr.version != DISK_HASH_VERSION) {
        close(fd);
        return NULL;
    }

    uint32_t slot_size = 1 + hdr.key_size + hdr.record_size;
    uint32_t expected_spb = (PAGE_SIZE - BUCKET_HEADER_SIZE) / slot_size;
    if (hdr.slots_per_bucket != expected_spb) {
        close(fd);
        return NULL;
    }

    /* Get actual file size for mmap */
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return NULL; }
    size_t file_size = (size_t)st.st_size;

    /* Ensure file is at least header + primary buckets */
    size_t min_size = PAGE_SIZE + hdr.bucket_count * (size_t)PAGE_SIZE;
    if (file_size < min_size) {
        if (ftruncate(fd, (off_t)min_size) != 0) {
            close(fd);
            return NULL;
        }
        file_size = min_size;
    }

    uint8_t *base = mmap(NULL, file_size, PROT_READ | PROT_WRITE,
                          MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    disk_hash_t *dh = calloc(1, sizeof(*dh));
    if (!dh) { munmap(base, file_size); close(fd); return NULL; }

    dh->fd               = fd;
    dh->base             = base;
    dh->mapped_size      = file_size;
    dh->key_size         = hdr.key_size;
    dh->record_size      = hdr.record_size;
    dh->slot_size        = slot_size;
    dh->slots_per_bucket = hdr.slots_per_bucket;
    dh->bucket_count     = hdr.bucket_count;
    dh->entry_count      = hdr.entry_count;
    dh->overflow_count   = hdr.overflow_count;
    dh->dirty            = (hdr.dirty != 0);

    if (dh->dirty) {
        if (!recover(dh)) {
            munmap(base, file_size);
            close(fd);
            free(dh);
            return NULL;
        }
    }

    return dh;
}

void disk_hash_destroy(disk_hash_t *dh) {
    if (!dh) return;
    if (dh->base) munmap(dh->base, dh->mapped_size);
    close(dh->fd);
    free(dh);
}

void disk_hash_clear(disk_hash_t *dh) {
    if (!dh) return;
    /* Zero all bucket pages */
    size_t bucket_start = PAGE_SIZE;
    size_t bucket_bytes = dh->bucket_count * (size_t)PAGE_SIZE;
    memset(dh->base + bucket_start, 0, bucket_bytes);

    /* If there were overflow pages, shrink the mapping */
    if (dh->overflow_count > 0) {
        size_t new_size = PAGE_SIZE + dh->bucket_count * (size_t)PAGE_SIZE;
        ftruncate(dh->fd, (off_t)new_size);
#ifdef __linux__
        uint8_t *p = mremap(dh->base, dh->mapped_size, new_size, MREMAP_MAYMOVE);
        if (p != MAP_FAILED) {
            dh->base = p;
            dh->mapped_size = new_size;
        }
#else
        munmap(dh->base, dh->mapped_size);
        dh->base = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, dh->fd, 0);
        dh->mapped_size = new_size;
#endif
    }

    dh->entry_count = 0;
    dh->overflow_count = 0;
    dh->dirty = false;
    write_header(dh);
}

/* =========================================================================
 * Internal: scan a bucket chain for a key
 * ========================================================================= */

typedef struct {
    bool     found;
    uint64_t match_bucket_id;
    uint32_t match_slot;
    int64_t  free_slot;
    uint64_t free_bucket_id;
    uint64_t last_bucket_id;
} scan_result_t;

static scan_result_t scan_chain_key(const disk_hash_t *dh,
                                     uint64_t start_bucket,
                                     const uint8_t *key) {
    scan_result_t r;
    memset(&r, 0, sizeof(r));
    r.found     = false;
    r.free_slot = -1;

    uint64_t bid = start_bucket;
    while (1) {
        const uint8_t *page = bucket_ptr_const(dh, bid);
        const bucket_header_t *bh = (const bucket_header_t *)page;

        for (uint32_t i = 0; i < dh->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dh->slot_size, i);
            uint8_t flags = s[0];

            if (flags == SLOT_OCCUPIED) {
                if (memcmp(s + 1, key, dh->key_size) == 0) {
                    r.found           = true;
                    r.match_bucket_id = bid;
                    r.match_slot      = i;
                    r.last_bucket_id  = bid;
                    return r;
                }
            } else if (r.free_slot < 0 &&
                       (flags == SLOT_EMPTY || flags == SLOT_TOMBSTONE)) {
                r.free_slot      = (int64_t)i;
                r.free_bucket_id = bid;
            }
        }

        r.last_bucket_id = bid;
        if (bh->overflow_id == 0) break;
        bid = bh->overflow_id;
    }

    return r;
}

/* =========================================================================
 * Single Operations
 * ========================================================================= */

static bool get_unlocked(const disk_hash_t *dh, const uint8_t *key, void *out) {
    uint64_t bid = hash_key(key) % dh->bucket_count;
    scan_result_t r = scan_chain_key(dh, bid, key);
    if (!r.found) return false;

    const uint8_t *page = bucket_ptr_const(dh, r.match_bucket_id);
    const uint8_t *s = slot_ptr_const(page, dh->slot_size, r.match_slot);
    if (out)
        memcpy(out, s + 1 + dh->key_size, dh->record_size);
    return true;
}

static bool put_unlocked(disk_hash_t *dh, const uint8_t *key,
                          const void *record) {
    mark_dirty(dh);
    uint64_t bid = hash_key(key) % dh->bucket_count;
    scan_result_t r = scan_chain_key(dh, bid, key);

    if (r.found) {
        /* Update existing */
        uint8_t *page = bucket_ptr(dh, r.match_bucket_id);
        uint8_t *s = slot_ptr(page, dh->slot_size, r.match_slot);
        memcpy(s + 1 + dh->key_size, record, dh->record_size);
        return true;
    }

    /* Insert new entry */
    if (r.free_slot >= 0) {
        uint8_t *page = bucket_ptr(dh, r.free_bucket_id);
        bucket_header_t *bh = (bucket_header_t *)page;
        uint8_t *s = slot_ptr(page, dh->slot_size, (uint32_t)r.free_slot);

        bool was_tombstone = (s[0] == SLOT_TOMBSTONE);
        s[0] = SLOT_OCCUPIED;
        memcpy(s + 1, key, dh->key_size);
        memcpy(s + 1 + dh->key_size, record, dh->record_size);

        bh->count++;
        if (was_tombstone && bh->tombstone_count > 0)
            bh->tombstone_count--;

        dh->entry_count++;
        return true;
    }

    /* No free slot — allocate overflow */
    uint64_t new_id = alloc_overflow(dh);
    if (new_id == UINT64_MAX) return false;

    uint8_t *page = bucket_ptr(dh, new_id);
    bucket_header_t *bh = (bucket_header_t *)page;
    bh->count = 1;
    uint8_t *s = slot_ptr(page, dh->slot_size, 0);
    s[0] = SLOT_OCCUPIED;
    memcpy(s + 1, key, dh->key_size);
    memcpy(s + 1 + dh->key_size, record, dh->record_size);

    /* Link from last bucket */
    uint8_t *last_page = bucket_ptr(dh, r.last_bucket_id);
    bucket_header_t *last_bh = (bucket_header_t *)last_page;
    last_bh->overflow_id = (uint32_t)new_id;

    dh->entry_count++;
    return true;
}

static bool delete_unlocked(disk_hash_t *dh, const uint8_t *key) {
    mark_dirty(dh);
    uint64_t bid = hash_key(key) % dh->bucket_count;
    scan_result_t r = scan_chain_key(dh, bid, key);
    if (!r.found) return false;

    uint8_t *page = bucket_ptr(dh, r.match_bucket_id);
    bucket_header_t *bh = (bucket_header_t *)page;
    uint8_t *s = slot_ptr(page, dh->slot_size, r.match_slot);

    s[0] = SLOT_TOMBSTONE;
    if (bh->count > 0) bh->count--;
    bh->tombstone_count++;

    dh->entry_count--;
    return true;
}

/* --- Public API --- */

bool disk_hash_get(const disk_hash_t *dh, const uint8_t *key, void *out) {
    if (!dh || !key) return false;
    bool ok = get_unlocked(dh, key, out);
    return ok;
}

bool disk_hash_put(disk_hash_t *dh, const uint8_t *key, const void *record) {
    if (!dh || !key || !record) return false;
    bool ok = put_unlocked(dh, key, record);
    return ok;
}

bool disk_hash_delete(disk_hash_t *dh, const uint8_t *key) {
    if (!dh || !key) return false;
    bool ok = delete_unlocked(dh, key);
    return ok;
}

bool disk_hash_contains(const disk_hash_t *dh, const uint8_t *key) {
    if (!dh || !key) return false;
    uint64_t bid = hash_key(key) % dh->bucket_count;
    scan_result_t r = scan_chain_key(dh, bid, key);
    return r.found;
}

/* =========================================================================
 * Batch Operations
 * ========================================================================= */

typedef struct {
    uint32_t idx;
    uint64_t bucket_id;
} batch_entry_t;

static int cmp_batch_entry(const void *a, const void *b) {
    const batch_entry_t *ea = (const batch_entry_t *)a;
    const batch_entry_t *eb = (const batch_entry_t *)b;
    if (ea->bucket_id < eb->bucket_id) return -1;
    if (ea->bucket_id > eb->bucket_id) return  1;
    return 0;
}

uint32_t disk_hash_batch_get(const disk_hash_t *dh,
                              const uint8_t *keys, void *records,
                              bool *found, uint32_t count) {
    if (!dh || !keys || count == 0) return 0;

    if (found) memset(found, 0, count * sizeof(bool));

    batch_entry_t *entries = malloc(count * sizeof(batch_entry_t));
    if (!entries) return 0;

    for (uint32_t i = 0; i < count; i++) {
        entries[i].idx       = i;
        entries[i].bucket_id = hash_key(keys + (uint64_t)i * dh->key_size)
                               % dh->bucket_count;
    }

    qsort(entries, count, sizeof(batch_entry_t), cmp_batch_entry);

    uint32_t found_count = 0;
    uint32_t gi = 0;

    while (gi < count) {
        uint64_t bid = entries[gi].bucket_id;

        uint64_t chain_bid = bid;
        bool chain_done = false;

        while (!chain_done) {
            const uint8_t *page = bucket_ptr_const(dh, chain_bid);
            const bucket_header_t *bh = (const bucket_header_t *)page;

            for (uint32_t g = gi; g < count && entries[g].bucket_id == bid; g++) {
                if (found && found[entries[g].idx]) continue;

                const uint8_t *search_key = keys +
                    (uint64_t)entries[g].idx * dh->key_size;

                for (uint32_t s = 0; s < dh->slots_per_bucket; s++) {
                    const uint8_t *slot = slot_ptr_const(page, dh->slot_size, s);
                    if (slot[0] == SLOT_OCCUPIED &&
                        memcmp(slot + 1, search_key, dh->key_size) == 0) {
                        if (records)
                            memcpy((uint8_t *)records +
                                   (uint64_t)entries[g].idx * dh->record_size,
                                   slot + 1 + dh->key_size,
                                   dh->record_size);
                        if (found) found[entries[g].idx] = true;
                        found_count++;
                        break;
                    }
                }
            }

            if (bh->overflow_id == 0)
                chain_done = true;
            else
                chain_bid = bh->overflow_id;
        }

        while (gi < count && entries[gi].bucket_id == bid) gi++;
    }

    free(entries);
    return found_count;
}

bool disk_hash_batch_put(disk_hash_t *dh,
                          const uint8_t *keys, const void *records,
                          uint32_t count) {
    if (!dh || !keys || !records || count == 0) return false;

    batch_entry_t *entries = malloc(count * sizeof(batch_entry_t));
    if (!entries) return false;

    for (uint32_t i = 0; i < count; i++) {
        entries[i].idx       = i;
        entries[i].bucket_id = hash_key(keys + (uint64_t)i * dh->key_size)
                               % dh->bucket_count;
    }

    qsort(entries, count, sizeof(batch_entry_t), cmp_batch_entry);

    uint32_t gi = 0;
    bool ok = true;

    while (gi < count && ok) {
        uint64_t bid = entries[gi].bucket_id;
        uint32_t gend = gi;
        while (gend < count && entries[gend].bucket_id == bid) gend++;

        for (uint32_t g = gi; g < gend && ok; g++) {
            uint32_t orig = entries[g].idx;
            const uint8_t *k = keys + (uint64_t)orig * dh->key_size;
            const uint8_t *r = (const uint8_t *)records +
                               (uint64_t)orig * dh->record_size;
            ok = put_unlocked(dh, k, r);
        }

        gi = gend;
    }

    free(entries);
    return ok;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t disk_hash_count(const disk_hash_t *dh) {
    return dh ? dh->entry_count : 0;
}

uint64_t disk_hash_capacity(const disk_hash_t *dh) {
    return dh ? dh->bucket_count * (uint64_t)dh->slots_per_bucket : 0;
}

/* =========================================================================
 * Iteration
 * ========================================================================= */

void disk_hash_foreach_key(const disk_hash_t *dh, disk_hash_key_cb_t cb,
                            void *user_data) {
    if (!dh || !cb) return;

    uint64_t total_pages = dh->bucket_count + dh->overflow_count;

    for (uint64_t bid = 0; bid < total_pages; bid++) {
        const uint8_t *page = bucket_ptr_const(dh, bid);
        for (uint32_t i = 0; i < dh->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dh->slot_size, i);
            if (s[0] == SLOT_OCCUPIED)
                cb(s + 1, user_data);
        }
    }
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void disk_hash_sync(disk_hash_t *dh) {
    if (!dh) return;
    dh->dirty = false;
    write_header(dh);
    msync(dh->base, dh->mapped_size, MS_SYNC);
}


