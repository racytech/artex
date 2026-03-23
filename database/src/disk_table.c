#define _GNU_SOURCE /* mremap */

/*
 * Disk Table — Memory-mapped hash table for pre-hashed keys.
 *
 * Optimized for keccak256/Pedersen keys where the first 8 bytes are
 * already uniformly distributed:
 *   - No hash function: raw first-8-bytes used as bucket index
 *   - Power-of-2 bucket count: bitmask instead of modulo
 *   - Fingerprint byte: key[8] pre-filter before 32-byte memcmp
 *   - 64-bit overflow IDs: no truncation risk
 *   - madvise hints for batch operations
 */

#include "../include/disk_table.h"

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

#define DISK_TABLE_MAGIC      0x4C425444U   /* "DTBL" little-endian */
#define DISK_TABLE_VERSION    1
#define PAGE_SIZE             4096
#define BUCKET_HEADER_SIZE    16

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
    uint64_t bucket_count;      /* always power of 2 */
    uint64_t bucket_mask;       /* bucket_count - 1 */
    uint64_t entry_count;
    uint64_t overflow_count;
    uint32_t slots_per_bucket;
    uint8_t  dirty;
    uint8_t  reserved[11];
} disk_table_header_t;

_Static_assert(sizeof(disk_table_header_t) == 64,
               "disk_table_header_t must be 64 bytes");

/* =========================================================================
 * Bucket header (first 16 bytes of each 4096-byte bucket page)
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint16_t count;
    uint16_t tombstone_count;
    uint32_t _pad;
    uint64_t overflow_id;       /* 0 = no overflow */
} bucket_header_t;

_Static_assert(sizeof(bucket_header_t) == BUCKET_HEADER_SIZE,
               "bucket_header_t must be 16 bytes");

/* =========================================================================
 * In-memory structure
 * ========================================================================= */

struct disk_table {
    int       fd;
    uint8_t  *base;
    size_t    mapped_size;
    uint32_t  key_size;
    uint32_t  record_size;
    uint32_t  slot_size;         /* 2 + key_size + record_size */
    uint32_t  slots_per_bucket;
    uint64_t  bucket_count;
    uint64_t  bucket_mask;
    uint64_t  entry_count;
    uint64_t  overflow_count;
    bool      dirty;
};

/* =========================================================================
 * Key → bucket index (no hash function)
 * ========================================================================= */

static inline uint64_t key_to_bucket(const uint8_t *key, uint64_t mask) {
    uint64_t h;
    memcpy(&h, key, 8);
    return h & mask;
}

static inline uint8_t key_fingerprint(const uint8_t *key) {
    return key[8];
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static inline size_t bucket_offset(uint64_t bucket_id) {
    return PAGE_SIZE + bucket_id * (size_t)PAGE_SIZE;
}

static inline uint8_t *bucket_ptr(disk_table_t *dt, uint64_t bucket_id) {
    return dt->base + bucket_offset(bucket_id);
}

static inline const uint8_t *bucket_ptr_const(const disk_table_t *dt,
                                               uint64_t bucket_id) {
    return dt->base + bucket_offset(bucket_id);
}

static inline uint8_t *slot_ptr(uint8_t *page, uint32_t slot_size,
                                 uint32_t i) {
    return page + BUCKET_HEADER_SIZE + (uint32_t)i * slot_size;
}

static inline const uint8_t *slot_ptr_const(const uint8_t *page,
                                             uint32_t slot_size,
                                             uint32_t i) {
    return page + BUCKET_HEADER_SIZE + (uint32_t)i * slot_size;
}

/** Round up to next power of 2. */
static uint64_t next_pow2(uint64_t v) {
    if (v == 0) return 1;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
}

static void write_header(disk_table_t *dt) {
    disk_table_header_t *hdr = (disk_table_header_t *)dt->base;
    hdr->magic            = DISK_TABLE_MAGIC;
    hdr->version          = DISK_TABLE_VERSION;
    hdr->key_size         = dt->key_size;
    hdr->record_size      = dt->record_size;
    hdr->bucket_count     = dt->bucket_count;
    hdr->bucket_mask      = dt->bucket_mask;
    hdr->entry_count      = dt->entry_count;
    hdr->overflow_count   = dt->overflow_count;
    hdr->slots_per_bucket = dt->slots_per_bucket;
    hdr->dirty            = dt->dirty ? 1 : 0;
}

static bool read_header(int fd, disk_table_header_t *hdr) {
    ssize_t n = pread(fd, hdr, sizeof(*hdr), 0);
    return n >= (ssize_t)sizeof(*hdr);
}

static bool remap(disk_table_t *dt, size_t new_size) {
    if (new_size <= dt->mapped_size) return true;

    if (ftruncate(dt->fd, (off_t)new_size) != 0)
        return false;

#ifdef __linux__
    uint8_t *p = mremap(dt->base, dt->mapped_size, new_size, MREMAP_MAYMOVE);
    if (p == MAP_FAILED) return false;
#else
    munmap(dt->base, dt->mapped_size);
    uint8_t *p = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                       MAP_SHARED, dt->fd, 0);
    if (p == MAP_FAILED) return false;
#endif

    dt->base = p;
    dt->mapped_size = new_size;
    return true;
}

static uint64_t alloc_overflow(disk_table_t *dt) {
    uint64_t new_id = dt->bucket_count + dt->overflow_count;
    dt->overflow_count++;

    size_t needed = bucket_offset(new_id + 1);
    if (needed > dt->mapped_size) {
        size_t grow = dt->mapped_size * 2;
        if (grow < needed) grow = needed;
        if (!remap(dt, grow)) {
            dt->overflow_count--;
            return UINT64_MAX;
        }
    }

    memset(bucket_ptr(dt, new_id), 0, PAGE_SIZE);
    return new_id;
}

/* =========================================================================
 * Crash safety
 * ========================================================================= */

static void mark_dirty(disk_table_t *dt) {
    if (dt->dirty) return;
    dt->dirty = true;
    write_header(dt);
}

static bool recover(disk_table_t *dt) {
    uint64_t total_pages = (dt->mapped_size - PAGE_SIZE) / PAGE_SIZE;
    if (total_pages > dt->bucket_count)
        dt->overflow_count = total_pages - dt->bucket_count;
    else
        dt->overflow_count = 0;

    uint64_t count = 0;
    uint64_t all_pages = dt->bucket_count + dt->overflow_count;

    for (uint64_t bid = 0; bid < all_pages; bid++) {
        const uint8_t *page = bucket_ptr_const(dt, bid);
        for (uint32_t i = 0; i < dt->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dt->slot_size, i);
            if (s[0] == SLOT_OCCUPIED)
                count++;
        }
    }

    dt->entry_count = count;
    dt->dirty = false;
    write_header(dt);
    msync(dt->base, PAGE_SIZE, MS_SYNC);
    return true;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

disk_table_t *disk_table_create(const char *path, uint32_t key_size,
                                uint32_t record_size, uint64_t capacity_hint) {
    if (!path || key_size < 9)
        return NULL;  /* need >= 9 bytes: 8 for index + 1 for fingerprint */

    /* slot: [1B flags][1B fingerprint][key][record] */
    uint32_t slot_size = 2 + key_size + record_size;
    uint32_t slots_per_bucket = (PAGE_SIZE - BUCKET_HEADER_SIZE) / slot_size;
    if (slots_per_bucket == 0) return NULL;

    uint64_t effective = (uint64_t)((double)slots_per_bucket * 0.75);
    if (effective == 0) effective = 1;
    uint64_t min_buckets = (capacity_hint + effective - 1) / effective;
    uint64_t bucket_count = next_pow2(min_buckets);
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

    disk_table_t *dt = calloc(1, sizeof(*dt));
    if (!dt) { munmap(base, file_size); close(fd); return NULL; }

    dt->fd               = fd;
    dt->base             = base;
    dt->mapped_size      = file_size;
    dt->key_size         = key_size;
    dt->record_size      = record_size;
    dt->slot_size        = slot_size;
    dt->slots_per_bucket = slots_per_bucket;
    dt->bucket_count     = bucket_count;
    dt->bucket_mask      = bucket_count - 1;
    dt->entry_count      = 0;
    dt->overflow_count   = 0;
    dt->dirty            = false;

    write_header(dt);
    return dt;
}

disk_table_t *disk_table_open(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR);
    if (fd < 0) return NULL;

    disk_table_header_t hdr;
    if (!read_header(fd, &hdr)) { close(fd); return NULL; }

    if (hdr.magic != DISK_TABLE_MAGIC || hdr.version != DISK_TABLE_VERSION) {
        close(fd);
        return NULL;
    }

    uint32_t slot_size = 2 + hdr.key_size + hdr.record_size;
    uint32_t expected_spb = (PAGE_SIZE - BUCKET_HEADER_SIZE) / slot_size;
    if (hdr.slots_per_bucket != expected_spb) {
        close(fd);
        return NULL;
    }

    /* Verify power-of-2 invariant */
    if (hdr.bucket_count == 0 ||
        (hdr.bucket_count & (hdr.bucket_count - 1)) != 0 ||
        hdr.bucket_mask != hdr.bucket_count - 1) {
        close(fd);
        return NULL;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return NULL; }
    size_t file_size = (size_t)st.st_size;

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

    disk_table_t *dt = calloc(1, sizeof(*dt));
    if (!dt) { munmap(base, file_size); close(fd); return NULL; }

    dt->fd               = fd;
    dt->base             = base;
    dt->mapped_size      = file_size;
    dt->key_size         = hdr.key_size;
    dt->record_size      = hdr.record_size;
    dt->slot_size        = slot_size;
    dt->slots_per_bucket = hdr.slots_per_bucket;
    dt->bucket_count     = hdr.bucket_count;
    dt->bucket_mask      = hdr.bucket_mask;
    dt->entry_count      = hdr.entry_count;
    dt->overflow_count   = hdr.overflow_count;
    dt->dirty            = (hdr.dirty != 0);

    if (dt->dirty) {
        if (!recover(dt)) {
            munmap(base, file_size);
            close(fd);
            free(dt);
            return NULL;
        }
    }

    return dt;
}

void disk_table_destroy(disk_table_t *dt) {
    if (!dt) return;
    if (dt->base) munmap(dt->base, dt->mapped_size);
    close(dt->fd);
    free(dt);
}

void disk_table_clear(disk_table_t *dt) {
    if (!dt) return;

    size_t bucket_start = PAGE_SIZE;
    size_t bucket_bytes = dt->bucket_count * (size_t)PAGE_SIZE;
    memset(dt->base + bucket_start, 0, bucket_bytes);

    if (dt->overflow_count > 0) {
        size_t new_size = PAGE_SIZE + dt->bucket_count * (size_t)PAGE_SIZE;
        if (ftruncate(dt->fd, (off_t)new_size) != 0) { /* best effort */ }
#ifdef __linux__
        uint8_t *p = mremap(dt->base, dt->mapped_size, new_size, MREMAP_MAYMOVE);
        if (p != MAP_FAILED) {
            dt->base = p;
            dt->mapped_size = new_size;
        }
#else
        munmap(dt->base, dt->mapped_size);
        dt->base = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, dt->fd, 0);
        dt->mapped_size = new_size;
#endif
    }

    dt->entry_count = 0;
    dt->overflow_count = 0;
    dt->dirty = false;
    write_header(dt);
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

static scan_result_t scan_chain_key(const disk_table_t *dt,
                                     uint64_t start_bucket,
                                     const uint8_t *key,
                                     uint8_t fp) {
    scan_result_t r;
    memset(&r, 0, sizeof(r));
    r.found     = false;
    r.free_slot = -1;

    uint64_t bid = start_bucket;
    while (1) {
        const uint8_t *page = bucket_ptr_const(dt, bid);
        const bucket_header_t *bh = (const bucket_header_t *)page;

        for (uint32_t i = 0; i < dt->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dt->slot_size, i);
            uint8_t flags = s[0];

            if (flags == SLOT_OCCUPIED) {
                /* Check fingerprint before full key compare */
                if (s[1] == fp &&
                    memcmp(s + 2, key, dt->key_size) == 0) {
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

static bool get_unlocked(const disk_table_t *dt, const uint8_t *key,
                          void *out) {
    uint64_t bid = key_to_bucket(key, dt->bucket_mask);
    uint8_t fp = key_fingerprint(key);
    scan_result_t r = scan_chain_key(dt, bid, key, fp);
    if (!r.found) return false;

    const uint8_t *page = bucket_ptr_const(dt, r.match_bucket_id);
    const uint8_t *s = slot_ptr_const(page, dt->slot_size, r.match_slot);
    if (out)
        memcpy(out, s + 2 + dt->key_size, dt->record_size);
    return true;
}

static bool put_unlocked(disk_table_t *dt, const uint8_t *key,
                          const void *record) {
    mark_dirty(dt);
    uint64_t bid = key_to_bucket(key, dt->bucket_mask);
    uint8_t fp = key_fingerprint(key);
    scan_result_t r = scan_chain_key(dt, bid, key, fp);

    if (r.found) {
        uint8_t *page = bucket_ptr(dt, r.match_bucket_id);
        uint8_t *s = slot_ptr(page, dt->slot_size, r.match_slot);
        memcpy(s + 2 + dt->key_size, record, dt->record_size);
        return true;
    }

    if (r.free_slot >= 0) {
        uint8_t *page = bucket_ptr(dt, r.free_bucket_id);
        bucket_header_t *bh = (bucket_header_t *)page;
        uint8_t *s = slot_ptr(page, dt->slot_size, (uint32_t)r.free_slot);

        bool was_tombstone = (s[0] == SLOT_TOMBSTONE);
        s[0] = SLOT_OCCUPIED;
        s[1] = fp;
        memcpy(s + 2, key, dt->key_size);
        memcpy(s + 2 + dt->key_size, record, dt->record_size);

        bh->count++;
        if (was_tombstone && bh->tombstone_count > 0)
            bh->tombstone_count--;

        dt->entry_count++;
        return true;
    }

    /* No free slot — allocate overflow */
    uint64_t new_id = alloc_overflow(dt);
    if (new_id == UINT64_MAX) return false;

    uint8_t *page = bucket_ptr(dt, new_id);
    bucket_header_t *bh = (bucket_header_t *)page;
    bh->count = 1;
    uint8_t *s = slot_ptr(page, dt->slot_size, 0);
    s[0] = SLOT_OCCUPIED;
    s[1] = fp;
    memcpy(s + 2, key, dt->key_size);
    memcpy(s + 2 + dt->key_size, record, dt->record_size);

    /* Link from last bucket */
    uint8_t *last_page = bucket_ptr(dt, r.last_bucket_id);
    bucket_header_t *last_bh = (bucket_header_t *)last_page;
    last_bh->overflow_id = new_id;

    dt->entry_count++;
    return true;
}

static bool delete_unlocked(disk_table_t *dt, const uint8_t *key) {
    mark_dirty(dt);
    uint64_t bid = key_to_bucket(key, dt->bucket_mask);
    uint8_t fp = key_fingerprint(key);
    scan_result_t r = scan_chain_key(dt, bid, key, fp);
    if (!r.found) return false;

    uint8_t *page = bucket_ptr(dt, r.match_bucket_id);
    bucket_header_t *bh = (bucket_header_t *)page;
    uint8_t *s = slot_ptr(page, dt->slot_size, r.match_slot);

    s[0] = SLOT_TOMBSTONE;
    if (bh->count > 0) bh->count--;
    bh->tombstone_count++;

    dt->entry_count--;
    return true;
}

/* --- Public API --- */

bool disk_table_get(const disk_table_t *dt, const uint8_t *key, void *out) {
    if (!dt || !key) return false;
    return get_unlocked(dt, key, out);
}

bool disk_table_put(disk_table_t *dt, const uint8_t *key, const void *record) {
    if (!dt || !key || !record) return false;
    return put_unlocked(dt, key, record);
}

bool disk_table_delete(disk_table_t *dt, const uint8_t *key) {
    if (!dt || !key) return false;
    return delete_unlocked(dt, key);
}

bool disk_table_contains(const disk_table_t *dt, const uint8_t *key) {
    if (!dt || !key) return false;
    uint64_t bid = key_to_bucket(key, dt->bucket_mask);
    uint8_t fp = key_fingerprint(key);
    scan_result_t r = scan_chain_key(dt, bid, key, fp);
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

uint32_t disk_table_batch_get(const disk_table_t *dt,
                              const uint8_t *keys, void *records,
                              bool *found, uint32_t count) {
    if (!dt || !keys || count == 0) return 0;

    if (found) memset(found, 0, count * sizeof(bool));

    batch_entry_t *entries = malloc(count * sizeof(batch_entry_t));
    if (!entries) return 0;

    for (uint32_t i = 0; i < count; i++) {
        entries[i].idx       = i;
        entries[i].bucket_id = key_to_bucket(
            keys + (uint64_t)i * dt->key_size, dt->bucket_mask);
    }

    qsort(entries, count, sizeof(batch_entry_t), cmp_batch_entry);

    /* Hint sequential access for sorted bucket scan */
    madvise(dt->base, dt->mapped_size, MADV_SEQUENTIAL);

    uint32_t found_count = 0;
    uint32_t gi = 0;

    while (gi < count) {
        uint64_t bid = entries[gi].bucket_id;

        uint64_t chain_bid = bid;
        bool chain_done = false;

        while (!chain_done) {
            const uint8_t *page = bucket_ptr_const(dt, chain_bid);
            const bucket_header_t *bh = (const bucket_header_t *)page;

            for (uint32_t g = gi; g < count && entries[g].bucket_id == bid; g++) {
                if (found && found[entries[g].idx]) continue;

                const uint8_t *search_key = keys +
                    (uint64_t)entries[g].idx * dt->key_size;
                uint8_t fp = key_fingerprint(search_key);

                for (uint32_t s = 0; s < dt->slots_per_bucket; s++) {
                    const uint8_t *slot = slot_ptr_const(page, dt->slot_size, s);
                    if (slot[0] == SLOT_OCCUPIED &&
                        slot[1] == fp &&
                        memcmp(slot + 2, search_key, dt->key_size) == 0) {
                        if (records)
                            memcpy((uint8_t *)records +
                                   (uint64_t)entries[g].idx * dt->record_size,
                                   slot + 2 + dt->key_size,
                                   dt->record_size);
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

    /* Restore random access hint for normal operations */
    madvise(dt->base, dt->mapped_size, MADV_RANDOM);

    free(entries);
    return found_count;
}

bool disk_table_batch_put(disk_table_t *dt,
                          const uint8_t *keys, const void *records,
                          uint32_t count) {
    if (!dt || !keys || !records || count == 0) return false;

    batch_entry_t *entries = malloc(count * sizeof(batch_entry_t));
    if (!entries) return false;

    for (uint32_t i = 0; i < count; i++) {
        entries[i].idx       = i;
        entries[i].bucket_id = key_to_bucket(
            keys + (uint64_t)i * dt->key_size, dt->bucket_mask);
    }

    qsort(entries, count, sizeof(batch_entry_t), cmp_batch_entry);

    madvise(dt->base, dt->mapped_size, MADV_SEQUENTIAL);

    uint32_t gi = 0;
    bool ok = true;

    while (gi < count && ok) {
        uint64_t bid = entries[gi].bucket_id;
        uint32_t gend = gi;
        while (gend < count && entries[gend].bucket_id == bid) gend++;

        for (uint32_t g = gi; g < gend && ok; g++) {
            uint32_t orig = entries[g].idx;
            const uint8_t *k = keys + (uint64_t)orig * dt->key_size;
            const uint8_t *r = (const uint8_t *)records +
                               (uint64_t)orig * dt->record_size;
            ok = put_unlocked(dt, k, r);
        }

        gi = gend;
    }

    madvise(dt->base, dt->mapped_size, MADV_RANDOM);

    free(entries);
    return ok;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t disk_table_count(const disk_table_t *dt) {
    return dt ? dt->entry_count : 0;
}

uint64_t disk_table_capacity(const disk_table_t *dt) {
    return dt ? dt->bucket_count * (uint64_t)dt->slots_per_bucket : 0;
}

/* =========================================================================
 * Iteration
 * ========================================================================= */

void disk_table_foreach_key(const disk_table_t *dt, disk_table_key_cb_t cb,
                            void *user_data) {
    if (!dt || !cb) return;

    uint64_t total_pages = dt->bucket_count + dt->overflow_count;

    for (uint64_t bid = 0; bid < total_pages; bid++) {
        const uint8_t *page = bucket_ptr_const(dt, bid);
        for (uint32_t i = 0; i < dt->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dt->slot_size, i);
            if (s[0] == SLOT_OCCUPIED)
                cb(s + 2, user_data);
        }
    }
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void disk_table_sync(disk_table_t *dt) {
    if (!dt) return;
    dt->dirty = false;
    write_header(dt);
}
