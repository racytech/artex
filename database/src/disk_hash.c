/*
 * Disk Hash — Zero-RAM disk-backed hash table.
 *
 * Bucket-based open hashing with pread/pwrite.  Each bucket is one
 * 4096-byte page containing a small header and up to 62 fixed-size slots
 * (for key=32, record=32).  Overflow buckets are chained when a primary
 * bucket fills.
 *
 * The only in-memory state is the struct itself (~48 bytes).  The hash
 * function maps keys directly to bucket offsets — no in-memory index.
 */

#include "../include/disk_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

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
    uint8_t  dirty;             /* 1 = unclean shutdown possible */
    /* 45 bytes used, pad to 64 */
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
    uint32_t  key_size;
    uint32_t  record_size;
    uint32_t  slot_size;            /* 1 + key_size + record_size */
    uint32_t  slots_per_bucket;
    uint64_t  bucket_count;
    uint64_t  entry_count;
    uint64_t  overflow_count;
    bool      dirty;                /* true = header needs recovery on open */
    pthread_rwlock_t rwlock;
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

static inline uint64_t bucket_offset(uint64_t bucket_id) {
    return PAGE_SIZE + bucket_id * (uint64_t)PAGE_SIZE;
}

/** Read a full bucket page.  Returns false on I/O error. */
static bool read_bucket(int fd, uint64_t bucket_id, uint8_t page[PAGE_SIZE]) {
    ssize_t n = pread(fd, page, PAGE_SIZE, (off_t)bucket_offset(bucket_id));
    if (n < PAGE_SIZE) {
        /* Sparse file: unwritten pages read as zeroes on Linux */
        if (n >= 0) {
            memset(page + n, 0, PAGE_SIZE - (size_t)n);
            return true;
        }
        return false;
    }
    return true;
}

/** Write a full bucket page. */
static bool write_bucket(int fd, uint64_t bucket_id,
                          const uint8_t page[PAGE_SIZE]) {
    ssize_t n = pwrite(fd, page, PAGE_SIZE, (off_t)bucket_offset(bucket_id));
    return n == PAGE_SIZE;
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
    uint8_t page[PAGE_SIZE];
    memset(page, 0, PAGE_SIZE);

    disk_hash_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic            = DISK_HASH_MAGIC;
    hdr.version          = DISK_HASH_VERSION;
    hdr.key_size         = dh->key_size;
    hdr.record_size      = dh->record_size;
    hdr.bucket_count     = dh->bucket_count;
    hdr.entry_count      = dh->entry_count;
    hdr.overflow_count   = dh->overflow_count;
    hdr.slots_per_bucket = dh->slots_per_bucket;
    hdr.dirty            = dh->dirty ? 1 : 0;
    memcpy(page, &hdr, sizeof(hdr));

    (void)pwrite(dh->fd, page, PAGE_SIZE, 0);
}

static bool read_header(int fd, disk_hash_header_t *hdr) {
    uint8_t page[PAGE_SIZE];
    ssize_t n = pread(fd, page, PAGE_SIZE, 0);
    if (n < (ssize_t)sizeof(disk_hash_header_t)) return false;
    memcpy(hdr, page, sizeof(*hdr));
    return true;
}

/** Allocate a new overflow bucket ID.  Returns its absolute bucket ID.
 *  Caller is responsible for writing the data page before linking. */
static uint64_t alloc_overflow(disk_hash_t *dh) {
    uint64_t new_id = dh->bucket_count + dh->overflow_count;
    dh->overflow_count++;
    return new_id;
}

/* =========================================================================
 * Crash safety — dirty flag + recovery
 * ========================================================================= */

/** Mark table as dirty on first mutation after open/sync.
 *  Writes header once per clean→dirty transition. */
static void mark_dirty(disk_hash_t *dh) {
    if (dh->dirty) return;
    dh->dirty = true;
    write_header(dh);
}

/** Recovery scan after unclean shutdown.
 *  Derives overflow_count from file size, counts occupied slots for
 *  entry_count. Clears dirty flag and writes corrected header. */
static bool recover(disk_hash_t *dh) {
    /* Derive total pages from file size */
    off_t file_size = lseek(dh->fd, 0, SEEK_END);
    if (file_size < (off_t)PAGE_SIZE) return false;

    uint64_t total_pages = ((uint64_t)file_size - PAGE_SIZE) / PAGE_SIZE;
    if (total_pages > dh->bucket_count)
        dh->overflow_count = total_pages - dh->bucket_count;
    else
        dh->overflow_count = 0;

    /* Count occupied slots across all bucket pages */
    uint64_t count = 0;
    uint8_t page[PAGE_SIZE];

    for (uint64_t bid = 0; bid < total_pages; bid++) {
        if (!read_bucket(dh->fd, bid, page)) continue;
        for (uint32_t i = 0; i < dh->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dh->slot_size, i);
            if (s[0] == SLOT_OCCUPIED)
                count++;
        }
    }

    dh->entry_count = count;

    /* Clear dirty flag and persist corrected header */
    dh->dirty = false;
    write_header(dh);
    fsync(dh->fd);
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

    /* bucket_count = ceil(capacity_hint / (slots_per_bucket * 0.75)) */
    uint64_t effective = (uint64_t)((double)slots_per_bucket * 0.75);
    if (effective == 0) effective = 1;
    uint64_t bucket_count = (capacity_hint + effective - 1) / effective;
    if (bucket_count == 0) bucket_count = 1;

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return NULL;

    /* Sparse file: set size without writing data */
    uint64_t file_size = PAGE_SIZE + bucket_count * (uint64_t)PAGE_SIZE;
    if (ftruncate(fd, (off_t)file_size) != 0) {
        close(fd);
        return NULL;
    }

    disk_hash_t *dh = calloc(1, sizeof(*dh));
    if (!dh) { close(fd); return NULL; }

    dh->fd               = fd;
    dh->key_size         = key_size;
    dh->record_size      = record_size;
    dh->slot_size        = slot_size;
    dh->slots_per_bucket = slots_per_bucket;
    dh->bucket_count     = bucket_count;
    dh->entry_count      = 0;
    dh->overflow_count   = 0;
    dh->dirty            = false;
    pthread_rwlock_init(&dh->rwlock, NULL);

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

    disk_hash_t *dh = calloc(1, sizeof(*dh));
    if (!dh) { close(fd); return NULL; }

    dh->fd               = fd;
    dh->key_size         = hdr.key_size;
    dh->record_size      = hdr.record_size;
    dh->slot_size        = slot_size;
    dh->slots_per_bucket = hdr.slots_per_bucket;
    dh->bucket_count     = hdr.bucket_count;
    dh->entry_count      = hdr.entry_count;
    dh->overflow_count   = hdr.overflow_count;
    dh->dirty            = (hdr.dirty != 0);
    pthread_rwlock_init(&dh->rwlock, NULL);

    /* Unclean shutdown: recover entry_count and overflow_count from disk */
    if (dh->dirty) {
        if (!recover(dh)) {
            close(fd);
            free(dh);
            return NULL;
        }
    }

    return dh;
}

disk_hash_t *disk_hash_open_norecovery(const char *path) {
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

    disk_hash_t *dh = calloc(1, sizeof(*dh));
    if (!dh) { close(fd); return NULL; }

    dh->fd               = fd;
    dh->key_size         = hdr.key_size;
    dh->record_size      = hdr.record_size;
    dh->slot_size        = slot_size;
    dh->slots_per_bucket = hdr.slots_per_bucket;
    dh->bucket_count     = hdr.bucket_count;
    dh->entry_count      = hdr.entry_count;
    dh->overflow_count   = hdr.overflow_count;
    dh->dirty            = false;  /* skip recovery — caller accepts stale metadata */
    pthread_rwlock_init(&dh->rwlock, NULL);

    return dh;
}

void disk_hash_destroy(disk_hash_t *dh) {
    if (!dh) return;
    pthread_rwlock_destroy(&dh->rwlock);
    close(dh->fd);
    free(dh);
}

void disk_hash_clear(disk_hash_t *dh) {
    if (!dh) return;
    /* Truncate to just the header, then re-extend to original bucket count.
     * This zeros all bucket pages (sparse file) without file close/reopen. */
    uint64_t file_size = PAGE_SIZE + dh->bucket_count * (uint64_t)PAGE_SIZE;
    ftruncate(dh->fd, PAGE_SIZE);
    ftruncate(dh->fd, (off_t)file_size);
    dh->entry_count = 0;
    dh->overflow_count = 0;
    dh->dirty = false;
    write_header(dh);
}

/* =========================================================================
 * Internal: scan a bucket chain for a key
 *
 * Returns:
 *   found = true   → *out_slot_idx, *out_bucket_id point to the match
 *   found = false  → *out_slot_idx = first free/tombstone slot (or -1),
 *                     *out_bucket_id = bucket containing that slot,
 *                     *out_last_bucket = last bucket in chain
 *
 * page[] is always left containing the bucket identified by *out_bucket_id
 * (the one with the match, or the one with the free slot, or the last one
 * in the chain).
 * ========================================================================= */

typedef struct {
    bool     found;
    uint64_t match_bucket_id;
    uint32_t match_slot;
    int64_t  free_slot;         /* -1 = none found */
    uint64_t free_bucket_id;
    uint64_t last_bucket_id;
} scan_result_t;

static scan_result_t scan_chain(const disk_hash_t *dh, uint64_t start_bucket,
                                 const uint8_t *key, uint8_t page[PAGE_SIZE]) {
    scan_result_t r;
    memset(&r, 0, sizeof(r));
    r.found        = false;
    r.free_slot    = -1;

    uint64_t bid = start_bucket;
    while (1) {
        if (!read_bucket(dh->fd, bid, page)) break;

        const bucket_header_t *bh = (const bucket_header_t *)page;

        for (uint32_t i = 0; i < dh->slots_per_bucket; i++) {
            const uint8_t *s = slot_ptr_const(page, dh->slot_size, i);
            uint8_t flags = s[0];

            if (flags == SLOT_OCCUPIED) {
                if (memcmp(s + 1, key, dh->key_size) == 0) {
                    r.found          = true;
                    r.match_bucket_id = bid;
                    r.match_slot     = i;
                    r.last_bucket_id = bid;
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

    /* Re-read the page containing the free slot (if it's not the last page) */
    if (r.free_slot >= 0 && r.free_bucket_id != r.last_bucket_id) {
        read_bucket(dh->fd, r.free_bucket_id, page);
    }

    return r;
}

/* =========================================================================
 * Single Operations
 * ========================================================================= */

/* --- Unlocked internals (caller holds appropriate lock) --- */

static bool get_unlocked(const disk_hash_t *dh, const uint8_t *key, void *out) {
    uint64_t bid = hash_key(key) % dh->bucket_count;
    uint8_t page[PAGE_SIZE];

    scan_result_t r = scan_chain(dh, bid, key, page);
    if (!r.found) return false;

    const uint8_t *s = slot_ptr_const(page, dh->slot_size, r.match_slot);
    if (out)
        memcpy(out, s + 1 + dh->key_size, dh->record_size);
    return true;
}

static bool put_unlocked(disk_hash_t *dh, const uint8_t *key, const void *record) {
    mark_dirty(dh);
    uint64_t bid = hash_key(key) % dh->bucket_count;
    uint8_t page[PAGE_SIZE];

    scan_result_t r = scan_chain(dh, bid, key, page);

    if (r.found) {
        /* Update existing — page contains the match bucket */
        uint8_t *s = slot_ptr(page, dh->slot_size, r.match_slot);
        memcpy(s + 1 + dh->key_size, record, dh->record_size);
        return write_bucket(dh->fd, r.match_bucket_id, page);
    }

    /* Insert new entry */
    if (r.free_slot >= 0) {
        /* Page already contains the free-slot bucket (scan_chain re-read it) */
        bucket_header_t *bh = (bucket_header_t *)page;
        uint8_t *s = slot_ptr(page, dh->slot_size, (uint32_t)r.free_slot);

        bool was_tombstone = (s[0] == SLOT_TOMBSTONE);
        s[0] = SLOT_OCCUPIED;
        memcpy(s + 1, key, dh->key_size);
        memcpy(s + 1 + dh->key_size, record, dh->record_size);

        bh->count++;
        if (was_tombstone && bh->tombstone_count > 0)
            bh->tombstone_count--;

        if (!write_bucket(dh->fd, r.free_bucket_id, page))
            return false;
        dh->entry_count++;
        return true;
    }

    /* No free slot in chain — allocate overflow bucket.
     * Crash-safe write order: data page FIRST, then parent link.
     * Crash after data but before link = orphaned page (harmless 4KB waste).
     * Crash after link = correct state. */
    uint64_t new_id = alloc_overflow(dh);

    /* Step 1: Write data into new overflow bucket */
    memset(page, 0, PAGE_SIZE);
    bucket_header_t *bh = (bucket_header_t *)page;
    bh->count = 1;
    uint8_t *s = slot_ptr(page, dh->slot_size, 0);
    s[0] = SLOT_OCCUPIED;
    memcpy(s + 1, key, dh->key_size);
    memcpy(s + 1 + dh->key_size, record, dh->record_size);

    if (!write_bucket(dh->fd, new_id, page))
        return false;

    /* Step 2: Link from last bucket in chain */
    if (!read_bucket(dh->fd, r.last_bucket_id, page))
        return false;
    bh = (bucket_header_t *)page;
    bh->overflow_id = (uint32_t)new_id;
    if (!write_bucket(dh->fd, r.last_bucket_id, page))
        return false;

    dh->entry_count++;
    return true;
}

static bool delete_unlocked(disk_hash_t *dh, const uint8_t *key) {
    mark_dirty(dh);
    uint64_t bid = hash_key(key) % dh->bucket_count;
    uint8_t page[PAGE_SIZE];

    scan_result_t r = scan_chain(dh, bid, key, page);
    if (!r.found) return false;

    /* scan_chain returns immediately on match → page = match bucket */
    bucket_header_t *bh = (bucket_header_t *)page;
    uint8_t *s = slot_ptr(page, dh->slot_size, r.match_slot);

    s[0] = SLOT_TOMBSTONE;
    if (bh->count > 0) bh->count--;
    bh->tombstone_count++;

    if (!write_bucket(dh->fd, r.match_bucket_id, page))
        return false;

    dh->entry_count--;
    return true;
}

/* --- Public API (locked) --- */

bool disk_hash_get(const disk_hash_t *dh, const uint8_t *key, void *out) {
    if (!dh || !key) return false;
    pthread_rwlock_rdlock((pthread_rwlock_t *)&dh->rwlock);
    bool ok = get_unlocked(dh, key, out);
    pthread_rwlock_unlock((pthread_rwlock_t *)&dh->rwlock);
    return ok;
}

bool disk_hash_put(disk_hash_t *dh, const uint8_t *key, const void *record) {
    if (!dh || !key || !record) return false;
    pthread_rwlock_wrlock(&dh->rwlock);
    bool ok = put_unlocked(dh, key, record);
    pthread_rwlock_unlock(&dh->rwlock);
    return ok;
}

bool disk_hash_delete(disk_hash_t *dh, const uint8_t *key) {
    if (!dh || !key) return false;
    pthread_rwlock_wrlock(&dh->rwlock);
    bool ok = delete_unlocked(dh, key);
    pthread_rwlock_unlock(&dh->rwlock);
    return ok;
}

bool disk_hash_contains(const disk_hash_t *dh, const uint8_t *key) {
    if (!dh || !key) return false;
    pthread_rwlock_rdlock((pthread_rwlock_t *)&dh->rwlock);
    uint64_t bid = hash_key(key) % dh->bucket_count;
    uint8_t page[PAGE_SIZE];
    scan_result_t r = scan_chain(dh, bid, key, page);
    pthread_rwlock_unlock((pthread_rwlock_t *)&dh->rwlock);
    return r.found;
}

/* =========================================================================
 * Batch Operations
 * ========================================================================= */

typedef struct {
    uint32_t idx;           /* original index in caller's array */
    uint64_t bucket_id;     /* hash_key(key) % bucket_count */
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

    /* Initialize found[] */
    if (found) memset(found, 0, count * sizeof(bool));

    pthread_rwlock_rdlock((pthread_rwlock_t *)&dh->rwlock);

    batch_entry_t *entries = malloc(count * sizeof(batch_entry_t));
    if (!entries) { pthread_rwlock_unlock((pthread_rwlock_t *)&dh->rwlock); return 0; }

    for (uint32_t i = 0; i < count; i++) {
        entries[i].idx       = i;
        entries[i].bucket_id = hash_key(keys + (uint64_t)i * dh->key_size)
                               % dh->bucket_count;
    }

    qsort(entries, count, sizeof(batch_entry_t), cmp_batch_entry);

    uint32_t found_count = 0;
    uint8_t page[PAGE_SIZE];
    uint32_t gi = 0;

    while (gi < count) {
        uint64_t bid = entries[gi].bucket_id;

        /* Walk the bucket chain once for this group */
        uint64_t chain_bid = bid;
        bool chain_done = false;

        while (!chain_done) {
            if (!read_bucket(dh->fd, chain_bid, page)) break;
            const bucket_header_t *bh = (const bucket_header_t *)page;

            /* Scan all slots for each key in this group */
            for (uint32_t g = gi; g < count && entries[g].bucket_id == bid; g++) {
                if (found && found[entries[g].idx]) continue; /* already found */

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

        /* Advance past this bucket group */
        while (gi < count && entries[gi].bucket_id == bid) gi++;
    }

    free(entries);
    pthread_rwlock_unlock((pthread_rwlock_t *)&dh->rwlock);
    return found_count;
}

bool disk_hash_batch_put(disk_hash_t *dh,
                          const uint8_t *keys, const void *records,
                          uint32_t count) {
    if (!dh || !keys || !records || count == 0) return false;

    pthread_rwlock_wrlock(&dh->rwlock);

    batch_entry_t *entries = malloc(count * sizeof(batch_entry_t));
    if (!entries) { pthread_rwlock_unlock(&dh->rwlock); return false; }

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

        /* Collect group end */
        uint32_t gend = gi;
        while (gend < count && entries[gend].bucket_id == bid) gend++;

        /* Process each entry in this group via unlocked put path. */
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
    pthread_rwlock_unlock(&dh->rwlock);
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
 * Durability
 * ========================================================================= */

void disk_hash_sync(disk_hash_t *dh) {
    if (!dh) return;
    pthread_rwlock_wrlock(&dh->rwlock);
    dh->dirty = false;
    write_header(dh);
    fdatasync(dh->fd);
    pthread_rwlock_unlock(&dh->rwlock);
}

void disk_hash_refresh(disk_hash_t *dh) {
    if (!dh) return;
    disk_hash_header_t hdr;
    if (read_header(dh->fd, &hdr)) {
        dh->entry_count    = hdr.entry_count;
        dh->overflow_count = hdr.overflow_count;
        dh->dirty          = false;
    }
}
