/*
 * Code Store — Content-Addressed, Append-Only, Deduplicated.
 *
 * Two-file design:
 *   <path>.idx — disk_table index: code_hash (32B) → {offset, length} (12B)
 *   <path>.dat — mmap'd append-only flat file of raw code bytes
 *
 * Deduplication is free: same code → same hash → disk_table_contains = true → skip.
 * Thread-safe: reads from mmap (lock-free), writes serialized by mutex.
 * Crash-safe: data written before index (orphaned bytes = harmless).
 */

#define _GNU_SOURCE  /* mremap */

#include "../include/code_store.h"
#include "../include/disk_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define CODE_STORE_MAGIC    0x54534443U   /* "CDST" little-endian */
#define CODE_STORE_VERSION  1
#define PAGE_SIZE           4096
#define CODE_HASH_SIZE      32
#define DAT_INITIAL_MAP_SIZE (4 * 1024 * 1024)  /* 4 MB */

/* =========================================================================
 * On-disk data file header (first 4096 bytes)
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t data_size;     /* total bytes of code data written */
    uint8_t  reserved[4080];
} code_store_header_t;

_Static_assert(sizeof(code_store_header_t) == PAGE_SIZE,
               "code_store_header_t must be 4096 bytes");

/* =========================================================================
 * Index record (stored in disk_table as the record)
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint64_t offset;    /* byte offset in data file (from start of data region) */
    uint32_t length;    /* code length in bytes */
} code_record_t;

_Static_assert(sizeof(code_record_t) == 12, "code_record_t must be 12 bytes");

/* =========================================================================
 * LRU Code Cache
 *
 * In-memory LRU cache for hot contract bytecode, keyed by code_hash.
 * Eliminates disk_table lookup for frequently-called contracts.
 *
 * Layout: flat pre-allocated entry array + hash table + doubly-linked
 * LRU list. All operations O(1). Variable-length code via malloc'd buffers.
 *
 * Default 32768 entries — covers the hot set through late mainnet.
 * At ~10KB average code size, that's ~320MB memory.
 * ========================================================================= */

#define CCACHE_SENTINEL 0xFFFFFFFFU
#ifndef CCACHE_DEFAULT_CAP
#define CCACHE_DEFAULT_CAP 32768
#endif

typedef struct {
    uint8_t   hash[32];
    uint8_t  *code;         /* malloc'd code bytes (owned, NULL if empty) */
    uint32_t  code_len;
    uint32_t  ht_next;      /* hash bucket chain */
    uint32_t  lru_prev;     /* doubly-linked LRU */
    uint32_t  lru_next;
} ccache_entry_t;

typedef struct {
    ccache_entry_t *entries;
    uint32_t       *buckets;
    uint32_t        capacity;
    uint32_t        bucket_mask;    /* bucket_count - 1 (power of 2) */
    uint32_t        count;
    uint32_t        lru_head;       /* most recently used */
    uint32_t        lru_tail;       /* least recently used */
    uint32_t        free_head;      /* free list (chained via lru_next) */
    uint64_t        hits;
    uint64_t        misses;
} code_cache_t;

static code_cache_t *ccache_create(uint32_t capacity) {
    if (capacity == 0) return NULL;
    code_cache_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    uint32_t bc = 1;
    while (bc < capacity * 2) bc <<= 1;

    c->entries = calloc(capacity, sizeof(ccache_entry_t));
    c->buckets = malloc(bc * sizeof(uint32_t));
    if (!c->entries || !c->buckets) {
        free(c->entries); free(c->buckets); free(c);
        return NULL;
    }

    c->capacity    = capacity;
    c->bucket_mask = bc - 1;
    c->lru_head    = CCACHE_SENTINEL;
    c->lru_tail    = CCACHE_SENTINEL;

    for (uint32_t i = 0; i < bc; i++)
        c->buckets[i] = CCACHE_SENTINEL;
    for (uint32_t i = 0; i < capacity; i++) {
        c->entries[i].lru_next = (i + 1 < capacity) ? i + 1 : CCACHE_SENTINEL;
        c->entries[i].code = NULL;
    }
    c->free_head = 0;
    return c;
}

static void ccache_destroy(code_cache_t *c) {
    if (!c) return;
    for (uint32_t i = 0; i < c->capacity; i++)
        free(c->entries[i].code);
    free(c->entries);
    free(c->buckets);
    free(c);
}

static inline uint32_t ccache_bucket(const code_cache_t *c,
                                      const uint8_t hash[32]) {
    uint64_t h;
    memcpy(&h, hash, 8);
    return (uint32_t)(h & c->bucket_mask);
}

static void ccache_lru_remove(code_cache_t *c, uint32_t idx) {
    ccache_entry_t *e = &c->entries[idx];
    if (e->lru_prev != CCACHE_SENTINEL)
        c->entries[e->lru_prev].lru_next = e->lru_next;
    else
        c->lru_head = e->lru_next;
    if (e->lru_next != CCACHE_SENTINEL)
        c->entries[e->lru_next].lru_prev = e->lru_prev;
    else
        c->lru_tail = e->lru_prev;
}

static void ccache_lru_push_front(code_cache_t *c, uint32_t idx) {
    ccache_entry_t *e = &c->entries[idx];
    e->lru_prev = CCACHE_SENTINEL;
    e->lru_next = c->lru_head;
    if (c->lru_head != CCACHE_SENTINEL)
        c->entries[c->lru_head].lru_prev = idx;
    c->lru_head = idx;
    if (c->lru_tail == CCACHE_SENTINEL)
        c->lru_tail = idx;
}

static void ccache_ht_remove(code_cache_t *c, uint32_t idx) {
    uint32_t b = ccache_bucket(c, c->entries[idx].hash);
    uint32_t *prev = &c->buckets[b];
    while (*prev != CCACHE_SENTINEL) {
        if (*prev == idx) {
            *prev = c->entries[idx].ht_next;
            return;
        }
        prev = &c->entries[*prev].ht_next;
    }
}

/* Look up code by hash. On hit, copies to buf and promotes to MRU.
 * Returns code_len, or 0 on miss. Zero-length codes are not cached. */
static uint32_t ccache_get(code_cache_t *c, const uint8_t hash[32],
                            uint8_t *buf, uint32_t buf_len) {
    uint32_t b = ccache_bucket(c, hash);
    uint32_t idx = c->buckets[b];
    while (idx != CCACHE_SENTINEL) {
        ccache_entry_t *e = &c->entries[idx];
        if (memcmp(e->hash, hash, 32) == 0) {
            c->hits++;
            ccache_lru_remove(c, idx);
            ccache_lru_push_front(c, idx);
            if (e->code_len > 0 && buf && buf_len >= e->code_len)
                memcpy(buf, e->code, e->code_len);
            return e->code_len;
        }
        idx = e->ht_next;
    }
    c->misses++;
    return 0;
}

/* Get code length without copying data. Promotes to MRU on hit. */
static uint32_t ccache_get_size(code_cache_t *c, const uint8_t hash[32]) {
    uint32_t b = ccache_bucket(c, hash);
    uint32_t idx = c->buckets[b];
    while (idx != CCACHE_SENTINEL) {
        ccache_entry_t *e = &c->entries[idx];
        if (memcmp(e->hash, hash, 32) == 0) {
            c->hits++;
            ccache_lru_remove(c, idx);
            ccache_lru_push_front(c, idx);
            return e->code_len;
        }
        idx = e->ht_next;
    }
    c->misses++;
    return 0;
}

/* Insert code into cache. Caller retains ownership of code bytes (copied). */
static void ccache_put(code_cache_t *c, const uint8_t hash[32],
                        const uint8_t *code, uint32_t code_len) {
    if (code_len == 0) return;  /* don't cache empty (ambiguous with miss) */

    /* Already present? Update in place */
    uint32_t b = ccache_bucket(c, hash);
    uint32_t idx = c->buckets[b];
    while (idx != CCACHE_SENTINEL) {
        if (memcmp(c->entries[idx].hash, hash, 32) == 0) {
            ccache_lru_remove(c, idx);
            ccache_lru_push_front(c, idx);
            return;
        }
        idx = c->entries[idx].ht_next;
    }

    /* Allocate slot */
    uint32_t slot;
    if (c->free_head != CCACHE_SENTINEL) {
        slot = c->free_head;
        c->free_head = c->entries[slot].lru_next;
    } else {
        /* Evict LRU tail */
        slot = c->lru_tail;
        free(c->entries[slot].code);
        c->entries[slot].code = NULL;
        ccache_lru_remove(c, slot);
        ccache_ht_remove(c, slot);
        c->count--;
    }

    /* Copy code bytes */
    uint8_t *copy = malloc(code_len);
    if (!copy) {
        /* OOM — return slot to free list */
        c->entries[slot].lru_next = c->free_head;
        c->free_head = slot;
        return;
    }
    memcpy(copy, code, code_len);

    /* Fill entry */
    ccache_entry_t *e = &c->entries[slot];
    memcpy(e->hash, hash, 32);
    e->code = copy;
    e->code_len = code_len;

    /* Insert into hash bucket */
    b = ccache_bucket(c, hash);
    e->ht_next = c->buckets[b];
    c->buckets[b] = slot;

    ccache_lru_push_front(c, slot);
    c->count++;
}

/* =========================================================================
 * In-memory structure
 * ========================================================================= */

struct code_store {
    disk_table_t     *index;         /* code_hash → code_record_t */
    int              data_fd;       /* backing fd for mmap */
    uint8_t         *data_base;     /* mmap base for .dat file */
    size_t           data_mapped;   /* current mmap size */
    uint64_t         data_size;     /* current write position (bytes after header) */
    pthread_mutex_t  write_lock;    /* serialize appends + mremap */
    char            *idx_path;      /* owned, for cleanup */
    char            *dat_path;      /* owned, for cleanup */

    /* LRU code cache (NULL = disabled) */
    code_cache_t    *cache;
};

/* =========================================================================
 * Helpers
 * ========================================================================= */

static char *make_path(const char *base, const char *ext) {
    size_t blen = strlen(base);
    size_t elen = strlen(ext);
    char *p = malloc(blen + elen + 1);
    if (!p) return NULL;
    memcpy(p, base, blen);
    memcpy(p + blen, ext, elen + 1);
    return p;
}

/* Write header to mmap'd .dat (first page) */
static void write_data_header(code_store_t *cs) {
    code_store_header_t *hdr = (code_store_header_t *)cs->data_base;
    hdr->magic     = CODE_STORE_MAGIC;
    hdr->version   = CODE_STORE_VERSION;
    hdr->data_size = cs->data_size;
}

static bool read_data_header(int fd, code_store_header_t *hdr) {
    ssize_t n = pread(fd, hdr, PAGE_SIZE, 0);
    if (n < (ssize_t)sizeof(*hdr)) return false;
    return true;
}

/* Grow the mmap'd .dat file to at least `needed` bytes.
 * Uses mremap(MREMAP_MAYMOVE) — fast, no munmap+mmap cycle. */
static bool dat_remap(code_store_t *cs, size_t needed) {
    /* Double until large enough */
    size_t new_sz = cs->data_mapped;
    while (new_sz < needed)
        new_sz *= 2;

    if (ftruncate(cs->data_fd, (off_t)new_sz) != 0)
        return false;

    void *p = mremap(cs->data_base, cs->data_mapped, new_sz, MREMAP_MAYMOVE);
    if (p == MAP_FAILED)
        return false;

    cs->data_base   = p;
    cs->data_mapped = new_sz;
    return true;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

code_store_t *code_store_create(const char *path, uint64_t capacity_hint) {
    if (!path) return NULL;

    char *idx_path = make_path(path, ".idx");
    char *dat_path = make_path(path, ".dat");
    if (!idx_path || !dat_path) { free(idx_path); free(dat_path); return NULL; }

    /* Create disk_table index: 32B key → 12B record */
    disk_table_t *index = disk_table_create(idx_path, CODE_HASH_SIZE,
                                           sizeof(code_record_t), capacity_hint);
    if (!index) { free(idx_path); free(dat_path); return NULL; }

    /* Create data file */
    int data_fd = open(dat_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (data_fd < 0) {
        disk_table_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    code_store_t *cs = calloc(1, sizeof(*cs));
    if (!cs) {
        close(data_fd);
        disk_table_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    /* mmap the .dat file */
    size_t init_sz = DAT_INITIAL_MAP_SIZE;
    if (ftruncate(data_fd, (off_t)init_sz) != 0) {
        close(data_fd);
        disk_table_destroy(index);
        free(idx_path); free(dat_path);
        free(cs);
        return NULL;
    }
    cs->data_base = mmap(NULL, init_sz, PROT_READ | PROT_WRITE,
                         MAP_SHARED, data_fd, 0);
    if (cs->data_base == MAP_FAILED) {
        close(data_fd);
        disk_table_destroy(index);
        free(idx_path); free(dat_path);
        free(cs);
        return NULL;
    }
    cs->data_mapped = init_sz;

    cs->index     = index;
    cs->data_fd   = data_fd;
    cs->data_size = 0;
    cs->idx_path  = idx_path;
    cs->dat_path  = dat_path;
    pthread_mutex_init(&cs->write_lock, NULL);
    cs->cache = ccache_create(CCACHE_DEFAULT_CAP);

    /* Write header to mmap */
    write_data_header(cs);

    return cs;
}

code_store_t *code_store_open(const char *path) {
    if (!path) return NULL;

    char *idx_path = make_path(path, ".idx");
    char *dat_path = make_path(path, ".dat");
    if (!idx_path || !dat_path) { free(idx_path); free(dat_path); return NULL; }

    /* Open disk_table index */
    disk_table_t *index = disk_table_open(idx_path);
    if (!index) { free(idx_path); free(dat_path); return NULL; }

    /* Open data file */
    int data_fd = open(dat_path, O_RDWR);
    if (data_fd < 0) {
        disk_table_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    /* Read and validate header */
    code_store_header_t hdr;
    if (!read_data_header(data_fd, &hdr) ||
        hdr.magic != CODE_STORE_MAGIC ||
        hdr.version != CODE_STORE_VERSION) {
        close(data_fd);
        disk_table_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    code_store_t *cs = calloc(1, sizeof(*cs));
    if (!cs) {
        close(data_fd);
        disk_table_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    /* mmap the .dat file */
    struct stat sb;
    if (fstat(data_fd, &sb) != 0 || sb.st_size < (off_t)PAGE_SIZE) {
        close(data_fd);
        disk_table_destroy(index);
        free(cs); free(idx_path); free(dat_path);
        return NULL;
    }
    size_t map_sz = (size_t)sb.st_size;
    if (map_sz < DAT_INITIAL_MAP_SIZE)
        map_sz = DAT_INITIAL_MAP_SIZE;
    /* Ensure file is large enough for the mapping */
    if ((size_t)sb.st_size < map_sz)
        ftruncate(data_fd, (off_t)map_sz);
    cs->data_base = mmap(NULL, map_sz, PROT_READ | PROT_WRITE,
                         MAP_SHARED, data_fd, 0);
    if (cs->data_base == MAP_FAILED) {
        close(data_fd);
        disk_table_destroy(index);
        free(cs); free(idx_path); free(dat_path);
        return NULL;
    }
    cs->data_mapped = map_sz;

    cs->index     = index;
    cs->data_fd   = data_fd;
    cs->data_size = hdr.data_size;
    cs->idx_path  = idx_path;
    cs->dat_path  = dat_path;
    pthread_mutex_init(&cs->write_lock, NULL);
    cs->cache = ccache_create(CCACHE_DEFAULT_CAP);

    return cs;
}

void code_store_destroy(code_store_t *cs) {
    if (!cs) return;
    ccache_destroy(cs->cache);
    pthread_mutex_destroy(&cs->write_lock);
    disk_table_destroy(cs->index);
    if (cs->data_base && cs->data_base != MAP_FAILED)
        munmap(cs->data_base, cs->data_mapped);
    close(cs->data_fd);
    free(cs->idx_path);
    free(cs->dat_path);
    free(cs);
}

/* =========================================================================
 * Operations
 * ========================================================================= */

bool code_store_put(code_store_t *cs, const uint8_t code_hash[32],
                    const uint8_t *code, uint32_t code_len) {
    if (!cs || !code_hash) return false;
    if (code_len > 0 && !code) return false;

    /* Fast path: already exists on disk (dedup) */
    if (disk_table_contains(cs->index, code_hash))
        return true;

    /* Acquire write lock, double-check, then write immediately */
    pthread_mutex_lock(&cs->write_lock);

    if (disk_table_contains(cs->index, code_hash)) {
        pthread_mutex_unlock(&cs->write_lock);
        return true;
    }

    /* Allocate offset in data region */
    uint64_t offset = cs->data_size;
    cs->data_size += code_len;

    /* Grow mmap if needed */
    size_t needed = PAGE_SIZE + offset + code_len;
    if (needed > cs->data_mapped) {
        if (!dat_remap(cs, needed)) {
            cs->data_size -= code_len;  /* rollback */
            pthread_mutex_unlock(&cs->write_lock);
            return false;
        }
    }

    /* Write code bytes directly to mmap'd .dat */
    if (code_len > 0)
        memcpy(cs->data_base + PAGE_SIZE + offset, code, code_len);

    /* Insert into disk_table index — makes it immediately findable */
    code_record_t rec = { .offset = offset, .length = code_len };
    if (!disk_table_put(cs->index, code_hash, &rec)) {
        fprintf(stderr, "FATAL: code_store disk_table_put failed\n");
        cs->data_size -= code_len;  /* rollback */
        pthread_mutex_unlock(&cs->write_lock);
        return false;
    }

    /* Update header so data_size survives crashes */
    write_data_header(cs);

    /* Pre-populate cache so first get() is a hit */
    if (cs->cache && code_len > 0)
        ccache_put(cs->cache, code_hash, code, code_len);

    pthread_mutex_unlock(&cs->write_lock);
    return true;
}

uint32_t code_store_get(const code_store_t *cs, const uint8_t code_hash[32],
                        uint8_t *buf, uint32_t buf_len) {
    if (!cs || !code_hash) return 0;

    /* Check LRU cache */
    if (cs->cache) {
        uint32_t cached = ccache_get(cs->cache, code_hash, buf, buf_len);
        if (cached > 0) return cached;
    }

    /* Look up in disk_table index */
    code_record_t rec;
    if (!disk_table_get(cs->index, code_hash, &rec))
        return 0;

    /* If buffer too small, return required size */
    if (buf_len < rec.length)
        return rec.length;

    /* Read code from mmap'd data file */
    if (rec.length > 0 && buf) {
        size_t dat_off = PAGE_SIZE + rec.offset;
        if (dat_off + rec.length > cs->data_mapped)
            return 0;  /* out of bounds — corrupt record */
        memcpy(buf, cs->data_base + dat_off, rec.length);

        /* Insert into cache for future lookups */
        if (cs->cache)
            ccache_put(cs->cache, code_hash, buf, rec.length);
    }

    return rec.length;
}

bool code_store_contains(const code_store_t *cs, const uint8_t code_hash[32]) {
    if (!cs || !code_hash) return false;
    return disk_table_contains(cs->index, code_hash);
}

uint32_t code_store_get_size(const code_store_t *cs, const uint8_t code_hash[32]) {
    if (!cs || !code_hash) return 0;

    /* Check LRU cache (avoids disk_table index lookup) */
    if (cs->cache) {
        uint32_t cached = ccache_get_size(cs->cache, code_hash);
        if (cached > 0) return cached;
    }

    code_record_t rec;
    if (!disk_table_get(cs->index, code_hash, &rec))
        return 0;
    return rec.length;
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void code_store_sync(code_store_t *cs) {
    if (!cs) return;
    write_data_header(cs);
    /* No msync — OS page cache handles writeback */
}

void code_store_flush(code_store_t *cs) {
    if (!cs) return;
    /* Write header to mmap. No msync — OS page cache handles writeback. */
    write_data_header(cs);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t code_store_count(const code_store_t *cs) {
    return cs ? disk_table_count(cs->index) : 0;
}

/* =========================================================================
 * Cache Configuration
 * ========================================================================= */

void code_store_set_cache(code_store_t *cs, uint32_t capacity) {
    if (!cs) return;
    ccache_destroy(cs->cache);
    cs->cache = NULL;
    if (capacity > 0)
        cs->cache = ccache_create(capacity);
}

code_store_cache_stats_t code_store_cache_stats(const code_store_t *cs) {
    code_store_cache_stats_t s = {0};
    if (!cs || !cs->cache) return s;
    code_cache_t *c = cs->cache;
    s.count    = c->count;
    s.capacity = c->capacity;
    s.hits     = c->hits;
    s.misses   = c->misses;
    return s;
}
