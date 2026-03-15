/*
 * Code Store — Content-Addressed, Append-Only, Deduplicated.
 *
 * Two-file design:
 *   <path>.idx — disk_hash index: code_hash (32B) → {offset, length} (12B)
 *   <path>.dat — append-only flat file of raw code bytes
 *
 * Deduplication is free: same code → same hash → disk_hash_contains = true → skip.
 * Thread-safe: reads via pread (lock-free), writes serialized by mutex.
 * Crash-safe: data written before index (orphaned bytes = harmless).
 */

#include "../include/code_store.h"
#include "../include/disk_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define CODE_STORE_MAGIC    0x54534443U   /* "CDST" little-endian */
#define CODE_STORE_VERSION  1
#define PAGE_SIZE           4096
#define CODE_HASH_SIZE      32

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
 * Index record (stored in disk_hash as the record)
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
 * Eliminates repeated pread() for frequently-called contracts.
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

/* =========================================================================
 * Deferred write buffer
 * ========================================================================= */

typedef struct {
    uint8_t   hash[32];
    uint8_t  *code;       /* malloc'd copy (NULL for zero-length code) */
    uint32_t  code_len;
    uint64_t  offset;     /* allocated position in data file */
} code_deferred_t;

#define CODE_DEF_INIT_CAP 32

struct code_store {
    disk_hash_t     *index;         /* code_hash → code_record_t */
    int              data_fd;       /* append-only flat file */
    uint64_t         data_size;     /* current write position (bytes after header) */
    pthread_mutex_t  write_lock;    /* serialize appends */
    char            *idx_path;      /* owned, for cleanup */
    char            *dat_path;      /* owned, for cleanup */

    /* Deferred write buffer — flushed at checkpoint */
    code_deferred_t *def_entries;
    size_t           def_count;
    size_t           def_cap;

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

static void write_data_header(int fd, uint64_t data_size) {
    code_store_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic     = CODE_STORE_MAGIC;
    hdr.version   = CODE_STORE_VERSION;
    hdr.data_size = data_size;
    (void)pwrite(fd, &hdr, PAGE_SIZE, 0);
}

static bool read_data_header(int fd, code_store_header_t *hdr) {
    ssize_t n = pread(fd, hdr, PAGE_SIZE, 0);
    if (n < (ssize_t)sizeof(*hdr)) return false;
    return true;
}

/* =========================================================================
 * Deferred buffer helpers
 * ========================================================================= */

static void def_init(code_store_t *cs) {
    cs->def_entries = NULL;
    cs->def_count   = 0;
    cs->def_cap     = 0;
}

static const code_deferred_t *def_find(const code_store_t *cs,
                                        const uint8_t hash[32]) {
    for (size_t i = 0; i < cs->def_count; i++) {
        if (memcmp(cs->def_entries[i].hash, hash, 32) == 0)
            return &cs->def_entries[i];
    }
    return NULL;
}

static bool def_append(code_store_t *cs, const uint8_t hash[32],
                        const uint8_t *code, uint32_t code_len,
                        uint64_t offset) {
    if (cs->def_count >= cs->def_cap) {
        size_t new_cap = cs->def_cap ? cs->def_cap * 2 : CODE_DEF_INIT_CAP;
        code_deferred_t *tmp = realloc(cs->def_entries,
                                        new_cap * sizeof(code_deferred_t));
        if (!tmp) return false;
        cs->def_entries = tmp;
        cs->def_cap     = new_cap;
    }
    code_deferred_t *e = &cs->def_entries[cs->def_count++];
    memcpy(e->hash, hash, 32);
    e->code_len = code_len;
    e->offset   = offset;
    if (code_len > 0 && code) {
        e->code = malloc(code_len);
        if (!e->code) { cs->def_count--; return false; }
        memcpy(e->code, code, code_len);
    } else {
        e->code = NULL;
    }
    return true;
}

static void def_free_all(code_store_t *cs) {
    for (size_t i = 0; i < cs->def_count; i++)
        free(cs->def_entries[i].code);
    free(cs->def_entries);
    def_init(cs);
}

/* Forward declaration */
void code_store_flush(code_store_t *cs);

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

code_store_t *code_store_create(const char *path, uint64_t capacity_hint) {
    if (!path) return NULL;

    char *idx_path = make_path(path, ".idx");
    char *dat_path = make_path(path, ".dat");
    if (!idx_path || !dat_path) { free(idx_path); free(dat_path); return NULL; }

    /* Create disk_hash index: 32B key → 12B record */
    disk_hash_t *index = disk_hash_create(idx_path, CODE_HASH_SIZE,
                                           sizeof(code_record_t), capacity_hint);
    if (!index) { free(idx_path); free(dat_path); return NULL; }

    /* Create data file */
    int data_fd = open(dat_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (data_fd < 0) {
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    /* Write header */
    write_data_header(data_fd, 0);

    code_store_t *cs = calloc(1, sizeof(*cs));
    if (!cs) {
        close(data_fd);
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    cs->index     = index;
    cs->data_fd   = data_fd;
    cs->data_size = 0;
    cs->idx_path  = idx_path;
    cs->dat_path  = dat_path;
    pthread_mutex_init(&cs->write_lock, NULL);
    def_init(cs);
    cs->cache = ccache_create(CCACHE_DEFAULT_CAP);

    return cs;
}

code_store_t *code_store_open(const char *path) {
    if (!path) return NULL;

    char *idx_path = make_path(path, ".idx");
    char *dat_path = make_path(path, ".dat");
    if (!idx_path || !dat_path) { free(idx_path); free(dat_path); return NULL; }

    /* Open disk_hash index */
    disk_hash_t *index = disk_hash_open(idx_path);
    if (!index) { free(idx_path); free(dat_path); return NULL; }

    /* Open data file */
    int data_fd = open(dat_path, O_RDWR);
    if (data_fd < 0) {
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    /* Read and validate header */
    code_store_header_t hdr;
    if (!read_data_header(data_fd, &hdr) ||
        hdr.magic != CODE_STORE_MAGIC ||
        hdr.version != CODE_STORE_VERSION) {
        close(data_fd);
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    code_store_t *cs = calloc(1, sizeof(*cs));
    if (!cs) {
        close(data_fd);
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    cs->index     = index;
    cs->data_fd   = data_fd;
    cs->data_size = hdr.data_size;
    cs->idx_path  = idx_path;
    cs->dat_path  = dat_path;
    pthread_mutex_init(&cs->write_lock, NULL);
    def_init(cs);
    cs->cache = ccache_create(CCACHE_DEFAULT_CAP);

    return cs;
}

void code_store_destroy(code_store_t *cs) {
    if (!cs) return;
    code_store_flush(cs);
    ccache_destroy(cs->cache);
    pthread_mutex_destroy(&cs->write_lock);
    disk_hash_destroy(cs->index);
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

    /* Fast path: already exists in deferred buffer or on disk (dedup) */
    if (def_find(cs, code_hash))
        return true;
    if (disk_hash_contains(cs->index, code_hash))
        return true;

    /* Slow path: acquire write lock, double-check, defer */
    pthread_mutex_lock(&cs->write_lock);

    if (def_find(cs, code_hash) ||
        disk_hash_contains(cs->index, code_hash)) {
        pthread_mutex_unlock(&cs->write_lock);
        return true;
    }

    /* Allocate offset and buffer in deferred array (no disk I/O) */
    uint64_t offset = cs->data_size;
    if (!def_append(cs, code_hash, code, code_len, offset)) {
        pthread_mutex_unlock(&cs->write_lock);
        return false;
    }
    cs->data_size += code_len;

    /* Pre-populate cache so first get() is a hit */
    if (cs->cache && code_len > 0)
        ccache_put(cs->cache, code_hash, code, code_len);

    pthread_mutex_unlock(&cs->write_lock);
    return true;
}

uint32_t code_store_get(const code_store_t *cs, const uint8_t code_hash[32],
                        uint8_t *buf, uint32_t buf_len) {
    if (!cs || !code_hash) return 0;

    /* Check deferred buffer first */
    const code_deferred_t *def = def_find(cs, code_hash);
    if (def) {
        if (buf_len < def->code_len)
            return def->code_len;
        if (def->code_len > 0 && buf && def->code)
            memcpy(buf, def->code, def->code_len);
        return def->code_len;
    }

    /* Check LRU cache */
    if (cs->cache) {
        code_cache_t *cc = cs->cache;
        uint32_t cached = ccache_get(cc, code_hash, buf, buf_len);
        if (cached > 0) return cached;
    }

    /* Fall back to disk */
    code_record_t rec;
    if (!disk_hash_get(cs->index, code_hash, &rec))
        return 0;

    /* If buffer too small, return required size */
    if (buf_len < rec.length)
        return rec.length;

    /* Read code from data file */
    if (rec.length > 0 && buf) {
        ssize_t n = pread(cs->data_fd, buf, rec.length,
                          (off_t)(PAGE_SIZE + rec.offset));
        if (n != (ssize_t)rec.length)
            return 0;  /* I/O error */

        /* Insert into cache for future lookups */
        if (cs->cache)
            ccache_put(cs->cache, code_hash, buf, rec.length);
    }

    return rec.length;
}

bool code_store_contains(const code_store_t *cs, const uint8_t code_hash[32]) {
    if (!cs || !code_hash) return false;
    if (def_find(cs, code_hash)) return true;
    return disk_hash_contains(cs->index, code_hash);
}

uint32_t code_store_get_size(const code_store_t *cs, const uint8_t code_hash[32]) {
    if (!cs || !code_hash) return 0;

    const code_deferred_t *def = def_find(cs, code_hash);
    if (def) return def->code_len;

    /* Check LRU cache (avoids disk_hash index lookup) */
    if (cs->cache) {
        uint32_t cached = ccache_get_size(cs->cache, code_hash);
        if (cached > 0) return cached;
    }

    code_record_t rec;
    if (!disk_hash_get(cs->index, code_hash, &rec))
        return 0;
    return rec.length;
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void code_store_sync(code_store_t *cs) {
    if (!cs) return;
    /* Write data header with current data_size */
    write_data_header(cs->data_fd, cs->data_size);
    fsync(cs->data_fd);
    /* Sync disk_hash index */
    disk_hash_sync(cs->index);
}

void code_store_flush(code_store_t *cs) {
    if (!cs) return;

    /* Write all deferred entries to disk */
    for (size_t i = 0; i < cs->def_count; i++) {
        code_deferred_t *e = &cs->def_entries[i];

        /* Write code bytes to .dat */
        if (e->code_len > 0 && e->code) {
            ssize_t written = pwrite(cs->data_fd, e->code, e->code_len,
                                     (off_t)(PAGE_SIZE + e->offset));
            if (written < 0 || (size_t)written != e->code_len) {
                fprintf(stderr, "FATAL: code_store pwrite failed: %zd/%zu bytes\n"
                        "  hint: disk full or I/O error\n",
                        written, e->code_len);
            }
        }

        /* Insert index entry */
        code_record_t rec = { .offset = e->offset, .length = e->code_len };
        if (!disk_hash_put(cs->index, e->hash, &rec)) {
            fprintf(stderr, "FATAL: code_store disk_hash_put failed\n"
                    "  hint: index may be corrupt or disk full\n");
        }
    }

    /* Free deferred buffer */
    def_free_all(cs);

    /* Sync to disk */
    code_store_sync(cs);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t code_store_count(const code_store_t *cs) {
    return cs ? disk_hash_count(cs->index) + cs->def_count : 0;
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
