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

    return cs;
}

void code_store_destroy(code_store_t *cs) {
    if (!cs) return;
    code_store_flush(cs);
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
            pwrite(cs->data_fd, e->code, e->code_len,
                   (off_t)(PAGE_SIZE + e->offset));
        }

        /* Insert index entry */
        code_record_t rec = { .offset = e->offset, .length = e->code_len };
        disk_hash_put(cs->index, e->hash, &rec);
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
    return cs ? disk_hash_count(cs->index) : 0;
}
