/*
 * MPT Store — Persistent Merkle Patricia Trie with Incremental Updates.
 *
 * Two-file design:
 *   <path>.idx — disk_hash index: node_hash (32B) → {offset, length} (12B)
 *   <path>.dat — slot-allocated flat file of RLP-encoded trie node data
 *
 * Slot allocation: nodes are stored in size-class slots (64, 128, 256, 512,
 * 1024 bytes). Deleted slots go onto per-class free lists (intrusive linked
 * lists on disk). New writes reuse free slots before appending. This
 * eliminates garbage accumulation — no compaction needed in steady state.
 *
 * Crash-safe: data written before index (orphaned bytes = harmless).
 * Root hash and free list heads written to .dat header only on sync().
 */

#include "mpt_store.h"
#include "disk_hash.h"
#include "keccak256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define MPT_STORE_MAGIC    0x5453504DU   /* "MPST" little-endian */
#define MPT_STORE_VERSION  1
#define PAGE_SIZE          4096
#define NODE_HASH_SIZE     32
#define MAX_NIBBLES        64           /* 32-byte key = 64 nibbles */
#define MAX_NODE_RLP       1024         /* generous upper bound for node RLP */
#define DIRTY_INIT_CAP     256
#define DEFERRED_INIT_CAP  256
#define DEFERRED_BUCKETS   4096   /* hash table buckets for deferred node index */

/* Size-class slot allocator: nodes are padded to the smallest class that fits.
 * Freed slots form intrusive linked lists (next-pointer stored in first 8B). */
#define NUM_SIZE_CLASSES    5
static const uint16_t SIZE_CLASSES[NUM_SIZE_CLASSES] = {64, 128, 256, 512, 1024};

/* Default LRU cache: 2 GB */
#define MPT_DEFAULT_CACHE_MB 2048

/* Depth pinning: nodes at trie depth <= this threshold are never evicted.
 * depth 0 = root (1 node), depth 1 = 16, depth 2 = 256, depth 3 = 4096,
 * depth 4 = 65536. Total pinned ~70K nodes × ~1KB = ~70MB — negligible. */
#define NCACHE_PIN_DEPTH 4
#define NCACHE_DEPTH_UNKNOWN 0xFF  /* for nodes inserted without depth info */

/* Empty trie root = keccak256(RLP("")) = keccak256(0x80) */
static const uint8_t EMPTY_ROOT[32] = {
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
};

/* =========================================================================
 * On-disk types
 * ========================================================================= */

/* Max free offsets that fit in the header (4096 - 76 = 4020 bytes = 502 offsets).
 * Overflow is silently dropped on sync — lost slots become padding waste. */
#define MAX_HDR_FREE_OFFSETS  502

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint8_t  root_hash[32];
    uint64_t data_size;                        /* total bytes allocated after header */
    uint64_t free_slot_bytes;                  /* total bytes on free lists */
    uint32_t free_counts[NUM_SIZE_CLASSES];    /* entries per size class (20 bytes) */
    uint8_t  free_data[4020];                  /* packed uint64_t offsets per class */
} mpt_store_header_t;

_Static_assert(sizeof(mpt_store_header_t) == PAGE_SIZE,
               "mpt_store_header_t must be 4096 bytes");

typedef struct __attribute__((packed)) {
    uint64_t offset;
    uint32_t length;
    uint32_t refcount;   /* shared-mode reference count (0 = free) */
} node_record_t;

_Static_assert(sizeof(node_record_t) == 16, "node_record_t must be 16 bytes");

/* =========================================================================
 * Dirty entry (staged update/delete)
 * ========================================================================= */

typedef struct {
    uint8_t  nibbles[MAX_NIBBLES];
    uint8_t *value;         /* RLP value (heap, owned). NULL = delete */
    size_t   value_len;
} dirty_entry_t;

/* =========================================================================
 * Node reference: stored node hash OR inline RLP (<32 bytes)
 * ========================================================================= */

typedef enum { REF_EMPTY, REF_HASH, REF_INLINE } ref_type_t;

typedef struct {
    ref_type_t type;
    union {
        uint8_t hash[32];
        struct { uint8_t data[31]; uint8_t len; } raw;
    };
} node_ref_t;

/* =========================================================================
 * Decoded node (transient, used during trie walks)
 * ========================================================================= */

typedef enum {
    MPT_NODE_BRANCH,
    MPT_NODE_EXTENSION,
    MPT_NODE_LEAF,
} mpt_node_type_t;

typedef struct {
    mpt_node_type_t type;
    union {
        struct {                        /* BRANCH */
            node_ref_t children[16];
            /* value slot always empty for state/storage tries */
        } branch;
        struct {                        /* EXTENSION */
            uint8_t   path[MAX_NIBBLES];
            uint8_t   path_len;
            node_ref_t child;
        } extension;
        struct {                        /* LEAF */
            uint8_t   path[MAX_NIBBLES];
            uint8_t   path_len;
            uint8_t  *value;            /* points into decode buffer */
            size_t    value_len;
        } leaf;
    };
} mpt_node_t;

/* =========================================================================
 * In-memory LRU node cache
 *
 * Caches raw RLP bytes keyed by node hash. Eliminates disk I/O for hot
 * trie nodes (top levels, recently written nodes). All operations O(1).
 *
 * Layout: flat pre-allocated entry array + index-based doubly-linked LRU
 * list + separate hash table (open chaining with index links).
 * ========================================================================= */

#define NCACHE_SENTINEL 0xFFFFFFFFU

typedef struct {
    uint8_t  hash[32];
    uint8_t  rlp[MAX_NODE_RLP];
    uint16_t rlp_len;
    uint8_t  depth;         /* trie depth (for pin eviction policy) */
    uint32_t ht_next;       /* next in hash bucket chain */
    uint32_t lru_prev;      /* doubly-linked LRU list */
    uint32_t lru_next;
} ncache_entry_t;

typedef struct {
    ncache_entry_t *entries;
    uint32_t       *buckets;        /* hash table → entry index */
    uint32_t        capacity;
    uint32_t        bucket_mask;    /* bucket_count - 1 (power of 2) */
    uint32_t        count;
    uint32_t        lru_head;       /* most recently used */
    uint32_t        lru_tail;       /* least recently used */
    uint32_t        free_head;      /* free list (uses lru_next for chain) */
    uint64_t        hits;
    uint64_t        misses;
    uint64_t        evict_skipped;  /* times eviction skipped a pinned node */
    uint32_t        pinned_count;   /* entries with depth <= PIN_DEPTH */
} node_cache_t;

static inline uint32_t ncache_bucket(const node_cache_t *c,
                                      const uint8_t hash[32]) {
    uint64_t h;
    memcpy(&h, hash, 8);
    return (uint32_t)(h & c->bucket_mask);
}

static node_cache_t *ncache_create(uint32_t capacity) {
    if (capacity == 0) return NULL;

    node_cache_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    /* Round bucket count up to next power of 2 (at least 2× capacity) */
    uint32_t bc = 1;
    while (bc < capacity * 2) bc <<= 1;

    c->entries = calloc(capacity, sizeof(ncache_entry_t));
    c->buckets = malloc(bc * sizeof(uint32_t));
    if (!c->entries || !c->buckets) {
        free(c->entries); free(c->buckets); free(c);
        return NULL;
    }

    c->capacity    = capacity;
    c->bucket_mask = bc - 1;
    c->count       = 0;
    c->lru_head    = NCACHE_SENTINEL;
    c->lru_tail    = NCACHE_SENTINEL;
    c->hits        = 0;
    c->misses      = 0;

    /* Initialize buckets to empty */
    for (uint32_t i = 0; i < bc; i++)
        c->buckets[i] = NCACHE_SENTINEL;

    /* Build free list (chain through lru_next) */
    for (uint32_t i = 0; i < capacity; i++)
        c->entries[i].lru_next = (i + 1 < capacity) ? i + 1 : NCACHE_SENTINEL;
    c->free_head = 0;

    return c;
}

static void ncache_destroy(node_cache_t *c) {
    if (!c) return;
    free(c->entries);
    free(c->buckets);
    free(c);
}

/* Remove entry from LRU list (does NOT touch hash table or free list) */
static void ncache_lru_remove(node_cache_t *c, uint32_t idx) {
    ncache_entry_t *e = &c->entries[idx];
    if (e->lru_prev != NCACHE_SENTINEL)
        c->entries[e->lru_prev].lru_next = e->lru_next;
    else
        c->lru_head = e->lru_next;
    if (e->lru_next != NCACHE_SENTINEL)
        c->entries[e->lru_next].lru_prev = e->lru_prev;
    else
        c->lru_tail = e->lru_prev;
}

/* Push entry to front of LRU list (most recently used) */
static void ncache_lru_push_front(node_cache_t *c, uint32_t idx) {
    ncache_entry_t *e = &c->entries[idx];
    e->lru_prev = NCACHE_SENTINEL;
    e->lru_next = c->lru_head;
    if (c->lru_head != NCACHE_SENTINEL)
        c->entries[c->lru_head].lru_prev = idx;
    c->lru_head = idx;
    if (c->lru_tail == NCACHE_SENTINEL)
        c->lru_tail = idx;
}

/* Remove entry from its hash bucket chain */
static void ncache_ht_remove(node_cache_t *c, uint32_t idx) {
    uint32_t b = ncache_bucket(c, c->entries[idx].hash);
    uint32_t *prev = &c->buckets[b];
    while (*prev != NCACHE_SENTINEL) {
        if (*prev == idx) {
            *prev = c->entries[idx].ht_next;
            return;
        }
        prev = &c->entries[*prev].ht_next;
    }
}

/* Look up a node by hash. On hit, copies RLP to buf and moves to LRU front. */
/* Lightweight existence check — no data copy, no LRU promotion. */
static bool ncache_contains(const node_cache_t *c, const uint8_t hash[32]) {
    uint32_t b = ncache_bucket(c, hash);
    uint32_t idx = c->buckets[b];
    while (idx != NCACHE_SENTINEL) {
        if (memcmp(c->entries[idx].hash, hash, 32) == 0)
            return true;
        idx = c->entries[idx].ht_next;
    }
    return false;
}

static bool ncache_get(node_cache_t *c, const uint8_t hash[32],
                        uint8_t *buf, uint16_t *out_len) {
    uint32_t b = ncache_bucket(c, hash);
    uint32_t idx = c->buckets[b];
    while (idx != NCACHE_SENTINEL) {
        ncache_entry_t *e = &c->entries[idx];
        if (memcmp(e->hash, hash, 32) == 0) {
            /* Hit — copy data and move to LRU front */
            memcpy(buf, e->rlp, e->rlp_len);
            *out_len = e->rlp_len;
            ncache_lru_remove(c, idx);
            ncache_lru_push_front(c, idx);
            c->hits++;
            return true;
        }
        idx = e->ht_next;
    }
    c->misses++;
    return false;
}

/* Insert or update a node in the cache. Evicts LRU tail if full.
 * depth = trie depth of this node (used for pin policy). */
static void ncache_put(node_cache_t *c, const uint8_t hash[32],
                        const uint8_t *rlp, uint16_t rlp_len, uint8_t depth) {
    if (rlp_len > MAX_NODE_RLP) return;

    /* Check if already present — update in place */
    uint32_t b = ncache_bucket(c, hash);
    uint32_t idx = c->buckets[b];
    while (idx != NCACHE_SENTINEL) {
        ncache_entry_t *e = &c->entries[idx];
        if (memcmp(e->hash, hash, 32) == 0) {
            memcpy(e->rlp, rlp, rlp_len);
            e->rlp_len = rlp_len;
            /* Update depth if we now have better info */
            if (depth < e->depth) e->depth = depth;
            ncache_lru_remove(c, idx);
            ncache_lru_push_front(c, idx);
            return;
        }
        idx = e->ht_next;
    }

    /* Allocate a slot */
    uint32_t slot;
    if (c->free_head != NCACHE_SENTINEL) {
        slot = c->free_head;
        c->free_head = c->entries[slot].lru_next;
    } else {
        /* Evict LRU tail — skip pinned entries (depth <= PIN_DEPTH) */
        slot = c->lru_tail;
        while (slot != NCACHE_SENTINEL &&
               c->entries[slot].depth <= NCACHE_PIN_DEPTH) {
            c->evict_skipped++;
            slot = c->entries[slot].lru_prev;
        }
        if (slot == NCACHE_SENTINEL) {
            /* All entries are pinned — fall back to true LRU tail.
             * This can only happen if cache is undersized for the trie. */
            slot = c->lru_tail;
        }
        /* Track pinned count change for evicted entry */
        if (c->entries[slot].depth <= NCACHE_PIN_DEPTH && c->pinned_count > 0)
            c->pinned_count--;
        ncache_lru_remove(c, slot);
        ncache_ht_remove(c, slot);
        c->count--;
    }

    /* Fill entry */
    ncache_entry_t *e = &c->entries[slot];
    memcpy(e->hash, hash, 32);
    memcpy(e->rlp, rlp, rlp_len);
    e->rlp_len = rlp_len;
    e->depth = depth;
    if (depth <= NCACHE_PIN_DEPTH) c->pinned_count++;

    /* Insert into hash bucket */
    b = ncache_bucket(c, hash);
    e->ht_next = c->buckets[b];
    c->buckets[b] = slot;

    /* Push to LRU front */
    ncache_lru_push_front(c, slot);
    c->count++;
}

/* Remove a node from the cache (if present). */
static void ncache_delete(node_cache_t *c, const uint8_t hash[32]) {
    uint32_t b = ncache_bucket(c, hash);
    uint32_t idx = c->buckets[b];
    uint32_t *prev = &c->buckets[b];
    while (idx != NCACHE_SENTINEL) {
        ncache_entry_t *e = &c->entries[idx];
        if (memcmp(e->hash, hash, 32) == 0) {
            /* Remove from hash chain */
            *prev = e->ht_next;
            /* Remove from LRU */
            ncache_lru_remove(c, idx);
            if (e->depth <= NCACHE_PIN_DEPTH && c->pinned_count > 0)
                c->pinned_count--;
            /* Return to free list */
            e->lru_next = c->free_head;
            c->free_head = idx;
            c->count--;
            return;
        }
        prev = &e->ht_next;
        idx = e->ht_next;
    }
}

/* =========================================================================
 * In-memory free list (one per size class)
 *
 * Simple dynamic array of free slot offsets. Zero disk I/O during normal
 * operation. Serialized into the .dat header on sync().
 * ========================================================================= */

typedef struct {
    uint64_t *offsets;
    uint32_t  count;
    uint32_t  capacity;
} free_list_t;

/* =========================================================================
 * Main struct
 * ========================================================================= */

/* Deferred write buffer entry — node not yet written to disk */
typedef struct {
    uint8_t  hash[32];
    uint8_t *rlp;        /* malloc'd copy of node RLP */
    uint32_t rlp_len;
    uint64_t offset;     /* allocated slot (in-memory alloc_slot result) */
    uint32_t refcount;   /* shared-mode reference count */
    int      ht_next;    /* next index in hash chain, -1 = end */
} deferred_entry_t;

typedef struct {
    uint8_t hash[32];
    int     ht_next;    /* next index in hash chain, -1 = end */
} del_entry_t;

struct mpt_store {
    disk_hash_t     *index;
    int              data_fd;
    uint64_t         data_size;
    uint64_t         live_bytes;     /* sum of live node RLP sizes */
    uint8_t          root_hash[32];

    /* Size-class free lists (in-memory, serialized to header on sync) */
    free_list_t      free_lists[NUM_SIZE_CLASSES];
    uint64_t         free_slot_bytes; /* total bytes on free lists */

    dirty_entry_t   *dirty;
    size_t           dirty_count;
    size_t           dirty_cap;
    bool             batch_active;

    /* Page-based arena for dirty entry values — bulk alloc/free.
     * Pages are never realloc'd, so value pointers remain stable. */
    uint8_t        **val_pages;       /* array of page pointers */
    size_t           val_page_count;
    size_t           val_page_cap;
    size_t           val_page_used;   /* bytes used in current page */
#define VAL_PAGE_SIZE 65536

    node_cache_t    *cache;          /* NULL = no caching */

    bool             shared;         /* multi-trie mode: skip node deletion */

    /* Deferred write buffer: nodes allocated but not yet pwrite'd to disk.
     * Flushed to disk on mpt_store_flush() at checkpoint time. */
    deferred_entry_t *def_entries;
    size_t            def_count;
    size_t            def_cap;
    int               def_ht[DEFERRED_BUCKETS]; /* hash table heads, -1 = empty */

    /* Page-based arena for deferred RLP data — eliminates per-node malloc */
    uint8_t        **def_rlp_pages;
    size_t           def_rlp_page_count;
    size_t           def_rlp_page_cap;
    size_t           def_rlp_page_used;
#define DEF_RLP_PAGE_SIZE 65536

    /* Pending deletes: on-disk node hashes to delete at flush time */
    del_entry_t      *def_deletes;
    size_t            def_del_count;
    size_t            def_del_cap;
    int               def_del_ht[DEFERRED_BUCKETS]; /* hash table for O(1) cancel */

    char            *idx_path;
    char            *dat_path;
    char            *free_path;   /* overflow free list file (.free) */
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

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

static bool free_list_push(free_list_t *fl, uint64_t offset) {
    if (fl->count >= fl->capacity) {
        uint32_t new_cap = fl->capacity ? fl->capacity * 2 : 64;
        uint64_t *new_buf = realloc(fl->offsets, new_cap * sizeof(uint64_t));
        if (!new_buf) return false;
        fl->offsets = new_buf;
        fl->capacity = new_cap;
    }
    fl->offsets[fl->count++] = offset;
    return true;
}

static bool free_list_pop(free_list_t *fl, uint64_t *out) {
    if (fl->count == 0) return false;
    *out = fl->offsets[--fl->count];
    return true;
}

static void free_list_clear(free_list_t *fl) {
    fl->count = 0;
}

static void free_list_destroy(free_list_t *fl) {
    free(fl->offsets);
    fl->offsets = NULL;
    fl->count = fl->capacity = 0;
}

/* Forward declaration (used by mpt_store_create/open) */
void mpt_store_set_cache_mb(mpt_store_t *ms, uint32_t megabytes);

/* Return index of smallest size class that fits `len` bytes */
static int size_class_for(uint32_t len) {
    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        if (len <= SIZE_CLASSES[i]) return i;
    return NUM_SIZE_CLASSES - 1; /* shouldn't happen: MAX_NODE_RLP == 1024 */
}

/* Write overflow free list offsets to .free file.
 * Format: [uint32_t count_per_class × 5] [uint64_t offsets...] */
static void write_free_overflow(const char *path, const mpt_store_t *ms,
                                const uint32_t hdr_counts[NUM_SIZE_CLASSES]) {
    uint32_t overflow_total = 0;
    uint32_t overflow_counts[NUM_SIZE_CLASSES];
    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        overflow_counts[i] = ms->free_lists[i].count > hdr_counts[i]
                           ? ms->free_lists[i].count - hdr_counts[i] : 0;
        overflow_total += overflow_counts[i];
    }

    if (overflow_total == 0) {
        /* No overflow — remove stale file if present */
        unlink(path);
        return;
    }

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;

    /* Write counts header */
    (void)write(fd, overflow_counts, sizeof(overflow_counts));

    /* Write overflow offsets per class (skip the first hdr_counts[i] already in header) */
    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        if (overflow_counts[i] > 0) {
            (void)write(fd, ms->free_lists[i].offsets + hdr_counts[i],
                        overflow_counts[i] * sizeof(uint64_t));
        }
    }
    fsync(fd);
    close(fd);
}

/* Read overflow free list offsets from .free file */
static void read_free_overflow(const char *path, mpt_store_t *ms) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return;

    uint32_t counts[NUM_SIZE_CLASSES];
    if (read(fd, counts, sizeof(counts)) != sizeof(counts)) {
        close(fd);
        return;
    }

    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        if (counts[i] == 0) continue;
        uint64_t *buf = malloc(counts[i] * sizeof(uint64_t));
        if (!buf) break;
        ssize_t got = read(fd, buf, counts[i] * sizeof(uint64_t));
        if (got == (ssize_t)(counts[i] * sizeof(uint64_t))) {
            for (uint32_t j = 0; j < counts[i]; j++)
                free_list_push(&ms->free_lists[i], buf[j]);
        }
        free(buf);
    }
    close(fd);
}

static void write_header(int fd, const mpt_store_t *ms) {
    mpt_store_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic     = MPT_STORE_MAGIC;
    hdr.version   = MPT_STORE_VERSION;
    hdr.data_size = ms->data_size;
    memcpy(hdr.root_hash, ms->root_hash, 32);
    hdr.free_slot_bytes = ms->free_slot_bytes;

    /* Pack offsets sequentially: class 0, class 1, ... Truncate if overflow */
    size_t pos = 0;
    uint64_t *dst = (uint64_t *)hdr.free_data;
    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        uint32_t avail = (MAX_HDR_FREE_OFFSETS > pos)
                       ? (uint32_t)(MAX_HDR_FREE_OFFSETS - pos) : 0;
        uint32_t n = ms->free_lists[i].count < avail
                   ? ms->free_lists[i].count : avail;
        hdr.free_counts[i] = n;
        for (uint32_t j = 0; j < n; j++)
            dst[pos++] = ms->free_lists[i].offsets[j];
    }

    (void)pwrite(fd, &hdr, PAGE_SIZE, 0);

    /* Write overflow to .free file */
    if (ms->free_path)
        write_free_overflow(ms->free_path, ms, hdr.free_counts);
}

static bool read_header(int fd, mpt_store_header_t *hdr) {
    ssize_t n = pread(fd, hdr, PAGE_SIZE, 0);
    return n == PAGE_SIZE;
}

static void bytes_to_nibbles(const uint8_t *bytes, size_t byte_len,
                             uint8_t *nibbles) {
    for (size_t i = 0; i < byte_len; i++) {
        nibbles[i * 2]     = (bytes[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] =  bytes[i]       & 0x0F;
    }
}

/* =========================================================================
 * RLP encoding helpers (stack-allocated, same approach as verkle/src/mpt.c)
 * ========================================================================= */

typedef struct {
    uint8_t data[MAX_NODE_RLP];
    size_t  len;
} rlp_buf_t;

static inline void rbuf_reset(rlp_buf_t *b) { b->len = 0; }

static inline bool rbuf_append(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (b->len + n > sizeof(b->data)) return false;
    if (n > 0) memcpy(b->data + b->len, d, n);
    b->len += n;
    return true;
}

/* RLP-encode a byte string */
static bool rbuf_encode_bytes(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (n == 1 && d[0] < 0x80)
        return rbuf_append(b, d, 1);
    if (n <= 55) {
        uint8_t pfx = 0x80 + (uint8_t)n;
        return rbuf_append(b, &pfx, 1) && (n > 0 ? rbuf_append(b, d, n) : true);
    }
    uint8_t hdr[3];
    size_t hlen;
    if (n <= 0xFF) {
        hdr[0] = 0xb8; hdr[1] = (uint8_t)n; hlen = 2;
    } else {
        hdr[0] = 0xb9; hdr[1] = (uint8_t)(n >> 8); hdr[2] = (uint8_t)(n & 0xFF); hlen = 3;
    }
    return rbuf_append(b, hdr, hlen) && rbuf_append(b, d, n);
}

/* RLP-encode empty string (0x80) */
static inline bool rbuf_encode_empty(rlp_buf_t *b) {
    uint8_t e = 0x80;
    return rbuf_append(b, &e, 1);
}

/* Wrap payload as RLP list */
static bool rbuf_list_wrap(rlp_buf_t *out, const rlp_buf_t *payload) {
    if (payload->len <= 55) {
        uint8_t pfx = 0xc0 + (uint8_t)payload->len;
        return rbuf_append(out, &pfx, 1) &&
               rbuf_append(out, payload->data, payload->len);
    }
    uint8_t lb[4];
    size_t ll = 0;
    size_t tmp = payload->len;
    while (tmp > 0) { lb[ll++] = tmp & 0xFF; tmp >>= 8; }
    uint8_t pfx = 0xf7 + (uint8_t)ll;
    if (!rbuf_append(out, &pfx, 1)) return false;
    for (int i = (int)ll - 1; i >= 0; i--)
        if (!rbuf_append(out, &lb[i], 1)) return false;
    return rbuf_append(out, payload->data, payload->len);
}

/* =========================================================================
 * Hex-prefix encoding (Ethereum Yellow Paper, Appendix C)
 * ========================================================================= */

static size_t hex_prefix_encode(const uint8_t *nibbles, size_t nibble_len,
                                bool is_leaf, uint8_t *out) {
    bool odd = (nibble_len % 2) == 1;
    uint8_t prefix = (is_leaf ? 2 : 0) + (odd ? 1 : 0);

    if (odd) {
        out[0] = (prefix << 4) | nibbles[0];
        for (size_t i = 1; i < nibble_len; i += 2)
            out[(i + 1) / 2] = (nibbles[i] << 4) | nibbles[i + 1];
        return (nibble_len + 1) / 2;
    } else {
        out[0] = prefix << 4;
        for (size_t i = 0; i < nibble_len; i += 2)
            out[(i + 2) / 2] = (nibbles[i] << 4) | nibbles[i + 1];
        return (nibble_len + 2) / 2;
    }
}

/* =========================================================================
 * Hex-prefix decoding (reverse of encoding)
 * ========================================================================= */

static size_t hex_prefix_decode(const uint8_t *encoded, size_t enc_len,
                                uint8_t *nibbles, bool *is_leaf) {
    if (enc_len == 0) { *is_leaf = false; return 0; }
    uint8_t first = encoded[0] >> 4;
    *is_leaf = (first >= 2);
    bool odd = (first & 1) != 0;

    size_t out = 0;
    if (odd) {
        nibbles[out++] = encoded[0] & 0x0F;
    }
    for (size_t i = 1; i < enc_len; i++) {
        nibbles[out++] = (encoded[i] >> 4) & 0x0F;
        nibbles[out++] =  encoded[i]       & 0x0F;
    }
    return out;
}

/* =========================================================================
 * RLP decoding helpers
 * ========================================================================= */

typedef struct {
    const uint8_t *data;
    size_t len;
} rlp_slice_t;

/* Decode RLP item header. Returns data start offset and sets *data_len.
 * Returns 0 on error (not a valid offset since header is at least 1 byte). */
static size_t rlp_decode_header(const uint8_t *buf, size_t buf_len,
                                size_t *data_len, bool *is_list) {
    if (buf_len == 0) return 0;
    uint8_t b = buf[0];

    if (b < 0x80) {
        *data_len = 1; *is_list = false; return 0; /* single byte IS the data */
    }
    if (b <= 0xb7) {
        *data_len = b - 0x80; *is_list = false; return 1;
    }
    if (b <= 0xbf) {
        size_t ll = b - 0xb7;
        if (1 + ll > buf_len) return 0;
        size_t len = 0;
        for (size_t i = 0; i < ll; i++)
            len = (len << 8) | buf[1 + i];
        *data_len = len; *is_list = false; return 1 + ll;
    }
    if (b <= 0xf7) {
        *data_len = b - 0xc0; *is_list = true; return 1;
    }
    /* b >= 0xf8 */
    size_t ll = b - 0xf7;
    if (1 + ll > buf_len) return 0;
    size_t len = 0;
    for (size_t i = 0; i < ll; i++)
        len = (len << 8) | buf[1 + i];
    *data_len = len; *is_list = true; return 1 + ll;
}

/* Iterate over items in an RLP list. Returns slices for each element. */
static size_t rlp_list_items(const uint8_t *list_data, size_t list_len,
                             rlp_slice_t *items, size_t max_items) {
    size_t pos = 0, count = 0;
    while (pos < list_len && count < max_items) {
        size_t data_len;
        bool is_list;
        uint8_t b = list_data[pos];

        if (b < 0x80) {
            /* Single byte */
            items[count].data = &list_data[pos];
            items[count].len = 1;
            pos += 1;
        } else {
            size_t hdr_off = rlp_decode_header(&list_data[pos], list_len - pos,
                                               &data_len, &is_list);
            /* Store the full encoded item (header + data) */
            size_t total;
            if (b < 0x80) {
                total = 1;
            } else {
                total = hdr_off + data_len;
            }
            items[count].data = &list_data[pos];
            items[count].len  = total;
            pos += total;
        }
        count++;
    }
    return count;
}

/* =========================================================================
 * Node RLP decoding
 * ========================================================================= */

static bool decode_node(const uint8_t *rlp, size_t rlp_len, mpt_node_t *node) {
    size_t data_len;
    bool is_list;
    size_t hdr_off = rlp_decode_header(rlp, rlp_len, &data_len, &is_list);
    if (!is_list) return false;

    const uint8_t *list_data = rlp + hdr_off;
    rlp_slice_t items[17];
    size_t item_count = rlp_list_items(list_data, data_len, items, 17);

    if (item_count == 17) {
        /* Branch node */
        node->type = MPT_NODE_BRANCH;
        for (int i = 0; i < 16; i++) {
            if (items[i].len == 1 && items[i].data[0] == 0x80) {
                node->branch.children[i].type = REF_EMPTY;
            } else if (items[i].len == 33 && items[i].data[0] == 0xa0) {
                /* 32-byte hash (RLP: 0xa0 + 32 bytes) */
                node->branch.children[i].type = REF_HASH;
                memcpy(node->branch.children[i].hash, items[i].data + 1, 32);
            } else {
                /* Inline node (raw RLP < 32 bytes) */
                node->branch.children[i].type = REF_INLINE;
                if (items[i].len > 31) return false;
                memcpy(node->branch.children[i].raw.data, items[i].data, items[i].len);
                node->branch.children[i].raw.len = (uint8_t)items[i].len;
            }
        }
        return true;
    }

    if (item_count == 2) {
        /* Leaf or extension — distinguished by hex-prefix flag */
        /* Decode the path from items[0] */
        size_t path_data_len;
        bool path_is_list;
        const uint8_t *path_encoded;
        size_t path_enc_len;

        if (items[0].data[0] < 0x80) {
            /* Single byte */
            path_encoded = items[0].data;
            path_enc_len = 1;
        } else {
            size_t ho = rlp_decode_header(items[0].data, items[0].len,
                                          &path_data_len, &path_is_list);
            path_encoded = items[0].data + ho;
            path_enc_len = path_data_len;
        }

        bool is_leaf;
        uint8_t nibbles[MAX_NIBBLES];
        size_t nib_len = hex_prefix_decode(path_encoded, path_enc_len,
                                           nibbles, &is_leaf);

        if (is_leaf) {
            node->type = MPT_NODE_LEAF;
            memcpy(node->leaf.path, nibbles, nib_len);
            node->leaf.path_len = (uint8_t)nib_len;

            /* Unwrap the RLP byte string to recover the raw value.
             * make_leaf wraps the value with rbuf_encode_bytes, so we
             * must strip that RLP header to get the original bytes. */
            size_t val_data_len;
            bool val_is_list;
            size_t val_ho = rlp_decode_header(items[1].data, items[1].len,
                                              &val_data_len, &val_is_list);
            node->leaf.value = (uint8_t *)(items[1].data + val_ho);
            node->leaf.value_len = val_data_len;
        } else {
            node->type = MPT_NODE_EXTENSION;
            memcpy(node->extension.path, nibbles, nib_len);
            node->extension.path_len = (uint8_t)nib_len;

            /* Child reference */
            if (items[1].len == 33 && items[1].data[0] == 0xa0) {
                node->extension.child.type = REF_HASH;
                memcpy(node->extension.child.hash, items[1].data + 1, 32);
            } else if (items[1].len == 1 && items[1].data[0] == 0x80) {
                node->extension.child.type = REF_EMPTY;
            } else {
                node->extension.child.type = REF_INLINE;
                if (items[1].len > 31) return false;
                memcpy(node->extension.child.raw.data, items[1].data, items[1].len);
                node->extension.child.raw.len = (uint8_t)items[1].len;
            }
        }
        return true;
    }

    return false;
}

/* =========================================================================
 * Deferred write buffer helpers
 * ========================================================================= */

static void def_init(mpt_store_t *ms) {
    memset(ms->def_ht, 0xff, sizeof(ms->def_ht));  /* -1 = empty */
    memset(ms->def_del_ht, 0xff, sizeof(ms->def_del_ht));
    ms->def_count = 0;
}

static uint32_t def_bucket(const uint8_t hash[32]) {
    uint32_t h;
    memcpy(&h, hash, 4);
    return h % DEFERRED_BUCKETS;
}

static bool def_contains(const mpt_store_t *ms, const uint8_t hash[32]) {
    uint32_t b = def_bucket(hash);
    int idx = ms->def_ht[b];
    while (idx >= 0) {
        if (memcmp(ms->def_entries[idx].hash, hash, 32) == 0 &&
            ms->def_entries[idx].rlp != NULL)
            return true;
        idx = ms->def_entries[idx].ht_next;
    }
    return false;
}

/* Find deferred entry by hash. Returns NULL if not found or removed. */
static const deferred_entry_t *def_find(const mpt_store_t *ms,
                                         const uint8_t hash[32]) {
    uint32_t b = def_bucket(hash);
    int idx = ms->def_ht[b];
    while (idx >= 0) {
        if (memcmp(ms->def_entries[idx].hash, hash, 32) == 0 &&
            ms->def_entries[idx].rlp != NULL)
            return &ms->def_entries[idx];
        idx = ms->def_entries[idx].ht_next;
    }
    return NULL;
}

/* Mutable version of def_find — for refcount updates. */
static deferred_entry_t *def_find_mut(mpt_store_t *ms,
                                       const uint8_t hash[32]) {
    uint32_t b = def_bucket(hash);
    int idx = ms->def_ht[b];
    while (idx >= 0) {
        if (memcmp(ms->def_entries[idx].hash, hash, 32) == 0 &&
            ms->def_entries[idx].rlp != NULL)
            return &ms->def_entries[idx];
        idx = ms->def_entries[idx].ht_next;
    }
    return NULL;
}

static uint8_t *def_rlp_arena_alloc(mpt_store_t *ms, size_t len) {
    if (ms->def_rlp_page_count == 0 ||
        ms->def_rlp_page_used + len > DEF_RLP_PAGE_SIZE) {
        if (ms->def_rlp_page_count >= ms->def_rlp_page_cap) {
            size_t nc = ms->def_rlp_page_cap ? ms->def_rlp_page_cap * 2 : 8;
            uint8_t **np = realloc(ms->def_rlp_pages, nc * sizeof(*np));
            if (!np) return NULL;
            ms->def_rlp_pages = np;
            ms->def_rlp_page_cap = nc;
        }
        size_t psz = len > DEF_RLP_PAGE_SIZE ? len : DEF_RLP_PAGE_SIZE;
        ms->def_rlp_pages[ms->def_rlp_page_count] = malloc(psz);
        if (!ms->def_rlp_pages[ms->def_rlp_page_count]) return NULL;
        ms->def_rlp_page_count++;
        ms->def_rlp_page_used = 0;
    }
    uint8_t *ptr = ms->def_rlp_pages[ms->def_rlp_page_count - 1] +
                   ms->def_rlp_page_used;
    ms->def_rlp_page_used += len;
    return ptr;
}

static bool def_append(mpt_store_t *ms, const uint8_t hash[32],
                        const uint8_t *rlp, uint32_t rlp_len, uint64_t offset) {
    if (ms->def_count >= ms->def_cap) {
        size_t new_cap = ms->def_cap ? ms->def_cap * 2 : DEFERRED_INIT_CAP;
        deferred_entry_t *p = realloc(ms->def_entries,
                                       new_cap * sizeof(deferred_entry_t));
        if (!p) return false;
        ms->def_entries = p;
        ms->def_cap = new_cap;
    }
    deferred_entry_t *e = &ms->def_entries[ms->def_count];
    memcpy(e->hash, hash, 32);
    e->rlp = def_rlp_arena_alloc(ms, rlp_len);
    if (!e->rlp) return false;
    memcpy(e->rlp, rlp, rlp_len);
    e->rlp_len = rlp_len;
    e->offset = offset;
    e->refcount = 1;

    uint32_t b = def_bucket(hash);
    e->ht_next = ms->def_ht[b];
    ms->def_ht[b] = (int)ms->def_count;
    ms->def_count++;
    return true;
}

/* Remove a deferred entry. Returns true if found and removed.
 * Returns the slot to the free list. */
static bool def_remove(mpt_store_t *ms, const uint8_t hash[32]) {
    uint32_t b = def_bucket(hash);
    int idx = ms->def_ht[b];
    int *prev = &ms->def_ht[b];
    while (idx >= 0) {
        deferred_entry_t *e = &ms->def_entries[idx];
        if (memcmp(e->hash, hash, 32) == 0 && e->rlp != NULL) {
            /* Unlink from hash chain */
            *prev = e->ht_next;
            /* Return slot to free list */
            int sc = size_class_for(e->rlp_len);
            free_list_push(&ms->free_lists[sc], e->offset);
            ms->free_slot_bytes += SIZE_CLASSES[sc];
            /* Mark as removed (RLP lives in arena, freed in bulk) */
            e->rlp = NULL;
            return true;
        }
        prev = &e->ht_next;
        idx = e->ht_next;
    }
    return false;
}

/* Pending deletes for on-disk nodes — hash-indexed for O(1) cancel */
static bool def_del_append(mpt_store_t *ms, const uint8_t hash[32]) {
    if (ms->def_del_count >= ms->def_del_cap) {
        size_t new_cap = ms->def_del_cap ? ms->def_del_cap * 2 : 64;
        del_entry_t *p = realloc(ms->def_deletes, new_cap * sizeof(del_entry_t));
        if (!p) return false;
        ms->def_deletes = p;
        ms->def_del_cap = new_cap;
    }
    size_t idx = ms->def_del_count++;
    memcpy(ms->def_deletes[idx].hash, hash, 32);
    uint32_t bkt = def_bucket(hash);
    ms->def_deletes[idx].ht_next = ms->def_del_ht[bkt];
    ms->def_del_ht[bkt] = (int)idx;
    return true;
}

/* Cancel a pending delete (when dedup detects the node is still needed). */
static void def_del_cancel(mpt_store_t *ms, const uint8_t hash[32]) {
    uint32_t bkt = def_bucket(hash);
    int *prev = &ms->def_del_ht[bkt];
    int idx = *prev;
    while (idx >= 0) {
        del_entry_t *e = &ms->def_deletes[idx];
        if (memcmp(e->hash, hash, 32) == 0) {
            *prev = e->ht_next;
            /* Swap with last entry to keep array packed */
            size_t last = ms->def_del_count - 1;
            if ((size_t)idx != last) {
                del_entry_t *tail = &ms->def_deletes[last];
                /* Fix chain pointing to 'last' before overwriting */
                uint32_t tail_bkt = def_bucket(tail->hash);
                int *tp = &ms->def_del_ht[tail_bkt];
                while (*tp >= 0) {
                    if (*tp == (int)last) { *tp = idx; break; }
                    tp = &ms->def_deletes[*tp].ht_next;
                }
                memcpy(e->hash, tail->hash, 32);
                e->ht_next = tail->ht_next;
            }
            ms->def_del_count--;
            return;
        }
        prev = &e->ht_next;
        idx = e->ht_next;
    }
}

static void def_free_all(mpt_store_t *ms) {
    free(ms->def_entries);
    ms->def_entries = NULL;
    ms->def_count = ms->def_cap = 0;
    /* Free deferred RLP arena pages */
    for (size_t i = 0; i < ms->def_rlp_page_count; i++)
        free(ms->def_rlp_pages[i]);
    free(ms->def_rlp_pages);
    ms->def_rlp_pages = NULL;
    ms->def_rlp_page_count = ms->def_rlp_page_cap = 0;
    ms->def_rlp_page_used = 0;
    /* Free pending deletes */
    free(ms->def_deletes);
    ms->def_deletes = NULL;
    ms->def_del_count = ms->def_del_cap = 0;
    def_init(ms);
}

/* =========================================================================
 * Node I/O
 * ========================================================================= */

/* Load a node from disk by its hash. buf must be MAX_NODE_RLP bytes.
 * depth = trie depth for cache pin policy.
 * Returns the RLP length, or 0 on failure. */
static size_t load_node_rlp(const mpt_store_t *ms, const uint8_t hash[32],
                            uint8_t *buf, uint8_t depth) {
    /* Check cache first */
    if (ms->cache) {
        uint16_t cached_len;
        if (ncache_get(ms->cache, hash, buf, &cached_len))
            return cached_len;
    }

    /* Check deferred buffer (for cache-evicted deferred entries) */
    const deferred_entry_t *de = def_find(ms, hash);
    if (de) {
        if (de->rlp_len <= MAX_NODE_RLP) {
            memcpy(buf, de->rlp, de->rlp_len);
            /* Re-populate cache */
            if (ms->cache)
                ncache_put(ms->cache, hash, buf, (uint16_t)de->rlp_len, depth);
        }
        return de->rlp_len;
    }

    node_record_t rec;
    if (!disk_hash_get(ms->index, hash, &rec))
        return 0;
    if (rec.length == 0 || rec.length > MAX_NODE_RLP)
        return 0;
    ssize_t n = pread(ms->data_fd, buf, rec.length,
                      (off_t)(PAGE_SIZE + rec.offset));
    if (n != (ssize_t)rec.length)
        return 0;

    /* Populate cache on miss */
    if (ms->cache)
        ncache_put(ms->cache, hash, buf, (uint16_t)rec.length, depth);

    return rec.length;
}

/* Write a node to the data file and insert its hash into the index.
 * Uses size-class free lists to reuse deleted slots before appending.
 * Returns true on success. */
static bool write_node(mpt_store_t *ms, const uint8_t *rlp, size_t rlp_len,
                       uint8_t out_hash[32]) {
    keccak(rlp, rlp_len, out_hash);

    /* Check if already exists */
    if (ms->shared) {
        /* Shared mode: increment refcount. Do NOT cancel pending deletes —
         * the delete will correctly decrement at flush time.
         * Example: refcount=1, delete queued (-1), write increments (+1)
         *   → at flush: refcount goes 2→1. Net = 1. Correct. */
        node_record_t existing;
        if (disk_hash_get(ms->index, out_hash, &existing)) {
            existing.refcount++;
            disk_hash_put(ms->index, out_hash, &existing);
            return true;
        }
        /* Check deferred buffer */
        deferred_entry_t *def = def_find_mut(ms, out_hash);
        if (def) {
            def->refcount++;
            return true;
        }
    } else {
        /* Non-shared: refcount always 1 — fast existence check */
        if ((ms->cache && ncache_contains(ms->cache, out_hash)) ||
            disk_hash_contains(ms->index, out_hash)) {
            def_del_cancel(ms, out_hash);
            return true;
        }
        if (def_contains(ms, out_hash))
            return true;
    }

    /* Allocate a slot: try free list first, then append */
    int sc = size_class_for((uint32_t)rlp_len);
    uint16_t slot_size = SIZE_CLASSES[sc];
    uint64_t write_offset;

    if (free_list_pop(&ms->free_lists[sc], &write_offset)) {
        /* Reusing a freed slot — no disk I/O needed */
        ms->free_slot_bytes -= slot_size;
    } else {
        /* Append at end of data area */
        write_offset = ms->data_size;
        ms->data_size += slot_size;
    }

    /* Buffer in deferred write buffer (no pwrite, no disk_hash_put) */
    if (!def_append(ms, out_hash, rlp, (uint32_t)rlp_len, write_offset))
        return false;

    ms->live_bytes += rlp_len;

    /* Cache for fast reads within this checkpoint interval */
    if (ms->cache && rlp_len <= MAX_NODE_RLP)
        ncache_put(ms->cache, out_hash, rlp, (uint16_t)rlp_len, NCACHE_DEPTH_UNKNOWN);

    return true;
}

/* Delete a node: buffer the delete for flush time */
static void delete_node(mpt_store_t *ms, const uint8_t hash[32]) {
    /* Check if this is a deferred entry (not on disk yet) */
    deferred_entry_t *def = def_find_mut(ms, hash);
    if (def) {
        if (def->refcount > 1) {
            /* Other references remain — decrement only */
            def->refcount--;
            return;
        }
        /* Last reference — remove entirely */
        def_remove(ms, hash);
        if (ms->cache)
            ncache_delete(ms->cache, hash);
        return;
    }

    /* On-disk node: defer the actual delete until flush */
    def_del_append(ms, hash);

    /* Evict from cache */
    if (ms->cache)
        ncache_delete(ms->cache, hash);
}

/* =========================================================================
 * Node RLP encoding
 * ========================================================================= */

/* Encode a child reference into the RLP payload buffer */
static bool encode_child_ref(rlp_buf_t *buf, const node_ref_t *ref) {
    switch (ref->type) {
    case REF_EMPTY:
        return rbuf_encode_empty(buf);
    case REF_HASH:
        return rbuf_encode_bytes(buf, ref->hash, 32);
    case REF_INLINE:
        /* Inline RLP is already encoded — append raw */
        return rbuf_append(buf, ref->raw.data, ref->raw.len);
    }
    return false;
}

/* Build a branch node RLP and write to store. Returns the node_ref. */
static node_ref_t make_branch(mpt_store_t *ms, const node_ref_t children[16]) {
    node_ref_t result = { .type = REF_EMPTY };

    rlp_buf_t payload; rbuf_reset(&payload);
    for (int i = 0; i < 16; i++) {
        if (!encode_child_ref(&payload, &children[i]))
            return result;
    }
    rbuf_encode_empty(&payload); /* value slot — always empty */

    rlp_buf_t node; rbuf_reset(&node);
    if (!rbuf_list_wrap(&node, &payload))
        return result;

    if (node.len < 32) {
        result.type = REF_INLINE;
        memcpy(result.raw.data, node.data, node.len);
        result.raw.len = (uint8_t)node.len;
    } else {
        result.type = REF_HASH;
        if (!write_node(ms, node.data, node.len, result.hash))
            result.type = REF_EMPTY;
    }
    return result;
}

/* Build a leaf node RLP and write to store. Returns the node_ref. */
static node_ref_t make_leaf(mpt_store_t *ms, const uint8_t *suffix,
                            size_t suffix_len, const uint8_t *value,
                            size_t value_len) {
    node_ref_t result = { .type = REF_EMPTY };

    uint8_t encoded_path[33];
    size_t enc_len = hex_prefix_encode(suffix, suffix_len, true, encoded_path);

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, encoded_path, enc_len);
    rbuf_encode_bytes(&payload, value, value_len);

    rlp_buf_t node; rbuf_reset(&node);
    if (!rbuf_list_wrap(&node, &payload))
        return result;

    if (node.len < 32) {
        result.type = REF_INLINE;
        memcpy(result.raw.data, node.data, node.len);
        result.raw.len = (uint8_t)node.len;
    } else {
        result.type = REF_HASH;
        if (!write_node(ms, node.data, node.len, result.hash))
            result.type = REF_EMPTY;
    }
    return result;
}

/* Build an extension node RLP and write to store. Returns the node_ref. */
static node_ref_t make_extension(mpt_store_t *ms, const uint8_t *path,
                                 size_t path_len, const node_ref_t *child) {
    node_ref_t result = { .type = REF_EMPTY };

    if (path_len == 0) {
        /* Degenerate: no extension needed, return child directly */
        return *child;
    }

    uint8_t encoded_path[33];
    size_t enc_len = hex_prefix_encode(path, path_len, false, encoded_path);

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, encoded_path, enc_len);
    encode_child_ref(&payload, child);

    rlp_buf_t node; rbuf_reset(&node);
    if (!rbuf_list_wrap(&node, &payload))
        return result;

    if (node.len < 32) {
        result.type = REF_INLINE;
        memcpy(result.raw.data, node.data, node.len);
        result.raw.len = (uint8_t)node.len;
    } else {
        result.type = REF_HASH;
        if (!write_node(ms, node.data, node.len, result.hash))
            result.type = REF_EMPTY;
    }
    return result;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

mpt_store_t *mpt_store_create(const char *path, uint64_t capacity_hint) {
    if (!path) return NULL;

    char *idx_path = make_path(path, ".idx");
    char *dat_path = make_path(path, ".dat");
    if (!idx_path || !dat_path) { free(idx_path); free(dat_path); return NULL; }

    disk_hash_t *index = disk_hash_create(idx_path, NODE_HASH_SIZE,
                                           sizeof(node_record_t), capacity_hint);
    if (!index) { free(idx_path); free(dat_path); return NULL; }

    int data_fd = open(dat_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (data_fd < 0) {
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    mpt_store_t *ms = calloc(1, sizeof(*ms));
    if (!ms) {
        close(data_fd);
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    ms->index     = index;
    ms->data_fd   = data_fd;
    ms->data_size  = 0;
    ms->live_bytes = 0;
    memcpy(ms->root_hash, EMPTY_ROOT, 32);
    ms->idx_path   = idx_path;
    ms->dat_path   = dat_path;
    ms->free_path  = make_path(path, ".free");
    /* free_heads and free_slot_bytes zeroed by calloc */
    def_init(ms);

    write_header(data_fd, ms);

    /* Enable default LRU cache */
    mpt_store_set_cache_mb(ms, MPT_DEFAULT_CACHE_MB);

    return ms;
}

mpt_store_t *mpt_store_open(const char *path) {
    if (!path) return NULL;

    char *idx_path = make_path(path, ".idx");
    char *dat_path = make_path(path, ".dat");
    if (!idx_path || !dat_path) { free(idx_path); free(dat_path); return NULL; }

    disk_hash_t *index = disk_hash_open(idx_path);
    if (!index) { free(idx_path); free(dat_path); return NULL; }

    int data_fd = open(dat_path, O_RDWR);
    if (data_fd < 0) {
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    mpt_store_header_t hdr;
    if (!read_header(data_fd, &hdr) ||
        hdr.magic != MPT_STORE_MAGIC ||
        hdr.version != MPT_STORE_VERSION) {
        close(data_fd);
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    mpt_store_t *ms = calloc(1, sizeof(*ms));
    if (!ms) {
        close(data_fd);
        disk_hash_destroy(index);
        free(idx_path); free(dat_path);
        return NULL;
    }

    ms->index      = index;
    ms->data_fd    = data_fd;
    ms->data_size  = hdr.data_size;
    ms->live_bytes = hdr.data_size; /* best estimate; will track from here */
    memcpy(ms->root_hash, hdr.root_hash, 32);
    /* Deserialize free lists from header */
    ms->free_slot_bytes = hdr.free_slot_bytes;
    {
        const uint64_t *src = (const uint64_t *)hdr.free_data;
        size_t pos = 0;
        for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
            uint32_t n = hdr.free_counts[i];
            if (n > MAX_HDR_FREE_OFFSETS - pos) n = 0; /* corrupt guard */
            for (uint32_t j = 0; j < n; j++)
                free_list_push(&ms->free_lists[i], src[pos++]);
        }
    }
    ms->idx_path   = idx_path;
    ms->dat_path   = dat_path;
    ms->free_path  = make_path(path, ".free");
    def_init(ms);

    /* Load overflow free offsets from .free file */
    if (ms->free_path)
        read_free_overflow(ms->free_path, ms);

    /* Enable default LRU cache */
    mpt_store_set_cache_mb(ms, MPT_DEFAULT_CACHE_MB);

    return ms;
}

void mpt_store_destroy(mpt_store_t *ms) {
    if (!ms) return;

    /* Free any pending batch entries */
    free(ms->dirty);
    for (size_t i = 0; i < ms->val_page_count; i++)
        free(ms->val_pages[i]);
    free(ms->val_pages);

    /* Free deferred write buffer */
    def_free_all(ms);

    ncache_destroy(ms->cache);
    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        free_list_destroy(&ms->free_lists[i]);
    disk_hash_destroy(ms->index);
    close(ms->data_fd);
    free(ms->idx_path);
    free(ms->dat_path);
    free(ms->free_path);
    free(ms);
}

void mpt_store_reset(mpt_store_t *ms) {
    if (!ms) return;

    /* Free pending batch entries */
    free(ms->dirty);
    ms->dirty = NULL;
    ms->dirty_count = ms->dirty_cap = 0;
    ms->batch_active = false;
    for (size_t i = 0; i < ms->val_page_count; i++)
        free(ms->val_pages[i]);
    free(ms->val_pages);
    ms->val_pages = NULL;
    ms->val_page_count = ms->val_page_cap = 0;
    ms->val_page_used = 0;

    /* Free deferred write buffer */
    def_free_all(ms);
    def_init(ms);

    /* Free pending deletes */
    free(ms->def_deletes);
    ms->def_deletes = NULL;
    ms->def_del_count = ms->def_del_cap = 0;

    /* Clear LRU cache (keep allocation, just flush entries) */
    if (ms->cache) {
        uint32_t cap = ms->cache->capacity;
        ncache_destroy(ms->cache);
        ms->cache = ncache_create(cap);
    }

    /* Clear free lists */
    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        ms->free_lists[i].count = 0;
    }
    ms->free_slot_bytes = 0;

    /* Clear disk index in-place */
    disk_hash_clear(ms->index);

    /* Truncate data file and rewrite header */
    ftruncate(ms->data_fd, 0);
    ms->data_size = 0;
    ms->live_bytes = 0;
    memcpy(ms->root_hash, EMPTY_ROOT, 32);
    write_header(ms->data_fd, ms);
}

void mpt_store_sync(mpt_store_t *ms) {
    if (!ms) return;
    write_header(ms->data_fd, ms);
    fsync(ms->data_fd);
    disk_hash_sync(ms->index);
}

static int def_offset_cmp(const void *a, const void *b) {
    const deferred_entry_t *ea = *(const deferred_entry_t *const *)a;
    const deferred_entry_t *eb = *(const deferred_entry_t *const *)b;
    return (ea->offset > eb->offset) - (ea->offset < eb->offset);
}

void mpt_store_flush(mpt_store_t *ms) {
    if (!ms) return;

    /* 1. Collect live entries and sort by offset for sequential I/O */
    size_t live = 0;
    for (size_t i = 0; i < ms->def_count; i++)
        if (ms->def_entries[i].rlp) live++;

    deferred_entry_t **sorted = NULL;
    if (live > 0) {
        sorted = malloc(live * sizeof(*sorted));
        if (sorted) {
            size_t j = 0;
            for (size_t i = 0; i < ms->def_count; i++)
                if (ms->def_entries[i].rlp)
                    sorted[j++] = &ms->def_entries[i];
            qsort(sorted, live, sizeof(*sorted), def_offset_cmp);
        }
    }

    /* Write sorted entries — sequential offsets reduce disk seeks */
    size_t n = sorted ? live : ms->def_count;
    for (size_t i = 0; i < n; i++) {
        deferred_entry_t *e = sorted ? sorted[i] : &ms->def_entries[i];
        if (!e->rlp) continue;

        pwrite(ms->data_fd, e->rlp, e->rlp_len,
               (off_t)(PAGE_SIZE + e->offset));

        node_record_t rec = { .offset = e->offset,
                              .length = e->rlp_len,
                              .refcount = e->refcount };
        disk_hash_put(ms->index, e->hash, &rec);
    }
    free(sorted);

    /* 2. Apply pending deletes (refcount-aware) */
    for (size_t i = 0; i < ms->def_del_count; i++) {
        node_record_t rec;
        if (disk_hash_get(ms->index, ms->def_deletes[i].hash, &rec)) {
            if (rec.refcount > 1) {
                /* Other references remain — decrement, keep node */
                rec.refcount--;
                disk_hash_put(ms->index, ms->def_deletes[i].hash, &rec);
            } else {
                /* Last reference — free the slot */
                if (ms->live_bytes >= rec.length)
                    ms->live_bytes -= rec.length;
                int sc = size_class_for(rec.length);
                free_list_push(&ms->free_lists[sc], rec.offset);
                ms->free_slot_bytes += SIZE_CLASSES[sc];
                disk_hash_delete(ms->index, ms->def_deletes[i].hash);
            }
        }
    }

    /* 3. Free deferred buffers */
    def_free_all(ms);

    /* 4. Sync header + fsync both files */
    write_header(ms->data_fd, ms);
    fsync(ms->data_fd);
    disk_hash_sync(ms->index);
}

/* =========================================================================
 * Root hash
 * ========================================================================= */

void mpt_store_root(const mpt_store_t *ms, uint8_t out[32]) {
    if (!ms) { memset(out, 0, 32); return; }
    memcpy(out, ms->root_hash, 32);
}

void mpt_store_set_root(mpt_store_t *ms, const uint8_t root[32]) {
    if (!ms) return;
    memcpy(ms->root_hash, root, 32);
}

void mpt_store_set_shared(mpt_store_t *ms, bool shared) {
    if (!ms) return;
    ms->shared = shared;
}

/* =========================================================================
 * Batch staging
 * ========================================================================= */

/* Allocate from the page-based dirty value arena. Never relocates. */
static uint8_t *val_arena_alloc(mpt_store_t *ms, size_t len) {
    /* Need a new page? */
    if (ms->val_page_count == 0 || ms->val_page_used + len > VAL_PAGE_SIZE) {
        if (ms->val_page_count >= ms->val_page_cap) {
            size_t nc = ms->val_page_cap ? ms->val_page_cap * 2 : 8;
            uint8_t **np = realloc(ms->val_pages, nc * sizeof(*np));
            if (!np) return NULL;
            ms->val_pages = np;
            ms->val_page_cap = nc;
        }
        size_t psz = len > VAL_PAGE_SIZE ? len : VAL_PAGE_SIZE;
        ms->val_pages[ms->val_page_count] = malloc(psz);
        if (!ms->val_pages[ms->val_page_count]) return NULL;
        ms->val_page_count++;
        ms->val_page_used = 0;
    }
    uint8_t *ptr = ms->val_pages[ms->val_page_count - 1] + ms->val_page_used;
    ms->val_page_used += len;
    return ptr;
}

bool mpt_store_begin_batch(mpt_store_t *ms) {
    if (!ms || ms->batch_active) return false;
    ms->batch_active   = true;
    ms->dirty_count    = 0;
    /* Reset arena: reuse first page, free extras */
    if (ms->val_page_count > 1) {
        for (size_t i = 1; i < ms->val_page_count; i++)
            free(ms->val_pages[i]);
        ms->val_page_count = 1;
    }
    ms->val_page_used = 0;
    return true;
}

bool mpt_store_update(mpt_store_t *ms, const uint8_t key[32],
                      const uint8_t *value, size_t value_len) {
    if (!ms || !ms->batch_active || !key || !value || value_len == 0)
        return false;

    /* Grow dirty array if needed */
    if (ms->dirty_count >= ms->dirty_cap) {
        size_t new_cap = ms->dirty_cap ? ms->dirty_cap * 2 : DIRTY_INIT_CAP;
        dirty_entry_t *new_arr = realloc(ms->dirty, new_cap * sizeof(*new_arr));
        if (!new_arr) return false;
        ms->dirty     = new_arr;
        ms->dirty_cap = new_cap;
    }

    dirty_entry_t *e = &ms->dirty[ms->dirty_count];
    bytes_to_nibbles(key, 32, e->nibbles);
    e->value = val_arena_alloc(ms, value_len);
    if (!e->value) return false;
    memcpy(e->value, value, value_len);
    e->value_len = value_len;

    ms->dirty_count++;
    return true;
}

bool mpt_store_delete(mpt_store_t *ms, const uint8_t key[32]) {
    if (!ms || !ms->batch_active || !key) return false;

    if (ms->dirty_count >= ms->dirty_cap) {
        size_t new_cap = ms->dirty_cap ? ms->dirty_cap * 2 : DIRTY_INIT_CAP;
        dirty_entry_t *new_arr = realloc(ms->dirty, new_cap * sizeof(*new_arr));
        if (!new_arr) return false;
        ms->dirty     = new_arr;
        ms->dirty_cap = new_cap;
    }

    dirty_entry_t *e = &ms->dirty[ms->dirty_count];
    bytes_to_nibbles(key, 32, e->nibbles);
    e->value     = NULL;
    e->value_len = 0;

    ms->dirty_count++;
    return true;
}

void mpt_store_discard_batch(mpt_store_t *ms) {
    if (!ms) return;
    ms->dirty_count  = 0;
    ms->val_page_used = 0;
    if (ms->val_page_count > 1) {
        for (size_t i = 1; i < ms->val_page_count; i++)
            free(ms->val_pages[i]);
        ms->val_page_count = 1;
    }
    ms->batch_active = false;
}

/* =========================================================================
 * Core trie update algorithm
 * ========================================================================= */

/* Forward declaration */
static node_ref_t update_subtrie(mpt_store_t *ms, const node_ref_t *current,
                                 dirty_entry_t *entries, size_t start,
                                 size_t end, size_t depth);

/* Build a fresh subtrie from dirty entries (no existing nodes) */
static node_ref_t build_fresh(mpt_store_t *ms, dirty_entry_t *entries,
                              size_t start, size_t end, size_t depth) {
    node_ref_t empty = { .type = REF_EMPTY };

    /* Filter out deletions — nothing to delete in a fresh subtrie */
    /* Count live entries */
    size_t live = 0;
    for (size_t i = start; i < end; i++) {
        if (entries[i].value != NULL) live++;
    }
    if (live == 0) return empty;

    /* All nibbles consumed — remaining entries have identical keys.
     * Should not happen after dedup, but handle defensively. */
    if (depth >= MAX_NIBBLES) {
        return make_leaf(ms, NULL, 0,
                         entries[start].value, entries[start].value_len);
    }

    /* Compact live entries to front (stable order).
     * NULL out moved entries' value pointers to prevent double-free
     * when commit_batch frees all dirty[0..count-1].value pointers. */
    if (live < end - start) {
        size_t w = start;
        for (size_t r = start; r < end; r++) {
            if (entries[r].value != NULL) {
                if (w != r) {
                    entries[w] = entries[r];
                    entries[r].value = NULL;
                }
                w++;
            }
        }
        end = start + live;
    }

    if (live == 1) {
        size_t idx = start;
        return make_leaf(ms, &entries[idx].nibbles[depth],
                         MAX_NIBBLES - depth,
                         entries[idx].value, entries[idx].value_len);
    }

    /* Find common prefix among all live entries at current depth */
    size_t common_len = MAX_NIBBLES - depth;
    for (size_t i = start + 1; i < end && common_len > 0; i++) {
        for (size_t j = 0; j < common_len; j++) {
            if (entries[start].nibbles[depth + j] != entries[i].nibbles[depth + j]) {
                common_len = j;
                break;
            }
        }
    }

    if (common_len > 0) {
        /* Extension node */
        node_ref_t child = build_fresh(ms, entries, start, end,
                                       depth + common_len);
        if (child.type == REF_EMPTY) return empty;
        return make_extension(ms, &entries[start].nibbles[depth],
                              common_len, &child);
    }

    /* Branch node — group by nibble at current depth */
    node_ref_t children[16];
    for (int i = 0; i < 16; i++) children[i].type = REF_EMPTY;

    size_t i = start;
    while (i < end) {
        uint8_t nibble = entries[i].nibbles[depth];
        size_t group_end = i + 1;
        while (group_end < end && entries[group_end].nibbles[depth] == nibble)
            group_end++;

        children[nibble] = build_fresh(ms, entries, i, group_end, depth + 1);
        i = group_end;
    }

    return make_branch(ms, children);
}

/* Load a node from a ref (hash or inline). Returns false if empty.
 * buf must be MAX_NODE_RLP bytes. depth = trie depth for cache pin policy. */
static bool load_from_ref(const mpt_store_t *ms, const node_ref_t *ref,
                          uint8_t *buf, size_t *buf_len, mpt_node_t *node,
                          uint8_t depth) {
    if (ref->type == REF_EMPTY) return false;

    if (ref->type == REF_INLINE) {
        memcpy(buf, ref->raw.data, ref->raw.len);
        *buf_len = ref->raw.len;
        return decode_node(buf, ref->raw.len, node);
    }

    /* REF_HASH */
    *buf_len = load_node_rlp(ms, ref->hash, buf, depth);
    if (*buf_len == 0) return false;
    return decode_node(buf, *buf_len, node);
}

/* Count non-empty children in a branch */
static int count_children(const node_ref_t children[16]) {
    int n = 0;
    for (int i = 0; i < 16; i++)
        if (children[i].type != REF_EMPTY) n++;
    return n;
}

/* Find the single non-empty child index. Returns -1 if != 1 non-empty. */
static int single_child_index(const node_ref_t children[16]) {
    int idx = -1;
    for (int i = 0; i < 16; i++) {
        if (children[i].type != REF_EMPTY) {
            if (idx >= 0) return -1; /* more than one */
            idx = i;
        }
    }
    return idx;
}

/* Delete a stored node referenced by a ref (no-op for inline/empty).
 * In shared mode, refcounting ensures nodes with multiple references
 * are only freed when the last reference is removed. */
static void delete_ref(mpt_store_t *ms, const node_ref_t *ref) {
    if (ref->type == REF_HASH)
        delete_node(ms, ref->hash);
    /* Inline nodes are embedded in parent — nothing to delete */
}

/* Collapse a branch with only one child into extension or leaf.
 * The branch node itself has already been (or will be) deleted. */
static node_ref_t collapse_branch(mpt_store_t *ms, node_ref_t children[16]) {
    int idx = single_child_index(children);
    if (idx < 0) {
        /* Should not happen — caller checked */
        return make_branch(ms, children);
    }

    node_ref_t child_ref = children[idx];
    uint8_t prefix_nibble = (uint8_t)idx;

    /* Load the child node to see if we can merge prefixes */
    uint8_t buf[MAX_NODE_RLP];
    size_t buf_len;
    mpt_node_t child_node;

    if (!load_from_ref(ms, &child_ref, buf, &buf_len, &child_node,
                        NCACHE_DEPTH_UNKNOWN)) {
        /* Can't load child — just make an extension with 1-nibble path */
        return make_extension(ms, &prefix_nibble, 1, &child_ref);
    }

    /* Delete the old child node since we're rebuilding it merged */
    delete_ref(ms, &child_ref);

    if (child_node.type == MPT_NODE_LEAF) {
        /* Merge: extension(1 nibble) + leaf(N nibbles) → leaf(1+N nibbles) */
        uint8_t merged_path[MAX_NIBBLES];
        merged_path[0] = prefix_nibble;
        memcpy(merged_path + 1, child_node.leaf.path, child_node.leaf.path_len);
        return make_leaf(ms, merged_path, 1 + child_node.leaf.path_len,
                         child_node.leaf.value, child_node.leaf.value_len);
    }

    if (child_node.type == MPT_NODE_EXTENSION) {
        /* Merge: ext(1) + ext(M) → ext(1+M) */
        uint8_t merged_path[MAX_NIBBLES];
        merged_path[0] = prefix_nibble;
        memcpy(merged_path + 1, child_node.extension.path,
               child_node.extension.path_len);
        return make_extension(ms, merged_path,
                              1 + child_node.extension.path_len,
                              &child_node.extension.child);
    }

    /* Child is a branch — just add a 1-nibble extension */
    /* Re-write the child since we deleted it above */
    node_ref_t new_child = make_branch(ms, child_node.branch.children);
    return make_extension(ms, &prefix_nibble, 1, &new_child);
}

/* Merge dirty entries into an existing branch node */
static node_ref_t merge_branch(mpt_store_t *ms, const node_ref_t *old_ref,
                                const mpt_node_t *branch,
                                dirty_entry_t *entries, size_t start,
                                size_t end, size_t depth) {
    node_ref_t children[16];
    memcpy(children, branch->branch.children, sizeof(children));
    bool changed = false;

    size_t i = start;
    while (i < end) {
        uint8_t nibble = entries[i].nibbles[depth];
        size_t group_end = i + 1;
        while (group_end < end && entries[group_end].nibbles[depth] == nibble)
            group_end++;

        node_ref_t old_child = children[nibble];
        node_ref_t new_child = update_subtrie(ms, &old_child, entries,
                                              i, group_end, depth + 1);

        if (old_child.type != new_child.type ||
            (new_child.type == REF_HASH &&
             memcmp(old_child.hash, new_child.hash, 32) != 0) ||
            (new_child.type == REF_INLINE &&
             (old_child.raw.len != new_child.raw.len ||
              memcmp(old_child.raw.data, new_child.raw.data, new_child.raw.len) != 0))) {
            children[nibble] = new_child;
            changed = true;
        }

        i = group_end;
    }

    if (!changed)
        return *old_ref;

    /* Delete old branch node */
    delete_ref(ms, old_ref);

    int non_empty = count_children(children);
    if (non_empty == 0) {
        return (node_ref_t){ .type = REF_EMPTY };
    }
    if (non_empty == 1) {
        return collapse_branch(ms, children);
    }

    return make_branch(ms, children);
}

/* Merge dirty entries into an existing leaf node */
static node_ref_t merge_leaf(mpt_store_t *ms, const node_ref_t *old_ref,
                              const mpt_node_t *leaf,
                              dirty_entry_t *entries, size_t start,
                              size_t end, size_t depth) {
    /* Check if any dirty key matches the existing leaf's full path */
    /* Reconstruct leaf's full nibble path at this depth */
    const uint8_t *leaf_suffix = leaf->leaf.path;
    size_t leaf_suffix_len = leaf->leaf.path_len;

    /* If only one dirty entry and it matches the leaf's key exactly */
    if (end - start == 1) {
        /* Check if the dirty key suffix matches */
        bool match = (leaf_suffix_len == MAX_NIBBLES - depth);
        if (match) {
            for (size_t j = 0; j < leaf_suffix_len; j++) {
                if (entries[start].nibbles[depth + j] != leaf_suffix[j]) {
                    match = false;
                    break;
                }
            }
        }

        if (match) {
            /* Update or delete the existing leaf */
            delete_ref(ms, old_ref);
            if (entries[start].value == NULL) {
                return (node_ref_t){ .type = REF_EMPTY };
            }
            return make_leaf(ms, leaf_suffix, leaf_suffix_len,
                             entries[start].value, entries[start].value_len);
        }
    }

    /* Need to merge: create a synthetic entry for the existing leaf,
     * then build a fresh subtrie from all entries combined */
    size_t total = (end - start) + 1;
    dirty_entry_t stack_buf[8];  /* stack fast path for small merges */
    dirty_entry_t *merged = (total <= 8) ? stack_buf
                            : malloc(total * sizeof(*merged));
    if (!merged) return *old_ref;  /* allocation failed — keep trie unchanged */

    delete_ref(ms, old_ref);

    /* Add existing leaf as first entry */
    memset(merged[0].nibbles, 0, MAX_NIBBLES);
    /* Reconstruct full nibble path: the nibbles before depth are irrelevant
     * for build_fresh since it starts at `depth`, but we need to set the
     * nibbles from `depth` onward */
    /* We can copy from any dirty entry for nibbles [0..depth) since they all
     * share the same prefix to reach this node */
    memcpy(merged[0].nibbles, entries[start].nibbles, depth);
    memcpy(merged[0].nibbles + depth, leaf_suffix, leaf_suffix_len);
    merged[0].value = (uint8_t *)leaf->leaf.value; /* borrowed, not freed */
    merged[0].value_len = leaf->leaf.value_len;

    /* Copy dirty entries */
    for (size_t i = 0; i < end - start; i++) {
        merged[1 + i] = entries[start + i];
        /* Mark value as borrowed so we don't double-free */
    }

    /* Check for exact match: if a dirty entry has the same key as the leaf,
     * remove the leaf entry (the dirty entry supersedes it) */
    for (size_t i = 1; i < total; i++) {
        if (memcmp(merged[0].nibbles, merged[i].nibbles, MAX_NIBBLES) == 0) {
            /* Leaf is superseded — remove it by shifting */
            merged[0] = merged[total - 1];
            total--;
            break;
        }
    }

    /* Sort by nibbles */
    /* Simple insertion sort — total is usually small */
    for (size_t i = 1; i < total; i++) {
        dirty_entry_t tmp = merged[i];
        size_t j = i;
        while (j > 0 && memcmp(tmp.nibbles, merged[j - 1].nibbles, MAX_NIBBLES) < 0) {
            merged[j] = merged[j - 1];
            j--;
        }
        merged[j] = tmp;
    }

    node_ref_t result = build_fresh(ms, merged, 0, total, depth);
    if (merged != stack_buf) free(merged);
    return result;
}

/* Merge dirty entries into an existing extension node */
static node_ref_t merge_extension(mpt_store_t *ms, const node_ref_t *old_ref,
                                   const mpt_node_t *ext,
                                   dirty_entry_t *entries, size_t start,
                                   size_t end, size_t depth) {
    const uint8_t *ext_path = ext->extension.path;
    size_t ext_len = ext->extension.path_len;

    /* Find how many nibbles of ext_path ALL dirty entries share */
    size_t shared = ext_len;
    for (size_t i = start; i < end; i++) {
        for (size_t j = 0; j < shared; j++) {
            if (entries[i].nibbles[depth + j] != ext_path[j]) {
                shared = j;
                break;
            }
        }
    }

    if (shared == ext_len) {
        /* All dirty entries pass through the extension entirely */
        node_ref_t old_child = ext->extension.child;
        node_ref_t new_child = update_subtrie(ms, &old_child, entries,
                                              start, end, depth + ext_len);

        /* Check if child changed */
        bool same = (old_child.type == new_child.type);
        if (same && new_child.type == REF_HASH)
            same = (memcmp(old_child.hash, new_child.hash, 32) == 0);
        if (same && new_child.type == REF_INLINE)
            same = (old_child.raw.len == new_child.raw.len &&
                    memcmp(old_child.raw.data, new_child.raw.data, new_child.raw.len) == 0);

        if (same) return *old_ref;

        /* Delete old extension */
        delete_ref(ms, old_ref);

        if (new_child.type == REF_EMPTY) {
            return (node_ref_t){ .type = REF_EMPTY };
        }

        /* Rebuild extension — may need to collapse if child became a leaf */
        uint8_t buf[MAX_NODE_RLP];
        size_t buf_len;
        mpt_node_t child_node;
        if (load_from_ref(ms, &new_child, buf, &buf_len, &child_node,
                          (uint8_t)depth)) {
            if (child_node.type == MPT_NODE_LEAF) {
                /* ext(N) + leaf(M) → leaf(N+M) */
                delete_ref(ms, &new_child);
                uint8_t merged_path[MAX_NIBBLES];
                memcpy(merged_path, ext_path, ext_len);
                memcpy(merged_path + ext_len, child_node.leaf.path,
                       child_node.leaf.path_len);
                return make_leaf(ms, merged_path, ext_len + child_node.leaf.path_len,
                                child_node.leaf.value, child_node.leaf.value_len);
            }
            if (child_node.type == MPT_NODE_EXTENSION) {
                /* ext(N) + ext(M) → ext(N+M) */
                delete_ref(ms, &new_child);
                uint8_t merged_path[MAX_NIBBLES];
                memcpy(merged_path, ext_path, ext_len);
                memcpy(merged_path + ext_len, child_node.extension.path,
                       child_node.extension.path_len);
                return make_extension(ms, merged_path,
                                      ext_len + child_node.extension.path_len,
                                      &child_node.extension.child);
            }
        }

        return make_extension(ms, ext_path, ext_len, &new_child);
    }

    /* Extension path diverges from some dirty entries.
     * Must split: [shared_ext] → branch → [remaining_ext, dirty subtries] */
    delete_ref(ms, old_ref);

    /* Build a branch at the divergence point */
    node_ref_t children[16];
    for (int i = 0; i < 16; i++) children[i].type = REF_EMPTY;

    /* The old extension's child goes under the nibble at position `shared` */
    uint8_t old_nibble = ext_path[shared];
    size_t remaining = ext_len - shared - 1;
    if (remaining > 0) {
        children[old_nibble] = make_extension(ms, &ext_path[shared + 1],
                                              remaining, &ext->extension.child);
    } else {
        children[old_nibble] = ext->extension.child;
    }

    /* Dirty entries go into their respective nibble slots */
    size_t i = start;
    while (i < end) {
        uint8_t nibble = entries[i].nibbles[depth + shared];
        size_t group_end = i + 1;
        while (group_end < end &&
               entries[group_end].nibbles[depth + shared] == nibble)
            group_end++;

        if (nibble == old_nibble) {
            /* Dirty entries share the same nibble as the old extension remnant.
             * Need to merge into the existing child */
            children[nibble] = update_subtrie(ms, &children[nibble], entries,
                                              i, group_end, depth + shared + 1);
        } else {
            /* Fresh subtrie for this nibble */
            children[nibble] = build_fresh(ms, entries, i, group_end,
                                           depth + shared + 1);
        }
        i = group_end;
    }

    /* Check if the branch is degenerate after processing dirty entries.
     * Dirty entries that were deletes of non-existent keys produce REF_EMPTY
     * children, so the branch may have fewer non-empty children than expected.
     * A branch with 0 or 1 children is non-canonical and must be collapsed. */
    int non_empty = count_children(children);

    if (non_empty == 0) {
        return (node_ref_t){ .type = REF_EMPTY };
    }

    node_ref_t subtrie_ref;
    if (non_empty == 1) {
        subtrie_ref = collapse_branch(ms, children);
    } else {
        subtrie_ref = make_branch(ms, children);
    }

    /* Prepend shared prefix (if any), maintaining canonical structure:
     * ext + leaf → leaf, ext + ext → ext, ext + branch → ext(branch) */
    if (shared > 0) {
        if (subtrie_ref.type == REF_EMPTY)
            return subtrie_ref;

        uint8_t buf2[MAX_NODE_RLP];
        size_t buf2_len;
        mpt_node_t sub_node;
        if (load_from_ref(ms, &subtrie_ref, buf2, &buf2_len, &sub_node,
                          (uint8_t)depth)) {
            if (sub_node.type == MPT_NODE_LEAF) {
                /* ext(shared) + leaf(M) → leaf(shared+M) */
                delete_ref(ms, &subtrie_ref);
                uint8_t mp[MAX_NIBBLES];
                memcpy(mp, ext_path, shared);
                memcpy(mp + shared, sub_node.leaf.path,
                       sub_node.leaf.path_len);
                return make_leaf(ms, mp, shared + sub_node.leaf.path_len,
                                 sub_node.leaf.value, sub_node.leaf.value_len);
            }
            if (sub_node.type == MPT_NODE_EXTENSION) {
                /* ext(shared) + ext(M) → ext(shared+M) */
                delete_ref(ms, &subtrie_ref);
                uint8_t mp[MAX_NIBBLES];
                memcpy(mp, ext_path, shared);
                memcpy(mp + shared, sub_node.extension.path,
                       sub_node.extension.path_len);
                return make_extension(ms, mp,
                                      shared + sub_node.extension.path_len,
                                      &sub_node.extension.child);
            }
        }
        return make_extension(ms, ext_path, shared, &subtrie_ref);
    }

    return subtrie_ref;
}

/* Core recursive function: update a subtrie with dirty entries */
static node_ref_t update_subtrie(mpt_store_t *ms, const node_ref_t *current,
                                 dirty_entry_t *entries, size_t start,
                                 size_t end, size_t depth) {
    if (start >= end) return *current;

    if (current->type == REF_EMPTY) {
        return build_fresh(ms, entries, start, end, depth);
    }

    /* Load existing node */
    uint8_t buf[MAX_NODE_RLP];
    size_t buf_len;
    mpt_node_t node;

    if (!load_from_ref(ms, current, buf, &buf_len, &node, (uint8_t)depth)) {
        /* Can't load — treat as empty */
        return build_fresh(ms, entries, start, end, depth);
    }

    switch (node.type) {
    case MPT_NODE_BRANCH:
        return merge_branch(ms, current, &node, entries, start, end, depth);
    case MPT_NODE_LEAF:
        return merge_leaf(ms, current, &node, entries, start, end, depth);
    case MPT_NODE_EXTENSION:
        return merge_extension(ms, current, &node, entries, start, end, depth);
    }

    return (node_ref_t){ .type = REF_EMPTY };
}

/* =========================================================================
 * Commit batch
 * ========================================================================= */

static int dirty_cmp(const void *a, const void *b) {
    const uint8_t *na = ((const dirty_entry_t *)a)->nibbles;
    const uint8_t *nb = ((const dirty_entry_t *)b)->nibbles;
    /* Fast path: compare first 8 nibbles as big-endian uint64 */
    uint64_t pa, pb;
    memcpy(&pa, na, 8);
    memcpy(&pb, nb, 8);
    if (pa != pb) {
        pa = __builtin_bswap64(pa);
        pb = __builtin_bswap64(pb);
        return pa < pb ? -1 : 1;
    }
    return memcmp(na + 8, nb + 8, MAX_NIBBLES - 8);
}

bool mpt_store_commit_batch(mpt_store_t *ms) {
    if (!ms || !ms->batch_active) return false;

    if (ms->dirty_count == 0) {
        ms->batch_active = false;
        return true;
    }

    /* Sort dirty entries by nibbles */
    qsort(ms->dirty, ms->dirty_count, sizeof(dirty_entry_t), dirty_cmp);

    /* Deduplicate: keep the LAST entry for each key (last staged wins) */
    if (ms->dirty_count > 1) {
        size_t w = 0;
        for (size_t r = 1; r < ms->dirty_count; r++) {
            if (memcmp(ms->dirty[w].nibbles, ms->dirty[r].nibbles,
                       MAX_NIBBLES) == 0) {
                /* Duplicate key — overwrite (old value dead in arena) */
                ms->dirty[w] = ms->dirty[r];
            } else {
                w++;
                if (w != r) ms->dirty[w] = ms->dirty[r];
            }
        }
        ms->dirty_count = w + 1;
    }

    /* Build current root ref */
    node_ref_t root_ref;
    if (memcmp(ms->root_hash, EMPTY_ROOT, 32) == 0) {
        root_ref.type = REF_EMPTY;
    } else {
        root_ref.type = REF_HASH;
        memcpy(root_ref.hash, ms->root_hash, 32);
    }

    /* Recursively update the trie */
    node_ref_t new_root = update_subtrie(ms, &root_ref, ms->dirty,
                                         0, ms->dirty_count, 0);

    /* Update root hash */
    if (new_root.type == REF_HASH) {
        memcpy(ms->root_hash, new_root.hash, 32);
    } else if (new_root.type == REF_EMPTY) {
        memcpy(ms->root_hash, EMPTY_ROOT, 32);
    } else {
        /* Root is inline (< 32 bytes RLP) — hash it for the root hash */
        keccak(new_root.raw.data, new_root.raw.len, ms->root_hash);
    }

    /* Reset dirty state — arena pages kept for reuse */
    ms->dirty_count   = 0;
    ms->val_page_used = 0;
    if (ms->val_page_count > 1) {
        for (size_t i = 1; i < ms->val_page_count; i++)
            free(ms->val_pages[i]);
        ms->val_page_count = 1;
    }
    ms->batch_active = false;

    return true;
}

/* =========================================================================
 * Point Lookup
 * ========================================================================= */

uint32_t mpt_store_get(const mpt_store_t *ms, const uint8_t key[32],
                        uint8_t *buf, uint32_t buf_len) {
    if (!ms) return 0;

    /* Empty trie */
    if (memcmp(ms->root_hash, EMPTY_ROOT, 32) == 0)
        return 0;

    /* Convert key to nibbles */
    uint8_t key_nibs[MAX_NIBBLES];
    bytes_to_nibbles(key, 32, key_nibs);
    size_t depth = 0;

    /* Start from root */
    node_ref_t ref;
    ref.type = REF_HASH;
    memcpy(ref.hash, ms->root_hash, 32);

    uint8_t node_buf[MAX_NODE_RLP];
    size_t node_buf_len;
    mpt_node_t node;

    for (;;) {
        if (!load_from_ref(ms, &ref, node_buf, &node_buf_len, &node,
                          (uint8_t)depth))
            return 0;

        switch (node.type) {
        case MPT_NODE_BRANCH: {
            if (depth >= MAX_NIBBLES)
                return 0;  /* consumed all nibbles at a branch = not found */
            uint8_t nib = key_nibs[depth];
            ref = node.branch.children[nib];
            if (ref.type == REF_EMPTY)
                return 0;  /* no child at this nibble */
            depth++;
            break;
        }

        case MPT_NODE_EXTENSION: {
            /* Verify path nibbles match */
            if (depth + node.extension.path_len > MAX_NIBBLES)
                return 0;
            if (memcmp(key_nibs + depth, node.extension.path,
                       node.extension.path_len) != 0)
                return 0;  /* path mismatch */
            depth += node.extension.path_len;
            ref = node.extension.child;
            if (ref.type == REF_EMPTY)
                return 0;
            break;
        }

        case MPT_NODE_LEAF: {
            /* Verify remaining key nibbles match leaf path */
            size_t remaining = MAX_NIBBLES - depth;
            if (node.leaf.path_len != remaining)
                return 0;
            if (memcmp(key_nibs + depth, node.leaf.path, remaining) != 0)
                return 0;  /* key mismatch */

            /* Found! Return value. */
            uint32_t vlen = (uint32_t)node.leaf.value_len;
            if (vlen == 0)
                return 0;
            if (buf_len < vlen)
                return vlen;  /* caller buffer too small */
            memcpy(buf, node.leaf.value, vlen);
            return vlen;
        }

        default:
            return 0;
        }
    }
}

/* =========================================================================
 * Compaction
 * ========================================================================= */

/* Walk the trie recursively, copying all reachable nodes to a new store */
static bool compact_walk(const mpt_store_t *old_ms, mpt_store_t *new_ms,
                         const node_ref_t *ref) {
    if (ref->type != REF_HASH) return true; /* inline/empty — nothing to copy */

    /* Load node from old store */
    uint8_t buf[MAX_NODE_RLP];
    size_t buf_len = load_node_rlp(old_ms, ref->hash, buf, NCACHE_DEPTH_UNKNOWN);
    if (buf_len == 0) return false;

    /* Write to new store */
    uint8_t hash[32];
    if (!write_node((mpt_store_t *)new_ms, buf, buf_len, hash))
        return false;

    /* Decode and recurse into children */
    mpt_node_t node;
    if (!decode_node(buf, buf_len, &node))
        return false;

    if (node.type == MPT_NODE_BRANCH) {
        for (int i = 0; i < 16; i++) {
            if (!compact_walk(old_ms, new_ms, &node.branch.children[i]))
                return false;
        }
    } else if (node.type == MPT_NODE_EXTENSION) {
        if (!compact_walk(old_ms, new_ms, &node.extension.child))
            return false;
    }
    /* Leaf — no children to recurse */

    return true;
}

/* Walk all leaves reachable from a node ref, calling cb for each leaf value. */
static bool walk_leaves_ref(const mpt_store_t *ms, const node_ref_t *ref,
                             mpt_leaf_cb_t cb, void *user_data) {
    uint8_t buf[MAX_NODE_RLP];
    size_t buf_len;
    mpt_node_t node;

    if (ref->type == REF_EMPTY) return true;

    if (ref->type == REF_INLINE) {
        if (!decode_node(ref->raw.data, ref->raw.len, &node))
            return false;
    } else {
        /* REF_HASH */
        buf_len = load_node_rlp(ms, ref->hash, buf, NCACHE_DEPTH_UNKNOWN);
        if (buf_len == 0) return false;
        if (!decode_node(buf, buf_len, &node))
            return false;
    }

    if (node.type == MPT_NODE_LEAF) {
        return cb(node.leaf.value, node.leaf.value_len, user_data);
    } else if (node.type == MPT_NODE_BRANCH) {
        for (int i = 0; i < 16; i++) {
            if (!walk_leaves_ref(ms, &node.branch.children[i], cb, user_data))
                return false;
        }
    } else if (node.type == MPT_NODE_EXTENSION) {
        if (!walk_leaves_ref(ms, &node.extension.child, cb, user_data))
            return false;
    }
    return true;
}

bool mpt_store_walk_leaves(const mpt_store_t *ms, mpt_leaf_cb_t cb,
                            void *user_data) {
    if (!ms || !cb) return false;
    if (memcmp(ms->root_hash, EMPTY_ROOT, 32) == 0) return true;

    node_ref_t root = { .type = REF_HASH };
    memcpy(root.hash, ms->root_hash, 32);
    return walk_leaves_ref(ms, &root, cb, user_data);
}

bool mpt_store_compact(mpt_store_t *ms) {
    if (!ms || ms->batch_active) return false;
    if (ms->shared) return false;  /* unsafe: would delete other tries' nodes */

    /* Create temp paths */
    char *tmp_path = make_path(ms->dat_path, ".compact");
    if (!tmp_path) return false;
    /* Remove ".dat.compact" suffix to get base path for temp store */
    size_t base_len = strlen(ms->dat_path) - 4; /* remove ".dat" */
    char *tmp_base = malloc(base_len + 9); /* ".compact\0" */
    if (!tmp_base) { free(tmp_path); return false; }
    memcpy(tmp_base, ms->dat_path, base_len);
    memcpy(tmp_base + base_len, ".compact", 9);

    /* Create new store (disable cache — temp store doesn't need it) */
    mpt_store_t *new_ms = mpt_store_create(tmp_base, disk_hash_count(ms->index));
    if (!new_ms) {
        free(tmp_path); free(tmp_base);
        return false;
    }
    mpt_store_set_cache(new_ms, 0); /* no cache for temp compaction store */

    /* Walk and copy all reachable nodes */
    node_ref_t root_ref;
    if (memcmp(ms->root_hash, EMPTY_ROOT, 32) == 0) {
        root_ref.type = REF_EMPTY;
    } else {
        root_ref.type = REF_HASH;
        memcpy(root_ref.hash, ms->root_hash, 32);
    }

    bool ok = compact_walk(ms, new_ms, &root_ref);
    if (!ok) {
        /* Save paths before destroy frees them */
        char *fail_idx = new_ms->idx_path;
        char *fail_dat = new_ms->dat_path;
        char *fail_free = new_ms->free_path;
        new_ms->idx_path = NULL;
        new_ms->dat_path = NULL;
        new_ms->free_path = NULL;
        mpt_store_destroy(new_ms);
        unlink(fail_idx);
        unlink(fail_dat);
        if (fail_free) unlink(fail_free);
        free(fail_idx);
        free(fail_dat);
        free(fail_free);
        free(tmp_path); free(tmp_base);
        return false;
    }

    /* Copy root hash and flush deferred writes to disk */
    memcpy(new_ms->root_hash, ms->root_hash, 32);
    mpt_store_flush(new_ms);

    /* Swap files: rename new over old */
    char *old_idx = ms->idx_path;
    char *old_dat = ms->dat_path;
    char *new_idx = new_ms->idx_path;
    char *new_dat = new_ms->dat_path;

    /* Close old files */
    disk_hash_destroy(ms->index);
    close(ms->data_fd);

    /* Rename */
    rename(new_idx, old_idx);
    rename(new_dat, old_dat);

    /* Reopen */
    ms->index = disk_hash_open(old_idx);
    ms->data_fd = open(old_dat, O_RDWR);
    ms->data_size  = new_ms->data_size;
    ms->live_bytes = new_ms->live_bytes; /* after compact, all data is live */

    /* Reset free lists — compacted store has no free slots */
    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        free_list_clear(&ms->free_lists[i]);
    ms->free_slot_bytes = 0;

    /* Invalidate cache — all offsets changed after compaction */
    if (ms->cache) {
        uint32_t cap = ms->cache->capacity;
        ncache_destroy(ms->cache);
        ms->cache = ncache_create(cap);
    }

    /* Remove overflow file — compacted store has no free slots */
    if (ms->free_path) unlink(ms->free_path);

    /* Cleanup new_ms (files already renamed, just free struct) */
    close(new_ms->data_fd);
    disk_hash_destroy(new_ms->index);
    if (new_ms->free_path) unlink(new_ms->free_path);
    free(new_ms->free_path);
    new_ms->idx_path = NULL;
    new_ms->dat_path = NULL;
    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        free_list_destroy(&new_ms->free_lists[i]);
    ncache_destroy(new_ms->cache);
    free(new_ms->dirty);
    free(new_ms);

    ms->idx_path = old_idx;
    ms->dat_path = old_dat;

    free(tmp_path);
    free(tmp_base);
    return ms->index != NULL && ms->data_fd >= 0;
}

bool mpt_store_compact_roots(mpt_store_t *ms,
                              const uint8_t (*roots)[32], size_t n_roots) {
    if (!ms || ms->batch_active || n_roots == 0) return false;

    /* Create temp paths */
    size_t base_len = strlen(ms->dat_path) - 4; /* remove ".dat" */
    char *tmp_base = malloc(base_len + 9);
    if (!tmp_base) return false;
    memcpy(tmp_base, ms->dat_path, base_len);
    memcpy(tmp_base + base_len, ".compact", 9);

    mpt_store_t *new_ms = mpt_store_create(tmp_base, disk_hash_count(ms->index));
    if (!new_ms) { free(tmp_base); return false; }
    mpt_store_set_cache(new_ms, 0);

    /* Walk all roots, copying reachable nodes.
     * compact_walk + write_node naturally dedup — if a node hash already
     * exists in new_ms, write_node is a no-op.  So shared subtrees across
     * different roots are only copied once. */
    bool ok = true;
    for (size_t i = 0; i < n_roots && ok; i++) {
        if (memcmp(roots[i], EMPTY_ROOT, 32) == 0) continue;
        node_ref_t ref = { .type = REF_HASH };
        memcpy(ref.hash, roots[i], 32);
        ok = compact_walk(ms, new_ms, &ref);
    }

    if (!ok) {
        char *fail_idx = new_ms->idx_path;
        char *fail_dat = new_ms->dat_path;
        char *fail_free = new_ms->free_path;
        new_ms->idx_path = NULL;
        new_ms->dat_path = NULL;
        new_ms->free_path = NULL;
        mpt_store_destroy(new_ms);
        unlink(fail_idx);
        unlink(fail_dat);
        if (fail_free) unlink(fail_free);
        free(fail_idx); free(fail_dat); free(fail_free);
        free(tmp_base);
        return false;
    }

    /* Flush deferred writes to disk before swapping files.
     * write_node() buffers all nodes in def_entries — mpt_store_sync()
     * only writes the header, so nodes would be lost on file swap. */
    mpt_store_flush(new_ms);

    /* Swap files */
    char *old_idx = ms->idx_path;
    char *old_dat = ms->dat_path;
    char *new_idx = new_ms->idx_path;
    char *new_dat = new_ms->dat_path;

    disk_hash_destroy(ms->index);
    close(ms->data_fd);

    rename(new_idx, old_idx);
    rename(new_dat, old_dat);

    ms->index = disk_hash_open(old_idx);
    ms->data_fd = open(old_dat, O_RDWR);
    ms->data_size  = new_ms->data_size;
    ms->live_bytes = new_ms->live_bytes;

    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        free_list_clear(&ms->free_lists[i]);
    ms->free_slot_bytes = 0;

    if (ms->cache) {
        uint32_t cap = ms->cache->capacity;
        ncache_destroy(ms->cache);
        ms->cache = ncache_create(cap);
    }

    /* Remove overflow file — compacted store has no free slots */
    if (ms->free_path) unlink(ms->free_path);

    close(new_ms->data_fd);
    disk_hash_destroy(new_ms->index);
    if (new_ms->free_path) unlink(new_ms->free_path);
    free(new_ms->free_path);
    new_ms->idx_path = NULL;
    new_ms->dat_path = NULL;
    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        free_list_destroy(&new_ms->free_lists[i]);
    ncache_destroy(new_ms->cache);
    free(new_ms->dirty);
    free(new_ms);

    ms->idx_path = old_idx;
    ms->dat_path = old_dat;

    free(tmp_base);
    return ms->index != NULL && ms->data_fd >= 0;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

void mpt_store_set_cache(mpt_store_t *ms, uint32_t max_entries) {
    if (!ms) return;
    ncache_destroy(ms->cache);
    ms->cache = ncache_create(max_entries); /* NULL if max_entries == 0 */
}

void mpt_store_set_cache_mb(mpt_store_t *ms, uint32_t megabytes) {
    if (!ms) return;
    if (megabytes == 0) {
        mpt_store_set_cache(ms, 0);
        return;
    }
    uint32_t entries = (uint32_t)(
        (uint64_t)megabytes * 1024 * 1024 / sizeof(ncache_entry_t));
    if (entries == 0) entries = 1;
    mpt_store_set_cache(ms, entries);
}

mpt_store_stats_t mpt_store_stats(const mpt_store_t *ms) {
    mpt_store_stats_t st = {0};
    if (!ms) return st;

    st.node_count = disk_hash_count(ms->index);

    struct stat sb;
    if (fstat(ms->data_fd, &sb) == 0)
        st.data_file_size = (uint64_t)sb.st_size;

    st.live_data_bytes = ms->live_bytes;
    st.free_bytes = ms->free_slot_bytes;
    st.garbage_bytes = ms->data_size > ms->live_bytes + ms->free_slot_bytes
                     ? ms->data_size - ms->live_bytes - ms->free_slot_bytes : 0;

    if (ms->cache) {
        st.cache_hits          = ms->cache->hits;
        st.cache_misses        = ms->cache->misses;
        st.cache_count         = ms->cache->count;
        st.cache_capacity      = ms->cache->capacity;
        st.cache_evict_skipped = ms->cache->evict_skipped;
        st.cache_pinned        = ms->cache->pinned_count;
    }

    return st;
}
