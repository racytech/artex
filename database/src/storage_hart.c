/**
 * Storage Hart — mmap-backed per-account Adaptive Radix Trie for storage slots.
 *
 * Adapts hashed_art.c trie logic to use a shared mmap pool file instead of
 * per-trie malloc arenas. Each account gets a contiguous arena region within
 * the pool. Node refs are arena-relative. Arena growth relocates via pool_alloc
 * + memcpy + pool_free.
 *
 * File layout: [4KB header] [data regions...]
 * Pool-level freelist: size-class powers of 2, same pattern as storage_flat.c.
 */

#define _GNU_SOURCE  /* mremap */

#include "storage_hart.h"
#include "hash.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <immintrin.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define SHRT_MAGIC       0x54524853U  /* "SHRT" little-endian */
#define SHRT_VERSION     1
#define PAGE_SIZE        4096
#define KEY_SIZE         32
#define VALUE_SIZE       32
#define LEAF_SIZE        64           /* key[32] + value[32] */

#define NODE_DIRTY       0x01
#define NODE_4           0
#define NODE_16          1
#define NODE_48          2
#define NODE_256         3
#define NODE48_EMPTY     255
#define SH_REF_NULL      0

#define NUM_SIZE_CLASSES  25
#define INITIAL_MAP_SIZE  (64ULL * 1024)
#define MAX_HDR_FREE      200

#define INITIAL_ARENA_CAP 256
#define SENTINEL_SIZE     64   /* first 64 bytes reserved as sentinel */

static const uint8_t EMPTY_ROOT[32] = {
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
};

/* =========================================================================
 * Node structures (same layout as hashed_art.c, but with sh_ref_t)
 * ========================================================================= */

typedef uint32_t sh_ref_t;

#define SH_IS_LEAF(r) ((r) & 0x80000000u)

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    uint8_t  keys[4];
    sh_ref_t children[4];
    uint8_t  hash[32];
} sh_node4_t;    /* 56 bytes */

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    uint8_t  keys[16];
    sh_ref_t children[16];
    uint8_t  hash[32];
} sh_node16_t;   /* 116 bytes */

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    uint8_t  index[256];
    sh_ref_t children[48];
    uint8_t  hash[32];
} sh_node48_t;   /* 484 bytes */

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    sh_ref_t children[256];
    uint8_t  hash[32];
} sh_node256_t;  /* 1060 bytes */

/* =========================================================================
 * On-disk header (fits in one page)
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t data_size;
    uint64_t free_total_bytes;
    uint32_t free_counts[NUM_SIZE_CLASSES];
    uint8_t  free_data[3972];
} shrt_header_t;

_Static_assert(sizeof(shrt_header_t) <= PAGE_SIZE, "header must fit in one page");

/* =========================================================================
 * In-memory free list (pool-level, one per size class)
 * ========================================================================= */

typedef struct {
    uint64_t *offsets;
    uint32_t  count;
    uint32_t  capacity;
} free_list_t;

static void fl_push(free_list_t *fl, uint64_t offset) {
    if (fl->count >= fl->capacity) {
        uint32_t nc = fl->capacity ? fl->capacity * 2 : 64;
        uint64_t *no = realloc(fl->offsets, nc * sizeof(uint64_t));
        if (!no) return;
        fl->offsets = no;
        fl->capacity = nc;
    }
    fl->offsets[fl->count++] = offset;
}

static bool fl_pop(free_list_t *fl, uint64_t *out) {
    if (fl->count == 0) return false;
    *out = fl->offsets[--fl->count];
    return true;
}

static void fl_destroy(free_list_t *fl) {
    free(fl->offsets);
    fl->offsets = NULL;
    fl->count = fl->capacity = 0;
}

/* =========================================================================
 * Pool struct
 * ========================================================================= */

struct storage_hart_pool {
    int       fd;
    uint8_t  *base;
    size_t    mapped;
    uint64_t  data_size;

    free_list_t free_lists[NUM_SIZE_CLASSES];
    uint64_t    free_total_bytes;

    char     *path;
};

/* =========================================================================
 * Size class helpers (pool-level, power-of-2 bytes)
 * ========================================================================= */

static int size_class_for_bytes(uint64_t bytes) {
    if (bytes <= 1) return 0;
    int cls = 0;
    uint64_t n = bytes - 1;
    while (n > 0) { cls++; n >>= 1; }
    if (cls >= NUM_SIZE_CLASSES) cls = NUM_SIZE_CLASSES - 1;
    return cls;
}

static uint64_t class_bytes(int cls) {
    return 1ULL << cls;
}

/* =========================================================================
 * mmap management
 * ========================================================================= */

static bool ensure_mapped(storage_hart_pool_t *pool, uint64_t need) {
    uint64_t total = PAGE_SIZE + need;
    if (total <= pool->mapped) return true;

    size_t new_size = pool->mapped;
    while (new_size < total)
        new_size = new_size < INITIAL_MAP_SIZE ? INITIAL_MAP_SIZE : new_size + new_size / 2;

    if (ftruncate(pool->fd, (off_t)new_size) != 0) {
        fprintf(stderr, "storage_hart: ftruncate failed size=%zu: %m\n", new_size);
        return false;
    }

    if (pool->base && pool->base != MAP_FAILED) {
        uint8_t *nb = mremap(pool->base, pool->mapped, new_size, MREMAP_MAYMOVE);
        if (nb == MAP_FAILED) {
            fprintf(stderr, "storage_hart: mremap failed %zu->%zu: %m\n",
                    pool->mapped, new_size);
            return false;
        }
        pool->base = nb;
    } else {
        pool->base = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                          MAP_SHARED, pool->fd, 0);
        if (pool->base == MAP_FAILED) return false;
    }
    pool->mapped = new_size;
    return true;
}

/* =========================================================================
 * Pool-level allocator
 * ========================================================================= */

static uint64_t pool_alloc(storage_hart_pool_t *pool, uint64_t bytes, uint64_t *out_cap) {
    if (bytes == 0) return 0;
    int cls = size_class_for_bytes(bytes);
    uint64_t cap = class_bytes(cls);
    if (out_cap) *out_cap = cap;

    uint64_t offset;
    if (fl_pop(&pool->free_lists[cls], &offset)) {
        pool->free_total_bytes -= cap;
        return offset;
    }

    offset = pool->data_size;
    pool->data_size += cap;
    if (!ensure_mapped(pool, pool->data_size))
        return 0;
    return offset;
}

static void pool_free(storage_hart_pool_t *pool, uint64_t offset, uint64_t cap) {
    if (offset == 0 || cap == 0) return;
    int cls = size_class_for_bytes(cap);
    if (class_bytes(cls) != cap) return;
    fl_push(&pool->free_lists[cls], offset);
    pool->free_total_bytes += cap;
}

/* =========================================================================
 * Header serialization
 * ========================================================================= */

static void write_header(storage_hart_pool_t *pool) {
    if (!pool->base || pool->mapped < PAGE_SIZE) return;
    shrt_header_t *hdr = (shrt_header_t *)pool->base;
    hdr->magic = SHRT_MAGIC;
    hdr->version = SHRT_VERSION;
    hdr->data_size = pool->data_size;
    hdr->free_total_bytes = pool->free_total_bytes;

    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        hdr->free_counts[i] = pool->free_lists[i].count;

    size_t pos = 0;
    size_t max = sizeof(hdr->free_data);
    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        uint32_t n = pool->free_lists[i].count;
        if (n > MAX_HDR_FREE) n = MAX_HDR_FREE;
        size_t need = n * sizeof(uint64_t);
        if (pos + need > max) { hdr->free_counts[i] = 0; continue; }
        memcpy(hdr->free_data + pos, pool->free_lists[i].offsets, need);
        hdr->free_counts[i] = n;
        pos += need;
    }
}

static bool read_header(storage_hart_pool_t *pool) {
    if (!pool->base || pool->mapped < PAGE_SIZE) return false;
    shrt_header_t *hdr = (shrt_header_t *)pool->base;
    if (hdr->magic != SHRT_MAGIC || hdr->version != SHRT_VERSION)
        return false;

    pool->data_size = hdr->data_size;
    if (pool->data_size < SENTINEL_SIZE) pool->data_size = SENTINEL_SIZE;
    pool->free_total_bytes = hdr->free_total_bytes;

    size_t pos = 0;
    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        uint32_t n = hdr->free_counts[i];
        size_t need = n * sizeof(uint64_t);
        if (pos + need > sizeof(hdr->free_data)) n = 0;
        for (uint32_t j = 0; j < n; j++) {
            uint64_t off;
            memcpy(&off, hdr->free_data + pos + j * sizeof(uint64_t), sizeof(off));
            fl_push(&pool->free_lists[i], off);
        }
        pos += n * sizeof(uint64_t);
    }
    return true;
}

/* =========================================================================
 * Pool lifecycle
 * ========================================================================= */

storage_hart_pool_t *storage_hart_pool_create(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return NULL;

    storage_hart_pool_t *pool = calloc(1, sizeof(*pool));
    if (!pool) { close(fd); return NULL; }

    pool->fd = fd;
    pool->path = strdup(path);
    pool->data_size = SENTINEL_SIZE;

    if (!ensure_mapped(pool, pool->data_size)) {
        close(fd);
        free(pool->path);
        free(pool);
        return NULL;
    }

    write_header(pool);
    return pool;
}

storage_hart_pool_t *storage_hart_pool_open(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR);
    if (fd < 0) return NULL;

    struct stat sb;
    if (fstat(fd, &sb) != 0 || sb.st_size < PAGE_SIZE) {
        close(fd);
        return NULL;
    }

    storage_hart_pool_t *pool = calloc(1, sizeof(*pool));
    if (!pool) { close(fd); return NULL; }

    pool->fd = fd;
    pool->path = strdup(path);
    pool->mapped = (size_t)sb.st_size;
    pool->base = mmap(NULL, pool->mapped, PROT_READ | PROT_WRITE,
                      MAP_SHARED, fd, 0);
    if (pool->base == MAP_FAILED) {
        close(fd);
        free(pool->path);
        free(pool);
        return NULL;
    }

    if (!read_header(pool)) {
        munmap(pool->base, pool->mapped);
        close(fd);
        free(pool->path);
        free(pool);
        return NULL;
    }

    return pool;
}

void storage_hart_pool_destroy(storage_hart_pool_t *pool) {
    if (!pool) return;
    if (pool->base && pool->base != MAP_FAILED)
        munmap(pool->base, pool->mapped);
    if (pool->fd >= 0) close(pool->fd);
    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        fl_destroy(&pool->free_lists[i]);
    free(pool->path);
    free(pool);
}

void storage_hart_pool_sync(storage_hart_pool_t *pool) {
    if (!pool) return;
    write_header(pool);
    if (pool->base && pool->mapped >= PAGE_SIZE)
        msync(pool->base, PAGE_SIZE, MS_SYNC);
}

/* =========================================================================
 * Arena helpers — per-account region within the pool
 * ========================================================================= */

/** Base pointer for an account's arena data within the mmap. */
static inline uint8_t *arena_base(const storage_hart_pool_t *pool,
                                  const storage_hart_t *sh) {
    return pool->base + PAGE_SIZE + sh->arena_offset;
}

/** Resolve an arena-relative ref to a pointer. */
static inline void *sh_ref_ptr(const storage_hart_pool_t *pool,
                               const storage_hart_t *sh, sh_ref_t ref) {
    return arena_base(pool, sh) + ((size_t)(ref & 0x7FFFFFFFu) << 4);
}

/** Prefetch a ref into L1 cache. */
static inline void sh_prefetch_ref(const storage_hart_pool_t *pool,
                                   const storage_hart_t *sh, sh_ref_t ref) {
    if (ref != SH_REF_NULL) {
        const void *p = sh_ref_ptr(pool, sh, ref);
        _mm_prefetch((const char *)p, _MM_HINT_T0);
    }
}

/** Ensure the arena has room for `needed` more bytes. May relocate in pool. */
static bool arena_ensure(storage_hart_pool_t *pool, storage_hart_t *sh,
                         uint32_t needed) {
    uint32_t aligned = (sh->arena_used + 15) & ~(uint32_t)15;
    if (aligned + needed <= sh->arena_cap) return true;

    /* Grow 1.5x until it fits */
    uint32_t new_cap = sh->arena_cap ? sh->arena_cap + sh->arena_cap / 2
                                     : INITIAL_ARENA_CAP;
    while (aligned + needed > new_cap)
        new_cap = new_cap + new_cap / 2;

    uint64_t pool_cap;
    uint64_t new_offset = pool_alloc(pool, new_cap, &pool_cap);
    if (new_offset == 0) return false;

    /* Copy old data */
    if (sh->arena_offset != 0 && sh->arena_used > 0) {
        uint8_t *old_base = pool->base + PAGE_SIZE + sh->arena_offset;
        uint8_t *new_base = pool->base + PAGE_SIZE + new_offset;
        memcpy(new_base, old_base, sh->arena_used);
        pool_free(pool, sh->arena_offset, sh->arena_cap);
    }

    sh->arena_offset = new_offset;
    sh->arena_cap = (uint32_t)pool_cap;
    return true;
}

/** Allocate bytes from the arena bump allocator. */
static sh_ref_t arena_alloc(storage_hart_pool_t *pool, storage_hart_t *sh,
                            uint32_t bytes, bool is_leaf) {
    uint32_t aligned = (sh->arena_used + 15) & ~(uint32_t)15;

    if (aligned + bytes > sh->arena_cap) {
        if (!arena_ensure(pool, sh, bytes)) return SH_REF_NULL;
        aligned = (sh->arena_used + 15) & ~(uint32_t)15;
    }

    sh->arena_used = aligned + bytes;
    uint8_t *p = arena_base(pool, sh) + aligned;
    memset(p, 0, bytes);
    sh_ref_t ref = (sh_ref_t)(aligned >> 4);
    if (is_leaf) ref |= 0x80000000u;
    return ref;
}

/** Get per-account freelist head pointer for a node type. */
static uint32_t *sh_free_list_for_type(storage_hart_t *sh, int type) {
    switch (type) {
    case NODE_4:   return &sh->free_node4;
    case NODE_16:  return &sh->free_node16;
    case NODE_48:  return &sh->free_node48;
    case NODE_256: return &sh->free_node256;
    default:       return &sh->free_leaf;
    }
}

/** Return a dead node/leaf to its per-account freelist. */
static void arena_free(storage_hart_pool_t *pool, storage_hart_t *sh,
                       sh_ref_t ref, int type) {
    if (ref == SH_REF_NULL) return;
    uint32_t ref_clean = ref & 0x7FFFFFFFu;
    size_t offset = (size_t)ref_clean << 4;
    uint32_t *head = sh_free_list_for_type(sh, type);
    *(uint32_t *)(arena_base(pool, sh) + offset) = *head;
    *head = ref_clean;
}

/** Try per-account freelist first, then fall back to bump alloc. */
static sh_ref_t arena_alloc_or_recycle(storage_hart_pool_t *pool,
                                       storage_hart_t *sh,
                                       uint32_t bytes, bool is_leaf,
                                       int node_type) {
    uint32_t *head = sh_free_list_for_type(sh, node_type);
    if (*head != 0) {
        uint32_t ref_val = *head;
        size_t offset = (size_t)ref_val << 4;
        /* Ensure arena is still valid (might have been relocated) */
        *head = *(uint32_t *)(arena_base(pool, sh) + offset);
        memset(arena_base(pool, sh) + offset, 0, bytes);
        sh_ref_t ref = ref_val;
        if (is_leaf) ref |= 0x80000000u;
        return ref;
    }
    return arena_alloc(pool, sh, bytes, is_leaf);
}

/* =========================================================================
 * Ensure arena is initialized (lazy init on first write)
 * ========================================================================= */

static bool arena_init(storage_hart_pool_t *pool, storage_hart_t *sh) {
    if (sh->arena_offset != 0) return true;

    uint64_t pool_cap;
    uint64_t offset = pool_alloc(pool, INITIAL_ARENA_CAP, &pool_cap);
    if (offset == 0) return false;

    sh->arena_offset = offset;
    sh->arena_cap = (uint32_t)pool_cap;
    sh->arena_used = 16;  /* skip ref 0 as sentinel */
    sh->root_ref = SH_REF_NULL;
    sh->count = 0;
    sh->free_node4 = 0;
    sh->free_node16 = 0;
    sh->free_node48 = 0;
    sh->free_node256 = 0;
    sh->free_leaf = 0;

    /* Zero out the sentinel area */
    memset(arena_base(pool, sh), 0, 16);
    return true;
}

/* =========================================================================
 * Node helpers
 * ========================================================================= */

static inline uint8_t sh_node_type(const void *n) {
    return *(const uint8_t *)n;
}

static inline void sh_mark_dirty(const storage_hart_pool_t *pool,
                                 const storage_hart_t *sh, sh_ref_t ref) {
    if (SH_IS_LEAF(ref) || ref == SH_REF_NULL) return;
    ((uint8_t *)sh_ref_ptr(pool, sh, ref))[2] |= NODE_DIRTY;
}

static inline bool sh_is_node_dirty(const storage_hart_pool_t *pool,
                                    const storage_hart_t *sh, sh_ref_t ref) {
    if (SH_IS_LEAF(ref) || ref == SH_REF_NULL) return true;
    return (((const uint8_t *)sh_ref_ptr(pool, sh, ref))[2] & NODE_DIRTY) != 0;
}

static inline void sh_clear_dirty(const storage_hart_pool_t *pool,
                                  const storage_hart_t *sh, sh_ref_t ref) {
    if (SH_IS_LEAF(ref) || ref == SH_REF_NULL) return;
    ((uint8_t *)sh_ref_ptr(pool, sh, ref))[2] &= ~NODE_DIRTY;
}

static inline const uint8_t *sh_node_hash_ptr(const void *n) {
    switch (sh_node_type(n)) {
    case NODE_4:   return ((const sh_node4_t *)n)->hash;
    case NODE_16:  return ((const sh_node16_t *)n)->hash;
    case NODE_48:  return ((const sh_node48_t *)n)->hash;
    case NODE_256: return ((const sh_node256_t *)n)->hash;
    default: return NULL;
    }
}

static inline uint8_t *sh_node_hash_mut(void *n) {
    switch (sh_node_type(n)) {
    case NODE_4:   return ((sh_node4_t *)n)->hash;
    case NODE_16:  return ((sh_node16_t *)n)->hash;
    case NODE_48:  return ((sh_node48_t *)n)->hash;
    case NODE_256: return ((sh_node256_t *)n)->hash;
    default: return NULL;
    }
}

/* =========================================================================
 * Leaf helpers
 * ========================================================================= */

static inline const uint8_t *sh_leaf_key(const storage_hart_pool_t *pool,
                                         const storage_hart_t *sh,
                                         sh_ref_t ref) {
    return (const uint8_t *)sh_ref_ptr(pool, sh, ref);
}

static inline const uint8_t *sh_leaf_value(const storage_hart_pool_t *pool,
                                           const storage_hart_t *sh,
                                           sh_ref_t ref) {
    return (const uint8_t *)sh_ref_ptr(pool, sh, ref) + KEY_SIZE;
}

static inline bool sh_leaf_matches(const storage_hart_pool_t *pool,
                                   const storage_hart_t *sh,
                                   sh_ref_t ref,
                                   const uint8_t key[32]) {
    return memcmp(sh_leaf_key(pool, sh, ref), key, KEY_SIZE) == 0;
}

static sh_ref_t sh_alloc_leaf(storage_hart_pool_t *pool, storage_hart_t *sh,
                              const uint8_t key[32], const uint8_t value[32]) {
    sh_ref_t ref = arena_alloc_or_recycle(pool, sh, LEAF_SIZE, true, -1);
    if (ref == SH_REF_NULL) return ref;
    uint8_t *p = sh_ref_ptr(pool, sh, ref);
    memcpy(p, key, KEY_SIZE);
    memcpy(p + KEY_SIZE, value, VALUE_SIZE);
    return ref;
}

/* =========================================================================
 * Child management
 * ========================================================================= */

static sh_ref_t *sh_find_child_ptr(const storage_hart_pool_t *pool,
                                   const storage_hart_t *sh,
                                   sh_ref_t nref, uint8_t byte) {
    void *n = sh_ref_ptr(pool, sh, nref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (nn->keys[i] == byte) return &nn->children[i];
        return NULL;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        __m128i kv = _mm_set1_epi8((char)byte);
        __m128i cmp = _mm_cmpeq_epi8(kv, _mm_loadu_si128((__m128i *)nn->keys));
        int mask = _mm_movemask_epi8(cmp) & ((1 << nn->num_children) - 1);
        if (mask) return &nn->children[__builtin_ctz(mask)];
        return NULL;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        uint8_t idx = nn->index[byte];
        return (idx != NODE48_EMPTY) ? &nn->children[idx] : NULL;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        return nn->children[byte] ? &nn->children[byte] : NULL;
    }
    }
    return NULL;
}

static sh_ref_t sh_alloc_node(storage_hart_pool_t *pool, storage_hart_t *sh,
                              uint8_t type) {
    uint32_t sz;
    switch (type) {
    case NODE_4:   sz = sizeof(sh_node4_t); break;
    case NODE_16:  sz = sizeof(sh_node16_t); break;
    case NODE_48:  sz = sizeof(sh_node48_t); break;
    case NODE_256: sz = sizeof(sh_node256_t); break;
    default: return SH_REF_NULL;
    }
    sh_ref_t ref = arena_alloc_or_recycle(pool, sh, sz, false, type);
    if (ref == SH_REF_NULL) return ref;
    void *n = sh_ref_ptr(pool, sh, ref);
    *(uint8_t *)n = type;
    ((uint8_t *)n)[2] = NODE_DIRTY;
    if (type == NODE_48)
        memset(((sh_node48_t *)n)->index, NODE48_EMPTY, 256);
    return ref;
}

static sh_ref_t sh_add_child(storage_hart_pool_t *pool, storage_hart_t *sh,
                             sh_ref_t nref, uint8_t byte, sh_ref_t child) {
    void *n = sh_ref_ptr(pool, sh, nref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        if (nn->num_children < 4) {
            int pos = 0;
            while (pos < nn->num_children && nn->keys[pos] < byte) pos++;
            memmove(nn->keys + pos + 1, nn->keys + pos,
                    nn->num_children - pos);
            memmove(nn->children + pos + 1, nn->children + pos,
                    (nn->num_children - pos) * sizeof(sh_ref_t));
            nn->keys[pos] = byte;
            nn->children[pos] = child;
            nn->num_children++;
            return nref;
        }
        /* Grow to node16 */
        sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_16);
        if (new_ref == SH_REF_NULL) return nref;
        nn = sh_ref_ptr(pool, sh, nref);  /* re-resolve after alloc */
        sh_node16_t *nn16 = sh_ref_ptr(pool, sh, new_ref);
        memcpy(nn16->keys, nn->keys, 4);
        memcpy(nn16->children, nn->children, 4 * sizeof(sh_ref_t));
        nn16->num_children = 4;
        int pos = 0;
        while (pos < 4 && nn16->keys[pos] < byte) pos++;
        memmove(nn16->keys + pos + 1, nn16->keys + pos, 4 - pos);
        memmove(nn16->children + pos + 1, nn16->children + pos,
                (4 - pos) * sizeof(sh_ref_t));
        nn16->keys[pos] = byte;
        nn16->children[pos] = child;
        nn16->num_children = 5;
        arena_free(pool, sh, nref, NODE_4);
        return new_ref;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        if (nn->num_children < 16) {
            int pos = 0;
            while (pos < nn->num_children && nn->keys[pos] < byte) pos++;
            memmove(nn->keys + pos + 1, nn->keys + pos,
                    nn->num_children - pos);
            memmove(nn->children + pos + 1, nn->children + pos,
                    (nn->num_children - pos) * sizeof(sh_ref_t));
            nn->keys[pos] = byte;
            nn->children[pos] = child;
            nn->num_children++;
            return nref;
        }
        /* Grow to node48 */
        sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_48);
        if (new_ref == SH_REF_NULL) return nref;
        nn = sh_ref_ptr(pool, sh, nref);
        sh_node48_t *nn48 = sh_ref_ptr(pool, sh, new_ref);
        for (int i = 0; i < 16; i++) {
            nn48->index[nn->keys[i]] = (uint8_t)i;
            nn48->children[i] = nn->children[i];
        }
        nn48->index[byte] = 16;
        nn48->children[16] = child;
        nn48->num_children = 17;
        arena_free(pool, sh, nref, NODE_16);
        return new_ref;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        if (nn->num_children < 48) {
            uint8_t slot = 0;
            for (; slot < 48; slot++)
                if (nn->children[slot] == SH_REF_NULL) break;
            nn->index[byte] = slot;
            nn->children[slot] = child;
            nn->num_children++;
            return nref;
        }
        /* Grow to node256 */
        sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_256);
        if (new_ref == SH_REF_NULL) return nref;
        nn = sh_ref_ptr(pool, sh, nref);
        sh_node256_t *nn256 = sh_ref_ptr(pool, sh, new_ref);
        for (int i = 0; i < 256; i++) {
            if (nn->index[i] != NODE48_EMPTY)
                nn256->children[i] = nn->children[nn->index[i]];
        }
        nn256->children[byte] = child;
        nn256->num_children = nn->num_children + 1;
        arena_free(pool, sh, nref, NODE_48);
        return new_ref;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        nn->children[byte] = child;
        nn->num_children++;
        return nref;
    }
    }
    return nref;
}

/* =========================================================================
 * Insert (recursive)
 * ========================================================================= */

static sh_ref_t sh_insert_recursive(storage_hart_pool_t *pool,
                                    storage_hart_t *sh, sh_ref_t ref,
                                    const uint8_t key[32],
                                    const uint8_t value[32],
                                    size_t depth, bool *inserted) {
    if (ref == SH_REF_NULL) {
        *inserted = true;
        return sh_alloc_leaf(pool, sh, key, value);
    }

    if (SH_IS_LEAF(ref)) {
        if (sh_leaf_matches(pool, sh, ref, key)) {
            /* Update existing leaf value */
            memcpy((uint8_t *)sh_ref_ptr(pool, sh, ref) + KEY_SIZE,
                   value, VALUE_SIZE);
            *inserted = false;
            return ref;
        }
        /* Split: create chain of Node4s for shared prefix */
        *inserted = true;
        sh_ref_t new_leaf = sh_alloc_leaf(pool, sh, key, value);
        if (new_leaf == SH_REF_NULL) return ref;

        const uint8_t *old_key = sh_leaf_key(pool, sh, ref);
        size_t diff = depth;
        while (diff < KEY_SIZE && old_key[diff] == key[diff]) diff++;

        sh_ref_t new_node = sh_alloc_node(pool, sh, NODE_4);
        if (new_node == SH_REF_NULL) return ref;
        old_key = sh_leaf_key(pool, sh, ref);  /* re-resolve */
        sh_node4_t *nn = sh_ref_ptr(pool, sh, new_node);
        uint8_t old_byte = old_key[diff];
        uint8_t new_byte = key[diff];
        if (old_byte < new_byte) {
            nn->keys[0] = old_byte; nn->children[0] = ref;
            nn->keys[1] = new_byte; nn->children[1] = new_leaf;
        } else {
            nn->keys[0] = new_byte; nn->children[0] = new_leaf;
            nn->keys[1] = old_byte; nn->children[1] = ref;
        }
        nn->num_children = 2;

        /* Wrap with single-child Node4s for each shared byte */
        sh_ref_t result = new_node;
        for (size_t d = diff; d > depth; d--) {
            sh_ref_t wrapper = sh_alloc_node(pool, sh, NODE_4);
            if (wrapper == SH_REF_NULL) return ref;
            old_key = sh_leaf_key(pool, sh, ref);  /* re-resolve */
            sh_node4_t *w = sh_ref_ptr(pool, sh, wrapper);
            w->keys[0] = old_key[d - 1];
            w->children[0] = result;
            w->num_children = 1;
            result = wrapper;
        }
        return result;
    }

    /* Inner node — dispatch on key byte */
    sh_mark_dirty(pool, sh, ref);

    uint8_t byte = key[depth];
    sh_ref_t *child_ptr = sh_find_child_ptr(pool, sh, ref, byte);

    if (child_ptr) {
        sh_ref_t old_child = *child_ptr;
        sh_prefetch_ref(pool, sh, old_child);
        sh_ref_t new_child = sh_insert_recursive(pool, sh, old_child,
                                                  key, value, depth + 1,
                                                  inserted);
        if (new_child != old_child) {
            /* Re-resolve: alloc may have relocated arena */
            child_ptr = sh_find_child_ptr(pool, sh, ref, byte);
            *child_ptr = new_child;
        }
    } else {
        *inserted = true;
        sh_ref_t leaf = sh_alloc_leaf(pool, sh, key, value);
        if (leaf != SH_REF_NULL)
            ref = sh_add_child(pool, sh, ref, byte, leaf);
    }
    return ref;
}

/* =========================================================================
 * Delete (recursive)
 * ========================================================================= */

static sh_ref_t sh_remove_child(storage_hart_pool_t *pool,
                                storage_hart_t *sh,
                                sh_ref_t nref, uint8_t byte) {
    void *n = sh_ref_ptr(pool, sh, nref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            if (nn->keys[i] == byte) {
                memmove(&nn->keys[i], &nn->keys[i+1],
                        nn->num_children - i - 1);
                memmove(&nn->children[i], &nn->children[i+1],
                        (nn->num_children - i - 1) * sizeof(sh_ref_t));
                nn->num_children--;
                return nref;
            }
        }
        return nref;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            if (nn->keys[i] == byte) {
                memmove(&nn->keys[i], &nn->keys[i+1],
                        nn->num_children - i - 1);
                memmove(&nn->children[i], &nn->children[i+1],
                        (nn->num_children - i - 1) * sizeof(sh_ref_t));
                nn->num_children--;
                if (nn->num_children <= 4) {
                    sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_4);
                    if (new_ref == SH_REF_NULL) return nref;
                    nn = sh_ref_ptr(pool, sh, nref);  /* re-resolve */
                    sh_node4_t *n4 = sh_ref_ptr(pool, sh, new_ref);
                    n4->num_children = nn->num_children;
                    memcpy(n4->keys, nn->keys, nn->num_children);
                    memcpy(n4->children, nn->children,
                           nn->num_children * sizeof(sh_ref_t));
                    arena_free(pool, sh, nref, NODE_16);
                    return new_ref;
                }
                return nref;
            }
        }
        return nref;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        uint8_t idx = nn->index[byte];
        if (idx != NODE48_EMPTY) {
            nn->children[idx] = SH_REF_NULL;
            nn->index[byte] = NODE48_EMPTY;
            nn->num_children--;
            if (nn->num_children <= 16) {
                sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_16);
                if (new_ref == SH_REF_NULL) return nref;
                nn = sh_ref_ptr(pool, sh, nref);
                sh_node16_t *n16 = sh_ref_ptr(pool, sh, new_ref);
                n16->num_children = 0;
                for (int i = 0; i < 256; i++) {
                    if (nn->index[i] != NODE48_EMPTY) {
                        n16->keys[n16->num_children] = (uint8_t)i;
                        n16->children[n16->num_children] =
                            nn->children[nn->index[i]];
                        n16->num_children++;
                    }
                }
                arena_free(pool, sh, nref, NODE_48);
                return new_ref;
            }
        }
        return nref;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        if (nn->children[byte] != SH_REF_NULL) {
            nn->children[byte] = SH_REF_NULL;
            nn->num_children--;
            if (nn->num_children <= 48) {
                sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_48);
                if (new_ref == SH_REF_NULL) return nref;
                nn = sh_ref_ptr(pool, sh, nref);
                sh_node48_t *n48 = sh_ref_ptr(pool, sh, new_ref);
                n48->num_children = 0;
                for (int i = 0; i < 256; i++) {
                    if (nn->children[i] != SH_REF_NULL) {
                        n48->index[i] = (uint8_t)n48->num_children;
                        n48->children[n48->num_children] = nn->children[i];
                        n48->num_children++;
                    }
                }
                arena_free(pool, sh, nref, NODE_256);
                return new_ref;
            }
        }
        return nref;
    }
    }
    return nref;
}

static sh_ref_t sh_delete_recursive(storage_hart_pool_t *pool,
                                    storage_hart_t *sh, sh_ref_t ref,
                                    const uint8_t key[32],
                                    size_t depth, bool *deleted) {
    if (ref == SH_REF_NULL) return SH_REF_NULL;

    if (SH_IS_LEAF(ref)) {
        if (sh_leaf_matches(pool, sh, ref, key)) {
            *deleted = true;
            arena_free(pool, sh, ref, -1);
            return SH_REF_NULL;
        }
        return ref;
    }

    uint8_t byte = key[depth];
    sh_ref_t *child_ptr = sh_find_child_ptr(pool, sh, ref, byte);
    if (!child_ptr) return ref;

    sh_mark_dirty(pool, sh, ref);

    sh_ref_t old_child = *child_ptr;
    sh_prefetch_ref(pool, sh, old_child);
    sh_ref_t new_child = sh_delete_recursive(pool, sh, old_child,
                                              key, depth + 1, deleted);

    if (new_child != old_child) {
        if (new_child == SH_REF_NULL) {
            ref = sh_remove_child(pool, sh, ref, byte);
        } else {
            child_ptr = sh_find_child_ptr(pool, sh, ref, byte);
            if (child_ptr) *child_ptr = new_child;
        }
    }

    /* Collapse empty nodes; promote lone leaf children */
    if (!SH_IS_LEAF(ref)) {
        void *node = sh_ref_ptr(pool, sh, ref);
        if (sh_node_type(node) == NODE_4) {
            sh_node4_t *n4 = (sh_node4_t *)node;
            if (n4->num_children == 0) return SH_REF_NULL;
            if (n4->num_children == 1 && SH_IS_LEAF(n4->children[0]))
                return n4->children[0];
        }
    }
    return ref;
}

/* =========================================================================
 * Public API: get / put / del / clear
 * ========================================================================= */

bool storage_hart_get(const storage_hart_pool_t *pool,
                      const storage_hart_t *sh,
                      const uint8_t key[32], uint8_t val[32]) {
    if (!pool || !sh || sh->arena_offset == 0 || sh->root_ref == SH_REF_NULL)
        return false;

    sh_ref_t ref = sh->root_ref;
    size_t depth = 0;
    while (ref != SH_REF_NULL) {
        if (SH_IS_LEAF(ref)) {
            if (sh_leaf_matches(pool, sh, ref, key)) {
                memcpy(val, sh_leaf_value(pool, sh, ref), VALUE_SIZE);
                return true;
            }
            return false;
        }
        sh_ref_t *child = sh_find_child_ptr(pool, sh, ref, key[depth]);
        if (!child) return false;
        ref = *child;
        sh_prefetch_ref(pool, sh, ref);
        depth++;
    }
    return false;
}

bool storage_hart_put(storage_hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32], const uint8_t val[32]) {
    if (!pool || !sh || !key || !val) return false;
    if (!arena_init(pool, sh)) return false;

    bool inserted = false;
    sh->root_ref = sh_insert_recursive(pool, sh, sh->root_ref,
                                        key, val, 0, &inserted);
    if (inserted) sh->count++;
    return true;
}

void storage_hart_del(storage_hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32]) {
    if (!pool || !sh || !key) return;
    if (sh->arena_offset == 0 || sh->root_ref == SH_REF_NULL) return;

    bool deleted = false;
    sh->root_ref = sh_delete_recursive(pool, sh, sh->root_ref,
                                        key, 0, &deleted);
    if (deleted) sh->count--;
}

void storage_hart_clear(storage_hart_pool_t *pool, storage_hart_t *sh) {
    if (!pool || !sh) return;
    if (sh->arena_offset != 0 && sh->arena_cap > 0)
        pool_free(pool, sh->arena_offset, sh->arena_cap);
    memset(sh, 0, sizeof(*sh));
}

/* =========================================================================
 * MPT Root Hash — adapted from hashed_art.c
 * ========================================================================= */

typedef struct { uint8_t data[1024]; size_t len; } rlp_buf_t;

static void rbuf_reset(rlp_buf_t *b) { b->len = 0; }

static bool rbuf_append(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (b->len + n > sizeof(b->data)) return false;
    if (n > 0) memcpy(b->data + b->len, d, n);
    b->len += n;
    return true;
}

static bool rbuf_encode_bytes(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (n == 1 && d[0] < 0x80) return rbuf_append(b, d, 1);
    if (n <= 55) {
        uint8_t pfx = 0x80 + (uint8_t)n;
        return rbuf_append(b, &pfx, 1) && (n > 0 ? rbuf_append(b, d, n) : true);
    }
    size_t ll = 1; size_t tmp = n;
    while (tmp > 255) { ll++; tmp >>= 8; }
    uint8_t hdr[6]; hdr[0] = 0xb7 + (uint8_t)ll;
    tmp = n;
    for (size_t i = ll; i > 0; i--) { hdr[i] = (uint8_t)(tmp & 0xFF); tmp >>= 8; }
    return rbuf_append(b, hdr, 1 + ll) && rbuf_append(b, d, n);
}

static bool rbuf_list_wrap(rlp_buf_t *out, const rlp_buf_t *payload) {
    if (payload->len <= 55) {
        uint8_t pfx = 0xc0 + (uint8_t)payload->len;
        return rbuf_append(out, &pfx, 1) &&
               rbuf_append(out, payload->data, payload->len);
    }
    size_t ll = 1; size_t tmp = payload->len;
    while (tmp > 255) { ll++; tmp >>= 8; }
    uint8_t pfx = 0xf7 + (uint8_t)ll;
    rbuf_append(out, &pfx, 1);
    uint8_t len_be[4]; size_t plen = payload->len;
    for (size_t i = ll; i > 0; i--) {
        len_be[i-1] = (uint8_t)(plen & 0xFF); plen >>= 8;
    }
    return rbuf_append(out, len_be, ll) &&
           rbuf_append(out, payload->data, payload->len);
}

static void sh_keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    hash_t h = hash_keccak256(data, len);
    memcpy(out, h.bytes, 32);
}

static size_t sh_hex_prefix_encode(const uint8_t *nibbles, size_t nib_len,
                                   bool is_leaf, uint8_t *out) {
    uint8_t prefix = (is_leaf ? 2 : 0) + (nib_len % 2 ? 1 : 0);
    size_t pos = 0;
    if (nib_len % 2) {
        out[pos++] = (prefix << 4) | nibbles[0];
        nibbles++; nib_len--;
    } else {
        out[pos++] = prefix << 4;
    }
    for (size_t i = 0; i < nib_len; i += 2)
        out[pos++] = (nibbles[i] << 4) | nibbles[i+1];
    return pos;
}

/* Forward declaration */
static size_t sh_hash_ref(storage_hart_pool_t *pool, storage_hart_t *sh,
                          sh_ref_t ref, size_t byte_depth,
                          storage_hart_encode_t encode, void *ctx,
                          const uint8_t *nib_prefix, size_t nib_prefix_len,
                          uint8_t *rlp_out);

static size_t sh_rlp_to_hashref(const uint8_t *rlp, size_t rlp_len,
                                uint8_t *out) {
    if (rlp_len < 32) { memcpy(out, rlp, rlp_len); return rlp_len; }
    sh_keccak(rlp, rlp_len, out);
    return 32;
}

static size_t sh_encode_child_ref(const uint8_t *child_rlp, size_t child_len,
                                  uint8_t *out) {
    if (child_len == 0) { out[0] = 0x80; return 1; }
    if (child_len < 32) { memcpy(out, child_rlp, child_len); return child_len; }
    if (child_len == 32) {
        out[0] = 0xa0; memcpy(out + 1, child_rlp, 32); return 33;
    }
    out[0] = 0xa0; sh_keccak(child_rlp, child_len, out + 1); return 33;
}

static size_t sh_encode_leaf_node(storage_hart_pool_t *pool,
                                  storage_hart_t *sh,
                                  sh_ref_t leaf_ref, size_t byte_depth,
                                  storage_hart_encode_t encode, void *ctx,
                                  const uint8_t *nib_prefix,
                                  size_t nib_prefix_len,
                                  uint8_t *rlp_out) {
    const uint8_t *key = sh_leaf_key(pool, sh, leaf_ref);

    uint8_t path[128];
    size_t path_len = 0;
    if (nib_prefix_len > 0) {
        memcpy(path, nib_prefix, nib_prefix_len);
        path_len = nib_prefix_len;
    }
    for (size_t i = byte_depth; i < KEY_SIZE; i++) {
        path[path_len++] = (key[i] >> 4) & 0x0F;
        path[path_len++] =  key[i]       & 0x0F;
    }

    uint8_t hp[33];
    size_t hp_len = sh_hex_prefix_encode(path, path_len, true, hp);

    uint8_t value_rlp[256];
    uint32_t value_len = encode(key, sh_leaf_value(pool, sh, leaf_ref),
                                value_rlp, ctx);
    if (value_len == 0) return 0;

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_encode_bytes(&payload, value_rlp, value_len);

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    return sh_rlp_to_hashref(encoded.data, encoded.len, rlp_out);
}

static size_t sh_encode_extension(const uint8_t *nibbles, size_t nib_len,
                                  const uint8_t *child_rlp, size_t child_len,
                                  uint8_t *rlp_out) {
    uint8_t hp[33];
    size_t hp_len = sh_hex_prefix_encode(nibbles, nib_len, false, hp);
    uint8_t cref[33];
    size_t cref_len = sh_encode_child_ref(child_rlp, child_len, cref);
    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_append(&payload, cref, cref_len);
    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    return sh_rlp_to_hashref(encoded.data, encoded.len, rlp_out);
}

static size_t sh_build_branch_rlp(uint8_t slots[16][33],
                                  uint8_t slot_lens[16],
                                  uint8_t *rlp_out) {
    uint8_t payload[600];
    size_t plen = 0;
    for (int i = 0; i < 16; i++) {
        if (slot_lens[i] == 0) {
            payload[plen++] = 0x80;
        } else if (slot_lens[i] < 32) {
            memcpy(payload + plen, slots[i], slot_lens[i]);
            plen += slot_lens[i];
        } else {
            payload[plen++] = 0xa0;
            memcpy(payload + plen, slots[i], 32);
            plen += 32;
        }
    }
    payload[plen++] = 0x80; /* value slot */

    uint8_t encoded[610];
    size_t elen;
    if (plen <= 55) {
        encoded[0] = 0xc0 + (uint8_t)plen;
        memcpy(encoded + 1, payload, plen);
        elen = 1 + plen;
    } else {
        size_t ll = 1; size_t tmp = plen;
        while (tmp > 255) { ll++; tmp >>= 8; }
        encoded[0] = 0xf7 + (uint8_t)ll;
        size_t p = plen;
        for (size_t i = ll; i > 0; i--) {
            encoded[i] = (uint8_t)(p & 0xFF); p >>= 8;
        }
        memcpy(encoded + 1 + ll, payload, plen);
        elen = 1 + ll + plen;
    }
    return sh_rlp_to_hashref(encoded, elen, rlp_out);
}

static size_t sh_hash_lo_group(storage_hart_pool_t *pool,
                               storage_hart_t *sh,
                               const uint8_t *lo_keys,
                               const sh_ref_t *lo_refs, int count,
                               size_t next_depth,
                               storage_hart_encode_t encode, void *ctx,
                               uint8_t *rlp_out) {
    uint8_t slots[16][33];
    uint8_t slot_lens[16] = {0};
    for (int j = 0; j < count; j++) {
        if (j + 1 < count)
            sh_prefetch_ref(pool, sh, lo_refs[j + 1]);
        slot_lens[lo_keys[j]] = (uint8_t)
            sh_hash_ref(pool, sh, lo_refs[j], next_depth, encode, ctx,
                        NULL, 0, slots[lo_keys[j]]);
    }
    return sh_build_branch_rlp(slots, slot_lens, rlp_out);
}

static size_t sh_hash_ref(storage_hart_pool_t *pool, storage_hart_t *sh,
                          sh_ref_t ref, size_t byte_depth,
                          storage_hart_encode_t encode, void *ctx,
                          const uint8_t *nib_prefix, size_t nib_prefix_len,
                          uint8_t *rlp_out) {
    if (ref == SH_REF_NULL) return 0;

    if (SH_IS_LEAF(ref))
        return sh_encode_leaf_node(pool, sh, ref, byte_depth, encode, ctx,
                                   nib_prefix, nib_prefix_len, rlp_out);

    if (!sh_is_node_dirty(pool, sh, ref)) {
        const uint8_t *cached = sh_node_hash_ptr(sh_ref_ptr(pool, sh, ref));
        if (cached) {
            if (nib_prefix_len > 0)
                return sh_encode_extension(nib_prefix, nib_prefix_len,
                                           cached, 32, rlp_out);
            memcpy(rlp_out, cached, 32);
            return 32;
        }
    }

    void *n = sh_ref_ptr(pool, sh, ref);
    uint8_t lo_keys[16][16];
    sh_ref_t lo_refs[16][16];
    uint8_t gcounts[16] = {0};
    int nchildren = 0;

    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            uint8_t hi = nn->keys[i] >> 4, lo = nn->keys[i] & 0xF;
            int g = gcounts[hi]++;
            lo_keys[hi][g] = lo;
            lo_refs[hi][g] = nn->children[i];
        }
        nchildren = nn->num_children;
        break;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            uint8_t hi = nn->keys[i] >> 4, lo = nn->keys[i] & 0xF;
            int g = gcounts[hi]++;
            lo_keys[hi][g] = lo;
            lo_refs[hi][g] = nn->children[i];
        }
        nchildren = nn->num_children;
        break;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        for (int i = 0; i < 256; i++) {
            if (nn->index[i] != NODE48_EMPTY) {
                uint8_t hi = i >> 4, lo = i & 0xF;
                int g = gcounts[hi]++;
                lo_keys[hi][g] = lo;
                lo_refs[hi][g] = nn->children[nn->index[i]];
                nchildren++;
            }
        }
        break;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        for (int i = 0; i < 256; i++) {
            if (nn->children[i]) {
                uint8_t hi = i >> 4, lo = i & 0xF;
                int g = gcounts[hi]++;
                lo_keys[hi][g] = lo;
                lo_refs[hi][g] = nn->children[i];
                nchildren++;
            }
        }
        break;
    }
    }

    size_t next_depth = byte_depth + 1;
    int hi_occupied = 0, single_hi = -1;
    for (int i = 0; i < 16; i++) {
        if (gcounts[i] > 0) { hi_occupied++; single_hi = i; }
    }

    if (nchildren == 1) {
        uint8_t pfx[128]; size_t pfx_len = 0;
        if (nib_prefix_len > 0) {
            memcpy(pfx, nib_prefix, nib_prefix_len);
            pfx_len = nib_prefix_len;
        }
        pfx[pfx_len++] = (uint8_t)single_hi;
        pfx[pfx_len++] = lo_keys[single_hi][0];
        return sh_hash_ref(pool, sh, lo_refs[single_hi][0], next_depth,
                           encode, ctx, pfx, pfx_len, rlp_out);
    }
    if (hi_occupied == 1) {
        uint8_t pfx[128]; size_t pfx_len = 0;
        if (nib_prefix_len > 0) {
            memcpy(pfx, nib_prefix, nib_prefix_len);
            pfx_len = nib_prefix_len;
        }
        pfx[pfx_len++] = (uint8_t)single_hi;
        if (gcounts[single_hi] == 1) {
            pfx[pfx_len++] = lo_keys[single_hi][0];
            return sh_hash_ref(pool, sh, lo_refs[single_hi][0], next_depth,
                               encode, ctx, pfx, pfx_len, rlp_out);
        }
        uint8_t lo_hash[33];
        size_t lo_len = sh_hash_lo_group(pool, sh, lo_keys[single_hi],
                                          lo_refs[single_hi],
                                          gcounts[single_hi],
                                          next_depth, encode, ctx, lo_hash);
        return sh_encode_extension(pfx, pfx_len, lo_hash, lo_len, rlp_out);
    }

    uint8_t hi_slots[16][33];
    uint8_t hi_slot_lens[16] = {0};
    for (int hi = 0; hi < 16; hi++) {
        if (gcounts[hi] == 0) continue;
        /* Prefetch next occupied slot's children */
        for (int next = hi + 1; next < 16; next++) {
            if (gcounts[next] > 0) {
                for (int j = 0; j < gcounts[next]; j++)
                    sh_prefetch_ref(pool, sh, lo_refs[next][j]);
                break;
            }
        }
        if (gcounts[hi] == 1) {
            uint8_t lo_nib = lo_keys[hi][0];
            hi_slot_lens[hi] = (uint8_t)
                sh_hash_ref(pool, sh, lo_refs[hi][0], next_depth,
                            encode, ctx, &lo_nib, 1, hi_slots[hi]);
        } else {
            hi_slot_lens[hi] = (uint8_t)
                sh_hash_lo_group(pool, sh, lo_keys[hi], lo_refs[hi],
                                 gcounts[hi], next_depth, encode, ctx,
                                 hi_slots[hi]);
        }
    }

    uint8_t node_hash[33];
    size_t node_hash_len = sh_build_branch_rlp(hi_slots, hi_slot_lens,
                                                node_hash);
    /* Cache hash in node */
    n = sh_ref_ptr(pool, sh, ref);
    memcpy(sh_node_hash_mut(n), node_hash, 32);
    sh_clear_dirty(pool, sh, ref);

    if (nib_prefix_len > 0)
        return sh_encode_extension(nib_prefix, nib_prefix_len,
                                   node_hash, node_hash_len, rlp_out);
    memcpy(rlp_out, node_hash, node_hash_len);
    return node_hash_len;
}

void storage_hart_root_hash(storage_hart_pool_t *pool,
                            storage_hart_t *sh,
                            storage_hart_encode_t encode,
                            void *ctx, uint8_t out[32]) {
    if (!pool || !sh || sh->root_ref == SH_REF_NULL) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }
    uint8_t rlp[1024];
    size_t len = sh_hash_ref(pool, sh, sh->root_ref, 0, encode, ctx,
                             NULL, 0, rlp);
    if (len == 32) {
        memcpy(out, rlp, 32);
    } else if (len > 0 && len < 32) {
        sh_keccak(rlp, len, out);
    } else {
        memcpy(out, EMPTY_ROOT, 32);
    }
}

/* =========================================================================
 * Invalidate / Mark dirty
 * ========================================================================= */

static void sh_invalidate_recursive(const storage_hart_pool_t *pool,
                                    const storage_hart_t *sh,
                                    sh_ref_t ref) {
    if (ref == SH_REF_NULL || SH_IS_LEAF(ref)) return;
    sh_mark_dirty(pool, sh, ref);
    void *n = sh_ref_ptr(pool, sh, ref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            sh_invalidate_recursive(pool, sh, nn->children[i]);
        break;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            sh_invalidate_recursive(pool, sh, nn->children[i]);
        break;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->index[i] != NODE48_EMPTY)
                sh_invalidate_recursive(pool, sh, nn->children[nn->index[i]]);
        break;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->children[i])
                sh_invalidate_recursive(pool, sh, nn->children[i]);
        break;
    }
    }
}

void storage_hart_invalidate(storage_hart_pool_t *pool,
                             storage_hart_t *sh) {
    if (!pool || !sh || sh->root_ref == SH_REF_NULL) return;
    sh_invalidate_recursive(pool, sh, sh->root_ref);
}

void storage_hart_mark_dirty(storage_hart_pool_t *pool, storage_hart_t *sh,
                             const uint8_t key[32]) {
    if (!pool || !sh || sh->root_ref == SH_REF_NULL) return;
    sh_ref_t ref = sh->root_ref;
    size_t depth = 0;
    while (ref != SH_REF_NULL) {
        if (SH_IS_LEAF(ref)) return;
        sh_mark_dirty(pool, sh, ref);
        sh_ref_t *child = sh_find_child_ptr(pool, sh, ref, key[depth]);
        if (!child) return;
        ref = *child;
        depth++;
    }
}

/* =========================================================================
 * Iteration (recursive DFS with callback)
 * ========================================================================= */

static bool sh_foreach_recursive(const storage_hart_pool_t *pool,
                                 const storage_hart_t *sh,
                                 sh_ref_t ref,
                                 storage_hart_iter_cb cb, void *ctx) {
    if (ref == SH_REF_NULL) return true;

    if (SH_IS_LEAF(ref)) {
        const uint8_t *key = sh_leaf_key(pool, sh, ref);
        const uint8_t *val = sh_leaf_value(pool, sh, ref);
        return cb(key, val, ctx);
    }

    void *n = sh_ref_ptr(pool, sh, ref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (!sh_foreach_recursive(pool, sh, nn->children[i], cb, ctx))
                return false;
        break;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (!sh_foreach_recursive(pool, sh, nn->children[i], cb, ctx))
                return false;
        break;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->index[i] != NODE48_EMPTY)
                if (!sh_foreach_recursive(pool, sh,
                        nn->children[nn->index[i]], cb, ctx))
                    return false;
        break;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->children[i])
                if (!sh_foreach_recursive(pool, sh,
                        nn->children[i], cb, ctx))
                    return false;
        break;
    }
    }
    return true;
}

void storage_hart_foreach(const storage_hart_pool_t *pool,
                          const storage_hart_t *sh,
                          storage_hart_iter_cb cb, void *ctx) {
    if (!pool || !sh || !cb) return;
    if (sh->arena_offset == 0 || sh->root_ref == SH_REF_NULL) return;
    sh_foreach_recursive(pool, sh, sh->root_ref, cb, ctx);
}

/* =========================================================================
 * Pool stats
 * ========================================================================= */

storage_hart_pool_stats_t storage_hart_pool_stats(
        const storage_hart_pool_t *pool) {
    storage_hart_pool_stats_t st = {0};
    if (!pool) return st;
    st.data_size = pool->data_size;
    st.free_bytes = pool->free_total_bytes;
    struct stat sb;
    if (fstat(pool->fd, &sb) == 0)
        st.file_size = (uint64_t)sb.st_size;
    return st;
}
