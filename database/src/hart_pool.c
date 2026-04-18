/*
 * hart_pool — single-pool slab allocator. See include/hart_pool.h and
 * docs/storage_hart_pool_design.md.
 */

#define _GNU_SOURCE  /* mremap */

#include "hart_pool.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/* =========================================================================
 * Internal types
 * ========================================================================= */

typedef struct {
    uint64_t *off;
    uint32_t  cnt;
    uint32_t  cap;
} fl_t;

struct hart_pool {
    uint8_t  *base;
    uint64_t  mapped;
    uint64_t  used;
    uint64_t  free_bytes;

    fl_t      fl[HART_SLAB_CLASSES];

    uint32_t  slab_count;
    uint32_t  slabs_per_class[HART_SLAB_CLASSES];
};

/* Every slab starts with this 16-byte header so free-time can recover the
 * chain link and capacity without an external map. Payload starts at +16. */
#define SLAB_HEADER_SIZE 16U

typedef struct {
    uint64_t chain_ref;  /* previous slab in this hart's chain (0 = none) */
    uint32_t cap;        /* slab capacity in bytes */
    uint32_t _reserved;
} slab_header_t;

/* =========================================================================
 * Size-class helpers
 * ========================================================================= */

static inline uint32_t slab_class(uint32_t cap) {
    uint32_t c = 0, v = cap / HART_SLAB_MIN;
    while (v > 1) { v >>= 1; c++; }
    return c;
}

static inline uint32_t slab_round(uint32_t bytes) {
    if (bytes <= HART_SLAB_MIN) return HART_SLAB_MIN;
    if (bytes >= HART_SLAB_MAX) return HART_SLAB_MAX;
    uint32_t v = HART_SLAB_MIN;
    while (v < bytes) v <<= 1;
    return v;
}

/* =========================================================================
 * Freelist ops
 * ========================================================================= */

static bool fl_push(fl_t *f, uint64_t off) {
    if (f->cnt == f->cap) {
        uint32_t nc = f->cap ? f->cap * 2 : 16;
        uint64_t *nb = realloc(f->off, (size_t)nc * sizeof(uint64_t));
        if (!nb) return false;
        f->off = nb;
        f->cap = nc;
    }
    f->off[f->cnt++] = off;
    return true;
}

static uint64_t fl_pop(fl_t *f) {
    if (f->cnt == 0) return 0;
    return f->off[--f->cnt];
}

/* =========================================================================
 * Slab header helpers
 * ========================================================================= */

static inline slab_header_t *slab_hdr(hart_pool_t *p, hart_pool_ref_t ref) {
    return (slab_header_t *)(p->base + ref);
}

static void write_slab_header(hart_pool_t *p, hart_pool_ref_t ref,
                              hart_pool_ref_t chain_ref, uint32_t cap) {
    slab_header_t *h = slab_hdr(p, ref);
    h->chain_ref = chain_ref;
    h->cap       = cap;
    h->_reserved = 0;
}

/* =========================================================================
 * Growth
 * ========================================================================= */

static bool ensure_mapped(hart_pool_t *p, uint64_t need) {
    if (need <= p->mapped) return true;

    uint64_t new_size = p->mapped;
    while (new_size < need) new_size += HART_POOL_GROW_STEP;

    void *nb = mremap(p->base, p->mapped, new_size, MREMAP_MAYMOVE);
    if (nb == MAP_FAILED) {
        fprintf(stderr, "hart_pool: mremap %zu->%zu failed: %s\n",
                (size_t)p->mapped, (size_t)new_size, strerror(errno));
        return false;
    }
    p->base   = nb;
    p->mapped = new_size;
    return true;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

hart_pool_t *hart_pool_create(void) {
    hart_pool_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;

    void *base = mmap(NULL, HART_POOL_INITIAL_MAP,
                      PROT_READ | PROT_WRITE,
                      MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (base == MAP_FAILED) {
        free(p);
        return NULL;
    }

    p->base   = base;
    p->mapped = HART_POOL_INITIAL_MAP;

    /* Reserve the first slab's worth of bytes as a sentinel so offset 0
     * naturally maps to HART_POOL_REF_NULL. First real slab starts at
     * HART_SLAB_MIN. */
    p->used = HART_SLAB_MIN;
    return p;
}

void hart_pool_destroy(hart_pool_t *p) {
    if (!p) return;
    if (p->base && p->base != MAP_FAILED)
        munmap(p->base, p->mapped);
    for (uint32_t i = 0; i < HART_SLAB_CLASSES; i++)
        free(p->fl[i].off);
    free(p);
}

/* =========================================================================
 * Slab reserve
 * ========================================================================= */

static hart_pool_ref_t slab_reserve(hart_pool_t *p, uint32_t cap) {
    uint32_t cls = slab_class(cap);

    /* Freelist first */
    uint64_t off = fl_pop(&p->fl[cls]);
    if (off != 0) {
        p->free_bytes -= cap;
        p->slabs_per_class[cls]++;
        p->slab_count++;
        return off;
    }

    /* Bump */
    off = p->used;
    uint64_t new_used = off + cap;
    if (!ensure_mapped(p, new_used)) return HART_POOL_REF_NULL;
    p->used = new_used;
    p->slabs_per_class[cls]++;
    p->slab_count++;
    return off;
}

/* =========================================================================
 * Public alloc
 * ========================================================================= */

hart_pool_ref_t hart_pool_alloc(hart_pool_t *p, hart_slab_t *slab,
                                 uint32_t bytes, uint32_t *out_cap) {
    if (!p || !slab || bytes == 0) return HART_POOL_REF_NULL;

    /* Max single node can't exceed slab payload. */
    if (bytes > HART_SLAB_MAX - SLAB_HEADER_SIZE) return HART_POOL_REF_NULL;

    /* 16-byte alignment for node payloads. */
    uint32_t aligned = (slab->head_used + 15u) & ~15u;

    /* Fast path: fits in current head slab. */
    if (slab->head_ref != HART_POOL_REF_NULL
        && aligned + bytes <= slab->head_cap) {
        hart_pool_ref_t ref = slab->head_ref + aligned;
        slab->head_used = aligned + bytes;
        if (out_cap) *out_cap = bytes;
        return ref;
    }

    /* Slow path: close current head, open a new, larger slab. */
    uint32_t new_cap;
    if (slab->head_cap == 0) {
        new_cap = HART_SLAB_INITIAL;
    } else if (slab->head_cap < HART_SLAB_MAX) {
        new_cap = slab->head_cap * 2;
    } else {
        new_cap = HART_SLAB_MAX;
    }
    /* Ensure the first allocation fits (after header + 16B alignment). */
    while (new_cap < SLAB_HEADER_SIZE + bytes && new_cap < HART_SLAB_MAX)
        new_cap *= 2;
    if (SLAB_HEADER_SIZE + bytes > new_cap) return HART_POOL_REF_NULL;

    hart_pool_ref_t new_ref = slab_reserve(p, new_cap);
    if (new_ref == HART_POOL_REF_NULL) return HART_POOL_REF_NULL;

    /* Install header so free_slabs can walk back later. */
    write_slab_header(p, new_ref, slab->head_ref, new_cap);

    /* Chain: old head moves into chain_ref of the new head. */
    slab->chain_ref = slab->head_ref;
    slab->head_ref  = new_ref;
    slab->head_cap  = new_cap;
    slab->head_used = SLAB_HEADER_SIZE;

    /* Now bump for the requested allocation (guaranteed to fit). */
    aligned = (slab->head_used + 15u) & ~15u;
    hart_pool_ref_t ref = slab->head_ref + aligned;
    slab->head_used = aligned + bytes;
    if (out_cap) *out_cap = bytes;
    return ref;
}

/* =========================================================================
 * Slab release
 * ========================================================================= */

void hart_pool_free_slabs(hart_pool_t *p, hart_slab_t *slab) {
    if (!p || !slab) return;

    hart_pool_ref_t ref = slab->head_ref;
    while (ref != HART_POOL_REF_NULL) {
        slab_header_t *h = slab_hdr(p, ref);
        hart_pool_ref_t prev = h->chain_ref;
        uint32_t cap = h->cap;
        uint32_t cls = slab_class(cap);

        fl_push(&p->fl[cls], ref);
        p->free_bytes += cap;
        if (p->slabs_per_class[cls] > 0) p->slabs_per_class[cls]--;
        if (p->slab_count > 0) p->slab_count--;

        ref = prev;
    }
    memset(slab, 0, sizeof(*slab));
}

/* =========================================================================
 * Pointer resolution
 * ========================================================================= */

void *hart_pool_ptr(const hart_pool_t *p, hart_pool_ref_t ref) {
    if (!p || ref == HART_POOL_REF_NULL) return NULL;
    return p->base + ref;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

hart_pool_stats_t hart_pool_stats(const hart_pool_t *p) {
    hart_pool_stats_t st = {0};
    if (!p) return st;
    st.mapped     = p->mapped;
    st.used       = p->used;
    st.free_bytes = p->free_bytes;
    st.slab_count = p->slab_count;
    for (uint32_t i = 0; i < HART_SLAB_CLASSES; i++) {
        st.slabs_per_class[i] = p->slabs_per_class[i];
        st.free_per_class[i]  = p->fl[i].cnt;
    }
    return st;
}
