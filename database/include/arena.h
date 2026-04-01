#ifndef ARENA_H
#define ARENA_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

/**
 * Arena — bump allocator with checkpoint reset.
 *
 * Reserves a large virtual address space (MAP_NORESERVE) and allocates
 * linearly via bump pointer. Physical pages are demand-paged.
 *
 * reset() releases all physical pages (MADV_DONTNEED) and resets the
 * bump pointer to zero. All previously allocated pointers become invalid.
 *
 * Usage: per-checkpoint-window scratch space for per-account storage arts.
 * At eviction, flush dirty data to disk, then arena_reset().
 */

typedef struct {
    uint8_t *base;        /* mmap'd region */
    size_t   reserved;    /* total virtual reservation */
    size_t   offset;      /* bump pointer (next allocation) */
    size_t   peak;        /* high-water mark (for stats) */
} arena_t;

/** Create arena with given virtual reservation (bytes). */
bool arena_init(arena_t *a, size_t reserve_bytes);

/** Release all memory. */
void arena_destroy(arena_t *a);

/**
 * Allocate `size` bytes (8-byte aligned).
 * Returns pointer into arena, or NULL if exhausted.
 * The returned pointer is valid until arena_reset() or arena_destroy().
 */
void *arena_alloc(arena_t *a, size_t size);

/**
 * Reset the arena — releases physical pages and resets bump pointer.
 * All previously allocated pointers become invalid.
 * The virtual reservation is retained.
 */
void arena_reset(arena_t *a);

/** Current bytes allocated (bump pointer position). */
static inline size_t arena_used(const arena_t *a) { return a ? a->offset : 0; }

/** Peak bytes ever allocated (high-water mark). */
static inline size_t arena_peak(const arena_t *a) { return a ? a->peak : 0; }

#endif /* ARENA_H */
