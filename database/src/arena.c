#include "arena.h"
#include <sys/mman.h>
#include <string.h>

#ifndef MAP_NORESERVE
#define MAP_NORESERVE 0
#endif

#define ARENA_ALIGN 8

bool arena_init(arena_t *a, size_t reserve_bytes) {
    if (!a || reserve_bytes == 0) return false;

    void *mem = mmap(NULL, reserve_bytes,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                     -1, 0);
    if (mem == MAP_FAILED) return false;

    a->base = mem;
    a->reserved = reserve_bytes;
    a->offset = 0;
    a->peak = 0;
    return true;
}

void arena_destroy(arena_t *a) {
    if (!a || !a->base) return;
    munmap(a->base, a->reserved);
    memset(a, 0, sizeof(*a));
}

void *arena_alloc(arena_t *a, size_t size) {
    if (!a || !a->base || size == 0) return NULL;

    /* Align up to 8 bytes */
    size = (size + (ARENA_ALIGN - 1)) & ~(ARENA_ALIGN - 1);

    if (a->offset + size > a->reserved) return NULL;

    void *ptr = a->base + a->offset;
    a->offset += size;
    if (a->offset > a->peak) a->peak = a->offset;
    return ptr;
}

void arena_reset(arena_t *a) {
    if (!a || !a->base) return;

    /* Release physical pages back to OS.
     * The virtual reservation remains — no munmap/mmap needed. */
    if (a->offset > 0)
        madvise(a->base, a->offset, MADV_DONTNEED);

    a->offset = 0;
}
