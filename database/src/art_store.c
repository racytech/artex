#define _GNU_SOURCE
#include "art_store.h"
#include "compact_art.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define ART_STORE_MAGIC       "ARTS"
#define ART_STORE_VERSION     1
#define ART_STORE_HEADER_SIZE 4096  /* page-aligned header */

#define SLOT_FLAG_FREE        0x00
#define SLOT_FLAG_OCCUPIED    0x01

#define FREE_LIST_INITIAL_CAP 1024
#define INITIAL_MMAP_SLOTS    65536  /* initial capacity before first remap */

/* =========================================================================
 * On-Disk Header (within first 4096-byte page)
 * ========================================================================= */

typedef struct {
    uint8_t  magic[4];
    uint32_t version;
    uint32_t key_size;
    uint32_t record_size;
    uint32_t slot_count;
    uint32_t live_count;
    uint8_t  reserved[40];
} art_store_header_t;

/* =========================================================================
 * Internal Structure
 * ========================================================================= */

struct art_store {
    compact_art_t index;
    int           fd;
    uint32_t      key_size;
    uint32_t      record_size;
    uint32_t      slot_size;    /* 1 + key_size + record_size */
    uint32_t      slot_count;   /* total allocated slots */
    uint32_t      live_count;   /* occupied slots */

    uint8_t      *base;         /* mmap base */
    size_t        mapped_size;  /* current mmap size */

    uint32_t     *free_slots;   /* recycled slot IDs (LIFO stack) */
    uint32_t      free_count;
    uint32_t      free_cap;
};

/* =========================================================================
 * Mmap helpers
 * ========================================================================= */

static inline uint8_t *slot_ptr(const art_store_t *s, uint32_t slot_id) {
    return s->base + ART_STORE_HEADER_SIZE + (size_t)slot_id * s->slot_size;
}

static bool ensure_mapped(art_store_t *s, uint32_t needed_slots) {
    size_t needed = ART_STORE_HEADER_SIZE + (size_t)needed_slots * s->slot_size;
    if (needed <= s->mapped_size) return true;

    /* Grow to at least 2x or needed, whichever is larger */
    size_t new_size = s->mapped_size ? s->mapped_size * 2 :
                      ART_STORE_HEADER_SIZE + (size_t)INITIAL_MMAP_SLOTS * s->slot_size;
    while (new_size < needed) new_size *= 2;

    if (ftruncate(s->fd, new_size) != 0) return false;

    uint8_t *new_base = mremap(s->base, s->mapped_size, new_size, MREMAP_MAYMOVE);
    if (new_base == MAP_FAILED) return false;

    s->base = new_base;
    s->mapped_size = new_size;
    return true;
}

/* =========================================================================
 * Header I/O (direct mmap access)
 * ========================================================================= */

static void write_header(art_store_t *s) {
    art_store_header_t *hdr = (art_store_header_t *)s->base;
    memcpy(hdr->magic, ART_STORE_MAGIC, 4);
    hdr->version     = ART_STORE_VERSION;
    hdr->key_size    = s->key_size;
    hdr->record_size = s->record_size;
    hdr->slot_count  = s->slot_count;
    hdr->live_count  = s->live_count;
}

static bool read_header(const uint8_t *base, art_store_header_t *out) {
    memcpy(out, base, sizeof(*out));
    if (memcmp(out->magic, ART_STORE_MAGIC, 4) != 0) return false;
    if (out->version != ART_STORE_VERSION) return false;
    return true;
}

/* =========================================================================
 * Free List
 * ========================================================================= */

static bool free_list_push(art_store_t *s, uint32_t slot_id) {
    if (s->free_count >= s->free_cap) {
        uint32_t new_cap = s->free_cap ? s->free_cap * 2 : FREE_LIST_INITIAL_CAP;
        uint32_t *tmp = realloc(s->free_slots, new_cap * sizeof(uint32_t));
        if (!tmp) return false;
        s->free_slots = tmp;
        s->free_cap = new_cap;
    }
    s->free_slots[s->free_count++] = slot_id;
    return true;
}

static inline uint32_t free_list_pop(art_store_t *s) {
    return s->free_slots[--s->free_count];
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

art_store_t *art_store_create(const char *path, uint32_t key_size,
                               uint32_t record_size)
{
    art_store_t *s = calloc(1, sizeof(art_store_t));
    if (!s) return NULL;

    s->key_size    = key_size;
    s->record_size = record_size;
    s->slot_size   = 1 + key_size + record_size;

    if (!compact_art_init(&s->index, key_size, sizeof(uint32_t))) {
        free(s);
        return NULL;
    }

    s->fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (s->fd < 0) {
        compact_art_destroy(&s->index);
        free(s);
        return NULL;
    }

    /* Initial mmap */
    size_t init_size = ART_STORE_HEADER_SIZE + (size_t)INITIAL_MMAP_SLOTS * s->slot_size;
    if (ftruncate(s->fd, init_size) != 0) {
        close(s->fd);
        compact_art_destroy(&s->index);
        free(s);
        return NULL;
    }

    s->base = mmap(NULL, init_size, PROT_READ | PROT_WRITE, MAP_SHARED, s->fd, 0);
    if (s->base == MAP_FAILED) {
        close(s->fd);
        compact_art_destroy(&s->index);
        free(s);
        return NULL;
    }
    s->mapped_size = init_size;

    write_header(s);
    return s;
}

art_store_t *art_store_open(const char *path) {
    int fd = open(path, O_RDWR);
    if (fd < 0) return NULL;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < ART_STORE_HEADER_SIZE) {
        close(fd);
        return NULL;
    }

    uint8_t *base = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    art_store_header_t hdr;
    if (!read_header(base, &hdr)) {
        munmap(base, st.st_size);
        close(fd);
        return NULL;
    }

    art_store_t *s = calloc(1, sizeof(art_store_t));
    if (!s) {
        munmap(base, st.st_size);
        close(fd);
        return NULL;
    }

    s->fd          = fd;
    s->base        = base;
    s->mapped_size = st.st_size;
    s->key_size    = hdr.key_size;
    s->record_size = hdr.record_size;
    s->slot_size   = 1 + hdr.key_size + hdr.record_size;
    s->slot_count  = hdr.slot_count;
    s->live_count  = 0;

    if (!compact_art_init(&s->index, hdr.key_size, sizeof(uint32_t))) {
        munmap(base, st.st_size);
        close(fd);
        free(s);
        return NULL;
    }

    /* Scan all slots: rebuild ART index + free list */
    for (uint32_t i = 0; i < s->slot_count; i++) {
        uint8_t *slot = slot_ptr(s, i);
        if (slot[0] == SLOT_FLAG_OCCUPIED) {
            const uint8_t *key = slot + 1;
            compact_art_insert(&s->index, key, &i);
            s->live_count++;
        } else {
            free_list_push(s, i);
        }
    }

    return s;
}

void art_store_destroy(art_store_t *s) {
    if (!s) return;
    write_header(s);
    compact_art_destroy(&s->index);
    if (s->base && s->base != MAP_FAILED)
        munmap(s->base, s->mapped_size);
    if (s->fd >= 0) close(s->fd);
    free(s->free_slots);
    free(s);
}

/* =========================================================================
 * Operations
 * ========================================================================= */

bool art_store_put(art_store_t *s, const uint8_t *key,
                    const void *record)
{
    /* Check if key already exists */
    const void *existing = compact_art_get(&s->index, key);
    if (existing) {
        /* Update: overwrite record data in existing slot */
        uint32_t slot_id;
        memcpy(&slot_id, existing, sizeof(uint32_t));
        uint8_t *slot = slot_ptr(s, slot_id);
        memcpy(slot + 1 + s->key_size, record, s->record_size);
        return true;
    }

    /* New key: allocate a slot */
    uint32_t slot_id;
    if (s->free_count > 0) {
        slot_id = free_list_pop(s);
    } else {
        slot_id = s->slot_count++;
        if (!ensure_mapped(s, s->slot_count))
            return false;
    }

    /* Write slot: [0x01 | key | record] */
    uint8_t *slot = slot_ptr(s, slot_id);
    slot[0] = SLOT_FLAG_OCCUPIED;
    memcpy(slot + 1, key, s->key_size);
    memcpy(slot + 1 + s->key_size, record, s->record_size);

    if (!compact_art_insert(&s->index, key, &slot_id))
        return false;

    s->live_count++;
    return true;
}

bool art_store_get(const art_store_t *s, const uint8_t *key,
                    void *out)
{
    const void *val = compact_art_get(&s->index, key);
    if (!val) return false;

    uint32_t slot_id;
    memcpy(&slot_id, val, sizeof(uint32_t));
    const uint8_t *slot = slot_ptr(s, slot_id);
    memcpy(out, slot + 1 + s->key_size, s->record_size);
    return true;
}

bool art_store_delete(art_store_t *s, const uint8_t *key) {
    const void *val = compact_art_get(&s->index, key);
    if (!val) return false;

    uint32_t slot_id;
    memcpy(&slot_id, val, sizeof(uint32_t));

    /* Mark slot free */
    uint8_t *slot = slot_ptr(s, slot_id);
    slot[0] = SLOT_FLAG_FREE;

    compact_art_delete(&s->index, key);
    free_list_push(s, slot_id);
    s->live_count--;
    return true;
}

bool art_store_contains(const art_store_t *s, const uint8_t *key) {
    return compact_art_contains(&s->index, key);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint32_t art_store_count(const art_store_t *s) {
    return s ? s->live_count : 0;
}

uint32_t art_store_slot_count(const art_store_t *s) {
    return s ? s->slot_count : 0;
}

uint32_t art_store_key_size(const art_store_t *s) {
    return s ? s->key_size : 0;
}

uint32_t art_store_record_size(const art_store_t *s) {
    return s ? s->record_size : 0;
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void art_store_sync(art_store_t *s) {
    if (!s) return;
    write_header(s);
    /* No msync — OS page cache handles writeback */
}
