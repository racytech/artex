#define _GNU_SOURCE
#include "flat_store.h"
#include "flat_index.h"

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

#define FLAT_STORE_MAGIC       "FLST"
#define FLAT_STORE_VERSION     1
#define FLAT_STORE_HEADER_SIZE 4096  /* page-aligned header */

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
} flat_store_header_t;

/* =========================================================================
 * Internal Structure
 * ========================================================================= */

struct flat_store {
    flat_index_t  index;
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

static inline uint8_t *slot_ptr(const flat_store_t *s, uint32_t slot_id) {
    return s->base + FLAT_STORE_HEADER_SIZE + (size_t)slot_id * s->slot_size;
}

static bool ensure_mapped(flat_store_t *s, uint32_t needed_slots) {
    size_t needed = FLAT_STORE_HEADER_SIZE + (size_t)needed_slots * s->slot_size;
    if (needed <= s->mapped_size) return true;

    /* Grow to at least 2x or needed, whichever is larger */
    size_t new_size = s->mapped_size ? s->mapped_size * 2 :
                      FLAT_STORE_HEADER_SIZE + (size_t)INITIAL_MMAP_SLOTS * s->slot_size;
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

static void write_header(flat_store_t *s) {
    flat_store_header_t *hdr = (flat_store_header_t *)s->base;
    memcpy(hdr->magic, FLAT_STORE_MAGIC, 4);
    hdr->version     = FLAT_STORE_VERSION;
    hdr->key_size    = s->key_size;
    hdr->record_size = s->record_size;
    hdr->slot_count  = s->slot_count;
    hdr->live_count  = s->live_count;
}

static bool read_header(const uint8_t *base, flat_store_header_t *out) {
    memcpy(out, base, sizeof(*out));
    if (memcmp(out->magic, FLAT_STORE_MAGIC, 4) != 0) return false;
    if (out->version != FLAT_STORE_VERSION) return false;
    return true;
}

/* =========================================================================
 * Free List
 * ========================================================================= */

static bool free_list_push(flat_store_t *s, uint32_t slot_id) {
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

static inline uint32_t free_list_pop(flat_store_t *s) {
    return s->free_slots[--s->free_count];
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

flat_store_t *flat_store_create(const char *path, uint32_t key_size,
                               uint32_t record_size)
{
    flat_store_t *s = calloc(1, sizeof(flat_store_t));
    if (!s) return NULL;

    s->key_size    = key_size;
    s->record_size = record_size;
    s->slot_size   = 1 + key_size + record_size;

    if (!flat_index_init(&s->index, FLAT_INDEX_INITIAL_CAP)) {
        free(s);
        return NULL;
    }

    s->fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (s->fd < 0) {
        flat_index_destroy(&s->index);
        free(s);
        return NULL;
    }

    /* Initial mmap */
    size_t init_size = FLAT_STORE_HEADER_SIZE + (size_t)INITIAL_MMAP_SLOTS * s->slot_size;
    if (ftruncate(s->fd, init_size) != 0) {
        close(s->fd);
        flat_index_destroy(&s->index);
        free(s);
        return NULL;
    }

    s->base = mmap(NULL, init_size, PROT_READ | PROT_WRITE, MAP_SHARED, s->fd, 0);
    if (s->base == MAP_FAILED) {
        close(s->fd);
        flat_index_destroy(&s->index);
        free(s);
        return NULL;
    }
    s->mapped_size = init_size;

    write_header(s);
    return s;
}

flat_store_t *flat_store_open(const char *path) {
    int fd = open(path, O_RDWR);
    if (fd < 0) return NULL;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < FLAT_STORE_HEADER_SIZE) {
        close(fd);
        return NULL;
    }

    uint8_t *base = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    flat_store_header_t hdr;
    if (!read_header(base, &hdr)) {
        munmap(base, st.st_size);
        close(fd);
        return NULL;
    }

    flat_store_t *s = calloc(1, sizeof(flat_store_t));
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

    /* Init index with capacity hint from slot count */
    uint32_t idx_cap = s->slot_count > 1024 ? s->slot_count * 2 : FLAT_INDEX_INITIAL_CAP;
    if (!flat_index_init(&s->index, idx_cap)) {
        munmap(base, st.st_size);
        close(fd);
        free(s);
        return NULL;
    }

    /* Scan all slots: rebuild index + free list */
    for (uint32_t i = 0; i < s->slot_count; i++) {
        uint8_t *slot = slot_ptr(s, i);
        if (slot[0] == SLOT_FLAG_OCCUPIED) {
            const uint8_t *key = slot + 1;
            if (flat_index_needs_grow(&s->index)) {
                flat_index_grow(&s->index, s->index.capacity * 2);
                /* Re-insert all previous entries */
                for (uint32_t j = 0; j < i; j++) {
                    uint8_t *prev = slot_ptr(s, j);
                    if (prev[0] == SLOT_FLAG_OCCUPIED)
                        flat_index_put(&s->index, prev + 1, j);
                }
            }
            flat_index_put(&s->index, key, i);
            s->live_count++;
        } else {
            free_list_push(s, i);
        }
    }

    return s;
}

void flat_store_destroy(flat_store_t *s) {
    if (!s) return;
    write_header(s);
    flat_index_destroy(&s->index);
    if (s->base && s->base != MAP_FAILED)
        munmap(s->base, s->mapped_size);
    if (s->fd >= 0) close(s->fd);
    free(s->free_slots);
    free(s);
}

/* =========================================================================
 * Operations
 * ========================================================================= */

bool flat_store_put(flat_store_t *s, const uint8_t *key,
                    const void *record)
{
    /* Check if key already exists */
    const uint32_t *existing = flat_index_get(&s->index, key);
    if (existing) {
        /* Update: overwrite record data in existing slot */
        uint32_t slot_id = *existing - 1;
        uint8_t *slot = slot_ptr(s, slot_id);
        memcpy(slot + 1 + s->key_size, record, s->record_size);
        return true;
    }

    /* Grow index if needed */
    if (flat_index_needs_grow(&s->index)) {
        uint32_t new_cap = s->index.capacity * 2;
        if (!flat_index_grow(&s->index, new_cap))
            return false;
        /* Re-insert all live entries from data file */
        for (uint32_t i = 0; i < s->slot_count; i++) {
            uint8_t *sl = slot_ptr(s, i);
            if (sl[0] == SLOT_FLAG_OCCUPIED)
                flat_index_put(&s->index, sl + 1, i);
        }
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

    if (!flat_index_put(&s->index, key, slot_id))
        return false;

    s->live_count++;
    return true;
}

bool flat_store_get(const flat_store_t *s, const uint8_t *key,
                    void *out)
{
    const uint32_t *val = flat_index_get(&s->index, key);
    if (!val) return false;

    uint32_t slot_id = *val - 1;
    const uint8_t *slot = slot_ptr(s, slot_id);
    memcpy(out, slot + 1 + s->key_size, s->record_size);
    return true;
}

bool flat_store_delete(flat_store_t *s, const uint8_t *key) {
    const uint32_t *val = flat_index_get(&s->index, key);
    if (!val) return false;

    uint32_t slot_id = *val - 1;

    /* Mark slot free */
    uint8_t *slot = slot_ptr(s, slot_id);
    slot[0] = SLOT_FLAG_FREE;

    flat_index_delete(&s->index, key);
    free_list_push(s, slot_id);
    s->live_count--;
    return true;
}

bool flat_store_contains(const flat_store_t *s, const uint8_t *key) {
    return flat_index_contains(&s->index, key);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint32_t flat_store_count(const flat_store_t *s) {
    return s ? s->live_count : 0;
}

uint32_t flat_store_slot_count(const flat_store_t *s) {
    return s ? s->slot_count : 0;
}

uint32_t flat_store_key_size(const flat_store_t *s) {
    return s ? s->key_size : 0;
}

uint32_t flat_store_record_size(const flat_store_t *s) {
    return s ? s->record_size : 0;
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void flat_store_sync(flat_store_t *s) {
    if (!s) return;
    write_header(s);
    /* No msync — OS page cache handles writeback */
}
