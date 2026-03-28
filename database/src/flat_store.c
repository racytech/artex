#define _GNU_SOURCE
#include "flat_store.h"
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
 * Key Fetch Callback (compact leaf mode)
 * ========================================================================= */

static bool flat_store_key_fetch(const void *value, uint8_t *key_out, void *user_data) {
    flat_store_t *s = (flat_store_t *)user_data;
    uint32_t slot_id;
    memcpy(&slot_id, value, sizeof(uint32_t));
    const uint8_t *slot = slot_ptr(s, slot_id);
    /* slot layout: [1B flag][key_size B key][record_size B data] */
    memcpy(key_out, slot + 1, s->key_size);
    return true;
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

    if (!compact_art_init(&s->index, key_size, sizeof(uint32_t),
                          true, flat_store_key_fetch, s)) {
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
    size_t init_size = FLAT_STORE_HEADER_SIZE + (size_t)INITIAL_MMAP_SLOTS * s->slot_size;
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

/* Compact the data file: rewrite with only live entries, eliminating gaps.
 * Returns true if compaction was performed. */
static bool compact_file(const char *path) {
    int fd = open(path, O_RDWR);
    if (fd < 0) return false;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < FLAT_STORE_HEADER_SIZE) {
        close(fd);
        return false;
    }

    uint8_t *base = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (base == MAP_FAILED) { close(fd); return false; }

    flat_store_header_t hdr;
    if (!read_header(base, &hdr)) {
        munmap(base, st.st_size); close(fd); return false;
    }

    uint32_t slot_size = 1 + hdr.key_size + hdr.record_size;
    uint32_t live = 0, total = hdr.slot_count;
    for (uint32_t i = 0; i < total; i++) {
        if (base[FLAT_STORE_HEADER_SIZE + (size_t)i * slot_size] == SLOT_FLAG_OCCUPIED)
            live++;
    }

    /* Only compact if > 50% waste */
    if (live == 0 || total < 2 * live) {
        munmap(base, st.st_size); close(fd);
        return false;
    }

    fprintf(stderr, "flat_store: compacting %s (%u live / %u total slots, %.1f%% waste)\n",
            path, live, total, 100.0 * (total - live) / total);

    /* Create temp file */
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s.compact", path);
    int tmp_fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (tmp_fd < 0) { munmap(base, st.st_size); close(fd); return false; }

    /* Write header + live slots sequentially */
    size_t new_size = FLAT_STORE_HEADER_SIZE + (size_t)live * slot_size;
    if (ftruncate(tmp_fd, new_size) != 0) {
        close(tmp_fd); unlink(tmp);
        munmap(base, st.st_size); close(fd);
        return false;
    }

    uint8_t *new_base = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                              MAP_SHARED, tmp_fd, 0);
    if (new_base == MAP_FAILED) {
        close(tmp_fd); unlink(tmp);
        munmap(base, st.st_size); close(fd);
        return false;
    }

    /* Copy header with updated counts */
    flat_store_header_t new_hdr = hdr;
    new_hdr.slot_count = live;
    new_hdr.live_count = live;
    memcpy(new_base, &new_hdr, sizeof(new_hdr));

    /* Copy live slots sequentially */
    uint32_t dst = 0;
    for (uint32_t i = 0; i < total; i++) {
        const uint8_t *slot = base + FLAT_STORE_HEADER_SIZE + (size_t)i * slot_size;
        if (slot[0] == SLOT_FLAG_OCCUPIED) {
            memcpy(new_base + FLAT_STORE_HEADER_SIZE + (size_t)dst * slot_size,
                   slot, slot_size);
            dst++;
        }
    }

    munmap(new_base, new_size);
    munmap(base, st.st_size);
    close(tmp_fd);
    close(fd);

    /* Swap files */
    rename(tmp, path);

    fprintf(stderr, "flat_store: compacted %u → %u slots (%.1f MB → %.1f MB)\n",
            total, live,
            (double)(FLAT_STORE_HEADER_SIZE + (size_t)total * slot_size) / (1024*1024),
            (double)new_size / (1024*1024));
    return true;
}

flat_store_t *flat_store_open(const char *path) {
    /* Auto-compact if > 50% waste */
    compact_file(path);

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

    if (!compact_art_init(&s->index, hdr.key_size, sizeof(uint32_t),
                          true, flat_store_key_fetch, s)) {
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
            compact_art_insert(&s->index, key, &i);
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

bool flat_store_put(flat_store_t *s, const uint8_t *key,
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

bool flat_store_get(const flat_store_t *s, const uint8_t *key,
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

bool flat_store_delete(flat_store_t *s, const uint8_t *key) {
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

bool flat_store_contains(const flat_store_t *s, const uint8_t *key) {
    return compact_art_contains(&s->index, key);
}

/* =========================================================================
 * Batch Operations
 * ========================================================================= */

typedef struct {
    uint32_t idx;       /* original index in keys/records array */
    uint32_t slot_id;   /* existing slot_id (UINT32_MAX = new entry) */
} batch_sort_t;

static int cmp_batch_slot(const void *a, const void *b) {
    const batch_sort_t *ea = (const batch_sort_t *)a;
    const batch_sort_t *eb = (const batch_sort_t *)b;
    if (ea->slot_id < eb->slot_id) return -1;
    if (ea->slot_id > eb->slot_id) return  1;
    return 0;
}

bool flat_store_batch_put(flat_store_t *s, const uint8_t *keys,
                           const void *records, uint32_t count) {
    if (!s || !keys || !records || count == 0) return false;

    /* Pass 1: look up slot_ids from compact_art (in-memory, fast) */
    batch_sort_t *entries = malloc(count * sizeof(batch_sort_t));
    if (!entries) return false;

    for (uint32_t i = 0; i < count; i++) {
        entries[i].idx = i;
        const void *val = compact_art_get(&s->index,
                                           keys + (size_t)i * s->key_size);
        if (val) {
            uint32_t sid;
            memcpy(&sid, val, sizeof(uint32_t));
            entries[i].slot_id = sid;
        } else {
            entries[i].slot_id = UINT32_MAX; /* new entry — append at end */
        }
    }

    /* Sort by slot_id — clusters writes by page location */
    qsort(entries, count, sizeof(batch_sort_t), cmp_batch_slot);

    /* Pass 2: write in sorted order */
    for (uint32_t i = 0; i < count; i++) {
        uint32_t orig = entries[i].idx;
        const uint8_t *key = keys + (size_t)orig * s->key_size;
        const uint8_t *rec = (const uint8_t *)records + (size_t)orig * s->record_size;

        if (entries[i].slot_id != UINT32_MAX) {
            /* Update existing slot — just overwrite record */
            uint8_t *slot = slot_ptr(s, entries[i].slot_id);
            memcpy(slot + 1 + s->key_size, rec, s->record_size);
        } else {
            /* New entry — allocate and write full slot */
            uint32_t slot_id;
            if (s->free_count > 0) {
                slot_id = free_list_pop(s);
            } else {
                slot_id = s->slot_count++;
                if (!ensure_mapped(s, s->slot_count)) {
                    free(entries);
                    return false;
                }
            }
            uint8_t *slot = slot_ptr(s, slot_id);
            slot[0] = SLOT_FLAG_OCCUPIED;
            memcpy(slot + 1, key, s->key_size);
            memcpy(slot + 1 + s->key_size, rec, s->record_size);
            compact_art_insert(&s->index, key, &slot_id);
            s->live_count++;
        }
    }

    free(entries);
    return true;
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
