#include "art_store.h"
#include "compact_art.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define ART_STORE_MAGIC       "ARTS"
#define ART_STORE_VERSION     1
#define ART_STORE_HEADER_SIZE 64

#define SLOT_FLAG_FREE        0x00
#define SLOT_FLAG_OCCUPIED    0x01

#define FREE_LIST_INITIAL_CAP 1024
#define STACK_BUF_SIZE        4096

/* =========================================================================
 * On-Disk Header (64 bytes)
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

    uint32_t     *free_slots;   /* recycled slot IDs (LIFO stack) */
    uint32_t      free_count;
    uint32_t      free_cap;
};

/* =========================================================================
 * Header I/O
 * ========================================================================= */

static bool write_header(art_store_t *s) {
    art_store_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, ART_STORE_MAGIC, 4);
    hdr.version     = ART_STORE_VERSION;
    hdr.key_size    = s->key_size;
    hdr.record_size = s->record_size;
    hdr.slot_count  = s->slot_count;
    hdr.live_count  = s->live_count;
    return pwrite(s->fd, &hdr, sizeof(hdr), 0) == sizeof(hdr);
}

static bool read_header(int fd, art_store_header_t *hdr) {
    if (pread(fd, hdr, sizeof(*hdr), 0) != sizeof(*hdr))
        return false;
    if (memcmp(hdr->magic, ART_STORE_MAGIC, 4) != 0)
        return false;
    if (hdr->version != ART_STORE_VERSION)
        return false;
    return true;
}

/* =========================================================================
 * Slot Offset
 * ========================================================================= */

static inline off_t slot_offset(const art_store_t *s, uint32_t slot_id) {
    return (off_t)ART_STORE_HEADER_SIZE + (off_t)slot_id * s->slot_size;
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

    if (!write_header(s)) {
        close(s->fd);
        compact_art_destroy(&s->index);
        free(s);
        return NULL;
    }

    return s;
}

art_store_t *art_store_open(const char *path) {
    int fd = open(path, O_RDWR);
    if (fd < 0) return NULL;

    art_store_header_t hdr;
    if (!read_header(fd, &hdr)) {
        close(fd);
        return NULL;
    }

    art_store_t *s = calloc(1, sizeof(art_store_t));
    if (!s) { close(fd); return NULL; }

    s->fd          = fd;
    s->key_size    = hdr.key_size;
    s->record_size = hdr.record_size;
    s->slot_size   = 1 + hdr.key_size + hdr.record_size;
    s->slot_count  = hdr.slot_count;
    s->live_count  = 0;

    if (!compact_art_init(&s->index, hdr.key_size, sizeof(uint32_t))) {
        close(fd);
        free(s);
        return NULL;
    }

    /* Scan all slots: rebuild ART index + free list */
    if (s->slot_count > 0) {
        /* Batch read for efficiency: read up to 256 slots at a time */
        uint32_t batch = 256;
        uint32_t buf_size = batch * s->slot_size;
        uint8_t *buf = malloc(buf_size);
        if (!buf) {
            compact_art_destroy(&s->index);
            close(fd);
            free(s);
            return NULL;
        }

        for (uint32_t base = 0; base < s->slot_count; base += batch) {
            uint32_t n = s->slot_count - base;
            if (n > batch) n = batch;

            off_t off = slot_offset(s, base);
            ssize_t bytes_needed = (ssize_t)n * s->slot_size;
            ssize_t r = pread(fd, buf, bytes_needed, off);
            if (r != bytes_needed) {
                s->slot_count = base;
                break;
            }

            for (uint32_t j = 0; j < n; j++) {
                uint8_t *slot = buf + j * s->slot_size;
                uint32_t slot_id = base + j;

                if (slot[0] == SLOT_FLAG_OCCUPIED) {
                    const uint8_t *key = slot + 1;
                    compact_art_insert(&s->index, key, &slot_id);
                    s->live_count++;
                } else {
                    free_list_push(s, slot_id);
                }
            }
        }

        free(buf);
    }

    return s;
}

void art_store_destroy(art_store_t *s) {
    if (!s) return;
    compact_art_destroy(&s->index);
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

        off_t data_off = slot_offset(s, slot_id) + 1 + s->key_size;
        return pwrite(s->fd, record, s->record_size, data_off) ==
               (ssize_t)s->record_size;
    }

    /* New key: allocate a slot */
    uint32_t slot_id;
    if (s->free_count > 0) {
        slot_id = free_list_pop(s);
    } else {
        slot_id = s->slot_count++;
    }

    /* Build slot: [0x01 | key | record] */
    uint8_t stack_buf[STACK_BUF_SIZE];
    uint8_t *buf;
    bool heap = s->slot_size > STACK_BUF_SIZE;

    if (heap) {
        buf = malloc(s->slot_size);
        if (!buf) return false;
    } else {
        buf = stack_buf;
    }

    buf[0] = SLOT_FLAG_OCCUPIED;
    memcpy(buf + 1, key, s->key_size);
    memcpy(buf + 1 + s->key_size, record, s->record_size);

    off_t off = slot_offset(s, slot_id);
    ssize_t w = pwrite(s->fd, buf, s->slot_size, off);

    if (heap) free(buf);
    if (w != (ssize_t)s->slot_size) return false;

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

    off_t data_off = slot_offset(s, slot_id) + 1 + s->key_size;
    return pread(s->fd, out, s->record_size, data_off) ==
           (ssize_t)s->record_size;
}

bool art_store_delete(art_store_t *s, const uint8_t *key) {
    const void *val = compact_art_get(&s->index, key);
    if (!val) return false;

    uint32_t slot_id;
    memcpy(&slot_id, val, sizeof(uint32_t));

    /* Mark slot free on disk */
    uint8_t flag = SLOT_FLAG_FREE;
    if (pwrite(s->fd, &flag, 1, slot_offset(s, slot_id)) != 1)
        return false;

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
    fsync(s->fd);
}
