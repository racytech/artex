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
#define FLAT_STORE_VERSION     2
#define FLAT_STORE_HEADER_SIZE 4096  /* page-aligned header */

#define SLOT_HEADER_SIZE       4     /* packed uint32_t per slot */
#define MAX_SIZE_CLASSES       8

#define INITIAL_DATA_SIZE      (4ULL * 1024 * 1024) /* 4 MB initial data region */

/* Maximum number of free offsets stored in header per class */
#define MAX_HDR_FREE_PER_CLASS 200

/* =========================================================================
 * Slot Header
 * ========================================================================= */

/* bits 0-2: class_idx (0-7), bits 3-13: data_len (0-2047), bits 14-31: reserved */
static inline uint32_t slot_header_pack(uint8_t class_idx, uint16_t data_len) {
    return ((uint32_t)class_idx & 0x7) | (((uint32_t)data_len & 0x7FF) << 3);
}

static inline void slot_header_unpack(uint32_t hdr, uint8_t *class_idx,
                                       uint16_t *data_len) {
    *class_idx = hdr & 0x7;
    *data_len = (hdr >> 3) & 0x7FF;
}

/* =========================================================================
 * On-Disk Header (within first 4096-byte page)
 * ========================================================================= */

typedef struct {
    uint8_t  magic[4];
    uint32_t version;
    uint32_t key_size;
    uint32_t max_record_size;
    uint32_t num_classes;
    uint32_t live_count;
    uint64_t data_size;          /* total bytes used in data region */
    uint32_t slot_sizes[MAX_SIZE_CLASSES];   /* slot size per class */
    uint32_t free_counts[MAX_SIZE_CLASSES];  /* free list count per class */
    /* Remaining space: packed free offsets (uint64_t each) */
} flat_store_header_t;

/* =========================================================================
 * Free List (per size class)
 * ========================================================================= */

typedef struct {
    uint64_t *offsets;
    uint32_t  count;
    uint32_t  cap;
} free_list_t;

static bool free_list_push(free_list_t *fl, uint64_t offset) {
    if (fl->count >= fl->cap) {
        uint32_t new_cap = fl->cap ? fl->cap * 2 : 1024;
        uint64_t *tmp = realloc(fl->offsets, new_cap * sizeof(uint64_t));
        if (!tmp) return false;
        fl->offsets = tmp;
        fl->cap = new_cap;
    }
    fl->offsets[fl->count++] = offset;
    return true;
}

static inline uint64_t free_list_pop(free_list_t *fl) {
    return fl->offsets[--fl->count];
}

static void free_list_destroy(free_list_t *fl) {
    free(fl->offsets);
    fl->offsets = NULL;
    fl->count = 0;
    fl->cap = 0;
}

/* =========================================================================
 * Internal Structure
 * ========================================================================= */

struct flat_store {
    compact_art_t index;
    int           fd;
    uint32_t      key_size;
    uint32_t      max_record_size;
    uint8_t       num_classes;
    uint32_t      slot_sizes[MAX_SIZE_CLASSES]; /* total slot size per class */
    uint32_t      live_count;

    uint8_t      *base;         /* mmap base */
    size_t        mapped_size;  /* current mmap size */
    uint64_t      data_size;    /* bytes used in data region (append offset) */

    free_list_t   free_lists[MAX_SIZE_CLASSES];
};

/* =========================================================================
 * Size Class Computation
 *
 * Given key_size and max_record_size, compute slot size classes.
 * Minimum slot = SLOT_HEADER_SIZE + key_size + small_record.
 * Classes are roughly geometric up to the max slot size.
 * ========================================================================= */

static void compute_size_classes(uint32_t key_size, uint32_t max_record_size,
                                  uint32_t *slot_sizes, uint8_t *num_classes) {
    uint32_t overhead = SLOT_HEADER_SIZE + key_size;
    uint32_t max_slot = overhead + max_record_size;

    /* Round up to multiple of 16 for alignment */
    #define ALIGN16(x) (((x) + 15) & ~15U)

    if (max_record_size <= 16) {
        /* Single class: everything fits in one slot */
        slot_sizes[0] = ALIGN16(max_slot);
        *num_classes = 1;
        return;
    }

    /* Build classes: small, medium, large (+ extra if max_record_size > 128) */
    uint32_t n = 0;

    /* Class 0: overhead + 12 bytes record capacity (empty EOAs, small values) */
    uint32_t c0_rec = 12;
    if (c0_rec >= max_record_size) c0_rec = max_record_size;
    slot_sizes[n++] = ALIGN16(overhead + c0_rec);

    if (c0_rec < max_record_size) {
        /* Class 1: overhead + 44 bytes record capacity (funded EOAs, most storage) */
        uint32_t c1_rec = 44;
        if (c1_rec >= max_record_size) c1_rec = max_record_size;
        slot_sizes[n++] = ALIGN16(overhead + c1_rec);

        if (c1_rec < max_record_size) {
            /* Class 2: full max (contracts, full accounts) */
            slot_sizes[n++] = ALIGN16(max_slot);
        }
    }

    *num_classes = (uint8_t)n;
    #undef ALIGN16
}

/* Return the index of the smallest class whose slot_size fits the needed bytes */
static inline int class_for_record(const flat_store_t *s, uint32_t record_len) {
    uint32_t needed = SLOT_HEADER_SIZE + s->key_size + record_len;
    for (int i = 0; i < s->num_classes; i++) {
        if (needed <= s->slot_sizes[i]) return i;
    }
    return s->num_classes - 1; /* largest class */
}

/* Record capacity of a given class */
static inline uint32_t class_record_capacity(const flat_store_t *s, int class_idx) {
    return s->slot_sizes[class_idx] - SLOT_HEADER_SIZE - s->key_size;
}

/* =========================================================================
 * Mmap helpers
 * ========================================================================= */

static inline uint8_t *data_ptr(const flat_store_t *s, uint64_t offset) {
    return s->base + FLAT_STORE_HEADER_SIZE + offset;
}

static bool ensure_mapped(flat_store_t *s, uint64_t needed_data_end) {
    size_t needed = FLAT_STORE_HEADER_SIZE + needed_data_end;
    if (needed <= s->mapped_size) return true;

    /* Grow to at least 2x or needed, whichever is larger */
    size_t new_size = s->mapped_size ? s->mapped_size * 2 :
                      FLAT_STORE_HEADER_SIZE + INITIAL_DATA_SIZE;
    while (new_size < needed) new_size *= 2;

    if (ftruncate(s->fd, new_size) != 0) return false;

    uint8_t *new_base = mremap(s->base, s->mapped_size, new_size, MREMAP_MAYMOVE);
    if (new_base == MAP_FAILED) return false;

    s->base = new_base;
    s->mapped_size = new_size;
    return true;
}

/* =========================================================================
 * Header I/O
 * ========================================================================= */

static void write_header(flat_store_t *s) {
    flat_store_header_t *hdr = (flat_store_header_t *)s->base;
    memcpy(hdr->magic, FLAT_STORE_MAGIC, 4);
    hdr->version         = FLAT_STORE_VERSION;
    hdr->key_size        = s->key_size;
    hdr->max_record_size = s->max_record_size;
    hdr->num_classes     = s->num_classes;
    hdr->live_count      = s->live_count;
    hdr->data_size       = s->data_size;
    memcpy(hdr->slot_sizes, s->slot_sizes, sizeof(hdr->slot_sizes));

    /* Write free list counts */
    for (int i = 0; i < MAX_SIZE_CLASSES; i++)
        hdr->free_counts[i] = (i < s->num_classes) ? s->free_lists[i].count : 0;

    /* Pack free offsets into remaining header space */
    size_t hdr_data_start = sizeof(flat_store_header_t);
    size_t hdr_space = FLAT_STORE_HEADER_SIZE - hdr_data_start;
    uint64_t *dst = (uint64_t *)(s->base + hdr_data_start);
    size_t pos = 0;
    size_t max_offsets = hdr_space / sizeof(uint64_t);

    for (int i = 0; i < s->num_classes; i++) {
        uint32_t to_write = s->free_lists[i].count;
        if (to_write > MAX_HDR_FREE_PER_CLASS) to_write = MAX_HDR_FREE_PER_CLASS;
        if (pos + to_write > max_offsets) to_write = (uint32_t)(max_offsets - pos);
        if (to_write > 0) {
            memcpy(dst + pos, s->free_lists[i].offsets, to_write * sizeof(uint64_t));
        }
        /* Update count to reflect what we actually stored */
        hdr->free_counts[i] = to_write;
        pos += to_write;
    }
}

static bool read_header(const uint8_t *base, flat_store_header_t *out) {
    memcpy(out, base, sizeof(*out));
    if (memcmp(out->magic, FLAT_STORE_MAGIC, 4) != 0) return false;
    if (out->version != FLAT_STORE_VERSION) return false;
    if (out->num_classes == 0 || out->num_classes > MAX_SIZE_CLASSES) return false;
    return true;
}

/* =========================================================================
 * Key Fetch Callback (compact leaf mode)
 * ========================================================================= */

static bool flat_store_key_fetch(const void *value, uint8_t *key_out, void *user_data) {
    flat_store_t *s = (flat_store_t *)user_data;
    uint64_t offset;
    memcpy(&offset, value, sizeof(uint64_t));
    const uint8_t *slot = data_ptr(s, offset);
    /* slot layout: [4B header][key_size B key][record data][padding] */
    memcpy(key_out, slot + SLOT_HEADER_SIZE, s->key_size);
    return true;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

flat_store_t *flat_store_create(const char *path, uint32_t key_size,
                               uint32_t max_record_size)
{
    flat_store_t *s = calloc(1, sizeof(flat_store_t));
    if (!s) return NULL;

    s->key_size        = key_size;
    s->max_record_size = max_record_size;
    compute_size_classes(key_size, max_record_size, s->slot_sizes, &s->num_classes);

    if (!compact_art_init(&s->index, key_size, sizeof(uint64_t),
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
    size_t init_size = FLAT_STORE_HEADER_SIZE + INITIAL_DATA_SIZE;
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

    s->fd              = fd;
    s->base            = base;
    s->mapped_size     = st.st_size;
    s->key_size        = hdr.key_size;
    s->max_record_size = hdr.max_record_size;
    s->num_classes     = (uint8_t)hdr.num_classes;
    s->data_size       = hdr.data_size;
    s->live_count      = 0;
    memcpy(s->slot_sizes, hdr.slot_sizes, sizeof(s->slot_sizes));

    if (!compact_art_init(&s->index, hdr.key_size, sizeof(uint64_t),
                          true, flat_store_key_fetch, s)) {
        munmap(base, st.st_size);
        close(fd);
        free(s);
        return NULL;
    }

    /* Restore free lists from header first */
    size_t hdr_data_start = sizeof(flat_store_header_t);
    const uint64_t *hdr_offsets = (const uint64_t *)(base + hdr_data_start);
    size_t pos = 0;
    for (int i = 0; i < s->num_classes; i++) {
        for (uint32_t j = 0; j < hdr.free_counts[i]; j++) {
            free_list_push(&s->free_lists[i], hdr_offsets[pos++]);
        }
    }

    /* Scan all slots: rebuild index + free lists (overwrites header-based lists) */
    /* Clear the header-loaded free lists — scan is authoritative */
    for (int i = 0; i < s->num_classes; i++) {
        s->free_lists[i].count = 0;
    }

    uint64_t offset = 0;
    while (offset + SLOT_HEADER_SIZE <= s->data_size) {
        uint32_t shdr;
        memcpy(&shdr, data_ptr(s, offset), SLOT_HEADER_SIZE);
        uint8_t class_idx;
        uint16_t data_len;
        slot_header_unpack(shdr, &class_idx, &data_len);
        int sc = (class_idx < s->num_classes) ? class_idx : 0;
        uint32_t slot_total = s->slot_sizes[sc];

        if (data_len > 0) {
            /* Occupied slot */
            const uint8_t *key = data_ptr(s, offset) + SLOT_HEADER_SIZE;
            compact_art_insert(&s->index, key, &offset);
            s->live_count++;
        } else {
            /* Free slot */
            free_list_push(&s->free_lists[sc], offset);
        }

        offset += slot_total;
    }

    return s;
}

void flat_store_destroy(flat_store_t *s) {
    if (!s) return;
    write_header(s);
    compact_art_destroy(&s->index);
    if (s->base && s->base != MAP_FAILED) {
        msync(s->base, s->mapped_size, MS_SYNC);
        munmap(s->base, s->mapped_size);
    }
    if (s->fd >= 0) close(s->fd);
    for (int i = 0; i < MAX_SIZE_CLASSES; i++)
        free_list_destroy(&s->free_lists[i]);
    free(s);
}

/* =========================================================================
 * Slot allocation
 * ========================================================================= */

/* Allocate a slot of the given class. Returns byte offset into data region.
 * Returns UINT64_MAX on failure. */
static uint64_t alloc_slot(flat_store_t *s, int class_idx) {
    /* Try free list first */
    if (s->free_lists[class_idx].count > 0) {
        return free_list_pop(&s->free_lists[class_idx]);
    }

    /* Append at end of data region */
    uint32_t slot_total = s->slot_sizes[class_idx];
    uint64_t offset = s->data_size;
    if (!ensure_mapped(s, offset + slot_total))
        return UINT64_MAX;

    s->data_size += slot_total;
    return offset;
}

/* Free a slot: zero the data_len in header, push to free list */
static void free_slot(flat_store_t *s, uint64_t offset, int class_idx) {
    /* Preserve class_idx so sequential scan can step correctly */
    uint32_t free_hdr = slot_header_pack((uint8_t)class_idx, 0);
    memcpy(data_ptr(s, offset), &free_hdr, SLOT_HEADER_SIZE);
    free_list_push(&s->free_lists[class_idx], offset);
}

/* =========================================================================
 * Operations
 * ========================================================================= */

bool flat_store_put(flat_store_t *s, const uint8_t *key,
                    const void *record, uint32_t record_len)
{
    if (!s || !key || (!record && record_len > 0)) return false;
    if (record_len > s->max_record_size) return false;

    int new_class = class_for_record(s, record_len);

    /* Check if key already exists */
    const void *existing = compact_art_get(&s->index, key);
    if (existing) {
        uint64_t old_offset;
        memcpy(&old_offset, existing, sizeof(uint64_t));

        /* Read old slot header to get class */
        uint32_t old_shdr;
        memcpy(&old_shdr, data_ptr(s, old_offset), SLOT_HEADER_SIZE);
        uint8_t old_class;
        uint16_t old_len;
        slot_header_unpack(old_shdr, &old_class, &old_len);

        if (new_class <= old_class) {
            /* Fits in current slot — update in place */
            uint32_t new_shdr = slot_header_pack(old_class, (uint16_t)record_len);
            memcpy(data_ptr(s, old_offset), &new_shdr, SLOT_HEADER_SIZE);
            if (record_len > 0) {
                memcpy(data_ptr(s, old_offset) + SLOT_HEADER_SIZE + s->key_size,
                       record, record_len);
            }
            /* Zero padding if record shrank */
            uint32_t capacity = class_record_capacity(s, old_class);
            if (record_len < capacity) {
                memset(data_ptr(s, old_offset) + SLOT_HEADER_SIZE + s->key_size + record_len,
                       0, capacity - record_len);
            }
            return true;
        }

        /* Needs larger class: free old slot, allocate new */
        free_slot(s, old_offset, old_class);

        uint64_t new_offset = alloc_slot(s, new_class);
        if (new_offset == UINT64_MAX) return false;

        /* Write new slot */
        uint8_t *slot = data_ptr(s, new_offset);
        uint32_t new_shdr = slot_header_pack((uint8_t)new_class, (uint16_t)record_len);
        memcpy(slot, &new_shdr, SLOT_HEADER_SIZE);
        memcpy(slot + SLOT_HEADER_SIZE, key, s->key_size);
        if (record_len > 0) {
            memcpy(slot + SLOT_HEADER_SIZE + s->key_size, record, record_len);
        }
        /* Zero padding */
        uint32_t capacity = class_record_capacity(s, new_class);
        if (record_len < capacity) {
            memset(slot + SLOT_HEADER_SIZE + s->key_size + record_len,
                   0, capacity - record_len);
        }

        /* Update index to point to new offset */
        compact_art_insert(&s->index, key, &new_offset);
        return true;
    }

    /* New key: allocate a slot */
    uint64_t offset = alloc_slot(s, new_class);
    if (offset == UINT64_MAX) return false;

    /* Write slot: [header | key | record | padding] */
    uint8_t *slot = data_ptr(s, offset);
    uint32_t shdr = slot_header_pack((uint8_t)new_class, (uint16_t)record_len);
    memcpy(slot, &shdr, SLOT_HEADER_SIZE);
    memcpy(slot + SLOT_HEADER_SIZE, key, s->key_size);
    if (record_len > 0) {
        memcpy(slot + SLOT_HEADER_SIZE + s->key_size, record, record_len);
    }
    /* Zero padding */
    uint32_t capacity = class_record_capacity(s, new_class);
    if (record_len < capacity) {
        memset(slot + SLOT_HEADER_SIZE + s->key_size + record_len,
               0, capacity - record_len);
    }

    if (!compact_art_insert(&s->index, key, &offset))
        return false;

    s->live_count++;
    return true;
}

bool flat_store_get(const flat_store_t *s, const uint8_t *key,
                    void *out, uint32_t buf_size, uint32_t *out_len)
{
    if (!s || !key) return false;

    const void *val = compact_art_get(&s->index, key);
    if (!val) return false;

    uint64_t offset;
    memcpy(&offset, val, sizeof(uint64_t));

    /* Read slot header to get data_len */
    uint32_t shdr;
    memcpy(&shdr, data_ptr(s, offset), SLOT_HEADER_SIZE);
    uint8_t class_idx;
    uint16_t data_len;
    slot_header_unpack(shdr, &class_idx, &data_len);

    if (out_len) *out_len = data_len;

    if (out && buf_size > 0) {
        uint32_t copy_len = data_len < buf_size ? data_len : buf_size;
        memcpy(out, data_ptr(s, offset) + SLOT_HEADER_SIZE + s->key_size, copy_len);
    }

    return true;
}

bool flat_store_delete(flat_store_t *s, const uint8_t *key) {
    if (!s || !key) return false;

    const void *val = compact_art_get(&s->index, key);
    if (!val) return false;

    uint64_t offset;
    memcpy(&offset, val, sizeof(uint64_t));

    /* Read class from header */
    uint32_t shdr;
    memcpy(&shdr, data_ptr(s, offset), SLOT_HEADER_SIZE);
    uint8_t class_idx;
    uint16_t data_len;
    slot_header_unpack(shdr, &class_idx, &data_len);

    free_slot(s, offset, class_idx);
    compact_art_delete(&s->index, key);
    s->live_count--;
    return true;
}

bool flat_store_contains(const flat_store_t *s, const uint8_t *key) {
    if (!s || !key) return false;
    return compact_art_contains(&s->index, key);
}

/* =========================================================================
 * Batch Operations
 * ========================================================================= */

typedef struct {
    uint32_t idx;         /* original index in keys/records array */
    uint64_t slot_offset; /* existing offset (UINT64_MAX = new entry) */
} batch_sort_t;

static int cmp_batch_offset(const void *a, const void *b) {
    const batch_sort_t *ea = (const batch_sort_t *)a;
    const batch_sort_t *eb = (const batch_sort_t *)b;
    if (ea->slot_offset < eb->slot_offset) return -1;
    if (ea->slot_offset > eb->slot_offset) return  1;
    return 0;
}

bool flat_store_batch_put(flat_store_t *s, const uint8_t *keys,
                           const void *records, const uint32_t *record_lens,
                           uint32_t count) {
    if (!s || !keys || !records || !record_lens || count == 0) return false;

    /* Compute offsets into concatenated records array */
    uint64_t *rec_offsets = malloc(count * sizeof(uint64_t));
    if (!rec_offsets) return false;
    uint64_t off = 0;
    for (uint32_t i = 0; i < count; i++) {
        rec_offsets[i] = off;
        off += record_lens[i];
    }

    /* Pass 1: look up existing offsets */
    batch_sort_t *entries = malloc(count * sizeof(batch_sort_t));
    if (!entries) { free(rec_offsets); return false; }

    for (uint32_t i = 0; i < count; i++) {
        entries[i].idx = i;
        const void *val = compact_art_get(&s->index,
                                           keys + (size_t)i * s->key_size);
        if (val) {
            memcpy(&entries[i].slot_offset, val, sizeof(uint64_t));
        } else {
            entries[i].slot_offset = UINT64_MAX;
        }
    }

    /* Sort by offset for sequential page access */
    qsort(entries, count, sizeof(batch_sort_t), cmp_batch_offset);

    /* Pass 2: write in sorted order */
    const uint8_t *rec_base = (const uint8_t *)records;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t orig = entries[i].idx;
        const uint8_t *key = keys + (size_t)orig * s->key_size;
        const uint8_t *rec = rec_base + rec_offsets[orig];
        uint32_t rec_len = record_lens[orig];

        /* Use the single-entry put for correctness (handles class migration) */
        flat_store_put(s, key, rec, rec_len);
    }

    free(entries);
    free(rec_offsets);
    return true;
}

/* =========================================================================
 * Stats
 * ========================================================================= */

uint32_t flat_store_count(const flat_store_t *s) {
    return s ? s->live_count : 0;
}

uint32_t flat_store_key_size(const flat_store_t *s) {
    return s ? s->key_size : 0;
}

uint32_t flat_store_max_record_size(const flat_store_t *s) {
    return s ? s->max_record_size : 0;
}

/* =========================================================================
 * Internal Access
 * ========================================================================= */

compact_art_t *flat_store_get_art(flat_store_t *s) {
    return s ? &s->index : NULL;
}

uint32_t flat_store_read_leaf_record(const flat_store_t *s,
                                      const void *leaf_val,
                                      uint8_t *buf, uint32_t buf_size) {
    if (!s || !leaf_val || !buf) return 0;
    uint64_t offset;
    memcpy(&offset, leaf_val, sizeof(uint64_t));
    const uint8_t *slot = data_ptr(s, offset);
    uint32_t shdr;
    memcpy(&shdr, slot, SLOT_HEADER_SIZE);
    uint8_t class_idx;
    uint16_t data_len;
    slot_header_unpack(shdr, &class_idx, &data_len);
    if (data_len == 0) return 0;
    if (data_len > buf_size) data_len = (uint16_t)buf_size;
    memcpy(buf, slot + SLOT_HEADER_SIZE + s->key_size, data_len);
    return data_len;
}

/* =========================================================================
 * Durability
 * ========================================================================= */

void flat_store_sync(flat_store_t *s) {
    if (!s) return;
    write_header(s);
    /* No msync — OS page cache handles writeback */
}
