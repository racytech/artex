#include "../include/hash_store.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>

// ============================================================================
// Constants
// ============================================================================

#define SHARD_MAGIC     "HASHSTR2"   // v2: configurable sizes
#define META_MAGIC      "HSHMTA02"   // v2: includes key_size, slot_size
#define META_FILENAME   "meta.dat"
#define HEADER_SIZE     HASH_STORE_HEADER_SIZE  // 64
#define MAX_PATH_LEN    512

// Slot offsets within variable-size slot
#define SLOT_OFF_FINGERPRINT  0
#define SLOT_OFF_FLAGS        8
#define SLOT_OFF_VALUE_LEN    9
#define SLOT_OFF_VALUE        10

// Meta header size (expanded for key_size + slot_size)
#define META_HDR_SIZE  40

// Max slot size supported (for stack buffers)
#define MAX_SLOT_SIZE  256

// ============================================================================
// Internal types
// ============================================================================

// Per-shard file header
typedef struct {
    uint8_t  magic[8];
    uint64_t capacity;
    uint64_t count;
    uint64_t tombstones;
    uint32_t slot_size;     // stored for validation
    uint8_t  reserved[28];
} shard_header_t;

// Shard runtime state
typedef struct {
    int      fd;
    uint8_t *map;           // mmap'd region
    size_t   map_size;      // HEADER_SIZE + shard_cap * slot_size
    uint32_t id;            // shard file number (shard_NNNN.dat)
    uint32_t local_depth;   // how many top bits this shard covers
    uint64_t count;         // occupied entries
    uint64_t tombstones;    // tombstone entries
    bool     header_dirty;
} shard_t;

// Main hash store (sharded)
struct hash_store {
    char     *dir;
    uint32_t  global_depth; // number of routing bits
    uint64_t  shard_cap;    // fixed slots per shard (power of 2)

    uint32_t  key_size;     // fixed key length in bytes
    uint32_t  slot_size;    // bytes per slot
    uint32_t  max_value;    // slot_size - 10 - key_size

    shard_t **shards;       // unique shard array (owned pointers)
    uint32_t  num_shards;
    uint32_t  shards_alloc; // allocated capacity of shards array

    shard_t **directory;    // 2^global_depth entries, each → shard
    uint32_t  dir_size;     // = 1 << global_depth

    uint32_t  next_shard_id; // monotonic counter for naming new shards
};

// Slot data for I/O (stack-allocated with max possible data)
// Layout on disk: [8B fp][1B flags][1B vlen][key_size B key][vlen B value]
typedef struct {
    uint64_t fingerprint;
    uint8_t  flags;
    uint8_t  value_len;
    uint8_t  data[MAX_SLOT_SIZE - 10]; // [key][value] combined
} slot_t;

// ============================================================================
// Helpers
// ============================================================================

static inline uint64_t extract_fingerprint(const uint8_t *key) {
    uint64_t fp;
    memcpy(&fp, key, 8);
    return fp;
}

static bool is_power_of_2(uint64_t v) {
    return v > 0 && (v & (v - 1)) == 0;
}

static uint64_t next_power_of_2(uint64_t v) {
    if (v == 0) return 1;
    v--;
    v |= v >> 1;  v |= v >> 2;  v |= v >> 4;
    v |= v >> 8;  v |= v >> 16; v |= v >> 32;
    return v + 1;
}

static void shard_path(const hash_store_t *hs, uint32_t shard_id,
                        char *out, size_t out_size) {
    snprintf(out, out_size, "%s/shard_%04u.dat", hs->dir, shard_id);
}

static void meta_path(const hash_store_t *hs, char *out, size_t out_size) {
    snprintf(out, out_size, "%s/%s", hs->dir, META_FILENAME);
}

static bool ensure_dir(const char *path) {
    if (mkdir(path, 0755) == 0) return true;
    return errno == EEXIST;
}

// Route fingerprint to directory index
static inline uint32_t dir_index_for(const hash_store_t *hs, uint64_t fp) {
    if (hs->global_depth == 0) return 0;
    return (uint32_t)(fp >> (64 - hs->global_depth));
}

// mmap a shard file
static bool mmap_shard(shard_t *sh, size_t size) {
    sh->map = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, sh->fd, 0);
    if (sh->map == MAP_FAILED) {
        sh->map = NULL;
        return false;
    }
    sh->map_size = size;
    return true;
}

// ============================================================================
// Shard Header I/O (direct memory access via mmap)
// ============================================================================

static bool shard_write_header(shard_t *sh, uint64_t capacity,
                                uint32_t slot_size) {
    shard_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, SHARD_MAGIC, 8);
    hdr.capacity = capacity;
    hdr.count = sh->count;
    hdr.tombstones = sh->tombstones;
    hdr.slot_size = slot_size;

    memcpy(sh->map, &hdr, HEADER_SIZE);
    sh->header_dirty = false;
    return true;
}

static bool shard_read_header(shard_t *sh, uint64_t expected_cap,
                               uint32_t expected_slot_size) {
    shard_header_t hdr;
    memcpy(&hdr, sh->map, HEADER_SIZE);
    if (memcmp(hdr.magic, SHARD_MAGIC, 8) != 0)
        return false;
    if (hdr.capacity != expected_cap)
        return false;
    if (hdr.slot_size != expected_slot_size)
        return false;

    sh->count = hdr.count;
    sh->tombstones = hdr.tombstones;
    return true;
}

// ============================================================================
// Slot I/O (direct pointer arithmetic via mmap)
// ============================================================================

static inline uint8_t *slot_ptr(const hash_store_t *hs, uint8_t *map,
                                 uint64_t idx) {
    return map + HEADER_SIZE + idx * hs->slot_size;
}

static void read_slot(const hash_store_t *hs, uint8_t *map, uint64_t idx,
                       slot_t *out) {
    const uint8_t *p = slot_ptr(hs, map, idx);

    memcpy(&out->fingerprint, p + SLOT_OFF_FINGERPRINT, 8);
    out->flags = p[SLOT_OFF_FLAGS];
    out->value_len = p[SLOT_OFF_VALUE_LEN];
    if (out->value_len > hs->max_value) out->value_len = hs->max_value;
    // Read key + value combined
    memcpy(out->data, p + SLOT_OFF_VALUE, hs->key_size + out->value_len);
}

static void write_slot(const hash_store_t *hs, uint8_t *map, uint64_t idx,
                        const slot_t *s) {
    uint8_t *p = slot_ptr(hs, map, idx);

    memset(p, 0, hs->slot_size);
    memcpy(p + SLOT_OFF_FINGERPRINT, &s->fingerprint, 8);
    p[SLOT_OFF_FLAGS] = s->flags;
    p[SLOT_OFF_VALUE_LEN] = s->value_len;
    // Write key + value combined
    memcpy(p + SLOT_OFF_VALUE, s->data, hs->key_size + s->value_len);
}

static inline void write_slot_flags(const hash_store_t *hs, uint8_t *map,
                                     uint64_t idx, uint8_t flags) {
    slot_ptr(hs, map, idx)[SLOT_OFF_FLAGS] = flags;
}

// Read fingerprint + flags for probing
static inline void read_slot_probe(const hash_store_t *hs, uint8_t *map,
                                    uint64_t idx,
                                    uint64_t *out_fp, uint8_t *out_flags) {
    const uint8_t *p = slot_ptr(hs, map, idx);
    memcpy(out_fp, p, 8);
    *out_flags = p[8];
}

// ============================================================================
// Meta File I/O (stays pread/pwrite — tiny, not hot path)
// ============================================================================

static bool write_meta(hash_store_t *hs) {
    char path[MAX_PATH_LEN];
    meta_path(hs, path, sizeof(path));

    // Write to .tmp then rename for atomicity
    char tmp[MAX_PATH_LEN + 8];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;

    // Header: 40 bytes
    uint8_t hdr[META_HDR_SIZE];
    memset(hdr, 0, META_HDR_SIZE);
    memcpy(hdr + 0,  META_MAGIC, 8);
    memcpy(hdr + 8,  &hs->global_depth, 4);
    memcpy(hdr + 12, &hs->next_shard_id, 4);
    memcpy(hdr + 16, &hs->shard_cap, 8);
    memcpy(hdr + 24, &hs->num_shards, 4);
    memcpy(hdr + 28, &hs->key_size, 4);
    memcpy(hdr + 32, &hs->slot_size, 4);
    if (write(fd, hdr, META_HDR_SIZE) != META_HDR_SIZE) goto fail;

    // Shard table: num_shards × 12 bytes
    for (uint32_t i = 0; i < hs->num_shards; i++) {
        uint8_t entry[12];
        memset(entry, 0, 12);
        memcpy(entry + 0, &hs->shards[i]->id, 4);
        memcpy(entry + 4, &hs->shards[i]->local_depth, 4);
        if (write(fd, entry, 12) != 12) goto fail;
    }

    // Directory table: dir_size × 4 bytes (shard index into shards[])
    for (uint32_t i = 0; i < hs->dir_size; i++) {
        uint32_t si = 0;
        for (uint32_t j = 0; j < hs->num_shards; j++) {
            if (hs->directory[i] == hs->shards[j]) { si = j; break; }
        }
        if (write(fd, &si, 4) != 4) goto fail;
    }

    close(fd);
    return rename(tmp, path) == 0;

fail:
    close(fd);
    unlink(tmp);
    return false;
}

static bool read_meta(hash_store_t *hs) {
    char path[MAX_PATH_LEN];
    meta_path(hs, path, sizeof(path));

    int fd = open(path, O_RDONLY);
    if (fd < 0) return false;

    // Read header
    uint8_t hdr[META_HDR_SIZE];
    if (read(fd, hdr, META_HDR_SIZE) != META_HDR_SIZE) goto fail;
    if (memcmp(hdr, META_MAGIC, 8) != 0) goto fail;

    memcpy(&hs->global_depth, hdr + 8, 4);
    memcpy(&hs->next_shard_id, hdr + 12, 4);
    memcpy(&hs->shard_cap, hdr + 16, 8);
    memcpy(&hs->num_shards, hdr + 24, 4);
    memcpy(&hs->key_size, hdr + 28, 4);
    memcpy(&hs->slot_size, hdr + 32, 4);

    if (hs->key_size < 8 ||
        hs->slot_size < 10 + hs->key_size + 1 ||
        hs->slot_size > MAX_SLOT_SIZE)
        goto fail;

    hs->max_value = hs->slot_size - 10 - hs->key_size;
    hs->dir_size = 1u << hs->global_depth;

    // Read shard table, open and mmap each shard file
    hs->shards_alloc = hs->num_shards + 8;
    hs->shards = calloc(hs->shards_alloc, sizeof(shard_t *));
    if (!hs->shards) goto fail;

    size_t shard_file_size = HEADER_SIZE + hs->shard_cap * hs->slot_size;

    for (uint32_t i = 0; i < hs->num_shards; i++) {
        uint8_t entry[12];
        if (read(fd, entry, 12) != 12) goto fail;

        shard_t *sh = calloc(1, sizeof(shard_t));
        if (!sh) goto fail;
        memcpy(&sh->id, entry + 0, 4);
        memcpy(&sh->local_depth, entry + 4, 4);

        char sp[MAX_PATH_LEN];
        shard_path(hs, sh->id, sp, sizeof(sp));
        sh->fd = open(sp, O_RDWR);
        if (sh->fd < 0) { free(sh); goto fail; }

        if (!mmap_shard(sh, shard_file_size)) {
            close(sh->fd); free(sh); goto fail;
        }

        if (!shard_read_header(sh, hs->shard_cap, hs->slot_size)) {
            munmap(sh->map, sh->map_size);
            close(sh->fd); free(sh); goto fail;
        }

        hs->shards[i] = sh;
    }

    // Read directory table
    hs->directory = calloc(hs->dir_size, sizeof(shard_t *));
    if (!hs->directory) goto fail;

    for (uint32_t i = 0; i < hs->dir_size; i++) {
        uint32_t si;
        if (read(fd, &si, 4) != 4) goto fail;
        if (si >= hs->num_shards) goto fail;
        hs->directory[i] = hs->shards[si];
    }

    close(fd);
    return true;

fail:
    close(fd);
    return false;
}

// ============================================================================
// Shard Lifecycle
// ============================================================================

static shard_t *create_shard(hash_store_t *hs, uint32_t local_depth) {
    uint32_t id = hs->next_shard_id++;

    char path[MAX_PATH_LEN];
    shard_path(hs, id, path, sizeof(path));

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return NULL;

    size_t file_size = HEADER_SIZE + hs->shard_cap * hs->slot_size;
    if (ftruncate(fd, (off_t)file_size) != 0) {
        close(fd);
        unlink(path);
        return NULL;
    }

    shard_t *sh = calloc(1, sizeof(shard_t));
    if (!sh) { close(fd); unlink(path); return NULL; }

    sh->fd = fd;
    sh->id = id;
    sh->local_depth = local_depth;
    sh->count = 0;
    sh->tombstones = 0;
    sh->header_dirty = false;

    if (!mmap_shard(sh, file_size)) {
        close(fd);
        unlink(path);
        free(sh);
        return NULL;
    }

    if (!shard_write_header(sh, hs->shard_cap, hs->slot_size)) {
        munmap(sh->map, sh->map_size);
        close(fd);
        unlink(path);
        free(sh);
        return NULL;
    }

    // Add to shards array (grow if needed)
    if (hs->num_shards >= hs->shards_alloc) {
        uint32_t new_alloc = hs->shards_alloc * 2;
        shard_t **new_arr = realloc(hs->shards, new_alloc * sizeof(shard_t *));
        if (!new_arr) {
            munmap(sh->map, sh->map_size);
            close(fd); unlink(path); free(sh);
            return NULL;
        }
        hs->shards = new_arr;
        hs->shards_alloc = new_alloc;
    }
    hs->shards[hs->num_shards++] = sh;

    return sh;
}

// ============================================================================
// Insert into shard (no split check — caller handles that)
// ============================================================================

static bool insert_into_shard(const hash_store_t *hs, shard_t *sh,
                              uint64_t fp, const uint8_t *key,
                              const uint8_t *value, uint8_t vlen) {
    uint64_t mask = hs->shard_cap - 1;
    uint64_t idx = fp & mask;
    uint64_t first_tombstone = UINT64_MAX;

    for (;;) {
        uint64_t slot_fp;
        uint8_t flags;
        read_slot_probe(hs, sh->map, idx, &slot_fp, &flags);

        if (flags == HASH_STORE_FLAG_EMPTY) {
            if (first_tombstone != UINT64_MAX) {
                idx = first_tombstone;
                sh->tombstones--;
            }
            slot_t s = { .fingerprint = fp,
                         .flags = HASH_STORE_FLAG_OCCUPIED,
                         .value_len = vlen };
            memcpy(s.data, key, hs->key_size);
            memcpy(s.data + hs->key_size, value, vlen);
            write_slot(hs, sh->map, idx, &s);
            sh->count++;
            sh->header_dirty = true;
            return true;
        }

        if (flags == HASH_STORE_FLAG_TOMBSTONE) {
            if (first_tombstone == UINT64_MAX)
                first_tombstone = idx;
            idx = (idx + 1) & mask;
            continue;
        }

        // Occupied — check if same key (update via full key compare)
        if (slot_fp == fp) {
            // Compare full key to handle fingerprint collisions
            const uint8_t *p = slot_ptr(hs, sh->map, idx);
            if (memcmp(p + SLOT_OFF_VALUE, key, hs->key_size) == 0) {
                slot_t s = { .fingerprint = fp,
                             .flags = HASH_STORE_FLAG_OCCUPIED,
                             .value_len = vlen };
                memcpy(s.data, key, hs->key_size);
                memcpy(s.data + hs->key_size, value, vlen);
                write_slot(hs, sh->map, idx, &s);
                return true;
            }
        }

        idx = (idx + 1) & mask;
    }
}

// ============================================================================
// Split (replaces old resize)
// ============================================================================

static bool split_shard(hash_store_t *hs, shard_t *old_shard) {
    uint32_t old_depth = old_shard->local_depth;
    uint32_t new_depth = old_depth + 1;

    // Step 1: If local_depth == global_depth, double the directory
    if (old_depth == hs->global_depth) {
        uint32_t new_dir_size = hs->dir_size * 2;
        if (hs->global_depth == 0) new_dir_size = 2;

        shard_t **new_dir = calloc(new_dir_size, sizeof(shard_t *));
        if (!new_dir) return false;

        // Each old entry i maps to new entries 2*i and 2*i+1
        for (uint32_t i = 0; i < hs->dir_size; i++) {
            new_dir[2 * i]     = hs->directory[i];
            new_dir[2 * i + 1] = hs->directory[i];
        }

        free(hs->directory);
        hs->directory = new_dir;
        hs->global_depth++;
        hs->dir_size = new_dir_size;
    }

    // Step 2: Create two new shards at new_depth
    shard_t *sh_a = create_shard(hs, new_depth);
    if (!sh_a) return false;
    shard_t *sh_b = create_shard(hs, new_depth);
    if (!sh_b) return false;

    // Step 3: Scan old shard, redistribute entries
    for (uint64_t i = 0; i < hs->shard_cap; i++) {
        slot_t s;
        read_slot(hs, old_shard->map, i, &s);
        if (s.flags != HASH_STORE_FLAG_OCCUPIED) continue;

        // Check bit at position new_depth from MSB
        uint32_t bit;
        if (new_depth >= 64) {
            bit = 0;
        } else {
            bit = (uint32_t)((s.fingerprint >> (64 - new_depth)) & 1);
        }

        shard_t *target = (bit == 0) ? sh_a : sh_b;
        if (!insert_into_shard(hs, target,
                               s.fingerprint, s.data,
                               s.data + hs->key_size, s.value_len)) {
            return false;
        }
    }

    // Write headers for new shards
    shard_write_header(sh_a, hs->shard_cap, hs->slot_size);
    shard_write_header(sh_b, hs->shard_cap, hs->slot_size);

    // Step 4: Update directory entries
    for (uint32_t i = 0; i < hs->dir_size; i++) {
        if (hs->directory[i] != old_shard) continue;
        uint32_t bit = (i >> (hs->global_depth - new_depth)) & 1;
        hs->directory[i] = (bit == 0) ? sh_a : sh_b;
    }

    // Step 5: munmap, close, and remove old shard
    munmap(old_shard->map, old_shard->map_size);
    close(old_shard->fd);
    char old_path[MAX_PATH_LEN];
    shard_path(hs, old_shard->id, old_path, sizeof(old_path));
    unlink(old_path);

    // Remove from shards array
    for (uint32_t i = 0; i < hs->num_shards; i++) {
        if (hs->shards[i] == old_shard) {
            hs->shards[i] = hs->shards[hs->num_shards - 1];
            hs->num_shards--;
            break;
        }
    }
    free(old_shard);

    // Step 6: Persist metadata
    return write_meta(hs);
}

// ============================================================================
// Lifecycle
// ============================================================================

hash_store_t *hash_store_create(const char *dir, uint64_t shard_capacity,
                                 uint32_t key_size, uint32_t slot_size) {
    if (!dir || shard_capacity == 0) return NULL;
    if (key_size < 8) return NULL;
    if (slot_size < 10 + key_size + 1 || slot_size > MAX_SLOT_SIZE) return NULL;

    if (!is_power_of_2(shard_capacity))
        shard_capacity = next_power_of_2(shard_capacity);

    if (!ensure_dir(dir)) return NULL;

    hash_store_t *hs = calloc(1, sizeof(hash_store_t));
    if (!hs) return NULL;

    hs->dir = strdup(dir);
    if (!hs->dir) { free(hs); return NULL; }

    hs->global_depth = 0;
    hs->shard_cap = shard_capacity;
    hs->key_size = key_size;
    hs->slot_size = slot_size;
    hs->max_value = slot_size - 10 - key_size;
    hs->dir_size = 1;
    hs->next_shard_id = 0;
    hs->num_shards = 0;
    hs->shards_alloc = 8;
    hs->shards = calloc(hs->shards_alloc, sizeof(shard_t *));
    if (!hs->shards) { free(hs->dir); free(hs); return NULL; }

    // Create initial shard
    shard_t *initial = create_shard(hs, 0);
    if (!initial) { free(hs->shards); free(hs->dir); free(hs); return NULL; }

    // Create directory with single entry
    hs->directory = calloc(1, sizeof(shard_t *));
    if (!hs->directory) {
        munmap(initial->map, initial->map_size);
        close(initial->fd); free(initial);
        free(hs->shards); free(hs->dir); free(hs);
        return NULL;
    }
    hs->directory[0] = initial;

    // Persist metadata
    if (!write_meta(hs)) {
        hash_store_destroy(hs);
        return NULL;
    }

    return hs;
}

hash_store_t *hash_store_open(const char *dir) {
    if (!dir) return NULL;

    hash_store_t *hs = calloc(1, sizeof(hash_store_t));
    if (!hs) return NULL;

    hs->dir = strdup(dir);
    if (!hs->dir) { free(hs); return NULL; }

    if (!read_meta(hs)) {
        free(hs->dir);
        free(hs);
        return NULL;
    }

    return hs;
}

void hash_store_destroy(hash_store_t *hs) {
    if (!hs) return;

    // Flush dirty shard headers and persist meta
    for (uint32_t i = 0; i < hs->num_shards; i++) {
        shard_t *sh = hs->shards[i];
        if (sh->header_dirty)
            shard_write_header(sh, hs->shard_cap, hs->slot_size);
        munmap(sh->map, sh->map_size);
        close(sh->fd);
        free(sh);
    }

    write_meta(hs);

    free(hs->shards);
    free(hs->directory);
    free(hs->dir);
    free(hs);
}

// ============================================================================
// Operations
// ============================================================================

bool hash_store_put(hash_store_t *hs, const uint8_t *key,
                    const void *value, uint8_t len) {
    if (!hs || !key || !value || len > hs->max_value) return false;

    uint64_t fp = extract_fingerprint(key);
    uint32_t di = dir_index_for(hs, fp);
    shard_t *sh = hs->directory[di];

    // Check if shard needs split (75% load factor)
    if ((sh->count + sh->tombstones) * 4 >= hs->shard_cap * 3) {
        if (!split_shard(hs, sh)) return false;
        // Re-route after split (directory changed)
        di = dir_index_for(hs, fp);
        sh = hs->directory[di];
    }

    return insert_into_shard(hs, sh, fp, key, value, len);
}

bool hash_store_get(const hash_store_t *hs, const uint8_t *key,
                    void *out_value, uint8_t *out_len) {
    if (!hs || !key) return false;

    uint64_t fp = extract_fingerprint(key);
    uint32_t di = dir_index_for(hs, fp);
    shard_t *sh = hs->directory[di];

    uint64_t mask = hs->shard_cap - 1;
    uint64_t idx = fp & mask;

    for (;;) {
        uint64_t slot_fp;
        uint8_t flags;
        read_slot_probe(hs, sh->map, idx, &slot_fp, &flags);

        if (flags == HASH_STORE_FLAG_EMPTY)
            return false;

        if (flags == HASH_STORE_FLAG_OCCUPIED && slot_fp == fp) {
            // Compare full key
            const uint8_t *p = slot_ptr(hs, sh->map, idx);
            if (memcmp(p + SLOT_OFF_VALUE, key, hs->key_size) == 0) {
                if (out_value || out_len) {
                    slot_t s;
                    read_slot(hs, sh->map, idx, &s);
                    if (out_value) memcpy(out_value, s.data + hs->key_size,
                                          s.value_len);
                    if (out_len) *out_len = s.value_len;
                }
                return true;
            }
        }

        idx = (idx + 1) & mask;
    }
}

bool hash_store_contains(const hash_store_t *hs, const uint8_t *key) {
    return hash_store_get(hs, key, NULL, NULL);
}

bool hash_store_delete(hash_store_t *hs, const uint8_t *key) {
    if (!hs || !key) return false;

    uint64_t fp = extract_fingerprint(key);
    uint32_t di = dir_index_for(hs, fp);
    shard_t *sh = hs->directory[di];

    uint64_t mask = hs->shard_cap - 1;
    uint64_t idx = fp & mask;

    for (;;) {
        uint64_t slot_fp;
        uint8_t flags;
        read_slot_probe(hs, sh->map, idx, &slot_fp, &flags);

        if (flags == HASH_STORE_FLAG_EMPTY)
            return false;

        if (flags == HASH_STORE_FLAG_OCCUPIED && slot_fp == fp) {
            // Compare full key
            const uint8_t *p = slot_ptr(hs, sh->map, idx);
            if (memcmp(p + SLOT_OFF_VALUE, key, hs->key_size) == 0) {
                write_slot_flags(hs, sh->map, idx, HASH_STORE_FLAG_TOMBSTONE);
                sh->count--;
                sh->tombstones++;
                sh->header_dirty = true;
                return true;
            }
        }

        idx = (idx + 1) & mask;
    }
}

// ============================================================================
// Stats
// ============================================================================

uint64_t hash_store_count(const hash_store_t *hs) {
    if (!hs) return 0;
    uint64_t total = 0;
    for (uint32_t i = 0; i < hs->num_shards; i++)
        total += hs->shards[i]->count;
    return total;
}

uint64_t hash_store_capacity(const hash_store_t *hs) {
    if (!hs) return 0;
    return (uint64_t)hs->num_shards * hs->shard_cap;
}

uint64_t hash_store_tombstones(const hash_store_t *hs) {
    if (!hs) return 0;
    uint64_t total = 0;
    for (uint32_t i = 0; i < hs->num_shards; i++)
        total += hs->shards[i]->tombstones;
    return total;
}

uint32_t hash_store_num_shards(const hash_store_t *hs) {
    return hs ? hs->num_shards : 0;
}

uint32_t hash_store_global_depth(const hash_store_t *hs) {
    return hs ? hs->global_depth : 0;
}

uint32_t hash_store_key_size(const hash_store_t *hs) {
    return hs ? hs->key_size : 0;
}

uint32_t hash_store_slot_size(const hash_store_t *hs) {
    return hs ? hs->slot_size : 0;
}

uint32_t hash_store_max_value(const hash_store_t *hs) {
    return hs ? hs->max_value : 0;
}

// ============================================================================
// Durability
// ============================================================================

void hash_store_sync(hash_store_t *hs) {
    if (!hs) return;
    for (uint32_t i = 0; i < hs->num_shards; i++) {
        shard_t *sh = hs->shards[i];
        if (sh->header_dirty)
            shard_write_header(sh, hs->shard_cap, hs->slot_size);
        // Durability deferred — msync/fdatasync commented out for now.
        // msync(sh->map, sh->map_size, MS_SYNC);
    }
    write_meta(hs);
}
