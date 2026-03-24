/*
 * rebuild_idx — Rebuild .idx (disk_table) from .dat file for MPT store.
 *
 * Scans the .dat file sequentially, skipping free slots (from .free file
 * and header free lists), hashes each occupied node's RLP, and populates
 * a fresh disk_table .idx file.
 *
 * Usage:
 *   rebuild_idx <base_path> [--verify] [--dry-run] [--capacity N]
 *
 * <base_path> is the prefix for .dat/.idx/.free files.
 * --verify: compare rebuilt .idx against existing .idx (does not overwrite)
 * --dry-run: scan and report stats without writing any files
 * --capacity N: set hash table capacity (default: node_count)
 */

#include "disk_table.h"
#include "keccak256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <limits.h>
#include <time.h>

/* =========================================================================
 * Constants (must match mpt_store.c)
 * ========================================================================= */

#define MPT_STORE_MAGIC    0x5453504DU   /* "MPST" little-endian */
#define MPT_STORE_VERSION  1
#define PAGE_SIZE_         4096
#define NODE_HASH_SIZE     32
#define MAX_NODE_RLP       1024

#define NUM_SIZE_CLASSES   5
static const uint16_t SIZE_CLASSES[NUM_SIZE_CLASSES] = {64, 128, 256, 512, 1024};

#define MAX_HDR_FREE_OFFSETS  502

/* =========================================================================
 * On-disk types (must match mpt_store.c)
 * ========================================================================= */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint8_t  root_hash[32];
    uint64_t data_size;
    uint64_t free_slot_bytes;
    uint32_t free_counts[NUM_SIZE_CLASSES];
    uint8_t  free_data[4020];
} mpt_store_header_t;

_Static_assert(sizeof(mpt_store_header_t) == 4096,
               "mpt_store_header_t must be 4096 bytes");

typedef struct __attribute__((packed)) {
    uint64_t offset;
    uint32_t length;
    uint32_t refcount;
} node_record_t;

_Static_assert(sizeof(node_record_t) == 16, "node_record_t must be 16 bytes");

/* =========================================================================
 * Free set: sorted array of {offset, size_class} for O(log n) lookup
 * ========================================================================= */

typedef struct {
    uint64_t offset;
    uint16_t slot_size;
} free_entry_t;

static int free_entry_cmp(const void *a, const void *b) {
    uint64_t oa = ((const free_entry_t *)a)->offset;
    uint64_t ob = ((const free_entry_t *)b)->offset;
    return (oa > ob) - (oa < ob);
}

typedef struct {
    free_entry_t *entries;
    uint32_t      count;
    uint32_t      capacity;
} free_set_t;

static bool free_set_add(free_set_t *fs, uint64_t offset, uint16_t slot_size) {
    if (fs->count >= fs->capacity) {
        uint32_t new_cap = fs->capacity ? fs->capacity * 2 : 1024;
        free_entry_t *p = realloc(fs->entries, new_cap * sizeof(free_entry_t));
        if (!p) return false;
        fs->entries = p;
        fs->capacity = new_cap;
    }
    fs->entries[fs->count++] = (free_entry_t){offset, slot_size};
    return true;
}

static void free_set_sort(free_set_t *fs) {
    qsort(fs->entries, fs->count, sizeof(free_entry_t), free_entry_cmp);
}

/* Returns slot_size if offset is in free set, 0 otherwise */
static uint16_t free_set_lookup(const free_set_t *fs, uint64_t offset) {
    uint32_t lo = 0, hi = fs->count;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (fs->entries[mid].offset < offset) lo = mid + 1;
        else if (fs->entries[mid].offset > offset) hi = mid;
        else return fs->entries[mid].slot_size;
    }
    return 0;
}

/* =========================================================================
 * Load free lists from .dat header + .free file
 * ========================================================================= */

static bool load_free_set(const mpt_store_header_t *hdr, const char *free_path,
                           free_set_t *fs) {
    /* Header free lists */
    const uint64_t *src = (const uint64_t *)hdr->free_data;
    uint32_t pos = 0;
    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        for (uint32_t j = 0; j < hdr->free_counts[i]; j++) {
            if (pos >= MAX_HDR_FREE_OFFSETS) break;
            if (!free_set_add(fs, src[pos], SIZE_CLASSES[i])) return false;
            pos++;
        }
    }

    /* Overflow .free file */
    int fd = open(free_path, O_RDONLY);
    if (fd < 0) {
        /* No .free file — that's fine, header has everything */
        free_set_sort(fs);
        return true;
    }

    uint32_t counts[NUM_SIZE_CLASSES];
    if (read(fd, counts, sizeof(counts)) != sizeof(counts)) {
        close(fd);
        free_set_sort(fs);
        return true;
    }

    for (int i = 0; i < NUM_SIZE_CLASSES; i++) {
        if (counts[i] == 0) continue;
        uint64_t *buf = malloc(counts[i] * sizeof(uint64_t));
        if (!buf) { close(fd); return false; }
        ssize_t got = read(fd, buf, counts[i] * sizeof(uint64_t));
        if (got == (ssize_t)(counts[i] * sizeof(uint64_t))) {
            for (uint32_t j = 0; j < counts[i]; j++)
                free_set_add(fs, buf[j], SIZE_CLASSES[i]);
        }
        free(buf);
    }
    close(fd);

    free_set_sort(fs);
    return true;
}

/* =========================================================================
 * RLP total length decoder
 * ========================================================================= */

/* Returns total RLP encoded length (prefix + payload), or 0 on invalid. */
static uint32_t decode_rlp_total_len(const uint8_t *data, size_t avail) {
    if (avail == 0) return 0;
    uint8_t b = data[0];

    /* Single byte */
    if (b < 0x80) return 1;

    /* Short string: 0x80..0xB7 */
    if (b <= 0xB7) return 1 + (b - 0x80);

    /* Long string: 0xB8..0xBF */
    if (b <= 0xBF) {
        uint8_t ll = b - 0xB7;
        if ((size_t)(1 + ll) > avail) return 0;
        uint64_t payload = 0;
        for (uint8_t i = 0; i < ll; i++)
            payload = (payload << 8) | data[1 + i];
        uint64_t total = 1 + ll + payload;
        if (total > MAX_NODE_RLP) return 0;
        return (uint32_t)total;
    }

    /* Short list: 0xC0..0xF7 — trie nodes are lists */
    if (b <= 0xF7) return 1 + (b - 0xC0);

    /* Long list: 0xF8..0xFF */
    uint8_t ll = b - 0xF7;
    if ((size_t)(1 + ll) > avail) return 0;
    uint64_t payload = 0;
    for (uint8_t i = 0; i < ll; i++)
        payload = (payload << 8) | data[1 + i];
    uint64_t total = 1 + ll + payload;
    if (total > MAX_NODE_RLP) return 0;
    return (uint32_t)total;
}

/* =========================================================================
 * Size class helper
 * ========================================================================= */

static int size_class_for(uint32_t len) {
    for (int i = 0; i < NUM_SIZE_CLASSES; i++)
        if (len <= SIZE_CLASSES[i]) return i;
    return NUM_SIZE_CLASSES - 1;
}

/* =========================================================================
 * Keccak256 helper
 * ========================================================================= */

static void hash_node(const uint8_t *data, uint32_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    if (len <= UINT16_MAX) {
        keccak_update(&ctx, data, (uint16_t)len);
    } else {
        keccak_update_long(&ctx, data, len);
    }
    keccak_final(&ctx, out);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    bool verify  = false;
    bool dry_run = false;
    uint64_t capacity_override = 0;
    const char *base_path = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--verify") == 0) verify = true;
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = true;
        else if (strcmp(argv[i], "--capacity") == 0 && i + 1 < argc) {
            capacity_override = strtoull(argv[++i], NULL, 10);
        }
        else if (!base_path) base_path = argv[i];
        else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            return 1;
        }
    }

    if (!base_path) {
        fprintf(stderr, "Usage: rebuild_idx <base_path> [--verify] [--dry-run] [--capacity N]\n");
        fprintf(stderr, "  --capacity N  Set hash table capacity (default: node_count, i.e. tight fit)\n");
        return 1;
    }

    /* Build file paths */
    char dat_path[PATH_MAX], idx_path[PATH_MAX], free_path[PATH_MAX];
    snprintf(dat_path,  sizeof(dat_path),  "%s.dat",  base_path);
    snprintf(idx_path,  sizeof(idx_path),  "%s.idx",  base_path);
    snprintf(free_path, sizeof(free_path), "%s.free", base_path);

    /* Open and mmap .dat read-only */
    int dat_fd = open(dat_path, O_RDONLY);
    if (dat_fd < 0) {
        perror(dat_path);
        return 1;
    }

    struct stat st;
    if (fstat(dat_fd, &st) != 0 || st.st_size < PAGE_SIZE_) {
        fprintf(stderr, "ERROR: %s too small or stat failed\n", dat_path);
        close(dat_fd);
        return 1;
    }

    uint8_t *dat_map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, dat_fd, 0);
    if (dat_map == MAP_FAILED) {
        perror("mmap");
        close(dat_fd);
        return 1;
    }
    madvise(dat_map, st.st_size, MADV_SEQUENTIAL);

    /* Parse header */
    const mpt_store_header_t *hdr = (const mpt_store_header_t *)dat_map;
    if (hdr->magic != MPT_STORE_MAGIC) {
        fprintf(stderr, "ERROR: bad magic in %s (got 0x%08X)\n",
                dat_path, hdr->magic);
        munmap(dat_map, st.st_size);
        close(dat_fd);
        return 1;
    }
    if (hdr->version != MPT_STORE_VERSION) {
        fprintf(stderr, "ERROR: unsupported version %u\n", hdr->version);
        munmap(dat_map, st.st_size);
        close(dat_fd);
        return 1;
    }

    printf("=== rebuild_idx ===\n");
    printf("File:      %s\n", dat_path);
    printf("Data size: %" PRIu64 " bytes (%.2f MB)\n",
           hdr->data_size, hdr->data_size / (1024.0 * 1024.0));
    printf("Root hash: ");
    for (int i = 0; i < 32; i++) printf("%02x", hdr->root_hash[i]);
    printf("\n");

    /* Load free set */
    free_set_t fs = {0};
    if (!load_free_set(hdr, free_path, &fs)) {
        fprintf(stderr, "ERROR: failed to load free lists\n");
        munmap(dat_map, st.st_size);
        close(dat_fd);
        return 1;
    }
    printf("Free slots: %u\n", fs.count);

    /* Scan data region */
    const uint8_t *data_base = dat_map + PAGE_SIZE_;
    uint64_t cursor = 0;
    uint64_t node_count = 0;
    uint64_t free_skipped = 0;
    uint64_t total_rlp_bytes = 0;

    /* Collect nodes: {hash, offset, length} */
    typedef struct {
        uint8_t  hash[32];
        uint64_t offset;
        uint32_t length;
    } found_node_t;

    uint32_t nodes_cap = 1024 * 1024;
    found_node_t *nodes = malloc(nodes_cap * sizeof(found_node_t));
    if (!nodes) {
        fprintf(stderr, "ERROR: malloc failed\n");
        free(fs.entries);
        munmap(dat_map, st.st_size);
        close(dat_fd);
        return 1;
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (cursor < hdr->data_size) {
        /* Check if this offset is free */
        uint16_t free_slot = free_set_lookup(&fs, cursor);
        if (free_slot > 0) {
            cursor += free_slot;
            free_skipped++;
            continue;
        }

        /* Read RLP at this position */
        uint64_t remaining = hdr->data_size - cursor;
        if (remaining > MAX_NODE_RLP) remaining = MAX_NODE_RLP;

        const uint8_t *node_data = data_base + cursor;
        uint32_t rlp_len = decode_rlp_total_len(node_data, (size_t)remaining);

        if (rlp_len == 0 || rlp_len > MAX_NODE_RLP) {
            fprintf(stderr, "WARNING: invalid RLP at offset %" PRIu64
                    " (byte 0x%02x), skipping min slot\n",
                    cursor, node_data[0]);
            cursor += SIZE_CLASSES[0]; /* skip minimum slot */
            continue;
        }

        uint16_t slot_size = SIZE_CLASSES[size_class_for(rlp_len)];

        /* Grow array if needed */
        if (node_count >= nodes_cap) {
            nodes_cap *= 2;
            found_node_t *p = realloc(nodes, nodes_cap * sizeof(found_node_t));
            if (!p) {
                fprintf(stderr, "ERROR: realloc failed at %" PRIu64 " nodes\n",
                        node_count);
                break;
            }
            nodes = p;
        }

        /* Hash the node */
        hash_node(node_data, rlp_len, nodes[node_count].hash);
        nodes[node_count].offset = cursor;
        nodes[node_count].length = rlp_len;
        node_count++;
        total_rlp_bytes += rlp_len;

        cursor += slot_size;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double scan_sec = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("\nScan complete:\n");
    printf("  Occupied nodes: %" PRIu64 "\n", node_count);
    printf("  Free slots skipped: %" PRIu64 "\n", free_skipped);
    printf("  Total RLP bytes: %" PRIu64 " (%.2f MB)\n",
           total_rlp_bytes, total_rlp_bytes / (1024.0 * 1024.0));
    printf("  Scan time: %.3f s\n", scan_sec);

    if (dry_run) {
        printf("\n--dry-run: not writing .idx\n");
        free(nodes);
        free(fs.entries);
        munmap(dat_map, st.st_size);
        close(dat_fd);
        return 0;
    }

    /* Decide output path */
    char out_idx[PATH_MAX];
    if (verify) {
        snprintf(out_idx, sizeof(out_idx), "%s.idx.rebuilt", base_path);
    } else {
        snprintf(out_idx, sizeof(out_idx), "%s.idx", base_path);
    }

    /* Create fresh disk_hash */
    uint64_t capacity = capacity_override > 0 ? capacity_override : node_count;
    printf("\nBuilding %s (%" PRIu64 " entries, capacity %" PRIu64 ")...\n",
           out_idx, node_count, capacity);
    clock_gettime(CLOCK_MONOTONIC, &t0);

    disk_table_t *dh = disk_table_create(out_idx, NODE_HASH_SIZE,
                                         sizeof(node_record_t), capacity);
    if (!dh) {
        fprintf(stderr, "ERROR: disk_table_create failed for %s\n", out_idx);
        free(nodes);
        free(fs.entries);
        munmap(dat_map, st.st_size);
        close(dat_fd);
        return 1;
    }

    /* Insert all nodes */
    uint64_t inserted = 0;
    for (uint64_t i = 0; i < node_count; i++) {
        node_record_t rec = {
            .offset   = nodes[i].offset,
            .length   = nodes[i].length,
            .refcount = 1,
        };
        if (disk_table_put(dh, nodes[i].hash, &rec))
            inserted++;
        else
            fprintf(stderr, "WARNING: disk_table_put failed for node %" PRIu64 "\n", i);
    }

    disk_table_sync(dh);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double build_sec = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("  Inserted: %" PRIu64 " / %" PRIu64 "\n", inserted, node_count);
    printf("  Build time: %.3f s\n", build_sec);
    printf("  Index entries: %" PRIu64 "\n", disk_table_count(dh));

    /* Verification: compare against original .idx if it exists */
    if (verify) {
        printf("\n=== Verification ===\n");
        disk_table_t *orig = disk_table_open(idx_path);
        if (!orig) {
            fprintf(stderr, "WARNING: cannot open original %s for verification\n",
                    idx_path);
        } else {
            uint64_t orig_count = disk_table_count(orig);
            uint64_t new_count  = disk_table_count(dh);
            printf("Original entries: %" PRIu64 "\n", orig_count);
            printf("Rebuilt entries:  %" PRIu64 "\n", new_count);

            /* Check every key in the rebuilt index exists in original */
            /* Check every key in the rebuilt index exists in original */
            uint64_t match = 0, mismatch = 0, missing = 0;
            for (uint64_t i = 0; i < node_count; i++) {
                node_record_t orig_rec;
                if (disk_table_get(orig, nodes[i].hash, &orig_rec)) {
                    node_record_t new_rec = {
                        .offset = nodes[i].offset,
                        .length = nodes[i].length,
                        .refcount = 1,
                    };
                    if (orig_rec.offset == new_rec.offset &&
                        orig_rec.length == new_rec.length) {
                        match++;
                    } else {
                        mismatch++;
                        if (mismatch <= 10) {
                            printf("  MISMATCH hash=");
                            for (int j = 0; j < 8; j++)
                                printf("%02x", nodes[i].hash[j]);
                            printf("... orig={off=%" PRIu64 ",len=%u} "
                                   "rebuilt={off=%" PRIu64 ",len=%u}\n",
                                   orig_rec.offset, orig_rec.length,
                                   new_rec.offset, new_rec.length);
                        }
                    }
                } else {
                    missing++;
                    if (missing <= 10) {
                        printf("  EXTRA (not in original) hash=");
                        for (int j = 0; j < 8; j++)
                            printf("%02x", nodes[i].hash[j]);
                        printf("...\n");
                    }
                }
            }

            printf("\nResults:\n");
            printf("  Match:    %" PRIu64 "\n", match);
            printf("  Mismatch: %" PRIu64 "\n", mismatch);
            printf("  Extra:    %" PRIu64 " (in rebuilt but not original)\n", missing);
            printf("  Original has %" PRIu64 " entries not checked "
                   "(may be ghost/orphan nodes)\n",
                   orig_count > match + mismatch ? orig_count - match - mismatch : 0);

            if (mismatch == 0 && missing == 0 && orig_count == new_count) {
                printf("\n  PASS: rebuilt index matches original exactly.\n");
            } else if (mismatch == 0) {
                printf("\n  PARTIAL PASS: no mismatches, but entry counts differ.\n");
            } else {
                printf("\n  FAIL: %"PRIu64" mismatches found.\n", mismatch);
            }

            disk_table_destroy(orig);

            /* Clean up the .rebuilt file */
            unlink(out_idx);
        }
    }

    disk_table_destroy(dh);
    free(nodes);
    free(fs.entries);
    munmap(dat_map, st.st_size);
    close(dat_fd);

    printf("\nDone.\n");
    return 0;
}
