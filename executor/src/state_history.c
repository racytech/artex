#include "state_history.h"
#include "evm_state.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sched.h>

/* =========================================================================
 * CRC-32C (Castagnoli) — hardware-accelerated via SSE4.2
 * ========================================================================= */

#ifdef __SSE4_2__
#include <nmmintrin.h>
static uint32_t crc32c(const uint8_t *data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    /* Process 8 bytes at a time */
    while (len >= 8) {
        crc = (uint32_t)_mm_crc32_u64(crc, *(const uint64_t *)data);
        data += 8;
        len -= 8;
    }
    while (len--) {
        crc = _mm_crc32_u8(crc, *data++);
    }
    return crc ^ 0xFFFFFFFF;
}
#else
/* Software fallback (slice-by-1) */
static uint32_t crc32c(const uint8_t *data, size_t len) {
    static const uint32_t poly = 0x82F63B78;
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++)
            crc = (crc >> 1) ^ (poly & -(crc & 1));
    }
    return crc ^ 0xFFFFFFFF;
}
#endif

/* =========================================================================
 * File format constants
 * ========================================================================= */

#define HIST_MAGIC   0x54534948  /* "HIST" in little-endian */
#define HIST_VERSION 2

/* Index header: magic(4) + version(4) + first_block(8) = 16 bytes */
#define IDX_HEADER_SIZE 16
/* Index entry: block_number(8) + dat_offset(8) = 16 bytes */
#define IDX_ENTRY_SIZE  16

/* Data record header: block_number(8) + record_len(4) + acct_count(2) + slot_count(2) = 16 bytes */
#define DAT_RECORD_HEADER 16

/* account_diff on disk: addr(20) + old_nonce(8) + new_nonce(8) + old_bal(32) + new_bal(32)
 *                       + old_code_hash(32) + new_code_hash(32) + flags(1) = 165 bytes */
#define ACCT_DIFF_DISK_SIZE 165

/* storage_diff on disk: addr(20) + slot(32) + old_val(32) + new_val(32) = 116 bytes */
#define SLOT_DIFF_DISK_SIZE 116

/* Each .dat record ends with a CRC32C (4 bytes) covering header + payload.
 * Total record size = DAT_RECORD_HEADER + accts*165 + slots*116 + 4 */

/* =========================================================================
 * Internal state
 * ========================================================================= */

struct state_history {
    /* Files */
    int         dat_fd;
    int         idx_fd;
    char       *dir_path;

    /* Index state */
    uint64_t    first_block;
    uint64_t    block_count;     /* blocks written so far */

    /* SPSC ring */
    diff_ring_t ring;

    /* Consumer thread */
    pthread_t   consumer_tid;
    atomic_bool stop;

    /* Write position in dat file */
    uint64_t    dat_offset;
};

/* =========================================================================
 * block_diff_t helpers
 * ========================================================================= */

void block_diff_free(block_diff_t *diff) {
    if (!diff) return;
    free(diff->accounts);
    free(diff->storage);
    diff->accounts = NULL;
    diff->storage = NULL;
    diff->account_count = 0;
    diff->storage_count = 0;
}

/* =========================================================================
 * SPSC ring buffer (same pattern as tx_pipeline.c)
 * ========================================================================= */

static void diff_ring_init(diff_ring_t *ring) {
    memset(ring->slots, 0, sizeof(ring->slots));
    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
}

/* Non-blocking push. Returns false if ring is full (diff will be dropped). */
static bool diff_ring_try_push(diff_ring_t *ring, const block_diff_t *diff) {
    size_t h = atomic_load_explicit(&ring->head, memory_order_relaxed);
    size_t t = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if (h - t >= DIFF_RING_CAP)
        return false;  /* ring full */

    ring->slots[h & (DIFF_RING_CAP - 1)] = *diff;
    atomic_store_explicit(&ring->head, h + 1, memory_order_release);
    return true;
}

/* Blocking pop with stop flag. Returns false only if stopped while empty. */
static bool diff_ring_pop(diff_ring_t *ring, block_diff_t *out,
                           const atomic_bool *stop) {
    size_t t = atomic_load_explicit(&ring->tail, memory_order_relaxed);

    for (;;) {
        size_t h = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (h > t)
            break;
        if (stop && atomic_load_explicit(stop, memory_order_relaxed))
            return false;
        sched_yield();
    }

    *out = ring->slots[t & (DIFF_RING_CAP - 1)];
    atomic_store_explicit(&ring->tail, t + 1, memory_order_release);
    return true;
}

/* =========================================================================
 * Serialization helpers (little-endian on disk)
 * ========================================================================= */

static void write_u16(uint8_t *buf, uint16_t v) {
    buf[0] = (uint8_t)(v);
    buf[1] = (uint8_t)(v >> 8);
}

static void write_u32(uint8_t *buf, uint32_t v) {
    buf[0] = (uint8_t)(v);
    buf[1] = (uint8_t)(v >> 8);
    buf[2] = (uint8_t)(v >> 16);
    buf[3] = (uint8_t)(v >> 24);
}

static void write_u64(uint8_t *buf, uint64_t v) {
    for (int i = 0; i < 8; i++)
        buf[i] = (uint8_t)(v >> (i * 8));
}

static uint16_t read_u16(const uint8_t *buf) {
    return (uint16_t)buf[0] | ((uint16_t)buf[1] << 8);
}

static uint32_t read_u32(const uint8_t *buf) {
    return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) |
           ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
}

static uint64_t read_u64(const uint8_t *buf) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v |= ((uint64_t)buf[i]) << (i * 8);
    return v;
}

/* =========================================================================
 * Record serialization
 * ========================================================================= */

static size_t serialize_diff(const block_diff_t *diff, uint8_t **out) {
    size_t acct_size = (size_t)diff->account_count * ACCT_DIFF_DISK_SIZE;
    size_t slot_size = (size_t)diff->storage_count * SLOT_DIFF_DISK_SIZE;
    size_t record_len = acct_size + slot_size;
    size_t total = DAT_RECORD_HEADER + record_len + 4; /* +4 for CRC32 */

    uint8_t *buf = malloc(total);
    if (!buf) return 0;

    /* Record header (record_len excludes CRC32 — stays compatible with reader) */
    write_u64(buf, diff->block_number);
    write_u32(buf + 8, (uint32_t)record_len);
    write_u16(buf + 12, (uint16_t)diff->account_count);
    write_u16(buf + 14, (uint16_t)diff->storage_count);

    /* Account diffs */
    uint8_t *p = buf + DAT_RECORD_HEADER;
    for (uint32_t i = 0; i < diff->account_count; i++) {
        const account_diff_t *a = &diff->accounts[i];
        memcpy(p, a->addr.bytes, 20); p += 20;
        write_u64(p, a->old_nonce); p += 8;
        write_u64(p, a->new_nonce); p += 8;
        uint256_to_bytes(&a->old_balance, p); p += 32;
        uint256_to_bytes(&a->new_balance, p); p += 32;
        memcpy(p, a->old_code_hash.bytes, 32); p += 32;
        memcpy(p, a->new_code_hash.bytes, 32); p += 32;
        *p++ = a->flags;
    }

    /* Storage diffs */
    for (uint32_t i = 0; i < diff->storage_count; i++) {
        const storage_diff_t *s = &diff->storage[i];
        memcpy(p, s->addr.bytes, 20); p += 20;
        uint256_to_bytes(&s->slot, p); p += 32;
        uint256_to_bytes(&s->old_value, p); p += 32;
        uint256_to_bytes(&s->new_value, p); p += 32;
    }

    /* CRC32C over header + payload (not the CRC itself) */
    uint32_t crc = crc32c(buf, DAT_RECORD_HEADER + record_len);
    write_u32(p, crc);

    *out = buf;
    return total;
}

static bool deserialize_diff(const uint8_t *buf, size_t buf_len,
                              block_diff_t *out) {
    if (buf_len < DAT_RECORD_HEADER) return false;

    out->block_number = read_u64(buf);
    uint32_t record_len = read_u32(buf + 8);
    out->account_count = read_u16(buf + 12);
    out->storage_count = read_u16(buf + 14);

    size_t expected = DAT_RECORD_HEADER +
                      (size_t)out->account_count * ACCT_DIFF_DISK_SIZE +
                      (size_t)out->storage_count * SLOT_DIFF_DISK_SIZE;
    if (buf_len < expected) return false;
    (void)record_len;

    /* Account diffs */
    out->accounts = NULL;
    if (out->account_count > 0) {
        out->accounts = calloc(out->account_count, sizeof(account_diff_t));
        if (!out->accounts) return false;
    }
    const uint8_t *p = buf + DAT_RECORD_HEADER;
    for (uint32_t i = 0; i < out->account_count; i++) {
        account_diff_t *a = &out->accounts[i];
        memcpy(a->addr.bytes, p, 20); p += 20;
        a->old_nonce = read_u64(p); p += 8;
        a->new_nonce = read_u64(p); p += 8;
        a->old_balance = uint256_from_bytes(p, 32); p += 32;
        a->new_balance = uint256_from_bytes(p, 32); p += 32;
        memcpy(a->old_code_hash.bytes, p, 32); p += 32;
        memcpy(a->new_code_hash.bytes, p, 32); p += 32;
        a->flags = *p++;
    }

    /* Storage diffs */
    out->storage = NULL;
    if (out->storage_count > 0) {
        out->storage = calloc(out->storage_count, sizeof(storage_diff_t));
        if (!out->storage) { free(out->accounts); return false; }
    }
    for (uint32_t i = 0; i < out->storage_count; i++) {
        storage_diff_t *s = &out->storage[i];
        memcpy(s->addr.bytes, p, 20); p += 20;
        s->slot = uint256_from_bytes(p, 32); p += 32;
        s->old_value = uint256_from_bytes(p, 32); p += 32;
        s->new_value = uint256_from_bytes(p, 32); p += 32;
    }

    return true;
}

/* =========================================================================
 * Consumer thread
 * ========================================================================= */

static void *consumer_thread(void *arg) {
    state_history_t *sh = (state_history_t *)arg;
    uint64_t blocks_since_sync = 0;

    while (!atomic_load_explicit(&sh->stop, memory_order_relaxed)) {
        block_diff_t diff;
        if (!diff_ring_pop(&sh->ring, &diff, &sh->stop))
            break;  /* stopped */

        /* Serialize */
        uint8_t *buf;
        size_t buf_len = serialize_diff(&diff, &buf);
        if (buf_len == 0) {
            block_diff_free(&diff);
            continue;
        }

        /* Write data record */
        uint64_t offset = sh->dat_offset;
        ssize_t nw = pwrite(sh->dat_fd, buf, buf_len, (off_t)offset);
        free(buf);
        if (nw < 0 || (size_t)nw != buf_len) {
            fprintf(stderr, "state_history: write failed for block %lu\n",
                    diff.block_number);
            block_diff_free(&diff);
            continue;
        }
        sh->dat_offset += buf_len;

        /* Write index entry */
        uint8_t idx_entry[IDX_ENTRY_SIZE];
        write_u64(idx_entry, diff.block_number);
        write_u64(idx_entry + 8, offset);
        off_t idx_pos = (off_t)(IDX_HEADER_SIZE + sh->block_count * IDX_ENTRY_SIZE);
        pwrite(sh->idx_fd, idx_entry, IDX_ENTRY_SIZE, idx_pos);

        if (sh->block_count == 0)
            sh->first_block = diff.block_number;
        sh->block_count++;

        block_diff_free(&diff);

        /* Periodic sync to disk */
        blocks_since_sync++;
        if (blocks_since_sync >= 256) {
            fdatasync(sh->dat_fd);
            fdatasync(sh->idx_fd);
            blocks_since_sync = 0;
        }
    }

    /* Drain remaining entries */
    for (;;) {
        size_t h = atomic_load_explicit(&sh->ring.head, memory_order_acquire);
        size_t t = atomic_load_explicit(&sh->ring.tail, memory_order_relaxed);
        if (h == t) break;

        block_diff_t diff;
        diff_ring_pop(&sh->ring, &diff, NULL);

        uint8_t *buf;
        size_t buf_len = serialize_diff(&diff, &buf);
        if (buf_len > 0) {
            uint64_t offset = sh->dat_offset;
            pwrite(sh->dat_fd, buf, buf_len, (off_t)offset);
            sh->dat_offset += buf_len;
            free(buf);

            uint8_t idx_entry[IDX_ENTRY_SIZE];
            write_u64(idx_entry, diff.block_number);
            write_u64(idx_entry + 8, offset);
            off_t idx_pos = (off_t)(IDX_HEADER_SIZE + sh->block_count * IDX_ENTRY_SIZE);
            pwrite(sh->idx_fd, idx_entry, IDX_ENTRY_SIZE, idx_pos);

            if (sh->block_count == 0)
                sh->first_block = diff.block_number;
            sh->block_count++;
        }
        block_diff_free(&diff);
    }

    fdatasync(sh->dat_fd);
    fdatasync(sh->idx_fd);
    return NULL;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

state_history_t *state_history_create(const char *dir_path) {
    if (!dir_path) return NULL;

    /* Ensure directory exists */
    mkdir(dir_path, 0755);

    state_history_t *sh = calloc(1, sizeof(state_history_t));
    if (!sh) return NULL;

    sh->dir_path = strdup(dir_path);
    sh->dat_fd = -1;
    sh->idx_fd = -1;

    /* Open data file */
    char path[512];
    snprintf(path, sizeof(path), "%s/state_history.dat", dir_path);
    sh->dat_fd = open(path, O_RDWR | O_CREAT, 0644);
    if (sh->dat_fd < 0) goto fail;

    /* Open index file */
    snprintf(path, sizeof(path), "%s/state_history.idx", dir_path);
    sh->idx_fd = open(path, O_RDWR | O_CREAT, 0644);
    if (sh->idx_fd < 0) goto fail;

    /* Read or write index header */
    uint8_t hdr[IDX_HEADER_SIZE];
    ssize_t nr = pread(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
    if (nr == IDX_HEADER_SIZE && read_u32(hdr) == HIST_MAGIC) {
        /* Existing index — resume */
        sh->first_block = read_u64(hdr + 8);

        /* Count existing entries from file size */
        off_t idx_size = lseek(sh->idx_fd, 0, SEEK_END);
        if (idx_size > IDX_HEADER_SIZE)
            sh->block_count = (uint64_t)(idx_size - IDX_HEADER_SIZE) / IDX_ENTRY_SIZE;

        /* Validate tail: walk backwards, verify last record's CRC */
        while (sh->block_count > 0) {
            uint64_t i = sh->block_count - 1;
            uint8_t ie[IDX_ENTRY_SIZE];
            off_t ie_pos = (off_t)(IDX_HEADER_SIZE + i * IDX_ENTRY_SIZE);
            if (pread(sh->idx_fd, ie, IDX_ENTRY_SIZE, ie_pos) != IDX_ENTRY_SIZE) {
                sh->block_count--;
                continue;
            }

            uint64_t bn = read_u64(ie);
            uint64_t doff = read_u64(ie + 8);

            /* Read record header */
            uint8_t rh[DAT_RECORD_HEADER];
            if (pread(sh->dat_fd, rh, DAT_RECORD_HEADER, (off_t)doff) != DAT_RECORD_HEADER) {
                sh->block_count--;
                continue;
            }

            /* Block number in record must match index */
            if (read_u64(rh) != bn) {
                sh->block_count--;
                continue;
            }

            uint32_t rlen = read_u32(rh + 8);
            size_t payload = DAT_RECORD_HEADER + rlen;
            size_t total = payload + 4;

            uint8_t *rec = malloc(total);
            if (!rec) { sh->block_count--; continue; }

            if (pread(sh->dat_fd, rec, total, (off_t)doff) != (ssize_t)total) {
                free(rec);
                sh->block_count--;
                continue;
            }

            uint32_t stored = read_u32(rec + payload);
            uint32_t actual = crc32c(rec, payload);
            free(rec);

            if (stored != actual) {
                sh->block_count--;
                continue;
            }

            /* Last record is valid — truncate files to this point */
            sh->dat_offset = doff + total;
            ftruncate(sh->dat_fd, (off_t)sh->dat_offset);
            ftruncate(sh->idx_fd,
                      (off_t)(IDX_HEADER_SIZE + sh->block_count * IDX_ENTRY_SIZE));
            break;
        }

        if (sh->block_count == 0) {
            /* All records corrupt or empty — start fresh */
            ftruncate(sh->dat_fd, 0);
            sh->dat_offset = 0;
            memset(hdr, 0, IDX_HEADER_SIZE);
            write_u32(hdr, HIST_MAGIC);
            write_u32(hdr + 4, HIST_VERSION);
            write_u64(hdr + 8, 0);
            pwrite(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
            ftruncate(sh->idx_fd, IDX_HEADER_SIZE);
        }
    } else {
        /* New index — write header */
        memset(hdr, 0, IDX_HEADER_SIZE);
        write_u32(hdr, HIST_MAGIC);
        write_u32(hdr + 4, HIST_VERSION);
        write_u64(hdr + 8, 0);  /* first_block set on first write */
        pwrite(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
        sh->dat_offset = 0;
    }

    diff_ring_init(&sh->ring);
    atomic_store_explicit(&sh->stop, false, memory_order_relaxed);

    /* Start consumer thread */
    if (pthread_create(&sh->consumer_tid, NULL, consumer_thread, sh) != 0)
        goto fail;

    return sh;

fail:
    if (sh->dat_fd >= 0) close(sh->dat_fd);
    if (sh->idx_fd >= 0) close(sh->idx_fd);
    free(sh->dir_path);
    free(sh);
    return NULL;
}

void state_history_destroy(state_history_t *sh) {
    if (!sh) return;

    /* Signal stop and wait for consumer */
    atomic_store_explicit(&sh->stop, true, memory_order_release);
    pthread_join(sh->consumer_tid, NULL);

    /* Update index header with final first_block */
    if (sh->block_count > 0) {
        uint8_t hdr[IDX_HEADER_SIZE];
        write_u32(hdr, HIST_MAGIC);
        write_u32(hdr + 4, HIST_VERSION);
        write_u64(hdr + 8, sh->first_block);
        pwrite(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
        fdatasync(sh->idx_fd);
    }

    close(sh->dat_fd);
    close(sh->idx_fd);
    free(sh->dir_path);
    free(sh);
}

/* =========================================================================
 * Producer: capture diff from evm_state
 * ========================================================================= */

void state_history_capture(state_history_t *sh, evm_state_t *es,
                            uint64_t block_number) {
    if (!sh || !es) return;

    block_diff_t diff;
    memset(&diff, 0, sizeof(diff));
    diff.block_number = block_number;

    evm_state_collect_block_diff(es, &diff);

    if (!diff_ring_try_push(&sh->ring, &diff)) {
        fprintf(stderr, "state_history: ring full, dropped diff for block %lu\n",
                block_number);
        block_diff_free(&diff);
    }
}

/* =========================================================================
 * Query API
 * ========================================================================= */

bool state_history_get_diff(const state_history_t *sh,
                             uint64_t block_number,
                             block_diff_t *out) {
    if (!sh || !out) return false;
    if (sh->block_count == 0) return false;
    if (block_number < sh->first_block) return false;

    uint64_t idx = block_number - sh->first_block;
    if (idx >= sh->block_count) return false;

    /* Read index entry */
    uint8_t idx_entry[IDX_ENTRY_SIZE];
    off_t idx_pos = (off_t)(IDX_HEADER_SIZE + idx * IDX_ENTRY_SIZE);
    if (pread(sh->idx_fd, idx_entry, IDX_ENTRY_SIZE, idx_pos) != IDX_ENTRY_SIZE)
        return false;

    uint64_t stored_bn = read_u64(idx_entry);
    uint64_t dat_offset = read_u64(idx_entry + 8);

    if (stored_bn != block_number)
        return false;  /* sanity check */

    /* Read record header to get size */
    uint8_t rec_hdr[DAT_RECORD_HEADER];
    if (pread(sh->dat_fd, rec_hdr, DAT_RECORD_HEADER, (off_t)dat_offset) != DAT_RECORD_HEADER)
        return false;

    uint32_t record_len = read_u32(rec_hdr + 8);
    size_t payload = DAT_RECORD_HEADER + record_len;
    size_t total = payload + 4; /* +4 for CRC32C */

    uint8_t *buf = malloc(total);
    if (!buf) return false;

    if (pread(sh->dat_fd, buf, total, (off_t)dat_offset) != (ssize_t)total) {
        free(buf);
        return false;
    }

    /* Verify CRC32C */
    uint32_t stored_crc = read_u32(buf + payload);
    uint32_t actual_crc = crc32c(buf, payload);
    if (stored_crc != actual_crc) {
        free(buf);
        return false;
    }

    bool ok = deserialize_diff(buf, payload, out);
    free(buf);
    return ok;
}

bool state_history_range(const state_history_t *sh,
                          uint64_t *first, uint64_t *last) {
    if (!sh || sh->block_count == 0) return false;
    if (first) *first = sh->first_block;
    if (last) *last = sh->first_block + sh->block_count - 1;
    return true;
}
