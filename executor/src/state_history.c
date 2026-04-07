#include "state_history.h"
#include "evm_state.h"
#include "state.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>

/* =========================================================================
 * CRC-32C (Castagnoli) — hardware-accelerated via SSE4.2
 * ========================================================================= */

#ifdef __SSE4_2__
#include <nmmintrin.h>
static uint32_t crc32c(const uint8_t *data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
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
 * File format constants (v3)
 * ========================================================================= */

#define HIST_MAGIC   0x54534948  /* "HIST" in little-endian */
#define HIST_VERSION 1

/* Index header: magic(4) + version(4) + first_block(8) = 16 bytes */
#define IDX_HEADER_SIZE 16
/* Index entry: block_number(8) + dat_offset(8) = 16 bytes */
#define IDX_ENTRY_SIZE  16

/* Data record header: block_number(8) + record_len(4) + group_count(2) + reserved(2) = 16 bytes */
#define DAT_RECORD_HEADER 16

/* Per-group fixed header: addr(20) + flags(1) + field_mask(1) + slot_count(2) = 24 bytes */
#define GROUP_HEADER_SIZE 24

/* =========================================================================
 * Internal state
 * ========================================================================= */

struct state_history {
    int         dat_fd;
    int         idx_fd;
    char       *dir_path;

    uint64_t    first_block;
    uint64_t    block_count;

    diff_ring_t ring;

    pthread_t   consumer_tid;
    atomic_bool stop;

    uint64_t    dat_offset;
};

/* =========================================================================
 * block_diff_t helpers
 * ========================================================================= */

void block_diff_free(block_diff_t *diff) {
    if (!diff) return;
    for (uint16_t i = 0; i < diff->group_count; i++)
        free(diff->groups[i].slots);
    free(diff->groups);
    diff->groups = NULL;
    diff->group_count = 0;
}

void block_diff_clone(const block_diff_t *src, block_diff_t *dst) {
    dst->block_number = src->block_number;
    dst->group_count = src->group_count;
    if (src->group_count == 0) {
        dst->groups = NULL;
        return;
    }
    dst->groups = malloc(src->group_count * sizeof(addr_diff_t));
    if (!dst->groups) { dst->group_count = 0; return; }
    for (uint16_t i = 0; i < src->group_count; i++) {
        dst->groups[i] = src->groups[i];
        if (src->groups[i].slot_count > 0) {
            size_t sz = src->groups[i].slot_count * sizeof(slot_diff_t);
            dst->groups[i].slots = malloc(sz);
            if (!dst->groups[i].slots) { dst->groups[i].slot_count = 0; continue; }
            memcpy(dst->groups[i].slots, src->groups[i].slots, sz);
        } else {
            dst->groups[i].slots = NULL;
        }
    }
}

/* =========================================================================
 * SPSC ring buffer
 * ========================================================================= */

#ifdef __x86_64__
#include <immintrin.h>
#define SPIN_PAUSE() _mm_pause()
#else
#define SPIN_PAUSE() ((void)0)
#endif

#define SPIN_TRIES 64

static void diff_ring_init(diff_ring_t *ring) {
    memset(ring->slots, 0, sizeof(ring->slots));
    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
    pthread_mutex_init(&ring->mtx, NULL);
    pthread_cond_init(&ring->not_empty, NULL);
}

static void diff_ring_destroy(diff_ring_t *ring) {
    pthread_cond_destroy(&ring->not_empty);
    pthread_mutex_destroy(&ring->mtx);
}

static bool diff_ring_try_push(diff_ring_t *ring, const block_diff_t *diff) {
    size_t h = atomic_load_explicit(&ring->head, memory_order_relaxed);
    size_t t = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (h - t >= DIFF_RING_CAP)
        return false;
    ring->slots[h & (DIFF_RING_CAP - 1)] = *diff;
    atomic_store_explicit(&ring->head, h + 1, memory_order_release);

    /* Signal consumer that data is available */
    pthread_mutex_lock(&ring->mtx);
    pthread_cond_signal(&ring->not_empty);
    pthread_mutex_unlock(&ring->mtx);
    return true;
}

static bool diff_ring_pop(diff_ring_t *ring, block_diff_t *out,
                           const atomic_bool *stop) {
    size_t t = atomic_load_explicit(&ring->tail, memory_order_relaxed);

    /* Fast path: spin briefly */
    for (int i = 0; i < SPIN_TRIES; i++) {
        size_t h = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (h > t) goto pop;
        if (stop && atomic_load_explicit(stop, memory_order_relaxed))
            return false;
        SPIN_PAUSE();
    }

    /* Slow path: block on condvar */
    pthread_mutex_lock(&ring->mtx);
    for (;;) {
        size_t h = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (h > t) {
            pthread_mutex_unlock(&ring->mtx);
            goto pop;
        }
        if (stop && atomic_load_explicit(stop, memory_order_relaxed)) {
            pthread_mutex_unlock(&ring->mtx);
            return false;
        }
        pthread_cond_wait(&ring->not_empty, &ring->mtx);
    }

pop:
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
 * Record serialization (v3 — grouped, new-values-only, field bitmask)
 * ========================================================================= */

/** Compute serialized size of a group (excluding addr/flags/mask/slot_count header). */
static size_t group_payload_size(const addr_diff_t *g) {
    size_t sz = 0;
    if (g->field_mask & FIELD_NONCE)     sz += 8;
    if (g->field_mask & FIELD_BALANCE)   sz += 32;
    if (g->field_mask & FIELD_CODE_HASH) sz += 32;
    sz += (size_t)g->slot_count * 64;  /* slot(32) + value(32) */
    return sz;
}

static size_t serialize_diff(const block_diff_t *diff, uint8_t **out) {
    /* Compute total record_len (everything after the 16-byte header, before CRC) */
    size_t record_len = 0;
    for (uint16_t i = 0; i < diff->group_count; i++)
        record_len += GROUP_HEADER_SIZE + group_payload_size(&diff->groups[i]);

    size_t total = DAT_RECORD_HEADER + record_len + 4; /* +4 CRC32 */
    uint8_t *buf = malloc(total);
    if (!buf) return 0;

    /* Record header */
    write_u64(buf, diff->block_number);
    write_u32(buf + 8, (uint32_t)record_len);
    write_u16(buf + 12, diff->group_count);
    write_u16(buf + 14, 0); /* reserved */

    /* Groups */
    uint8_t *p = buf + DAT_RECORD_HEADER;
    for (uint16_t i = 0; i < diff->group_count; i++) {
        const addr_diff_t *g = &diff->groups[i];

        /* Group header */
        memcpy(p, g->addr.bytes, 20); p += 20;
        *p++ = g->flags;
        *p++ = g->field_mask;
        write_u16(p, g->slot_count); p += 2;

        /* Conditional account fields */
        if (g->field_mask & FIELD_NONCE) {
            write_u64(p, g->nonce); p += 8;
        }
        if (g->field_mask & FIELD_BALANCE) {
            uint256_to_bytes(&g->balance, p); p += 32;
        }
        if (g->field_mask & FIELD_CODE_HASH) {
            memcpy(p, g->code_hash.bytes, 32); p += 32;
        }

        /* Storage slots */
        for (uint16_t j = 0; j < g->slot_count; j++) {
            uint256_to_bytes(&g->slots[j].slot, p); p += 32;
            uint256_to_bytes(&g->slots[j].value, p); p += 32;
        }
    }

    /* CRC32C */
    uint32_t crc = crc32c(buf, DAT_RECORD_HEADER + record_len);
    write_u32(p, crc);

    *out = buf;
    return total;
}

static bool deserialize_diff(const uint8_t *buf, size_t buf_len,
                              block_diff_t *out) {
    if (buf_len < DAT_RECORD_HEADER) return false;

    out->block_number = read_u64(buf);
    /* record_len at buf+8, used by caller for sizing */
    out->group_count = read_u16(buf + 12);

    out->groups = NULL;
    if (out->group_count > 0) {
        out->groups = calloc(out->group_count, sizeof(addr_diff_t));
        if (!out->groups) return false;
    }

    const uint8_t *p = buf + DAT_RECORD_HEADER;
    const uint8_t *end = buf + buf_len;

    for (uint16_t i = 0; i < out->group_count; i++) {
        addr_diff_t *g = &out->groups[i];

        if (p + GROUP_HEADER_SIZE > end) goto fail;
        memcpy(g->addr.bytes, p, 20); p += 20;
        g->flags = *p++;
        g->field_mask = *p++;
        g->slot_count = read_u16(p); p += 2;

        if (g->field_mask & FIELD_NONCE) {
            if (p + 8 > end) goto fail;
            g->nonce = read_u64(p); p += 8;
        }
        if (g->field_mask & FIELD_BALANCE) {
            if (p + 32 > end) goto fail;
            g->balance = uint256_from_bytes(p, 32); p += 32;
        }
        if (g->field_mask & FIELD_CODE_HASH) {
            if (p + 32 > end) goto fail;
            memcpy(g->code_hash.bytes, p, 32); p += 32;
        }

        g->slots = NULL;
        if (g->slot_count > 0) {
            if (p + (size_t)g->slot_count * 64 > end) goto fail;
            g->slots = calloc(g->slot_count, sizeof(slot_diff_t));
            if (!g->slots) goto fail;
            for (uint16_t j = 0; j < g->slot_count; j++) {
                g->slots[j].slot = uint256_from_bytes(p, 32); p += 32;
                g->slots[j].value = uint256_from_bytes(p, 32); p += 32;
            }
        }
    }
    return true;

fail:
    block_diff_free(out);
    return false;
}

/* =========================================================================
 * Consumer thread
 * ========================================================================= */

static void *consumer_thread(void *arg) {
    state_history_t *sh = (state_history_t *)arg;
    uint64_t blocks_since_sync = 0;
    uint64_t interval_accounts = 0;
    uint64_t interval_slots = 0;
    uint64_t interval_bytes = 0;
    uint64_t interval_created = 0;

    while (!atomic_load_explicit(&sh->stop, memory_order_relaxed)) {
        block_diff_t diff;
        if (!diff_ring_pop(&sh->ring, &diff, &sh->stop))
            break;

        uint8_t *buf;
        size_t buf_len = serialize_diff(&diff, &buf);
        if (buf_len == 0) {
            block_diff_free(&diff);
            continue;
        }

        uint64_t offset = sh->dat_offset;
        ssize_t nw = pwrite(sh->dat_fd, buf, buf_len, (off_t)offset);
        free(buf);
        if (nw < 0 || (size_t)nw != buf_len) {
            LOG_HIST_ERROR( "history: write failed for block %lu",
                   diff.block_number);
            block_diff_free(&diff);
            continue;
        }
        sh->dat_offset += buf_len;

        uint8_t idx_entry[IDX_ENTRY_SIZE];
        write_u64(idx_entry, diff.block_number);
        write_u64(idx_entry + 8, offset);
        off_t idx_pos = (off_t)(IDX_HEADER_SIZE + sh->block_count * IDX_ENTRY_SIZE);
        pwrite(sh->idx_fd, idx_entry, IDX_ENTRY_SIZE, idx_pos);

        if (sh->block_count == 0)
            sh->first_block = diff.block_number;
        sh->block_count++;

        /* Accumulate stats for periodic log */
        interval_accounts += diff.group_count;
        for (uint16_t i = 0; i < diff.group_count; i++) {
            interval_slots += diff.groups[i].slot_count;
            if (diff.groups[i].flags & ACCT_DIFF_CREATED)
                interval_created++;
        }
        interval_bytes += buf_len;

        block_diff_free(&diff);

        blocks_since_sync++;
        if (blocks_since_sync >= 256) {
            /* No fdatasync — OS page cache handles writeback */

            LOG_HIST_INFO(
                   "history: blk %lu (+256)  %lu accts  %lu slots  %lu created  %.1f KB",
                   diff.block_number,
                   interval_accounts, interval_slots, interval_created,
                   (double)interval_bytes / 1024.0);

            blocks_since_sync = 0;
            interval_accounts = 0;
            interval_slots = 0;
            interval_bytes = 0;
            interval_created = 0;
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
            ssize_t nw = pwrite(sh->dat_fd, buf, buf_len, (off_t)offset);
            free(buf);
            if (nw < 0 || (size_t)nw != buf_len) {
                LOG_HIST_ERROR("history: drain write failed for block %lu",
                       diff.block_number);
                block_diff_free(&diff);
                continue;
            }
            sh->dat_offset += buf_len;

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

    return NULL;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

state_history_t *state_history_create(const char *dir_path) {
    if (!dir_path) return NULL;

    mkdir(dir_path, 0755);

    state_history_t *sh = calloc(1, sizeof(state_history_t));
    if (!sh) return NULL;

    sh->dir_path = strdup(dir_path);
    sh->dat_fd = -1;
    sh->idx_fd = -1;

    char path[512];
    snprintf(path, sizeof(path), "%s/state_history.dat", dir_path);
    sh->dat_fd = open(path, O_RDWR | O_CREAT, 0644);
    if (sh->dat_fd < 0) goto fail;

    snprintf(path, sizeof(path), "%s/state_history.idx", dir_path);
    sh->idx_fd = open(path, O_RDWR | O_CREAT, 0644);
    if (sh->idx_fd < 0) goto fail;

    /* Read or write index header */
    uint8_t hdr[IDX_HEADER_SIZE];
    ssize_t nr = pread(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
    if (nr == IDX_HEADER_SIZE && read_u32(hdr) == HIST_MAGIC) {
        uint32_t version = read_u32(hdr + 4);
        if (version != HIST_VERSION) {
            LOG_HIST_ERROR( "history: incompatible version %u (expected %u), "
               "delete files to regenerate", version, HIST_VERSION);
            goto fail;
        }

        sh->first_block = read_u64(hdr + 8);

        off_t idx_size = lseek(sh->idx_fd, 0, SEEK_END);
        if (idx_size > (off_t)IDX_HEADER_SIZE)
            sh->block_count = (uint64_t)(idx_size - (off_t)IDX_HEADER_SIZE) / IDX_ENTRY_SIZE;

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

            uint8_t rh[DAT_RECORD_HEADER];
            if (pread(sh->dat_fd, rh, DAT_RECORD_HEADER, (off_t)doff) != DAT_RECORD_HEADER) {
                sh->block_count--;
                continue;
            }

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

            sh->dat_offset = doff + total;
            ftruncate(sh->dat_fd, (off_t)sh->dat_offset);
            ftruncate(sh->idx_fd,
                      (off_t)(IDX_HEADER_SIZE + sh->block_count * IDX_ENTRY_SIZE));
            break;
        }

        if (sh->block_count == 0) {
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
        memset(hdr, 0, IDX_HEADER_SIZE);
        write_u32(hdr, HIST_MAGIC);
        write_u32(hdr + 4, HIST_VERSION);
        write_u64(hdr + 8, 0);
        pwrite(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
        sh->dat_offset = 0;
    }

    diff_ring_init(&sh->ring);
    atomic_store_explicit(&sh->stop, false, memory_order_relaxed);

    if (pthread_create(&sh->consumer_tid, NULL, consumer_thread, sh) != 0)
        goto fail;

    if (sh->block_count > 0)
        LOG_HIST_INFO( "history: resumed at %s - %lu blocks (first: %lu)",
               dir_path, sh->block_count, sh->first_block);
    else
        LOG_HIST_INFO( "history: created at %s", dir_path);

    return sh;

fail:
    LOG_HIST_ERROR( "history: failed to create at %s", dir_path);
    if (sh->dat_fd >= 0) close(sh->dat_fd);
    if (sh->idx_fd >= 0) close(sh->idx_fd);
    free(sh->dir_path);
    free(sh);
    return NULL;
}

void state_history_destroy(state_history_t *sh) {
    if (!sh) return;

    atomic_store_explicit(&sh->stop, true, memory_order_release);

    /* Wake consumer if blocked on condvar */
    pthread_mutex_lock(&sh->ring.mtx);
    pthread_cond_signal(&sh->ring.not_empty);
    pthread_mutex_unlock(&sh->ring.mtx);

    pthread_join(sh->consumer_tid, NULL);
    diff_ring_destroy(&sh->ring);

    if (sh->block_count > 0) {
        uint8_t hdr[IDX_HEADER_SIZE];
        write_u32(hdr, HIST_MAGIC);
        write_u32(hdr + 4, HIST_VERSION);
        write_u64(hdr + 8, sh->first_block);
        pwrite(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
    }

    close(sh->dat_fd);
    close(sh->idx_fd);
    free(sh->dir_path);
    free(sh);
}

/* =========================================================================
 * Producer
 * ========================================================================= */

void state_history_capture(state_history_t *sh, evm_state_t *es,
                            uint64_t block_number) {
    if (!sh || !es) return;

    block_diff_t diff;
    memset(&diff, 0, sizeof(diff));
    diff.block_number = block_number;

    evm_state_collect_block_diff(es, &diff);

    if (!diff_ring_try_push(&sh->ring, &diff)) {
        LOG_HIST_WARN( "history: ring full, dropped block %lu",
               block_number);
        block_diff_free(&diff);
    }
}

void state_history_push(state_history_t *sh, block_diff_t *diff) {
    if (!sh || !diff) return;

    if (!diff_ring_try_push(&sh->ring, diff)) {
        LOG_HIST_WARN( "history: ring full, dropped block %lu",
               diff->block_number);
        block_diff_free(diff);
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

    uint8_t idx_entry[IDX_ENTRY_SIZE];
    off_t idx_pos = (off_t)(IDX_HEADER_SIZE + idx * IDX_ENTRY_SIZE);
    if (pread(sh->idx_fd, idx_entry, IDX_ENTRY_SIZE, idx_pos) != IDX_ENTRY_SIZE)
        return false;

    uint64_t stored_bn = read_u64(idx_entry);
    uint64_t dat_offset = read_u64(idx_entry + 8);

    if (stored_bn != block_number)
        return false;

    uint8_t rec_hdr[DAT_RECORD_HEADER];
    if (pread(sh->dat_fd, rec_hdr, DAT_RECORD_HEADER, (off_t)dat_offset) != DAT_RECORD_HEADER)
        return false;

    uint32_t record_len = read_u32(rec_hdr + 8);
    size_t payload = DAT_RECORD_HEADER + record_len;
    size_t total = payload + 4;

    uint8_t *buf = malloc(total);
    if (!buf) return false;

    if (pread(sh->dat_fd, buf, total, (off_t)dat_offset) != (ssize_t)total) {
        free(buf);
        return false;
    }

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

uint64_t state_history_disk_bytes(const state_history_t *sh) {
    return sh ? sh->dat_offset : 0;
}

uint64_t state_history_block_count(const state_history_t *sh) {
    return sh ? sh->block_count : 0;
}

/* =========================================================================
 * Truncation
 * ========================================================================= */

void state_history_truncate(state_history_t *sh, uint64_t last_block) {
    if (!sh || sh->block_count == 0) return;

    /* Nothing to truncate if last_block is at or past our last recorded */
    uint64_t our_last = sh->first_block + sh->block_count - 1;
    if (last_block >= our_last) return;

    if (last_block < sh->first_block) {
        /* Truncate everything */
        ftruncate(sh->dat_fd, 0);
        sh->dat_offset = 0;
        sh->block_count = 0;
        sh->first_block = 0;

        uint8_t hdr[IDX_HEADER_SIZE];
        write_u32(hdr, HIST_MAGIC);
        write_u32(hdr + 4, HIST_VERSION);
        write_u64(hdr + 8, 0);
        pwrite(sh->idx_fd, hdr, IDX_HEADER_SIZE, 0);
        ftruncate(sh->idx_fd, IDX_HEADER_SIZE);

        LOG_HIST_INFO("history: truncated all blocks (requested last=%lu < first=%lu)",
               last_block, sh->first_block);
        return;
    }

    /* Keep blocks [first_block .. last_block] */
    uint64_t keep = last_block - sh->first_block + 1;

    /* Read the last kept entry to find the dat truncation point */
    uint8_t ie[IDX_ENTRY_SIZE];
    off_t ie_pos = (off_t)(IDX_HEADER_SIZE + (keep - 1) * IDX_ENTRY_SIZE);
    if (pread(sh->idx_fd, ie, IDX_ENTRY_SIZE, ie_pos) != IDX_ENTRY_SIZE) {
        LOG_HIST_ERROR("history: truncate failed to read last kept index entry");
        return;
    }

    uint64_t dat_off = read_u64(ie + 8);

    /* Read record header to get its total size */
    uint8_t rh[DAT_RECORD_HEADER];
    if (pread(sh->dat_fd, rh, DAT_RECORD_HEADER, (off_t)dat_off) != DAT_RECORD_HEADER) {
        LOG_HIST_ERROR("history: truncate failed to read record header");
        return;
    }
    uint32_t rlen = read_u32(rh + 8);
    uint64_t new_dat_end = dat_off + DAT_RECORD_HEADER + rlen + 4; /* +4 CRC */

    ftruncate(sh->dat_fd, (off_t)new_dat_end);
    ftruncate(sh->idx_fd, (off_t)(IDX_HEADER_SIZE + keep * IDX_ENTRY_SIZE));

    uint64_t removed = sh->block_count - keep;
    sh->block_count = keep;
    sh->dat_offset = new_dat_end;

    LOG_HIST_INFO("history: truncated %lu blocks, kept %lu (first=%lu last=%lu)",
           removed, keep, sh->first_block, last_block);
}

/* =========================================================================
 * Forward reconstruction: apply_diff + replay
 * ========================================================================= */

void state_history_apply_diff(evm_state_t *es, const block_diff_t *diff) {
    if (!es || !diff) return;

    for (uint16_t i = 0; i < diff->group_count; i++) {
        const addr_diff_t *g = &diff->groups[i];

        /* Don't skip any entry — even CREATED+empty entries need processing
         * since they represent accounts that must exist in the trie pre-EIP-161. */

        if (g->flags & ACCT_DIFF_DESTRUCTED) {
            /* Zero the account: set nonce/balance to 0, code_hash to empty */
            evm_state_set_nonce(es, &g->addr, 0);
            uint256_t zero = UINT256_ZERO_INIT;
            evm_state_set_balance(es, &g->addr, &zero);
            /* Storage slots from the diff will set the final values below */
        }

        if (g->field_mask & FIELD_NONCE)
            evm_state_set_nonce(es, &g->addr, g->nonce);

        if (g->field_mask & FIELD_BALANCE)
            evm_state_set_balance(es, &g->addr, &g->balance);

        if (g->field_mask & FIELD_CODE_HASH) {
            /* Code hash is set via code — but during reconstruction from
             * history alone, we don't have the actual bytecode. We store
             * the code_hash directly. The code_store must be intact for
             * actual EVM execution after reconstruction. */
            evm_state_set_code_hash(es, &g->addr, &g->code_hash);
        }

        for (uint16_t j = 0; j < g->slot_count; j++) {
            evm_state_set_storage(es, &g->addr,
                                   &g->slots[j].slot, &g->slots[j].value);
        }
    }

    /* Note: caller decides when to commit/finalize.
     * For bulk reconstruction, skip per-block commit for performance. */
}

void state_history_revert_diff(evm_state_t *es, const block_diff_t *diff) {
    if (!es || !diff) return;

    state_t *st = evm_state_get_state(es);
    if (!st) return;

    /* Process groups in reverse order using raw writes
     * (skip journal, dirty tracking, block originals) */
    for (int i = (int)diff->group_count - 1; i >= 0; i--) {
        const addr_diff_t *g = &diff->groups[i];

        /* Revert storage slots first (reverse order) */
        for (int j = (int)g->slot_count - 1; j >= 0; j--) {
            state_set_storage_raw(st, &g->addr,
                                  &g->slots[j].slot, &g->slots[j].old_value);
        }

        /* Revert account fields */
        if (g->field_mask & FIELD_BALANCE)
            state_set_balance_raw(st, &g->addr, &g->old_balance);

        if (g->field_mask & FIELD_NONCE)
            state_set_nonce_raw(st, &g->addr, g->old_nonce);

        if (g->flags & ACCT_DIFF_CREATED) {
            /* Account was created in this block — zero it out to revert */
            state_set_nonce_raw(st, &g->addr, 0);
            uint256_t zero = UINT256_ZERO;
            state_set_balance_raw(st, &g->addr, &zero);
        }

        if (g->flags & ACCT_DIFF_DESTRUCTED) {
            /* Account was destructed — restore old values */
            state_set_nonce_raw(st, &g->addr, g->old_nonce);
            state_set_balance_raw(st, &g->addr, &g->old_balance);
        }
    }

    /* Caller must invalidate hashes and mark dirty after revert */
}

uint64_t state_history_replay(state_history_t *sh,
                               evm_state_t *es,
                               uint64_t first_block,
                               uint64_t last_block) {
    if (!sh || !es) return 0;
    if (first_block > last_block) return 0;

    uint64_t count = 0;
    for (uint64_t bn = first_block; bn <= last_block; bn++) {
        block_diff_t diff;
        if (!state_history_get_diff(sh, bn, &diff)) {
            LOG_HIST_ERROR("history: replay failed to read block %lu", bn);
            return count;
        }

        state_history_apply_diff(es, &diff);
        block_diff_free(&diff);
        count++;

        if (count % 10000 == 0)
            LOG_HIST_INFO("history: replay progress %lu/%lu blocks",
                   count, last_block - first_block + 1);
    }

    LOG_HIST_INFO("history: replay complete — %lu blocks applied (%lu..%lu)",
           count, first_block, last_block);
    return count;
}

uint64_t state_history_revert_to(state_history_t *sh,
                                  evm_state_t *es,
                                  uint64_t target_block) {
    if (!sh || !es) return 0;

    /* Find the current head block from the ring */
    size_t tail = atomic_load(&sh->ring.tail);
    size_t head = atomic_load(&sh->ring.head);
    if (head == tail) return 0;  /* ring empty */

    /* Find the newest block number in the ring */
    size_t newest_idx = (head - 1) & (DIFF_RING_CAP - 1);
    uint64_t newest_block = sh->ring.slots[newest_idx].block_number;

    if (target_block >= newest_block) return 0;  /* nothing to revert */

    uint64_t reverted = 0;

    /* Walk backwards from newest to target+1, reverting each */
    for (uint64_t bn = newest_block; bn > target_block; bn--) {
        /* Find this block in the ring */
        bool found = false;
        for (size_t idx = head; idx != tail; ) {
            idx = (idx - 1) & (DIFF_RING_CAP - 1);
            if (sh->ring.slots[idx].block_number == bn) {
                state_history_revert_diff(es, &sh->ring.slots[idx]);
                reverted++;
                found = true;
                break;
            }
            if (idx == tail) break;
        }
        if (!found) {
            LOG_HIST_ERROR("history: revert failed — block %lu not in ring", bn);
            break;
        }
    }

    if (reverted > 0) {
        /* Invalidate all cached hashes — state has been rewound */
        evm_state_invalidate_all(es);

        /* Truncate ring: remove reverted entries */
        state_history_truncate(sh, target_block);

        LOG_HIST_INFO("history: reverted %lu blocks (%lu → %lu)",
               reverted, newest_block, target_block);
    }

    return reverted;
}
