/**
 * Benchmark: hart disk cycle — read → build → modify → root → serialize → write
 *
 * Simulates the per-account storage lifecycle if all storage lived on disk:
 *   1. pread entries from file
 *   2. Build hart from entries
 *   3. Modify N slots (SSTORE)
 *   4. Compute MPT root hash (incremental)
 *   5. Serialize back to buffer
 *   6. pwrite entries to file
 *   7. Destroy hart
 *
 * Tests various account sizes: 10, 50, 100, 500, 1000, 5000, 10000 slots.
 */

#include "hashed_art.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

#define STOR_VAL_SIZE 32

static double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

/* Generate deterministic key/value */
static void make_entry(uint64_t acct, uint64_t slot, uint8_t key[32], uint8_t val[32]) {
    memset(key, 0, 32);
    memset(val, 0, 32);
    uint64_t k = acct * 1000000 + slot;
    for (int i = 7; i >= 0; i--) { key[24 + i] = k & 0xFF; k >>= 8; }
    hash_t h = hash_keccak256(key, 32);
    memcpy(key, h.bytes, 32);
    val[31] = (uint8_t)(slot + 1);
    val[30] = (uint8_t)((slot + 1) >> 8);
}

/* RLP-encode a 32-byte value for hart_root_hash callback */
static uint32_t stor_encode(const uint8_t key[32], const void *leaf_val,
                             uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    const uint8_t *v = (const uint8_t *)leaf_val;
    /* Skip leading zeros */
    int skip = 0;
    while (skip < 31 && v[skip] == 0) skip++;
    int len = 32 - skip;
    if (len == 1 && v[skip] <= 0x7f) {
        rlp_out[0] = v[skip];
        return 1;
    }
    rlp_out[0] = 0x80 + (uint8_t)len;
    memcpy(rlp_out + 1, v + skip, len);
    return 1 + (uint32_t)len;
}

static void bench_cycle(int fd, uint32_t n_slots, uint32_t n_modify, int iterations) {
    /* Pre-generate entries on disk */
    size_t entry_size = 64; /* key[32] + val[32] */
    size_t total_bytes = (size_t)n_slots * entry_size;
    uint8_t *disk_buf = malloc(total_bytes);

    for (uint32_t s = 0; s < n_slots; s++) {
        make_entry(0, s, disk_buf + s * 64, disk_buf + s * 64 + 32);
    }
    pwrite(fd, disk_buf, total_bytes, 0);
    fsync(fd);

    double t_read = 0, t_build = 0, t_modify = 0, t_root = 0;
    double t_serialize = 0, t_write = 0, t_destroy = 0;

    for (int iter = 0; iter < iterations; iter++) {
        /* 1. Read from disk */
        double t0 = now_us();
        ssize_t rd = pread(fd, disk_buf, total_bytes, 0);
        (void)rd;
        double t1 = now_us();
        t_read += t1 - t0;

        /* 2. Build hart */
        hart_t ht;
        size_t cap = (size_t)n_slots * 96;
        if (cap < 1024) cap = 1024;
        hart_init_cap(&ht, STOR_VAL_SIZE, cap);

        for (uint32_t s = 0; s < n_slots; s++) {
            hart_insert(&ht, disk_buf + s * 64, disk_buf + s * 64 + 32);
        }
        double t2 = now_us();
        t_build += t2 - t1;

        /* 3. Modify N slots */
        for (uint32_t m = 0; m < n_modify; m++) {
            uint8_t key[32], val[32];
            make_entry(0, m % n_slots, key, val);
            val[31] = (uint8_t)(iter + m + 42); /* different value */
            hart_insert(&ht, key, val);
        }
        double t3 = now_us();
        t_modify += t3 - t2;

        /* 4. Compute root */
        uint8_t root[32];
        hart_root_hash(&ht, stor_encode, NULL, root);
        double t4 = now_us();
        t_root += t4 - t3;

        /* 5. Serialize */
        size_t pos = 0;
        hart_iter_t *it = hart_iter_create(&ht);
        if (it) {
            while (hart_iter_next(it)) {
                memcpy(disk_buf + pos, hart_iter_key(it), 32);
                memcpy(disk_buf + pos + 32, hart_iter_value(it), 32);
                pos += 64;
            }
            hart_iter_destroy(it);
        }
        double t5 = now_us();
        t_serialize += t5 - t4;

        /* 6. Write back to disk */
        pwrite(fd, disk_buf, pos, 0);
        double t6 = now_us();
        t_write += t6 - t5;

        /* 7. Destroy */
        hart_destroy(&ht);
        double t7 = now_us();
        t_destroy += t7 - t6;
    }

    double total = t_read + t_build + t_modify + t_root + t_serialize + t_write + t_destroy;

    printf("  slots=%5u modify=%2u | read=%5.0f build=%6.0f modify=%4.0f root=%6.0f ser=%5.0f write=%5.0f free=%4.0f | total=%7.0f μs\n",
           n_slots, n_modify,
           t_read / iterations, t_build / iterations, t_modify / iterations,
           t_root / iterations, t_serialize / iterations, t_write / iterations,
           t_destroy / iterations, total / iterations);

    free(disk_buf);
}

int main(void) {
    const char *path = "/tmp/bench_hart_disk.dat";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror("open"); return 1; }

    /* Pre-allocate 10MB */
    ftruncate(fd, 10 * 1024 * 1024);

    printf("=== Hart Disk Cycle Benchmark ===\n");
    printf("  (read → build → modify → root → serialize → write → destroy)\n\n");

    /* Warm up page cache */
    bench_cycle(fd, 100, 5, 10);
    printf("\n");

    uint32_t sizes[] = {10, 50, 100, 500, 1000, 5000, 10000};
    int n_sizes = sizeof(sizes) / sizeof(sizes[0]);

    for (int i = 0; i < n_sizes; i++) {
        uint32_t mod = sizes[i] < 10 ? sizes[i] : 5;
        int iters = sizes[i] <= 1000 ? 1000 : 100;
        bench_cycle(fd, sizes[i], mod, iters);
    }

    printf("\n--- Simulated block (200 accounts × 5 SSTOREs each) ---\n");
    {
        /* 200 accounts of varying sizes */
        uint32_t acct_sizes[] = {10, 50, 100, 100, 500}; /* distribution */
        double block_total = 0;
        int n_accounts = 200;
        int iters = 10;

        for (int iter = 0; iter < iters; iter++) {
            for (int a = 0; a < n_accounts; a++) {
                uint32_t sz = acct_sizes[a % 5];
                size_t total_bytes = (size_t)sz * 64;
                uint8_t *buf = malloc(total_bytes);

                /* Read */
                double t0 = now_us();
                for (uint32_t s = 0; s < sz; s++)
                    make_entry(a, s, buf + s * 64, buf + s * 64 + 32);

                /* Build */
                hart_t ht;
                hart_init_cap(&ht, STOR_VAL_SIZE, sz * 96 < 1024 ? 1024 : sz * 96);
                for (uint32_t s = 0; s < sz; s++)
                    hart_insert(&ht, buf + s * 64, buf + s * 64 + 32);

                /* Modify 5 slots */
                for (int m = 0; m < 5; m++) {
                    uint8_t key[32], val[32];
                    make_entry(a, m % sz, key, val);
                    val[31] = (uint8_t)(iter + m + 99);
                    hart_insert(&ht, key, val);
                }

                /* Root */
                uint8_t root[32];
                hart_root_hash(&ht, stor_encode, NULL, root);

                /* Cleanup */
                hart_destroy(&ht);
                free(buf);
                double t1 = now_us();
                block_total += t1 - t0;
            }
        }

        printf("  avg block time: %.1f ms (%.0f μs / account)\n",
               block_total / iters / 1000.0,
               block_total / iters / n_accounts);
    }

    close(fd);
    unlink(path);
    printf("\n=== done ===\n");
    return 0;
}
