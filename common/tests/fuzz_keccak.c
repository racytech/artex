/**
 * Differential fuzz: scalar keccak256 vs AVX2 x4 vs AVX-512 x8.
 *
 * Generates random inputs of random lengths (0-135 bytes),
 * hashes with all three implementations, asserts they match.
 * Runs indefinitely until Ctrl+C or mismatch.
 */
#include "keccak256.h"
#include "keccak256_avx2.h"
#include "keccak256_avx512.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <signal.h>

#define MAX_INPUT 135  /* must be < KECCAK_RATE (136) */

static volatile int g_stop = 0;
static void on_sigint(int sig) { (void)sig; g_stop = 1; }

/* Simple xoshiro256** PRNG */
static uint64_t rng_state[4];

static uint64_t rng_next(void) {
    uint64_t *s = rng_state;
    uint64_t result = ((s[1] * 5) << 7 | (s[1] * 5) >> 57) * 9;
    uint64_t t = s[1] << 17;
    s[2] ^= s[0]; s[3] ^= s[1]; s[1] ^= s[2]; s[0] ^= s[3];
    s[2] ^= t; s[3] = (s[3] << 45) | (s[3] >> 19);
    return result;
}

static void rng_seed(uint64_t seed) {
    rng_state[0] = seed;
    rng_state[1] = seed ^ 0x9E3779B97F4A7C15ULL;
    rng_state[2] = seed ^ 0x6A09E667F3BCC908ULL;
    rng_state[3] = seed ^ 0xBB67AE8584CAA73BULL;
    for (int i = 0; i < 8; i++) rng_next();
}

static void rng_bytes(uint8_t *buf, size_t n) {
    for (size_t i = 0; i < n; i += 8) {
        uint64_t v = rng_next();
        size_t left = n - i < 8 ? n - i : 8;
        memcpy(buf + i, &v, left);
    }
}

static void scalar_hash(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    if (len > 0) keccak_update_long(&ctx, data, len);
    keccak_final(&ctx, out);
}

static void print_hex(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) printf("%02x", data[i]);
}

int main(int argc, char **argv) {
    uint64_t seed = (argc > 1) ? (uint64_t)atoll(argv[1]) : (uint64_t)time(NULL);
    rng_seed(seed);
    signal(SIGINT, on_sigint);

    printf("Keccak differential fuzz (seed=%lu)\n", seed);
    printf("Testing: scalar vs avx2_x4 vs avx512_x8\n\n");

    uint64_t rounds = 0;
    uint64_t total_hashes = 0;
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (!g_stop) {
        /* Generate 8 random inputs with random lengths */
        uint8_t inputs[8][MAX_INPUT];
        size_t  lens[8];
        uint8_t scalar_out[8][32];

        for (int i = 0; i < 8; i++) {
            lens[i] = rng_next() % (MAX_INPUT + 1);
            rng_bytes(inputs[i], lens[i]);
            scalar_hash(inputs[i], lens[i], scalar_out[i]);
        }

        /* AVX2 x4: two batches */
        for (int batch = 0; batch < 2; batch++) {
            int base = batch * 4;
            const uint8_t *d[4] = { inputs[base], inputs[base+1],
                                    inputs[base+2], inputs[base+3] };
            size_t l[4] = { lens[base], lens[base+1],
                            lens[base+2], lens[base+3] };
            uint8_t o0[32], o1[32], o2[32], o3[32];
            uint8_t *o[4] = { o0, o1, o2, o3 };
            keccak256_avx2_x4(d, l, o);

            for (int j = 0; j < 4; j++) {
                if (memcmp(o[j], scalar_out[base+j], 32) != 0) {
                    printf("MISMATCH avx2 at round %lu, slot %d (len=%zu)\n",
                           rounds, base+j, lens[base+j]);
                    printf("  input:  "); print_hex(inputs[base+j], lens[base+j]); printf("\n");
                    printf("  scalar: "); print_hex(scalar_out[base+j], 32); printf("\n");
                    printf("  avx2:   "); print_hex(o[j], 32); printf("\n");
                    printf("  seed=%lu\n", seed);
                    return 1;
                }
            }
        }

        /* AVX-512 x8: one batch */
        {
            const uint8_t *d[8];
            size_t l[8];
            uint8_t outs[8][32];
            uint8_t *o[8];
            for (int i = 0; i < 8; i++) {
                d[i] = inputs[i];
                l[i] = lens[i];
                o[i] = outs[i];
            }
            keccak256_avx512_x8(d, l, o);

            for (int j = 0; j < 8; j++) {
                if (memcmp(outs[j], scalar_out[j], 32) != 0) {
                    printf("MISMATCH avx512 at round %lu, slot %d (len=%zu)\n",
                           rounds, j, lens[j]);
                    printf("  input:  "); print_hex(inputs[j], lens[j]); printf("\n");
                    printf("  scalar: "); print_hex(scalar_out[j], 32); printf("\n");
                    printf("  avx512: "); print_hex(outs[j], 32); printf("\n");
                    printf("  seed=%lu\n", seed);
                    return 1;
                }
            }
        }

        rounds++;
        total_hashes += 8 * 3;  /* 8 hashes × 3 implementations */

        if (rounds % 100000 == 0) {
            struct timespec t1;
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double dt = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
            printf("  %lu rounds (%lu hashes) in %.1fs — %.0f hash/s — all match\n",
                   rounds, total_hashes, dt, total_hashes / dt);
        }
    }

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double dt = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    printf("\nStopped after %lu rounds (%lu hashes) in %.1fs. All matched.\n",
           rounds, total_hashes, dt);
    return 0;
}
