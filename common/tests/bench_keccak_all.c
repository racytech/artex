/**
 * Benchmark: scalar vs AVX2 x4 vs AVX-512 x8 keccak-256.
 */
#include "keccak256.h"
#include "keccak256_avx2.h"
#include "keccak256_avx512.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

#define ITERS 1000000

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* Verify AVX-512 correctness against scalar */
static int verify(void) {
    printf("Correctness:\n");
    int errors = 0;
    uint8_t input[136];

    /* Test various lengths */
    for (int len = 0; len <= 136; len += 7) {
        for (int i = 0; i < len; i++) input[i] = (uint8_t)(i * 37 + len);

        SHA3_CTX ctx;
        keccak_init(&ctx);
        if (len > 0) keccak_update_long(&ctx, input, len);
        uint8_t expected[32];
        keccak_final(&ctx, expected);

        uint8_t avx512_out[32];
        keccak256_avx512(input, len, avx512_out);

        if (memcmp(expected, avx512_out, 32) != 0) {
            printf("  FAIL at len=%d\n", len);
            errors++;
        }
    }

    /* Test x8 with mixed lengths */
    uint8_t ins[8][136];
    size_t lens[8] = { 0, 1, 32, 64, 100, 120, 130, 135 };
    const uint8_t *ptrs[8];
    uint8_t outs[8][32];
    uint8_t *opts[8];

    for (int s = 0; s < 8; s++) {
        for (int i = 0; i < (int)lens[s]; i++) ins[s][i] = (uint8_t)(i + s * 13);
        ptrs[s] = ins[s];
        opts[s] = outs[s];
    }
    keccak256_avx512_x8(ptrs, lens, opts);

    for (int s = 0; s < 8; s++) {
        SHA3_CTX ctx2;
        keccak_init(&ctx2);
        if (lens[s] > 0) keccak_update_long(&ctx2, ins[s], lens[s]);
        uint8_t exp[32];
        keccak_final(&ctx2, exp);
        if (memcmp(outs[s], exp, 32) != 0) {
            printf("  FAIL x8 slot %d (len=%zu)\n", s, lens[s]);
            errors++;
        }
    }

    printf("  %s\n", errors ? "FAILED" : "OK");
    return errors;
}

static void bench(int input_len) {
    uint8_t in0[136]; for (int i = 0; i < input_len; i++) in0[i] = (uint8_t)i;
    uint8_t out[32];

    /* Scalar */
    double t0 = now_ms();
    for (int i = 0; i < ITERS; i++) {
        in0[0] = (uint8_t)i;
        SHA3_CTX ctx; keccak_init(&ctx);
        keccak_update_long(&ctx, in0, input_len);
        keccak_final(&ctx, out);
    }
    double scalar_ms = now_ms() - t0;
    double scalar_rate = ITERS / scalar_ms;

    /* AVX2 x4 */
    uint8_t in1[136], in2[136], in3[136];
    memcpy(in1, in0, 136); memcpy(in2, in0, 136); memcpy(in3, in0, 136);
    const uint8_t *d4[4] = { in0, in1, in2, in3 };
    size_t l4[4] = { (size_t)input_len, (size_t)input_len,
                     (size_t)input_len, (size_t)input_len };
    uint8_t o0[32], o1[32], o2[32], o3[32];
    uint8_t *p4[4] = { o0, o1, o2, o3 };

    int batches4 = ITERS / 4;
    t0 = now_ms();
    for (int i = 0; i < batches4; i++) {
        in0[0] = (uint8_t)i;
        keccak256_avx2_x4(d4, l4, p4);
    }
    double avx2_ms = now_ms() - t0;
    double avx2_rate = (batches4 * 4.0) / avx2_ms;

    /* AVX-512 x8 */
    uint8_t in4[136], in5[136], in6[136], in7[136];
    memcpy(in4, in0, 136); memcpy(in5, in0, 136);
    memcpy(in6, in0, 136); memcpy(in7, in0, 136);
    const uint8_t *d8[8] = { in0, in1, in2, in3, in4, in5, in6, in7 };
    size_t l8[8] = { (size_t)input_len, (size_t)input_len,
                     (size_t)input_len, (size_t)input_len,
                     (size_t)input_len, (size_t)input_len,
                     (size_t)input_len, (size_t)input_len };
    uint8_t o4[32], o5[32], o6[32], o7[32];
    uint8_t *p8[8] = { o0, o1, o2, o3, o4, o5, o6, o7 };

    int batches8 = ITERS / 8;
    t0 = now_ms();
    for (int i = 0; i < batches8; i++) {
        in0[0] = (uint8_t)i;
        keccak256_avx512_x8(d8, l8, p8);
    }
    double avx512_ms = now_ms() - t0;
    double avx512_rate = (batches8 * 8.0) / avx512_ms;

    printf("  scalar:   %7.1f ms  %6.0f h/ms\n", scalar_ms, scalar_rate);
    printf("  avx2 x4:  %7.1f ms  %6.0f h/ms  (%.1fx)\n",
           avx2_ms, avx2_rate, avx2_rate / scalar_rate);
    printf("  avx512 x8:%7.1f ms  %6.0f h/ms  (%.1fx)\n",
           avx512_ms, avx512_rate, avx512_rate / scalar_rate);
}

int main(void) {
    if (verify() != 0) return 1;

    int sizes[] = { 32, 64, 100, 136 };
    for (int s = 0; s < 4; s++) {
        printf("\nInput size: %d bytes\n", sizes[s]);
        bench(sizes[s]);
    }
    return 0;
}
