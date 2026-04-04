/**
 * Benchmark: scalar keccak256 vs AVX2 4-way keccak256.
 *
 * Tests throughput for MPT-sized inputs (32-200 bytes).
 */
#include "keccak256.h"
#include "keccak256_avx2.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

#define ITERS 1000000

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* Verify AVX2 produces same hash as scalar */
static int verify_correctness(void) {
    printf("Correctness check:\n");
    int errors = 0;

    for (int len = 1; len <= 136; len += 17) {
        uint8_t input[136];
        for (int i = 0; i < len; i++) input[i] = (uint8_t)(i * 37 + len);

        /* Scalar */
        SHA3_CTX ctx;
        keccak_init(&ctx);
        keccak_update_long(&ctx, input, len);
        uint8_t scalar_out[32];
        keccak_final(&ctx, scalar_out);

        /* AVX2 single */
        uint8_t avx2_out[32];
        keccak256_avx2(input, len, avx2_out);

        if (memcmp(scalar_out, avx2_out, 32) != 0) {
            printf("  FAIL at len=%d\n", len);
            printf("    scalar: ");
            for (int i = 0; i < 32; i++) printf("%02x", scalar_out[i]);
            printf("\n    avx2:   ");
            for (int i = 0; i < 32; i++) printf("%02x", avx2_out[i]);
            printf("\n");
            errors++;
        }
    }

    /* Test x4 with different inputs */
    uint8_t in0[64], in1[80], in2[32], in3[100];
    for (int i = 0; i < 64; i++) in0[i] = (uint8_t)i;
    for (int i = 0; i < 80; i++) in1[i] = (uint8_t)(i + 50);
    for (int i = 0; i < 32; i++) in2[i] = (uint8_t)(i + 100);
    for (int i = 0; i < 100; i++) in3[i] = (uint8_t)(i + 200);

    const uint8_t *data[4] = { in0, in1, in2, in3 };
    size_t lens[4] = { 64, 80, 32, 100 };
    uint8_t out0[32], out1[32], out2[32], out3[32];
    uint8_t *outs[4] = { out0, out1, out2, out3 };

    keccak256_avx2_x4(data, lens, outs);

    /* Verify each against scalar */
    for (int s = 0; s < 4; s++) {
        SHA3_CTX ctx2;
        keccak_init(&ctx2);
        keccak_update_long(&ctx2, data[s], lens[s]);
        uint8_t expected[32];
        keccak_final(&ctx2, expected);
        if (memcmp(outs[s], expected, 32) != 0) {
            printf("  FAIL x4 slot %d (len=%zu)\n", s, lens[s]);
            errors++;
        }
    }

    if (errors == 0) printf("  OK — all lengths match\n");
    return errors;
}

static void bench_scalar(int input_len) {
    uint8_t input[136];
    for (int i = 0; i < input_len; i++) input[i] = (uint8_t)i;
    uint8_t out[32];

    double t0 = now_ms();
    for (int i = 0; i < ITERS; i++) {
        input[0] = (uint8_t)i; /* vary input slightly */
        SHA3_CTX ctx;
        keccak_init(&ctx);
        keccak_update_long(&ctx, input, input_len);
        keccak_final(&ctx, out);
    }
    double dt = now_ms() - t0;
    printf("  scalar x1:  %7.1f ms  (%6.0f hash/ms)\n",
           dt, ITERS / dt);
}

static void bench_avx2_x4(int input_len) {
    uint8_t in0[136], in1[136], in2[136], in3[136];
    for (int i = 0; i < input_len; i++) {
        in0[i] = (uint8_t)i;
        in1[i] = (uint8_t)(i + 50);
        in2[i] = (uint8_t)(i + 100);
        in3[i] = (uint8_t)(i + 200);
    }
    uint8_t out0[32], out1[32], out2[32], out3[32];

    const uint8_t *data[4] = { in0, in1, in2, in3 };
    size_t lens[4] = { (size_t)input_len, (size_t)input_len,
                       (size_t)input_len, (size_t)input_len };
    uint8_t *outs[4] = { out0, out1, out2, out3 };

    int batches = ITERS / 4;
    double t0 = now_ms();
    for (int i = 0; i < batches; i++) {
        in0[0] = (uint8_t)i;
        keccak256_avx2_x4(data, lens, outs);
    }
    double dt = now_ms() - t0;
    printf("  avx2   x4:  %7.1f ms  (%6.0f hash/ms)  [%.1fx]\n",
           dt, (batches * 4.0) / dt,
           (ITERS / dt) > 0 ? ((batches * 4.0) / dt) / (1.0) : 0);
}

int main(void) {
    if (verify_correctness() != 0) return 1;

    int sizes[] = { 32, 64, 100, 136 };
    for (int s = 0; s < 4; s++) {
        printf("\nInput size: %d bytes\n", sizes[s]);
        bench_scalar(sizes[s]);
        bench_avx2_x4(sizes[s]);
    }

    return 0;
}
