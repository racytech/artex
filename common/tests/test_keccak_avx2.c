/**
 * Test keccak256_avx2 against known Ethereum Keccak-256 test vectors.
 */
#include "keccak256.h"
#include "keccak256_avx2.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int from_hex(const char *hex, uint8_t *out, size_t max_len) {
    size_t hlen = strlen(hex);
    if (hlen % 2 != 0 || hlen / 2 > max_len) return -1;
    for (size_t i = 0; i < hlen; i += 2) {
        unsigned int byte;
        sscanf(hex + i, "%2x", &byte);
        out[i / 2] = (uint8_t)byte;
    }
    return (int)(hlen / 2);
}

typedef struct {
    const char *name;
    const char *input_hex;   /* "" for empty */
    const char *expected_hex; /* 64 hex chars */
} test_vector_t;

static const test_vector_t vectors[] = {
    { "empty",
      "",
      "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470" },
    { "abc",
      "616263",
      "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45" },
    { "empty RLP list (0xc0)",
      "c0",
      "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347" },
    { "single zero byte",
      "00",
      "bc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a" },
    { "hello world",
      "68656c6c6f20776f726c64",
      "47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad" },
    { "RLP empty string (0x80)",
      "80",
      "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421" },
    { "testing",
      "74657374696e67",
      "5f16f4c7f149ac4f9510d9cf8cf384038ad348b3bcdc01915f95de12df9d1b02" },
    { "32 zero bytes",
      "0000000000000000000000000000000000000000000000000000000000000000",
      "290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563" },
};

#define N_VECTORS (sizeof(vectors) / sizeof(vectors[0]))

static void print_hex(const uint8_t *data, int len) {
    for (int i = 0; i < len; i++) printf("%02x", data[i]);
}

int main(void) {
    int errors = 0;

    printf("=== Keccak-256 AVX2 Test Vectors ===\n\n");

    /* Test each vector against both scalar and AVX2 */
    for (size_t v = 0; v < N_VECTORS; v++) {
        const test_vector_t *tv = &vectors[v];
        uint8_t input[256] = {0};
        int input_len = from_hex(tv->input_hex, input, sizeof(input));
        if (input_len < 0) input_len = 0;

        uint8_t expected[32];
        from_hex(tv->expected_hex, expected, 32);

        /* Scalar */
        SHA3_CTX ctx;
        keccak_init(&ctx);
        if (input_len > 0)
            keccak_update_long(&ctx, input, (size_t)input_len);
        uint8_t scalar_out[32];
        keccak_final(&ctx, scalar_out);

        /* AVX2 single */
        uint8_t avx2_out[32];
        keccak256_avx2(input, (size_t)input_len, avx2_out);

        int scalar_ok = memcmp(scalar_out, expected, 32) == 0;
        int avx2_ok   = memcmp(avx2_out, expected, 32) == 0;

        printf("  %-25s scalar=%s  avx2=%s",
               tv->name,
               scalar_ok ? "OK" : "FAIL",
               avx2_ok   ? "OK" : "FAIL");

        if (!scalar_ok || !avx2_ok) {
            printf("\n    expected: %s\n", tv->expected_hex);
            if (!scalar_ok) { printf("    scalar:   "); print_hex(scalar_out, 32); printf("\n"); }
            if (!avx2_ok)   { printf("    avx2:     "); print_hex(avx2_out, 32); printf("\n"); }
            errors++;
        } else {
            printf("\n");
        }
    }

    /* Test x4 with all vectors batched in groups of 4 */
    printf("\n=== x4 batch test ===\n");
    for (size_t base = 0; base + 3 < N_VECTORS; base += 4) {
        uint8_t ins[4][256] = {0};
        size_t lens[4];
        const uint8_t *ptrs[4];
        uint8_t outs[4][32];
        uint8_t *opt[4];

        for (int i = 0; i < 4; i++) {
            int l = from_hex(vectors[base + i].input_hex, ins[i], 256);
            lens[i] = l > 0 ? (size_t)l : 0;
            ptrs[i] = ins[i];
            opt[i] = outs[i];
        }

        keccak256_avx2_x4(ptrs, lens, opt);

        for (int i = 0; i < 4; i++) {
            uint8_t expected2[32];
            from_hex(vectors[base + i].expected_hex, expected2, 32);
            int ok = memcmp(outs[i], expected2, 32) == 0;
            printf("  x4[%zu] %-20s %s\n", base + i, vectors[base + i].name,
                   ok ? "OK" : "FAIL");
            if (!ok) errors++;
        }
    }

    printf("\n=== %s (%d errors) ===\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
