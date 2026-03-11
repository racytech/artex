/*
 * Keccak-256 regression tests.
 * Validates the optimized fused-permutation implementation against
 * known Ethereum/NIST test vectors and edge cases.
 */
#include <stdio.h>
#include <string.h>
#include "keccak256.h"
#include "hash.h"

static int passed = 0, failed = 0;

static void check_hash(const char *label,
                       const uint8_t *data, size_t len,
                       const char *expected_hex) {
    hash_t h = hash_keccak256(data, len);
    char got[65];
    for (int i = 0; i < 32; i++)
        sprintf(got + i*2, "%02x", h.bytes[i]);
    got[64] = 0;

    /* case-insensitive compare */
    int match = 1;
    for (int i = 0; i < 64; i++) {
        char a = got[i]; if (a >= 'A' && a <= 'F') a += 32;
        char b = expected_hex[i]; if (b >= 'A' && b <= 'F') b += 32;
        if (a != b) { match = 0; break; }
    }

    if (match) {
        passed++;
    } else {
        failed++;
        printf("FAIL %s:\n  expected: %s\n  got:      %s\n", label, expected_hex, got);
    }
}

/* ================================================================
 * Known test vectors
 * ================================================================ */
static void test_known_vectors(void) {
    /* Empty input */
    check_hash("empty", (const uint8_t*)"", 0,
        "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");

    /* "hello" */
    check_hash("hello", (const uint8_t*)"hello", 5,
        "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8");

    /* Single byte 0x80 (RLP empty string) = HASH_EMPTY_STORAGE */
    {
        uint8_t data[] = {0x80};
        hash_t h = hash_keccak256(data, 1);
        if (hash_equal(&h, &HASH_EMPTY_STORAGE)) {
            passed++;
        } else {
            failed++;
            printf("FAIL keccak(0x80) != HASH_EMPTY_STORAGE\n");
        }
    }

    /* Single byte 0xc0 (RLP empty list) */
    check_hash("0xc0", (const uint8_t*)"\xc0", 1,
        "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347");

    /* Single zero byte */
    check_hash("0x00", (const uint8_t*)"\x00", 1,
        "bc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a");

    /* "abc" — widely used NIST test vector */
    check_hash("abc", (const uint8_t*)"abc", 3,
        "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45");

    /* 32 zero bytes */
    {
        uint8_t zeros[32] = {0};
        check_hash("32_zeros", zeros, 32,
            "290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563");
    }
}

/* ================================================================
 * Block boundary tests (rate = 136 bytes)
 * ================================================================ */
static void test_block_boundaries(void) {
    /* Exactly 1 block (136 bytes) */
    {
        uint8_t data[136];
        memset(data, 'A', 136);
        hash_t h = hash_keccak256(data, 136);
        /* Just verify it doesn't crash and produces non-zero */
        int nonzero = 0;
        for (int i = 0; i < 32; i++) if (h.bytes[i]) nonzero = 1;
        if (nonzero) passed++; else { failed++; printf("FAIL 136 bytes all zero\n"); }
    }

    /* 135 bytes (one less than block) */
    {
        uint8_t data[135];
        memset(data, 'B', 135);
        hash_t h1 = hash_keccak256(data, 135);
        data[134] = 'C'; /* change last byte */
        hash_t h2 = hash_keccak256(data, 135);
        /* Different inputs should give different hashes */
        if (!hash_equal(&h1, &h2)) passed++;
        else { failed++; printf("FAIL 135 bytes: different inputs same hash\n"); }
    }

    /* 137 bytes (one more than block — crosses boundary) */
    {
        uint8_t data[137];
        memset(data, 'C', 137);
        hash_t h = hash_keccak256(data, 137);
        int nonzero = 0;
        for (int i = 0; i < 32; i++) if (h.bytes[i]) nonzero = 1;
        if (nonzero) passed++; else { failed++; printf("FAIL 137 bytes all zero\n"); }
    }

    /* Exactly 2 blocks (272 bytes) */
    {
        uint8_t data[272];
        memset(data, 'D', 272);
        hash_t h = hash_keccak256(data, 272);
        int nonzero = 0;
        for (int i = 0; i < 32; i++) if (h.bytes[i]) nonzero = 1;
        if (nonzero) passed++; else { failed++; printf("FAIL 272 bytes all zero\n"); }
    }
}

/* ================================================================
 * Large input stress test
 * ================================================================ */
static void test_large_input(void) {
    /* 10KB input */
    {
        uint8_t data[10240];
        for (size_t i = 0; i < 10240; i++) data[i] = (uint8_t)(i & 0xFF);
        hash_t h = hash_keccak256(data, 10240);
        int nonzero = 0;
        for (int i = 0; i < 32; i++) if (h.bytes[i]) nonzero = 1;
        if (nonzero) passed++; else { failed++; printf("FAIL 10KB all zero\n"); }
    }

    /* Verify same input always gives same output */
    {
        uint8_t data[1000];
        memset(data, 0x42, 1000);
        hash_t h1 = hash_keccak256(data, 1000);
        hash_t h2 = hash_keccak256(data, 1000);
        if (hash_equal(&h1, &h2)) passed++;
        else { failed++; printf("FAIL determinism: same input different hash\n"); }
    }
}

/* ================================================================
 * Incremental (update in chunks) vs single-call
 * ================================================================ */
static void test_incremental(void) {
    uint8_t data[300];
    for (int i = 0; i < 300; i++) data[i] = (uint8_t)(i * 7 + 3);

    /* Single call */
    hash_t h_single = hash_keccak256(data, 300);

    /* Chunked: 100 + 100 + 100 */
    {
        SHA3_CTX ctx;
        keccak_init(&ctx);
        keccak_update(&ctx, data, 100);
        keccak_update(&ctx, data + 100, 100);
        keccak_update(&ctx, data + 200, 100);
        uint8_t digest[32];
        keccak_final(&ctx, digest);

        hash_t h_chunked;
        memcpy(h_chunked.bytes, digest, 32);

        if (hash_equal(&h_single, &h_chunked)) passed++;
        else { failed++; printf("FAIL incremental 100+100+100 != single\n"); }
    }

    /* Chunked: 1 byte at a time */
    {
        SHA3_CTX ctx;
        keccak_init(&ctx);
        for (int i = 0; i < 300; i++)
            keccak_update(&ctx, data + i, 1);
        uint8_t digest[32];
        keccak_final(&ctx, digest);

        hash_t h_byte;
        memcpy(h_byte.bytes, digest, 32);

        if (hash_equal(&h_single, &h_byte)) passed++;
        else { failed++; printf("FAIL incremental byte-at-a-time != single\n"); }
    }

    /* Chunked: 136 + 164 (crosses block boundary mid-chunk) */
    {
        SHA3_CTX ctx;
        keccak_init(&ctx);
        keccak_update(&ctx, data, 136);
        keccak_update(&ctx, data + 136, 164);
        uint8_t digest[32];
        keccak_final(&ctx, digest);

        hash_t h_split;
        memcpy(h_split.bytes, digest, 32);

        if (hash_equal(&h_single, &h_split)) passed++;
        else { failed++; printf("FAIL incremental 136+164 != single\n"); }
    }
}

/* ================================================================
 * Avalanche: flipping one bit changes the hash
 * ================================================================ */
static void test_avalanche(void) {
    uint8_t data[64];
    memset(data, 0, 64);
    hash_t base = hash_keccak256(data, 64);

    int all_different = 1;
    for (int bit = 0; bit < 64 * 8; bit++) {
        int byte_idx = bit / 8;
        int bit_idx = bit % 8;
        data[byte_idx] ^= (1 << bit_idx);
        hash_t flipped = hash_keccak256(data, 64);
        data[byte_idx] ^= (1 << bit_idx); /* restore */
        if (hash_equal(&base, &flipped)) {
            all_different = 0;
            failed++;
            printf("FAIL avalanche: flipping bit %d gave same hash\n", bit);
            break;
        }
    }
    if (all_different) passed++;
}

/* ================================================================
 * MAIN
 * ================================================================ */
int main(void) {
    printf("=== keccak256 regression tests ===\n\n");

    test_known_vectors();
    test_block_boundaries();
    test_large_input();
    test_incremental();
    test_avalanche();

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
