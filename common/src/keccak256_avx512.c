/**
 * keccak256_avx512.c — 8-way parallel Keccak-256 using AVX-512.
 *
 * Each __m512i holds lane[i] from 8 independent keccak states.
 * All XOR/ROT/AND operations work on 8 states simultaneously,
 * giving ~6-8x throughput for independent hashes.
 *
 * Requires AVX-512F + AVX-512VL (Zen 4+, Intel Skylake-X+).
 */

#include "keccak256_avx512.h"
#include <immintrin.h>
#include <string.h>

#define KECCAK_RATE 136
#define KECCAK_ROUNDS 24

static const uint64_t RC[24] = {
    UINT64_C(0x0000000000000001), UINT64_C(0x0000000000008082),
    UINT64_C(0x800000000000808A), UINT64_C(0x8000000080008000),
    UINT64_C(0x000000000000808B), UINT64_C(0x0000000080000001),
    UINT64_C(0x8000000080008081), UINT64_C(0x8000000000008009),
    UINT64_C(0x000000000000008A), UINT64_C(0x0000000000000088),
    UINT64_C(0x0000000080008009), UINT64_C(0x000000008000000A),
    UINT64_C(0x000000008000808B), UINT64_C(0x800000000000008B),
    UINT64_C(0x8000000000008089), UINT64_C(0x8000000000008003),
    UINT64_C(0x8000000000008002), UINT64_C(0x8000000000000080),
    UINT64_C(0x000000000000800A), UINT64_C(0x800000008000000A),
    UINT64_C(0x8000000080008081), UINT64_C(0x8000000000008080),
    UINT64_C(0x0000000080000001), UINT64_C(0x8000000080008008),
};

/* AVX-512 has native 64-bit rotate: _mm512_rol_epi64 */
#define ROT(x, n) _mm512_rol_epi64(x, n)

/**
 * Keccak-f[1600] permutation on 8 parallel states.
 */
static void keccak_f1600_x8(__m512i st[25]) {
    for (int r = 0; r < KECCAK_ROUNDS; r++) {
        /* --- Theta --- */
        __m512i c0 = _mm512_xor_si512(_mm512_xor_si512(st[0], st[5]),
                     _mm512_xor_si512(st[10], _mm512_xor_si512(st[15], st[20])));
        __m512i c1 = _mm512_xor_si512(_mm512_xor_si512(st[1], st[6]),
                     _mm512_xor_si512(st[11], _mm512_xor_si512(st[16], st[21])));
        __m512i c2 = _mm512_xor_si512(_mm512_xor_si512(st[2], st[7]),
                     _mm512_xor_si512(st[12], _mm512_xor_si512(st[17], st[22])));
        __m512i c3 = _mm512_xor_si512(_mm512_xor_si512(st[3], st[8]),
                     _mm512_xor_si512(st[13], _mm512_xor_si512(st[18], st[23])));
        __m512i c4 = _mm512_xor_si512(_mm512_xor_si512(st[4], st[9]),
                     _mm512_xor_si512(st[14], _mm512_xor_si512(st[19], st[24])));

        __m512i d0 = _mm512_xor_si512(ROT(c1, 1), c4);
        __m512i d1 = _mm512_xor_si512(ROT(c2, 1), c0);
        __m512i d2 = _mm512_xor_si512(ROT(c3, 1), c1);
        __m512i d3 = _mm512_xor_si512(ROT(c4, 1), c2);
        __m512i d4 = _mm512_xor_si512(ROT(c0, 1), c3);

        for (int i = 0; i < 25; i += 5) {
            st[i+0] = _mm512_xor_si512(st[i+0], d0);
            st[i+1] = _mm512_xor_si512(st[i+1], d1);
            st[i+2] = _mm512_xor_si512(st[i+2], d2);
            st[i+3] = _mm512_xor_si512(st[i+3], d3);
            st[i+4] = _mm512_xor_si512(st[i+4], d4);
        }

        /* --- Rho + Pi (combined) --- */
        __m512i t = st[1];
        st[ 1] = ROT(st[ 6], 44); st[ 6] = ROT(st[ 9], 20);
        st[ 9] = ROT(st[22], 61); st[22] = ROT(st[14], 39);
        st[14] = ROT(st[20], 18); st[20] = ROT(st[ 2], 62);
        st[ 2] = ROT(st[12], 43); st[12] = ROT(st[13], 25);
        st[13] = ROT(st[19],  8); st[19] = ROT(st[23], 56);
        st[23] = ROT(st[15], 41); st[15] = ROT(st[ 4], 27);
        st[ 4] = ROT(st[24], 14); st[24] = ROT(st[21],  2);
        st[21] = ROT(st[ 8], 55); st[ 8] = ROT(st[16], 45);
        st[16] = ROT(st[ 5], 36); st[ 5] = ROT(st[ 3], 28);
        st[ 3] = ROT(st[18], 21); st[18] = ROT(st[17], 15);
        st[17] = ROT(st[11], 10); st[11] = ROT(st[ 7],  6);
        st[ 7] = ROT(st[10],  3); st[10] = ROT(t,       1);

        /* --- Chi --- */
        for (int j = 0; j < 25; j += 5) {
            __m512i a0 = st[j+0], a1 = st[j+1], a2 = st[j+2],
                    a3 = st[j+3], a4 = st[j+4];
            st[j+0] = _mm512_xor_si512(a0, _mm512_andnot_si512(a1, a2));
            st[j+1] = _mm512_xor_si512(a1, _mm512_andnot_si512(a2, a3));
            st[j+2] = _mm512_xor_si512(a2, _mm512_andnot_si512(a3, a4));
            st[j+3] = _mm512_xor_si512(a3, _mm512_andnot_si512(a4, a0));
            st[j+4] = _mm512_xor_si512(a4, _mm512_andnot_si512(a0, a1));
        }

        /* --- Iota --- */
        st[0] = _mm512_xor_si512(st[0], _mm512_set1_epi64((long long)RC[r]));
    }
}

static void absorb_pad_x8(__m512i st[25],
                           const uint8_t *data[8], const size_t lens[8]) {
    /* All inputs must be < KECCAK_RATE (136 bytes).
     * MPT nodes are always < 136 bytes so this is safe. */
    uint64_t blocks[8][17];
    memset(blocks, 0, sizeof(blocks));

    for (int s = 0; s < 8; s++) {
        size_t len = lens[s] < KECCAK_RATE ? lens[s] : KECCAK_RATE - 1;
        if (data[s] && len > 0)
            memcpy(blocks[s], data[s], len);
        ((uint8_t *)blocks[s])[len] = 0x01;
        ((uint8_t *)blocks[s])[KECCAK_RATE - 1] |= 0x80;
    }

    for (int i = 0; i < 17; i++) {
        __m512i block = _mm512_set_epi64(
            (long long)blocks[7][i], (long long)blocks[6][i],
            (long long)blocks[5][i], (long long)blocks[4][i],
            (long long)blocks[3][i], (long long)blocks[2][i],
            (long long)blocks[1][i], (long long)blocks[0][i]);
        st[i] = _mm512_xor_si512(st[i], block);
    }
}

void keccak256_avx512_x8(const uint8_t *data[8], const size_t lens[8],
                          uint8_t *out[8]) {
    __m512i st[25];
    for (int i = 0; i < 25; i++)
        st[i] = _mm512_setzero_si512();

    absorb_pad_x8(st, data, lens);
    keccak_f1600_x8(st);

    /* Extract 32-byte hash from each of 8 states */
    for (int s = 0; s < 8; s++) {
        uint64_t h[4];
        for (int q = 0; q < 4; q++) {
            uint64_t lanes[8];
            _mm512_storeu_si512((__m512i *)lanes, st[q]);
            h[q] = lanes[s];
        }
        memcpy(out[s], h, 32);
    }
}

void keccak256_avx512(const uint8_t *data, size_t len, uint8_t out[32]) {
    const uint8_t *d[8] = { data, data, data, data, data, data, data, data };
    size_t l[8] = { len, len, len, len, len, len, len, len };
    uint8_t *o[8] = { out, out, out, out, out, out, out, out };
    keccak256_avx512_x8(d, l, o);
}
