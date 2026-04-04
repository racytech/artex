/**
 * keccak256_avx2.c — 4-way parallel Keccak-256 using AVX2.
 *
 * Each __m256i holds lane[i] from 4 independent keccak states.
 * All XOR/ROT/AND operations work on 4 states simultaneously,
 * giving ~3-4x throughput for independent hashes.
 *
 * Optimized for MPT node hashing where we often need to hash
 * multiple branch children independently.
 */

#include "keccak256_avx2.h"
#include <immintrin.h>
#include <string.h>

#define KECCAK_RATE 136  /* (1600 - 256*2) / 8 bytes */
#define KECCAK_ROUNDS 24

/* Round constants */
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

/* 64-bit rotate left via AVX2 shifts */
static inline __m256i rot64(__m256i x, int n) {
    return _mm256_or_si256(_mm256_slli_epi64(x, n),
                           _mm256_srli_epi64(x, 64 - n));
}

/**
 * Keccak-f[1600] permutation on 4 parallel states.
 * st[25] where each __m256i = { state0[i], state1[i], state2[i], state3[i] }
 */
static void keccak_f1600_x4(__m256i st[25]) {
    for (int r = 0; r < KECCAK_ROUNDS; r++) {
        /* --- Theta --- */
        __m256i c0 = _mm256_xor_si256(_mm256_xor_si256(st[0], st[5]),
                     _mm256_xor_si256(st[10], _mm256_xor_si256(st[15], st[20])));
        __m256i c1 = _mm256_xor_si256(_mm256_xor_si256(st[1], st[6]),
                     _mm256_xor_si256(st[11], _mm256_xor_si256(st[16], st[21])));
        __m256i c2 = _mm256_xor_si256(_mm256_xor_si256(st[2], st[7]),
                     _mm256_xor_si256(st[12], _mm256_xor_si256(st[17], st[22])));
        __m256i c3 = _mm256_xor_si256(_mm256_xor_si256(st[3], st[8]),
                     _mm256_xor_si256(st[13], _mm256_xor_si256(st[18], st[23])));
        __m256i c4 = _mm256_xor_si256(_mm256_xor_si256(st[4], st[9]),
                     _mm256_xor_si256(st[14], _mm256_xor_si256(st[19], st[24])));

        __m256i d0 = _mm256_xor_si256(rot64(c1, 1), c4);
        __m256i d1 = _mm256_xor_si256(rot64(c2, 1), c0);
        __m256i d2 = _mm256_xor_si256(rot64(c3, 1), c1);
        __m256i d3 = _mm256_xor_si256(rot64(c4, 1), c2);
        __m256i d4 = _mm256_xor_si256(rot64(c0, 1), c3);

        st[ 0] = _mm256_xor_si256(st[ 0], d0);
        st[ 5] = _mm256_xor_si256(st[ 5], d0);
        st[10] = _mm256_xor_si256(st[10], d0);
        st[15] = _mm256_xor_si256(st[15], d0);
        st[20] = _mm256_xor_si256(st[20], d0);
        st[ 1] = _mm256_xor_si256(st[ 1], d1);
        st[ 6] = _mm256_xor_si256(st[ 6], d1);
        st[11] = _mm256_xor_si256(st[11], d1);
        st[16] = _mm256_xor_si256(st[16], d1);
        st[21] = _mm256_xor_si256(st[21], d1);
        st[ 2] = _mm256_xor_si256(st[ 2], d2);
        st[ 7] = _mm256_xor_si256(st[ 7], d2);
        st[12] = _mm256_xor_si256(st[12], d2);
        st[17] = _mm256_xor_si256(st[17], d2);
        st[22] = _mm256_xor_si256(st[22], d2);
        st[ 3] = _mm256_xor_si256(st[ 3], d3);
        st[ 8] = _mm256_xor_si256(st[ 8], d3);
        st[13] = _mm256_xor_si256(st[13], d3);
        st[18] = _mm256_xor_si256(st[18], d3);
        st[23] = _mm256_xor_si256(st[23], d3);
        st[ 4] = _mm256_xor_si256(st[ 4], d4);
        st[ 9] = _mm256_xor_si256(st[ 9], d4);
        st[14] = _mm256_xor_si256(st[14], d4);
        st[19] = _mm256_xor_si256(st[19], d4);
        st[24] = _mm256_xor_si256(st[24], d4);

        /* --- Rho + Pi (combined) --- */
        __m256i t = st[1];
        st[ 1] = rot64(st[ 6], 44); st[ 6] = rot64(st[ 9], 20);
        st[ 9] = rot64(st[22], 61); st[22] = rot64(st[14], 39);
        st[14] = rot64(st[20], 18); st[20] = rot64(st[ 2], 62);
        st[ 2] = rot64(st[12], 43); st[12] = rot64(st[13], 25);
        st[13] = rot64(st[19],  8); st[19] = rot64(st[23], 56);
        st[23] = rot64(st[15], 41); st[15] = rot64(st[ 4], 27);
        st[ 4] = rot64(st[24], 14); st[24] = rot64(st[21],  2);
        st[21] = rot64(st[ 8], 55); st[ 8] = rot64(st[16], 45);
        st[16] = rot64(st[ 5], 36); st[ 5] = rot64(st[ 3], 28);
        st[ 3] = rot64(st[18], 21); st[18] = rot64(st[17], 15);
        st[17] = rot64(st[11], 10); st[11] = rot64(st[ 7],  6);
        st[ 7] = rot64(st[10],  3); st[10] = rot64(t,       1);

        /* --- Chi --- */
        for (int j = 0; j < 25; j += 5) {
            __m256i a0 = st[j+0], a1 = st[j+1], a2 = st[j+2],
                    a3 = st[j+3], a4 = st[j+4];
            st[j+0] = _mm256_xor_si256(a0, _mm256_andnot_si256(a1, a2));
            st[j+1] = _mm256_xor_si256(a1, _mm256_andnot_si256(a2, a3));
            st[j+2] = _mm256_xor_si256(a2, _mm256_andnot_si256(a3, a4));
            st[j+3] = _mm256_xor_si256(a3, _mm256_andnot_si256(a4, a0));
            st[j+4] = _mm256_xor_si256(a4, _mm256_andnot_si256(a0, a1));
        }

        /* --- Iota --- */
        st[0] = _mm256_xor_si256(st[0], _mm256_set1_epi64x((long long)RC[r]));
    }
}

/**
 * Absorb + pad for a single block (len <= KECCAK_RATE).
 * Pads each of the 4 messages independently and XORs into state.
 */
static void absorb_pad_x4(__m256i st[25],
                           const uint8_t *data[4], const size_t lens[4]) {
    /* All inputs must be < KECCAK_RATE (136 bytes). */
    uint64_t blocks[4][17];
    memset(blocks, 0, sizeof(blocks));

    for (int s = 0; s < 4; s++) {
        size_t len = lens[s] < KECCAK_RATE ? lens[s] : KECCAK_RATE - 1;
        if (data[s] && len > 0)
            memcpy(blocks[s], data[s], len);
        ((uint8_t *)blocks[s])[len] = 0x01;
        ((uint8_t *)blocks[s])[KECCAK_RATE - 1] |= 0x80;
    }

    for (int i = 0; i < 17; i++) {
        __m256i block = _mm256_set_epi64x(
            (long long)blocks[3][i], (long long)blocks[2][i],
            (long long)blocks[1][i], (long long)blocks[0][i]);
        st[i] = _mm256_xor_si256(st[i], block);
    }
}

void keccak256_avx2_x4(const uint8_t *data[4], const size_t lens[4],
                        uint8_t *out[4]) {
    /* Zero-init 4 parallel states */
    __m256i st[25];
    for (int i = 0; i < 25; i++)
        st[i] = _mm256_setzero_si256();

    /* Absorb + pad (single block, all inputs <= 136 bytes) */
    absorb_pad_x4(st, data, lens);

    /* Permute */
    keccak_f1600_x4(st);

    /* Extract 256-bit (32-byte) hash from each of the 4 states */
    /* First 4 qwords of state = first 32 bytes of hash */
    for (int i = 0; i < 4; i++) {
        uint64_t h[4];
        for (int q = 0; q < 4; q++) {
            /* Extract lane q from the 4-way interleaved state[q] */
            uint64_t lanes[4];
            _mm256_storeu_si256((__m256i *)lanes, st[q]);
            h[q] = lanes[i];
        }
        memcpy(out[i], h, 32);
    }
}

void keccak256_avx2(const uint8_t *data, size_t len, uint8_t out[32]) {
    /* Single hash — just duplicate input across all 4 slots */
    const uint8_t *d[4] = { data, data, data, data };
    size_t l[4] = { len, len, len, len };
    uint8_t *o[4] = { out, out, out, out };
    keccak256_avx2_x4(d, l, o);
}
