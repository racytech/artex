#ifndef KECCAK256_AVX512_H
#define KECCAK256_AVX512_H

#include <stdint.h>
#include <stddef.h>

/**
 * keccak256_avx512_x8 — compute 8 independent keccak-256 hashes in parallel.
 *
 * Uses AVX-512 to process 8 keccak states simultaneously.
 * All inputs must be <= 136 bytes (single-block; MPT nodes fit this).
 *
 * @param data   Array of 8 input pointers
 * @param lens   Array of 8 input lengths (each <= 136)
 * @param out    Array of 8 output buffers (each 32 bytes)
 */
void keccak256_avx512_x8(const uint8_t *data[8], const size_t lens[8],
                          uint8_t *out[8]);

/**
 * keccak256_avx512 — single hash using AVX-512 (for benchmarking).
 */
void keccak256_avx512(const uint8_t *data, size_t len, uint8_t out[32]);

#endif /* KECCAK256_AVX512_H */
