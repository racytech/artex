#ifndef KECCAK256_AVX2_H
#define KECCAK256_AVX2_H

#include <stdint.h>
#include <stddef.h>

/**
 * keccak256_avx2_x4 — compute 4 independent keccak-256 hashes in parallel.
 *
 * Uses AVX2 to process 4 keccak states simultaneously.
 * Each input can have a different length (all padded independently).
 * All inputs must be <= 136 bytes (single-block; MPT nodes fit this).
 *
 * @param data   Array of 4 input pointers
 * @param lens   Array of 4 input lengths (each <= 136)
 * @param out    Array of 4 output buffers (each 32 bytes)
 */
void keccak256_avx2_x4(const uint8_t *data[4], const size_t lens[4],
                        uint8_t *out[4]);

/**
 * keccak256_avx2 — single hash using AVX2 (for benchmarking).
 */
void keccak256_avx2(const uint8_t *data, size_t len, uint8_t out[32]);

#endif /* KECCAK256_AVX2_H */
