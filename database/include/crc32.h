/**
 * CRC32-C (Castagnoli) Checksum Utilities
 * 
 * Hardware-accelerated CRC32-C using SSE4.2 instructions when available,
 * with software fallback for compatibility.
 */

#ifndef CRC32_H
#define CRC32_H

#include <stdint.h>
#include <stddef.h>

/**
 * Compute CRC32-C checksum
 * 
 * Uses hardware CRC32-C (SSE4.2) if available, otherwise falls back to
 * software table lookup.
 * 
 * @param data Pointer to data to checksum
 * @param len Length of data in bytes
 * @return CRC32-C checksum value
 */
uint32_t compute_crc32(const uint8_t *data, size_t len);

/**
 * Initialize CRC32 module (call once at startup)
 *
 * Detects CPU capabilities and initializes lookup tables if needed.
 */
void crc32_init(void);

/**
 * Incremental CRC32-C API for streaming computation.
 *
 * Usage:
 *   uint32_t crc = compute_crc32_begin();
 *   crc = compute_crc32_update(crc, chunk1, len1);
 *   crc = compute_crc32_update(crc, chunk2, len2);
 *   uint32_t final = compute_crc32_finish(crc);
 */
uint32_t compute_crc32_begin(void);
uint32_t compute_crc32_update(uint32_t crc, const uint8_t *data, size_t len);
uint32_t compute_crc32_finish(uint32_t crc);

#endif // CRC32_H
