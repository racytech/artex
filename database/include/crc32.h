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

#endif // CRC32_H
