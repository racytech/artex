/**
 * CRC32-C (Castagnoli) Checksum Implementation
 * 
 * Hardware-accelerated implementation using SSE4.2 instructions.
 */

#include "crc32.h"

#include <string.h>

#ifdef __x86_64__
#include <cpuid.h>
#include <nmmintrin.h>  // SSE4.2
#endif

// CRC32-C lookup table for software fallback
static uint32_t crc32c_table[256];
static int crc32c_initialized = 0;
static int has_sse42 = -1;  // -1 = unknown, 0 = no, 1 = yes

// Check if CPU supports SSE4.2 (hardware CRC32-C)
#ifdef __x86_64__
static int cpu_has_sse42(void) {
    if (has_sse42 != -1) {
        return has_sse42;
    }
    
    uint32_t eax, ebx, ecx, edx;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        has_sse42 = (ecx & bit_SSE4_2) ? 1 : 0;
    } else {
        has_sse42 = 0;
    }
    
    return has_sse42;
}

// Hardware CRC32-C using SSE4.2 instructions (10-20x faster than table lookup)
static uint32_t compute_crc32_hw(const uint8_t *data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    
    // Process 8 bytes at a time with hardware instruction
    while (len >= 8) {
        uint64_t chunk;
        memcpy(&chunk, data, 8);  // Safe unaligned load
        crc = _mm_crc32_u64(crc, chunk);
        data += 8;
        len -= 8;
    }
    
    // Process remaining bytes
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *data);
        data++;
        len--;
    }
    
    return ~crc;
}
#endif

// Initialize CRC32-C lookup table
static void init_crc32_table(void) {
    if (crc32c_initialized) return;
    
    // CRC32-C (Castagnoli) polynomial: 0x82F63B78 reversed = 0x1EDC6F41
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1));
        }
        crc32c_table[i] = crc;
    }
    crc32c_initialized = 1;
}

// Software CRC32-C using table lookup (fallback for CPUs without SSE4.2)
static uint32_t compute_crc32_sw(const uint8_t *data, size_t len) {
    if (!crc32c_initialized) {
        init_crc32_table();
    }
    
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ data[i]) & 0xFF];
    }
    return ~crc;
}

// Public API

void crc32_init(void) {
    // Initialize lookup table
    init_crc32_table();
    
    // Detect CPU capabilities
#ifdef __x86_64__
    cpu_has_sse42();
#endif
}

uint32_t compute_crc32(const uint8_t *data, size_t len) {
#ifdef __x86_64__
    if (cpu_has_sse42()) {
        return compute_crc32_hw(data, len);
    }
#endif
    return compute_crc32_sw(data, len);
}

// Incremental CRC32-C API

uint32_t compute_crc32_begin(void) {
    return 0xFFFFFFFF;
}

uint32_t compute_crc32_update(uint32_t crc, const uint8_t *data, size_t len) {
#ifdef __x86_64__
    if (cpu_has_sse42()) {
        while (len >= 8) {
            uint64_t chunk;
            memcpy(&chunk, data, 8);
            crc = _mm_crc32_u64(crc, chunk);
            data += 8;
            len -= 8;
        }
        while (len > 0) {
            crc = _mm_crc32_u8(crc, *data);
            data++;
            len--;
        }
        return crc;
    }
#endif
    if (!crc32c_initialized) init_crc32_table();
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ data[i]) & 0xFF];
    }
    return crc;
}

uint32_t compute_crc32_finish(uint32_t crc) {
    return ~crc;
}
