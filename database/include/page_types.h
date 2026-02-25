#ifndef PAGE_TYPES_H
#define PAGE_TYPES_H

#include <stdint.h>

// Page size (4KB)
#define PAGE_SIZE 4096

// Page header (64 bytes at start of each page)
#define PAGE_HEADER_SIZE 64

// Torn page detection: last 4 bytes of page mirror the write counter
#define PAGE_TAIL_MARKER_OFFSET (PAGE_SIZE - sizeof(uint32_t))  // byte 4092

// Usable data area per page (excludes header and tail marker)
#define PAGE_DATA_SIZE (PAGE_SIZE - PAGE_HEADER_SIZE - sizeof(uint32_t))  // 4028 bytes

typedef struct {
    uint64_t page_id;              // Unique page ID
    uint64_t version;              // MVCC version this page was created in

    // Free space management (for within-page allocation)
    uint32_t free_offset;          // Next free byte (grows from PAGE_HEADER_SIZE)
    uint32_t num_nodes;            // Number of nodes allocated in this page
    uint32_t fragmented_bytes;     // Wasted space from deletions

    // Compression metadata
    uint8_t compression_type;      // 0=NONE, 1=LZ4, 2=ZSTD_5, 3=ZSTD_19
    uint32_t compressed_size;      // Compressed size on disk (0 if uncompressed)
    uint32_t uncompressed_size;    // Always PAGE_SIZE (4096)

    // Integrity & versioning
    uint32_t checksum;             // CRC32 of page data
    uint32_t write_counter;        // Torn page detection (mirrored at page tail)
    uint64_t prev_version;         // Previous version page_id (for CoW chain)
    uint64_t last_access_time;     // Timestamp for compression tier policy

    uint8_t padding[3];            // Padding to 64 bytes (61 + 3 = 64)
} __attribute__((packed)) page_header_t;

// Page structure (in-memory representation)
typedef struct {
    page_header_t header;
    uint8_t data[PAGE_SIZE - sizeof(page_header_t)];  // 4032 bytes
} page_t;

#endif // PAGE_TYPES_H
