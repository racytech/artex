#ifndef PAGE_MANAGER_H
#define PAGE_MANAGER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>

// Page size (4KB)
#define PAGE_SIZE 4096

// Max file size (512 MB per data file)
#define MAX_FILE_SIZE (512ULL * 1024 * 1024)
#define PAGES_PER_FILE (MAX_FILE_SIZE / PAGE_SIZE)  // 131,072 pages per file

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
    
    // Compression metadata (Phase 3)
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

// Page allocator (append-only)
typedef struct {
    // Statistics
    uint64_t total_pages;
    uint64_t allocated_pages;

    // Next page ID for new allocations (monotonically increasing)
    uint64_t next_page_id;

    // Multi-file management
    uint32_t num_data_files;
    int *data_file_fds;  // Array of open file descriptors

    // Append cursor (append-only storage)
    uint32_t current_file_idx;     // Which file to append to
    uint64_t current_file_offset;  // Next byte offset in current file
} page_allocator_t;

// Forward declaration - full definition in page_gc.h
struct page_gc_metadata;

// Page index (tracks all pages with GC metadata)
typedef struct {
    struct page_gc_metadata *entries;  // Array of page metadata
    size_t count;                      // Number of entries
    size_t capacity;                   // Array capacity
    uint64_t total_file_size;          // Sum of all compressed sizes
    uint64_t dead_bytes;               // Bytes consumed by dead pages
    pthread_rwlock_t lock;             // Protects concurrent access
} page_index_t;

// Database health state
typedef enum {
    DB_HEALTH_OK       = 0,  // All systems normal
    DB_HEALTH_DEGRADED = 1,  // Transient errors encountered (retries succeeded)
    DB_HEALTH_FAILING  = 2,  // Persistent I/O errors (retries exhausted)
} db_health_state_t;

typedef struct {
    db_health_state_t state;
    uint64_t total_fsync_calls;
    uint64_t fsync_retries;      // Total retries across all calls
    uint64_t fsync_failures;     // Calls that failed after all retries
    uint64_t last_error_errno;   // errno from last failure
} db_health_t;

// Page manager (coordinates allocation + I/O)
typedef struct {
    char *db_path;              // Database directory path
    page_allocator_t *allocator;
    page_index_t *index;        // Page index with GC metadata

    // File handles
    int metadata_fd;

    // Configuration
    bool read_only;
    bool compression_enabled;
    uint8_t default_compression_type;  // COMPRESSION_NONE, LZ4, ZSTD_5, ZSTD_19
    uint32_t fsync_retry_max;        // Max fsync retries (default: 3)
    uint32_t fsync_retry_delay_us;   // Initial backoff in microseconds (default: 100)

    // Health monitoring
    db_health_t health;

    // Statistics
    uint64_t pages_read;
    uint64_t pages_written;
    uint64_t bytes_read;
    uint64_t bytes_written;
} page_manager_t;

// Compression types
typedef enum {
    COMPRESSION_NONE    = 0,
    COMPRESSION_LZ4     = 1,
    COMPRESSION_ZSTD_5  = 2,
    COMPRESSION_ZSTD_19 = 3,
} compression_type_t;

// Result codes
typedef enum {
    PAGE_SUCCESS = 0,
    PAGE_ERROR_NOT_FOUND = -1,
    PAGE_ERROR_IO = -2,
    PAGE_ERROR_DISK_FULL = -3,
    PAGE_ERROR_CORRUPTION = -4,
    PAGE_ERROR_INVALID_ARG = -5,
    PAGE_ERROR_OUT_OF_MEMORY = -6,
    PAGE_ERROR_TORN_WRITE = -7,
} page_result_t;

// ============================================================================
// Initialization & Cleanup
// ============================================================================

/**
 * Create a new page manager
 * 
 * @param db_path Directory path for database files
 * @param read_only Open in read-only mode
 * @return Page manager instance, or NULL on error
 */
page_manager_t *page_manager_create(const char *db_path, bool read_only);

/**
 * Destroy page manager and release resources
 * 
 * @param pm Page manager instance
 */
void page_manager_destroy(page_manager_t *pm);

// ============================================================================
// Page Allocation
// ============================================================================

/**
 * Allocate a new page ID
 * 
 * @param pm Page manager instance
 * @param size_needed Minimum free space needed in page (0 for empty page)
 * @return Page ID, or 0 on error
 */
uint64_t page_manager_alloc(page_manager_t *pm, size_t size_needed);

/**
 * Free a page (mark as dead in append-only storage)
 *
 * Dead pages are not reused — space reclaimed by compaction.
 *
 * @param pm Page manager instance
 * @param page_id Page ID to free
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_free(page_manager_t *pm, uint64_t page_id);

// ============================================================================
// Page I/O (Uncompressed)
// ============================================================================

/**
 * Read a page from disk
 * 
 * @param pm Page manager instance
 * @param page_id Page ID to read
 * @param page_out Output buffer (must be PAGE_SIZE bytes)
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_read(page_manager_t *pm, 
                                uint64_t page_id, 
                                page_t *page_out);

/**
 * Write a page to disk
 * 
 * @param pm Page manager instance
 * @param page Page to write (page_id must be set in header)
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_write(page_manager_t *pm, page_t *page);

/**
 * Sync all pending writes to disk
 * 
 * @param pm Page manager instance
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_sync(page_manager_t *pm);

// ============================================================================
// Page I/O (Compressed) - Phase 3
// ============================================================================

/**
 * Read a compressed page from disk (decompresses automatically)
 * 
 * @param pm Page manager instance
 * @param page_id Page ID to read
 * @param page_out Output buffer (decompressed, PAGE_SIZE bytes)
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_read_compressed(page_manager_t *pm,
                                           uint64_t page_id,
                                           page_t *page_out);

/**
 * Write a page with compression
 * 
 * @param pm Page manager instance
 * @param page Page to write
 * @param compression_type Compression algorithm (0=none, 1=LZ4, 2=ZSTD)
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_write_compressed(page_manager_t *pm,
                                            page_t *page,
                                            uint8_t compression_type);

/**
 * Enable/disable page compression
 *
 * @param pm Page manager instance
 * @param compression_type Compression algorithm (COMPRESSION_NONE to disable)
 */
void page_manager_set_compression(page_manager_t *pm, uint8_t compression_type);

// ============================================================================
// Multi-File Management
// ============================================================================

/**
 * Get file descriptor for page ID
 * 
 * @param pm Page manager instance
 * @param page_id Page ID
 * @param fd_out Output: file descriptor
 * @param offset_out Output: offset within file
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_get_file_location(page_manager_t *pm,
                                             uint64_t page_id,
                                             int *fd_out,
                                             uint64_t *offset_out);

/**
 * Create a new data file (when current file full)
 * 
 * @param pm Page manager instance
 * @return PAGE_SUCCESS on success, error code otherwise
 */
page_result_t page_manager_create_data_file(page_manager_t *pm);

// ============================================================================
// Statistics & Monitoring
// ============================================================================

typedef struct {
    // Allocation statistics
    uint64_t total_pages;
    uint64_t allocated_pages;
    uint64_t dead_pages;           // Pages marked dead (append-only)
    uint64_t dead_bytes;           // Bytes consumed by dead pages

    // I/O statistics
    uint64_t pages_read;
    uint64_t pages_written;
    uint64_t bytes_read;
    uint64_t bytes_written;

    // File statistics
    uint32_t num_data_files;
    uint64_t total_file_size;

    // Performance metrics
    double avg_read_time_us;
    double avg_write_time_us;
} page_manager_stats_t;

/**
 * Get page manager statistics
 * 
 * @param pm Page manager instance
 * @param stats_out Output: statistics structure
 */
void page_manager_get_stats(page_manager_t *pm, page_manager_stats_t *stats_out);

/**
 * Print statistics (for debugging)
 *
 * @param pm Page manager instance
 */
void page_manager_print_stats(page_manager_t *pm);

/**
 * Get database health state
 *
 * @param pm Page manager instance
 * @param health_out Output: health information
 */
void page_manager_get_health(page_manager_t *pm, db_health_t *health_out);

// ============================================================================
// Validation & Integrity
// ============================================================================

/**
 * Verify page checksum
 * 
 * @param page Page to verify
 * @return true if checksum valid, false otherwise
 */
bool page_verify_checksum(const page_t *page);

/**
 * Compute page checksum
 * 
 * @param page Page to checksum (updates header.checksum)
 */
void page_compute_checksum(page_t *page);

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Initialize a new page
 * 
 * @param page Page to initialize
 * @param page_id Page ID
 * @param version Version number
 */
void page_init(page_t *page, uint64_t page_id, uint64_t version);

// ============================================================================
// Page Index Access (for page_gc integration)
// ============================================================================

// Forward declaration - actual struct defined in page_gc.h
struct page_gc_metadata;

/**
 * Get page metadata from index by page ID
 * 
 * Used by page_gc for reference counting.
 * Returns pointer to metadata in page index (not a copy).
 * 
 * @param pm Page manager instance
 * @param page_id Page ID to look up
 * @return Pointer to metadata, or NULL if not found
 */
struct page_gc_metadata *page_manager_get_metadata(page_manager_t *pm, uint64_t page_id);

/**
 * Get page metadata from index by array index
 * 
 * Used by page_gc for scanning all pages.
 * 
 * @param pm Page manager instance  
 * @param index Array index (0 to num_pages-1)
 * @return Pointer to metadata, or NULL if out of bounds
 */
struct page_gc_metadata *page_manager_get_metadata_by_index(page_manager_t *pm, size_t index);

/**
 * Get total number of pages in index
 * 
 * @param pm Page manager instance
 * @return Number of pages
 */
size_t page_manager_get_num_pages(page_manager_t *pm);

/**
 * Get total size of all data files
 * 
 * @param pm Page manager instance
 * @return Total bytes across all data files
 */
uint64_t page_manager_get_total_file_size(page_manager_t *pm);

/**
 * Update dead page statistics
 * 
 * Called by page_gc when marking a page as dead.
 * Updates internal counters for dead page tracking.
 * 
 * @param pm Page manager instance
 * @param dead_bytes Bytes consumed by newly dead page
 */
void page_manager_update_dead_stats(page_manager_t *pm, uint32_t dead_bytes);

// ============================================================================
// Metadata Persistence
// ============================================================================

/**
 * Get the next page ID that will be allocated.
 */
uint64_t page_manager_get_next_page_id(page_manager_t *pm);

/**
 * Set the next page ID (used during recovery).
 * Only increases — never sets to a value lower than current.
 */
void page_manager_set_next_page_id(page_manager_t *pm, uint64_t next_id);

/**
 * Save allocator metadata to disk (metadata.bin).
 * Writes atomically via tmp + rename.
 *
 * @param checkpoint_lsn LSN of the checkpoint being saved
 */
bool page_manager_save_metadata(page_manager_t *pm, uint64_t checkpoint_lsn);

/**
 * Load allocator metadata from disk (metadata.bin).
 * Returns true if metadata was loaded, false if file missing or invalid.
 * On failure, allocator state is left unchanged (defaults apply).
 */
bool page_manager_load_metadata(page_manager_t *pm);

// ============================================================================
// Page Index Persistence (append-only storage)
// ============================================================================

/**
 * Save page index to disk (pages.idx).
 * Writes atomically via tmp + fsync + rename.
 */
bool page_manager_save_index(page_manager_t *pm);

/**
 * Load page index from disk (pages.idx).
 * Returns true if index was loaded, false if file missing or invalid.
 */
bool page_manager_load_index(page_manager_t *pm);

/**
 * Clear all entries from the page index.
 * Used during full WAL replay (start_lsn=0) to rebuild from scratch.
 */
void page_index_clear(page_manager_t *pm);

// ============================================================================
// Compaction Support
// ============================================================================

/**
 * Get a snapshot copy of all page index entries.
 * Caller must free() the returned array.
 *
 * @param pm Page manager instance
 * @param count_out Number of entries in returned array
 * @return Heap-allocated copy of index entries, or NULL if empty/error
 */
struct page_gc_metadata *page_manager_get_index_snapshot(page_manager_t *pm, size_t *count_out);

/**
 * Replace the entire page index with new entries.
 * Takes ownership of new_entries (caller must not free).
 *
 * @param pm Page manager instance
 * @param new_entries New entries array (heap-allocated, ownership transferred)
 * @param count Number of entries
 * @param total_file_size Sum of all live compressed sizes
 * @param dead_bytes Dead bytes (typically 0 after compaction)
 */
void page_manager_replace_index(page_manager_t *pm,
                                struct page_gc_metadata *new_entries,
                                size_t count,
                                uint64_t total_file_size,
                                uint64_t dead_bytes);

/**
 * Reset the append cursor to a specific position.
 * Used after compaction to point to end of compacted files.
 *
 * @param pm Page manager instance
 * @param file_idx Data file index
 * @param file_offset Byte offset within that file
 */
void page_manager_reset_cursor(page_manager_t *pm, uint32_t file_idx, uint64_t file_offset);

/**
 * Get file descriptor for a specific data file index.
 *
 * @param pm Page manager instance
 * @param file_idx Data file index
 * @return File descriptor, or -1 if invalid
 */
int page_manager_get_data_fd(page_manager_t *pm, uint32_t file_idx);

/**
 * Replace data file descriptor (close old, set new).
 * Used during compaction atomic cutover.
 *
 * @param pm Page manager instance
 * @param file_idx Data file index
 * @param new_fd New file descriptor
 */
void page_manager_replace_data_fd(page_manager_t *pm, uint32_t file_idx, int new_fd);

/**
 * Update a single page's file_offset in the index.
 * Used by compaction (in-place moves) and compaction recovery.
 *
 * @param pm Page manager instance
 * @param page_id Page ID to update
 * @param new_offset New file offset
 */
void page_manager_update_page_offset(page_manager_t *pm, uint64_t page_id,
                                     uint64_t new_offset);

#endif // PAGE_MANAGER_H
