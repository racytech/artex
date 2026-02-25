/*
 * Page Manager Implementation — Append-Only Storage
 *
 * Pages are always appended to the end of data files (never overwritten).
 * A page index maps page_id → (file_idx, file_offset) for reads.
 * Dead pages are tracked for future compaction.
 */

#include "page_manager.h"
#include "page_gc.h"
#include "logger.h"
#include "crc32.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>
#include <stddef.h>

// Forward declaration for page index management
static void page_index_insert_or_update(page_manager_t *pm, uint64_t page_id,
                                        uint32_t file_idx, uint64_t file_offset,
                                        uint32_t compressed_size);

// Metadata file format (written atomically during checkpoint)
#define METADATA_MAGIC   0x4D455441  // "META"
#define METADATA_VERSION 2

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t next_page_id;
    uint64_t last_checkpoint_lsn;
    uint32_t current_file_idx;
    uint64_t current_file_offset;
    uint32_t checksum;
    uint32_t padding;
} __attribute__((packed)) db_metadata_t;

// Page index file format
#define PAGE_INDEX_MAGIC   0x50494458  // "PIDX"
#define PAGE_INDEX_VERSION 1

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t entry_count;
    uint64_t total_file_size;
    uint64_t dead_bytes;
    uint32_t checksum;      // CRC32 of header up to this field
    uint32_t padding;
} __attribute__((packed)) page_index_header_t;

// Reopen existing data files found in db_path
static void reopen_existing_data_files(page_manager_t *pm);

// Initialize append cursor from loaded index
static void init_append_cursor(page_manager_t *pm);

// ============================================================================
// Page Manager Core
// ============================================================================

page_manager_t *page_manager_create(const char *db_path, bool read_only) {
    if (!db_path) {
        LOG_ERROR("db_path is NULL");
        return NULL;
    }

    LOG_INFO("Creating page manager: path=%s, read_only=%d", db_path, read_only);

    // Initialize CRC32 module
    crc32_init();

    // Allocate page manager
    page_manager_t *pm = calloc(1, sizeof(page_manager_t));
    if (!pm) {
        LOG_ERROR("Failed to allocate page manager");
        return NULL;
    }

    pm->db_path = strdup(db_path);
    if (!pm->db_path) {
        LOG_ERROR("Failed to duplicate db_path");
        free(pm);
        return NULL;
    }

    pm->read_only = read_only;
    pm->compression_enabled = false;
    pm->fsync_retry_max = 3;
    pm->fsync_retry_delay_us = 100;
    pm->health.state = DB_HEALTH_OK;

    // Create database directory if it doesn't exist
    if (!read_only) {
        if (mkdir(db_path, 0755) == -1 && errno != EEXIST) {
            LOG_ERROR("Failed to create database directory: %s", strerror(errno));
            free(pm->db_path);
            free(pm);
            return NULL;
        }
    }

    // Allocate page allocator
    pm->allocator = calloc(1, sizeof(page_allocator_t));
    if (!pm->allocator) {
        LOG_ERROR("Failed to allocate page allocator");
        free(pm->db_path);
        free(pm);
        return NULL;
    }

    // Initialize allocator
    pm->allocator->next_page_id = 1;  // Page ID 0 is reserved (NULL_PAGE)
    pm->allocator->num_data_files = 0;
    pm->allocator->data_file_fds = NULL;
    pm->allocator->current_file_idx = 0;
    pm->allocator->current_file_offset = 0;

    // Allocate and initialize page index
    pm->index = calloc(1, sizeof(page_index_t));
    if (!pm->index) {
        LOG_ERROR("Failed to allocate page index");
        free(pm->allocator);
        free(pm->db_path);
        free(pm);
        return NULL;
    }

    pm->index->capacity = 1024;  // Initial capacity
    pm->index->entries = (struct page_gc_metadata *)calloc(pm->index->capacity, sizeof(page_gc_metadata_t));
    if (!pm->index->entries) {
        LOG_ERROR("Failed to allocate page index entries");
        free(pm->index);
        free(pm->allocator);
        free(pm->db_path);
        free(pm);
        return NULL;
    }

    pthread_rwlock_init(&pm->index->lock, NULL);
    pm->index->count = 0;
    pm->index->total_file_size = 0;
    pm->index->dead_bytes = 0;

    // Try to reopen existing data files first
    reopen_existing_data_files(pm);

    // Load persisted metadata (next_page_id, cursor, etc.)
    if (page_manager_load_metadata(pm)) {
        LOG_INFO("Loaded metadata: next_page_id=%lu, cursor=file%u@%lu",
                 pm->allocator->next_page_id,
                 pm->allocator->current_file_idx,
                 pm->allocator->current_file_offset);
    }

    // Load persisted page index
    if (page_manager_load_index(pm)) {
        LOG_INFO("Loaded page index: %zu entries", pm->index->count);
        // If metadata didn't have cursor (v1 upgrade), derive from index
        if (pm->allocator->current_file_offset == 0 && pm->index->count > 0) {
            init_append_cursor(pm);
        }
    }

    // Create first data file if none exist
    if (pm->allocator->num_data_files == 0 && !read_only) {
        if (page_manager_create_data_file(pm) != PAGE_SUCCESS) {
            LOG_ERROR("Failed to create initial data file");
            free(pm->index->entries);
            free(pm->index);
            free(pm->allocator);
            free(pm->db_path);
            free(pm);
            return NULL;
        }
    }

    LOG_INFO("Page manager created successfully");
    return pm;
}

void page_manager_destroy(page_manager_t *pm) {
    if (!pm) return;

    LOG_INFO("Destroying page manager");

    // Save page index before closing (for non-read-only mode)
    if (!pm->read_only) {
        page_manager_save_index(pm);
    }

    // Close all data files
    if (pm->allocator && pm->allocator->data_file_fds) {
        for (uint32_t i = 0; i < pm->allocator->num_data_files; i++) {
            if (pm->allocator->data_file_fds[i] >= 0) {
                close(pm->allocator->data_file_fds[i]);
            }
        }
        free(pm->allocator->data_file_fds);
    }

    if (pm->allocator) {
        free(pm->allocator);
    }

    // Destroy page index
    if (pm->index) {
        pthread_rwlock_destroy(&pm->index->lock);
        free(pm->index->entries);
        free(pm->index);
    }

    // Close metadata file
    if (pm->metadata_fd >= 0) {
        close(pm->metadata_fd);
    }

    free(pm->db_path);
    free(pm);

    LOG_INFO("Page manager destroyed");
}

// ============================================================================
// Page Allocation (Append-Only)
// ============================================================================

uint64_t page_manager_alloc(page_manager_t *pm, size_t size_needed) {
    if (!pm || !pm->allocator) {
        LOG_ERROR("Invalid page manager");
        return 0;
    }

    (void)size_needed;  // Size hint unused in append-only mode

    // Always allocate new page ID (monotonically increasing)
    uint64_t page_id = pm->allocator->next_page_id++;
    pm->allocator->total_pages++;
    pm->allocator->allocated_pages++;

    LOG_DEBUG("Allocated new page: page_id=%lu, total_pages=%lu",
             page_id, pm->allocator->total_pages);

    // Add placeholder to page index with sentinel values.
    // The page has no disk location yet — page_manager_read() will return
    // a zero-initialized page for sentinel entries (not yet written to disk).
    // The actual disk location is set when page_manager_write() is called.
    page_index_insert_or_update(pm, page_id,
                                UINT32_MAX, UINT64_MAX, PAGE_SIZE);

    return page_id;
}

page_result_t page_manager_free(page_manager_t *pm, uint64_t page_id) {
    if (!pm || !pm->allocator) {
        return PAGE_ERROR_INVALID_ARG;
    }

    if (page_id == 0) {
        LOG_ERROR("Cannot free page 0 (reserved)");
        return PAGE_ERROR_INVALID_ARG;
    }

    LOG_DEBUG("Marking page %lu as dead (append-only)", page_id);

    // Mark page as dead in the index (space reclaimed by compaction)
    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, page_id);
    if (meta) {
        meta->flags |= PAGE_GC_FLAG_DEAD;
        page_manager_update_dead_stats(pm, meta->compressed_size);
    }

    if (pm->allocator->allocated_pages > 0) {
        pm->allocator->allocated_pages--;
    }

    return PAGE_SUCCESS;
}

// ============================================================================
// Page I/O (Append-Only)
// ============================================================================

page_result_t page_manager_get_file_location(page_manager_t *pm, uint64_t page_id,
                                             int *fd_out, uint64_t *offset_out) {
    if (!pm || !pm->allocator || page_id == 0) {
        return PAGE_ERROR_INVALID_ARG;
    }

    // Look up page in index
    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, page_id);
    if (!meta) {
        LOG_ERROR("Page %lu not found in index", page_id);
        return PAGE_ERROR_NOT_FOUND;
    }

    if (meta->file_idx >= pm->allocator->num_data_files) {
        LOG_ERROR("Page %lu references non-existent file %u", page_id, meta->file_idx);
        return PAGE_ERROR_NOT_FOUND;
    }

    *fd_out = pm->allocator->data_file_fds[meta->file_idx];
    *offset_out = meta->file_offset;

    return PAGE_SUCCESS;
}

page_result_t page_manager_read(page_manager_t *pm, uint64_t page_id,
                                page_t *page_out) {
    if (!pm || !page_out) {
        return PAGE_ERROR_INVALID_ARG;
    }

    // Check if page is allocated but not yet written (sentinel entry)
    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, page_id);
    if (meta && meta->file_idx == UINT32_MAX) {
        // Page allocated but not yet written — return zero-initialized page
        memset(page_out, 0, sizeof(*page_out));
        page_out->header.page_id = page_id;
        page_out->header.free_offset = PAGE_HEADER_SIZE;
        page_compute_checksum(page_out);
        LOG_TRACE("Page %lu not yet written, returning zero-initialized page", page_id);
        return PAGE_SUCCESS;
    }

    int fd;
    uint64_t offset;
    page_result_t result = page_manager_get_file_location(pm, page_id, &fd, &offset);
    if (result != PAGE_SUCCESS) {
        return result;
    }

    // Read page from disk
    ssize_t bytes_read = pread(fd, page_out, PAGE_SIZE, offset);
    if (bytes_read == -1) {
        if (errno == ENOSPC) {
            LOG_ERROR("Disk full reading page %lu", page_id);
            return PAGE_ERROR_DISK_FULL;
        }
        LOG_ERROR("Failed to read page %lu: %s", page_id, strerror(errno));
        return PAGE_ERROR_IO;
    }

    if (bytes_read != PAGE_SIZE) {
        if (bytes_read == 0) {
            LOG_DEBUG("Page %lu not yet written to disk (0 bytes read)", page_id);
        } else {
            LOG_ERROR("Short read for page %lu: got %zd bytes, expected %d",
                     page_id, bytes_read, PAGE_SIZE);
        }
        memset(page_out, 0, sizeof(*page_out));
        return PAGE_ERROR_IO;
    }

    // Verify checksum
    if (!page_verify_checksum(page_out)) {
        uint32_t tail_counter;
        memcpy(&tail_counter, (uint8_t *)page_out + PAGE_TAIL_MARKER_OFFSET,
               sizeof(tail_counter));
        if (page_out->header.write_counter != tail_counter) {
            LOG_CRITICAL("Torn write detected for page %lu "
                         "(header_counter=%u, tail_counter=%u)",
                         page_id, page_out->header.write_counter, tail_counter);
            return PAGE_ERROR_TORN_WRITE;
        }
        LOG_CRITICAL("Checksum mismatch for page %lu", page_id);
        return PAGE_ERROR_CORRUPTION;
    }

    // Verify torn page detection (CRC passed but counters mismatch)
    uint32_t tail_counter;
    memcpy(&tail_counter, (uint8_t *)page_out + PAGE_TAIL_MARKER_OFFSET,
           sizeof(tail_counter));
    if (page_out->header.write_counter != tail_counter) {
        LOG_CRITICAL("Torn write detected for page %lu "
                     "(header_counter=%u, tail_counter=%u, CRC OK)",
                     page_id, page_out->header.write_counter, tail_counter);
        return PAGE_ERROR_TORN_WRITE;
    }

    pm->pages_read++;
    pm->bytes_read += PAGE_SIZE;

    LOG_TRACE("Read page %lu successfully", page_id);

    return PAGE_SUCCESS;
}

page_result_t page_manager_write(page_manager_t *pm, page_t *page) {
    if (!pm || !page) {
        return PAGE_ERROR_INVALID_ARG;
    }

    if (pm->read_only) {
        LOG_ERROR("Cannot write in read-only mode");
        return PAGE_ERROR_INVALID_ARG;
    }

    uint64_t page_id = page->header.page_id;

    // Stamp torn page detection markers (header + tail must match)
    page->header.write_counter++;
    uint32_t counter = page->header.write_counter;
    memcpy((uint8_t *)page + PAGE_TAIL_MARKER_OFFSET, &counter, sizeof(counter));
    page_compute_checksum(page);

    // Check if current file has room; if not, create new file
    uint32_t file_idx = pm->allocator->current_file_idx;
    uint64_t byte_offset = pm->allocator->current_file_offset;

    if (byte_offset + PAGE_SIZE > MAX_FILE_SIZE) {
        if (page_manager_create_data_file(pm) != PAGE_SUCCESS) {
            LOG_ERROR("Failed to create new data file for append");
            return PAGE_ERROR_IO;
        }
        file_idx = pm->allocator->num_data_files - 1;
        byte_offset = 0;
        pm->allocator->current_file_idx = file_idx;
        pm->allocator->current_file_offset = 0;
    }

    if (file_idx >= pm->allocator->num_data_files) {
        LOG_ERROR("Append cursor file_idx %u out of range (num_files=%u)",
                  file_idx, pm->allocator->num_data_files);
        return PAGE_ERROR_IO;
    }

    int fd = pm->allocator->data_file_fds[file_idx];

    // Append page at cursor position
    ssize_t bytes_written = pwrite(fd, page, PAGE_SIZE, byte_offset);
    if (bytes_written == -1) {
        if (errno == ENOSPC) {
            LOG_ERROR("Disk full writing page %lu", page_id);
            return PAGE_ERROR_DISK_FULL;
        }
        LOG_ERROR("Failed to write page %lu: %s", page_id, strerror(errno));
        return PAGE_ERROR_IO;
    }

    if (bytes_written != PAGE_SIZE) {
        LOG_ERROR("Short write for page %lu: wrote %zd bytes, expected %d",
                 page_id, bytes_written, PAGE_SIZE);
        return PAGE_ERROR_IO;
    }

    // Advance append cursor
    pm->allocator->current_file_offset += PAGE_SIZE;

    pm->pages_written++;
    pm->bytes_written += PAGE_SIZE;

    // Update page index: map page_id → (file_idx, byte_offset)
    page_index_insert_or_update(pm, page_id, file_idx, byte_offset, PAGE_SIZE);

    LOG_TRACE("Wrote page %lu at file%u@%lu (append-only)", page_id, file_idx, byte_offset);

    return PAGE_SUCCESS;
}

/**
 * Fsync with retry and exponential backoff
 */
static page_result_t fsync_with_retry(page_manager_t *pm, int fd, const char *context) {
    uint32_t retry_count = 0;
    uint64_t delay_us = pm->fsync_retry_delay_us;

    while (retry_count < pm->fsync_retry_max) {
        if (fsync(fd) == 0) {
            if (retry_count > 0) {
                LOG_WARN("fsync succeeded after %u retries (%s)", retry_count, context);
                pm->health.fsync_retries += retry_count;
                if (pm->health.state == DB_HEALTH_OK) {
                    pm->health.state = DB_HEALTH_DEGRADED;
                }
            }
            pm->health.total_fsync_calls++;
            return PAGE_SUCCESS;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == EAGAIN || errno == EIO) {
            LOG_WARN("fsync retry %u/%u: %s (%s)",
                     retry_count + 1, pm->fsync_retry_max, strerror(errno), context);
            usleep(delay_us);
            delay_us *= 2;
            retry_count++;
            continue;
        }

        break;
    }

    int saved_errno = errno;
    LOG_CRITICAL("fsync failed after %u retries: %s (%s)",
                 retry_count, strerror(saved_errno), context);
    pm->health.fsync_failures++;
    pm->health.last_error_errno = saved_errno;
    pm->health.state = DB_HEALTH_FAILING;

    return (saved_errno == ENOSPC) ? PAGE_ERROR_DISK_FULL : PAGE_ERROR_IO;
}

page_result_t page_manager_sync(page_manager_t *pm) {
    if (!pm || !pm->allocator) {
        return PAGE_ERROR_INVALID_ARG;
    }

    if (pm->read_only) {
        return PAGE_SUCCESS;
    }

    LOG_DEBUG("Syncing all data files");

    for (uint32_t i = 0; i < pm->allocator->num_data_files; i++) {
        char ctx[32];
        snprintf(ctx, sizeof(ctx), "data_file_%u", i);
        page_result_t result = fsync_with_retry(pm, pm->allocator->data_file_fds[i], ctx);
        if (result != PAGE_SUCCESS) {
            return result;
        }
    }

    LOG_INFO("All data files synced successfully");

    return PAGE_SUCCESS;
}

// ============================================================================
// Multi-File Management
// ============================================================================

page_result_t page_manager_create_data_file(page_manager_t *pm) {
    if (!pm || !pm->allocator) {
        return PAGE_ERROR_INVALID_ARG;
    }

    uint32_t file_idx = pm->allocator->num_data_files;

    // Create filename: pages_00000.dat, pages_00001.dat, etc.
    char filename[256];
    snprintf(filename, sizeof(filename), "%s/pages_%05u.dat", pm->db_path, file_idx);

    LOG_INFO("Creating data file: %s", filename);

    // Open file
    int flags = O_RDWR | O_CREAT;
    if (!pm->read_only) {
        flags |= O_SYNC;  // Write-through for durability
    }

    int fd = open(filename, flags, 0644);
    if (fd == -1) {
        LOG_ERROR("Failed to create data file %s: %s", filename, strerror(errno));
        return PAGE_ERROR_IO;
    }

    // Expand file descriptor array
    int *new_fds = realloc(pm->allocator->data_file_fds,
                           (file_idx + 1) * sizeof(int));
    if (!new_fds) {
        LOG_ERROR("Failed to expand file descriptor array");
        close(fd);
        return PAGE_ERROR_OUT_OF_MEMORY;
    }

    pm->allocator->data_file_fds = new_fds;
    pm->allocator->data_file_fds[file_idx] = fd;
    pm->allocator->num_data_files++;

    LOG_INFO("Data file created: index=%u, fd=%d", file_idx, fd);

    return PAGE_SUCCESS;
}

// ============================================================================
// Utility Functions
// ============================================================================

void page_init(page_t *page, uint64_t page_id, uint64_t version) {
    if (!page) return;

    memset(page, 0, PAGE_SIZE);

    page->header.page_id = page_id;
    page->header.version = version;
    page->header.free_offset = PAGE_HEADER_SIZE;
    page->header.num_nodes = 0;
    page->header.fragmented_bytes = 0;
    page->header.compression_type = 0;
    page->header.compressed_size = 0;
    page->header.uncompressed_size = PAGE_SIZE;
    page->header.write_counter = 0;
    page->header.prev_version = 0;
    page->header.last_access_time = time(NULL);

    page_compute_checksum(page);
}

bool page_verify_checksum(const page_t *page) {
    if (!page) return false;

    uint32_t stored_checksum = page->header.checksum;
    uint32_t computed_checksum = compute_crc32(page->data,
                                               PAGE_SIZE - PAGE_HEADER_SIZE);

    return stored_checksum == computed_checksum;
}

void page_compute_checksum(page_t *page) {
    if (!page) return;

    page->header.checksum = compute_crc32(page->data,
                                         PAGE_SIZE - PAGE_HEADER_SIZE);
}

// ============================================================================
// Page Index Management (Append-Only)
// ============================================================================

// Helper: Insert or update page in index (maintains sorted order by page_id)
static void page_index_insert_or_update(page_manager_t *pm, uint64_t page_id,
                                        uint32_t file_idx, uint64_t file_offset,
                                        uint32_t compressed_size) {
    if (!pm || !pm->index) return;

    page_index_t *index = pm->index;
    pthread_rwlock_wrlock(&index->lock);

    page_gc_metadata_t *entries = (page_gc_metadata_t *)index->entries;

    // Binary search to find existing entry or insertion point
    size_t left = 0, right = index->count;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (entries[mid].page_id == page_id) {
            // Update existing entry — old location becomes dead space
            if (entries[mid].file_idx != UINT32_MAX) {
                // Only count as dead if the old location was real (not a placeholder)
                index->dead_bytes += entries[mid].compressed_size;
            }
            entries[mid].file_idx = file_idx;
            entries[mid].file_offset = file_offset;
            entries[mid].compressed_size = compressed_size;
            entries[mid].version++;
            if (file_idx != UINT32_MAX) {
                index->total_file_size += compressed_size;
            }
            pthread_rwlock_unlock(&index->lock);
            return;
        } else if (entries[mid].page_id < page_id) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    // Not found - need to insert at position 'left'
    // Expand capacity if needed
    if (index->count >= index->capacity) {
        size_t new_capacity = index->capacity * 2;
        page_gc_metadata_t *new_entries = realloc(index->entries,
                                                   new_capacity * sizeof(page_gc_metadata_t));
        if (!new_entries) {
            LOG_ERROR("Failed to expand page index capacity");
            pthread_rwlock_unlock(&index->lock);
            return;
        }
        index->entries = (struct page_gc_metadata *)new_entries;
        index->capacity = new_capacity;
        entries = new_entries;
    }

    // Shift entries to make room
    if (left < index->count) {
        memmove(&entries[left + 1], &entries[left],
                (index->count - left) * sizeof(page_gc_metadata_t));
    }

    // Insert new entry
    memset(&entries[left], 0, sizeof(page_gc_metadata_t));
    entries[left].page_id = page_id;
    entries[left].file_idx = file_idx;
    entries[left].file_offset = file_offset;
    entries[left].compressed_size = compressed_size;
    entries[left].compression_type = 0;
    entries[left].version = 1;
    entries[left].ref_count = 0;  // Will be initialized by page_gc_init_ref
    entries[left].flags = 0;

    index->count++;
    if (file_idx != UINT32_MAX) {
        index->total_file_size += compressed_size;
    }

    pthread_rwlock_unlock(&index->lock);

    LOG_TRACE("Added page %lu to index (count=%zu, total_size=%lu)",
             page_id, index->count, index->total_file_size);
}

// ============================================================================
// Statistics
// ============================================================================

void page_manager_get_stats(page_manager_t *pm, page_manager_stats_t *stats_out) {
    if (!pm || !stats_out) return;

    memset(stats_out, 0, sizeof(page_manager_stats_t));

    if (pm->allocator) {
        stats_out->total_pages = pm->allocator->total_pages;
        stats_out->allocated_pages = pm->allocator->allocated_pages;
        stats_out->num_data_files = pm->allocator->num_data_files;
    }

    if (pm->index) {
        pthread_rwlock_rdlock(&pm->index->lock);
        stats_out->total_file_size = pm->index->total_file_size;
        stats_out->dead_bytes = pm->index->dead_bytes;

        // Count dead pages
        page_gc_metadata_t *entries = (page_gc_metadata_t *)pm->index->entries;
        for (size_t i = 0; i < pm->index->count; i++) {
            if (entries[i].flags & PAGE_GC_FLAG_DEAD) {
                stats_out->dead_pages++;
            }
        }
        pthread_rwlock_unlock(&pm->index->lock);
    }

    stats_out->pages_read = pm->pages_read;
    stats_out->pages_written = pm->pages_written;
    stats_out->bytes_read = pm->bytes_read;
    stats_out->bytes_written = pm->bytes_written;

    stats_out->avg_read_time_us = 0.0;
    stats_out->avg_write_time_us = 0.0;
}

void page_manager_print_stats(page_manager_t *pm) {
    if (!pm) return;

    page_manager_stats_t stats;
    page_manager_get_stats(pm, &stats);

    LOG_INFO("========== Page Manager Statistics ==========");
    LOG_INFO("Total pages:      %lu", stats.total_pages);
    LOG_INFO("Allocated pages:  %lu", stats.allocated_pages);
    LOG_INFO("Dead pages:       %lu", stats.dead_pages);
    LOG_INFO("Dead bytes:       %lu", stats.dead_bytes);
    LOG_INFO("");
    LOG_INFO("I/O Statistics:");
    LOG_INFO("  Pages read:    %lu", stats.pages_read);
    LOG_INFO("  Pages written: %lu", stats.pages_written);
    LOG_INFO("  Bytes read:    %lu (%.2f MB)", stats.bytes_read,
             stats.bytes_read / (1024.0 * 1024.0));
    LOG_INFO("  Bytes written: %lu (%.2f MB)", stats.bytes_written,
             stats.bytes_written / (1024.0 * 1024.0));
    LOG_INFO("");
    LOG_INFO("Files:");
    LOG_INFO("  Data files:    %u", stats.num_data_files);
    LOG_INFO("  Total size:    %lu bytes (%.2f MB)", stats.total_file_size,
             stats.total_file_size / (1024.0 * 1024.0));
    LOG_INFO("=============================================");
}

void page_manager_get_health(page_manager_t *pm, db_health_t *health_out) {
    if (!pm || !health_out) return;
    *health_out = pm->health;
}

// ============================================================================
// Compressed I/O (Stubs — will use append-only with variable sizes)
// ============================================================================

page_result_t page_manager_read_compressed(page_manager_t *pm, uint64_t page_id,
                                           page_t *page_out) {
    LOG_WARN("Compressed read not yet implemented, falling back to uncompressed");
    return page_manager_read(pm, page_id, page_out);
}

page_result_t page_manager_write_compressed(page_manager_t *pm, page_t *page,
                                            uint8_t compression_type) {
    (void)compression_type;
    LOG_WARN("Compressed write not yet implemented, falling back to uncompressed");
    return page_manager_write(pm, page);
}

// ============================================================================
// Page GC Helper Functions
// ============================================================================

struct page_gc_metadata *page_manager_get_metadata(page_manager_t *pm, uint64_t page_id) {
    if (!pm || !pm->index) {
        return NULL;
    }

    page_index_t *index = pm->index;
    pthread_rwlock_rdlock(&index->lock);

    page_gc_metadata_t *entries = (page_gc_metadata_t *)index->entries;

    // Binary search by page_id (sorted array)
    size_t left = 0;
    size_t right = index->count;
    struct page_gc_metadata *result = NULL;

    while (left < right) {
        size_t mid = left + (right - left) / 2;
        page_gc_metadata_t *entry = &entries[mid];

        if (entry->page_id == page_id) {
            result = (struct page_gc_metadata *)entry;
            break;
        } else if (entry->page_id < page_id) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    pthread_rwlock_unlock(&index->lock);
    return result;
}

struct page_gc_metadata *page_manager_get_metadata_by_index(page_manager_t *pm, size_t idx) {
    if (!pm || !pm->index) {
        return NULL;
    }

    page_index_t *index = pm->index;
    pthread_rwlock_rdlock(&index->lock);

    page_gc_metadata_t *entries = (page_gc_metadata_t *)index->entries;

    struct page_gc_metadata *result = NULL;
    if (idx < index->count) {
        result = (struct page_gc_metadata *)&entries[idx];
    }

    pthread_rwlock_unlock(&index->lock);
    return result;
}

size_t page_manager_get_num_pages(page_manager_t *pm) {
    if (!pm || !pm->index) {
        return 0;
    }

    page_index_t *index = pm->index;
    pthread_rwlock_rdlock(&index->lock);
    size_t count = index->count;
    pthread_rwlock_unlock(&index->lock);

    return count;
}

uint64_t page_manager_get_total_file_size(page_manager_t *pm) {
    if (!pm || !pm->index) {
        return 0;
    }

    page_index_t *index = pm->index;
    pthread_rwlock_rdlock(&index->lock);
    uint64_t size = index->total_file_size;
    pthread_rwlock_unlock(&index->lock);

    return size;
}

void page_manager_update_dead_stats(page_manager_t *pm, uint32_t dead_bytes) {
    if (!pm || !pm->index) {
        return;
    }

    page_index_t *index = pm->index;
    pthread_rwlock_wrlock(&index->lock);
    index->dead_bytes += dead_bytes;
    pthread_rwlock_unlock(&index->lock);
}

// ============================================================================
// Metadata Persistence (Version 2 — includes append cursor)
// ============================================================================

uint64_t page_manager_get_next_page_id(page_manager_t *pm) {
    if (!pm || !pm->allocator) return 0;
    return pm->allocator->next_page_id;
}

void page_manager_set_next_page_id(page_manager_t *pm, uint64_t next_id) {
    if (!pm || !pm->allocator) return;
    if (next_id > pm->allocator->next_page_id) {
        pm->allocator->next_page_id = next_id;
        LOG_INFO("Set next_page_id to %lu", next_id);
    }
}

bool page_manager_save_metadata(page_manager_t *pm, uint64_t checkpoint_lsn) {
    if (!pm || !pm->allocator || !pm->db_path) return false;

    db_metadata_t meta = {0};
    meta.magic = METADATA_MAGIC;
    meta.version = METADATA_VERSION;
    meta.next_page_id = pm->allocator->next_page_id;
    meta.last_checkpoint_lsn = checkpoint_lsn;
    meta.current_file_idx = pm->allocator->current_file_idx;
    meta.current_file_offset = pm->allocator->current_file_offset;
    meta.checksum = compute_crc32((const uint8_t *)&meta,
                                   offsetof(db_metadata_t, checksum));

    // Write to tmp file, fsync, rename (atomic on POSIX)
    char tmp_path[512], final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/metadata.bin", pm->db_path);
    snprintf(tmp_path, sizeof(tmp_path), "%s/metadata.bin.tmp", pm->db_path);

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_ERROR("Failed to open %s: %s", tmp_path, strerror(errno));
        return false;
    }

    ssize_t written = write(fd, &meta, sizeof(meta));
    if (written != (ssize_t)sizeof(meta)) {
        LOG_ERROR("Failed to write metadata: %s", strerror(errno));
        close(fd);
        unlink(tmp_path);
        return false;
    }

    if (fsync(fd) != 0) {
        LOG_ERROR("Failed to fsync metadata: %s", strerror(errno));
        close(fd);
        unlink(tmp_path);
        return false;
    }
    close(fd);

    if (rename(tmp_path, final_path) != 0) {
        LOG_ERROR("Failed to rename metadata: %s", strerror(errno));
        unlink(tmp_path);
        return false;
    }

    LOG_INFO("Saved metadata: next_page_id=%lu, checkpoint_lsn=%lu, cursor=file%u@%lu",
             meta.next_page_id, meta.last_checkpoint_lsn,
             meta.current_file_idx, meta.current_file_offset);
    return true;
}

bool page_manager_load_metadata(page_manager_t *pm) {
    if (!pm || !pm->allocator || !pm->db_path) return false;

    char path[512];
    snprintf(path, sizeof(path), "%s/metadata.bin", pm->db_path);

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return false;
    }

    // Read the file to determine size (version 1 is smaller)
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return false;
    }

    db_metadata_t meta = {0};
    ssize_t bytes_read = read(fd, &meta, sizeof(meta));
    close(fd);

    // Accept both v1 (smaller) and v2 (current)
    if (bytes_read < 32) {  // minimum for v1 header
        LOG_WARN("Metadata file truncated (%zd bytes)", bytes_read);
        return false;
    }

    if (meta.magic != METADATA_MAGIC) {
        LOG_WARN("Metadata file has bad magic: 0x%08X", meta.magic);
        return false;
    }

    if (meta.version == 1) {
        // Version 1: no cursor fields — derive from index later
        // V1 layout: magic(4) + version(4) + next_page_id(8) + last_checkpoint_lsn(8) + checksum(4) + padding(4) = 32 bytes
        // The checksum field is at a different offset in v1
        typedef struct {
            uint32_t magic;
            uint32_t version;
            uint64_t next_page_id;
            uint64_t last_checkpoint_lsn;
            uint32_t checksum;
            uint32_t padding;
        } __attribute__((packed)) db_metadata_v1_t;

        db_metadata_v1_t v1;
        memcpy(&v1, &meta, sizeof(v1));

        uint32_t expected_crc = compute_crc32((const uint8_t *)&v1,
                                               offsetof(db_metadata_v1_t, checksum));
        if (v1.checksum != expected_crc) {
            LOG_WARN("Metadata v1 checksum mismatch");
            return false;
        }

        page_manager_set_next_page_id(pm, v1.next_page_id);
        // Cursor will be derived from index in page_manager_create()
        LOG_INFO("Loaded v1 metadata: next_page_id=%lu (cursor derived from index)",
                 v1.next_page_id);
        return true;
    }

    if (meta.version != METADATA_VERSION) {
        LOG_WARN("Metadata file version mismatch: %u (expected %u)",
                 meta.version, METADATA_VERSION);
        return false;
    }

    if (bytes_read != (ssize_t)sizeof(meta)) {
        LOG_WARN("Metadata v2 file truncated (%zd bytes, expected %zu)",
                 bytes_read, sizeof(meta));
        return false;
    }

    uint32_t expected_crc = compute_crc32((const uint8_t *)&meta,
                                           offsetof(db_metadata_t, checksum));
    if (meta.checksum != expected_crc) {
        LOG_WARN("Metadata file checksum mismatch");
        return false;
    }

    page_manager_set_next_page_id(pm, meta.next_page_id);
    pm->allocator->current_file_idx = meta.current_file_idx;
    pm->allocator->current_file_offset = meta.current_file_offset;
    return true;
}

// ============================================================================
// Page Index Persistence
// ============================================================================

bool page_manager_save_index(page_manager_t *pm) {
    if (!pm || !pm->index || !pm->db_path) return false;

    page_index_t *index = pm->index;
    pthread_rwlock_rdlock(&index->lock);

    page_index_header_t hdr = {0};
    hdr.magic = PAGE_INDEX_MAGIC;
    hdr.version = PAGE_INDEX_VERSION;
    hdr.entry_count = index->count;
    hdr.total_file_size = index->total_file_size;
    hdr.dead_bytes = index->dead_bytes;
    hdr.checksum = compute_crc32((const uint8_t *)&hdr,
                                  offsetof(page_index_header_t, checksum));

    char tmp_path[512], final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/pages.idx", pm->db_path);
    snprintf(tmp_path, sizeof(tmp_path), "%s/pages.idx.tmp", pm->db_path);

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_ERROR("Failed to open %s: %s", tmp_path, strerror(errno));
        pthread_rwlock_unlock(&index->lock);
        return false;
    }

    // Write header
    if (write(fd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr)) {
        LOG_ERROR("Failed to write index header");
        close(fd);
        unlink(tmp_path);
        pthread_rwlock_unlock(&index->lock);
        return false;
    }

    // Write entries
    size_t entries_size = index->count * sizeof(page_gc_metadata_t);
    if (entries_size > 0) {
        ssize_t written = write(fd, index->entries, entries_size);
        if (written != (ssize_t)entries_size) {
            LOG_ERROR("Failed to write index entries");
            close(fd);
            unlink(tmp_path);
            pthread_rwlock_unlock(&index->lock);
            return false;
        }
    }

    // Write footer CRC (covers all entries)
    uint32_t entries_crc = 0;
    if (entries_size > 0) {
        entries_crc = compute_crc32((const uint8_t *)index->entries, entries_size);
    }
    if (write(fd, &entries_crc, sizeof(entries_crc)) != sizeof(entries_crc)) {
        LOG_ERROR("Failed to write index footer CRC");
        close(fd);
        unlink(tmp_path);
        pthread_rwlock_unlock(&index->lock);
        return false;
    }

    pthread_rwlock_unlock(&index->lock);

    if (fsync(fd) != 0) {
        LOG_ERROR("Failed to fsync index: %s", strerror(errno));
        close(fd);
        unlink(tmp_path);
        return false;
    }
    close(fd);

    if (rename(tmp_path, final_path) != 0) {
        LOG_ERROR("Failed to rename index: %s", strerror(errno));
        unlink(tmp_path);
        return false;
    }

    LOG_INFO("Saved page index: %lu entries to %s", hdr.entry_count, final_path);
    return true;
}

bool page_manager_load_index(page_manager_t *pm) {
    if (!pm || !pm->index || !pm->db_path) return false;

    char path[512];
    snprintf(path, sizeof(path), "%s/pages.idx", pm->db_path);

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return false;
    }

    // Read header
    page_index_header_t hdr;
    if (read(fd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr)) {
        LOG_WARN("Index file header truncated");
        close(fd);
        return false;
    }

    if (hdr.magic != PAGE_INDEX_MAGIC) {
        LOG_WARN("Index file has bad magic: 0x%08X", hdr.magic);
        close(fd);
        return false;
    }

    if (hdr.version != PAGE_INDEX_VERSION) {
        LOG_WARN("Index file version mismatch: %u", hdr.version);
        close(fd);
        return false;
    }

    uint32_t expected_crc = compute_crc32((const uint8_t *)&hdr,
                                           offsetof(page_index_header_t, checksum));
    if (hdr.checksum != expected_crc) {
        LOG_WARN("Index file header checksum mismatch");
        close(fd);
        return false;
    }

    // Read entries
    size_t entries_size = hdr.entry_count * sizeof(page_gc_metadata_t);
    page_gc_metadata_t *entries = NULL;

    if (hdr.entry_count > 0) {
        entries = malloc(entries_size);
        if (!entries) {
            LOG_ERROR("Failed to allocate index entries for load");
            close(fd);
            return false;
        }

        ssize_t bytes_read = read(fd, entries, entries_size);
        if (bytes_read != (ssize_t)entries_size) {
            LOG_WARN("Index file entries truncated");
            free(entries);
            close(fd);
            return false;
        }
    }

    // Read and verify footer CRC
    uint32_t stored_crc;
    if (read(fd, &stored_crc, sizeof(stored_crc)) != sizeof(stored_crc)) {
        LOG_WARN("Index file footer CRC missing");
        free(entries);
        close(fd);
        return false;
    }

    uint32_t computed_crc = 0;
    if (entries_size > 0) {
        computed_crc = compute_crc32((const uint8_t *)entries, entries_size);
    }
    if (stored_crc != computed_crc) {
        LOG_WARN("Index file entries CRC mismatch");
        free(entries);
        close(fd);
        return false;
    }

    close(fd);

    // Populate index
    page_index_t *index = pm->index;
    pthread_rwlock_wrlock(&index->lock);

    // Ensure capacity
    if (hdr.entry_count > index->capacity) {
        size_t new_cap = hdr.entry_count;
        page_gc_metadata_t *new_buf = realloc(index->entries,
                                               new_cap * sizeof(page_gc_metadata_t));
        if (!new_buf) {
            LOG_ERROR("Failed to expand index for loaded entries");
            pthread_rwlock_unlock(&index->lock);
            free(entries);
            return false;
        }
        index->entries = (struct page_gc_metadata *)new_buf;
        index->capacity = new_cap;
    }

    if (hdr.entry_count > 0) {
        memcpy(index->entries, entries, entries_size);
    }
    index->count = hdr.entry_count;
    index->total_file_size = hdr.total_file_size;
    index->dead_bytes = hdr.dead_bytes;

    pthread_rwlock_unlock(&index->lock);
    free(entries);

    LOG_INFO("Loaded page index: %lu entries from %s", hdr.entry_count, path);
    return true;
}

void page_index_clear(page_manager_t *pm) {
    if (!pm || !pm->index) return;

    page_index_t *index = pm->index;
    pthread_rwlock_wrlock(&index->lock);

    index->count = 0;
    index->total_file_size = 0;
    index->dead_bytes = 0;

    pthread_rwlock_unlock(&index->lock);

    // Reset append cursor to beginning
    if (pm->allocator) {
        pm->allocator->current_file_idx = 0;
        pm->allocator->current_file_offset = 0;
    }

    LOG_INFO("Page index cleared");
}

// ============================================================================
// Append Cursor Initialization
// ============================================================================

static void init_append_cursor(page_manager_t *pm) {
    if (!pm || !pm->allocator || !pm->index) return;

    page_index_t *index = pm->index;
    pthread_rwlock_rdlock(&index->lock);

    page_gc_metadata_t *entries = (page_gc_metadata_t *)index->entries;
    uint32_t max_file_idx = 0;
    uint64_t max_end_offset = 0;

    for (size_t i = 0; i < index->count; i++) {
        if (entries[i].file_idx == UINT32_MAX) continue;  // Skip placeholders
        uint64_t end = entries[i].file_offset + entries[i].compressed_size;
        if (entries[i].file_idx > max_file_idx ||
            (entries[i].file_idx == max_file_idx && end > max_end_offset)) {
            max_file_idx = entries[i].file_idx;
            max_end_offset = end;
        }
    }

    pthread_rwlock_unlock(&index->lock);

    pm->allocator->current_file_idx = max_file_idx;
    pm->allocator->current_file_offset = max_end_offset;

    LOG_INFO("Initialized append cursor from index: file%u@%lu",
             max_file_idx, max_end_offset);
}

// ============================================================================
// Reopen Existing Data Files
// ============================================================================

static int cmp_uint32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    return (va > vb) - (va < vb);
}

static void reopen_existing_data_files(page_manager_t *pm) {
    DIR *dir = opendir(pm->db_path);
    if (!dir) return;

    // Collect file indices
    uint32_t *indices = NULL;
    size_t count = 0, cap = 0;

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        uint32_t idx;
        if (sscanf(entry->d_name, "pages_%05u.dat", &idx) == 1) {
            if (count >= cap) {
                cap = (cap == 0) ? 8 : cap * 2;
                indices = realloc(indices, cap * sizeof(uint32_t));
            }
            indices[count++] = idx;
        }
    }
    closedir(dir);

    if (count == 0) {
        free(indices);
        return;
    }

    // Sort indices so files are opened in order
    qsort(indices, count, sizeof(uint32_t), cmp_uint32);

    // Open each file
    for (size_t i = 0; i < count; i++) {
        char filename[512];
        snprintf(filename, sizeof(filename), "%s/pages_%05u.dat",
                 pm->db_path, indices[i]);

        int flags = pm->read_only ? O_RDONLY : (O_RDWR | O_SYNC);
        int fd = open(filename, flags);
        if (fd < 0) {
            LOG_WARN("Failed to reopen %s: %s", filename, strerror(errno));
            continue;
        }

        // Expand fd array to fit this index
        uint32_t needed = indices[i] + 1;
        if (needed > pm->allocator->num_data_files) {
            int *new_fds = realloc(pm->allocator->data_file_fds,
                                    needed * sizeof(int));
            if (!new_fds) {
                close(fd);
                continue;
            }
            // Initialize any gaps to -1
            for (uint32_t g = pm->allocator->num_data_files; g < needed; g++)
                new_fds[g] = -1;
            pm->allocator->data_file_fds = new_fds;
            pm->allocator->num_data_files = needed;
        }

        pm->allocator->data_file_fds[indices[i]] = fd;
        LOG_INFO("Reopened data file: index=%u, fd=%d", indices[i], fd);
    }

    free(indices);
    LOG_INFO("Reopened %zu existing data files", count);
}
