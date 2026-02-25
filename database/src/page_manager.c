/*
 * Page Manager Implementation
 * 
 * Handles page allocation, I/O, and free space tracking for persistent ART.
 * Phase 1: Basic allocation and uncompressed I/O
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

// Forward declaration for page index management
static void page_index_insert_or_update(page_manager_t *pm, uint64_t page_id,
                                        uint32_t compressed_size);

// Metadata file format (written atomically during checkpoint)
#define METADATA_MAGIC   0x4D455441  // "META"
#define METADATA_VERSION 1

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t next_page_id;
    uint64_t last_checkpoint_lsn;
    uint32_t checksum;
    uint32_t padding;
} __attribute__((packed)) db_metadata_t;

// Reopen existing data files found in db_path
static void reopen_existing_data_files(page_manager_t *pm);

// ============================================================================
// Free List Management
// ============================================================================

static void add_to_free_list(page_allocator_t *alloc, size_class_t size_class, 
                             uint64_t page_id, uint32_t free_bytes) {
    free_list_node_t *node = malloc(sizeof(free_list_node_t));
    if (!node) {
        LOG_ERROR("Failed to allocate free list node");
        return;
    }
    
    node->page_id = page_id;
    node->free_bytes = free_bytes;
    node->next = alloc->free_lists[size_class];
    alloc->free_lists[size_class] = node;
    
    alloc->pages_per_class[size_class]++;
    alloc->free_pages++;
}

static void remove_from_free_list(page_allocator_t *alloc, size_class_t size_class,
                                  uint64_t page_id) {
    free_list_node_t **curr = &alloc->free_lists[size_class];
    
    while (*curr) {
        if ((*curr)->page_id == page_id) {
            free_list_node_t *to_free = *curr;
            *curr = (*curr)->next;
            free(to_free);
            
            alloc->pages_per_class[size_class]--;
            alloc->free_pages--;
            return;
        }
        curr = &(*curr)->next;
    }
}

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
    pm->compression_enabled = false;  // Phase 1: no compression
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

    // Load persisted metadata (next_page_id, etc.)
    if (page_manager_load_metadata(pm)) {
        LOG_INFO("Loaded metadata: next_page_id=%lu", pm->allocator->next_page_id);
    }

    // Create first data file if none exist
    if (pm->allocator->num_data_files == 0 && !read_only) {
        if (page_manager_create_data_file(pm) != PAGE_SUCCESS) {
            LOG_ERROR("Failed to create initial data file");
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
    
    // Close all data files
    if (pm->allocator && pm->allocator->data_file_fds) {
        for (uint32_t i = 0; i < pm->allocator->num_data_files; i++) {
            if (pm->allocator->data_file_fds[i] >= 0) {
                close(pm->allocator->data_file_fds[i]);
            }
        }
        free(pm->allocator->data_file_fds);
    }
    
    // Free all free lists
    if (pm->allocator) {
        for (int i = 0; i < SIZE_CLASS_COUNT; i++) {
            free_list_node_t *curr = pm->allocator->free_lists[i];
            while (curr) {
                free_list_node_t *next = curr->next;
                free(curr);
                curr = next;
            }
        }
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
// Page Allocation
// ============================================================================

uint64_t page_manager_alloc(page_manager_t *pm, size_t size_needed) {
    if (!pm || !pm->allocator) {
        LOG_ERROR("Invalid page manager");
        return 0;
    }
    
    // Determine minimum size class needed
    size_class_t min_class = page_get_size_class(size_needed);
    
    // Try to find an existing page with enough space
    for (size_class_t class = min_class; class < SIZE_CLASS_COUNT; class++) {
        free_list_node_t *node = pm->allocator->free_lists[class];
        if (node) {
            uint64_t page_id = node->page_id;
            
            LOG_TRACE("Reusing page %lu from size class %d (free=%u bytes)",
                     page_id, class, node->free_bytes);
            
            // Remove from free list (caller will update when they modify the page)
            remove_from_free_list(pm->allocator, class, page_id);
            
            return page_id;
        }
    }
    
    // No existing page has space - allocate new page
    uint64_t page_id = pm->allocator->next_page_id++;
    pm->allocator->total_pages++;
    pm->allocator->allocated_pages++;
    
    LOG_DEBUG("Allocated new page: page_id=%lu, total_pages=%lu",
             page_id, pm->allocator->total_pages);
    
    // Check if we need a new data file
    uint32_t file_idx = (page_id - 1) / PAGES_PER_FILE;
    if (file_idx >= pm->allocator->num_data_files) {
        if (page_manager_create_data_file(pm) != PAGE_SUCCESS) {
            LOG_ERROR("Failed to create new data file");
            return 0;
        }
    }
    
    // Add to page index (will be initialized with ref_count by caller)
    page_index_insert_or_update(pm, page_id, PAGE_SIZE);
    
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
    
    LOG_DEBUG("Freeing page %lu", page_id);
    
    // Add to empty page list
    add_to_free_list(pm->allocator, SIZE_CLASS_EMPTY, page_id, 
                     PAGE_SIZE - PAGE_HEADER_SIZE);
    
    pm->allocator->allocated_pages--;
    
    return PAGE_SUCCESS;
}

page_result_t page_manager_update_free_space(page_manager_t *pm, uint64_t page_id,
                                             uint32_t new_free_bytes) {
    if (!pm || !pm->allocator) {
        return PAGE_ERROR_INVALID_ARG;
    }
    
    size_class_t old_class = SIZE_CLASS_COUNT;
    size_class_t new_class = page_get_size_class(new_free_bytes);
    
    // Find and remove from current free list
    for (size_class_t class = 0; class < SIZE_CLASS_COUNT; class++) {
        free_list_node_t *curr = pm->allocator->free_lists[class];
        while (curr) {
            if (curr->page_id == page_id) {
                old_class = class;
                break;
            }
            curr = curr->next;
        }
        if (old_class != SIZE_CLASS_COUNT) break;
    }
    
    // Remove from old class if found
    if (old_class != SIZE_CLASS_COUNT) {
        remove_from_free_list(pm->allocator, old_class, page_id);
    }
    
    // Add to new class if there's still free space
    if (new_free_bytes > 0) {
        add_to_free_list(pm->allocator, new_class, page_id, new_free_bytes);
    }
    
    LOG_TRACE("Updated page %lu: old_class=%d, new_class=%d, free_bytes=%u",
             page_id, old_class, new_class, new_free_bytes);
    
    return PAGE_SUCCESS;
}

// ============================================================================
// Page I/O
// ============================================================================

page_result_t page_manager_get_file_location(page_manager_t *pm, uint64_t page_id,
                                             int *fd_out, uint64_t *offset_out) {
    if (!pm || !pm->allocator || page_id == 0) {
        return PAGE_ERROR_INVALID_ARG;
    }
    
    // Calculate file index and offset within file
    uint32_t file_idx = (page_id - 1) / PAGES_PER_FILE;
    uint64_t page_offset = (page_id - 1) % PAGES_PER_FILE;
    uint64_t byte_offset = page_offset * PAGE_SIZE;
    
    if (file_idx >= pm->allocator->num_data_files) {
        LOG_ERROR("Page %lu belongs to non-existent file %u", page_id, file_idx);
        return PAGE_ERROR_NOT_FOUND;
    }
    
    *fd_out = pm->allocator->data_file_fds[file_idx];
    *offset_out = byte_offset;
    
    return PAGE_SUCCESS;
}

page_result_t page_manager_read(page_manager_t *pm, uint64_t page_id, 
                                page_t *page_out) {
    if (!pm || !page_out) {
        return PAGE_ERROR_INVALID_ARG;
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
            // Page allocated but not yet written - normal in concurrent scenarios
            LOG_DEBUG("Page %lu not yet written to disk (0 bytes read)", page_id);
        } else {
            // Partial read - actual error
            LOG_ERROR("Short read for page %lu: got %zd bytes, expected %d",
                     page_id, bytes_read, PAGE_SIZE);
        }
        // Clear the buffer to avoid using partial/garbage data
        memset(page_out, 0, sizeof(*page_out));
        return PAGE_ERROR_IO;
    }
    
    // Verify checksum
    if (!page_verify_checksum(page_out)) {
        LOG_CRITICAL("Checksum mismatch for page %lu", page_id);
        return PAGE_ERROR_CORRUPTION;
    }
    
    pm->pages_read++;
    pm->bytes_read += PAGE_SIZE;
    
    LOG_TRACE("Read page %lu successfully", page_id);
    
    return PAGE_SUCCESS;
}

page_result_t page_manager_write(page_manager_t *pm, const page_t *page) {
    if (!pm || !page) {
        return PAGE_ERROR_INVALID_ARG;
    }
    
    if (pm->read_only) {
        LOG_ERROR("Cannot write in read-only mode");
        return PAGE_ERROR_INVALID_ARG;
    }
    
    uint64_t page_id = page->header.page_id;
    
    int fd;
    uint64_t offset;
    page_result_t result = page_manager_get_file_location(pm, page_id, &fd, &offset);
    if (result != PAGE_SUCCESS) {
        return result;
    }
    
    // Write page to disk
    ssize_t bytes_written = pwrite(fd, page, PAGE_SIZE, offset);
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
    
    pm->pages_written++;
    pm->bytes_written += PAGE_SIZE;
    
    // Update page index with current compressed size
    // Note: Using PAGE_SIZE for now; real compression would update this
    page_index_insert_or_update(pm, page_id, PAGE_SIZE);
    
    LOG_TRACE("Wrote page %lu successfully", page_id);
    
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
            continue;  // Signal interrupt — retry immediately
        }

        if (errno == EAGAIN || errno == EIO) {
            LOG_WARN("fsync retry %u/%u: %s (%s)",
                     retry_count + 1, pm->fsync_retry_max, strerror(errno), context);
            usleep(delay_us);
            delay_us *= 2;
            retry_count++;
            continue;
        }

        // ENOSPC or other unrecoverable error — no point retrying
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

size_class_t page_get_size_class(uint32_t free_bytes) {
    if (free_bytes == 0) {
        return SIZE_CLASS_TINY;
    }
    if (free_bytes >= PAGE_SIZE - PAGE_HEADER_SIZE) {
        return SIZE_CLASS_EMPTY;
    }
    if (free_bytes >= 3072) {
        return SIZE_CLASS_HUGE;
    }
    if (free_bytes >= 2048) {
        return SIZE_CLASS_LARGE;
    }
    if (free_bytes >= 1024) {
        return SIZE_CLASS_MEDIUM;
    }
    if (free_bytes >= 512) {
        return SIZE_CLASS_SMALL;
    }
    return SIZE_CLASS_TINY;
}

void page_init(page_t *page, uint64_t page_id, uint64_t version) {
    if (!page) return;
    
    memset(page, 0, PAGE_SIZE);
    
    page->header.page_id = page_id;
    page->header.version = version;
    page->header.free_offset = PAGE_HEADER_SIZE;
    page->header.num_nodes = 0;
    page->header.fragmented_bytes = 0;
    page->header.compression_type = 0;  // No compression
    page->header.compressed_size = 0;
    page->header.uncompressed_size = PAGE_SIZE;
    page->header.flags = 0;
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
// Page Index Management
// ============================================================================

// Helper: Insert or update page in index (maintains sorted order by page_id)
static void page_index_insert_or_update(page_manager_t *pm, uint64_t page_id,
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
            // Update existing entry
            entries[mid].compressed_size = compressed_size;
            entries[mid].version++;
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
        entries = new_entries;  // Update local pointer
    }
    
    // Shift entries to make room
    if (left < index->count) {
        memmove(&entries[left + 1], &entries[left],
                (index->count - left) * sizeof(page_gc_metadata_t));
    }
    
    // Insert new entry
    memset(&entries[left], 0, sizeof(page_gc_metadata_t));
    entries[left].page_id = page_id;
    entries[left].file_offset = index->total_file_size;
    entries[left].compressed_size = compressed_size;
    entries[left].compression_type = 0;  // No compression
    entries[left].version = 1;
    entries[left].ref_count = 0;  // Will be initialized by page_gc_init_ref
    entries[left].flags = 0;
    
    index->count++;
    index->total_file_size += compressed_size;
    
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
        stats_out->free_pages = pm->allocator->free_pages;
        
        for (int i = 0; i < SIZE_CLASS_COUNT; i++) {
            stats_out->pages_per_class[i] = pm->allocator->pages_per_class[i];
        }
        
        stats_out->num_data_files = pm->allocator->num_data_files;
        stats_out->total_file_size = pm->allocator->num_data_files * MAX_FILE_SIZE;
    }
    
    stats_out->pages_read = pm->pages_read;
    stats_out->pages_written = pm->pages_written;
    stats_out->bytes_read = pm->bytes_read;
    stats_out->bytes_written = pm->bytes_written;
    
    // Calculate average I/O times (placeholder - would need timing implementation)
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
    LOG_INFO("Free pages:       %lu", stats.free_pages);
    LOG_INFO("");
    LOG_INFO("Pages per size class:");
    LOG_INFO("  TINY:   %lu", stats.pages_per_class[SIZE_CLASS_TINY]);
    LOG_INFO("  SMALL:  %lu", stats.pages_per_class[SIZE_CLASS_SMALL]);
    LOG_INFO("  MEDIUM: %lu", stats.pages_per_class[SIZE_CLASS_MEDIUM]);
    LOG_INFO("  LARGE:  %lu", stats.pages_per_class[SIZE_CLASS_LARGE]);
    LOG_INFO("  HUGE:   %lu", stats.pages_per_class[SIZE_CLASS_HUGE]);
    LOG_INFO("  EMPTY:  %lu", stats.pages_per_class[SIZE_CLASS_EMPTY]);
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
// Compressed I/O (Phase 3 - Stubs for now)
// ============================================================================

page_result_t page_manager_read_compressed(page_manager_t *pm, uint64_t page_id,
                                           page_t *page_out) {
    // Phase 3: Implement compression
    LOG_WARN("Compressed read not yet implemented, falling back to uncompressed");
    return page_manager_read(pm, page_id, page_out);
}

page_result_t page_manager_write_compressed(page_manager_t *pm, const page_t *page,
                                            uint8_t compression_type) {
    // Phase 3: Implement compression
    (void)compression_type;
    LOG_WARN("Compressed write not yet implemented, falling back to uncompressed");
    return page_manager_write(pm, page);
}

// ============================================================================
// Page GC Helper Functions
// ============================================================================

#include "page_gc.h"

struct page_gc_metadata *page_manager_get_metadata(page_manager_t *pm, uint64_t page_id) {
    if (!pm || !pm->index) {
        return NULL;
    }
    
    page_index_t *index = pm->index;
    pthread_rwlock_rdlock(&index->lock);
    
    // Cast to page_gc_metadata_t * for indexing (since full definition is available here)
    page_gc_metadata_t *entries = (page_gc_metadata_t *)index->entries;
    
    // Binary search by page_id (assumes entries are sorted)
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

struct page_gc_metadata *page_manager_get_metadata_by_index(page_manager_t *pm, size_t index) {
    if (!pm || !pm->index) {
        return NULL;
    }
    
    page_index_t *idx = pm->index;
    pthread_rwlock_rdlock(&idx->lock);
    
    // Cast to page_gc_metadata_t * for indexing
    page_gc_metadata_t *entries = (page_gc_metadata_t *)idx->entries;
    
    struct page_gc_metadata *result = NULL;
    if (index < idx->count) {
        result = (struct page_gc_metadata *)&entries[index];
    }
    
    pthread_rwlock_unlock(&idx->lock);
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
// Metadata Persistence
// ============================================================================

uint64_t page_manager_get_next_page_id(page_manager_t *pm) {
    if (!pm || !pm->allocator) return 0;
    return pm->allocator->next_page_id;
}

void page_manager_set_next_page_id(page_manager_t *pm, uint64_t next_id) {
    if (!pm || !pm->allocator) return;
    // Only increase — never set to a value lower than current
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
    if (written != sizeof(meta)) {
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

    LOG_INFO("Saved metadata: next_page_id=%lu, checkpoint_lsn=%lu",
             meta.next_page_id, meta.last_checkpoint_lsn);
    return true;
}

bool page_manager_load_metadata(page_manager_t *pm) {
    if (!pm || !pm->allocator || !pm->db_path) return false;

    char path[512];
    snprintf(path, sizeof(path), "%s/metadata.bin", pm->db_path);

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        // File doesn't exist on first run — not an error
        return false;
    }

    db_metadata_t meta;
    ssize_t bytes_read = read(fd, &meta, sizeof(meta));
    close(fd);

    if (bytes_read != sizeof(meta)) {
        LOG_WARN("Metadata file truncated (%zd bytes)", bytes_read);
        return false;
    }

    if (meta.magic != METADATA_MAGIC) {
        LOG_WARN("Metadata file has bad magic: 0x%08X", meta.magic);
        return false;
    }

    if (meta.version != METADATA_VERSION) {
        LOG_WARN("Metadata file version mismatch: %u (expected %u)",
                 meta.version, METADATA_VERSION);
        return false;
    }

    uint32_t expected_crc = compute_crc32((const uint8_t *)&meta,
                                           offsetof(db_metadata_t, checksum));
    if (meta.checksum != expected_crc) {
        LOG_WARN("Metadata file checksum mismatch");
        return false;
    }

    page_manager_set_next_page_id(pm, meta.next_page_id);
    return true;
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
