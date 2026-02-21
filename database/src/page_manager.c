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
#include <time.h>

// Forward declaration for page index management
static void page_index_insert_or_update(page_manager_t *pm, uint64_t page_id,
                                        uint32_t compressed_size);

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
    
    // Create first data file
    if (!read_only) {
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

page_result_t page_manager_sync(page_manager_t *pm) {
    if (!pm || !pm->allocator) {
        return PAGE_ERROR_INVALID_ARG;
    }
    
    if (pm->read_only) {
        return PAGE_SUCCESS;  // Nothing to sync
    }
    
    LOG_DEBUG("Syncing all data files");
    
    // Sync all data files
    for (uint32_t i = 0; i < pm->allocator->num_data_files; i++) {
        if (fsync(pm->allocator->data_file_fds[i]) == -1) {
            LOG_ERROR("Failed to sync data file %u: %s", i, strerror(errno));
            return PAGE_ERROR_IO;
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
