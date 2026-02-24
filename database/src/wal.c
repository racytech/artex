/**
 * Write-Ahead Log (WAL) Implementation
 * 
 * See wal.h for API documentation.
 */

#include "wal.h"
#include "db_error.h"
#include "logger.h"
#include "crc32.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>

// ============================================================================
// Internal Helper Functions
// ============================================================================

/**
 * Calculate CRC32 checksum for WAL entry
 */
static uint32_t calculate_entry_checksum(const wal_entry_header_t *header,
                                         const void *payload) {
    // Checksum covers header (excluding checksum field) + payload
    uint32_t crc = 0;
    
    // Hash header up to checksum field
    crc = compute_crc32((const uint8_t *)header, offsetof(wal_entry_header_t, checksum));
    
    // Hash payload if present
    if (header->payload_len > 0 && payload != NULL) {
        crc = compute_crc32((const uint8_t *)payload, header->payload_len);
    }
    
    return crc;
}

/**
 * Format segment filename
 */
static void format_segment_filename(char *buf, size_t buflen,
                                    const char *wal_dir, uint64_t segment_id) {
    snprintf(buf, buflen, "%s/wal_%06lu.log", wal_dir, segment_id);
}

/**
 * Create directory if it doesn't exist
 */
static bool ensure_directory_exists(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            return true;
        }
        LOG_ERROR("Path exists but is not a directory: %s", path);
        return false;
    }
    
    if (mkdir(path, 0755) == -1) {
        LOG_ERROR("Failed to create directory %s: %s", path, strerror(errno));
        return false;
    }
    
    return true;
}

/**
 * Get current time in seconds
 */
static inline uint64_t get_time_sec(void) {
    return (uint64_t)time(NULL);
}

/**
 * Fsync with retry logic (from ERROR_HANDLING.md)
 */
static bool fsync_with_retry(int fd, wal_t *wal, const char *context) {
    uint32_t retry_count = 0;
    uint64_t retry_delay_us = wal->config.fsync_retry_delay_us;
    
    while (retry_count < wal->config.fsync_retry_max) {
        if (fsync(fd) == 0) {
            if (retry_count > 0) {
                LOG_WARN("fsync succeeded after %u retries (%s)",
                        retry_count, context);
                wal->stats.fsync_retries += retry_count;
            }
            wal->stats.fsync_calls++;
            return true;
        }
        
        // Check error type
        if (errno == EINTR) {
            // Interrupted by signal - retry immediately
            continue;
        }
        
        if (errno == EAGAIN || errno == EIO) {
            // Transient error - retry with backoff
            LOG_WARN("fsync retry %u/%u: %s (%s)",
                    retry_count + 1, wal->config.fsync_retry_max,
                    strerror(errno), context);
            
            usleep(retry_delay_us);
            retry_delay_us *= 2;  // Exponential backoff
            retry_count++;
            continue;
        }
        
        // Unrecoverable error
        break;
    }
    
    // Failed after retries or unrecoverable error
    LOG_CRITICAL("fsync failed after %u retries: %s (%s)",
                retry_count, strerror(errno), context);
    wal->stats.fsync_failures++;
    return false;
}

// ============================================================================
// Segment Management
// ============================================================================

/**
 * Create new segment header
 */
static wal_segment_header_t create_segment_header(uint64_t segment_id,
                                                   uint64_t first_lsn) {
    wal_segment_header_t header;
    memset(&header, 0, sizeof(header));
    
    header.magic = WAL_MAGIC;
    header.version = WAL_VERSION;
    header.segment_id = segment_id;
    header.first_lsn = first_lsn;
    header.last_lsn = first_lsn;
    header.created_at = get_time_sec();
    header.entry_count = 0;
    
    // Calculate checksum (excluding checksum field itself)
    header.checksum = compute_crc32((const uint8_t *)&header, offsetof(wal_segment_header_t, checksum));
    
    return header;
}

/**
 * Open or create a segment file
 */
static int open_segment(wal_t *wal, uint64_t segment_id, bool create) {
    char filename[512];
    format_segment_filename(filename, sizeof(filename), wal->wal_dir, segment_id);
    
    int flags = O_RDWR;
    if (create) {
        flags |= O_CREAT | O_TRUNC;
    }
    
    int fd = open(filename, flags, 0644);
    if (fd == -1) {
        LOG_ERROR("Failed to open segment %lu: %s", segment_id, strerror(errno));
        return -1;
    }
    
    if (create) {
        // Write segment header
        wal_segment_header_t header = create_segment_header(segment_id, wal->next_lsn);
        
        if (write(fd, &header, sizeof(header)) != sizeof(header)) {
            LOG_ERROR("Failed to write segment header: %s", strerror(errno));
            close(fd);
            return -1;
        }
        
        // Allocate in-memory copy
        wal->segment_header = malloc(sizeof(wal_segment_header_t));
        if (!wal->segment_header) {
            LOG_ERROR("Failed to allocate segment header");
            close(fd);
            return -1;
        }
        *wal->segment_header = header;
        
        wal->stats.total_segments++;
        LOG_INFO("Created WAL segment %lu at %s", segment_id, filename);
    } else {
        // Read existing segment header
        wal_segment_header_t header;
        if (read(fd, &header, sizeof(header)) != sizeof(header)) {
            LOG_ERROR("Failed to read segment header: %s", strerror(errno));
            close(fd);
            return -1;
        }
        
        // Validate header
        if (header.magic != WAL_MAGIC) {
            LOG_ERROR("Invalid segment magic: 0x%x", header.magic);
            close(fd);
            return -1;
        }
        
        if (header.version != WAL_VERSION) {
            LOG_ERROR("Unsupported WAL version: %lu", header.version);
            close(fd);
            return -1;
        }
        
        // Allocate in-memory copy
        wal->segment_header = malloc(sizeof(wal_segment_header_t));
        if (!wal->segment_header) {
            LOG_ERROR("Failed to allocate segment header");
            close(fd);
            return -1;
        }
        *wal->segment_header = header;
        
        // Seek to end for appending
        off_t end = lseek(fd, 0, SEEK_END);
        if (end == -1) {
            LOG_ERROR("Failed to seek to end of segment: %s", strerror(errno));
            close(fd);
            return -1;
        }
        wal->segment_offset = end;
        
        LOG_INFO("Opened existing WAL segment %lu", segment_id);
    }
    
    return fd;
}

/**
 * Close current segment and update header
 */
static bool close_segment(wal_t *wal) {
    if (wal->segment_fd == -1) {
        return true;  // Already closed
    }
    
    // Update segment header with final stats
    wal->segment_header->last_lsn = wal->next_lsn - 1;
    
    // Write updated header back to file
    if (lseek(wal->segment_fd, 0, SEEK_SET) == -1) {
        LOG_ERROR("Failed to seek to segment header: %s", strerror(errno));
        close(wal->segment_fd);
        return false;
    }
    
    if (write(wal->segment_fd, wal->segment_header, sizeof(*wal->segment_header))
        != sizeof(*wal->segment_header)) {
        LOG_ERROR("Failed to write updated segment header: %s", strerror(errno));
        close(wal->segment_fd);
        return false;
    }
    
    // Fsync and close
    if (!fsync_with_retry(wal->segment_fd, wal, "segment close")) {
        close(wal->segment_fd);
        return false;
    }
    
    close(wal->segment_fd);
    wal->segment_fd = -1;
    
    if (wal->segment_header) {
        free(wal->segment_header);
        wal->segment_header = NULL;
    }
    
    return true;
}

/**
 * Rotate to new segment when current is full
 */
static bool rotate_segment(wal_t *wal) {
    LOG_INFO("Rotating to new WAL segment (current segment %lu is full)",
             wal->segment_id);
    
    // Close current segment
    if (!close_segment(wal)) {
        LOG_ERROR("Failed to close current segment during rotation");
        return false;
    }
    
    // Open new segment
    wal->segment_id++;
    wal->segment_offset = sizeof(wal_segment_header_t);
    
    wal->segment_fd = open_segment(wal, wal->segment_id, true);
    if (wal->segment_fd == -1) {
        LOG_ERROR("Failed to create new segment during rotation");
        return false;
    }
    
    wal->stats.current_segment_id = wal->segment_id;
    wal->stats.active_segments++;
    
    return true;
}

// ============================================================================
// Write Buffer Management
// ============================================================================

/**
 * Flush write buffer to segment file
 */
static bool flush_write_buffer(wal_t *wal) {
    if (wal->buffer_offset == 0) {
        return true;  // Nothing to flush
    }
    
    // Write buffer to current segment
    ssize_t written = write(wal->segment_fd, wal->write_buffer, wal->buffer_offset);
    if (written != (ssize_t)wal->buffer_offset) {
        LOG_ERROR("Failed to write buffer to segment: %s", strerror(errno));
        return false;
    }
    
    // Update segment offset
    wal->segment_offset += written;
    
    // Reset buffer
    wal->buffer_offset = 0;
    
    return true;
}

/**
 * Append data to write buffer, flushing if necessary
 */
static bool append_to_buffer(wal_t *wal, const void *data, size_t len) {
    // Check if data fits in buffer
    if (wal->buffer_offset + len > wal->buffer_size) {
        // Flush buffer to make room
        if (!flush_write_buffer(wal)) {
            return false;
        }
    }
    
    // Check if entry is too large for buffer
    if (len > wal->buffer_size) {
        // Write directly to segment without buffering
        ssize_t written = write(wal->segment_fd, data, len);
        if (written != (ssize_t)len) {
            LOG_ERROR("Failed to write large entry to segment: %s", strerror(errno));
            return false;
        }
        wal->segment_offset += written;
    } else {
        // Copy to buffer
        memcpy(wal->write_buffer + wal->buffer_offset, data, len);
        wal->buffer_offset += len;
    }
    
    // Update checkpoint tracking
    wal->bytes_since_checkpoint += len;
    
    return true;
}

// ============================================================================
// Public API - Lifecycle
// ============================================================================

wal_config_t wal_default_config(void) {
    wal_config_t config;
    
    // Checkpoint triggers
    config.checkpoint_size_threshold = WAL_CHECKPOINT_SIZE_THRESHOLD;
    config.checkpoint_time_threshold = WAL_CHECKPOINT_TIME_THRESHOLD;
    config.checkpoint_pages_threshold = WAL_CHECKPOINT_PAGES_THRESHOLD;
    
    // I/O behavior
    config.fsync_on_commit = true;
    config.fsync_retry_max = 3;
    config.fsync_retry_delay_us = 100;
    
    // Segment management
    config.segment_size = WAL_SEGMENT_SIZE;
    config.auto_truncate = true;
    config.keep_segments = 2;
    
    return config;
}

wal_t *wal_open(const char *wal_dir, const wal_config_t *config) {
    if (!wal_dir) {
        LOG_ERROR("WAL directory path is NULL");
        return NULL;
    }
    
    // Ensure directory exists
    if (!ensure_directory_exists(wal_dir)) {
        return NULL;
    }
    
    // Allocate WAL instance
    wal_t *wal = calloc(1, sizeof(wal_t));
    if (!wal) {
        LOG_ERROR("Failed to allocate WAL instance");
        return NULL;
    }
    
    // Set configuration
    if (config) {
        wal->config = *config;
    } else {
        wal->config = wal_default_config();
    }
    
    // Store directory path
    wal->wal_dir = strdup(wal_dir);
    if (!wal->wal_dir) {
        LOG_ERROR("Failed to allocate WAL directory path");
        free(wal);
        return NULL;
    }
    
    // Allocate write buffer
    wal->write_buffer = malloc(WAL_BUFFER_SIZE);
    if (!wal->write_buffer) {
        LOG_ERROR("Failed to allocate write buffer");
        free(wal->wal_dir);
        free(wal);
        return NULL;
    }
    wal->buffer_size = WAL_BUFFER_SIZE;
    wal->buffer_offset = 0;
    
    // Initialize lock
    if (pthread_rwlock_init(&wal->lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize WAL lock");
        free(wal->write_buffer);
        free(wal->wal_dir);
        free(wal);
        return NULL;
    }
    
    // Find existing segments
    uint32_t segment_count;
    uint64_t *segments = wal_list_segments(wal_dir, &segment_count);
    
    if (segment_count > 0) {
        // Open last segment for appending
        wal->segment_id = segments[segment_count - 1];
        wal->segment_fd = open_segment(wal, wal->segment_id, false);
        
        if (wal->segment_fd == -1) {
            LOG_ERROR("Failed to open existing segment %lu", wal->segment_id);
            free(segments);
            pthread_rwlock_destroy(&wal->lock);
            free(wal->write_buffer);
            free(wal->wal_dir);
            free(wal);
            return NULL;
        }
        
        // Set LSN to continue from last entry
        wal->next_lsn = wal->segment_header->last_lsn + 1;
        wal->stats.active_segments = segment_count;
        
        LOG_INFO("Opened existing WAL with %u segments, next_lsn=%lu",
                segment_count, wal->next_lsn);
        
        free(segments);
    } else {
        // Create first segment
        wal->segment_id = 0;
        wal->segment_offset = sizeof(wal_segment_header_t);
        wal->next_lsn = 1;  // LSN starts at 1
        
        wal->segment_fd = open_segment(wal, wal->segment_id, true);
        if (wal->segment_fd == -1) {
            LOG_ERROR("Failed to create first segment");
            pthread_rwlock_destroy(&wal->lock);
            free(wal->write_buffer);
            free(wal->wal_dir);
            free(wal);
            return NULL;
        }
        
        wal->stats.active_segments = 1;
        
        LOG_INFO("Created new WAL at %s", wal_dir);
    }
    
    // Initialize checkpoint tracking
    wal->last_checkpoint_lsn = 0;
    wal->bytes_since_checkpoint = 0;
    wal->last_checkpoint_time = get_time_sec();
    wal->checkpoint_in_progress = false;
    wal->dirty_pages_count = 0;
    
    // Update stats
    wal->stats.current_segment_id = wal->segment_id;
    
    return wal;
}

void wal_close(wal_t *wal) {
    if (!wal) {
        return;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    
    // Flush any remaining buffered data
    flush_write_buffer(wal);
    
    // Close current segment
    close_segment(wal);
    
    pthread_rwlock_unlock(&wal->lock);
    
    // Cleanup
    pthread_rwlock_destroy(&wal->lock);
    
    if (wal->write_buffer) {
        free(wal->write_buffer);
    }
    
    if (wal->wal_dir) {
        free(wal->wal_dir);
    }
    
    free(wal);
    
    LOG_INFO("WAL closed");
}

uint64_t wal_current_lsn(const wal_t *wal) {
    if (!wal) {
        return 0;
    }
    
    // Return last assigned LSN
    return wal->next_lsn > 1 ? wal->next_lsn - 1 : 0;
}

// ============================================================================
// Public API - Write Entries
// ============================================================================

bool wal_log_insert(wal_t *wal, uint64_t txn_id,
                    const uint8_t *key, uint32_t key_len,
                    const uint8_t *value, uint32_t value_len,
                    uint64_t *lsn_out) {
    if (!wal || !key || !value || key_len == 0 || value_len == 0) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "invalid arguments");
        return false;
    }

    pthread_rwlock_wrlock(&wal->lock);

    // Create entry header
    wal_entry_header_t header;
    header.magic = WAL_MAGIC;
    header.entry_type = WAL_ENTRY_INSERT;
    header.lsn = wal->next_lsn++;
    header.txn_id = txn_id;
    header.payload_len = sizeof(wal_insert_payload_t) + key_len + value_len;
    header.timestamp = get_time_sec();

    // Check if entry would exceed segment size - rotate BEFORE writing anything
    size_t total_entry_size = sizeof(header) + header.payload_len;
    if (wal->segment_offset + wal->buffer_offset + total_entry_size > wal->config.segment_size) {
        if (!flush_write_buffer(wal) || !rotate_segment(wal)) {
            pthread_rwlock_unlock(&wal->lock);
            DB_ERROR(DB_ERROR_WAL_FULL, "segment rotation failed");
            return false;
        }
    }

    // Create payload
    size_t payload_size = header.payload_len;
    uint8_t *payload = malloc(payload_size);
    if (!payload) {
        pthread_rwlock_unlock(&wal->lock);
        DB_ERROR(DB_ERROR_OUT_OF_MEMORY, "failed to allocate payload (%zu bytes)", payload_size);
        return false;
    }
    
    wal_insert_payload_t *insert_payload = (wal_insert_payload_t *)payload;
    insert_payload->key_len = key_len;
    insert_payload->value_len = value_len;
    memcpy(insert_payload->data, key, key_len);
    memcpy(insert_payload->data + key_len, value, value_len);
    
    // Calculate checksum
    header.checksum = calculate_entry_checksum(&header, payload);
    
    // Write header and payload to buffer
    bool success = append_to_buffer(wal, &header, sizeof(header)) &&
                   append_to_buffer(wal, payload, payload_size);
    
    free(payload);
    
    if (success) {
        wal->stats.total_entries++;
        wal->stats.total_bytes += sizeof(header) + payload_size;
        wal->stats.inserts++;
        wal->segment_header->entry_count++;
        wal->segment_header->last_lsn = header.lsn;
        
        if (lsn_out) {
            *lsn_out = header.lsn;
        }
    }
    
    pthread_rwlock_unlock(&wal->lock);
    
    return success;
}

bool wal_log_delete(wal_t *wal, uint64_t txn_id,
                    const uint8_t *key, uint32_t key_len,
                    uint64_t *lsn_out) {
    if (!wal || !key || key_len == 0) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "invalid arguments");
        return false;
    }

    pthread_rwlock_wrlock(&wal->lock);

    // Create entry header
    wal_entry_header_t header;
    header.magic = WAL_MAGIC;
    header.entry_type = WAL_ENTRY_DELETE;
    header.lsn = wal->next_lsn++;
    header.txn_id = txn_id;
    header.payload_len = sizeof(wal_delete_payload_t) + key_len;
    header.timestamp = get_time_sec();

    // Check if entry would exceed segment size - rotate BEFORE writing anything
    size_t total_entry_size = sizeof(header) + header.payload_len;
    if (wal->segment_offset + wal->buffer_offset + total_entry_size > wal->config.segment_size) {
        if (!flush_write_buffer(wal) || !rotate_segment(wal)) {
            pthread_rwlock_unlock(&wal->lock);
            DB_ERROR(DB_ERROR_WAL_FULL, "segment rotation failed");
            return false;
        }
    }

    // Create payload
    size_t payload_size = header.payload_len;
    uint8_t *payload = malloc(payload_size);
    if (!payload) {
        pthread_rwlock_unlock(&wal->lock);
        DB_ERROR(DB_ERROR_OUT_OF_MEMORY, "failed to allocate payload (%zu bytes)", payload_size);
        return false;
    }
    
    wal_delete_payload_t *delete_payload = (wal_delete_payload_t *)payload;
    delete_payload->key_len = key_len;
    memcpy(delete_payload->key, key, key_len);
    
    // Calculate checksum
    header.checksum = calculate_entry_checksum(&header, payload);
    
    // Write header and payload to buffer
    bool success = append_to_buffer(wal, &header, sizeof(header)) &&
                   append_to_buffer(wal, payload, payload_size);
    
    free(payload);
    
    if (success) {
        wal->stats.total_entries++;
        wal->stats.total_bytes += sizeof(header) + payload_size;
        wal->stats.deletes++;
        wal->segment_header->entry_count++;
        wal->segment_header->last_lsn = header.lsn;
        
        if (lsn_out) {
            *lsn_out = header.lsn;
        }
    }
    
    pthread_rwlock_unlock(&wal->lock);
    
    return success;
}

bool wal_log_begin_txn(wal_t *wal, uint64_t txn_id, uint64_t *lsn_out) {
    if (!wal) {
        return false;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    
    // Use the provided transaction ID (allocated by MVCC manager)
    // WAL no longer allocates its own transaction IDs
    
    // Create entry header
    wal_entry_header_t header;
    header.magic = WAL_MAGIC;
    header.entry_type = WAL_ENTRY_BEGIN_TXN;
    header.lsn = wal->next_lsn++;
    header.txn_id = txn_id;
    header.payload_len = sizeof(wal_txn_payload_t);
    header.timestamp = get_time_sec();
    
    // Create payload
    wal_txn_payload_t payload;
    payload.txn_id = txn_id;
    payload.timestamp = header.timestamp;
    
    // Calculate checksum
    header.checksum = calculate_entry_checksum(&header, &payload);
    
    // Write header and payload to buffer
    bool success = append_to_buffer(wal, &header, sizeof(header)) &&
                   append_to_buffer(wal, &payload, sizeof(payload));
    
    if (success) {
        wal->stats.total_entries++;
        wal->stats.total_bytes += sizeof(header) + sizeof(payload);
        wal->segment_header->entry_count++;
        wal->segment_header->last_lsn = header.lsn;
        
        if (lsn_out) {
            *lsn_out = header.lsn;
        }
    }
    
    pthread_rwlock_unlock(&wal->lock);
    
    return success;
}

bool wal_log_commit_txn(wal_t *wal, uint64_t txn_id, uint64_t *lsn_out) {
    if (!wal) {
        return false;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    
    // Create entry header
    wal_entry_header_t header;
    header.magic = WAL_MAGIC;
    header.entry_type = WAL_ENTRY_COMMIT_TXN;
    header.lsn = wal->next_lsn++;
    header.txn_id = txn_id;
    header.payload_len = sizeof(wal_txn_payload_t);
    header.timestamp = get_time_sec();
    
    // Create payload
    wal_txn_payload_t payload;
    payload.txn_id = txn_id;
    payload.timestamp = header.timestamp;
    
    // Calculate checksum
    header.checksum = calculate_entry_checksum(&header, &payload);
    
    // Write header and payload to buffer
    bool success = append_to_buffer(wal, &header, sizeof(header)) &&
                   append_to_buffer(wal, &payload, sizeof(payload));
    
    if (!success) {
        pthread_rwlock_unlock(&wal->lock);
        return false;
    }
    
    // CRITICAL: Flush buffer and fsync for durability
    if (!flush_write_buffer(wal)) {
        pthread_rwlock_unlock(&wal->lock);
        return false;
    }
    
    if (wal->config.fsync_on_commit) {
        if (!fsync_with_retry(wal->segment_fd, wal, "commit")) {
            pthread_rwlock_unlock(&wal->lock);
            return false;
        }
        wal->last_fsynced_lsn = header.lsn;
    }
    
    wal->stats.total_entries++;
    wal->stats.total_bytes += sizeof(header) + sizeof(payload);
    wal->stats.commits++;
    wal->segment_header->entry_count++;
    wal->segment_header->last_lsn = header.lsn;
    
    if (lsn_out) {
        *lsn_out = header.lsn;
    }
    
    pthread_rwlock_unlock(&wal->lock);
    
    return true;
}

bool wal_log_abort_txn(wal_t *wal, uint64_t txn_id, uint64_t *lsn_out) {
    if (!wal) {
        return false;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    
    // Create entry header
    wal_entry_header_t header;
    header.magic = WAL_MAGIC;
    header.entry_type = WAL_ENTRY_ABORT_TXN;
    header.lsn = wal->next_lsn++;
    header.txn_id = txn_id;
    header.payload_len = sizeof(wal_txn_payload_t);
    header.timestamp = get_time_sec();
    
    // Create payload
    wal_txn_payload_t payload;
    payload.txn_id = txn_id;
    payload.timestamp = header.timestamp;
    
    // Calculate checksum
    header.checksum = calculate_entry_checksum(&header, &payload);
    
    // Write header and payload to buffer
    bool success = append_to_buffer(wal, &header, sizeof(header)) &&
                   append_to_buffer(wal, &payload, sizeof(payload));
    
    if (success) {
        wal->stats.total_entries++;
        wal->stats.total_bytes += sizeof(header) + sizeof(payload);
        wal->stats.aborts++;
        wal->segment_header->entry_count++;
        wal->segment_header->last_lsn = header.lsn;
        
        if (lsn_out) {
            *lsn_out = header.lsn;
        }
    }
    
    pthread_rwlock_unlock(&wal->lock);
    
    return success;
}

bool wal_log_checkpoint(wal_t *wal,
                        uint64_t root_page_id, uint32_t root_offset,
                        uint64_t tree_size, uint64_t next_page_id,
                        uint64_t *lsn_out) {
    if (!wal) {
        return false;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    
    // Create entry header
    wal_entry_header_t header;
    header.magic = WAL_MAGIC;
    header.entry_type = WAL_ENTRY_CHECKPOINT;
    header.lsn = wal->next_lsn++;
    header.txn_id = 0;  // Checkpoints are not part of transactions
    header.payload_len = sizeof(wal_checkpoint_payload_t);
    header.timestamp = get_time_sec();
    
    // Create payload
    wal_checkpoint_payload_t payload;
    payload.root_page_id = root_page_id;
    payload.root_offset = root_offset;
    payload.tree_size = tree_size;
    payload.next_page_id = next_page_id;
    payload.timestamp = header.timestamp;
    payload.padding1 = 0;
    
    // Calculate checksum
    header.checksum = calculate_entry_checksum(&header, &payload);
    
    // Write header and payload to buffer
    bool success = append_to_buffer(wal, &header, sizeof(header)) &&
                   append_to_buffer(wal, &payload, sizeof(payload));
    
    if (!success) {
        pthread_rwlock_unlock(&wal->lock);
        return false;
    }
    
    // CRITICAL: Flush buffer and fsync for checkpoint durability
    if (!flush_write_buffer(wal)) {
        pthread_rwlock_unlock(&wal->lock);
        return false;
    }
    
    if (!fsync_with_retry(wal->segment_fd, wal, "checkpoint")) {
        pthread_rwlock_unlock(&wal->lock);
        return false;
    }
    
    wal->last_fsynced_lsn = header.lsn;
    wal->last_checkpoint_lsn = header.lsn;
    
    wal->stats.total_entries++;
    wal->stats.total_bytes += sizeof(header) + sizeof(payload);
    wal->stats.checkpoints++;
    wal->segment_header->entry_count++;
    wal->segment_header->last_lsn = header.lsn;
    
    if (lsn_out) {
        *lsn_out = header.lsn;
    }
    
    pthread_rwlock_unlock(&wal->lock);
    
    return true;
}

// ============================================================================
// Public API - Durability
// ============================================================================

bool wal_fsync(wal_t *wal) {
    if (!wal) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "wal is NULL");
        return false;
    }

    pthread_rwlock_wrlock(&wal->lock);

    // Flush write buffer
    if (!flush_write_buffer(wal)) {
        pthread_rwlock_unlock(&wal->lock);
        DB_ERROR(DB_ERROR_IO, "failed to flush write buffer");
        return false;
    }

    // Fsync segment file
    bool success = fsync_with_retry(wal->segment_fd, wal, "manual fsync");
    if (success) {
        wal->last_fsynced_lsn = wal->next_lsn - 1;
    } else {
        DB_ERROR(DB_ERROR_IO, "fsync failed after retries");
    }
    
    pthread_rwlock_unlock(&wal->lock);
    
    return success;
}

bool wal_should_checkpoint(const wal_t *wal, uint32_t *trigger_out) {
    if (!wal) {
        if (trigger_out) *trigger_out = 0;
        return false;
    }
    
    // Check WAL size threshold (PRIMARY)
    if (wal->bytes_since_checkpoint >= wal->config.checkpoint_size_threshold) {
        if (trigger_out) *trigger_out = 1;
        return true;
    }
    
    // Check time threshold (BACKUP)
    uint64_t time_since = get_time_sec() - wal->last_checkpoint_time;
    if (time_since >= wal->config.checkpoint_time_threshold) {
        if (trigger_out) *trigger_out = 2;
        return true;
    }
    
    // Check dirty pages threshold (SAFETY)
    if (wal->dirty_pages_count >= wal->config.checkpoint_pages_threshold) {
        if (trigger_out) *trigger_out = 3;
        return true;
    }
    
    if (trigger_out) *trigger_out = 0;
    return false;
}

void wal_update_dirty_pages(wal_t *wal, uint64_t dirty_pages_count) {
    if (!wal) {
        return;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    wal->dirty_pages_count = dirty_pages_count;
    pthread_rwlock_unlock(&wal->lock);
}

void wal_checkpoint_completed(wal_t *wal, uint64_t checkpoint_lsn) {
    if (!wal) {
        return;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    
    wal->last_checkpoint_lsn = checkpoint_lsn;
    wal->last_checkpoint_time = get_time_sec();
    wal->bytes_since_checkpoint = 0;
    wal->checkpoint_in_progress = false;
    
    // Update stats based on which trigger was hit
    // This would be set by caller before checkpoint started
    
    pthread_rwlock_unlock(&wal->lock);
}

uint32_t wal_truncate(wal_t *wal, uint64_t checkpoint_lsn) {
    if (!wal) {
        return 0;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    
    // Find segments that can be deleted
    uint32_t segment_count;
    uint64_t *segments = wal_list_segments(wal->wal_dir, &segment_count);
    
    if (!segments || segment_count <= wal->config.keep_segments) {
        free(segments);
        pthread_rwlock_unlock(&wal->lock);
        return 0;
    }
    
    uint32_t truncated = 0;
    
    // Keep at least keep_segments segments
    for (uint32_t i = 0; i < segment_count - wal->config.keep_segments; i++) {
        // Check if segment's last LSN is before checkpoint
        char filename[512];
        format_segment_filename(filename, sizeof(filename), wal->wal_dir, segments[i]);
        
        // Quick check: read segment header to get last_lsn
        int fd = open(filename, O_RDONLY);
        if (fd != -1) {
            wal_segment_header_t seg_header;
            if (read(fd, &seg_header, sizeof(seg_header)) == sizeof(seg_header)) {
                // checkpoint_lsn == 0 means delete all safe segments (respecting keep_segments)
                // Otherwise, only delete if all entries in segment are before checkpoint
                if (checkpoint_lsn == 0 || seg_header.last_lsn < checkpoint_lsn) {
                    close(fd);
                    
                    if (unlink(filename) == 0) {
                        truncated++;
                        wal->stats.active_segments--;
                        LOG_INFO("Truncated WAL segment %lu (last_lsn=%lu, checkpoint_lsn=%lu)",
                                segments[i], seg_header.last_lsn, checkpoint_lsn);
                    } else {
                        LOG_WARN("Failed to truncate segment %lu: %s", 
                                segments[i], strerror(errno));
                    }
                    continue;
                }
            }
            close(fd);
        }
        
        // If we can't verify or segment is still needed, keep it
        LOG_DEBUG("Keeping segment %lu (needed for recovery)", segments[i]);
        // If we can't verify or segment is still needed, keep it
        LOG_DEBUG("Keeping segment %lu (needed for recovery)", segments[i]);
    }
    
    free(segments);
    
    pthread_rwlock_unlock(&wal->lock);
    
    return truncated;
}

// ============================================================================
// Public API - Statistics
// ============================================================================

void wal_get_stats(const wal_t *wal, wal_stats_t *stats_out) {
    if (!wal || !stats_out) {
        return;
    }
    
    pthread_rwlock_rdlock((pthread_rwlock_t*)&wal->lock);
    *stats_out = wal->stats;
    pthread_rwlock_unlock((pthread_rwlock_t*)&wal->lock);
}

void wal_reset_stats(wal_t *wal) {
    if (!wal) {
        return;
    }
    
    pthread_rwlock_wrlock(&wal->lock);
    memset(&wal->stats, 0, sizeof(wal->stats));
    pthread_rwlock_unlock(&wal->lock);
}

void wal_print_stats(const wal_t *wal) {
    if (!wal) {
        return;
    }
    
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    
    printf("=== WAL Statistics ===\n");
    printf("Entries: %lu (inserts=%lu, deletes=%lu, commits=%lu, aborts=%lu)\n",
           stats.total_entries, stats.inserts, stats.deletes, 
           stats.commits, stats.aborts);
    printf("Bytes: %lu (%.2f MB)\n", stats.total_bytes, 
           stats.total_bytes / (1024.0 * 1024.0));
    printf("Segments: current=%u, total=%u, active=%u\n",
           stats.current_segment_id, stats.total_segments, stats.active_segments);
    printf("Fsync: calls=%lu, retries=%lu, failures=%lu\n",
           stats.fsync_calls, stats.fsync_retries, stats.fsync_failures);
    printf("Checkpoints: wal=%lu, time=%lu, pages=%lu, total=%lu\n",
           stats.checkpoint_wal_triggered, stats.checkpoint_time_triggered,
           stats.checkpoint_pages_triggered, stats.checkpoints);
}

uint64_t *wal_list_segments(const char *wal_dir, uint32_t *count_out) {
    if (!wal_dir || !count_out) {
        return NULL;
    }
    
    *count_out = 0;
    
    DIR *dir = opendir(wal_dir);
    if (!dir) {
        return NULL;
    }
    
    // Count matching files
    struct dirent *entry;
    uint32_t count = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "wal_", 4) == 0 &&
            strstr(entry->d_name, ".log") != NULL) {
            count++;
        }
    }
    
    if (count == 0) {
        closedir(dir);
        return NULL;
    }
    
    // Allocate array
    uint64_t *segments = malloc(count * sizeof(uint64_t));
    if (!segments) {
        closedir(dir);
        return NULL;
    }
    
    // Extract segment IDs
    rewinddir(dir);
    uint32_t idx = 0;
    while ((entry = readdir(dir)) != NULL && idx < count) {
        if (strncmp(entry->d_name, "wal_", 4) == 0) {
            uint64_t segment_id;
            if (sscanf(entry->d_name, "wal_%lu.log", &segment_id) == 1) {
                segments[idx++] = segment_id;
            }
        }
    }
    
    closedir(dir);
    
    // Sort segments
    for (uint32_t i = 0; i < idx - 1; i++) {
        for (uint32_t j = i + 1; j < idx; j++) {
            if (segments[i] > segments[j]) {
                uint64_t temp = segments[i];
                segments[i] = segments[j];
                segments[j] = temp;
            }
        }
    }
    
    *count_out = idx;
    return segments;
}

// ============================================================================
// Public API - Recovery (Implementation)
// ============================================================================

int64_t wal_replay(wal_t *wal, uint64_t start_lsn, uint64_t end_lsn,
                   void *context,
                   bool (*apply_fn)(void *context, 
                                   const wal_entry_header_t *header,
                                   const void *payload)) {
    if (!wal || !apply_fn) {
        LOG_ERROR("Invalid parameters for WAL replay");
        return -1;
    }
    
    if (start_lsn > end_lsn) {
        LOG_ERROR("Invalid LSN range: start=%lu > end=%lu", start_lsn, end_lsn);
        return -1;
    }
    
    LOG_INFO("Starting WAL replay: LSN %lu to %lu", start_lsn, end_lsn);
    
    pthread_rwlock_wrlock(&wal->lock);
    
    // Flush write buffer to ensure all entries are on disk
    if (!flush_write_buffer(wal)) {
        pthread_rwlock_unlock(&wal->lock);
        LOG_ERROR("Failed to flush write buffer before replay");
        return -1;
    }
    
    // Downgrade to read lock for replay
    pthread_rwlock_unlock(&wal->lock);
    pthread_rwlock_rdlock(&wal->lock);
    
    // Get all segments
    uint32_t segment_count;
    uint64_t *segments = wal_list_segments(wal->wal_dir, &segment_count);
    
    if (!segments) {
        pthread_rwlock_unlock(&wal->lock);
        LOG_WARN("No segments found for replay");
        return 0;
    }
    
    int64_t entries_replayed = 0;
    int64_t entries_skipped = 0;
    int64_t errors = 0;
    
    // Replay entries from each segment
    for (uint32_t seg_idx = 0; seg_idx < segment_count; seg_idx++) {
        char filename[512];
        format_segment_filename(filename, sizeof(filename), 
                               wal->wal_dir, segments[seg_idx]);
        
        int fd = open(filename, O_RDONLY);
        if (fd == -1) {
            LOG_ERROR("Failed to open segment %lu for replay: %s", 
                     segments[seg_idx], strerror(errno));
            errors++;
            continue;
        }
        
        // Read segment header
        wal_segment_header_t seg_header;
        if (read(fd, &seg_header, sizeof(seg_header)) != sizeof(seg_header)) {
            LOG_ERROR("Failed to read segment header: %s", strerror(errno));
            close(fd);
            errors++;
            continue;
        }
        
        // If this is the current active segment, use the in-memory header
        // which has the updated last_lsn, not the on-disk version
        if (segments[seg_idx] == wal->segment_id) {
            seg_header = *wal->segment_header;
        }
        
        // Skip segment if all entries are before start_lsn
        if (seg_header.last_lsn < start_lsn) {
            close(fd);
            continue;
        }
        
        // Skip segment if all entries are after end_lsn
        if (seg_header.first_lsn > end_lsn) {
            close(fd);
            break;  // Segments are sorted, no more to check
        }
        
        // Read and replay entries
        while (true) {
            wal_entry_header_t entry_header;
            ssize_t bytes_read = read(fd, &entry_header, sizeof(entry_header));
            
            if (bytes_read == 0) {
                break;  // End of segment
            }
            
            if (bytes_read != sizeof(entry_header)) {
                LOG_WARN("Incomplete entry header in segment %lu, stopping replay", 
                        segments[seg_idx]);
                break;
            }
            
            // Validate magic
            if (entry_header.magic != WAL_MAGIC) {
                LOG_WARN("Invalid entry magic in segment %lu, stopping replay", 
                        segments[seg_idx]);
                break;
            }
            
            // Check LSN range
            if (entry_header.lsn < start_lsn) {
                // Skip this entry and its payload
                if (entry_header.payload_len > 0) {
                    lseek(fd, entry_header.payload_len, SEEK_CUR);
                }
                entries_skipped++;
                continue;
            }
            
            if (entry_header.lsn > end_lsn) {
                break;  // Done replaying
            }
            
            // Read payload
            void *payload = NULL;
            if (entry_header.payload_len > 0) {
                payload = malloc(entry_header.payload_len);
                if (!payload) {
                    LOG_ERROR("Failed to allocate payload buffer");
                    errors++;
                    break;
                }
                
                if (read(fd, payload, entry_header.payload_len) 
                    != (ssize_t)entry_header.payload_len) {
                    LOG_WARN("Incomplete payload for LSN %lu", entry_header.lsn);
                    free(payload);
                    break;
                }
            }
            
            // Verify checksum
            uint32_t computed_checksum = calculate_entry_checksum(&entry_header, payload);
            if (computed_checksum != entry_header.checksum) {
                LOG_ERROR("Checksum mismatch for LSN %lu (expected 0x%x, got 0x%x)", 
                         entry_header.lsn, entry_header.checksum, computed_checksum);
                free(payload);
                errors++;
                break;
            }
            
            // Apply entry
            bool success = apply_fn(context, &entry_header, payload);
            
            free(payload);
            
            if (!success) {
                LOG_ERROR("Failed to apply entry LSN %lu", entry_header.lsn);
                errors++;
                // Continue with next entry (don't abort entire replay)
            } else {
                entries_replayed++;
            }
        }
        
        close(fd);
    }
    
    free(segments);
    pthread_rwlock_unlock(&wal->lock);
    
    LOG_INFO("WAL replay complete: %ld entries replayed, %ld skipped, %ld errors",
             entries_replayed, entries_skipped, errors);
    
    return errors > 0 ? -1 : entries_replayed;
}

uint64_t wal_find_last_lsn(const wal_t *wal) {
    if (!wal) {
        return 0;
    }
    
    pthread_rwlock_rdlock((pthread_rwlock_t*)&wal->lock);
    
    // Get all segments
    uint32_t segment_count;
    uint64_t *segments = wal_list_segments(wal->wal_dir, &segment_count);
    
    if (!segments || segment_count == 0) {
        pthread_rwlock_unlock((pthread_rwlock_t*)&wal->lock);
        return 0;
    }
    
    uint64_t last_valid_lsn = 0;
    
    // Start from the last segment and work backwards
    for (int seg_idx = segment_count - 1; seg_idx >= 0; seg_idx--) {
        char filename[512];
        format_segment_filename(filename, sizeof(filename), 
                               wal->wal_dir, segments[seg_idx]);
        
        int fd = open(filename, O_RDONLY);
        if (fd == -1) {
            continue;
        }
        
        // Read segment header
        wal_segment_header_t seg_header;
        if (read(fd, &seg_header, sizeof(seg_header)) != sizeof(seg_header)) {
            close(fd);
            continue;
        }
        
        // Use last_lsn from segment header as starting point
        uint64_t candidate_lsn = seg_header.last_lsn;
        
        // Scan all entries to find actual last valid one
        while (true) {
            wal_entry_header_t entry_header;
            ssize_t bytes_read = read(fd, &entry_header, sizeof(entry_header));
            
            if (bytes_read == 0) {
                break;  // End of segment
            }
            
            if (bytes_read != sizeof(entry_header)) {
                break;  // Incomplete header
            }
            
            // Validate magic
            if (entry_header.magic != WAL_MAGIC) {
                break;  // Invalid entry
            }
            
            // Read and validate payload if present
            bool valid = true;
            if (entry_header.payload_len > 0) {
                void *payload = malloc(entry_header.payload_len);
                if (payload) {
                    if (read(fd, payload, entry_header.payload_len) 
                        == (ssize_t)entry_header.payload_len) {
                        // Verify checksum
                        uint32_t computed = calculate_entry_checksum(&entry_header, payload);
                        if (computed != entry_header.checksum) {
                            valid = false;
                        }
                    } else {
                        valid = false;
                    }
                    free(payload);
                } else {
                    valid = false;
                }
            }
            
            if (valid && entry_header.lsn > last_valid_lsn) {
                last_valid_lsn = entry_header.lsn;
            }
            
            if (!valid) {
                break;  // Stop at first invalid entry
            }
        }
        
        close(fd);
        
        // If we found valid entries in this segment, we're done
        if (last_valid_lsn > 0) {
            break;
        }
    }
    
    free(segments);
    pthread_rwlock_unlock((pthread_rwlock_t*)&wal->lock);
    
    return last_valid_lsn;
}

bool wal_validate_segment(const wal_t *wal, uint64_t segment_id, 
                          uint32_t *errors_out) {
    if (!wal) {
        if (errors_out) *errors_out = 0;
        return false;
    }
    
    char filename[512];
    format_segment_filename(filename, sizeof(filename), wal->wal_dir, segment_id);
    
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        LOG_ERROR("Failed to open segment %lu for validation: %s", 
                 segment_id, strerror(errno));
        if (errors_out) *errors_out = 1;
        return false;
    }
    
    uint32_t error_count = 0;
    uint32_t entry_count = 0;
    uint64_t prev_lsn = 0;
    
    // Read and validate segment header
    wal_segment_header_t seg_header;
    if (read(fd, &seg_header, sizeof(seg_header)) != sizeof(seg_header)) {
        LOG_ERROR("Failed to read segment header");
        close(fd);
        if (errors_out) *errors_out = 1;
        return false;
    }
    
    // Validate segment header magic
    if (seg_header.magic != WAL_MAGIC) {
        LOG_ERROR("Invalid segment header magic: 0x%x", seg_header.magic);
        error_count++;
    }
    
    // Validate segment header version
    if (seg_header.version != WAL_VERSION) {
        LOG_ERROR("Invalid segment version: %u", seg_header.version);
        error_count++;
    }
    
    // Validate segment ID
    if (seg_header.segment_id != segment_id) {
        LOG_ERROR("Segment ID mismatch: expected %lu, got %lu", 
                 segment_id, seg_header.segment_id);
        error_count++;
    }
    
    // Scan all entries
    while (true) {
        wal_entry_header_t entry_header;
        ssize_t bytes_read = read(fd, &entry_header, sizeof(entry_header));
        
        if (bytes_read == 0) {
            break;  // End of segment
        }
        
        if (bytes_read != sizeof(entry_header)) {
            LOG_ERROR("Incomplete entry header at position %ld", lseek(fd, 0, SEEK_CUR));
            error_count++;
            break;
        }
        
        // Validate entry magic
        if (entry_header.magic != WAL_MAGIC) {
            LOG_ERROR("Invalid entry magic at LSN %lu: 0x%x", 
                     entry_header.lsn, entry_header.magic);
            error_count++;
            break;
        }
        
        // Validate LSN ordering
        if (entry_count > 0 && entry_header.lsn <= prev_lsn) {
            LOG_ERROR("LSN ordering violation: %lu -> %lu", prev_lsn, entry_header.lsn);
            error_count++;
        }
        prev_lsn = entry_header.lsn;
        
        // Read payload
        void *payload = NULL;
        if (entry_header.payload_len > 0) {
            payload = malloc(entry_header.payload_len);
            if (!payload) {
                LOG_ERROR("Failed to allocate payload buffer");
                error_count++;
                break;
            }
            
            if (read(fd, payload, entry_header.payload_len) 
                != (ssize_t)entry_header.payload_len) {
                LOG_ERROR("Incomplete payload for LSN %lu", entry_header.lsn);
                error_count++;
                free(payload);
                break;
            }
        }
        
        // Validate checksum
        uint32_t computed_checksum = calculate_entry_checksum(&entry_header, payload);
        if (computed_checksum != entry_header.checksum) {
            LOG_ERROR("Checksum mismatch for LSN %lu (expected 0x%x, got 0x%x)", 
                     entry_header.lsn, entry_header.checksum, computed_checksum);
            error_count++;
        }
        
        free(payload);
        entry_count++;
    }
    
    close(fd);
    
    // Validate entry count matches header
    if (seg_header.entry_count != entry_count && error_count == 0) {
        LOG_WARN("Entry count mismatch: header says %lu, found %u",
                seg_header.entry_count, entry_count);
        // This is a warning, not an error (segment may not be complete)
    }
    
    if (errors_out) {
        *errors_out = error_count;
    }
    
    LOG_INFO("Validated segment %lu: %u entries, %u errors", 
             segment_id, entry_count, error_count);
    
    return error_count == 0;
}
