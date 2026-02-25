/**
 * Database Compaction Implementation — In-Place Defragmentation
 *
 * Moves live pages forward within each data file, closing gaps
 * left by dead pages, then ftruncate() to reclaim space.
 *
 * Key properties:
 * - No temporary files or extra disk space needed
 * - Pages only move to lower offsets (write cursor always behind read cursor)
 * - Crash-safe via compaction journal (replay on recovery)
 * - Compressed pages copied as raw bytes (no decompress/recompress)
 */

#include "db_compaction.h"
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

// ============================================================================
// Helpers
// ============================================================================

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

// Compare entries by (file_idx, file_offset) for sequential I/O
static int cmp_by_location(const void *a, const void *b) {
    const page_gc_metadata_t *ea = (const page_gc_metadata_t *)a;
    const page_gc_metadata_t *eb = (const page_gc_metadata_t *)b;

    if (ea->file_idx != eb->file_idx) {
        return (ea->file_idx > eb->file_idx) - (ea->file_idx < eb->file_idx);
    }
    return (ea->file_offset > eb->file_offset) - (ea->file_offset < eb->file_offset);
}

// Compare entries by page_id for sorted index
static int cmp_by_page_id(const void *a, const void *b) {
    const page_gc_metadata_t *ea = (const page_gc_metadata_t *)a;
    const page_gc_metadata_t *eb = (const page_gc_metadata_t *)b;
    return (ea->page_id > eb->page_id) - (ea->page_id < eb->page_id);
}

// Get total size of all data files by stat()
static uint64_t get_total_data_file_size(page_manager_t *pm) {
    uint64_t total = 0;
    uint32_t num_files = pm->allocator->num_data_files;

    for (uint32_t i = 0; i < num_files; i++) {
        int fd = page_manager_get_data_fd(pm, i);
        if (fd < 0) continue;

        struct stat st;
        if (fstat(fd, &st) == 0) {
            total += (uint64_t)st.st_size;
        }
    }

    return total;
}

// ============================================================================
// Journal I/O
// ============================================================================

static void get_journal_path(const char *db_path, char *out, size_t out_size) {
    snprintf(out, out_size, "%s/compaction.journal", db_path);
}

static bool write_compaction_journal(const char *db_path,
                                     uint32_t file_idx,
                                     const compaction_journal_entry_t *entries,
                                     uint32_t num_entries,
                                     uint64_t final_file_size) {
    char path[512];
    get_journal_path(db_path, path, sizeof(path));

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_ERROR("Failed to create compaction journal: %s", strerror(errno));
        return false;
    }

    compaction_journal_header_t header = {
        .magic = COMPACTION_JOURNAL_MAGIC,
        .version = COMPACTION_JOURNAL_VERSION,
        .state = COMPACTION_STATE_COMPACTING,
        .file_idx = file_idx,
        .num_entries = num_entries,
        .final_file_size = final_file_size,
        .crc = 0,
        .padding = 0,
    };

    // CRC covers all header fields before the crc field
    header.crc = compute_crc32((const uint8_t *)&header,
                               offsetof(compaction_journal_header_t, crc));

    ssize_t wr = write(fd, &header, sizeof(header));
    if (wr != (ssize_t)sizeof(header)) {
        LOG_ERROR("Short write on journal header");
        close(fd);
        unlink(path);
        return false;
    }

    if (num_entries > 0) {
        size_t entries_size = num_entries * sizeof(compaction_journal_entry_t);
        wr = write(fd, entries, entries_size);
        if (wr != (ssize_t)entries_size) {
            LOG_ERROR("Short write on journal entries");
            close(fd);
            unlink(path);
            return false;
        }
    }

    fsync(fd);
    close(fd);
    return true;
}

static void delete_compaction_journal(const char *db_path) {
    char path[512];
    get_journal_path(db_path, path, sizeof(path));
    unlink(path);
}

// ============================================================================
// Analysis
// ============================================================================

bool db_compaction_analyze(page_manager_t *pm, compaction_stats_t *stats_out) {
    if (!pm || !stats_out) return false;

    memset(stats_out, 0, sizeof(*stats_out));

    // Get dead page stats from page_gc
    dead_page_stats_t dead_stats;
    page_gc_get_dead_stats(pm, &dead_stats);

    stats_out->total_pages_before = dead_stats.total_pages;
    stats_out->live_pages = dead_stats.live_pages;
    stats_out->dead_pages = dead_stats.dead_pages;
    stats_out->file_size_before = get_total_data_file_size(pm);
    stats_out->old_data_files = pm->allocator->num_data_files;

    // Estimate output
    stats_out->total_pages_after = dead_stats.live_pages;
    stats_out->file_size_after = stats_out->file_size_before - dead_stats.dead_bytes;
    stats_out->space_reclaimed = dead_stats.dead_bytes;

    stats_out->success = true;
    return true;
}

bool db_compaction_is_recommended(page_manager_t *pm) {
    if (!pm) return false;

    dead_page_stats_t stats;
    page_gc_get_dead_stats(pm, &stats);

    return stats.fragmentation_pct > 30.0;
}

// ============================================================================
// Core Compaction — In-Place
// ============================================================================

bool db_compaction_run(page_manager_t *pm,
                       const compaction_config_t *config,
                       compaction_stats_t *stats_out) {
    if (!pm || !config || !stats_out) {
        if (stats_out) {
            stats_out->success = false;
            stats_out->error_message = "NULL argument";
        }
        return false;
    }

    memset(stats_out, 0, sizeof(*stats_out));
    uint64_t start_time = now_ms();

    // Dry run: just analyze
    if (config->dry_run) {
        bool ok = db_compaction_analyze(pm, stats_out);
        stats_out->duration_ms = now_ms() - start_time;
        return ok;
    }

    // 1. Snapshot page index
    size_t total_count = 0;
    page_gc_metadata_t *snapshot = (page_gc_metadata_t *)
        page_manager_get_index_snapshot(pm, &total_count);

    if (!snapshot || total_count == 0) {
        stats_out->success = true;
        stats_out->duration_ms = now_ms() - start_time;
        free(snapshot);
        LOG_INFO("Compaction: nothing to do (empty index)");
        return true;
    }

    stats_out->total_pages_before = total_count;
    stats_out->file_size_before = get_total_data_file_size(pm);
    stats_out->old_data_files = pm->allocator->num_data_files;

    // 2. Partition into live and dead
    size_t live_count = 0;
    size_t dead_count = 0;

    for (size_t i = 0; i < total_count; i++) {
        bool is_dead = (snapshot[i].flags & PAGE_GC_FLAG_DEAD) != 0;
        bool is_placeholder = (snapshot[i].file_idx == UINT32_MAX);
        if (is_dead || is_placeholder) {
            dead_count++;
        } else {
            live_count++;
        }
    }

    stats_out->live_pages = live_count;
    stats_out->dead_pages = dead_count;

    if (dead_count == 0) {
        stats_out->success = true;
        stats_out->duration_ms = now_ms() - start_time;
        free(snapshot);
        LOG_INFO("Compaction: no dead pages, nothing to do");
        return true;
    }

    if (config->verbose) {
        LOG_INFO("Compaction: %zu total, %zu live, %zu dead",
                 total_count, live_count, dead_count);
    }

    // 3. Build live entries array, sorted by location for sequential I/O
    page_gc_metadata_t *live_entries = malloc(live_count * sizeof(page_gc_metadata_t));
    if (!live_entries) {
        stats_out->success = false;
        stats_out->error_message = "Failed to allocate live entries";
        free(snapshot);
        return false;
    }

    size_t li = 0;
    for (size_t i = 0; i < total_count; i++) {
        bool is_dead = (snapshot[i].flags & PAGE_GC_FLAG_DEAD) != 0;
        bool is_placeholder = (snapshot[i].file_idx == UINT32_MAX);
        if (!is_dead && !is_placeholder) {
            live_entries[li++] = snapshot[i];
        }
    }
    free(snapshot);

    // Sort by (file_idx, file_offset) for sequential read
    qsort(live_entries, live_count, sizeof(page_gc_metadata_t), cmp_by_location);

    // 4. Process each data file in-place
    uint32_t num_files = pm->allocator->num_data_files;
    uint8_t copy_buf[PAGE_SIZE];
    bool failed = false;
    uint32_t last_file_with_data = 0;
    uint64_t last_file_cursor = 0;

    size_t entry_start = 0;
    for (uint32_t fi = 0; fi < num_files && !failed; fi++) {
        // Find range of entries for this file
        size_t entry_end = entry_start;
        while (entry_end < live_count && live_entries[entry_end].file_idx == fi) {
            entry_end++;
        }
        size_t file_live_count = entry_end - entry_start;

        if (file_live_count == 0) {
            // No live pages in this file — truncate to 0
            int fd = page_manager_get_data_fd(pm, fi);
            if (fd >= 0) {
                ftruncate(fd, 0);
            }
            entry_start = entry_end;
            continue;
        }

        // Compute move plan
        uint64_t write_cursor = 0;
        size_t moves_needed = 0;

        compaction_journal_entry_t *journal_entries =
            malloc(file_live_count * sizeof(compaction_journal_entry_t));
        if (!journal_entries) {
            stats_out->success = false;
            stats_out->error_message = "Failed to allocate journal entries";
            failed = true;
            break;
        }

        for (size_t i = entry_start; i < entry_end; i++) {
            uint64_t old_offset = live_entries[i].file_offset;
            uint32_t size = live_entries[i].compressed_size;

            if (write_cursor != old_offset) {
                // Page needs to move
                journal_entries[moves_needed] = (compaction_journal_entry_t){
                    .page_id = live_entries[i].page_id,
                    .old_offset = old_offset,
                    .new_offset = write_cursor,
                    .compressed_size = size,
                    .padding = 0,
                };
                moves_needed++;
            }

            // Update entry in-memory for later index replacement
            live_entries[i].file_offset = write_cursor;
            write_cursor += size;
        }

        if (moves_needed == 0) {
            // No moves needed — just truncate if file has trailing dead space
            int fd = page_manager_get_data_fd(pm, fi);
            if (fd >= 0) {
                struct stat st;
                if (fstat(fd, &st) == 0 && (uint64_t)st.st_size > write_cursor) {
                    ftruncate(fd, write_cursor);
                }
            }
            free(journal_entries);
            last_file_with_data = fi;
            last_file_cursor = write_cursor;
            entry_start = entry_end;
            continue;
        }

        // Write journal (crash safety)
        if (!write_compaction_journal(pm->db_path, fi, journal_entries,
                                      (uint32_t)moves_needed, write_cursor)) {
            stats_out->success = false;
            stats_out->error_message = "Failed to write compaction journal";
            free(journal_entries);
            failed = true;
            break;
        }

        // Execute moves: pread from old_offset → pwrite to new_offset
        int fd = page_manager_get_data_fd(pm, fi);
        if (fd < 0) {
            stats_out->success = false;
            stats_out->error_message = "Bad file descriptor";
            free(journal_entries);
            failed = true;
            break;
        }

        for (size_t m = 0; m < moves_needed; m++) {
            compaction_journal_entry_t *je = &journal_entries[m];

            ssize_t rd = pread(fd, copy_buf, je->compressed_size, je->old_offset);
            if (rd != (ssize_t)je->compressed_size) {
                LOG_ERROR("Compaction: short read page %lu: got %zd, expected %u",
                         je->page_id, rd, je->compressed_size);
                failed = true;
                break;
            }

            ssize_t wr = pwrite(fd, copy_buf, je->compressed_size, je->new_offset);
            if (wr != (ssize_t)je->compressed_size) {
                LOG_ERROR("Compaction: short write page %lu: wrote %zd, expected %u",
                         je->page_id, wr, je->compressed_size);
                failed = true;
                break;
            }

            stats_out->pages_moved++;
            stats_out->bytes_read += je->compressed_size;
            stats_out->bytes_written += je->compressed_size;
        }

        free(journal_entries);

        if (failed) break;

        // fsync data file, then truncate
        fsync(fd);
        ftruncate(fd, write_cursor);

        // Update page index offsets for moved pages
        // (live_entries already updated in-memory above)

        // Delete journal (compaction for this file is complete)
        delete_compaction_journal(pm->db_path);

        last_file_with_data = fi;
        last_file_cursor = write_cursor;
        entry_start = entry_end;

        // Check timeout
        if (config->max_duration_sec > 0) {
            uint64_t elapsed = now_ms() - start_time;
            if (elapsed > config->max_duration_sec * 1000) {
                LOG_WARN("Compaction: timeout after %lu ms", elapsed);
                failed = true;
                break;
            }
        }
    }

    if (failed) {
        delete_compaction_journal(pm->db_path);
        free(live_entries);
        stats_out->success = false;
        if (!stats_out->error_message)
            stats_out->error_message = "Compaction failed";
        stats_out->duration_ms = now_ms() - start_time;
        return false;
    }

    // 5. Replace index with live-only entries (sorted by page_id)
    qsort(live_entries, live_count, sizeof(page_gc_metadata_t), cmp_by_page_id);

    uint64_t total_live_size = 0;
    for (size_t i = 0; i < live_count; i++) {
        live_entries[i].flags &= ~PAGE_GC_FLAG_DEAD;
        total_live_size += live_entries[i].compressed_size;
    }

    page_manager_replace_index(pm,
                               (struct page_gc_metadata *)live_entries,
                               live_count,
                               total_live_size,
                               0);  // dead_bytes = 0 after compaction

    // Reset cursor to end of last file with data
    page_manager_reset_cursor(pm, last_file_with_data, last_file_cursor);

    // Save index and metadata
    page_manager_save_index(pm);
    page_manager_save_metadata(pm, 0);

    // 6. Fill stats
    stats_out->total_pages_after = live_count;
    stats_out->file_size_after = get_total_data_file_size(pm);
    stats_out->space_reclaimed = stats_out->file_size_before > stats_out->file_size_after
        ? stats_out->file_size_before - stats_out->file_size_after : 0;
    stats_out->new_data_files = pm->allocator->num_data_files;
    stats_out->success = true;
    stats_out->duration_ms = now_ms() - start_time;

    // Note: live_entries ownership transferred to page_manager_replace_index
    if (config->verbose) {
        db_compaction_print_stats(stats_out);
    }

    LOG_INFO("Compaction completed: reclaimed %lu bytes (%.1f%% reduction)",
             stats_out->space_reclaimed,
             stats_out->file_size_before > 0
                ? (double)stats_out->space_reclaimed / stats_out->file_size_before * 100.0
                : 0.0);

    return true;
}

// ============================================================================
// Crash Recovery
// ============================================================================

bool db_compaction_recover(page_manager_t *pm) {
    if (!pm) return false;

    char journal_path[512];
    get_journal_path(pm->db_path, journal_path, sizeof(journal_path));

    // Check if journal exists
    if (access(journal_path, F_OK) != 0) {
        return true;  // No journal, nothing to do
    }

    LOG_INFO("Compaction recovery: found journal, checking state...");

    int fd = open(journal_path, O_RDONLY);
    if (fd < 0) {
        LOG_ERROR("Failed to open compaction journal: %s", strerror(errno));
        return false;
    }

    // Read header
    compaction_journal_header_t header;
    ssize_t rd = read(fd, &header, sizeof(header));
    if (rd != (ssize_t)sizeof(header)) {
        LOG_WARN("Short read on compaction journal header, deleting");
        close(fd);
        unlink(journal_path);
        return true;
    }

    // Validate magic and version
    if (header.magic != COMPACTION_JOURNAL_MAGIC ||
        header.version != COMPACTION_JOURNAL_VERSION) {
        LOG_WARN("Invalid compaction journal (magic=%08x, version=%u), deleting",
                 header.magic, header.version);
        close(fd);
        unlink(journal_path);
        return true;
    }

    // Verify CRC
    uint32_t expected_crc = compute_crc32(
        (const uint8_t *)&header,
        offsetof(compaction_journal_header_t, crc));
    if (header.crc != expected_crc) {
        LOG_WARN("Compaction journal CRC mismatch, deleting");
        close(fd);
        unlink(journal_path);
        return true;
    }

    // If DONE, just delete
    if (header.state == COMPACTION_STATE_DONE) {
        LOG_INFO("Compaction journal: state=DONE, deleting");
        close(fd);
        unlink(journal_path);
        return true;
    }

    // state == COMPACTING: replay moves
    LOG_INFO("Compaction recovery: replaying %u moves for file %u (final_size=%lu)",
             header.num_entries, header.file_idx, header.final_file_size);

    // Read entries
    if (header.num_entries == 0) {
        close(fd);
        // No moves, just truncate
        int data_fd = page_manager_get_data_fd(pm, header.file_idx);
        if (data_fd >= 0) {
            ftruncate(data_fd, header.final_file_size);
        }
        unlink(journal_path);
        return true;
    }

    compaction_journal_entry_t *entries =
        malloc(header.num_entries * sizeof(compaction_journal_entry_t));
    if (!entries) {
        LOG_ERROR("Failed to allocate journal entries for recovery");
        close(fd);
        return false;
    }

    size_t entries_size = header.num_entries * sizeof(compaction_journal_entry_t);
    rd = read(fd, entries, entries_size);
    close(fd);

    if (rd != (ssize_t)entries_size) {
        LOG_WARN("Short read on journal entries (%zd/%zu), deleting incomplete journal",
                 rd, entries_size);
        free(entries);
        unlink(journal_path);
        return true;
    }

    // Get data file fd
    int data_fd = page_manager_get_data_fd(pm, header.file_idx);
    if (data_fd < 0) {
        LOG_ERROR("Bad fd for file %u during recovery", header.file_idx);
        free(entries);
        return false;
    }

    // Replay moves (idempotent)
    uint8_t buf[PAGE_SIZE];

    for (uint32_t i = 0; i < header.num_entries; i++) {
        compaction_journal_entry_t *e = &entries[i];

        // Try reading from old_offset
        ssize_t rd_data = pread(data_fd, buf, e->compressed_size, e->old_offset);
        if (rd_data == (ssize_t)e->compressed_size) {
            // Write to new_offset (idempotent — may already be there)
            ssize_t wr = pwrite(data_fd, buf, e->compressed_size, e->new_offset);
            if (wr != (ssize_t)e->compressed_size) {
                LOG_ERROR("Recovery: failed to write page %lu to offset %lu",
                         e->page_id, e->new_offset);
                free(entries);
                return false;
            }
        } else {
            // old_offset may be past EOF (file already truncated)
            // Data should already be at new_offset
            ssize_t check = pread(data_fd, buf, e->compressed_size, e->new_offset);
            if (check != (ssize_t)e->compressed_size) {
                LOG_ERROR("Recovery: page %lu data lost (old=%lu, new=%lu)",
                         e->page_id, e->old_offset, e->new_offset);
                free(entries);
                return false;
            }
            // Data already at new_offset, no action needed
        }

        // Update page index offset
        page_manager_update_page_offset(pm, e->page_id, e->new_offset);
    }

    // fsync and truncate
    fsync(data_fd);
    ftruncate(data_fd, header.final_file_size);

    // Save updated index
    page_manager_save_index(pm);

    // Delete journal
    unlink(journal_path);

    free(entries);
    LOG_INFO("Compaction recovery completed: %u pages replayed for file %u",
             header.num_entries, header.file_idx);
    return true;
}

// ============================================================================
// Utilities
// ============================================================================

void db_compaction_print_stats(const compaction_stats_t *stats) {
    if (!stats) return;

    LOG_INFO("========== Compaction Statistics ==========");
    LOG_INFO("Input:  %lu pages (%lu live, %lu dead)",
             stats->total_pages_before, stats->live_pages, stats->dead_pages);
    LOG_INFO("Output: %lu pages", stats->total_pages_after);
    LOG_INFO("Files:  %lu old → %lu new",
             stats->old_data_files, stats->new_data_files);
    LOG_INFO("Size:   %lu → %lu bytes (reclaimed %lu bytes)",
             stats->file_size_before, stats->file_size_after, stats->space_reclaimed);
    LOG_INFO("I/O:    %lu pages moved, %lu bytes read, %lu bytes written",
             stats->pages_moved, stats->bytes_read, stats->bytes_written);
    LOG_INFO("Time:   %lu ms", stats->duration_ms);
    LOG_INFO("Result: %s", stats->success ? "SUCCESS" : stats->error_message);
    LOG_INFO("===========================================");
}

double db_compaction_fragmentation_pct(const compaction_stats_t *stats) {
    if (!stats || stats->total_pages_before == 0) return 0.0;
    return (double)stats->dead_pages / (double)stats->total_pages_before * 100.0;
}
