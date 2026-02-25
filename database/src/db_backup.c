/**
 * Database Backup - Online Snapshot Export/Import
 */

#include "db_backup.h"
#include "crc32.h"
#include "db_error.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stddef.h>

// Import batch size (entries per batch insert call)
#define IMPORT_BATCH_SIZE 1000

// ============================================================================
// Internal Helpers
// ============================================================================

static inline uint32_t flags_get_key_size(uint32_t flags) {
    return (flags >> 16) & 0xFFFF;
}

static inline uint32_t flags_set_key_size(uint32_t flags, uint32_t key_size) {
    return (flags & 0xFFFF) | ((key_size & 0xFFFF) << 16);
}

static bool read_and_validate_header(FILE *fp, db_backup_header_t *hdr) {
    if (fread(hdr, sizeof(*hdr), 1, fp) != 1) {
        DB_ERROR(DB_ERROR_IO, "failed to read backup header");
        return false;
    }
    if (hdr->magic != DB_BACKUP_MAGIC) {
        DB_ERROR(DB_ERROR_CORRUPTION, "invalid backup magic: 0x%08x", hdr->magic);
        return false;
    }
    if (hdr->version != DB_BACKUP_VERSION) {
        DB_ERROR(DB_ERROR_CORRUPTION, "unsupported backup version: %u", hdr->version);
        return false;
    }
    uint32_t expected = compute_crc32((const uint8_t *)hdr,
                                       offsetof(db_backup_header_t, header_checksum));
    if (hdr->header_checksum != expected) {
        DB_ERROR(DB_ERROR_CORRUPTION, "header checksum mismatch");
        return false;
    }
    return true;
}

// ============================================================================
// Export
// ============================================================================

bool db_backup_export(data_art_tree_t *tree, const char *backup_path) {
    if (!tree || !backup_path) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree or backup_path is NULL");
        return false;
    }

    FILE *fp = fopen(backup_path, "wb");
    if (!fp) {
        DB_ERROR(DB_ERROR_IO, "cannot open backup file for writing: %s", backup_path);
        return false;
    }

    // Write placeholder header
    db_backup_header_t header = {
        .magic = DB_BACKUP_MAGIC,
        .version = DB_BACKUP_VERSION,
        .entry_count = 0,
        .timestamp = (uint64_t)time(NULL),
        .flags = flags_set_key_size(0, (uint32_t)tree->key_size),
        .header_checksum = 0,
    };
    if (fwrite(&header, sizeof(header), 1, fp) != 1) {
        DB_ERROR(DB_ERROR_IO, "failed to write header placeholder");
        fclose(fp);
        remove(backup_path);
        return false;
    }

    // Create iterator (captures committed root atomically)
    data_art_iterator_t *iter = data_art_iterator_create(tree);
    if (!iter) {
        DB_ERROR(DB_ERROR_IO, "failed to create iterator for export");
        fclose(fp);
        remove(backup_path);
        return false;
    }

    uint64_t count = 0;
    uint32_t data_crc = compute_crc32_begin();
    bool ok = true;

    while (data_art_iterator_next(iter)) {
        size_t key_len = 0, value_len = 0;
        const uint8_t *key = data_art_iterator_key(iter, &key_len);
        const void *value = data_art_iterator_value(iter, &value_len);

        if (!key) {
            DB_ERROR(DB_ERROR_IO, "iterator returned NULL key at entry %lu", count);
            ok = false;
            break;
        }

        uint32_t kl = (uint32_t)key_len;
        uint32_t vl = (uint32_t)value_len;

        // Write entry: key_len + value_len + key + value
        if (fwrite(&kl, 4, 1, fp) != 1 || fwrite(&vl, 4, 1, fp) != 1) {
            DB_ERROR(DB_ERROR_IO, "failed to write entry lengths at %lu", count);
            ok = false;
            break;
        }
        data_crc = compute_crc32_update(data_crc, (const uint8_t *)&kl, 4);
        data_crc = compute_crc32_update(data_crc, (const uint8_t *)&vl, 4);

        if (key_len > 0 && fwrite(key, key_len, 1, fp) != 1) {
            DB_ERROR(DB_ERROR_IO, "failed to write key at entry %lu", count);
            ok = false;
            break;
        }
        if (key_len > 0) {
            data_crc = compute_crc32_update(data_crc, key, key_len);
        }

        if (value_len > 0) {
            if (!value) {
                DB_ERROR(DB_ERROR_IO, "NULL value with non-zero length at entry %lu", count);
                ok = false;
                break;
            }
            if (fwrite(value, value_len, 1, fp) != 1) {
                DB_ERROR(DB_ERROR_IO, "failed to write value at entry %lu", count);
                ok = false;
                break;
            }
            data_crc = compute_crc32_update(data_crc, (const uint8_t *)value, value_len);
        }

        count++;
    }

    data_art_iterator_destroy(iter);

    if (!ok) {
        fclose(fp);
        remove(backup_path);
        return false;
    }

    // Write footer
    uint32_t final_crc = compute_crc32_finish(data_crc);
    db_backup_footer_t footer = {
        .magic = DB_BACKUP_FOOTER_MAGIC,
        .entry_count = count,
        .data_checksum = final_crc,
    };
    if (fwrite(&footer, sizeof(footer), 1, fp) != 1) {
        DB_ERROR(DB_ERROR_IO, "failed to write footer");
        fclose(fp);
        remove(backup_path);
        return false;
    }

    // Rewrite header with final entry count and checksum
    header.entry_count = count;
    header.header_checksum = compute_crc32((const uint8_t *)&header,
                                            offsetof(db_backup_header_t, header_checksum));
    if (fseek(fp, 0, SEEK_SET) != 0 ||
        fwrite(&header, sizeof(header), 1, fp) != 1) {
        DB_ERROR(DB_ERROR_IO, "failed to rewrite header");
        fclose(fp);
        remove(backup_path);
        return false;
    }

    fflush(fp);
    fclose(fp);

    LOG_INFO("Backup exported: %lu entries to %s", count, backup_path);
    return true;
}

// ============================================================================
// Import
// ============================================================================

bool db_backup_import(data_art_tree_t *tree, const char *backup_path) {
    if (!tree || !backup_path) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree or backup_path is NULL");
        return false;
    }

    FILE *fp = fopen(backup_path, "rb");
    if (!fp) {
        DB_ERROR(DB_ERROR_IO, "cannot open backup file: %s", backup_path);
        return false;
    }

    db_backup_header_t header;
    if (!read_and_validate_header(fp, &header)) {
        fclose(fp);
        return false;
    }

    // Validate key_size matches
    uint32_t backup_key_size = flags_get_key_size(header.flags);
    if (backup_key_size != 0 && backup_key_size != (uint32_t)tree->key_size) {
        DB_ERROR(DB_ERROR_INVALID_ARG,
                 "backup key_size %u != tree key_size %zu",
                 backup_key_size, tree->key_size);
        fclose(fp);
        return false;
    }

    // Allocate batch arrays
    size_t batch_cap = IMPORT_BATCH_SIZE;
    const uint8_t **keys = malloc(batch_cap * sizeof(*keys));
    size_t *key_lens = malloc(batch_cap * sizeof(*key_lens));
    const void **values = malloc(batch_cap * sizeof(*values));
    size_t *value_lens = malloc(batch_cap * sizeof(*value_lens));
    void **key_bufs = malloc(batch_cap * sizeof(*key_bufs));
    void **val_bufs = malloc(batch_cap * sizeof(*val_bufs));

    if (!keys || !key_lens || !values || !value_lens || !key_bufs || !val_bufs) {
        DB_ERROR(DB_ERROR_OUT_OF_MEMORY, "failed to allocate import batch arrays");
        free(keys); free(key_lens); free(values); free(value_lens);
        free(key_bufs); free(val_bufs);
        fclose(fp);
        return false;
    }

    uint32_t data_crc = compute_crc32_begin();
    uint64_t entries_read = 0;
    size_t batch_count = 0;
    bool ok = true;

    while (entries_read < header.entry_count) {
        uint32_t kl, vl;
        if (fread(&kl, 4, 1, fp) != 1 || fread(&vl, 4, 1, fp) != 1) {
            DB_ERROR(DB_ERROR_IO, "failed to read entry header at %lu", entries_read);
            ok = false;
            break;
        }
        data_crc = compute_crc32_update(data_crc, (uint8_t *)&kl, 4);
        data_crc = compute_crc32_update(data_crc, (uint8_t *)&vl, 4);

        if (kl > 1024 * 1024 || vl > 64 * 1024 * 1024) {
            DB_ERROR(DB_ERROR_CORRUPTION, "unreasonable entry sizes: key=%u value=%u", kl, vl);
            ok = false;
            break;
        }

        // Read key
        uint8_t *key_buf = malloc(kl > 0 ? kl : 1);
        if (!key_buf) {
            DB_ERROR(DB_ERROR_OUT_OF_MEMORY, "malloc key failed at entry %lu", entries_read);
            ok = false;
            break;
        }
        if (kl > 0 && fread(key_buf, kl, 1, fp) != 1) {
            DB_ERROR(DB_ERROR_IO, "failed to read key at entry %lu", entries_read);
            free(key_buf);
            ok = false;
            break;
        }
        if (kl > 0) {
            data_crc = compute_crc32_update(data_crc, key_buf, kl);
        }

        // Read value
        void *val_buf = NULL;
        if (vl > 0) {
            val_buf = malloc(vl);
            if (!val_buf) {
                DB_ERROR(DB_ERROR_OUT_OF_MEMORY, "malloc value failed at entry %lu", entries_read);
                free(key_buf);
                ok = false;
                break;
            }
            if (fread(val_buf, vl, 1, fp) != 1) {
                DB_ERROR(DB_ERROR_IO, "failed to read value at entry %lu", entries_read);
                free(key_buf);
                free(val_buf);
                ok = false;
                break;
            }
            data_crc = compute_crc32_update(data_crc, val_buf, vl);
        }

        // Add to batch
        keys[batch_count] = key_buf;
        key_lens[batch_count] = kl;
        values[batch_count] = val_buf;
        value_lens[batch_count] = vl;
        key_bufs[batch_count] = key_buf;
        val_bufs[batch_count] = val_buf;
        batch_count++;
        entries_read++;

        // Flush batch when full
        if (batch_count == batch_cap) {
            if (!data_art_insert_batch(tree, keys, key_lens, values, value_lens, batch_count)) {
                DB_ERROR(DB_ERROR_IO, "batch insert failed at entry %lu", entries_read);
                ok = false;
            }
            for (size_t i = 0; i < batch_count; i++) {
                free(key_bufs[i]);
                free(val_bufs[i]);
            }
            batch_count = 0;
            if (!ok) break;
        }
    }

    // Flush remaining batch
    if (ok && batch_count > 0) {
        if (!data_art_insert_batch(tree, keys, key_lens, values, value_lens, batch_count)) {
            DB_ERROR(DB_ERROR_IO, "final batch insert failed");
            ok = false;
        }
        for (size_t i = 0; i < batch_count; i++) {
            free(key_bufs[i]);
            free(val_bufs[i]);
        }
        batch_count = 0;
    }

    // Validate footer
    if (ok) {
        db_backup_footer_t footer;
        if (fread(&footer, sizeof(footer), 1, fp) != 1) {
            DB_ERROR(DB_ERROR_IO, "failed to read footer");
            ok = false;
        } else {
            if (footer.magic != DB_BACKUP_FOOTER_MAGIC) {
                DB_ERROR(DB_ERROR_CORRUPTION, "invalid footer magic: 0x%08x", footer.magic);
                ok = false;
            }
            if (footer.entry_count != header.entry_count) {
                DB_ERROR(DB_ERROR_CORRUPTION,
                         "footer entry_count %lu != header %lu",
                         footer.entry_count, header.entry_count);
                ok = false;
            }
            uint32_t final_crc = compute_crc32_finish(data_crc);
            if (footer.data_checksum != final_crc) {
                DB_ERROR(DB_ERROR_CORRUPTION, "data checksum mismatch");
                ok = false;
            }
        }
    }

    // Cleanup on error: free remaining batch buffers
    if (!ok) {
        for (size_t i = 0; i < batch_count; i++) {
            free(key_bufs[i]);
            free(val_bufs[i]);
        }
    }

    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);
    free(key_bufs);
    free(val_bufs);
    fclose(fp);

    if (ok) {
        LOG_INFO("Backup imported: %lu entries from %s", entries_read, backup_path);
    }
    return ok;
}

// ============================================================================
// Info
// ============================================================================

bool db_backup_info(const char *backup_path, db_backup_info_t *info_out) {
    if (!backup_path || !info_out) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "NULL argument");
        return false;
    }

    FILE *fp = fopen(backup_path, "rb");
    if (!fp) {
        DB_ERROR(DB_ERROR_IO, "cannot open backup file: %s", backup_path);
        return false;
    }

    db_backup_header_t header;
    bool ok = read_and_validate_header(fp, &header);
    fclose(fp);

    if (!ok) return false;

    info_out->entry_count = header.entry_count;
    info_out->timestamp = header.timestamp;
    info_out->version = header.version;
    info_out->flags = header.flags & 0xFFFF;
    info_out->key_size = flags_get_key_size(header.flags);

    return true;
}
