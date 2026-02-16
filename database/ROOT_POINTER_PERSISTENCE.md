# Root Pointer Persistence

## Overview

The database metadata (root pointer, version, WAL state, page allocation state) must be persisted to disk so the database can be reopened after restart. This document describes the **double-buffered metadata strategy** for atomic updates.

## The Problem

When the database restarts, it needs to know:
- Where is the tree root? (which page, which offset)
- What is the current version? (for MVCC)
- Where to resume WAL recovery from?
- What is the next available page ID?

This information must be stored on disk and updated atomically on every commit. A partial write (due to crash/power loss) would corrupt the database.

## Solution: Double-Buffered Metadata Files

### Strategy

Use **two alternating metadata files** with version numbers:
- `metadata.0.dat`
- `metadata.1.dat`

On startup, read both files and use the one with the **highest valid version number**. On commit, write to the **opposite** file from the current one. This ensures there's always at least one valid metadata file, even if a crash occurs during write.

### Metadata File Structure

```c
#define DB_MAGIC 0x4152544442  // "ARTDB" in hex
#define DB_VERSION 1

typedef struct {
    // Header validation
    uint64_t magic;              // DB_MAGIC (detect valid file)
    uint64_t version;            // Schema version for compatibility
    
    // Tree state - THE CRITICAL DATA
    node_ref_t current_root;     // Root of latest committed version (page_id, offset)
    uint64_t current_version_id; // MVCC version number (monotonically increasing)
    size_t tree_size;            // Total number of entries in tree
    
    // WAL recovery state
    uint64_t checkpoint_lsn;     // Last checkpointed log sequence number
    uint64_t wal_segment_id;     // Current WAL file number
    
    // Page allocation state
    uint64_t next_page_id;       // Next available page ID to allocate
    uint32_t num_data_files;     // Number of pages_XXXXX.dat files
    
    // Compression configuration
    compression_config_t compression;
    
    // Integrity
    uint64_t last_modified;      // Unix timestamp
    uint32_t checksum;           // CRC32 of entire header (excluding this field)
    
    uint8_t padding[384];        // Reserved for future use (total 512 bytes)
} database_header_t;

_Static_assert(sizeof(database_header_t) == 512, "Header must be exactly 512 bytes");
```

### Atomic Update Protocol

```c
// On startup: Load metadata
database_header_t *load_metadata(const char *db_path) {
    database_header_t meta0, meta1;
    bool valid0 = false, valid1 = false;
    
    // Try to read metadata.0.dat
    int fd0 = open_metadata_file(db_path, 0);
    if (fd0 >= 0) {
        read(fd0, &meta0, sizeof(meta0));
        close(fd0);
        
        // Validate magic and checksum
        if (meta0.magic == DB_MAGIC && verify_checksum(&meta0)) {
            valid0 = true;
        }
    }
    
    // Try to read metadata.1.dat
    int fd1 = open_metadata_file(db_path, 1);
    if (fd1 >= 0) {
        read(fd1, &meta1, sizeof(meta1));
        close(fd1);
        
        if (meta1.magic == DB_MAGIC && verify_checksum(&meta1)) {
            valid1 = true;
        }
    }
    
    // Choose the valid file with highest version
    if (!valid0 && !valid1) {
        // Both corrupt or missing - initialize new database
        return initialize_new_database(db_path);
    }
    
    if (valid0 && !valid1) return &meta0;
    if (!valid0 && valid1) return &meta1;
    
    // Both valid - use higher version
    return (meta0.current_version_id > meta1.current_version_id) ? &meta0 : &meta1;
}

// On commit: Update metadata atomically
void update_metadata(database_header_t *meta, node_ref_t new_root) {
    // Increment version
    meta->current_version_id++;
    meta->current_root = new_root;
    meta->last_modified = time(NULL);
    
    // Calculate checksum
    meta->checksum = crc32(meta, offsetof(database_header_t, checksum));
    
    // Determine which file to write to (opposite of current)
    int current_file = meta->current_version_id % 2;
    int target_file = 1 - current_file;
    
    // Write to target file
    int fd = open_metadata_file(db_path, target_file);
    write(fd, meta, sizeof(database_header_t));
    fsync(fd);  // Force to disk
    close(fd);
    
    // Done! New version is now available
    // If crash before fsync: old file still valid
    // If crash after fsync: new file valid, old file still valid
    // Never in corrupt state
}
```

### File Paths

```c
char *get_metadata_path(const char *db_path, int file_num) {
    char *path = malloc(strlen(db_path) + 64);
    sprintf(path, "%s/metadata.%d.dat", db_path, file_num);
    return path;
}
```

### Recovery Scenarios

| Scenario | State | Recovery |
|----------|-------|----------|
| Clean shutdown | Both files valid | Use file with higher version |
| Crash during write | One file valid, one partial/corrupt | Use valid file (ignore corrupt) |
| Crash after write, before close | New file valid, old file valid | Use new file (higher version) |
| Both files corrupt | Database unrecoverable | Error - reconstruct from WAL if possible |
| First open (no files) | Neither exists | Initialize new database |

### Why This Works

1. **Always have valid metadata**: While writing file 1, file 0 remains untouched and valid
2. **Atomic switch**: Version number increments atomically on successful write
3. **Crash-safe**: At any point, at least one file is valid
4. **Simple**: Just alternate between 0 and 1
5. **Detectable corruption**: Magic number and checksum catch invalid files

### Storage Overhead

- **Space**: 1 KB total (2 × 512 bytes) - negligible
- **I/O**: Read both on startup (~1 KB), write one on commit (512 bytes)
- **Benefit**: Eliminates single point of failure for critical metadata

## Implementation Phases

### Phase 1: Basic Structure
- Define `database_header_t` struct
- Implement checksum calculation (CRC32)
- File I/O helpers (open, read, write metadata files)

### Phase 2: Load/Save
- `load_metadata()` - read both files, choose valid one
- `initialize_new_database()` - create initial metadata
- `update_metadata()` - atomic write to alternate file

### Phase 3: Integration
- Call `load_metadata()` on database open
- Call `update_metadata()` on transaction commit
- Handle corruption errors gracefully

### Phase 4: Testing
- Test crash during write (kill process mid-write)
- Test both files valid with different versions
- Test one file corrupt, one valid
- Test both files corrupt (error handling)
- Test first initialization (no files exist)

## Open Questions

✅ **RESOLVED**: Use single file or versioned?
- **Decision**: Double-buffered (2 files) for safety

🟡 **TODO**: Backup strategy?
- Keep N old versions? (metadata.0.dat, metadata.1.dat, metadata.2.dat.bak)
- Or just rely on WAL for disaster recovery?

🟡 **TODO**: Checksum algorithm?
- CRC32 (fast, good enough for 512 bytes)
- Or stronger hash (SHA256)?

🟡 **TODO**: Endianness handling?
- Store in little-endian for portability?
- Or native endian for performance?
