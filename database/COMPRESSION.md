# Data Compression Design

## Overview

Tiered compression strategy to reduce disk space and I/O while maintaining performance for hot data. Compression happens at the page manager level, transparent to ART operations and buffer pool.

**Key Insight**: Pages are stored with **variable sizes** on disk (compressed), but always expanded to **4KB in memory**. A page index maps logical page IDs to physical disk locations, enabling 2-5x real disk space savings.

## Compression Tiers

### Hot Pages (Uncompressed)
- **Definition**: Pages accessed within last N minutes (configurable, default 10 min)
- **Location**: In buffer pool cache
- **On Disk**: Uncompressed (4KB raw)
- **Access Time**: ~0µs (memory access)
- **Use Case**: Recent blocks, active accounts, frequently queried state

### Warm Pages (LZ4 Compressed)
- **Definition**: Pages accessed within last hour, not currently in cache
- **Location**: Disk only (evicted from cache)
- **On Disk**: LZ4 compressed (~1.5-2KB typical)
- **Access Time**: ~5-10µs (decompress + memory access)
- **Compression Ratio**: ~2-3x for structured data
- **Use Case**: Moderately recent state, occasional queries

### Cold Pages (Zstd Level 5)
- **Definition**: Pages not accessed for hours
- **Location**: Disk only
- **On Disk**: Zstd compressed (~800-1200 bytes typical)
- **Access Time**: ~20-50µs (decompress + memory access)
- **Compression Ratio**: ~3-5x for structured data
- **Use Case**: Historical state, old blocks, rarely queried

### Archival Pages (Zstd Level 19)
- **Definition**: Old versions eligible for garbage collection, or long-term archival
- **Location**: Disk only (separate archive files)
- **On Disk**: Maximum compression (~500-800 bytes typical)
- **Access Time**: ~100-200µs (heavy decompression)
- **Compression Ratio**: ~5-8x for structured data
- **Use Case**: Historical snapshots, compliance/audit, chain history

## Page Metadata

### Extended Page Header
```c
typedef struct {
    uint64_t page_id;
    uint64_t version;
    
    // Compression metadata
    uint8_t compression_type;    // NONE, LZ4, ZSTD_5, ZSTD_19
    uint32_t compressed_size;    // Actual bytes on disk
    uint32_t uncompressed_size;  // Always 4096 for pages
    
    uint32_t node_count;
    uint32_t free_space_offset;
    uint32_t checksum;           // On compressed data
    uint32_t flags;
    uint64_t prev_version;
    uint64_t last_access_time;   // For tiering decisions
} page_metadata_t;
```

### Compression Type Constants
```c
typedef enum {
    COMPRESSION_NONE = 0,     // Raw 4KB page
    COMPRESSION_LZ4 = 1,      // LZ4 compressed
    COMPRESSION_ZSTD_5 = 2,   // Zstd level 5
    COMPRESSION_ZSTD_19 = 3,  // Zstd level 19 (archival)
} compression_type_t;
```

## Storage Architecture: Variable-Size Pages on Disk

### The Problem

If we store compressed pages in fixed 4KB disk blocks, **we waste space**:
```
Page 0: [4KB allocated] - only uses 800 bytes compressed, wastes 3.2KB
Page 1: [4KB allocated] - only uses 1.2KB compressed, wastes 2.8KB
```
**Result**: No real disk space savings, only I/O bandwidth improvement.

### The Solution: Page Index + Variable-Size Storage

Store compressed pages **packed sequentially** with variable sizes:

```
Data file layout:
Offset 0:     [800 bytes]  - Page 0 (compressed)
Offset 800:   [1200 bytes] - Page 1 (compressed)
Offset 2000:  [650 bytes]  - Page 2 (compressed)
Offset 2650:  [2100 bytes] - Page 3 (compressed, not very compressible)
...
```

Use a **page index** to map page_id → disk location:

```c
typedef struct {
    uint64_t page_id;
    uint64_t file_offset;      // Where in data file?
    uint32_t compressed_size;  // How many bytes to read?
    uint8_t compression_type;  // NONE, LZ4, ZSTD_5, ZSTD_19
    uint32_t checksum;         // CRC32 of compressed data
    uint64_t version;          // MVCC version
} page_index_entry_t;

typedef struct {
    uint64_t num_entries;
    page_index_entry_t entries[];  // Sorted by page_id for binary search
} page_index_t;
```

### File Structure

```
database/
  metadata.0.dat              # Database header (alternating)
  metadata.1.dat              # Database header (alternating)
  pages.idx                   # Page index (maps page_id -> offset)
  data/
    pages_00000000.dat        # Packed compressed pages (512 MB each)
    pages_00000001.dat
    pages_00000002.dat
    ...
  wal/
    wal_00000000.log          # Write-ahead log
```

### Benefits

| Aspect | Fixed-Size Pages | Variable-Size Pages (Our Approach) |
|--------|-----------------|-------------------------------------|
| **Disk space** | 4KB per page (no savings) | 500-2000 bytes per page (2-5x savings) |
| **Read operation** | `seek(page_id * 4096)` | `index_lookup(page_id) → seek(offset)` |
| **Write operation** | Overwrite in place | Append to data file, update index |
| **Fragmentation** | None | Can fragment, needs compaction |
| **Index overhead** | None | ~24 bytes per page in index |

**For TB-scale Ethereum state, variable-size is essential** - the index lookup overhead (~100ns) is negligible compared to 2-5x disk space savings.

### Page Index Operations

```c
// Load page index on database open
page_index_t *load_page_index(const char *db_path) {
    int fd = open("pages.idx", O_RDONLY);
    
    // Read number of entries
    uint64_t num_entries;
    read(fd, &num_entries, sizeof(num_entries));
    
    // Allocate and read all entries
    page_index_t *index = malloc(sizeof(page_index_t) + 
                                  num_entries * sizeof(page_index_entry_t));
    index->num_entries = num_entries;
    read(fd, index->entries, num_entries * sizeof(page_index_entry_t));
    
    close(fd);
    return index;
}

// Lookup page location (binary search on sorted index)
page_index_entry_t *lookup_page(page_index_t *index, uint64_t page_id) {
    // Binary search on index->entries (sorted by page_id)
    size_t left = 0, right = index->num_entries;
    
    while (left < right) {
        size_t mid = (left + right) / 2;
        if (index->entries[mid].page_id == page_id) {
            return &index->entries[mid];
        } else if (index->entries[mid].page_id < page_id) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    
    return NULL;  // Page not found
}

// Add new page to index (append to end, will be sorted later)
void index_add_page(page_index_t **index, page_index_entry_t entry) {
    // Reallocate to add one more entry
    size_t new_count = (*index)->num_entries + 1;
    *index = realloc(*index, sizeof(page_index_t) + 
                     new_count * sizeof(page_index_entry_t));
    
    (*index)->entries[(*index)->num_entries] = entry;
    (*index)->num_entries = new_count;
    
    // Keep sorted (or batch sort later for performance)
    qsort((*index)->entries, new_count, sizeof(page_index_entry_t), 
          compare_page_id);
}

// Persist index to disk
void save_page_index(page_index_t *index, const char *db_path) {
    int fd = open("pages.idx", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    
    // Write number of entries
    write(fd, &index->num_entries, sizeof(index->num_entries));
    
    // Write all entries
    write(fd, index->entries, 
          index->num_entries * sizeof(page_index_entry_t));
    
    fsync(fd);
    close(fd);
}
```

## Page Metadata (In-Page Header)

Each page (when uncompressed to 4KB) has a header:

### Page Manager API Extensions

```c
// Load page (automatically decompresses if needed)
page_t *page_manager_load(page_manager_t *pm, uint64_t page_id, 
                          uint64_t version) {
    page_metadata_t meta = read_page_metadata(pm, page_id);
    
    // Decompress if needed
    uint8_t *page_data = malloc(4096);  // Always 4KB uncompressed
    
    if (entry->compression_type == COMPRESSION_NONE) {
        // Already uncompressed
        memcpy(page_data, compressed, 4096);
    } else {
        // Decompress to 4KB
        decompress_page(compressed, entry->compressed_size, 
                       entry->compression_type, 
                       page_data, 4096);
    }
    
    free(compressed);
    return page_data;
}

// Write page to disk (handles compression, append, index update)
void page_manager_write(page_manager_t *pm, uint64_t page_id, 
                        const uint8_t *page_data, 
                        compression_type_t compression) {
    uint8_t *write_data;
    uint32_t write_size;
    
    if (compression == COMPRESSION_NONE) {
        // Write uncompressed
        write_data = (uint8_t*)page_data;
        write_size = 4096;
    } else {
        // Compress
        write_data = malloc(4096);
        write_size = compress_page(page_data, 4096, compression, write_data);
        
        // Only use compression if significant savings (>10%)
        if (write_size >= 4096 * 0.9) {
            compression = COMPRESSION_NONE;
            free(write_data);
            write_data = (uint8_t*)page_data;
            write_size = 4096;
        }
    }
    
    // Calculate checksum
    uint32_t checksum = crc32(write_data, write_size);
    
    // Determine current data file and append offset
    uint32_t file_num = pm->current_data_file;
    uint64_t file_offset = pm->current_file_offset;
    
    // Check if need to create new data file (512MB limit)
    if (file_offset + write_size > 512 * 1024 * 1024) {
        file_num++;
        file_offset = 0;
        pm->current_data_file = file_num;
        pm->current_file_offset = 0;
    }
    
    // Open data file for append
    int fd = open_data_file(pm->db_path, file_num);
    lseek(fd, file_offset, SEEK_SET);
    write(fd, write_data, write_size);
    fsync(fd);
    close(fd);
    
    // Update index
    page_index_entry_t entry = {
        .page_id = page_id,
        .file_offset = (file_num * 512ULL * 1024 * 1024) + file_offset,
        .compressed_size = write_size,
        .compression_type = compression,
        .checksum = checksum,
        .version = pm->current_version
    };
    index_add_page(&pm->index, entry);
    
    // Update file offset
    pm->current_file_offset += write_size;
    
    if (compression != COMPRESSION_NONE) {
        free(write_data);
    }
}

// Promote compression level (hot → warm → cold → archival)
void page_manager_recompress(page_manager_t *pm, uint64_t page_id,
                              compression_type_t new_compression) {
    // Load existing page (decompresses)
    page_t *page_data = page_manager_load(pm, page_id, 0);
    
    // Write with new compression level
    page_manager_write(pm, page_id, page_data, new_compression);
    
    free(page_data);
}
```

### Compression Library Wrappers

```c
// LZ4 compression
size_t compress_lz4(const uint8_t *src, size_t src_size,
                    uint8_t *dst, size_t dst_capacity) {
    return LZ4_compress_default((const char*)src, (char*)dst, 
                                src_size, dst_capacity);
}

size_t decompress_lz4(const uint8_t *src, size_t src_size,
                      uint8_t *dst, size_t dst_capacity) {
    return LZ4_decompress_safe((const char*)src, (char*)dst,
                               src_size, dst_capacity);
}

// Zstd compression (with level)
size_t compress_zstd(const uint8_t *src, size_t src_size,
                     uint8_t *dst, size_t dst_capacity, int level) {
    return ZSTD_compress(dst, dst_capacity, src, src_size, level);
}

size_t decompress_zstd(const uint8_t *src, size_t src_size,
                       uint8_t *dst, size_t dst_capacity) {
    return ZSTD_decompress(dst, dst_capacity, src, src_size);
}

// Unified interface
size_t compress_page(const uint8_t *page, size_t page_size,
                     compression_type_t type, uint8_t *compressed) {
    switch (type) {
        case COMPRESSION_LZ4:
            return compress_lz4(page, page_size, compressed, page_size);
        case COMPRESSION_ZSTD_5:
            return compress_zstd(page, page_size, compressed, page_size, 5);
        case COMPRESSION_ZSTD_19:
            return compress_zstd(page, page_size, compressed, page_size, 19);
        default:
            memcpy(compressed, page, page_size);
            return page_size;
    }
}

size_t decompress_page(const uint8_t *compressed, size_t compressed_size,
                       compression_type_t type, 
                       uint8_t *page, size_t page_size) {
    switch (type) {
        case COMPRESSION_LZ4:
            return decompress_lz4(compressed, compressed_size, page, page_size);
        case COMPRESSION_ZSTD_5:
        case COMPRESSION_ZSTD_19:
            return decompress_zstd(compressed, compressed_size, page, page_size);
        default:
            memcpy(page, compressed, compressed_size);
            return compressed_size;
    }
}
```

## Background Compactor

### Compactor Thread

Runs periodically to promote pages to higher compression levels based on access patterns.

```c
typedef struct {
    page_manager_t *pm;
    buffer_pool_t *cache;
    bool running;
    pthread_t thread;
    
    // Configuration
    uint64_t hot_threshold_sec;    // Default: 600 (10 min)
    uint64_t warm_threshold_sec;   // Default: 3600 (1 hour)
    uint64_t cold_threshold_sec;   // Default: 86400 (24 hours)
    uint64_t compact_interval_sec; // Default: 300 (5 min)
} compactor_t;

void *compactor_thread(void *arg) {
    compactor_t *comp = (compactor_t *)arg;
    
    while (comp->running) {
        uint64_t now = time(NULL);
        
        // Scan all pages and recompress based on access time
        for (page_id in all_pages) {
            page_metadata_t meta = read_page_metadata(comp->pm, page_id);
            uint64_t idle_time = now - meta.last_access_time;
            
            // Skip if page is in cache (hot)
            if (buffer_pool_contains(comp->cache, page_id)) {
                continue;
            }
            
            // Promote compression level based on idle time
            if (idle_time > comp->cold_threshold_sec && 
                meta.compression_type < COMPRESSION_ZSTD_5) {
                // Cold: compress with Zstd level 5
                page_manager_recompress(comp->pm, page_id, COMPRESSION_ZSTD_5);
                
            } else if (idle_time > comp->warm_threshold_sec && 
                       meta.compression_type == COMPRESSION_NONE) {
                // Warm: compress with LZ4
                page_manager_recompress(comp->pm, page_id, COMPRESSION_LZ4);
            }
        }
        
        sleep(comp->compact_interval_sec);
    }
    
    return NULL;
}
```

### Write Policy

When buffer pool evicts a dirty page:

```c
void buffer_pool_evict_page(buffer_pool_t *pool, cached_page_t *page) {
    if (page->dirty) {
        uint64_t now = time(NULL);
        uint64_t idle_time = now - page->last_access;
        
        // Decide compression level at write time
        compression_type_t compression;
        if (idle_time < pool->hot_threshold) {
            compression = COMPRESSION_NONE;  // Write uncompressed
        } else if (idle_time < pool->warm_threshold) {
            compression = COMPRESSION_LZ4;   // Write LZ4 compressed
        } else {
            compression = COMPRESSION_ZSTD_5; // Write Zstd compressed
        }
        
        page_manager_write(pool->pm, page->page_id, page->data, compression);
    }
    
    // Remove from cache
    remove_from_lru(pool, page);
    free(page->data);
    free(page);
}
```

## Ethereum-Specific Optimizations

### Domain-Specific Compression

Additional compression for Ethereum state structures:

```c
// Empty account detection
bool is_empty_account(const uint8_t *account_rlp, size_t len) {
    // Check for nonce=0, balance=0, code_hash=empty, storage_root=empty
    // Can represent as single flag byte
    return check_empty_pattern(account_rlp, len);
}

// Zero value storage slots
bool is_zero_storage_value(const uint8_t *value, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (value[i] != 0) return false;
    }
    return true;
}

// Compress leaf value before storing in page
size_t compress_leaf_value(const uint8_t *value, size_t value_len,
                           uint8_t *compressed, compression_ctx_t *ctx) {
    // Try domain-specific patterns first
    if (ctx->is_account && is_empty_account(value, value_len)) {
        compressed[0] = SPECIAL_EMPTY_ACCOUNT;
        return 1;
    }
    
    if (ctx->is_storage && is_zero_storage_value(value, value_len)) {
        compressed[0] = SPECIAL_ZERO_STORAGE;
        return 1;
    }
    
    // Fall back to generic compression
    return compress_generic(value, value_len, compressed);
}
```

## Performance Characteristics

### Compression Speeds (4KB page)

| Algorithm | Compression | Decompression | Ratio | Use Case |
|-----------|-------------|---------------|-------|----------|
| None | N/A | N/A | 1x | Hot data |
| LZ4 | ~400 MB/s (~10µs) | ~2-4 GB/s (~1µs) | 2-3x | Warm data |
| Zstd-5 | ~200 MB/s (~20µs) | ~500 MB/s (~8µs) | 3-5x | Cold data |
| Zstd-19 | ~5 MB/s (~800µs) | ~500 MB/s (~8µs) | 5-8x | Archival |

### Disk Space Savings (Ethereum State)

| Data Type | Uncompressed | LZ4 | Zstd-5 | Savings |
|-----------|-------------|-----|--------|---------|
| Account RLP | 100 bytes | 45 bytes | 30 bytes | 70% |
| Storage Slots | 32 bytes | 20 bytes | 12 bytes | 62% |
| Empty Accounts | 100 bytes | 1 byte | 1 byte | 99% |
| Full Pages (4KB) | 4096 bytes | 1800 bytes | 1100 bytes | 73% |

**Total State Reduction**: 1.5-3 TB → 600 GB - 1 TB on disk

## Fragmentation and Compaction

### The Fragmentation Problem

With variable-size append-only writes, **fragmentation occurs** when pages are updated:

```
Initial state:
Offset 0:    [Page 1: 800 bytes]
Offset 800:  [Page 2: 1200 bytes]
Offset 2000: [Page 3: 600 bytes]

After updating Page 2 (CoW creates new version):
Offset 0:    [Page 1: 800 bytes]
Offset 800:  [OLD Page 2: 1200 bytes] ← Dead space (old version)
Offset 2000: [Page 3: 600 bytes]
Offset 2600: [NEW Page 2: 1300 bytes] ← Appended at end

Result: 1200 bytes wasted
```

### Compaction Strategy

Periodically **repack live pages** to reclaim dead space:

```c
void compact_data_file(page_manager_t *pm, uint32_t file_num) {
    char *old_file = get_data_file_path(pm->db_path, file_num);
    char *new_file = malloc(strlen(old_file) + 10);
    sprintf(new_file, "%s.compact", old_file);
    
    int old_fd = open(old_file, O_RDONLY);
    int new_fd = open(new_file, O_WRONLY | O_CREAT, 0644);
    
    uint64_t new_offset = 0;
    
    // For each page in this file
    for (each page_index_entry in file_num) {
        // Skip if page has newer version (dead)
        if (is_page_dead(pm->index, entry.page_id, entry.version)) {
            continue;  // Don't copy dead pages
        }
        
        // Read live page
        uint8_t *data = malloc(entry.compressed_size);
        lseek(old_fd, entry.file_offset, SEEK_SET);
        read(old_fd, data, entry.compressed_size);
        
        // Write to new file at new offset
        write(new_fd, data, entry.compressed_size);
        
        // Update index with new offset
        update_index_offset(pm->index, entry.page_id, 
                           (file_num * 512ULL * 1024 * 1024) + new_offset);
        
        new_offset += entry.compressed_size;
        free(data);
    }
    
    fsync(new_fd);
    close(old_fd);
    close(new_fd);
    
    // Atomically replace old file
    rename(new_file, old_file);
    
    free(new_file);
}
```

### When to Compact

Trigger compaction when:
1. **Fragmentation ratio > 30%**: More than 30% of file is dead space
2. **File size > threshold**: File grows beyond expected size (e.g., 512 MB file is now 700 MB due to fragmentation)
3. **Periodic schedule**: Every 24 hours during low-load period

```c
typedef struct {
    uint64_t total_bytes;      // File size on disk
    uint64_t live_bytes;       // Bytes for live pages
    uint64_t dead_bytes;       // Bytes for dead/old versions
    float fragmentation_ratio; // dead_bytes / total_bytes
} file_stats_t;

bool should_compact_file(file_stats_t *stats) {
    return stats->fragmentation_ratio > 0.30 ||
           stats->total_bytes > (512 * 1024 * 1024 * 1.3);  // 30% overgrowth
}
```

### Compaction Frequency

- **Hot files** (recent data): Compact rarely (high churn)
- **Warm files** (moderate age): Compact when fragmentation > 30%
- **Cold files** (old data): Compact aggressively (stable, rarely change)

## Configuration

```c
typedef struct {
    // Compression thresholds (seconds)
    uint64_t hot_threshold;    // Default: 600 (10 min)
    uint64_t warm_threshold;   // Default: 3600 (1 hour)
    uint64_t cold_threshold;   // Default: 86400 (24 hours)
    
    // Compactor settings
    uint64_t compact_interval; // Default: 300 (5 min)
    bool enable_compactor;     // Default: true
    
    // Compression ratio threshold
    float min_compression_ratio; // Default: 0.9 (must save 10%+)
    
    // Algorithm selection
    bool enable_lz4;           // Default: true
    bool enable_zstd;          // Default: true
    int zstd_cold_level;       // Default: 5
    int zstd_archival_level;   // Default: 19
} compression_config_t;
```

## Testing Strategy

### Unit Tests
- [ ] Compress/decompress round-trip for each algorithm
- [ ] Verify checksums on compressed data
- [ ] Test compression ratio calculation
- [ ] Test fallback to uncompressed when ratio insufficient

### Integration Tests
- [ ] Page load with each compression type
- [ ] Buffer pool eviction with compression
- [ ] Background compactor promotions (hot → warm → cold)
- [ ] Mixed workload (hot reads, cold reads, writes)

### Performance Tests
- [ ] Compression speed benchmark (all algorithms)
- [ ] Decompression speed benchmark
- [ ] End-to-end latency with compression
- [ ] Disk space savings measurement
- [ ] Cache hit rate with compression (should be same)

### Ethereum-Specific Tests
- [ ] Compress real Ethereum account data
- [ ] Compress storage slots
- [ ] Empty account optimization
- [ ] Full state compression ratio

## Implementation Phases

### Phase 1: Page Index Infrastructure
- [ ] Implement page index structure (page_id → offset mapping)
- [ ] Add index load/save functions with binary search
- [ ] Add index persistence to disk
- [ ] Test index operations (add, lookup, update)

### Phase 2: Variable-Size Page Storage
- [ ] Implement append-only page writes with variable sizes
- [ ] Update page_manager_load to use index lookup
- [ ] Update page_manager_write to append and update index
- [ ] Add multi-file support (512 MB segments)

### Phase 3: Compression Integration
- [ ] Implement compression/decompression wrappers (LZ4, Zstd)
- [ ] Add compression to page_manager_write/load
- [ ] Add compression tier selection logic
- [ ] Add tests for compression round-trip

### Phase 4: Background Compactor
- [ ] Implement compactor thread for tier promotion
- [ ] Add file compaction to reclaim dead space
- [ ] Add fragmentation tracking and statistics
- [ ] Add monitoring/logging

### Phase 5: Optimization
- [ ] Ethereum-specific compression patterns
- [ ] Compression statistics and adaptive tuning
- [ ] Parallel compression for bulk operations
- [ ] Index caching for hot paths

## Dependencies

**Required Libraries**:
- `liblz4` (LZ4 compression)
- `libzstd` (Zstandard compression)

**Installation**:
```bash
# Ubuntu/Debian
apt-get install liblz4-dev libzstd-dev

# macOS
brew install lz4 zstd
```

**CMake Integration**:
```cmake
find_package(LZ4 REQUIRED)
find_package(ZSTD REQUIRED)

target_link_libraries(art_database
    LZ4::LZ4
    ZSTD::ZSTD
)
```
