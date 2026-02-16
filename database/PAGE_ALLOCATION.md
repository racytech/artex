# Page Allocation Strategy

## Overview

Design for efficiently allocating, tracking, and reclaiming 4KB pages on disk. The page allocator must support:
- Fast allocation of new pages
- Tracking free space within pages
- Efficient reclamation of deleted pages
- Minimal fragmentation
- Support for multi-file storage (when DB > 1GB)

## Core Requirements

1. **Variable-size nodes**: Nodes range from ~48 bytes (NODE_4) to ~3KB (NODE_256)
2. **Node growth**: NODE_4 → NODE_16 → NODE_48 → NODE_256 may require larger space
3. **Leaves**: Variable size (key_len + value_len), can be hundreds of bytes
4. **Page size**: Fixed 4KB pages
5. **Copy-on-write**: Modified nodes allocated on new pages (for MVCC)

## Page Structure

```c
#define PAGE_SIZE 4096
#define PAGE_HEADER_SIZE 64

typedef struct {
    uint64_t page_id;
    uint64_t version;              // MVCC version
    
    // Free space management
    uint32_t free_offset;          // Next free byte (grows from PAGE_HEADER_SIZE)
    uint32_t num_nodes;            // Number of nodes in page
    uint32_t fragmented_bytes;     // Wasted space from deletions
    
    // Compression metadata
    uint8_t compression_type;      // NONE, LZ4, ZSTD_5, ZSTD_19
    uint32_t compressed_size;      // If compressed
    uint32_t uncompressed_size;    // Always 4096
    
    // Integrity
    uint32_t checksum;             // CRC32 of page data
    uint32_t flags;                // Various flags
    uint64_t prev_version;         // For CoW chain
    uint64_t last_access_time;     // For compression tier
    
    uint8_t padding[8];            // Align to 64 bytes
} page_header_t;

// Page layout:
// [0-63]:     page_header_t
// [64-free_offset): Allocated nodes (packed sequentially)
// [free_offset-4096): Free space
```

## Allocation Strategy: Free List with Size Classes

Group pages by available free space into size classes for fast allocation.

```c
typedef enum {
    SIZE_CLASS_TINY,      // 0-128 bytes free
    SIZE_CLASS_SMALL,     // 128-512 bytes free
    SIZE_CLASS_MEDIUM,    // 512-1024 bytes free
    SIZE_CLASS_LARGE,     // 1024-2048 bytes free
    SIZE_CLASS_HUGE,      // 2048+ bytes free
    SIZE_CLASS_EMPTY,     // Completely empty (4032 bytes free)
    SIZE_CLASS_COUNT
} size_class_t;

typedef struct {
    // Array of free lists, one per size class
    uint64_t *lists[SIZE_CLASS_COUNT];
    size_t counts[SIZE_CLASS_COUNT];
    size_t capacities[SIZE_CLASS_COUNT];
    
    // Next page ID for new allocations
    uint64_t next_page_id;
    
    // Statistics
    uint64_t total_pages;
    uint64_t allocated_pages;
    uint64_t fragmented_pages;
} page_allocator_t;

// Determine size class from free space
size_class_t get_size_class(uint32_t free_bytes) {
    if (free_bytes < 128) return SIZE_CLASS_TINY;
    if (free_bytes < 512) return SIZE_CLASS_SMALL;
    if (free_bytes < 1024) return SIZE_CLASS_MEDIUM;
    if (free_bytes < 2048) return SIZE_CLASS_LARGE;
    if (free_bytes >= 4032) return SIZE_CLASS_EMPTY;
    return SIZE_CLASS_HUGE;
}

// Allocate space for a node of given size
node_ref_t page_allocator_alloc(page_allocator_t *alloc, size_t node_size) {
    // Determine minimum size class needed
    size_class_t min_class = get_size_class(node_size);
    
    // Try to find page with enough space, starting from min_class
    for (size_class_t class = min_class; class < SIZE_CLASS_COUNT; class++) {
        if (alloc->counts[class] > 0) {
            // Found a page with space
            uint64_t page_id = alloc->lists[class][0];
            
            // Load page header to get free offset
            page_header_t header = load_page_header(page_id);
            uint32_t offset = header.free_offset;
            
            // Allocate at free_offset
            header.free_offset += node_size;
            header.num_nodes++;
            
            // Update size class if free space changed
            uint32_t new_free = PAGE_SIZE - header.free_offset;
            size_class_t new_class = get_size_class(new_free);
            
            if (new_class != class) {
                // Remove from current class
                remove_from_free_list(alloc, class, page_id);
                
                // Add to new class (if not full)
                if (new_free > 0) {
                    add_to_free_list(alloc, new_class, page_id);
                }
            }
            
            // Write updated header
            write_page_header(page_id, &header);
            
            return (node_ref_t){page_id, offset};
        }
    }
    
    // No existing page has space - allocate new page
    uint64_t page_id = alloc->next_page_id++;
    
    page_header_t header = {
        .page_id = page_id,
        .free_offset = PAGE_HEADER_SIZE + node_size,
        .num_nodes = 1,
        .fragmented_bytes = 0,
        // ... initialize rest
    };
    
    write_page_header(page_id, &header);
    
    // Add to appropriate free list
    uint32_t remaining_free = PAGE_SIZE - header.free_offset;
    add_to_free_list(alloc, get_size_class(remaining_free), page_id);
    
    alloc->total_pages++;
    alloc->allocated_pages++;
    
    return (node_ref_t){page_id, PAGE_HEADER_SIZE};
}
```

**Why This Strategy:**
- **O(1) allocation** - Fast lookup in appropriate size class
- **Minimal fragmentation** - Best-fit within class, pages move between classes as needed
- **Simple implementation** - Straightforward free list management
- **Good for variable sizes** - NODE_4 (48B) to NODE_256 (3KB) handled efficiently
- **CoW friendly** - Easy to allocate new pages, old pages naturally freed

---

## Implementation Details

#### Free List Management
```c
void add_to_free_list(page_allocator_t *alloc, size_class_t class, uint64_t page_id) {
    // Grow array if needed
    if (alloc->counts[class] >= alloc->capacities[class]) {
        size_t new_capacity = alloc->capacities[class] * 2;
        alloc->lists[class] = realloc(alloc->lists[class], 
                                       new_capacity * sizeof(uint64_t));
        alloc->capacities[class] = new_capacity;
    }
    
    alloc->lists[class][alloc->counts[class]++] = page_id;
}

void remove_from_free_list(page_allocator_t *alloc, size_class_t class, 
                           uint64_t page_id) {
    // Find page in list
    for (size_t i = 0; i < alloc->counts[class]; i++) {
        if (alloc->lists[class][i] == page_id) {
            // Swap with last element and decrement count
            alloc->lists[class][i] = alloc->lists[class][alloc->counts[class] - 1];
            alloc->counts[class]--;
            return;
        }
    }
}
```

#### Within-Page Allocation
```c
// Pages allocate sequentially from free_offset
// When a node is deleted, space is marked as fragmented

void mark_node_deleted(page_header_t *header, uint32_t offset, size_t size) {
    header->num_nodes--;
    header->fragmented_bytes += size;
    
    // If fragmentation is high, trigger page compaction
    uint32_t free_space = PAGE_SIZE - header->free_offset;
    if (header->fragmented_bytes > free_space && 
        header->fragmented_bytes > 512) {
        // This page needs compaction
        mark_for_compaction(header->page_id);
    }
}
```

#### Page Compaction (for fragmented pages)
```c
void compact_page(uint64_t page_id) {
    // Load page
    page_t *old_page = load_page(page_id);
    
    // Allocate new page
    uint64_t new_page_id = alloc_new_page();
    page_t *new_page = create_page(new_page_id);
    
    // Copy live nodes to new page (skip deleted/fragmented space)
    uint32_t new_offset = PAGE_HEADER_SIZE;
    
    for (each node in old_page) {
        if (!is_deleted(node)) {
            memcpy(new_page->data + new_offset, node, node->size);
            
            // Update parent references to point to new location
            update_parent_reference(node, new_page_id, new_offset);
            
            new_offset += node->size;
        }
    }
    
    new_page->header.free_offset = new_offset;
    new_page->header.fragmented_bytes = 0;
    
    // Write new page, free old page
    write_page(new_page);
    free_page(old_page_id);
}
```

### Optimization: Page Pooling

Keep recently freed pages in a "hot" pool for reuse without disk I/O.

```c
typedef struct {
    uint64_t *hot_pages;     // Recently freed pages
    size_t hot_count;
    size_t hot_capacity;     // Max 100 pages (~400KB metadata)
} page_pool_t;

node_ref_t alloc_with_pool(page_allocator_t *alloc, page_pool_t *pool, 
                            size_t node_size) {
    // Try hot pool first
    if (pool->hot_count > 0) {
        uint64_t page_id = pool->hot_pages[--pool->hot_count];
        
        // Check if page has enough space
        page_header_t header = load_page_header(page_id);
        if (PAGE_SIZE - header.free_offset >= node_size) {
            // Use this page
            uint32_t offset = header.free_offset;
            header.free_offset += node_size;
            header.num_nodes++;
            write_page_header(page_id, &header);
            
            return (node_ref_t){page_id, offset};
        }
        
        // Page doesn't have space, put back in regular allocator
        size_class_t class = get_size_class(PAGE_SIZE - header.free_offset);
        add_to_free_list(alloc, class, page_id);
    }
    
    // Fall back to regular allocation
    return page_allocator_alloc(alloc, node_size);
}
```

## Multi-File Support

When database grows beyond 1GB, split across multiple files.

```c
#define PAGES_PER_FILE 131072  // 512 MB / 4096 bytes = 128K pages

uint32_t get_file_num(uint64_t page_id) {
    return page_id / PAGES_PER_FILE;
}

uint32_t get_page_offset(uint64_t page_id) {
    return page_id % PAGES_PER_FILE;
}

char *get_data_file_path(const char *db_path, uint32_t file_num) {
    char *path = malloc(strlen(db_path) + 64);
    sprintf(path, "%s/data/pages_%08u.dat", db_path, file_num);
    return path;
}

int open_data_file(const char *db_path, uint32_t file_num) {
    char *path = get_data_file_path(db_path, file_num);
    
    // Create if doesn't exist
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    
    if (fd >= 0) {
        // Pre-allocate 512 MB for performance
        posix_fallocate(fd, 0, PAGES_PER_FILE * PAGE_SIZE);
    }
    
    free(path);
    return fd;
}
```

## Persistence

The page allocator state must be persisted in metadata.

```c
typedef struct {
    // Free list state (size classes)
    uint32_t free_list_counts[SIZE_CLASS_COUNT];
    uint64_t free_list_offsets[SIZE_CLASS_COUNT];  // Offset in allocator.dat
    
    // Global state
    uint64_t next_page_id;
    uint64_t total_pages;
    uint64_t allocated_pages;
    uint64_t fragmented_pages;
    
} allocator_metadata_t;

// Saved to {db_path}/allocator.dat
// Free lists themselves saved as binary arrays
```

## Statistics & Monitoring

```c
typedef struct {
    // Allocation stats
    uint64_t allocations;
    uint64_t frees;
    uint64_t compactions;
    
    // Space usage
    uint64_t total_space;      // Total allocated pages * 4096
    uint64_t used_space;       // Actual data (excluding headers, fragmentation)
    uint64_t fragmented_space; // Wasted space
    
    // Per-class stats
    uint64_t allocs_per_class[SIZE_CLASS_COUNT];
    uint64_t pages_per_class[SIZE_CLASS_COUNT];
    
    // Performance
    uint64_t avg_alloc_time_ns;
    uint64_t avg_free_time_ns;
} allocator_stats_t;
```

## Testing Strategy

1. **Unit Tests**:
   - Allocate single node
   - Allocate multiple nodes in same page
   - Allocate nodes of different sizes
   - Free nodes and verify free list updates
   - Test size class transitions

2. **Integration Tests**:
   - Allocate/free mixed workload
   - Trigger page compaction
   - Test multi-file growth
   - Recovery from crash (reload metadata)

3. **Stress Tests**:
   - Allocate millions of nodes
   - Measure fragmentation over time
   - Test allocation speed under load
   - Verify no memory leaks

4. **Correctness**:
   - No double allocation (same offset returned twice)
   - All frees are valid
   - Free lists are consistent
   - Statistics add up correctly
