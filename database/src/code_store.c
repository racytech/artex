#include "../include/code_store.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// ============================================================================
// Internal types
// ============================================================================

typedef struct {
    uint64_t offset;
    uint32_t length;
} code_entry_t;

// ============================================================================
// Opaque struct
// ============================================================================

struct code_store {
    int fd;
    code_entry_t *entries;
    uint32_t count;
    uint32_t cap;
    uint64_t file_size;
};

// ============================================================================
// Internal helpers
// ============================================================================

static code_store_t *cs_alloc_struct(void) {
    code_store_t *cs = malloc(sizeof(code_store_t));
    if (!cs) return NULL;
    cs->fd = -1;
    cs->count = 0;
    cs->cap = 1024;
    cs->file_size = 0;
    cs->entries = malloc(cs->cap * sizeof(code_entry_t));
    if (!cs->entries) {
        free(cs);
        return NULL;
    }
    return cs;
}

// ============================================================================
// Lifecycle
// ============================================================================

code_store_t *code_store_create(const char *path) {
    code_store_t *cs = cs_alloc_struct();
    if (!cs) return NULL;

    cs->fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (cs->fd < 0) {
        free(cs->entries);
        free(cs);
        return NULL;
    }
    return cs;
}

code_store_t *code_store_open(const char *path) {
    code_store_t *cs = cs_alloc_struct();
    if (!cs) return NULL;

    cs->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (cs->fd < 0) {
        free(cs->entries);
        free(cs);
        return NULL;
    }

    // Derive file_size from existing file
    struct stat st;
    if (fstat(cs->fd, &st) == 0 && st.st_size > 0) {
        cs->file_size = (uint64_t)st.st_size;
    }
    return cs;
}

void code_store_destroy(code_store_t *cs) {
    if (!cs) return;
    if (cs->fd >= 0) close(cs->fd);
    free(cs->entries);
    free(cs);
}

// ============================================================================
// Write (append-only)
// ============================================================================

uint32_t code_store_append(code_store_t *cs, const void *bytecode, uint32_t len) {
    // Grow entries array if needed
    if (cs->count >= cs->cap) {
        cs->cap *= 2;
        cs->entries = realloc(cs->entries, cs->cap * sizeof(code_entry_t));
    }

    // Append bytecode to file
    uint64_t offset = cs->file_size;
    ssize_t w = pwrite(cs->fd, bytecode, len, (off_t)offset);
    if (w != (ssize_t)len) {
        // Write failed — don't add entry
        return UINT32_MAX;
    }

    // Record entry
    uint32_t index = cs->count;
    cs->entries[index] = (code_entry_t){ .offset = offset, .length = len };
    cs->count++;
    cs->file_size += len;

    return index;
}

// ============================================================================
// Read
// ============================================================================

uint32_t code_store_length(const code_store_t *cs, uint32_t index) {
    if (!cs || index >= cs->count) return 0;
    return cs->entries[index].length;
}

bool code_store_read(code_store_t *cs, uint32_t index,
                     void *out, uint32_t buf_len) {
    if (!cs || index >= cs->count || !out) return false;

    code_entry_t *e = &cs->entries[index];
    if (buf_len < e->length) return false;

    ssize_t r = pread(cs->fd, out, e->length, (off_t)e->offset);
    return r == (ssize_t)e->length;
}

// ============================================================================
// Durability
// ============================================================================

void code_store_sync(code_store_t *cs) {
    if (cs && cs->fd >= 0) fdatasync(cs->fd);
}

// ============================================================================
// Stats / Accessors
// ============================================================================

uint32_t code_store_count(const code_store_t *cs) {
    return cs ? cs->count : 0;
}

uint64_t code_store_file_size(const code_store_t *cs) {
    return cs ? cs->file_size : 0;
}
