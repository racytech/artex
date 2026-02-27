#include "../include/state_store.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// ============================================================================
// Opaque struct
// ============================================================================

struct state_store {
    int fd;
    uint32_t next_slot;
    uint32_t *free_list;
    uint32_t free_count;
    uint32_t free_cap;
};

// ============================================================================
// Internal helpers
// ============================================================================

static state_store_t *store_alloc_struct(void) {
    state_store_t *s = malloc(sizeof(state_store_t));
    if (!s) return NULL;
    s->fd = -1;
    s->next_slot = 0;
    s->free_count = 0;
    s->free_cap = 1024;
    s->free_list = malloc(s->free_cap * sizeof(uint32_t));
    if (!s->free_list) {
        free(s);
        return NULL;
    }
    return s;
}

// ============================================================================
// Lifecycle
// ============================================================================

state_store_t *state_store_create(const char *path) {
    state_store_t *s = store_alloc_struct();
    if (!s) return NULL;

    s->fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (s->fd < 0) {
        free(s->free_list);
        free(s);
        return NULL;
    }
    return s;
}

state_store_t *state_store_open(const char *path) {
    state_store_t *s = store_alloc_struct();
    if (!s) return NULL;

    s->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (s->fd < 0) {
        free(s->free_list);
        free(s);
        return NULL;
    }

    // Derive next_slot from existing file size
    struct stat st;
    if (fstat(s->fd, &st) == 0 && st.st_size > 0) {
        s->next_slot = (uint32_t)(st.st_size / STATE_STORE_SLOT_SIZE);
    }
    return s;
}

void state_store_destroy(state_store_t *s) {
    if (!s) return;
    if (s->fd >= 0) close(s->fd);
    free(s->free_list);
    free(s);
}

// ============================================================================
// Slot Operations
// ============================================================================

uint32_t state_store_alloc(state_store_t *s) {
    if (s->free_count > 0) {
        return s->free_list[--s->free_count];
    }
    return s->next_slot++;
}

void state_store_free(state_store_t *s, uint32_t slot) {
    if (s->free_count >= s->free_cap) {
        s->free_cap *= 2;
        s->free_list = realloc(s->free_list, s->free_cap * sizeof(uint32_t));
    }
    s->free_list[s->free_count++] = slot;
}

bool state_store_write(state_store_t *s, uint32_t slot,
                       const void *value, uint16_t len) {
    uint8_t buf[STATE_STORE_SLOT_SIZE] = {0};
    memcpy(buf, &len, 2);
    memcpy(buf + 2, value, len);
    ssize_t w = pwrite(s->fd, buf, STATE_STORE_SLOT_SIZE,
                       (off_t)slot * STATE_STORE_SLOT_SIZE);
    return w == STATE_STORE_SLOT_SIZE;
}

bool state_store_read(state_store_t *s, uint32_t slot,
                      void *out_value, uint16_t *out_len) {
    uint8_t buf[STATE_STORE_SLOT_SIZE];
    ssize_t r = pread(s->fd, buf, STATE_STORE_SLOT_SIZE,
                      (off_t)slot * STATE_STORE_SLOT_SIZE);
    if (r != STATE_STORE_SLOT_SIZE) return false;

    uint16_t len;
    memcpy(&len, buf, 2);
    if (len > STATE_STORE_MAX_VALUE) return false;

    if (out_len) *out_len = len;
    if (out_value) memcpy(out_value, buf + 2, len);
    return true;
}

// ============================================================================
// Durability
// ============================================================================

void state_store_sync(state_store_t *s) {
    if (s && s->fd >= 0) fdatasync(s->fd);
}

// ============================================================================
// Stats / Accessors
// ============================================================================

uint32_t state_store_next_slot(const state_store_t *s) {
    return s ? s->next_slot : 0;
}

uint32_t state_store_free_count(const state_store_t *s) {
    return s ? s->free_count : 0;
}

int state_store_fd(const state_store_t *s) {
    return s ? s->fd : -1;
}
