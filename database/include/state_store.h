#ifndef STATE_STORE_H
#define STATE_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * State Store — Fixed 64-byte slot allocator backed by a flat file.
 *
 * Layout per slot: [2-byte length prefix][up to 62 bytes value]
 *
 * Supports alloc/free with a free list for slot reuse.
 * Opaque handle — struct defined in state_store.c.
 */

#define STATE_STORE_SLOT_SIZE    64
#define STATE_STORE_MAX_VALUE    62  // SLOT_SIZE - 2 (length prefix)

typedef struct state_store state_store_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new state store, truncating any existing file at path.
 * Returns NULL on failure.
 */
state_store_t *state_store_create(const char *path);

/**
 * Open an existing state store without truncating.
 * Sets next_slot from file size. For recovery (Stage 3).
 * Returns NULL on failure.
 */
state_store_t *state_store_open(const char *path);

/**
 * Close file descriptor and free resources. Does NOT unlink the file.
 */
void state_store_destroy(state_store_t *s);

// ============================================================================
// Slot Operations
// ============================================================================

/**
 * Allocate a slot (reuses from free list, or bumps next_slot).
 */
uint32_t state_store_alloc(state_store_t *s);

/**
 * Return a slot to the free list for reuse.
 */
void state_store_free(state_store_t *s, uint32_t slot);

/**
 * Write value to a slot. len must be <= STATE_STORE_MAX_VALUE.
 */
bool state_store_write(state_store_t *s, uint32_t slot,
                       const void *value, uint16_t len);

/**
 * Read value from a slot. out_len receives the stored length.
 */
bool state_store_read(state_store_t *s, uint32_t slot,
                      void *out_value, uint16_t *out_len);

// ============================================================================
// Durability
// ============================================================================

/**
 * Flush pending writes to disk (fdatasync).
 */
void state_store_sync(state_store_t *s);

// ============================================================================
// Stats / Accessors
// ============================================================================

uint32_t state_store_next_slot(const state_store_t *s);
uint32_t state_store_free_count(const state_store_t *s);
int      state_store_fd(const state_store_t *s);

#endif // STATE_STORE_H
