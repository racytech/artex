#ifndef CODE_STORE_H
#define CODE_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Code Store — Append-only storage for contract bytecode.
 *
 * code.dat: raw bytecode concatenated, no framing or length prefix.
 * In-memory index array tracks {offset, length} per entry (12 bytes each).
 *
 * Content-addressed by keccak256(bytecode) — dedup handled by caller
 * via compact_art lookup before calling code_store_append.
 *
 * Opaque handle — struct defined in code_store.c.
 */

typedef struct code_store code_store_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new code store, truncating any existing file at path.
 * Returns NULL on failure.
 */
code_store_t *code_store_create(const char *path);

/**
 * Open an existing code store without truncating.
 * Sets file_size from fstat. Entries array starts empty
 * (populated by checkpoint/recovery in Stage 3).
 * Returns NULL on failure.
 */
code_store_t *code_store_open(const char *path);

/**
 * Close file descriptor and free resources. Does NOT unlink the file.
 */
void code_store_destroy(code_store_t *cs);

// ============================================================================
// Write (append-only)
// ============================================================================

/**
 * Append bytecode to code.dat. Returns entry index.
 * Caller is responsible for dedup (check compact_art before calling).
 */
uint32_t code_store_append(code_store_t *cs, const void *bytecode, uint32_t len);

// ============================================================================
// Read
// ============================================================================

/**
 * Get bytecode length for a given entry index.
 * Returns 0 if index is out of range.
 */
uint32_t code_store_length(const code_store_t *cs, uint32_t index);

/**
 * Read bytecode into buf. buf_len must be >= code_store_length().
 * Returns false on error or if index is out of range.
 */
bool code_store_read(code_store_t *cs, uint32_t index,
                     void *out, uint32_t buf_len);

// ============================================================================
// Durability
// ============================================================================

/**
 * Flush pending writes to disk (fdatasync).
 */
void code_store_sync(code_store_t *cs);

// ============================================================================
// Stats / Accessors
// ============================================================================

uint32_t code_store_count(const code_store_t *cs);
uint64_t code_store_file_size(const code_store_t *cs);

#endif // CODE_STORE_H
