#ifndef DB_ERROR_H
#define DB_ERROR_H

#include <stdint.h>
#include <stdio.h>

/**
 * Unified error codes for the database
 *
 * Values -1 through -6 intentionally match page_result_t so that
 * DB_ERROR_FROM_PAGE() is a trivial cast.
 *
 * Thread-local: each thread maintains its own error trace stack.
 */
typedef enum {
    DB_OK                     =  0,
    DB_ERROR_NOT_FOUND        = -1,
    DB_ERROR_IO               = -2,
    DB_ERROR_DISK_FULL        = -3,
    DB_ERROR_CORRUPTION       = -4,
    DB_ERROR_INVALID_ARG      = -5,
    DB_ERROR_OUT_OF_MEMORY    = -6,
    DB_ERROR_TXN_NOT_FOUND    = -7,
    DB_ERROR_TXN_CONFLICT     = -8,
    DB_ERROR_WAL_FULL         = -9,
    DB_ERROR_SNAPSHOT_EXPIRED  = -10,
    DB_ERROR_READ_ONLY        = -11,
    DB_ERROR_INTERNAL         = -12,
} db_error_t;

/** Convert page_result_t to db_error_t (values -1..-6 are identical) */
#define DB_ERROR_FROM_PAGE(pr) ((pr) >= 0 ? DB_OK : (db_error_t)(pr))

// ============================================================================
// Error Trace Stack
// ============================================================================

#define DB_ERROR_TRACE_MAX   8    // max frames per trace
#define DB_ERROR_MSG_SIZE    256  // message buffer per frame

/** Single frame in the error trace */
typedef struct {
    db_error_t   code;
    const char  *file;           // __FILE__ (static, not copied)
    const char  *func;           // __func__ (static, not copied)
    int          line;
    char         msg[DB_ERROR_MSG_SIZE];
} db_error_frame_t;

/**
 * Push an error frame onto the thread-local trace stack.
 * Called via the DB_ERROR() macro — do not call directly.
 */
void db_error_push(db_error_t code, const char *file, int line,
                   const char *func, const char *fmt, ...)
    __attribute__((format(printf, 5, 6)));

/**
 * DB_ERROR(code, fmt, ...) — primary error reporting macro.
 *
 * Pushes a frame with file, line, function and formatted message.
 * Use at every return-false site:
 *
 *   DB_ERROR(DB_ERROR_IO, "write failed: %s", strerror(errno));
 *   return false;
 */
#define DB_ERROR(code, ...) \
    db_error_push((code), __FILE__, __LINE__, __func__, __VA_ARGS__)

// ============================================================================
// Query API
// ============================================================================

/** Get error code from the most recent (outermost) frame, or DB_OK */
db_error_t db_get_last_error(void);

/** Get message from the most recent (outermost) frame, or "" */
const char *db_get_last_error_msg(void);

/** Get number of frames in the trace */
int db_error_trace_depth(void);

/** Get frame at index (0 = root cause, depth-1 = outermost). NULL if out of range */
const db_error_frame_t *db_error_trace_get(int index);

/** Get static human-readable string for an error code */
const char *db_error_string(db_error_t code);

/**
 * Print the full error trace to a FILE stream.
 *
 * Output format:
 *   error: out of memory (2 frames)
 *     [0] mvcc_begin_txn (mvcc.c:469) — failed to allocate txn_info_t
 *     [1] data_art_insert (data_art_insert.c:925) — failed to begin auto-commit txn
 *
 * Frame [0] is the root cause, last frame is the outermost caller.
 */
void db_error_trace_print(FILE *out);

// ============================================================================
// Lifecycle
// ============================================================================

/** Reset the error trace (clear all frames) */
void db_clear_error(void);

// ============================================================================
// Backward compatibility
// ============================================================================

/** Set error code (clears trace, pushes single frame with no source info) */
void db_set_last_error(db_error_t code);

/** Set error code with message (clears trace, pushes single frame with no source info) */
void db_set_last_error_msg(db_error_t code, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

#endif /* DB_ERROR_H */
