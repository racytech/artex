#ifndef DB_ERROR_H
#define DB_ERROR_H

#include <stdint.h>

/**
 * Unified error codes for the database
 *
 * Values -1 through -6 intentionally match page_result_t so that
 * DB_ERROR_FROM_PAGE() is a trivial cast.
 *
 * Thread-local: each thread maintains its own last-error state.
 * Set via db_set_last_error() or db_set_last_error_msg() before
 * returning false/NULL from any public API function.
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

/** Set thread-local error code (clears the message buffer) */
void db_set_last_error(db_error_t code);

/** Set thread-local error code with a formatted message */
void db_set_last_error_msg(db_error_t code, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

/** Get thread-local error code (DB_OK if no error) */
db_error_t db_get_last_error(void);

/** Get thread-local error message (empty string if none) */
const char *db_get_last_error_msg(void);

/** Get static human-readable string for an error code */
const char *db_error_string(db_error_t code);

/** Reset thread-local error to DB_OK and clear message */
void db_clear_error(void);

#endif /* DB_ERROR_H */
