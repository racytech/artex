#include "db_error.h"
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#define DB_ERROR_MSG_BUFSIZE 512

static __thread db_error_t tls_last_error = DB_OK;
static __thread char       tls_last_msg[DB_ERROR_MSG_BUFSIZE] = {0};

void db_set_last_error(db_error_t code) {
    tls_last_error = code;
    tls_last_msg[0] = '\0';
}

void db_set_last_error_msg(db_error_t code, const char *fmt, ...) {
    tls_last_error = code;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(tls_last_msg, DB_ERROR_MSG_BUFSIZE, fmt, ap);
    va_end(ap);
    tls_last_msg[DB_ERROR_MSG_BUFSIZE - 1] = '\0';
}

db_error_t db_get_last_error(void) {
    return tls_last_error;
}

const char *db_get_last_error_msg(void) {
    return tls_last_msg;
}

void db_clear_error(void) {
    tls_last_error = DB_OK;
    tls_last_msg[0] = '\0';
}

const char *db_error_string(db_error_t code) {
    switch (code) {
        case DB_OK:                    return "OK";
        case DB_ERROR_NOT_FOUND:       return "not found";
        case DB_ERROR_IO:              return "I/O error";
        case DB_ERROR_DISK_FULL:       return "disk full";
        case DB_ERROR_CORRUPTION:      return "data corruption";
        case DB_ERROR_INVALID_ARG:     return "invalid argument";
        case DB_ERROR_OUT_OF_MEMORY:   return "out of memory";
        case DB_ERROR_TXN_NOT_FOUND:   return "transaction not found";
        case DB_ERROR_TXN_CONFLICT:    return "transaction conflict";
        case DB_ERROR_WAL_FULL:        return "WAL full";
        case DB_ERROR_SNAPSHOT_EXPIRED: return "snapshot expired";
        case DB_ERROR_READ_ONLY:       return "read-only";
        case DB_ERROR_INTERNAL:        return "internal error";
        default:                       return "unknown error";
    }
}
