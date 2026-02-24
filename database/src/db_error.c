#include "db_error.h"
#include <string.h>
#include <stdarg.h>

// Thread-local error trace stack
static __thread db_error_frame_t tls_frames[DB_ERROR_TRACE_MAX];
static __thread int              tls_depth = 0;

void db_error_push(db_error_t code, const char *file, int line,
                   const char *func, const char *fmt, ...) {
    if (tls_depth < DB_ERROR_TRACE_MAX) {
        db_error_frame_t *f = &tls_frames[tls_depth];
        f->code = code;
        f->file = file;
        f->func = func;
        f->line = line;

        va_list ap;
        va_start(ap, fmt);
        vsnprintf(f->msg, DB_ERROR_MSG_SIZE, fmt, ap);
        va_end(ap);
        f->msg[DB_ERROR_MSG_SIZE - 1] = '\0';

        tls_depth++;
    }
    // If stack is full, silently drop (root cause is already captured)
}

// ============================================================================
// Query API
// ============================================================================

db_error_t db_get_last_error(void) {
    if (tls_depth == 0) return DB_OK;
    return tls_frames[tls_depth - 1].code;
}

const char *db_get_last_error_msg(void) {
    if (tls_depth == 0) return "";
    return tls_frames[tls_depth - 1].msg;
}

int db_error_trace_depth(void) {
    return tls_depth;
}

const db_error_frame_t *db_error_trace_get(int index) {
    if (index < 0 || index >= tls_depth) return NULL;
    return &tls_frames[index];
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

void db_error_trace_print(FILE *out) {
    if (tls_depth == 0) {
        fprintf(out, "(no error)\n");
        return;
    }

    // Outermost frame determines the headline
    const db_error_frame_t *top = &tls_frames[tls_depth - 1];
    fprintf(out, "error: %s (%d frame%s)\n",
            db_error_string(top->code), tls_depth,
            tls_depth == 1 ? "" : "s");

    for (int i = 0; i < tls_depth; i++) {
        const db_error_frame_t *f = &tls_frames[i];
        fprintf(out, "  [%d] %s (%s:%d)", i, f->func, f->file, f->line);
        if (f->msg[0] != '\0') {
            fprintf(out, " — %s", f->msg);
        }
        fprintf(out, "\n");
    }
}

// ============================================================================
// Lifecycle
// ============================================================================

void db_clear_error(void) {
    tls_depth = 0;
}

// ============================================================================
// Backward compatibility
// ============================================================================

void db_set_last_error(db_error_t code) {
    tls_depth = 0;

    db_error_frame_t *f = &tls_frames[0];
    f->code = code;
    f->file = NULL;
    f->func = NULL;
    f->line = 0;
    f->msg[0] = '\0';
    tls_depth = 1;
}

void db_set_last_error_msg(db_error_t code, const char *fmt, ...) {
    tls_depth = 0;

    if (tls_depth < DB_ERROR_TRACE_MAX) {
        db_error_frame_t *f = &tls_frames[0];
        f->code = code;
        f->file = NULL;
        f->func = NULL;
        f->line = 0;

        va_list ap;
        va_start(ap, fmt);
        vsnprintf(f->msg, DB_ERROR_MSG_SIZE, fmt, ap);
        va_end(ap);
        f->msg[DB_ERROR_MSG_SIZE - 1] = '\0';

        tls_depth = 1;
    }
}
