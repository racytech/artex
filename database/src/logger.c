/*
 * Minimal logging implementation for ART database
 */

#include "logger.h"
#include <stdarg.h>
#include <time.h>
#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Global State
 * ============================================================================ */

static struct {
    log_level_t min_level;
    FILE* output;
} g_logger = {
    .min_level = LOG_LEVEL_INFO,
    .output = NULL  /* NULL means stderr */
};

/* ============================================================================
 * String Tables
 * ============================================================================ */

static const char* LEVEL_STRINGS[] = {
    "TRACE",
    "DEBUG",
    "INFO ",
    "WARN ",
    "ERROR",
    "CRIT "
};

static const char* LEVEL_COLORS[] = {
    "\x1b[37m",  /* TRACE - white */
    "\x1b[36m",  /* DEBUG - cyan */
    "\x1b[32m",  /* INFO  - green */
    "\x1b[33m",  /* WARN  - yellow */
    "\x1b[31m",  /* ERROR - red */
    "\x1b[35m"   /* CRITICAL - magenta */
};

static const char* COLOR_RESET = "\x1b[0m";

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

/* Extract filename from full path */
static const char* extract_filename(const char* path) {
    const char* slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

/* Check if output supports colors (simple tty check) */
static int supports_color(FILE* stream) {
    if (stream == stderr) {
        return isatty(fileno(stderr));
    } else if (stream == stdout) {
        return isatty(fileno(stdout));
    }
    return 0;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

void log_init(log_level_t min_level, FILE* output) {
    g_logger.min_level = min_level;
    g_logger.output = output;
}

void log_set_level(log_level_t level) {
    g_logger.min_level = level;
}

log_level_t log_get_level(void) {
    return g_logger.min_level;
}

void log_write(
    log_level_t level,
    const char* file,
    int line,
    const char* fmt,
    ...
) {
    /* Check if we should log this level */
    if (level < g_logger.min_level) {
        return;
    }
    
    /* Determine output stream */
    FILE* out = g_logger.output ? g_logger.output : stderr;
    
    /* Check for color support */
    int use_color = supports_color(out);
    
    /* Get timestamp */
    time_t now = time(NULL);
    struct tm* tm_info = localtime(&now);
    char time_buf[32];
    strftime(time_buf, sizeof(time_buf), "%H:%M:%S", tm_info);
    
    /* Print log prefix with color */
    if (use_color) {
        fprintf(out, "%s[%s] %s%s%s [%s:%d] ",
            LEVEL_COLORS[level],
            time_buf,
            LEVEL_COLORS[level],
            LEVEL_STRINGS[level],
            COLOR_RESET,
            extract_filename(file),
            line);
    } else {
        fprintf(out, "[%s] %s [%s:%d] ",
            time_buf,
            LEVEL_STRINGS[level],
            extract_filename(file),
            line);
    }
    
    /* Print message */
    va_list args;
    va_start(args, fmt);
    vfprintf(out, fmt, args);
    va_end(args);
    
    fprintf(out, "\n");
    fflush(out);
}
