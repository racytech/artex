/*
 * Minimal logging implementation
 */

#include "logger.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Global State
 * ============================================================================ */

static struct {
    log_level_t min_level;
    FILE       *output;
    log_sink_fn sink;
} g_logger = {
    .min_level = LOG_LEVEL_INFO,
    .output    = NULL,  /* NULL means stderr */
    .sink      = NULL,
};

/* ============================================================================
 * String Tables
 * ============================================================================ */

static const char *LEVEL_STRINGS[] = {
    "TRACE",
    "DEBUG",
    "INFO ",
    "WARN ",
    "ERROR",
    "FATAL"
};

static const char *LEVEL_COLORS[] = {
    "\x1b[37m",  /* TRACE - white */
    "\x1b[36m",  /* DEBUG - cyan */
    "\x1b[32m",  /* INFO  - green */
    "\x1b[33m",  /* WARN  - yellow */
    "\x1b[31m",  /* ERROR - red */
    "\x1b[35m"   /* FATAL - magenta */
};

static const char *COMPONENT_STRINGS[] = {
    "db",
    "state",
    "evm",
    "common",
    "sync",
    "history"
};

static const char *COLOR_RESET = "\x1b[0m";

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

static const char *extract_filename(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

static int supports_color(FILE *stream) {
    if (stream == stderr)
        return isatty(fileno(stderr));
    if (stream == stdout)
        return isatty(fileno(stdout));
    return 0;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

void log_init(log_level_t min_level, FILE *output) {
    g_logger.min_level = min_level;
    g_logger.output = output;
}

void log_set_level(log_level_t level) {
    g_logger.min_level = level;
}

log_level_t log_get_level(void) {
    return g_logger.min_level;
}

void log_set_sink(log_sink_fn sink) {
    g_logger.sink = sink;
}

void log_write(
    log_level_t level,
    log_component_t component,
    const char *file,
    int line,
    const char *fmt,
    ...
) {
    if (level < g_logger.min_level)
        return;

    /* Format the user message */
    char msg[512];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);

    /* If a sink is registered, send there and return */
    if (g_logger.sink) {
        g_logger.sink(level, component, msg);
        return;
    }

    /* Default: write to file/stderr */
    FILE *out = g_logger.output ? g_logger.output : stderr;
    int use_color = supports_color(out);

    if (use_color) {
        fprintf(out, "%s%s%s [%s] %s\n",
                LEVEL_COLORS[level], LEVEL_STRINGS[level], COLOR_RESET,
                COMPONENT_STRINGS[component], msg);
    } else {
        fprintf(out, "%s [%s] %s\n",
                LEVEL_STRINGS[level], COMPONENT_STRINGS[component], msg);
    }
    fflush(out);
}
