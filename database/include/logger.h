/*
 * Minimal logging library for ART database
 */

#ifndef ART_LOGGER_H
#define ART_LOGGER_H

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Log levels */
typedef enum {
    LOG_LEVEL_TRACE = 0,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARN,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_CRITICAL
} log_level_t;

/* Initialize logger (optional, uses defaults if not called) */
void log_init(log_level_t min_level, FILE* output);

/* Set minimum log level */
void log_set_level(log_level_t level);

/* Get current log level */
log_level_t log_get_level(void);

/* Core logging function (use macros instead) */
void log_write(
    log_level_t level,
    const char* file,
    int line,
    const char* fmt,
    ...
) __attribute__((format(printf, 4, 5)));

/* ============================================================================
 * Convenience Macros
 * ============================================================================ */

// In Release builds, compile out TRACE and DEBUG logging completely
#ifdef NDEBUG
    #define LOG_TRACE(...) ((void)0)
    #define LOG_DEBUG(...) ((void)0)
#else
    #define LOG_TRACE(...) log_write(LOG_LEVEL_TRACE, __FILE__, __LINE__, __VA_ARGS__)
    #define LOG_DEBUG(...) log_write(LOG_LEVEL_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#endif

#define LOG_INFO(...)  log_write(LOG_LEVEL_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  log_write(LOG_LEVEL_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) log_write(LOG_LEVEL_ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_CRITICAL(...) log_write(LOG_LEVEL_CRITICAL, __FILE__, __LINE__, __VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif /* ART_LOGGER_H */
