/*
 * Minimal logging library for ART execution engine
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
    LOG_LEVEL_FATAL
} log_level_t;

/* Component tags */
typedef enum {
    LOG_COMPONENT_DATABASE = 0,
    LOG_COMPONENT_STATE,
    LOG_COMPONENT_EVM,
    LOG_COMPONENT_COMMON
} log_component_t;

/* Initialize logger (optional, uses defaults if not called) */
void log_init(log_level_t min_level, FILE* output);

/* Set minimum log level */
void log_set_level(log_level_t level);

/* Get current log level */
log_level_t log_get_level(void);

/* Core logging function (use macros instead) */
void log_write(
    log_level_t level,
    log_component_t component,
    const char* file,
    int line,
    const char* fmt,
    ...
) __attribute__((format(printf, 5, 6)));

/* ============================================================================
 * Convenience Macros
 * ============================================================================ */

/* Database logging */
#define LOG_DB_TRACE(...) log_write(LOG_LEVEL_TRACE, LOG_COMPONENT_DATABASE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_DB_DEBUG(...) log_write(LOG_LEVEL_DEBUG, LOG_COMPONENT_DATABASE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_DB_INFO(...)  log_write(LOG_LEVEL_INFO,  LOG_COMPONENT_DATABASE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_DB_WARN(...)  log_write(LOG_LEVEL_WARN,  LOG_COMPONENT_DATABASE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_DB_ERROR(...) log_write(LOG_LEVEL_ERROR, LOG_COMPONENT_DATABASE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_DB_FATAL(...) log_write(LOG_LEVEL_FATAL, LOG_COMPONENT_DATABASE, __FILE__, __LINE__, __VA_ARGS__)

/* State logging */
#define LOG_STATE_TRACE(...) log_write(LOG_LEVEL_TRACE, LOG_COMPONENT_STATE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_STATE_DEBUG(...) log_write(LOG_LEVEL_DEBUG, LOG_COMPONENT_STATE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_STATE_INFO(...)  log_write(LOG_LEVEL_INFO,  LOG_COMPONENT_STATE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_STATE_WARN(...)  log_write(LOG_LEVEL_WARN,  LOG_COMPONENT_STATE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_STATE_ERROR(...) log_write(LOG_LEVEL_ERROR, LOG_COMPONENT_STATE, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_STATE_FATAL(...) log_write(LOG_LEVEL_FATAL, LOG_COMPONENT_STATE, __FILE__, __LINE__, __VA_ARGS__)

/* EVM logging */
#define LOG_EVM_TRACE(...) log_write(LOG_LEVEL_TRACE, LOG_COMPONENT_EVM, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_EVM_DEBUG(...) log_write(LOG_LEVEL_DEBUG, LOG_COMPONENT_EVM, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_EVM_INFO(...)  log_write(LOG_LEVEL_INFO,  LOG_COMPONENT_EVM, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_EVM_WARN(...)  log_write(LOG_LEVEL_WARN,  LOG_COMPONENT_EVM, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_EVM_ERROR(...) log_write(LOG_LEVEL_ERROR, LOG_COMPONENT_EVM, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_EVM_FATAL(...) log_write(LOG_LEVEL_FATAL, LOG_COMPONENT_EVM, __FILE__, __LINE__, __VA_ARGS__)

/* Common logging */
#define LOG_TRACE(...) log_write(LOG_LEVEL_TRACE, LOG_COMPONENT_COMMON, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_DEBUG(...) log_write(LOG_LEVEL_DEBUG, LOG_COMPONENT_COMMON, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  log_write(LOG_LEVEL_INFO,  LOG_COMPONENT_COMMON, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  log_write(LOG_LEVEL_WARN,  LOG_COMPONENT_COMMON, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) log_write(LOG_LEVEL_ERROR, LOG_COMPONENT_COMMON, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_FATAL(...) log_write(LOG_LEVEL_FATAL, LOG_COMPONENT_COMMON, __FILE__, __LINE__, __VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif /* ART_LOGGER_H */
