#ifndef TUI_H
#define TUI_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ============================================================================
 * Stats Panel Data
 * ============================================================================ */

typedef struct {
    /* Block progress */
    uint64_t block_number;
    uint64_t target_block;

    /* Throughput (current window) */
    double   blocks_per_sec;
    double   tps;
    double   mgas_per_sec;
    double   window_secs;
    uint64_t window_txs;
    uint64_t window_transfers;
    uint64_t window_calls;

    /* Checkpoint */
    uint32_t checkpoint_interval;

    /* MPT timing */
    double   root_stor_ms;
    double   root_acct_ms;
    size_t   root_dirty_count;

    /* Flush timing */
    double   flush_ms;
    double   checkpoint_total_ms;

    /* Cache stats */
    size_t   cache_accounts;
    size_t   cache_slots;
    size_t   cache_arena_mb;

    /* Flat state */
    uint64_t flat_acct_count;
    uint64_t flat_stor_count;

    /* Code store */
    uint64_t code_count;
    double   code_cache_hit_pct;

    /* History */
    uint64_t history_blocks;
    double   history_mb;

    /* Memory */
    size_t   rss_mb;

    /* Cumulative */
    uint64_t total_blocks_ok;
    uint64_t total_blocks_fail;
    double   elapsed_secs;
} tui_stats_t;

/* ============================================================================
 * Log Severity
 * ============================================================================ */

typedef enum {
    TUI_LOG_INFO,
    TUI_LOG_WARN,
    TUI_LOG_ERROR,
} tui_log_level_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/** Initialize ncurses TUI. Returns false if not a terminal. */
bool tui_init(void);

/** Set build info string shown in the top bar (call after tui_init). */
void tui_set_build_info(const char *info);

/** Shut down TUI, restore terminal. */
void tui_shutdown(void);

/* ============================================================================
 * Update
 * ============================================================================ */

/** Update stats panel. */
void tui_update_stats(const tui_stats_t *stats);

/** Append a log message (printf-style). */
void tui_log(tui_log_level_t level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

/**
 * Process input and refresh. Non-blocking.
 * Returns false if user pressed 'q' to quit (only works after tui_set_finished).
 */
bool tui_tick(void);

/** Mark process as finished — enables 'q' to quit and shows prompt. */
void tui_set_finished(void);

#endif /* TUI_H */
