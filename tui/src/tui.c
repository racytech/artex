#include "tui.h"
#include "logger.h"
#include <ncurses.h>
#include <string.h>
#include <stdarg.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <termios.h>
#include <sys/ioctl.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

#define STATS_HISTORY   5   /* number of rolling stat rows */
#define STATS_HEIGHT    (4 + STATS_HISTORY + 4) /* pad + progress + hline + header + N rows + hline + status + hline */
#define LOG_RING_SIZE   4096
#define LOG_MSG_LEN     512

/* Color pairs */
#define CP_NORMAL    0
#define CP_INFO      1
#define CP_WARN      2
#define CP_ERROR     3
#define CP_HIGHLIGHT 4
#define CP_PROGRESS  5
#define CP_BORDER    6
#define CP_STATUS_OK   7   /* black on green */
#define CP_STATUS_FAIL 8   /* black on red */

/* ============================================================================
 * Log Ring Buffer
 * ============================================================================ */

typedef struct {
    tui_log_level_t level;
    char msg[LOG_MSG_LEN];
} log_entry_t;

/* Snapshot of one stats window row */
typedef struct {
    uint64_t block_number;
    double   window_secs;
    uint64_t window_txs;
    double   tps;
    double   mgas_per_sec;
    size_t   cache_arena_mb;
    double   root_total_ms;
} stats_snap_t;

/* ============================================================================
 * Global State
 * ============================================================================ */

static volatile sig_atomic_t g_needs_resize = 0;

static void sigwinch_handler(int sig) {
    (void)sig;
    g_needs_resize = 1;
}

static struct {
    WINDOW *stats_win;
    WINDOW *log_win;
    SCREEN *screen;         /* ncurses screen tied to /dev/tty */
    FILE   *tty_out;        /* /dev/tty for ncurses output */
    FILE   *tty_in;         /* /dev/tty for ncurses input */

    tui_stats_t stats;

    /* Rolling stats history */
    stats_snap_t history[STATS_HISTORY];
    size_t hist_head;       /* next write position */
    size_t hist_count;      /* total entries written */

    log_entry_t ring[LOG_RING_SIZE];
    size_t ring_head;       /* next write position */
    size_t ring_count;      /* total entries ever written */
    pthread_mutex_t ring_mutex;

    int scroll_offset;      /* 0 = auto-scroll, >0 = scrolled up */
    bool initialized;
    bool finished;          /* process done — 'q' enabled */
    bool logs_dirty;        /* true when logs need redraw */

    int stats_h;            /* actual stats height (clamped to LINES) */

    struct termios saved_termios;   /* terminal state before ncurses */
    bool termios_saved;
} g;

/* ============================================================================
 * Internal Helpers
 * ============================================================================ */

static void draw_stats(void) {
    const tui_stats_t *s = &g.stats;
    int h = g.stats_h;
    int w = getmaxx(g.stats_win);

    werase(g.stats_win);

    int row = 0;

    /* Row 0: Top padding */
    row++;

    /* Row 1: Block progress bar */
    if (row >= h) goto done;
    {
        double pct = s->target_block > 0
            ? 100.0 * s->block_number / s->target_block : 0;
        int bar_start = 36;
        int bar_max = w - bar_start - 10;
        if (bar_max < 10) bar_max = 10;
        int filled = (int)(pct / 100.0 * bar_max);
        if (filled > bar_max) filled = bar_max;

        mvwprintw(g.stats_win, row, 1, " block:");
        wattron(g.stats_win, A_BOLD | COLOR_PAIR(CP_HIGHLIGHT));
        wprintw(g.stats_win, " %10lu", s->block_number);
        wattroff(g.stats_win, A_BOLD | COLOR_PAIR(CP_HIGHLIGHT));
        wprintw(g.stats_win, " / %-10lu", s->target_block);

        mvwprintw(g.stats_win, row, bar_start, "[");
        wattron(g.stats_win, COLOR_PAIR(CP_PROGRESS));
        for (int i = 0; i < filled; i++) waddch(g.stats_win, '#');
        wattroff(g.stats_win, COLOR_PAIR(CP_PROGRESS));
        for (int i = filled; i < bar_max; i++) waddch(g.stats_win, ' ');
        wprintw(g.stats_win, "] %5.1f%%", pct);
    }
    row++;

    /* Separator */
    if (row >= h) goto done;
    wattron(g.stats_win, A_DIM);
    mvwhline(g.stats_win, row, 0, ACS_HLINE, w);
    wattroff(g.stats_win, A_DIM);
    row++;

    /* Column headers */
    if (row >= h) goto done;
    {
        int cx = 1;
        wattron(g.stats_win, A_DIM);
        mvwprintw(g.stats_win, row, cx, "%9s", "BLOCK");   cx += 10;
        mvwprintw(g.stats_win, row, cx, "%8s", "TIME");    cx += 9;
        mvwprintw(g.stats_win, row, cx, "%8s", "TXNS");    cx += 9;
        mvwprintw(g.stats_win, row, cx, "%8s", "TPS");     cx += 9;
        mvwprintw(g.stats_win, row, cx, "%8s", "MGas/s");  cx += 9;
        mvwprintw(g.stats_win, row, cx, "%8s", "CACHE");   cx += 9;
        mvwprintw(g.stats_win, row, cx, "%9s", "ROOT(ms)");
        wattroff(g.stats_win, A_DIM);
    }
    row++;

    /* Rolling history rows: oldest at top, newest at bottom */
    {
        /* How many rows to render */
        size_t n = g.hist_count < STATS_HISTORY ? g.hist_count : STATS_HISTORY;
        /* Start from empty rows if history is not full yet */
        int empty_rows = STATS_HISTORY - (int)n;
        for (int i = 0; i < empty_rows; i++) {
            if (row >= h) goto done;
            row++;
        }
        /* Draw history entries oldest first */
        for (size_t i = 0; i < n; i++) {
            if (row >= h) goto done;
            /* oldest entry is at (hist_head - n), newest at (hist_head - 1) */
            size_t idx = (g.hist_head - n + i) % STATS_HISTORY;
            stats_snap_t *snap = &g.history[idx];
            int cx = 1;
            /* Dim older rows, bold the newest */
            bool is_newest = (i == n - 1);
            int attr = is_newest ? A_BOLD : A_DIM;

            wattron(g.stats_win, attr);
            mvwprintw(g.stats_win, row, cx, "%9lu", snap->block_number); cx += 10;
            mvwprintw(g.stats_win, row, cx, "%7.1fs", snap->window_secs); cx += 9;
            mvwprintw(g.stats_win, row, cx, "%8lu", snap->window_txs); cx += 9;
            mvwprintw(g.stats_win, row, cx, "%8.0f", snap->tps); cx += 9;
            mvwprintw(g.stats_win, row, cx, "%8.1f", snap->mgas_per_sec); cx += 9;
            mvwprintw(g.stats_win, row, cx, "%6zuMB", snap->cache_arena_mb); cx += 9;
            mvwprintw(g.stats_win, row, cx, "%9.1f", snap->root_total_ms);
            wattroff(g.stats_win, attr);
            row++;
        }
    }

    /* Separator */
    if (row >= h) goto done;
    wattron(g.stats_win, A_DIM);
    mvwhline(g.stats_win, row, 0, ACS_HLINE, w);
    wattroff(g.stats_win, A_DIM);
    row++;

    /* Status bar (green bg on OK, red bg on fail) */
    if (row >= h) goto done;
    {
        int hr = (int)(s->elapsed_secs / 3600);
        int mn = (int)((s->elapsed_secs - hr * 3600) / 60);
        int sec = (int)(s->elapsed_secs) % 60;
        double disk_gb = s->acct_mpt_gb + s->stor_mpt_gb;

        int cp = s->total_blocks_fail > 0 ? CP_STATUS_FAIL : CP_STATUS_OK;
        wattron(g.stats_win, A_BOLD | COLOR_PAIR(cp));

        /* Fill entire row with background color */
        mvwhline(g.stats_win, row, 0, ' ', w);

        mvwprintw(g.stats_win, row, 1,
                  " %3dh %02dm %02ds   DISK: %5.1f/%5.1fGB   RSS: %5zuMB   BLK/S: %6.0f   %10lu OK   %10lu FAIL",
                  hr, mn, sec, s->acct_mpt_gb, s->stor_mpt_gb, s->rss_mb,
                  s->blocks_per_sec, s->total_blocks_ok, s->total_blocks_fail);

        wattroff(g.stats_win, A_BOLD | COLOR_PAIR(cp));
    }
    row++;

    /* Bottom separator */
    if (row >= h) goto done;
    wattron(g.stats_win, A_DIM);
    mvwhline(g.stats_win, row, 0, ACS_HLINE, w);
    wattroff(g.stats_win, A_DIM);

done:
    wrefresh(g.stats_win);
}

static void draw_logs(void) {
    if (!g.log_win) return;

    werase(g.log_win);

    int log_h = getmaxy(g.log_win);
    int log_w = getmaxx(g.log_win);
    if (log_h <= 0 || log_w <= 0) return;

    pthread_mutex_lock(&g.ring_mutex);

    size_t visible = g.ring_count < LOG_RING_SIZE ? g.ring_count : LOG_RING_SIZE;

    /* Number of lines we can display */
    size_t n_show = visible < (size_t)log_h ? visible : (size_t)log_h;

    /* Start index into ring: skip scroll_offset from the end */
    size_t end_idx = visible - (size_t)g.scroll_offset;
    if (g.scroll_offset >= (int)visible) end_idx = 0;
    size_t begin_idx = end_idx > n_show ? end_idx - n_show : 0;

    /* Render bottom-aligned: newest at bottom of panel */
    int first_row = log_h - (int)(end_idx - begin_idx);
    if (first_row < 0) first_row = 0;

    for (size_t i = begin_idx; i < end_idx; i++) {
        int row = first_row + (int)(i - begin_idx);
        if (row >= log_h) break;

        size_t ring_idx = (g.ring_head - visible + i) % LOG_RING_SIZE;
        log_entry_t *e = &g.ring[ring_idx];

        int cp;
        const char *prefix;
        switch (e->level) {
            case TUI_LOG_WARN:  cp = CP_WARN;  prefix = "WARN "; break;
            case TUI_LOG_ERROR: cp = CP_ERROR; prefix = "ERR  "; break;
            default:            cp = CP_INFO;  prefix = "INFO "; break;
        }

        wattron(g.log_win, COLOR_PAIR(cp));
        mvwprintw(g.log_win, row, 0, " %s", prefix);
        wattroff(g.log_win, COLOR_PAIR(cp));
        wprintw(g.log_win, "%-.*s", log_w - 7, e->msg);
    }

    pthread_mutex_unlock(&g.ring_mutex);

    /* Scroll indicator */
    if (g.scroll_offset > 0) {
        wattron(g.log_win, A_BOLD | COLOR_PAIR(CP_WARN));
        mvwprintw(g.log_win, log_h - 1, log_w - 20, " [SCROLLED +%d] ", g.scroll_offset);
        wattroff(g.log_win, A_BOLD | COLOR_PAIR(CP_WARN));
    }

    wrefresh(g.log_win);
}

static void create_windows(void) {
    /* Stats window: full STATS_HEIGHT or all available rows (min 2 for border) */
    g.stats_h = LINES < STATS_HEIGHT ? LINES : STATS_HEIGHT;
    if (g.stats_h < 2) g.stats_h = 2;

    g.stats_win = newwin(g.stats_h, COLS, 0, 0);

    /* Log window: only if there's space below stats */
    int log_h = LINES - g.stats_h;
    if (log_h > 0) {
        g.log_win = newwin(log_h, COLS, g.stats_h, 0);
    } else {
        g.log_win = NULL;
    }
}

static void handle_resize(void) {
    /* Get actual terminal size via the tty fd */
    struct winsize ws;
    int tty_fd = g.tty_out ? fileno(g.tty_out) : STDIN_FILENO;
    if (ioctl(tty_fd, TIOCGWINSZ, &ws) == -1)
        return;

    /* Destroy old windows */
    if (g.stats_win) { delwin(g.stats_win); g.stats_win = NULL; }
    if (g.log_win)   { delwin(g.log_win);   g.log_win = NULL; }

    /* End curses mode, resize, restart */
    endwin();
    resizeterm(ws.ws_row, ws.ws_col);
    refresh();

    /* Re-apply terminal settings that endwin() resets */
    cbreak();
    noecho();
    curs_set(0);
    nodelay(stdscr, TRUE);
    keypad(stdscr, TRUE);

    /* Flush stale input (KEY_RESIZE etc) */
    flushinp();

    /* Recreate windows and redraw */
    create_windows();
    draw_stats();
    draw_logs();
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/* Logger sink: routes log_write() calls into the TUI log panel */
static void tui_log_sink(log_level_t level, log_component_t component,
                          const char *msg) {
    (void)component;
    tui_log_level_t tl = TUI_LOG_INFO;
    if (level >= LOG_LEVEL_ERROR) tl = TUI_LOG_ERROR;
    else if (level >= LOG_LEVEL_WARN) tl = TUI_LOG_WARN;
    tui_log(tl, "%s", msg);
}

bool tui_init(void) {
    /* Check if we have a real terminal */
    if (!isatty(STDIN_FILENO))
        return false;

    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.ring_mutex, NULL);

    /* Save terminal state before ncurses touches it */
    if (tcgetattr(STDIN_FILENO, &g.saved_termios) == 0)
        g.termios_saved = true;

    /* Open /dev/tty directly so ncurses is independent of stdout/stderr redirects */
    g.tty_out = fopen("/dev/tty", "w");
    g.tty_in  = fopen("/dev/tty", "r");
    if (!g.tty_out || !g.tty_in) {
        if (g.tty_out) fclose(g.tty_out);
        if (g.tty_in)  fclose(g.tty_in);
        return false;
    }

    g.screen = newterm(NULL, g.tty_out, g.tty_in);
    if (!g.screen) {
        fclose(g.tty_out);
        fclose(g.tty_in);
        return false;
    }
    set_term(g.screen);

    cbreak();
    noecho();
    curs_set(0);
    nodelay(stdscr, TRUE);
    keypad(stdscr, TRUE);

    if (has_colors()) {
        start_color();
        use_default_colors();
        init_pair(CP_INFO,      COLOR_GREEN,  -1);
        init_pair(CP_WARN,      COLOR_YELLOW, -1);
        init_pair(CP_ERROR,     COLOR_RED,    -1);
        init_pair(CP_HIGHLIGHT, COLOR_CYAN,   -1);
        init_pair(CP_PROGRESS,    COLOR_GREEN,  -1);
        init_pair(CP_BORDER,      COLOR_CYAN,   -1);
        init_pair(CP_STATUS_OK,   COLOR_BLACK, COLOR_GREEN);
        init_pair(CP_STATUS_FAIL, COLOR_BLACK, COLOR_RED);
    }

    /* Install SIGWINCH handler for reliable resize detection.
     * SA_RESTART: don't interrupt blocking syscalls (read/write/etc) with EINTR */
    struct sigaction sa_winch;
    memset(&sa_winch, 0, sizeof(sa_winch));
    sa_winch.sa_handler = sigwinch_handler;
    sa_winch.sa_flags = SA_RESTART;
    sigaction(SIGWINCH, &sa_winch, NULL);

    create_windows();
    g.initialized = true;

    /* Redirect all log_write() calls into TUI panel */
    log_set_sink(tui_log_sink);

    draw_stats();
    draw_logs();

    return true;
}

void tui_shutdown(void) {
    if (!g.initialized) return;
    log_set_sink(NULL);  /* restore default stderr logging */
    g.initialized = false;
    if (g.stats_win) delwin(g.stats_win);
    if (g.log_win) delwin(g.log_win);
    endwin();

    if (g.screen) delscreen(g.screen);
    if (g.tty_out) fclose(g.tty_out);
    if (g.tty_in)  fclose(g.tty_in);

    /* Force-restore terminal state in case endwin() didn't fully clean up */
    if (g.termios_saved)
        tcsetattr(STDIN_FILENO, TCSANOW, &g.saved_termios);

    /* Restore default SIGWINCH handler */
    signal(SIGWINCH, SIG_DFL);

    pthread_mutex_destroy(&g.ring_mutex);
    g.initialized = false;
}

void tui_update_stats(const tui_stats_t *stats) {
    if (!g.initialized) return;
    if (g_needs_resize) {
        g_needs_resize = 0;
        handle_resize();
    }
    g.stats = *stats;

    /* Push snapshot into history ring (skip empty/initial updates) */
    if (stats->window_txs > 0 || stats->blocks_per_sec > 0) {
        double root_total = stats->root_stor_ms + stats->root_acct_ms;
        stats_snap_t *snap = &g.history[g.hist_head % STATS_HISTORY];
        snap->block_number  = stats->block_number;
        snap->window_secs   = stats->window_secs;
        snap->window_txs    = stats->window_txs;
        snap->tps           = stats->tps;
        snap->mgas_per_sec  = stats->mgas_per_sec;
        snap->cache_arena_mb = stats->cache_arena_mb;
        snap->root_total_ms = root_total;
        g.hist_head++;
        if (g.hist_count < STATS_HISTORY) g.hist_count++;
    }

    draw_stats();
}

void tui_log(tui_log_level_t level, const char *fmt, ...) {
    if (!g.initialized) return;

    pthread_mutex_lock(&g.ring_mutex);

    log_entry_t *e = &g.ring[g.ring_head % LOG_RING_SIZE];
    e->level = level;

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(e->msg, LOG_MSG_LEN, fmt, ap);
    va_end(ap);

    g.ring_head++;
    g.ring_count++;
    g.logs_dirty = true;

    pthread_mutex_unlock(&g.ring_mutex);
}

void tui_set_finished(void) {
    if (!g.initialized) return;
    g.finished = true;
    tui_log(TUI_LOG_WARN, "Process finished - press 'q' to exit");
}

bool tui_tick(void) {
    if (!g.initialized) return true;

    /* Check SIGWINCH flag — resize even if no KEY_RESIZE arrived */
    if (g_needs_resize) {
        g_needs_resize = 0;
        handle_resize();
    }

    bool scrolled = false;
    int ch;
    while ((ch = getch()) != ERR) {
        switch (ch) {
            case 'q': case 'Q':
                if (g.finished) return false;
                break;  /* ignore while running */
            case KEY_UP:
                g.scroll_offset++;
                scrolled = true;
                break;
            case KEY_DOWN:
                if (g.scroll_offset > 0) { g.scroll_offset--; scrolled = true; }
                break;
            case KEY_PPAGE:
                g.scroll_offset += 10;
                scrolled = true;
                break;
            case KEY_NPAGE:
                g.scroll_offset -= 10;
                if (g.scroll_offset < 0) g.scroll_offset = 0;
                scrolled = true;
                break;
            case KEY_END:
                g.scroll_offset = 0;
                scrolled = true;
                break;
            case KEY_HOME: {
                size_t visible = g.ring_count < LOG_RING_SIZE
                    ? g.ring_count : LOG_RING_SIZE;
                int log_h = g.log_win ? getmaxy(g.log_win) : 0;
                g.scroll_offset = (int)visible - log_h;
                if (g.scroll_offset < 0) g.scroll_offset = 0;
                scrolled = true;
                break;
            }
            case KEY_RESIZE:
                /* Handled via SIGWINCH + g_needs_resize flag */
                break;
        }
    }

    /* Only redraw logs when content changed or user scrolled */
    if (g.logs_dirty || scrolled) {
        g.logs_dirty = false;
        draw_logs();
    }
    return true;
}
