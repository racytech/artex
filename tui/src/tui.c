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
#define STATS_HEIGHT    (4 + STATS_HISTORY + 5) /* build bar + hline + progress + hline + header + N rows + hline + status + hline */
#define TAB_BAR_HEIGHT  1
#define LOG_RING_SIZE   4096
#define LOG_MSG_LEN     512

/* Tabs */
typedef enum {
    TAB_LOG = 0,
    TAB_DETAIL,
    TAB_COUNT,
} tui_tab_t;

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
#define CP_BUILD_BAR   9   /* black on blue */

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
    bool detail_dirty;      /* true when detail tab needs redraw */

    tui_tab_t active_tab;
    WINDOW *tab_win;        /* tab bar window (1 row) */

    int stats_h;            /* actual stats height (clamped to LINES) */

    struct termios saved_termios;   /* terminal state before ncurses */
    bool termios_saved;

    char build_info[256];          /* build feature string for top bar */
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

    /* Row 0: Build info bar (black on blue background) */
    if (g.build_info[0]) {
        wattron(g.stats_win, A_BOLD | COLOR_PAIR(CP_BUILD_BAR));
        mvwhline(g.stats_win, row, 0, ' ', w);
        mvwprintw(g.stats_win, row, 1, " %s", g.build_info);
        wattroff(g.stats_win, A_BOLD | COLOR_PAIR(CP_BUILD_BAR));
    }
    row++;

    /* Separator after build bar */
    if (row >= h) goto done;
    wattron(g.stats_win, A_DIM);
    mvwhline(g.stats_win, row, 0, ACS_HLINE, w);
    wattroff(g.stats_win, A_DIM);
    row++;

    /* Row 1: Block progress bar */
    if (row >= h) goto done;
    {
        double pct = s->target_block > 0
            ? 100.0 * s->block_number / s->target_block : 0;
        int bar_start = 28;
        int bar_max = w - bar_start - 10;
        if (bar_max < 10) bar_max = 10;
        int filled = (int)(pct / 100.0 * bar_max);
        if (filled > bar_max) filled = bar_max;

        wattron(g.stats_win, A_BOLD | COLOR_PAIR(CP_HIGHLIGHT));
        mvwprintw(g.stats_win, row, 1, "%9lu", s->block_number);
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
        int cp = s->total_blocks_fail > 0 ? CP_STATUS_FAIL : CP_STATUS_OK;
        wattron(g.stats_win, A_BOLD | COLOR_PAIR(cp));

        /* Fill entire row with background color */
        mvwhline(g.stats_win, row, 0, ' ', w);

        int cx = 1;
        mvwprintw(g.stats_win, row, cx,
                  " %3dh %02dm %02ds   DISK: %5.1f/%5.1fGB   RSS: %5zuMB   BLK/S: %6.0f   %10lu OK   %10lu FAIL",
                  hr, mn, sec, s->acct_mpt_gb, s->stor_mpt_gb, s->rss_mb,
                  s->blocks_per_sec, s->total_blocks_ok, s->total_blocks_fail);
        if (s->history_blocks > 0) {
            int cur_x = getcurx(g.stats_win);
            if (cur_x + 16 < w)
                wprintw(g.stats_win, "   HIST: %5.1fMB", s->history_mb);
        }

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

static void draw_tab_bar(void) {
    if (!g.tab_win) return;
    int w = getmaxx(g.tab_win);

    werase(g.tab_win);

    static const char *tab_names[] = { "LOG", "STATS" };
    int cx = 1;

    for (int i = 0; i < TAB_COUNT; i++) {
        bool active = (i == (int)g.active_tab);
        if (active)
            wattron(g.tab_win, A_BOLD | A_REVERSE);
        else
            wattron(g.tab_win, A_DIM);

        mvwprintw(g.tab_win, 0, cx, " %d:%s ", i + 1, tab_names[i]);

        if (active)
            wattroff(g.tab_win, A_BOLD | A_REVERSE);
        else
            wattroff(g.tab_win, A_DIM);

        cx += (int)strlen(tab_names[i]) + 5;
    }

    /* Fill rest with dim line */
    wattron(g.tab_win, A_DIM);
    for (int x = cx; x < w; x++)
        mvwaddch(g.tab_win, 0, x, ACS_HLINE);
    wattroff(g.tab_win, A_DIM);

    wrefresh(g.tab_win);
}

static void draw_detail(void) {
    if (!g.log_win) return;

    werase(g.log_win);
    int h = getmaxy(g.log_win);
    int w = getmaxx(g.log_win);
    if (h <= 0 || w <= 0) return;

    const tui_stats_t *s = &g.stats;
    int row = 0;
    int col1 = 2;   /* label column */
    int col2 = 24;  /* value column */
    int col3 = 42;  /* second label */
    int col4 = 64;  /* second value */

    /* Section: MPT Roots */
    wattron(g.log_win, A_BOLD);
    mvwprintw(g.log_win, row, col1, "MPT Roots");
    wattroff(g.log_win, A_BOLD);
    row++;

    if (row < h) { mvwprintw(g.log_win, row, col1, "acct root:");
                    mvwprintw(g.log_win, row, col2, "%8.1f ms", s->root_acct_ms);
                    mvwprintw(g.log_win, row, col3, "stor root:");
                    mvwprintw(g.log_win, row, col4, "%8.1f ms", s->root_stor_ms); row++; }
    if (row < h) { mvwprintw(g.log_win, row, col1, "dirty count:");
                    mvwprintw(g.log_win, row, col2, "%8zu", s->root_dirty_count); row++; }

    /* Section: Flush */
    row++;
    if (row < h) { wattron(g.log_win, A_BOLD);
                    mvwprintw(g.log_win, row, col1, "Flush / Checkpoint");
                    wattroff(g.log_win, A_BOLD); row++; }

    if (row < h) { mvwprintw(g.log_win, row, col1, "flush:");
                    mvwprintw(g.log_win, row, col2, "%8.1f ms", s->flush_ms); row++; }
    if (row < h) { mvwprintw(g.log_win, row, col1, "checkpoint:");
                    mvwprintw(g.log_win, row, col2, "%8.1f ms", s->checkpoint_total_ms); row++; }

    /* Section: Cache */
    row++;
    if (row < h) { wattron(g.log_win, A_BOLD);
                    mvwprintw(g.log_win, row, col1, "Cache");
                    wattroff(g.log_win, A_BOLD); row++; }

    if (row < h) { mvwprintw(g.log_win, row, col1, "accounts:");
                    mvwprintw(g.log_win, row, col2, "%8zu", s->cache_accounts);
                    mvwprintw(g.log_win, row, col3, "slots:");
                    mvwprintw(g.log_win, row, col4, "%8zu", s->cache_slots); row++; }
    if (row < h) { mvwprintw(g.log_win, row, col1, "arena:");
                    mvwprintw(g.log_win, row, col2, "%6zu MB", s->cache_arena_mb); row++; }

    /* Section: MPT Store */
    row++;
    if (row < h) { wattron(g.log_win, A_BOLD);
                    mvwprintw(g.log_win, row, col1, "MPT Store");
                    wattroff(g.log_win, A_BOLD); row++; }

    if (row < h) { mvwprintw(g.log_win, row, col1, "acct nodes:");
                    mvwprintw(g.log_win, row, col2, "%8lu", s->acct_mpt_nodes);
                    mvwprintw(g.log_win, row, col3, "stor nodes:");
                    mvwprintw(g.log_win, row, col4, "%8lu", s->stor_mpt_nodes); row++; }
    if (row < h) { mvwprintw(g.log_win, row, col1, "acct disk:");
                    mvwprintw(g.log_win, row, col2, "%7.2f GB", s->acct_mpt_gb);
                    mvwprintw(g.log_win, row, col3, "stor disk:");
                    mvwprintw(g.log_win, row, col4, "%7.2f GB", s->stor_mpt_gb); row++; }

    /* Section: Code Store */
    row++;
    if (row < h) { wattron(g.log_win, A_BOLD);
                    mvwprintw(g.log_win, row, col1, "Code Store");
                    wattroff(g.log_win, A_BOLD); row++; }

    if (row < h) { mvwprintw(g.log_win, row, col1, "contracts:");
                    mvwprintw(g.log_win, row, col2, "%8lu", s->code_count);
                    mvwprintw(g.log_win, row, col3, "cache hit:");
                    mvwprintw(g.log_win, row, col4, "%7.1f %%", s->code_cache_hit_pct); row++; }

    /* Section: History */
    if (s->history_blocks > 0) {
        row++;
        if (row < h) { wattron(g.log_win, A_BOLD);
                        mvwprintw(g.log_win, row, col1, "History");
                        wattroff(g.log_win, A_BOLD); row++; }

        if (row < h) { mvwprintw(g.log_win, row, col1, "blocks:");
                        mvwprintw(g.log_win, row, col2, "%8lu", s->history_blocks);
                        mvwprintw(g.log_win, row, col3, "disk:");
                        mvwprintw(g.log_win, row, col4, "%7.1f MB", s->history_mb); row++; }
    }

    wrefresh(g.log_win);
}

static void draw_content(void) {
    switch (g.active_tab) {
        case TAB_LOG:    draw_logs();   break;
        case TAB_DETAIL: draw_detail(); break;
        default: break;
    }
}

static void create_windows(void) {
    /* Stats window: full STATS_HEIGHT or all available rows (min 2 for border) */
    g.stats_h = LINES < STATS_HEIGHT ? LINES : STATS_HEIGHT;
    if (g.stats_h < 2) g.stats_h = 2;

    g.stats_win = newwin(g.stats_h, COLS, 0, 0);

    /* Tab bar: 1 row below stats */
    int tab_y = g.stats_h;
    if (tab_y < LINES) {
        g.tab_win = newwin(TAB_BAR_HEIGHT, COLS, tab_y, 0);
    } else {
        g.tab_win = NULL;
    }

    /* Content window: below tab bar */
    int content_y = g.stats_h + TAB_BAR_HEIGHT;
    int content_h = LINES - content_y;
    if (content_h > 0) {
        g.log_win = newwin(content_h, COLS, content_y, 0);
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
    if (g.tab_win)   { delwin(g.tab_win);   g.tab_win = NULL; }
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
    draw_tab_bar();
    draw_content();
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
        init_pair(CP_BUILD_BAR,   COLOR_BLACK, COLOR_BLUE);
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
    draw_tab_bar();
    draw_content();

    return true;
}

void tui_shutdown(void) {
    if (!g.initialized) return;
    log_set_sink(NULL);  /* restore default stderr logging */
    g.initialized = false;
    if (g.stats_win) delwin(g.stats_win);
    if (g.tab_win) delwin(g.tab_win);
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
    g.detail_dirty = true;
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

void tui_set_build_info(const char *info) {
    if (!info) return;
    snprintf(g.build_info, sizeof(g.build_info), "%s", info);
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
            case '1':
                if (g.active_tab != TAB_LOG) {
                    g.active_tab = TAB_LOG;
                    draw_tab_bar();
                    g.logs_dirty = true;
                }
                break;
            case '2':
                if (g.active_tab != TAB_DETAIL) {
                    g.active_tab = TAB_DETAIL;
                    draw_tab_bar();
                    g.detail_dirty = true;
                }
                break;
            case '\t':  /* Tab key cycles */
                g.active_tab = (g.active_tab + 1) % TAB_COUNT;
                draw_tab_bar();
                if (g.active_tab == TAB_LOG) g.logs_dirty = true;
                else g.detail_dirty = true;
                break;
            case KEY_RESIZE:
                /* Handled via SIGWINCH + g_needs_resize flag */
                break;
        }
    }

    /* Redraw content when dirty or scrolled */
    if (g.active_tab == TAB_LOG && (g.logs_dirty || scrolled)) {
        g.logs_dirty = false;
        draw_logs();
    } else if (g.active_tab == TAB_DETAIL && g.detail_dirty) {
        g.detail_dirty = false;
        draw_detail();
    }
    return true;
}
