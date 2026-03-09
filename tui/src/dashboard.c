#include "dashboard.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>

/* =========================================================================
 * ANSI Escape Helpers
 * ========================================================================= */

#define ESC_CURSOR_HOME     "\033[H"
#define ESC_CLEAR_LINE      "\033[K"
#define ESC_HIDE_CURSOR     "\033[?25l"
#define ESC_SHOW_CURSOR     "\033[?25h"
#define ESC_ALT_SCREEN_ON   "\033[?1049h"
#define ESC_ALT_SCREEN_OFF  "\033[?1049l"
#define ESC_RESET           "\033[0m"
#define ESC_BOLD            "\033[1m"
#define ESC_DIM             "\033[2m"

/* Box drawing (UTF-8) */
#define BOX_TL  "┌"
#define BOX_TR  "┐"
#define BOX_BL  "└"
#define BOX_BR  "┘"
#define BOX_H   "─"
#define BOX_V   "│"

/* Minimum section inner width (excluding borders) */
#define MIN_INNER_WIDTH  16

/* Padding between label and value */
#define LABEL_PAD  2

/* Gap between side-by-side sections */
#define SECTION_GAP  1

/* =========================================================================
 * Internal: Terminal Size
 * ========================================================================= */

static void get_terminal_size(int *w, int *h) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0) {
        *w = ws.ws_col;
        *h = ws.ws_row;
    } else {
        *w = 80;
        *h = 24;
    }
}

/* =========================================================================
 * Internal: Compute section inner width
 * ========================================================================= */

static int compute_inner_width(const dashboard_section_t *sec) {
    int title_len = (int)strlen(sec->title) + 2;  /* "─ Title ─" */
    int max_field = 0;

    for (int i = 0; i < sec->field_count; i++) {
        if (!sec->fields[i].active) continue;
        int fw = (int)strlen(sec->fields[i].label) + LABEL_PAD +
                 (int)strlen(sec->fields[i].value);
        if (fw > max_field) max_field = fw;
    }

    int w = title_len > max_field ? title_len : max_field;
    if (w < MIN_INNER_WIDTH) w = MIN_INNER_WIDTH;

    /* Add 2 for left/right padding inside the box */
    return w + 2;
}

/* =========================================================================
 * Internal: Output helpers (write to buffer to reduce syscalls)
 * ========================================================================= */

/* Large render buffer — flushed once per render */
#define RENDER_BUF_SIZE  (64 * 1024)

typedef struct {
    char  buf[RENDER_BUF_SIZE];
    int   pos;
} render_ctx_t;

static void rb_flush(render_ctx_t *ctx) {
    if (ctx->pos > 0) {
        write(STDOUT_FILENO, ctx->buf, ctx->pos);
        ctx->pos = 0;
    }
}

static void rb_write(render_ctx_t *ctx, const char *s, int len) {
    if (ctx->pos + len > RENDER_BUF_SIZE)
        rb_flush(ctx);
    if (len > RENDER_BUF_SIZE) {
        write(STDOUT_FILENO, s, len);
        return;
    }
    memcpy(ctx->buf + ctx->pos, s, len);
    ctx->pos += len;
}

static void rb_puts(render_ctx_t *ctx, const char *s) {
    rb_write(ctx, s, (int)strlen(s));
}

static void rb_printf(render_ctx_t *ctx, const char *fmt, ...) {
    char tmp[256];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n > 0) rb_write(ctx, tmp, n);
}

static void rb_repeat(render_ctx_t *ctx, const char *s, int count) {
    for (int i = 0; i < count; i++)
        rb_puts(ctx, s);
}

static void rb_spaces(render_ctx_t *ctx, int count) {
    for (int i = 0; i < count; i++)
        rb_write(ctx, " ", 1);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

void dashboard_init(dashboard_t *db, int columns) {
    memset(db, 0, sizeof(*db));
    db->columns = columns > 0 ? columns : 1;
}

void dashboard_cleanup(dashboard_t *db) {
    (void)db;
}

/* =========================================================================
 * Terminal Control
 * ========================================================================= */

void dashboard_enter(void) {
    write(STDOUT_FILENO, ESC_ALT_SCREEN_ON ESC_HIDE_CURSOR,
          strlen(ESC_ALT_SCREEN_ON ESC_HIDE_CURSOR));
}

void dashboard_leave(void) {
    write(STDOUT_FILENO, ESC_SHOW_CURSOR ESC_ALT_SCREEN_OFF,
          strlen(ESC_SHOW_CURSOR ESC_ALT_SCREEN_OFF));
}

/* =========================================================================
 * Sections
 * ========================================================================= */

int dashboard_add_section(dashboard_t *db, const char *title) {
    if (db->section_count >= DASHBOARD_MAX_SECTIONS) return -1;
    int id = db->section_count++;
    dashboard_section_t *sec = &db->sections[id];
    memset(sec, 0, sizeof(*sec));
    snprintf(sec->title, DASHBOARD_MAX_TITLE, "%s", title);
    sec->active = true;
    return id;
}

void dashboard_remove_section(dashboard_t *db, int section_id) {
    if (section_id < 0 || section_id >= db->section_count) return;
    for (int i = section_id; i < db->section_count - 1; i++)
        db->sections[i] = db->sections[i + 1];
    db->section_count--;
}

void dashboard_set_section_width(dashboard_t *db, int section_id, int width) {
    if (section_id < 0 || section_id >= db->section_count) return;
    db->sections[section_id].width = width;
}

/* =========================================================================
 * Fields
 * ========================================================================= */

int dashboard_add_field(dashboard_t *db, int section_id, const char *label) {
    if (section_id < 0 || section_id >= db->section_count) return -1;
    dashboard_section_t *sec = &db->sections[section_id];
    if (sec->field_count >= DASHBOARD_MAX_FIELDS) return -1;
    int id = sec->field_count++;
    dashboard_field_t *f = &sec->fields[id];
    memset(f, 0, sizeof(*f));
    snprintf(f->label, DASHBOARD_MAX_LABEL, "%s", label);
    f->active = true;
    return id;
}

void dashboard_remove_field(dashboard_t *db, int section_id, int field_id) {
    if (section_id < 0 || section_id >= db->section_count) return;
    dashboard_section_t *sec = &db->sections[section_id];
    if (field_id < 0 || field_id >= sec->field_count) return;
    for (int i = field_id; i < sec->field_count - 1; i++)
        sec->fields[i] = sec->fields[i + 1];
    sec->field_count--;
}

/* =========================================================================
 * Values
 * ========================================================================= */

void dashboard_set(dashboard_t *db, int section_id, int field_id,
                   const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    dashboard_vset(db, section_id, field_id, fmt, ap);
    va_end(ap);
}

void dashboard_vset(dashboard_t *db, int section_id, int field_id,
                    const char *fmt, va_list ap) {
    if (section_id < 0 || section_id >= db->section_count) return;
    dashboard_section_t *sec = &db->sections[section_id];
    if (field_id < 0 || field_id >= sec->field_count) return;
    vsnprintf(sec->fields[field_id].value, DASHBOARD_MAX_VALUE, fmt, ap);
}

void dashboard_set_color(dashboard_t *db, int section_id, int field_id,
                         int ansi_color) {
    if (section_id < 0 || section_id >= db->section_count) return;
    dashboard_section_t *sec = &db->sections[section_id];
    if (field_id < 0 || field_id >= sec->field_count) return;
    sec->fields[field_id].color = ansi_color;
}

/* =========================================================================
 * Rendering
 * ========================================================================= */

/* Draw the top border of a section:  ┌─ Title ──────────┐ */
static void render_top_border(render_ctx_t *ctx, const dashboard_section_t *sec,
                              int inner_w) {
    int title_len = (int)strlen(sec->title);
    int dashes_after = inner_w - title_len - 2;  /* 2 = "─ " before title */
    if (dashes_after < 1) dashes_after = 1;

    rb_puts(ctx, BOX_TL);
    rb_puts(ctx, BOX_H);
    rb_puts(ctx, " ");
    rb_puts(ctx, ESC_BOLD);
    rb_puts(ctx, sec->title);
    rb_puts(ctx, ESC_RESET);
    rb_puts(ctx, " ");
    rb_repeat(ctx, BOX_H, dashes_after);
    rb_puts(ctx, BOX_TR);
}

/* Draw a field row:  │ Label:   value              │ */
static void render_field(render_ctx_t *ctx, const dashboard_field_t *f,
                         int inner_w, int max_label) {
    int label_len = (int)strlen(f->label);
    int value_len = (int)strlen(f->value);
    int pad = max_label - label_len + LABEL_PAD;
    int remaining = inner_w - max_label - LABEL_PAD - value_len;
    if (remaining < 0) remaining = 0;

    rb_puts(ctx, BOX_V);
    rb_write(ctx, " ", 1);
    rb_puts(ctx, ESC_DIM);
    rb_puts(ctx, f->label);
    rb_puts(ctx, ":");
    rb_puts(ctx, ESC_RESET);
    rb_spaces(ctx, pad);

    if (f->color) {
        rb_printf(ctx, "\033[%dm", f->color);
    }
    rb_puts(ctx, f->value);
    if (f->color) {
        rb_puts(ctx, ESC_RESET);
    }

    rb_spaces(ctx, remaining);
    rb_write(ctx, " ", 1);
    rb_puts(ctx, BOX_V);
}

/* Draw an empty field row (padding for shorter sections):  │                    │ */
static void render_empty_row(render_ctx_t *ctx, int inner_w) {
    rb_puts(ctx, BOX_V);
    rb_spaces(ctx, inner_w + 2);  /* +2 for left/right padding */
    rb_puts(ctx, BOX_V);
}

/* Draw the bottom border:  └──────────────────────┘ */
static void render_bottom_border(render_ctx_t *ctx, int inner_w) {
    rb_puts(ctx, BOX_BL);
    rb_repeat(ctx, BOX_H, inner_w + 2);  /* +2 for padding */
    rb_puts(ctx, BOX_BR);
}

/* Get maximum label length in a section */
static int section_max_label(const dashboard_section_t *sec) {
    int max = 0;
    for (int i = 0; i < sec->field_count; i++) {
        if (!sec->fields[i].active) continue;
        int len = (int)strlen(sec->fields[i].label);
        if (len > max) max = len;
    }
    return max;
}

/* Get max field count across a row of sections */
static int row_max_fields(const dashboard_t *db, int row_start, int row_end) {
    int max = 0;
    for (int i = row_start; i < row_end; i++) {
        if (db->sections[i].field_count > max)
            max = db->sections[i].field_count;
    }
    return max;
}

void dashboard_render(dashboard_t *db) {
    get_terminal_size(&db->term_w, &db->term_h);

    render_ctx_t ctx;
    ctx.pos = 0;

    /* Cursor home */
    rb_puts(&ctx, ESC_CURSOR_HOME);
    rb_puts(&ctx, ESC_HIDE_CURSOR);

    /* Compute inner widths for all sections */
    int widths[DASHBOARD_MAX_SECTIONS];
    int max_labels[DASHBOARD_MAX_SECTIONS];
    for (int i = 0; i < db->section_count; i++) {
        if (db->sections[i].width > 0)
            widths[i] = db->sections[i].width;
        else
            widths[i] = compute_inner_width(&db->sections[i]);
        max_labels[i] = section_max_label(&db->sections[i]);
    }

    int line = 0;

    /* Process sections in rows */
    for (int row_start = 0; row_start < db->section_count;
         row_start += db->columns) {
        int row_end = row_start + db->columns;
        if (row_end > db->section_count) row_end = db->section_count;
        int max_fields = row_max_fields(db, row_start, row_end);

        /* Top borders */
        for (int c = row_start; c < row_end; c++) {
            if (c > row_start) rb_spaces(&ctx, SECTION_GAP);
            render_top_border(&ctx, &db->sections[c], widths[c]);
        }
        rb_puts(&ctx, ESC_CLEAR_LINE "\n");
        line++;

        /* Field rows */
        for (int f = 0; f < max_fields; f++) {
            for (int c = row_start; c < row_end; c++) {
                if (c > row_start) rb_spaces(&ctx, SECTION_GAP);
                const dashboard_section_t *sec = &db->sections[c];
                if (f < sec->field_count && sec->fields[f].active) {
                    render_field(&ctx, &sec->fields[f],
                                 widths[c], max_labels[c]);
                } else {
                    render_empty_row(&ctx, widths[c]);
                }
            }
            rb_puts(&ctx, ESC_CLEAR_LINE "\n");
            line++;
        }

        /* Bottom borders */
        for (int c = row_start; c < row_end; c++) {
            if (c > row_start) rb_spaces(&ctx, SECTION_GAP);
            render_bottom_border(&ctx, widths[c]);
        }
        rb_puts(&ctx, ESC_CLEAR_LINE "\n");
        line++;
    }

    /* Clear remaining lines to remove stale content */
    for (int i = line; i < db->term_h; i++) {
        rb_puts(&ctx, ESC_CLEAR_LINE "\n");
    }

    rb_flush(&ctx);
}
