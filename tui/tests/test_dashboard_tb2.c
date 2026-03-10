#define TB_IMPL
#include "termbox2.h"

#include <stdint.h>
#include <stdio.h>
#include <time.h>

/* =========================================================================
 * Box drawing helpers
 * ========================================================================= */

static void draw_box(int x, int y, int w, int h,
                     uintattr_t fg, uintattr_t bg) {
    /* Corners */
    tb_set_cell(x,         y,         0x250C, fg, bg); /* ┌ */
    tb_set_cell(x + w - 1, y,         0x2510, fg, bg); /* ┐ */
    tb_set_cell(x,         y + h - 1, 0x2514, fg, bg); /* └ */
    tb_set_cell(x + w - 1, y + h - 1, 0x2518, fg, bg); /* ┘ */

    /* Horizontal edges */
    for (int i = 1; i < w - 1; i++) {
        tb_set_cell(x + i, y,         0x2500, fg, bg); /* ─ */
        tb_set_cell(x + i, y + h - 1, 0x2500, fg, bg);
    }

    /* Vertical edges */
    for (int i = 1; i < h - 1; i++) {
        tb_set_cell(x,         y + i, 0x2502, fg, bg); /* │ */
        tb_set_cell(x + w - 1, y + i, 0x2502, fg, bg);
    }

    /* Fill interior */
    for (int row = 1; row < h - 1; row++)
        for (int col = 1; col < w - 1; col++)
            tb_set_cell(x + col, y + row, ' ', fg, bg);
}

static void draw_title(int x, int y, const char *title,
                       uintattr_t fg, uintattr_t bg) {
    /* Draw " Title " starting at x+2 on the top border row */
    tb_set_cell(x + 1, y, ' ', fg, bg);
    tb_printf(x + 2, y, fg | TB_BOLD, bg, "%s", title);
    int len = (int)strlen(title);
    tb_set_cell(x + 2 + len, y, ' ', fg, bg);
}

static void draw_field(int x, int y, int label_w, int value_w,
                       const char *label, const char *value,
                       uintattr_t label_fg, uintattr_t value_fg,
                       uintattr_t bg) {
    tb_printf(x, y, label_fg, bg, "%-*s", label_w, label);
    tb_printf(x + label_w, y, value_fg, bg, "%-*s", value_w, value);
}

/* =========================================================================
 * Section data
 * ========================================================================= */

#define MAX_FIELDS 8
#define MAX_LABEL  16
#define MAX_VALUE  32

typedef struct {
    char label[MAX_LABEL];
    char value[MAX_VALUE];
    uintattr_t color;
} field_t;

typedef struct {
    char    title[MAX_LABEL];
    field_t fields[MAX_FIELDS];
    int     count;
    int     width;
} section_t;

static int section_add_field(section_t *s, const char *label) {
    if (s->count >= MAX_FIELDS) return -1;
    int id = s->count++;
    snprintf(s->fields[id].label, MAX_LABEL, "%s:", label);
    s->fields[id].value[0] = '\0';
    s->fields[id].color = TB_WHITE;
    return id;
}

static void section_set(section_t *s, int fid, const char *fmt, ...) {
    if (fid < 0 || fid >= s->count) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(s->fields[fid].value, MAX_VALUE, fmt, ap);
    va_end(ap);
}

static void section_draw(const section_t *s, int x, int y) {
    int h = s->count + 2; /* top + bottom border */
    uintattr_t border_fg = TB_CYAN;
    uintattr_t bg = TB_DEFAULT;

    draw_box(x, y, s->width, h, border_fg, bg);
    draw_title(x, y, s->title, TB_WHITE, bg);

    int label_w = 0;
    for (int i = 0; i < s->count; i++) {
        int len = (int)strlen(s->fields[i].label);
        if (len > label_w) label_w = len;
    }
    label_w += 1; /* space after label */

    int value_w = s->width - 2 - label_w; /* -2 for side padding */
    if (value_w < 1) value_w = 1;

    for (int i = 0; i < s->count; i++) {
        draw_field(x + 1, y + 1 + i,
                   label_w, value_w,
                   s->fields[i].label, s->fields[i].value,
                   TB_WHITE | TB_DIM, s->fields[i].color, bg);
    }
}

/* =========================================================================
 * Number formatting
 * ========================================================================= */

static const char *fmt_num(uint64_t n, char *buf, int size) {
    if (n >= 1000000000ULL)
        snprintf(buf, size, "%.1fB", (double)n / 1e9);
    else if (n >= 1000000ULL)
        snprintf(buf, size, "%.1fM", (double)n / 1e6);
    else if (n >= 1000ULL)
        snprintf(buf, size, "%lu,%03lu", (unsigned long)(n / 1000),
                 (unsigned long)(n % 1000));
    else
        snprintf(buf, size, "%lu", (unsigned long)n);
    return buf;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    int rc = tb_init();
    if (rc != 0) {
        fprintf(stderr, "tb_init() failed: %d\n", rc);
        return 1;
    }
    tb_hide_cursor();

    /* Setup sections */
    section_t sec_sync  = { .title = "Block Sync",  .width = 28 };
    section_t sec_state = { .title = "State",        .width = 24 };
    section_t sec_last  = { .title = "Last Block",   .width = 28 };
    section_t sec_net   = { .title = "Network",      .width = 24 };

    int f_block = section_add_field(&sec_sync, "Block");
    int f_tps   = section_add_field(&sec_sync, "TPS");
    int f_gas   = section_add_field(&sec_sync, "Gas/s");
    int f_eta   = section_add_field(&sec_sync, "ETA");

    int f_accts = section_add_field(&sec_state, "Accounts");
    int f_stor  = section_add_field(&sec_state, "Storage");
    int f_disk  = section_add_field(&sec_state, "Disk");
    int f_ram   = section_add_field(&sec_state, "RAM");

    int f_txs   = section_add_field(&sec_last, "Txs");
    int f_bgas  = section_add_field(&sec_last, "Gas Used");
    int f_time  = section_add_field(&sec_last, "Exec Time");
    int f_root  = section_add_field(&sec_last, "State Root");

    int f_peers = section_add_field(&sec_net, "Peers");
    int f_recv  = section_add_field(&sec_net, "Recv");
    int f_sent  = section_add_field(&sec_net, "Sent");

    uint64_t target = 19500000, current = 19234000;
    uint64_t accounts = 312000000, storage = 1024000000;
    int frame = 0;
    char buf[64], buf2[64];

    for (;;) {
        /* Check for quit key */
        struct tb_event ev;
        if (tb_peek_event(&ev, 250) == TB_OK) {
            if (ev.type == TB_EVENT_KEY &&
                (ev.key == TB_KEY_ESC || ev.key == TB_KEY_CTRL_C ||
                 ev.ch == 'q'))
                break;
        }

        /* Update data */
        current += 47 + (frame % 20);
        accounts += 12 + (frame % 5);
        storage += 340 + (frame % 100);

        int tps = 1200 + (frame % 200) - 100;
        double gas_s = 42.5 + (frame % 10) * 0.5;
        int peers = 8 + (frame % 7);

        /* Sync */
        fmt_num(current, buf, sizeof(buf));
        fmt_num(target, buf2, sizeof(buf2));
        section_set(&sec_sync, f_block, "%s / %s", buf, buf2);
        section_set(&sec_sync, f_tps, "%d", tps);
        sec_sync.fields[f_tps].color = tps > 1200 ? TB_GREEN : TB_YELLOW;
        section_set(&sec_sync, f_gas, "%.1fM", gas_s);
        section_set(&sec_sync, f_eta, "~%dh %dm",
                    (int)((target - current) / ((uint64_t)tps * 3600)),
                    (int)(((target - current) / ((uint64_t)tps * 60)) % 60));

        /* State */
        fmt_num(accounts, buf, sizeof(buf));
        section_set(&sec_state, f_accts, "%s", buf);
        fmt_num(storage, buf, sizeof(buf));
        section_set(&sec_state, f_stor, "%s", buf);
        section_set(&sec_state, f_disk, "%.1f GB", 285.3 + frame * 0.01);
        section_set(&sec_state, f_ram, "%.1f GB", 2.1 + (frame % 5) * 0.1);

        /* Last Block */
        section_set(&sec_last, f_txs, "%d", 150 + (frame % 80));
        section_set(&sec_last, f_bgas, "%.1fM", 12.0 + (frame % 8) * 0.5);
        section_set(&sec_last, f_time, "%dms", 30 + (frame % 40));
        section_set(&sec_last, f_root, "0x%04x...%04x",
                    (unsigned)(current & 0xFFFF),
                    (unsigned)((current * 7) & 0xFFFF));

        /* Network */
        section_set(&sec_net, f_peers, "%d", peers);
        sec_net.fields[f_peers].color = peers > 10 ? TB_GREEN : TB_YELLOW;
        section_set(&sec_net, f_recv, "%.1f MB/s", 2.8 + (frame % 10) * 0.2);
        section_set(&sec_net, f_sent, "%.1f MB/s", 0.5 + (frame % 8) * 0.1);

        /* Draw */
        tb_clear();

        int col2_x = sec_sync.width + 1;
        section_draw(&sec_sync,  0,       0);
        section_draw(&sec_state, col2_x,  0);
        section_draw(&sec_last,  0,       6);
        section_draw(&sec_net,   col2_x,  6);

        /* Status bar */
        tb_printf(0, 12, TB_WHITE | TB_DIM, TB_DEFAULT,
                  "Press 'q' or ESC to quit");

        tb_present();
        frame++;
    }

    tb_shutdown();
    printf("termbox2 dashboard exited cleanly.\n");
    return 0;
}
