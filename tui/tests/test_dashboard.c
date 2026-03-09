#include "dashboard.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>

/* =========================================================================
 * Signal handler for clean exit
 * ========================================================================= */

static volatile int g_running = 1;

static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

/* =========================================================================
 * Fake data helpers
 * ========================================================================= */

static const char *format_number(uint64_t n, char *buf, int size) {
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
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    dashboard_t db;
    dashboard_init(&db, 2);

    /* --- Block Sync section --- */
    int sec_sync = dashboard_add_section(&db, "Block Sync");
    int f_block  = dashboard_add_field(&db, sec_sync, "Block");
    int f_tps    = dashboard_add_field(&db, sec_sync, "TPS");
    int f_gas    = dashboard_add_field(&db, sec_sync, "Gas/s");
    int f_eta    = dashboard_add_field(&db, sec_sync, "ETA");

    /* --- State section --- */
    int sec_state = dashboard_add_section(&db, "State");
    int f_accts   = dashboard_add_field(&db, sec_state, "Accounts");
    int f_storage = dashboard_add_field(&db, sec_state, "Storage");
    int f_disk    = dashboard_add_field(&db, sec_state, "Disk");
    int f_ram     = dashboard_add_field(&db, sec_state, "RAM");

    /* --- Last Block section --- */
    int sec_last  = dashboard_add_section(&db, "Last Block");
    int f_txs     = dashboard_add_field(&db, sec_last, "Txs");
    int f_bgas    = dashboard_add_field(&db, sec_last, "Gas Used");
    int f_time    = dashboard_add_field(&db, sec_last, "Exec Time");
    int f_root    = dashboard_add_field(&db, sec_last, "State Root");

    /* --- Network section --- */
    int sec_net   = dashboard_add_section(&db, "Network");
    int f_peers   = dashboard_add_field(&db, sec_net, "Peers");
    int f_recv    = dashboard_add_field(&db, sec_net, "Recv");
    int f_sent    = dashboard_add_field(&db, sec_net, "Sent");

    dashboard_enter();

    /* Simulate sync progress */
    uint64_t target_block = 19500000;
    uint64_t current_block = 19234000;
    uint64_t accounts = 312000000;
    uint64_t storage_slots = 1024000000;
    int frame = 0;
    char buf[64];

    struct timespec ts = { .tv_sec = 0, .tv_nsec = 250000000 }; /* 250ms */

    while (g_running) {
        current_block += 47 + (frame % 20);
        accounts += 12 + (frame % 5);
        storage_slots += 340 + (frame % 100);

        uint64_t remaining = target_block - current_block;
        int tps = 1200 + (frame % 200) - 100;
        double gas_per_s = 42.5 + (frame % 10) * 0.5;
        int eta_hours = (int)(remaining / (tps * 3600));
        int eta_mins = (int)((remaining / (tps * 60)) % 60);

        /* Update Block Sync */
        format_number(current_block, buf, sizeof(buf));
        char buf2[64];
        format_number(target_block, buf2, sizeof(buf2));
        dashboard_set(&db, sec_sync, f_block, "%s / %s", buf, buf2);

        dashboard_set(&db, sec_sync, f_tps, "%d", tps);
        dashboard_set_color(&db, sec_sync, f_tps, tps > 1200 ? 32 : 33);

        dashboard_set(&db, sec_sync, f_gas, "%.1fM", gas_per_s);
        dashboard_set(&db, sec_sync, f_eta, "~%dh %dm", eta_hours, eta_mins);

        /* Update State */
        format_number(accounts, buf, sizeof(buf));
        dashboard_set(&db, sec_state, f_accts, "%s", buf);
        format_number(storage_slots, buf, sizeof(buf));
        dashboard_set(&db, sec_state, f_storage, "%s", buf);
        dashboard_set(&db, sec_state, f_disk, "%.1f GB", 285.3 + frame * 0.01);
        dashboard_set(&db, sec_state, f_ram, "%.1f GB", 2.1 + (frame % 5) * 0.1);

        /* Update Last Block */
        int txs = 150 + (frame % 80);
        dashboard_set(&db, sec_last, f_txs, "%d", txs);
        dashboard_set(&db, sec_last, f_bgas, "%.1fM", 12.0 + (frame % 8) * 0.5);
        dashboard_set(&db, sec_last, f_time, "%dms", 30 + (frame % 40));
        dashboard_set(&db, sec_last, f_root, "0x%04x...%04x",
                      (unsigned)(current_block & 0xFFFF),
                      (unsigned)((current_block * 7) & 0xFFFF));

        /* Update Network */
        dashboard_set(&db, sec_net, f_peers, "%d", 8 + (frame % 7));
        dashboard_set_color(&db, sec_net, f_peers, (8 + frame % 7) > 10 ? 32 : 33);
        dashboard_set(&db, sec_net, f_recv, "%.1f MB/s", 2.8 + (frame % 10) * 0.2);
        dashboard_set(&db, sec_net, f_sent, "%.1f MB/s", 0.5 + (frame % 8) * 0.1);

        dashboard_render(&db);
        nanosleep(&ts, NULL);
        frame++;
    }

    dashboard_leave();
    dashboard_cleanup(&db);

    printf("Dashboard demo exited cleanly.\n");
    return 0;
}
