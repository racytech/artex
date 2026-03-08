/*
 * bench_mpt_store — Realistic Ethereum workload benchmark for MPT store.
 *
 * Simulates Ethereum block processing at configurable scale:
 *   - Each block creates new accounts and updates existing ones
 *   - Hot account distribution: recent/popular accounts touched more often
 *   - Account values are ~104 bytes (nonce + balance + storage_root + code_hash)
 *   - Supports resume (re-opens existing store, continues from last block)
 *
 * Usage:
 *   bench_mpt_store <num_blocks> [options]
 *
 * Options:
 *   --path <dir>           Store directory (default: /tmp/mpt_bench)
 *   --new-per-block <n>    New accounts per block (default: 13)
 *   --updates-per-block <n> Account updates per block (default: 150)
 *   --report <n>           Report every N blocks (default: 10000)
 *   --compact <n>          Compact every N blocks (0 = never, default: 0)
 *   --resume               Resume from existing store
 *
 * Realistic Ethereum stats (for reference):
 *   ~19M blocks → ~250M accounts
 *   ~13 new accounts/block average
 *   ~150 account state changes/block (balance, nonce)
 *   Zipf-like access: DeFi contracts touched far more than dormant EOAs
 */

#include "mpt_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <getopt.h>

#include "keccak256.h"

/* =========================================================================
 * PRNG — splitmix64 (fast, deterministic, good distribution)
 * ========================================================================= */

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static inline rng_t rng_create(uint64_t seed) {
    rng_t r = { .state = seed };
    rng_next(&r);
    return r;
}

/* =========================================================================
 * Keccak helper
 * ========================================================================= */

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

/* =========================================================================
 * Account key and value generation
 *
 * Key: keccak256(account_index) — 32 bytes
 * Value: hand-encoded RLP of [nonce, balance, storage_root, code_hash]
 *   ~104 bytes total
 * ========================================================================= */

/* Empty storage root: keccak256(RLP([])) = keccak256(0x80) */
static const uint8_t EMPTY_STORAGE_ROOT[32] = {
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
};

/* Empty code hash: keccak256("") */
static const uint8_t EMPTY_CODE_HASH[32] = {
    0xc5,0xd2,0x46,0x01,0x86,0xf7,0x23,0x3c,
    0x92,0x7e,0x7d,0xb2,0xdc,0xc7,0x03,0xc0,
    0xe5,0x00,0xb6,0x53,0xca,0x82,0x27,0x3b,
    0x7b,0xfa,0xd8,0x04,0x5d,0x85,0xa4,0x70
};

static void make_account_key(uint64_t index, uint8_t out[32]) {
    uint8_t buf[8];
    memcpy(buf, &index, 8);
    keccak(buf, 8, out);
}

/* RLP-encode a uint64 into buf, return length */
static size_t rlp_encode_u64(uint8_t *buf, uint64_t val) {
    if (val == 0) {
        buf[0] = 0x80;
        return 1;
    }
    if (val < 0x80) {
        buf[0] = (uint8_t)val;
        return 1;
    }
    /* Big-endian encoding */
    uint8_t be[8];
    int start = 8;
    uint64_t v = val;
    while (v > 0) {
        be[--start] = (uint8_t)(v & 0xFF);
        v >>= 8;
    }
    int len = 8 - start;
    buf[0] = (uint8_t)(0x80 + len);
    memcpy(buf + 1, be + start, len);
    return 1 + len;
}

/* RLP-encode 32-byte string: 0xa0 + 32 bytes = 33 bytes */
static size_t rlp_encode_hash(uint8_t *buf, const uint8_t hash[32]) {
    buf[0] = 0xa0; /* 0x80 + 32 */
    memcpy(buf + 1, hash, 32);
    return 33;
}

/*
 * Build an account RLP value: RLP([nonce, balance, storage_root, code_hash])
 * Returns the total length written to buf.
 * buf must be at least 200 bytes.
 */
static size_t make_account_value(uint8_t *buf, uint64_t nonce,
                                  uint64_t balance_hi, uint64_t balance_lo) {
    /* Encode each field into a temp buffer */
    uint8_t items[180];
    size_t pos = 0;

    /* nonce */
    pos += rlp_encode_u64(items + pos, nonce);

    /* balance: encode as big-endian bytes (up to 16 bytes) */
    if (balance_hi == 0 && balance_lo == 0) {
        items[pos++] = 0x80; /* empty string = 0 */
    } else if (balance_hi == 0 && balance_lo < 0x80) {
        items[pos++] = (uint8_t)balance_lo;
    } else {
        uint8_t be[16];
        int start = 16;
        uint64_t lo = balance_lo, hi = balance_hi;
        while (lo > 0 || hi > 0) {
            be[--start] = (uint8_t)(lo & 0xFF);
            lo = (lo >> 8) | (hi << 56);
            hi >>= 8;
        }
        int len = 16 - start;
        items[pos++] = (uint8_t)(0x80 + len);
        memcpy(items + pos, be + start, len);
        pos += len;
    }

    /* storage_root */
    pos += rlp_encode_hash(items + pos, EMPTY_STORAGE_ROOT);

    /* code_hash */
    pos += rlp_encode_hash(items + pos, EMPTY_CODE_HASH);

    /* Wrap in RLP list */
    if (pos <= 55) {
        buf[0] = (uint8_t)(0xc0 + pos);
        memcpy(buf + 1, items, pos);
        return 1 + pos;
    } else {
        /* pos fits in 1 byte for our sizes */
        buf[0] = 0xf8;
        buf[1] = (uint8_t)pos;
        memcpy(buf + 2, items, pos);
        return 2 + pos;
    }
}

/* =========================================================================
 * Hot account distribution
 *
 * Models real Ethereum access patterns:
 *   60% chance: pick from last 1000 accounts (very hot — active contracts)
 *   25% chance: pick from last 10000 accounts (warm — recent users)
 *   10% chance: pick from last 100000 accounts (lukewarm)
 *    5% chance: pick uniformly from all accounts (cold — dormant EOAs)
 * ========================================================================= */

static uint64_t pick_hot_account(rng_t *rng, uint64_t total_accounts) {
    if (total_accounts == 0) return 0;
    uint64_t r = rng_next(rng) % 100;
    uint64_t window;

    if (r < 60) {
        window = total_accounts < 1000 ? total_accounts : 1000;
    } else if (r < 85) {
        window = total_accounts < 10000 ? total_accounts : 10000;
    } else if (r < 95) {
        window = total_accounts < 100000 ? total_accounts : 100000;
    } else {
        window = total_accounts;
    }

    uint64_t offset = rng_next(rng) % window;
    return total_accounts - 1 - offset;
}

/* =========================================================================
 * System helpers
 * ========================================================================= */

static size_t get_rss_kb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    size_t rss = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            rss = (size_t)atol(line + 6);
            break;
        }
    }
    fclose(f);
    return rss;
}

static size_t get_file_size(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? (size_t)st.st_size : 0;
}

static double time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void format_count(char *buf, size_t buflen, uint64_t n) {
    if (n >= 1000000000ULL)
        snprintf(buf, buflen, "%.2fB", n / 1e9);
    else if (n >= 1000000ULL)
        snprintf(buf, buflen, "%.2fM", n / 1e6);
    else if (n >= 1000ULL)
        snprintf(buf, buflen, "%.1fK", n / 1e3);
    else
        snprintf(buf, buflen, "%" PRIu64, n);
}

static void format_bytes(char *buf, size_t buflen, size_t bytes) {
    if (bytes >= (size_t)1 << 30)
        snprintf(buf, buflen, "%.2f GB", bytes / (double)(1 << 30));
    else if (bytes >= (size_t)1 << 20)
        snprintf(buf, buflen, "%.1f MB", bytes / (double)(1 << 20));
    else if (bytes >= (size_t)1 << 10)
        snprintf(buf, buflen, "%.1f KB", bytes / (double)(1 << 10));
    else
        snprintf(buf, buflen, "%zu B", bytes);
}

static void print_hash(const uint8_t h[32]) {
    for (int i = 0; i < 8; i++) printf("%02x", h[i]);
    printf("...");
}

/* =========================================================================
 * Benchmark state file — tracks progress for resume
 *
 * Written to <store_path>.progress as a simple text file:
 *   block_number total_accounts
 * ========================================================================= */

static void save_progress(const char *store_path, uint64_t block,
                           uint64_t total_accounts) {
    char path[512];
    snprintf(path, sizeof(path), "%s.progress", store_path);
    FILE *f = fopen(path, "w");
    if (f) {
        fprintf(f, "%" PRIu64 " %" PRIu64 "\n", block, total_accounts);
        fclose(f);
    }
}

static bool load_progress(const char *store_path, uint64_t *block,
                            uint64_t *total_accounts) {
    char path[512];
    snprintf(path, sizeof(path), "%s.progress", store_path);
    FILE *f = fopen(path, "r");
    if (!f) return false;
    int rc = fscanf(f, "%" SCNu64 " %" SCNu64, block, total_accounts);
    fclose(f);
    return rc == 2;
}

/* =========================================================================
 * Main
 * ========================================================================= */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s <num_blocks> [options]\n"
        "\n"
        "Options:\n"
        "  --path <dir>             Store path (default: /tmp/mpt_bench)\n"
        "  --new-per-block <n>      New accounts per block (default: 13)\n"
        "  --updates-per-block <n>  Account updates per block (default: 150)\n"
        "  --report <n>             Report every N blocks (default: 10000)\n"
        "  --compact <n>            Compact every N blocks (0=never, default: 0)\n"
        "  --cache <n>              Node cache entries (0=off, default: 0; 32768=~34MB)\n"
        "  --resume                 Resume from existing store\n"
        "\n"
        "Examples:\n"
        "  %s 1000000                          # 1M blocks (~13M accounts)\n"
        "  %s 1000000 --cache 32768            # 1M blocks with 32K node cache\n"
        "  %s 10000000 --path /data/mpt_bench  # 10M blocks (~130M accounts)\n"
        "  %s 100000000 --resume               # Continue to 100M blocks\n",
        prog, prog, prog, prog, prog);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    /* Defaults */
    uint64_t num_blocks = 0;
    const char *store_path = "/tmp/mpt_bench";
    uint32_t new_per_block = 13;
    uint32_t updates_per_block = 150;
    uint32_t report_interval = 10000;
    uint32_t compact_interval = 0;
    uint32_t cache_entries = 0;  /* 0 = disabled */
    bool resume = false;

    /* Parse args */
    num_blocks = strtoull(argv[1], NULL, 10);
    if (num_blocks == 0) {
        fprintf(stderr, "Invalid block count: %s\n", argv[1]);
        return 1;
    }

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
            store_path = argv[++i];
        } else if (strcmp(argv[i], "--new-per-block") == 0 && i + 1 < argc) {
            new_per_block = (uint32_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--updates-per-block") == 0 && i + 1 < argc) {
            updates_per_block = (uint32_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--report") == 0 && i + 1 < argc) {
            report_interval = (uint32_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--compact") == 0 && i + 1 < argc) {
            compact_interval = (uint32_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--cache") == 0 && i + 1 < argc) {
            cache_entries = (uint32_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--resume") == 0) {
            resume = true;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    /* Derived paths */
    char idx_path[512], dat_path[512];
    snprintf(idx_path, sizeof(idx_path), "%s.idx", store_path);
    snprintf(dat_path, sizeof(dat_path), "%s.dat", store_path);

    /* Print config */
    char blk_str[32], acct_str[32];
    format_count(blk_str, sizeof(blk_str), num_blocks);
    format_count(acct_str, sizeof(acct_str),
                 (uint64_t)num_blocks * new_per_block);
    printf("MPT Store Benchmark\n");
    printf("====================\n");
    printf("  Blocks:           %s (%" PRIu64 ")\n", blk_str, num_blocks);
    printf("  ~Total accounts:  %s\n", acct_str);
    printf("  New/block:        %u\n", new_per_block);
    printf("  Updates/block:    %u\n", updates_per_block);
    printf("  Store path:       %s\n", store_path);
    printf("  Report every:     %u blocks\n", report_interval);
    if (compact_interval > 0)
        printf("  Compact every:    %u blocks\n", compact_interval);
    if (cache_entries > 0) {
        char cache_mem[32];
        format_bytes(cache_mem, sizeof(cache_mem),
                     (size_t)cache_entries * 1070);
        printf("  Node cache:       %u entries (~%s)\n",
               cache_entries, cache_mem);
    }
    printf("  Resume:           %s\n", resume ? "yes" : "no");
    printf("\n");

    /* Create or open store */
    uint64_t start_block = 0;
    uint64_t total_accounts = 0;
    mpt_store_t *ms = NULL;

    if (resume) {
        ms = mpt_store_open(store_path);
        if (!ms) {
            fprintf(stderr, "Failed to open store at %s — starting fresh\n",
                    store_path);
            resume = false;
        } else {
            if (load_progress(store_path, &start_block, &total_accounts)) {
                char sb[32], sa[32];
                format_count(sb, sizeof(sb), start_block);
                format_count(sa, sizeof(sa), total_accounts);
                printf("Resuming from block %s (%s accounts)\n\n",
                       sb, sa);
            } else {
                fprintf(stderr, "No progress file found, starting from 0\n");
                start_block = 0;
                total_accounts = 0;
            }
        }
    }

    if (!ms) {
        /* Clean start */
        unlink(idx_path);
        unlink(dat_path);

        /* Estimate capacity: ~2 nodes per account (leaves + internal) */
        uint64_t est_nodes = (uint64_t)num_blocks * new_per_block * 2;
        if (est_nodes < 100000) est_nodes = 100000;
        /* Cap at 2B to avoid oversizing */
        if (est_nodes > 2000000000ULL) est_nodes = 2000000000ULL;

        ms = mpt_store_create(store_path, est_nodes);
        if (!ms) {
            fprintf(stderr, "Failed to create store at %s\n", store_path);
            return 1;
        }
    }

    /* Enable node cache if requested */
    if (cache_entries > 0)
        mpt_store_set_cache(ms, cache_entries);

    /* PRNG for account selection */
    rng_t rng = rng_create(0xDEADBEEFCAFE1234ULL + start_block);
    /* Fast-forward PRNG state if resuming */
    for (uint64_t i = 0; i < start_block; i++) {
        for (uint32_t j = 0; j < updates_per_block + 2; j++)
            rng_next(&rng);
    }

    /* Buffers */
    uint8_t key[32];
    uint8_t value[200];
    size_t base_rss_kb = get_rss_kb();

    /* Timing */
    double t_start = time_sec();
    double t_last_report = t_start;
    uint64_t ops_since_report = 0;

    /* Print header */
    if (cache_entries > 0) {
        printf("%-12s  %-10s  %-10s  %-12s  %-10s  %-10s  %-8s  %-8s  %s\n",
               "Block", "Accounts", "blk/s", "ops/s", "RSS", "Disk",
               "Garbage", "CacheHit", "Root");
        printf("%-12s  %-10s  %-10s  %-12s  %-10s  %-10s  %-8s  %-8s  %s\n",
               "-----", "--------", "-----", "-----", "---", "----",
               "-------", "--------", "----");
    } else {
        printf("%-12s  %-10s  %-10s  %-12s  %-10s  %-10s  %-8s  %s\n",
               "Block", "Accounts", "blk/s", "ops/s", "RSS", "Disk",
               "Garbage", "Root");
        printf("%-12s  %-10s  %-10s  %-12s  %-10s  %-10s  %-8s  %s\n",
               "-----", "--------", "-----", "-----", "---", "----",
               "-------", "----");
    }

    for (uint64_t block = start_block; block < num_blocks; block++) {
        mpt_store_begin_batch(ms);

        /* 1. Create new accounts */
        for (uint32_t i = 0; i < new_per_block; i++) {
            uint64_t acct_idx = total_accounts + i;
            make_account_key(acct_idx, key);

            /* Initial account: nonce=0, balance = random small amount */
            uint64_t bal = rng_next(&rng) % 10000000000ULL; /* up to 10 gwei */
            size_t vlen = make_account_value(value, 0, 0, bal);
            mpt_store_update(ms, key, value, vlen);
        }
        total_accounts += new_per_block;

        /* 2. Update existing accounts */
        uint32_t num_updates = updates_per_block;
        if (total_accounts < updates_per_block)
            num_updates = (uint32_t)total_accounts;

        for (uint32_t i = 0; i < num_updates; i++) {
            uint64_t acct_idx = pick_hot_account(&rng, total_accounts);
            make_account_key(acct_idx, key);

            /* Simulate nonce increment + balance change */
            uint64_t nonce = (block * 3 + i) % 100000;
            uint64_t bal = rng_next(&rng);
            size_t vlen = make_account_value(value, nonce, 0, bal);
            mpt_store_update(ms, key, value, vlen);
        }

        mpt_store_commit_batch(ms);
        ops_since_report += new_per_block + num_updates;

        /* Compact if requested */
        if (compact_interval > 0 && (block + 1) % compact_interval == 0) {
            printf("  [compacting...]\n");
            double tc0 = time_sec();
            mpt_store_compact(ms);
            double tc1 = time_sec();
            printf("  [compact done in %.1fs]\n", tc1 - tc0);
        }

        /* Report */
        if ((block + 1) % report_interval == 0 || block + 1 == num_blocks) {
            double t_now = time_sec();
            double elapsed = t_now - t_last_report;
            double total_elapsed = t_now - t_start;

            double blk_per_sec = report_interval / elapsed;
            double ops_per_sec = ops_since_report / elapsed;

            size_t rss_kb = get_rss_kb();
            size_t rss_net = rss_kb > base_rss_kb ? rss_kb - base_rss_kb : 0;
            size_t disk = get_file_size(dat_path) + get_file_size(idx_path);

            mpt_store_stats_t stats = mpt_store_stats(ms);

            uint8_t root[32];
            mpt_store_root(ms, root);

            char blk_s[32], acct_s[32], bps[32], ops[32];
            char rss_s[32], disk_s[32], garb_s[32];
            format_count(blk_s, sizeof(blk_s), block + 1);
            format_count(acct_s, sizeof(acct_s), total_accounts);
            snprintf(bps, sizeof(bps), "%.0f", blk_per_sec);
            format_count(ops, sizeof(ops), (uint64_t)ops_per_sec);
            format_bytes(rss_s, sizeof(rss_s), rss_net * 1024);
            format_bytes(disk_s, sizeof(disk_s), disk);

            double garb_pct = stats.data_file_size > 0
                ? 100.0 * stats.garbage_bytes / stats.data_file_size
                : 0.0;
            snprintf(garb_s, sizeof(garb_s), "%.1f%%", garb_pct);

            if (cache_entries > 0) {
                uint64_t total_lookups = stats.cache_hits + stats.cache_misses;
                char hit_s[32];
                if (total_lookups > 0)
                    snprintf(hit_s, sizeof(hit_s), "%.1f%%",
                             100.0 * stats.cache_hits / total_lookups);
                else
                    snprintf(hit_s, sizeof(hit_s), "—");
                printf("%-12s  %-10s  %-10s  %-12s  %-10s  %-10s  %-8s  %-8s  ",
                       blk_s, acct_s, bps, ops, rss_s, disk_s, garb_s, hit_s);
            } else {
                printf("%-12s  %-10s  %-10s  %-12s  %-10s  %-10s  %-8s  ",
                       blk_s, acct_s, bps, ops, rss_s, disk_s, garb_s);
            }
            print_hash(root);
            printf("\n");

            /* Flush and sync periodically */
            fflush(stdout);
            mpt_store_sync(ms);
            save_progress(store_path, block + 1, total_accounts);

            t_last_report = time_sec();
            ops_since_report = 0;

            /* ETA */
            if (block + 1 < num_blocks) {
                double avg_bps = (block + 1 - start_block) / total_elapsed;
                uint64_t remaining = num_blocks - block - 1;
                double eta_sec = remaining / avg_bps;
                if (eta_sec < 120)
                    printf("  ETA: %.0fs\n", eta_sec);
                else if (eta_sec < 7200)
                    printf("  ETA: %.1f min\n", eta_sec / 60);
                else
                    printf("  ETA: %.1f hours\n", eta_sec / 3600);
            }
        }
    }

    /* Final summary */
    double total_time = time_sec() - t_start;
    uint64_t total_blocks = num_blocks - start_block;

    printf("\n");
    printf("=== Benchmark Complete ===\n");
    printf("  Blocks processed: %" PRIu64 "\n", total_blocks);
    printf("  Total accounts:   %" PRIu64 "\n", total_accounts);
    printf("  Total time:       ");
    if (total_time < 120)
        printf("%.1f seconds\n", total_time);
    else if (total_time < 7200)
        printf("%.1f minutes\n", total_time / 60);
    else
        printf("%.1f hours\n", total_time / 3600);
    printf("  Avg blocks/sec:   %.1f\n",
           total_blocks / total_time);
    printf("  Avg ops/sec:      %.0f\n",
           total_blocks * (new_per_block + updates_per_block) / total_time);

    size_t final_rss = get_rss_kb();
    size_t final_disk = get_file_size(dat_path) + get_file_size(idx_path);
    mpt_store_stats_t final_stats = mpt_store_stats(ms);

    char rss_s[32], disk_s[32], live_s[32], garb_s[32];
    format_bytes(rss_s, sizeof(rss_s),
                 (final_rss > base_rss_kb ? final_rss - base_rss_kb : 0) * 1024);
    format_bytes(disk_s, sizeof(disk_s), final_disk);
    format_bytes(live_s, sizeof(live_s), final_stats.live_data_bytes);
    format_bytes(garb_s, sizeof(garb_s), final_stats.garbage_bytes);

    printf("  RSS (net):        %s\n", rss_s);
    printf("  Disk total:       %s\n", disk_s);
    printf("  Live nodes:       %" PRIu64 "\n", final_stats.node_count);
    printf("  Live data:        %s\n", live_s);
    printf("  Garbage:          %s (%.1f%%)\n", garb_s,
           final_stats.data_file_size > 0
           ? 100.0 * final_stats.garbage_bytes / final_stats.data_file_size
           : 0.0);

    if (cache_entries > 0) {
        uint64_t total_lookups = final_stats.cache_hits + final_stats.cache_misses;
        printf("  Cache:            %u/%u entries",
               final_stats.cache_count, final_stats.cache_capacity);
        if (total_lookups > 0)
            printf(", %.1f%% hit rate (%" PRIu64 "h/%" PRIu64 "m)",
                   100.0 * final_stats.cache_hits / total_lookups,
                   final_stats.cache_hits, final_stats.cache_misses);
        printf("\n");
    }

    uint8_t final_root[32];
    mpt_store_root(ms, final_root);
    printf("  Final root:       ");
    for (int i = 0; i < 32; i++) printf("%02x", final_root[i]);
    printf("\n");

    mpt_store_sync(ms);
    save_progress(store_path, num_blocks, total_accounts);
    mpt_store_destroy(ms);

    return 0;
}
