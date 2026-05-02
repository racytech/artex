/**
 * Chain Replay Tool
 *
 * Re-executes Ethereum blocks from Era1 archive files.
 * Uses the sync engine for execution, validation, and checkpointing.
 * After reaching Paris (The Merge), optionally starts the Engine API
 * for CL-driven post-merge sync.
 *
 * Usage: ./chain_replay [options] <era1_dir> <genesis.json> [start_block] [end_block]
 */

#include "sync.h"
#include "evm_state.h"
#include "state.h"
#include "era1.h"
#include "era.h"
#include "block.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <pthread.h>
#include "evm_tracer.h"
#include "logger.h"

#ifdef ENABLE_DEBUG
bool g_trace_calls = false;
#endif

#define LOG_ERR LOG_ERROR

#define ERA1_BLOCKS_PER_FILE 8192
#define PARIS_BLOCK          15537394  /* last PoW block (The Merge) */

/* Reconstruct metadata — same format as state_reconstruct.c */
#define META_MAGIC   0x54504D52  /* "RMPT" */
#define META_VERSION 1

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t last_block;
    uint8_t  state_root[32];
    uint8_t  prune_empty;
    uint8_t  reserved[7];
} reconstruct_meta_t;  /* 56 bytes */

static bool meta_write(const char *path, uint64_t block,
                       const uint8_t root[32], bool prune_empty) {
    reconstruct_meta_t meta = {0};
    meta.magic = META_MAGIC;
    meta.version = META_VERSION;
    meta.last_block = block;
    memcpy(meta.state_root, root, 32);
    meta.prune_empty = prune_empty ? 1 : 0;

    FILE *f = fopen(path, "wb");
    if (!f) return false;
    bool ok = fwrite(&meta, sizeof(meta), 1, f) == 1;
    fclose(f);
    return ok;
}

#ifndef CHECKPOINT_INTERVAL
#define CHECKPOINT_INTERVAL  256
#endif

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

/* Default persistent store paths */
#define DEFAULT_DATA_DIR_NAME ".artex"

static char value_dir[512];
static char commit_dir[512];
static char mpt_path[512];
static char flat_path[512];
static char code_path[512];
static char history_path[512];

static void set_data_paths(const char *data_dir) {
    snprintf(value_dir, sizeof(value_dir), "%s/chain_replay_values", data_dir);
    snprintf(commit_dir, sizeof(commit_dir), "%s/chain_replay_commits", data_dir);
    snprintf(mpt_path, sizeof(mpt_path), "%s/chain_replay_mpt", data_dir);
    snprintf(flat_path, sizeof(flat_path), "%s/chain_replay_flat", data_dir);
    snprintf(code_path, sizeof(code_path), "%s/chain_replay_code", data_dir);
    snprintf(history_path, sizeof(history_path), "%s/chain_replay_history", data_dir);
}

/* =========================================================================
 * Graceful shutdown via SIGINT
 * ========================================================================= */

static volatile sig_atomic_t g_shutdown = 0;
static volatile sig_atomic_t g_shutdown_pending = 0;

/* Flush PGO profile data on signal (only exists with -fprofile-generate) */
void __gcov_dump(void) __attribute__((weak));

static void sigint_handler(int sig) {
    (void)sig;
    if (g_shutdown) {
        /* Second Ctrl+C — flush PGO and force exit */
        if (__gcov_dump) __gcov_dump();
        _exit(0);
    } else {
        g_shutdown = 1;
    }
}

/* =========================================================================
 * Era1 file management
 * ========================================================================= */

typedef struct {
    char     **paths;     /* sorted file paths */
    size_t     count;
    era1_t     current;   /* currently open file */
    int        current_idx;
} era1_archive_t;

static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static bool archive_open(era1_archive_t *ar, const char *dir) {
    memset(ar, 0, sizeof(*ar));
    ar->current_idx = -1;

    DIR *d = opendir(dir);
    if (!d) {
        perror("opendir");
        return false;
    }

    /* Collect .era1 files */
    size_t cap = 64;
    ar->paths = malloc(cap * sizeof(char *));
    if (!ar->paths) { closedir(d); return false; }

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen < 5 || strcmp(ent->d_name + nlen - 5, ".era1") != 0)
            continue;

        if (ar->count >= cap) {
            cap *= 2;
            ar->paths = realloc(ar->paths, cap * sizeof(char *));
        }

        size_t pathlen = strlen(dir) + 1 + nlen + 1;
        ar->paths[ar->count] = malloc(pathlen);
        snprintf(ar->paths[ar->count], pathlen, "%s/%s", dir, ent->d_name);
        ar->count++;
    }
    closedir(d);

    if (ar->count == 0) {
        fprintf(stderr, "No .era1 files found in %s\n", dir);
        free(ar->paths);
        ar->paths = NULL;
        return false;
    }

    /* Sort by filename (alphabetical = chronological for era1) */
    qsort(ar->paths, ar->count, sizeof(char *), cmp_strings);
    return true;
}

static void archive_close(era1_archive_t *ar) {
    if (ar->current_idx >= 0)
        era1_close(&ar->current);
    for (size_t i = 0; i < ar->count; i++)
        free(ar->paths[i]);
    free(ar->paths);
    memset(ar, 0, sizeof(*ar));
    ar->current_idx = -1;
}

/* Rescan the era1 directory for newly downloaded files */
static bool archive_rescan(era1_archive_t *ar, const char *dir) {
    /* Close current file */
    if (ar->current_idx >= 0) {
        era1_close(&ar->current);
        ar->current_idx = -1;
    }

    /* Free old paths */
    for (size_t i = 0; i < ar->count; i++)
        free(ar->paths[i]);
    free(ar->paths);
    ar->paths = NULL;
    ar->count = 0;

    /* Re-open directory */
    DIR *d = opendir(dir);
    if (!d) return false;

    size_t cap = 64;
    ar->paths = malloc(cap * sizeof(char *));
    if (!ar->paths) { closedir(d); return false; }

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen < 5 || strcmp(ent->d_name + nlen - 5, ".era1") != 0)
            continue;
        if (ar->count >= cap) {
            cap *= 2;
            ar->paths = realloc(ar->paths, cap * sizeof(char *));
        }
        size_t pathlen = strlen(dir) + 1 + nlen + 1;
        ar->paths[ar->count] = malloc(pathlen);
        snprintf(ar->paths[ar->count], pathlen, "%s/%s", dir, ent->d_name);
        ar->count++;
    }
    closedir(d);

    if (ar->count > 0)
        qsort(ar->paths, ar->count, sizeof(char *), cmp_strings);
    return true;
}

/* Ensure the correct era1 file is open for the given block.
 * If follow=true and the file isn't available yet, poll until it appears. */
static bool archive_ensure(era1_archive_t *ar, uint64_t block_number,
                           bool follow, const char *dir) {
    /* Check if current file contains this block */
    if (ar->current_idx >= 0 && era1_contains(&ar->current, block_number))
        return true;

    /* Close current */
    if (ar->current_idx >= 0) {
        era1_close(&ar->current);
        ar->current_idx = -1;
    }

    /* Find the right file */
    int file_idx = (int)(block_number / ERA1_BLOCKS_PER_FILE);

    /* If file not available, either fail or wait */
    while ((size_t)file_idx >= ar->count) {
        if (!follow) {
            fprintf(stderr, "No era1 file for block %lu (have %zu files)\n",
                    block_number, ar->count);
            return false;
        }
        if (g_shutdown) return false;
        printf("Waiting for era1 file %05d (block %lu)...\n",
               file_idx, block_number);
        sleep(5);
        archive_rescan(ar, dir);
    }

    if (!era1_open(&ar->current, ar->paths[file_idx])) {
        fprintf(stderr, "Failed to open %s\n", ar->paths[file_idx]);
        return false;
    }

    ar->current_idx = file_idx;

    if (!era1_contains(&ar->current, block_number)) {
        fprintf(stderr, "File %s doesn't contain block %lu (has %lu-%lu)\n",
                ar->paths[file_idx], block_number,
                ar->current.start_block,
                ar->current.start_block + ar->current.count - 1);
        era1_close(&ar->current);
        ar->current_idx = -1;
        return false;
    }

    return true;
}

/* =========================================================================
 * Era archive (post-merge) — sequential iteration over .era files
 * ========================================================================= */

typedef struct {
    char     **paths;
    size_t     count;
    era_t      current;
    era_iter_t iter;
    int        current_idx;
    uint64_t   last_block;  /* continuity check */
} era_archive_t;

static bool era_archive_open(era_archive_t *ar, const char *dir) {
    memset(ar, 0, sizeof(*ar));
    ar->current_idx = -1;

    DIR *d = opendir(dir);
    if (!d) return false;

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen < 4 || strcmp(ent->d_name + nlen - 4, ".era") != 0)
            continue;
        if (nlen >= 5 && strcmp(ent->d_name + nlen - 5, ".era1") == 0)
            continue;
        char **np = realloc(ar->paths, (ar->count + 1) * sizeof(char *));
        if (!np) { closedir(d); return false; }
        ar->paths = np;
        char full[1024];
        snprintf(full, sizeof(full), "%s/%s", dir, ent->d_name);
        ar->paths[ar->count++] = strdup(full);
    }
    closedir(d);

    if (ar->count == 0) return false;
    qsort(ar->paths, ar->count, sizeof(char *), cmp_strings);
    return true;
}

/**
 * Skip ahead in the era archive to approximately target_block.
 * Probes the first block of each era file to find the right starting point.
 * Only decompresses one block per file — avoids scanning millions of blocks.
 */
static void era_archive_skip_to(era_archive_t *ar, uint64_t target_block) {
    if (ar->count == 0 || target_block == 0) return;

    for (size_t i = 0; i < ar->count; i++) {
        era_t probe;
        if (!era_open(&probe, ar->paths[i])) continue;
        era_iter_t it = era_iter(&probe);
        block_header_t hdr;
        block_body_t body;
        uint8_t hash[32];
        uint64_t slot;
        bool got = era_iter_next(&it, &hdr, &body, hash, &slot);
        era_close(&probe);
        if (!got) continue;
        block_body_free(&body);

        if (hdr.number > target_block && i > 0) {
            ar->current_idx = (int)(i - 1) - 1;
            return;
        }
    }
    ar->current_idx = (int)(ar->count - 1) - 1;
}

static void era_archive_close(era_archive_t *ar) {
    if (ar->current_idx >= 0) era_close(&ar->current);
    for (size_t i = 0; i < ar->count; i++) free(ar->paths[i]);
    free(ar->paths);
    memset(ar, 0, sizeof(*ar));
    ar->current_idx = -1;
}

static bool era_archive_next(era_archive_t *ar,
                             block_header_t *hdr, block_body_t *body,
                             uint8_t block_hash[32]) {
    while (1) {
        if (ar->current_idx >= 0) {
            uint64_t slot;
            if (era_iter_next(&ar->iter, hdr, body, block_hash, &slot)) {
                /* Continuity check: blocks must be sequential */
                if (ar->last_block > 0 && hdr->number != ar->last_block + 1) {
                    LOG_WARN("Era block gap: expected %lu, got %lu (file: %s)",
                             ar->last_block + 1, hdr->number,
                             ar->paths[ar->current_idx]);
                }
                ar->last_block = hdr->number;
                return true;
            }
            era_close(&ar->current);
            ar->current_idx++;
        } else {
            ar->current_idx = 0;
        }
        if ((size_t)ar->current_idx >= ar->count)
            return false;
        if (!era_open(&ar->current, ar->paths[ar->current_idx]))
            return false;
        ar->iter = era_iter(&ar->current);
    }
}

/* =========================================================================
 * Block prefetch — decode next block while executor runs current one
 * ========================================================================= */

typedef struct {
    /* Decoded block data (owned by prefetch, swapped to consumer) */
    uint8_t        *hdr_rlp;
    size_t          hdr_len;
    uint8_t        *body_rlp;
    size_t          body_len;
    block_header_t  header;
    block_body_t    body;
    hash_t          blk_hash;
    uint64_t        block_number;
    bool            valid;       /* decode succeeded */
    bool            era_transition; /* first post-merge block */
} prefetched_block_t;

typedef struct {
    pthread_t       thread;
    pthread_mutex_t mutex;
    pthread_cond_t  cond_ready;    /* prefetch → main: block is ready */
    pthread_cond_t  cond_consumed; /* main → prefetch: buffer consumed */

    era1_archive_t  archive;       /* own archive instance (pre-merge) */
    const char     *era1_dir;
    const char     *era_dir;       /* post-merge era directory */
    era_archive_t   era_archive;   /* post-merge era archive */
    bool            era_mode;      /* switched to post-merge */
    bool            follow_mode;

    prefetched_block_t buf;        /* single-slot buffer */
    bool            has_data;      /* buffer contains a ready block */
    uint64_t        next_block;    /* next block to prefetch */
    uint64_t        end_block;
    bool            stop;          /* signal thread to exit */
    bool            error;         /* prefetch hit a fatal error */
} block_prefetch_t;

static bool prefetch_era1_block(block_prefetch_t *pf, prefetched_block_t *blk,
                                uint64_t bn) {
    if (!archive_ensure(&pf->archive, bn, pf->follow_mode, pf->era1_dir))
        return false;
    if (!era1_read_block(&pf->archive.current, bn,
                         &blk->hdr_rlp, &blk->hdr_len,
                         &blk->body_rlp, &blk->body_len))
        return false;
    blk->blk_hash = hash_keccak256(blk->hdr_rlp, blk->hdr_len);
    if (!block_header_decode_rlp(&blk->header, blk->hdr_rlp, blk->hdr_len) ||
        !block_body_decode_rlp(&blk->body, blk->body_rlp, blk->body_len)) {
        free(blk->hdr_rlp);
        free(blk->body_rlp);
        blk->hdr_rlp = NULL;
        blk->body_rlp = NULL;
        return false;
    }
    return true;
}

static bool prefetch_era_block(block_prefetch_t *pf, prefetched_block_t *blk) {
    if (!era_archive_next(&pf->era_archive, &blk->header, &blk->body,
                          blk->blk_hash.bytes))
        return false;
    blk->hdr_rlp = NULL;   /* no raw RLP for era blocks */
    blk->body_rlp = NULL;
    return true;
}

static void *prefetch_thread_fn(void *arg) {
    block_prefetch_t *pf = (block_prefetch_t *)arg;

    while (1) {
        pthread_mutex_lock(&pf->mutex);

        while (pf->has_data && !pf->stop)
            pthread_cond_wait(&pf->cond_consumed, &pf->mutex);

        if (pf->stop || pf->next_block > pf->end_block) {
            pthread_mutex_unlock(&pf->mutex);
            break;
        }

        uint64_t bn = pf->next_block++;
        pthread_mutex_unlock(&pf->mutex);

        prefetched_block_t blk;
        memset(&blk, 0, sizeof(blk));
        blk.block_number = bn;
        blk.valid = false;

        bool ok;
        if (!pf->era_mode) {
            ok = prefetch_era1_block(pf, &blk, bn);
            if (!ok && bn >= PARIS_BLOCK && pf->era_dir) {
                /* era1 exhausted — switch to post-merge era files */
                LOG_INFO("Switching to post-merge era files at block %lu", bn);
                if (era_archive_open(&pf->era_archive, pf->era_dir)) {
                    pf->era_mode = true;
                    /* Skip era blocks until we reach bn */
                    while (1) {
                        ok = prefetch_era_block(pf, &blk);
                        if (!ok) break;
                        if (blk.header.number == bn) {
                            blk.era_transition = true;
                            break;
                        }
                        if (blk.header.number > bn) { ok = false; break; }
                        block_body_free(&blk.body);
                    }
                }
            }
        } else {
            ok = prefetch_era_block(pf, &blk);
        }

        if (!ok) {
            pthread_mutex_lock(&pf->mutex);
            pf->error = true;
            pf->has_data = false;
            pthread_cond_signal(&pf->cond_ready);
            pthread_mutex_unlock(&pf->mutex);
            break;
        }

        blk.valid = true;

        pthread_mutex_lock(&pf->mutex);
        pf->buf = blk;
        pf->has_data = true;
        pthread_cond_signal(&pf->cond_ready);
        pthread_mutex_unlock(&pf->mutex);
    }

    return NULL;
}

static bool prefetch_start(block_prefetch_t *pf, const char *era1_dir,
                           const char *era_dir,
                           uint64_t start_block, uint64_t end_block,
                           bool follow) {
    memset(pf, 0, sizeof(*pf));
    pthread_mutex_init(&pf->mutex, NULL);
    pthread_cond_init(&pf->cond_ready, NULL);
    pthread_cond_init(&pf->cond_consumed, NULL);

    pf->era1_dir = era1_dir;
    pf->era_dir = era_dir;
    pf->follow_mode = follow;
    pf->next_block = start_block;
    pf->end_block = end_block;

    /* Start in era mode if already past Paris */
    if (start_block >= PARIS_BLOCK && era_dir) {
        if (!era_archive_open(&pf->era_archive, era_dir))
            return false;
        pf->era_mode = true;
        /* Skip to the right era file, then scan within it */
        era_archive_skip_to(&pf->era_archive, start_block);
        while (1) {
            prefetched_block_t tmp;
            memset(&tmp, 0, sizeof(tmp));
            if (!era_archive_next(&pf->era_archive, &tmp.header, &tmp.body,
                                  tmp.blk_hash.bytes))
                return false;
            if (tmp.header.number >= start_block) {
                /* This is the block the thread needs. Stash it in the
                 * prefetch buffer so the thread delivers it first. */
                tmp.valid = true;
                tmp.block_number = tmp.header.number;
                tmp.hdr_rlp = NULL;
                tmp.body_rlp = NULL;
                pf->buf = tmp;
                pf->has_data = true;
                pf->next_block = start_block + 1; /* thread picks up after this */
                break;
            }
            block_body_free(&tmp.body);
        }
    } else {
        if (!archive_open(&pf->archive, era1_dir))
            return false;
    }

    pthread_create(&pf->thread, NULL, prefetch_thread_fn, pf);
    return true;
}

/** Get next prefetched block. Caller takes ownership of rlp buffers + body. */
static bool prefetch_get(block_prefetch_t *pf, prefetched_block_t *out) {
    pthread_mutex_lock(&pf->mutex);

    while (!pf->has_data && !pf->error && !pf->stop)
        pthread_cond_wait(&pf->cond_ready, &pf->mutex);

    if (pf->error || !pf->has_data) {
        pthread_mutex_unlock(&pf->mutex);
        return false;
    }

    *out = pf->buf;
    pf->has_data = false;
    pthread_cond_signal(&pf->cond_consumed);
    pthread_mutex_unlock(&pf->mutex);
    return true;
}

static void prefetch_stop(block_prefetch_t *pf) {
    pthread_mutex_lock(&pf->mutex);
    pf->stop = true;
    pthread_cond_signal(&pf->cond_consumed);
    pthread_mutex_unlock(&pf->mutex);

    pthread_join(pf->thread, NULL);
    archive_close(&pf->archive);
    if (pf->era_mode) era_archive_close(&pf->era_archive);
    pthread_mutex_destroy(&pf->mutex);
    pthread_cond_destroy(&pf->cond_ready);
    pthread_cond_destroy(&pf->cond_consumed);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    /* EVM allows 1024-deep call stacks. Increase stack to 32MB for deep recursion. */
    struct rlimit rl;
    if (getrlimit(RLIMIT_STACK, &rl) == 0 && rl.rlim_cur < 32UL * 1024 * 1024) {
        rl.rlim_cur = 32UL * 1024 * 1024;
        setrlimit(RLIMIT_STACK, &rl);
    }

    /* Parse flags */
    bool force_clean = false;
    bool follow_mode = false;
#ifdef ENABLE_EVM_TRACE
    uint64_t trace_block = UINT64_MAX;  /* UINT64_MAX = no tracing */
    const char *trace_out_path = NULL;  /* NULL → default data/trace/block_<N>.jsonl;
                                         * "-"  → stderr (legacy behavior) */
    FILE *trace_fp = NULL;              /* open across enable/disable blocks */
    bool trace_memory = false;          /* --trace-memory: include per-op memory */
    bool trace_storage = false;         /* --trace-storage: include per-op storage dump */
#endif
    uint64_t dump_prestate_block = UINT64_MAX;
    const char *dump_prestate_path = NULL;
    uint64_t save_state_block = UINT64_MAX;
    const char *save_state_path = NULL;
    const char *load_state_path = NULL;
    bool no_validate = false;
    uint64_t snapshot_every = 0;  /* 0 = disabled */
    uint64_t replay_history_to_block = 0;  /* 0 = no replay */
#ifdef ENABLE_HISTORY
    bool no_history = false;
#endif
    uint32_t validate_every = CHECKPOINT_INTERVAL;
    /* Default data dir: ~/.artex */
    char default_data_dir[512];
    const char *home = getenv("HOME");
    snprintf(default_data_dir, sizeof(default_data_dir), "%s/%s",
             home ? home : ".", DEFAULT_DATA_DIR_NAME);
    const char *data_dir = default_data_dir;
    int arg_offset = 0;
    while (arg_offset + 1 < argc && argv[1 + arg_offset][0] == '-') {
        if (strcmp(argv[1 + arg_offset], "--clean") == 0) {
            force_clean = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--follow") == 0) {
            follow_mode = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--data-dir") == 0 && arg_offset + 2 < argc) {
            data_dir = argv[2 + arg_offset];
            arg_offset += 2;
#ifdef ENABLE_HISTORY
        } else if (strcmp(argv[1 + arg_offset], "--no-history") == 0) {
            no_history = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--replay-history-to") == 0 && arg_offset + 2 < argc) {
            replay_history_to_block = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
#endif
        } else if (strcmp(argv[1 + arg_offset], "--validate-every") == 0 && arg_offset + 2 < argc) {
            validate_every = (uint32_t)atoi(argv[2 + arg_offset]);
            if (validate_every == 0) validate_every = 1;
            arg_offset += 2;
#ifdef ENABLE_EVM_TRACE
        } else if (strcmp(argv[1 + arg_offset], "--trace-block") == 0 && arg_offset + 2 < argc) {
            trace_block = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--trace-out") == 0 && arg_offset + 2 < argc) {
            trace_out_path = argv[2 + arg_offset];
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--trace-memory") == 0) {
            trace_memory = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--trace-storage") == 0) {
            trace_storage = true;
            arg_offset++;
#endif
        } else if (strcmp(argv[1 + arg_offset], "--no-validate") == 0) {
            no_validate = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--snapshot-every") == 0 && arg_offset + 2 < argc) {
            snapshot_every = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--save-state") == 0 && arg_offset + 2 < argc) {
            save_state_block = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--load-state") == 0 && arg_offset + 2 < argc) {
            load_state_path = argv[2 + arg_offset];
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--dump-prestate") == 0 && arg_offset + 2 < argc) {
            dump_prestate_block = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
            /* Optional output path */
            if (arg_offset + 1 < argc && argv[1 + arg_offset][0] != '-') {
                dump_prestate_path = argv[1 + arg_offset];
                arg_offset++;
            }
        } else {
            break;
        }
    }

    if (argc - arg_offset > 1 && strcmp(argv[1 + arg_offset], "--help") == 0) {
        fprintf(stderr,
            "Usage: %s [options] [start_block] [end_block]\n"
            "\n"
            "Options:\n"
            "  --clean               Delete existing state files, start from genesis\n"
            "  --follow              Wait for new era1 files instead of stopping\n"
            "  --data-dir DIR        Set data directory for all state files (default: ~/.artex)\n"
            "  --no-history          Disable per-block state diff history\n"
            "  --replay-history-to N Fast-forward state via history diffs from\n"
            "                        loaded block+1 to N, then continue normal\n"
            "                        execute from N+1. Requires history already\n"
            "                        persisted covering the range.\n"
            "  --trace-block N       Enable EIP-3155 EVM trace for block N\n"
            "                        (default output: data/trace/block_<N>.jsonl)\n"
            "  --trace-out PATH      Override trace output file. '-' for stderr.\n"
            "  --trace-memory        Include per-op memory dump (array of\n"
            "                        32-byte words) — big; off by default\n"
            "  --trace-storage       Include per-op storage dump (touched slots)\n"
            "                        — inflates trace size 5-10x; off by default\n"
            "  --no-validate         Skip all root validation, compute once at end\n"
            "  --snapshot-every N    Auto-save state every N blocks (e.g. 1000000)\n"
            "  --save-state N [P]    Save full state after block N to binary file P\n"
            "                        (default: state_<N>.bin)\n"
            "  --load-state P        Load state from binary file P, resume from that block\n"
            "  --dump-prestate N [P] Dump pre-state alloc.json for block N to path P\n"
            "  --validate-every N    Validate state root every N blocks (default: %d)\n"
            "\n",
            argv[0], CHECKPOINT_INTERVAL);
        return 0;
    }

    set_data_paths(data_dir);

    /* Ensure data directory exists */
    mkdir(data_dir, 0755);

    const char *era1_dir     = "data/era1";
    const char *era_dir      = "data/era";
    const char *genesis_path = "data/mainnet_genesis.json";
    uint64_t user_start = (argc - arg_offset > 1)
                          ? (uint64_t)atoll(argv[1 + arg_offset]) : 0;
    uint64_t end_block  = (argc - arg_offset > 2)
                          ? (uint64_t)atoll(argv[2 + arg_offset]) : UINT64_MAX;

    /* Install signal handler — no SA_RESTART so blocking reads can be interrupted */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT, &sa, NULL);  /* intentionally no SA_RESTART for SIGINT */

    /* Open era1 archive (optional — not needed for post-merge snapshot loads) */
    era1_archive_t archive;
    memset(&archive, 0, sizeof(archive));
    if (archive_open(&archive, era1_dir)) {
        LOG_INFO("Era1 archive: %zu files in %s", archive.count, era1_dir);
    }

    /* Clean up old state if requested */
    if (force_clean) {
        printf("--clean: removing existing state files\n");
        char cmd[1024];
        snprintf(cmd, sizeof(cmd),
                 "rm -rf %s %s %s.idx %s.dat %s_storage.idx %s_storage.dat"
                 " %s.idx %s.dat %s_acct.art %s_stor.art %s.meta"
                 " 2>/dev/null",
                 value_dir, commit_dir, mpt_path, mpt_path, mpt_path, mpt_path,
                 code_path, code_path, flat_path, flat_path, mpt_path);
        (void)system(cmd);
#ifdef ENABLE_HISTORY
        snprintf(cmd, sizeof(cmd), "rm -rf %s 2>/dev/null", history_path);
        (void)system(cmd);
#endif
    }

    /* Create sync engine */
    sync_config_t cfg = {
        .chain_config        = chain_config_mainnet(),
        .verkle_value_dir    = NULL,
        .verkle_commit_dir   = NULL,
        .mpt_path            = NULL,
        .checkpoint_interval = validate_every,
        .validate_state_root = !no_validate,
    };
#ifdef ENABLE_HISTORY
    if (!no_history)
        cfg.history_dir = history_path;
#endif
    cfg.mpt_path = mpt_path;
    cfg.code_store_path = code_path;

    LOG_INFO("Loading state...");
    sync_t *sync = sync_create(&cfg);
    if (!sync) {
        LOG_ERR("Failed to create sync engine");
        archive_close(&archive);
        return 1;
    }
    /* Set storage pool path (mmap-backed per-account ART tries) */
    evm_state_set_storage_path(sync_get_state(sync), data_dir);

    uint64_t start_block = (user_start > 0) ? user_start : 1;

    if (load_state_path) {
        /* Load state from binary snapshot */
        evm_state_t *es = sync_get_state(sync);
        state_t *st = evm_state_get_state(es);
        hash_t loaded_root;
        if (!state_load(st, load_state_path, &loaded_root)) {
            LOG_ERR("Failed to load state from %s", load_state_path);
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }
        state_stats_t ss = state_get_stats(st);
        /* current_block was set by state_load */
        uint64_t loaded_block = state_get_block(st);
        start_block = loaded_block + 1;
        char root_hex[67];
        hash_to_hex(&loaded_root, root_hex);
        LOG_INFO("Loaded state: %u accounts at block %lu from %s (root: %s)",
                 ss.account_count, loaded_block, load_state_path, root_hex);

        /* Recompute MPT root from scratch to verify snapshot integrity */
        LOG_INFO("Recomputing MPT root...");
        /* evm_state_invalidate_all(es); */
        hash_t recomputed = evm_state_compute_mpt_root(es, loaded_block >= 2675000);
        char recomp_hex[67];
        hash_to_hex(&recomputed, recomp_hex);
        if (memcmp(loaded_root.bytes, recomputed.bytes, 32) == 0) {
            LOG_INFO("Root verified: %s", recomp_hex);
        } else {
            LOG_WARN("Root mismatch! loaded=%s recomputed=%s", root_hex, recomp_hex);
        }

        /* Populate block hash ring from era1 or era files.
         * Post-merge blocks are in era files, not era1. */
        uint64_t hash_start = loaded_block >= 256 ? loaded_block - 255 : 0;
        size_t hash_count = (size_t)(loaded_block - hash_start + 1);
        hash_t *hashes = calloc(hash_count, sizeof(hash_t));
        if (hashes) {
            if (loaded_block < PARIS_BLOCK) {
                /* Pre-merge: read from era1 */
                for (uint64_t i = 0; i < hash_count; i++) {
                    uint64_t hbn = hash_start + i;
                    if (archive_ensure(&archive, hbn, false, era1_dir)) {
                        uint8_t *h_rlp = NULL; size_t h_len = 0;
                        uint8_t *b_rlp = NULL; size_t b_len = 0;
                        if (era1_read_block(&archive.current, hbn,
                                             &h_rlp, &h_len, &b_rlp, &b_len)) {
                            hashes[i] = hash_keccak256(h_rlp, h_len);
                            free(h_rlp); free(b_rlp);
                        }
                    }
                }
            } else if (era_dir) {
                /* Post-merge: read block hashes from era files */
                era_archive_t hash_ar;
                if (era_archive_open(&hash_ar, era_dir)) {
                    era_archive_skip_to(&hash_ar, hash_start);
                    block_header_t hdr;
                    block_body_t body;
                    uint8_t blk_hash[32];
                    /* Skip to hash_start */
                    while (era_archive_next(&hash_ar, &hdr, &body, blk_hash)) {
                        if (hdr.number >= hash_start) {
                            if (hdr.number <= loaded_block) {
                                size_t idx = (size_t)(hdr.number - hash_start);
                                memcpy(hashes[idx].bytes, blk_hash, 32);
                            }
                            block_body_free(&body);
                            if (hdr.number >= loaded_block) break;
                            continue;
                        }
                        block_body_free(&body);
                    }
                    era_archive_close(&hash_ar);
                }
            }
            sync_resume(sync, loaded_block, hashes, hash_count);
            free(hashes);
        }

        /* --replay-history-to: fast-forward state by applying persisted
         * per-block diffs, then re-seed the hash ring to the new anchor
         * so normal execute can pick up from there. */
#ifdef ENABLE_HISTORY
        if (replay_history_to_block > 0) {
            if (replay_history_to_block <= loaded_block) {
                LOG_ERROR("--replay-history-to %lu must be > loaded block %lu",
                          replay_history_to_block, loaded_block);
                sync_destroy(sync);
                archive_close(&archive);
                return 1;
            }
            LOG_INFO("Replaying history diffs: %lu -> %lu (%lu blocks)...",
                     loaded_block + 1, replay_history_to_block,
                     replay_history_to_block - loaded_block);
            struct timespec t_rep0, t_rep1;
            clock_gettime(CLOCK_MONOTONIC, &t_rep0);
            uint64_t applied = sync_replay_history(sync, replay_history_to_block);
            clock_gettime(CLOCK_MONOTONIC, &t_rep1);
            uint64_t want = replay_history_to_block - loaded_block;
            double rep_sec = (t_rep1.tv_sec - t_rep0.tv_sec) +
                             (t_rep1.tv_nsec - t_rep0.tv_nsec) / 1e9;
            if (applied != want) {
                LOG_ERROR("history replay incomplete: applied %lu/%lu blocks",
                          applied, want);
                sync_destroy(sync);
                archive_close(&archive);
                return 1;
            }
            LOG_INFO("Replay complete in %.1fs (%.1f blk/s)",
                     rep_sec, rep_sec > 0 ? applied / rep_sec : 0.0);

            /* Re-seed the block hash ring for the new anchor. */
            uint64_t new_anchor = replay_history_to_block;
            uint64_t hash_start2 = new_anchor >= 256 ? new_anchor - 255 : 0;
            size_t hash_count2 = (size_t)(new_anchor - hash_start2 + 1);
            hash_t *hashes2 = calloc(hash_count2, sizeof(hash_t));
            if (hashes2) {
                if (new_anchor < PARIS_BLOCK) {
                    for (uint64_t i = 0; i < hash_count2; i++) {
                        uint64_t hbn = hash_start2 + i;
                        if (archive_ensure(&archive, hbn, false, era1_dir)) {
                            uint8_t *h_rlp = NULL; size_t h_len = 0;
                            uint8_t *b_rlp = NULL; size_t b_len = 0;
                            if (era1_read_block(&archive.current, hbn,
                                                 &h_rlp, &h_len, &b_rlp, &b_len)) {
                                hashes2[i] = hash_keccak256(h_rlp, h_len);
                                free(h_rlp); free(b_rlp);
                            }
                        }
                    }
                } else if (era_dir) {
                    era_archive_t hash_ar;
                    if (era_archive_open(&hash_ar, era_dir)) {
                        era_archive_skip_to(&hash_ar, hash_start2);
                        block_header_t hdr;
                        block_body_t body;
                        uint8_t blk_hash[32];
                        while (era_archive_next(&hash_ar, &hdr, &body, blk_hash)) {
                            if (hdr.number >= hash_start2) {
                                if (hdr.number <= new_anchor) {
                                    size_t idx = (size_t)(hdr.number - hash_start2);
                                    memcpy(hashes2[idx].bytes, blk_hash, 32);
                                }
                                block_body_free(&body);
                                if (hdr.number >= new_anchor) break;
                                continue;
                            }
                            block_body_free(&body);
                        }
                        era_archive_close(&hash_ar);
                    }
                }
                sync_reseed_block_hashes(sync, new_anchor, hashes2, hash_count2);
                free(hashes2);
            }
            start_block = new_anchor + 1;
        }
#else
        if (replay_history_to_block > 0) {
            LOG_ERROR("--replay-history-to requires ENABLE_HISTORY build flag");
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }
#endif
    } else {
        /* Read genesis block hash from era1 */
        hash_t gen_hash = {0};
        if (archive_ensure(&archive, 0, false, era1_dir)) {
            uint8_t *hdr_rlp, *body_rlp;
            size_t hdr_len, body_len;
            if (era1_read_block(&archive.current, 0,
                                &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
                gen_hash = hash_keccak256(hdr_rlp, hdr_len);
                free(hdr_rlp);
                free(body_rlp);
            }
        }

        if (!sync_load_genesis(sync, genesis_path, &gen_hash)) {
            LOG_ERR("Failed to load genesis state");
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }
    }

    /* Progress tracking */
    struct timespec t_start, t_now;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    uint64_t display_end = end_block;
    if (end_block == UINT64_MAX) {
        if (start_block >= PARIS_BLOCK)
            display_end = 0;  /* unknown end — era files don't have a fixed count */
        else
            display_end = (uint64_t)(archive.count * ERA1_BLOCKS_PER_FILE - 1);
    }

    printf("Replaying blocks %lu to %lu...\n\n", start_block, display_end);

    uint64_t window_txs = 0;
    uint64_t window_gas = 0;
    uint64_t window_transfers = 0;
    uint64_t window_calls = 0;
    struct timespec t_window;
    clock_gettime(CLOCK_MONOTONIC, &t_window);


    /* Start block prefetch thread */
    block_prefetch_t prefetch;
    if (!prefetch_start(&prefetch, era1_dir, era_dir, start_block, end_block, follow_mode)) {
        LOG_ERR("Failed to start block prefetch");
        sync_destroy(sync);
        archive_close(&archive);
        return 1;
    }

    for (uint64_t bn = start_block; bn <= end_block; bn++) {
        /* Graceful shutdown: finish current batch, checkpoint at next boundary */
        if (g_shutdown && !g_shutdown_pending) {
            g_shutdown_pending = true;
            LOG_WARN("SIGINT received - finishing batch to next checkpoint...");
            LOG_WARN("(press Ctrl+C again to stop immediately)");
        }
        /* Second Ctrl+C — stop immediately, sync_destroy handles cleanup */
        if (g_shutdown >= 2) {
            LOG_WARN("Forced stop - cleaning up...");
            break;
        }

        /* Get next block from prefetch thread */
        prefetched_block_t blk;
        if (!prefetch_get(&prefetch, &blk)) {
            LOG_INFO("No more blocks at %lu (era1 + era exhausted)", bn);

            /* Save state snapshot before exiting */
            sync_status_t exit_st = sync_get_status(sync);
            uint64_t last_block = exit_st.last_block;
            if (last_block > 0 && exit_st.blocks_fail == 0) {
                char save_path[512];
                snprintf(save_path, sizeof(save_path), "%s/state_%lu.bin",
                         data_dir, last_block);
                LOG_INFO("Saving state snapshot to %s ...", save_path);
                evm_state_t *es = sync_get_state(sync);
                state_t *st = evm_state_get_state(es);
                bool prune = (last_block >= 2675000);
                evm_state_invalidate_all(es);
                hash_t root = evm_state_compute_mpt_root(es, prune);
                if (state_save(st, save_path, &root)) {
                    char hex[67];
                    hash_to_hex(&root, hex);
                    LOG_INFO("State saved: block %lu, root %s", last_block, hex);
                } else {
                    LOG_ERROR("Failed to save state to %s", save_path);
                }
            }
            break;
        }

        uint8_t *hdr_rlp = blk.hdr_rlp;
        uint8_t *body_rlp = blk.body_rlp;
        block_header_t header = blk.header;
        block_body_t body = blk.body;
        hash_t blk_hash = blk.blk_hash;

        /* Era transition: validate + save snapshot at last pre-merge block */
        if (blk.era_transition) {
            uint64_t last_pre_merge = bn - 1;
            LOG_INFO("Paris transition at block %lu — computing pre-merge state root", bn);
            evm_state_t *snap_es = sync_get_state(sync);
            state_t *snap_st = evm_state_get_state(snap_es);
            evm_state_invalidate_all(snap_es);
            hash_t pre_merge_root = evm_state_compute_mpt_root(snap_es, true);

            /* Read expected root from last era1 block header */
            hash_t expected = {0};
            if (archive_ensure(&archive, last_pre_merge, false, era1_dir)) {
                uint8_t *h_rlp = NULL; size_t h_len = 0;
                uint8_t *b_rlp = NULL; size_t b_len = 0;
                if (era1_read_block(&archive.current, last_pre_merge,
                                     &h_rlp, &h_len, &b_rlp, &b_len)) {
                    block_header_t last_hdr;
                    if (block_header_decode_rlp(&last_hdr, h_rlp, h_len))
                        expected = last_hdr.state_root;
                    free(h_rlp); free(b_rlp);
                }
            }

            char got_hex[67], exp_hex[67];
            hash_to_hex(&pre_merge_root, got_hex);
            hash_to_hex(&expected, exp_hex);

            if (memcmp(pre_merge_root.bytes, expected.bytes, 32) == 0) {
                LOG_INFO("Pre-merge root MATCHES at block %lu", last_pre_merge);
                char snap_path[256];
                snprintf(snap_path, sizeof(snap_path), "%s/state_%lu.bin",
                         data_dir, last_pre_merge);
                if (state_save(snap_st, snap_path, &pre_merge_root)) {
                    LOG_INFO("Saved pre-merge snapshot: %s", snap_path);
                } else {
                    LOG_ERROR("Failed to save pre-merge snapshot");
                }
            } else {
                LOG_ERROR("Pre-merge root MISMATCH at block %lu", last_pre_merge);
                LOG_ERROR("got:      %s", got_hex);
                LOG_ERROR("expected: %s", exp_hex);
                block_body_free(&body);
                free(hdr_rlp); free(body_rlp);
                break;
            }
        }

        /* Prepare for dump-prestate: keep undo log + enable access tracking */
        if (bn == dump_prestate_block) {
            evm_t *evm = sync_get_evm(sync);
            evm->keep_undo = true;
            evm_state_enable_access_tracking(sync_get_state(sync));
            fprintf(stderr, "dump-prestate: block %lu — undo log + access tracking enabled\n", bn);
        }

        /* Enable EVM tracing for the target block. Trace output goes to a
         * dedicated .jsonl file by default so it's parseable as strict JSON
         * Lines; banners only appear on stderr. Override with --trace-out or
         * --trace-out - (stderr). */
#ifdef ENABLE_EVM_TRACE
        if (bn == trace_block) {
            FILE *out;
            char default_path[512];
            const char *resolved_path;
            if (trace_out_path && strcmp(trace_out_path, "-") == 0) {
                out = stderr;
                resolved_path = "stderr";
            } else {
                if (trace_out_path) {
                    resolved_path = trace_out_path;
                } else {
                    mkdir("data", 0755);
                    mkdir("data/trace", 0755);
                    snprintf(default_path, sizeof(default_path),
                             "data/trace/block_%lu.jsonl", bn);
                    resolved_path = default_path;
                }
                out = fopen(resolved_path, "w");
                if (!out) {
                    LOG_ERROR("failed to open %s for trace output; "
                              "falling back to stderr", resolved_path);
                    out = stderr;
                    resolved_path = "stderr";
                } else {
                    trace_fp = out;
                }
            }
            evm_tracer_init(out);
            evm_tracer_set_dump_memory(trace_memory);
            evm_tracer_set_dump_storage(trace_storage);
            fprintf(stderr, "=== EVM TRACE: block %lu → %s%s%s ===\n",
                    bn, resolved_path,
                    trace_memory  ? " (+memory)" : "",
                    trace_storage ? " (+storage)" : "");
        }
#endif

        /* Execute + validate via sync engine */
        sync_block_result_t result;
        if (!sync_execute_block(sync, &header, &body, &blk_hash, &result)) {
            LOG_ERROR("Block %lu: fatal execution error", bn);
            LOG_WARN("hint: sync_execute_block returned false (internal error, not validation)");
            LOG_WARN("hint: check for OOM, disk I/O errors, or corrupt era1 data");
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        /* Disable tracing after the target block */
#ifdef ENABLE_EVM_TRACE
        if (bn == trace_block) {
            g_evm_tracer.enabled = false;
            if (trace_fp && trace_fp != stderr) {
                fclose(trace_fp);
                trace_fp = NULL;
            }
            fprintf(stderr, "=== END EVM TRACE: block %lu ===\n", bn);
        }
#endif

        /* Dump prestate for this block */
        if (bn == dump_prestate_block) {
            evm_state_t *es = sync_get_state(sync);
            evm_state_disable_access_tracking(es);

            /* Collect dirty addresses and storage keys from execution */
            #define MAX_ADDRS  8192
            #define MAX_SLOTS  65536
            address_t *addrs = malloc(MAX_ADDRS * sizeof(address_t));
            size_t n_addrs = evm_state_collect_addresses(es, addrs, MAX_ADDRS);

            /* Also include coinbase (miner reward touches it) */
            {
                bool have_cb = false;
                for (size_t ci = 0; ci < n_addrs; ci++) {
                    if (memcmp(addrs[ci].bytes, header.coinbase.bytes, 20) == 0)
                        { have_cb = true; break; }
                }
                if (!have_cb && n_addrs < MAX_ADDRS)
                    addrs[n_addrs++] = header.coinbase;
            }
            fprintf(stderr, "dump-prestate: %zu dirty addresses\n", n_addrs);

            typedef struct { address_t addr; uint256_t *keys; size_t count; } addr_slots_t;
            addr_slots_t *addr_slots = malloc(n_addrs * sizeof(addr_slots_t));
            uint256_t *slot_buf = malloc(MAX_SLOTS * sizeof(uint256_t));
            size_t total_slots = 0;
            for (size_t i = 0; i < n_addrs; i++) {
                addr_slots[i].addr = addrs[i];
                size_t n = evm_state_collect_storage_keys(es, &addrs[i],
                    slot_buf + total_slots, MAX_SLOTS - total_slots);
                addr_slots[i].keys = slot_buf + total_slots;
                addr_slots[i].count = n;
                total_slots += n;
            }
            fprintf(stderr, "dump-prestate: %zu storage keys\n", total_slots);

            /* Use pre-block values from undo log (no temp file needed).
             * blk_orig_acct has pre-block nonce/balance for modified accounts.
             * blk_orig_stor has pre-block storage values for modified slots.
             * Unmodified accounts/slots: current state = pre-block state. */
            state_t *st = evm_state_get_state(es);

            /* Build output path */
            char default_path[512];
            if (!dump_prestate_path) {
                char prestate_dir[256];
                snprintf(prestate_dir, sizeof(prestate_dir),
                         "known_issues/block_%lu", bn);
                mkdir("known_issues", 0755);
                mkdir(prestate_dir, 0755);
                snprintf(default_path, sizeof(default_path),
                         "known_issues/block_%lu/alloc.json", bn);
                dump_prestate_path = default_path;
            }

            FILE *out = fopen(dump_prestate_path, "w");
            if (!out) {
                fprintf(stderr, "Failed to open %s for writing\n", dump_prestate_path);
                free(addrs); free(addr_slots); free(slot_buf);
                break;
            }

            fprintf(out, "{\n");
            for (size_t i = 0; i < n_addrs; i++) {
                address_t *a = &addrs[i];
                uint64_t nonce;
                uint256_t balance;
                state_get_preblock_account(st, a, &nonce, &balance);

                if (i > 0) fprintf(out, ",\n");
                fprintf(out, "  \"0x");
                for (int j = 0; j < 20; j++) fprintf(out, "%02x", a->bytes[j]);
                fprintf(out, "\": {\n");

                /* Balance */
                uint8_t bal[32]; uint256_to_bytes(&balance, bal);
                fprintf(out, "    \"balance\": \"0x");
                int bstart = 0; while (bstart < 31 && bal[bstart] == 0) bstart++;
                for (int j = bstart; j < 32; j++) fprintf(out, "%02x", bal[j]);
                fprintf(out, "\",\n");

                /* Nonce */
                fprintf(out, "    \"nonce\": \"0x%lx\"", nonce);

                /* Code — code doesn't change within a block for existing accounts,
                 * except for EIP-7702 delegations set by type-4 txs in THIS block.
                 * Current code is the post-block code; for pre-block we'd need the
                 * original. For now, use current code (correct for most cases). */
                uint32_t code_size = evm_state_get_code_size(es, a);
                if (code_size > 0) {
                    const uint8_t *code = evm_state_get_code_ptr(es, a, &code_size);
                    if (code && code_size > 0) {
                        fprintf(out, ",\n    \"code\": \"0x");
                        for (uint32_t c = 0; c < code_size; c++)
                            fprintf(out, "%02x", code[c]);
                        fprintf(out, "\"");
                    }
                }

                /* Storage — use pre-block values from undo log */
                if (addr_slots[i].count > 0) {
                    fprintf(out, ",\n    \"storage\": {");
                    bool first_slot = true;
                    for (size_t si = 0; si < addr_slots[i].count; si++) {
                        uint256_t val = state_get_preblock_storage(st, a, &addr_slots[i].keys[si]);
                        if (uint256_is_zero(&val)) continue;
                        if (!first_slot) fprintf(out, ",");
                        first_slot = false;
                        uint8_t kbe[32], vbe[32];
                        uint256_to_bytes(&addr_slots[i].keys[si], kbe);
                        uint256_to_bytes(&val, vbe);
                        fprintf(out, "\n      \"0x");
                        for (int j = 0; j < 32; j++) fprintf(out, "%02x", kbe[j]);
                        fprintf(out, "\": \"0x");
                        for (int j = 0; j < 32; j++) fprintf(out, "%02x", vbe[j]);
                        fprintf(out, "\"");
                    }
                    fprintf(out, "\n    }");
                }

                fprintf(out, "\n  }");
            }
            fprintf(out, "\n}\n");
            fclose(out);

            fprintf(stderr, "Prestate dumped to %s (%zu accounts, %zu storage keys)\n",
                   dump_prestate_path, n_addrs, total_slots);

            /* Write env.json */
            {
                char env_path[512];
                const char *last_slash = strrchr(dump_prestate_path, '/');
                if (last_slash) {
                    size_t dir_len = (size_t)(last_slash - dump_prestate_path);
                    snprintf(env_path, sizeof(env_path), "%.*s/env.json",
                             (int)dir_len, dump_prestate_path);
                } else {
                    snprintf(env_path, sizeof(env_path), "env.json");
                }

                FILE *ef = fopen(env_path, "w");
                if (ef) {
                    fprintf(ef, "{\n");
                    fprintf(ef, "  \"currentCoinbase\": \"0x");
                    for (int j = 0; j < 20; j++) fprintf(ef, "%02x", header.coinbase.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"currentDifficulty\": \"0x%lx\",\n",
                            uint256_to_uint64(&header.difficulty));
                    fprintf(ef, "  \"currentGasLimit\": \"0x%lx\",\n", header.gas_limit);
                    fprintf(ef, "  \"currentNumber\": \"0x%lx\",\n", header.number);
                    fprintf(ef, "  \"currentTimestamp\": \"0x%lx\",\n", header.timestamp);
                    fprintf(ef, "  \"parentHash\": \"0x");
                    for (int j = 0; j < 32; j++) fprintf(ef, "%02x", header.parent_hash.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"blockHashes\": {\n");
                    bool first_bh = true;
                    uint64_t start_bh = bn > 256 ? bn - 256 : 1;
                    for (uint64_t bh_num = start_bh; bh_num < bn; bh_num++) {
                        hash_t bh;
                        if (sync_get_block_hash(sync, bh_num, &bh)) {
                            if (!first_bh) fprintf(ef, ",\n");
                            first_bh = false;
                            fprintf(ef, "    \"%lu\": \"0x", bh_num);
                            for (int j = 0; j < 32; j++) fprintf(ef, "%02x", bh.bytes[j]);
                            fprintf(ef, "\"");
                        }
                    }
                    fprintf(ef, "\n  }\n}\n");
                    fclose(ef);
                    fprintf(stderr, "Environment dumped to %s\n", env_path);
                }
            }

            free(addrs);
            free(addr_slots);
            free(slot_buf);
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        window_txs += result.tx_count;
        window_transfers += result.transfer_count;
        window_calls += result.call_count;
        window_gas += result.gas_used;


        /* Progress every CHECKPOINT_INTERVAL blocks, on failure, or last block */
        if (bn % CHECKPOINT_INTERVAL == 0 || !result.ok || bn == end_block) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                             (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
            double win_secs = (t_now.tv_sec - t_window.tv_sec) +
                              (t_now.tv_nsec - t_window.tv_nsec) / 1e9;
            double bps = CHECKPOINT_INTERVAL / (win_secs > 0 ? win_secs : 1);
            double tps = window_txs / (win_secs > 0 ? win_secs : 1);
            double mgps = (window_gas / 1e6) / (win_secs > 0 ? win_secs : 1);
            evm_state_stats_t ss = sync_get_state_stats(sync);
            sync_history_stats_t hs = sync_get_history_stats(sync);
            size_t rss_mb = get_rss_kb() / 1024;
            sync_status_t st = sync_get_status(sync);

            {
                printf("Block %lu | %lu txs (%luT %luC) | %.0f tps | %.1f Mgas/s | %.0f blk/s | %.1fs/256blk\n",
                       bn, window_txs, window_transfers, window_calls,
                       tps, mgps, bps, win_secs);

                /* Get detailed memory breakdown directly from state */
                state_t *_st = evm_state_get_state(sync_get_state(sync));
                state_stats_t ms = _st ? state_get_stats(_st) : (state_stats_t){0};

                printf("  └ %uK live (%uK slots, %uK free) %uK res (%uK free) | vec=%zuMB res=%zuMB idx=%zuMB | tracked=%zuMB RSS=%zuMB\n",
                       ms.account_live / 1000,
                       ms.account_count / 1000,
                       ms.acct_free_count / 1000,
                       ms.storage_account_count / 1000,
                       ms.res_free_count / 1000,
                       ms.acct_vec_bytes / (1024*1024),
                       ms.res_vec_bytes / (1024*1024),
                       ms.acct_arena_bytes / (1024*1024),
                       ms.total_tracked / (1024*1024),
                       rss_mb);
                {
                    double root_ms = ss.wait_flush_ms;
                    double evm_ms = ss.exec_ms - root_ms;
                    double other_ms = win_secs * 1000.0 - ss.exec_ms;
                    printf("  └ evm=%.0fms  root=%.0fms  other=%.0fms\n",
                           evm_ms > 0 ? evm_ms : 0,
                           root_ms,
                           other_ms > 0 ? other_ms : 0);
                }
            }
            window_txs = 0;
            window_gas = 0;
            window_transfers = 0;
            window_calls = 0;
            clock_gettime(CLOCK_MONOTONIC, &t_window);
        }

        /* .meta writes DISABLED — storage_hart2 uses state_save/load
         * snapshots; state_reconstruct/mpt_store pipeline is no longer
         * used, so the .meta file (last-good-checkpoint pointer) is
         * dead weight. */

        /* SIGINT: exit after validated checkpoint */
        if (g_shutdown_pending && result.ok &&
            bn % CHECKPOINT_INTERVAL == 0) {
            LOG_INFO("Checkpoint validated at block %lu - exiting.", bn);
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        /* --save-state: save full state after target block */
        if (bn == save_state_block) {
            state_t *st = evm_state_get_state(sync_get_state(sync));
            char default_save[256];
            const char *sp = save_state_path;
            if (!sp) {
                snprintf(default_save, sizeof(default_save),
                         "state_%lu.bin", bn);
                sp = default_save;
            }
            fprintf(stderr, "Saving state at block %lu to %s...\n", bn, sp);
            if (state_save(st, sp, &header.state_root)) {
                state_stats_t ss = state_get_stats(st);
                fprintf(stderr, "  saved: %u accounts, %.0fMB tracked\n",
                        ss.account_count, ss.total_tracked / 1e6);
            } else {
                fprintf(stderr, "  FAILED to save state!\n");
            }
        }

        /* --snapshot-every: validate root + save state at regular intervals.
         * Only saves if root matches — every snapshot on disk is guaranteed correct.
         * If root mismatches, stop — bug is in the last snapshot_every blocks.
         * Aligned to checkpoint boundaries so incremental root is valid. */
        if (snapshot_every > 0 && bn % validate_every == 0 &&
            bn >= snapshot_every &&
            (bn / snapshot_every) > ((bn - validate_every) / snapshot_every)) {
            evm_state_t *snap_es = sync_get_state(sync);
            state_t *st = evm_state_get_state(snap_es);
            bool prune = (bn >= 2675000);

            /* Force full recomputation — no stale cached hashes */
            evm_state_invalidate_all(snap_es);

            struct timespec _sr0, _sr1;
            clock_gettime(CLOCK_MONOTONIC, &_sr0);
            hash_t snap_root = evm_state_compute_mpt_root(snap_es, prune);
            clock_gettime(CLOCK_MONOTONIC, &_sr1);
            double sr_ms = (_sr1.tv_sec - _sr0.tv_sec) * 1000.0 +
                           (_sr1.tv_nsec - _sr0.tv_nsec) / 1e6;

            if (memcmp(snap_root.bytes, header.state_root.bytes, 32) != 0) {
                char got_hex[67], exp_hex[67];
                hash_to_hex(&snap_root, got_hex);
                hash_to_hex(&header.state_root, exp_hex);
                LOG_ERROR("Snapshot validation FAILED at block %lu (%.0fms)", bn, sr_ms);
                LOG_ERROR("got:      %s", got_hex);
                LOG_ERROR("expected: %s", exp_hex);
                LOG_WARN("hint: bug is in blocks %lu..%lu",
                         bn > snapshot_every ? bn - snapshot_every + 1 : 1, bn);
                block_body_free(&body);
                free(hdr_rlp);
                free(body_rlp);
                break;
            }

            char snap_path[256];
            snprintf(snap_path, sizeof(snap_path), "%s/state_%lu.bin",
                     data_dir, bn);
            if (state_save(st, snap_path, &snap_root)) {
                state_stats_t ss = state_get_stats(st);
                fprintf(stderr, "  snapshot @%lu: root OK, %u accts, %.0fMB, %.0fms → %s\n",
                        bn, ss.account_count, ss.total_tracked / 1e6, sr_ms, snap_path);
            }
        }

        bool block_ok = result.ok;

        block_body_free(&body);
        free(hdr_rlp);
        free(body_rlp);

        if (!block_ok) {
            if (result.error == SYNC_GAS_MISMATCH) {
                LOG_ERROR("Block %lu: GAS MISMATCH  got %lu  expected %lu  diff %+ld",
                        bn, result.actual_gas, result.expected_gas,
                        (long)result.actual_gas - (long)result.expected_gas);
                LOG_WARN("hint: gas diff usually means opcode cost bug or missing gas rule");

                /* Log our per-tx gas for comparison against era1 expected */
                if (result.receipts && result.receipt_count > 0) {
                    LOG_INFO("per-tx gas (%zu txs):", result.receipt_count);
                    for (size_t ti = 0; ti < result.receipt_count; ti++) {
                        LOG_INFO("  tx %3zu: gas=%7lu  cum=%lu  status=%u",
                                ti, result.receipts[ti].gas_used,
                                result.receipts[ti].cumulative_gas,
                                result.receipts[ti].status_code);
                    }
                    /* Free receipts */
                    for (size_t ti = 0; ti < result.receipt_count; ti++) {
                        if (result.receipts[ti].logs)
                            free(result.receipts[ti].logs);
                    }
                    free(result.receipts);
                    result.receipts = NULL;
                }
            } else if (result.error == SYNC_ROOT_MISMATCH) {
                char got_hex[67], exp_hex[67];
                hash_to_hex(&result.actual_root, got_hex);
                hash_to_hex(&result.expected_root, exp_hex);
                LOG_ERROR("Block %lu: STATE ROOT MISMATCH (at batch checkpoint)", bn);
                LOG_ERROR("got:      %s", got_hex);
                LOG_ERROR("expected: %s", exp_hex);
                LOG_WARN("hint: root is validated every %u blocks - bug is in range [%lu..%lu]",
                        validate_every,
                        bn > validate_every ? bn - validate_every + 1 : 1, bn);
                LOG_WARN("hint: if resumed from checkpoint, try --clean to replay fresh");
                LOG_WARN("hint: use evm_statetest for per-block differential fuzzing against geth");
#ifdef ENABLE_DEBUG
                evm_state_debug_dump(sync_get_state(sync));
#endif
            }

            /* .meta write DISABLED — see note at per-block write site above. */
            if (0) {
                uint64_t safe_block = ((bn - 1) / CHECKPOINT_INTERVAL) * CHECKPOINT_INTERVAL;
                bool pe = (safe_block >= 2675000);
                uint8_t safe_root[32] = {0};
                if (archive_ensure(&archive, safe_block, false, era1_dir)) {
                    uint8_t *sr_hdr = NULL; size_t sr_hlen = 0;
                    uint8_t *sr_body = NULL; size_t sr_blen = 0;
                    if (era1_read_block(&archive.current, safe_block,
                                         &sr_hdr, &sr_hlen, &sr_body, &sr_blen)) {
                        block_header_t sr_header;
                        if (block_header_decode_rlp(&sr_header, sr_hdr, sr_hlen))
                            memcpy(safe_root, sr_header.state_root.bytes, 32);
                        free(sr_hdr); free(sr_body);
                    }
                }
                char meta_path[512];
                snprintf(meta_path, sizeof(meta_path), "%s.meta", mpt_path);
                if (meta_write(meta_path, safe_block, safe_root, pe)) {
                    LOG_WARN("wrote %s — last good checkpoint: block %lu", meta_path, safe_block);
                    LOG_WARN("to debug: state_reconstruct --resume to block %lu, "
                             "then replay blocks %lu..%lu with per-block validation",
                             safe_block, safe_block + 1, bn);
                }
            }

            /* Auto-dump pre/post state for the failing block */
            {
                char auto_dir[512];
                snprintf(auto_dir, sizeof(auto_dir), "known_issues/block_%lu", bn);
                mkdir("known_issues", 0755);
                mkdir(auto_dir, 0755);
                LOG_INFO("auto-dumping pre/post state to %s/ ...", auto_dir);

                evm_state_t *es = sync_get_state(sync);

#ifdef ENABLE_HISTORY
                /* pre_alloc.json (original values) + post_alloc.json (current values)
                 * with debug flags on each account */
                evm_state_dump_debug(es, auto_dir);
#endif

                /* Write env.json */
                char env_p[512];
                snprintf(env_p, sizeof(env_p), "%s/env.json", auto_dir);
                FILE *ef = fopen(env_p, "w");
                if (ef) {
                    fprintf(ef, "{\n");
                    fprintf(ef, "  \"currentCoinbase\": \"0x");
                    for (int j = 0; j < 20; j++) fprintf(ef, "%02x", header.coinbase.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"currentDifficulty\": \"0x%lx\",\n",
                            uint256_to_uint64(&header.difficulty));
                    fprintf(ef, "  \"currentGasLimit\": \"0x%lx\",\n", header.gas_limit);
                    fprintf(ef, "  \"currentNumber\": \"0x%lx\",\n", header.number);
                    fprintf(ef, "  \"currentTimestamp\": \"0x%lx\",\n", header.timestamp);
                    fprintf(ef, "  \"parentHash\": \"0x");
                    for (int j = 0; j < 32; j++) fprintf(ef, "%02x", header.parent_hash.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"blockHashes\": {\n");
                    bool fbh = true;
                    uint64_t sbh = bn > 256 ? bn - 256 : 1;
                    for (uint64_t bhn = sbh; bhn < bn; bhn++) {
                        hash_t bh;
                        if (sync_get_block_hash(sync, bhn, &bh)) {
                            if (!fbh) fprintf(ef, ",\n");
                            fbh = false;
                            fprintf(ef, "    \"%lu\": \"0x", bhn);
                            for (int j = 0; j < 32; j++) fprintf(ef, "%02x", bh.bytes[j]);
                            fprintf(ef, "\"");
                        }
                    }
                    fprintf(ef, "\n  }\n}\n");
                    fclose(ef);
                }

            }

            /* Discard dirty state so destroy doesn't flush failing block to MPT.
             * MPT on disk stays at last good checkpoint C. */
            evm_state_discard_pending(sync_get_state(sync));

            LOG_ERROR("First failure at block %lu - stopping.", bn);
            break;
        }
    }

    /* --no-validate: compute and verify root at the last processed block.
     * Skip if no blocks were actually executed (e.g. loaded snapshot at Paris).
     *
     * DISABLED: skip the final root+save. Era-exhaustion path (line ~1030)
     * already computes root and saves when the replay naturally finishes,
     * which is the case we actually care about. This block otherwise runs
     * on every Ctrl-C exit and costs ~15+ minutes at 24M-scale. */
    if (false && no_validate && sync_get_status(sync).blocks_ok > 0) {
        sync_status_t fst = sync_get_status(sync);
        uint64_t last = fst.last_block;
        if (last > 0) {
            fprintf(stderr, "\nComputing final state root at block %lu...\n", last);
            evm_state_t *es = sync_get_state(sync);
            evm_state_invalidate_all(es);
            bool prune = (last >= 2675000);
            struct timespec _fr0, _fr1;
            clock_gettime(CLOCK_MONOTONIC, &_fr0);
            hash_t actual = evm_state_compute_mpt_root(es, prune);
            clock_gettime(CLOCK_MONOTONIC, &_fr1);
            double fr_ms = (_fr1.tv_sec - _fr0.tv_sec) * 1000.0 +
                           (_fr1.tv_nsec - _fr0.tv_nsec) / 1e6;

            /* Read expected root from era1 header */
            hash_t expected = {0};
            if (archive_ensure(&archive, last, false, era1_dir)) {
                uint8_t *h_rlp = NULL; size_t h_len = 0;
                uint8_t *b_rlp = NULL; size_t b_len = 0;
                if (era1_read_block(&archive.current, last,
                                     &h_rlp, &h_len, &b_rlp, &b_len)) {
                    block_header_t hdr;
                    if (block_header_decode_rlp(&hdr, h_rlp, h_len))
                        expected = hdr.state_root;
                    free(h_rlp); free(b_rlp);
                }
            }

            char got_hex[67], exp_hex[67];
            hash_to_hex(&actual, got_hex);
            hash_to_hex(&expected, exp_hex);

            if (memcmp(actual.bytes, expected.bytes, 32) == 0) {
                fprintf(stderr, "  PASS: root matches at block %lu (%.0fms)\n",
                        last, fr_ms);
                /* Save final snapshot — starting point for CL sync */
                state_t *st = evm_state_get_state(es);
                char final_path[256];
                snprintf(final_path, sizeof(final_path), "%s/state_%lu.bin",
                         data_dir, last);
                if (state_save(st, final_path, &actual)) {
                    state_stats_t ss = state_get_stats(st);
                    fprintf(stderr, "  saved final snapshot: %u accts, %.0fMB → %s\n",
                            ss.account_count, ss.total_tracked / 1e6, final_path);
                }
            } else {
                fprintf(stderr, "  FAIL: root mismatch at block %lu (%.0fms)\n",
                        last, fr_ms);
                fprintf(stderr, "  got:      %s\n", got_hex);
                fprintf(stderr, "  expected: %s\n", exp_hex);
            }
        }
    }

    /* Block SIGINT during cleanup — flush must complete cleanly */
    sigset_t block_set, old_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGINT);
    sigprocmask(SIG_BLOCK, &block_set, &old_set);

    clock_gettime(CLOCK_MONOTONIC, &t_now);
    double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                     (t_now.tv_nsec - t_start.tv_nsec) / 1e9;

    sync_status_t st = sync_get_status(sync);

    {
        printf("\n===== Summary =====\n");
        printf("Blocks OK:     %lu\n", st.blocks_ok);
        printf("Blocks failed: %lu\n", st.blocks_fail);
        printf("Total gas:     %lu\n", st.total_gas);
        printf("Elapsed:       %.1f s\n", elapsed);
        printf("Speed:         %.0f blk/s\n",
               (st.blocks_ok + st.blocks_fail) / (elapsed > 0 ? elapsed : 1));
    }

    /* Stop prefetch and destroy sync (flushes + msyncs all data to disk) */
    prefetch_stop(&prefetch);
    sync_ensure_flushed(sync);
    sync_destroy(sync);

    /* .meta write DISABLED — storage_hart2 snapshots replace this. */
    archive_close(&archive);

    return st.blocks_fail > 0 ? 1 : 0;
}
