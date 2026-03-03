#include "verkle_journal.h"
#include "verkle_snapshot.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

static const uint8_t FWD_MAGIC[8] = {'V','R','K','L','J','R','N','L'};
static const uint32_t FWD_VERSION = 1;

#define BLOCK_BEGIN_TAG  0xBB
#define BLOCK_COMMIT_TAG 0xBC

#define INITIAL_ENTRY_CAP 1024
#define INITIAL_BLOCK_CAP 64

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

verkle_journal_t *verkle_journal_create(verkle_tree_t *tree)
{
    if (!tree) return NULL;

    verkle_journal_t *j = calloc(1, sizeof(verkle_journal_t));
    if (!j) return NULL;

    j->tree = tree;
    j->fwd_fd = -1;
    j->ckpt_pid = -1;

    j->entries = malloc(INITIAL_ENTRY_CAP * sizeof(vj_entry_t));
    if (!j->entries) { free(j); return NULL; }
    j->entry_cap = INITIAL_ENTRY_CAP;

    j->blocks = malloc(INITIAL_BLOCK_CAP * sizeof(vj_block_t));
    if (!j->blocks) { free(j->entries); free(j); return NULL; }
    j->block_cap = INITIAL_BLOCK_CAP;

    return j;
}

void verkle_journal_destroy(verkle_journal_t *j)
{
    if (!j) return;

    if (j->fwd_fd >= 0) close(j->fwd_fd);
    free(j->fwd_path);
    free(j->entries);
    free(j->blocks);

    /* If a checkpoint child is still running, wait for it */
    if (j->ckpt_pid > 0) {
        waitpid(j->ckpt_pid, NULL, 0);
    }

    free(j);
}

/* =========================================================================
 * Block Operations
 * ========================================================================= */

bool verkle_journal_begin_block(verkle_journal_t *j, uint64_t block_number)
{
    if (!j || j->block_active) return false;

    /* Grow blocks array if needed */
    if (j->block_count >= j->block_cap) {
        uint32_t new_cap = j->block_cap * 2;
        vj_block_t *nb = realloc(j->blocks, new_cap * sizeof(vj_block_t));
        if (!nb) return false;
        j->blocks = nb;
        j->block_cap = new_cap;
    }

    j->blocks[j->block_count].start_index = j->entry_count;
    j->blocks[j->block_count].block_number = block_number;
    j->block_count++;
    j->block_active = true;
    return true;
}

bool verkle_journal_set(verkle_journal_t *j,
                        const uint8_t key[VERKLE_KEY_LEN],
                        const uint8_t value[VERKLE_VALUE_LEN])
{
    if (!j || !j->block_active) return false;

    /* Grow entries array if needed */
    if (j->entry_count >= j->entry_cap) {
        uint32_t new_cap = j->entry_cap * 2;
        vj_entry_t *ne = realloc(j->entries, new_cap * sizeof(vj_entry_t));
        if (!ne) return false;
        j->entries = ne;
        j->entry_cap = new_cap;
    }

    /* Record pre-mutation state */
    vj_entry_t *e = &j->entries[j->entry_count];
    memcpy(e->key, key, VERKLE_KEY_LEN);
    e->had_value = verkle_get(j->tree, key, e->old_value);
    if (!e->had_value)
        memset(e->old_value, 0, VERKLE_VALUE_LEN);
    j->entry_count++;

    /* Apply the mutation */
    return verkle_set(j->tree, key, value);
}

/* =========================================================================
 * Forward Journal Write Helpers
 * ========================================================================= */

static bool fwd_write_all(int fd, const void *data, size_t len)
{
    const uint8_t *p = data;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) return false;
        p += n;
        len -= (size_t)n;
    }
    return true;
}

static bool fwd_write_block(verkle_journal_t *j, uint32_t blk_idx)
{
    if (j->fwd_fd < 0) return true; /* forward journal not enabled */

    vj_block_t *blk = &j->blocks[blk_idx];
    uint32_t start = blk->start_index;
    uint32_t end = (blk_idx + 1 < j->block_count)
                   ? j->blocks[blk_idx + 1].start_index
                   : j->entry_count;
    uint32_t count = end - start;

    /* BLOCK_BEGIN: tag + block_number + entry_count */
    uint8_t tag = BLOCK_BEGIN_TAG;
    if (!fwd_write_all(j->fwd_fd, &tag, 1)) return false;
    if (!fwd_write_all(j->fwd_fd, &blk->block_number, 8)) return false;
    if (!fwd_write_all(j->fwd_fd, &count, 4)) return false;

    /* Entries: read current value from tree */
    for (uint32_t i = start; i < end; i++) {
        vj_entry_t *e = &j->entries[i];
        uint8_t current[VERKLE_VALUE_LEN];

        if (!fwd_write_all(j->fwd_fd, e->key, VERKLE_KEY_LEN))
            return false;

        if (verkle_get(j->tree, e->key, current)) {
            if (!fwd_write_all(j->fwd_fd, current, VERKLE_VALUE_LEN))
                return false;
        } else {
            /* Key was unset (shouldn't happen after a set, but handle it) */
            memset(current, 0, VERKLE_VALUE_LEN);
            if (!fwd_write_all(j->fwd_fd, current, VERKLE_VALUE_LEN))
                return false;
        }
    }

    /* BLOCK_COMMIT: tag + block_number */
    tag = BLOCK_COMMIT_TAG;
    if (!fwd_write_all(j->fwd_fd, &tag, 1)) return false;
    if (!fwd_write_all(j->fwd_fd, &blk->block_number, 8)) return false;

    return true;
}

/* =========================================================================
 * Commit / Revert
 * ========================================================================= */

bool verkle_journal_commit_block(verkle_journal_t *j)
{
    if (!j || !j->block_active) return false;

    /* Write to forward journal if enabled */
    uint32_t blk_idx = j->block_count - 1;
    if (!fwd_write_block(j, blk_idx))
        return false;

    j->block_active = false;
    return true;
}

bool verkle_journal_revert_block(verkle_journal_t *j)
{
    if (!j || j->block_count == 0) return false;

    /* Can revert the active (uncommitted) block too */
    vj_block_t *blk = &j->blocks[j->block_count - 1];
    uint32_t start = blk->start_index;

    /* Replay in reverse */
    for (uint32_t i = j->entry_count; i > start; i--) {
        vj_entry_t *e = &j->entries[i - 1];
        if (e->had_value) {
            verkle_set(j->tree, e->key, e->old_value);
        } else {
            verkle_unset(j->tree, e->key);
        }
    }

    j->entry_count = start;
    j->block_count--;
    j->block_active = false;
    return true;
}

void verkle_journal_trim(verkle_journal_t *j, uint64_t up_to_block)
{
    if (!j || j->block_count == 0) return;

    /* Find the first block to keep */
    uint32_t keep_from = 0;
    for (uint32_t i = 0; i < j->block_count; i++) {
        if (j->blocks[i].block_number > up_to_block) {
            keep_from = i;
            goto found;
        }
    }
    /* All blocks trimmed */
    j->entry_count = 0;
    j->block_count = 0;
    return;

found:;
    uint32_t entry_start = j->blocks[keep_from].start_index;
    uint32_t entries_to_keep = j->entry_count - entry_start;
    uint32_t blocks_to_keep = j->block_count - keep_from;

    if (entry_start > 0)
        memmove(j->entries, j->entries + entry_start,
                entries_to_keep * sizeof(vj_entry_t));
    j->entry_count = entries_to_keep;

    /* Shift block markers and fix start_index */
    for (uint32_t i = 0; i < blocks_to_keep; i++) {
        j->blocks[i] = j->blocks[keep_from + i];
        j->blocks[i].start_index -= entry_start;
    }
    j->block_count = blocks_to_keep;
}

/* =========================================================================
 * Forward Journal — Enable / Replay
 * ========================================================================= */

bool verkle_journal_enable_fwd(verkle_journal_t *j, const char *path,
                               uint32_t snapshot_block)
{
    if (!j || !path) return false;

    /* Close previous if any */
    if (j->fwd_fd >= 0) {
        close(j->fwd_fd);
        j->fwd_fd = -1;
    }
    free(j->fwd_path);
    j->fwd_path = NULL;

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;

    /* Write header */
    uint32_t version = FWD_VERSION;
    if (!fwd_write_all(fd, FWD_MAGIC, 8)) { close(fd); return false; }
    if (!fwd_write_all(fd, &version, 4)) { close(fd); return false; }
    if (!fwd_write_all(fd, &snapshot_block, 4)) { close(fd); return false; }

    j->fwd_fd = fd;
    j->fwd_path = strdup(path);
    return true;
}

/* Read helpers for replay */
static bool fwd_read_all(FILE *f, void *out, size_t len)
{
    return fread(out, 1, len, f) == len;
}

bool verkle_journal_replay_fwd(const char *path, verkle_tree_t *tree,
                               uint64_t *last_block_out)
{
    if (!path || !tree) return false;

    FILE *f = fopen(path, "rb");
    if (!f) return false;

    /* Verify header */
    uint8_t magic[8];
    uint32_t version, snap_block;

    if (!fwd_read_all(f, magic, 8)) { fclose(f); return false; }
    if (memcmp(magic, FWD_MAGIC, 8) != 0) { fclose(f); return false; }
    if (!fwd_read_all(f, &version, 4)) { fclose(f); return false; }
    if (version != FWD_VERSION) { fclose(f); return false; }
    if (!fwd_read_all(f, &snap_block, 4)) { fclose(f); return false; }

    uint64_t last_block = 0;
    bool has_block = false;

    /* Read blocks until EOF */
    while (1) {
        /* Try to read BLOCK_BEGIN */
        uint8_t tag;
        if (!fwd_read_all(f, &tag, 1)) break; /* EOF = done */
        if (tag != BLOCK_BEGIN_TAG) break;      /* corrupt = stop */

        uint64_t block_number;
        uint32_t entry_count;
        if (!fwd_read_all(f, &block_number, 8)) break;
        if (!fwd_read_all(f, &entry_count, 4)) break;

        /* Read all entries into a temp buffer before applying
         * (so incomplete blocks are discarded atomically) */
        uint8_t (*keys)[VERKLE_KEY_LEN] = NULL;
        uint8_t (*vals)[VERKLE_VALUE_LEN] = NULL;
        bool ok = true;

        if (entry_count > 0) {
            keys = malloc(entry_count * VERKLE_KEY_LEN);
            vals = malloc(entry_count * VERKLE_VALUE_LEN);
            if (!keys || !vals) { free(keys); free(vals); break; }

            for (uint32_t i = 0; i < entry_count; i++) {
                if (!fwd_read_all(f, keys[i], VERKLE_KEY_LEN) ||
                    !fwd_read_all(f, vals[i], VERKLE_VALUE_LEN)) {
                    ok = false;
                    break;
                }
            }
        }

        if (!ok) { free(keys); free(vals); break; }

        /* Read BLOCK_COMMIT */
        uint64_t commit_block;
        if (!fwd_read_all(f, &tag, 1) || tag != BLOCK_COMMIT_TAG ||
            !fwd_read_all(f, &commit_block, 8) ||
            commit_block != block_number) {
            /* Incomplete block — discard */
            free(keys);
            free(vals);
            break;
        }

        /* Apply all entries */
        for (uint32_t i = 0; i < entry_count; i++) {
            verkle_set(tree, keys[i], vals[i]);
        }

        free(keys);
        free(vals);

        last_block = block_number;
        has_block = true;
    }

    fclose(f);

    if (last_block_out) *last_block_out = last_block;
    return has_block || true; /* success even if no blocks replayed */
}

/* =========================================================================
 * Background Checkpoint
 * ========================================================================= */

bool verkle_journal_checkpoint_start(verkle_journal_t *j,
                                     const char *snapshot_path)
{
    if (!j || !snapshot_path) return false;
    if (j->ckpt_pid > 0) return false; /* already in progress */

    /* Record the latest committed block number for trim after completion */
    if (j->block_count > 0) {
        uint32_t last_idx = j->block_active
            ? (j->block_count > 1 ? j->block_count - 2 : 0)
            : j->block_count - 1;
        j->ckpt_block = (uint32_t)j->blocks[last_idx].block_number;
    } else {
        j->ckpt_block = 0;
    }

    pid_t pid = fork();
    if (pid < 0) return false;

    if (pid == 0) {
        /* === CHILD PROCESS === */
        /* Close the forward journal fd — parent owns it */
        if (j->fwd_fd >= 0) {
            close(j->fwd_fd);
            j->fwd_fd = -1;
        }

        /* Build temp path */
        char tmp_path[4096];
        snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%d",
                 snapshot_path, getpid());

        bool ok = verkle_snapshot_save(j->tree, tmp_path);
        if (ok) {
            /* fsync the snapshot file */
            int fd = open(tmp_path, O_RDONLY);
            if (fd >= 0) {
                fsync(fd);
                close(fd);
            }
            /* Atomic rename */
            ok = (rename(tmp_path, snapshot_path) == 0);
        }

        if (!ok) unlink(tmp_path);
        _exit(ok ? 0 : 1);
    }

    /* === PARENT PROCESS === */
    j->ckpt_pid = pid;
    return true;
}

bool verkle_journal_checkpoint_poll(verkle_journal_t *j, bool *success)
{
    if (!j || j->ckpt_pid <= 0) return false;

    int status;
    pid_t result = waitpid(j->ckpt_pid, &status, WNOHANG);
    if (result == 0) return false; /* still running */

    j->ckpt_pid = -1;

    bool ok = (result > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    if (success) *success = ok;

    if (ok) {
        /* Truncate forward journal with updated snapshot block */
        if (j->fwd_path && j->fwd_fd >= 0) {
            close(j->fwd_fd);
            j->fwd_fd = -1;
            verkle_journal_enable_fwd(j, j->fwd_path, j->ckpt_block);
        }
        /* Trim in-memory entries */
        verkle_journal_trim(j, j->ckpt_block);
    }

    return true;
}

bool verkle_journal_checkpoint_wait(verkle_journal_t *j)
{
    if (!j || j->ckpt_pid <= 0) return false;

    int status;
    waitpid(j->ckpt_pid, &status, 0);
    j->ckpt_pid = -1;

    bool ok = (WIFEXITED(status) && WEXITSTATUS(status) == 0);

    if (ok) {
        if (j->fwd_path && j->fwd_fd >= 0) {
            close(j->fwd_fd);
            j->fwd_fd = -1;
            verkle_journal_enable_fwd(j, j->fwd_path, j->ckpt_block);
        }
        verkle_journal_trim(j, j->ckpt_block);
    }

    return ok;
}
