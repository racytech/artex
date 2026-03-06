#ifndef VERKLE_JOURNAL_H
#define VERKLE_JOURNAL_H

#include "verkle.h"
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Journal — Block Revert + Crash Recovery + Background Checkpoint
 *
 * Three components:
 *   1. In-memory undo journal — records old values before each verkle_set,
 *      enables block revert by replaying in reverse.
 *   2. Forward journal file — appends new values to disk after commit_block
 *      (no fsync, just write()). On crash: load snapshot + replay forward.
 *   3. Background checkpoint — fork() for consistent snapshot via CoW,
 *      parent continues executing without blocking.
 */

/* =========================================================================
 * Data Structures
 * ========================================================================= */

/** Single undo entry — pre-mutation state of one verkle_set. */
typedef struct {
    uint8_t key[VERKLE_KEY_LEN];
    uint8_t old_value[VERKLE_VALUE_LEN];
    bool    had_value;
} vj_entry_t;

/** Block marker — separates blocks' entries. */
typedef struct {
    uint32_t start_index;
    uint64_t block_number;
} vj_block_t;

/** Journal handle. */
typedef struct {
    verkle_tree_t *tree;        /* NOT owned */

    vj_entry_t    *entries;
    uint32_t       entry_count;
    uint32_t       entry_cap;

    vj_block_t    *blocks;
    uint32_t       block_count;
    uint32_t       block_cap;

    bool           block_active;

    int            fwd_fd;      /* forward journal fd, -1 if disabled */
    char          *fwd_path;

    pid_t          ckpt_pid;    /* checkpoint child, -1 if none */
    uint32_t       ckpt_block;  /* block number at checkpoint start */
} verkle_journal_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

verkle_journal_t *verkle_journal_create(verkle_tree_t *tree);
void              verkle_journal_destroy(verkle_journal_t *j);

/* =========================================================================
 * Block Operations
 * ========================================================================= */

/** Begin a new block. Must be called before verkle_journal_set(). */
bool verkle_journal_begin_block(verkle_journal_t *j, uint64_t block_number);

/** Commit the current block. Writes forward journal if enabled. */
bool verkle_journal_commit_block(verkle_journal_t *j);

/** Journaled set — records old value, then calls verkle_set. */
bool verkle_journal_set(verkle_journal_t *j,
                        const uint8_t key[VERKLE_KEY_LEN],
                        const uint8_t value[VERKLE_VALUE_LEN]);

/** Revert the most recent committed block. */
bool verkle_journal_revert_block(verkle_journal_t *j);

/** Discard journal entries for blocks up to (and including) up_to_block. */
void verkle_journal_trim(verkle_journal_t *j, uint64_t up_to_block);

/* =========================================================================
 * Forward Journal (Crash Recovery)
 * ========================================================================= */

/** Enable forward journal. Creates/truncates the file with a header. */
bool verkle_journal_enable_fwd(verkle_journal_t *j, const char *path,
                               uint32_t snapshot_block);

/** Replay a forward journal file onto a tree. Returns last replayed block
 *  number via last_block_out. Discards any incomplete trailing block. */
bool verkle_journal_replay_fwd(const char *path, verkle_tree_t *tree,
                               uint64_t *last_block_out);

/* =========================================================================
 * Background Checkpoint (fork-based)
 * ========================================================================= */

/** Start a background snapshot via fork(). Non-blocking.
 *  Returns false if a checkpoint is already in progress. */
bool verkle_journal_checkpoint_start(verkle_journal_t *j,
                                     const char *snapshot_path);

/** Poll whether the background checkpoint finished. Non-blocking.
 *  Returns true if done, sets *success. */
bool verkle_journal_checkpoint_poll(verkle_journal_t *j, bool *success);

/** Block until the current checkpoint completes.
 *  Returns true if checkpoint succeeded. */
bool verkle_journal_checkpoint_wait(verkle_journal_t *j);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_JOURNAL_H */
