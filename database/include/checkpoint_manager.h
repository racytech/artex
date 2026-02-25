/**
 * Checkpoint Manager - Background Checkpointing for Persistent ART
 *
 * Runs a background thread that periodically calls data_art_checkpoint()
 * to save the mmap header and sync to disk.
 */

#ifndef CHECKPOINT_MANAGER_H
#define CHECKPOINT_MANAGER_H

#include <stdint.h>
#include <stdbool.h>

// Forward declaration
typedef struct data_art_tree data_art_tree_t;
typedef struct checkpoint_manager checkpoint_manager_t;

typedef struct {
    uint32_t check_interval_ms;  // Poll interval in ms (default: 1000)
} checkpoint_manager_config_t;

/**
 * Get default configuration
 * @return Default config (check_interval_ms = 1000)
 */
checkpoint_manager_config_t checkpoint_manager_default_config(void);

/**
 * Create a checkpoint manager
 *
 * @param tree Tree instance
 * @param config Configuration (NULL for defaults)
 * @return Manager instance, or NULL on failure
 */
checkpoint_manager_t *checkpoint_manager_create(data_art_tree_t *tree,
                                                const checkpoint_manager_config_t *config);

/**
 * Destroy checkpoint manager
 * Stops background thread if running, then frees resources.
 */
void checkpoint_manager_destroy(checkpoint_manager_t *mgr);

/**
 * Start the background checkpoint thread
 * @return true on success, false if already running or thread creation fails
 */
bool checkpoint_manager_start(checkpoint_manager_t *mgr);

/**
 * Stop the background checkpoint thread
 * Sets stop flag and joins the thread. Safe to call if not running.
 */
void checkpoint_manager_stop(checkpoint_manager_t *mgr);

/**
 * Force an immediate checkpoint
 * Wakes the background thread to run a checkpoint now.
 * Blocks until the checkpoint completes.
 *
 * @return true if checkpoint succeeded, false on error
 */
bool checkpoint_manager_force(checkpoint_manager_t *mgr);

/**
 * Get checkpoint statistics
 *
 * @param mgr Manager instance
 * @param checkpoints_completed Output: number of checkpoints done (can be NULL)
 * @param segments_truncated Output: unused (kept for API compat, can be NULL)
 */
void checkpoint_manager_get_stats(const checkpoint_manager_t *mgr,
                                  uint64_t *checkpoints_completed,
                                  uint64_t *segments_truncated);

#endif // CHECKPOINT_MANAGER_H
