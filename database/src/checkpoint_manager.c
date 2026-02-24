/**
 * Checkpoint Manager - Background Checkpointing for Persistent ART
 *
 * Background thread that periodically runs the checkpoint sequence:
 *   wal_should_checkpoint() → data_art_checkpoint() →
 *   wal_checkpoint_completed() → wal_truncate()
 */

#include "checkpoint_manager.h"
#include "data_art.h"
#include "wal.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>

struct checkpoint_manager {
    data_art_tree_t *tree;
    checkpoint_manager_config_t config;

    pthread_t thread;
    volatile bool running;
    volatile bool stop;
    volatile bool force_requested;

    // Stats
    uint64_t checkpoints_completed;
    uint64_t segments_truncated;

    // Synchronization
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    pthread_cond_t  done_cond;  // Signaled after force checkpoint completes
};

// Run one checkpoint cycle. Returns true if checkpoint was performed.
static bool do_checkpoint(checkpoint_manager_t *mgr) {
    uint64_t checkpoint_lsn = 0;

    if (!data_art_checkpoint(mgr->tree, &checkpoint_lsn)) {
        LOG_ERROR("Checkpoint failed");
        return false;
    }

    wal_checkpoint_completed(mgr->tree->wal, checkpoint_lsn);
    uint32_t truncated = wal_truncate(mgr->tree->wal, checkpoint_lsn);

    pthread_mutex_lock(&mgr->lock);
    mgr->checkpoints_completed++;
    mgr->segments_truncated += truncated;
    pthread_mutex_unlock(&mgr->lock);

    LOG_INFO("Checkpoint completed (LSN=%lu, truncated=%u segments)",
             checkpoint_lsn, truncated);
    return true;
}

static void *checkpoint_thread_fn(void *arg) {
    checkpoint_manager_t *mgr = (checkpoint_manager_t *)arg;

    while (!mgr->stop) {
        // Wait for check_interval_ms or until signaled
        pthread_mutex_lock(&mgr->lock);
        if (!mgr->stop && !mgr->force_requested) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            uint64_t ms = mgr->config.check_interval_ms;
            ts.tv_sec  += ms / 1000;
            ts.tv_nsec += (ms % 1000) * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            pthread_cond_timedwait(&mgr->cond, &mgr->lock, &ts);
        }
        bool force = mgr->force_requested;
        mgr->force_requested = false;
        pthread_mutex_unlock(&mgr->lock);

        if (mgr->stop) break;

        // Check if checkpoint is needed (or forced)
        uint32_t trigger = 0;
        bool should = force || wal_should_checkpoint(mgr->tree->wal, &trigger);

        if (should) {
            do_checkpoint(mgr);
        }

        // Signal anyone waiting on force completion
        if (force) {
            pthread_mutex_lock(&mgr->lock);
            pthread_cond_broadcast(&mgr->done_cond);
            pthread_mutex_unlock(&mgr->lock);
        }
    }

    return NULL;
}

checkpoint_manager_config_t checkpoint_manager_default_config(void) {
    return (checkpoint_manager_config_t){
        .check_interval_ms = 1000,
    };
}

checkpoint_manager_t *checkpoint_manager_create(data_art_tree_t *tree,
                                                const checkpoint_manager_config_t *config) {
    if (!tree || !tree->wal) {
        LOG_ERROR("checkpoint_manager_create: tree or WAL is NULL");
        return NULL;
    }

    checkpoint_manager_t *mgr = calloc(1, sizeof(*mgr));
    if (!mgr) return NULL;

    mgr->tree = tree;
    mgr->config = config ? *config : checkpoint_manager_default_config();

    pthread_mutex_init(&mgr->lock, NULL);
    pthread_cond_init(&mgr->cond, NULL);
    pthread_cond_init(&mgr->done_cond, NULL);

    return mgr;
}

void checkpoint_manager_destroy(checkpoint_manager_t *mgr) {
    if (!mgr) return;

    if (mgr->running) {
        checkpoint_manager_stop(mgr);
    }

    pthread_cond_destroy(&mgr->done_cond);
    pthread_cond_destroy(&mgr->cond);
    pthread_mutex_destroy(&mgr->lock);
    free(mgr);
}

bool checkpoint_manager_start(checkpoint_manager_t *mgr) {
    if (!mgr || mgr->running) return false;

    mgr->stop = false;
    mgr->running = true;

    if (pthread_create(&mgr->thread, NULL, checkpoint_thread_fn, mgr) != 0) {
        LOG_ERROR("Failed to create checkpoint thread");
        mgr->running = false;
        return false;
    }

    LOG_INFO("Checkpoint manager started (interval=%ums)", mgr->config.check_interval_ms);
    return true;
}

void checkpoint_manager_stop(checkpoint_manager_t *mgr) {
    if (!mgr || !mgr->running) return;

    // Signal thread to stop
    pthread_mutex_lock(&mgr->lock);
    mgr->stop = true;
    pthread_cond_signal(&mgr->cond);
    pthread_mutex_unlock(&mgr->lock);

    pthread_join(mgr->thread, NULL);
    mgr->running = false;

    LOG_INFO("Checkpoint manager stopped");
}

bool checkpoint_manager_force(checkpoint_manager_t *mgr) {
    if (!mgr) return false;

    // If not running, do checkpoint directly
    if (!mgr->running) {
        return do_checkpoint(mgr);
    }

    // Signal the background thread and wait for completion
    pthread_mutex_lock(&mgr->lock);
    mgr->force_requested = true;
    pthread_cond_signal(&mgr->cond);

    // Wait for the checkpoint to complete
    // Use a timeout to avoid hanging if something goes wrong
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 30;  // 30 second timeout

    int rc = pthread_cond_timedwait(&mgr->done_cond, &mgr->lock, &ts);
    pthread_mutex_unlock(&mgr->lock);

    if (rc == ETIMEDOUT) {
        LOG_ERROR("Force checkpoint timed out");
        return false;
    }

    return true;
}

void checkpoint_manager_get_stats(const checkpoint_manager_t *mgr,
                                  uint64_t *checkpoints_completed,
                                  uint64_t *segments_truncated) {
    if (!mgr) return;

    // Lock for consistent read (cast away const for mutex)
    pthread_mutex_lock((pthread_mutex_t *)&mgr->lock);
    if (checkpoints_completed) *checkpoints_completed = mgr->checkpoints_completed;
    if (segments_truncated) *segments_truncated = mgr->segments_truncated;
    pthread_mutex_unlock((pthread_mutex_t *)&mgr->lock);
}
