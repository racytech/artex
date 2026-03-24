#ifndef VF_THREADPOOL_H
#define VF_THREADPOOL_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

/**
 * Verkle Flat Thread Pool — fixed-size pool for parallel stem processing.
 *
 * Workers pick tasks from a queue, execute them, signal completion.
 * Main thread submits tasks and waits for all to finish.
 *
 * Thread-safe: queue protected by mutex + condvar.
 */

typedef void (*vf_task_fn)(void *arg);

typedef struct {
    vf_task_fn fn;
    void      *arg;
} vf_task_t;

typedef struct vf_threadpool {
    /* Worker threads */
    pthread_t   *threads;
    int          thread_count;

    /* Task queue (ring buffer) */
    vf_task_t   *queue;
    int          queue_cap;
    int          queue_head;   /* next slot to read */
    int          queue_tail;   /* next slot to write */
    int          queue_count;

    /* Synchronization */
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;  /* workers wait on this */
    pthread_cond_t  not_full;   /* submitter waits on this */
    pthread_cond_t  all_done;   /* wait_all waits on this */

    /* Completion tracking */
    int          pending;       /* tasks submitted but not completed */
    bool         shutdown;
} vf_threadpool_t;

/** Create a thread pool with `num_threads` workers. */
vf_threadpool_t *vf_threadpool_create(int num_threads);

/** Destroy pool: signal shutdown, join all threads, free resources. */
void vf_threadpool_destroy(vf_threadpool_t *pool);

/** Submit a task. Blocks if queue is full. */
void vf_threadpool_submit(vf_threadpool_t *pool, vf_task_fn fn, void *arg);

/** Wait for all submitted tasks to complete. */
void vf_threadpool_wait(vf_threadpool_t *pool);

#endif /* VF_THREADPOOL_H */
