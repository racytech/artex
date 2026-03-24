#include "vf_threadpool.h"
#include <stdlib.h>
#include <string.h>

#define DEFAULT_QUEUE_CAP 4096

static void *worker_fn(void *arg) {
    vf_threadpool_t *pool = (vf_threadpool_t *)arg;

    for (;;) {
        pthread_mutex_lock(&pool->mutex);

        /* Wait for work or shutdown */
        while (pool->queue_count == 0 && !pool->shutdown)
            pthread_cond_wait(&pool->not_empty, &pool->mutex);

        if (pool->shutdown && pool->queue_count == 0) {
            pthread_mutex_unlock(&pool->mutex);
            return NULL;
        }

        /* Dequeue task */
        vf_task_t task = pool->queue[pool->queue_head];
        pool->queue_head = (pool->queue_head + 1) % pool->queue_cap;
        pool->queue_count--;

        pthread_cond_signal(&pool->not_full);
        pthread_mutex_unlock(&pool->mutex);

        /* Execute task */
        task.fn(task.arg);

        /* Signal completion */
        pthread_mutex_lock(&pool->mutex);
        pool->pending--;
        if (pool->pending == 0)
            pthread_cond_broadcast(&pool->all_done);
        pthread_mutex_unlock(&pool->mutex);
    }
}

vf_threadpool_t *vf_threadpool_create(int num_threads) {
    if (num_threads <= 0) return NULL;

    vf_threadpool_t *pool = calloc(1, sizeof(vf_threadpool_t));
    if (!pool) return NULL;

    pool->queue_cap = DEFAULT_QUEUE_CAP;
    pool->queue = calloc(pool->queue_cap, sizeof(vf_task_t));
    if (!pool->queue) { free(pool); return NULL; }

    pthread_mutex_init(&pool->mutex, NULL);
    pthread_cond_init(&pool->not_empty, NULL);
    pthread_cond_init(&pool->not_full, NULL);
    pthread_cond_init(&pool->all_done, NULL);

    pool->thread_count = num_threads;
    pool->threads = calloc(num_threads, sizeof(pthread_t));
    if (!pool->threads) {
        free(pool->queue);
        free(pool);
        return NULL;
    }

    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&pool->threads[i], NULL, worker_fn, pool) != 0) {
            /* Partial creation — shut down what we have */
            pool->thread_count = i;
            vf_threadpool_destroy(pool);
            return NULL;
        }
    }

    return pool;
}

void vf_threadpool_destroy(vf_threadpool_t *pool) {
    if (!pool) return;

    pthread_mutex_lock(&pool->mutex);
    pool->shutdown = true;
    pthread_cond_broadcast(&pool->not_empty);
    pthread_mutex_unlock(&pool->mutex);

    for (int i = 0; i < pool->thread_count; i++)
        pthread_join(pool->threads[i], NULL);

    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->not_empty);
    pthread_cond_destroy(&pool->not_full);
    pthread_cond_destroy(&pool->all_done);

    free(pool->threads);
    free(pool->queue);
    free(pool);
}

void vf_threadpool_submit(vf_threadpool_t *pool, vf_task_fn fn, void *arg) {
    pthread_mutex_lock(&pool->mutex);

    while (pool->queue_count == pool->queue_cap)
        pthread_cond_wait(&pool->not_full, &pool->mutex);

    pool->queue[pool->queue_tail] = (vf_task_t){ .fn = fn, .arg = arg };
    pool->queue_tail = (pool->queue_tail + 1) % pool->queue_cap;
    pool->queue_count++;
    pool->pending++;

    pthread_cond_signal(&pool->not_empty);
    pthread_mutex_unlock(&pool->mutex);
}

void vf_threadpool_wait(vf_threadpool_t *pool) {
    pthread_mutex_lock(&pool->mutex);
    while (pool->pending > 0)
        pthread_cond_wait(&pool->all_done, &pool->mutex);
    pthread_mutex_unlock(&pool->mutex);
}
