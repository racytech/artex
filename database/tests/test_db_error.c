#include "db_error.h"
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_RESET  "\033[0m"

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, COLOR_RED "   FAILED: %s\n" COLOR_RESET, msg); \
        fprintf(stderr, "     at %s:%d\n", __FILE__, __LINE__); \
        return false; \
    } \
} while(0)

static bool test_basic_set_get(void) {
    printf("Running: test_basic_set_get\n");

    db_clear_error();
    TEST_ASSERT(db_get_last_error() == DB_OK, "Initial should be DB_OK");
    TEST_ASSERT(db_get_last_error_msg()[0] == '\0', "Initial message should be empty");

    db_set_last_error(DB_ERROR_IO);
    TEST_ASSERT(db_get_last_error() == DB_ERROR_IO, "Should be DB_ERROR_IO");
    TEST_ASSERT(db_get_last_error_msg()[0] == '\0', "Message should be empty after set_last_error");

    db_set_last_error_msg(DB_ERROR_DISK_FULL, "disk full at offset %lu", (unsigned long)42);
    TEST_ASSERT(db_get_last_error() == DB_ERROR_DISK_FULL, "Should be DB_ERROR_DISK_FULL");
    TEST_ASSERT(strstr(db_get_last_error_msg(), "42") != NULL, "Message should contain '42'");

    db_clear_error();
    TEST_ASSERT(db_get_last_error() == DB_OK, "Should be cleared");
    TEST_ASSERT(db_get_last_error_msg()[0] == '\0', "Message should be cleared");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

typedef struct {
    db_error_t to_set;
    db_error_t observed;
} isolation_arg_t;

static void *isolation_thread(void *arg) {
    isolation_arg_t *a = arg;
    db_clear_error();
    db_set_last_error(a->to_set);

    struct timespec ts = {0, 20 * 1000000}; // 20ms
    nanosleep(&ts, NULL);

    a->observed = db_get_last_error();
    return NULL;
}

static bool test_thread_isolation(void) {
    printf("Running: test_thread_isolation\n");

    isolation_arg_t a1 = {DB_ERROR_IO, DB_OK};
    isolation_arg_t a2 = {DB_ERROR_CORRUPTION, DB_OK};

    pthread_t t1, t2;
    pthread_create(&t1, NULL, isolation_thread, &a1);
    pthread_create(&t2, NULL, isolation_thread, &a2);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    TEST_ASSERT(a1.observed == DB_ERROR_IO,
                "Thread 1 should see DB_ERROR_IO");
    TEST_ASSERT(a2.observed == DB_ERROR_CORRUPTION,
                "Thread 2 should see DB_ERROR_CORRUPTION");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

static bool test_message_truncation(void) {
    printf("Running: test_message_truncation\n");

    char long_msg[600];
    memset(long_msg, 'A', sizeof(long_msg) - 1);
    long_msg[sizeof(long_msg) - 1] = '\0';

    db_set_last_error_msg(DB_ERROR_INTERNAL, "%s", long_msg);
    const char *msg = db_get_last_error_msg();
    TEST_ASSERT(strlen(msg) < 512, "Message must be truncated to < 512");
    TEST_ASSERT(db_get_last_error() == DB_ERROR_INTERNAL, "Error code should be set");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

static bool test_error_strings(void) {
    printf("Running: test_error_strings\n");

    db_error_t codes[] = {
        DB_OK, DB_ERROR_NOT_FOUND, DB_ERROR_IO, DB_ERROR_DISK_FULL,
        DB_ERROR_CORRUPTION, DB_ERROR_INVALID_ARG, DB_ERROR_OUT_OF_MEMORY,
        DB_ERROR_TXN_NOT_FOUND, DB_ERROR_TXN_CONFLICT, DB_ERROR_WAL_FULL,
        DB_ERROR_SNAPSHOT_EXPIRED, DB_ERROR_READ_ONLY, DB_ERROR_INTERNAL
    };
    for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
        const char *s = db_error_string(codes[i]);
        TEST_ASSERT(s != NULL && s[0] != '\0', "Should return non-empty string");
        TEST_ASSERT(strstr(s, "unknown") == NULL, "Known codes must not return 'unknown'");
    }

    // Unknown code should return "unknown error"
    const char *s = db_error_string((db_error_t)-99);
    TEST_ASSERT(strstr(s, "unknown") != NULL, "Unknown code should say 'unknown'");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

static bool test_page_result_conversion(void) {
    printf("Running: test_page_result_conversion\n");

    TEST_ASSERT(DB_ERROR_FROM_PAGE(0) == DB_OK, "PAGE_SUCCESS -> DB_OK");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-1) == DB_ERROR_NOT_FOUND, "PAGE_ERROR_NOT_FOUND -> DB_ERROR_NOT_FOUND");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-2) == DB_ERROR_IO, "PAGE_ERROR_IO -> DB_ERROR_IO");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-3) == DB_ERROR_DISK_FULL, "PAGE_ERROR_DISK_FULL -> DB_ERROR_DISK_FULL");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-4) == DB_ERROR_CORRUPTION, "PAGE_ERROR_CORRUPTION -> DB_ERROR_CORRUPTION");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-5) == DB_ERROR_INVALID_ARG, "PAGE_ERROR_INVALID_ARG -> DB_ERROR_INVALID_ARG");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-6) == DB_ERROR_OUT_OF_MEMORY, "PAGE_ERROR_OUT_OF_MEMORY -> DB_ERROR_OUT_OF_MEMORY");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

#define NUM_THREADS 16

typedef struct {
    int idx;
    bool passed;
} multi_arg_t;

static void *multi_thread_fn(void *arg) {
    multi_arg_t *a = arg;
    db_clear_error();
    db_set_last_error_msg(DB_ERROR_IO, "thread %d error", a->idx);

    struct timespec ts = {0, 1000000}; // 1ms
    nanosleep(&ts, NULL);

    const char *msg = db_get_last_error_msg();
    char expected[64];
    snprintf(expected, sizeof(expected), "thread %d error", a->idx);
    a->passed = (strcmp(msg, expected) == 0);
    return NULL;
}

static bool test_many_threads(void) {
    printf("Running: test_many_threads\n");

    pthread_t threads[NUM_THREADS];
    multi_arg_t args[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].idx = i;
        args[i].passed = false;
        pthread_create(&threads[i], NULL, multi_thread_fn, &args[i]);
    }

    bool all_passed = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        if (!args[i].passed) {
            fprintf(stderr, "  Thread %d saw wrong message\n", i);
            all_passed = false;
        }
    }

    TEST_ASSERT(all_passed, "All 16 threads should see their own error message");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

static bool test_overwrite_semantics(void) {
    printf("Running: test_overwrite_semantics\n");

    db_set_last_error_msg(DB_ERROR_IO, "first error");
    TEST_ASSERT(db_get_last_error() == DB_ERROR_IO, "Should be IO");

    // Second error overwrites the first
    db_set_last_error_msg(DB_ERROR_DISK_FULL, "second error");
    TEST_ASSERT(db_get_last_error() == DB_ERROR_DISK_FULL, "Should be DISK_FULL");
    TEST_ASSERT(strstr(db_get_last_error_msg(), "second") != NULL, "Should see second message");

    // Setting code without message clears the message
    db_set_last_error(DB_ERROR_CORRUPTION);
    TEST_ASSERT(db_get_last_error() == DB_ERROR_CORRUPTION, "Should be CORRUPTION");
    TEST_ASSERT(db_get_last_error_msg()[0] == '\0', "Message should be cleared");

    db_clear_error();
    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

int main(void) {
    printf("\n");
    printf("========================================\n");
    printf(" DB Error System Tests\n");
    printf("========================================\n\n");

    int passed = 0;
    int total = 0;

    #define RUN_TEST(fn) do { \
        total++; \
        if (fn()) passed++; \
        else printf(COLOR_RED "   FAILED\n" COLOR_RESET); \
        printf("\n"); \
    } while(0)

    RUN_TEST(test_basic_set_get);
    RUN_TEST(test_thread_isolation);
    RUN_TEST(test_message_truncation);
    RUN_TEST(test_error_strings);
    RUN_TEST(test_page_result_conversion);
    RUN_TEST(test_many_threads);
    RUN_TEST(test_overwrite_semantics);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", passed, total);
    printf("========================================\n\n");

    return (passed == total) ? 0 : 1;
}
