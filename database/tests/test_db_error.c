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

// ============================================================================
// Backward compatibility tests (db_set_last_error / db_set_last_error_msg)
// ============================================================================

static bool test_basic_set_get(void) {
    printf("Running: test_basic_set_get\n");

    db_clear_error();
    TEST_ASSERT(db_get_last_error() == DB_OK, "Initial should be DB_OK");
    TEST_ASSERT(db_get_last_error_msg()[0] == '\0', "Initial message should be empty");

    db_set_last_error(DB_ERROR_IO);
    TEST_ASSERT(db_get_last_error() == DB_ERROR_IO, "Should be DB_ERROR_IO");

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
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-1) == DB_ERROR_NOT_FOUND, "-1 -> NOT_FOUND");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-2) == DB_ERROR_IO, "-2 -> IO");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-3) == DB_ERROR_DISK_FULL, "-3 -> DISK_FULL");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-4) == DB_ERROR_CORRUPTION, "-4 -> CORRUPTION");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-5) == DB_ERROR_INVALID_ARG, "-5 -> INVALID_ARG");
    TEST_ASSERT(DB_ERROR_FROM_PAGE(-6) == DB_ERROR_OUT_OF_MEMORY, "-6 -> OUT_OF_MEMORY");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

// ============================================================================
// Trace stack tests (DB_ERROR macro, multi-frame traces)
// ============================================================================

static bool test_trace_single_frame(void) {
    printf("Running: test_trace_single_frame\n");

    db_clear_error();
    TEST_ASSERT(db_error_trace_depth() == 0, "Depth should be 0 after clear");

    DB_ERROR(DB_ERROR_IO, "disk write failed at sector %d", 42);

    TEST_ASSERT(db_error_trace_depth() == 1, "Depth should be 1");
    TEST_ASSERT(db_get_last_error() == DB_ERROR_IO, "Last error should be IO");
    TEST_ASSERT(strstr(db_get_last_error_msg(), "sector 42") != NULL, "Message should contain 'sector 42'");

    const db_error_frame_t *f = db_error_trace_get(0);
    TEST_ASSERT(f != NULL, "Frame 0 should exist");
    TEST_ASSERT(f->code == DB_ERROR_IO, "Frame code should be IO");
    TEST_ASSERT(f->line > 0, "Frame should have line number");
    TEST_ASSERT(f->file != NULL, "Frame should have file");
    TEST_ASSERT(f->func != NULL, "Frame should have function name");
    TEST_ASSERT(strstr(f->func, "test_trace_single_frame") != NULL, "Should capture function name");

    db_clear_error();
    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

// Helper functions to simulate a call chain
static bool inner_op(void) {
    DB_ERROR(DB_ERROR_IO, "write() returned -1, errno=ENOSPC");
    return false;
}

static bool middle_op(void) {
    if (!inner_op()) {
        DB_ERROR(DB_ERROR_IO, "failed to write node to page");
        return false;
    }
    return true;
}

static bool outer_op(void) {
    db_clear_error();
    if (!middle_op()) {
        DB_ERROR(DB_ERROR_IO, "insert operation failed");
        return false;
    }
    return true;
}

static bool test_trace_multi_frame(void) {
    printf("Running: test_trace_multi_frame\n");

    bool result = outer_op();
    TEST_ASSERT(!result, "outer_op should fail");
    TEST_ASSERT(db_error_trace_depth() == 3, "Should have 3 frames");

    // Frame 0 = root cause (inner_op)
    const db_error_frame_t *f0 = db_error_trace_get(0);
    TEST_ASSERT(f0 != NULL, "Frame 0 should exist");
    TEST_ASSERT(f0->code == DB_ERROR_IO, "Frame 0 should be IO");
    TEST_ASSERT(strstr(f0->func, "inner_op") != NULL, "Frame 0 should be inner_op");
    TEST_ASSERT(strstr(f0->msg, "ENOSPC") != NULL, "Frame 0 should mention ENOSPC");

    // Frame 1 = middle_op
    const db_error_frame_t *f1 = db_error_trace_get(1);
    TEST_ASSERT(f1 != NULL, "Frame 1 should exist");
    TEST_ASSERT(strstr(f1->func, "middle_op") != NULL, "Frame 1 should be middle_op");
    TEST_ASSERT(strstr(f1->msg, "write node") != NULL, "Frame 1 should mention write node");

    // Frame 2 = outermost (outer_op)
    const db_error_frame_t *f2 = db_error_trace_get(2);
    TEST_ASSERT(f2 != NULL, "Frame 2 should exist");
    TEST_ASSERT(strstr(f2->func, "outer_op") != NULL, "Frame 2 should be outer_op");

    // Last error should be from the outermost frame
    TEST_ASSERT(db_get_last_error() == DB_ERROR_IO, "Last error from outermost");
    TEST_ASSERT(strstr(db_get_last_error_msg(), "insert operation") != NULL, "Last msg from outermost");

    // Out of range should return NULL
    TEST_ASSERT(db_error_trace_get(-1) == NULL, "Negative index should be NULL");
    TEST_ASSERT(db_error_trace_get(3) == NULL, "Index 3 should be NULL (only 0-2)");

    db_clear_error();
    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

static bool test_trace_overflow(void) {
    printf("Running: test_trace_overflow\n");

    db_clear_error();

    // Push more than DB_ERROR_TRACE_MAX (8) frames
    for (int i = 0; i < 12; i++) {
        DB_ERROR(DB_ERROR_IO, "frame %d", i);
    }

    TEST_ASSERT(db_error_trace_depth() == DB_ERROR_TRACE_MAX,
                "Depth should be capped at DB_ERROR_TRACE_MAX");

    // Root cause (frame 0) should still be frame 0
    const db_error_frame_t *f0 = db_error_trace_get(0);
    TEST_ASSERT(f0 != NULL, "Frame 0 should exist");
    TEST_ASSERT(strstr(f0->msg, "frame 0") != NULL, "Frame 0 should be the first push");

    // Last frame should be frame 7 (0-indexed), not frame 11
    const db_error_frame_t *last = db_error_trace_get(DB_ERROR_TRACE_MAX - 1);
    TEST_ASSERT(last != NULL, "Last frame should exist");
    TEST_ASSERT(strstr(last->msg, "frame 7") != NULL, "Last frame should be frame 7");

    db_clear_error();
    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

static bool test_trace_print(void) {
    printf("Running: test_trace_print\n");

    db_clear_error();

    // No error — should print "(no error)"
    char buf[2048];
    FILE *f = fmemopen(buf, sizeof(buf), "w");
    db_error_trace_print(f);
    fclose(f);
    TEST_ASSERT(strstr(buf, "no error") != NULL, "Empty trace should say 'no error'");

    // Push 2 frames and print
    outer_op();  // pushes 3 frames (inner, middle, outer)

    memset(buf, 0, sizeof(buf));
    f = fmemopen(buf, sizeof(buf), "w");
    db_error_trace_print(f);
    fclose(f);

    TEST_ASSERT(strstr(buf, "error:") != NULL, "Should have 'error:' header");
    TEST_ASSERT(strstr(buf, "3 frames") != NULL, "Should say '3 frames'");
    TEST_ASSERT(strstr(buf, "[0]") != NULL, "Should have frame [0]");
    TEST_ASSERT(strstr(buf, "[1]") != NULL, "Should have frame [1]");
    TEST_ASSERT(strstr(buf, "[2]") != NULL, "Should have frame [2]");
    TEST_ASSERT(strstr(buf, "inner_op") != NULL, "Should show inner_op");
    TEST_ASSERT(strstr(buf, "ENOSPC") != NULL, "Should show root cause message");

    db_clear_error();
    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

static bool test_backward_compat_clears_trace(void) {
    printf("Running: test_backward_compat_clears_trace\n");

    db_clear_error();

    // Push multiple frames
    DB_ERROR(DB_ERROR_IO, "frame 1");
    DB_ERROR(DB_ERROR_IO, "frame 2");
    DB_ERROR(DB_ERROR_IO, "frame 3");
    TEST_ASSERT(db_error_trace_depth() == 3, "Should have 3 frames");

    // db_set_last_error should clear trace and push single frame
    db_set_last_error(DB_ERROR_CORRUPTION);
    TEST_ASSERT(db_error_trace_depth() == 1, "set_last_error should leave 1 frame");
    TEST_ASSERT(db_get_last_error() == DB_ERROR_CORRUPTION, "Should be CORRUPTION");

    // db_set_last_error_msg should also clear and push single frame
    DB_ERROR(DB_ERROR_IO, "extra frame");
    TEST_ASSERT(db_error_trace_depth() == 2, "Should have 2 frames now");

    db_set_last_error_msg(DB_ERROR_DISK_FULL, "no space at %d", 100);
    TEST_ASSERT(db_error_trace_depth() == 1, "set_last_error_msg should leave 1 frame");
    TEST_ASSERT(db_get_last_error() == DB_ERROR_DISK_FULL, "Should be DISK_FULL");
    TEST_ASSERT(strstr(db_get_last_error_msg(), "100") != NULL, "Should contain '100'");

    db_clear_error();
    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

// ============================================================================
// Thread isolation of traces
// ============================================================================

typedef struct {
    int thread_idx;
    bool passed;
} trace_isolation_arg_t;

static void *trace_isolation_thread(void *arg) {
    trace_isolation_arg_t *a = arg;
    db_clear_error();

    // Each thread pushes its own trace
    DB_ERROR(DB_ERROR_IO, "thread %d root cause", a->thread_idx);
    DB_ERROR(DB_ERROR_IO, "thread %d middle", a->thread_idx);
    DB_ERROR(DB_ERROR_IO, "thread %d outer", a->thread_idx);

    struct timespec ts = {0, 5 * 1000000}; // 5ms
    nanosleep(&ts, NULL);

    // Verify this thread's trace is independent
    a->passed = true;
    if (db_error_trace_depth() != 3) {
        a->passed = false;
        return NULL;
    }

    const db_error_frame_t *f0 = db_error_trace_get(0);
    if (!f0 || !strstr(f0->msg, "root cause")) {
        a->passed = false;
        return NULL;
    }

    char expected[64];
    snprintf(expected, sizeof(expected), "thread %d root cause", a->thread_idx);
    if (strcmp(f0->msg, expected) != 0) {
        a->passed = false;
    }

    return NULL;
}

#define NUM_THREADS 16

static bool test_trace_thread_isolation(void) {
    printf("Running: test_trace_thread_isolation\n");

    pthread_t threads[NUM_THREADS];
    trace_isolation_arg_t args[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].thread_idx = i;
        args[i].passed = false;
        pthread_create(&threads[i], NULL, trace_isolation_thread, &args[i]);
    }

    bool all_passed = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        if (!args[i].passed) {
            fprintf(stderr, "  Thread %d trace was corrupted\n", i);
            all_passed = false;
        }
    }

    TEST_ASSERT(all_passed, "All threads should have independent traces");

    printf(COLOR_GREEN "   PASSED\n" COLOR_RESET);
    return true;
}

// ============================================================================
// Main
// ============================================================================

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

    // Backward compatibility
    RUN_TEST(test_basic_set_get);
    RUN_TEST(test_thread_isolation);
    RUN_TEST(test_error_strings);
    RUN_TEST(test_page_result_conversion);

    // Trace stack
    RUN_TEST(test_trace_single_frame);
    RUN_TEST(test_trace_multi_frame);
    RUN_TEST(test_trace_overflow);
    RUN_TEST(test_trace_print);
    RUN_TEST(test_backward_compat_clears_trace);
    RUN_TEST(test_trace_thread_isolation);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", passed, total);
    printf("========================================\n\n");

    return (passed == total) ? 0 : 1;
}
