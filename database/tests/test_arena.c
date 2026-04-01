/**
 * Arena allocator tests.
 *
 * Tests bump allocation, alignment, reset, and memory behavior.
 */

#include "arena.h"
#include <stdio.h>
#include <string.h>
#include <stdint.h>

static int passed = 0, failed = 0;

#define CHECK(cond, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL: " __VA_ARGS__); \
        fprintf(stderr, " (line %d)\n", __LINE__); \
        failed++; return; \
    } \
} while(0)

#define PASS(msg) do { printf("  OK: %s\n", msg); passed++; } while(0)

/* ========================================================================= */

static void test_basic_alloc(void) {
    printf("test_basic_alloc:\n");
    arena_t a;
    CHECK(arena_init(&a, 1024 * 1024), "init 1MB");
    CHECK(arena_used(&a) == 0, "initially empty");

    void *p1 = arena_alloc(&a, 100);
    CHECK(p1 != NULL, "alloc 100");
    CHECK(arena_used(&a) == 104, "100 aligned to 104");  /* 8-byte aligned */

    void *p2 = arena_alloc(&a, 8);
    CHECK(p2 != NULL, "alloc 8");
    CHECK((uint8_t *)p2 == (uint8_t *)p1 + 104, "sequential");
    CHECK(arena_used(&a) == 112, "104 + 8 = 112");

    /* Write to allocated memory */
    memset(p1, 0xAB, 100);
    memset(p2, 0xCD, 8);
    CHECK(((uint8_t *)p1)[0] == 0xAB, "p1 writable");
    CHECK(((uint8_t *)p2)[0] == 0xCD, "p2 writable");

    arena_destroy(&a);
    PASS("basic alloc");
}

static void test_alignment(void) {
    printf("test_alignment:\n");
    arena_t a;
    arena_init(&a, 1024 * 1024);

    for (int i = 1; i <= 64; i++) {
        void *p = arena_alloc(&a, i);
        CHECK(p != NULL, "alloc %d", i);
        CHECK(((uintptr_t)p % 8) == 0, "aligned to 8 (size=%d)", i);
    }

    arena_destroy(&a);
    PASS("alignment");
}

static void test_reset(void) {
    printf("test_reset:\n");
    arena_t a;
    arena_init(&a, 4 * 1024 * 1024);

    /* Allocate some data */
    void *p1 = arena_alloc(&a, 1000);
    memset(p1, 0xFF, 1000);
    arena_alloc(&a, 2000);
    size_t before = arena_used(&a);
    CHECK(before > 0, "used > 0");

    /* Reset */
    arena_reset(&a);
    CHECK(arena_used(&a) == 0, "reset to 0");
    CHECK(arena_peak(&a) == before, "peak preserved");

    /* Allocate again — should reuse same space */
    void *p2 = arena_alloc(&a, 1000);
    CHECK(p2 == p1, "reuses same address");

    /* Data should be zeroed (MADV_DONTNEED zeros pages) */
    int nonzero = 0;
    for (int i = 0; i < 1000; i++)
        if (((uint8_t *)p2)[i] != 0) nonzero++;
    CHECK(nonzero == 0, "pages zeroed after reset");

    arena_destroy(&a);
    PASS("reset");
}

static void test_exhaustion(void) {
    printf("test_exhaustion:\n");
    arena_t a;
    arena_init(&a, 4096);  /* tiny arena */

    void *p1 = arena_alloc(&a, 4000);
    CHECK(p1 != NULL, "alloc 4000");

    void *p2 = arena_alloc(&a, 200);
    CHECK(p2 == NULL, "4000 + 200 > 4096 → NULL");

    arena_destroy(&a);
    PASS("exhaustion");
}

static void test_many_small_allocs(void) {
    printf("test_many_small_allocs:\n");
    arena_t a;
    arena_init(&a, 64 * 1024 * 1024);  /* 64 MB */

    /* Simulate 10K per-account art struct allocations */
    int count = 0;
    for (int i = 0; i < 10000; i++) {
        void *p = arena_alloc(&a, 200);  /* ~compact_art struct */
        if (!p) break;
        memset(p, (uint8_t)i, 200);  /* touch pages */
        count++;
    }
    CHECK(count == 10000, "10K allocs");
    CHECK(arena_used(&a) == 10000 * 200, "10K * 200 = 2MB");

    /* Reset and reallocate */
    arena_reset(&a);
    CHECK(arena_used(&a) == 0, "reset");

    void *p = arena_alloc(&a, 200);
    CHECK(p != NULL, "realloc after reset");

    arena_destroy(&a);
    PASS("many small allocs (10K)");
}

static void test_large_reservation(void) {
    printf("test_large_reservation:\n");
    arena_t a;

    /* 1 GB virtual — should succeed on 64-bit */
    CHECK(arena_init(&a, 1ULL * 1024 * 1024 * 1024), "init 1GB");

    /* Allocate 1 MB — only ~256 physical pages committed */
    void *p = arena_alloc(&a, 1024 * 1024);
    CHECK(p != NULL, "alloc 1MB from 1GB");
    memset(p, 0x42, 1024 * 1024);

    arena_destroy(&a);
    PASS("large reservation (1GB virtual, 1MB physical)");
}

static void test_reset_cycle(void) {
    printf("test_reset_cycle:\n");
    arena_t a;
    arena_init(&a, 16 * 1024 * 1024);

    /* Simulate 50 checkpoint windows */
    for (int cycle = 0; cycle < 50; cycle++) {
        /* Each window: allocate for 100 accounts, ~1KB each */
        for (int i = 0; i < 100; i++) {
            void *p = arena_alloc(&a, 1024);
            CHECK(p != NULL, "cycle %d alloc %d", cycle, i);
            memset(p, (uint8_t)(cycle + i), 1024);
        }
        CHECK(arena_used(&a) == 100 * 1024, "100KB per cycle");
        arena_reset(&a);
    }

    CHECK(arena_used(&a) == 0, "final reset");
    CHECK(arena_peak(&a) == 100 * 1024, "peak = one cycle");

    arena_destroy(&a);
    PASS("reset cycle (50 rounds)");
}

/* ========================================================================= */

int main(void) {
    printf("=== Arena Tests ===\n\n");

    test_basic_alloc();
    test_alignment();
    test_reset();
    test_exhaustion();
    test_many_small_allocs();
    test_large_reservation();
    test_reset_cycle();

    printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
