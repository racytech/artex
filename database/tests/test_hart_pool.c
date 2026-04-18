/**
 * test_hart_pool — unit tests for the single-pool slab allocator.
 *
 * Covers:
 *   - lifecycle
 *   - alloc within a slab (fast path)
 *   - slab-chain growth (slow path + chain_ref)
 *   - geometric slab sizing (512, 1K, 2K, 4K, 8K clamp)
 *   - free_slabs returns all slabs to freelist
 *   - reuse from freelist on next alloc
 *   - pool growth via mremap
 *   - stats counters
 */

#include "hart_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int tests_run = 0;
static int tests_failed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (!(cond)) { \
        printf("  FAIL (line %d): %s\n", __LINE__, msg); \
        tests_failed++; \
    } \
} while (0)

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

static void test_lifecycle(void) {
    printf("test_lifecycle:\n");
    hart_pool_t *p = hart_pool_create();
    CHECK(p != NULL, "pool created");

    hart_pool_stats_t st = hart_pool_stats(p);
    CHECK(st.mapped > 0, "initial mapping > 0");
    CHECK(st.used == 512, "sentinel used = 512 (first slab starts at 512)");
    CHECK(st.free_bytes == 0, "no free bytes yet");
    CHECK(st.slab_count == 0, "no slabs yet");

    hart_pool_destroy(p);
}

/* =========================================================================
 * Single-slab alloc
 * ========================================================================= */

static void test_alloc_fits_in_head(void) {
    printf("test_alloc_fits_in_head:\n");
    hart_pool_t *p = hart_pool_create();
    hart_slab_t slab = HART_SLAB_INIT;

    uint32_t cap = 0;
    hart_pool_ref_t r1 = hart_pool_alloc(p, &slab, 64, &cap);
    CHECK(r1 != HART_POOL_REF_NULL, "first alloc succeeds");
    CHECK(cap == 64, "out_cap reflects request");
    CHECK(slab.head_cap == 512, "first slab is 512 B (HART_SLAB_INITIAL)");
    CHECK(slab.head_used == 16 + 64, "bump past header + payload");
    CHECK(slab.chain_ref == 0, "no chain yet");

    /* Second alloc should fit in same slab. */
    hart_pool_ref_t r2 = hart_pool_alloc(p, &slab, 80, NULL);
    CHECK(r2 != HART_POOL_REF_NULL, "second alloc succeeds");
    CHECK(slab.head_ref == r1 - 16, "still same head slab (r1 was payload at +16)");
    (void)r2;

    hart_pool_stats_t st = hart_pool_stats(p);
    CHECK(st.slab_count == 1, "still 1 slab");
    CHECK(st.slabs_per_class[0] == 1, "one 512B slab");

    hart_pool_free_slabs(p, &slab);
    hart_pool_destroy(p);
}

/* =========================================================================
 * Slab-chain growth
 * ========================================================================= */

static void test_slab_chain_growth(void) {
    printf("test_slab_chain_growth:\n");
    hart_pool_t *p = hart_pool_create();
    hart_slab_t slab = HART_SLAB_INIT;

    /* Fill the first 512B slab with ~480 bytes of 80-byte allocs.
     * Header is 16B, so payload budget is 496B. Six 80B allocs = 480B. */
    for (int i = 0; i < 6; i++) {
        hart_pool_ref_t r = hart_pool_alloc(p, &slab, 80, NULL);
        CHECK(r != HART_POOL_REF_NULL, "in-slab alloc");
    }
    CHECK(slab.head_cap == 512, "still on first slab");

    hart_pool_ref_t old_head = slab.head_ref;

    /* Next alloc should trigger a new slab (doubled to 1024 B). */
    hart_pool_ref_t r = hart_pool_alloc(p, &slab, 80, NULL);
    CHECK(r != HART_POOL_REF_NULL, "chain alloc");
    CHECK(slab.head_cap == 1024, "grew to 1024 B");
    CHECK(slab.chain_ref == old_head, "old head linked via chain_ref");
    CHECK(slab.head_ref != old_head, "new head is a different slab");

    hart_pool_stats_t st = hart_pool_stats(p);
    CHECK(st.slab_count == 2, "2 live slabs after growth");
    CHECK(st.slabs_per_class[0] == 1, "one 512B slab");
    CHECK(st.slabs_per_class[1] == 1, "one 1024B slab");

    hart_pool_free_slabs(p, &slab);
    hart_pool_destroy(p);
}

/* =========================================================================
 * Slab size capped at MAX
 * ========================================================================= */

static void test_slab_geometric_cap(void) {
    printf("test_slab_geometric_cap:\n");
    hart_pool_t *p = hart_pool_create();
    hart_slab_t slab = HART_SLAB_INIT;

    /* Force many slab rolls by asking for chunks that don't fit the head. */
    uint32_t caps_seen[HART_SLAB_CLASSES + 2] = {0};
    uint32_t idx = 0;
    for (int i = 0; i < 10; i++) {
        /* Force a new slab every time by requesting > current head_cap/2. */
        uint32_t want = slab.head_cap ? (slab.head_cap - 16) : 400;
        hart_pool_ref_t r = hart_pool_alloc(p, &slab, want, NULL);
        CHECK(r != HART_POOL_REF_NULL, "alloc in growth loop");
        if (idx < sizeof(caps_seen)/sizeof(caps_seen[0]))
            caps_seen[idx++] = slab.head_cap;
    }
    /* After >=4 growth events, slab cap must hit 8192 and stay. */
    CHECK(slab.head_cap == HART_SLAB_MAX, "capped at HART_SLAB_MAX");

    hart_pool_stats_t st = hart_pool_stats(p);
    CHECK(st.slabs_per_class[4] >= 1, "at least one 8KB slab allocated");

    hart_pool_free_slabs(p, &slab);
    hart_pool_destroy(p);
}

/* =========================================================================
 * Free + reuse
 * ========================================================================= */

static void test_free_and_reuse(void) {
    printf("test_free_and_reuse:\n");
    hart_pool_t *p = hart_pool_create();

    /* Hart A: fills up past 512 → gets a 1024B chain. */
    hart_slab_t a = HART_SLAB_INIT;
    for (int i = 0; i < 7; i++)
        hart_pool_alloc(p, &a, 80, NULL);

    hart_pool_ref_t a_head = a.head_ref;
    hart_pool_ref_t a_chain = a.chain_ref;
    CHECK(a_head != HART_POOL_REF_NULL, "hart a has head");
    CHECK(a_chain != HART_POOL_REF_NULL, "hart a has chain");

    hart_pool_stats_t before = hart_pool_stats(p);
    CHECK(before.slab_count == 2, "2 live slabs before free");

    /* Free whole chain */
    hart_pool_free_slabs(p, &a);
    CHECK(a.head_ref == 0, "slab state cleared");

    hart_pool_stats_t after_free = hart_pool_stats(p);
    CHECK(after_free.slab_count == 0, "no live slabs after free");
    CHECK(after_free.free_per_class[0] == 1, "one 512B slab on freelist");
    CHECK(after_free.free_per_class[1] == 1, "one 1024B slab on freelist");
    CHECK(after_free.free_bytes == 512 + 1024, "total free bytes match");

    /* Hart B allocates small → should reuse A's 512B slab. */
    hart_slab_t b = HART_SLAB_INIT;
    hart_pool_alloc(p, &b, 80, NULL);
    CHECK(b.head_ref == a_chain /* old 512B was the first (chain) slab */,
          "reused freed 512B slab");

    hart_pool_stats_t after_reuse = hart_pool_stats(p);
    CHECK(after_reuse.slab_count == 1, "1 live slab after reuse");
    CHECK(after_reuse.free_per_class[0] == 0, "512B freelist drained");
    CHECK(after_reuse.free_per_class[1] == 1, "1024B still on freelist");

    hart_pool_free_slabs(p, &b);
    hart_pool_destroy(p);
}

/* =========================================================================
 * Pool growth via mremap
 * ========================================================================= */

static void test_pool_growth(void) {
    printf("test_pool_growth:\n");
    hart_pool_t *p = hart_pool_create();
    hart_pool_stats_t st0 = hart_pool_stats(p);

    /* Allocate enough 8KB slabs to blow past HART_POOL_INITIAL_MAP (1MB). */
    enum { N = 200 };
    hart_slab_t slabs[N];
    memset(slabs, 0, sizeof(slabs));

    for (int i = 0; i < N; i++) {
        /* Push one alloc that's big enough to force a max slab quickly.
         * Then pad to force growth: ask for 5000 B so we skip past 4KB. */
        hart_pool_ref_t r1 = hart_pool_alloc(p, &slabs[i], 400, NULL);
        hart_pool_ref_t r2 = hart_pool_alloc(p, &slabs[i], 5000, NULL);
        (void)r1; (void)r2;
    }
    hart_pool_stats_t st1 = hart_pool_stats(p);
    CHECK(st1.mapped > st0.mapped, "pool mapping grew");
    CHECK(st1.slab_count >= N, "at least N slabs live");

    /* Release all harts — everything should end up on freelist. */
    for (int i = 0; i < N; i++)
        hart_pool_free_slabs(p, &slabs[i]);
    hart_pool_stats_t st2 = hart_pool_stats(p);
    CHECK(st2.slab_count == 0, "all slabs freed");
    CHECK(st2.free_bytes > 0, "freelists now hold bytes");

    hart_pool_destroy(p);
}

/* =========================================================================
 * Pointer resolution / round-trip writes
 * ========================================================================= */

static void test_ptr_roundtrip(void) {
    printf("test_ptr_roundtrip:\n");
    hart_pool_t *p = hart_pool_create();
    hart_slab_t slab = HART_SLAB_INIT;

    hart_pool_ref_t r = hart_pool_alloc(p, &slab, 32, NULL);
    uint8_t *mem = hart_pool_ptr(p, r);
    CHECK(mem != NULL, "non-null ptr");

    for (int i = 0; i < 32; i++) mem[i] = (uint8_t)(i * 7);

    /* After an intervening growth alloc, re-resolve the ref. */
    for (int i = 0; i < 20; i++) hart_pool_alloc(p, &slab, 80, NULL);

    mem = hart_pool_ptr(p, r);
    for (int i = 0; i < 32; i++)
        CHECK(mem[i] == (uint8_t)(i * 7), "byte round-trips after growth");

    hart_pool_free_slabs(p, &slab);
    hart_pool_destroy(p);
}

/* ======================================================================= */

int main(void) {
    test_lifecycle();
    test_alloc_fits_in_head();
    test_slab_chain_growth();
    test_slab_geometric_cap();
    test_free_and_reuse();
    test_pool_growth();
    test_ptr_roundtrip();

    printf("\n%d/%d checks passed\n", tests_run - tests_failed, tests_run);
    return tests_failed ? 1 : 0;
}
