/**
 * Storage file tests — packed per-account storage persistence.
 */

#include "storage_file.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
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

static const char *TEST_PATH = "/dev/shm/test_storage_file.dat";

static void cleanup(void) { unlink(TEST_PATH); }

static void test_create_destroy(void) {
    printf("test_create_destroy:\n");
    cleanup();
    storage_file_t *sf = storage_file_create(TEST_PATH);
    CHECK(sf != NULL, "create");
    CHECK(storage_file_size(sf) == 16, "header only");
    storage_file_destroy(sf);
    cleanup();
    PASS("create/destroy");
}

static void test_write_read(void) {
    printf("test_write_read:\n");
    cleanup();
    storage_file_t *sf = storage_file_create(TEST_PATH);

    /* Write 3 slots */
    uint8_t slots[3 * 64];
    for (int i = 0; i < 3; i++) {
        memset(slots + i * 64, (uint8_t)(i + 1), 32);      /* slot_hash */
        memset(slots + i * 64 + 32, (uint8_t)(i + 0x10), 32); /* value */
    }
    uint64_t offset = storage_file_write_section(sf, slots, 3);
    CHECK(offset == 16, "offset == header size");
    CHECK(storage_file_size(sf) == 16 + 3 * 64, "size grew");

    /* Read back */
    uint8_t buf[3 * 64];
    CHECK(storage_file_read_section(sf, offset, 3, buf), "read");
    CHECK(memcmp(slots, buf, sizeof(slots)) == 0, "data matches");

    storage_file_destroy(sf);
    cleanup();
    PASS("write/read");
}

static void test_multiple_sections(void) {
    printf("test_multiple_sections:\n");
    cleanup();
    storage_file_t *sf = storage_file_create(TEST_PATH);

    /* Account A: 2 slots */
    uint8_t a_slots[2 * 64];
    memset(a_slots, 0xAA, sizeof(a_slots));
    uint64_t a_off = storage_file_write_section(sf, a_slots, 2);

    /* Account B: 5 slots */
    uint8_t b_slots[5 * 64];
    memset(b_slots, 0xBB, sizeof(b_slots));
    uint64_t b_off = storage_file_write_section(sf, b_slots, 5);

    CHECK(a_off != b_off, "different offsets");
    CHECK(b_off == a_off + 2 * 64, "sequential");

    /* Read A */
    uint8_t buf_a[2 * 64];
    CHECK(storage_file_read_section(sf, a_off, 2, buf_a), "read A");
    CHECK(memcmp(buf_a, a_slots, sizeof(buf_a)) == 0, "A matches");

    /* Read B */
    uint8_t buf_b[5 * 64];
    CHECK(storage_file_read_section(sf, b_off, 5, buf_b), "read B");
    CHECK(memcmp(buf_b, b_slots, sizeof(buf_b)) == 0, "B matches");

    storage_file_destroy(sf);
    cleanup();
    PASS("multiple sections");
}

static void test_reopen(void) {
    printf("test_reopen:\n");
    cleanup();

    /* Write */
    storage_file_t *sf = storage_file_create(TEST_PATH);
    uint8_t slots[2 * 64];
    memset(slots, 0x42, sizeof(slots));
    uint64_t offset = storage_file_write_section(sf, slots, 2);
    storage_file_destroy(sf);

    /* Reopen */
    sf = storage_file_create(TEST_PATH);
    CHECK(sf != NULL, "reopen");
    CHECK(storage_file_size(sf) == 16 + 2 * 64, "size preserved");

    uint8_t buf[2 * 64];
    CHECK(storage_file_read_section(sf, offset, 2, buf), "read after reopen");
    CHECK(memcmp(buf, slots, sizeof(buf)) == 0, "data preserved");

    storage_file_destroy(sf);
    cleanup();
    PASS("reopen");
}

static void test_reset(void) {
    printf("test_reset:\n");
    cleanup();
    storage_file_t *sf = storage_file_create(TEST_PATH);

    uint8_t slots[10 * 64];
    memset(slots, 0xFF, sizeof(slots));
    storage_file_write_section(sf, slots, 10);
    CHECK(storage_file_size(sf) > 16, "has data");

    storage_file_reset(sf);
    CHECK(storage_file_size(sf) == 16, "reset to header");

    /* Can write again */
    uint64_t off = storage_file_write_section(sf, slots, 2);
    CHECK(off == 16, "writes after reset start at header");

    storage_file_destroy(sf);
    cleanup();
    PASS("reset");
}

static void test_large_write(void) {
    printf("test_large_write:\n");
    cleanup();
    storage_file_t *sf = storage_file_create(TEST_PATH);

    /* 100K slots = 6.4 MB — triggers file growth */
    uint32_t count = 100000;
    uint8_t *big = calloc(count, 64);
    for (uint32_t i = 0; i < count; i++) {
        uint32_t v = i;
        memcpy(big + i * 64, &v, 4);
        memcpy(big + i * 64 + 32, &v, 4);
    }
    uint64_t off = storage_file_write_section(sf, big, count);
    CHECK(off != UINT64_MAX, "large write ok");

    uint8_t *readback = calloc(count, 64);
    CHECK(storage_file_read_section(sf, off, count, readback), "large read");
    CHECK(memcmp(big, readback, (size_t)count * 64) == 0, "large data matches");

    free(big);
    free(readback);
    storage_file_destroy(sf);
    cleanup();
    PASS("large write (100K slots)");
}

int main(void) {
    printf("=== Storage File Tests ===\n\n");
    test_create_destroy();
    test_write_read();
    test_multiple_sections();
    test_reopen();
    test_reset();
    test_large_write();
    printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
