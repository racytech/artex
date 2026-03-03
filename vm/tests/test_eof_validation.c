/**
 * EOF Validation Tests
 *
 * Tests EOF container parsing and code validation:
 *   1. Valid minimal container (STOP)
 *   2. Valid multi-function (CALLF + RETF)
 *   3. Invalid magic
 *   4. Invalid version
 *   5. Truncated header
 *   6. Legacy opcode rejected (JUMP)
 *   7. Stack underflow (ADD with empty stack)
 *   8. Stack overflow (exceeds max_stack_height)
 *   9. Invalid RJUMP target (lands mid-immediate)
 *  10. Unreachable code after RJUMP
 *  11. CALLF out-of-bounds function index
 *  12. Missing terminator (no STOP/RETURN/RETF at end)
 *  13. Stack height mismatch at RJUMPI join point
 */

#include "eof.h"
#include "vm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
    do { \
        tests_run++; \
        printf("  [%2d] %-50s", tests_run, name); \
    } while(0)

#define PASS() \
    do { \
        tests_passed++; \
        printf("PASS\n"); \
    } while(0)

#define FAIL(msg) \
    do { \
        printf("FAIL: %s\n", msg); \
    } while(0)

#define ASSERT(cond, msg) \
    do { \
        if (!(cond)) { FAIL(msg); return; } \
    } while(0)

//==============================================================================
// Helper: Build EOF container bytes
//==============================================================================

/**
 * Build a minimal EOF1 container with a single code section.
 *
 * Header layout:
 *   EF 00 01                       - magic + version
 *   01 00 04                       - type section: kind=1, size=4
 *   02 00 01 XX XX                 - code section: kind=2, count=1, size=XX
 *   04 00 00                       - data section: kind=4, size=0
 *   00                             - terminator
 *   II OO HH HH                   - type entry: inputs, outputs, max_stack_height
 *   <code bytes>                   - code section body
 */
static uint8_t *build_eof1_single(uint8_t inputs, uint8_t outputs,
                                   uint16_t max_stack_height,
                                   const uint8_t *code, uint16_t code_size,
                                   size_t *out_len)
{
    // Header: 3(magic+ver) + 3(type) + 5(code) + 3(data) + 1(term) = 15
    // Type section: 4 bytes
    // Code section: code_size bytes
    size_t total = 15 + 4 + code_size;
    uint8_t *buf = (uint8_t *)calloc(total, 1);
    if (!buf) return NULL;

    size_t pos = 0;

    // Magic + version
    buf[pos++] = 0xEF;
    buf[pos++] = 0x00;
    buf[pos++] = 0x01;

    // Type section header: kind=1, size=4
    buf[pos++] = 0x01;
    buf[pos++] = 0x00;
    buf[pos++] = 0x04;

    // Code section header: kind=2, count=1, size=code_size
    buf[pos++] = 0x02;
    buf[pos++] = 0x00;
    buf[pos++] = 0x01;
    buf[pos++] = (uint8_t)(code_size >> 8);
    buf[pos++] = (uint8_t)(code_size & 0xFF);

    // Data section header: kind=0xFF, size=0
    buf[pos++] = 0xFF;
    buf[pos++] = 0x00;
    buf[pos++] = 0x00;

    // Terminator
    buf[pos++] = 0x00;

    // Type entry
    buf[pos++] = inputs;
    buf[pos++] = outputs;
    buf[pos++] = (uint8_t)(max_stack_height >> 8);
    buf[pos++] = (uint8_t)(max_stack_height & 0xFF);

    // Code section body
    memcpy(&buf[pos], code, code_size);
    pos += code_size;

    *out_len = pos;
    return buf;
}

/**
 * Build an EOF1 container with two code sections (functions).
 */
static uint8_t *build_eof1_two_funcs(uint8_t in0, uint8_t out0, uint16_t msh0,
                                      const uint8_t *code0, uint16_t code0_size,
                                      uint8_t in1, uint8_t out1, uint16_t msh1,
                                      const uint8_t *code1, uint16_t code1_size,
                                      size_t *out_len)
{
    // Header: 3 + 3(type, size=8) + 7(code, count=2, 2 sizes) + 3(data) + 1(term) = 17
    // Type entries: 8 bytes
    // Code sections: code0_size + code1_size
    size_t total = 17 + 8 + code0_size + code1_size;
    uint8_t *buf = (uint8_t *)calloc(total, 1);
    if (!buf) return NULL;

    size_t pos = 0;

    // Magic + version
    buf[pos++] = 0xEF; buf[pos++] = 0x00; buf[pos++] = 0x01;

    // Type section: kind=1, size=8 (2 functions * 4 bytes)
    buf[pos++] = 0x01;
    buf[pos++] = 0x00;
    buf[pos++] = 0x08;

    // Code section: kind=2, count=2
    buf[pos++] = 0x02;
    buf[pos++] = 0x00;
    buf[pos++] = 0x02;
    buf[pos++] = (uint8_t)(code0_size >> 8);
    buf[pos++] = (uint8_t)(code0_size & 0xFF);
    buf[pos++] = (uint8_t)(code1_size >> 8);
    buf[pos++] = (uint8_t)(code1_size & 0xFF);

    // Data section: kind=0xFF, size=0
    buf[pos++] = 0xFF;
    buf[pos++] = 0x00;
    buf[pos++] = 0x00;

    // Terminator
    buf[pos++] = 0x00;

    // Type entries
    // Function 0
    buf[pos++] = in0;
    buf[pos++] = out0;
    buf[pos++] = (uint8_t)(msh0 >> 8);
    buf[pos++] = (uint8_t)(msh0 & 0xFF);
    // Function 1
    buf[pos++] = in1;
    buf[pos++] = out1;
    buf[pos++] = (uint8_t)(msh1 >> 8);
    buf[pos++] = (uint8_t)(msh1 & 0xFF);

    // Code sections
    memcpy(&buf[pos], code0, code0_size);
    pos += code0_size;
    memcpy(&buf[pos], code1, code1_size);
    pos += code1_size;

    *out_len = pos;
    return buf;
}

//==============================================================================
// Tests
//==============================================================================

/** 1. Valid minimal: single function with STOP */
static void test_valid_minimal(void)
{
    TEST("Valid minimal (STOP)");

    // Code: STOP (0x00)
    uint8_t code[] = { 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_VALID, eof_error_string(err));
    ASSERT(c != NULL, "container is NULL");
    ASSERT(c->num_functions == 1, "expected 1 function");
    ASSERT(c->functions[0].inputs == 0, "expected 0 inputs");
    ASSERT(c->functions[0].outputs == 0x80, "expected non-returning");
    ASSERT(c->functions[0].code_size == 1, "expected code_size=1");

    eof_container_free(c);
    free(eof);
    PASS();
}

/** 2. Valid multi-function: CALLF 1 + STOP, function 1 RETF */
static void test_valid_multi_function(void)
{
    TEST("Valid multi-function (CALLF + RETF)");

    // Function 0: CALLF 0x0001 (E3 00 01) + STOP (00)
    // inputs=0, outputs=0x80 (non-returning), max_stack=0
    uint8_t code0[] = { 0xE3, 0x00, 0x01, 0x00 };

    // Function 1: RETF (E4)
    // inputs=0, outputs=0, max_stack=0
    uint8_t code1[] = { 0xE4 };

    size_t len;
    uint8_t *eof = build_eof1_two_funcs(
        0, 0x80, 0,    code0, sizeof(code0),
        0, 0, 0,       code1, sizeof(code1),
        &len
    );
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_VALID, eof_error_string(err));
    ASSERT(c->num_functions == 2, "expected 2 functions");

    eof_container_free(c);
    free(eof);
    PASS();
}

/** 3. Invalid magic */
static void test_invalid_magic(void)
{
    TEST("Invalid magic (0xEF01)");

    uint8_t bad[] = { 0xEF, 0x01, 0x01, 0x01, 0x00, 0x04,
                      0x02, 0x00, 0x01, 0x00, 0x01,
                      0xFF, 0x00, 0x00,
                      0x00,
                      0x00, 0x80, 0x00, 0x00,
                      0x00 };
    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(bad, sizeof(bad), &c);

    ASSERT(err == EOF_INVALID_MAGIC, "expected INVALID_MAGIC");
    ASSERT(c == NULL, "container should be NULL");
    PASS();
}

/** 4. Invalid version */
static void test_invalid_version(void)
{
    TEST("Invalid version (0x02)");

    uint8_t bad[] = { 0xEF, 0x00, 0x02, 0x01, 0x00, 0x04,
                      0x02, 0x00, 0x01, 0x00, 0x01,
                      0xFF, 0x00, 0x00,
                      0x00,
                      0x00, 0x80, 0x00, 0x00,
                      0x00 };
    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(bad, sizeof(bad), &c);

    ASSERT(err == EOF_INVALID_VERSION, "expected INVALID_VERSION");
    ASSERT(c == NULL, "container should be NULL");
    PASS();
}

/** 5. Truncated header */
static void test_truncated_header(void)
{
    TEST("Truncated header");

    uint8_t bad[] = { 0xEF, 0x00, 0x01, 0x01, 0x00 };
    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(bad, sizeof(bad), &c);

    ASSERT(err != EOF_VALID, "should not validate");
    ASSERT(c == NULL, "container should be NULL");
    PASS();
}

/** 6. Legacy opcode rejected: JUMP (0x56) */
static void test_legacy_opcode_rejected(void)
{
    TEST("Legacy opcode rejected (JUMP 0x56)");

    // Code: JUMP (0x56) — invalid in EOF
    uint8_t code[] = { 0x56 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_UNKNOWN_OPCODE, "expected UNKNOWN_OPCODE");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 7. Stack underflow: ADD with empty stack */
static void test_stack_underflow(void)
{
    TEST("Stack underflow (ADD with 0 items)");

    // Code: ADD (0x01) + STOP (0x00)
    // ADD needs 2 items, but stack starts empty
    uint8_t code[] = { 0x01, 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 1, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_STACK_UNDERFLOW, "expected STACK_UNDERFLOW");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 8. Stack overflow: PUSHes exceeding max_stack_height */
static void test_stack_overflow(void)
{
    TEST("Stack height mismatch (declared too low)");

    // Code: PUSH0 PUSH0 ADD STOP
    // max_stack_height is 2 (PUSH0 PUSH0 → height=2, ADD → height=1)
    // but we declare max_stack_height=1 → mismatch
    uint8_t code[] = { 0x5F, 0x5F, 0x01, 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 1, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    // max_stack computed is 2, declared is 1 → mismatch
    ASSERT(err == EOF_STACK_HEIGHT_MISMATCH, "expected STACK_HEIGHT_MISMATCH");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 9. Invalid RJUMP: target lands in middle of PUSH2 immediate */
static void test_invalid_rjump_target(void)
{
    TEST("Invalid RJUMP target (mid-immediate)");

    // Code: RJUMP +1 (E0 00 01), PUSH2 XX XX (61 AA BB), STOP (00)
    // RJUMP target = offset 3 + 1 = offset 4, which is inside PUSH2's immediate
    uint8_t code[] = { 0xE0, 0x00, 0x01, 0x61, 0xAA, 0xBB, 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_INVALID_RJUMP_TARGET, "expected INVALID_RJUMP_TARGET");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 10. Unreachable code after RJUMP */
static void test_unreachable_code(void)
{
    TEST("Unreachable code after RJUMP");

    // Code: RJUMP +1 (E0 00 01), PUSH0 (5F), STOP (00)
    // RJUMP jumps over PUSH0 to STOP at offset 4
    // PUSH0 at offset 3 is unreachable
    uint8_t code[] = { 0xE0, 0x00, 0x01, 0x5F, 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_UNREACHABLE_CODE, "expected UNREACHABLE_CODE");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 11. CALLF out-of-bounds function index */
static void test_callf_out_of_bounds(void)
{
    TEST("CALLF out-of-bounds function index");

    // Code: CALLF 0x0005 (E3 00 05), STOP (00)
    // Only 1 function exists → index 5 is out of bounds
    uint8_t code[] = { 0xE3, 0x00, 0x05, 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_INVALID_FUNCTION_INDEX, "expected INVALID_FUNCTION_INDEX");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 12. Missing terminator: code ends without STOP/RETURN/RETF */
static void test_missing_terminator(void)
{
    TEST("Missing terminator (code ends on ADD)");

    // Code: PUSH0 (5F), PUSH0 (5F), ADD (01)
    // ADD is not a terminating instruction
    uint8_t code[] = { 0x5F, 0x5F, 0x01 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_INVALID_TERMINATION, "expected INVALID_TERMINATION");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 13. Stack height mismatch at backward RJUMP */
static void test_stack_height_mismatch_rjumpi(void)
{
    TEST("Stack height mismatch at backward RJUMP");

    // Code:
    //   offset 0: PUSH0 (5F)           → stack: 0→1
    //   offset 1: RJUMP +2 (E0 00 02)  → stack: 1 (goto offset 6)
    //   offset 4: STOP (00)            → unreachable? no...
    //   offset 5: POP (50)             → would need stack
    //   offset 6: PUSH0 (5F)           → stack: 1→2 (from RJUMP target)
    //   offset 7: RJUMP -7 (E0 FF F9)  → backward jump to offset 0, stack: 2
    //
    // At offset 0: first visit height={1,1} (from entry with 0 inputs + PUSH0),
    // backward jump arrives with {2,2}. Since 2!=1, this is a mismatch.
    //
    // Actually let me simplify. Use RJUMPI for a backward jump scenario:
    //
    //   offset 0: PUSH0 (5F)            → stack: 0→1
    //   offset 1: PUSH0 (5F)            → stack: 1→2
    //   offset 2: RJUMPI -3 (E1 FF FD)  → pops 1 (cond), stack: 2→1
    //      fall-through: offset 5, stack {1,1}
    //      backward target: offset 2 + 3 + (-3) = offset 2, stack {1,1}
    //   But offset 2 was first reached with height {2,2}. So {1,1} != {2,2} → mismatch!
    //
    uint8_t code[] = {
        0x5F,                   // offset 0: PUSH0
        0x5F,                   // offset 1: PUSH0
        0xE1, 0xFF, 0xFD,      // offset 2: RJUMPI -3 → backward to offset 2
        0x00,                   // offset 5: STOP
    };
    size_t len;
    // max_stack_height=2 (offset 1: PUSH0 PUSH0 → 2)
    uint8_t *eof = build_eof1_single(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_STACK_HEIGHT_MISMATCH, "expected STACK_HEIGHT_MISMATCH");
    ASSERT(c == NULL, "container should be NULL");

    free(eof);
    PASS();
}

/** 14. Valid PUSH0 + ADD + STOP (correct max_stack_height) */
static void test_valid_push_add(void)
{
    TEST("Valid PUSH0 PUSH0 ADD STOP (msh=2)");

    // Code: PUSH0 PUSH0 ADD STOP → max stack is 2
    uint8_t code[] = { 0x5F, 0x5F, 0x01, 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_VALID, eof_error_string(err));
    ASSERT(c != NULL, "container is NULL");
    ASSERT(c->functions[0].max_stack_height == 2, "expected msh=2");

    eof_container_free(c);
    free(eof);
    PASS();
}

/** 15. Valid RJUMP (forward jump) */
static void test_valid_rjump(void)
{
    TEST("Valid RJUMP forward");

    // Code: RJUMP +0 (E0 00 00) STOP (00)
    // Jump offset 0 → lands at offset 3 (just past the immediate) = STOP
    uint8_t code[] = { 0xE0, 0x00, 0x00, 0x00 };
    size_t len;
    uint8_t *eof = build_eof1_single(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof, len, &c);

    ASSERT(err == EOF_VALID, eof_error_string(err));

    eof_container_free(c);
    free(eof);
    PASS();
}

//==============================================================================
// VM Lifecycle Tests
//==============================================================================

/** 16. VM create/destroy */
static void test_vm_lifecycle(void)
{
    TEST("VM create/destroy");

    vm_t *vm = vm_create();
    ASSERT(vm != NULL, "vm_create returned NULL");
    ASSERT(vm->memory != NULL, "memory not created");
    ASSERT(vm->return_stack != NULL, "return_stack not created");
    ASSERT(vm->stack == NULL, "stack should be NULL before execute");

    vm_destroy(vm);
    PASS();
}

/** 17. VM execute stub returns success */
static void test_vm_execute_stub(void)
{
    TEST("VM execute stub returns success");

    // Build a valid minimal container
    uint8_t code[] = { 0x00 }; // STOP
    size_t len;
    uint8_t *eof_bytes = build_eof1_single(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof_bytes, "build failed");

    eof_container_t *c = NULL;
    eof_validation_error_t err = eof_validate(eof_bytes, len, &c);
    ASSERT(err == EOF_VALID, "validation failed");

    vm_t *vm = vm_create();
    ASSERT(vm, "vm_create failed");

    vm_message_t msg = { .gas = 100000 };
    vm_result_t result;
    bool ok = vm_execute(vm, c, &msg, &result);

    ASSERT(ok, "vm_execute returned false");
    ASSERT(result.status == VM_SUCCESS, "expected VM_SUCCESS");
    ASSERT(result.gas_left == 100000, "gas should be preserved in stub");

    vm_result_free(&result);
    vm_destroy(vm);
    eof_container_free(c);
    free(eof_bytes);
    PASS();
}

//==============================================================================
// Main
//==============================================================================

int main(void)
{
    printf("\n=== EOF Validation Tests ===\n\n");

    // EOF validation tests
    test_valid_minimal();
    test_valid_multi_function();
    test_invalid_magic();
    test_invalid_version();
    test_truncated_header();
    test_legacy_opcode_rejected();
    test_stack_underflow();
    test_stack_overflow();
    test_invalid_rjump_target();
    test_unreachable_code();
    test_callf_out_of_bounds();
    test_missing_terminator();
    test_stack_height_mismatch_rjumpi();
    test_valid_push_add();
    test_valid_rjump();

    // VM lifecycle tests
    test_vm_lifecycle();
    test_vm_execute_stub();

    printf("\n=== Results: %d/%d passed ===\n\n", tests_passed, tests_run);

    return (tests_passed == tests_run) ? 0 : 1;
}
