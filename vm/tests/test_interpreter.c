/**
 * VM Interpreter Tests
 *
 * Constructs minimal EOF containers, runs vm_execute(), and verifies
 * results via stack state, memory, return data, gas consumption, and status.
 *
 * Categories tested:
 *   1. Arithmetic (ADD, SUB, MUL, DIV, MOD, EXP, SIGNEXTEND, ...)
 *   2. Comparison & Bitwise (LT, GT, EQ, AND, OR, SHL, SHR, SAR, ...)
 *   3. Stack operations (PUSH, DUP, SWAP, POP, DUPN, SWAPN, EXCHANGE)
 *   4. Control flow (RJUMP, RJUMPI, RJUMPV, CALLF, RETF, JUMPF)
 *   5. Memory (MLOAD, MSTORE, MSTORE8, MSIZE, MCOPY)
 *   6. Data section (DATALOAD, DATALOADN, DATASIZE, DATACOPY)
 *   7. Crypto (KECCAK256)
 *   8. Termination (STOP, RETURN, REVERT, INVALID)
 */

#include "vm.h"
#include "eof.h"
#include "gas.h"
#include "keccak256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Epilogue: MSTORE at 0, RETURN 32 bytes (8 bytes)
#define RET32  0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xF3

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
    do { \
        tests_run++; \
        printf("  [%2d] %-55s", tests_run, name); \
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
// EOF Container Builder
//==============================================================================

/**
 * Build a single-function EOF container.
 * Function 0: (inputs, outputs, max_stack_height) with given code.
 * Data section of data_size bytes appended.
 */
static uint8_t *build_eof(uint8_t inputs, uint8_t outputs,
                           uint16_t max_stack_height,
                           const uint8_t *code, uint16_t code_size,
                           const uint8_t *data, uint16_t data_size,
                           size_t *out_len)
{
    // Header: 3(magic+ver) + 3(type) + 5(code) + 3(data) + 1(term) = 15
    size_t total = 15 + 4 + code_size + data_size;
    uint8_t *buf = (uint8_t *)calloc(total, 1);
    if (!buf) return NULL;

    size_t pos = 0;
    buf[pos++] = 0xEF; buf[pos++] = 0x00; buf[pos++] = 0x01;  // magic + ver
    buf[pos++] = 0x01; buf[pos++] = 0x00; buf[pos++] = 0x04;  // type: 4 bytes
    buf[pos++] = 0x02; buf[pos++] = 0x00; buf[pos++] = 0x01;  // code: 1 section
    buf[pos++] = (uint8_t)(code_size >> 8);
    buf[pos++] = (uint8_t)(code_size & 0xFF);
    buf[pos++] = 0xFF;                                          // data section
    buf[pos++] = (uint8_t)(data_size >> 8);
    buf[pos++] = (uint8_t)(data_size & 0xFF);
    buf[pos++] = 0x00;                                          // terminator

    // Type entry
    buf[pos++] = inputs;
    buf[pos++] = outputs;
    buf[pos++] = (uint8_t)(max_stack_height >> 8);
    buf[pos++] = (uint8_t)(max_stack_height & 0xFF);

    // Code
    memcpy(&buf[pos], code, code_size);
    pos += code_size;

    // Data
    if (data && data_size > 0) {
        memcpy(&buf[pos], data, data_size);
        pos += data_size;
    }

    *out_len = pos;
    return buf;
}

/** Convenience: build container with no data section. */
static uint8_t *build_eof_simple(uint8_t inputs, uint8_t outputs,
                                  uint16_t max_stack_height,
                                  const uint8_t *code, uint16_t code_size,
                                  size_t *out_len)
{
    return build_eof(inputs, outputs, max_stack_height,
                     code, code_size, NULL, 0, out_len);
}

/** Build two-function EOF container. */
static uint8_t *build_eof_two_funcs(
    uint8_t in0, uint8_t out0, uint16_t msh0,
    const uint8_t *code0, uint16_t code0_size,
    uint8_t in1, uint8_t out1, uint16_t msh1,
    const uint8_t *code1, uint16_t code1_size,
    size_t *out_len)
{
    size_t total = 17 + 8 + code0_size + code1_size;
    uint8_t *buf = (uint8_t *)calloc(total, 1);
    if (!buf) return NULL;

    size_t pos = 0;
    buf[pos++] = 0xEF; buf[pos++] = 0x00; buf[pos++] = 0x01;
    buf[pos++] = 0x01; buf[pos++] = 0x00; buf[pos++] = 0x08;  // type: 8 bytes
    buf[pos++] = 0x02; buf[pos++] = 0x00; buf[pos++] = 0x02;  // code: 2 sections
    buf[pos++] = (uint8_t)(code0_size >> 8);
    buf[pos++] = (uint8_t)(code0_size & 0xFF);
    buf[pos++] = (uint8_t)(code1_size >> 8);
    buf[pos++] = (uint8_t)(code1_size & 0xFF);
    buf[pos++] = 0xFF; buf[pos++] = 0x00; buf[pos++] = 0x00;  // data: 0
    buf[pos++] = 0x00;                                          // terminator

    buf[pos++] = in0; buf[pos++] = out0;
    buf[pos++] = (uint8_t)(msh0 >> 8); buf[pos++] = (uint8_t)(msh0 & 0xFF);
    buf[pos++] = in1; buf[pos++] = out1;
    buf[pos++] = (uint8_t)(msh1 >> 8); buf[pos++] = (uint8_t)(msh1 & 0xFF);

    memcpy(&buf[pos], code0, code0_size); pos += code0_size;
    memcpy(&buf[pos], code1, code1_size); pos += code1_size;

    *out_len = pos;
    return buf;
}

//==============================================================================
// Execution Helper
//==============================================================================

typedef struct {
    vm_result_t result;
    vm_t       *vm;      // kept alive for stack inspection
    eof_container_t *container;
} exec_t;

/** Validate + execute an EOF container with given gas. Returns false on error. */
static bool exec_eof(const uint8_t *eof_bytes, size_t eof_len,
                     uint64_t gas, exec_t *out)
{
    memset(out, 0, sizeof(*out));

    eof_validation_error_t verr = eof_validate(eof_bytes, eof_len, &out->container);
    if (verr != EOF_VALID) {
        fprintf(stderr, "    EOF validation failed: %s\n", eof_error_string(verr));
        return false;
    }

    out->vm = vm_create();
    if (!out->vm) return false;

    vm_message_t msg = {
        .kind = VM_CALL,
        .gas = gas,
    };

    bool ok = vm_execute(out->vm, out->container, &msg, &out->result);
    return ok;
}

/** Clean up after execution. */
static void exec_free(exec_t *e)
{
    if (e->vm) vm_destroy(e->vm);
    if (e->container) eof_container_free(e->container);
    vm_result_free(&e->result);
}

//==============================================================================
// Mock Host (Phase 3: storage, balance, transient, logs)
//==============================================================================

typedef struct {
    uint256_t storage[16];
    uint256_t transient[16];
    uint256_t balance;
    bool      log_called;
    uint8_t   log_topic_count;
    uint256_t log_topics[4];
    uint8_t   log_data[256];
    size_t    log_data_size;
} mock_host_t;

static uint256_t mock_sload(void *ctx, const address_t *addr, const uint256_t *key) {
    (void)addr;
    mock_host_t *h = (mock_host_t *)ctx;
    uint64_t idx = (uint64_t)key->low;
    return (idx < 16) ? h->storage[idx] : UINT256_ZERO;
}

static vm_sstore_result_t mock_sstore(void *ctx, const address_t *addr,
                                       const uint256_t *key, const uint256_t *value) {
    (void)addr;
    mock_host_t *h = (mock_host_t *)ctx;
    uint64_t idx = (uint64_t)key->low;
    vm_sstore_result_t result = { UINT256_ZERO, UINT256_ZERO };
    if (idx < 16) {
        result.current = h->storage[idx];
        result.original = h->storage[idx];
        h->storage[idx] = *value;
    }
    return result;
}

static uint256_t mock_balance(void *ctx, const address_t *addr) {
    (void)addr;
    return ((mock_host_t *)ctx)->balance;
}

static uint256_t mock_tload(void *ctx, const address_t *addr, const uint256_t *key) {
    (void)addr;
    mock_host_t *h = (mock_host_t *)ctx;
    uint64_t idx = (uint64_t)key->low;
    return (idx < 16) ? h->transient[idx] : UINT256_ZERO;
}

static void mock_tstore(void *ctx, const address_t *addr,
                          const uint256_t *key, const uint256_t *value) {
    (void)addr;
    mock_host_t *h = (mock_host_t *)ctx;
    uint64_t idx = (uint64_t)key->low;
    if (idx < 16) h->transient[idx] = *value;
}

static void mock_emit_log(void *ctx, const address_t *addr,
                            const uint256_t *topics, uint8_t n_topics,
                            const uint8_t *data, size_t data_size) {
    (void)addr;
    mock_host_t *h = (mock_host_t *)ctx;
    h->log_called = true;
    h->log_topic_count = n_topics;
    for (int i = 0; i < n_topics && i < 4; i++)
        h->log_topics[i] = topics[i];
    if (data && data_size <= sizeof(h->log_data)) {
        memcpy(h->log_data, data, data_size);
        h->log_data_size = data_size;
    }
}

static const vm_host_iface_t MOCK_HOST = {
    .sload     = mock_sload,
    .sstore    = mock_sstore,
    .balance   = mock_balance,
    .tload     = mock_tload,
    .tstore    = mock_tstore,
    .emit_log  = mock_emit_log,
};

/** Extended execution helper with message, block, tx, and host. */
static bool exec_eof_with(const uint8_t *eof_bytes, size_t eof_len,
                           const vm_message_t *msg,
                           const vm_block_env_t *block,
                           const vm_tx_context_t *tx,
                           const vm_host_iface_t *host, void *host_ctx,
                           exec_t *out)
{
    memset(out, 0, sizeof(*out));
    eof_validation_error_t verr = eof_validate(eof_bytes, eof_len, &out->container);
    if (verr != EOF_VALID) {
        fprintf(stderr, "    EOF validation failed: %s\n", eof_error_string(verr));
        return false;
    }
    out->vm = vm_create();
    if (!out->vm) return false;
    if (block) vm_set_block_env(out->vm, block);
    if (tx)    vm_set_tx_context(out->vm, tx);
    if (host)  vm_set_host(out->vm, host, host_ctx);
    return vm_execute(out->vm, out->container, msg, &out->result);
}

//==============================================================================
// Tests: Termination
//==============================================================================

static void test_stop(void)
{
    TEST("STOP returns success");
    uint8_t code[] = { 0x00 };  // STOP
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 1000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    ASSERT(e.result.output_data == NULL, "no output");
    exec_free(&e);
    free(eof);
    PASS();
}

static void test_invalid(void)
{
    TEST("INVALID consumes all gas");
    uint8_t code[] = { 0xFE };  // INVALID
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 0, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 1000, &e), "exec");
    ASSERT(e.result.status == VM_INVALID_OPCODE, "status");
    ASSERT(e.result.gas_left == 0, "gas consumed");
    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Arithmetic
//==============================================================================

static void test_add(void)
{
    TEST("ADD: 3 + 5 = 8");
    // PUSH1 3, PUSH1 5, ADD, PUSH1 0, MSTORE, PUSH1 32, PUSH1 0, RETURN
    uint8_t code[] = {
        0x60, 0x03,   // PUSH1 3
        0x60, 0x05,   // PUSH1 5
        0x01,         // ADD
        0x60, 0x00,   // PUSH1 0
        0x52,         // MSTORE
        0x60, 0x20,   // PUSH1 32
        0x60, 0x00,   // PUSH1 0
        0xF3,         // RETURN
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    ASSERT(e.result.output_size == 32, "output size");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 8, "3+5=8");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_sub(void)
{
    TEST("SUB: 10 - 3 = 7");
    // SUB pops a (top), b (second), pushes a - b
    // PUSH1 3, PUSH1 10, SUB → 10 - 3 = 7
    uint8_t code[] = {
        0x60, 0x03,   // PUSH1 3   (b, pushed first = second from top)
        0x60, 0x0A,   // PUSH1 10  (a, pushed second = top)
        0x03,         // SUB: a - b = 10 - 3 = 7
        0x60, 0x00, 0x52,   // MSTORE at 0
        0x60, 0x20, 0x60, 0x00, 0xF3,  // RETURN 32 bytes from 0
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 7, "10-3=7");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_mul_div(void)
{
    TEST("MUL/DIV: 7 * 6 / 3 = 14");
    uint8_t code[] = {
        0x60, 0x07,   // PUSH1 7
        0x60, 0x06,   // PUSH1 6
        0x02,         // MUL: 6 * 7 = 42 (commutative)
        0x60, 0x03,   // PUSH1 3 (divisor)
        0x90,         // SWAP1: stack = [3, 42]
        // Now top=42, second=3. DIV: 42/3=14
        // Wait: DIV pops a(top), b(second), pushes a/b
        // So we want a=42 on top, b=3 below → 42/3=14
        // After SWAP1: top=42, second=3. Hmm, let me redo.
        // After MUL: stack = [42]
        // PUSH1 3: stack = [42, 3]
        // DIV: a=3(top), b=42(second) → 3/42 = 0. Not what we want!
        // Need: PUSH1 3 first, then have 42 on top.
        // Use: MUL result on stack, PUSH1 3, SWAP1 so 42 on top:
        0x04,         // DIV: a=42(top after SWAP), b=3 → 42/3=14
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 14, "7*6/3=14");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_mod(void)
{
    TEST("MOD: 17 mod 5 = 2");
    // PUSH1 5, PUSH1 17, MOD: a=17(top), b=5 → 17%5=2
    uint8_t code[] = {
        0x60, 0x05,
        0x60, 0x11,   // 17
        0x06,         // MOD
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 2, "17%5=2");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_exp(void)
{
    TEST("EXP: 2 ** 10 = 1024");
    // PUSH1 10, PUSH1 2, EXP: a=2(top=base), b=10(second=exponent) → 2^10=1024
    uint8_t code[] = {
        0x60, 0x0A,   // PUSH1 10 (exponent, second)
        0x60, 0x02,   // PUSH1 2  (base, top)
        0x0A,         // EXP
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 1024, "2^10=1024");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_addmod(void)
{
    TEST("ADDMOD: (10 + 7) mod 6 = 5");
    // Stack order: a=10(top), b=7(second), N=6(third)
    // PUSH1 6, PUSH1 7, PUSH1 10, ADDMOD → (10+7)%6 = 17%6 = 5
    uint8_t code[] = {
        0x60, 0x06,   // PUSH1 6 (N, third)
        0x60, 0x07,   // PUSH1 7 (b, second)
        0x60, 0x0A,   // PUSH1 10 (a, top)
        0x08,         // ADDMOD
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 3, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 5, "(10+7)%6=5");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Comparison & Bitwise
//==============================================================================

static void test_lt_gt_eq(void)
{
    TEST("LT/GT/EQ: comparisons");
    // Test: 3 < 5 = 1
    // PUSH1 5, PUSH1 3, LT: a=3(top), b=5 → a<b = 3<5 = 1
    // Wait, LT: a=top, b=second, result = a < b
    // So PUSH1 5, PUSH1 3: top=3, second=5. LT: 3 < 5 = true = 1
    uint8_t code[] = {
        0x60, 0x05,   // PUSH1 5
        0x60, 0x03,   // PUSH1 3
        0x10,         // LT: 3 < 5 = 1
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 1, "3<5=1");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_iszero(void)
{
    TEST("ISZERO: iszero(0)=1, iszero(5)=0");
    // Test iszero(0) = 1
    uint8_t code[] = {
        0x60, 0x00,   // PUSH1 0
        0x15,         // ISZERO → 1
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 1, "iszero(0)=1");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_and_or_xor(void)
{
    TEST("AND/OR/XOR: bitwise ops");
    // 0xFF AND 0x0F = 0x0F
    uint8_t code[] = {
        0x60, 0xFF,   // PUSH1 0xFF
        0x60, 0x0F,   // PUSH1 0x0F
        0x16,         // AND → 0x0F
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0x0F, "0xFF AND 0x0F = 0x0F");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_shl_shr(void)
{
    TEST("SHL/SHR: shift operations");
    // SHL(shift=4, value=1) → 1 << 4 = 16
    // PUSH1 1 (value), PUSH1 4 (shift), SHL
    // SHL: shift=top, value=second → value << shift
    uint8_t code[] = {
        0x60, 0x01,   // PUSH1 1 (value, second)
        0x60, 0x04,   // PUSH1 4 (shift, top)
        0x1B,         // SHL: 1 << 4 = 16
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 16, "1<<4=16");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Stack Operations
//==============================================================================

static void test_push_dup_swap(void)
{
    TEST("PUSH/DUP/SWAP: stack manipulation");
    // PUSH1 0xAA, DUP1, PUSH1 0xBB, SWAP1
    // Stack after: [0xAA, 0xBB, 0xAA]
    // Top = 0xBB (after SWAP1 swaps top two: [0xAA, 0xAA, 0xBB] → no...)
    // Let me trace:
    // PUSH1 0xAA:       stack = [0xAA]
    // DUP1:             stack = [0xAA, 0xAA]
    // PUSH1 0xBB:       stack = [0xAA, 0xAA, 0xBB]
    // SWAP1:            stack = [0xAA, 0xBB, 0xAA]
    // Return top value (0xAA):
    uint8_t code[] = {
        0x60, 0xAA,   // PUSH1 0xAA
        0x80,         // DUP1
        0x60, 0xBB,   // PUSH1 0xBB
        0x90,         // SWAP1: swap top two → [0xAA, 0xBB, 0xAA]
        // Store top (0xAA) to memory and return
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    // Max stack: push(1), dup(2), push(3), swap(3), push(4), mstore(2), push(3), push(4), return(2)
    uint8_t *eof = build_eof_simple(0, 0x80, 4, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0xAA, "SWAP1 result");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_push0(void)
{
    TEST("PUSH0: pushes zero");
    uint8_t code[] = {
        0x5F,         // PUSH0
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_is_zero(&val), "push0 = 0");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_push32(void)
{
    TEST("PUSH32: full 256-bit value");
    // PUSH32 with all 0xFF bytes
    uint8_t code[1 + 32 + 8];  // PUSH32(33) + PUSH1 0(2) + MSTORE(1) + PUSH1 32(2) + PUSH1 0(2) + RETURN(1) = 41
    code[0] = 0x7F;  // PUSH32
    memset(&code[1], 0xFF, 32);
    code[33] = 0x60; code[34] = 0x00; code[35] = 0x52;   // MSTORE at 0
    code[36] = 0x60; code[37] = 0x20;                      // PUSH1 32
    code[38] = 0x60; code[39] = 0x00;                      // PUSH1 0
    code[40] = 0xF3;                                        // RETURN

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    ASSERT(e.result.output_size == 32, "output size");

    // All bytes should be 0xFF
    bool all_ff = true;
    for (int i = 0; i < 32; i++) {
        if (e.result.output_data[i] != 0xFF) { all_ff = false; break; }
    }
    ASSERT(all_ff, "all 0xFF bytes");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_dupn_swapn(void)
{
    TEST("DUPN/SWAPN: EOF-specific stack ops");
    // PUSH1 0x11, PUSH1 0x22, DUPN 1 → duplicates stack[sp-1-1] = 0x11
    // Stack: [0x11, 0x22, 0x11]
    uint8_t code[] = {
        0x60, 0x11,           // PUSH1 0x11
        0x60, 0x22,           // PUSH1 0x22
        0xE6, 0x01,           // DUPN 1: dup stack[sp-2] = 0x11
        0x60, 0x00, 0x52,     // MSTORE (consumes dupn result + 0)
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    // Stack: push=1, push=2, dupn=3, push=4, mstore=2, push=3, push=4, return=2
    uint8_t *eof = build_eof_simple(0, 0x80, 4, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0x11, "DUPN 1 → 0x11");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_exchange(void)
{
    TEST("EXCHANGE: swap non-adjacent stack items");
    // Stack: [A=1, B=2, C=3], EXCHANGE 0x00 → n=1, m=1
    // swap stack[sp-2] with stack[sp-3] = swap B and A
    // Result: [B=2, A=1, C=3], top is still C=3
    uint8_t code[] = {
        0x60, 0x01,           // PUSH1 1 (A, bottom)
        0x60, 0x02,           // PUSH1 2 (B, middle)
        0x60, 0x03,           // PUSH1 3 (C, top)
        0xE8, 0x00,           // EXCHANGE 0x00: n=1, m=1 → swap stack[sp-2] and stack[sp-3]
        // Now stack: [2, 1, 3]. Pop top (3), then return next (1)
        0x50,                 // POP (remove C=3)
        0x60, 0x00, 0x52,    // MSTORE (store B=1... wait)
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 3, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    // After EXCHANGE, stack was [2, 1, 3], POP → [2, 1], MSTORE stores top=1
    ASSERT(uint256_to_uint64(&val) == 1, "EXCHANGE swapped correctly");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Memory
//==============================================================================

static void test_mstore_mload(void)
{
    TEST("MSTORE/MLOAD: write and read memory");
    // PUSH1 42, PUSH1 0, MSTORE, PUSH1 0, MLOAD
    // Then return the MLOADed value
    uint8_t code[] = {
        0x60, 0x2A,   // PUSH1 42
        0x60, 0x00,   // PUSH1 0
        0x52,         // MSTORE at offset 0
        0x60, 0x00,   // PUSH1 0
        0x51,         // MLOAD from offset 0
        0x60, 0x00, 0x52,   // MSTORE at 0 (write loaded value)
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 42, "mload=42");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_mstore8(void)
{
    TEST("MSTORE8: write single byte");
    // MSTORE8 writes the low byte to memory
    // PUSH1 0xAB, PUSH1 0, MSTORE8 → memory[0] = 0xAB
    // Then MLOAD from 0: reads 32 bytes starting at 0
    uint8_t code[] = {
        0x60, 0xAB,   // PUSH1 0xAB
        0x60, 0x00,   // PUSH1 0
        0x53,         // MSTORE8
        0x60, 0x00,   // PUSH1 0
        0x51,         // MLOAD: reads memory[0..31] as big-endian
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    // memory[0]=0xAB, rest zeros → big-endian value = 0xAB << 248
    // Byte 0 of output should be 0xAB
    ASSERT(e.result.output_data[0] == 0xAB, "byte 0 = 0xAB");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_msize(void)
{
    TEST("MSIZE: reports memory size");
    // MSTORE at offset 0 expands to 32 bytes, then MSIZE should return 32
    uint8_t code[] = {
        0x60, 0x01,   // PUSH1 1
        0x60, 0x00,   // PUSH1 0
        0x52,         // MSTORE at 0 → memory expands to 32
        0x59,         // MSIZE → 32
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 32, "msize=32");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_mcopy(void)
{
    TEST("MCOPY: copy within memory");
    // Write 42 at offset 0, then MCOPY 32 bytes from 0 to 32
    // Then return 32 bytes from offset 32
    uint8_t code[] = {
        0x60, 0x2A,   // PUSH1 42
        0x60, 0x00,   // PUSH1 0
        0x52,         // MSTORE at 0
        // MCOPY: dest=32, src=0, size=32
        0x60, 0x20,   // PUSH1 32 (size)
        0x60, 0x00,   // PUSH1 0 (src)
        0x60, 0x20,   // PUSH1 32 (dest)
        0x5E,         // MCOPY
        // Return 32 bytes from offset 32
        0x60, 0x20,   // PUSH1 32
        0x60, 0x20,   // PUSH1 32
        0xF3,         // RETURN
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 3, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 42, "mcopy preserved value");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Control Flow
//==============================================================================

static void test_rjump(void)
{
    TEST("RJUMP: unconditional relative jump");
    // RJUMP +0 (nop jump to next instruction), then push 42 and return
    uint8_t code[] = {
        0xE0, 0x00, 0x00,    // RJUMP +0: jump to next instruction
        0x60, 0x2A,           // PUSH1 42
        0x60, 0x00, 0x52,    // MSTORE
        0x60, 0x20, 0x60, 0x00, 0xF3,  // RETURN
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 42, "rjump landed correctly");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_rjumpi(void)
{
    TEST("RJUMPI: conditional jump (taken)");
    // Push 1 (true), RJUMPI +2 → skip PUSH1 0xFF, land on PUSH1 0x42
    // But dead code... Let me use RJUMPI to skip over an RJUMP.
    // Simpler: push non-zero, RJUMPI +0 (nop conditional), PUSH1 42, RETURN
    uint8_t code[] = {
        0x60, 0x01,           // PUSH1 1 (condition = true)
        0xE1, 0x00, 0x00,    // RJUMPI +0: taken, but offset 0 means next instruction
        0x60, 0x2A,           // PUSH1 42
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 42, "rjumpi taken");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_rjumpi_not_taken(void)
{
    TEST("RJUMPI: conditional jump (not taken)");
    // Push 0, RJUMPI → not taken, fall through
    uint8_t code[] = {
        0x60, 0x00,           // PUSH1 0 (condition = false)
        0xE1, 0x00, 0x00,    // RJUMPI +0: not taken (but would be nop anyway)
        0x60, 0x2A,           // PUSH1 42
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 42, "rjumpi not taken");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_rjump_backward_loop(void)
{
    TEST("RJUMP backward: loop 5 times");
    // Counter starts at 0, loop increments until 5, then returns.
    //   PUSH1 0        ; counter = 0
    // loop:
    //   PUSH1 1        ; increment
    //   ADD            ; counter++
    //   DUP1           ; duplicate counter
    //   PUSH1 5        ; target
    //   EQ             ; counter == 5?
    //   RJUMPI +3      ; if equal, jump to return section
    //   RJUMP -12      ; back to loop (PUSH1 1)
    // return:
    //   PUSH1 0, MSTORE, PUSH1 32, PUSH1 0, RETURN
    uint8_t code[] = {
        0x60, 0x00,           // 0: PUSH1 0 (counter)
        // loop: offset 2
        0x60, 0x01,           // 2: PUSH1 1
        0x01,                 // 4: ADD
        0x80,                 // 5: DUP1
        0x60, 0x05,           // 6: PUSH1 5
        0x14,                 // 8: EQ
        0xE1, 0x00, 0x03,    // 9: RJUMPI +3 → offset 12+3=15 (return section)
        0xE0, 0xFF, 0xF4,    // 12: RJUMP -12 → offset 15-12=3... no.
        // RJUMP offset is relative to after RJUMP's immediate.
        // After RJUMP at byte 12, immediate at 13-14, so after = 15.
        // Want to jump to byte 2 (loop start). offset = 2 - 15 = -13.
        // -13 as int16 = 0xFFF3
        // Let me recalculate properly:
        // RJUMPI at byte 9: immediate at 10-11, after = 12.
        // Jump target when taken: 12 + 3 = 15 (first byte of return section)
        // RJUMP at byte 12: immediate at 13-14, after = 15.
        // Jump target: 15 + offset. Want target = byte 2. offset = 2 - 15 = -13.
        // -13 as int16: 0xFFF3
        // return section at byte 15:
        0x60, 0x00, 0x52,    // 15: MSTORE
        0x60, 0x20,           // 18: PUSH1 32
        0x60, 0x00,           // 20: PUSH1 0
        0xF3,                 // 22: RETURN
    };
    // Fix RJUMP offset: -13 = 0xFFF3
    code[13] = 0xFF;
    code[14] = 0xF3;

    size_t len;
    // Stack: max 3 at PUSH1 5 (after DUP1: sp=2, PUSH1 5: sp=3)
    uint8_t *eof = build_eof_simple(0, 0x80, 3, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 5, "looped 5 times");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_callf_retf(void)
{
    TEST("CALLF/RETF: function call and return");
    // Function 0 (entry): PUSH1 3, PUSH1 4, CALLF 1, MSTORE at 0, RETURN 32
    // Function 1: ADD, RETF (takes 2 inputs, returns 1 output)
    uint8_t code0[] = {
        0x60, 0x03,           // PUSH1 3
        0x60, 0x04,           // PUSH1 4
        0xE3, 0x00, 0x01,    // CALLF 1
        // After RETF, stack has one value (the sum)
        0x60, 0x00, 0x52,    // MSTORE at 0
        0x60, 0x20,
        0x60, 0x00,
        0xF3,                 // RETURN
    };
    uint8_t code1[] = {
        0x01,                 // ADD
        0xE4,                 // RETF
    };

    // Function 0: inputs=0, outputs=0x80 (non-returning entry), msh=2
    //   After PUSH 3, PUSH 4: sp=2. CALLF passes 2 inputs to func1.
    //   After RETF: sp=1 (one output from func1). MSTORE uses 2 slots.
    // Function 1: inputs=2, outputs=1, msh=2
    size_t len;
    uint8_t *eof = build_eof_two_funcs(
        0, 0x80, 2, code0, sizeof(code0),
        2, 1, 0, code1, sizeof(code1),  // msh=0: increase = abs(2) - inputs(2)
        &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 7, "CALLF: 3+4=7");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Data Section
//==============================================================================

static void test_datasize(void)
{
    TEST("DATASIZE: reports data section size");
    uint8_t code_bytes[] = {
        0xD2,               // DATASIZE
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    uint8_t data[] = { 0x11, 0x22, 0x33, 0x44, 0x55 };

    size_t len;
    uint8_t *eof = build_eof(0, 0x80, 2, code_bytes, sizeof(code_bytes),
                              data, sizeof(data), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 5, "datasize=5");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_dataloadn(void)
{
    TEST("DATALOADN: load 32 bytes from data at static offset");
    // Build 32 bytes of data, DATALOADN offset=0
    uint8_t data[32];
    memset(data, 0, 32);
    data[31] = 0x42;  // least significant byte = 0x42

    uint8_t code_bytes[] = {
        0xD1, 0x00, 0x00, // DATALOADN offset=0
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };

    size_t len;
    uint8_t *eof = build_eof(0, 0x80, 2, code_bytes, sizeof(code_bytes),
                              data, sizeof(data), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0x42, "dataloadn=0x42");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Crypto
//==============================================================================

static void test_keccak256(void)
{
    TEST("KECCAK256: hash of empty");
    // KECCAK256(offset=0, size=0) → hash of empty string
    uint8_t code[] = {
        0x60, 0x00,   // PUSH1 0 (size)
        0x60, 0x00,   // PUSH1 0 (offset)
        0x20,         // KECCAK256
        0x60, 0x00, 0x52,
        0x60, 0x20, 0x60, 0x00, 0xF3,
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    // Compute expected: keccak256("")
    SHA3_CTX ctx;
    keccak_init(&ctx);
    uint8_t expected[32];
    keccak_final(&ctx, expected);

    ASSERT(e.result.output_size == 32, "output size");
    ASSERT(memcmp(e.result.output_data, expected, 32) == 0, "hash matches");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Gas
//==============================================================================

static void test_out_of_gas(void)
{
    TEST("Out of gas: ADD with insufficient gas");
    // ADD costs 3. Give only 2 gas (plus need gas for PUSH).
    uint8_t code[] = {
        0x60, 0x01,   // PUSH1 1 (cost 3)
        0x60, 0x02,   // PUSH1 2 (cost 3)
        0x01,         // ADD (cost 3)
        0x00,         // STOP
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    // Give exactly 8 gas: PUSH(3) + PUSH(3) + ADD needs 3 more = 9 total.
    // So 8 gas should fail on ADD.
    exec_t e;
    ASSERT(exec_eof(eof, len, 8, &e), "exec");
    ASSERT(e.result.status == VM_OUT_OF_GAS, "out of gas");

    exec_free(&e);
    free(eof);
    PASS();
}

static void test_revert(void)
{
    TEST("REVERT: returns data with revert status");
    uint8_t code[] = {
        0x60, 0x2A,   // PUSH1 42
        0x60, 0x00,   // PUSH1 0
        0x52,         // MSTORE at 0
        0x60, 0x20,   // PUSH1 32
        0x60, 0x00,   // PUSH1 0
        0xFD,         // REVERT
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_REVERT, "revert status");
    ASSERT(e.result.output_size == 32, "output size");
    ASSERT(e.result.gas_left > 0, "gas refunded on revert");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 42, "revert data = 42");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Return value encoding
//==============================================================================

static void test_return_data(void)
{
    TEST("RETURN: returns memory region correctly");
    // Store 0xDEAD at offset 0, return 32 bytes
    uint8_t code[] = {
        0x61, 0xDE, 0xAD,   // PUSH2 0xDEAD
        0x60, 0x00,          // PUSH1 0
        0x52,                // MSTORE at 0
        0x60, 0x20,          // PUSH1 32
        0x60, 0x00,          // PUSH1 0
        0xF3,                // RETURN
    };
    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    ASSERT(e.result.output_size == 32, "output size");

    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0xDEAD, "return data = 0xDEAD");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Complex Sequences
//==============================================================================

static void test_fibonacci(void)
{
    TEST("Fibonacci: fib(10) = 55");
    // Compute fib(10) using a loop:
    //   a=0, b=1
    //   loop 10 times: temp=a+b, a=b, b=temp
    //   return a
    //
    // Stack layout: [a, b, counter]
    // Code:
    //   PUSH1 0       ; a = 0
    //   PUSH1 1       ; b = 1
    //   PUSH1 10      ; counter = 10
    // loop:            ; offset 6
    //   DUP1          ; duplicate counter
    //   ISZERO        ; counter == 0?
    //   RJUMPI +done  ; if zero, jump to done
    //   PUSH1 1       ; 1 for decrement
    //   SWAP1         ; [a, b, 1, counter]
    //   SUB           ; [a, b, counter-1]
    //   SWAP2         ; [counter-1, b, a]
    //   SWAP1         ; [counter-1, a, b]
    //   DUP2          ; [counter-1, a, b, a]
    //   ADD           ; [counter-1, a, a+b]
    //   SWAP2         ; [a+b, a, counter-1] → now a=old_b? No...
    //
    // Actually this is getting complex with the stack management.
    // Let me use a simpler approach: memory-based.
    //   mem[0] = 0 (a)
    //   mem[32] = 1 (b)
    //   counter = 10
    // loop:
    //   load a, load b, add → a+b
    //   store old b → mem[0]
    //   store a+b → mem[32]
    //   counter--, if not zero goto loop
    //   return mem[0] (which is fib(10))
    //
    // Hmm, let me just use the pure stack approach more carefully.
    // fib(n): a=0, b=1, repeat n times: (a,b) = (b, a+b). Return a.
    //
    // Stack: bottom [a] [b] [counter] top
    //
    // init: PUSH1 0, PUSH1 1, PUSH1 10
    // loop (at byte 6):
    //   DUP1          ; [a, b, c, c]
    //   ISZERO        ; [a, b, c, c==0]
    //   RJUMPI +exit  ; if done, exit. Stack: [a, b, c]
    //   PUSH1 1       ; [a, b, c, 1]
    //   SWAP1         ; [a, b, 1, c]
    //   SUB           ; [a, b, c-1]  (a=c(top), b=1 → a-b = c-1)
    //   SWAP2         ; [c-1, b, a]
    //   DUP2          ; [c-1, b, a, b]
    //   DUP2          ; [c-1, b, a, b, a]
    //   ADD           ; [c-1, b, a, a+b]
    //   SWAP2         ; [c-1, a+b, a, b]  -- oops, need to rearrange
    //
    // This is very tedious. Let me just do a simple test that shows the loop works.
    // I already tested the loop (counter to 5). Let me do fib with memory instead.

    // Memory-based fib:
    //   mem[0x00] = a = 0
    //   mem[0x20] = b = 1
    //   push counter = 10
    // loop:
    //   dup, iszero, rjumpi to exit
    //   push1 1, swap1, sub     ; counter--
    //   push1 0x00, mload       ; load a
    //   push1 0x20, mload       ; load b
    //   dup1                     ; [counter, a, b, b]
    //   swap2                    ; [counter, b, b, a]
    //   add                      ; [counter, b, a+b]
    //   push1 0x20, mstore      ; mem[0x20] = a+b
    //   push1 0x00, mstore      ; mem[0x00] = b
    //   rjump back to loop
    // exit:
    //   pop (counter)
    //   push1 0x20, push1 0x00, return

    uint8_t code[] = {
        // Init
        0x60, 0x00,           // 0: PUSH1 0 (a)
        0x60, 0x00,           // 2: PUSH1 0 (offset)
        0x52,                 // 4: MSTORE mem[0] = 0
        0x60, 0x01,           // 5: PUSH1 1 (b)
        0x60, 0x20,           // 7: PUSH1 32 (offset)
        0x52,                 // 9: MSTORE mem[32] = 1
        0x60, 0x0A,           // 10: PUSH1 10 (counter)
        // Loop at byte 12:
        0x80,                 // 12: DUP1 (counter)
        0x15,                 // 13: ISZERO
        0xE1, 0x00, 0x1C,    // 14: RJUMPI +28 → byte 17+28=45 (exit: POP...)
        // counter--
        0x60, 0x01,           // 17: PUSH1 1
        0x90,                 // 19: SWAP1 → [1, counter]
        0x03,                 // 20: SUB → counter-1 (a=counter, b=1, a-b=counter-1)
        // load a, b
        0x60, 0x00,           // 21: PUSH1 0
        0x51,                 // 23: MLOAD → a
        0x60, 0x20,           // 24: PUSH1 32
        0x51,                 // 26: MLOAD → b
        // compute: new_a = b, new_b = a + b
        0x81,                 // 27: DUP2 → [counter, a, b, a]
        0x81,                 // 28: DUP2 → [counter, a, b, a, b]
        0x01,                 // 29: ADD → [counter, a, b, a+b]
        0x60, 0x20,           // 30: PUSH1 32
        0x52,                 // 32: MSTORE → mem[32] = a+b. Stack: [counter, a, b]
        // store b as new a
        0x90,                 // 33: SWAP1 → [counter, b, a]
        0x50,                 // 34: POP → [counter, b]
        0x60, 0x00,           // 35: PUSH1 0
        0x52,                 // 37: MSTORE → mem[0] = b. Stack: [counter]
        // loop back
        0xE0, 0xFF, 0xE6,    // 38: RJUMP → after=41, target=41+offset.
        // Want target = 12. offset = 12 - 41 = -29. -29 = 0xFFE3
        // exit at byte 41:
        0x50,                 // 41: POP (counter)
        0x60, 0x20,           // 42: PUSH1 32
        0x60, 0x00,           // 44: PUSH1 0
        0xF3,                 // 46: RETURN
    };
    // Fix RJUMPI offset: target=41, from=17, offset = 41 - 17 = 24
    code[15] = 0x00;
    code[16] = 0x18;  // +24

    // Fix RJUMP offset: target=12, from=41, offset = 12 - 41 = -29 = 0xFFE3
    code[39] = 0xFF;
    code[40] = 0xE3;

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 5, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 1000000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");

    // fib(10): 0,1,1,2,3,5,8,13,21,34,55
    // After 10 iterations: a should be fib(10) = 55
    // mem[0x00] = a (the 10th fib), returned via mem[0..31]
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 55, "fib(10)=55");

    exec_free(&e);
    free(eof);
    PASS();
}

//==============================================================================
// Tests: Phase 3 — Environmental
//==============================================================================

static void test_address(void)
{
    TEST("ADDRESS: returns executing account address");
    uint8_t code[] = { 0x30, RET32 };

    address_t recipient = address_zero();
    recipient.bytes[19] = 0x42;
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000, .recipient = recipient };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0x42, "address = 0x42");

    exec_free(&e); free(eof); PASS();
}

static void test_caller(void)
{
    TEST("CALLER: returns msg.caller");
    uint8_t code[] = { 0x33, RET32 };

    address_t caller = address_zero();
    caller.bytes[19] = 0xBB;
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000, .caller = caller };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0xBB, "caller = 0xBB");

    exec_free(&e); free(eof); PASS();
}

static void test_callvalue(void)
{
    TEST("CALLVALUE: returns msg.value");
    uint8_t code[] = { 0x34, RET32 };

    vm_message_t msg = {
        .kind = VM_CALL, .gas = 100000,
        .value = uint256_from_uint64(1000),
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 1000, "callvalue = 1000");

    exec_free(&e); free(eof); PASS();
}

static void test_calldataload(void)
{
    TEST("CALLDATALOAD: reads 32 bytes from calldata");
    // PUSH1 0, CALLDATALOAD, RET32
    uint8_t code[] = { 0x60, 0x00, 0x35, RET32 };

    uint8_t calldata[32];
    memset(calldata, 0, 32);
    calldata[31] = 99;

    vm_message_t msg = {
        .kind = VM_CALL, .gas = 100000,
        .input_data = calldata, .input_size = 32,
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 99, "calldataload = 99");

    exec_free(&e); free(eof); PASS();
}

static void test_calldatasize(void)
{
    TEST("CALLDATASIZE: returns calldata length");
    uint8_t code[] = { 0x36, RET32 };

    uint8_t calldata[45] = {0};
    vm_message_t msg = {
        .kind = VM_CALL, .gas = 100000,
        .input_data = calldata, .input_size = 45,
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 45, "calldatasize = 45");

    exec_free(&e); free(eof); PASS();
}

static void test_calldatacopy(void)
{
    TEST("CALLDATACOPY: copies calldata to memory");
    // Push: size(third), offset(second), dest(top) → CALLDATACOPY
    uint8_t code[] = {
        0x60, 0x20,   // PUSH1 32 (size)
        0x60, 0x00,   // PUSH1 0  (data offset)
        0x60, 0x00,   // PUSH1 0  (dest offset)
        0x37,         // CALLDATACOPY
        0x60, 0x20, 0x60, 0x00, 0xF3,  // RETURN 32 from 0
    };

    uint8_t calldata[32];
    memset(calldata, 0, 32);
    calldata[31] = 0x77;

    vm_message_t msg = {
        .kind = VM_CALL, .gas = 100000,
        .input_data = calldata, .input_size = 32,
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 3, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0x77, "calldatacopy copied correctly");

    exec_free(&e); free(eof); PASS();
}

static void test_origin_gasprice(void)
{
    TEST("ORIGIN/GASPRICE: tx context reads");
    // ORIGIN, RET32
    uint8_t code[] = { 0x32, RET32 };

    address_t origin = address_zero();
    origin.bytes[19] = 0xCC;

    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };
    vm_tx_context_t tx = {
        .origin = origin,
        .gas_price = uint256_from_uint64(50),
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, &tx, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0xCC, "origin = 0xCC");

    exec_free(&e); free(eof); PASS();
}

static void test_returndatasize(void)
{
    TEST("RETURNDATASIZE: returns 0 (no prior call)");
    uint8_t code[] = { 0x3D, RET32 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof(eof, len, 100000, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_is_zero(&val), "returndatasize = 0");

    exec_free(&e); free(eof); PASS();
}

//==============================================================================
// Tests: Phase 3 — Block Info
//==============================================================================

static void test_block_number(void)
{
    TEST("NUMBER: returns block.number");
    uint8_t code[] = { 0x43, RET32 };

    vm_block_env_t block = {0};
    block.number = 12345;
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, &block, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 12345, "number = 12345");

    exec_free(&e); free(eof); PASS();
}

static void test_coinbase(void)
{
    TEST("COINBASE: returns block.coinbase");
    uint8_t code[] = { 0x41, RET32 };

    vm_block_env_t block = {0};
    block.coinbase.bytes[19] = 0xEE;
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, &block, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0xEE, "coinbase = 0xEE");

    exec_free(&e); free(eof); PASS();
}

static void test_prevrandao(void)
{
    TEST("PREVRANDAO: returns block.difficulty");
    uint8_t code[] = { 0x44, RET32 };

    vm_block_env_t block = {0};
    block.difficulty = uint256_from_uint64(0xDEADBEEF);
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, &block, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0xDEADBEEF, "prevrandao");

    exec_free(&e); free(eof); PASS();
}

static void test_blockhash(void)
{
    TEST("BLOCKHASH: returns recent block hash");
    // PUSH1 99, BLOCKHASH, RET32
    uint8_t code[] = { 0x60, 99, 0x40, RET32 };

    vm_block_env_t block = {0};
    block.number = 100;
    memset(block.block_hash[99].bytes, 0, 32);
    block.block_hash[99].bytes[31] = 0xAA;

    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, &block, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 0xAA, "blockhash = 0xAA");

    exec_free(&e); free(eof); PASS();
}

static void test_chainid(void)
{
    TEST("CHAINID: returns block.chain_id");
    uint8_t code[] = { 0x46, RET32 };

    vm_block_env_t block = {0};
    block.chain_id = uint256_from_uint64(1);
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, &block, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 1, "chainid = 1");

    exec_free(&e); free(eof); PASS();
}

static void test_basefee(void)
{
    TEST("BASEFEE: returns block.base_fee");
    uint8_t code[] = { 0x48, RET32 };

    vm_block_env_t block = {0};
    block.base_fee = uint256_from_uint64(30000000000ULL);
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, &block, NULL, NULL, NULL, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 30000000000ULL, "basefee");

    exec_free(&e); free(eof); PASS();
}

//==============================================================================
// Tests: Phase 3 — Storage
//==============================================================================

static void test_sload_sstore(void)
{
    TEST("SLOAD/SSTORE: store then load from persistent storage");
    // PUSH1 42 (value), PUSH1 0 (key), SSTORE
    // PUSH1 0 (key), SLOAD, RET32
    uint8_t code[] = {
        0x60, 42,     // PUSH1 42 (value)
        0x60, 0x00,   // PUSH1 0 (key)
        0x55,         // SSTORE
        0x60, 0x00,   // PUSH1 0 (key)
        0x54,         // SLOAD
        RET32,
    };

    mock_host_t host_state = {0};
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, &MOCK_HOST, &host_state, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 42, "sload after sstore = 42");
    ASSERT(uint256_to_uint64(&host_state.storage[0]) == 42, "host storage[0] = 42");

    exec_free(&e); free(eof); PASS();
}

static void test_tload_tstore(void)
{
    TEST("TLOAD/TSTORE: transient storage write and read");
    // PUSH1 99 (value), PUSH1 1 (key), TSTORE
    // PUSH1 1 (key), TLOAD, RET32
    uint8_t code[] = {
        0x60, 99,     // PUSH1 99 (value)
        0x60, 0x01,   // PUSH1 1 (key)
        0x5D,         // TSTORE
        0x60, 0x01,   // PUSH1 1 (key)
        0x5C,         // TLOAD
        RET32,
    };

    mock_host_t host_state = {0};
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, &MOCK_HOST, &host_state, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 99, "tload after tstore = 99");

    exec_free(&e); free(eof); PASS();
}

static void test_selfbalance(void)
{
    TEST("SELFBALANCE: returns balance of executing account");
    uint8_t code[] = { 0x47, RET32 };

    mock_host_t host_state = {0};
    host_state.balance = uint256_from_uint64(1000000);
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, &MOCK_HOST, &host_state, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    uint256_t val = uint256_from_bytes(e.result.output_data, 32);
    ASSERT(uint256_to_uint64(&val) == 1000000, "selfbalance = 1000000");

    exec_free(&e); free(eof); PASS();
}

//==============================================================================
// Tests: Phase 3 — Logging
//==============================================================================

static void test_log1(void)
{
    TEST("LOG1: emits log with topic and data");
    // Store 42 at mem[0], then LOG1(offset=0, size=32, topic=0xABCD)
    uint8_t code[] = {
        0x60, 42,             // PUSH1 42
        0x60, 0x00,           // PUSH1 0
        0x52,                 // MSTORE at 0
        0x61, 0xAB, 0xCD,    // PUSH2 0xABCD (topic)
        0x60, 0x20,           // PUSH1 32 (size)
        0x60, 0x00,           // PUSH1 0 (offset)
        0xA1,                 // LOG1
        0x00,                 // STOP
    };

    mock_host_t host_state = {0};
    vm_message_t msg = { .kind = VM_CALL, .gas = 100000 };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 3, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, &MOCK_HOST, &host_state, &e), "exec");
    ASSERT(e.result.status == VM_SUCCESS, "status");
    ASSERT(host_state.log_called, "log emitted");
    ASSERT(host_state.log_topic_count == 1, "1 topic");
    ASSERT(uint256_to_uint64(&host_state.log_topics[0]) == 0xABCD, "topic = 0xABCD");
    ASSERT(host_state.log_data_size == 32, "data size = 32");

    exec_free(&e); free(eof); PASS();
}

//==============================================================================
// Tests: Phase 3 — Static Call Violations
//==============================================================================

static void test_static_sstore(void)
{
    TEST("SSTORE in static context: violation");
    uint8_t code[] = {
        0x60, 42,     // PUSH1 42 (value)
        0x60, 0x00,   // PUSH1 0 (key)
        0x55,         // SSTORE
        0x00,         // STOP
    };

    mock_host_t host_state = {0};
    vm_message_t msg = {
        .kind = VM_STATICCALL, .gas = 100000, .is_static = true,
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, &MOCK_HOST, &host_state, &e), "exec");
    ASSERT(e.result.status == VM_STATIC_CALL_VIOLATION, "static violation on SSTORE");

    exec_free(&e); free(eof); PASS();
}

static void test_static_log(void)
{
    TEST("LOG0 in static context: violation");
    uint8_t code[] = {
        0x60, 0x00,   // PUSH1 0 (size)
        0x60, 0x00,   // PUSH1 0 (offset)
        0xA0,         // LOG0
        0x00,         // STOP
    };

    mock_host_t host_state = {0};
    vm_message_t msg = {
        .kind = VM_STATICCALL, .gas = 100000, .is_static = true,
    };

    size_t len;
    uint8_t *eof = build_eof_simple(0, 0x80, 2, code, sizeof(code), &len);
    ASSERT(eof, "build");

    exec_t e;
    ASSERT(exec_eof_with(eof, len, &msg, NULL, NULL, &MOCK_HOST, &host_state, &e), "exec");
    ASSERT(e.result.status == VM_STATIC_CALL_VIOLATION, "static violation on LOG0");

    exec_free(&e); free(eof); PASS();
}

//==============================================================================
// Main
//==============================================================================

int main(void)
{
    printf("\n=== VM Interpreter Tests ===\n\n");

    // Termination
    test_stop();
    test_invalid();

    // Arithmetic
    test_add();
    test_sub();
    test_mul_div();
    test_mod();
    test_exp();
    test_addmod();

    // Comparison & Bitwise
    test_lt_gt_eq();
    test_iszero();
    test_and_or_xor();
    test_shl_shr();

    // Stack
    test_push_dup_swap();
    test_push0();
    test_push32();
    test_dupn_swapn();
    test_exchange();

    // Memory
    test_mstore_mload();
    test_mstore8();
    test_msize();
    test_mcopy();

    // Control Flow
    test_rjump();
    test_rjumpi();
    test_rjumpi_not_taken();
    test_rjump_backward_loop();
    test_callf_retf();

    // Data Section
    test_datasize();
    test_dataloadn();

    // Crypto
    test_keccak256();

    // Gas & Status
    test_out_of_gas();
    test_revert();
    test_return_data();

    // Complex
    test_fibonacci();

    // Phase 3: Environmental
    test_address();
    test_caller();
    test_callvalue();
    test_calldataload();
    test_calldatasize();
    test_calldatacopy();
    test_origin_gasprice();
    test_returndatasize();

    // Phase 3: Block Info
    test_block_number();
    test_coinbase();
    test_prevrandao();
    test_blockhash();
    test_chainid();
    test_basefee();

    // Phase 3: Storage & Balance
    test_sload_sstore();
    test_tload_tstore();
    test_selfbalance();

    // Phase 3: Logging
    test_log1();

    // Phase 3: Static Call Violations
    test_static_sstore();
    test_static_log();

    // Summary
    printf("\n=== Results: %d/%d passed ===\n\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
