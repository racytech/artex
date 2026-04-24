/**
 * EVM Tracer — EIP-3155 compliant execution trace output
 *
 * Outputs one JSON object per line to g_evm_tracer.out.
 * Uses a "delayed emit" pattern: we capture pre-state at each DISPATCH,
 * then emit the previous opcode's trace line at the START of the next
 * DISPATCH (with gasCost = prev_gas - current_gas).
 */

#ifdef ENABLE_EVM_TRACE

#include "evm_tracer.h"
#include "evm.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include <string.h>
#include <inttypes.h>

/* ---- Opcode names table ---- */
static const char *OPCODE_NAMES[256] = {
    [0x00] = "STOP",       [0x01] = "ADD",        [0x02] = "MUL",
    [0x03] = "SUB",        [0x04] = "DIV",        [0x05] = "SDIV",
    [0x06] = "MOD",        [0x07] = "SMOD",       [0x08] = "ADDMOD",
    [0x09] = "MULMOD",     [0x0A] = "EXP",        [0x0B] = "SIGNEXTEND",
    [0x10] = "LT",         [0x11] = "GT",         [0x12] = "SLT",
    [0x13] = "SGT",        [0x14] = "EQ",         [0x15] = "ISZERO",
    [0x16] = "AND",        [0x17] = "OR",         [0x18] = "XOR",
    [0x19] = "NOT",        [0x1A] = "BYTE",       [0x1B] = "SHL",
    [0x1C] = "SHR",        [0x1D] = "SAR",
    [0x20] = "KECCAK256",
    [0x30] = "ADDRESS",    [0x31] = "BALANCE",    [0x32] = "ORIGIN",
    [0x33] = "CALLER",     [0x34] = "CALLVALUE",  [0x35] = "CALLDATALOAD",
    [0x36] = "CALLDATASIZE",[0x37] = "CALLDATACOPY",
    [0x38] = "CODESIZE",   [0x39] = "CODECOPY",   [0x3A] = "GASPRICE",
    [0x3B] = "EXTCODESIZE",[0x3C] = "EXTCODECOPY",
    [0x3D] = "RETURNDATASIZE", [0x3E] = "RETURNDATACOPY",
    [0x3F] = "EXTCODEHASH",
    [0x40] = "BLOCKHASH",  [0x41] = "COINBASE",   [0x42] = "TIMESTAMP",
    [0x43] = "NUMBER",     [0x44] = "DIFFICULTY",  [0x45] = "GASLIMIT",
    [0x46] = "CHAINID",    [0x47] = "SELFBALANCE", [0x48] = "BASEFEE",
    [0x49] = "BLOBHASH",   [0x4A] = "BLOBBASEFEE",
    [0x50] = "POP",        [0x51] = "MLOAD",      [0x52] = "MSTORE",
    [0x53] = "MSTORE8",    [0x54] = "SLOAD",      [0x55] = "SSTORE",
    [0x56] = "JUMP",       [0x57] = "JUMPI",      [0x58] = "PC",
    [0x59] = "MSIZE",      [0x5A] = "GAS",        [0x5B] = "JUMPDEST",
    [0x5C] = "TLOAD",      [0x5D] = "TSTORE",     [0x5E] = "MCOPY",
    [0x5F] = "PUSH0",
    /* PUSH1-PUSH32 */
    [0x60] = "PUSH1",  [0x61] = "PUSH2",  [0x62] = "PUSH3",  [0x63] = "PUSH4",
    [0x64] = "PUSH5",  [0x65] = "PUSH6",  [0x66] = "PUSH7",  [0x67] = "PUSH8",
    [0x68] = "PUSH9",  [0x69] = "PUSH10", [0x6A] = "PUSH11", [0x6B] = "PUSH12",
    [0x6C] = "PUSH13", [0x6D] = "PUSH14", [0x6E] = "PUSH15", [0x6F] = "PUSH16",
    [0x70] = "PUSH17", [0x71] = "PUSH18", [0x72] = "PUSH19", [0x73] = "PUSH20",
    [0x74] = "PUSH21", [0x75] = "PUSH22", [0x76] = "PUSH23", [0x77] = "PUSH24",
    [0x78] = "PUSH25", [0x79] = "PUSH26", [0x7A] = "PUSH27", [0x7B] = "PUSH28",
    [0x7C] = "PUSH29", [0x7D] = "PUSH30", [0x7E] = "PUSH31", [0x7F] = "PUSH32",
    /* DUP1-DUP16 */
    [0x80] = "DUP1",   [0x81] = "DUP2",   [0x82] = "DUP3",   [0x83] = "DUP4",
    [0x84] = "DUP5",   [0x85] = "DUP6",   [0x86] = "DUP7",   [0x87] = "DUP8",
    [0x88] = "DUP9",   [0x89] = "DUP10",  [0x8A] = "DUP11",  [0x8B] = "DUP12",
    [0x8C] = "DUP13",  [0x8D] = "DUP14",  [0x8E] = "DUP15",  [0x8F] = "DUP16",
    /* SWAP1-SWAP16 */
    [0x90] = "SWAP1",  [0x91] = "SWAP2",  [0x92] = "SWAP3",  [0x93] = "SWAP4",
    [0x94] = "SWAP5",  [0x95] = "SWAP6",  [0x96] = "SWAP7",  [0x97] = "SWAP8",
    [0x98] = "SWAP9",  [0x99] = "SWAP10", [0x9A] = "SWAP11", [0x9B] = "SWAP12",
    [0x9C] = "SWAP13", [0x9D] = "SWAP14", [0x9E] = "SWAP15", [0x9F] = "SWAP16",
    /* LOG0-LOG4 */
    [0xA0] = "LOG0",   [0xA1] = "LOG1",   [0xA2] = "LOG2",
    [0xA3] = "LOG3",   [0xA4] = "LOG4",
    /* System */
    [0xF0] = "CREATE",     [0xF1] = "CALL",       [0xF2] = "CALLCODE",
    [0xF3] = "RETURN",     [0xF4] = "DELEGATECALL",
    [0xF5] = "CREATE2",
    [0xFA] = "STATICCALL",
    [0xFD] = "REVERT",
    [0xFE] = "INVALID",
    [0xFF] = "SELFDESTRUCT",
};

static const char *opcode_name(uint8_t op) {
    const char *n = OPCODE_NAMES[op];
    return n ? n : "UNKNOWN";
}

/* ---- Global state ---- */

evm_tracer_t g_evm_tracer = {
    .out = NULL,
    .enabled = false,
    .dump_memory = false,
    .dump_storage = false,
};

/* Per-thread storage map — flat array of (addr, key, value) entries.
 * Grown on demand, reused across transactions via on_tx_start which
 * just resets the count. Linear scan on update — for typical tx
 * working sets (<200 distinct slots) this is faster than a hash table
 * due to cache behavior. Cleared when tracer is disabled is not
 * necessary (a new tx_start resets state anyway). */
typedef struct {
    uint8_t addr[20];
    uint8_t key[32];
    uint8_t value[32];
} storage_entry_t;

static __thread storage_entry_t *g_storage_map = NULL;
static __thread size_t           g_storage_map_count = 0;
static __thread size_t           g_storage_map_cap = 0;

/* Per-thread pending trace (single-threaded EVM, so global is fine). */
/* One per call depth, max 1024+1. We only allocate one and reuse since
   each depth has its own interpreter loop. */
static __thread evm_trace_pending_t pending = { .active = false };

/* ---- Helpers ---- */

/* Write a uint256 as minimal hex "0x..." into buf. Returns chars written. */
static int u256_to_hex(const uint8_t be[32], char *buf, size_t bufsz) {
    /* Skip leading zeros */
    int start = 0;
    while (start < 31 && be[start] == 0) start++;
    /* Write */
    int pos = 0;
    pos += snprintf(buf + pos, bufsz - pos, "0x");
    /* First byte without leading zero (unless it's the only byte) */
    if (start == 31 && be[31] == 0) {
        pos += snprintf(buf + pos, bufsz - pos, "0");
    } else {
        pos += snprintf(buf + pos, bufsz - pos, "%x", be[start]);
        for (int i = start + 1; i < 32; i++)
            pos += snprintf(buf + pos, bufsz - pos, "%02x", be[i]);
    }
    return pos;
}

static void emit_pending(uint64_t gas_now, const char *error) {
    if (!pending.active) return;

    FILE *f = g_evm_tracer.out;
    uint64_t gas_cost = (pending.gas >= gas_now) ? (pending.gas - gas_now) : 0;

    /* EIP-3155 field order: pc, op, gas, gasCost, memSize, stack, depth,
     * returnData, refund, opName, error, memory, storage. */

    fprintf(f, "{\"pc\":%" PRIu64 ",\"op\":%u,\"gas\":\"0x%" PRIx64 "\","
               "\"gasCost\":\"0x%" PRIx64 "\",\"memSize\":%zu",
            pending.pc, pending.op, pending.gas, gas_cost, pending.mem_size);

    /* stack (bottom to top) */
    fprintf(f, ",\"stack\":[");
    for (int i = 0; i < pending.stack_count; i++) {
        if (i > 0) fputc(',', f);
        char hex[68];
        u256_to_hex(pending.stack[i], hex, sizeof(hex));
        fprintf(f, "\"%s\"", hex);
    }
    fprintf(f, "]");

    fprintf(f, ",\"depth\":%d", pending.depth);

    /* returnData — required. Empty "0x" when no sub-call has completed. */
    fprintf(f, ",\"returnData\":\"0x");
    for (size_t i = 0; i < pending.return_data_len; i++)
        fprintf(f, "%02x", pending.return_data_buf[i]);
    fprintf(f, "\"");

    fprintf(f, ",\"refund\":\"0x%" PRIx64 "\"", (uint64_t)pending.refund);

    fprintf(f, ",\"opName\":\"%s\"", opcode_name(pending.op));

    if (error)
        fprintf(f, ",\"error\":\"%s\"", error);

    /* Optional "memory" — only emitted when dump_memory is on. Array of
     * Hex-Strings, one entry per 32-byte word, leading zeros stripped
     * (same minimal-hex encoding as stack items). */
    if (g_evm_tracer.dump_memory) {
        fprintf(f, ",\"memory\":[");
        size_t n_words = pending.memory_len / 32;
        char hex[68];
        for (size_t w = 0; w < n_words; w++) {
            if (w > 0) fputc(',', f);
            u256_to_hex(&pending.memory_buf[w * 32], hex, sizeof(hex));
            fprintf(f, "\"%s\"", hex);
        }
        /* Safety: if memory_len isn't a multiple of 32 for some
         * unexpected reason, zero-pad into a 32-byte buffer and encode. */
        size_t tail = pending.memory_len % 32;
        if (tail != 0) {
            if (n_words > 0) fputc(',', f);
            uint8_t padded[32] = {0};
            memcpy(padded, &pending.memory_buf[n_words * 32], tail);
            u256_to_hex(padded, hex, sizeof(hex));
            fprintf(f, "\"%s\"", hex);
        }
        fprintf(f, "]");
    }

    /* Optional "storage" — touched slots in current tx filtered to the
     * currently-executing contract. Key/value stripped of leading zeros. */
    if (g_evm_tracer.dump_storage) {
        fprintf(f, ",\"storage\":{");
        bool first = true;
        for (size_t i = 0; i < g_storage_map_count; i++) {
            if (memcmp(g_storage_map[i].addr,
                       pending.current_addr, 20) != 0) continue;
            if (!first) fputc(',', f);
            first = false;
            char khex[68], vhex[68];
            u256_to_hex(g_storage_map[i].key, khex, sizeof(khex));
            u256_to_hex(g_storage_map[i].value, vhex, sizeof(vhex));
            fprintf(f, "\"%s\":\"%s\"", khex, vhex);
        }
        fprintf(f, "}");
    }

    fprintf(f, "}\n");

    pending.active = false;
}

static void capture_state(evm_t *evm) {
    pending.active = true;
    pending.pc = evm->pc;
    pending.op = evm->code[evm->pc];
    pending.gas = evm->gas_left;
    pending.depth = evm->msg.depth + 1;  /* 1-based like geth */
    pending.refund = evm->gas_refund;
    pending.mem_size = evm_memory_size(evm->memory);

    /* Memory snapshot — only when dump_memory is enabled. Grow the
     * per-thread buffer as needed; it's reused across ops and lives
     * for the process lifetime. */
    if (g_evm_tracer.dump_memory && pending.mem_size > 0) {
        if (pending.mem_size > pending.memory_buf_cap) {
            uint8_t *nb = realloc(pending.memory_buf, pending.mem_size);
            if (nb) {
                pending.memory_buf = nb;
                pending.memory_buf_cap = pending.mem_size;
            } else {
                pending.memory_len = 0;
                goto stack_capture;
            }
        }
        const uint8_t *src = evm_memory_get_ptr(evm->memory, 0);
        if (src) {
            memcpy(pending.memory_buf, src, pending.mem_size);
            pending.memory_len = pending.mem_size;
        } else {
            pending.memory_len = 0;
        }
    } else {
        pending.memory_len = 0;
    }

    /* Capture current contract address (used by storage dump filter). */
    memcpy(pending.current_addr, evm->msg.recipient.bytes, 20);

    /* Return data snapshot — always captured because EIP-3155
     * requires the field. Lazy-grown reused buffer. */
    if (evm->return_data_size > 0) {
        if (evm->return_data_size > pending.return_data_buf_cap) {
            uint8_t *nb = realloc(pending.return_data_buf, evm->return_data_size);
            if (nb) {
                pending.return_data_buf = nb;
                pending.return_data_buf_cap = evm->return_data_size;
            } else {
                pending.return_data_len = 0;
                goto stack_capture;
            }
        }
        memcpy(pending.return_data_buf, evm->return_data, evm->return_data_size);
        pending.return_data_len = evm->return_data_size;
    } else {
        pending.return_data_len = 0;
    }
stack_capture:

    /* Snapshot stack (bottom to top) */
    size_t sz = evm_stack_size(evm->stack);
    if (sz > TRACE_MAX_STACK) sz = TRACE_MAX_STACK;
    pending.stack_count = (int)sz;

    /* evm_stack_get uses index 0 = top of stack.
       We want bottom-to-top for output (index 0 = bottom).
       So output[i] = stack_get(sz - 1 - i). */
    for (int i = 0; i < pending.stack_count; i++) {
        uint256_t val;
        evm_stack_get(evm->stack, sz - 1 - i, &val);
        /* Convert to big-endian 32 bytes */
        uint256_to_bytes(&val, pending.stack[i]);
    }
}

/* ---- Public API ---- */

void evm_tracer_init(FILE *out) {
    g_evm_tracer.out = out;
    g_evm_tracer.enabled = (out != NULL);
    pending.active = false;
}

void evm_tracer_set_dump_memory(bool enabled) {
    g_evm_tracer.dump_memory = enabled;
}

void evm_tracer_set_dump_storage(bool enabled) {
    g_evm_tracer.dump_storage = enabled;
}

void evm_tracer_on_tx_start(void) {
    g_storage_map_count = 0;
}

static void storage_upsert(const uint8_t addr[20],
                           const uint8_t key[32],
                           const uint8_t value[32]) {
    /* Find existing entry for this (addr, key) — linear scan. */
    for (size_t i = 0; i < g_storage_map_count; i++) {
        if (memcmp(g_storage_map[i].addr, addr, 20) == 0 &&
            memcmp(g_storage_map[i].key, key, 32) == 0) {
            memcpy(g_storage_map[i].value, value, 32);
            return;
        }
    }
    /* Append. Grow the buffer if needed. */
    if (g_storage_map_count >= g_storage_map_cap) {
        size_t new_cap = g_storage_map_cap ? g_storage_map_cap * 2 : 64;
        storage_entry_t *nb = realloc(g_storage_map,
                                      new_cap * sizeof(storage_entry_t));
        if (!nb) return;  /* best effort — drop the entry on OOM */
        g_storage_map = nb;
        g_storage_map_cap = new_cap;
    }
    storage_entry_t *e = &g_storage_map[g_storage_map_count++];
    memcpy(e->addr, addr, 20);
    memcpy(e->key,  key,  32);
    memcpy(e->value, value, 32);
}

void evm_tracer_on_sload(const uint8_t addr[20], const uint8_t key[32],
                          const uint8_t value[32]) {
    if (!g_evm_tracer.enabled || !g_evm_tracer.dump_storage) return;
    storage_upsert(addr, key, value);
}

void evm_tracer_on_sstore(const uint8_t addr[20], const uint8_t key[32],
                           const uint8_t value[32]) {
    if (!g_evm_tracer.enabled || !g_evm_tracer.dump_storage) return;
    storage_upsert(addr, key, value);
}

void evm_tracer_on_dispatch(evm_t *evm) {
    /* 1. Emit previous opcode's trace (if any at THIS depth) */
    emit_pending(evm->gas_left, NULL);

    /* 2. Capture pre-state for current opcode */
    capture_state(evm);
}

void evm_tracer_on_exit(evm_t *evm, const char *error) {
    /* Emit the last pending opcode (STOP/RETURN/REVERT/SELFDESTRUCT/OOG) */
    emit_pending(evm->gas_left, error);
}

void evm_tracer_on_implicit_stop(evm_t *evm) {
    /* Emit the previous pending opcode's trace, then emit STOP with gasCost=0.
       Called when pc >= code_size (implicit STOP at end of code). */
    emit_pending(evm->gas_left, NULL);

    /* Emit synthetic STOP trace */
    FILE *f = g_evm_tracer.out;
    int depth = evm->msg.depth + 1;
    size_t mem_size = evm_memory_size(evm->memory);

    /* EIP-3155 field order: pc, op, gas, gasCost, memSize, stack, depth,
     * returnData, refund, opName, memory. */

    fprintf(f, "{\"pc\":%" PRIu64 ",\"op\":0,\"gas\":\"0x%" PRIx64 "\","
               "\"gasCost\":\"0x0\",\"memSize\":%zu",
            evm->pc, evm->gas_left, mem_size);

    /* stack (bottom to top) */
    fprintf(f, ",\"stack\":[");
    size_t sz = evm_stack_size(evm->stack);
    for (size_t i = 0; i < sz; i++) {
        if (i > 0) fputc(',', f);
        uint256_t val;
        evm_stack_get(evm->stack, sz - 1 - i, &val);
        uint8_t be[32];
        uint256_to_bytes(&val, be);
        char hex[68];
        u256_to_hex(be, hex, sizeof(hex));
        fprintf(f, "\"%s\"", hex);
    }
    fprintf(f, "]");

    fprintf(f, ",\"depth\":%d", depth);

    fprintf(f, ",\"returnData\":\"0x");
    if (evm->return_data && evm->return_data_size > 0) {
        for (size_t i = 0; i < evm->return_data_size; i++)
            fprintf(f, "%02x", evm->return_data[i]);
    }
    fprintf(f, "\"");

    fprintf(f, ",\"refund\":\"0x%" PRIx64 "\"", (uint64_t)evm->gas_refund);

    fprintf(f, ",\"opName\":\"STOP\"");

    if (g_evm_tracer.dump_memory) {
        fprintf(f, ",\"memory\":[");
        const uint8_t *src = (mem_size > 0) ? evm_memory_get_ptr(evm->memory, 0) : NULL;
        size_t n_words = mem_size / 32;
        char hex[68];
        uint8_t zero[32] = {0};
        for (size_t w = 0; w < n_words; w++) {
            if (w > 0) fputc(',', f);
            u256_to_hex(src ? &src[w * 32] : zero, hex, sizeof(hex));
            fprintf(f, "\"%s\"", hex);
        }
        fprintf(f, "]");
    }

    fprintf(f, "}\n");
}

void evm_tracer_on_return(const uint8_t *output, size_t output_len,
                          uint64_t gas_used, const char *error) {
    FILE *f = g_evm_tracer.out;
    fprintf(f, "{\"output\":\"");
    if (output && output_len > 0) {
        for (size_t i = 0; i < output_len; i++)
            fprintf(f, "%02x", output[i]);
    }
    fprintf(f, "\",\"gasUsed\":\"0x%" PRIx64 "\"", gas_used);
    if (error) {
        fprintf(f, ",\"error\":\"%s\"", error);
    }
    fprintf(f, "}\n");
}

void evm_tracer_tx_summary(const uint8_t *state_root_32,
                           const uint8_t *output, size_t output_len,
                           uint64_t gas_used, bool pass) {
    FILE *f = g_evm_tracer.out;
    if (!f) return;

    fprintf(f, "{\"stateRoot\":\"0x");
    if (state_root_32) {
        for (int i = 0; i < 32; i++)
            fprintf(f, "%02x", state_root_32[i]);
    }
    fprintf(f, "\",\"output\":\"");
    if (output && output_len > 0) {
        for (size_t i = 0; i < output_len; i++)
            fprintf(f, "%02x", output[i]);
    }
    fprintf(f, "\",\"gasUsed\":\"0x%" PRIx64 "\"", gas_used);
    fprintf(f, ",\"pass\":%s}\n", pass ? "true" : "false");
}

#endif /* ENABLE_EVM_TRACE */
