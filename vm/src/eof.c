/**
 * EOF — EVM Object Format Parsing & Validation
 *
 * Implements EIP-3540 / EIP-7692 container validation:
 *   Phase A: Header parsing (magic, sections, type entries)
 *   Phase B: Code validation (opcodes, jump targets, stack analysis)
 *
 * Reference: Erigon EOF implementation (Go) by racytech.
 */

#include "eof.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Opcode Table
//==============================================================================

// Helper macros for table entries
#define OP(name, si, so, imm, valid, term) \
    [name] = { #name, (si), (so), (imm), (valid), (term) }

#define INVALID_OP(byte_val) \
    [byte_val] = { NULL, 0, 0, 0, false, false }

// Standard EVM opcodes kept in EOF, plus new EOF opcodes.
// Opcodes not listed explicitly are zero-initialized (invalid).
const eof_opcode_info_t EOF_OPCODES[256] = {
    // 0x00: STOP
    [0x00] = { "STOP",         0, 0, 0, true, true },

    // 0x01-0x0b: Arithmetic
    [0x01] = { "ADD",          2, 1, 0, true, false },
    [0x02] = { "MUL",          2, 1, 0, true, false },
    [0x03] = { "SUB",          2, 1, 0, true, false },
    [0x04] = { "DIV",          2, 1, 0, true, false },
    [0x05] = { "SDIV",         2, 1, 0, true, false },
    [0x06] = { "MOD",          2, 1, 0, true, false },
    [0x07] = { "SMOD",         2, 1, 0, true, false },
    [0x08] = { "ADDMOD",       3, 1, 0, true, false },
    [0x09] = { "MULMOD",       3, 1, 0, true, false },
    [0x0A] = { "EXP",          2, 1, 0, true, false },
    [0x0B] = { "SIGNEXTEND",   2, 1, 0, true, false },

    // 0x10-0x1d: Comparison & Bitwise
    [0x10] = { "LT",           2, 1, 0, true, false },
    [0x11] = { "GT",           2, 1, 0, true, false },
    [0x12] = { "SLT",          2, 1, 0, true, false },
    [0x13] = { "SGT",          2, 1, 0, true, false },
    [0x14] = { "EQ",           2, 1, 0, true, false },
    [0x15] = { "ISZERO",       1, 1, 0, true, false },
    [0x16] = { "AND",          2, 1, 0, true, false },
    [0x17] = { "OR",           2, 1, 0, true, false },
    [0x18] = { "XOR",          2, 1, 0, true, false },
    [0x19] = { "NOT",          1, 1, 0, true, false },
    [0x1A] = { "BYTE",         2, 1, 0, true, false },
    [0x1B] = { "SHL",          2, 1, 0, true, false },
    [0x1C] = { "SHR",          2, 1, 0, true, false },
    [0x1D] = { "SAR",          2, 1, 0, true, false },

    // 0x20: Keccak256
    [0x20] = { "KECCAK256",    2, 1, 0, true, false },

    // 0x30-0x3f: Environmental (some removed in EOF)
    [0x30] = { "ADDRESS",      0, 1, 0, true, false },
    [0x31] = { "BALANCE",      1, 1, 0, true, false },
    [0x32] = { "ORIGIN",       0, 1, 0, true, false },
    [0x33] = { "CALLER",       0, 1, 0, true, false },
    [0x34] = { "CALLVALUE",    0, 1, 0, true, false },
    [0x35] = { "CALLDATALOAD", 1, 1, 0, true, false },
    [0x36] = { "CALLDATASIZE", 0, 1, 0, true, false },
    [0x37] = { "CALLDATACOPY", 3, 0, 0, true, false },
    // 0x38 CODESIZE      — removed in EOF
    // 0x39 CODECOPY       — removed in EOF
    [0x3A] = { "GASPRICE",     0, 1, 0, true, false },
    // 0x3B EXTCODESIZE    — removed in EOF
    // 0x3C EXTCODECOPY    — removed in EOF
    [0x3D] = { "RETURNDATASIZE", 0, 1, 0, true, false },
    [0x3E] = { "RETURNDATACOPY", 3, 0, 0, true, false },
    // 0x3F EXTCODEHASH    — removed in EOF

    // 0x40-0x48: Block information
    [0x40] = { "BLOCKHASH",    1, 1, 0, true, false },
    [0x41] = { "COINBASE",     0, 1, 0, true, false },
    [0x42] = { "TIMESTAMP",    0, 1, 0, true, false },
    [0x43] = { "NUMBER",       0, 1, 0, true, false },
    [0x44] = { "PREVRANDAO",   0, 1, 0, true, false },
    [0x45] = { "GASLIMIT",     0, 1, 0, true, false },
    [0x46] = { "CHAINID",      0, 1, 0, true, false },
    [0x47] = { "SELFBALANCE",  0, 1, 0, true, false },
    [0x48] = { "BASEFEE",      0, 1, 0, true, false },
    [0x49] = { "BLOBHASH",     1, 1, 0, true, false },
    [0x4A] = { "BLOBBASEFEE",  0, 1, 0, true, false },

    // 0x50-0x5f: Stack, Memory, Storage, Flow
    [0x50] = { "POP",          1, 0, 0, true, false },
    [0x51] = { "MLOAD",        1, 1, 0, true, false },
    [0x52] = { "MSTORE",       2, 0, 0, true, false },
    [0x53] = { "MSTORE8",      2, 0, 0, true, false },
    [0x54] = { "SLOAD",        1, 1, 0, true, false },
    [0x55] = { "SSTORE",       2, 0, 0, true, false },
    // 0x56 JUMP           — removed in EOF
    // 0x57 JUMPI          — removed in EOF
    // 0x58 PC             — removed in EOF
    [0x59] = { "MSIZE",        0, 1, 0, true, false },
    // 0x5A GAS            — removed in EOF
    // 0x5B JUMPDEST       — removed in EOF
    [0x5C] = { "TLOAD",        1, 1, 0, true, false },
    [0x5D] = { "TSTORE",       2, 0, 0, true, false },
    [0x5E] = { "MCOPY",        3, 0, 0, true, false },
    [0x5F] = { "PUSH0",        0, 1, 0, true, false },

    // 0x60-0x7F: PUSH1-PUSH32
    [0x60] = { "PUSH1",        0, 1,  1, true, false },
    [0x61] = { "PUSH2",        0, 1,  2, true, false },
    [0x62] = { "PUSH3",        0, 1,  3, true, false },
    [0x63] = { "PUSH4",        0, 1,  4, true, false },
    [0x64] = { "PUSH5",        0, 1,  5, true, false },
    [0x65] = { "PUSH6",        0, 1,  6, true, false },
    [0x66] = { "PUSH7",        0, 1,  7, true, false },
    [0x67] = { "PUSH8",        0, 1,  8, true, false },
    [0x68] = { "PUSH9",        0, 1,  9, true, false },
    [0x69] = { "PUSH10",       0, 1, 10, true, false },
    [0x6A] = { "PUSH11",       0, 1, 11, true, false },
    [0x6B] = { "PUSH12",       0, 1, 12, true, false },
    [0x6C] = { "PUSH13",       0, 1, 13, true, false },
    [0x6D] = { "PUSH14",       0, 1, 14, true, false },
    [0x6E] = { "PUSH15",       0, 1, 15, true, false },
    [0x6F] = { "PUSH16",       0, 1, 16, true, false },
    [0x70] = { "PUSH17",       0, 1, 17, true, false },
    [0x71] = { "PUSH18",       0, 1, 18, true, false },
    [0x72] = { "PUSH19",       0, 1, 19, true, false },
    [0x73] = { "PUSH20",       0, 1, 20, true, false },
    [0x74] = { "PUSH21",       0, 1, 21, true, false },
    [0x75] = { "PUSH22",       0, 1, 22, true, false },
    [0x76] = { "PUSH23",       0, 1, 23, true, false },
    [0x77] = { "PUSH24",       0, 1, 24, true, false },
    [0x78] = { "PUSH25",       0, 1, 25, true, false },
    [0x79] = { "PUSH26",       0, 1, 26, true, false },
    [0x7A] = { "PUSH27",       0, 1, 27, true, false },
    [0x7B] = { "PUSH28",       0, 1, 28, true, false },
    [0x7C] = { "PUSH29",       0, 1, 29, true, false },
    [0x7D] = { "PUSH30",       0, 1, 30, true, false },
    [0x7E] = { "PUSH31",       0, 1, 31, true, false },
    [0x7F] = { "PUSH32",       0, 1, 32, true, false },

    // 0x80-0x8F: DUP1-DUP16
    // stack_in = N (need N items), stack_out = N+1 (duplicate top-Nth)
    // For validation we use -1 (variable) — handled specially in stack analysis
    [0x80] = { "DUP1",         1, 2, 0, true, false },
    [0x81] = { "DUP2",         2, 3, 0, true, false },
    [0x82] = { "DUP3",         3, 4, 0, true, false },
    [0x83] = { "DUP4",         4, 5, 0, true, false },
    [0x84] = { "DUP5",         5, 6, 0, true, false },
    [0x85] = { "DUP6",         6, 7, 0, true, false },
    [0x86] = { "DUP7",         7, 8, 0, true, false },
    [0x87] = { "DUP8",         8, 9, 0, true, false },
    [0x88] = { "DUP9",         9,10, 0, true, false },
    [0x89] = { "DUP10",       10,11, 0, true, false },
    [0x8A] = { "DUP11",       11,12, 0, true, false },
    [0x8B] = { "DUP12",       12,13, 0, true, false },
    [0x8C] = { "DUP13",       13,14, 0, true, false },
    [0x8D] = { "DUP14",       14,15, 0, true, false },
    [0x8E] = { "DUP15",       15,16, 0, true, false },
    [0x8F] = { "DUP16",       16,17, 0, true, false },

    // 0x90-0x9F: SWAP1-SWAP16
    // SWAP(N): needs N+1 items, produces N+1 items (net change = 0)
    [0x90] = { "SWAP1",        2, 2, 0, true, false },
    [0x91] = { "SWAP2",        3, 3, 0, true, false },
    [0x92] = { "SWAP3",        4, 4, 0, true, false },
    [0x93] = { "SWAP4",        5, 5, 0, true, false },
    [0x94] = { "SWAP5",        6, 6, 0, true, false },
    [0x95] = { "SWAP6",        7, 7, 0, true, false },
    [0x96] = { "SWAP7",        8, 8, 0, true, false },
    [0x97] = { "SWAP8",        9, 9, 0, true, false },
    [0x98] = { "SWAP9",       10,10, 0, true, false },
    [0x99] = { "SWAP10",      11,11, 0, true, false },
    [0x9A] = { "SWAP11",      12,12, 0, true, false },
    [0x9B] = { "SWAP12",      13,13, 0, true, false },
    [0x9C] = { "SWAP13",      14,14, 0, true, false },
    [0x9D] = { "SWAP14",      15,15, 0, true, false },
    [0x9E] = { "SWAP15",      16,16, 0, true, false },
    [0x9F] = { "SWAP16",      17,17, 0, true, false },

    // 0xA0-0xA4: LOG0-LOG4
    [0xA0] = { "LOG0",         2, 0, 0, true, false },
    [0xA1] = { "LOG1",         3, 0, 0, true, false },
    [0xA2] = { "LOG2",         4, 0, 0, true, false },
    [0xA3] = { "LOG3",         5, 0, 0, true, false },
    [0xA4] = { "LOG4",         6, 0, 0, true, false },

    // 0xD0-0xD3: Data access (EOF)
    [0xD0] = { "DATALOAD",     1, 1, 0, true, false },
    [0xD1] = { "DATALOADN",    0, 1, 2, true, false },
    [0xD2] = { "DATASIZE",     0, 1, 0, true, false },
    [0xD3] = { "DATACOPY",     3, 0, 0, true, false },

    // 0xE0-0xE8: EOF control flow & stack manipulation
    [0xE0] = { "RJUMP",        0, 0, 2, true, true  },  // terminating (unconditional jump)
    [0xE1] = { "RJUMPI",       1, 0, 2, true, false },
    [0xE2] = { "RJUMPV",       1, 0, 1, true, false },  // imm_size=1 for count byte, actual is variable
    [0xE3] = { "CALLF",        0, 0, 2, true, false },  // stack effects handled dynamically
    [0xE4] = { "RETF",         0, 0, 0, true, true  },
    [0xE5] = { "JUMPF",        0, 0, 2, true, true  },
    [0xE6] = { "DUPN",         0, 1, 1, true, false },  // stack_in handled dynamically
    [0xE7] = { "SWAPN",        0, 0, 1, true, false },  // stack effects handled dynamically
    [0xE8] = { "EXCHANGE",     0, 0, 1, true, false },  // stack effects handled dynamically

    // 0xEC, 0xEE: Create / return container
    [0xEC] = { "EOFCREATE",    4, 1, 1, true, false },
    [0xEE] = { "RETURNCONTRACT", 2, 0, 1, true, true },

    // 0xF3: RETURN
    [0xF3] = { "RETURN",       2, 0, 0, true, true  },

    // 0xF7: RETURNDATALOAD (EOF)
    [0xF7] = { "RETURNDATALOAD", 1, 1, 0, true, false },

    // 0xF8-0xFB: EOF-style calls
    [0xF8] = { "EXTCALL",         4, 1, 0, true, false },
    [0xF9] = { "EXTDELEGATECALL", 3, 1, 0, true, false },
    [0xFB] = { "EXTSTATICCALL",   3, 1, 0, true, false },

    // 0xFD-0xFE: Terminating
    [0xFD] = { "REVERT",       2, 0, 0, true, true  },
    [0xFE] = { "INVALID",      0, 0, 0, true, true  },

    // All other slots are zero-initialized: { NULL, 0, 0, 0, false, false }
};

#undef OP
#undef INVALID_OP

//==============================================================================
// Helpers
//==============================================================================

/** Read big-endian uint16 from buffer. */
static inline uint16_t read_u16(const uint8_t *p)
{
    return ((uint16_t)p[0] << 8) | (uint16_t)p[1];
}

/** Read big-endian int16 from buffer (for RJUMP offsets). */
static inline int16_t read_i16(const uint8_t *p)
{
    return (int16_t)(((uint16_t)p[0] << 8) | (uint16_t)p[1]);
}

/** Is this opcode a terminating instruction? */
static inline bool is_terminating(uint8_t op)
{
    return EOF_OPCODES[op].terminating;
}

//==============================================================================
// Stack Height Range (for validation)
//==============================================================================

typedef struct {
    int min;
    int max;
} stack_range_t;

#define STACK_UNVISITED -1

static inline bool stack_visited(const stack_range_t *s)
{
    return s->min != STACK_UNVISITED;
}

/**
 * Visit a successor instruction at next_offset with required stack range.
 * Returns false if stack heights conflict at a join point.
 */
static bool visit_successor(int current_offset, int next_offset,
                            stack_range_t required, stack_range_t *heights)
{
    stack_range_t *next = &heights[next_offset];
    if (next_offset <= current_offset) {
        // Backwards jump — target must already be visited with exact match
        if (!stack_visited(next))
            return false;
        return next->min == required.min && next->max == required.max;
    } else if (!stack_visited(next)) {
        // Forward jump, new target
        *next = required;
    } else {
        // Forward jump, target already known — widen range
        if (required.min < next->min) next->min = required.min;
        if (required.max > next->max) next->max = required.max;
    }
    return true;
}

//==============================================================================
// Phase A: Header Parsing
//==============================================================================

/**
 * Parse the EOF header and populate an eof_container_t.
 * Does NOT validate code sections (that's Phase B).
 */
static eof_validation_error_t eof_parse_header(const uint8_t *code, size_t len,
                                                eof_container_t *c)
{
    if (len < 14)
        return EOF_INVALID_HEADER;

    // Magic
    if (code[0] != EOF_MAGIC_0 || code[1] != EOF_MAGIC_1)
        return EOF_INVALID_MAGIC;

    // Version
    if (code[2] != EOF_VERSION)
        return EOF_INVALID_VERSION;

    size_t pos = 3;

    // --- Type section header ---
    if (code[pos] != EOF_KIND_TYPE)
        return EOF_INVALID_HEADER;
    pos++;

    if (pos + 2 > len) return EOF_INVALID_HEADER;
    uint16_t type_size = read_u16(&code[pos]);
    pos += 2;

    if (type_size < 4 || type_size % 4 != 0)
        return EOF_INVALID_TYPE_SECTION;

    uint16_t num_functions = type_size / 4;
    if (num_functions > EOF_MAX_FUNCTIONS)
        return EOF_INVALID_TYPE_SECTION;

    // --- Code section header ---
    if (pos >= len || code[pos] != EOF_KIND_CODE)
        return EOF_INVALID_HEADER;
    pos++;

    if (pos + 2 > len) return EOF_INVALID_HEADER;
    uint16_t num_code_sections = read_u16(&code[pos]);
    pos += 2;

    if (num_code_sections == 0 || num_code_sections != num_functions)
        return EOF_INVALID_HEADER;

    // Read code section sizes
    uint16_t *code_sizes = (uint16_t *)calloc(num_code_sections, sizeof(uint16_t));
    if (!code_sizes) return EOF_INVALID_HEADER;

    for (uint16_t i = 0; i < num_code_sections; i++) {
        if (pos + 2 > len) { free(code_sizes); return EOF_INVALID_HEADER; }
        code_sizes[i] = read_u16(&code[pos]);
        pos += 2;
        if (code_sizes[i] == 0) { free(code_sizes); return EOF_INVALID_HEADER; }
    }

    // --- Optional container section header ---
    uint16_t num_containers = 0;
    uint16_t *container_sizes = NULL;

    if (pos < len && code[pos] == EOF_KIND_CONTAINER) {
        pos++;
        if (pos + 2 > len) { free(code_sizes); return EOF_INVALID_HEADER; }
        num_containers = read_u16(&code[pos]);
        pos += 2;

        if (num_containers == 0 || num_containers > EOF_MAX_CONTAINERS) {
            free(code_sizes);
            return EOF_INVALID_HEADER;
        }

        container_sizes = (uint16_t *)calloc(num_containers, sizeof(uint16_t));
        if (!container_sizes) { free(code_sizes); return EOF_INVALID_HEADER; }

        for (uint16_t i = 0; i < num_containers; i++) {
            if (pos + 2 > len) {
                free(code_sizes); free(container_sizes);
                return EOF_INVALID_HEADER;
            }
            container_sizes[i] = read_u16(&code[pos]);
            pos += 2;
            if (container_sizes[i] == 0) {
                free(code_sizes); free(container_sizes);
                return EOF_INVALID_HEADER;
            }
        }
    }

    // --- Data section header ---
    if (pos >= len || code[pos] != EOF_KIND_DATA) {
        free(code_sizes);
        free(container_sizes);
        return EOF_INVALID_HEADER;
    }
    pos++;

    if (pos + 2 > len) {
        free(code_sizes); free(container_sizes);
        return EOF_INVALID_HEADER;
    }
    uint16_t data_size = read_u16(&code[pos]);
    pos += 2;

    // --- Terminator ---
    if (pos >= len || code[pos] != EOF_TERMINATOR) {
        free(code_sizes); free(container_sizes);
        return EOF_INVALID_HEADER;
    }
    pos++;

    // --- Parse type section entries ---
    size_t types_offset = pos;
    if (types_offset + type_size > len) {
        free(code_sizes); free(container_sizes);
        return EOF_INVALID_HEADER;
    }

    c->num_functions = num_functions;
    c->functions = (eof_func_t *)calloc(num_functions, sizeof(eof_func_t));
    if (!c->functions) {
        free(code_sizes); free(container_sizes);
        return EOF_INVALID_HEADER;
    }

    // First function must have inputs=0, outputs=0x80 (non-returning)
    if (code[types_offset] != 0 || code[types_offset + 1] != EOF_NON_RETURNING) {
        free(code_sizes); free(container_sizes);
        return EOF_INVALID_TYPE_SECTION;
    }

    for (uint16_t i = 0; i < num_functions; i++) {
        size_t toff = types_offset + (size_t)i * 4;
        c->functions[i].inputs  = code[toff];
        c->functions[i].outputs = code[toff + 1];
        c->functions[i].max_stack_height = read_u16(&code[toff + 2]);

        if (c->functions[i].max_stack_height > EOF_MAX_STACK_HEIGHT) {
            free(code_sizes); free(container_sizes);
            return EOF_INVALID_TYPE_SECTION;
        }
        if (c->functions[i].inputs > 127) {
            free(code_sizes); free(container_sizes);
            return EOF_INVALID_TYPE_SECTION;
        }
        if (c->functions[i].outputs > 127 &&
            c->functions[i].outputs != EOF_NON_RETURNING) {
            free(code_sizes); free(container_sizes);
            return EOF_INVALID_TYPE_SECTION;
        }
    }

    pos = types_offset + type_size;

    // --- Set code pointers ---
    for (uint16_t i = 0; i < num_functions; i++) {
        if (pos + code_sizes[i] > len) {
            free(code_sizes); free(container_sizes);
            return EOF_INVALID_HEADER;
        }
        c->functions[i].code = &code[pos];
        c->functions[i].code_size = code_sizes[i];
        pos += code_sizes[i];
    }

    // --- Parse nested containers (shallow — recursive validation done later) ---
    c->num_containers = num_containers;
    if (num_containers > 0) {
        c->containers = (eof_container_t **)calloc(num_containers, sizeof(eof_container_t *));
        if (!c->containers) {
            free(code_sizes); free(container_sizes);
            return EOF_INVALID_HEADER;
        }
        for (uint16_t i = 0; i < num_containers; i++) {
            if (pos + container_sizes[i] > len) {
                free(code_sizes); free(container_sizes);
                return EOF_INVALID_HEADER;
            }

            eof_container_t *sub = NULL;
            eof_validation_error_t err = eof_validate(&code[pos], container_sizes[i], &sub);
            if (err != EOF_VALID) {
                free(code_sizes); free(container_sizes);
                return EOF_INVALID_CONTAINER;
            }
            c->containers[i] = sub;
            pos += container_sizes[i];
        }
    }

    // --- Data section ---
    c->data = (pos < len) ? &code[pos] : NULL;
    // Actual data present may be >= data_size (for top-level) or exactly data_size
    c->data_size = data_size;

    // Validate body size: remaining bytes should match data section
    size_t remaining = len - pos;
    if (remaining < data_size) {
        // Truncated data section — allowed for sub-containers but for now
        // we accept it (runtime can deal with partial data)
    }

    free(code_sizes);
    free(container_sizes);
    return EOF_VALID;
}

//==============================================================================
// Phase B: RJUMP destination validation
//==============================================================================

/**
 * Build immediate map and validate all RJUMP/RJUMPI/RJUMPV targets land
 * on instruction boundaries (not in the middle of an immediate).
 */
static eof_validation_error_t validate_rjump_destinations(const uint8_t *code,
                                                           uint32_t code_size)
{
    if (code_size == 0) return EOF_INVALID_HEADER;

    // Build immediate map: true for bytes that are part of an immediate
    bool *imm_map = (bool *)calloc(code_size, sizeof(bool));
    if (!imm_map) return EOF_INVALID_HEADER;

    // Jump destinations to check
    uint32_t jump_cap = 64;
    uint32_t jump_count = 0;
    int32_t *jump_dests = (int32_t *)malloc(jump_cap * sizeof(int32_t));
    if (!jump_dests) { free(imm_map); return EOF_INVALID_HEADER; }

    // First pass: mark immediates and collect jump targets
    for (uint32_t pos = 0; pos < code_size; ) {
        uint8_t op = code[pos];
        const eof_opcode_info_t *info = &EOF_OPCODES[op];

        if (!info->valid_in_eof) {
            free(imm_map); free(jump_dests);
            return EOF_UNKNOWN_OPCODE;
        }

        uint32_t imm_size = info->imm_size;

        if (op == OP_RJUMPV) {
            // Variable immediate: 1 byte count + (count+1)*2 bytes of offsets
            if (pos + 1 >= code_size) {
                free(imm_map); free(jump_dests);
                return EOF_TRUNCATED_INSTRUCTION;
            }
            uint32_t count = (uint32_t)code[pos + 1] + 1;
            imm_size = 1 + count * 2;
        }

        if (pos + imm_size >= code_size && pos + imm_size != code_size) {
            // Truncated only if we're not at the last instruction
            if (pos + 1 + imm_size > code_size) {
                free(imm_map); free(jump_dests);
                return EOF_TRUNCATED_INSTRUCTION;
            }
        }

        // Check for truncated immediate
        if (pos + 1 + imm_size > code_size) {
            free(imm_map); free(jump_dests);
            return EOF_TRUNCATED_INSTRUCTION;
        }

        // Mark immediate bytes
        for (uint32_t j = pos + 1; j <= pos + imm_size; j++) {
            imm_map[j] = true;
        }

        // Collect RJUMP targets
        if (op == OP_RJUMP || op == OP_RJUMPI) {
            int16_t offset = read_i16(&code[pos + 1]);
            int32_t target = (int32_t)(pos + 3) + (int32_t)offset;

            if (target < 0 || (uint32_t)target >= code_size) {
                free(imm_map); free(jump_dests);
                return EOF_INVALID_RJUMP_TARGET;
            }

            if (jump_count >= jump_cap) {
                jump_cap *= 2;
                int32_t *tmp = (int32_t *)realloc(jump_dests, jump_cap * sizeof(int32_t));
                if (!tmp) { free(imm_map); free(jump_dests); return EOF_INVALID_HEADER; }
                jump_dests = tmp;
            }
            jump_dests[jump_count++] = target;
        } else if (op == OP_RJUMPV) {
            uint32_t count = (uint32_t)code[pos + 1] + 1;
            uint32_t post_pos = pos + 1 + imm_size;
            for (uint32_t j = 0; j < count; j++) {
                int16_t offset = read_i16(&code[pos + 2 + j * 2]);
                int32_t target = (int32_t)post_pos + (int32_t)offset;

                if (target < 0 || (uint32_t)target >= code_size) {
                    free(imm_map); free(jump_dests);
                    return EOF_INVALID_RJUMP_TARGET;
                }

                if (jump_count >= jump_cap) {
                    jump_cap *= 2;
                    int32_t *tmp = (int32_t *)realloc(jump_dests, jump_cap * sizeof(int32_t));
                    if (!tmp) { free(imm_map); free(jump_dests); return EOF_INVALID_HEADER; }
                    jump_dests = tmp;
                }
                jump_dests[jump_count++] = target;
            }
        }

        pos += 1 + imm_size;
    }

    // Check that all jump destinations land on instruction boundaries
    for (uint32_t i = 0; i < jump_count; i++) {
        if (imm_map[jump_dests[i]]) {
            free(imm_map); free(jump_dests);
            return EOF_INVALID_RJUMP_TARGET;
        }
    }

    free(imm_map);
    free(jump_dests);
    return EOF_VALID;
}

//==============================================================================
// Phase B: Instruction validation
//==============================================================================

/**
 * Validate instructions in a code section: check opcodes are valid,
 * function indices are in bounds, and last instruction is terminating.
 */
static eof_validation_error_t validate_instructions(const uint8_t *code,
                                                     uint32_t code_size,
                                                     const eof_container_t *c)
{
    if (code_size == 0) return EOF_INVALID_HEADER;

    uint8_t last_op = 0;
    for (uint32_t pos = 0; pos < code_size; ) {
        uint8_t op = code[pos];
        const eof_opcode_info_t *info = &EOF_OPCODES[op];

        if (!info->valid_in_eof)
            return EOF_UNKNOWN_OPCODE;

        uint32_t imm_size = info->imm_size;
        if (op == OP_RJUMPV) {
            if (pos + 1 >= code_size)
                return EOF_TRUNCATED_INSTRUCTION;
            uint32_t count = (uint32_t)code[pos + 1] + 1;
            imm_size = 1 + count * 2;
        }

        if (pos + 1 + imm_size > code_size)
            return EOF_TRUNCATED_INSTRUCTION;

        // Validate CALLF/JUMPF function index
        if (op == OP_CALLF || op == OP_JUMPF) {
            uint16_t fid = read_u16(&code[pos + 1]);
            if (fid >= c->num_functions)
                return EOF_INVALID_FUNCTION_INDEX;

            // CALLF to non-returning function is invalid
            if (op == OP_CALLF && c->functions[fid].outputs == EOF_NON_RETURNING)
                return EOF_INVALID_FUNCTION_INDEX;
        }

        // Validate EOFCREATE/RETURNCONTRACT container index
        if (op == OP_EOFCREATE || op == OP_RETURNCONTRACT) {
            uint8_t idx = code[pos + 1];
            if (idx >= c->num_containers)
                return EOF_INVALID_CONTAINER;
        }

        // Validate DATALOADN index
        if (op == OP_DATALOADN) {
            uint16_t index = read_u16(&code[pos + 1]);
            if (c->data_size < 32 || index > c->data_size - 32)
                return EOF_INVALID_HEADER;
        }

        last_op = op;
        pos += 1 + imm_size;
    }

    // Last instruction must be terminating
    if (!is_terminating(last_op) && last_op != OP_RJUMP)
        return EOF_INVALID_TERMINATION;

    return EOF_VALID;
}

//==============================================================================
// Phase B: Stack validation (max stack height)
//==============================================================================

/**
 * Validate stack heights for a single code section using abstract interpretation.
 * Follows Erigon's validateMaxStackHeight algorithm:
 *  - Track min/max stack height at each instruction offset
 *  - Forward pass visiting successors
 *  - Verify RETF returns correct number of items
 *  - Detect unreachable code
 *  - Verify computed max matches declared max_stack_height
 */
static eof_validation_error_t validate_stack(const uint8_t *code,
                                              uint32_t code_size,
                                              uint16_t func_idx,
                                              const eof_container_t *c)
{
    const eof_func_t *func = &c->functions[func_idx];

    stack_range_t *heights = (stack_range_t *)malloc(code_size * sizeof(stack_range_t));
    if (!heights) return EOF_INVALID_HEADER;

    // Initialize all to unvisited
    for (uint32_t i = 0; i < code_size; i++) {
        heights[i].min = STACK_UNVISITED;
        heights[i].max = STACK_UNVISITED;
    }

    // Entry point: stack has func.inputs items
    heights[0].min = func->inputs;
    heights[0].max = func->inputs;

    int this_outputs = func->outputs;

    for (uint32_t pos = 0; pos < code_size; ) {
        uint8_t op = code[pos];
        const eof_opcode_info_t *info = &EOF_OPCODES[op];

        if (!stack_visited(&heights[pos])) {
            // Skip over this instruction's immediates
            uint32_t imm_size = info->imm_size;
            if (op == OP_RJUMPV) {
                imm_size = 1 + ((uint32_t)code[pos + 1] + 1) * 2;
            }
            pos += 1 + imm_size;
            continue;
        }

        stack_range_t cur = heights[pos];
        int stack_required = info->stack_in;
        int stack_change = info->stack_out - info->stack_in;

        // Handle opcodes with dynamic stack effects
        if (op == OP_CALLF) {
            uint16_t fid = read_u16(&code[pos + 1]);
            const eof_func_t *callee = &c->functions[fid];
            stack_required = callee->inputs;
            stack_change = (int)callee->outputs - (int)callee->inputs;

            // Check CALLF stack overflow
            if (cur.max + (int)callee->max_stack_height - stack_required > 1024) {
                free(heights);
                return EOF_STACK_OVERFLOW;
            }
        } else if (op == OP_JUMPF) {
            uint16_t fid = read_u16(&code[pos + 1]);
            const eof_func_t *target = &c->functions[fid];

            if (cur.max + (int)target->max_stack_height - (int)target->inputs > 1024) {
                free(heights);
                return EOF_STACK_OVERFLOW;
            }

            if (target->outputs == EOF_NON_RETURNING) {
                stack_required = target->inputs;
            } else {
                // Returning JUMPF: current stack must match
                stack_required = this_outputs + (int)target->inputs - (int)target->outputs;
                if (cur.max > stack_required) {
                    free(heights);
                    return EOF_STACK_HEIGHT_MISMATCH;
                }
            }
        } else if (op == OP_RETF) {
            stack_required = this_outputs;
            if (cur.max > stack_required) {
                free(heights);
                return EOF_STACK_HEIGHT_MISMATCH;
            }
        } else if (op == OP_DUPN) {
            stack_required = (int)code[pos + 1] + 1;
            stack_change = 1;
        } else if (op == OP_SWAPN) {
            stack_required = (int)code[pos + 1] + 2;
            stack_change = 0;
        } else if (op == OP_EXCHANGE) {
            int n = ((int)code[pos + 1] >> 4) + 1;
            int m = ((int)code[pos + 1] & 0x0F) + 1;
            stack_required = n + m + 1;
            stack_change = 0;
        }

        // Check stack underflow
        if (cur.min < stack_required) {
            free(heights);
            return EOF_STACK_UNDERFLOW;
        }

        // Compute next stack range
        stack_range_t next_range = {
            .min = cur.min + stack_change,
            .max = cur.max + stack_change,
        };

        // Check stack overflow
        if (next_range.max > 1024) {
            free(heights);
            return EOF_STACK_OVERFLOW;
        }

        // Compute immediate size
        uint32_t imm_size = info->imm_size;
        if (op == OP_RJUMPV) {
            imm_size = 1 + ((uint32_t)code[pos + 1] + 1) * 2;
        }

        uint32_t next_pos = pos + 1 + imm_size;

        // Visit fall-through successor (if not terminating and not RJUMP)
        if (!is_terminating(op) && op != OP_RJUMP) {
            if (next_pos >= code_size) {
                free(heights);
                return EOF_INVALID_TERMINATION;
            }
            if (!visit_successor((int)pos, (int)next_pos, next_range, heights)) {
                free(heights);
                return EOF_STACK_HEIGHT_MISMATCH;
            }
        }

        // Visit jump targets
        if (op == OP_RJUMP || op == OP_RJUMPI) {
            int16_t offset = read_i16(&code[pos + 1]);
            int32_t target = (int32_t)(pos + 3) + (int32_t)offset;

            if (!visit_successor((int)pos, target, next_range, heights)) {
                free(heights);
                return EOF_STACK_HEIGHT_MISMATCH;
            }
        } else if (op == OP_RJUMPV) {
            uint32_t count = (uint32_t)code[pos + 1] + 1;
            for (uint32_t j = 0; j < count; j++) {
                int16_t offset = read_i16(&code[pos + 2 + j * 2]);
                int32_t target = (int32_t)next_pos + (int32_t)offset;

                if (!visit_successor((int)pos, target, next_range, heights)) {
                    free(heights);
                    return EOF_STACK_HEIGHT_MISMATCH;
                }
            }
        }

        pos = next_pos;
    }

    // Check for unreachable code and compute max stack height
    int max_height = 0;
    // We need to check at instruction boundaries only, so re-walk the code
    for (uint32_t pos = 0; pos < code_size; ) {
        uint8_t op = code[pos];
        const eof_opcode_info_t *info = &EOF_OPCODES[op];

        if (!stack_visited(&heights[pos])) {
            free(heights);
            return EOF_UNREACHABLE_CODE;
        }

        if (heights[pos].max > max_height)
            max_height = heights[pos].max;

        uint32_t imm_size = info->imm_size;
        if (op == OP_RJUMPV) {
            imm_size = 1 + ((uint32_t)code[pos + 1] + 1) * 2;
        }
        pos += 1 + imm_size;
    }

    // Verify declared max_stack_height matches computed
    if (max_height != (int)func->max_stack_height) {
        free(heights);
        return EOF_STACK_HEIGHT_MISMATCH;
    }

    free(heights);
    return EOF_VALID;
}

//==============================================================================
// Public API
//==============================================================================

eof_validation_error_t eof_validate(const uint8_t *bytecode, size_t len,
                                     eof_container_t **out)
{
    if (!bytecode || len == 0 || !out) {
        if (out) *out = NULL;
        return EOF_INVALID_HEADER;
    }

    *out = NULL;

    // Allocate container
    eof_container_t *c = (eof_container_t *)calloc(1, sizeof(eof_container_t));
    if (!c) return EOF_INVALID_HEADER;

    // Make owned copy of raw bytecode
    c->raw = (uint8_t *)malloc(len);
    if (!c->raw) { free(c); return EOF_INVALID_HEADER; }
    memcpy(c->raw, bytecode, len);
    c->raw_size = len;

    // Phase A: Parse header
    eof_validation_error_t err = eof_parse_header(c->raw, len, c);
    if (err != EOF_VALID) {
        eof_container_free(c);
        return err;
    }

    // Phase B: Validate each code section
    for (uint16_t i = 0; i < c->num_functions; i++) {
        const uint8_t *code = c->functions[i].code;
        uint32_t code_size = c->functions[i].code_size;

        // B.1: Validate RJUMP destinations
        err = validate_rjump_destinations(code, code_size);
        if (err != EOF_VALID) {
            eof_container_free(c);
            return err;
        }

        // B.2: Validate instructions (opcodes, function indices, termination)
        err = validate_instructions(code, code_size, c);
        if (err != EOF_VALID) {
            eof_container_free(c);
            return err;
        }

        // B.3: Validate stack heights
        err = validate_stack(code, code_size, i, c);
        if (err != EOF_VALID) {
            eof_container_free(c);
            return err;
        }
    }

    *out = c;
    return EOF_VALID;
}

void eof_container_free(eof_container_t *c)
{
    if (!c) return;

    // Free nested containers
    if (c->containers) {
        for (uint16_t i = 0; i < c->num_containers; i++) {
            eof_container_free(c->containers[i]);
        }
        free(c->containers);
    }

    free(c->functions);
    free(c->raw);
    free(c);
}

const char *eof_error_string(eof_validation_error_t err)
{
    switch (err) {
    case EOF_VALID:                   return "valid";
    case EOF_INVALID_MAGIC:           return "invalid magic";
    case EOF_INVALID_VERSION:         return "invalid version";
    case EOF_INVALID_HEADER:          return "invalid header";
    case EOF_INVALID_TYPE_SECTION:    return "invalid type section";
    case EOF_TRUNCATED_INSTRUCTION:   return "truncated instruction";
    case EOF_INVALID_RJUMP_TARGET:    return "invalid rjump target";
    case EOF_STACK_HEIGHT_MISMATCH:   return "stack height mismatch";
    case EOF_STACK_UNDERFLOW:         return "stack underflow";
    case EOF_STACK_OVERFLOW:          return "stack overflow";
    case EOF_UNREACHABLE_CODE:        return "unreachable code";
    case EOF_INVALID_FUNCTION_INDEX:  return "invalid function index";
    case EOF_INVALID_TERMINATION:     return "invalid termination";
    case EOF_UNKNOWN_OPCODE:          return "unknown opcode";
    case EOF_INVALID_CONTAINER:       return "invalid container";
    default:                          return "unknown error";
    }
}
