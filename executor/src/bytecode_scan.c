/**
 * Fast bytecode scanner — detect if contract makes external calls.
 *
 * Linear walk over bytecode, skips PUSH operands, checks for
 * CALL/CREATE family opcodes.
 */

#include "bytecode_scan.h"

/* Call-family opcodes */
#define OP_CREATE       0xF0
#define OP_CALL         0xF1
#define OP_CALLCODE     0xF2
#define OP_DELEGATECALL 0xF4
#define OP_CREATE2      0xF5
#define OP_STATICCALL   0xFA

/* PUSH range */
#define OP_PUSH1  0x60
#define OP_PUSH32 0x7F

static inline bool is_call_opcode(uint8_t op) {
    return op == OP_CREATE || op == OP_CALL || op == OP_CALLCODE ||
           op == OP_DELEGATECALL || op == OP_CREATE2 || op == OP_STATICCALL;
}

bool bytecode_has_calls(const uint8_t *code, size_t len) {
    if (!code || len == 0) return false;

    size_t i = 0;
    while (i < len) {
        uint8_t op = code[i];
        if (is_call_opcode(op))
            return true;
        if (op >= OP_PUSH1 && op <= OP_PUSH32)
            i += 1 + (op - OP_PUSH1 + 1);  /* skip push data */
        else
            i++;
    }
    return false;
}
