/**
 * EOF — EVM Object Format (EIP-3540 / EIP-7692)
 *
 * Structured bytecode container with:
 *   - Header: magic, version, section kinds/sizes
 *   - Type section: per-function input/output/max_stack_height
 *   - Code sections: one per function (validated at deploy time)
 *   - Container sections: nested EOF for EOFCREATE (optional)
 *   - Data section: constants, immutables
 *
 * Validation guarantees (checked once at deploy, trusted at runtime):
 *   - All opcodes are valid EOF opcodes
 *   - All RJUMP/RJUMPI/RJUMPV targets land on opcode boundaries
 *   - Stack height is statically determinable on every path
 *   - No stack underflow or overflow possible
 *   - Every code path terminates
 *   - No unreachable code
 *   - CALLF/JUMPF function indices are in bounds
 */

#ifndef ART_VM_EOF_H
#define ART_VM_EOF_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

//==============================================================================
// Constants
//==============================================================================

#define EOF_MAGIC_0         0xEF
#define EOF_MAGIC_1         0x00
#define EOF_VERSION         0x01

#define EOF_KIND_TYPE       0x01
#define EOF_KIND_CODE       0x02
#define EOF_KIND_CONTAINER  0x03
#define EOF_KIND_DATA       0x04
#define EOF_TERMINATOR      0x00

// Function outputs marker for non-returning functions (entry point)
#define EOF_NON_RETURNING   0x80

#define EOF_MAX_STACK_HEIGHT 1023
#define EOF_MAX_FUNCTIONS    1024
#define EOF_MAX_CONTAINERS   256

//==============================================================================
// EOF Opcode Definitions (for validation)
//==============================================================================

// Control flow
#define OP_STOP           0x00
#define OP_RJUMP          0xE0
#define OP_RJUMPI         0xE1
#define OP_RJUMPV         0xE2
#define OP_CALLF          0xE3
#define OP_RETF           0xE4
#define OP_JUMPF          0xE5

// Stack manipulation (new EOF)
#define OP_DUPN           0xE6
#define OP_SWAPN          0xE7
#define OP_EXCHANGE       0xE8

// Data access
#define OP_DATALOAD       0xD0
#define OP_DATALOADN      0xD1
#define OP_DATASIZE       0xD2
#define OP_DATACOPY       0xD3

// Create / return container
#define OP_EOFCREATE      0xEC
#define OP_RETURNCONTRACT 0xEE

// Calls (new EOF style)
#define OP_EXTCALL        0xF8
#define OP_EXTDELEGATECALL 0xF9
#define OP_EXTSTATICCALL  0xFB

// Terminating opcodes from EVM kept in EOF
#define OP_RETURN         0xF3
#define OP_REVERT         0xFD
#define OP_INVALID        0xFE

//==============================================================================
// Opcode Info (for validation)
//==============================================================================

typedef struct {
    const char *name;
    int8_t  stack_in;       // items consumed (-1 = variable)
    int8_t  stack_out;      // items produced (-1 = variable)
    uint8_t imm_size;       // bytes of immediate data after opcode
    bool    valid_in_eof;   // allowed in EOF container
    bool    terminating;    // ends a basic block (STOP, RETURN, RETF, JUMPF, etc.)
} eof_opcode_info_t;

/** Static table of all 256 opcodes. */
extern const eof_opcode_info_t EOF_OPCODES[256];

//==============================================================================
// EOF Container Types
//==============================================================================

/** Parsed EOF function descriptor. */
typedef struct {
    uint8_t        inputs;            // stack items consumed
    uint8_t        outputs;           // stack items produced (EOF_NON_RETURNING = non-returning)
    uint16_t       max_stack_height;  // max stack during execution
    const uint8_t *code;              // pointer into container's raw bytes (not owned)
    uint32_t       code_size;
} eof_func_t;

/** Parsed EOF container. */
typedef struct eof_container {
    uint8_t                  version;
    uint16_t                 num_functions;
    eof_func_t              *functions;       // heap-allocated array
    const uint8_t           *data;            // pointer into raw bytes
    uint32_t                 data_size;
    uint16_t                 num_containers;
    struct eof_container   **containers;      // nested containers (for EOFCREATE)
    uint8_t                 *raw;             // owned copy of full bytecode
    size_t                   raw_size;
} eof_container_t;

//==============================================================================
// Validation
//==============================================================================

typedef enum {
    EOF_VALID = 0,
    EOF_INVALID_MAGIC,
    EOF_INVALID_VERSION,
    EOF_INVALID_HEADER,
    EOF_INVALID_TYPE_SECTION,
    EOF_TRUNCATED_INSTRUCTION,
    EOF_INVALID_RJUMP_TARGET,
    EOF_STACK_HEIGHT_MISMATCH,
    EOF_STACK_UNDERFLOW,
    EOF_STACK_OVERFLOW,
    EOF_UNREACHABLE_CODE,
    EOF_INVALID_FUNCTION_INDEX,
    EOF_INVALID_TERMINATION,
    EOF_UNKNOWN_OPCODE,
    EOF_INVALID_CONTAINER,
} eof_validation_error_t;

/**
 * Validate raw EOF bytecode. On success, returns EOF_VALID and writes
 * a heap-allocated container to *out. On failure, *out is NULL.
 */
eof_validation_error_t eof_validate(const uint8_t *bytecode, size_t len,
                                     eof_container_t **out);

/** Free a parsed container (recursive for nested containers). */
void eof_container_free(eof_container_t *c);

/** Human-readable error string. */
const char *eof_error_string(eof_validation_error_t err);

#ifdef __cplusplus
}
#endif

#endif /* ART_VM_EOF_H */
